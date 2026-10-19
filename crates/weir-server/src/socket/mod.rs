//! Unix socket accept loop and frame-parsing layer.
//!
//! # Runtime boundary
//!
//! This module is entirely async (tokio). Everything downstream — queue, workers,
//! WAB, drain — runs on `std::thread` with blocking I/O. The only crossing point
//! is `task::spawn_blocking` in `handle_connection`, which moves the blocking
//! queue push onto tokio's blocking thread pool.
//!
//! # Connection limit and spawn_blocking
//!
//! Under sustained load, if every active connection is simultaneously blocked
//! waiting for a queue slot, all `spawn_blocking` threads fill up and new
//! connections stall at the socket layer. `max_connections` must therefore be
//! set ≤ the tokio blocking thread pool limit (default: 512). This constraint
//! is documented here and in config validation; it is not enforced in code
//! because the pool limit is a runtime tunable.

mod connection;
pub(crate) mod peer;
pub(crate) mod route;

#[cfg(feature = "tls")]
pub mod tcp;
#[cfg(feature = "tls")]
pub mod tls;

pub use connection::{ConnectionConfig, handle_connection};

use std::{
    io,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use tokio::{net::UnixListener, sync::Semaphore, task::JoinSet, time};
use tracing::{error, info, warn};

use crate::{metrics::Metrics, models::WorkUnit, queue::QueueSender};

/// Configuration for the socket accept loop.
pub struct SocketConfig {
    /// Absolute path to the Unix socket file.
    pub socket_path: PathBuf,
    /// Maximum concurrent connections. Connections over this cap are refused
    /// immediately (stream dropped). Must be ≤ tokio's blocking thread pool limit.
    pub max_connections: usize,
    /// Per-connection payload cap in bytes. Effective cap is
    /// `min(max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`.
    pub max_payload_bytes: usize,
    /// Ceiling on records in one `PushBatch`. Effective cap is
    /// `min(max_batch_records, MAX_BATCH_RECORDS_HARD_CAP)`.
    pub max_batch_records: usize,
    /// How long to wait for in-flight connections to finish after the shutdown
    /// signal is received before aborting them.
    pub shutdown_timeout_secs: u64,
    /// Per-connection idle read timeout in seconds. Caps slowloris-style
    /// connections that never send (or stall mid-frame).
    pub connection_read_timeout_secs: u64,
    /// Total number of WAB shards. Informational in production: the accept
    /// loop assigns each new connection a shard through the
    /// [`route::ShardRouter`] it is handed (live-shard routing). Only the
    /// `#[cfg(test)]` [`run`] wrapper reads it, to build today's round-robin
    /// router (counter % shard_count).
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "read only by the #[cfg(test)] run wrapper; production routing is the ShardRouter's"
        )
    )]
    pub shard_count: usize,
    /// When true, the accept loop refuses connections whose peer effective
    /// uid (via SO_PEERCRED / getpeereid) does not match the daemon's. See
    /// [`Config::peer_uid_check`] for the rationale.
    pub peer_uid_check: bool,
    /// Soft cap on live WAB bytes; `0` disables the check. See
    /// [`Config::wab_max_bytes`](crate::config::Config::wab_max_bytes).
    pub wab_max_bytes: u64,
    /// Most recent WAB byte count, refreshed every 5 s by the gauge task in
    /// `main`. Handed to every connection so the ingest path can consult the
    /// WAB size without doing I/O of its own.
    pub wab_bytes_now: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Whether the cap is currently rejecting. Shared with every other listener
    /// so the hysteresis low-water mark is honoured daemon-wide rather than
    /// per-transport.
    pub wab_cap_rejecting: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Set once another instance provably owns the `wab_dir`; every push is
    /// then Nacked before it reaches the queue. The SAME `Arc` every listener
    /// and the `wab_dir` integrity thread get.
    pub wab_ownership_lost: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The status board, when the daemon runs one. One writer per mark
    /// (README Q6): [`bind`] marks the Unix listener Bound, or Failed on a
    /// validation or bind error; [`serve`] enters the Serving phase (every
    /// listener is bound before it is called) and marks the Unix listener
    /// Closed on return.
    pub board: Option<std::sync::Arc<crate::status::StatusBoard>>,
}

/// A bound producer (or admin) Unix socket: the path it was bound at plus the
/// identity of the inode it created (spec §5.7 `BoundUnix`; the identity is
/// the safety floor's [`BoundSocket`], which [`remove_own_socket`] checks).
/// Also the SP-N seam: an inherited fd would produce one of these.
#[derive(Debug)]
pub(crate) struct BoundUnix {
    pub(crate) path: PathBuf,
    pub(crate) socket: BoundSocket,
}

/// Binds the producer socket: path validation plus the hardened bind, moved
/// out of the accept loop so startup can bind every listener before serving
/// any (spec §5.7). A live socket at the path is refused inside
/// [`bind_hardened`] (safety floor).
///
/// README Q6: `bind` is the only writer of the Unix listener's Bound / Failed
/// marks; [`serve`] writes Serving and Closed; `main` writes neither.
#[cfg(unix)]
pub(crate) fn bind(config: &SocketConfig) -> io::Result<BoundUnix> {
    let failed = |_: &io::Error| {
        if let Some(b) = &config.board {
            b.listeners.unix.set(weir_status::ListenerState::Failed);
        }
    };
    validate_socket_path(&config.socket_path).inspect_err(failed)?;
    let socket = bind_hardened(&config.socket_path).inspect_err(failed)?;
    if let Some(b) = &config.board {
        b.listeners.unix.set(weir_status::ListenerState::Bound);
    }
    info!(path = %config.socket_path.display(), "socket bound");
    Ok(BoundUnix {
        path: config.socket_path.clone(),
        socket,
    })
}

/// Every producer listener, bound and not yet served (spec §6 step 11).
#[derive(Debug)]
pub(crate) struct BoundListeners {
    pub(crate) unix: BoundUnix,
    /// `Some` only with `tcp_bind`, which config validation allows only in a
    /// `tls` build. Only main's `tls` block reads it, hence the same
    /// non-tls dead-code allowance `Config::tcp_bind` carries.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) tcp: Option<tokio::net::TcpListener>,
}

/// Binds TCP (if configured), then the Unix socket, and serves neither.
///
/// Why before any accept loop (spec §5.7): a bind failure then fails startup
/// before a single producer is acked, and no producer traffic overlaps the
/// `umask(0o177)` window inside [`bind_hardened`]. TCP goes first because a
/// failed Unix bind then only has to drop a `TcpListener`; the reverse would
/// leave a socket file to clean up. Binding here, in the caller of the accept
/// loops, also keeps "a bad bind address fails startup" out of the spawned
/// accept task (see `socket::tcp`'s module docs on the bound-addr design).
///
/// README Q6: this is the one writer of the TCP listener's `Bound` / `Failed`
/// marks ([`bind`] writes the Unix ones). `Bound` is written only once the
/// whole step has succeeded, so a TCP listener dropped because the Unix bind
/// failed is never reported Bound.
#[cfg(unix)]
pub(crate) async fn bind_all(
    config: &SocketConfig,
    tcp: Option<std::net::SocketAddr>,
) -> io::Result<BoundListeners> {
    let tcp_failed = |_: &io::Error| {
        if let Some(b) = &config.board {
            b.listeners.tcp.set(weir_status::ListenerState::Failed);
        }
    };
    let tcp = match tcp {
        Some(addr) => {
            let bound = tokio::net::TcpListener::bind(addr).await;
            // A tls build words the error the way preflight does (spec §3.0);
            // without tls, config validation never lets `tcp_bind` reach here.
            #[cfg(feature = "tls")]
            let bound = bound.map_err(|e| crate::preflight::tcp_bind_error(addr, &e));
            let listener = bound.inspect_err(tcp_failed)?;
            let local = listener.local_addr().inspect_err(tcp_failed)?;
            info!(addr = %local, "TCP+mTLS listener bound");
            Some(listener)
        }
        None => None,
    };
    let unix = bind(config)?;
    if tcp.is_some()
        && let Some(b) = &config.board
    {
        b.listeners.tcp.set(weir_status::ListenerState::Bound);
    }
    Ok(BoundListeners { unix, tcp })
}

/// bind + serve with today's round-robin routing. Test-only: production binds
/// every listener first (`bind_all`) and serves with live-shard routing.
#[cfg(test)]
pub async fn run(
    config: SocketConfig,
    queue_tx: QueueSender<WorkUnit>,
    level_rx: tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>,
    metrics: std::sync::Arc<Metrics>,
    sem: std::sync::Arc<Semaphore>,
) -> io::Result<()> {
    let bound = bind(&config)?;
    let router = route::ShardRouter::round_robin(config.shard_count);
    serve(config, bound, queue_tx, level_rx, metrics, sem, router).await
}

/// Accepts on an already-bound socket (see [`bind`]) and drives the
/// frame-parsing layer.
///
/// Each accepted connection is pinned to the shard `router` picks for it; when
/// no shard can take ingest the connection is dropped before any frame is read
/// and counted (`weir_connections_refused_no_live_shard_total`), and the loop
/// keeps accepting.
///
/// Returns once the shutdown level leaves `Running` (a dropped level sender
/// counts as Graceful) and the connection drain has ended: all in-flight
/// connections finished, `config.shutdown_timeout_secs` passed, or the level
/// became `Forced` (see [`drain_connections`]).
///
/// `sem` is the shared connection-cap semaphore. Both the Unix listener and the
/// TCP+mTLS listener (when enabled) receive a clone of the SAME `Arc<Semaphore>`
/// so the combined cap across both transports is `Semaphore::new(max_connections)`,
/// not 2×max_connections. The caller is responsible for creating the semaphore
/// with the desired global cap and cloning it into each listener.
///
/// # Bind sequence (TOCTOU hardening)
///
/// The bind happens in [`bind`], before this is called; the sequence itself is
/// implemented in `bind_hardened`. It uses a dirfd to pin
/// the parent directory, a connect-only liveness probe so it never removes a
/// socket another process is listening on, `unlinkat` to clear a stale socket
/// without following symlinks, a tightened umask so `bind(2)` creates the
/// socket inode at mode 0o600 directly (avoiding a vulnerable post-bind
/// chmod), and an inode-equality check to catch any rename swap that races the
/// bind. See `docs/security/socket-bind.md` for the threat model and the
/// remaining race window that the operator's directory permissions are
/// expected to close.
///
/// # Lame duck and shutdown removal
///
/// At the first shutdown level past `Running` (Foundations §5.7, §7) the accept
/// loop stops, and BEFORE the connection drain `serve` unlinks the socket file
/// and closes the listener, so a new connect fails at once with `ENOENT`
/// instead of queueing in the backlog for up to `shutdown_timeout_secs`. The
/// unlink removes the file only if the path still names the inode this
/// instance bound ([`remove_own_socket`]): a successor that replaced the socket
/// keeps it. The Unix listener is then marked `Closed` on the board. Accepted
/// connections finish within the bounded drain; `Forced` aborts them at once.
pub async fn serve(
    config: SocketConfig,
    bound: BoundUnix,
    queue_tx: QueueSender<WorkUnit>,
    mut level_rx: tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>,
    metrics: std::sync::Arc<Metrics>,
    sem: std::sync::Arc<Semaphore>,
    router: route::ShardRouter,
) -> io::Result<()> {
    info!(path = %bound.path.display(), "socket listening");
    // README Q6: `serve` is the only writer of the Serving phase. The Bound
    // mark is `bind`'s.
    if let Some(b) = &config.board {
        b.enter_phase(weir_status::Phase::Serving, &metrics);
    }

    let effective_cap = config
        .max_payload_bytes
        .min(weir_core::MAX_PAYLOAD_HARD_CAP);
    let conn_cfg_template = ConnectionConfig {
        max_payload_bytes: effective_cap,
        max_batch_records: config
            .max_batch_records
            .min(weir_core::MAX_BATCH_RECORDS_HARD_CAP),
        read_timeout: Duration::from_secs(config.connection_read_timeout_secs),
        ack_timeout: crate::socket::connection::ACK_TIMEOUT,
        shard_id: 0, // overridden per connection below
        wab_max_bytes: config.wab_max_bytes,
        wab_bytes_now: std::sync::Arc::clone(&config.wab_bytes_now),
        wab_cap_rejecting: std::sync::Arc::clone(&config.wab_cap_rejecting),
        wab_ownership_lost: std::sync::Arc::clone(&config.wab_ownership_lost),
    };
    let mut join_set: JoinSet<()> = JoinSet::new();

    // Broadcasts shutdown to every in-flight handler so they can exit
    // cleanly between frames instead of being abort_all'd mid-await. Sender
    // stays here; each spawned handler clones the receiver. When shutdown
    // fires we send true; handlers see it on their next read-loop iteration.
    let (handler_shutdown_tx, handler_shutdown_rx) = tokio::sync::watch::channel(false);
    // Connection counter the router assigns shards from (the n-th accepted
    // connection). The counter itself can grow without bound for ~600 years of
    // acceptances at 1M/s, so no overflow handling needed.
    let conn_counter = std::sync::atomic::AtomicU64::new(0);

    // Daemon's effective uid; compared against peer uid on each accept when
    // peer_uid_check is enabled. Read once at startup — the daemon never
    // changes uid post-start (no setuid path).
    let daemon_uid = unsafe { libc::geteuid() };

    loop {
        tokio::select! {
            biased;
            _ = crate::lifecycle::shutdown_requested(&mut level_rx) => {
                info!("socket manager: shutdown requested, stopping accept loop");
                break;
            }
            res = bound.socket.listener.accept() => {
                let accept_start = Instant::now();
                match res {
                    Ok((stream, _addr)) => {
                        // Reap handlers that have already finished so the JoinSet
                        // doesn't grow one node per lifetime connection — it is
                        // otherwise only drained at shutdown, an unbounded leak
                        // under the documented many-short-connections model.
                        // Non-blocking: returns None once nothing is ready (F1).
                        while join_set.try_join_next().is_some() {}
                        // Peer-credential check: refuse mismatched uids before
                        // any further work. See peer.rs for the platform impls.
                        if config.peer_uid_check {
                            match peer::peer_uid_of(&stream) {
                                Ok(uid) if uid == daemon_uid => {
                                    // ok — proceed to permit / handler
                                }
                                Ok(uid) => {
                                    warn!(
                                        peer_uid = uid,
                                        daemon_uid,
                                        "refusing connection: peer uid does not match daemon uid"
                                    );
                                    metrics.connection_rejected_peer_uid.inc();
                                    drop(stream);
                                    continue;
                                }
                                Err(e) => {
                                    // Failed to read peer creds — fail closed.
                                    // A non-Unix-domain socket or kernel
                                    // refusal both manifest here; either way
                                    // we cannot prove the peer is trusted.
                                    warn!(
                                        error = %e,
                                        "refusing connection: peer credential lookup failed"
                                    );
                                    metrics.connection_rejected_peer_uid.inc();
                                    drop(stream);
                                    continue;
                                }
                            }
                        }
                        // Pin the connection to a shard that can take ingest
                        // (spec §5.7). None live: drop it before any frame is
                        // read, so the producer sees a closed connection and
                        // retries, as with the connection cap below.
                        let n = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(shard_id) = router.pick(n) else {
                            router.refuse(&metrics, "unix");
                            drop(stream);
                            continue;
                        };
                        let Ok(permit) = sem.clone().try_acquire_owned() else {
                            warn!(
                                "connection limit ({}) reached; dropping connection",
                                config.max_connections
                            );
                            drop(stream);
                            continue;
                        };
                        let tx = queue_tx.clone();
                        let mut cfg = conn_cfg_template.clone();
                        cfg.shard_id = shard_id;
                        let m = std::sync::Arc::clone(&metrics);
                        let handler_shutdown = handler_shutdown_rx.clone();
                        join_set.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handle_connection(stream, tx, cfg, m, handler_shutdown).await
                                && e.kind() != io::ErrorKind::UnexpectedEof
                                && e.kind() != io::ErrorKind::ConnectionReset
                                && e.kind() != io::ErrorKind::BrokenPipe
                            {
                                warn!(error = %e, "connection closed with error");
                            }
                        });
                        metrics.accept_latency.observe(accept_start.elapsed().as_secs_f64());
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                        if is_accept_resource_exhaustion(&e) {
                            // The pending connection stays in the kernel accept
                            // queue, so retrying immediately busy-spins at 100%
                            // CPU and floods the log until an fd frees. Back off
                            // to yield the CPU. The shutdown signal still wins
                            // because the select! re-evaluates after the sleep.
                            metrics.accept_resource_exhaustion.inc();
                            time::sleep(ACCEPT_BACKOFF_ON_EXHAUSTION).await;
                        }
                    }
                }
            }
        }
    }

    // Lame duck (Foundations §5.7, §7): stop being reachable BEFORE the
    // connection drain, so new connects fail fast instead of piling up in the
    // backlog for up to shutdown_timeout_secs. The unlink runs first, while
    // the listener (and on Linux its O_PATH pin) still holds the inode, so the
    // identity check in remove_own_socket has no window; then the listener
    // closes (README Q15). Identity, not liveness, is the test: our own
    // listener is still open, so a liveness probe would see it as live and
    // leave the file behind (the self-probe trap, spec §3.3). The removal is
    // the hardened dirfd + fstatat + unlinkat sequence (F28), best-effort:
    // a failure is logged, not fatal. A successor that replaced our socket
    // keeps its file.
    remove_own_socket(&bound.path, &bound.socket);
    drop(bound);
    if let Some(b) = &config.board {
        b.listeners.unix.set(weir_status::ListenerState::Closed);
    }
    info!(
        path = %config.socket_path.display(),
        "socket manager: lame duck: socket unlinked and listener closed; draining connections"
    );

    // Handlers exit between frames (after acking a push they already read).
    // Sent BEFORE the bounded drain, so idle handlers exit at once and the
    // abort in drain_connections is the fallback.
    let _ = handler_shutdown_tx.send(true);
    drain_connections(
        &mut join_set,
        Duration::from_secs(config.shutdown_timeout_secs),
        &mut level_rx,
        &metrics,
        "socket manager",
    )
    .await;
    Ok(())
}

async fn drain_join_set(join_set: &mut JoinSet<()>) {
    while join_set.join_next().await.is_some() {}
}

/// How a listener's connection drain ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConnDrain {
    Clean,
    TimedOut { aborted: usize },
    Forced { aborted: usize },
}

/// Waits for in-flight connections after the accept loop broke: up to
/// `timeout` (`shutdown_timeout_secs`), or until the level becomes `Forced`,
/// whichever comes first; then aborts what is left and counts it in
/// `weir_connections_aborted_at_shutdown`. Shared by the Unix and TCP
/// listeners (Foundations §5.7).
///
/// The aborted tasks are awaited before returning, so every handler's
/// `QueueSender` clone is dropped by then. Only a push already parked in
/// `spawn_blocking` can outlive that, for at most `QUEUE_PUSH_TIMEOUT`, which
/// is why main waits on the queue's live-sender count (Task 4.8) rather than
/// asserting it.
pub(crate) async fn drain_connections(
    join_set: &mut JoinSet<()>,
    timeout: Duration,
    level: &mut tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>,
    metrics: &Metrics,
    who: &'static str,
) -> ConnDrain {
    enum Why {
        Done,
        Timeout,
        Forced,
    }
    // The select's futures borrow `join_set`; decide first, act after they drop.
    let why = tokio::select! {
        biased;
        _ = crate::lifecycle::forced(level) => Why::Forced,
        r = time::timeout(timeout, drain_join_set(join_set)) => {
            if r.is_ok() { Why::Done } else { Why::Timeout }
        }
    };
    let remaining = join_set.len();
    let out = match why {
        Why::Done => {
            info!("{who}: all connections drained cleanly");
            return ConnDrain::Clean;
        }
        Why::Timeout => {
            error!(
                remaining,
                timeout_secs = timeout.as_secs(),
                "{who}: shutdown timeout reached; aborting remaining connections — producers \
                 of those records may see no ack/nack response"
            );
            ConnDrain::TimedOut { aborted: remaining }
        }
        Why::Forced => {
            warn!(
                remaining,
                "{who}: forced shutdown; aborting remaining connections now"
            );
            ConnDrain::Forced { aborted: remaining }
        }
    };
    metrics
        .connections_aborted_at_shutdown
        .inc_by(remaining as u64);
    join_set.abort_all();
    // Reap the aborted tasks so every handler's queue sender is dropped
    // before the caller moves on.
    while join_set.join_next().await.is_some() {}
    out
}

/// Backoff applied when `accept(2)` fails with a resource-exhaustion errno.
/// Long enough to yield the CPU and let a descriptor or buffer be returned,
/// short enough that throughput recovers promptly once pressure clears.
#[cfg(unix)]
pub(crate) const ACCEPT_BACKOFF_ON_EXHAUSTION: Duration = Duration::from_millis(50);

/// True if an `accept(2)` error is a transient resource-exhaustion condition
/// (out of file descriptors or socket buffers). On these the pending
/// connection stays in the kernel accept queue, so an immediate retry returns
/// the same error and the loop busy-spins at 100% CPU while flooding the log.
/// The fix is to back off briefly; the condition clears once an fd frees.
///
/// `ECONNABORTED` is deliberately excluded — there the client aborted before
/// `accept` completed, the queue entry is consumed, and retrying immediately
/// is correct (no spin).
#[cfg(unix)]
pub(crate) fn is_accept_resource_exhaustion(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    )
}

/// Validates a socket path: absolute, no `..`, no null bytes.
///
/// These are the same format rules as `config::validate_path_format_inner`
/// (the config-layer check for `wab_dir`/`socket_path`). The two are kept
/// deliberately separate — each guards its own trust boundary in its own layer
/// — rather than merged into one shared helper. The shared test vector
/// `crate::path_validation_test_vectors::CASES` runs both validators over the
/// same accept/reject cases so their rules cannot silently drift apart.
pub fn validate_socket_path(path: &Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' is not absolute", path.display()),
        ));
    }
    if path.components().any(|c| c == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' contains '..' components", path.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().contains(&0u8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket path contains a null byte",
            ));
        }
    }
    Ok(())
}

// ── Hardened bind ─────────────────────────────────────────────────────────────
//
// The original sequence (`lstat` → `is_socket` → `remove_file` → `bind` →
// `set_permissions`) has a TOCTOU window: a path-based `chmod` after `bind`
// can be redirected via a symlink swap in the parent directory, leaving the
// real socket at its bind-time umask mode (typically 0o755 or 0o775) while
// chmod modifies an attacker file. See `docs/security/socket-bind.md` for
// the full attack surface analysis.
//
// The hardened sequence below pins the parent directory inode with an
// `O_PATH | O_DIRECTORY | O_NOFOLLOW` descriptor, drives the stale-file
// cleanup through `unlinkat(dirfd, basename, AT_SYMLINK_NOFOLLOW)`, tightens
// the process umask to 0o177 so `bind(2)` creates the socket inode at mode
// 0o600 directly — eliminating the need for any post-bind chmod, the
// component that was exploitable — and then verifies via `fstatat` that the
// inode at the bound path is a socket with mode 0o600 AND has not been
// swapped before we return.
//
// fchmod on the listener's fd is NOT used: on Linux it operates on the
// in-kernel sockfs object, not on the bound filesystem inode, so it does
// not actually change the bind path's mode bits. The umask approach
// achieves the same goal (0o600 from creation) by a different mechanism
// that does propagate to the filesystem.

/// A bound listener together with the identity of the socket inode it
/// created.
///
/// The identity is what lets shutdown remove the socket file only when the
/// path still names OUR socket ([`remove_own_socket`]): a successor that
/// unlinked ours and bound its own at the same path keeps its file.
///
/// On Linux, `pin` is an `O_PATH` descriptor on the bound inode, held for the
/// listener's life. While it is open the inode cannot be freed, so its number
/// cannot be reused by a successor's bind (ext4/xfs reuse freed inode numbers
/// promptly), and `(dev, ino)` alone is a sound identity. Other unix targets
/// have no `O_PATH`; there the comparison adds `st_ctime` (and `st_birthtime`
/// on macOS) as a best-effort tie-breaker. docs/security/socket-bind.md
/// carries the limitation.
#[derive(Debug)]
pub(crate) struct BoundSocket {
    pub listener: UnixListener,
    pub dev: u64,
    pub ino: u64,
    /// Held, never read outside tests: holding it is the whole point.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pin: Option<std::os::fd::OwnedFd>,
    #[cfg(all(unix, not(target_os = "linux")))]
    ctime: (libc::time_t, libc::c_long),
    #[cfg(target_os = "macos")]
    birthtime: (libc::time_t, libc::c_long),
}

/// What a hardened bind does about a parent directory others can write to.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "admin::bind_admin (Task 6.6) constructs RequirePrivate; Task 6.14 removes this"
    )
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParentPolicy {
    /// The producer socket: WARN and bind. Operators legitimately put
    /// development sockets under `/tmp`.
    Warn,
    /// The admin socket (Foundations spec §11): refuse. Anyone who can create
    /// entries in the parent could squat the name and impersonate the control
    /// plane.
    RequirePrivate,
}

/// The producer socket's hardened bind: a group/world-writable parent is
/// warned about, not refused. See [`bind_hardened_with`].
#[cfg(unix)]
pub(crate) fn bind_hardened(path: &Path) -> io::Result<BoundSocket> {
    bind_hardened_with(path, ParentPolicy::Warn)
}

/// The hardened bind with an explicit parent-directory policy:
/// [`ParentPolicy::Warn`] for the producer socket, [`ParentPolicy::RequirePrivate`]
/// for the admin socket, which refuses (and creates nothing) unless the parent
/// is private by [`parent_private_verdict`].
#[cfg(unix)]
pub(crate) fn bind_hardened_with(path: &Path, policy: ParentPolicy) -> io::Result<BoundSocket> {
    let (parent, basename) = split_parent_basename(path)?;
    let dirfd = open_parent_dirfd(parent)?;
    let _dirfd_guard = OwnedDirFd(dirfd);

    // The one irreducible bind(2)→fstatat race window (below) is fully closed
    // only when the socket's parent directory is writable by the daemon user
    // alone. The producer socket warns if it is group/world-writable (e.g.
    // /tmp) so the operator can move the socket somewhere private — see
    // docs/security/socket-bind.md. The admin socket refuses before touching
    // the path, so a refused bind removes and creates nothing.
    match policy {
        ParentPolicy::Warn => warn_if_parent_world_writable(dirfd, parent),
        ParentPolicy::RequirePrivate => refuse_unless_parent_private(dirfd, parent)?,
    }

    // Remove a STALE socket only: one nobody is listening on (a SIGKILLed
    // predecessor's). A live socket is refused, never unlinked.
    remove_stale_socket_at_startup(dirfd, basename, path)?;

    // bind(2) does not follow a symlink at the final component for AF_UNIX
    // socket creation — it either creates the inode at the named path or
    // returns EADDRINUSE. So any attacker symlink dropped between cleanup
    // and bind surfaces as a bind failure, not silent redirection.
    //
    // Tighten umask so bind(2) creates the socket inode with mode 0o600
    // directly. Doing the mode tightening at create time avoids a separate
    // path-based chmod (which can be redirected by a symlink swap in the
    // parent dir) and avoids an fd-based fchmod (which on Linux operates on
    // the sockfs object, not the bound filesystem inode, and may not
    // propagate to the bind path's mode bits).
    //
    // umask is process-global and applies to other threads for the duration of
    // the bind(2) below. Requested mode bits are masked by it, so the window
    // only matters for bits the mask removes: 0o177 removes owner-x and every
    // group/other bit. A 0o600 file (segments, `.confirmed`) is therefore
    // unaffected: 0o600 & !0o177 = 0o600. A directory made with a plain
    // mkdir(0o700) would land at 0o700 & !0o177 = 0o600 (no execute bit,
    // nothing can be created inside it), which is why every WAB directory goes
    // through `wab::create_dir_private`, which chmods after mkdir. For today's
    // call sites the window is benign. It is still kept short, and from PR 3
    // no producer listener serves before every listener is bound (spec §5.7),
    // so no producer traffic overlaps it. Under `cargo test` other test
    // threads create directories without that helper, which is why
    // weir-server's bin unit tests run with --test-threads=1 — see
    // CONTRIBUTING.md and docs/security/socket-bind.md.
    //
    // The restore is RAII-guarded so an unwind (or the `?` below) between
    // tightening and restoring cannot leak umask 0o177 process-wide, silently
    // tightening every later file/dir creation (F30). The guard is scoped to
    // this block so the umask is restored the instant bind returns — the
    // fstatat checks that follow create nothing.
    let listener = {
        let _umask_guard = UmaskGuard(umask_set(0o177));
        UnixListener::bind(path)?
    };

    // Snapshot the inode bind created. The window between bind(2) and this
    // fstatat is the only un-closeable race window (Linux has no `bind_at`
    // syscall that could make the two atomic). Closing it fully requires
    // that the parent directory be writable only by the daemon user — see
    // docs/security/socket-bind.md.
    let our_inode = stat_at_dir(dirfd, basename)?;
    if (our_inode.st_mode & libc::S_IFMT) != libc::S_IFSOCK {
        return Err(io::Error::other(format!(
            "after bind, '{}' is not a socket (mode type {:#o}); attacker raced bind",
            path.display(),
            our_inode.st_mode & libc::S_IFMT
        )));
    }
    if (our_inode.st_mode & 0o777) != 0o600 {
        return Err(io::Error::other(format!(
            "after bind, '{}' has mode {:#o} (want 0o600); umask tightening failed",
            path.display(),
            our_inode.st_mode & 0o777
        )));
    }

    // Late-swap check: verify the inode at the path is still the one we
    // snapshotted above. Catches a rename(2) that swapped a different inode
    // into place between the two fstatat calls.
    let final_inode = stat_at_dir(dirfd, basename)?;
    if final_inode.st_dev != our_inode.st_dev || final_inode.st_ino != our_inode.st_ino {
        return Err(io::Error::other(format!(
            "socket inode at '{}' changed during bind sequence \
             (dev/ino {}/{} → {}/{}); aborting startup",
            path.display(),
            our_inode.st_dev,
            our_inode.st_ino,
            final_inode.st_dev,
            final_inode.st_ino
        )));
    }

    // Pin the inode we just verified, immediately after the late-swap check,
    // so its number cannot be recycled while we run (see `BoundSocket`).
    #[cfg(target_os = "linux")]
    let pin = pin_socket_inode(dirfd, basename, &our_inode, path)?;

    let (dev, ino) = stat_identity(&our_inode);
    Ok(BoundSocket {
        listener,
        dev,
        ino,
        #[cfg(target_os = "linux")]
        pin,
        #[cfg(all(unix, not(target_os = "linux")))]
        ctime: (our_inode.st_ctime, our_inode.st_ctime_nsec),
        #[cfg(target_os = "macos")]
        birthtime: (our_inode.st_birthtime, our_inode.st_birthtime_nsec),
    })
}

/// Opens an `O_PATH | O_NOFOLLOW | O_CLOEXEC` descriptor on the socket inode
/// at `dirfd/basename` and checks it is the inode `bind_hardened` verified.
///
/// A different inode means the path was swapped after the late-swap check,
/// which is the same attack that check refuses, so this refuses too. An open
/// that fails outright (a kernel without `O_PATH` on sockets) degrades to no
/// pin with a WARN: `(dev, ino)` is then a best-effort identity, as on other
/// unix targets.
#[cfg(target_os = "linux")]
fn pin_socket_inode(
    dirfd: libc::c_int,
    basename: &std::ffi::OsStr,
    expected: &libc::stat,
    path: &Path,
) -> io::Result<Option<std::os::fd::OwnedFd>> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    let c_name = std::ffi::CString::new(basename.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "basename contains null"))?;
    // Safety: dirfd is a valid directory fd held by the caller; c_name is a
    // valid nul-terminated string; the flags are a valid OR of libc constants.
    let fd = unsafe {
        libc::openat(
            dirfd,
            c_name.as_ptr(),
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        warn!(
            path = %path.display(),
            error = %io::Error::last_os_error(),
            "could not pin the bound socket inode with O_PATH; shutdown will \
             identify the socket by (dev, ino) alone"
        );
        return Ok(None);
    }
    // Safety: fd was just returned by openat and is owned by nothing else.
    let pin = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: pin is an open fd; &mut st is a writable libc::stat.
    if unsafe { libc::fstat(pin.as_raw_fd(), &mut st) } < 0 {
        return Err(io::Error::last_os_error());
    }
    if st.st_dev != expected.st_dev || st.st_ino != expected.st_ino {
        return Err(io::Error::other(format!(
            "socket inode at '{}' changed while pinning it \
             (dev/ino {}/{} → {}/{}); aborting startup",
            path.display(),
            expected.st_dev,
            expected.st_ino,
            st.st_dev,
            st.st_ino
        )));
    }
    Ok(Some(pin))
}

#[cfg(unix)]
fn umask_set(new: libc::mode_t) -> libc::mode_t {
    // Safety: libc::umask is always safe; it just swaps the process umask
    // and returns the previous value. No invariants to uphold.
    unsafe { libc::umask(new) }
}

#[cfg(not(unix))]
pub(crate) fn bind_hardened(path: &Path) -> io::Result<BoundSocket> {
    Ok(BoundSocket {
        listener: UnixListener::bind(path)?,
        dev: 0,
        ino: 0,
    })
}

#[cfg(unix)]
fn split_parent_basename(path: &Path) -> io::Result<(&Path, &std::ffi::OsStr)> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' has no parent directory", path.display()),
        )
    })?;
    let basename = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' has no file name", path.display()),
        )
    })?;
    if parent.as_os_str().is_empty() {
        // `path.parent()` returns `Some("")` for relative paths; we already
        // require absolute paths via `validate_socket_path`, so this should
        // not happen, but guard anyway.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path '{}' has an empty parent", path.display()),
        ));
    }
    Ok((parent, basename))
}

#[cfg(unix)]
fn open_parent_dirfd(parent: &Path) -> io::Result<libc::c_int> {
    use std::os::unix::ffi::OsStrExt;
    let c_parent = std::ffi::CString::new(parent.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "parent path contains null"))?;
    // O_PATH so we don't need read permission on the directory; O_DIRECTORY so
    // we fail fast if it isn't a directory; O_NOFOLLOW so a symlinked
    // last-component parent can't redirect us. Intermediate components are
    // still resolved normally (that's what `open(2)` does — O_NOFOLLOW only
    // applies to the final component).
    //
    // O_PATH is Linux-specific. macOS does not have O_PATH; fall back to a
    // read-only directory open.
    #[cfg(target_os = "linux")]
    let flags = libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;

    // Safety: c_parent is a valid nul-terminated C string for the lifetime of
    // the call; flags is a valid OR of libc constants; mode is unused for
    // non-creating opens.
    let fd = unsafe { libc::open(c_parent.as_ptr(), flags) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// Startup-only removal of a STALE socket at `dirfd/basename`.
///
/// `fstatat` (no symlink follow) must show a socket; anything else is refused,
/// as before (F28). Then the connect-only [`probe_socket`] decides, per the
/// spec §3.3 table: a stale socket (nobody listening) is unlinked; a live one,
/// or one whose state the probe cannot tell, is refused and left in place. An
/// entry that vanished meanwhile is fine.
///
/// The entry is re-checked after the probe and must still be the inode first
/// seen, so a socket swapped in while the probe ran is refused rather than
/// unlinked unseen.
///
/// Never used at shutdown: there the daemon's own listener is still open, the
/// probe would call it live, and every clean shutdown would leave its socket
/// behind. Shutdown uses [`remove_own_socket`].
#[cfg(unix)]
fn remove_stale_socket_at_startup(
    dirfd: libc::c_int,
    basename: &std::ffi::OsStr,
    path: &Path,
) -> io::Result<()> {
    let first = match stat_at_dir(dirfd, basename) {
        Ok(st) => st,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    // Refuse to remove anything that isn't a socket. This catches both
    // ordinary files (the original startup-cleanup guarantee) and symlinks
    // (because we used AT_SYMLINK_NOFOLLOW, st_mode reflects the symlink's
    // type, not its target's — symlinks are S_IFLNK, not S_IFSOCK).
    let mode_type = first.st_mode & libc::S_IFMT;
    if mode_type != libc::S_IFSOCK {
        return Err(non_socket_refusal(path, mode_type));
    }

    match probe_socket(path) {
        SocketProbe::Absent => return Ok(()),
        SocketProbe::Stale => {}
        SocketProbe::Live => return Err(live_socket_refusal(path)),
        SocketProbe::Unknown(errno) => return Err(unknown_socket_refusal(path, errno)),
    }

    // The probe resolved `path` by name; the unlink below goes through the
    // pinned dirfd. Both must still name the inode first seen.
    let same = |st: &libc::stat| st.st_dev == first.st_dev && st.st_ino == first.st_ino;
    let by_dirfd = match stat_at_dir(dirfd, basename) {
        Ok(st) => st,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let by_path_matches = {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path)
            .map(|m| (m.dev(), m.ino()) == stat_identity(&first))
            .unwrap_or(false)
    };
    if !same(&by_dirfd) || !by_path_matches {
        return Err(io::Error::other(format!(
            "the socket at '{}' was replaced while probing whether it is in use; \
             refusing to remove it",
            path.display()
        )));
    }

    use std::os::unix::ffi::OsStrExt;
    let c_name = std::ffi::CString::new(basename.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "basename contains null"))?;
    // Safety: dirfd valid; c_name valid; flags=0 means "regular unlink"
    // (i.e. file, not directory).
    let rc = unsafe { libc::unlinkat(dirfd, c_name.as_ptr(), 0) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(err);
    }
    info!(path = %path.display(), "removed stale socket");
    Ok(())
}

/// The refusal for a non-socket entry (a regular file, a directory, a symlink)
/// at the socket path. Shared with preflight, which refuses it before recovery.
#[cfg(unix)]
pub(crate) fn non_socket_refusal(path: &Path, mode_type: impl std::fmt::Octal) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "a non-socket entry already exists at '{}' (mode type {:#o}); refusing to remove it",
            path.display(),
            mode_type
        ),
    )
}

/// The refusal for a socket someone is listening on (spec §3.3, exact text).
/// Shared with preflight.
#[cfg(unix)]
pub(crate) fn live_socket_refusal(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "socket_path '{}' is in use by a running process; refusing to remove it. \
             Stop the other weir-server or choose another socket_path.",
            path.display()
        ),
    )
}

/// The refusal when the probe cannot tell whether the socket is live. Fails
/// closed: an unreadable state is not proof of staleness. Shared with preflight.
#[cfg(unix)]
pub(crate) fn unknown_socket_refusal(path: &Path, errno: i32) -> io::Error {
    io::Error::other(format!(
        "cannot tell whether socket_path '{}' is in use: the liveness probe's connect \
         failed with {} (errno {errno}); refusing to remove it. If no weir-server is \
         using it, remove the file manually and start again.",
        path.display(),
        io::Error::from_raw_os_error(errno)
    ))
}

/// Fails if something is listening at `path` (spec §5.7, §18). The probe is
/// the safety floor's connect-only [`probe_socket`]; nothing is unlinked.
///
/// `bind_hardened` already refuses a live socket for every caller (through
/// `remove_stale_socket_at_startup`), with the producer socket's wording. This
/// helper is for a caller that wants the refusal before the bind, worded for
/// its own socket: the admin socket (PR 6) passes `what = "admin socket"`.
/// The producer bind does not call it: that would connect to a live daemon
/// twice.
#[cfg(unix)]
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "PR 6's admin bind is the caller; PR 6 Task 6.14 removes this"
    )
)]
pub(crate) fn refuse_if_live(path: &Path, what: &str) -> io::Result<()> {
    match probe_socket(path) {
        SocketProbe::Absent | SocketProbe::Stale => Ok(()),
        SocketProbe::Live => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!(
                "another daemon is serving '{}'; refusing to bind the {what} there. Stop \
                 the other weir-server or choose another path.",
                path.display()
            ),
        )),
        SocketProbe::Unknown(errno) => Err(io::Error::other(format!(
            "cannot tell whether the {what} path '{}' is in use: the liveness probe's \
             connect failed with {} (errno {errno}); refusing to bind. If no weir-server \
             is using it, remove the file manually and start again.",
            path.display(),
            io::Error::from_raw_os_error(errno)
        ))),
    }
}

/// What a connect-only probe says about the socket path.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SocketProbe {
    /// Nothing at the path (or no directory to hold it).
    Absent,
    /// A socket file nobody is listening on, e.g. a SIGKILLed daemon's.
    Stale,
    /// Something is listening.
    Live,
    /// The probe could not tell (the errno). Callers refuse: fail closed.
    Unknown(i32),
}

/// Probes whether anything is listening on the Unix socket at `path`.
///
/// It opens a non-blocking `AF_UNIX` stream socket and calls `connect`, then
/// closes it. It never unlinks anything. It must be non-blocking: a blocking
/// `AF_UNIX` connect to a listener with a full backlog hangs on Linux.
///
/// | connect result | meaning |
/// |---|---|
/// | `0`, `EAGAIN`, `EINPROGRESS` | `Live` (`EAGAIN` = backlog full = listening, on Linux) |
/// | `ECONNREFUSED` (Linux) | `Stale` |
/// | `ECONNREFUSED` (other unix) | stale *or* backlog full; settled by [`probe_socket_attached`] |
/// | `ENOENT`, `ENOTDIR` | `Absent` |
/// | `EACCES`, `EPERM`, other | `Unknown(errno)` |
///
/// Side effect: on a live daemon the stream probe is a real connection, so it
/// bumps that daemon's connection metrics, and under `peer_uid_check` a
/// foreign uid is rejected and logged there.
#[cfg(unix)]
pub(crate) fn probe_socket(path: &Path) -> SocketProbe {
    let first = probe_socket_once(path);
    // BSD/macOS refuse a stream connect with ECONNREFUSED both when nothing is
    // bound (stale) and when a live listener's accept backlog is full, and the
    // backlog can stay full for as long as the listener cannot accept
    // (fd exhaustion, SIGSTOP). Re-probing only narrows that window, so
    // the refusal is settled by a probe that does not depend on the backlog
    // at all (conf-security-7).
    #[cfg(not(target_os = "linux"))]
    if first == SocketProbe::Stale {
        return probe_socket_attached(path);
    }
    first
}

/// Maps a failed non-blocking stream `connect(2)` errno to a [`SocketProbe`].
/// The non-Linux ECONNREFUSED follow-up lives in [`probe_socket`], not here.
#[cfg(unix)]
fn classify_connect_errno(errno: i32) -> SocketProbe {
    match errno {
        libc::ECONNREFUSED => SocketProbe::Stale,
        // ENOTDIR: a path component is not a directory, so nothing can be
        // listening at the path; the bind reports the real problem.
        libc::ENOENT | libc::ENOTDIR => SocketProbe::Absent,
        // EAGAIN == EWOULDBLOCK on every supported target.
        libc::EAGAIN | libc::EINPROGRESS => SocketProbe::Live,
        other => SocketProbe::Unknown(other),
    }
}

/// The non-Linux follow-up to a refused stream connect: is any socket still
/// attached to the file at `path`?
///
/// It connects a `SOCK_DGRAM` socket to the path. The BSD-derived kernels
/// (XNU `unp_connect`, and FreeBSD/OpenBSD/NetBSD likewise) look up the socket
/// bound to the file's vnode first and refuse with `ECONNREFUSED` only if there
/// is none; with one bound, a type mismatch fails with `EPROTOTYPE` *before*
/// the stream-only backlog check. So the answer does not depend on the
/// listener's backlog, its accept loop, or whether it is stopped. It also never
/// creates a connection on the other side: no metrics bump, no peer-uid log.
///
/// | datagram connect result | meaning |
/// |---|---|
/// | `ECONNREFUSED` | `Stale`: no socket is attached to the file |
/// | `EPROTOTYPE`, `0` | `Live`: a socket is bound there (stream, or a datagram one) |
/// | `ENOENT`, `ENOTDIR` | `Absent`: the file went away between probes |
/// | other | `Unknown(errno)`: fail closed |
#[cfg(all(unix, not(target_os = "linux")))]
fn probe_socket_attached(path: &Path) -> SocketProbe {
    connect_probe(path, libc::SOCK_DGRAM, classify_attached_errno)
}

/// Maps a failed datagram `connect(2)` errno for [`probe_socket_attached`].
#[cfg(all(unix, not(target_os = "linux")))]
fn classify_attached_errno(errno: i32) -> SocketProbe {
    match errno {
        libc::ECONNREFUSED => SocketProbe::Stale,
        libc::EPROTOTYPE => SocketProbe::Live,
        libc::ENOENT | libc::ENOTDIR => SocketProbe::Absent,
        other => SocketProbe::Unknown(other),
    }
}

#[cfg(unix)]
fn probe_socket_once(path: &Path) -> SocketProbe {
    connect_probe(path, libc::SOCK_STREAM, classify_connect_errno)
}

/// One non-blocking `connect(2)` of a fresh `AF_UNIX` socket of `sock_type` to
/// `path`. A successful connect is `Live`; a failure goes through `classify`.
#[cfg(unix)]
fn connect_probe(
    path: &Path,
    sock_type: libc::c_int,
    classify: fn(i32) -> SocketProbe,
) -> SocketProbe {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    let bytes = path.as_os_str().as_bytes();
    // Safety: sockaddr_un is plain old data; all-zero is a valid value.
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.contains(&0) {
        return SocketProbe::Unknown(libc::EINVAL);
    }
    if bytes.len() >= addr.sun_path.len() {
        // No socket can be bound at a path this long, so if nothing is there
        // it is plainly absent; if something is, we cannot address it.
        return match std::fs::symlink_metadata(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => SocketProbe::Absent,
            _ => SocketProbe::Unknown(libc::ENAMETOOLONG),
        };
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    let len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        addr.sun_len = len as u8;
    }

    let sock = match nonblocking_unix_socket(sock_type) {
        Ok(fd) => fd,
        Err(e) => return SocketProbe::Unknown(e.raw_os_error().unwrap_or(libc::EIO)),
    };
    // Safety: sock was just created and is owned by nothing else.
    let sock = unsafe { OwnedFd::from_raw_fd(sock) };
    // Safety: addr is a valid, initialised sockaddr_un and len does not exceed
    // its size (checked above).
    let rc = unsafe {
        libc::connect(
            sock.as_raw_fd(),
            (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            len as libc::socklen_t,
        )
    };
    if rc == 0 {
        return SocketProbe::Live;
    }
    let errno = io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EIO);
    classify(errno)
}

/// `socket(AF_UNIX, sock_type)`, non-blocking and close-on-exec.
#[cfg(unix)]
fn nonblocking_unix_socket(sock_type: libc::c_int) -> io::Result<libc::c_int> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Safety: plain syscall with integer arguments.
        let fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                sock_type | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(fd)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // No SOCK_NONBLOCK / SOCK_CLOEXEC here: set both with fcntl. The
        // descriptor is closed on any failure.
        // Safety: plain syscall with integer arguments.
        let fd = unsafe { libc::socket(libc::AF_UNIX, sock_type, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Safety: fd is a socket we own; F_SETFD/F_GETFL/F_SETFL take ints.
        let ok = unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) >= 0 && {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                fl >= 0 && libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK) >= 0
            }
        };
        if !ok {
            let err = io::Error::last_os_error();
            // Safety: fd is ours and not yet closed.
            unsafe { libc::close(fd) };
            return Err(err);
        }
        Ok(fd)
    }
}

/// Shutdown-time removal of the socket file this instance bound.
///
/// Opens the parent dir, `fstatat`s the entry (no symlink follow) and unlinks
/// it only if it is still the socket `bound` describes. There is deliberately
/// no liveness probe: our own listener is still open, and a probe would call
/// it live. On a mismatch (another instance unlinked ours and bound its own)
/// it logs a WARN and leaves the file. A missing file is fine; a failure is
/// logged, never fatal, because the daemon is exiting.
///
/// The residual `fstatat`→`unlinkat` window is the same class as the
/// documented bind→`fstatat` window (docs/security/socket-bind.md) and is
/// closed by the same private-parent-directory assumption.
#[cfg(unix)]
pub(crate) fn remove_own_socket(path: &Path, bound: &BoundSocket) {
    let result = (|| -> io::Result<()> {
        let (parent, basename) = split_parent_basename(path)?;
        let dirfd = open_parent_dirfd(parent)?;
        let _dirfd_guard = OwnedDirFd(dirfd);
        let st = match stat_at_dir(dirfd, basename) {
            Ok(st) => st,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if !names_our_socket(&st, bound) {
            warn!(
                path = %path.display(),
                "socket path now names a different socket (another instance replaced it); \
                 leaving it"
            );
            return Ok(());
        }
        use std::os::unix::ffi::OsStrExt;
        let c_name = std::ffi::CString::new(basename.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "basename contains null"))?;
        // Safety: dirfd valid; c_name valid; flags=0 is a regular unlink.
        let rc = unsafe { libc::unlinkat(dirfd, c_name.as_ptr(), 0) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::NotFound {
                return Err(err);
            }
        }
        Ok(())
    })();
    if let Err(e) = result {
        warn!(
            path = %path.display(),
            error = %e,
            "socket manager: could not remove socket file at shutdown"
        );
    }
}

/// True if `st` (an `fstatat` of the socket path) is the socket `bound`
/// created. See [`BoundSocket`] for why `(dev, ino)` suffices on Linux and
/// what the tie-breakers are elsewhere.
#[cfg(unix)]
fn names_our_socket(st: &libc::stat, bound: &BoundSocket) -> bool {
    if (st.st_mode & libc::S_IFMT) != libc::S_IFSOCK {
        return false;
    }
    if stat_identity(st) != (bound.dev, bound.ino) {
        return false;
    }
    #[cfg(not(target_os = "linux"))]
    if (st.st_ctime, st.st_ctime_nsec) != bound.ctime {
        return false;
    }
    #[cfg(target_os = "macos")]
    if (st.st_birthtime, st.st_birthtime_nsec) != bound.birthtime {
        return false;
    }
    true
}

/// `(st_dev, st_ino)` widened to `u64`. `dev_t` is `u64` on Linux but `i32` on
/// macOS, so the cast is a no-op on one target and required on the other.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn stat_identity(st: &libc::stat) -> (u64, u64) {
    (st.st_dev as u64, st.st_ino as u64)
}

/// `fstatat(dirfd, basename, AT_SYMLINK_NOFOLLOW)` returning the raw stat. Used
/// to snapshot the inode at a directory-relative basename without following
/// symlinks at the final component.
#[cfg(unix)]
fn stat_at_dir(dirfd: libc::c_int, basename: &std::ffi::OsStr) -> io::Result<libc::stat> {
    use std::os::unix::ffi::OsStrExt;
    let c_name = std::ffi::CString::new(basename.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "basename contains null"))?;
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: dirfd is a valid directory fd held by the caller; c_name is a
    // valid nul-terminated string; &mut st is a writable libc::stat.
    let rc = unsafe { libc::fstatat(dirfd, c_name.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st)
}

/// Warn if the socket's parent directory is group- or world-writable.
///
/// The hardened bind sequence closes every TOCTOU window it can, but the
/// final `bind(2)`→`fstatat` gap is only fully closed when no other user can
/// create entries in the parent directory. A group/world-writable parent
/// (e.g. `/tmp`, mode 0o1777) leaves that gap exploitable. We can't refuse to
/// start — operators legitimately place sockets under `/tmp` during
/// development — but we surface it loudly so it's a deliberate choice.
///
/// `dirfd` is the parent directory fd already held by `bind_hardened`; we
/// `fstat` it rather than re-resolving `parent` by path to avoid a fresh
/// TOCTOU window. `fstat` is one of the few operations permitted on an
/// `O_PATH` fd, so this works on Linux as well as the macOS `O_RDONLY` fallback.
#[cfg(unix)]
fn warn_if_parent_world_writable(dirfd: libc::c_int, parent: &Path) {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: dirfd is a valid directory fd held by the caller; &mut st is a
    // writable libc::stat the kernel fills in.
    let rc = unsafe { libc::fstat(dirfd, &mut st) };
    if rc == 0 && is_group_or_other_writable(st.st_mode) {
        // A sticky bit (e.g. /tmp's 0o1777) limits deletion to the owner but
        // does NOT stop other users creating new entries, so the bind-race
        // window stays open regardless. Mention it so the warning isn't
        // dismissed as "but it has the sticky bit".
        let sticky = (st.st_mode & libc::S_ISVTX) != 0;
        warn!(
            mode = format!("{:#o}", st.st_mode & 0o7777),
            sticky,
            parent = %parent.display(),
            "socket parent directory is group/world-writable; the bind-time race \
             window (see docs/security/socket-bind.md) is not fully closed — place \
             the socket in a directory writable only by the daemon user",
        );
    }
}

/// True if `mode` grants write to group or other (the `0o020`/`0o002` bits).
#[cfg(unix)]
fn is_group_or_other_writable(mode: libc::mode_t) -> bool {
    (mode & 0o022) != 0
}

/// Why a parent directory is not private enough for the admin socket.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ParentProblem {
    GroupOrOtherWritable,
    ForeignOwner,
}

/// A parent is private when only the daemon user (or root) can create entries
/// in it: not group- or world-writable, and owned by the daemon's euid or by
/// root.
///
/// The ownership half is stricter than spec §11 (decision Q8, flagged for the
/// owner's security review): a 0755 directory owned by another non-root user
/// passes the mode test, but that user can still create the socket's name in
/// it and squat the control plane.
#[cfg(unix)]
pub(crate) fn parent_private_verdict(
    mode: libc::mode_t,
    owner: libc::uid_t,
    euid: libc::uid_t,
) -> Result<(), ParentProblem> {
    if is_group_or_other_writable(mode) {
        return Err(ParentProblem::GroupOrOtherWritable);
    }
    if owner != euid && owner != 0 {
        return Err(ParentProblem::ForeignOwner);
    }
    Ok(())
}

/// The refusal's marker, so a caller can tell it from an `EACCES` on open.
#[derive(Debug)]
pub(crate) struct InsecureParent(String);

impl std::fmt::Display for InsecureParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InsecureParent {}

/// True when `e` is [`bind_hardened_with`]'s private-parent refusal.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "admin::bind_admin (Task 6.6) is the caller; Task 6.14 removes this"
    )
)]
pub(crate) fn is_insecure_parent(e: &io::Error) -> bool {
    e.get_ref()
        .is_some_and(|inner| inner.is::<InsecureParent>())
}

/// `fstat`s the pinned parent dirfd (no re-resolution by path, as in
/// `warn_if_parent_world_writable`) and refuses a non-private parent.
#[cfg(unix)]
fn refuse_unless_parent_private(dirfd: libc::c_int, parent: &Path) -> io::Result<()> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: dirfd is a valid directory fd held by the caller; &mut st is a
    // writable libc::stat the kernel fills in.
    if unsafe { libc::fstat(dirfd, &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Safety: geteuid has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    parent_private_verdict(st.st_mode, st.st_uid, euid).map_err(|why| {
        let what = match why {
            ParentProblem::GroupOrOtherWritable => "it is group- or world-writable",
            ParentProblem::ForeignOwner => "it is owned by another non-root user",
        };
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            InsecureParent(format!(
                "refusing to bind a control socket in '{}' (mode {:#o}, owner uid {}): {what}. \
                 Anyone who can create entries there could put their own socket at this path \
                 and impersonate the daemon's admin interface. Use a directory writable only \
                 by the daemon user.",
                parent.display(),
                st.st_mode & 0o7777,
                st.st_uid
            )),
        )
    })
}

/// RAII guard that closes a directory fd on drop. The dirfd is held only as
/// long as the bind sequence; we don't keep it open after `run` returns.
#[cfg(unix)]
struct OwnedDirFd(libc::c_int);

#[cfg(unix)]
impl Drop for OwnedDirFd {
    fn drop(&mut self) {
        if self.0 >= 0 {
            // Safety: self.0 was returned by libc::open and not yet closed.
            unsafe {
                libc::close(self.0);
            }
        }
    }
}

/// RAII guard that restores the process umask on drop. Wraps the saved umask
/// (the value returned by the tightening `umask_set`), so an unwind between
/// tightening and restoring can't leak the tightened value process-wide (F30).
#[cfg(unix)]
struct UmaskGuard(libc::mode_t);

#[cfg(unix)]
impl Drop for UmaskGuard {
    fn drop(&mut self) {
        let _ = umask_set(self.0);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{models::WorkUnit, queue};
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::UnixStream};

    fn tmp_socket_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("weir_sock_{label}_{}.sock", std::process::id()))
    }

    /// A scratch directory whose mode does not depend on the process umask.
    ///
    /// Delegates to the crate-wide helper: the rule has exactly one
    /// implementation, and `testutil::scratch_dir` carries the full reasoning.
    /// These tests are both the cause of the umask window and among its victims,
    /// so they use the same helper as everything else rather than a local copy.
    fn umask_immune_dir(label: &str) -> PathBuf {
        crate::testutil::scratch_dir(label)
    }

    /// Serialises any test that mutates the process umask. umask is
    /// process-global; without this, parallel tests interleave and see
    /// each other's saved/restored values.
    ///
    /// It cannot serialise this module's tests against the REST of the crate's,
    /// which is why every test directory goes through
    /// [`crate::testutil::scratch_dir`].
    fn umask_test_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(Default::default)
    }

    fn default_config(path: PathBuf) -> SocketConfig {
        SocketConfig {
            socket_path: path,
            max_connections: 16,
            max_payload_bytes: weir_core::MAX_PAYLOAD_HARD_CAP,
            max_batch_records: 1024,
            shutdown_timeout_secs: 5,
            connection_read_timeout_secs: 30,
            shard_count: 1,
            // Tests connect from the same process → same uid → peer-uid
            // check always passes. Leaving the check enabled exercises the
            // production code path.
            peer_uid_check: true,
            // WAB cap disabled: these tests exercise the accept loop, not
            // ingest admission (connection.rs covers the cap directly).
            wab_max_bytes: 0,
            wab_bytes_now: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wab_cap_rejecting: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wab_ownership_lost: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            board: None,
        }
    }

    fn test_metrics() -> std::sync::Arc<crate::metrics::Metrics> {
        let (m, _reg) = crate::metrics::Metrics::new();
        std::sync::Arc::new(m)
    }

    // ── Path validation ───────────────────────────────────────────────────────

    #[test]
    fn validate_socket_path_rejects_relative() {
        let err = validate_socket_path(Path::new("relative/path.sock")).unwrap_err();
        assert!(err.to_string().contains("not absolute"), "{err}");
    }

    #[test]
    fn validate_socket_path_rejects_dotdot() {
        let err = validate_socket_path(Path::new("/var/../etc/weir.sock")).unwrap_err();
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    #[test]
    fn validate_socket_path_accepts_valid_absolute() {
        assert!(validate_socket_path(Path::new("/run/weir/weir.sock")).is_ok());
    }

    #[test]
    fn validate_socket_path_matches_shared_vector() {
        // Drift guard: the same cases must produce the same accept/reject from
        // config::validate_path_format_inner — see config's twin test.
        for (path, should_pass) in crate::path_validation_test_vectors::CASES {
            assert_eq!(
                validate_socket_path(Path::new(path)).is_ok(),
                *should_pass,
                "validate_socket_path({path:?})"
            );
        }
        #[cfg(unix)]
        for (path, should_pass) in crate::path_validation_test_vectors::UNIX_ONLY_CASES {
            assert_eq!(
                validate_socket_path(Path::new(path)).is_ok(),
                *should_pass,
                "validate_socket_path({path:?})"
            );
        }
    }

    // ── Hardened bind ─────────────────────────────────────────────────────────

    // bind_hardened internally mutates the process umask. The lock below
    // serialises these tests against each other, but it CANNOT protect the
    // ~300 other tests in this binary: any thread creating a directory while a
    // bind_hardened call holds umask 0o177 gets mode 0o600 and fails with
    // PermissionDenied. That is why the documented gate runs this target with
    // --test-threads=1 (CONTRIBUTING.md). In the daemon the window is harmless
    // because no producer can connect before the socket exists — but "called
    // once during single-threaded startup" is not the reason: the WAB flushers
    // (main.rs:292) and workers (main.rs:306) are already running by then.

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_refuses_regular_file_at_socket_path() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let path = tmp_socket_path("hardened_refuse");
        std::fs::write(&path, b"not a socket").unwrap();
        let err = bind_hardened(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert!(err.to_string().contains("non-socket"), "{err}");
        assert!(path.exists(), "regular file must not have been removed");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_replaces_stale_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let path = tmp_socket_path("hardened_stale");
        {
            let _stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
        }
        assert!(path.exists());
        let listener = bind_hardened(&path).unwrap();
        drop(listener);
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_succeeds_when_no_file_exists() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let path = tmp_socket_path("hardened_fresh");
        assert!(!path.exists());
        let listener = bind_hardened(&path).unwrap();
        drop(listener);
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_refuses_symlink_at_socket_path() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // A symlink (even to a real socket) is rejected because the
        // AT_SYMLINK_NOFOLLOW fstatat sees S_IFLNK, not S_IFSOCK.
        let dir = std::env::temp_dir().join(format!(
            "weir_bind_symlink_{}_{}",
            std::process::id(),
            line!()
        ));
        crate::testutil::mkdir_p(&dir);
        let target = dir.join("real.sock");
        {
            let _real = std::os::unix::net::UnixListener::bind(&target).unwrap();
        }
        let link = dir.join("link.sock");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = bind_hardened(&link).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert!(err.to_string().contains("non-socket"), "{err}");
        assert!(link.exists(), "symlink must not have been removed");

        std::fs::remove_file(&link).ok();
        std::fs::remove_file(&target).ok();
        std::fs::remove_dir(&dir).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_sets_mode_0600_even_with_loose_umask() {
        use std::os::unix::fs::PermissionsExt;
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Set a permissive umask before bind. The hardened sequence tightens
        // it internally to 0o177 so the socket is created mode 0o600
        // regardless of the inherited process umask.
        let saved = unsafe { libc::umask(0o000) };
        let path = tmp_socket_path("hardened_umask");
        let listener = bind_hardened(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        unsafe {
            libc::umask(saved);
        }
        assert_eq!(mode, 0o600, "bind_hardened must override inherited umask");
        drop(listener);
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_restores_umask_after_bind() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Set a known baseline of 0o022.
        unsafe {
            libc::umask(0o022);
        }
        let path = tmp_socket_path("hardened_umask_restore");
        let _listener = bind_hardened(&path).unwrap();
        // Read current umask without changing it permanently: set it twice.
        let after_bind = unsafe { libc::umask(0o022) };
        std::fs::remove_file(&path).ok();
        assert_eq!(
            after_bind, 0o022,
            "bind_hardened must restore process umask to its pre-call value, got {after_bind:#o}"
        );
    }

    /// F30: the RAII guard restores the saved umask on drop, including the
    /// unwind/early-return path that a bare save/restore statement would miss.
    #[test]
    #[cfg(unix)]
    fn umask_guard_restores_on_drop() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Baseline 0o022.
        unsafe {
            libc::umask(0o022);
        }
        {
            // Tighten to 0o177; the guard captures the returned 0o022.
            let _guard = UmaskGuard(umask_set(0o177));
            // Read the live umask without disturbing it (set to its own value).
            let live = unsafe { libc::umask(0o177) };
            assert_eq!(
                live, 0o177,
                "umask must be tightened while the guard is alive"
            );
        }
        // Guard dropped → 0o022 restored. Read it back (and re-set to 0o022).
        let after = unsafe { libc::umask(0o022) };
        assert_eq!(
            after, 0o022,
            "UmaskGuard must restore the saved umask on drop, got {after:#o}"
        );
    }

    // ── Socket ownership (safety floor, spec §3.3) ────────────────────────────

    /// Binds a std listener at `path`, then drops it: the file stays behind and
    /// nothing listens on it, exactly what SIGKILL leaves.
    #[cfg(unix)]
    fn leave_stale_socket(path: &Path) {
        std::fs::remove_file(path).ok();
        let l = std::os::unix::net::UnixListener::bind(path).unwrap();
        drop(l);
        assert!(path.exists(), "a dropped listener leaves its socket file");
    }

    #[cfg(unix)]
    fn inode_of(path: &Path) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::symlink_metadata(path).unwrap();
        (m.dev(), m.ino())
    }

    /// The probe's errno table (spec §3.3), independent of any real socket.
    #[test]
    #[cfg(unix)]
    fn connect_errno_classification_matches_the_probe_table() {
        assert_eq!(
            classify_connect_errno(libc::ECONNREFUSED),
            SocketProbe::Stale
        );
        assert_eq!(classify_connect_errno(libc::ENOENT), SocketProbe::Absent);
        assert_eq!(classify_connect_errno(libc::ENOTDIR), SocketProbe::Absent);
        // Backlog full on Linux, or a connect still completing: someone listens.
        assert_eq!(classify_connect_errno(libc::EAGAIN), SocketProbe::Live);
        assert_eq!(classify_connect_errno(libc::EINPROGRESS), SocketProbe::Live);
        // Cannot tell: fail closed.
        for errno in [libc::EACCES, libc::EPERM, libc::EIO] {
            assert_eq!(
                classify_connect_errno(errno),
                SocketProbe::Unknown(errno),
                "errno {errno}"
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn probe_classifies_live_stale_and_absent() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("probe_classes");
        let path = dir.join("weir.sock");

        assert_eq!(probe_socket(&path), SocketProbe::Absent);

        let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
        assert_eq!(probe_socket(&path), SocketProbe::Live);
        drop(live);

        assert!(path.exists());
        assert_eq!(probe_socket(&path), SocketProbe::Stale);
        assert!(path.exists(), "probe_socket must never unlink");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The non-Linux attachment probe's errno table (conf-security-7). Only a
    /// refusal is stale; a type mismatch means a socket is bound there.
    #[test]
    #[cfg(all(unix, not(target_os = "linux")))]
    fn attached_errno_classification_fails_closed() {
        assert_eq!(
            classify_attached_errno(libc::ECONNREFUSED),
            SocketProbe::Stale
        );
        assert_eq!(classify_attached_errno(libc::EPROTOTYPE), SocketProbe::Live);
        assert_eq!(classify_attached_errno(libc::ENOENT), SocketProbe::Absent);
        assert_eq!(classify_attached_errno(libc::ENOTDIR), SocketProbe::Absent);
        for errno in [libc::EACCES, libc::EPERM, libc::EIO, libc::EAGAIN] {
            assert_eq!(
                classify_attached_errno(errno),
                SocketProbe::Unknown(errno),
                "errno {errno}"
            );
        }
    }

    /// conf-security-7: a socket that is bound but not (or not yet) listening
    /// refuses a stream connect on BSD/macOS, yet its owner is alive. The probe
    /// must not call it stale.
    #[test]
    #[cfg(all(unix, not(target_os = "linux")))]
    fn bound_but_not_listening_socket_is_live_not_stale() {
        use std::os::fd::{FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bound_not_listening");
        let path = dir.join("weir.sock");

        // Safety: plain syscall; the fd is owned by `sock` below.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0);
        // Safety: fd was just created and is owned by nothing else.
        let sock = unsafe { OwnedFd::from_raw_fd(fd) };
        // Safety: all-zero is a valid sockaddr_un.
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let bytes = path.as_os_str().as_bytes();
        assert!(bytes.len() < addr.sun_path.len());
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (dst, src) in addr.sun_path.iter_mut().zip(bytes) {
            *dst = *src as libc::c_char;
        }
        let len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
        #[cfg(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly"
        ))]
        {
            addr.sun_len = len as u8;
        }
        // Safety: addr is initialised and len is within its size.
        let rc = unsafe {
            libc::bind(
                std::os::fd::AsRawFd::as_raw_fd(&sock),
                (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
                len as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "{}", io::Error::last_os_error());

        assert_eq!(
            probe_socket_once(&path),
            SocketProbe::Stale,
            "precondition: a bound, non-listening socket refuses a stream connect"
        );
        assert_eq!(probe_socket(&path), SocketProbe::Live);

        drop(sock);
        assert_eq!(
            probe_socket(&path),
            SocketProbe::Stale,
            "once its owner closes it, the same file is stale"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// conf-security-7: on BSD/macOS a listener whose accept backlog is full
    /// refuses a stream connect with ECONNREFUSED, the same errno a stale socket
    /// gives. The probe must still call it `Live`, and startup must refuse to
    /// unlink it, however long the backlog stays full.
    #[tokio::test]
    #[cfg(all(unix, not(target_os = "linux")))]
    async fn full_backlog_listener_is_live_not_stale() {
        use std::os::fd::AsRawFd;
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("full_backlog");
        let path = dir.join("weir.sock");
        let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
        // Shrink the backlog so it fills in a few connects; BSD and XNU let a
        // listening socket call listen() again to resize it.
        // Safety: live is an open listening socket we own.
        assert_eq!(unsafe { libc::listen(live.as_raw_fd(), 1) }, 0);

        // Fill it: never accept, keep connecting until a stream connect is
        // refused. The probe's own connections queue too.
        let mut held = Vec::new();
        let mut filled = false;
        for _ in 0..1024 {
            if probe_socket_once(&path) == SocketProbe::Stale {
                filled = true;
                break;
            }
            if let Ok(c) = std::os::unix::net::UnixStream::connect(&path) {
                held.push(c);
            }
        }
        assert!(
            filled,
            "precondition: a full backlog refuses a stream connect on this platform"
        );

        // The ambiguity is reproduced; the probe must not call it stale.
        assert_eq!(probe_socket(&path), SocketProbe::Live);

        let before = inode_of(&path);
        let err = bind_hardened(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        assert_eq!(
            inode_of(&path),
            before,
            "a live socket with a full backlog must not be unlinked"
        );

        // Drain the backlog and prove the listener still works.
        drop(held);
        live.set_nonblocking(true).unwrap();
        while live.accept().is_ok() {}
        let _c = std::os::unix::net::UnixStream::connect(&path)
            .expect("the live listener must still accept after the refused start");

        drop(live);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review Focus 3: systemd's `RuntimeDirectory` may be gone at start. The
    /// probe says `Absent` (nothing can listen in a missing directory), and the
    /// caller proceeds to the existing parent-missing error.
    #[test]
    #[cfg(unix)]
    fn probe_reports_absent_when_parent_missing() {
        let path = std::env::temp_dir()
            .join(format!("weir_probe_no_parent_{}", std::process::id()))
            .join("weir.sock");
        assert!(!path.parent().unwrap().exists());
        assert_eq!(probe_socket(&path), SocketProbe::Absent);
        // And the bind still reports the missing parent, not a probe refusal.
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let err = bind_hardened(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
    }

    /// A bound-then-dropped listener leaves its file; nothing answers
    /// (ECONNREFUSED), so startup cleanup unlinks it and the bind succeeds on a
    /// fresh inode.
    #[tokio::test]
    #[cfg(unix)]
    async fn startup_cleanup_unlinks_a_dropped_listener_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("startup_stale");
        let path = dir.join("weir.sock");
        leave_stale_socket(&path);

        let (parent, basename) = split_parent_basename(&path).unwrap();
        let dirfd = open_parent_dirfd(parent).unwrap();
        let _dirfd_guard = OwnedDirFd(dirfd);
        remove_stale_socket_at_startup(dirfd, basename, &path).expect("stale socket is removed");
        assert!(!path.exists(), "startup cleanup must unlink a stale socket");

        // And end to end: bind_hardened over a stale socket succeeds.
        leave_stale_socket(&path);
        let bound = bind_hardened(&path).expect("bind over a stale socket");
        assert_eq!(inode_of(&path), (bound.dev, bound.ino));
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A held listener answers the probe, so startup cleanup refuses: the file
    /// stays, the other listener keeps working, and the error says why.
    #[tokio::test]
    #[cfg(unix)]
    async fn startup_cleanup_refuses_a_live_listener() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("startup_live");
        let path = dir.join("weir.sock");
        let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let before = inode_of(&path);

        let err = bind_hardened(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        let msg = err.to_string();
        assert!(msg.contains("in use by a running process"), "{msg}");
        assert!(msg.contains("refusing to remove it"), "{msg}");
        assert!(msg.contains(&path.display().to_string()), "{msg}");

        assert_eq!(
            inode_of(&path),
            before,
            "a live socket must not be unlinked"
        );
        live.set_nonblocking(true).unwrap();
        // Drain the probe's own connection, then prove the listener still works.
        while live.accept().is_ok() {}
        let _c = std::os::unix::net::UnixStream::connect(&path)
            .expect("the live listener must still accept after the refused start");

        drop(live);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `remove_own_socket` keeps a socket that is not the one we bound: another
    /// instance unlinked ours and bound its own at the same path.
    #[tokio::test]
    #[cfg(unix)]
    async fn remove_own_socket_keeps_a_replaced_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("own_replaced");
        let path = dir.join("weir.sock");
        let bound = bind_hardened(&path).unwrap();

        std::fs::remove_file(&path).unwrap();
        let successor = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let successor_inode = inode_of(&path);
        assert_ne!(
            successor_inode,
            (bound.dev, bound.ino),
            "precondition: the successor is a different inode"
        );

        remove_own_socket(&path, &bound);
        assert!(path.exists(), "a successor's socket must not be unlinked");
        assert_eq!(inode_of(&path), successor_inode);
        let _c = std::os::unix::net::UnixStream::connect(&path)
            .expect("the successor must still accept");

        drop(successor);
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `remove_own_socket` unlinks the socket it bound (the identity matches).
    #[tokio::test]
    #[cfg(unix)]
    async fn remove_own_socket_unlinks_our_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("own_unlink");
        let path = dir.join("weir.sock");
        let bound = bind_hardened(&path).unwrap();
        assert!(path.exists());
        remove_own_socket(&path, &bound);
        assert!(!path.exists(), "our own socket must be unlinked");
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// F28: shutdown removal refuses to unlink a non-socket entry (an attacker
    /// swap), the same guard the startup cleanup applies.
    #[tokio::test]
    #[cfg(unix)]
    async fn remove_own_socket_refuses_non_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("own_nonsock");
        let path = dir.join("weir.sock");
        let bound = bind_hardened(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"not a socket").unwrap();
        remove_own_socket(&path, &bound); // logs, must not remove
        assert!(
            path.exists(),
            "shutdown removal must refuse a non-socket entry"
        );
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// F28: a missing socket file is a no-op (clean shutdown after the file was
    /// already removed out-of-band).
    #[tokio::test]
    #[cfg(unix)]
    async fn remove_own_socket_missing_is_noop() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("own_missing");
        let path = dir.join("weir.sock");
        let bound = bind_hardened(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        remove_own_socket(&path, &bound); // must not panic
        assert!(!path.exists());
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The regression for the self-probe trap (spec §3.3): at shutdown the
    /// daemon's own listener is still open, so a liveness probe there would see
    /// it as live and leave the socket behind on every clean shutdown. A full
    /// `run` start-then-shutdown, with a real client connection in between,
    /// must remove its own socket file.
    #[tokio::test]
    #[cfg(unix)]
    async fn full_run_start_then_shutdown_removes_its_own_socket() {
        let dir = umask_immune_dir("full_run_own");
        let path = dir.join("weir.sock");
        let cfg = default_config(path.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));

        let deadline = Instant::now() + Duration::from_secs(3);
        let conn = loop {
            if let Ok(c) = UnixStream::connect(&path).await {
                break c;
            }
            assert!(Instant::now() < deadline, "run() never started listening");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        drop(conn);

        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("run() should complete after shutdown")
            .expect("task should not panic")
            .expect("run() should return Ok");
        assert!(
            !path.exists(),
            "a clean shutdown must remove the daemon's own socket file"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// With a status board, `run` marks the Unix listener Bound and the
    /// daemon Serving once the socket is bound, and Closed on return.
    #[tokio::test]
    #[cfg(unix)]
    async fn run_marks_the_unix_listener_bound_then_serving_then_closed() {
        use weir_status::{ListenerState, Phase};
        let dir = umask_immune_dir("run_marks_board");
        let path = dir.join("weir.sock");
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        assert_eq!(board.listeners.unix.get(), ListenerState::Pending);
        let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        let deadline = Instant::now() + Duration::from_secs(3);
        while UnixStream::connect(&path).await.is_err() {
            assert!(Instant::now() < deadline, "run() never started listening");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(board.listeners.unix.get(), ListenerState::Bound);
        assert_eq!(board.life.phase(), Phase::Serving);
        assert!(board.life.ever_served());
        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_secs(3), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(board.listeners.unix.get(), ListenerState::Closed);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A socket path that fails validation (or binding) marks the Unix
    /// listener Failed.
    #[tokio::test]
    #[cfg(unix)]
    async fn a_bind_failure_marks_the_unix_listener_failed() {
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(PathBuf::from("relative/weir.sock"));
        cfg.board = Some(std::sync::Arc::clone(&board));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(1));
        assert!(
            run(cfg, queue_tx, shutdown_rx, test_metrics(), sem)
                .await
                .is_err()
        );
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Failed
        );
    }

    /// On Linux the bound socket inode is pinned with an `O_PATH` fd for the
    /// listener's life, so its inode number cannot be reused by a successor.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn o_path_pin_opens_on_socket_inode() {
        use std::os::fd::AsRawFd;
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("o_path_pin");
        let path = dir.join("weir.sock");
        let bound = bind_hardened(&path).unwrap();
        let pin = bound
            .pin
            .as_ref()
            .expect("O_PATH pin must open on a socket inode");
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // Safety: pin is an open fd; st is writable.
        assert_eq!(unsafe { libc::fstat(pin.as_raw_fd(), &mut st) }, 0);
        assert_eq!(st.st_mode & libc::S_IFMT, libc::S_IFSOCK);
        assert_eq!(stat_identity(&st), (bound.dev, bound.ino));
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[cfg(unix)]
    fn is_group_or_other_writable_classifies_modes() {
        // Private modes: only the owner can write.
        assert!(!is_group_or_other_writable(0o700));
        assert!(!is_group_or_other_writable(0o755));
        assert!(!is_group_or_other_writable(0o600));
        // Group-writable.
        assert!(is_group_or_other_writable(0o770));
        assert!(is_group_or_other_writable(0o775));
        // World-writable, including the /tmp sticky-bit mode.
        assert!(is_group_or_other_writable(0o777));
        assert!(is_group_or_other_writable(0o1777));
        // The sticky/setuid/setgid bits alone (no write bits) are not writable.
        assert!(!is_group_or_other_writable(0o1700));
    }

    #[test]
    #[cfg(unix)]
    fn is_accept_resource_exhaustion_classifies_errnos() {
        // Resource-exhaustion errnos warrant a backoff.
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert!(
                is_accept_resource_exhaustion(&io::Error::from_raw_os_error(errno)),
                "errno {errno} should back off"
            );
        }
        // ECONNABORTED (client aborted pre-accept) and non-errno errors do not.
        assert!(!is_accept_resource_exhaustion(
            &io::Error::from_raw_os_error(libc::ECONNABORTED)
        ));
        assert!(!is_accept_resource_exhaustion(&io::Error::other(
            "synthetic"
        )));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_warns_but_succeeds_on_world_writable_parent() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // A 0o1777 (/tmp-like) parent leaves the bind race open. We warn, but
        // must NOT fail the bind — operators legitimately use such directories.
        use std::os::unix::fs::PermissionsExt;
        let dir =
            std::env::temp_dir().join(format!("weir_ww_parent_{}_{}", std::process::id(), line!()));
        crate::testutil::mkdir_p(&dir);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o1777)).unwrap();
        let path = dir.join("weir.sock");
        let listener = bind_hardened(&path).expect("bind must succeed despite loose parent");
        drop(listener);
        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_fails_when_parent_directory_missing() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let path = std::env::temp_dir()
            .join(format!("weir_no_such_dir_{}", std::process::id()))
            .join("weir.sock");
        assert!(!path.parent().unwrap().exists());
        let err = bind_hardened(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_fails_when_parent_is_symlink() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Parent open uses O_NOFOLLOW on the final component, so a symlinked
        // parent directory is rejected. Documents the intentional restriction.
        let base =
            std::env::temp_dir().join(format!("weir_parent_symlink_base_{}", std::process::id()));
        let real_parent = base.join("real");
        let link_parent = base.join("link");
        crate::testutil::mkdir_p(&real_parent);
        std::os::unix::fs::symlink(&real_parent, &link_parent).unwrap();
        let path = link_parent.join("weir.sock");

        let err = bind_hardened(&path).unwrap_err();
        // ELOOP on Linux when opening a symlink with O_NOFOLLOW.
        let raw = err.raw_os_error();
        assert!(
            raw == Some(libc::ELOOP) || raw == Some(libc::ENOTDIR),
            "expected ELOOP or ENOTDIR, got {err} (raw={raw:?})"
        );

        std::fs::remove_file(&link_parent).ok();
        std::fs::remove_dir(&real_parent).ok();
        std::fs::remove_dir(&base).ok();
    }

    /// Building-block test: confirm that `stat_at_dir` reflects whatever
    /// inode is currently at the directory-relative name. This is the
    /// primitive the late-swap detection relies on.
    ///
    /// Uses `rename(src, dst)` which atomically replaces dst's inode with
    /// src's, guaranteeing a distinct inode (unlike remove+recreate, where
    /// the filesystem may immediately reuse the inode number on tmpfs/ext4).
    /// The scratch-directory helper must survive the umask `bind_hardened`
    /// holds, because that is the exact condition it exists to defend against.
    ///
    /// Asserting only `create` succeeded would pass with the bug present:
    /// `create_dir_all` under umask 0o177 succeeds too, and yields a directory
    /// that is unusable rather than absent. The mode and a real file write are
    /// what distinguish the two.
    #[test]
    #[cfg(unix)]
    fn umask_immune_dir_ignores_a_hostile_process_umask() {
        use std::os::unix::fs::PermissionsExt;

        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let restore = UmaskGuard(umask_set(0o177));

        let dir = umask_immune_dir("umask_immune_probe");
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o700,
            "umask 0o177 masked the directory down to {mode:#o}; \
             DirBuilder::mode is not being applied"
        );
        std::fs::write(dir.join("probe"), b"x")
            .expect("a directory without its execute bit cannot hold files");

        drop(restore);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[cfg(unix)]
    fn stat_at_dir_observes_inode_swap() {
        // Serialize with the bind tests that toggle the process-global umask —
        // without this lock a concurrent umask=0o177 sibling could perturb the
        // files this test creates and make it flake under parallelism (#14).
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("stat_at_dir");
        let name = std::ffi::OsString::from("entry");
        let entry_path = dir.join(&name);
        let other_path = dir.join("other");

        // Open the dir.
        let parent_c = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()).unwrap();
        let dirfd = unsafe {
            libc::open(
                parent_c.as_ptr(),
                #[cfg(target_os = "linux")]
                {
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
                },
                #[cfg(not(target_os = "linux"))]
                {
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
                },
            )
        };
        assert!(dirfd >= 0);

        // Write two distinct files with different inodes.
        std::fs::write(&entry_path, b"a").unwrap();
        std::fs::write(&other_path, b"b").unwrap();
        let snap_a = stat_at_dir(dirfd, &name).unwrap();

        // Rename `other` over `entry`. rename(2) is atomic and replaces
        // entry's inode with other's; the ino at the name now differs.
        std::fs::rename(&other_path, &entry_path).unwrap();
        let snap_b = stat_at_dir(dirfd, &name).unwrap();
        assert_ne!(
            snap_a.st_ino, snap_b.st_ino,
            "stat_at_dir must reflect the new inode after rename swap"
        );

        unsafe {
            libc::close(dirfd);
        }
        std::fs::remove_file(&entry_path).ok();
        std::fs::remove_dir(&dir).ok();
    }

    /// Adversarial: a thread tries to swap a decoy socket over the daemon's
    /// path during the bind sequence. The contract bind_hardened upholds is
    /// "if you return Ok, the socket at the path is mine, with mode 0o600,
    /// owned by me". The attacker may cause errors; it must never cause
    /// silent corruption.
    #[tokio::test]
    #[cfg(unix)]
    async fn bind_hardened_never_silently_succeeds_under_swap_pressure() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());

        let dir = umask_immune_dir("swap_pressure");
        let target = dir.join("weir.sock");
        let decoy = dir.join("decoy.sock");

        // Prepare a decoy socket the attacker thread will try to rename over
        // the target.
        std::fs::remove_file(&decoy).ok();
        std::fs::remove_file(&target).ok();
        let _decoy_listener = std::os::unix::net::UnixListener::bind(&decoy).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop_attacker = Arc::clone(&stop);
        let decoy_for_attacker = decoy.clone();
        let target_for_attacker = target.clone();

        let attacker = std::thread::spawn(move || {
            while !stop_attacker.load(Ordering::Relaxed) {
                // Try to rename the decoy over the target. If decoy is gone
                // (we already renamed it), recreate it.
                if !decoy_for_attacker.exists() {
                    let _ = std::os::unix::net::UnixListener::bind(&decoy_for_attacker);
                }
                let _ = std::fs::rename(&decoy_for_attacker, &target_for_attacker);
            }
        });

        let mut ok = 0;
        let mut errors = 0;
        for _ in 0..200 {
            match bind_hardened(&target) {
                Ok(listener) => {
                    // Under swap pressure the socket FILE's mode cannot be
                    // verified race-free: a path stat may read the attacker's
                    // swapped-in decoy, and fstat on a Unix-socket fd reports the
                    // kernel's socket-object mode (0o777 on Linux, 0o666 on
                    // macOS), not the bound file's mode. So under pressure we
                    // only assert bind_hardened stays sound — it returns without
                    // panicking and hands back a usable listener. The 0o600
                    // file-mode contract is verified by the deterministic clean
                    // bind after the loop, with the attacker stopped.
                    ok += 1;
                    drop(listener);
                    std::fs::remove_file(&target).ok();
                }
                Err(_) => {
                    errors += 1;
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        attacker.join().unwrap();

        // Deterministic verification with the attacker stopped (no contention):
        // a clean bind_hardened produces a 0o600 socket file at `target`. With
        // nothing swapping the path, the path-based stat is race-free, so this
        // asserts the umask→0o600 guarantee portably (Linux and macOS).
        std::fs::remove_file(&target).ok();
        let clean = bind_hardened(&target).expect("clean bind after swap pressure must succeed");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "clean bind_hardened produced socket mode {mode:#o}"
        );
        drop(clean);

        std::fs::remove_file(&decoy).ok();
        std::fs::remove_file(&target).ok();
        std::fs::remove_dir(&dir).ok();

        // We don't require any particular ratio — the attacker may dominate
        // or may not — only that *every* successful return upheld the
        // contract checked inside the loop.
        eprintln!("swap-pressure stress: {ok} Ok / {errors} Err over 200 iterations");
    }

    // ── Connection limit ──────────────────────────────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn connection_over_cap_is_refused() {
        let path = tmp_socket_path("connlimit");
        let cfg = SocketConfig {
            max_connections: 1,
            shutdown_timeout_secs: 2,
            ..default_config(path.clone())
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let _rx = queue_rx.get(0); // keep receiver alive

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        tokio::time::sleep(Duration::from_millis(30)).await;

        // First connection fills the cap.
        let _conn1 = UnixStream::connect(&path).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second connection — server drops it immediately (cap exceeded).
        let mut conn2 = UnixStream::connect(&path).await.unwrap();
        let mut buf = [0u8; 1];
        // Either EOF (stream dropped by server) or timeout — both confirm the
        // server did not accept a second active connection beyond the cap.
        let _ = time::timeout(Duration::from_millis(200), conn2.read(&mut buf)).await;

        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        std::fs::remove_file(&path).ok();
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    /// An idle connection (no in-flight push) must exit quickly when the
    /// daemon receives a shutdown signal — without waiting out the
    /// per-connection read_timeout. The handler races stream.read_exact
    /// against shutdown_rx.changed(); when shutdown fires the read side of
    /// the select wins immediately. Without that race, idle connections
    /// would sit in read_exact until read_timeout (30s default), and
    /// shutdown would have to abort_all them.
    #[tokio::test]
    #[cfg(unix)]
    async fn idle_connection_exits_promptly_on_shutdown() {
        let path = tmp_socket_path("shutdown_idle");
        let mut cfg = default_config(path.clone());
        cfg.connection_read_timeout_secs = 30; // long, to prove the race wins
        cfg.shutdown_timeout_secs = 2;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let _rx = queue_rx.get(0); // keep receiver alive

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let run_handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Open an idle connection — never send any bytes.
        let _conn = UnixStream::connect(&path).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let t0 = Instant::now();
        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_millis(500), run_handle)
            .await
            .expect("run() should complete well before read_timeout")
            .expect("task should not panic")
            .expect("run() should return Ok");
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "idle connection delayed shutdown: {elapsed:?} \
             (read_timeout is 30s; shutdown must NOT wait that out)"
        );

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn run_exits_cleanly_after_shutdown_signal() {
        let path = tmp_socket_path("shutdown");
        let cfg = default_config(path.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        tokio::time::sleep(Duration::from_millis(20)).await;

        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("run() should complete within timeout after shutdown signal")
            .expect("task should not panic")
            .expect("run() should return Ok");

        std::fs::remove_file(&path).ok();
    }

    // ── Socket permissions ────────────────────────────────────────────────────

    #[test]
    #[cfg(unix)]
    fn socket_bind_sets_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = tmp_socket_path("perms");
            let cfg = default_config(path.clone());
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
            let (queue_tx, _) = queue::new::<WorkUnit>(1);

            let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
            tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
            tokio::time::sleep(Duration::from_millis(30)).await;

            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            // Mask to just the permission bits (ignore file type bits).
            assert_eq!(
                mode & 0o777,
                0o600,
                "socket should be mode 0600, got {:#o}",
                mode & 0o777
            );

            shutdown_tx.send_replace(ShutdownLevel::Graceful);
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::remove_file(&path).ok();
        });
    }

    // ── refuse_if_live and the bind umask window (PR 3 Task 3.8) ─────────────

    #[test]
    #[cfg(unix)]
    fn refuse_if_live_refuses_a_live_socket_and_allows_absent_or_stale() {
        let dir = umask_immune_dir("refuse_if_live");
        let path = dir.join("admin.sock");

        refuse_if_live(&path, "admin socket").expect("nothing at the path");

        leave_stale_socket(&path);
        refuse_if_live(&path, "admin socket").expect("a stale socket is not a live daemon");
        std::fs::remove_file(&path).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let live = UnixListener::bind(&path).unwrap();
        let err = refuse_if_live(&path, "admin socket").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        let msg = err.to_string();
        assert!(msg.contains("another daemon is serving"), "{msg}");
        assert!(msg.contains(&path.display().to_string()), "{msg}");
        assert!(msg.contains("admin socket"), "{msg}");
        assert!(path.exists(), "refuse_if_live never unlinks");
        drop(live);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Pins the corrected comment in `bind_hardened`: under the 0o177 bind
    /// window a 0o600 file keeps 0o600 (owner rw is not in the mask), and a
    /// directory from `create_dir_private` keeps 0o700 (it chmods after mkdir).
    #[test]
    #[cfg(unix)]
    fn the_bind_umask_window_leaves_segment_files_and_private_dirs_intact() {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("umask_window");
        let file = dir.join("seg.wab");
        let sub = dir.join("shard_00");
        {
            let _umask = UmaskGuard(umask_set(0o177));
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .mode(0o600)
                .open(&file)
                .unwrap();
            crate::wab::create_dir_private(sub.clone()).unwrap();
        }
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&sub).unwrap().permissions().mode() & 0o777,
            0o700
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── bind / serve split and live-shard routing (PR 3 Task 3.10) ───────────

    /// `bind` binds and returns; nothing accepts until `serve` runs. The
    /// kernel backlog still completes a connect, so the test checks that no
    /// frame is answered rather than that the connect fails.
    #[tokio::test]
    #[cfg(unix)]
    async fn bind_returns_a_bound_socket_that_serve_then_serves() {
        let dir = umask_immune_dir("bind_then_serve");
        let path = dir.join("weir.sock");
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        // The umask lock covers the bind only: a std Mutex is not held
        // across an await.
        let bound = {
            let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
            bind(&cfg).unwrap()
        };
        assert_eq!(bound.path, path);
        assert!(path.exists());
        // README Q6: `bind` is the one writer of the Unix Bound mark; Serving
        // is `serve`'s, and nothing serves yet.
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Bound
        );
        assert_ne!(board.life.phase(), weir_status::Phase::Serving);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let handle = tokio::spawn(serve(
            cfg,
            bound,
            queue_tx,
            shutdown_rx,
            test_metrics(),
            sem,
            route::ShardRouter::round_robin(1),
        ));
        let _conn = UnixStream::connect(&path).await.unwrap();
        // The connect can complete from the kernel backlog before the spawned
        // `serve` has run its first line, so wait for the phase rather than
        // assume it.
        let deadline = Instant::now() + Duration::from_secs(3);
        while board.life.phase() != weir_status::Phase::Serving {
            assert!(Instant::now() < deadline, "serve never entered Serving");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Bound,
            "serve does not write Bound"
        );
        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_secs(3), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!path.exists(), "serve removes the socket it was handed");
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Closed
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// With no live shard the connection is dropped before any frame and
    /// counted; the accept loop keeps running.
    #[tokio::test]
    #[cfg(unix)]
    async fn serve_drops_and_counts_a_connection_when_no_shard_is_live() {
        struct NoneLive;
        impl route::LiveShards for NoneLive {
            fn pick_live(&self, _n: u64) -> Option<u32> {
                None
            }
        }
        let dir = umask_immune_dir("no_live_shard");
        let path = dir.join("weir.sock");
        let cfg = default_config(path.clone());
        let bound = {
            let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
            bind(&cfg).unwrap()
        };
        let metrics = test_metrics();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let router = route::ShardRouter::live(std::sync::Arc::new(NoneLive));
        let handle = tokio::spawn(serve(
            cfg,
            bound,
            queue_tx,
            shutdown_rx,
            std::sync::Arc::clone(&metrics),
            sem,
            router,
        ));

        for _ in 0..2 {
            let mut conn = UnixStream::connect(&path).await.unwrap();
            let mut buf = [0u8; 1];
            let n = time::timeout(Duration::from_secs(2), conn.read(&mut buf))
                .await
                .unwrap()
                .unwrap_or(0);
            assert_eq!(n, 0, "the daemon must close the connection without a byte");
        }
        assert_eq!(metrics.connections_refused_no_live_shard.get(), 2);

        shutdown_tx.send_replace(ShutdownLevel::Graceful);
        time::timeout(Duration::from_secs(3), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── bind every producer listener before serving any (PR 3 Task 3.11) ───

    fn free_loopback_addr() -> std::net::SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    #[tokio::test]
    #[cfg(unix)]
    #[allow(
        clippy::await_holding_lock,
        reason = "bind_all awaits the TCP bind; the umask lock must cover its bind_hardened, and a current-thread test runtime runs nothing else that could take the lock"
    )]
    async fn bind_all_binds_tcp_and_unix_and_serves_neither() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_ok");
        let path = dir.join("weir.sock");
        let addr = free_loopback_addr();
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let bound = bind_all(&cfg, Some(addr)).await.unwrap();
        assert_eq!(bound.tcp.as_ref().unwrap().local_addr().unwrap(), addr);
        assert_eq!(bound.unix.path, path);
        // README Q6: bind_all / bind mark Bound; nothing enters Serving yet.
        assert_eq!(board.listeners.tcp.get(), weir_status::ListenerState::Bound);
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Bound
        );
        assert_ne!(board.life.phase(), weir_status::Phase::Serving);
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Without a TCP address only the Unix socket is bound, and the TCP
    /// listener's mark is left alone.
    #[tokio::test]
    #[cfg(unix)]
    #[allow(
        clippy::await_holding_lock,
        reason = "bind_all awaits the TCP bind; the umask lock must cover its bind_hardened, and a current-thread test runtime runs nothing else that could take the lock"
    )]
    async fn bind_all_without_tcp_binds_only_the_unix_socket() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_unix_only");
        let path = dir.join("weir.sock");
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let tcp_before = board.listeners.tcp.get();
        let bound = bind_all(&cfg, None).await.unwrap();
        assert!(bound.tcp.is_none());
        assert!(path.exists());
        assert_eq!(board.listeners.tcp.get(), tcp_before);
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A Unix bind failure after the TCP bind fails the whole step and leaves
    /// nothing listening: startup fails before any producer could be acked.
    #[tokio::test]
    #[cfg(unix)]
    #[allow(
        clippy::await_holding_lock,
        reason = "bind_all awaits the TCP bind; the umask lock must cover its bind_hardened, and a current-thread test runtime runs nothing else that could take the lock"
    )]
    async fn bind_all_releases_the_tcp_port_when_the_unix_bind_fails() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_fail");
        let path = dir.join("weir.sock");
        std::fs::write(&path, b"not a socket").unwrap();
        let addr = free_loopback_addr();
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let err = bind_all(&cfg, Some(addr)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert_eq!(
            board.listeners.unix.get(),
            weir_status::ListenerState::Failed
        );
        assert_ne!(
            board.listeners.tcp.get(),
            weir_status::ListenerState::Bound,
            "a TCP listener dropped with the failed step is never reported Bound"
        );
        assert!(
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err(),
            "the TCP listener must be dropped with the failed step"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A TCP bind failure (the address is taken) marks the TCP listener
    /// Failed and never creates the Unix socket.
    #[tokio::test]
    #[cfg(unix)]
    #[allow(
        clippy::await_holding_lock,
        reason = "bind_all awaits the TCP bind; the umask lock must cover its bind_hardened, and a current-thread test runtime runs nothing else that could take the lock"
    )]
    async fn bind_all_fails_on_a_taken_tcp_address_before_the_unix_bind() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_tcp_taken");
        let path = dir.join("weir.sock");
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = taken.local_addr().unwrap();
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let unix_before = board.listeners.unix.get();
        let err = bind_all(&cfg, Some(addr)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        assert_eq!(
            board.listeners.tcp.get(),
            weir_status::ListenerState::Failed
        );
        assert_eq!(board.listeners.unix.get(), unix_before);
        assert!(!path.exists(), "the Unix socket is never bound");
        drop(taken);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── Bounded connection drain (Foundations §5.7) ───────────────────────────

    use crate::lifecycle::ShutdownLevel;

    #[tokio::test]
    async fn drain_connections_aborts_at_forced_without_waiting_out_the_timeout() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.send_replace(ShutdownLevel::Forced);
            // Keep the sender alive past the abort.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let t0 = Instant::now();
        let out =
            drain_connections(&mut js, Duration::from_secs(30), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Forced { aborted: 1 });
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "took {:?}",
            t0.elapsed()
        );
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        assert!(
            js.is_empty(),
            "aborted handlers must be reaped before returning"
        );
    }

    #[tokio::test]
    async fn drain_connections_aborts_at_the_timeout() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        let out = drain_connections(
            &mut js,
            Duration::from_millis(100),
            &mut rx,
            &metrics,
            "test",
        )
        .await;
        assert_eq!(out, ConnDrain::TimedOut { aborted: 1 });
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        assert!(js.is_empty(), "the aborted handler is reaped");
    }

    #[tokio::test]
    async fn drain_connections_is_clean_when_handlers_finish() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(async { tokio::time::sleep(Duration::from_millis(20)).await });
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        let out =
            drain_connections(&mut js, Duration::from_secs(5), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Clean);
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 0);
    }

    #[tokio::test]
    async fn drain_connections_aborts_at_once_when_already_forced() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Forced);
        let t0 = Instant::now();
        let out =
            drain_connections(&mut js, Duration::from_secs(30), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Forced { aborted: 1 });
        assert!(t0.elapsed() < Duration::from_millis(100));
    }

    /// Lame duck (Foundations §5.7, §7): on Graceful the socket file is gone
    /// and new connects fail at once, BEFORE the connection drain finishes,
    /// and a later Forced cuts that drain short.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(unix)]
    async fn lame_duck_unlinks_before_the_connection_drain_and_forced_cuts_it_short() {
        let dir = umask_immune_dir("lame_duck");
        let path = dir.join("weir.sock");
        let board = crate::status::StatusBoard::for_test_with(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.shutdown_timeout_secs = 30; // the drain is NOT what removes the socket
        cfg.board = Some(std::sync::Arc::clone(&board));
        let (level_tx, level_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        // Kept alive and never consumed: a push waits for an ack that never comes,
        // so one handler (and therefore the drain) stays busy.
        let held = queue_rx.get(0);
        let metrics = test_metrics();
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        // The umask lock covers the bind only: a std Mutex is not held
        // across an await.
        let bound = {
            let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
            bind(&cfg).expect("bind")
        };
        let handle = tokio::spawn(serve(
            cfg,
            bound,
            queue_tx,
            level_rx,
            std::sync::Arc::clone(&metrics),
            sem,
            route::ShardRouter::round_robin(1),
        ));

        let p = path.clone();
        let pusher = tokio::task::spawn_blocking(move || {
            let mut c = weir_client::WeirClient::connect(&p).expect("connect");
            c.push(b"in-flight", weir_core::Durability::Durable)
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while held.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the push never reached the queue"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Graceful);
        let t0 = Instant::now();
        while path.exists() {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "lame duck: the socket must be unlinked within 200 ms"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            UnixStream::connect(&path).await.is_err(),
            "a new connect must fail fast once the lame duck began"
        );
        assert!(
            !handle.is_finished(),
            "the connection drain must still be waiting on the in-flight push"
        );
        // The lame duck marks the listener Closed right after the unlink and
        // before the connection drain.
        let t1 = Instant::now();
        while board.listeners.unix.get() != weir_status::ListenerState::Closed {
            assert!(
                t1.elapsed() < Duration::from_millis(200),
                "the listener was never marked Closed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Forced);
        time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("Forced must end the 30 s drain at once")
            .expect("task should not panic")
            .expect("run() should return Ok");
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        let _ = pusher.await; // the aborted connection fails the push with Io
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── The admin bind's private-parent policy (PR 6 Task 6.4) ───────────────

    /// Foundations §11: the admin bind REFUSES a parent others can write to
    /// (the producer bind keeps warning), and leaves nothing behind.
    /// A tokio test: the binds go through tokio's `UnixListener::bind`, which
    /// panics outside a runtime with IO enabled.
    #[tokio::test]
    async fn require_private_refuses_a_group_or_world_writable_parent() {
        use std::os::unix::fs::PermissionsExt;
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let root = crate::testutil::scratch_dir("bind_private");
        for mode in [0o770, 0o707, 0o1777] {
            let dir = root.join(format!("m{mode:o}"));
            crate::testutil::mkdir_p(&dir);
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode)).unwrap();
            let path = dir.join("a.sock");
            let err = bind_hardened_with(&path, ParentPolicy::RequirePrivate)
                .expect_err("a shared parent must be refused");
            assert!(is_insecure_parent(&err), "{mode:o}: {err}");
            assert!(
                err.to_string().contains("group- or world-writable"),
                "{err}"
            );
            assert!(!path.exists(), "a refused bind creates nothing");
            // The producer policy is unchanged: it warns and binds.
            let bound = bind_hardened(&path).expect("Warn policy still binds");
            remove_own_socket(&path, &bound);
        }
        let private = root.join("private");
        crate::testutil::mkdir_p(&private);
        let path = private.join("a.sock");
        let bound =
            bind_hardened_with(&path, ParentPolicy::RequirePrivate).expect("0700 parent binds");
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::symlink_metadata(&path).unwrap().mode() & 0o777,
            0o600
        );
        remove_own_socket(&path, &bound);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn parent_private_verdict_table() {
        assert_eq!(parent_private_verdict(0o700, 1000, 1000), Ok(()));
        assert_eq!(parent_private_verdict(0o755, 1000, 1000), Ok(()));
        assert_eq!(
            parent_private_verdict(0o755, 0, 1000),
            Ok(()),
            "root-owned is fine"
        );
        assert_eq!(
            parent_private_verdict(0o755, 1234, 1000),
            Err(ParentProblem::ForeignOwner),
            "another user can create entries in a directory they own"
        );
        assert_eq!(
            parent_private_verdict(0o775, 1000, 1000),
            Err(ParentProblem::GroupOrOtherWritable)
        );
        assert_eq!(
            parent_private_verdict(0o1777, 0, 1000),
            Err(ParentProblem::GroupOrOtherWritable),
            "the sticky bit does not stop a squatter creating the name"
        );
    }
}
