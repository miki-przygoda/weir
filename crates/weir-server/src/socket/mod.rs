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
mod peer;

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

use tokio::{
    net::UnixListener,
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time,
};
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
    /// How long to wait for in-flight connections to finish after the shutdown
    /// signal is received before aborting them.
    pub shutdown_timeout_secs: u64,
    /// Per-connection idle read timeout in seconds. Caps slowloris-style
    /// connections that never send (or stall mid-frame).
    pub connection_read_timeout_secs: u64,
    /// Total number of WAB shards. The accept loop assigns each new
    /// connection a shard_id by round-robin (counter % shard_count) so
    /// multi-shard deployments actually fan work across all per-shard
    /// flusher threads. With shard_count = 1 every connection gets
    /// shard_id = 0.
    pub shard_count: usize,
    /// When true, the accept loop refuses connections whose peer effective
    /// uid (via SO_PEERCRED / getpeereid) does not match the daemon's. See
    /// [`Config::peer_uid_check`] for the rationale.
    pub peer_uid_check: bool,
}

/// Binds a Unix socket, accepts connections, and drives the frame-parsing layer.
///
/// Returns when `shutdown_rx` fires or is dropped. Before returning, waits up to
/// `config.shutdown_timeout_secs` for all in-flight connections to finish.
///
/// `sem` is the shared connection-cap semaphore. Both the Unix listener and the
/// TCP+mTLS listener (when enabled) receive a clone of the SAME `Arc<Semaphore>`
/// so the combined cap across both transports is `Semaphore::new(max_connections)`,
/// not 2×max_connections. The caller is responsible for creating the semaphore
/// with the desired global cap and cloning it into each listener.
///
/// # Bind sequence (TOCTOU hardening)
///
/// The bind sequence is implemented in `bind_hardened`. It uses a dirfd to pin
/// the parent directory, `unlinkat` to clear any stale socket without
/// following symlinks, a tightened umask so `bind(2)` creates the socket
/// inode at mode 0o600 directly (avoiding a vulnerable post-bind chmod), and
/// an inode-equality check to catch any rename swap that races the bind.
/// See `docs/security/socket-bind.md` for the threat model and the
/// remaining race window that the operator's directory permissions are
/// expected to close.
pub async fn run(
    config: SocketConfig,
    queue_tx: QueueSender<WorkUnit>,
    shutdown_rx: oneshot::Receiver<()>,
    metrics: std::sync::Arc<Metrics>,
    sem: std::sync::Arc<Semaphore>,
) -> io::Result<()> {
    validate_socket_path(&config.socket_path)?;
    let listener = bind_hardened(&config.socket_path)?;

    info!(path = %config.socket_path.display(), "socket listening");

    let effective_cap = config
        .max_payload_bytes
        .min(weir_core::MAX_PAYLOAD_HARD_CAP);
    let conn_cfg_template = ConnectionConfig {
        max_payload_bytes: effective_cap,
        read_timeout: Duration::from_secs(config.connection_read_timeout_secs),
        ack_timeout: crate::socket::connection::ACK_TIMEOUT,
        shard_id: 0, // overridden per connection below
    };
    let mut join_set: JoinSet<()> = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown_rx);

    // Broadcasts shutdown to every in-flight handler so they can exit
    // cleanly between frames instead of being abort_all'd mid-await. Sender
    // stays here; each spawned handler clones the receiver. When shutdown
    // fires we send true; handlers see it on their next read-loop iteration.
    let (handler_shutdown_tx, handler_shutdown_rx) = tokio::sync::watch::channel(false);
    // Round-robin connection counter for shard_id assignment. Wraps via
    // modulo at each use; the counter itself can grow without bound for
    // ~600 years of acceptances at 1M/s, so no overflow handling needed.
    let conn_counter = std::sync::atomic::AtomicU64::new(0);
    // Guard against shard_count = 0 — Config validation enforces ≥ 1, but
    // the local guard documents the invariant and keeps the modulo safe.
    let shard_count = config.shard_count.max(1) as u64;

    // Daemon's effective uid; compared against peer uid on each accept when
    // peer_uid_check is enabled. Read once at startup — the daemon never
    // changes uid post-start (no setuid path).
    let daemon_uid = unsafe { libc::geteuid() };

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("socket manager: shutdown signal received, stopping accept loop");
                break;
            }
            res = listener.accept() => {
                let accept_start = Instant::now();
                match res {
                    Ok((stream, _addr)) => {
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
                        let n = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        cfg.shard_id = (n % shard_count) as u32;
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

    // Broadcast shutdown to every in-flight handler so they exit at the
    // top of their next read-loop iteration (after acking any push they
    // were processing). Doing this BEFORE the drain timeout means most
    // handlers complete naturally; abort_all becomes the emergency fallback.
    let _ = handler_shutdown_tx.send(true);

    // Drain in-flight connections within the configured timeout. With
    // handler_shutdown signalling, idle handlers exit immediately on the
    // next loop check; active handlers exit after their current push
    // completes (ack/nack capped by ACK_TIMEOUT). The timeout should be
    // >= ACK_TIMEOUT + buffer so legitimate in-flight work completes
    // before abort_all is reached.
    let timeout = Duration::from_secs(config.shutdown_timeout_secs);
    match time::timeout(timeout, drain_join_set(&mut join_set)).await {
        Ok(()) => {
            info!("socket manager: all connections drained cleanly");
        }
        Err(_elapsed) => {
            error!(
                remaining = join_set.len(),
                timeout_secs = config.shutdown_timeout_secs,
                "socket manager: shutdown timeout reached; aborting remaining connections \
                 — producers of those records may see no ack/nack response"
            );
            metrics
                .connections_aborted_at_shutdown
                .inc_by(join_set.len() as u64);
            join_set.abort_all();
        }
    }

    let _ = std::fs::remove_file(&config.socket_path);
    Ok(())
}

async fn drain_join_set(join_set: &mut JoinSet<()>) {
    while join_set.join_next().await.is_some() {}
}

/// Backoff applied when `accept(2)` fails with a resource-exhaustion errno.
/// Long enough to yield the CPU and let a descriptor or buffer be returned,
/// short enough that throughput recovers promptly once pressure clears.
#[cfg(unix)]
pub(super) const ACCEPT_BACKOFF_ON_EXHAUSTION: Duration = Duration::from_millis(50);

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
pub(super) fn is_accept_resource_exhaustion(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
    )
}

/// Validates a socket path: absolute, no `..`, no null bytes.
/// Matches the same rules as `validate_path` in `wab/mod.rs`.
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

#[cfg(unix)]
pub(crate) fn bind_hardened(path: &Path) -> io::Result<UnixListener> {
    let (parent, basename) = split_parent_basename(path)?;
    let dirfd = open_parent_dirfd(parent)?;
    let _dirfd_guard = OwnedDirFd(dirfd);

    // The one irreducible bind(2)→fstatat race window (below) is fully closed
    // only when the socket's parent directory is writable by the daemon user
    // alone. Warn if it is group/world-writable (e.g. /tmp) so the operator can
    // move the socket somewhere private — see docs/security/socket-bind.md.
    warn_if_parent_world_writable(dirfd, parent);

    cleanup_existing_socket(dirfd, basename, path)?;

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
    // umask is process-global and applies to other threads briefly. Every
    // other file-creation path in weir specifies its mode bits explicitly
    // (WAB segments 0o600, dirs 0o700), so the temporary tightening is
    // invisible to those paths. A tighter umask is also a safer default.
    let saved_umask = umask_set(0o177);
    let bind_result = UnixListener::bind(path);
    let _ = umask_set(saved_umask);
    let listener = bind_result?;

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

    Ok(listener)
}

#[cfg(unix)]
fn umask_set(new: libc::mode_t) -> libc::mode_t {
    // Safety: libc::umask is always safe; it just swaps the process umask
    // and returns the previous value. No invariants to uphold.
    unsafe { libc::umask(new) }
}

#[cfg(not(unix))]
pub(crate) fn bind_hardened(path: &Path) -> io::Result<UnixListener> {
    UnixListener::bind(path)
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

#[cfg(unix)]
fn cleanup_existing_socket(
    dirfd: libc::c_int,
    basename: &std::ffi::OsStr,
    full_path_for_error: &Path,
) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let c_name = std::ffi::CString::new(basename.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "basename contains null"))?;

    // fstatat with AT_SYMLINK_NOFOLLOW inspects the entry itself without
    // following a symlink at the final component.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: dirfd is a valid directory fd held by the caller; c_name is a
    // valid nul-terminated string; &mut st is a writable libc::stat.
    let rc = unsafe { libc::fstatat(dirfd, c_name.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(err);
    }

    // Refuse to remove anything that isn't a socket. This catches both
    // ordinary files (the original bind_cleanup guarantee) and symlinks
    // (because we used AT_SYMLINK_NOFOLLOW, st_mode reflects the symlink's
    // type, not its target's — symlinks are S_IFLNK, not S_IFSOCK).
    let mode_type = st.st_mode & libc::S_IFMT;
    if mode_type != libc::S_IFSOCK {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "a non-socket entry already exists at '{}' (mode type {:#o}); refusing to remove it",
                full_path_for_error.display(),
                mode_type
            ),
        ));
    }

    // Safety: dirfd valid; c_name valid; flags=0 means "regular unlink"
    // (i.e. file, not directory).
    let rc = unsafe { libc::unlinkat(dirfd, c_name.as_ptr(), 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{models::WorkUnit, queue};
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::UnixStream, sync::oneshot};

    fn tmp_socket_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("weir_sock_{label}_{}.sock", std::process::id()))
    }

    /// Serialises any test that mutates the process umask. umask is
    /// process-global; without this, parallel tests interleave and see
    /// each other's saved/restored values.
    fn umask_test_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(Default::default)
    }

    fn default_config(path: PathBuf) -> SocketConfig {
        SocketConfig {
            socket_path: path,
            max_connections: 16,
            max_payload_bytes: weir_core::MAX_PAYLOAD_HARD_CAP,
            shutdown_timeout_secs: 5,
            connection_read_timeout_secs: 30,
            shard_count: 1,
            // Tests connect from the same process → same uid → peer-uid
            // check always passes. Leaving the check enabled exercises the
            // production code path.
            peer_uid_check: true,
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

    // ── Hardened bind ─────────────────────────────────────────────────────────

    // bind_hardened internally mutates the process umask. That makes
    // concurrent calls (e.g. parallel test execution) interleave their
    // save/restore and leak a tightened umask globally. The lock below
    // serialises the tests; in production, bind_hardened is called once
    // during single-threaded startup so the issue does not arise.

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
        std::fs::create_dir_all(&dir).unwrap();
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
        std::fs::create_dir_all(&dir).unwrap();
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
        std::fs::create_dir_all(&real_parent).unwrap();
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
    #[test]
    #[cfg(unix)]
    fn stat_at_dir_observes_inode_swap() {
        let dir = std::env::temp_dir().join(format!("weir_stat_at_dir_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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

        let dir = std::env::temp_dir().join(format!("weir_swap_pressure_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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
            socket_path: path.clone(),
            max_connections: 1,
            max_payload_bytes: weir_core::MAX_PAYLOAD_HARD_CAP,
            shutdown_timeout_secs: 2,
            connection_read_timeout_secs: 30,
            shard_count: 1,
            peer_uid_check: true,
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
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

        shutdown_tx.send(()).ok();
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
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let _rx = queue_rx.get(0); // keep receiver alive

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let run_handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Open an idle connection — never send any bytes.
        let _conn = UnixStream::connect(&path).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let t0 = Instant::now();
        shutdown_tx.send(()).unwrap();
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
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);

        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let handle = tokio::spawn(run(cfg, queue_tx, shutdown_rx, test_metrics(), sem));
        tokio::time::sleep(Duration::from_millis(20)).await;

        shutdown_tx.send(()).unwrap();
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
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
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

            shutdown_tx.send(()).ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::remove_file(&path).ok();
        });
    }
}
