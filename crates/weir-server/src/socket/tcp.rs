//! TCP accept loop with mutual TLS (feature = "tls").
//!
//! Mirrors the Unix accept loop ([`super::serve`]): shares a global connection-limit
//! semaphore (passed in by the caller; the same `Arc<Semaphore>` is also given to
//! the Unix loop so the combined cap across both transports is exactly
//! `max_connections`), shard assignment through the shared
//! [`ShardRouter`](super::route::ShardRouter), and a graceful-shutdown watch
//! channel managed entirely inside `run`. The difference is the transport: this
//! loop accepts TCP and requires a mutual-TLS handshake (a CA-signed client
//! certificate) — completed under a handshake timeout — before the shared
//! [`handle_connection`] sees a single byte of application data.
//!
//! # Bound-addr design
//!
//! `run` takes an **already-bound** [`TcpListener`]. The CALLER binds and can
//! therefore read `local_addr()` before handing the listener over. This serves
//! both consumers cleanly:
//!
//! * production `main.rs` binds `config.tcp_bind` (a concrete address) through
//!   [`super::bind_all`], together with the Unix socket and before either
//!   accept loop serves, then passes the listener in; and
//! * integration tests configure the daemon with a fixed loopback port (chosen
//!   via the testkit's `free_port()` under the process lock), so the test
//!   already knows the address it will connect to.
//!
//! Binding in the caller also keeps the "fail fast on a bad bind address"
//! behaviour at startup rather than inside the spawned accept task.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time,
};
use tracing::{debug, error, info, warn};

use crate::{
    metrics::{Metrics, TlsHandshakeFailureLabel, TlsHandshakeFailureReason},
    models::WorkUnit,
    queue::QueueSender,
    socket::{
        connection::{ConnectionConfig, handle_connection},
        tls::ReloadableServerConfig,
    },
};

/// Configuration for the TCP+mTLS accept loop. Mirrors the relevant subset of
/// [`super::SocketConfig`] plus the TLS handshake timeout.
pub struct TcpConfig {
    /// Informational copy of the global max_connections limit. Used in the
    /// "connection limit reached" warning log. The actual semaphore that enforces
    /// the cap is passed as `sem` to [`run`] and is SHARED with the Unix listener
    /// — so the combined connection count across both transports is bounded by
    /// `max_connections`, not 2×max_connections.
    pub max_connections: usize,
    /// Per-connection payload cap in bytes. Effective cap is
    /// `min(max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`.
    pub max_payload_bytes: usize,
    /// Ceiling on records in one `PushBatch`; see `SocketConfig`.
    pub max_batch_records: usize,
    /// Total number of WAB shards. Informational; routing is the
    /// [`ShardRouter`](super::route::ShardRouter)'s, which `run` is handed.
    #[allow(dead_code, reason = "informational; routing is the ShardRouter's")]
    pub shard_count: usize,
    /// How long to wait for in-flight connections to finish after the shutdown
    /// signal before aborting them.
    pub shutdown_timeout_secs: u64,
    /// Per-connection idle read timeout in seconds (slowloris guard).
    pub connection_read_timeout_secs: u64,
    /// How long the TLS handshake may take before the connection is dropped.
    /// Caps a client that completes the TCP handshake but stalls the TLS one.
    pub handshake_timeout_secs: u64,
    /// Soft cap on live WAB bytes; `0` disables the check. A TLS producer fills
    /// the same WAB as a Unix-socket one, so the cap has to be enforced on both
    /// transports or it isn't enforced at all.
    pub wab_max_bytes: u64,
    /// Most recent WAB byte count — the SAME `Arc` the Unix listener gets.
    pub wab_bytes_now: Arc<AtomicU64>,
    /// Whether the cap is currently rejecting — the SAME `Arc` the Unix listener
    /// gets, so the hysteresis low-water mark is daemon-wide.
    pub wab_cap_rejecting: Arc<AtomicBool>,
    /// Set once another instance provably owns the `wab_dir` — the SAME `Arc`
    /// the Unix listener gets, so neither transport acks after the loss.
    pub wab_ownership_lost: Arc<AtomicBool>,
}

/// Binds nothing — accepts on the already-bound `listener` (see module docs for
/// the bound-addr rationale) — and drives the TCP+mTLS frame-parsing layer.
///
/// `sem` is the SHARED connection-cap semaphore (same `Arc<Semaphore>` as the
/// Unix listener). Both transports draw permits from the same pool, so the
/// combined cap across Unix + TCP is exactly `config.max_connections` — not
/// 2×max_connections. The handler-shutdown watch channel is created internally,
/// mirroring the Unix loop: after the accept loop breaks,
/// `handler_shutdown_tx.send(true)` fires BEFORE the drain so idle handlers exit
/// promptly without waiting out `connection_read_timeout_secs`.
///
/// Each accepted connection is pinned to the shard `router` picks; with no
/// shard able to take ingest the connection is dropped before the TLS
/// handshake and counted. Pass a clone of the Unix loop's router so both
/// transports share one refusal limiter.
///
/// Returns once the shutdown level leaves `Running` (a dropped level sender
/// counts as Graceful) and the connection drain has ended. Lame duck
/// (Foundations §5.7, §7): the listener is closed and marked `Closed` on
/// `board` BEFORE the drain, so new connects are refused at once; the drain
/// is bounded by `shutdown_timeout_secs`, and `Forced` aborts it at once
/// ([`super::drain_connections`]). `run` is the TCP listener's only writer
/// after the bind (README Q6): `Closed` at the lame duck, `Failed` on its
/// one `Err` return.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: TcpConfig,
    listener: TcpListener,
    tls: ReloadableServerConfig,
    queue_tx: QueueSender<WorkUnit>,
    sem: Arc<Semaphore>,
    mut level_rx: watch::Receiver<crate::lifecycle::ShutdownLevel>,
    metrics: Arc<Metrics>,
    router: crate::socket::route::ShardRouter,
    board: Arc<crate::status::StatusBoard>,
) -> std::io::Result<()> {
    // The only `?` in `run`: a later one must mark `Failed` the same way.
    let local_addr = listener.local_addr().inspect_err(|_| {
        board.listeners.tcp.set(weir_status::ListenerState::Failed);
    })?;
    info!(addr = %local_addr, "TCP+mTLS listener accepting");

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
        wab_bytes_now: Arc::clone(&config.wab_bytes_now),
        wab_cap_rejecting: Arc::clone(&config.wab_cap_rejecting),
        wab_ownership_lost: Arc::clone(&config.wab_ownership_lost),
    };
    let handshake_timeout = Duration::from_secs(config.handshake_timeout_secs);

    // Broadcasts shutdown to every in-flight handler so they can exit cleanly
    // between frames instead of being abort_all'd mid-await. Sender stays here;
    // each spawned handler clones the receiver. After the accept loop breaks we
    // send true BEFORE the drain — mirroring the Unix loop — so idle handlers
    // exit immediately on their next read-loop iteration rather than waiting out
    // connection_read_timeout_secs.
    let (handler_shutdown_tx, handler_shutdown_rx) = watch::channel(false);

    let mut join_set: JoinSet<()> = JoinSet::new();

    // Connection counter the router assigns shards from (the n-th accepted
    // connection); it can grow without bound for centuries at any realistic
    // accept rate.
    let conn_counter = AtomicU64::new(0);

    loop {
        tokio::select! {
            biased;
            _ = crate::lifecycle::shutdown_requested(&mut level_rx) => {
                info!("TCP manager: shutdown requested, stopping accept loop");
                break;
            }
            res = listener.accept() => {
                let accept_start = Instant::now();
                match res {
                    Ok((stream, peer_addr)) => {
                        // Reap handlers that have already finished so the JoinSet
                        // doesn't grow one node per lifetime connection — it is
                        // otherwise only drained at shutdown. Non-blocking (F1).
                        while join_set.try_join_next().is_some() {}
                        // Pin the connection to a shard that can take ingest
                        // (spec §5.7); none live drops it before the handshake.
                        let n = conn_counter.fetch_add(1, Ordering::Relaxed);
                        let Some(shard_id) = router.pick(n) else {
                            router.refuse(&metrics, "tcp");
                            drop(stream);
                            continue;
                        };
                        // Acquire a permit from the shared semaphore. This is the
                        // same Arc<Semaphore> as the Unix listener — the combined
                        // cap across both transports is config.max_connections.
                        let Ok(permit) = sem.clone().try_acquire_owned() else {
                            warn!(
                                %peer_addr,
                                "connection limit ({}) reached; dropping TCP connection",
                                config.max_connections
                            );
                            drop(stream);
                            continue;
                        };

                        // Disable Nagle: the wire protocol is request/response
                        // with small frames, so coalescing would add latency.
                        if let Err(e) = stream.set_nodelay(true) {
                            warn!(%peer_addr, error = %e, "set_nodelay failed; dropping connection");
                            drop(permit);
                            drop(stream);
                            continue;
                        }

                        let acceptor =
                            tokio_rustls::TlsAcceptor::from(tls.current());
                        let tx = queue_tx.clone();
                        let mut cfg = conn_cfg_template.clone();
                        cfg.shard_id = shard_id;
                        let m = Arc::clone(&metrics);
                        let mut handler_shutdown = handler_shutdown_rx.clone();

                        join_set.spawn(async move {
                            let _permit = permit;

                            // ── TLS handshake under a timeout, raced against
                            // shutdown ───────────────────────────────────────
                            // A client that completes the TCP accept then stalls
                            // mid-TLS-handshake at shutdown must not hold its
                            // permit + JoinSet slot until handshake_timeout,
                            // eating the drain window and forcing abort_all (F29).
                            // No application data has been exchanged yet, so
                            // dropping on shutdown is clean.
                            let handshake = tokio::select! {
                                biased;
                                res = time::timeout(handshake_timeout, acceptor.accept(stream)) => res,
                                _ = handler_shutdown.changed() => return,
                            };
                            let tls_stream = match handshake {
                                Ok(Ok(s)) => s,
                                Ok(Err(e)) => {
                                    let reason = classify_handshake_error(&e);
                                    m.tls_handshake_failures
                                        .get_or_create(&TlsHandshakeFailureLabel { reason })
                                        .inc();
                                    debug!(
                                        %peer_addr,
                                        error = %e,
                                        ?reason,
                                        "TLS handshake failed; dropping connection"
                                    );
                                    return;
                                }
                                Err(_elapsed) => {
                                    m.tls_handshake_failures
                                        .get_or_create(&TlsHandshakeFailureLabel {
                                            reason: TlsHandshakeFailureReason::timeout,
                                        })
                                        .inc();
                                    debug!(
                                        %peer_addr,
                                        timeout_secs = handshake_timeout.as_secs(),
                                        "TLS handshake timed out; dropping connection"
                                    );
                                    return;
                                }
                            };

                            // The mTLS verifier has already proved the client
                            // presented a CA-signed certificate by this point.
                            // (The client-cert Subject CN used to be logged here
                            // as a purely-diagnostic span field, but that pulled a
                            // full X.509 parser into the build for a debug-only
                            // label; dropped to keep the tls build slim.)
                            debug!(%peer_addr, "TLS handshake ok");

                            if let Err(e) =
                                handle_connection(tls_stream, tx, cfg, m, handler_shutdown).await
                                && e.kind() != std::io::ErrorKind::UnexpectedEof
                                && e.kind() != std::io::ErrorKind::ConnectionReset
                                && e.kind() != std::io::ErrorKind::BrokenPipe
                            {
                                warn!(%peer_addr, error = %e, "TCP connection closed with error");
                            }
                        });

                        metrics
                            .accept_latency
                            .observe(accept_start.elapsed().as_secs_f64());
                    }
                    Err(e) => {
                        error!(error = %e, "TCP accept error");
                        if super::is_accept_resource_exhaustion(&e) {
                            // Same rationale as the Unix loop: the pending
                            // connection stays queued, so an immediate retry
                            // busy-spins. Back off to yield the CPU until a
                            // descriptor or buffer frees.
                            metrics.accept_resource_exhaustion.inc();
                            time::sleep(super::ACCEPT_BACKOFF_ON_EXHAUSTION).await;
                        }
                    }
                }
            }
        }
    }

    // Lame duck (Foundations §7): close the TCP listener before the connection
    // drain, so new connects are refused at once instead of queueing in the
    // backlog for up to shutdown_timeout_secs.
    drop(listener);
    board.listeners.tcp.set(weir_status::ListenerState::Closed);
    info!(addr = %local_addr, "TCP manager: lame duck: listener closed; draining connections");

    // Handlers exit between frames (after acking a push they already read).
    // Sent BEFORE the bounded drain, so idle handlers exit at once and the
    // abort in drain_connections is the fallback.
    let _ = handler_shutdown_tx.send(true);
    super::drain_connections(
        &mut join_set,
        Duration::from_secs(config.shutdown_timeout_secs),
        &mut level_rx,
        &metrics,
        "TCP manager",
    )
    .await;
    Ok(())
}

/// Maps a rustls handshake error to a metric reason. rustls surfaces handshake
/// alerts through `std::io::Error` whose `to_string()` carries the underlying
/// `rustls::Error`; we classify on that text because rustls does not expose a
/// stable typed error through the tokio-rustls `io::Error` boundary.
fn classify_handshake_error(e: &std::io::Error) -> TlsHandshakeFailureReason {
    let msg = e.to_string();
    if msg.contains("NoCertificatesPresented")
        || msg.contains("peer sent no certificates")
        || msg.contains("CertificateRequired")
        || msg.contains("certificate required")
    {
        // The client presented no certificate but the verifier requires one.
        // As the SERVER, rustls surfaces this as `NoCertificatesPresented`
        // ("peer sent no certificates") — NOT the `CertificateRequired` alert
        // string, which is what the *client* sees. Match the server-side strings
        // first so this label actually fires (the documented unauthorized-probe
        // alert depends on it); the client-side strings are kept for safety.
        TlsHandshakeFailureReason::no_client_cert
    } else if msg.contains("InvalidCertificate")
        || msg.contains("UnknownIssuer")
        || msg.contains("UnknownCA")
        || msg.contains("unknown ca")
        || msg.contains("BadCertificate")
        || msg.contains("bad certificate")
        || msg.contains("DecryptError")
        || msg.contains("decrypt error")
        || msg.contains("Expired")
        || msg.contains("expired")
        || msg.contains("CertificateExpired")
        || msg.contains("BadEncoding")
        || msg.contains("NotValidForName")
    {
        // Client cert was presented but rejected: wrong CA (UnknownIssuer /
        // UnknownCA / DecryptError alert), expired (Expired / CertificateExpired),
        // or otherwise malformed (BadCertificate / BadEncoding / NotValidForName).
        // rustls 0.23 sends a DecryptError fatal alert when WebPki path
        // validation fails due to a wrong-CA cert, so that string must live in
        // this bucket rather than falling through to `other`.
        TlsHandshakeFailureReason::bad_cert
    } else {
        TlsHandshakeFailureReason::other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_no_client_cert() {
        // The SERVER-side string rustls actually emits when a client presents no
        // cert — this is the one that must classify, since weir is the server.
        for msg in [
            "peer sent no certificates", // rustls Error::NoCertificatesPresented Display
            "NoCertificatesPresented",
            "received fatal alert: CertificateRequired", // client-side, kept for safety
        ] {
            let e = std::io::Error::other(msg);
            assert_eq!(
                classify_handshake_error(&e),
                TlsHandshakeFailureReason::no_client_cert,
                "{msg:?} should classify as no_client_cert"
            );
        }
    }

    #[test]
    fn classify_bad_cert_unknown_issuer() {
        let e = std::io::Error::other("invalid peer certificate: UnknownIssuer");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::bad_cert
        );
    }

    #[test]
    fn classify_bad_cert_expired() {
        let e = std::io::Error::other("invalid peer certificate: Expired");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::bad_cert
        );
    }

    #[test]
    fn classify_bad_cert_decrypt_error() {
        // rustls 0.23 sends a DecryptError fatal alert when WebPki path
        // validation fails for a wrong-CA client cert. Verify it lands in
        // bad_cert, not other.
        let e = std::io::Error::other("received fatal alert: DecryptError");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::bad_cert
        );
    }

    #[test]
    fn classify_bad_cert_unknown_ca() {
        let e = std::io::Error::other("received fatal alert: UnknownCA");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::bad_cert
        );
    }

    #[test]
    fn classify_bad_cert_bad_certificate() {
        let e = std::io::Error::other("received fatal alert: BadCertificate");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::bad_cert
        );
    }

    #[test]
    fn classify_other_fallback() {
        let e = std::io::Error::other("peer closed connection abruptly");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::other
        );
    }

    /// Lame duck on the TCP side (Foundations §5.7, §7): on Graceful the
    /// listener is closed and marked `Closed` at once, BEFORE the connection
    /// drain, which is still waiting on an in-flight Durable push; Forced then
    /// cuts that 30 s drain short. Mirrors the Unix
    /// `lame_duck_unlinks_before_the_connection_drain_and_forced_cuts_it_short`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_lame_duck_closes_the_listener_before_the_connection_drain() {
        use crate::lifecycle::ShutdownLevel;

        let fx = weir_testkit::tls::TlsFixture::generate("tcp_lame_duck");
        let tls = ReloadableServerConfig::load(
            fx.server_cert_path.clone(),
            fx.server_key_path.clone(),
            fx.ca_cert_path.clone(),
        )
        .expect("load the fixture's TLS material");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let board = crate::status::StatusBoard::for_test_with(1, 1);
        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        let (queue_tx, queue_rx) = crate::queue::new::<WorkUnit>(1);
        // Kept alive and never consumed: the push waits for an ack that never
        // comes, so one handler (and therefore the drain) stays busy.
        let held = queue_rx.get(0);
        let config = TcpConfig {
            max_connections: 4,
            max_payload_bytes: 1 << 20,
            max_batch_records: 64,
            shard_count: 1,
            shutdown_timeout_secs: 30, // the drain is NOT what closes the listener
            connection_read_timeout_secs: 30,
            handshake_timeout_secs: 5,
            wab_max_bytes: 0,
            wab_bytes_now: Arc::new(AtomicU64::new(0)),
            wab_cap_rejecting: Arc::new(AtomicBool::new(false)),
            wab_ownership_lost: Arc::new(AtomicBool::new(false)),
        };
        let (level_tx, level_rx) = watch::channel(ShutdownLevel::Running);
        let handle = tokio::spawn(run(
            config,
            listener,
            tls,
            queue_tx,
            Arc::new(Semaphore::new(4)),
            level_rx,
            Arc::clone(&metrics),
            crate::socket::route::ShardRouter::round_robin(1),
            Arc::clone(&board),
        ));

        let (ca, cert, key) = (
            fx.ca_cert_path.clone(),
            fx.client_cert_path.clone(),
            fx.client_key_path.clone(),
        );
        let pusher = tokio::task::spawn_blocking(move || {
            let mut c = weir_client::WeirClient::connect_tls(
                addr,
                weir_client::ClientTlsConfig {
                    client_cert: &cert,
                    client_key: &key,
                    ca_cert: &ca,
                    server_name: "weir-server",
                    default_durability: None,
                },
            )
            .expect("mTLS connect");
            c.push(b"in-flight", weir_core::Durability::Durable)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while held.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the push never reached the queue"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Graceful);
        let t0 = Instant::now();
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "lame duck: the TCP listener must refuse new connects within 200 ms"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while board.listeners.tcp.get() != weir_status::ListenerState::Closed {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "the TCP listener was never marked Closed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !handle.is_finished(),
            "the connection drain must still be waiting on the in-flight push"
        );

        level_tx.send_replace(ShutdownLevel::Forced);
        time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("Forced must end the 30 s drain at once")
            .expect("task should not panic")
            .expect("run() should return Ok");
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        let _ = pusher.await; // the aborted connection fails the push
    }
}
