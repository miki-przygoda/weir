//! TCP accept loop with mutual TLS (feature = "tls").
//!
//! Mirrors the Unix accept loop ([`super::run`]): shares a global connection-limit
//! semaphore (passed in by the caller; the same `Arc<Semaphore>` is also given to
//! the Unix loop so the combined cap across both transports is exactly
//! `max_connections`), round-robin shard assignment, and a graceful-shutdown watch
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
//! * production `main.rs` binds `config.tcp_bind` (a concrete address) and logs
//!   the bound address, then passes the listener in; and
//! * integration tests configure the daemon with a fixed loopback port (chosen
//!   via the testkit's `free_port()` under the process lock), so the test
//!   already knows the address it will connect to.
//!
//! Binding in the caller also keeps the "fail fast on a bad bind address"
//! behaviour at startup rather than inside the spawned accept task.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot, watch},
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
    /// Total number of WAB shards; new connections are assigned a shard_id
    /// round-robin (counter % shard_count).
    pub shard_count: usize,
    /// How long to wait for in-flight connections to finish after the shutdown
    /// signal before aborting them.
    pub shutdown_timeout_secs: u64,
    /// Per-connection idle read timeout in seconds (slowloris guard).
    pub connection_read_timeout_secs: u64,
    /// How long the TLS handshake may take before the connection is dropped.
    /// Caps a client that completes the TCP handshake but stalls the TLS one.
    pub handshake_timeout_secs: u64,
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
/// Returns when `shutdown_rx` fires or is dropped, after draining in-flight
/// connections within `shutdown_timeout_secs`.
pub async fn run(
    config: TcpConfig,
    listener: TcpListener,
    tls: ReloadableServerConfig,
    queue_tx: QueueSender<WorkUnit>,
    sem: Arc<Semaphore>,
    shutdown_rx: oneshot::Receiver<()>,
    metrics: Arc<Metrics>,
) -> std::io::Result<()> {
    let local_addr = listener.local_addr()?;
    info!(addr = %local_addr, "TCP+mTLS listener accepting");

    let effective_cap = config
        .max_payload_bytes
        .min(weir_core::MAX_PAYLOAD_HARD_CAP);
    let conn_cfg_template = ConnectionConfig {
        max_payload_bytes: effective_cap,
        read_timeout: Duration::from_secs(config.connection_read_timeout_secs),
        ack_timeout: crate::socket::connection::ACK_TIMEOUT,
        shard_id: 0, // overridden per connection below
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
    let mut shutdown = std::pin::pin!(shutdown_rx);

    // Round-robin connection counter for shard_id assignment. Same wrap-via-
    // modulo story as the Unix loop; the counter can grow without bound for
    // centuries at any realistic accept rate.
    let conn_counter = AtomicU64::new(0);
    // Config validation enforces shard_count >= 1; the local guard documents
    // the invariant and keeps the modulo safe.
    let shard_count = config.shard_count.max(1) as u64;

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("TCP manager: shutdown signal received, stopping accept loop");
                break;
            }
            res = listener.accept() => {
                let accept_start = Instant::now();
                match res {
                    Ok((stream, peer_addr)) => {
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
                        let n = conn_counter.fetch_add(1, Ordering::Relaxed);
                        cfg.shard_id = (n % shard_count) as u32;
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

                            // Best-effort client-cert CN for the tracing span.
                            // Subject CN is NOT a security control (the mTLS
                            // verifier already proved a CA-signed client cert);
                            // it's purely diagnostic. See client_cert_cn.
                            let cn = client_cert_cn(&tls_stream);
                            debug!(%peer_addr, client_cn = ?cn, "TLS handshake ok");

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

    // Broadcast shutdown to every in-flight handler so they exit at the top of
    // their next read-loop iteration (after acking any push they were
    // processing). Doing this BEFORE the drain timeout means most handlers
    // complete naturally; abort_all becomes the emergency fallback.
    let _ = handler_shutdown_tx.send(true);

    // Drain in-flight connections within the configured timeout. With
    // handler_shutdown signalling, idle handlers exit immediately on the next
    // loop check; active handlers exit after their current push completes
    // (ack/nack capped by ACK_TIMEOUT). The timeout should be >= ACK_TIMEOUT +
    // buffer so legitimate in-flight work completes before abort_all is reached.
    let timeout = Duration::from_secs(config.shutdown_timeout_secs);
    match time::timeout(timeout, drain_join_set(&mut join_set)).await {
        Ok(()) => {
            info!("TCP manager: all connections drained cleanly");
        }
        Err(_elapsed) => {
            error!(
                remaining = join_set.len(),
                timeout_secs = config.shutdown_timeout_secs,
                "TCP manager: shutdown timeout reached; aborting remaining connections"
            );
            metrics
                .connections_aborted_at_shutdown
                .inc_by(join_set.len() as u64);
            join_set.abort_all();
        }
    }

    Ok(())
}

async fn drain_join_set(join_set: &mut JoinSet<()>) {
    while join_set.join_next().await.is_some() {}
}

/// Maps a rustls handshake error to a metric reason. rustls surfaces handshake
/// alerts through `std::io::Error` whose `to_string()` carries the underlying
/// `rustls::Error`; we classify on that text because rustls does not expose a
/// stable typed error through the tokio-rustls `io::Error` boundary.
fn classify_handshake_error(e: &std::io::Error) -> TlsHandshakeFailureReason {
    let msg = e.to_string();
    if msg.contains("CertificateRequired") || msg.contains("certificate required") {
        // The client presented no certificate but the verifier requires one.
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

/// Best-effort client-certificate Common Name for the tracing span.
///
/// Returns `None` when no peer certificate is present or it can't be parsed.
/// This is purely diagnostic: the mTLS verifier has already proven the client
/// presented a CA-signed certificate before this runs, so the CN is not used
/// for any authorization decision. (Test fixtures set every Subject CN to the
/// generic rcgen value, so callers must NOT key behaviour off this.)
fn client_cert_cn<IO>(stream: &tokio_rustls::server::TlsStream<IO>) -> Option<String> {
    let (_io, conn) = stream.get_ref();
    let cert = conn.peer_certificates()?.first()?;
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    parsed
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_no_client_cert() {
        let e = std::io::Error::other("received fatal alert: CertificateRequired");
        assert_eq!(
            classify_handshake_error(&e),
            TlsHandshakeFailureReason::no_client_cert
        );
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
}
