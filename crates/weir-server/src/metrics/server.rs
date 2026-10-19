//! Minimal HTTP exposition server for the `/metrics` endpoint.
//!
//! Bind a `TcpListener` and pass it to [`spawn`] along with the registry from
//! [`super::Metrics::new`]. The server runs as a tokio task and serves every
//! request with a fresh encode of the registry — no persistent state between scrapes.
//!
//! Only `GET /metrics` is expected. Any request path is accepted; the server
//! always returns the full metric exposition.

use std::sync::Arc;

use std::time::Duration;

use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinHandle,
};
use tracing::debug;

/// Backoff after an accept() error, to avoid a hot spin on a persistent
/// condition (e.g. EMFILE) while keeping metrics exposition alive.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Per-connection I/O deadline for a scrape. A scrape is a single tiny request
/// and a small response against a local Prometheus; anything slower than this is
/// a stuck or slowloris peer. Bounding read+write stops such a peer from parking
/// a handler (and its semaphore permit) forever — which would exhaust the small
/// connection pool and blind monitoring until restart.
const SCRAPE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Cheap slots for the head phase (spec §5.8 step 1). Every connection takes
/// one before anything else, and holds it for at most `HEAD_READ_TIMEOUT`.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
pub(crate) const HEAD_READ_SLOTS: usize = 64;
/// A peer that has not sent a complete request head by then is closed, so
/// it can no longer hold a scrape or probe permit for `SCRAPE_IO_TIMEOUT`.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
pub(crate) const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(1);
/// The request-head bound (spec §10): a head that has not ended by then is
/// not a scrape or a probe.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
pub(crate) const HEAD_MAX_BYTES: usize = 4096;
/// The four health routes' own pool: a scrape storm cannot starve kubelet,
/// and a probe flood cannot starve Prometheus.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
pub(crate) const PROBE_SLOTS: usize = 4;
/// After an oversized head: how long to read and discard what the peer still
/// sends after our FIN, so the close is not a RST.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
const OVERSIZE_DRAIN: Duration = Duration::from_millis(100);

/// How a request-head read ended.
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HeadRead {
    /// `\r\n\r\n` arrived. Holds every byte read.
    Complete(Vec<u8>),
    /// `max` bytes and no terminator.
    TooLarge(Vec<u8>),
    /// The peer half-closed before a terminator. Possibly empty.
    Eof(Vec<u8>),
    /// The deadline passed first.
    TimedOut,
    /// A read error.
    Failed,
}

/// Reads until `\r\n\r\n`, `max` bytes, EOF or `deadline`, whichever comes
/// first. Reading the whole head also drains the request headers, so a close
/// after responding sends FIN, not RST (spec §5.8).
#[cfg_attr(not(test), allow(dead_code, reason = "used by handle() from Task 5.8"))]
pub(crate) async fn read_head<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
    deadline: Duration,
) -> HeadRead {
    let read = async move {
        let mut buf: Vec<u8> = Vec::with_capacity(512);
        let mut chunk = [0u8; 1024];
        loop {
            let room = max.saturating_sub(buf.len()).min(chunk.len());
            let n = match r.read(&mut chunk[..room]).await {
                Ok(n) => n,
                Err(_) => return HeadRead::Failed,
            };
            if n == 0 {
                return HeadRead::Eof(buf);
            }
            // Re-scan 3 bytes of overlap: the terminator may straddle reads.
            let from = buf.len().saturating_sub(3);
            buf.extend_from_slice(&chunk[..n]);
            if buf[from..].windows(4).any(|w| w == b"\r\n\r\n") {
                return HeadRead::Complete(buf);
            }
            if buf.len() >= max {
                return HeadRead::TooLarge(buf);
            }
        }
    };
    match tokio::time::timeout(deadline, read).await {
        Ok(outcome) => outcome,
        Err(_) => HeadRead::TimedOut,
    }
}

/// Spawns the metrics HTTP server as a detached tokio task.
///
/// `max_connections` bounds the in-flight scrape concurrency. A scrape that
/// arrives while the semaphore is exhausted has its socket closed
/// immediately — Prometheus retries on the next scrape interval. This caps
/// the fork-bomb surface on an endpoint with no authentication.
///
/// The returned `JoinHandle` can be ignored if the server should run for the
/// lifetime of the process. The task never ends on its own: it owns `listener`,
/// and an accept error is logged and retried after a short backoff rather than
/// ending it (G14). It stops when its runtime shuts down or the handle is
/// aborted.
///
/// # Tokio runtime
///
/// Must be called from within a tokio runtime context (i.e. inside an `async` fn
/// or a task). The spawned task runs on the same runtime.
pub(crate) fn spawn(
    listener: TcpListener,
    registry: Arc<Registry>,
    max_connections: usize,
) -> JoinHandle<()> {
    tokio::spawn(serve(listener, registry, max_connections))
}

async fn serve(listener: TcpListener, registry: Arc<Registry>, max_connections: usize) {
    let semaphore = Arc::new(Semaphore::new(max_connections));
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                // A transient accept error (ECONNABORTED, or resource exhaustion
                // like EMFILE) must NOT take metrics exposition down permanently —
                // the previous `else { return }` did exactly that. Log, back off
                // briefly to avoid a hot spin on a persistent error, and continue;
                // Prometheus retries on its next scrape regardless (G14).
                debug!(error = %e, "metrics: accept error; backing off and continuing");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        // try_acquire_owned never blocks; if the cap is reached we drop the
        // connection immediately. Prometheus will retry on its next scrape
        // interval — no scraper holds a connection waiting.
        let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
            // stream drops here → kernel closes the connection
            continue;
        };
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            handle(stream, registry).await;
            drop(permit);
        });
    }
}

async fn handle(mut stream: TcpStream, registry: Arc<Registry>) {
    // Drain the request headers (we don't inspect them — any path returns
    // metrics). Bounded by SCRAPE_IO_TIMEOUT so a silent peer can't park this
    // handler (and its semaphore permit) forever (F20).
    let mut buf = [0u8; 4096];
    if tokio::time::timeout(SCRAPE_IO_TIMEOUT, stream.read(&mut buf))
        .await
        .is_err()
    {
        return; // request read timed out — drop the connection, releasing the permit
    }

    // Encode the registry into the OpenMetrics text format.
    let mut body = String::new();
    if encode(&mut body, &registry).is_err() {
        let _ = stream
            .write_all(b"HTTP/1.1 500 Internal Server Error\r\n\r\n")
            .await;
        return;
    }

    let body_bytes = body.as_bytes();
    let response_head = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         \r\n",
        body_bytes.len()
    );

    // Bound the writes too: a peer that stops reading mid-response must not pin
    // the handler/permit (F20).
    let write = async {
        stream.write_all(response_head.as_bytes()).await?;
        stream.write_all(body_bytes).await
    };
    let _ = tokio::time::timeout(SCRAPE_IO_TIMEOUT, write).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn scrape(addr: std::net::SocketAddr) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn endpoint_returns_200_ok() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 8);

        let response = scrape(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "unexpected response: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_content_type_is_openmetrics() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 8);

        let response = scrape(addr).await;
        assert!(
            response.contains("Content-Type: application/openmetrics-text"),
            "unexpected Content-Type in: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_body_contains_metric_names() {
        let (metrics, registry) = Metrics::new();
        metrics.recovery_records_replayed.inc_by(7);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 8);

        let response = scrape(addr).await;
        assert!(
            response.contains("weir_recovery_records_replayed_total 7"),
            "counter value not in response: {response}"
        );
        assert!(
            response.contains("weir_drain_state"),
            "drain_state metric missing: {response}"
        );
    }

    #[tokio::test]
    async fn endpoint_caps_concurrent_connections() {
        // With max_connections = 1, a second connection arriving while the
        // first is still being handled must be immediately closed. The cap
        // bounds the fork-bomb surface on an endpoint with no authn.
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 1);

        // First connection: open but send NOTHING. The handler will park in
        // `stream.read(...).await` waiting for request bytes, holding the
        // single permit indefinitely.
        let _conn1 = TcpStream::connect(addr).await.unwrap();

        // Let the server accept conn1 and spawn its handler.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Second connection: should be immediately dropped by the server.
        // try_acquire_owned returns Err, the stream is dropped, the kernel
        // closes the connection. The client sees EOF on first read.
        let mut conn2 = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 256];
        let read =
            tokio::time::timeout(std::time::Duration::from_millis(500), conn2.read(&mut buf))
                .await
                .expect("conn2 read should not block — server should close immediately")
                .expect("conn2 read should not error");
        assert_eq!(
            read,
            0,
            "conn2 should see EOF (0 bytes) — got {read} bytes: {:?}",
            &buf[..read]
        );
    }

    #[tokio::test]
    async fn endpoint_handles_multiple_requests() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 8);

        for _ in 0..3 {
            let response = scrape(addr).await;
            assert!(response.starts_with("HTTP/1.1 200 OK"));
        }
    }

    /// S34: the accept loop keeps serving after connections that fail at the
    /// handler level — each problematic connection is isolated to its own task and
    /// never takes the loop down. The OS-level `accept()` error branch
    /// (ECONNABORTED / EMFILE, the G14 `continue`) cannot be injected against a
    /// real `TcpListener` without an fd-exhaustion hack or an accept-source
    /// abstraction in production; this test pins the observable survival property
    /// that fix exists to protect.
    #[tokio::test]
    async fn endpoint_survives_problematic_connections() {
        use tokio::io::AsyncWriteExt;
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry), 8);

        // A connection that opens then immediately closes without a request.
        drop(TcpStream::connect(addr).await.unwrap());
        // A connection that sends a few junk bytes then closes mid-stream.
        {
            let mut bad = TcpStream::connect(addr).await.unwrap();
            let _ = bad.write_all(b"GARBAGE not http").await;
        }
        // Let the server accept and dispatch both before we scrape.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The loop must still be alive and serving.
        let response = scrape(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "metrics server stopped serving after problematic connections: {response}"
        );
    }

    use std::time::Instant;
    use tokio::io::duplex;

    #[tokio::test]
    async fn head_split_across_writes_and_across_the_terminator_is_complete() {
        let (mut client, mut server) = duplex(8192);
        let reader =
            tokio::spawn(
                async move { read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await },
            );
        client.write_all(b"GET /readyz HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"Host: x\r\n\r").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.write_all(b"\n").await.unwrap();
        assert_eq!(
            reader.await.unwrap(),
            HeadRead::Complete(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n".to_vec())
        );
    }

    #[tokio::test]
    async fn head_without_terminator_stops_at_the_byte_bound() {
        let (mut client, mut server) = duplex(16384);
        client.write_all(&[b'a'; 5000]).await.unwrap();
        match read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await {
            HeadRead::TooLarge(b) => assert_eq!(b.len(), HEAD_MAX_BYTES),
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn silent_peer_times_out_at_the_deadline() {
        let (mut client, mut server) = duplex(1024);
        client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap(); // no blank line, ever
        let t = Instant::now();
        assert_eq!(
            read_head(&mut server, HEAD_MAX_BYTES, Duration::from_millis(200)).await,
            HeadRead::TimedOut
        );
        let e = t.elapsed();
        assert!(
            e >= Duration::from_millis(190) && e < Duration::from_millis(800),
            "{e:?}"
        );
        drop(client);
    }

    #[tokio::test]
    async fn half_closed_peer_returns_what_it_sent() {
        let (mut client, mut server) = duplex(1024);
        client.write_all(b"GARBAGE not http").await.unwrap();
        drop(client);
        assert_eq!(
            read_head(&mut server, HEAD_MAX_BYTES, HEAD_READ_TIMEOUT).await,
            HeadRead::Eof(b"GARBAGE not http".to_vec())
        );
    }

    /// The head-phase constants are spec constants, not knobs (spec §13;
    /// plan Global Constraints): pinned here so a change is deliberate.
    #[test]
    fn head_phase_constants_are_the_spec_s() {
        assert_eq!(HEAD_READ_SLOTS, 64);
        assert_eq!(HEAD_READ_TIMEOUT, Duration::from_secs(1));
        assert_eq!(HEAD_MAX_BYTES, 4096);
        assert_eq!(PROBE_SLOTS, 4);
        // The post-oversize discard stays well inside the head deadline.
        assert!(OVERSIZE_DRAIN < HEAD_READ_TIMEOUT);
    }
}
