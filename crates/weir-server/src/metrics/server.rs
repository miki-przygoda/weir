//! Minimal HTTP exposition server for the `/metrics` endpoint.
//!
//! Bind a `TcpListener` and pass it to [`spawn`] along with the registry from
//! [`super::Metrics::new`]. The server runs as a tokio task and serves every
//! request with a fresh encode of the registry — no persistent state between scrapes.
//!
//! Only `GET /metrics` is expected. Any request path is accepted; the server
//! always returns the full metric exposition.

use std::sync::Arc;

use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinHandle,
};

/// Spawns the metrics HTTP server as a detached tokio task.
///
/// `max_connections` bounds the in-flight scrape concurrency. A scrape that
/// arrives while the semaphore is exhausted has its socket closed
/// immediately — Prometheus retries on the next scrape interval. This caps
/// the fork-bomb surface on an endpoint with no authentication.
///
/// The returned `JoinHandle` can be ignored if the server should run for the
/// lifetime of the process. The task exits when `listener` is dropped or an
/// unrecoverable accept error occurs.
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
        let Ok((stream, _peer)) = listener.accept().await else {
            return;
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
    // Drain the request headers (we don't inspect them — any path returns metrics).
    let mut buf = [0u8; 4096];
    let _ = stream.read(&mut buf).await;

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

    let _ = stream.write_all(response_head.as_bytes()).await;
    let _ = stream.write_all(body_bytes).await;
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
}
