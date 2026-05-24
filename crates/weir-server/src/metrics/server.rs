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
    task::JoinHandle,
};

/// Spawns the metrics HTTP server as a detached tokio task.
///
/// The returned `JoinHandle` can be ignored if the server should run for the
/// lifetime of the process. The task exits when `listener` is dropped or an
/// unrecoverable accept error occurs.
///
/// # Tokio runtime
///
/// Must be called from within a tokio runtime context (i.e. inside an `async` fn
/// or a task). The spawned task runs on the same runtime.
pub(crate) fn spawn(listener: TcpListener, registry: Arc<Registry>) -> JoinHandle<()> {
    tokio::spawn(serve(listener, registry))
}

async fn serve(listener: TcpListener, registry: Arc<Registry>) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => return,
        };
        tokio::spawn(handle(stream, Arc::clone(&registry)));
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
        let _handle = spawn(listener, Arc::new(registry));

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
        let _handle = spawn(listener, Arc::new(registry));

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
        let _handle = spawn(listener, Arc::new(registry));

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
    async fn endpoint_handles_multiple_requests() {
        let (_metrics, registry) = Metrics::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = spawn(listener, Arc::new(registry));

        for _ in 0..3 {
            let response = scrape(addr).await;
            assert!(response.starts_with("HTTP/1.1 200 OK"));
        }
    }
}
