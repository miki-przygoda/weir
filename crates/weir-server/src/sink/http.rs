//! HTTP sink: POSTs each record as a `application/octet-stream` body to a
//! configurable URL. Classifies HTTP/network failures into the
//! transient/permanent buckets the drain expects.
//!
//! # Protocol
//!
//! - One HTTP POST per record. The body is the raw payload bytes; no
//!   serialisation, no encoding, no framing — endpoints decide what to do
//!   with the bytes.
//! - `Content-Type: application/octet-stream`.
//! - Optional `Authorization: Bearer <token>` if `WEIR_SINK_BEARER_TOKEN`
//!   was set at startup (token never appears in config files or logs).
//! - Per-request timeout via `sink_timeout_secs`.
//!
//! # Error classification
//!
//! - **Committed** (2xx): record is accepted, drain confirms.
//! - **Permanent** (4xx except 408/429): bad request from the daemon's POV;
//!   record goes to dead-letter.
//! - **Transient** (408, 429, 5xx, connect error, timeout, body-send error):
//!   the drain retries the whole segment. Per-record idempotency is the
//!   endpoint's responsibility (documented in the sink trait).
//!
//! # Why one POST per record (for now)
//!
//! Simple endpoint contract: any HTTP server that accepts a POST body
//! works. No need to agree on a batch format. Tradeoff is N round-trips
//! per batch; a future iteration could add a batched mode for endpoints
//! that support it.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use tracing::{debug, warn};
use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

/// Configuration for `HttpSink`.
///
/// `bearer_token` is deliberately omitted from the `Debug` impl so it never
/// reaches a log line via `?config` interpolation.
#[derive(Clone)]
pub struct HttpSinkConfig {
    pub url: String,
    pub timeout: Duration,
    pub max_batch_size: usize,
    /// Optional bearer token. Read from env at startup (`WEIR_SINK_BEARER_TOKEN`);
    /// never sourced from the config file. Wrapped in `Arc` so the value can be
    /// shared without copying.
    pub bearer_token: Option<Arc<str>>,
}

impl std::fmt::Debug for HttpSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSinkConfig")
            .field("url", &self.url)
            .field("timeout", &self.timeout)
            .field("max_batch_size", &self.max_batch_size)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// HTTP sink. Holds a reusable `reqwest::Client` (TLS context, connection
/// pool); cheap to clone.
#[derive(Debug)]
pub struct HttpSink {
    config: HttpSinkConfig,
    client: Client,
}

impl HttpSink {
    /// Build a new HTTP sink. Fails if the URL is invalid or the TLS stack
    /// can't be initialised (typically a rustls feature-flag misconfiguration).
    pub fn new(config: HttpSinkConfig) -> Result<Self, HttpSinkBuildError> {
        if config.url.is_empty() {
            return Err(HttpSinkBuildError::EmptyUrl);
        }
        // Parse the URL to fail fast at startup rather than per-request.
        let _ = reqwest::Url::parse(&config.url)
            .map_err(|e| HttpSinkBuildError::InvalidUrl(e.to_string()))?;

        let client = Client::builder()
            .timeout(config.timeout)
            // Keep idle connections warm between batches. The drain calls
            // commit in a tight loop while a segment is being processed.
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .pool_max_idle_per_host(8)
            .user_agent(concat!("weir/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(HttpSinkBuildError::ClientBuild)?;

        Ok(Self { config, client })
    }

    async fn post_record(&self, payload: &[u8]) -> Result<(), HttpSinkError> {
        let mut req = self
            .client
            .post(&self.config.url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(payload.to_vec());

        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token.as_ref());
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                // Network-layer errors are all transient. Connect refused,
                // DNS failure, timeout, broken pipe — drain retries.
                debug!(error = %e, "sink POST failed at transport layer");
                return Err(HttpSinkError::Transport(e.to_string()));
            }
        };

        let status = resp.status();
        if status.is_success() {
            // Drain the body to release the connection back to the pool.
            // We don't care about the response body for v0 (the endpoint's
            // status code is the only signal).
            let _ = resp.bytes().await;
            return Ok(());
        }

        if classify_status_transient(status) {
            debug!(status = %status, "sink POST returned transient HTTP status");
            return Err(HttpSinkError::TransientStatus(status));
        }

        // Permanent. Capture a short body excerpt for the dead-letter reason
        // string so operators can debug what the endpoint complained about.
        // Cap at 256 bytes to avoid logging unbounded server output.
        let body_excerpt = match resp.bytes().await {
            Ok(bytes) => {
                let cut = bytes.len().min(256);
                String::from_utf8_lossy(&bytes[..cut]).into_owned()
            }
            Err(_) => String::from("<body read failed>"),
        };
        Err(HttpSinkError::PermanentStatus {
            status,
            body_excerpt,
        })
    }
}

/// Returns true if the HTTP status code should be classified as transient
/// (the drain should retry) vs permanent (dead-letter).
fn classify_status_transient(status: StatusCode) -> bool {
    // 408 Request Timeout: server-side hint to retry.
    // 429 Too Many Requests: explicit rate-limit; absolutely retry.
    // 5xx: server errors are always transient from the client's POV.
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

#[derive(Debug)]
pub enum HttpSinkError {
    /// reqwest transport error (connect, timeout, body send). Always transient.
    Transport(String),
    /// HTTP status code in the transient bucket (408/429/5xx).
    TransientStatus(StatusCode),
    /// HTTP status code in the permanent bucket (4xx except 408/429).
    PermanentStatus {
        status: StatusCode,
        body_excerpt: String,
    },
}

impl std::fmt::Display for HttpSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpSinkError::Transport(e) => write!(f, "http sink transport error: {e}"),
            HttpSinkError::TransientStatus(s) => {
                write!(f, "http sink transient status: {s}")
            }
            HttpSinkError::PermanentStatus {
                status,
                body_excerpt,
            } => write!(
                f,
                "http sink permanent status: {status}; body: {body_excerpt}"
            ),
        }
    }
}

impl std::error::Error for HttpSinkError {}

impl SinkError for HttpSinkError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            HttpSinkError::Transport(_) | HttpSinkError::TransientStatus(_)
        )
    }
}

/// Errors that can occur during `HttpSink::new()`.
#[derive(Debug)]
pub enum HttpSinkBuildError {
    EmptyUrl,
    InvalidUrl(String),
    ClientBuild(reqwest::Error),
}

impl std::fmt::Display for HttpSinkBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpSinkBuildError::EmptyUrl => write!(f, "http sink url is empty"),
            HttpSinkBuildError::InvalidUrl(e) => write!(f, "http sink url invalid: {e}"),
            HttpSinkBuildError::ClientBuild(e) => write!(f, "http sink client build failed: {e}"),
        }
    }
}

impl std::error::Error for HttpSinkBuildError {}

impl Sink for HttpSink {
    type Record = Payload;
    type Error = HttpSinkError;

    async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, HttpSinkError> {
        let mut committed = Vec::with_capacity(batch.len());
        let mut dead_lettered: Vec<(Payload, String)> = Vec::new();

        for record in batch {
            match self.post_record(&record).await {
                Ok(()) => committed.push(record),
                Err(HttpSinkError::PermanentStatus {
                    status,
                    body_excerpt,
                }) => {
                    warn!(
                        status = %status,
                        body = %body_excerpt,
                        "record permanently rejected by HTTP sink; dead-lettering"
                    );
                    dead_lettered.push((record, format!("http {status}: {body_excerpt}")));
                }
                Err(e) => {
                    // Transient: abort the batch so the drain retries the
                    // whole segment. Records already committed in this call
                    // may be re-sent — the idempotency contract on the
                    // endpoint covers this (documented in sink/mod.rs).
                    return Err(e);
                }
            }
        }

        Ok(CommitResult {
            committed,
            dead_lettered,
        })
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        // Coarse health probe: HEAD the URL. If it returns 2xx/3xx, healthy;
        // 4xx (other than auth challenges) suggests an endpoint misconfig
        // (degraded); 5xx or transport failure suggests the endpoint is down.
        match self.client.head(&self.config.url).send().await {
            Ok(resp) if resp.status().is_success() || resp.status().is_redirection() => {
                SinkHealth::Healthy
            }
            Ok(resp) if resp.status().is_client_error() => {
                SinkHealth::Degraded(format!("HEAD returned {}", resp.status()))
            }
            Ok(resp) => SinkHealth::Down(format!("HEAD returned {}", resp.status())),
            Err(e) => SinkHealth::Down(format!("HEAD failed: {e}")),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Spawns a minimal HTTP/1.1 server that answers each incoming request
    /// with the next response from `responses`, cycling if exhausted. Returns
    /// the bound address and a counter that tracks the number of requests
    /// served (incremented after the response is sent).
    ///
    /// Not RFC-perfect — just enough to drive the sink tests. Reads request
    /// bytes until it sees `\r\n\r\n` plus the Content-Length body (if any),
    /// then writes the canned response and closes the connection.
    async fn spawn_mock_server(responses: Vec<&'static str>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_for_task = Arc::clone(&counter);

        tokio::spawn(async move {
            let mut idx = 0usize;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let response = responses[idx % responses.len().max(1)];
                idx += 1;
                let counter = Arc::clone(&counter_for_task);

                tokio::spawn(async move {
                    // Read request bytes until we have the header block.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    let mut header_end = None;
                    while header_end.is_none() {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if let Some(pos) = find_double_crlf(&buf) {
                                    header_end = Some(pos);
                                }
                            }
                        }
                    }
                    let header_end = header_end.unwrap();
                    // Parse Content-Length to know how much body to read.
                    let header_str = String::from_utf8_lossy(&buf[..header_end]);
                    let content_length = parse_content_length(&header_str).unwrap_or(0);
                    let already_have = buf.len().saturating_sub(header_end + 4);
                    let still_need = content_length.saturating_sub(already_have);
                    let mut remaining = still_need;
                    while remaining > 0 {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                remaining = remaining.saturating_sub(n);
                            }
                        }
                    }
                    // Write canned response.
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
        });

        (url, counter)
    }

    fn find_double_crlf(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn parse_content_length(headers: &str) -> Option<usize> {
        for line in headers.lines() {
            if let Some((name, value)) = line.split_once(':')
                && name.trim().eq_ignore_ascii_case("content-length")
            {
                return value.trim().parse().ok();
            }
        }
        None
    }

    fn cfg(url: &str) -> HttpSinkConfig {
        HttpSinkConfig {
            url: url.to_string(),
            timeout: Duration::from_secs(5),
            max_batch_size: 100,
            bearer_token: None,
        }
    }

    #[tokio::test]
    async fn empty_url_rejected_at_build() {
        let mut c = cfg("");
        c.url = String::new();
        let err = HttpSink::new(c).unwrap_err();
        assert!(matches!(err, HttpSinkBuildError::EmptyUrl));
    }

    #[tokio::test]
    async fn invalid_url_rejected_at_build() {
        let err = HttpSink::new(cfg("not-a-url")).unwrap_err();
        assert!(matches!(err, HttpSinkBuildError::InvalidUrl(_)));
    }

    #[tokio::test]
    async fn http_200_records_committed() {
        let (url, counter) =
            spawn_mock_server(vec!["HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]).await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink
            .commit(vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()])
            .await
            .unwrap();
        assert_eq!(result.committed.len(), 3);
        assert!(result.dead_lettered.is_empty());
        // Give the mock server a moment to finish counting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn http_400_dead_letters() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nbad payload!",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![b"alpha".to_vec()]).await.unwrap();
        assert!(result.committed.is_empty());
        assert_eq!(result.dead_lettered.len(), 1);
        let (_, reason) = &result.dead_lettered[0];
        assert!(reason.contains("400"), "reason: {reason}");
        assert!(reason.contains("bad payload"), "reason: {reason}");
    }

    #[tokio::test]
    async fn http_500_returns_transient_error() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![b"alpha".to_vec()]).await.unwrap_err();
        assert!(err.is_transient(), "500 must be transient: {err}");
        assert!(matches!(err, HttpSinkError::TransientStatus(s) if s.as_u16() == 500));
    }

    #[tokio::test]
    async fn http_429_returns_transient_error() {
        // 429 Too Many Requests: explicit rate-limit signal.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![b"alpha".to_vec()]).await.unwrap_err();
        assert!(err.is_transient(), "429 must be transient: {err}");
    }

    #[tokio::test]
    async fn http_408_returns_transient_error() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 408 Request Timeout\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![b"alpha".to_vec()]).await.unwrap_err();
        assert!(err.is_transient(), "408 must be transient: {err}");
    }

    #[tokio::test]
    async fn http_401_dead_letters() {
        // 401 Unauthorized: permanent from the daemon's POV; retrying with
        // the same token won't change the answer.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![b"alpha".to_vec()]).await.unwrap();
        assert_eq!(result.dead_lettered.len(), 1);
    }

    #[tokio::test]
    async fn connect_refused_is_transient() {
        // Pick a port that's almost certainly closed.
        let sink = HttpSink::new(cfg("http://127.0.0.1:1/")).unwrap();
        let err = sink.commit(vec![b"alpha".to_vec()]).await.unwrap_err();
        assert!(
            err.is_transient(),
            "connect refused must be transient: {err}"
        );
        assert!(matches!(err, HttpSinkError::Transport(_)));
    }

    #[tokio::test]
    async fn mixed_batch_committed_and_dead_lettered() {
        // First request 200, second 400, third 200. Order of records in the
        // batch determines which gets which response.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink
            .commit(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .await
            .unwrap();
        assert_eq!(result.committed.len(), 2);
        assert_eq!(result.dead_lettered.len(), 1);
    }

    #[tokio::test]
    async fn transient_in_middle_of_batch_returns_err() {
        // First 200, second 500. Per the sink contract, a transient error
        // mid-batch aborts so the drain can retry the whole segment.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink
            .commit(vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap_err();
        assert!(err.is_transient());
    }

    #[tokio::test]
    async fn classify_status_transient_table() {
        // Spot-check the classifier directly.
        assert!(classify_status_transient(StatusCode::REQUEST_TIMEOUT));
        assert!(classify_status_transient(StatusCode::TOO_MANY_REQUESTS));
        assert!(classify_status_transient(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(classify_status_transient(StatusCode::BAD_GATEWAY));
        assert!(classify_status_transient(StatusCode::SERVICE_UNAVAILABLE));
        assert!(classify_status_transient(StatusCode::GATEWAY_TIMEOUT));

        assert!(!classify_status_transient(StatusCode::BAD_REQUEST));
        assert!(!classify_status_transient(StatusCode::UNAUTHORIZED));
        assert!(!classify_status_transient(StatusCode::FORBIDDEN));
        assert!(!classify_status_transient(StatusCode::NOT_FOUND));
        assert!(!classify_status_transient(StatusCode::CONFLICT));
        assert!(!classify_status_transient(StatusCode::PAYLOAD_TOO_LARGE));
    }
}
