//! HTTP sink: POSTs records to a configurable URL, either one-per-record
//! (default) or as one newline-delimited batch (`batch_mode = Ndjson`).
//! Classifies HTTP/network failures into the transient/permanent buckets
//! the drain expects.
//!
//! # Protocol (default per-record mode)
//!
//! - One HTTP POST per record. The body is the raw payload bytes; no
//!   serialisation, no encoding, no framing — endpoints decide what to do
//!   with the bytes. (See "Batch framing" below for the NDJSON mode.)
//! - `Content-Type: application/octet-stream`.
//! - `Idempotency-Key: sha256:<hex>` of the payload, unless explicitly
//!   disabled via `send_idempotency_key = false`. The drain guarantees
//!   at-least-once delivery per segment, so retries re-POST records that
//!   may have already been accepted; the idempotency key lets the endpoint
//!   deduplicate without computing the hash itself.
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
//! # Retry-After
//!
//! Transient responses (408/429/5xx) may carry a `Retry-After` header. The
//! delay-seconds form is parsed and propagated to the drain via
//! `SinkError::retry_after()`, which honours it as the next retry delay
//! (capped at 5 minutes). The HTTP-date form is not parsed in v0.
//!
//! # Batch framing (`batch_mode`)
//!
//! - **`PerRecord`** (default): one POST per record. Simple endpoint
//!   contract — any HTTP server that accepts a single-record POST body
//!   works, no batch format to agree on, and per-record dead-lettering.
//!   Tradeoff is N round-trips per batch (mitigated by `concurrency`).
//! - **`Ndjson`**: the whole commit batch goes in ONE POST as
//!   newline-delimited bodies (`application/x-ndjson`), with a single
//!   per-batch `Idempotency-Key`. One round-trip per batch instead of N —
//!   the framing log/trace ingesters (Loki, ES `_bulk`) expect. The
//!   endpoint returns one status, so the result is all-or-nothing: a 2xx
//!   commits the batch, a permanent 4xx dead-letters the whole batch, and
//!   a transient error retries the whole segment. Records must contain no
//!   embedded newlines.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};
use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

/// How the HTTP sink frames a commit batch into HTTP requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpBatchMode {
    /// One POST per record (default): per-record `Idempotency-Key`, per-record
    /// dead-lettering, up to `concurrency` POSTs in flight.
    #[default]
    PerRecord,
    /// One POST per commit batch: newline-delimited (NDJSON) record bodies, a
    /// single per-batch `Idempotency-Key`, and whole-batch commit/retry/
    /// dead-letter (the endpoint returns one status, so there is no per-record
    /// granularity). Records must contain no embedded newlines.
    Ndjson,
}

/// Configuration for `HttpSink`.
///
/// `bearer_token` is deliberately omitted from the `Debug` impl so it never
/// reaches a log line via `?config` interpolation.
#[derive(Clone)]
pub struct HttpSinkConfig {
    pub url: String,
    pub timeout: Duration,
    pub max_batch_size: usize,
    /// Request framing: per-record POSTs (default) or one NDJSON POST per batch.
    pub batch_mode: HttpBatchMode,
    /// Optional bearer token. Read from env at startup (`WEIR_SINK_BEARER_TOKEN`);
    /// never sourced from the config file. Wrapped in `Arc` so the value can be
    /// shared without copying.
    pub bearer_token: Option<Arc<str>>,
    /// Send `Idempotency-Key: sha256:<hex>` with each request so the endpoint
    /// can deduplicate retries that follow the drain's at-least-once contract.
    /// Default: true. Set false only if your endpoint can't tolerate the
    /// extra header (e.g. strict CORS, header allow-lists).
    pub send_idempotency_key: bool,
    /// Maximum number of per-record POSTs kept in flight per `commit()` batch.
    /// The drain runs on one thread, but the POSTs are async, so up to this many
    /// overlap their network round-trips — collapsing a segment's serial RTT
    /// cost without changing the per-record protocol or failure granularity.
    pub concurrency: usize,
}

impl std::fmt::Debug for HttpSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSinkConfig")
            // The URL may carry userinfo (user:password@host); redact the password
            // so it never lands in a Debug-formatted log line (S25).
            .field("url", &crate::sink::redact_url_password(&self.url))
            .field("timeout", &self.timeout)
            .field("max_batch_size", &self.max_batch_size)
            .field("batch_mode", &self.batch_mode)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("send_idempotency_key", &self.send_idempotency_key)
            .field("concurrency", &self.concurrency)
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
            // Never follow redirects. reqwest's default follows up to 10, and on a
            // 301/302/303 it re-issues the POST as a BODILESS GET — if that lands a
            // 2xx the sink would report the record committed though the payload was
            // never delivered, and the drain would confirm+delete the segment: a
            // false ack (G01). With redirects disabled a 3xx surfaces to
            // post_record and is dead-lettered, never committed.
            .redirect(reqwest::redirect::Policy::none())
            // Keep idle connections warm between batches. The drain calls
            // commit in a tight loop while a segment is being processed. Size
            // the idle pool to the in-flight concurrency so an HTTP/1.1 endpoint
            // has a connection ready for each concurrent POST (HTTP/2 multiplexes
            // and needs fewer, but sizing up doesn't hurt).
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .pool_max_idle_per_host(config.concurrency.max(1))
            .user_agent(concat!("weir/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(HttpSinkBuildError::ClientBuild)?;

        Ok(Self { config, client })
    }

    /// A POST request builder for `sink_url` carrying the bearer token (if any).
    /// Callers add the Content-Type, Idempotency-Key, and body.
    fn base_request(&self) -> reqwest::RequestBuilder {
        let mut req = self.client.post(&self.config.url);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token.as_ref());
        }
        req
    }

    /// Per-record POST (the default mode): one record per request, with the
    /// record's own Idempotency-Key.
    async fn post_record(&self, payload: Payload) -> Result<(), HttpSinkError> {
        let mut req = self
            .base_request()
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream");
        if self.config.send_idempotency_key {
            req = req.header("Idempotency-Key", payload_idempotency_key(&payload));
        }
        // No-copy: reqwest::Body implements From<bytes::Bytes> — the payload's
        // inner Bytes is handed to reqwest without any allocation or memcopy.
        self.send_and_classify(req.body(payload.into_bytes())).await
    }

    /// Single POST of a whole batch as newline-delimited (NDJSON) bodies, with
    /// one per-batch Idempotency-Key (sha256 of the joined body). The endpoint
    /// returns one status for the whole batch, so the result is all-or-nothing.
    async fn post_batch(&self, body: Vec<u8>) -> Result<(), HttpSinkError> {
        let mut req = self
            .base_request()
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson");
        if self.config.send_idempotency_key {
            req = req.header("Idempotency-Key", payload_idempotency_key(&body));
        }
        self.send_and_classify(req.body(body)).await
    }

    /// Sends a prepared request and classifies the response into the
    /// committed / transient / permanent buckets the drain expects. Shared by
    /// the per-record and NDJSON-batch paths.
    async fn send_and_classify(&self, req: reqwest::RequestBuilder) -> Result<(), HttpSinkError> {
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                // Network-layer errors are all transient. Connect refused,
                // DNS failure, timeout, broken pipe — drain retries.
                //
                // Defence in depth (S31): this string is stored in
                // `Transport` and re-logged at warn! by the drain on *every*
                // retry, so a `scheme://user:pass@host` sink URL must never
                // bleed its password here. reqwest 0.12 already strips URL
                // userinfo from its error Display (it lifts it into a Basic-auth
                // header), but we own the redaction rather than delegating the
                // guarantee to a transitive dep's formatting — `redact_url_password`
                // is a no-op when no userinfo is present, so the useful host
                // diagnostic survives.
                let rendered = crate::sink::redact_url_password(&e.to_string());
                debug!(error = %rendered, "sink POST failed at transport layer");
                return Err(HttpSinkError::Transport(rendered));
            }
        };

        let status = resp.status();
        if status.is_success() {
            // Drain the body (capped) to release the connection back to the pool.
            // We don't care about the response body — the status code is the only
            // signal — so a hostile downstream can't make us buffer it (S28).
            let _ = crate::sink::read_body_capped(resp, crate::sink::RESPONSE_BODY_CAP).await;
            return Ok(());
        }

        // A 3xx surfaces here only because redirect-following is disabled (G01).
        // Treat it as a permanent misconfiguration — never committed: the
        // configured sink URL should point straight at the ingest endpoint, not a
        // redirector. Reported with the Location so the operator can repoint it.
        if status.is_redirection() {
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("<no Location header>")
                .to_string();
            return Err(HttpSinkError::PermanentStatus {
                status,
                body_excerpt: format!(
                    "unexpected redirect to '{location}'; redirects are not followed — \
                     point sink_url directly at the ingest endpoint"
                ),
            });
        }

        if classify_status_transient(status) {
            // Honour Retry-After if present. Delay-seconds form only — the
            // HTTP-date form is rare in practice and adding a date parser
            // would inflate the dep tree.
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after_seconds);
            debug!(
                status = %status,
                retry_after_secs = retry_after.map(|d| d.as_secs()),
                "sink POST returned transient HTTP status"
            );
            return Err(HttpSinkError::TransientStatus {
                status,
                retry_after,
            });
        }

        // Permanent. Capture a short body excerpt for the dead-letter reason
        // string so operators can debug what the endpoint complained about.
        // Cap at 256 bytes to avoid logging unbounded server output.
        // Read at most the cap (S28), then take a 256-byte excerpt. Strip control
        // characters: the body is downstream-controlled and is interpolated into
        // log lines and the dead-letter reason, so a hostile endpoint could
        // otherwise inject forged log records or terminal escape sequences (S29).
        let bytes = crate::sink::read_body_capped(resp, crate::sink::RESPONSE_BODY_CAP).await;
        let cut = bytes.len().min(256);
        let body_excerpt =
            crate::sink::sanitize_log_excerpt(&String::from_utf8_lossy(&bytes[..cut]));
        Err(HttpSinkError::PermanentStatus {
            status,
            body_excerpt,
        })
    }
}

/// Computes the `Idempotency-Key` header value for a payload.
///
/// Format: `sha256:<lowercase-hex>` — the prefix lets the endpoint
/// distinguish our scheme from other key sources and switch on the algorithm
/// if we add more (e.g. blake3) in the future. Pure function of the payload:
/// the same bytes always produce the same key, which is exactly the property
/// the drain's at-least-once retry semantics need.
fn payload_idempotency_key(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in digest {
        // Manual hex avoids pulling in the `hex` crate.
        out.push(HEX_CHARS[(byte >> 4) as usize] as char);
        out.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    out
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Parses the delay-seconds form of an HTTP `Retry-After` header value.
/// Returns `None` for the HTTP-date form (not supported in v0), values that
/// fail to parse, and unreasonable values (zero or > 3600 seconds — the
/// drain caps at this anyway, and clamping here keeps the log line honest).
fn parse_retry_after_seconds(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    let secs: u64 = trimmed.parse().ok()?;
    // 0 is technically valid but indistinguishable from "no hint"; clamp.
    // 3600s (1 hour) upper bound — anything longer is the endpoint asking us
    // to give up. We honour the cap; the drain has its own MAX_RETRIES.
    if !(1..=3600).contains(&secs) {
        return None;
    }
    Some(Duration::from_secs(secs))
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

#[derive(Debug, thiserror::Error)]
pub enum HttpSinkError {
    /// reqwest transport error (connect, timeout, body send). Always transient.
    #[error("http sink transport error: {0}")]
    Transport(String),
    /// HTTP status code in the transient bucket (408/429/5xx). `retry_after`
    /// carries the parsed `Retry-After` header value if the server sent one
    /// (delay-seconds form only; HTTP-date form is not parsed in v0).
    #[error("http sink transient status: {status}{}", fmt_retry_after(retry_after))]
    TransientStatus {
        status: StatusCode,
        retry_after: Option<Duration>,
    },
    /// HTTP status code in the permanent bucket (4xx except 408/429).
    #[error("http sink permanent status: {status}; body: {body_excerpt}")]
    PermanentStatus {
        status: StatusCode,
        body_excerpt: String,
    },
}

/// Helper used by the `TransientStatus` Display format string: appends
/// ` (Retry-After: Ns)` when the header was present, empty otherwise.
fn fmt_retry_after(retry_after: &Option<Duration>) -> String {
    match retry_after {
        Some(d) => format!(" (Retry-After: {}s)", d.as_secs()),
        None => String::new(),
    }
}

impl SinkError for HttpSinkError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            HttpSinkError::Transport(_) | HttpSinkError::TransientStatus { .. }
        )
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            HttpSinkError::TransientStatus { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Errors that can occur during `HttpSink::new()`.
#[derive(Debug, thiserror::Error)]
pub enum HttpSinkBuildError {
    #[error("http sink url is empty")]
    EmptyUrl,
    #[error("http sink url invalid: {0}")]
    InvalidUrl(String),
    #[error("http sink client build failed: {0}")]
    ClientBuild(#[from] reqwest::Error),
}

impl Sink for HttpSink {
    type Record = Payload;
    type Error = HttpSinkError;

    async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, HttpSinkError> {
        match self.config.batch_mode {
            HttpBatchMode::PerRecord => self.commit_per_record(batch).await,
            HttpBatchMode::Ndjson => self.commit_ndjson(batch).await,
        }
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        self.probe_health().await
    }
}

impl HttpSink {
    /// Per-record commit: one POST per record, up to `concurrency` in flight.
    async fn commit_per_record(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, HttpSinkError> {
        use futures_util::stream::StreamExt;

        let concurrency = self.config.concurrency.max(1);

        // One POST per record (per-record Idempotency-Key + per-record
        // dead-lettering), but up to `concurrency` in flight. The drain thread is
        // single-threaded; these futures are async, so their network round-trips
        // overlap — a 1000-record segment no longer pays 1000× the serial RTT.
        // `buffered` (not `buffer_unordered`) keeps the results in batch order, so
        // `committed` stays in submission order.
        let stream = futures_util::stream::iter(batch)
            .map(|record| async move {
                // Clone the Bytes handle (O(1) ref-bump) so we keep the record
                // for accounting while posting it.
                let outcome = self.post_record(record.clone()).await;
                (record, outcome)
            })
            .buffered(concurrency);
        let mut stream = std::pin::pin!(stream);

        let mut committed = Vec::new();
        let mut dead_lettered: Vec<(Payload, String)> = Vec::new();
        while let Some((record, outcome)) = stream.next().await {
            match outcome {
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
                    // Transient: abort the batch so the drain retries the whole
                    // segment. Dropping the stream cancels any still-in-flight
                    // POSTs. Records already committed in this call may be
                    // re-sent — the endpoint's idempotency contract covers this
                    // (documented in sink/mod.rs).
                    return Err(e);
                }
            }
        }

        Ok(CommitResult::new(committed, dead_lettered))
    }

    /// NDJSON-batch commit: the whole batch goes in ONE POST as newline-delimited
    /// bodies, with a single per-batch Idempotency-Key. The endpoint returns one
    /// status, so the result is all-or-nothing: 2xx commits the whole batch, a
    /// permanent 4xx dead-letters the whole batch, and a transient error retries
    /// the whole segment. Records must contain no embedded newlines.
    async fn commit_ndjson(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, HttpSinkError> {
        if batch.is_empty() {
            return Ok(CommitResult::new(vec![], vec![]));
        }
        // Join records with '\n' (each record terminated by a newline).
        let total: usize = batch.iter().map(|r| r.len() + 1).sum();
        let mut body = Vec::with_capacity(total);
        for r in &batch {
            body.extend_from_slice(r.as_ref());
            body.push(b'\n');
        }

        match self.post_batch(body).await {
            Ok(()) => Ok(CommitResult::new(batch, vec![])),
            Err(HttpSinkError::PermanentStatus {
                status,
                body_excerpt,
            }) => {
                warn!(
                    status = %status,
                    body = %body_excerpt,
                    count = batch.len(),
                    "NDJSON batch permanently rejected by HTTP sink; dead-lettering the whole batch"
                );
                let reason = format!("http {status} (ndjson batch): {body_excerpt}");
                let dead = batch.into_iter().map(|r| (r, reason.clone())).collect();
                Ok(CommitResult::new(vec![], dead))
            }
            // Transient (or transport): retry the whole segment.
            Err(e) => Err(e),
        }
    }

    async fn probe_health(&self) -> SinkHealth {
        // Coarse health probe: HEAD the URL. This probe is UNAUTHENTICATED —
        // the bearer token is attached per-POST in commit(), not as a client
        // default — so a 401/403 here is an expected auth challenge from a
        // reachable endpoint, not a misconfiguration; the real (authenticated)
        // commit path will succeed, so treat it as healthy. 2xx/3xx are healthy.
        //
        // 405 Method Not Allowed / 501 Not Implemented mean the endpoint is
        // REACHABLE but simply doesn't implement HEAD — extremely common for
        // POST-only ingest APIs (observability collectors, webhooks). Treating
        // those as "down" was a recurring false-alarm: the sink delivers every
        // record via POST while the gauge reads down and the log spams ERROR.
        // The real signal is commit success, so treat HEAD-unsupported as healthy.
        // Other 4xx suggest an endpoint misconfig (degraded); other 5xx or a
        // transport failure suggests the endpoint is down.
        match self.client.head(&self.config.url).send().await {
            Ok(resp) if resp.status().is_success() || resp.status().is_redirection() => {
                SinkHealth::Healthy
            }
            Ok(resp)
                if resp.status() == reqwest::StatusCode::UNAUTHORIZED
                    || resp.status() == reqwest::StatusCode::FORBIDDEN
                    || resp.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED
                    || resp.status() == reqwest::StatusCode::NOT_IMPLEMENTED =>
            {
                SinkHealth::Healthy
            }
            Ok(resp) if resp.status().is_client_error() => {
                SinkHealth::Degraded(format!("HEAD returned {}", resp.status()))
            }
            Ok(resp) => SinkHealth::Down(format!("HEAD returned {}", resp.status())),
            // Redact any URL password the transport error might carry (S31); see
            // the matching note on the commit path.
            Err(e) => SinkHealth::Down(format!(
                "HEAD failed: {}",
                crate::sink::redact_url_password(&e.to_string())
            )),
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

    /// Test helper: build a `Payload` from a static byte string literal.
    fn p(s: &'static [u8]) -> Payload {
        Payload::from_static(s)
    }

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

    /// Reads a full HTTP request off `socket` and returns its body bytes (the
    /// raw payload the sink POSTed). Returns an empty Vec if the connection
    /// closes before the headers complete (e.g. the client cancelled the POST).
    /// Lets content-routing mock servers decide the response per record so a
    /// test can pin which payload got which outcome.
    async fn read_request_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        let header_end = loop {
            match socket.read(&mut tmp).await {
                Ok(0) | Err(_) => return Vec::new(),
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = find_double_crlf(&buf) {
                        break pos;
                    }
                }
            }
        };
        let header_str = String::from_utf8_lossy(&buf[..header_end]);
        let content_length = parse_content_length(&header_str).unwrap_or(0);
        let mut body = buf[header_end + 4..].to_vec();
        while body.len() < content_length {
            match socket.read(&mut tmp).await {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&tmp[..n]),
            }
        }
        body.truncate(content_length);
        body
    }

    fn cfg(url: &str) -> HttpSinkConfig {
        HttpSinkConfig {
            url: url.to_string(),
            timeout: Duration::from_secs(5),
            max_batch_size: 100,
            batch_mode: HttpBatchMode::PerRecord,
            bearer_token: None,
            send_idempotency_key: true,
            concurrency: 8,
        }
    }

    #[test]
    fn debug_redacts_bearer_token_and_url_password() {
        // Lock the credential-redaction contract: neither the bearer token nor a
        // URL password may appear in Debug output (S30 / S25).
        let mut c = cfg("https://user:sup3rS3cret@example.com/ingest");
        c.bearer_token = Some(Arc::from("t0p-s3cret-token"));
        let dbg = format!("{c:?}");
        assert!(
            !dbg.contains("t0p-s3cret-token"),
            "bearer token leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("sup3rS3cret"),
            "URL password leaked in Debug: {dbg}"
        );
        assert!(dbg.contains("<redacted>"));
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
            .commit(vec![p(b"alpha"), p(b"beta"), p(b"gamma")])
            .await
            .unwrap();
        assert_eq!(result.committed.len(), 3);
        assert!(result.dead_lettered.is_empty());
        // Give the mock server a moment to finish counting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn http_commit_preserves_record_order_under_concurrency() {
        // `buffered` (not `buffer_unordered`) keeps `committed` in submission
        // order even though the POSTs run concurrently.
        let (url, _counter) =
            spawn_mock_server(vec!["HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]).await;
        let mut c = cfg(&url);
        c.concurrency = 4;
        let sink = HttpSink::new(c).unwrap();
        let batch = vec![p(b"r0"), p(b"r1"), p(b"r2"), p(b"r3"), p(b"r4")];
        let result = sink.commit(batch.clone()).await.unwrap();
        assert_eq!(result.committed, batch);
        assert!(result.dead_lettered.is_empty());
    }

    #[tokio::test]
    async fn http_posts_run_concurrently() {
        // A server that holds every connection open until N of them are
        // simultaneously connected, then releases all at once. If the sink
        // POSTed serially, the 2nd POST would never start until the 1st
        // returned — so the barrier (and thus the commit) would deadlock.
        // Completing within the timeout proves the POSTs overlap.
        use tokio::sync::Barrier;
        const N: usize = 4;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        let barrier = Arc::new(Barrier::new(N));
        tokio::spawn(async move {
            for _ in 0..N {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    // Read the request header block, then wait for all N peers.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    loop {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if find_double_crlf(&buf).is_some() {
                                    break;
                                }
                            }
                        }
                    }
                    barrier.wait().await;
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                });
            }
        });

        let mut c = cfg(&url);
        c.concurrency = N;
        let sink = HttpSink::new(c).unwrap();
        let batch: Vec<Payload> = (0..N)
            .map(|i| Payload::from(format!("rec{i}").into_bytes()))
            .collect();
        let result = tokio::time::timeout(Duration::from_secs(5), sink.commit(batch))
            .await
            .expect("commit deadlocked — POSTs did not run concurrently")
            .unwrap();
        assert_eq!(result.committed.len(), N);
    }

    #[tokio::test]
    async fn http_400_dead_letters() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nbad payload!",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![p(b"alpha")]).await.unwrap();
        assert!(result.committed.is_empty());
        assert_eq!(result.dead_lettered.len(), 1);
        let (_, reason) = &result.dead_lettered[0];
        assert!(reason.contains("400"), "reason: {reason}");
        assert!(reason.contains("bad payload"), "reason: {reason}");
    }

    /// Coverage gap (T12): a verbose/hostile 4xx body must NOT land unbounded in
    /// the dead-letter reason — the excerpt is cut at 256 bytes.
    #[tokio::test]
    async fn http_permanent_status_truncates_large_body_excerpt() {
        let body = "A".repeat(4096);
        let resp = format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        // spawn_mock_server wants &'static str; leak the owned response (test-only).
        let resp: &'static str = Box::leak(resp.into_boxed_str());
        let (url, _counter) = spawn_mock_server(vec![resp]).await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![p(b"x")]).await.unwrap();
        assert_eq!(result.dead_lettered.len(), 1);
        let (_, reason) = &result.dead_lettered[0];
        assert!(reason.contains("400"), "reason: {reason}");
        assert!(
            !reason.contains(&"A".repeat(300)),
            "the 4096-byte body must be truncated to a bounded excerpt, not carried whole"
        );
    }

    #[tokio::test]
    async fn http_500_returns_transient_error() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient(), "500 must be transient: {err}");
        assert!(
            matches!(err, HttpSinkError::TransientStatus { status, .. } if status.as_u16() == 500)
        );
    }

    #[tokio::test]
    async fn http_429_returns_transient_error() {
        // 429 Too Many Requests: explicit rate-limit signal.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient(), "429 must be transient: {err}");
    }

    #[tokio::test]
    async fn http_408_returns_transient_error() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 408 Request Timeout\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient(), "408 must be transient: {err}");
    }

    #[tokio::test]
    async fn http_redirect_is_dead_lettered_never_committed() {
        // G01: a 3xx must NOT be followed (which would re-issue a bodiless GET
        // and let a 2xx there masquerade as a committed record — a false ack).
        // With redirects disabled the 302 surfaces and is dead-lettered.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/elsewhere\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![p(b"alpha")]).await.unwrap();
        assert!(
            result.committed.is_empty(),
            "a redirected POST must never be reported committed"
        );
        assert_eq!(result.dead_lettered.len(), 1);
        let (_, reason) = &result.dead_lettered[0];
        assert!(reason.contains("302"), "reason: {reason}");
        assert!(reason.contains("redirect"), "reason: {reason}");
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
        let result = sink.commit(vec![p(b"alpha")]).await.unwrap();
        assert_eq!(result.dead_lettered.len(), 1);
    }

    #[tokio::test]
    async fn connect_refused_is_transient() {
        // Pick a port that's almost certainly closed.
        let sink = HttpSink::new(cfg("http://127.0.0.1:1/")).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(
            err.is_transient(),
            "connect refused must be transient: {err}"
        );
        assert!(matches!(err, HttpSinkError::Transport(_)));
    }

    #[tokio::test]
    async fn transport_error_does_not_leak_url_password() {
        // S31: a reqwest transport error must NOT carry the sink URL's password
        // into the error string. That string is stored in HttpSinkError::Transport
        // and re-logged at warn! by the drain on every retry (drain/mod.rs), so a
        // `https://user:pass@host` URL + a brief outage would otherwise print the
        // password to stderr/journald repeatedly. reqwest's Error Display appends
        // ` for url (<url>)` with the FULL url (incl. userinfo) unless stripped.
        let sink =
            HttpSink::new(cfg("http://weiruser:s3cr3t-passw0rd@127.0.0.1:1/ingest")).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(
            err.is_transient(),
            "connect refused must be transient: {err}"
        );
        let rendered = err.to_string();
        assert!(
            !rendered.contains("s3cr3t-passw0rd"),
            "URL password leaked into the transport error string: {rendered}"
        );
    }

    #[tokio::test]
    async fn health_down_does_not_leak_url_password() {
        // S31: the health HEAD probe's failure message is surfaced to operators;
        // it must not embed the URL password either.
        let sink =
            HttpSink::new(cfg("http://weiruser:s3cr3t-passw0rd@127.0.0.1:1/ingest")).unwrap();
        match sink.health().await {
            SinkHealth::Down(msg) => assert!(
                !msg.contains("s3cr3t-passw0rd"),
                "URL password leaked into health Down message: {msg}"
            ),
            other => panic!("expected Down for a closed port, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mixed_batch_committed_and_dead_lettered() {
        // This server cycles responses by request ARRIVAL order, which under
        // concurrency 8 is non-deterministic — so this test pins only the COUNTS
        // (2 committed, 1 dead-lettered). Record↔outcome pairing is pinned by
        // http_pairs_each_record_with_its_own_outcome_under_concurrency (F39),
        // which routes responses on request content instead.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let result = sink.commit(vec![p(b"a"), p(b"b"), p(b"c")]).await.unwrap();
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
        let err = sink.commit(vec![p(b"a"), p(b"b")]).await.unwrap_err();
        assert!(err.is_transient());
    }

    /// F39: under concurrency the completion order of POSTs is non-deterministic,
    /// so a count-only assertion can't catch a record↔outcome mis-pairing. This
    /// mock routes the response on request CONTENT (400 for b"bad", 200 else) and
    /// asserts the RIGHT payload lands in committed vs dead_lettered.
    #[tokio::test]
    async fn http_pairs_each_record_with_its_own_outcome_under_concurrency() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let body = read_request_body(&mut socket).await;
                    let response = if body == b"bad" {
                        "HTTP/1.1 400 Bad Request\r\nContent-Length: 3\r\n\r\nno!"
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        let mut c = cfg(&url);
        c.concurrency = 8;
        let sink = HttpSink::new(c).unwrap();
        let result = sink
            .commit(vec![p(b"good-1"), p(b"bad"), p(b"good-2")])
            .await
            .unwrap();

        // committed holds exactly the two good payloads, in submission order
        // (buffered preserves order even though POSTs complete out of order).
        assert_eq!(result.committed, vec![p(b"good-1"), p(b"good-2")]);
        // dead_lettered holds exactly the bad payload, with its 400 reason.
        assert_eq!(result.dead_lettered.len(), 1);
        assert_eq!(result.dead_lettered[0].0, p(b"bad"));
        assert!(
            result.dead_lettered[0].1.contains("400"),
            "reason: {}",
            result.dead_lettered[0].1
        );
    }

    /// F38: a transient error early in the batch must CANCEL the still-in-flight
    /// POSTs (the buffered stream is dropped on the early return), not wait them
    /// out. The mock returns 500 immediately for b"boom" and stalls 10 s for any
    /// other body; with boom first and concurrency >= batch size, commit must
    /// return Err well under 10 s, and the stalled POSTs must never complete.
    #[tokio::test]
    async fn http_transient_cancels_in_flight_posts() {
        const SLOW: Duration = Duration::from_secs(10);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_task = Arc::clone(&completed);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let completed = Arc::clone(&completed_task);
                tokio::spawn(async move {
                    let body = read_request_body(&mut socket).await;
                    if body == b"boom" {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
                            )
                            .await;
                    } else {
                        // Stall: respond only after SLOW. If the client cancels the
                        // in-flight POST, this socket closes and we never reach the
                        // increment — exactly what the test asserts.
                        tokio::time::sleep(SLOW).await;
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                    }
                    let _ = socket.shutdown().await;
                    completed.fetch_add(1, Ordering::SeqCst);
                });
            }
        });

        let mut c = cfg(&url);
        c.concurrency = 8;
        let sink = HttpSink::new(c).unwrap();
        // boom FIRST: `buffered` yields results in submission order, so its 500 is
        // observed before the stalled ones, triggering the early-return + drop.
        let batch = vec![p(b"boom"), p(b"slow-1"), p(b"slow-2"), p(b"slow-3")];

        let start = tokio::time::Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(3), sink.commit(batch))
            .await
            .expect("commit must return promptly, not wait out the stalled in-flight POSTs")
            .unwrap_err();
        assert!(err.is_transient(), "expected transient error, got: {err}");
        assert!(
            start.elapsed() < SLOW,
            "commit waited on the stalled POSTs instead of cancelling them"
        );

        // Grace period far shorter than SLOW: only boom should have completed;
        // the stalled POSTs must have been cancelled, not awaited.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "stalled in-flight POSTs were not cancelled on the transient error"
        );
    }

    // ── NDJSON batch-mode tests ───────────────────────────────────────────

    fn ndjson_cfg(url: &str) -> HttpSinkConfig {
        let mut c = cfg(url);
        c.batch_mode = HttpBatchMode::Ndjson;
        c
    }

    #[tokio::test]
    async fn ndjson_commits_whole_batch_in_one_post() {
        // The whole batch goes in ONE POST; a 2xx commits every record. The
        // request counter proves it was a single round-trip, not N.
        let (url, counter) =
            spawn_mock_server(vec!["HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]).await;
        let sink = HttpSink::new(ndjson_cfg(&url)).unwrap();
        let batch = vec![p(b"alpha"), p(b"beta"), p(b"gamma")];
        let result = sink.commit(batch.clone()).await.unwrap();
        assert_eq!(result.committed, batch);
        assert!(result.dead_lettered.is_empty());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "NDJSON mode must POST the whole batch in a single request"
        );
    }

    #[tokio::test]
    async fn ndjson_dead_letters_whole_batch_on_4xx() {
        // The endpoint returns one status for the batch; a permanent 4xx
        // dead-letters every record (no per-record granularity in this mode).
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 8\r\n\r\nbad ndj!",
        ])
        .await;
        let sink = HttpSink::new(ndjson_cfg(&url)).unwrap();
        let batch = vec![p(b"alpha"), p(b"beta"), p(b"gamma")];
        let result = sink.commit(batch).await.unwrap();
        assert!(result.committed.is_empty());
        assert_eq!(result.dead_lettered.len(), 3);
        for (_, reason) in &result.dead_lettered {
            assert!(reason.contains("400"), "reason: {reason}");
            assert!(reason.contains("ndjson batch"), "reason: {reason}");
        }
    }

    #[tokio::test]
    async fn ndjson_5xx_is_transient_for_whole_segment() {
        // A transient status retries the whole segment — surfaced as Err.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(ndjson_cfg(&url)).unwrap();
        let err = sink
            .commit(vec![p(b"alpha"), p(b"beta")])
            .await
            .unwrap_err();
        assert!(err.is_transient(), "503 must be transient: {err}");
    }

    #[tokio::test]
    async fn ndjson_sends_newline_framed_body_and_one_idempotency_key() {
        // Verify the wire format: records joined with '\n' (each terminated by
        // one), Content-Type application/x-ndjson, and a SINGLE per-batch
        // Idempotency-Key (sha256 of the joined body).
        let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let bodies = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        let captured_task = Arc::clone(&captured);
        let bodies_task = Arc::clone(&bodies);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let captured = Arc::clone(&captured_task);
                let bodies = Arc::clone(&bodies_task);
                tokio::spawn(async move {
                    // Read the header block first so we can capture it, then the body.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    let header_end = loop {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if let Some(pos) = find_double_crlf(&buf) {
                                    break pos;
                                }
                            }
                        }
                    };
                    let header_str = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length = parse_content_length(&header_str).unwrap_or(0);
                    let mut body = buf[header_end + 4..].to_vec();
                    while body.len() < content_length {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => body.extend_from_slice(&tmp[..n]),
                        }
                    }
                    body.truncate(content_length);
                    captured.lock().unwrap().push(header_str);
                    bodies.lock().unwrap().push(body);
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        let sink = HttpSink::new(ndjson_cfg(&url)).unwrap();
        sink.commit(vec![p(b"alpha"), p(b"beta"), p(b"gamma")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let headers = captured.lock().unwrap();
        let bodies = bodies.lock().unwrap();
        assert_eq!(
            headers.len(),
            1,
            "NDJSON mode must send exactly one request"
        );
        assert_eq!(bodies.len(), 1);
        // Body: each record terminated by '\n'.
        assert_eq!(bodies[0], b"alpha\nbeta\ngamma\n");
        // Content-Type advertises NDJSON.
        assert!(
            headers[0]
                .lines()
                .any(|l| l.to_ascii_lowercase().starts_with("content-type:")
                    && l.contains("application/x-ndjson")),
            "missing/incorrect Content-Type:\n{}",
            headers[0]
        );
        // Exactly one Idempotency-Key, computed over the joined body.
        let expected_key = payload_idempotency_key(b"alpha\nbeta\ngamma\n");
        let key_lines: Vec<&str> = headers[0]
            .lines()
            .filter(|l| l.to_ascii_lowercase().starts_with("idempotency-key:"))
            .collect();
        assert_eq!(key_lines.len(), 1, "expected exactly one Idempotency-Key");
        assert!(
            key_lines[0].contains(&expected_key),
            "Idempotency-Key not over the joined body: {} (want {expected_key})",
            key_lines[0]
        );
    }

    #[tokio::test]
    async fn ndjson_empty_batch_makes_no_request() {
        let (url, counter) =
            spawn_mock_server(vec!["HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]).await;
        let sink = HttpSink::new(ndjson_cfg(&url)).unwrap();
        let result = sink.commit(vec![]).await.unwrap();
        assert!(result.committed.is_empty());
        assert!(result.dead_lettered.is_empty());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "an empty batch must not hit the network"
        );
    }

    // ── Idempotency key tests ─────────────────────────────────────────────

    #[test]
    fn idempotency_key_is_deterministic() {
        // The whole point: same payload always produces the same key. Without
        // this property the endpoint can't dedupe retries.
        let key1 = payload_idempotency_key(b"hello, weir");
        let key2 = payload_idempotency_key(b"hello, weir");
        assert_eq!(key1, key2);
    }

    #[test]
    fn idempotency_key_differs_for_different_payloads() {
        let key1 = payload_idempotency_key(b"hello, weir");
        let key2 = payload_idempotency_key(b"hello, world");
        assert_ne!(key1, key2);
    }

    #[test]
    fn idempotency_key_has_expected_shape() {
        let key = payload_idempotency_key(b"");
        // Empty input's SHA-256 is a well-known constant; this asserts the
        // hex encoding is correct, lowercase, and the prefix is present.
        assert_eq!(
            key,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Spawns a mock server that captures the first request's header block
    /// into `captured`, then returns 200 OK. Used to assert on what headers
    /// the sink actually sent.
    async fn spawn_header_capture_server(captured: Arc<std::sync::Mutex<Vec<String>>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let captured = Arc::clone(&captured);
                tokio::spawn(async move {
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
                    let headers = String::from_utf8_lossy(&buf[..header_end.unwrap()]).to_string();
                    captured.lock().unwrap().push(headers);
                    // Drain the body so the client doesn't see ECONNRESET.
                    let content_length = {
                        let s = captured.lock().unwrap();
                        let last = s.last().unwrap();
                        parse_content_length(last).unwrap_or(0)
                    };
                    let already_have = buf.len().saturating_sub(header_end.unwrap() + 4);
                    let mut remaining = content_length.saturating_sub(already_have);
                    while remaining > 0 {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => remaining = remaining.saturating_sub(n),
                        }
                    }
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        url
    }

    #[tokio::test]
    async fn idempotency_key_header_sent_by_default() {
        let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let url = spawn_header_capture_server(Arc::clone(&captured)).await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        sink.commit(vec![p(b"hello, weir")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let headers = captured.lock().unwrap();
        assert_eq!(headers.len(), 1);
        let expected_key = payload_idempotency_key(b"hello, weir");
        assert!(
            headers[0].lines().any(|line| line
                .to_ascii_lowercase()
                .starts_with("idempotency-key:")
                && line.contains(&expected_key)),
            "no Idempotency-Key header with expected value found in:\n{}",
            headers[0]
        );
    }

    #[tokio::test]
    async fn idempotency_key_header_omitted_when_disabled() {
        let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let url = spawn_header_capture_server(Arc::clone(&captured)).await;
        let mut c = cfg(&url);
        c.send_idempotency_key = false;
        let sink = HttpSink::new(c).unwrap();
        sink.commit(vec![p(b"hello, weir")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let headers = captured.lock().unwrap();
        assert_eq!(headers.len(), 1);
        assert!(
            !headers[0]
                .lines()
                .any(|line| line.to_ascii_lowercase().starts_with("idempotency-key:")),
            "Idempotency-Key header should be absent when disabled, found:\n{}",
            headers[0]
        );
    }

    // ── Health probe tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn health_treats_auth_challenge_as_healthy() {
        // The HEAD probe is unauthenticated; a 401/403 from a reachable
        // endpoint is an expected auth challenge, not a misconfig. The
        // authenticated commit path still works, so report healthy.
        for resp in [
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n",
        ] {
            let (url, _counter) = spawn_mock_server(vec![resp]).await;
            let sink = HttpSink::new(cfg(&url)).unwrap();
            assert!(
                matches!(sink.health().await, SinkHealth::Healthy),
                "{resp} should be healthy"
            );
        }
    }

    #[tokio::test]
    async fn health_treats_head_unsupported_as_healthy() {
        // 405/501 mean the endpoint is reachable but doesn't implement HEAD —
        // common for POST-only ingest APIs. The sink delivers fine via POST, so
        // these must NOT flap the sink to "down" (recurring usage-sweep finding).
        for resp in [
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n",
        ] {
            let (url, _counter) = spawn_mock_server(vec![resp]).await;
            let sink = HttpSink::new(cfg(&url)).unwrap();
            assert!(
                matches!(sink.health().await, SinkHealth::Healthy),
                "{resp} should be healthy (endpoint reachable, HEAD unsupported)"
            );
        }
    }

    #[tokio::test]
    async fn health_treats_other_4xx_as_degraded_and_5xx_as_down() {
        let (url, _c) =
            spawn_mock_server(vec!["HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"]).await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        assert!(matches!(sink.health().await, SinkHealth::Degraded(_)));

        let (url, _c) = spawn_mock_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        assert!(matches!(sink.health().await, SinkHealth::Down(_)));
    }

    // ── Retry-After header tests ──────────────────────────────────────────

    #[test]
    fn parse_retry_after_accepts_integer_seconds() {
        assert_eq!(parse_retry_after_seconds("5"), Some(Duration::from_secs(5)));
        assert_eq!(
            parse_retry_after_seconds(" 30 "),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn parse_retry_after_rejects_unparseable_values() {
        // HTTP-date form is not supported; should silently return None
        // rather than fail the request.
        assert_eq!(
            parse_retry_after_seconds("Fri, 31 Dec 1999 23:59:59 GMT"),
            None
        );
        assert_eq!(parse_retry_after_seconds("abc"), None);
        assert_eq!(parse_retry_after_seconds(""), None);
        assert_eq!(parse_retry_after_seconds("-1"), None);
    }

    #[test]
    fn parse_retry_after_clamps_unreasonable_values() {
        // 0 is technically valid but indistinguishable from "no hint".
        assert_eq!(parse_retry_after_seconds("0"), None);
        // Anything above 1 hour is the endpoint asking us to give up.
        assert_eq!(parse_retry_after_seconds("3601"), None);
        assert_eq!(
            parse_retry_after_seconds("3600"),
            Some(Duration::from_secs(3600))
        );
    }

    #[tokio::test]
    async fn http_429_with_retry_after_propagates_hint() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nRetry-After: 7\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient());
        assert_eq!(err.retry_after(), Some(Duration::from_secs(7)));
    }

    #[tokio::test]
    async fn http_503_without_retry_after_returns_none() {
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient());
        assert_eq!(err.retry_after(), None);
    }

    #[tokio::test]
    async fn http_429_with_malformed_retry_after_returns_none() {
        // Endpoint returns garbage in the header. Should NOT fail the
        // request — just lose the hint.
        let (url, _counter) = spawn_mock_server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nRetry-After: tomorrow\r\n\r\n",
        ])
        .await;
        let sink = HttpSink::new(cfg(&url)).unwrap();
        let err = sink.commit(vec![p(b"alpha")]).await.unwrap_err();
        assert!(err.is_transient());
        assert_eq!(err.retry_after(), None);
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
