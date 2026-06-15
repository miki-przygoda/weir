//! ClickHouse sink — HTTP `INSERT … FORMAT RowBinary` with a content-derived
//! `insert_deduplication_token`. Reuses `sql_common` (identifier validation,
//! password redaction, `SqlSinkError`). Structurally mirrors `postgres.rs`.
//!
//! # The IOPS-compression story
//!
//! N records arrive at the drain → one HTTP `POST` of
//! `INSERT INTO db.table (col) FORMAT RowBinary` whose body is the batch
//! encoded as length-prefixed bytes → one ClickHouse insert. Same headline as
//! the SQL sinks, over HTTP instead of a SQL wire protocol.
//!
//! # At-least-once and idempotency
//!
//! The drain may re-call `commit()` with a byte-identical batch after a crash
//! (at-least-once per segment). This sink derives an `insert_deduplication_token`
//! from `sha256(batch)`, so a `Replicated*MergeTree` engine (or a `MergeTree`
//! with `non_replicated_deduplication_window` set) deduplicates the re-inserted
//! block. ClickHouse has no `ON CONFLICT`; dedup is the token + the engine's
//! choice — weir stays un-opinionated about the table model.
//!
//! # Target column type (F36)
//!
//! The RowBinary body is `leb128(len) ++ bytes` per record — the wire form for a
//! single ClickHouse **`String`** column. The configured `column` MUST be a
//! `String` (not `FixedString`/`LowCardinality`/`Nullable`/numeric), or the
//! insert is rejected. weir does not validate the remote column type; this is the
//! operator's responsibility.

use std::time::Duration;

use weir_core::Payload;

use super::sql_common;
use super::{CommitResult, Sink, SinkHealth};

/// ClickHouse identifier length cap. ClickHouse's own limit is generous; 63 is
/// a safe conservative bound (shared with the PG sink's `NAMEDATALEN - 1`).
const IDENTIFIER_MAX_LEN: usize = 63;

// ── RowBinary encoding ──────────────────────────────────────────────────────────

/// Append an unsigned LEB128 varint — the RowBinary string-length prefix.
fn write_leb128(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// Encode a batch as ClickHouse RowBinary for a single `String` column: each
/// payload is `leb128(len) ++ bytes`. Binary-safe — no escaping, handles
/// arbitrary payload bytes (unlike the `Values` text format).
fn encode_rowbinary(batch: &[Payload]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in batch {
        write_leb128(&mut out, p.len() as u64);
        out.extend_from_slice(p);
    }
    out
}

// ── Dedup token ─────────────────────────────────────────────────────────────────

/// Content-derived dedup token: `sha256(len(p₀) ++ p₀ ++ len(p₁) ++ p₁ ++ …)`,
/// lower-hex. A crash-replayed byte-identical batch produces the same token, so
/// a dedup-capable engine deduplicates the re-inserted block.
///
/// Each payload is length-prefixed before hashing so the digest is unambiguous
/// across different batch boundaries. Concatenating payloads without delimiters
/// would make `["ab", "c"]` and `["a", "bc"]` hash identically — ClickHouse
/// would then drop the second, genuinely-distinct block as a duplicate, losing
/// data. The 8-byte little-endian length restores a prefix-free framing.
///
/// Caveat (F35): the token covers exactly the sub-batch handed to `commit()`,
/// which the drain sizes by `sink_max_batch_size`. If that config changes across
/// a restart, a replayed segment re-splits into differently-sized sub-batches
/// whose tokens differ from the originals, so ClickHouse's
/// `insert_deduplication_token` won't recognise them as duplicates — at-least-once
/// can then double-insert. Keep `sink_max_batch_size` stable for the dedup
/// guarantee to hold across restarts.
fn dedup_token(batch: &[Payload]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for p in batch {
        hasher.update((p.len() as u64).to_le_bytes());
        hasher.update(p);
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

// ── Config + build error ────────────────────────────────────────────────────────

pub struct ClickHouseSinkConfig {
    /// `http://[user:password@]host:8123`. Required.
    pub url: String,
    pub database: String,
    pub table: String,
    pub column: String,
    /// Maximum records per `commit()` call.
    pub max_batch_size: usize,
    /// Per-request timeout. Applied to the send future via `tokio::time::timeout`.
    pub timeout: Duration,
}

impl std::fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSinkConfig")
            .field("url", &sql_common::redact_password(&self.url))
            .field("database", &self.database)
            .field("table", &self.table)
            .field("column", &self.column)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseSinkBuildError {
    #[error("clickhouse sink: url is empty")]
    EmptyUrl,
    #[error("clickhouse sink: url is not a valid http(s) URL: {0}")]
    InvalidUrl(String),
    #[error(
        "clickhouse sink {field} {value:?} is not a valid identifier \
         (must be [A-Za-z_][A-Za-z0-9_]{{0,62}})"
    )]
    InvalidIdentifier { field: String, value: String },
    #[error("clickhouse sink: building HTTP client: {0}")]
    Client(String),
}

impl From<sql_common::InvalidIdentifier> for ClickHouseSinkBuildError {
    fn from(e: sql_common::InvalidIdentifier) -> Self {
        ClickHouseSinkBuildError::InvalidIdentifier {
            field: e.field,
            value: e.value,
        }
    }
}

// ── Sink ────────────────────────────────────────────────────────────────────────

pub struct ClickHouseSink {
    config: ClickHouseSinkConfig,
    client: reqwest::Client,
    /// Base URL with any `user:password@` userinfo stripped; credentials are
    /// applied per-request as HTTP basic auth instead, so they never appear in
    /// the request URL.
    base_url: String,
    credentials: Option<(String, String)>,
}

impl std::fmt::Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink")
            .field("config", &self.config)
            .finish()
    }
}

impl ClickHouseSink {
    /// Build a new ClickHouse sink. Validates the URL is a non-empty http(s)
    /// URL and the database/table/column identifiers, then constructs an HTTP
    /// client. Only the *connection* is lazy — a reachability failure surfaces
    /// on the first `commit()`; a malformed URL is rejected here at startup.
    pub fn new(config: ClickHouseSinkConfig) -> Result<Self, ClickHouseSinkBuildError> {
        if config.url.is_empty() {
            return Err(ClickHouseSinkBuildError::EmptyUrl);
        }
        sql_common::validate_identifier("database", &config.database, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("table", &config.table, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("column", &config.column, IDENTIFIER_MAX_LEN)?;

        let (base_url, credentials) = split_credentials(&config.url);

        // Validate the credential-stripped URL parses as http(s) at build time
        // so an obviously-malformed URL fails at startup instead of only
        // surfacing on the first commit (where it would look like a transient
        // network fault and retry forever). base_url has any userinfo removed,
        // so neither arm can leak a password; the parse-error arm redacts
        // config.url defensively in case split_credentials left a secret in.
        match reqwest::Url::parse(&base_url) {
            Ok(u) if u.scheme() == "http" || u.scheme() == "https" => {}
            Ok(u) => {
                return Err(ClickHouseSinkBuildError::InvalidUrl(format!(
                    "scheme '{}' is not http or https (in {})",
                    u.scheme(),
                    base_url
                )));
            }
            Err(e) => {
                return Err(ClickHouseSinkBuildError::InvalidUrl(format!(
                    "{e} (in {})",
                    sql_common::redact_password(&config.url)
                )));
            }
        }

        let client = reqwest::Client::builder()
            // Whole-request timeout (covers the error-body read too). The explicit
            // tokio::time::timeout around send() only bounds the headers; without a
            // client timeout, resp.text() on an error response could hang until the
            // drain's coarse commit_timeout backstop fired (F32). Matches the HTTP
            // sink, which sets a client timeout.
            .timeout(config.timeout)
            // Never follow redirects: a redirected INSERT POST that reqwest re-issues
            // as a bodiless GET could return 2xx and be reported as committed though
            // the rows were never inserted — a false ack (G01). A 3xx now surfaces to
            // commit() and is dead-lettered, never committed. Matches the HTTP sink.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ClickHouseSinkBuildError::Client(e.to_string()))?;
        Ok(Self {
            config,
            client,
            base_url,
            credentials,
        })
    }

    fn insert_query(&self) -> String {
        format!(
            "INSERT INTO {}.{} ({}) FORMAT RowBinary",
            self.config.database, self.config.table, self.config.column
        )
    }
}

/// Whether an HTTP status from ClickHouse is transient (retry the segment) vs
/// permanent (dead-letter). 5xx, 408 (Request Timeout), and 429 (Too Many
/// Requests) are back-pressure: ClickHouse returns 429 under
/// `max_concurrent_queries` and a fronting proxy/LB commonly returns 429/503, so
/// the drain must retry rather than dead-letter live data (matches the HTTP
/// sink). Other 4xx (bad query, auth, unknown table/column) are permanent.
fn status_is_transient(status: reqwest::StatusCode) -> bool {
    status.is_server_error()
        || status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// Percent-decodes a URL userinfo component (RFC 3986 `%XX`). Invalid escapes
/// are left verbatim. ClickHouse — like the SQL sink drivers — expects the
/// DECODED credential, so a percent-encoded password (`p%40ss`) must
/// authenticate as `p@ss` rather than reaching the server literally (F34).
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (
                (b[i + 1] as char).to_digit(16),
                (b[i + 2] as char).to_digit(16),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Split an optional `user[:password]@` component out of the URL. Returns
/// `(base_url_without_userinfo, Some((user, password)))` if userinfo is present
/// (password defaults to empty when only a username is given), else `(url, None)`.
/// User and password are percent-decoded (F34).
fn split_credentials(url: &str) -> (String, Option<(String, String)>) {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        // userinfo lives in the authority (up to the first '/', '?', '#'); the
        // userinfo/host boundary is the LAST '@' there. A password may contain
        // unencoded '@' characters — using the first '@' would split mid-password,
        // authenticate with the wrong credentials, and splice a secret fragment
        // into the base URL (which then ends up in the request + access logs).
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        if let Some(at) = rest[..authority_end].rfind('@') {
            let creds = &rest[..at];
            // `user:password`, or a bare `username` with no ':' — either way the
            // userinfo must be stripped from base_url so it isn't sent in the URL
            // or logged (F33: a username-only authority was previously left in).
            let (user_raw, pass_raw) = match creds.find(':') {
                Some(colon) => (&creds[..colon], &creds[colon + 1..]),
                None => (creds, ""),
            };
            let base = format!("{}://{}", &url[..scheme_end], &rest[at + 1..]);
            return (
                base,
                Some((percent_decode(user_raw), percent_decode(pass_raw))),
            );
        }
    }
    (url.to_string(), None)
}

impl Sink for ClickHouseSink {
    type Record = Payload;
    type Error = sql_common::SqlSinkError;

    async fn commit(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, sql_common::SqlSinkError> {
        if batch.is_empty() {
            return Ok(CommitResult {
                committed: Vec::new(),
                dead_lettered: Vec::new(),
            });
        }
        let body = encode_rowbinary(&batch);
        let token = dedup_token(&batch);

        let mut req = self
            .client
            .post(&self.base_url)
            .query(&[
                ("query", self.insert_query().as_str()),
                ("insert_deduplication_token", token.as_str()),
            ])
            .body(body);
        if let Some((user, pass)) = &self.credentials {
            req = req.basic_auth(user, Some(pass));
        }

        let resp = match tokio::time::timeout(self.config.timeout, req.send()).await {
            Err(_elapsed) => return Err(sql_common::SqlSinkError::timeout("clickhouse")),
            Ok(Err(e)) if e.is_timeout() => {
                return Err(sql_common::SqlSinkError::timeout("clickhouse"));
            }
            // connect / DNS / network reset → transient (retry the segment).
            Ok(Err(e)) => {
                return Err(sql_common::SqlSinkError::transient(
                    "clickhouse",
                    format!("request: {e}"),
                ));
            }
            Ok(Ok(r)) => r,
        };

        let status = resp.status();
        if status.is_success() {
            Ok(CommitResult {
                committed: batch,
                dead_lettered: Vec::new(),
            })
        } else if status.is_redirection() {
            // Surfaces only because redirect-following is disabled (G01). An
            // INSERT endpoint that redirects is a misconfiguration; dead-letter
            // (permanent), never commit — the rows were not inserted here.
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("<no Location header>")
                .to_string();
            Err(sql_common::SqlSinkError::permanent(
                "clickhouse",
                format!(
                    "http {status}: unexpected redirect to '{location}'; point the sink URL \
                     directly at the ClickHouse HTTP endpoint (redirects are not followed)"
                ),
            ))
        } else if status_is_transient(status) {
            Err(sql_common::SqlSinkError::transient(
                "clickhouse",
                format!("http {status}"),
            ))
        } else {
            // Other 4xx → permanent (bad query, auth, unknown table/column). Dead-letter.
            let detail = resp.text().await.unwrap_or_default();
            let detail: String = detail.chars().take(500).collect();
            Err(sql_common::SqlSinkError::permanent(
                "clickhouse",
                format!("http {status}: {detail}"),
            ))
        }
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        let mut req = self
            .client
            .get(&self.base_url)
            .query(&[("query", "SELECT 1")]);
        if let Some((user, pass)) = &self.credentials {
            req = req.basic_auth(user, Some(pass));
        }
        match tokio::time::timeout(self.config.timeout, req.send()).await {
            Ok(Ok(r)) if r.status().is_success() => SinkHealth::Healthy,
            Ok(Ok(r)) => SinkHealth::Down(format!("http {}", r.status())),
            Ok(Err(e)) => SinkHealth::Down(format!("request: {e}")),
            Err(_) => SinkHealth::Down("health probe timed out".to_string()),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    // `is_transient` comes from the SinkError trait; bring it into scope for the
    // commit() classification tests below.
    use weir_sink_sdk::SinkError;

    fn p(s: &'static [u8]) -> Payload {
        Payload::from_static(s)
    }

    // ── RowBinary ──────────────────────────────────────────────────────────

    #[test]
    fn rowbinary_empty_batch_is_empty() {
        assert!(encode_rowbinary(&[]).is_empty());
    }

    #[test]
    fn rowbinary_single_short_payload() {
        let out = encode_rowbinary(&[p(b"ab")]);
        assert_eq!(out, vec![0x02, b'a', b'b']);
    }

    #[test]
    fn rowbinary_multi_payload_concatenates_rows() {
        let out = encode_rowbinary(&[p(b"a"), p(b"bc")]);
        assert_eq!(out, vec![0x01, b'a', 0x02, b'b', b'c']);
    }

    #[test]
    fn rowbinary_len_uses_multibyte_leb128_past_127() {
        // 200 = 0xC8 → LEB128 [0xC8, 0x01].
        let payload = Payload::from(vec![0u8; 200]);
        let out = encode_rowbinary(&[payload]);
        assert_eq!(&out[..2], &[0xC8, 0x01]);
        assert_eq!(out.len(), 2 + 200);
    }

    #[test]
    fn rowbinary_handles_empty_payload() {
        assert_eq!(encode_rowbinary(&[Payload::new()]), vec![0x00]);
    }

    // ── Dedup token ────────────────────────────────────────────────────────

    #[test]
    fn dedup_token_is_deterministic() {
        let b = vec![p(b"x"), p(b"yy")];
        assert_eq!(dedup_token(&b), dedup_token(&b));
    }

    #[test]
    fn dedup_token_changes_on_reorder() {
        let a = vec![p(b"x"), p(b"yy")];
        let b = vec![p(b"yy"), p(b"x")];
        assert_ne!(dedup_token(&a), dedup_token(&b));
    }

    #[test]
    fn dedup_token_distinguishes_different_batch_boundaries() {
        // ["ab","c"] and ["a","bc"] concatenate to the same bytes but are
        // different batches — they must NOT share a dedup token, or ClickHouse
        // would drop the second as a duplicate and lose data.
        let a = vec![p(b"ab"), p(b"c")];
        let b = vec![p(b"a"), p(b"bc")];
        assert_ne!(dedup_token(&a), dedup_token(&b));
    }

    #[test]
    fn dedup_token_is_64_hex_chars() {
        let t = dedup_token(&[p(b"hello")]);
        assert_eq!(t.len(), 64);
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ── Config / new ───────────────────────────────────────────────────────

    fn cfg(url: &str, table: &str, column: &str) -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            url: url.to_string(),
            database: "default".to_string(),
            table: table.to_string(),
            column: column.to_string(),
            max_batch_size: 1000,
            timeout: Duration::from_secs(30),
        }
    }

    #[test]
    fn new_rejects_empty_url() {
        let e = ClickHouseSink::new(cfg("", "t", "payload")).unwrap_err();
        assert!(matches!(e, ClickHouseSinkBuildError::EmptyUrl));
    }

    #[test]
    fn new_rejects_bad_table_identifier() {
        let e =
            ClickHouseSink::new(cfg("http://h:8123", "bad-table; DROP", "payload")).unwrap_err();
        assert!(matches!(
            e,
            ClickHouseSinkBuildError::InvalidIdentifier { .. }
        ));
    }

    #[test]
    fn new_accepts_valid_config() {
        assert!(ClickHouseSink::new(cfg("http://h:8123", "weir_records", "payload")).is_ok());
        assert!(ClickHouseSink::new(cfg("https://h:8443", "weir_records", "payload")).is_ok());
    }

    #[test]
    fn new_rejects_url_without_scheme() {
        let e = ClickHouseSink::new(cfg("localhost:8123", "t", "payload")).unwrap_err();
        assert!(
            matches!(e, ClickHouseSinkBuildError::InvalidUrl(_)),
            "{e:?}"
        );
    }

    #[test]
    fn new_rejects_non_http_scheme() {
        let e = ClickHouseSink::new(cfg("ftp://h:8123", "t", "payload")).unwrap_err();
        assert!(
            matches!(e, ClickHouseSinkBuildError::InvalidUrl(_)),
            "{e:?}"
        );
    }

    #[test]
    fn new_invalid_url_error_redacts_password() {
        // A malformed but credentialed URL must not leak the password in the
        // build error. "ht!tp" is an invalid scheme so url parsing fails.
        let e = ClickHouseSink::new(cfg("ht!tp://user:secret@host", "t", "payload")).unwrap_err();
        let msg = e.to_string();
        assert!(
            matches!(e, ClickHouseSinkBuildError::InvalidUrl(_)),
            "{e:?}"
        );
        assert!(!msg.contains("secret"), "password leaked: {msg}");
    }

    #[test]
    fn debug_redacts_url_password() {
        let s = ClickHouseSink::new(cfg("http://user:secret@h:8123", "t", "payload")).unwrap();
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("secret"), "password leaked: {dbg}");
        assert!(dbg.contains("<redacted>"));
    }

    #[test]
    fn split_credentials_extracts_userinfo() {
        let (base, creds) = split_credentials("http://user:pw@host:8123");
        assert_eq!(base, "http://host:8123");
        assert_eq!(creds, Some(("user".to_string(), "pw".to_string())));
    }

    #[test]
    fn split_credentials_leaves_plain_url() {
        let (base, creds) = split_credentials("http://host:8123");
        assert_eq!(base, "http://host:8123");
        assert_eq!(creds, None);
    }

    #[test]
    fn status_classification_treats_backpressure_as_transient() {
        use reqwest::StatusCode;
        // Back-pressure / transient — retry, never dead-letter.
        assert!(status_is_transient(StatusCode::TOO_MANY_REQUESTS)); // 429
        assert!(status_is_transient(StatusCode::REQUEST_TIMEOUT)); // 408
        assert!(status_is_transient(StatusCode::INTERNAL_SERVER_ERROR)); // 500
        assert!(status_is_transient(StatusCode::SERVICE_UNAVAILABLE)); // 503
        // Permanent — dead-letter.
        assert!(!status_is_transient(StatusCode::BAD_REQUEST)); // 400
        assert!(!status_is_transient(StatusCode::UNAUTHORIZED)); // 401
        assert!(!status_is_transient(StatusCode::NOT_FOUND)); // 404
        assert!(!status_is_transient(StatusCode::CONFLICT)); // 409
    }

    #[test]
    fn split_credentials_handles_username_only_userinfo() {
        // F33: a bare username with no password must still be stripped from
        // base_url (previously left "user@" in the URL).
        let (base, creds) = split_credentials("http://justuser@host:8123");
        assert_eq!(base, "http://host:8123");
        assert_eq!(creds, Some(("justuser".to_string(), String::new())));
    }

    #[test]
    fn split_credentials_percent_decodes_userinfo() {
        // F34: percent-encoded credentials must be decoded before basic auth,
        // matching the SQL drivers — p%40ss is the password p@ss.
        let (base, creds) = split_credentials("http://user:p%40ss%21@host:8123");
        assert_eq!(base, "http://host:8123");
        assert_eq!(creds, Some(("user".to_string(), "p@ss!".to_string())));
    }

    #[test]
    fn percent_decode_leaves_invalid_escapes_verbatim() {
        assert_eq!(percent_decode("ab%2"), "ab%2"); // truncated escape
        assert_eq!(percent_decode("a%zzb"), "a%zzb"); // non-hex
        assert_eq!(percent_decode("plain"), "plain");
    }

    #[test]
    fn split_credentials_handles_at_sign_in_password() {
        // A '@' in the password must not split mid-password: the wrong split
        // authenticates with truncated creds AND splices the secret tail into the
        // base URL (which lands in the request + access logs).
        let (base, creds) = split_credentials("http://user:p@ss@host:8123");
        assert_eq!(
            base, "http://host:8123",
            "secret fragment must not leak into base"
        );
        assert_eq!(creds, Some(("user".to_string(), "p@ss".to_string())));
    }

    // ── Query builder ──────────────────────────────────────────────────────

    #[test]
    fn insert_query_is_well_formed() {
        let s = ClickHouseSink::new(cfg("http://h:8123", "weir_records", "payload")).unwrap();
        assert_eq!(
            s.insert_query(),
            "INSERT INTO default.weir_records (payload) FORMAT RowBinary"
        );
    }

    // ── commit() request/response classification (F37) ──────────────────────
    //
    // Previously commit() had zero in-process coverage — only an #[ignore]
    // live-docker integration test — so a status-classification or dedup-token
    // regression passed the running suite. These drive commit() against an
    // in-process mock that captures the request line and returns a canned status.

    /// Minimal HTTP/1.1 mock: replies to each request with `status_line` + `body`
    /// and records every request line (method + target, which carries the query
    /// string) in the returned buffer. `sleep_before` stalls the response to
    /// exercise the timeout path.
    async fn spawn_ch_mock(
        status_line: &'static str,
        body: &'static str,
        sleep_before: Duration,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/");
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let captured_task = std::sync::Arc::clone(&captured);
        let response = format!(
            "{status_line}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let captured = std::sync::Arc::clone(&captured_task);
                let response = response.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    let header_end = loop {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    break pos;
                                }
                            }
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let request_line = head.lines().next().unwrap_or("").to_string();
                    // Drain the body so the client's send() completes cleanly.
                    let content_length = head
                        .lines()
                        .find_map(|l| {
                            let (n, v) = l.split_once(':')?;
                            n.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    let mut have = buf.len() - (header_end + 4);
                    while have < content_length {
                        match socket.read(&mut tmp).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => have += n,
                        }
                    }
                    captured.lock().unwrap().push(request_line);
                    if !sleep_before.is_zero() {
                        tokio::time::sleep(sleep_before).await;
                    }
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        (url, captured)
    }

    #[tokio::test]
    async fn commit_2xx_commits_batch_and_sends_query_and_dedup_token() {
        let (url, captured) = spawn_ch_mock("HTTP/1.1 200 OK", "", Duration::ZERO).await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let result = sink.commit(vec![p(b"a"), p(b"b")]).await.unwrap();
        assert_eq!(result.committed.len(), 2);
        assert!(result.dead_lettered.is_empty());

        tokio::time::sleep(Duration::from_millis(50)).await;
        let reqs = captured.lock().unwrap();
        assert_eq!(reqs.len(), 1, "expected exactly one POST");
        // The INSERT query and the dedup token must be on the wire.
        assert!(reqs[0].contains("query="), "no query param: {}", reqs[0]);
        assert!(
            reqs[0].contains("insert_deduplication_token="),
            "no dedup token on the wire: {}",
            reqs[0]
        );
    }

    #[tokio::test]
    async fn commit_500_is_transient() {
        let (url, _c) =
            spawn_ch_mock("HTTP/1.1 500 Internal Server Error", "", Duration::ZERO).await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let err = sink.commit(vec![p(b"a")]).await.unwrap_err();
        assert!(err.is_transient(), "500 must be transient: {err}");
    }

    #[tokio::test]
    async fn commit_429_is_transient() {
        let (url, _c) = spawn_ch_mock("HTTP/1.1 429 Too Many Requests", "", Duration::ZERO).await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let err = sink.commit(vec![p(b"a")]).await.unwrap_err();
        assert!(err.is_transient(), "429 must be transient: {err}");
    }

    #[tokio::test]
    async fn commit_redirect_is_permanent_never_committed() {
        // G01: a redirected INSERT must never be reported committed (the rows
        // weren't inserted). Redirects are disabled, so the 302 surfaces and is
        // dead-lettered (permanent).
        let (url, _c) = spawn_ch_mock(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/x",
            "",
            Duration::ZERO,
        )
        .await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let err = sink.commit(vec![p(b"a")]).await.unwrap_err();
        assert!(!err.is_transient(), "a redirect must be permanent: {err}");
        let msg = err.to_string();
        assert!(msg.contains("302"), "{msg}");
        assert!(msg.contains("redirect"), "{msg}");
    }

    #[tokio::test]
    async fn commit_4xx_is_permanent_with_detail() {
        let (url, _c) = spawn_ch_mock(
            "HTTP/1.1 400 Bad Request",
            "Unknown column foo",
            Duration::ZERO,
        )
        .await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let err = sink.commit(vec![p(b"a")]).await.unwrap_err();
        assert!(!err.is_transient(), "400 must be permanent: {err}");
        let msg = err.to_string();
        assert!(msg.contains("400"), "{msg}");
        assert!(msg.contains("Unknown column"), "detail dropped: {msg}");
    }

    #[tokio::test]
    async fn commit_timeout_maps_to_timeout_error() {
        // Server stalls 5s; the sink's 300ms timeout must fire first.
        let (url, _c) = spawn_ch_mock("HTTP/1.1 200 OK", "", Duration::from_secs(5)).await;
        let mut c = cfg(&url, "weir_records", "payload");
        c.timeout = Duration::from_millis(300);
        let sink = ClickHouseSink::new(c).unwrap();
        let err = tokio::time::timeout(Duration::from_secs(2), sink.commit(vec![p(b"a")]))
            .await
            .expect("commit must honour its own timeout, not hang")
            .unwrap_err();
        // A timeout is classified transient (retry the segment).
        assert!(err.is_transient(), "timeout must be transient: {err}");
    }

    #[tokio::test]
    async fn commit_empty_batch_makes_no_request() {
        let (url, captured) = spawn_ch_mock("HTTP/1.1 200 OK", "", Duration::ZERO).await;
        let sink = ClickHouseSink::new(cfg(&url, "weir_records", "payload")).unwrap();
        let result = sink.commit(vec![]).await.unwrap();
        assert!(result.committed.is_empty() && result.dead_lettered.is_empty());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            captured.lock().unwrap().is_empty(),
            "an empty batch must not POST"
        );
    }
}
