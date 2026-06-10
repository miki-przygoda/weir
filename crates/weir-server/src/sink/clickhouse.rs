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

/// Content-derived dedup token: `sha256(payload₀ ++ payload₁ ++ …)`, lower-hex.
/// A crash-replayed byte-identical batch produces the same token, so a
/// dedup-capable engine deduplicates the re-inserted block.
fn dedup_token(batch: &[Payload]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for p in batch {
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
    /// Build a new ClickHouse sink. Validates the URL is non-empty and the
    /// database/table/column identifiers, then constructs an HTTP client. The
    /// first request is lazy — connection failures surface on `commit()`.
    pub fn new(config: ClickHouseSinkConfig) -> Result<Self, ClickHouseSinkBuildError> {
        if config.url.is_empty() {
            return Err(ClickHouseSinkBuildError::EmptyUrl);
        }
        sql_common::validate_identifier("database", &config.database, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("table", &config.table, IDENTIFIER_MAX_LEN)?;
        sql_common::validate_identifier("column", &config.column, IDENTIFIER_MAX_LEN)?;

        let (base_url, credentials) = split_credentials(&config.url);

        let client = reqwest::Client::builder()
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

/// Split an optional `user:password@` component out of the URL. Returns
/// `(base_url_without_userinfo, Some((user, password)))` if present, else
/// `(url, None)`.
fn split_credentials(url: &str) -> (String, Option<(String, String)>) {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            let creds = &rest[..at];
            if let Some(colon) = creds.find(':') {
                let user = creds[..colon].to_string();
                let pass = creds[colon + 1..].to_string();
                let base = format!("{}://{}", &url[..scheme_end], &rest[at + 1..]);
                return (base, Some((user, pass)));
            }
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
        } else if status.is_server_error() {
            Err(sql_common::SqlSinkError::transient(
                "clickhouse",
                format!("http {status}"),
            ))
        } else {
            // 4xx → permanent (bad query, auth, unknown table/column). Dead-letter.
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

    // ── RowBinary ──────────────────────────────────────────────────────────

    #[test]
    fn rowbinary_empty_batch_is_empty() {
        assert!(encode_rowbinary(&[]).is_empty());
    }

    #[test]
    fn rowbinary_single_short_payload() {
        let out = encode_rowbinary(&[b"ab".to_vec()]);
        assert_eq!(out, vec![0x02, b'a', b'b']);
    }

    #[test]
    fn rowbinary_multi_payload_concatenates_rows() {
        let out = encode_rowbinary(&[b"a".to_vec(), b"bc".to_vec()]);
        assert_eq!(out, vec![0x01, b'a', 0x02, b'b', b'c']);
    }

    #[test]
    fn rowbinary_len_uses_multibyte_leb128_past_127() {
        // 200 = 0xC8 → LEB128 [0xC8, 0x01].
        let payload = vec![0u8; 200];
        let out = encode_rowbinary(&[payload]);
        assert_eq!(&out[..2], &[0xC8, 0x01]);
        assert_eq!(out.len(), 2 + 200);
    }

    #[test]
    fn rowbinary_handles_empty_payload() {
        assert_eq!(encode_rowbinary(&[Vec::new()]), vec![0x00]);
    }

    // ── Dedup token ────────────────────────────────────────────────────────

    #[test]
    fn dedup_token_is_deterministic() {
        let b = vec![b"x".to_vec(), b"yy".to_vec()];
        assert_eq!(dedup_token(&b), dedup_token(&b));
    }

    #[test]
    fn dedup_token_changes_on_reorder() {
        let a = vec![b"x".to_vec(), b"yy".to_vec()];
        let b = vec![b"yy".to_vec(), b"x".to_vec()];
        assert_ne!(dedup_token(&a), dedup_token(&b));
    }

    #[test]
    fn dedup_token_is_64_hex_chars() {
        let t = dedup_token(&[b"hello".to_vec()]);
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

    // ── Query builder ──────────────────────────────────────────────────────

    #[test]
    fn insert_query_is_well_formed() {
        let s = ClickHouseSink::new(cfg("http://h:8123", "weir_records", "payload")).unwrap();
        assert_eq!(
            s.insert_query(),
            "INSERT INTO default.weir_records (payload) FORMAT RowBinary"
        );
    }
}
