//! MySQL sink: writes a whole batch with one multi-row `INSERT` statement.
//!
//! # The IOPS-compression story
//!
//! N records arrive at the drain → one `INSERT INTO t (col) VALUES (?), (?), …`
//! → one network round-trip → one server-side commit. This is the headline
//! claim weir was built for and the reason this sink exists alongside
//! `HttpSink`.
//!
//! # Schema contract
//!
//! The sink does **not** auto-create the target table. The operator
//! provisions a table with a column wide enough to hold the payload bytes
//! (typically `VARBINARY(N)` or `BLOB`); see
//! [`docs/operations/configuration.md`](../../../../docs/operations/configuration.md)
//! for a reference schema.
//!
//! # At-least-once and idempotency
//!
//! The drain may re-call `commit()` for a segment that was partially
//! committed pre-crash (the `.confirmed` sidecar was not written). The
//! sink defaults to `INSERT IGNORE`, which silently drops duplicates if
//! the target table has a `UNIQUE` constraint. With `insert_mode = "plain"`
//! the operator opts out — and accepts the resulting duplicate rows on
//! crash-recovery retries.
//!
//! # Error classification
//!
//! - Connection / pool / IO failures → transient (drain retries).
//! - MySQL error codes 1205 (`ER_LOCK_WAIT_TIMEOUT`), 1213
//!   (`ER_LOCK_DEADLOCK`), 1290 (`ER_OPTION_PREVENTS_STATEMENT`, e.g.
//!   `--read-only`) → transient.
//! - Codes 1062 (`ER_DUP_ENTRY`) — never seen under `INSERT IGNORE`; under
//!   `plain` mode treated as transient so the drain retries the segment
//!   after backoff, in case the duplicate is from a stale concurrent
//!   writer rather than a re-commit.
//! - All other server errors (syntax, missing table, missing column,
//!   access denied, …) → permanent. The whole batch is dead-lettered
//!   with the server-supplied error message so an operator can debug.
//!
//! # Authentication
//!
//! Credentials are taken from the connection URL
//! (`mysql://user:pass@host:3306/db`). The URL is read from
//! `WEIR_SINK_MYSQL_URL` at startup; never sourced from the TOML config.
//! `Debug` impls redact the password before logging.

use std::sync::Arc;
use std::time::Duration;

use mysql_async::{Pool, prelude::Queryable};
use tracing::{debug, warn};
use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

// ── Configuration ─────────────────────────────────────────────────────────────

/// How to phrase the INSERT statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertMode {
    /// `INSERT IGNORE INTO …` — duplicate-key errors are silently dropped by
    /// the server. Recommended default: pair with a `UNIQUE` constraint on
    /// a payload hash column so crash-recovery retries are idempotent
    /// without consumer-side dedup.
    Ignore,
    /// `INSERT INTO …` — duplicates surface as `ER_DUP_ENTRY` and are
    /// classified as transient (drain retries).
    Plain,
}

/// Configuration for `MySqlSink`.
///
/// The connection URL carries credentials in plain text. The `Debug` impl
/// redacts the password so a `?config` log line cannot leak it.
#[derive(Clone)]
pub struct MySqlSinkConfig {
    /// `mysql://user:password@host:3306/database`. Required.
    pub url: String,
    /// Target table. Must match `[A-Za-z_][A-Za-z0-9_]*`, length ≤ 64
    /// (MySQL identifier rules — we don't escape, we validate at build).
    pub table: String,
    /// Target column. Same identifier rules as `table`.
    pub column: String,
    /// How to phrase the INSERT statement.
    pub insert_mode: InsertMode,
    /// Maximum records per `commit()` call. Larger batches reduce IOPS at
    /// the cost of statement length; MySQL's default `max_allowed_packet`
    /// is 64 MiB and per-statement cost grows linearly, so very large
    /// batches don't help.
    pub max_batch_size: usize,
    /// Per-query timeout. Applied to `exec_drop` via a `tokio::time::timeout`
    /// wrapper at the call site.
    pub timeout: Duration,
}

impl std::fmt::Debug for MySqlSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlSinkConfig")
            .field("url", &redact_password(&self.url))
            .field("table", &self.table)
            .field("column", &self.column)
            .field("insert_mode", &self.insert_mode)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// Best-effort password redaction for log lines. Matches
/// `scheme://user:PASSWORD@host` and replaces `PASSWORD` with `<redacted>`.
/// If the URL doesn't match that shape we return it unchanged.
fn redact_password(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    let Some(at) = rest.find('@') else {
        return url.to_string();
    };
    let creds = &rest[..at];
    let Some(colon) = creds.find(':') else {
        return url.to_string();
    };
    let user = &creds[..colon];
    let tail = &rest[at..];
    format!("{}://{}:<redacted>{}", &url[..scheme_end], user, tail)
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// MySQL sink. The pool is cheap to clone (`Arc` inside).
pub struct MySqlSink {
    config: MySqlSinkConfig,
    pool: Arc<Pool>,
}

impl std::fmt::Debug for MySqlSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlSink")
            .field("config", &self.config)
            .finish()
    }
}

impl MySqlSink {
    /// Build a new MySQL sink. Validates the URL shape, table and column
    /// identifiers, and constructs a connection pool. The first connection
    /// is lazy — failures show up on the first `commit()`, not here.
    pub fn new(config: MySqlSinkConfig) -> Result<Self, MySqlSinkBuildError> {
        if config.url.is_empty() {
            return Err(MySqlSinkBuildError::EmptyUrl);
        }
        validate_identifier("table", &config.table)?;
        validate_identifier("column", &config.column)?;
        let opts = mysql_async::Opts::from_url(&config.url)
            .map_err(|e| MySqlSinkBuildError::InvalidUrl(e.to_string()))?;
        let pool = Pool::new(opts);
        Ok(Self {
            config,
            pool: Arc::new(pool),
        })
    }

    /// Build the multi-row INSERT statement for a batch of size `n`. Public
    /// for unit testing; do not call from the hot path (commit() already
    /// inlines this).
    pub(crate) fn build_insert_sql(&self, n: usize) -> String {
        let placeholders: String = std::iter::repeat_n("(?)", n)
            .collect::<Vec<_>>()
            .join(", ");
        let verb = match self.config.insert_mode {
            InsertMode::Ignore => "INSERT IGNORE INTO",
            InsertMode::Plain => "INSERT INTO",
        };
        // Identifiers are pre-validated to `[A-Za-z_][A-Za-z0-9_]{0,63}`, so
        // there is no SQL injection vector through `table` or `column`.
        format!(
            "{verb} `{}` (`{}`) VALUES {placeholders}",
            self.config.table, self.config.column
        )
    }
}

/// MySQL identifiers used in the sink must be pure ASCII alphanumerics plus
/// underscore, starting with a letter or underscore, ≤ 64 chars. This is a
/// strict subset of what MySQL allows in backtick-quoted form, and the
/// strictness is intentional: validating here means the sink can build SQL
/// strings via `format!` with no escaping logic to get wrong.
fn validate_identifier(field: &str, value: &str) -> Result<(), MySqlSinkBuildError> {
    if value.is_empty() || value.len() > 64 {
        return Err(MySqlSinkBuildError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
        });
    }
    let mut chars = value.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(MySqlSinkBuildError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
        });
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(MySqlSinkBuildError::InvalidIdentifier {
                field: field.to_string(),
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors that can occur during `MySqlSink::new()`. All build-time —
/// permanent by definition, no `SinkError` impl needed.
#[derive(Debug)]
pub enum MySqlSinkBuildError {
    EmptyUrl,
    InvalidUrl(String),
    InvalidIdentifier { field: String, value: String },
}

impl std::fmt::Display for MySqlSinkBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MySqlSinkBuildError::EmptyUrl => write!(f, "mysql sink url is empty"),
            MySqlSinkBuildError::InvalidUrl(e) => write!(f, "mysql sink url invalid: {e}"),
            MySqlSinkBuildError::InvalidIdentifier { field, value } => write!(
                f,
                "mysql sink {field} {value:?} is not a valid identifier \
                 (must match [A-Za-z_][A-Za-z0-9_]{{0,63}})"
            ),
        }
    }
}

impl std::error::Error for MySqlSinkBuildError {}

/// Errors returned by `MySqlSink::commit`.
#[derive(Debug)]
pub enum MySqlSinkError {
    /// Connection / pool / IO failures and explicit-transient server codes.
    /// Drain retries the whole segment.
    Transient(String),
    /// Permanent error — bad schema, bad auth, syntax error, payload too
    /// large for the column. Drain dead-letters the batch with this string
    /// as the reason.
    Permanent(String),
    /// Per-query timeout — `sink_timeout_secs` exceeded. Transient: the
    /// server is probably overloaded; backoff and retry.
    Timeout,
}

impl std::fmt::Display for MySqlSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MySqlSinkError::Transient(e) => write!(f, "mysql sink transient: {e}"),
            MySqlSinkError::Permanent(e) => write!(f, "mysql sink permanent: {e}"),
            MySqlSinkError::Timeout => write!(f, "mysql sink timeout"),
        }
    }
}

impl std::error::Error for MySqlSinkError {}

impl SinkError for MySqlSinkError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            MySqlSinkError::Transient(_) | MySqlSinkError::Timeout
        )
    }
}

/// Classify a `mysql_async::Error` into the drain-facing transient /
/// permanent buckets. Public for unit testing.
pub(crate) fn classify(err: mysql_async::Error) -> MySqlSinkError {
    use mysql_async::Error as E;
    match err {
        // Network / pool / driver-level IO: always transient.
        E::Io(e) => MySqlSinkError::Transient(format!("io: {e}")),
        E::Driver(e) => MySqlSinkError::Transient(format!("driver: {e}")),
        // URL parse errors are permanent (config error).
        E::Url(e) => MySqlSinkError::Permanent(format!("url: {e}")),
        // Server-reported errors. Code 0 should not happen but if it does,
        // treat as permanent so we don't loop forever.
        E::Server(srv) => {
            let code = srv.code;
            let msg = srv.message.clone();
            if is_transient_server_code(code) {
                MySqlSinkError::Transient(format!("server {code}: {msg}"))
            } else {
                MySqlSinkError::Permanent(format!("server {code}: {msg}"))
            }
        }
        E::Other(e) => MySqlSinkError::Permanent(format!("other: {e}")),
    }
}

/// Returns true for server error codes the drain should retry rather than
/// dead-letter. Conservatively short list — adding a code here risks an
/// infinite retry loop, removing one risks dead-lettering a recoverable
/// situation.
pub(crate) fn is_transient_server_code(code: u16) -> bool {
    matches!(
        code,
        1205  // ER_LOCK_WAIT_TIMEOUT
            | 1213  // ER_LOCK_DEADLOCK
            | 1290  // ER_OPTION_PREVENTS_STATEMENT (server in --read-only)
            | 1317  // ER_QUERY_INTERRUPTED
            | 1062  // ER_DUP_ENTRY — see module docs (Plain mode only)
    )
}

// ── Sink trait impl ───────────────────────────────────────────────────────────

impl Sink for MySqlSink {
    type Record = Payload;
    type Error = MySqlSinkError;

    async fn commit(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, MySqlSinkError> {
        if batch.is_empty() {
            return Ok(CommitResult {
                committed: Vec::new(),
                dead_lettered: Vec::new(),
            });
        }

        let sql = self.build_insert_sql(batch.len());
        let params: Vec<mysql_async::Value> = batch
            .iter()
            .map(|p| mysql_async::Value::Bytes(p.clone()))
            .collect();

        let conn_fut = async {
            let mut conn = self.pool.get_conn().await.map_err(classify)?;
            conn.exec_drop(&sql, mysql_async::Params::Positional(params))
                .await
                .map_err(classify)
        };

        match tokio::time::timeout(self.config.timeout, conn_fut).await {
            Ok(Ok(())) => {
                debug!(
                    records = batch.len(),
                    "mysql sink committed batch as single INSERT"
                );
                Ok(CommitResult {
                    committed: batch,
                    dead_lettered: Vec::new(),
                })
            }
            Ok(Err(e)) => {
                if !e.is_transient() {
                    let reason = format!("{e}");
                    warn!(
                        records = batch.len(),
                        error = %reason,
                        "mysql sink permanently rejected batch; dead-lettering"
                    );
                    let dead_lettered = batch
                        .into_iter()
                        .map(|r| (r, reason.clone()))
                        .collect();
                    Ok(CommitResult {
                        committed: Vec::new(),
                        dead_lettered,
                    })
                } else {
                    Err(e)
                }
            }
            Err(_elapsed) => Err(MySqlSinkError::Timeout),
        }
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        let probe = async {
            let mut conn = self.pool.get_conn().await?;
            conn.query_drop("SELECT 1").await
        };
        match tokio::time::timeout(self.config.timeout, probe).await {
            Ok(Ok(())) => SinkHealth::Healthy,
            Ok(Err(e)) => SinkHealth::Down(format!("mysql probe failed: {e}")),
            Err(_) => SinkHealth::Down("mysql probe timed out".into()),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> MySqlSinkConfig {
        MySqlSinkConfig {
            url: "mysql://user:pw@127.0.0.1:3306/db".to_string(),
            table: "weir_records".to_string(),
            column: "payload".to_string(),
            insert_mode: InsertMode::Ignore,
            max_batch_size: 1000,
            timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn empty_url_rejected_at_build() {
        let mut c = cfg();
        c.url = String::new();
        assert!(matches!(
            MySqlSink::new(c).unwrap_err(),
            MySqlSinkBuildError::EmptyUrl
        ));
    }

    #[test]
    fn invalid_url_rejected_at_build() {
        let mut c = cfg();
        c.url = "not-a-url".to_string();
        assert!(matches!(
            MySqlSink::new(c).unwrap_err(),
            MySqlSinkBuildError::InvalidUrl(_)
        ));
    }

    #[test]
    fn valid_identifiers_accepted() {
        assert!(validate_identifier("t", "weir_records").is_ok());
        assert!(validate_identifier("t", "_internal").is_ok());
        assert!(validate_identifier("t", "T123").is_ok());
        assert!(validate_identifier("t", "a").is_ok());
        // 64 chars (MySQL's identifier length limit).
        let max = "a".repeat(64);
        assert!(validate_identifier("t", &max).is_ok());
    }

    #[test]
    fn injection_attempts_rejected_by_identifier_validation() {
        // The whole point of identifier validation is to make these unrepresentable.
        for bad in [
            "",
            " ",
            "1starts_with_digit",
            "has space",
            "has`backtick",
            "has-hyphen",
            "has;semi",
            "has\"quote",
            "has'quote",
            "has--comment",
            "drop_table; DROP TABLE x",
            &"a".repeat(65),
        ] {
            assert!(
                validate_identifier("t", bad).is_err(),
                "identifier {bad:?} should have been rejected"
            );
        }
    }

    #[test]
    fn build_sql_ignore_mode_single_row() {
        let sink = MySqlSink::new(cfg()).unwrap();
        assert_eq!(
            sink.build_insert_sql(1),
            "INSERT IGNORE INTO `weir_records` (`payload`) VALUES (?)"
        );
    }

    #[test]
    fn build_sql_ignore_mode_multi_row() {
        let sink = MySqlSink::new(cfg()).unwrap();
        assert_eq!(
            sink.build_insert_sql(3),
            "INSERT IGNORE INTO `weir_records` (`payload`) VALUES (?), (?), (?)"
        );
    }

    #[test]
    fn build_sql_plain_mode() {
        let mut c = cfg();
        c.insert_mode = InsertMode::Plain;
        let sink = MySqlSink::new(c).unwrap();
        assert_eq!(
            sink.build_insert_sql(2),
            "INSERT INTO `weir_records` (`payload`) VALUES (?), (?)"
        );
    }

    #[test]
    fn build_sql_uses_configured_table_and_column() {
        let mut c = cfg();
        c.table = "events".to_string();
        c.column = "blob_data".to_string();
        let sink = MySqlSink::new(c).unwrap();
        assert!(sink.build_insert_sql(1).contains("`events`"));
        assert!(sink.build_insert_sql(1).contains("`blob_data`"));
    }

    #[test]
    fn redact_password_replaces_secret() {
        let url = "mysql://alice:s3cret@db.example.com:3306/weir";
        let r = redact_password(url);
        assert!(!r.contains("s3cret"), "password leaked: {r}");
        assert!(r.contains("alice"));
        assert!(r.contains("db.example.com"));
    }

    #[test]
    fn redact_password_leaves_unauthenticated_urls_alone() {
        let url = "mysql://localhost:3306/weir";
        assert_eq!(redact_password(url), url);
    }

    #[test]
    fn debug_impl_does_not_leak_password() {
        let c = MySqlSinkConfig {
            url: "mysql://alice:topsecret@db.example.com:3306/weir".into(),
            ..cfg()
        };
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("topsecret"), "password leaked: {dbg}");
        assert!(dbg.contains("alice"), "user should still appear: {dbg}");
    }

    #[test]
    fn is_transient_server_code_table() {
        // Things we know need to retry.
        assert!(is_transient_server_code(1205)); // lock wait timeout
        assert!(is_transient_server_code(1213)); // deadlock
        assert!(is_transient_server_code(1290)); // read-only
        assert!(is_transient_server_code(1062)); // dup entry (Plain mode)

        // Things that must dead-letter — retrying won't help.
        assert!(!is_transient_server_code(1064)); // syntax error
        assert!(!is_transient_server_code(1146)); // no such table
        assert!(!is_transient_server_code(1054)); // bad field
        assert!(!is_transient_server_code(1045)); // access denied
        assert!(!is_transient_server_code(1044)); // db access denied
        assert!(!is_transient_server_code(1406)); // data too long for column
    }

    #[tokio::test]
    async fn empty_batch_is_a_noop() {
        // Builds a sink against a URL whose host is unreachable; if the
        // empty-batch fast path didn't exist, this would attempt to open
        // a connection and fail.
        let mut c = cfg();
        c.url = "mysql://x:y@127.0.0.1:1/weir".into();
        c.timeout = Duration::from_millis(100);
        let sink = MySqlSink::new(c).unwrap();
        let result = sink.commit(Vec::new()).await.unwrap();
        assert!(result.committed.is_empty());
        assert!(result.dead_lettered.is_empty());
    }

    #[tokio::test]
    async fn connect_refused_returns_transient_error() {
        let mut c = cfg();
        c.url = "mysql://x:y@127.0.0.1:1/weir".into();
        c.timeout = Duration::from_secs(2);
        let sink = MySqlSink::new(c).unwrap();
        let err = sink.commit(vec![b"hello".to_vec()]).await.unwrap_err();
        assert!(
            err.is_transient(),
            "connect-refused must be transient, got: {err}"
        );
    }

    #[tokio::test]
    async fn very_short_timeout_returns_timeout_error() {
        let mut c = cfg();
        // Pick a non-routable IP so connect hangs rather than refuses quickly.
        c.url = "mysql://x:y@10.255.255.1:3306/weir".into();
        c.timeout = Duration::from_millis(50);
        let sink = MySqlSink::new(c).unwrap();
        let err = sink.commit(vec![b"hello".to_vec()]).await.unwrap_err();
        assert!(matches!(err, MySqlSinkError::Timeout), "got: {err}");
        assert!(err.is_transient());
    }

    #[test]
    fn permanent_error_is_not_transient() {
        let e = MySqlSinkError::Permanent("syntax".into());
        assert!(!e.is_transient());
    }

    #[test]
    fn transient_error_is_transient() {
        let e = MySqlSinkError::Transient("io".into());
        assert!(e.is_transient());
    }
}
