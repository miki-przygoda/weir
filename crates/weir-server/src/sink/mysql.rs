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

use super::sql_common::{self, SqlSinkError};
use super::{CommitResult, Sink, SinkError, SinkHealth};

/// Static driver tag used by [`SqlSinkError`] variants emitted from this
/// module. Keeps `"mysql sink ..."` consistent in log lines without
/// repeating the literal at every classify-site.
const DRIVER: &str = "mysql";

/// MySQL's identifier length limit (per its docs — quoted identifiers up
/// to 64 characters). Drives the `max_len` argument to
/// [`sql_common::validate_identifier`].
const IDENTIFIER_MAX_LEN: usize = 64;

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
            .field("url", &sql_common::redact_password(&self.url))
            .field("table", &self.table)
            .field("column", &self.column)
            .field("insert_mode", &self.insert_mode)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .finish()
    }
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
        sql_common::validate_identifier("table", &config.table, IDENTIFIER_MAX_LEN)
            .map_err(MySqlSinkBuildError::from)?;
        sql_common::validate_identifier("column", &config.column, IDENTIFIER_MAX_LEN)
            .map_err(MySqlSinkBuildError::from)?;
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

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors that can occur during `MySqlSink::new()`. All build-time —
/// permanent by definition, no `SinkError` impl needed.
#[derive(Debug, thiserror::Error)]
pub enum MySqlSinkBuildError {
    #[error("mysql sink url is empty")]
    EmptyUrl,
    #[error("mysql sink url invalid: {0}")]
    InvalidUrl(String),
    #[error(
        "mysql sink {field} {value:?} is not a valid identifier \
         (must match [A-Za-z_][A-Za-z0-9_]{{0,63}})"
    )]
    InvalidIdentifier { field: String, value: String },
}

impl From<sql_common::InvalidIdentifier> for MySqlSinkBuildError {
    fn from(e: sql_common::InvalidIdentifier) -> Self {
        MySqlSinkBuildError::InvalidIdentifier {
            field: e.field,
            value: e.value,
        }
    }
}

/// Classify a `mysql_async::Error` into the drain-facing transient /
/// permanent buckets. Public for unit testing.
pub(crate) fn classify(err: mysql_async::Error) -> SqlSinkError {
    use mysql_async::Error as E;
    match err {
        // Network / pool / driver-level IO: always transient.
        E::Io(e) => SqlSinkError::transient(DRIVER, format!("io: {e}")),
        E::Driver(e) => SqlSinkError::transient(DRIVER, format!("driver: {e}")),
        // URL parse errors are permanent (config error).
        E::Url(e) => SqlSinkError::permanent(DRIVER, format!("url: {e}")),
        // Server-reported errors. Code 0 should not happen but if it does,
        // treat as permanent so we don't loop forever.
        E::Server(srv) => {
            let code = srv.code;
            let msg = srv.message.clone();
            if is_transient_server_code(code) {
                SqlSinkError::transient(DRIVER, format!("server {code}: {msg}"))
            } else {
                SqlSinkError::permanent(DRIVER, format!("server {code}: {msg}"))
            }
        }
        E::Other(e) => SqlSinkError::permanent(DRIVER, format!("other: {e}")),
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
    type Error = SqlSinkError;

    async fn commit(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, SqlSinkError> {
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
            Err(_elapsed) => Err(SqlSinkError::timeout(DRIVER)),
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

    // The full identifier-validation matrix lives in
    // `sql_common::tests` (driver-agnostic). The two tests below verify
    // the per-sink WIRING — that `MySqlSink::new` calls
    // `sql_common::validate_identifier` with `IDENTIFIER_MAX_LEN = 64`
    // and that failures map to `MySqlSinkBuildError::InvalidIdentifier`
    // via the `From` impl.

    #[test]
    fn invalid_identifier_maps_to_mysql_build_error() {
        let mut c = cfg();
        c.table = "1bad".to_string();
        let err = MySqlSink::new(c).unwrap_err();
        match err {
            MySqlSinkBuildError::InvalidIdentifier { field, value } => {
                assert_eq!(field, "table");
                assert_eq!(value, "1bad");
            }
            other => panic!("expected InvalidIdentifier, got {other:?}"),
        }
    }

    #[test]
    fn identifier_length_limit_is_64_for_mysql() {
        let mut c = cfg();
        // 64 chars accepted (MySQL identifier limit).
        c.table = "a".repeat(64);
        assert!(MySqlSink::new(c).is_ok());

        let mut c = cfg();
        // 65 chars rejected.
        c.table = "a".repeat(65);
        assert!(matches!(
            MySqlSink::new(c).unwrap_err(),
            MySqlSinkBuildError::InvalidIdentifier { .. }
        ));
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

    // Direct `redact_password` coverage lives in `sql_common::tests`.
    // We still verify here that `MySqlSinkConfig::Debug` actually
    // CALLS the redactor — i.e. the wiring is correct.

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
        assert!(matches!(err, SqlSinkError::Timeout { .. }), "got: {err}");
        assert!(err.is_transient());
        // Driver context survives into the Display string.
        assert!(format!("{err}").contains("mysql"), "got: {err}");
    }
}
