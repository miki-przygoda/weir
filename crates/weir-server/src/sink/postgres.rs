//! PostgreSQL sink: writes a whole batch with one multi-row `INSERT`
//! statement.
//!
//! Mirror of [`super::mysql`] with PG-flavoured idioms: `ON CONFLICT DO
//! NOTHING` in place of `INSERT IGNORE`, `$N` positional parameters in
//! place of `?`, double-quoted identifiers in place of backticks, SQLSTATE
//! `23505` (`unique_violation`) and `40P01` (`deadlock_detected`) in the
//! transient-codes list.
//!
//! # The IOPS-compression story
//!
//! N records arrive at the drain → one
//! `INSERT INTO t (col) VALUES ($1), ($2), … [ON CONFLICT DO NOTHING]`
//! → one network round-trip → one server-side commit. Same headline as
//! the MySQL sink.
//!
//! # Schema contract
//!
//! The sink does **not** auto-create the target table. The operator
//! provisions a table with a column wide enough to hold the payload bytes
//! (typically `BYTEA`). For the default `OnConflictDoNothing` insert mode
//! the table should also have a `UNIQUE` constraint (typically on a
//! payload hash column) so crash-recovery retries are idempotent without
//! consumer-side dedup. See
//! [`docs/operations/configuration.md`](../../../../docs/operations/configuration.md)
//! for a reference schema.
//!
//! # At-least-once and idempotency
//!
//! Same contract as the MySQL sink: the drain may re-call `commit()` for
//! a segment that was partially committed pre-crash. The default
//! `OnConflictDoNothing` mode silently drops duplicates so re-commits are
//! idempotent. `Plain` mode opts out — duplicate-key errors surface as
//! SQLSTATE `23505` and are classified as transient so the drain retries
//! the segment after backoff (in case the duplicate is from a stale
//! concurrent writer rather than a re-commit).
//!
//! # Error classification
//!
//! - Connection / pool / IO failures → transient (drain retries).
//! - SQLSTATE `40P01` (`deadlock_detected`),
//!   `55P03` (`lock_not_available`),
//!   `57014` (`query_canceled`),
//!   `57P01` (`admin_shutdown`),
//!   `57P02` (`crash_shutdown`),
//!   `57P03` (`cannot_connect_now`) → transient.
//! - `23505` (`unique_violation`) — never seen under
//!   `OnConflictDoNothing`; under `Plain` mode treated as transient so
//!   the drain retries the segment after backoff, in case the duplicate
//!   is from a stale concurrent writer rather than a re-commit.
//! - All other server errors (syntax, missing table, missing column,
//!   access denied, …) → permanent. The whole batch is dead-lettered
//!   with the server-supplied error message so an operator can debug.
//!
//! # Authentication
//!
//! Credentials are taken from the connection URL
//! (`postgres://user:pass@host:5432/db`). The URL is read from
//! `WEIR_SINK_URL` at startup; never sourced from the TOML config.
//! `Debug` impls redact the password before logging.

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config as PgConfig, NoTls, error::SqlState};
use tracing::{debug, warn};
use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

// ── Configuration ─────────────────────────────────────────────────────────────

/// How to phrase the INSERT statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertMode {
    /// `INSERT INTO … ON CONFLICT DO NOTHING` — duplicate-key errors are
    /// silently dropped by the server. Recommended default: pair with a
    /// `UNIQUE` constraint on a payload hash column so crash-recovery
    /// retries are idempotent without consumer-side dedup.
    OnConflictDoNothing,
    /// `INSERT INTO …` — duplicates surface as SQLSTATE `23505` and are
    /// classified as transient (drain retries).
    Plain,
}

/// Configuration for `PostgresSink`.
///
/// The connection URL carries credentials in plain text. The `Debug` impl
/// redacts the password so a `?config` log line cannot leak it.
#[derive(Clone)]
pub struct PostgresSinkConfig {
    /// `postgres://user:password@host:5432/database`. Required.
    pub url: String,
    /// Target table. Must match `[A-Za-z_][A-Za-z0-9_]*`, length ≤ 63
    /// (PG identifier limit is 63 chars — `NAMEDATALEN - 1`).
    pub table: String,
    /// Target column. Same identifier rules as `table`.
    pub column: String,
    /// How to phrase the INSERT statement.
    pub insert_mode: InsertMode,
    /// Maximum records per `commit()` call. Larger batches reduce IOPS at
    /// the cost of statement length; PG accepts very large multi-row
    /// inserts but the wire protocol's parameter ceiling is 65 535
    /// per statement, so this MUST stay below that.
    pub max_batch_size: usize,
    /// Per-query timeout. Applied to the commit future via a
    /// `tokio::time::timeout` wrapper at the call site.
    pub timeout: Duration,
}

impl std::fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSinkConfig")
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

/// Postgres sink. The pool is cheap to clone (`Arc` inside).
pub struct PostgresSink {
    config: PostgresSinkConfig,
    pool: Arc<Pool>,
}

impl std::fmt::Debug for PostgresSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSink")
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresSink {
    /// Build a new Postgres sink. Validates the URL shape, table and column
    /// identifiers, and constructs a connection pool. The first connection
    /// is lazy — failures show up on the first `commit()`, not here.
    pub fn new(config: PostgresSinkConfig) -> Result<Self, PostgresSinkBuildError> {
        if config.url.is_empty() {
            return Err(PostgresSinkBuildError::EmptyUrl);
        }
        validate_identifier("table", &config.table)?;
        validate_identifier("column", &config.column)?;
        let pg_config: PgConfig = config
            .url
            .parse()
            .map_err(|e: tokio_postgres::Error| PostgresSinkBuildError::InvalidUrl(e.to_string()))?;
        let mgr = Manager::from_config(
            pg_config,
            // No TLS for the initial sink — see module-level docs on the
            // deliberate trade-off.
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            // Conservative pool: the drain calls commit() sequentially so
            // one connection covers the steady state. Two extras leave
            // headroom for the periodic health() probe (which may run
            // while a commit is in flight) and for re-establishing a
            // connection after a transient failure without waiting on
            // the one in-flight statement.
            .max_size(4)
            .build()
            .map_err(|e| PostgresSinkBuildError::PoolBuild(e.to_string()))?;
        Ok(Self {
            config,
            pool: Arc::new(pool),
        })
    }

    /// Build the multi-row INSERT statement for a batch of size `n`. Public
    /// for unit testing; do not call from the hot path (commit() already
    /// inlines this).
    pub(crate) fn build_insert_sql(&self, n: usize) -> String {
        // PG uses $1, $2, … positional parameters. Each row in the
        // multi-row VALUES list takes one parameter (the BYTEA payload).
        let placeholders: String = (1..=n)
            .map(|i| format!("(${i})"))
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = match self.config.insert_mode {
            InsertMode::OnConflictDoNothing => " ON CONFLICT DO NOTHING",
            InsertMode::Plain => "",
        };
        // Identifiers are pre-validated to `[A-Za-z_][A-Za-z0-9_]{0,62}`, so
        // there is no SQL injection vector through `table` or `column`.
        format!(
            "INSERT INTO \"{}\" (\"{}\") VALUES {placeholders}{suffix}",
            self.config.table, self.config.column
        )
    }
}

/// Postgres identifiers used in the sink must be pure ASCII alphanumerics
/// plus underscore, starting with a letter or underscore, ≤ 63 chars (PG's
/// `NAMEDATALEN - 1` limit). Strict subset of what PG allows in
/// double-quoted form — same rationale as `mysql::validate_identifier`:
/// validating here means the sink can build SQL strings via `format!` with
/// no escaping logic to get wrong.
fn validate_identifier(field: &str, value: &str) -> Result<(), PostgresSinkBuildError> {
    if value.is_empty() || value.len() > 63 {
        return Err(PostgresSinkBuildError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
        });
    }
    let mut chars = value.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(PostgresSinkBuildError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
        });
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(PostgresSinkBuildError::InvalidIdentifier {
                field: field.to_string(),
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors that can occur during `PostgresSink::new()`. All build-time —
/// permanent by definition, no `SinkError` impl needed.
#[derive(Debug, thiserror::Error)]
pub enum PostgresSinkBuildError {
    #[error("postgres sink url is empty")]
    EmptyUrl,
    #[error("postgres sink url invalid: {0}")]
    InvalidUrl(String),
    #[error(
        "postgres sink {field} {value:?} is not a valid identifier \
         (must match [A-Za-z_][A-Za-z0-9_]{{0,62}})"
    )]
    InvalidIdentifier { field: String, value: String },
    #[error("postgres sink pool build failed: {0}")]
    PoolBuild(String),
}

/// Errors returned by `PostgresSink::commit`.
#[derive(Debug, thiserror::Error)]
pub enum PostgresSinkError {
    /// Connection / pool / IO failures and explicit-transient server codes.
    /// Drain retries the whole segment.
    #[error("postgres sink transient: {0}")]
    Transient(String),
    /// Permanent error — bad schema, bad auth, syntax error, payload too
    /// large for the column. Drain dead-letters the batch with this string
    /// as the reason.
    #[error("postgres sink permanent: {0}")]
    Permanent(String),
    /// Per-query timeout — `sink_timeout_secs` exceeded. Transient: the
    /// server is probably overloaded; backoff and retry.
    #[error("postgres sink timeout")]
    Timeout,
}

impl SinkError for PostgresSinkError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            PostgresSinkError::Transient(_) | PostgresSinkError::Timeout
        )
    }
}

/// Classify a `tokio_postgres::Error` into the drain-facing transient /
/// permanent buckets. Public for unit testing.
pub(crate) fn classify(err: tokio_postgres::Error) -> PostgresSinkError {
    // Server-reported errors carry a SQLSTATE we can inspect; everything
    // else is treated as connection / IO and therefore transient. PG's
    // error model is much flatter than mysql_async's — we lean on the
    // SQLSTATE for the categorisation.
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code();
        let msg = db_err.message().to_string();
        if is_transient_sqlstate(code) {
            PostgresSinkError::Transient(format!("server {}: {msg}", code.code()))
        } else {
            PostgresSinkError::Permanent(format!("server {}: {msg}", code.code()))
        }
    } else {
        // No DbError ⇒ wire/IO/protocol layer. Transient by default; the
        // pool will reconnect on the next commit.
        PostgresSinkError::Transient(format!("io: {err}"))
    }
}

/// Returns true for SQLSTATE codes the drain should retry rather than
/// dead-letter. Conservatively short list — adding a code here risks an
/// infinite retry loop, removing one risks dead-lettering a recoverable
/// situation.
pub(crate) fn is_transient_sqlstate(state: &SqlState) -> bool {
    // Compare via the 5-char code — using the constants where possible
    // makes typos compile-fail.
    state == &SqlState::T_R_DEADLOCK_DETECTED         // 40P01
        || state == &SqlState::LOCK_NOT_AVAILABLE     // 55P03
        || state == &SqlState::QUERY_CANCELED         // 57014
        || state == &SqlState::ADMIN_SHUTDOWN         // 57P01
        || state == &SqlState::CRASH_SHUTDOWN         // 57P02
        || state == &SqlState::CANNOT_CONNECT_NOW     // 57P03
        || state == &SqlState::UNIQUE_VIOLATION       // 23505 — see module docs (Plain mode only)
}

// ── Sink trait impl ───────────────────────────────────────────────────────────

impl Sink for PostgresSink {
    type Record = Payload;
    type Error = PostgresSinkError;

    async fn commit(
        &self,
        batch: Vec<Payload>,
    ) -> Result<CommitResult<Payload>, PostgresSinkError> {
        if batch.is_empty() {
            return Ok(CommitResult {
                committed: Vec::new(),
                dead_lettered: Vec::new(),
            });
        }

        let sql = self.build_insert_sql(batch.len());

        let commit_fut = async {
            let client = self.pool.get().await.map_err(|e| {
                // Pool errors (no available connection, timeout fetching
                // one, backend unreachable) are categorically transient.
                PostgresSinkError::Transient(format!("pool: {e}"))
            })?;
            // tokio-postgres takes parameters as `&[&(dyn ToSql + Sync)]`.
            // We need a Vec of `&[u8]` first, then a Vec of references to
            // those — both have to outlive the `execute` call.
            let slices: Vec<&[u8]> = batch.iter().map(|p| p.as_slice()).collect();
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = slices
                .iter()
                .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            client
                .execute(&sql, &params)
                .await
                .map(|_rows| ())
                .map_err(classify)
        };

        match tokio::time::timeout(self.config.timeout, commit_fut).await {
            Ok(Ok(())) => {
                debug!(
                    records = batch.len(),
                    "postgres sink committed batch as single INSERT"
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
                        "postgres sink permanently rejected batch; dead-lettering"
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
            Err(_elapsed) => Err(PostgresSinkError::Timeout),
        }
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        let probe = async {
            let client = self.pool.get().await.map_err(|e| format!("pool: {e}"))?;
            client
                .execute("SELECT 1", &[])
                .await
                .map(|_| ())
                .map_err(|e| format!("query: {e}"))
        };
        match tokio::time::timeout(self.config.timeout, probe).await {
            Ok(Ok(())) => SinkHealth::Healthy,
            Ok(Err(e)) => SinkHealth::Down(format!("postgres probe failed: {e}")),
            Err(_) => SinkHealth::Down("postgres probe timed out".into()),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PostgresSinkConfig {
        PostgresSinkConfig {
            url: "postgres://user:pw@127.0.0.1:5432/db".to_string(),
            table: "weir_records".to_string(),
            column: "payload".to_string(),
            insert_mode: InsertMode::OnConflictDoNothing,
            max_batch_size: 1000,
            timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn empty_url_rejected_at_build() {
        let mut c = cfg();
        c.url = String::new();
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::EmptyUrl
        ));
    }

    #[test]
    fn invalid_url_rejected_at_build() {
        let mut c = cfg();
        c.url = "not-a-url".to_string();
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::InvalidUrl(_)
        ));
    }

    #[test]
    fn invalid_table_identifier_rejected() {
        let mut c = cfg();
        c.table = "weir records".to_string(); // space is illegal
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::InvalidIdentifier { .. }
        ));
    }

    #[test]
    fn invalid_column_identifier_rejected() {
        let mut c = cfg();
        c.column = "1payload".to_string(); // can't start with a digit
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::InvalidIdentifier { .. }
        ));
    }

    #[test]
    fn identifier_at_63_chars_accepted() {
        let mut c = cfg();
        c.table = "a".repeat(63);
        assert!(PostgresSink::new(c).is_ok());
    }

    #[test]
    fn identifier_at_64_chars_rejected() {
        let mut c = cfg();
        c.table = "a".repeat(64);
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::InvalidIdentifier { .. }
        ));
    }

    #[test]
    fn build_insert_sql_with_on_conflict() {
        let sink = PostgresSink::new(cfg()).unwrap();
        let sql = sink.build_insert_sql(3);
        assert_eq!(
            sql,
            "INSERT INTO \"weir_records\" (\"payload\") VALUES ($1), ($2), ($3) ON CONFLICT DO NOTHING"
        );
    }

    #[test]
    fn build_insert_sql_plain() {
        let mut c = cfg();
        c.insert_mode = InsertMode::Plain;
        let sink = PostgresSink::new(c).unwrap();
        let sql = sink.build_insert_sql(2);
        assert_eq!(
            sql,
            "INSERT INTO \"weir_records\" (\"payload\") VALUES ($1), ($2)"
        );
    }

    #[test]
    fn build_insert_sql_single_row() {
        let sink = PostgresSink::new(cfg()).unwrap();
        let sql = sink.build_insert_sql(1);
        assert_eq!(
            sql,
            "INSERT INTO \"weir_records\" (\"payload\") VALUES ($1) ON CONFLICT DO NOTHING"
        );
    }

    #[test]
    fn debug_redacts_password() {
        let c = cfg();
        let s = format!("{c:?}");
        assert!(!s.contains("pw"), "password leaked: {s}");
        assert!(s.contains("<redacted>"), "redaction marker missing: {s}");
    }

    #[test]
    fn debug_redacts_password_with_special_chars() {
        let mut c = cfg();
        c.url = "postgres://user:p%40ss%21@host/db".to_string();
        let s = format!("{c:?}");
        assert!(!s.contains("p%40ss%21"), "password leaked: {s}");
    }

    #[test]
    fn debug_handles_url_without_credentials() {
        let mut c = cfg();
        c.url = "postgres://host/db".to_string();
        let s = format!("{c:?}");
        // No credentials to redact; URL should appear unchanged.
        assert!(s.contains("postgres://host/db"));
    }

    #[test]
    fn transient_sqlstates_are_transient() {
        assert!(is_transient_sqlstate(&SqlState::T_R_DEADLOCK_DETECTED));
        assert!(is_transient_sqlstate(&SqlState::LOCK_NOT_AVAILABLE));
        assert!(is_transient_sqlstate(&SqlState::QUERY_CANCELED));
        assert!(is_transient_sqlstate(&SqlState::ADMIN_SHUTDOWN));
        assert!(is_transient_sqlstate(&SqlState::CANNOT_CONNECT_NOW));
        // 23505 is in the transient list because under Plain mode the
        // drain should retry — see module docs.
        assert!(is_transient_sqlstate(&SqlState::UNIQUE_VIOLATION));
    }

    #[test]
    fn permanent_sqlstates_are_permanent() {
        assert!(!is_transient_sqlstate(&SqlState::UNDEFINED_TABLE));
        assert!(!is_transient_sqlstate(&SqlState::UNDEFINED_COLUMN));
        assert!(!is_transient_sqlstate(&SqlState::SYNTAX_ERROR));
        assert!(!is_transient_sqlstate(&SqlState::INVALID_PASSWORD));
    }

    #[test]
    fn postgres_sink_error_transient_classification() {
        let t = PostgresSinkError::Transient("io".to_string());
        let p = PostgresSinkError::Permanent("bad schema".to_string());
        let to = PostgresSinkError::Timeout;
        assert!(t.is_transient());
        assert!(to.is_transient());
        assert!(!p.is_transient());
    }
}
