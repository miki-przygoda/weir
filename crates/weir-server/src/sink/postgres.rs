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
//! the table also needs a `UNIQUE` constraint so crash-recovery retries are
//! idempotent without consumer-side dedup — see `id_column` below for what
//! that constraint should be on. See
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
//! ## What the UNIQUE constraint should be on
//!
//! With `id_column` unset the sink writes payload bytes only, so the
//! constraint has to be on the payload (or a generated hash of it). That is
//! **content** identity, and it is wrong in both directions:
//!
//! - It **loses**. Two genuinely distinct records that happen to carry
//!   identical bytes — a metering "one unit consumed" event, a heartbeat, any
//!   fixed-shape event — collide, and `ON CONFLICT DO NOTHING` discards the
//!   second. weir acked it, wrote it to disk and delivered it; the operator's
//!   schema drops it.
//! - It is not what a replay needs anyway. The re-delivered record is
//!   recognised only because its bytes repeat, which is the same reason a
//!   *new* record is wrongly discarded.
//!
//! Setting `id_column` makes the sink write each record's
//! [`RecordId`](weir_sink_sdk::RecordId) alongside the payload. That is the
//! record's WAB coordinate — its segment plus its absolute index within that
//! segment — hashed with its bytes, so it is unique across records with
//! identical bytes **and** unchanged by a replay, including a replay that
//! re-splits the segment because `sink_max_batch_size` changed. Put the
//! `UNIQUE` constraint on that column and `ON CONFLICT DO NOTHING` becomes a
//! true idempotency filter rather than a deduplicator of coincidences.
//!
//! Deliberately opt-in: turning it on requires a column the existing table
//! does not have, so it cannot be a default without breaking every deployed
//! schema.
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
//! (`postgres://user:pass@host:5432/db`). Prefer setting the URL via the
//! `WEIR_SINK_URL` environment variable so the embedded password never lands
//! on disk. The URL can also be set via `--sink-url` or the TOML config
//! (`sink_url`), but a credential-bearing URL written to TOML is stored on
//! disk in plaintext. `Debug` impls redact the password before logging.

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config as PgConfig, NoTls, config::SslMode, error::SqlState};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, info, warn};

use super::sql_common::{self, SqlSinkError};
use super::{CommitResult, Sink, SinkBatch, SinkError, SinkHealth};

/// Static driver tag used by [`SqlSinkError`] variants emitted from this
/// module. Counterpart to `mysql::DRIVER` — keeps `"postgres sink ..."`
/// consistent in log lines.
const DRIVER: &str = "postgres";

/// Postgres's identifier length limit (`NAMEDATALEN - 1`).
/// Drives the `max_len` argument to
/// [`sql_common::validate_identifier`].
const IDENTIFIER_MAX_LEN: usize = 63;

// ── Configuration ─────────────────────────────────────────────────────────────

/// How to phrase the INSERT statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertMode {
    /// `INSERT INTO … ON CONFLICT DO NOTHING` — duplicate-key errors are
    /// silently dropped by the server. Recommended default: pair with a
    /// `UNIQUE` constraint on the `id_column` so crash-recovery retries are
    /// idempotent without consumer-side dedup. A `UNIQUE` on a payload hash
    /// instead makes this discard distinct records that share bytes — see the
    /// module docs.
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
    /// Optional column receiving each record's
    /// [`RecordId`](weir_sink_sdk::RecordId) as 64 lower-hex characters — the
    /// per-record idempotency key. `None` (the default) keeps the single-column
    /// INSERT weir has always written. See the module docs for why a `UNIQUE`
    /// on this beats a `UNIQUE` on a payload hash.
    pub id_column: Option<String>,
    /// How to phrase the INSERT statement.
    pub insert_mode: InsertMode,
    /// Maximum records per `commit()` call. Larger batches reduce IOPS at
    /// the cost of statement length; PG accepts very large multi-row
    /// inserts but the wire protocol's parameter ceiling is 65 535
    /// per statement, so this MUST stay below that.
    ///
    /// With `id_column` set each row takes **two** parameters, halving the
    /// headroom. `sink_max_batch_size` is validated to `1..=10_000` in
    /// `config::mod`, so the worst case is 20 000 — still well inside the
    /// ceiling. If that cap is ever raised, this is the constraint it has to
    /// be raised against.
    pub max_batch_size: usize,
    /// Per-query timeout. Applied to the commit future via a
    /// `tokio::time::timeout` wrapper at the call site.
    pub timeout: Duration,
}

impl std::fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSinkConfig")
            .field("url", &sql_common::redact_password(&self.url))
            .field("table", &self.table)
            .field("column", &self.column)
            .field("id_column", &self.id_column)
            .field("insert_mode", &self.insert_mode)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .finish()
    }
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
        sql_common::validate_identifier("table", &config.table, IDENTIFIER_MAX_LEN)
            .map_err(PostgresSinkBuildError::from)?;
        sql_common::validate_identifier("column", &config.column, IDENTIFIER_MAX_LEN)
            .map_err(PostgresSinkBuildError::from)?;
        if let Some(id_column) = &config.id_column {
            sql_common::validate_identifier("id_column", id_column, IDENTIFIER_MAX_LEN)
                .map_err(PostgresSinkBuildError::from)?;
            // Rejected at build, not at first commit: the same name twice
            // produces `INSERT INTO t (payload, payload) VALUES ($1, $2)`,
            // which PG rejects as 42701 — a *permanent* error, so the drain
            // would dead-letter every batch it ever received. By the time a
            // record reaches the sink it is already acked, so a config mistake
            // has to stop the daemon starting.
            if id_column == &config.column {
                return Err(PostgresSinkBuildError::IdColumnIsPayloadColumn {
                    name: id_column.clone(),
                });
            }
        }
        let pg_config: PgConfig = config.url.parse().map_err(|_e: tokio_postgres::Error| {
            // Do not surface the driver's error verbatim — it can embed the
            // connection URL including the user:password@ component, leaking the
            // password to stderr/logs. Report the redacted URL instead (the same
            // redaction the Debug impl uses).
            PostgresSinkBuildError::InvalidUrl(sql_common::redact_password(&config.url))
        })?;

        // TLS opt-in via `?sslmode=require` in the URL. The other two
        // tokio-postgres SslMode values keep the cleartext path so we
        // don't surprise operators on upgrade:
        //
        //   - `disable` (explicit): NoTls. Operator said no.
        //   - `prefer`  (the tokio-postgres default if no sslmode set):
        //                NoTls. Strictly speaking PG's "prefer" means
        //                "try TLS, fall back" — implementing that in a
        //                single-connector design is awkward, and
        //                quietly enabling TLS on what's currently a
        //                cleartext deployment is a worse failure mode
        //                than the opposite. Operators who actually
        //                want TLS write `?sslmode=require` and get
        //                exactly that.
        //   - `require` (explicit): MakeRustlsConnect with webpki roots.
        //                Connection refuses to fall back to cleartext.
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = match pg_config.get_ssl_mode() {
            SslMode::Require => {
                info!(
                    table = %config.table,
                    "postgres sink: TLS required via sslmode=require (webpki roots, rustls)"
                );
                Manager::from_config(pg_config, build_tls_connector(), mgr_config)
            }
            // `Prefer` is the silent default; `Disable` is explicit. Both
            // get NoTls — see comment above.
            _ => Manager::from_config(pg_config, NoTls, mgr_config),
        };
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
        let suffix = match self.config.insert_mode {
            InsertMode::OnConflictDoNothing => " ON CONFLICT DO NOTHING",
            InsertMode::Plain => "",
        };
        // Identifiers are pre-validated to `[A-Za-z_][A-Za-z0-9_]{0,62}`, so
        // there is no SQL injection vector through `table`, `column` or
        // `id_column`.
        match &self.config.id_column {
            // PG uses $1, $2, … positional parameters. One parameter per row:
            // the BYTEA payload.
            None => {
                let placeholders: String = (1..=n)
                    .map(|i| format!("(${i})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO \"{}\" (\"{}\") VALUES {placeholders}{suffix}",
                    self.config.table, self.config.column
                )
            }
            // Two parameters per row, id first, so `commit` binds
            // (id, payload) pairs in the same order the column list names
            // them. The id leads because that is the column the operator's
            // UNIQUE constraint is on, and a reader of a slow-query log
            // should see the key before the blob.
            Some(id_column) => {
                let placeholders: String = (0..n)
                    .map(|row| format!("(${}, ${})", row * 2 + 1, row * 2 + 2))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO \"{}\" (\"{}\", \"{}\") VALUES {placeholders}{suffix}",
                    self.config.table, id_column, self.config.column
                )
            }
        }
    }
}

/// One bound parameter, borrowed from the batch. Named rather than left as a
/// `&dyn ToSql` so the binding order is unit-testable: a real Postgres server
/// is the only other way to observe it, and getting `(id, payload)` the wrong
/// way round would write each record's key into the payload column and each
/// payload into the `UNIQUE` key — which the sink reports as success.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BoundParam<'a> {
    /// A `RecordId` in lower hex — the `id_column` value.
    Id(&'a String),
    /// The record's bytes.
    Payload(&'a [u8]),
}

/// Positional parameters for the statement `build_insert_sql` produced, in
/// `$1, $2, …` order. Borrows throughout: `Payload` is ref-counted `Bytes` and
/// a batch can be 10 000 records, so copying here to make it owned would be a
/// real cost on the commit path.
///
/// `id_hex` is `None` when no `id_column` is configured, and otherwise carries
/// exactly one hex `RecordId` per record (enforced upstream by
/// [`sql_common::per_record_id_hex`]).
fn build_params<'a>(
    batch: &'a [weir_core::Payload],
    id_hex: Option<&'a [String]>,
) -> Vec<BoundParam<'a>> {
    match id_hex {
        None => batch
            .iter()
            .map(|p| BoundParam::Payload(p.as_ref()))
            .collect(),
        // Zipping rather than indexing keeps the two sequences in step by
        // construction, and a length mismatch truncates rather than panicking
        // — which cannot happen, because per_record_id_hex already rejected it.
        Some(ids) => ids
            .iter()
            .zip(batch.iter())
            .flat_map(|(id, payload)| [BoundParam::Id(id), BoundParam::Payload(payload.as_ref())])
            .collect(),
    }
}

// ── TLS connector ─────────────────────────────────────────────────────────────

/// Builds the rustls `ClientConfig` used by `Manager::from_config` when
/// the URL opts in via `?sslmode=require`. Uses webpki-roots (bundled
/// Mozilla CA store, no host system-CA dependency) and explicitly
/// selects ring as the crypto provider via `builder_with_provider`.
///
/// Naming the provider explicitly (rather than the default `builder()`)
/// avoids rustls's "could not determine CryptoProvider" auto-detect path
/// entirely, and keeps this code robust even if a future dependency
/// reintroduces a second provider. It also never touches the
/// process-global default, so this is safe to call multiple times and
/// never races with other rustls users in the same binary.
///
/// Called once per `PostgresSink::new` call (i.e. once at daemon
/// startup), so memoisation would buy nothing.
fn build_tls_connector() -> MakeRustlsConnect {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let client_config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring default provider supports the safe TLS versions")
    .with_root_certificates(root_store)
    .with_no_client_auth();
    MakeRustlsConnect::new(client_config)
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
    #[error(
        "postgres sink id column {name:?} is also the payload column; \
         they must be different columns"
    )]
    IdColumnIsPayloadColumn { name: String },
    #[error("postgres sink pool build failed: {0}")]
    PoolBuild(String),
}

impl From<sql_common::InvalidIdentifier> for PostgresSinkBuildError {
    fn from(e: sql_common::InvalidIdentifier) -> Self {
        PostgresSinkBuildError::InvalidIdentifier {
            field: e.field,
            value: e.value,
        }
    }
}

/// Classify a `tokio_postgres::Error` into the drain-facing transient /
/// permanent buckets. Public for unit testing.
pub(crate) fn classify(err: tokio_postgres::Error) -> SqlSinkError {
    // Server-reported errors carry a SQLSTATE we can inspect; everything
    // else is treated as connection / IO and therefore transient. PG's
    // error model is much flatter than mysql_async's — we lean on the
    // SQLSTATE for the categorisation.
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code();
        let msg = db_err.message().to_string();
        if is_transient_sqlstate(code) {
            SqlSinkError::transient(DRIVER, format!("server {}: {msg}", code.code()))
        } else {
            SqlSinkError::permanent(DRIVER, format!("server {}: {msg}", code.code()))
        }
    } else {
        // No DbError ⇒ wire/IO/protocol layer. Transient by default; the
        // pool will reconnect on the next commit.
        SqlSinkError::transient(DRIVER, format!("io: {err}"))
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
        || state == &SqlState::UNIQUE_VIOLATION // 23505 — see module docs (Plain mode only)
}

// ── Sink trait impl ───────────────────────────────────────────────────────────

impl Sink for PostgresSink {
    type Error = SqlSinkError;

    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, SqlSinkError> {
        let (batch, record_ids) = batch.into_records_with_ids();
        if batch.is_empty() {
            return Ok(CommitResult::new(Vec::new(), Vec::new()));
        }
        let id_hex = sql_common::per_record_id_hex(
            DRIVER,
            self.config.id_column.as_deref(),
            batch.len(),
            record_ids,
        )?;

        let sql = self.build_insert_sql(batch.len());

        let commit_fut = async {
            let client = self.pool.get().await.map_err(|e| {
                // Pool errors (no available connection, timeout fetching
                // one, backend unreachable) are categorically transient.
                SqlSinkError::transient(DRIVER, format!("pool: {e}"))
            })?;
            // tokio-postgres takes parameters as `&[&(dyn ToSql + Sync)]`.
            // We need a Vec of `&[u8]` first, then a Vec of references to
            // those — both have to outlive the `execute` call.
            let bound = build_params(&batch, id_hex.as_deref());
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = bound
                .iter()
                .map(|slot| match slot {
                    BoundParam::Id(hex) => *hex as &(dyn tokio_postgres::types::ToSql + Sync),
                    BoundParam::Payload(bytes) => {
                        bytes as &(dyn tokio_postgres::types::ToSql + Sync)
                    }
                })
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
                Ok(CommitResult::new(batch, Vec::new()))
            }
            Ok(Err(e)) => {
                if !e.is_transient() {
                    let reason = format!("{e}");
                    warn!(
                        records = batch.len(),
                        error = %reason,
                        "postgres sink permanently rejected batch; dead-lettering"
                    );
                    let dead_lettered = batch.into_iter().map(|r| (r, reason.clone())).collect();
                    Ok(CommitResult::new(Vec::new(), dead_lettered))
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
            id_column: None,
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
    fn invalid_url_error_redacts_password() {
        // A URL carrying a password but failing to parse: the build error must
        // report the REDACTED url, never the driver's verbatim string (which can
        // embed user:password@) — otherwise the password leaks to stderr/logs.
        let mut c = cfg();
        c.url = "postgres://user:supersecret@host:notaport/db".to_string();
        let err = PostgresSink::new(c).unwrap_err();
        assert!(
            matches!(err, PostgresSinkBuildError::InvalidUrl(_)),
            "expected InvalidUrl, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("<redacted>"), "url must be redacted: {msg}");
        assert!(
            !msg.contains("supersecret"),
            "password must not leak: {msg}"
        );
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

    // ── Per-record idempotency key (`id_column`) ─────────────────────────────

    /// Two parameters per row, numbered consecutively, with the id column
    /// first. The numbering is the part worth pinning: an off-by-one in the
    /// `$N` sequence is a runtime error PG reports as a *permanent* failure,
    /// which the drain then dead-letters an entire backlog over.
    #[test]
    fn build_insert_sql_with_id_column_numbers_two_placeholders_per_row() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = PostgresSink::new(c).unwrap();
        assert_eq!(
            sink.build_insert_sql(3),
            "INSERT INTO \"weir_records\" (\"record_id\", \"payload\") \
             VALUES ($1, $2), ($3, $4), ($5, $6) ON CONFLICT DO NOTHING"
        );
    }

    /// The historical shape must not move for anyone who leaves the knob
    /// unset — this is the whole reason it is opt-in. (`build_insert_sql_*`
    /// above already pin it; this states the intent alongside the new arm so a
    /// future editor sees both branches in one place.)
    #[test]
    fn build_insert_sql_without_id_column_keeps_one_placeholder_per_row() {
        let sink = PostgresSink::new(cfg()).unwrap();
        assert_eq!(
            sink.build_insert_sql(2),
            "INSERT INTO \"weir_records\" (\"payload\") VALUES ($1), ($2) ON CONFLICT DO NOTHING"
        );
    }

    /// The ids the drain would produce for a whole ten-record segment:
    /// `RecordId::for_record(segment, absolute_index, payload)`.
    fn segment_ids(payloads: &[weir_core::Payload]) -> Vec<weir_sink_sdk::RecordId> {
        payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                weir_sink_sdk::RecordId::for_record(
                    "shard_00/seg_00000001.wab.sealed",
                    i as u64 + 1,
                    payload,
                )
            })
            .collect()
    }

    /// The hex keys `commit` would bind for one sub-batch, through the sink's
    /// own extraction path rather than a re-derivation in the test.
    fn bound_hex(
        sink: &PostgresSink,
        payloads: &[weir_core::Payload],
        ids: &[weir_sink_sdk::RecordId],
    ) -> Vec<String> {
        let hex = sql_common::per_record_id_hex(
            DRIVER,
            sink.config.id_column.as_deref(),
            payloads.len(),
            Some(ids.to_vec()),
        )
        .unwrap();
        build_params(payloads, hex.as_deref())
            .into_iter()
            .filter_map(|slot| match slot {
                BoundParam::Id(hex) => Some(hex.clone()),
                BoundParam::Payload(_) => None,
            })
            .collect()
    }

    /// The column list says `("record_id", "payload")`, so the bound
    /// parameters must alternate id, payload — in that order. Getting it
    /// backwards writes each payload into the UNIQUE key column and each key
    /// into the payload column, and the sink reports success either way.
    #[test]
    fn bound_params_alternate_id_then_payload() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = PostgresSink::new(c).unwrap();

        let payloads = vec![
            weir_core::Payload::from(b"first".as_ref()),
            weir_core::Payload::from(b"second".as_ref()),
        ];
        let ids = segment_ids(&payloads);
        let hex = sql_common::per_record_id_hex(
            DRIVER,
            sink.config.id_column.as_deref(),
            payloads.len(),
            Some(ids.clone()),
        )
        .unwrap();
        let bound = build_params(&payloads, hex.as_deref());

        let first_hex = ids[0].to_hex();
        let second_hex = ids[1].to_hex();
        assert_eq!(
            bound,
            vec![
                BoundParam::Id(&first_hex),
                BoundParam::Payload(b"first"),
                BoundParam::Id(&second_hex),
                BoundParam::Payload(b"second"),
            ]
        );
    }

    /// **The guarantee, at the sink boundary.** The same ten records of one WAB
    /// segment, delivered as four sub-batches at `sink_max_batch_size = 3` and
    /// then as two at `7`, must present the same ten idempotency keys — so a
    /// `UNIQUE (record_id)` recognises the second delivery as a replay of the
    /// first and `ON CONFLICT DO NOTHING` drops it.
    ///
    /// This is what a `DedupToken` cannot do: it names the sub-batch, and the
    /// sub-batches are not the same across the two splits.
    #[test]
    fn the_bound_keys_do_not_move_when_the_batch_size_changes() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = PostgresSink::new(c).unwrap();

        let payloads: Vec<weir_core::Payload> = (1..=10)
            .map(|i| weir_core::Payload::copy_from_slice(format!("record-{i:02}").as_bytes()))
            .collect();
        let ids = segment_ids(&payloads);

        let keys_at = |chunk: usize| -> Vec<String> {
            payloads
                .chunks(chunk)
                .zip(ids.chunks(chunk))
                .flat_map(|(pc, ic)| bound_hex(&sink, pc, ic))
                .collect()
        };

        // Not vacuous: the mistake this guards against — indexing a record
        // within its BATCH rather than within its segment — really does move
        // the keys under the same re-split. If this ever stops holding, the
        // assertion below has stopped proving anything.
        let batch_relative_keys_at = |chunk: usize| -> Vec<String> {
            payloads
                .chunks(chunk)
                .flat_map(|pc| {
                    pc.iter().enumerate().map(|(i, payload)| {
                        weir_sink_sdk::RecordId::for_record(
                            "shard_00/seg_00000001.wab.sealed",
                            i as u64 + 1,
                            payload,
                        )
                        .to_hex()
                    })
                })
                .collect()
        };
        assert_ne!(
            batch_relative_keys_at(3),
            batch_relative_keys_at(7),
            "precondition: a batch-relative index must be the thing that breaks"
        );

        let at_3 = keys_at(3);
        let at_7 = keys_at(7);
        assert_eq!(at_3.len(), 10, "one key per record, whatever the split");
        assert_eq!(
            at_3[0],
            ids[0].to_hex(),
            "the values compared below must be the keys (`bound_params_alternate_id_then_payload` \
             pins which parameter slot they occupy)"
        );
        assert_eq!(
            at_3, at_7,
            "a record's idempotency key must be a property of its WAB \
             coordinate, not of the sub-batch it happened to land in — \
             otherwise a replay at a different sink_max_batch_size inserts \
             every record a second time"
        );
    }

    /// A batch with an `id_column` configured but no ids can only come from a
    /// hand-built `SinkBatch` — never the drain. It must fail loudly rather
    /// than invent a key: a placeholder collides every record on the operator's
    /// UNIQUE constraint, and a content hash reinstates the identical-bytes
    /// collision the id column exists to remove.
    #[test]
    fn a_batch_without_ids_is_rejected_when_an_id_column_is_configured() {
        let err = sql_common::per_record_id_hex(DRIVER, Some("record_id"), 2, None).unwrap_err();
        assert!(
            !err.is_transient(),
            "retrying cannot make ids appear, so this must be permanent"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("record_id") && msg.contains("0 record ids for 2 records"),
            "the error must name the column and the mismatch: {msg}"
        );
    }

    /// The other half: ids that are present but not parallel to the records
    /// would pair one record's key with another record's bytes.
    #[test]
    fn a_length_mismatch_between_ids_and_records_is_rejected() {
        let payloads = vec![weir_core::Payload::from(b"a".as_ref())];
        let ids = segment_ids(&payloads);
        let err =
            sql_common::per_record_id_hex(DRIVER, Some("record_id"), 3, Some(ids)).unwrap_err();
        assert!(!err.is_transient());
        assert!(
            err.to_string().contains("1 record ids for 3 records"),
            "the error must report both counts: {err}"
        );
    }

    /// The same name for both columns produces `("payload", "payload")`, which
    /// PG rejects as 42701 — permanent, so every batch would be dead-lettered.
    /// Catch it at startup, before any record is acked against this config.
    #[test]
    fn an_id_column_equal_to_the_payload_column_is_rejected_at_build() {
        let mut c = cfg();
        c.id_column = Some(c.column.clone());
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::IdColumnIsPayloadColumn { .. }
        ));
    }

    #[test]
    fn an_invalid_id_column_identifier_is_rejected_at_build() {
        let mut c = cfg();
        c.id_column = Some("record id\"; DROP TABLE weir_records".to_string());
        assert!(matches!(
            PostgresSink::new(c).unwrap_err(),
            PostgresSinkBuildError::InvalidIdentifier { ref field, .. } if field == "id_column"
        ));
    }

    // Direct `redact_password` coverage lives in `sql_common::tests`
    // (including special-char and missing-credentials cases). The single
    // test below verifies `PostgresSinkConfig::Debug` actually CALLS the
    // shared redactor — i.e. the wiring is correct.

    #[test]
    fn debug_impl_does_not_leak_password() {
        let c = PostgresSinkConfig {
            url: "postgres://alice:topsecret@db.example.com:5432/weir".into(),
            ..cfg()
        };
        let s = format!("{c:?}");
        assert!(!s.contains("topsecret"), "password leaked: {s}");
        assert!(s.contains("alice"), "user should still appear: {s}");
        assert!(s.contains("<redacted>"), "redaction marker missing: {s}");
    }

    // ── TLS opt-in via `?sslmode=require` ────────────────────────────────────

    /// Verifies the URL-scheme-based TLS opt-in compiles and constructs
    /// a sink without panicking. We can't test the actual TLS handshake
    /// from a unit test (no live PG with a cert available), but we can
    /// verify the build path: parses the URL, sees `SslMode::Require`,
    /// picks `MakeRustlsConnect`, hands it to `Manager::from_config`.
    /// Catches a regression where the URL parser fails to surface
    /// sslmode or `build_tls_connector` panics under
    /// `ring::default_provider()` initialisation.
    #[test]
    fn sslmode_require_builds_sink_with_tls_connector() {
        let mut c = cfg();
        c.url = "postgres://user:pw@127.0.0.1:5432/db?sslmode=require".to_string();
        assert!(
            PostgresSink::new(c).is_ok(),
            "sink should build successfully with sslmode=require"
        );
    }

    /// The default (no sslmode in URL) maps to `SslMode::Prefer` in
    /// tokio-postgres. We deliberately treat that as "no TLS" so an
    /// upgrade doesn't silently enable TLS on a previously-cleartext
    /// deployment (see the long comment in `PostgresSink::new`). Pin
    /// that behaviour with a test so a future refactor can't drift.
    #[test]
    fn default_sslmode_prefer_does_not_enable_tls_silently() {
        let mut c = cfg();
        c.url = "postgres://user:pw@127.0.0.1:5432/db".to_string();
        // Sanity: tokio-postgres parses this with the default sslmode.
        let parsed: PgConfig = c.url.parse().unwrap();
        assert_eq!(parsed.get_ssl_mode(), SslMode::Prefer);
        // And the sink still builds (NoTls path).
        assert!(PostgresSink::new(c).is_ok());
    }

    /// Explicit `?sslmode=disable` also builds, NoTls path.
    #[test]
    fn sslmode_disable_builds_sink_with_no_tls() {
        let mut c = cfg();
        c.url = "postgres://user:pw@127.0.0.1:5432/db?sslmode=disable".to_string();
        let parsed: PgConfig = c.url.parse().unwrap();
        assert_eq!(parsed.get_ssl_mode(), SslMode::Disable);
        assert!(PostgresSink::new(c).is_ok());
    }

    // SQLSTATE classification is driver-specific and stays here.

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

    // Driver-tag wiring: a `Timeout`/`Transient` produced from this
    // module carries `driver = "postgres"` so log lines preserve the
    // backend identifier. (`SqlSinkError`'s `is_transient` semantics are
    // tested in `sql_common::tests`; here we only verify the wiring.)

    #[test]
    fn timeout_error_carries_postgres_driver_tag() {
        let err = SqlSinkError::timeout(DRIVER);
        assert!(err.is_transient());
        assert!(format!("{err}").starts_with("postgres "), "got: {err}");
    }

    #[test]
    fn invalid_identifier_maps_to_postgres_build_error() {
        let mut c = cfg();
        c.table = "1bad".to_string();
        let err = PostgresSink::new(c).unwrap_err();
        match err {
            PostgresSinkBuildError::InvalidIdentifier { field, value } => {
                assert_eq!(field, "table");
                assert_eq!(value, "1bad");
            }
            other => panic!("expected InvalidIdentifier, got {other:?}"),
        }
    }
}
