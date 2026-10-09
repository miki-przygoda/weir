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
//! the server reports a duplicate instead, and the sink resolves it row by row
//! (see *Plain mode and duplicate keys* below).
//!
//! ## Plain mode and duplicate keys
//!
//! A plain multi-row `INSERT` is all-or-nothing, so one duplicate fails the
//! whole statement. Retrying it cannot help — it hits the same key every time
//! — so on `ER_DUP_ENTRY` the sink hands the batch to
//! [`sql_common::insert_with_dup_fallback`]:
//!
//! - **With `id_column`**: one `SELECT … WHERE id IN (…)` finds the rows already
//!   present. Their id is their WAB coordinate, so they are this record,
//!   already delivered, and count as committed. One `INSERT` writes the rest.
//! - **Without `id_column`**: the batch is bisected, and each row that is still
//!   a duplicate on its own is dead-lettered with
//!   `duplicate key (insert_mode=plain, no id_column): <server message>`.
//!
//! All of it runs inside the one per-commit `timeout`. Plain mode without an
//! `id_column` draws a config-load warning, because there every duplicate
//! costs a dead-letter record.
//!
//! ## What the UNIQUE constraint should be on
//!
//! With `id_column` unset the sink writes payload bytes only, so the
//! constraint has to be on the payload (or a prefix / hash of it). That is
//! **content** identity, and it is wrong in both directions:
//!
//! - It **loses**. Two genuinely distinct records that happen to carry
//!   identical bytes — a metering "one unit consumed" event, a heartbeat, any
//!   fixed-shape event — collide, and `INSERT IGNORE` discards the second.
//!   weir acked it, wrote it to disk and delivered it; the operator's schema
//!   drops it. The reference schema's `UNIQUE KEY uq_payload (payload(255))`
//!   is worse still, colliding on a shared 255-byte *prefix*.
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
//! `UNIQUE` constraint on that column and `INSERT IGNORE` becomes a true
//! idempotency filter rather than a deduplicator of coincidences.
//!
//! Deliberately opt-in: turning it on requires a column the existing table
//! does not have, so it cannot be a default without breaking every deployed
//! schema.
//!
//! # Error classification
//!
//! - Connection / pool / IO failures → transient (drain retries).
//! - MySQL error codes 1205 (`ER_LOCK_WAIT_TIMEOUT`), 1213
//!   (`ER_LOCK_DEADLOCK`), 1290 (`ER_OPTION_PREVENTS_STATEMENT`, e.g.
//!   `--read-only`) → transient.
//! - Access-denied codes 1045/1044/1142 → transient: a recoverable
//!   misconfiguration (wrong password, missing grant), not the record's fault,
//!   so the drain retries until it's fixed rather than dead-lettering live data.
//!   This matches the Postgres sink, where a connection-time auth failure is a
//!   non-DbError and already transient.
//! - Code 1062 (`ER_DUP_ENTRY`) — never seen under `INSERT IGNORE`; under
//!   `plain` mode resolved row by row as described above, never retried
//!   as-is (retrying the identical statement was a livelock).
//! - All other server errors (syntax, missing table, missing column, …) →
//!   permanent. The whole batch is dead-lettered with the server-supplied
//!   error message so an operator can debug.
//!
//! # Authentication
//!
//! Credentials are taken from the connection URL
//! (`mysql://user:pass@host:3306/db`). Prefer setting the URL via the
//! `WEIR_SINK_URL` environment variable so the embedded password never lands
//! on disk. The URL can also be set via `--sink-url` or the TOML config
//! (`sink_url`), but a credential-bearing URL written to TOML is stored on
//! disk in plaintext. `Debug` impls redact the password before logging.

use std::sync::Arc;
use std::time::Duration;

use mysql_async::{Pool, prelude::Queryable};

use super::sql_common::{self, SqlSinkError, StmtError};
use super::{CommitResult, Sink, SinkBatch, SinkHealth};

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
    /// the server. Recommended default: pair with a `UNIQUE` constraint on the
    /// `id_column` so crash-recovery retries are idempotent without
    /// consumer-side dedup. A `UNIQUE` on the payload instead makes this
    /// discard distinct records that share bytes — see the module docs.
    Ignore,
    /// `INSERT INTO …` — duplicates surface as `ER_DUP_ENTRY` and are
    /// resolved row by row: committed when the `id_column` shows the record is
    /// already present, dead-lettered otherwise. See the module docs.
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
    /// Optional column receiving each record's
    /// [`RecordId`](weir_sink_sdk::RecordId) as 64 lower-hex characters — the
    /// per-record idempotency key. `None` (the default) keeps the single-column
    /// INSERT weir has always written. See the module docs for why a `UNIQUE`
    /// on this beats a `UNIQUE` on the payload.
    pub id_column: Option<String>,
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
            .field("id_column", &self.id_column)
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
        if let Some(id_column) = &config.id_column {
            sql_common::validate_identifier("id_column", id_column, IDENTIFIER_MAX_LEN)
                .map_err(MySqlSinkBuildError::from)?;
            // Rejected at build, not at first commit: the same name twice
            // produces ``INSERT INTO `t` (`payload`, `payload`)``, which MySQL
            // rejects as ER_FIELD_SPECIFIED_TWICE — a *permanent* error, so the
            // drain would dead-letter every batch it ever received. By the time
            // a record reaches the sink it is already acked, so a config
            // mistake has to stop the daemon starting.
            if id_column == &config.column {
                return Err(MySqlSinkBuildError::IdColumnIsPayloadColumn {
                    name: id_column.clone(),
                });
            }
        }
        let opts = mysql_async::Opts::from_url(&config.url).map_err(|_e| {
            // Do not surface the driver's error verbatim — it can embed the
            // connection URL including the user:password@ component, leaking the
            // password to stderr/logs. Report the redacted URL instead (the same
            // redaction the Debug impl uses).
            MySqlSinkBuildError::InvalidUrl(sql_common::redact_password(&config.url))
        })?;
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
        let verb = match self.config.insert_mode {
            InsertMode::Ignore => "INSERT IGNORE INTO",
            InsertMode::Plain => "INSERT INTO",
        };
        // Identifiers are pre-validated to `[A-Za-z_][A-Za-z0-9_]{0,63}`, so
        // there is no SQL injection vector through `table`, `column` or
        // `id_column`.
        match &self.config.id_column {
            None => {
                let placeholders: String =
                    std::iter::repeat_n("(?)", n).collect::<Vec<_>>().join(", ");
                format!(
                    "{verb} `{}` (`{}`) VALUES {placeholders}",
                    self.config.table, self.config.column
                )
            }
            // Two placeholders per row, id first, so `commit` binds
            // (id, payload) pairs in the same order the column list names
            // them. The id leads because that is the column the operator's
            // UNIQUE constraint is on, and a reader of a slow-query log should
            // see the key before the blob.
            Some(id_column) => {
                let placeholders: String = std::iter::repeat_n("(?, ?)", n)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{verb} `{}` (`{}`, `{}`) VALUES {placeholders}",
                    self.config.table, id_column, self.config.column
                )
            }
        }
    }

    /// The plain-mode duplicate-key lookup: which of `n` ids are already in
    /// the table. One placeholder per id; `sink_max_batch_size` is capped at
    /// 10 000, well under MySQL's 65 535-placeholder limit.
    pub(crate) fn build_lookup_sql(&self, id_column: &str, n: usize) -> String {
        let placeholders = std::iter::repeat_n("?", n).collect::<Vec<_>>().join(", ");
        format!(
            "SELECT `{id_column}` FROM `{}` WHERE `{id_column}` IN ({placeholders})",
            self.config.table
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
    #[error(
        "mysql sink id column {name:?} is also the payload column; \
         they must be different columns"
    )]
    IdColumnIsPayloadColumn { name: String },
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
            // Access-denied family — recoverable misconfiguration, NOT the
            // record's fault, so retry (don't dead-letter live data) until the
            // operator fixes the grant/password. This matches the Postgres sink,
            // where a connection-time auth failure surfaces as a non-DbError and
            // is already transient; classifying these as permanent was an
            // asymmetry that lost live data on a fixable auth error (F31).
            | 1045  // ER_ACCESS_DENIED_ERROR (bad user/password)
            | 1044  // ER_DBACCESS_DENIED_ERROR (no access to database)
            | 1142 // ER_TABLEACCESS_DENIED_ERROR (no access to table)
    )
}

/// `ER_DUP_ENTRY`.
const ER_DUP_ENTRY: u16 = 1062;

/// Classifies one statement's failure for the duplicate-key fallback. In plain
/// mode `ER_DUP_ENTRY` becomes [`StmtError::DupKey`]; everything else, and
/// every error in `ignore` mode, goes through [`classify`] unchanged.
pub(crate) fn classify_stmt(err: mysql_async::Error, mode: InsertMode) -> StmtError {
    if mode == InsertMode::Plain
        && let mysql_async::Error::Server(srv) = &err
        && srv.code == ER_DUP_ENTRY
    {
        return StmtError::DupKey(format!("server {}: {}", srv.code, srv.message));
    }
    StmtError::Other(classify(err))
}

/// Positional parameters for the statement `build_insert_sql` produced, in
/// placeholder order.
///
/// Split out of `commit` so the binding order is unit-testable: a real MySQL
/// server is the only other way to observe it, and getting `(id, payload)` the
/// wrong way round would write each record's key into the payload column and
/// each payload into the `UNIQUE` key — corrupting the table in a way the sink
/// itself would report as success.
///
/// `id_hex` is `None` when no `id_column` is configured, and otherwise carries
/// exactly one hex `RecordId` per record (enforced upstream by
/// [`sql_common::per_record_id_hex`]).
///
/// `rows` names the records to bind, by index into `batch`: every index for
/// the first statement, a subset for the duplicate-key fallback. A record's id
/// and payload are read at the same index, so they cannot drift apart.
fn build_params(
    batch: &[weir_core::Payload],
    id_hex: Option<&[String]>,
    rows: &[usize],
) -> Vec<mysql_async::Value> {
    match id_hex {
        None => rows
            .iter()
            .map(|&i| mysql_async::Value::Bytes(batch[i].to_vec()))
            .collect(),
        Some(ids) => rows
            .iter()
            .flat_map(|&i| {
                [
                    mysql_async::Value::Bytes(ids[i].clone().into_bytes()),
                    mysql_async::Value::Bytes(batch[i].to_vec()),
                ]
            })
            .collect(),
    }
}

// ── Sink trait impl ───────────────────────────────────────────────────────────

impl Sink for MySqlSink {
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

        // One deadline for the whole commit, fallback included: the
        // duplicate-key path never stretches a commit past the sink timeout.
        let deadline = tokio::time::Instant::now() + self.config.timeout;
        let mode = self.config.insert_mode;
        let (batch, id_hex) = (&batch, id_hex.as_deref());

        let insert = |rows: Vec<usize>| {
            let sql = self.build_insert_sql(rows.len());
            let params = build_params(batch, id_hex, &rows);
            async move {
                let mut conn = self
                    .pool
                    .get_conn()
                    .await
                    .map_err(|e| StmtError::Other(classify(e)))?;
                conn.exec_drop(&sql, mysql_async::Params::Positional(params))
                    .await
                    .map_err(|e| classify_stmt(e, mode))
            }
        };
        let lookup_present = |rows: Vec<usize>| {
            // Only called when an id column is configured (the driver gets
            // `ids = Some` exactly then); the `None` arm is unreachable but
            // answers with an error rather than a panic.
            let query = self
                .config
                .id_column
                .as_deref()
                .zip(id_hex)
                .map(|(col, ids)| {
                    let params: Vec<mysql_async::Value> = rows
                        .iter()
                        .map(|&i| mysql_async::Value::Bytes(ids[i].clone().into_bytes()))
                        .collect();
                    (self.build_lookup_sql(col, rows.len()), params)
                });
            async move {
                let Some((sql, params)) = query else {
                    return Err(StmtError::Other(SqlSinkError::permanent(
                        DRIVER,
                        "duplicate-key lookup without an id column",
                    )));
                };
                let mut conn = self
                    .pool
                    .get_conn()
                    .await
                    .map_err(|e| StmtError::Other(classify(e)))?;
                let found: Vec<String> = conn
                    .exec(&sql, mysql_async::Params::Positional(params))
                    .await
                    .map_err(|e| StmtError::Other(classify(e)))?;
                Ok(found.into_iter().collect())
            }
        };

        sql_common::insert_with_dup_fallback(
            DRIVER,
            batch,
            id_hex,
            deadline,
            insert,
            lookup_present,
        )
        .await
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
    use crate::sink::SinkError;
    use weir_core::Payload;

    fn p(s: &'static [u8]) -> Payload {
        Payload::from_static(s)
    }

    fn cfg() -> MySqlSinkConfig {
        MySqlSinkConfig {
            url: "mysql://user:pw@127.0.0.1:3306/db".to_string(),
            table: "weir_records".to_string(),
            column: "payload".to_string(),
            id_column: None,
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
    fn invalid_url_error_redacts_password() {
        // A URL carrying a password but failing to parse: the build error must
        // report the REDACTED url, never the driver's verbatim string (which can
        // embed user:password@) — otherwise the password leaks to stderr/logs.
        let mut c = cfg();
        c.url = "mysql://user:supersecret@host:notaport/db".to_string();
        let err = MySqlSink::new(c).unwrap_err();
        assert!(
            matches!(err, MySqlSinkBuildError::InvalidUrl(_)),
            "expected InvalidUrl, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("<redacted>"), "url must be redacted: {msg}");
        assert!(
            !msg.contains("supersecret"),
            "password must not leak: {msg}"
        );
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

    // ── Per-record idempotency key (`id_column`) ─────────────────────────────

    /// The ids the drain would produce for a whole ten-record segment:
    /// `RecordId::for_record(segment, absolute_index, payload)`. Built once and
    /// re-split below, which is exactly what the drain does when
    /// `sink_max_batch_size` changes between two delivery attempts.
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

    /// Every bound value for one sub-batch, as the driver would see them.
    fn bound(
        sink: &MySqlSink,
        payloads: &[weir_core::Payload],
        ids: &[weir_sink_sdk::RecordId],
    ) -> Vec<mysql_async::Value> {
        let hex = sql_common::per_record_id_hex(
            DRIVER,
            sink.config.id_column.as_deref(),
            payloads.len(),
            Some(ids.to_vec()),
        )
        .unwrap();
        let all: Vec<usize> = (0..payloads.len()).collect();
        build_params(payloads, hex.as_deref(), &all)
    }

    #[test]
    fn build_sql_with_id_column_binds_two_placeholders_per_row() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = MySqlSink::new(c).unwrap();
        assert_eq!(
            sink.build_insert_sql(2),
            "INSERT IGNORE INTO `weir_records` (`record_id`, `payload`) VALUES (?, ?), (?, ?)"
        );
    }

    /// The historical shape must not move for anyone who leaves the knob
    /// unset — this is the whole reason it is opt-in.
    #[test]
    fn build_sql_without_id_column_is_unchanged() {
        let sink = MySqlSink::new(cfg()).unwrap();
        assert_eq!(
            sink.build_insert_sql(2),
            "INSERT IGNORE INTO `weir_records` (`payload`) VALUES (?), (?)"
        );
    }

    /// The column list says `(record_id, payload)`, so the bound values must
    /// alternate id, payload — in that order. Getting it backwards writes each
    /// payload into the UNIQUE key column and each key into the payload column,
    /// and the sink reports success either way.
    #[test]
    fn bound_params_alternate_id_then_payload() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = MySqlSink::new(c).unwrap();

        let payloads = vec![p(b"first"), p(b"second")];
        let ids = segment_ids(&payloads);
        let values = bound(&sink, &payloads, &ids);

        assert_eq!(values.len(), 4, "two rows, two parameters each");
        assert_eq!(
            values[0],
            mysql_async::Value::Bytes(ids[0].to_hex().into_bytes()),
            "parameter 1 is the first row's id"
        );
        assert_eq!(
            values[1],
            mysql_async::Value::Bytes(b"first".to_vec()),
            "parameter 2 is the first row's payload"
        );
        assert_eq!(
            values[2],
            mysql_async::Value::Bytes(ids[1].to_hex().into_bytes())
        );
        assert_eq!(values[3], mysql_async::Value::Bytes(b"second".to_vec()));
    }

    /// **The guarantee, at the sink boundary.** The same ten records of one WAB
    /// segment, delivered as four sub-batches at `sink_max_batch_size = 3` and
    /// then as two at `7`, must present the same ten idempotency keys — so a
    /// `UNIQUE (record_id)` recognises the second delivery as a replay of the
    /// first and `INSERT IGNORE` drops it.
    ///
    /// This is what a `DedupToken` cannot do: it names the sub-batch, and the
    /// sub-batches are not the same across the two splits.
    #[test]
    fn the_bound_keys_do_not_move_when_the_batch_size_changes() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = MySqlSink::new(c).unwrap();

        let payloads: Vec<Payload> = (1..=10)
            .map(|i| Payload::copy_from_slice(format!("record-{i:02}").as_bytes()))
            .collect();
        let ids = segment_ids(&payloads);

        // Only the id parameters: every even index is a key, every odd one a
        // payload (pinned by `bound_params_alternate_id_then_payload`).
        let keys_at = |chunk: usize| -> Vec<mysql_async::Value> {
            payloads
                .chunks(chunk)
                .zip(ids.chunks(chunk))
                .flat_map(|(pc, ic)| bound(&sink, pc, ic))
                .step_by(2)
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
        // Anchor the extraction: without this, `step_by(2)` reading the
        // *payload* half would satisfy the equality below just as well, and
        // the test would pass a sink that bound (payload, id).
        assert_eq!(
            at_3[0],
            mysql_async::Value::Bytes(ids[0].to_hex().into_bytes()),
            "the values compared below must be the keys, not the payloads"
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

    /// The same name for both columns produces `(payload, payload)`, which
    /// MySQL rejects permanently — so every batch would be dead-lettered. Catch
    /// it at startup, before any record is acked against this config.
    #[test]
    fn an_id_column_equal_to_the_payload_column_is_rejected_at_build() {
        let mut c = cfg();
        c.id_column = Some(c.column.clone());
        assert!(matches!(
            MySqlSink::new(c).unwrap_err(),
            MySqlSinkBuildError::IdColumnIsPayloadColumn { .. }
        ));
    }

    #[test]
    fn an_invalid_id_column_identifier_is_rejected_at_build() {
        let mut c = cfg();
        c.id_column = Some("record id; DROP TABLE weir_records".to_string());
        assert!(matches!(
            MySqlSink::new(c).unwrap_err(),
            MySqlSinkBuildError::InvalidIdentifier { ref field, .. } if field == "id_column"
        ));
    }

    // ── Plain-mode duplicate keys (spec §4.7) ────────────────────────────────

    fn server_err(code: u16) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: "Duplicate entry 'x' for key 'uq'".to_string(),
            state: "23000".to_string(),
        })
    }

    /// Only plain mode ever sees ER_DUP_ENTRY, and only plain mode hands it to
    /// the fallback. Under `INSERT IGNORE` it keeps today's classification.
    #[test]
    fn dup_entry_is_a_dup_key_in_plain_mode_only() {
        match classify_stmt(server_err(1062), InsertMode::Plain) {
            StmtError::DupKey(msg) => {
                assert!(
                    msg.contains("1062") && msg.contains("Duplicate entry"),
                    "{msg}"
                )
            }
            other => panic!("expected DupKey, got {other:?}"),
        }
        assert!(matches!(
            classify_stmt(server_err(1062), InsertMode::Ignore),
            StmtError::Other(e) if e.is_transient()
        ));
        // Any other code goes through the ordinary classifier.
        assert!(matches!(
            classify_stmt(server_err(1406), InsertMode::Plain),
            StmtError::Other(e) if !e.is_transient()
        ));
    }

    #[test]
    fn lookup_sql_selects_the_id_column_with_one_placeholder_per_id() {
        let mut c = cfg();
        c.id_column = Some("record_id".to_string());
        let sink = MySqlSink::new(c).unwrap();
        assert_eq!(
            sink.build_lookup_sql("record_id", 3),
            "SELECT `record_id` FROM `weir_records` WHERE `record_id` IN (?, ?, ?)"
        );
    }

    /// The fallback inserts subsets of the batch; the bound values must be
    /// exactly those rows, in the order the indices name them.
    #[test]
    fn bound_params_cover_only_the_selected_rows() {
        let payloads = vec![p(b"a"), p(b"b"), p(b"c")];
        let ids = segment_ids(&payloads);
        let hex: Vec<String> = ids.iter().map(|id| id.to_hex()).collect();
        let values = build_params(&payloads, Some(&hex), &[0, 2]);
        assert_eq!(
            values,
            vec![
                mysql_async::Value::Bytes(hex[0].clone().into_bytes()),
                mysql_async::Value::Bytes(b"a".to_vec()),
                mysql_async::Value::Bytes(hex[2].clone().into_bytes()),
                mysql_async::Value::Bytes(b"c".to_vec()),
            ]
        );
        assert_eq!(
            build_params(&payloads, None, &[1]),
            vec![mysql_async::Value::Bytes(b"b".to_vec())]
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
        // Access-denied family — recoverable misconfig, retry not dead-letter (F31).
        assert!(is_transient_server_code(1045)); // access denied (bad password)
        assert!(is_transient_server_code(1044)); // db access denied
        assert!(is_transient_server_code(1142)); // table access denied

        // Things that must dead-letter — retrying won't help (the data is at fault).
        assert!(!is_transient_server_code(1064)); // syntax error
        assert!(!is_transient_server_code(1146)); // no such table
        assert!(!is_transient_server_code(1054)); // bad field
        assert!(!is_transient_server_code(1406)); // data too long for column
    }

    #[test]
    fn classify_maps_driver_errors_to_transient_or_permanent() {
        use mysql_async::{Error as E, ServerError};
        let server = |code| {
            E::Server(ServerError {
                code,
                message: "msg".to_string(),
                state: "HY000".to_string(),
            })
        };
        // Server error: routed through is_transient_server_code.
        assert!(
            classify(server(1205)).is_transient(),
            "lock-wait-timeout (1205) must be transient"
        );
        assert!(
            classify(server(1045)).is_transient(),
            "access-denied (1045) is a fixable misconfig — transient, not dead-letter (F31)"
        );
        assert!(
            !classify(server(1064)).is_transient(),
            "syntax error (1064) is the data's fault — permanent"
        );
        // Driver/IO errors are always transient (network/pool blips).
        assert!(classify(E::Driver(mysql_async::DriverError::ConnectionClosed)).is_transient());
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
        let result = sink.commit(SinkBatch::from(Vec::new())).await.unwrap();
        assert!(result.committed.is_empty());
        assert!(result.dead_lettered.is_empty());
    }

    #[tokio::test]
    async fn connect_refused_returns_transient_error() {
        let mut c = cfg();
        c.url = "mysql://x:y@127.0.0.1:1/weir".into();
        c.timeout = Duration::from_secs(2);
        let sink = MySqlSink::new(c).unwrap();
        let err = sink
            .commit(SinkBatch::from(vec![p(b"hello")]))
            .await
            .unwrap_err();
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
        let err = sink
            .commit(SinkBatch::from(vec![p(b"hello")]))
            .await
            .unwrap_err();
        assert!(matches!(err, SqlSinkError::Timeout { .. }), "got: {err}");
        assert!(err.is_transient());
        // Driver context survives into the Display string.
        assert!(format!("{err}").contains("mysql"), "got: {err}");
    }

    // ── Live server: plain-mode duplicate keys (spec §4.7) ───────────────────
    //
    // `#[ignore]`d: they need the MySQL of the docker-compose rig. Run with
    // `bash deploy/run-sink-integration-tests.sh`, or with the rig up:
    //
    //   WEIR_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:33306/weir_test \
    //     cargo test -p weir-server --bin weir-server -- --ignored \
    //     --test-threads=1 sink::mysql::tests::live_
    //
    // They drive the sink directly rather than through the daemon, because
    // the scenario is a REPLAY of the same records under the same ids, which
    // the daemon only produces after a crash.

    mod live {
        use super::*;

        fn url() -> String {
            std::env::var("WEIR_TEST_MYSQL_URL")
                .expect("WEIR_TEST_MYSQL_URL not set; see deploy/run-sink-integration-tests.sh")
        }

        /// A fresh table of the given shape. Each test owns its table name, so
        /// the tests do not disturb each other or the rig's reference tables.
        async fn fresh_table(table: &str, keyed: bool) {
            let pool = Pool::new(mysql_async::Opts::from_url(&url()).unwrap());
            let mut conn = pool.get_conn().await.unwrap();
            conn.query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
                .await
                .unwrap();
            let ddl = if keyed {
                format!(
                    "CREATE TABLE `{table}` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                     record_id CHAR(64) NOT NULL, payload VARBINARY(4096) NOT NULL, \
                     UNIQUE KEY uq_record_id (record_id))"
                )
            } else {
                format!(
                    "CREATE TABLE `{table}` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                     payload VARBINARY(4096) NOT NULL, UNIQUE KEY uq_payload (payload(255)))"
                )
            };
            conn.query_drop(ddl).await.unwrap();
            drop(conn);
            pool.disconnect().await.unwrap();
        }

        async fn exec(sql: &str) {
            let pool = Pool::new(mysql_async::Opts::from_url(&url()).unwrap());
            let mut conn = pool.get_conn().await.unwrap();
            conn.query_drop(sql).await.unwrap();
            drop(conn);
            pool.disconnect().await.unwrap();
        }

        async fn count(table: &str) -> u64 {
            let pool = Pool::new(mysql_async::Opts::from_url(&url()).unwrap());
            let mut conn = pool.get_conn().await.unwrap();
            let n: Option<u64> = conn
                .query_first(format!("SELECT COUNT(*) FROM `{table}`"))
                .await
                .unwrap();
            drop(conn);
            pool.disconnect().await.unwrap();
            n.unwrap()
        }

        fn sink(
            table: &str,
            id_column: bool,
            max_batch_size: usize,
            timeout: Duration,
        ) -> MySqlSink {
            MySqlSink::new(MySqlSinkConfig {
                url: url(),
                table: table.to_string(),
                column: "payload".to_string(),
                id_column: id_column.then(|| "record_id".to_string()),
                insert_mode: InsertMode::Plain,
                max_batch_size,
                timeout,
            })
            .unwrap()
        }

        /// The drain's batch for records `0..n` of one segment: the same
        /// coordinates, hence the same ids, on every replay.
        fn batch(payloads: &[Payload]) -> SinkBatch {
            SinkBatch::with_record_ids(
                payloads.to_vec(),
                weir_sink_sdk::DedupToken::for_payloads(payloads.iter()),
                segment_ids(payloads),
            )
        }

        fn names(ps: &[Payload]) -> Vec<String> {
            ps.iter()
                .map(|p| String::from_utf8(p.to_vec()).unwrap())
                .collect()
        }

        fn abc() -> Vec<Payload> {
            vec![p(b"a"), p(b"b"), p(b"c")]
        }

        #[tokio::test]
        #[ignore = "requires WEIR_TEST_MYSQL_URL (deploy/run-sink-integration-tests.sh)"]
        async fn live_plain_id_column_replay_counts_present_rows_as_committed() {
            let table = "weir_dup_id_replay";
            fresh_table(table, true).await;
            let sink = sink(table, true, 1000, Duration::from_secs(10));
            let all = abc();

            let first = sink.commit(batch(&all[..2])).await.unwrap();
            assert_eq!(names(&first.committed), ["a", "b"]);

            let second = sink.commit(batch(&all)).await.unwrap();
            assert_eq!(names(&second.committed), ["a", "b", "c"]);
            assert!(
                second.dead_lettered.is_empty(),
                "{:?}",
                second.dead_lettered
            );
            assert_eq!(
                count(table).await,
                3,
                "exactly three rows: nothing written twice"
            );
        }

        #[tokio::test]
        #[ignore = "requires WEIR_TEST_MYSQL_URL (deploy/run-sink-integration-tests.sh)"]
        async fn live_plain_without_id_column_dead_letters_the_duplicates() {
            let table = "weir_dup_bisect";
            fresh_table(table, false).await;
            let sink = sink(table, false, 1000, Duration::from_secs(10));
            let all = abc();

            sink.commit(SinkBatch::from(all[..2].to_vec()))
                .await
                .unwrap();
            let second = sink.commit(SinkBatch::from(all.clone())).await.unwrap();
            assert_eq!(names(&second.committed), ["c"]);
            assert_eq!(
                second
                    .dead_lettered
                    .iter()
                    .map(|(p, _)| String::from_utf8(p.to_vec()).unwrap())
                    .collect::<Vec<_>>(),
                ["a", "b"]
            );
            for (_, reason) in &second.dead_lettered {
                assert!(
                    reason.starts_with(
                        "duplicate key (insert_mode=plain, no id_column): server 1062:"
                    ),
                    "{reason}"
                );
            }
            assert_eq!(count(table).await, 3);
        }

        #[tokio::test]
        #[ignore = "requires WEIR_TEST_MYSQL_URL (deploy/run-sink-integration-tests.sh)"]
        async fn live_plain_id_column_full_replay_of_5000_completes_within_the_timeout() {
            let table = "weir_dup_replay_5000";
            fresh_table(table, true).await;
            let timeout = Duration::from_secs(10);
            let sink = sink(table, true, 5000, timeout);
            let all: Vec<Payload> = (0..5000)
                .map(|i| Payload::copy_from_slice(format!("rec-{i:05}").as_bytes()))
                .collect();

            sink.commit(batch(&all)).await.unwrap();
            let t = std::time::Instant::now();
            let replay = sink.commit(batch(&all)).await.unwrap();
            let took = t.elapsed();
            assert_eq!(replay.committed.len(), 5000);
            assert!(replay.dead_lettered.is_empty());
            assert!(took < timeout, "replay took {took:?}");
            assert_eq!(count(table).await, 5000);
        }

        /// The livelock's own trigger: the server applied the INSERT, the
        /// client timed out. Latency is injected with a per-row trigger.
        #[tokio::test]
        #[ignore = "requires WEIR_TEST_MYSQL_URL (deploy/run-sink-integration-tests.sh)"]
        async fn live_plain_attempt_that_timed_out_midway_is_completed_by_the_next() {
            let table = "weir_dup_timed_out";
            fresh_table(table, true).await;
            exec(&format!(
                "CREATE TRIGGER `{table}_slow` BEFORE INSERT ON `{table}` \
                 FOR EACH ROW SET @weir_slow = SLEEP(0.4)"
            ))
            .await;
            let all = abc();

            let err = sink(table, true, 1000, Duration::from_millis(300))
                .commit(batch(&all))
                .await
                .unwrap_err();
            assert!(matches!(err, SqlSinkError::Timeout { .. }), "{err}");
            // Let the server finish the statement the client gave up on.
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(
                count(table).await,
                3,
                "precondition: the server applied the attempt the client timed out on"
            );

            let r = sink(table, true, 1000, Duration::from_secs(10))
                .commit(batch(&all))
                .await
                .unwrap();
            assert_eq!(names(&r.committed), ["a", "b", "c"]);
            assert!(r.dead_lettered.is_empty(), "{:?}", r.dead_lettered);
            assert_eq!(count(table).await, 3);
        }
    }
}
