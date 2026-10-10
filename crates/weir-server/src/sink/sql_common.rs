//! Shared helpers + types for SQL sinks.
//!
//! The MySQL and Postgres sinks share an architectural shape — multi-row
//! INSERT, identifier-validated table/column, password-redacted Debug, and a
//! Transient/Permanent/Timeout error split — but speak to different drivers
//! and SQL dialects. This module factors the parts that are genuinely
//! identical so each sink only carries the driver-specific glue.
//!
//! ## What lives here
//!
//! - [`validate_identifier`] — strict ASCII identifier validation, parametric
//!   on the dialect's max length (MySQL = 64, Postgres = 63). Both sinks
//!   route their `table`/`column` config through this so the SQL-string
//!   builder can `format!` identifiers in directly with no escaping logic.
//! - [`redact_password`] — URL password redaction for `Debug` log lines.
//!   Operator-credentials hygiene; identical in both sinks.
//! - [`SqlSinkError`] — the runtime error enum returned by both sinks'
//!   `commit()`. Variants carry a `driver: &'static str` so error messages
//!   keep their driver context (`"mysql sink transient: ..."`).
//!
//! ## What stays in the per-sink module
//!
//! - The driver-specific error classifier (`classify`) — `mysql_async::Error`
//!   and `tokio_postgres::Error` are different types with different shapes,
//!   no shared abstraction is honest here.
//! - The driver-specific transient-codes table (`is_transient_server_code`
//!   for MySQL error numbers, `is_transient_sqlstate` for PG SQLSTATEs).
//! - The build-error enum — each sink has driver-specific variants
//!   (Postgres' `PoolBuild`, possibly more in future) and the per-sink
//!   prefix in Display strings is the operator-friendly choice.
//! - The Sink trait impl, the SQL-string builder, the config struct.

#[cfg(any(feature = "mysql-sink", feature = "postgres-sink"))]
use super::CommitResult;
use super::SinkError;

// ── Identifier validation ─────────────────────────────────────────────────────

/// The parts of an identifier-validation failure that don't depend on the
/// driver. Per-sink build-error enums carry their own `InvalidIdentifier`
/// variant and convert from this via a 3-line `map_err`. We deliberately
/// don't bake the max-length into Display here: the sink's own variant
/// puts the right limit in the error message (`{0,63}` vs `{0,64}`).
#[derive(Debug)]
pub(super) struct InvalidIdentifier {
    pub field: String,
    pub value: String,
}

/// Strict identifier validation: `[A-Za-z_][A-Za-z0-9_]{0,max_len-1}`.
///
/// This is the security-critical chokepoint that makes
/// `format!("INSERT INTO {table} ...")` safe — every accepted identifier
/// is also a valid SQL identifier in both MySQL backticks and Postgres
/// double-quotes, with no characters that could escape the quote context.
///
/// `max_len` differs by dialect (MySQL = 64, Postgres = 63 — PG's
/// `NAMEDATALEN - 1` limit) and is supplied by the caller; the validation
/// logic is otherwise identical.
pub(super) fn validate_identifier(
    field: &str,
    value: &str,
    max_len: usize,
) -> Result<(), InvalidIdentifier> {
    let mk_err = || InvalidIdentifier {
        field: field.to_string(),
        value: value.to_string(),
    };
    if value.is_empty() || value.len() > max_len {
        return Err(mk_err());
    }
    let mut chars = value.chars();
    let first = chars.next().expect("non-empty checked above");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(mk_err());
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(mk_err());
        }
    }
    Ok(())
}

// ── Password redaction ────────────────────────────────────────────────────────

/// Best-effort password redaction for log lines. Matches
/// `scheme://user:PASSWORD@host` and replaces `PASSWORD` with `<redacted>`.
/// If the URL doesn't match that shape we return it unchanged (no
/// credentials to redact).
///
/// Used by both SQL sinks' `Debug` impls so a `?config` log line cannot
/// leak the password component of `sink_url`. The function is deliberately
/// permissive about URL parsing (it doesn't try to validate the URL
/// itself, only locate the password substring) so debug formatting can
/// never fail on a malformed URL.
///
/// The implementation lives in [`crate::sink::redact_url_password`] (always
/// compiled) so the config layer can share it regardless of which sink
/// features are built in; this is the SQL-sink-facing alias (F59).
pub(super) fn redact_password(url: &str) -> String {
    super::redact_url_password(url)
}

// ── Runtime error type ────────────────────────────────────────────────────────

/// Runtime error returned by SQL sinks' `commit()`. Both MySQL and Postgres
/// classify their driver errors into this shared shape; the `driver` field
/// preserves operator-friendly error messages
/// (`"mysql sink transient: ..."` vs `"postgres sink transient: ..."`).
///
/// No external code names this type — the drain consumes sinks via the
/// `Sink::Error` associated type. That gave us the freedom to collapse
/// what used to be two near-identical enums into one shared definition.
#[derive(Debug, thiserror::Error)]
pub enum SqlSinkError {
    /// Connection / pool / IO failures and explicit-transient server codes.
    /// Drain retries the whole segment.
    #[error("{driver} sink transient: {reason}")]
    Transient {
        driver: &'static str,
        reason: String,
    },
    /// Permanent error — bad schema, bad auth, syntax error, payload too
    /// large for the column. Drain dead-letters the batch with this
    /// string as the reason.
    #[error("{driver} sink permanent: {reason}")]
    Permanent {
        driver: &'static str,
        reason: String,
    },
    /// Per-query timeout — `sink_timeout_secs` exceeded. Transient: the
    /// server is probably overloaded; backoff and retry.
    #[error("{driver} sink timeout")]
    Timeout { driver: &'static str },
}

impl SqlSinkError {
    /// Construct a `Transient` variant. Helpers are provided per variant
    /// so callers don't repeat the `driver:` field tag inline at every
    /// classify-site, which adds up across two sinks' classify
    /// functions.
    pub(super) fn transient(driver: &'static str, reason: impl Into<String>) -> Self {
        SqlSinkError::Transient {
            driver,
            reason: reason.into(),
        }
    }

    pub(super) fn permanent(driver: &'static str, reason: impl Into<String>) -> Self {
        SqlSinkError::Permanent {
            driver,
            reason: reason.into(),
        }
    }

    pub(super) fn timeout(driver: &'static str) -> Self {
        SqlSinkError::Timeout { driver }
    }
}

impl SinkError for SqlSinkError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            SqlSinkError::Transient { .. } | SqlSinkError::Timeout { .. }
        )
    }
}

// ── Per-record idempotency keys ───────────────────────────────────────────────

/// The per-record idempotency keys a SQL sink should bind alongside its
/// payloads, as 64-character lower-hex `RecordId`s, or `None` when the sink has
/// no `id_column` configured (in which case the batch's record ids go unused
/// and the INSERT keeps its historical single-column shape).
///
/// Shared because both SQL sinks make exactly the same decision here and the
/// reasoning below is the load-bearing part, not the four lines of code.
///
/// # Why a `RecordId` and not a payload hash
///
/// A `RecordId` is the record's WAB coordinate — its segment plus its absolute
/// index within that segment — hashed with its bytes. That gives it the two
/// properties a `UNIQUE`-constraint dedup key needs and a content hash cannot
/// have at the same time:
///
/// - **Unique across records with identical bytes.** A content hash makes a
///   repeated heartbeat or a repeated "one unit consumed" metering event
///   collide, and `ON CONFLICT DO NOTHING` / `INSERT IGNORE` then discards
///   records weir acked, stored and delivered.
/// - **Stable across a replay, including a re-batched one.** The index is the
///   record's ordinal in the segment, not in the batch, so it does not move
///   when `sink_max_batch_size` changes between two delivery attempts. The
///   batch-scoped `DedupToken` does move, which is why it cannot do this job.
///
/// # Errors
///
/// Permanent, when an `id_column` is configured but the batch does not carry
/// one id per record. The drain always supplies them
/// (`SinkBatch::with_segment_context`), so the only way here is a hand-built
/// `SinkBatch`. Every alternative to failing is worse: a placeholder key makes
/// every record collide on the operator's `UNIQUE` constraint and discards all
/// but one, and falling back to a content hash silently reinstates exactly the
/// identical-bytes collision the id column exists to remove. Permanent rather
/// than transient because retrying cannot make ids appear — the drain
/// dead-letters the batch loudly instead of stranding the whole backlog.
pub(super) fn per_record_id_hex(
    driver: &'static str,
    id_column: Option<&str>,
    record_count: usize,
    record_ids: Option<Vec<weir_sink_sdk::RecordId>>,
) -> Result<Option<Vec<String>>, SqlSinkError> {
    let Some(id_column) = id_column else {
        return Ok(None);
    };
    // `SinkBatch`'s constructors already enforce the parallel-length
    // invariant. It is re-checked because the failure it prevents — writing
    // one record's key against another record's bytes — is worse than writing
    // no key at all, and this is the last point where it is checkable.
    match record_ids {
        Some(ids) if ids.len() == record_count => {
            Ok(Some(ids.iter().map(|id| id.to_hex()).collect()))
        }
        other => Err(SqlSinkError::permanent(
            driver,
            format!(
                "id column {id_column:?} is configured but the batch carries {} \
                 record ids for {record_count} records; the drain always supplies \
                 one per record, so this batch did not come from the drain",
                other.map_or(0, |ids| ids.len()),
            ),
        )),
    }
}

// ── Plain-mode duplicate-key fallback ─────────────────────────────────────────

/// How one statement failed, as [`insert_with_dup_fallback`] needs to see it.
#[cfg(any(feature = "mysql-sink", feature = "postgres-sink"))]
#[derive(Debug)]
pub(crate) enum StmtError {
    /// A `UNIQUE` violation (MySQL 1062, Postgres 23505), carrying the server's
    /// message. The sinks only produce it in `insert_mode = "plain"`; in the
    /// idempotent modes the server never raises one.
    DupKey(String),
    /// Anything else, already classified by the sink's own `classify`.
    Other(SqlSinkError),
}

/// Writes `rows` with one multi-row `INSERT`, and resolves a duplicate-key
/// failure instead of retrying the identical statement forever (spec §4.7).
///
/// # Why
///
/// In plain mode a duplicate key used to be transient. The statement is
/// all-or-nothing, so the drain re-sent exactly the same `INSERT` and hit the
/// same `UNIQUE` key on every attempt and after every restart: a livelock. The
/// commonest trigger needs no concurrent writer at all — a `commit_timeout` on
/// a commit the server *did* apply leaves `durable_through` where it was, and
/// the retry collides with its own rows.
///
/// # What it does
///
/// - **First statement.** All rows, one statement — so a batch without
///   duplicates still costs exactly one round trip. Success commits all; a
///   transient error is returned as is; any other permanent error
///   dead-letters the whole batch with that reason (unchanged behaviour).
/// - **Duplicate key, with ids** (`ids` is `Some`: an `id_column` is set).
///   An id is the record's WAB coordinate hashed with its bytes, so a row whose
///   id is already present *is this record, already delivered*. One
///   `lookup_present` over every id moves the present rows to `committed`, then
///   one `INSERT` writes the remainder. Progress is monotonic across attempts:
///   rows an earlier attempt inserted before timing out are found by the
///   lookup. A duplicate key on the remainder means a concurrent writer or a
///   *different* `UNIQUE` constraint, and the remainder is bisected, each
///   duplicate row dead-lettered with the reason
///   `duplicate key (insert_mode=plain) on a record whose id was not present:
///   a different UNIQUE constraint or a concurrent writer: <server message>`.
///   The lookup needs `SELECT` on the table. If it fails permanently (the
///   role has `INSERT` only), the error is logged at WARN and the whole batch
///   is bisected; a duplicate row is then dead-lettered with
///   `duplicate key (insert_mode=plain); the id lookup failed (<lookup error>),
///   so the row could not be proven already delivered: <server message>`.
/// - **Duplicate key, without ids.** Bisection: split the failing set in
///   halves and try each. A half that inserts is committed. A single row with
///   a duplicate key is not provably the same record, so it is dead-lettered —
///   kept losslessly — with the reason
///   `duplicate key (insert_mode=plain, no id_column): <server message>`.
///   All rows duplicates costs 2N−1 statements.
/// - **Inside the fallback**, another permanent error is bisected down to its
///   row and dead-letters only that row, with its own reason; another
///   transient error returns transient for the whole batch (nothing is
///   reported committed, so the drain retries it all).
///
/// # The deadline
///
/// Every statement, the first included, is preceded by a deadline check and
/// bounded by `deadline` — the sink's existing per-commit `timeout`, not a new
/// one. Passing it returns [`SqlSinkError::Timeout`] (transient). That bounds
/// the fallback, and leaves one residual the spec records as unresolved: an
/// all-duplicate batch without ids whose (2N−1) round trips exceed the timeout
/// never completes. The config-load WARN for plain mode without an
/// `id_column` is how an operator hears about that before it happens.
///
/// # The partition
///
/// On `Ok`, every index of `rows` appears exactly once across `committed` and
/// `dead_lettered`, each in batch order. A row is reported committed only
/// after a statement containing it succeeded or the lookup found its id —
/// never on the strength of a statement that failed.
///
/// # Shape
///
/// The driver owns the decisions; the sink owns only the statements. `insert`
/// writes the rows at the given indices as one multi-row `INSERT`;
/// `lookup_present` returns which of the given indices' ids exist in the table
/// (it is only called when `ids` is `Some`). Owned index vectors keep the
/// returned futures free of borrows from the arguments.
#[cfg(any(feature = "mysql-sink", feature = "postgres-sink"))]
pub(crate) async fn insert_with_dup_fallback<I, IFut, L, LFut>(
    driver: &'static str,
    rows: &[weir_core::Payload],
    ids: Option<&[String]>,
    deadline: tokio::time::Instant,
    mut insert: I,
    mut lookup_present: L,
) -> Result<CommitResult, SqlSinkError>
where
    I: FnMut(Vec<usize>) -> IFut,
    IFut: std::future::Future<Output = Result<(), StmtError>>,
    L: FnMut(Vec<usize>) -> LFut,
    LFut: std::future::Future<Output = Result<std::collections::HashSet<String>, StmtError>>,
{
    let n = rows.len();

    // ── First statement: the whole batch ─────────────────────────────────────
    let first_dup = match bounded(driver, deadline, insert((0..n).collect())).await {
        Ok(()) => {
            tracing::debug!(
                records = n,
                "{driver} sink committed batch as single INSERT"
            );
            return Ok(CommitResult::new(rows.to_vec(), Vec::new()));
        }
        Err(StmtError::Other(e)) if e.is_transient() => return Err(e),
        Err(StmtError::Other(e)) => {
            let reason = e.to_string();
            tracing::warn!(
                records = n,
                error = %reason,
                "{driver} sink permanently rejected batch; dead-lettering"
            );
            let dead_lettered = rows.iter().map(|r| (r.clone(), reason.clone())).collect();
            return Ok(CommitResult::new(Vec::new(), dead_lettered));
        }
        Err(StmtError::DupKey(msg)) => msg,
    };

    let mut committed: Vec<usize> = Vec::with_capacity(n);
    let mut dead_lettered: Vec<(usize, String)> = Vec::new();
    // Set when an id lookup was attempted and rejected: the ids were then
    // never checked, and a duplicate row's reason has to say so.
    let mut lookup_failed: Option<String> = None;

    // ── Ids: one lookup, then the remainder ──────────────────────────────────
    let (to_bisect, failure) = match ids {
        Some(ids) => {
            match bounded(driver, deadline, lookup_present((0..n).collect())).await {
                Ok(present) => {
                    // A CHAR(n > 64) column hands its value back space-padded
                    // on Postgres; the ids themselves never carry padding.
                    let present: std::collections::HashSet<&str> = present
                        .iter()
                        .map(|id| id.trim_end_matches([' ', '\0']))
                        .collect();
                    let mut remainder = Vec::new();
                    for (i, id) in ids.iter().enumerate() {
                        if present.contains(id.as_str()) {
                            committed.push(i);
                        } else {
                            remainder.push(i);
                        }
                    }
                    if remainder.is_empty() {
                        (Vec::new(), None)
                    } else {
                        match bounded(driver, deadline, insert(remainder.clone())).await {
                            Ok(()) => {
                                committed.extend(&remainder);
                                (Vec::new(), None)
                            }
                            Err(StmtError::Other(e)) if e.is_transient() => return Err(e),
                            Err(err) => (remainder, Some(err)),
                        }
                    }
                }
                Err(StmtError::Other(e)) if e.is_transient() => return Err(e),
                // The lookup itself was rejected: the ingest role has INSERT
                // but no SELECT (Postgres 42501), or a column type the driver
                // cannot read back. Bisection needs no lookup and is lossless,
                // so fall back to it rather than dead-lettering — but the
                // error is the operator's to see, and it travels into every
                // duplicate row's reason (below).
                Err(err) => {
                    let lookup_err = match err {
                        StmtError::Other(e) => e.to_string(),
                        StmtError::DupKey(msg) => msg,
                    };
                    tracing::warn!(
                        records = n,
                        error = %lookup_err,
                        "{driver} sink: insert_mode=plain id lookup failed; bisecting the batch \
                         instead, and duplicate rows cannot be proven already delivered"
                    );
                    lookup_failed = Some(lookup_err);
                    ((0..n).collect(), Some(StmtError::DupKey(first_dup.clone())))
                }
            }
        }
        None => ((0..n).collect(), Some(StmtError::DupKey(first_dup.clone()))),
    };

    // ── Bisection ────────────────────────────────────────────────────────────
    if let Some(failure) = failure {
        let dup_reason = |msg: &str| match (ids, &lookup_failed) {
            (None, _) => format!("duplicate key (insert_mode=plain, no id_column): {msg}"),
            // The ids were never checked; on a replay the row is most likely
            // already delivered, so the reason must not suggest otherwise —
            // `dl requeue` would write it again under a new id.
            (Some(_), Some(lookup_err)) => format!(
                "duplicate key (insert_mode=plain); the id lookup failed ({lookup_err}), \
                 so the row could not be proven already delivered: {msg}"
            ),
            (Some(_), None) => format!(
                "duplicate key (insert_mode=plain) on a record whose id was not present: \
                 a different UNIQUE constraint or a concurrent writer: {msg}"
            ),
        };
        // `set` is known to have failed with `err`: dead-letter a single row,
        // split anything larger. Left half is pushed last so it runs first.
        let mut stack: Vec<Vec<usize>> = Vec::new();
        let mut on_failure = |mut set: Vec<usize>, err: StmtError, stack: &mut Vec<Vec<usize>>| {
            if set.len() == 1 {
                let reason = match err {
                    StmtError::DupKey(msg) => dup_reason(&msg),
                    StmtError::Other(e) => e.to_string(),
                };
                dead_lettered.push((set[0], reason));
            } else {
                let right = set.split_off(set.len() / 2);
                stack.push(right);
                stack.push(set);
            }
        };
        on_failure(to_bisect, failure, &mut stack);
        while let Some(set) = stack.pop() {
            match bounded(driver, deadline, insert(set.clone())).await {
                Ok(()) => committed.extend(&set),
                Err(StmtError::Other(e)) if e.is_transient() => return Err(e),
                Err(err) => on_failure(set, err, &mut stack),
            }
        }
    }

    committed.sort_unstable();
    dead_lettered.sort_unstable_by_key(|(i, _)| *i);
    debug_assert_eq!(
        committed.len() + dead_lettered.len(),
        n,
        "committed ∪ dead_lettered must partition the batch"
    );
    // A replay resolved entirely by id is routine after a commit timeout, so
    // it is INFO; a row sent to the dead-letter store is the operator's to see.
    if dead_lettered.is_empty() {
        tracing::info!(
            records = n,
            committed = committed.len(),
            with_id_column = ids.is_some(),
            first_error = %first_dup,
            "{driver} sink: duplicate key in insert_mode=plain resolved; every row committed"
        );
    } else {
        tracing::warn!(
            records = n,
            committed = committed.len(),
            dead_lettered = dead_lettered.len(),
            with_id_column = ids.is_some(),
            lookup_error = lookup_failed.as_deref(),
            first_error = %first_dup,
            "{driver} sink: duplicate key in insert_mode=plain; dead-lettering the duplicate rows"
        );
    }
    Ok(CommitResult::new(
        committed.into_iter().map(|i| rows[i].clone()).collect(),
        dead_lettered
            .into_iter()
            .map(|(i, reason)| (rows[i].clone(), reason))
            .collect(),
    ))
}

/// One statement, preceded by a deadline check and bounded by the deadline.
/// Either way out is [`SqlSinkError::Timeout`]: transient, so the drain
/// retries the batch.
#[cfg(any(feature = "mysql-sink", feature = "postgres-sink"))]
async fn bounded<T>(
    driver: &'static str,
    deadline: tokio::time::Instant,
    stmt: impl std::future::Future<Output = Result<T, StmtError>>,
) -> Result<T, StmtError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(StmtError::Other(SqlSinkError::timeout(driver)));
    }
    tokio::time::timeout_at(deadline, stmt)
        .await
        .unwrap_or_else(|_elapsed| Err(StmtError::Other(SqlSinkError::timeout(driver))))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_identifier ─────────────────────────────────────────────────

    #[test]
    fn valid_identifiers_accepted() {
        // Common shapes both sinks would see in practice.
        assert!(validate_identifier("t", "weir_records", 64).is_ok());
        assert!(validate_identifier("t", "_internal", 64).is_ok());
        assert!(validate_identifier("t", "T123", 64).is_ok());
        assert!(validate_identifier("t", "a", 64).is_ok());
    }

    #[test]
    fn at_max_length_accepted_and_one_over_rejected() {
        // 64-char limit (MySQL).
        let max = "a".repeat(64);
        assert!(validate_identifier("t", &max, 64).is_ok());
        let over = "a".repeat(65);
        assert!(validate_identifier("t", &over, 64).is_err());

        // 63-char limit (Postgres). Parametric `max_len` is the whole
        // point of moving validate_identifier here.
        let pg_max = "a".repeat(63);
        assert!(validate_identifier("t", &pg_max, 63).is_ok());
        let pg_over = "a".repeat(64);
        assert!(validate_identifier("t", &pg_over, 63).is_err());
    }

    #[test]
    fn injection_attempts_rejected() {
        // The whole point of identifier validation is to make these
        // SQL-string-builder inputs unrepresentable. If any of these
        // start being accepted, the SQL builder's `format!` becomes an
        // injection sink.
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
            "tab\there",
            "newline\nhere",
            "null\0byte",
            "unicode_éaccent",
            &"a".repeat(65),
        ] {
            assert!(
                validate_identifier("t", bad, 64).is_err(),
                "identifier {bad:?} should have been rejected"
            );
        }
    }

    #[test]
    fn invalid_identifier_carries_field_and_value() {
        let err = validate_identifier("table", "1bad", 64).unwrap_err();
        assert_eq!(err.field, "table");
        assert_eq!(err.value, "1bad");
    }

    // ── redact_password ─────────────────────────────────────────────────────

    #[test]
    fn redact_password_replaces_secret() {
        let url = "mysql://alice:s3cret@db.example.com:3306/weir";
        let r = redact_password(url);
        assert!(!r.contains("s3cret"), "password leaked: {r}");
        assert!(r.contains("alice"), "user should still appear: {r}");
        assert!(
            r.contains("db.example.com"),
            "host should still appear: {r}"
        );
        assert!(r.contains("<redacted>"), "redaction marker missing: {r}");
    }

    #[test]
    fn redact_password_works_for_postgres_urls_too() {
        // The function is dialect-agnostic — same logic for both.
        let url = "postgres://bob:hunter2@pg.example.com:5432/weir";
        let r = redact_password(url);
        assert!(!r.contains("hunter2"));
        assert!(r.contains("bob"));
        assert!(r.contains("pg.example.com"));
    }

    #[test]
    fn redact_password_leaves_unauthenticated_urls_alone() {
        // No user:pass@ component → nothing to redact, URL returned
        // unchanged. (Important: must NOT crash on malformed inputs.)
        assert_eq!(
            redact_password("mysql://localhost:3306/weir"),
            "mysql://localhost:3306/weir"
        );
        assert_eq!(redact_password("postgres://host/db"), "postgres://host/db");
    }

    #[test]
    fn redact_password_leaves_malformed_urls_alone() {
        // Function must be infallible — Debug formatting can't fail.
        assert_eq!(redact_password(""), "");
        assert_eq!(redact_password("not-a-url"), "not-a-url");
        assert_eq!(redact_password("://no-scheme@host"), "://no-scheme@host");
    }

    #[test]
    fn redact_password_handles_special_chars_in_password() {
        // URL-encoded password should still be fully redacted.
        let url = "postgres://user:p%40ss%21@host/db";
        let r = redact_password(url);
        assert!(!r.contains("p%40ss%21"), "url-encoded password leaked: {r}");
    }

    #[test]
    fn redact_password_redacts_password_containing_at_sign() {
        // A raw '@' in the password must not split mid-password and leak the tail.
        let url = "mysql://alice:p@ss@word@db.example.com:3306/weir";
        let r = redact_password(url);
        assert!(!r.contains("p@ss@word"), "password leaked: {r}");
        assert!(!r.contains("ss@word"), "password tail leaked: {r}");
        assert!(r.contains("alice"), "user should remain: {r}");
        assert!(r.contains("db.example.com"), "host should remain: {r}");
        assert!(r.contains("<redacted>"), "redaction marker missing: {r}");
    }

    // ── SqlSinkError ────────────────────────────────────────────────────────

    #[test]
    fn sql_sink_error_is_transient_classification() {
        assert!(SqlSinkError::transient("mysql", "io: broken pipe").is_transient());
        assert!(SqlSinkError::timeout("postgres").is_transient());
        assert!(!SqlSinkError::permanent("mysql", "bad schema").is_transient());
    }

    #[test]
    fn sql_sink_error_display_carries_driver_name() {
        // Driver context survives into the Display string — operators
        // reading a log line can tell mysql sink errors from postgres
        // sink errors without inspecting other context.
        let m = SqlSinkError::transient("mysql", "io: connection reset");
        let p = SqlSinkError::permanent("postgres", "syntax error at $1");
        let t = SqlSinkError::timeout("mysql");

        assert_eq!(format!("{m}"), "mysql sink transient: io: connection reset");
        assert_eq!(
            format!("{p}"),
            "postgres sink permanent: syntax error at $1"
        );
        assert_eq!(format!("{t}"), "mysql sink timeout");
    }
}

// ── Plain-mode duplicate-key fallback: tests against a fake executor ──────────

#[cfg(all(test, any(feature = "mysql-sink", feature = "postgres-sink")))]
mod dup_fallback_tests {
    //! The driver is exercised against an in-memory table rather than a server.
    //! It reproduces what matters about a real multi-row `INSERT` in autocommit:
    //! the statement is all-or-nothing, and its error is the first failing
    //! row's. The live-server versions of the spec §4.7 tests are the
    //! `#[ignore]`d ones in `mysql.rs` / `postgres.rs`, run by
    //! `deploy/run-sink-integration-tests.sh`.

    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::time::Instant;
    use weir_core::Payload;

    const SERVER_DUP: &str = "server 1062: Duplicate entry";
    const LOOKUP_DENIED: &str = "server 42501: permission denied for table weir_records";

    /// Which column the fake table's `UNIQUE` constraint is on.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Unique {
        /// `UNIQUE (record_id)` — pairs with an `id_column`.
        Id,
        /// `UNIQUE (payload)` — the content-keyed schema, no `id_column`.
        Payload,
    }

    struct FakeTable {
        unique: Unique,
        /// The rows stored, as (id, payload).
        rows: RefCell<Vec<(Option<String>, Vec<u8>)>>,
        /// Payloads the server rejects permanently (e.g. "data too long").
        permanent: HashSet<Vec<u8>>,
        /// 1-based statement number that fails transiently.
        transient_on: Option<usize>,
        /// The lookup is rejected permanently (e.g. the ingest role has
        /// INSERT but no SELECT: Postgres 42501).
        lookup_denied: bool,
        /// Per-statement latency, applied AFTER the statement has taken
        /// effect — so a statement that outlives the deadline has still been
        /// applied, which is exactly the `commit_timeout` case in §4.7.
        latency: Duration,
        statements: Cell<usize>,
        lookups: Cell<usize>,
    }

    impl FakeTable {
        fn new(unique: Unique) -> Self {
            Self {
                unique,
                rows: RefCell::new(Vec::new()),
                permanent: HashSet::new(),
                transient_on: None,
                lookup_denied: false,
                latency: Duration::ZERO,
                statements: Cell::new(0),
                lookups: Cell::new(0),
            }
        }

        fn key(&self, id: Option<&String>, payload: &[u8]) -> Vec<u8> {
            match self.unique {
                Unique::Id => id.expect("a UNIQUE on id needs ids").clone().into_bytes(),
                Unique::Payload => payload.to_vec(),
            }
        }

        fn bump(&self) -> usize {
            let n = self.statements.get() + 1;
            self.statements.set(n);
            n
        }

        async fn insert(
            &self,
            rows: &[Payload],
            ids: Option<&[String]>,
            idx: Vec<usize>,
        ) -> Result<(), StmtError> {
            let n = self.bump();
            if self.transient_on == Some(n) {
                return Err(StmtError::Other(SqlSinkError::transient(
                    "mysql",
                    "io: connection reset",
                )));
            }
            let mut present: HashSet<Vec<u8>> = self
                .rows
                .borrow()
                .iter()
                .map(|(id, p)| self.key(id.as_ref(), p))
                .collect();
            // All-or-nothing: the first failing row decides the error, and a
            // failing statement stores nothing.
            for &i in &idx {
                let id = ids.map(|ids| &ids[i]);
                if self.permanent.contains(rows[i].as_ref()) {
                    return Err(StmtError::Other(SqlSinkError::permanent(
                        "mysql",
                        "server 1406: Data too long for column 'payload'",
                    )));
                }
                if !present.insert(self.key(id, &rows[i])) {
                    return Err(StmtError::DupKey(SERVER_DUP.to_string()));
                }
            }
            self.rows.borrow_mut().extend(
                idx.iter()
                    .map(|&i| (ids.map(|ids| ids[i].clone()), rows[i].to_vec())),
            );
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            Ok(())
        }

        async fn lookup(
            &self,
            ids: &[String],
            idx: Vec<usize>,
        ) -> Result<HashSet<String>, StmtError> {
            let n = self.bump();
            self.lookups.set(self.lookups.get() + 1);
            if self.transient_on == Some(n) {
                return Err(StmtError::Other(SqlSinkError::transient(
                    "mysql",
                    "io: connection reset",
                )));
            }
            if self.lookup_denied {
                return Err(StmtError::Other(SqlSinkError::permanent(
                    "postgres",
                    LOOKUP_DENIED,
                )));
            }
            let stored: HashSet<String> = self
                .rows
                .borrow()
                .iter()
                .filter_map(|(id, _)| id.clone())
                .collect();
            Ok(idx
                .iter()
                .map(|&i| ids[i].clone())
                .filter(|id| stored.contains(id))
                .collect())
        }

        fn stored_payloads(&self) -> Vec<Vec<u8>> {
            self.rows.borrow().iter().map(|(_, p)| p.clone()).collect()
        }
    }

    fn payloads(names: &[&str]) -> Vec<Payload> {
        names
            .iter()
            .map(|n| Payload::copy_from_slice(n.as_bytes()))
            .collect()
    }

    /// Ids exactly as the drain derives them: the segment coordinate, so a
    /// replay of the same segment carries the same ids.
    fn ids_for(rows: &[Payload]) -> Vec<String> {
        rows.iter()
            .enumerate()
            .map(|(i, p)| {
                weir_sink_sdk::RecordId::for_record(
                    "shard_00/seg_00000001.wab.sealed",
                    i as u64 + 1,
                    p,
                )
                .to_hex()
            })
            .collect()
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    async fn run(
        table: &FakeTable,
        rows: &[Payload],
        ids: Option<&[String]>,
        deadline: Instant,
    ) -> Result<CommitResult, SqlSinkError> {
        insert_with_dup_fallback(
            "mysql",
            rows,
            ids,
            deadline,
            |idx| table.insert(rows, ids, idx),
            |idx| table.lookup(ids.expect("lookup only runs with ids"), idx),
        )
        .await
    }

    fn names(ps: &[Payload]) -> Vec<String> {
        ps.iter()
            .map(|p| String::from_utf8(p.to_vec()).unwrap())
            .collect()
    }

    fn dl_names(dl: &[(Payload, String)]) -> Vec<String> {
        dl.iter()
            .map(|(p, _)| String::from_utf8(p.to_vec()).unwrap())
            .collect()
    }

    // ── The two spec scenarios ───────────────────────────────────────────────

    /// §4.7, id path: `[a,b]` then `[a,b,c]`. a and b are already present under
    /// the same ids, so they are the same records already delivered: committed,
    /// not dead-lettered, and not written twice.
    #[tokio::test]
    async fn id_path_a_replay_counts_present_rows_as_committed() {
        let table = FakeTable::new(Unique::Id);
        let all = payloads(&["a", "b", "c"]);
        let ids = ids_for(&all);

        let first = run(&table, &all[..2], Some(&ids[..2]), far())
            .await
            .unwrap();
        assert_eq!(names(&first.committed), ["a", "b"]);

        table.statements.set(0);
        let second = run(&table, &all, Some(&ids), far()).await.unwrap();
        assert_eq!(names(&second.committed), ["a", "b", "c"]);
        assert!(
            second.dead_lettered.is_empty(),
            "{:?}",
            second.dead_lettered
        );
        assert_eq!(
            table.stored_payloads(),
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            table.statements.get(),
            3,
            "a constant number of round trips: the failed INSERT, one lookup, the remainder"
        );
    }

    /// §4.7, no `id_column`: `[a,b]` then `[a,b,c]`. a and b are not provably the
    /// same records, so they are kept losslessly in the DL, with the reason.
    #[tokio::test]
    async fn bisection_path_dead_letters_the_duplicate_rows_with_the_reason() {
        let table = FakeTable::new(Unique::Payload);
        let all = payloads(&["a", "b", "c"]);

        run(&table, &all[..2], None, far()).await.unwrap();
        let second = run(&table, &all, None, far()).await.unwrap();

        assert_eq!(names(&second.committed), ["c"]);
        assert_eq!(dl_names(&second.dead_lettered), ["a", "b"]);
        for (_, reason) in &second.dead_lettered {
            assert_eq!(
                reason,
                &format!("duplicate key (insert_mode=plain, no id_column): {SERVER_DUP}")
            );
        }
        assert_eq!(table.stored_payloads().len(), 3);
    }

    // ── Cost ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_batch_without_duplicates_still_costs_one_statement() {
        for (unique, with_ids) in [(Unique::Payload, false), (Unique::Id, true)] {
            let table = FakeTable::new(unique);
            let rows = payloads(&["a", "b", "c", "d"]);
            let ids = ids_for(&rows);
            let r = run(&table, &rows, with_ids.then_some(&ids[..]), far())
                .await
                .unwrap();
            assert_eq!(r.committed.len(), 4);
            assert_eq!(table.statements.get(), 1, "{unique:?}");
        }
    }

    /// The spec's worst case: every row a duplicate costs 2N−1 statements.
    #[tokio::test]
    async fn bisection_worst_case_is_2n_minus_1_statements() {
        let table = FakeTable::new(Unique::Payload);
        let rows = payloads(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        run(&table, &rows, None, far()).await.unwrap();
        table.statements.set(0);

        let r = run(&table, &rows, None, far()).await.unwrap();
        assert!(r.committed.is_empty());
        assert_eq!(r.dead_lettered.len(), 8);
        assert_eq!(table.statements.get(), 2 * 8 - 1);
    }

    // ── Deadline ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_passed_deadline_is_transient_and_runs_no_statement() {
        let table = FakeTable::new(Unique::Payload);
        let rows = payloads(&["a"]);
        let err = run(
            &table,
            &rows,
            None,
            Instant::now() - Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(err.is_transient(), "{err}");
        assert!(matches!(err, SqlSinkError::Timeout { .. }), "{err}");
        assert_eq!(table.statements.get(), 0);
    }

    /// The deadline is checked before each statement of the fallback, not only
    /// around the whole thing — so an all-duplicate batch cannot run past the
    /// sink timeout one statement at a time.
    #[tokio::test]
    async fn a_deadline_passing_mid_bisection_is_transient() {
        let mut table = FakeTable::new(Unique::Payload);
        let rows = payloads(&["a", "b", "c", "d", "e", "f", "g", "h"]);
        run(&table, &rows, None, far()).await.unwrap();
        table.statements.set(0);
        table.latency = Duration::from_millis(30);

        // Dup-key statements fail before the fake's latency, so only the
        // successful ones are slow. Interleave fresh rows with duplicates so the
        // bisection reaches slow single-row successes early: `i` succeeds at
        // ~30 ms, `j` at ~60 ms, and the check before the next statement trips.
        let fresh = payloads(&["i", "j", "k", "l"]);
        let mixed: Vec<Payload> = fresh
            .iter()
            .zip(&rows[..4])
            .flat_map(|(f, d)| [f.clone(), d.clone()])
            .collect();
        let err = run(
            &table,
            &mixed,
            None,
            Instant::now() + Duration::from_millis(45),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SqlSinkError::Timeout { .. }), "{err}");
        assert!(
            table.statements.get() < 2 * 8 - 1,
            "the fallback must stop at the deadline, ran {} statements",
            table.statements.get()
        );
    }

    /// §4.7: a commit the server applied but the client timed out on. The next
    /// attempt finds every row by id: all committed, nothing dead-lettered.
    #[tokio::test]
    async fn an_attempt_that_applied_but_timed_out_is_completed_by_the_next() {
        let mut table = FakeTable::new(Unique::Id);
        let rows = payloads(&["a", "b", "c"]);
        let ids = ids_for(&rows);
        table.latency = Duration::from_millis(200);

        let err = run(
            &table,
            &rows,
            Some(&ids),
            Instant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SqlSinkError::Timeout { .. }), "{err}");
        assert_eq!(
            table.stored_payloads().len(),
            3,
            "precondition: the server applied it"
        );

        table.latency = Duration::ZERO;
        let r = run(&table, &rows, Some(&ids), far()).await.unwrap();
        assert_eq!(names(&r.committed), ["a", "b", "c"]);
        assert!(r.dead_lettered.is_empty());
        assert_eq!(table.stored_payloads().len(), 3, "nothing written twice");
    }

    // ── Other errors inside the fallback ─────────────────────────────────────

    #[tokio::test]
    async fn another_permanent_error_dead_letters_only_that_row() {
        // No id_column: `a` is a duplicate, `c` is permanently rejected.
        let mut table = FakeTable::new(Unique::Payload);
        table.permanent.insert(b"c".to_vec());
        run(&table, &payloads(&["a"]), None, far()).await.unwrap();

        let r = run(&table, &payloads(&["a", "b", "c", "d"]), None, far())
            .await
            .unwrap();
        assert_eq!(names(&r.committed), ["b", "d"]);
        assert_eq!(dl_names(&r.dead_lettered), ["a", "c"]);
        assert!(r.dead_lettered[0].1.starts_with("duplicate key"));
        assert!(
            r.dead_lettered[1].1.contains("Data too long"),
            "the permanent row keeps its own reason: {}",
            r.dead_lettered[1].1
        );

        // With an id_column: `a` is present (a replay), `c` permanent.
        let mut table = FakeTable::new(Unique::Id);
        table.permanent.insert(b"c".to_vec());
        let rows = payloads(&["a", "b", "c", "d"]);
        let ids = ids_for(&rows);
        run(&table, &rows[..1], Some(&ids[..1]), far())
            .await
            .unwrap();
        let r = run(&table, &rows, Some(&ids), far()).await.unwrap();
        assert_eq!(names(&r.committed), ["a", "b", "d"]);
        assert_eq!(dl_names(&r.dead_lettered), ["c"]);
        assert!(r.dead_lettered[0].1.contains("Data too long"));
    }

    /// A lookup the server rejects permanently (an ingest role with INSERT but
    /// no SELECT) falls back to bisection. The ids were never checked, so a
    /// duplicate row's reason must say the lookup failed and carry its error,
    /// not claim the id "was not present" — on a replay it almost certainly
    /// is, and that claim would steer the operator to `dl requeue`, which
    /// writes real downstream duplicates.
    #[tokio::test]
    async fn a_permanently_failed_lookup_says_so_in_the_reason() {
        let mut table = FakeTable::new(Unique::Id);
        let rows = payloads(&["a", "b", "c"]);
        let ids = ids_for(&rows);
        run(&table, &rows[..2], Some(&ids[..2]), far())
            .await
            .unwrap();
        table.lookup_denied = true;

        let r = run(&table, &rows, Some(&ids), far()).await.unwrap();
        assert_eq!(names(&r.committed), ["c"]);
        assert_eq!(dl_names(&r.dead_lettered), ["a", "b"]);
        let lookup_err = SqlSinkError::permanent("postgres", LOOKUP_DENIED).to_string();
        for (_, reason) in &r.dead_lettered {
            assert_eq!(
                reason,
                &format!(
                    "duplicate key (insert_mode=plain); the id lookup failed ({lookup_err}), \
                     so the row could not be proven already delivered: {SERVER_DUP}"
                )
            );
            assert!(!reason.contains("was not present"), "{reason}");
        }
    }

    /// The other id-path reason: the lookup succeeded, the row's id was not
    /// there, and its insert still hit a duplicate — a different UNIQUE
    /// constraint or a concurrent writer.
    #[tokio::test]
    async fn a_duplicate_after_a_successful_lookup_keeps_the_not_present_reason() {
        // UNIQUE on payload, but ids supplied: `a` is stored under a different
        // id (another writer), so the lookup misses it and the insert collides.
        let table = FakeTable::new(Unique::Payload);
        let rows = payloads(&["a", "b"]);
        let ids = ids_for(&rows);
        table
            .rows
            .borrow_mut()
            .push((Some("someone-else".to_string()), b"a".to_vec()));

        let r = run(&table, &rows, Some(&ids), far()).await.unwrap();
        assert_eq!(names(&r.committed), ["b"]);
        assert_eq!(dl_names(&r.dead_lettered), ["a"]);
        assert_eq!(
            r.dead_lettered[0].1,
            format!(
                "duplicate key (insert_mode=plain) on a record whose id was not present: \
                 a different UNIQUE constraint or a concurrent writer: {SERVER_DUP}"
            )
        );
    }

    #[tokio::test]
    async fn another_transient_error_makes_the_whole_batch_transient() {
        // No id_column: the second statement (first bisection half) fails.
        let mut table = FakeTable::new(Unique::Payload);
        run(&table, &payloads(&["a"]), None, far()).await.unwrap();
        table.statements.set(0);
        table.transient_on = Some(2);
        let err = run(&table, &payloads(&["a", "b", "c", "d"]), None, far())
            .await
            .unwrap_err();
        assert!(err.is_transient(), "{err}");

        // With an id_column: the lookup (statement 2) fails.
        let mut table = FakeTable::new(Unique::Id);
        let rows = payloads(&["a", "b"]);
        let ids = ids_for(&rows);
        run(&table, &rows[..1], Some(&ids[..1]), far())
            .await
            .unwrap();
        table.statements.set(0);
        table.transient_on = Some(2);
        let err = run(&table, &rows, Some(&ids), far()).await.unwrap_err();
        assert!(err.is_transient(), "{err}");
        assert_eq!(table.lookups.get(), 1);
    }

    /// Unchanged behaviour: a permanent error on the FIRST statement (no
    /// duplicate involved) dead-letters the whole batch with that reason, in
    /// one statement, exactly as before.
    #[tokio::test]
    async fn a_permanent_first_statement_still_dead_letters_the_whole_batch() {
        let mut table = FakeTable::new(Unique::Payload);
        table.permanent.insert(b"a".to_vec());
        let r = run(&table, &payloads(&["a", "b"]), None, far())
            .await
            .unwrap();
        assert!(r.committed.is_empty());
        assert_eq!(dl_names(&r.dead_lettered), ["a", "b"]);
        assert_eq!(table.statements.get(), 1);
    }

    // ── The partition ────────────────────────────────────────────────────────

    #[derive(Clone, Copy, Debug)]
    enum Row {
        New,
        Dup,
        Permanent,
    }

    fn row_strategy() -> impl proptest::strategy::Strategy<Value = Row> {
        use proptest::prelude::*;
        prop_oneof![6 => Just(Row::New), 3 => Just(Row::Dup), 1 => Just(Row::Permanent)]
    }

    proptest::proptest! {
        /// `committed ∪ dead_lettered = batch` (the drain checks the count at
        /// drain/mod.rs, but identity is this driver's job), every committed row
        /// is really in the table — never a false ack — and nothing is stored
        /// twice under the UNIQUE key.
        #[test]
        fn the_partition_always_holds(
            shape in proptest::collection::vec(row_strategy(), 1..24),
            with_ids in proptest::bool::ANY,
            transient_on in proptest::option::weighted(0.2, 1usize..8),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            rt.block_on(async {
                let mut table = FakeTable::new(if with_ids { Unique::Id } else { Unique::Payload });
                let rows: Vec<Payload> = (0..shape.len())
                    .map(|i| Payload::copy_from_slice(format!("row-{i:02}").as_bytes()))
                    .collect();
                let ids = ids_for(&rows);
                for (i, kind) in shape.iter().enumerate() {
                    match kind {
                        Row::Dup => table.rows.borrow_mut().push((Some(ids[i].clone()), rows[i].to_vec())),
                        Row::Permanent => { table.permanent.insert(rows[i].to_vec()); }
                        Row::New => {}
                    }
                }
                table.transient_on = transient_on;
                let ids_arg = with_ids.then_some(&ids[..]);

                match run(&table, &rows, ids_arg, far()).await {
                    Err(e) => {
                        assert!(e.is_transient(), "only a transient fault may fail the batch: {e}");
                        assert!(transient_on.is_some(), "no fault injected, yet: {e}");
                    }
                    Ok(r) => {
                        let mut seen: Vec<String> = names(&r.committed);
                        seen.extend(dl_names(&r.dead_lettered));
                        seen.sort();
                        let mut want = names(&rows);
                        want.sort();
                        assert_eq!(seen, want, "committed ∪ dead_lettered must be the batch, once each");

                        let stored = table.stored_payloads();
                        for c in &r.committed {
                            assert!(stored.contains(&c.to_vec()), "false ack: {c:?} committed but not stored");
                        }
                        let mut uniq = stored.clone();
                        uniq.sort();
                        uniq.dedup();
                        assert_eq!(uniq.len(), stored.len(), "a row stored twice");

                        // A permanent error on the FIRST statement (the first
                        // failing row is not a duplicate) keeps today's
                        // behaviour: the whole batch is dead-lettered.
                        let first_failure = shape.iter().find(|k| !matches!(k, Row::New));
                        if matches!(first_failure, Some(Row::Permanent)) {
                            assert!(r.committed.is_empty());
                            return;
                        }
                        for (i, kind) in shape.iter().enumerate() {
                            let name = format!("row-{i:02}");
                            match kind {
                                Row::Permanent => assert!(dl_names(&r.dead_lettered).contains(&name)),
                                // With ids, a present row is the same record:
                                // committed, never dead-lettered.
                                Row::Dup if with_ids => assert!(names(&r.committed).contains(&name)),
                                Row::Dup => assert!(dl_names(&r.dead_lettered).contains(&name)),
                                Row::New => {}
                            }
                        }
                    }
                }
            });
        }
    }
}
