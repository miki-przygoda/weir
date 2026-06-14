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
pub(super) fn redact_password(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    // The userinfo lives in the authority, which ends at the first '/', '?', or
    // '#'. Restrict the search there so a '@' in the path/query can't be mistaken
    // for the userinfo/host separator.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    // The userinfo/host boundary is the LAST '@' in the authority: a password may
    // contain unencoded '@' characters, so the FIRST '@' would split mid-password
    // and leak the tail.
    let Some(at) = rest[..authority_end].rfind('@') else {
        return url.to_string();
    };
    let creds = &rest[..at];
    // user:password splits at the FIRST ':' — a username carries no unencoded ':'.
    let Some(colon) = creds.find(':') else {
        return url.to_string();
    };
    let user = &creds[..colon];
    let tail = &rest[at..];
    format!("{}://{}:<redacted>{}", &url[..scheme_end], user, tail)
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
