//! Built-in sink implementations + a re-export of the [`weir_sink_sdk`] contract.
//!
//! The `Sink` trait, its error/record/result types, and the idempotency contract
//! live in the published [`weir_sink_sdk`] crate so third parties can implement
//! sinks without depending on the daemon. This module re-exports them and houses
//! weir's own built-in sinks (feature-gated — see crate `[features]`):
//!
//! - [`noop::NoopSink`] — always compiled; accepts all records, forwards nothing.
//!   The default when `sink_type = "noop"`. Useful for soak-testing the pipeline.
//! - [`http::HttpSink`] — feature `http-sink`; POSTs each record to a configurable
//!   URL with transient/permanent error classification.
//! - [`mysql::MySqlSink`] — feature `mysql-sink`; writes a whole batch with one
//!   multi-row `INSERT`. The IOPS-compression sink: N records → 1 statement.
//! - [`postgres::PostgresSink`] — feature `postgres-sink`; Postgres counterpart,
//!   `ON CONFLICT DO NOTHING` for idempotency.
//! - [`clickhouse::ClickHouseSink`] — feature `clickhouse-sink`; HTTP
//!   `INSERT … FORMAT RowBinary` with a sha256 `insert_deduplication_token`.

#[cfg(feature = "clickhouse-sink")]
pub mod clickhouse;
#[cfg(feature = "http-sink")]
pub mod http;
#[cfg(feature = "mysql-sink")]
pub mod mysql;
pub mod noop;
#[cfg(feature = "postgres-sink")]
pub mod postgres;
#[cfg(feature = "_sql-sink")]
mod sql_common;

pub use weir_sink_sdk::{CommitResult, Sink, SinkError, SinkHealth, SinkRecord};

/// Redacts the password component of a URL for `Debug`/log output, returning
/// `scheme://user:<redacted>@host…`. Leaves URLs without userinfo (and
/// malformed URLs) untouched — it locates the password substring rather than
/// validating the URL, so debug formatting can never fail.
///
/// Lives here (always compiled) rather than in the feature-gated `sql_common`
/// so the config layer's `RedactedUrl` Debug impl can reuse one implementation
/// regardless of which sink features are built in (F59). The SQL sinks delegate
/// to it via `sql_common::redact_password`.
pub(crate) fn redact_url_password(url: &str) -> String {
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

/// Replaces ASCII/Unicode control characters with `.` for safe logging.
///
/// Downstream sink response bodies are interpolated into log lines and
/// dead-letter reason strings. Without this, a hostile or compromised endpoint
/// could embed newlines (forging extra log records) or terminal escape sequences
/// in its body and have the daemon emit them verbatim (S29). Only the HTTP and
/// ClickHouse sinks read a response body, so it is compiled only for those.
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) fn sanitize_log_excerpt(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '.' } else { c })
        .collect()
}

/// Upper bound on how many response-body bytes a sink will buffer. Success bodies
/// are discarded and error bodies are truncated to a short excerpt, so 64 KiB is
/// ample for any legitimate endpoint while capping peak memory at
/// `concurrency × 64 KiB` against a hostile/compromised downstream (S28).
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) const RESPONSE_BODY_CAP: usize = 64 * 1024;

/// Reads at most `cap` bytes of a response body, then stops — dropping the rest
/// (and the connection, which is fine: a partially-read response is not pooled).
/// Bounds memory regardless of what the downstream sends; the success path
/// discards the body and the error path only needs a short excerpt (S28).
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) async fn read_body_capped(mut resp: reqwest::Response, cap: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    while buf.len() < cap {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let take = (cap - buf.len()).min(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
                if take < chunk.len() {
                    break; // hit the cap mid-chunk
                }
            }
            Ok(None) => break, // end of body
            Err(_) => break,   // read error — return what we have so far
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_password_covers_adversarial_inputs() {
        // No userinfo → unchanged.
        assert_eq!(
            redact_url_password("https://host/path"),
            "https://host/path"
        );
        // Simple user:pass.
        assert_eq!(
            redact_url_password("https://u:p@host/path"),
            "https://u:<redacted>@host/path"
        );
        // Password containing '@' must split at the LAST '@', not leak the tail.
        assert_eq!(
            redact_url_password("https://u:p@ss@host/db"),
            "https://u:<redacted>@host/db"
        );
        // A '@' in the path must not be mistaken for the userinfo separator.
        assert_eq!(
            redact_url_password("https://host/p@th"),
            "https://host/p@th"
        );
        // Username only (no password) → unchanged (nothing to redact).
        assert_eq!(
            redact_url_password("https://user@host/x"),
            "https://user@host/x"
        );
        // The literal password must never survive in the output.
        let red = redact_url_password("postgres://admin:sup3rS3cret@db:5432/app");
        assert!(!red.contains("sup3rS3cret"), "password leaked: {red}");
        assert!(red.contains("<redacted>"));
    }

    #[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
    #[test]
    fn sanitize_log_excerpt_strips_control_chars() {
        assert_eq!(
            sanitize_log_excerpt("ok\n2026 INFO forged log line\r\x1b[31m"),
            "ok.2026 INFO forged log line..[31m"
        );
        // Normal text and non-control Unicode pass through untouched.
        assert_eq!(sanitize_log_excerpt("plain café"), "plain café");
    }
}
