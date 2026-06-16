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
