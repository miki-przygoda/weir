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
