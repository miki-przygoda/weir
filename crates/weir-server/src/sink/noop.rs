//! Pass-through sink. Accepts every record, forwards nothing.
//!
//! Used as the default `sink_type = "noop"` to make the daemon runnable
//! without a downstream and as a known-good sink in test scenarios.

use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

pub struct NoopSink;

/// Inhabited but never constructed in the current implementation; the
/// `Sink` trait requires an associated `Error` type even when the impl
/// can't fail. Kept as an explicit type rather than `Infallible` so the
/// trait's variance is exactly the same shape as the HTTP and MySQL
/// sinks.
#[derive(Debug, thiserror::Error)]
#[error("noop error (unreachable)")]
pub struct NoopError;

impl SinkError for NoopError {
    fn is_transient(&self) -> bool {
        false
    }
}

impl Sink for NoopSink {
    type Record = Payload;
    type Error = NoopError;

    async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, NoopError> {
        Ok(CommitResult::new(batch, vec![]))
    }

    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}
