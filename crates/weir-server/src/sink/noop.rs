//! Pass-through sink. Accepts every record, forwards nothing.
//!
//! Used as the default `sink_type = "noop"` to make the daemon runnable
//! without a downstream and as a known-good sink in test scenarios.

use weir_core::Payload;

use super::{CommitResult, Sink, SinkError, SinkHealth};

pub struct NoopSink;

#[derive(Debug)]
pub struct NoopError;

impl std::fmt::Display for NoopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "noop error (unreachable)")
    }
}

impl std::error::Error for NoopError {}

impl SinkError for NoopError {
    fn is_transient(&self) -> bool {
        false
    }
}

impl Sink for NoopSink {
    type Record = Payload;
    type Error = NoopError;

    async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, NoopError> {
        Ok(CommitResult {
            committed: batch,
            dead_lettered: vec![],
        })
    }

    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}
