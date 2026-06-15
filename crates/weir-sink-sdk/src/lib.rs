//! Sink trait and error contract for building sinks for the [weir] daemon.
//!
//! [weir]: https://github.com/miki-przygoda/weir
//!
//! A weir **sink** is a downstream commit target. The daemon's drain reads
//! batches of records out of sealed write-ahead-buffer segments and hands them to
//! a sink to commit to a database, HTTP endpoint, object store, etc. Implement
//! [`Sink`] (and [`SinkError`] for your error type); the drain retries transient
//! failures with backoff and dead-letters permanent ones.
//!
//! ```
//! use weir_sink_sdk::{CommitResult, Payload, Sink, SinkError, SinkHealth};
//!
//! struct StdoutSink;
//!
//! #[derive(Debug)]
//! struct Never;
//! impl std::fmt::Display for Never {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         write!(f, "unreachable")
//!     }
//! }
//! impl std::error::Error for Never {}
//! impl SinkError for Never {
//!     fn is_transient(&self) -> bool {
//!         true
//!     }
//! }
//!
//! impl Sink for StdoutSink {
//!     type Record = Payload;
//!     type Error = Never;
//!     async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, Never> {
//!         for r in &batch {
//!             println!("{} bytes", r.len());
//!         }
//!         Ok(CommitResult { committed: batch, dead_lettered: Vec::new() })
//!     }
//!     async fn health(&self) -> SinkHealth {
//!         SinkHealth::Healthy
//!     }
//! }
//! ```
//!
//! # Idempotency contract
//!
//! The drain guarantees **at-least-once delivery per segment**, not per record.
//! If the daemon crashes after a partial commit but before recording the segment
//! as confirmed, `commit` is called again with the full segment — including
//! records already committed. Implementations **must** handle duplicates
//! gracefully (upsert, `INSERT IGNORE`, a content-derived dedup key, etc.). This
//! is the explicit durability trade-off, not a protocol weakness.

// The drain is always generic over `S: Sink` and stores `Arc<S>` — it never uses
// `dyn Sink`. So the Send-bound ergonomics the `async_fn_in_trait` lint warns
// about do not apply here, and sink authors get clean `async fn` signatures.
#![allow(async_fn_in_trait)]
#![deny(missing_docs)]

/// Opaque record payload bytes (re-exported from `weir-core`). A newtype over
/// ref-counted `bytes::Bytes` that derefs to `[u8]`, so clones through the drain
/// are O(1).
pub use weir_core::Payload;

/// A record type carried through the drain pipeline.
///
/// The simplest implementation is `type Record = Payload` (opaque bytes). Richer
/// sinks can define a concrete record type that deserialises the payload.
pub trait SinkRecord: Send + 'static {
    /// Build the record from the raw payload bytes handed over by the drain.
    fn from_payload(payload: Payload) -> Self;
    /// Recover the raw payload bytes (used when a record must be dead-lettered).
    fn into_payload(self) -> Payload;
}

/// `Payload` is the pass-through record: the drain hands raw bytes to the sink
/// without interpretation.
impl SinkRecord for Payload {
    fn from_payload(payload: Payload) -> Self {
        payload
    }

    fn into_payload(self) -> Payload {
        self
    }
}

/// An error returned by [`Sink::commit`].
///
/// Implementations must classify every error as transient or permanent:
/// - **Transient**: the drain retries the whole segment with exponential backoff.
/// - **Permanent**: the affected records are dead-lettered and the segment is confirmed.
pub trait SinkError: Send + Sync + std::error::Error + 'static {
    /// Whether the drain should retry the segment (`true`) or dead-letter it (`false`).
    fn is_transient(&self) -> bool;

    /// Hint for how long to wait before retrying, e.g. parsed from an HTTP
    /// `Retry-After` header on a 429 / 503. The drain uses this in place of its
    /// exponential-backoff delay when present (subject to a sanity cap).
    fn retry_after(&self) -> Option<std::time::Duration> {
        None
    }
}

/// The result of a successful [`Sink::commit`].
#[derive(Debug)]
pub struct CommitResult<R> {
    /// Records the sink accepted.
    pub committed: Vec<R>,
    /// Records the sink permanently rejected, each with a human-readable reason.
    pub dead_lettered: Vec<(R, String)>,
}

/// Coarse health signal from [`Sink::health`].
#[derive(Clone, Debug)]
pub enum SinkHealth {
    /// The downstream is fully available.
    Healthy,
    /// The downstream is partially available / degraded; the reason is for operators.
    Degraded(String),
    /// The downstream is unavailable; the reason is for operators.
    Down(String),
}

/// A downstream commit target for weir records.
///
/// The drain calls [`commit`](Sink::commit) with batches of records read from
/// sealed segments. Implementations may be async (tokio, sqlx, reqwest, …); they
/// run on a dedicated single-threaded tokio runtime in the drain thread.
pub trait Sink: Send + Sync + 'static {
    /// The record type this sink consumes (often just [`Payload`]).
    type Record: SinkRecord;
    /// The error type this sink returns; must classify transient vs permanent.
    type Error: SinkError;

    /// Commit a batch of records. Returns the committed records and any
    /// permanently rejected ones. Return `Err(e)` with `e.is_transient() == true`
    /// to have the drain retry the whole segment.
    async fn commit(
        &self,
        batch: Vec<Self::Record>,
    ) -> Result<CommitResult<Self::Record>, Self::Error>;

    /// Maximum number of records per `commit` call. The drain splits larger
    /// segments into sub-batches of this size.
    fn max_batch_size(&self) -> usize {
        1000
    }

    /// Health probe feeding the daemon's `weir_sink_health` gauge. The drain
    /// calls it after a segment is processed in the Draining state, and on a
    /// wall-clock interval while idle or blocked on a full dead-letter dir — so
    /// the gauge keeps moving even when no segments are flowing. It is NOT called
    /// after every individual commit (retries don't re-probe). Keep it cheap (a
    /// single ping / HEAD) — it runs under a timeout backstop on the drain thread.
    async fn health(&self) -> SinkHealth;
}
