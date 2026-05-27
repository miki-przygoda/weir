//! Sink trait, error contract, and built-in implementations.
//!
//! Built-in sinks:
//! - [`noop::NoopSink`] — accepts all records, forwards nothing. The default
//!   when `sink_type = "noop"`. Useful for soak-testing the daemon pipeline
//!   without a real downstream.
//! - [`http::HttpSink`] — POSTs each record to a configurable URL with
//!   transient/permanent error classification. Use when `sink_type = "http"`.
//! - [`mysql::MySqlSink`] — writes a whole batch with one multi-row
//!   `INSERT` statement. The IOPS-compression sink: N records → 1 statement
//!   → 1 server-side commit. Use when `sink_type = "mysql"`.
//!
//! A `Sink` receives batches of records from the drain and commits them to a
//! downstream store. Implementations decide their own connection management and
//! retry policy for transient failures.
//!
//! # Idempotency contract
//!
//! The drain guarantees **at-least-once delivery per segment**, not per record.
//! If the daemon crashes after a partial commit but before writing the `.confirmed`
//! file, `commit` will be called again with the full segment — including records
//! already committed. Implementations **must** handle duplicates gracefully (e.g.
//! via upsert, `INSERT IGNORE`, or application-level dedup). This is not a protocol
//! weakness; it is the explicit durability trade-off.
//!
//! # Object safety
//!
//! `Sink` uses `async fn` in trait (AFIT, stable since Rust 1.75). The drain is
//! generic over `S: Sink` and stores `Arc<S>`, so no `dyn Sink` is needed. Using
//! `dyn Sink` with AFIT requires boxing the returned futures, which is left to the
//! caller if they need type erasure.

pub mod http;
pub mod mysql;
pub mod noop;

use weir_core::Payload;

/// A record type carried through the drain pipeline.
///
/// The simplest implementation is `type Record = Payload` (opaque bytes). Richer
/// sinks can define a concrete record type that deserialises the payload.
pub trait SinkRecord: Send + 'static {
    fn from_payload(payload: Payload) -> Self;
    fn into_payload(self) -> Payload;
}

/// `Payload` (= `Vec<u8>`) is the pass-through implementation: the drain hands
/// raw bytes to the sink without interpretation.
impl SinkRecord for Payload {
    fn from_payload(payload: Payload) -> Self {
        payload
    }

    fn into_payload(self) -> Payload {
        self
    }
}

/// An error returned by `Sink::commit`.
///
/// Implementations must classify every error as transient or permanent.
/// - **Transient**: the drain retries with exponential backoff (up to `MAX_RETRIES`).
/// - **Permanent**: the record is dead-lettered and the segment is confirmed.
pub trait SinkError: Send + Sync + std::error::Error + 'static {
    fn is_transient(&self) -> bool;

    /// Hint from the sink about how long to wait before retrying, e.g. parsed
    /// from an HTTP `Retry-After` header on a 429 / 503 response. The drain
    /// uses this in place of its exponential-backoff delay when present
    /// (subject to a sanity cap). Default: no hint.
    fn retry_after(&self) -> Option<std::time::Duration> {
        None
    }
}

/// The result of a successful `Sink::commit` call.
///
/// `committed`: records that were accepted by the sink.
/// `dead_lettered`: records the sink permanently rejected, with a reason string.
#[derive(Debug)]
pub struct CommitResult<R> {
    pub committed: Vec<R>,
    pub dead_lettered: Vec<(R, String)>,
}

/// Coarse health signal from `Sink::health`.
#[derive(Clone, Debug)]
pub enum SinkHealth {
    Healthy,
    Degraded(String),
    Down(String),
}

/// A downstream commit target for WAB records.
///
/// The drain calls `commit` with batches of records read from sealed WAB segments.
/// Implementations may be async (tokio, sqlx, etc.); they run on a dedicated
/// single-threaded tokio runtime in the drain thread.
pub trait Sink: Send + Sync + 'static {
    type Record: SinkRecord;
    type Error: SinkError;

    /// Commit a batch of records. Returns committed records and any permanently
    /// rejected records. Transiently failed commits should return `Err(Transient)`;
    /// the drain will retry the whole segment.
    async fn commit(
        &self,
        batch: Vec<Self::Record>,
    ) -> Result<CommitResult<Self::Record>, Self::Error>;

    /// Maximum number of records per `commit` call. The drain splits larger
    /// segments into sub-batches of this size.
    fn max_batch_size(&self) -> usize {
        1000
    }

    /// Periodic health probe. Called by the drain (a) after every segment
    /// commit attempt and (b) on a wall-clock interval (default every 30 s)
    /// so the `weir_sink_health{state}` gauge keeps moving even when no
    /// segments are flowing. Implementations should keep this cheap — a
    /// single HEAD or ping is the typical pattern.
    async fn health(&self) -> SinkHealth;
}
