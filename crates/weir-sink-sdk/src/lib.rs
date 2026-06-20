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
//!         Ok(CommitResult::new(batch, Vec::new()))
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
//!
//! # Running your sink in the daemon
//!
//! This crate lets you **implement and test** a sink against a stable trait,
//! independent of the daemon's internals. *Running* it is a separate matter: the
//! released `weir-server` binary wires only the built-in sinks selected by the
//! `sink_type` config. There is **no dynamic plugin or registration path yet** —
//! to run a custom sink today you build a `weir-server` with your sink compiled
//! into the sink-selection path (effectively a small fork). A first-class
//! entry-point for downstream sinks is a candidate for a future minor release;
//! because it is purely additive it would be a SemVer-compatible change.

// The drain is always generic over `S: Sink` and stores `Arc<S>` — it never uses
// `dyn Sink`. So the Send-bound ergonomics the `async_fn_in_trait` lint warns
// about do not apply here, and sink authors get clean `async fn` signatures.
#![allow(async_fn_in_trait)]
#![deny(missing_docs)]

/// Opaque record payload bytes (re-exported from `weir-core`). A newtype over
/// ref-counted `bytes::Bytes` that derefs to `[u8]`, so clones through the drain
/// are O(1). Sinks normally *receive* `Payload`s from the drain; to build one
/// yourself (e.g. in a unit test) use `Payload::copy_from_slice(bytes)`,
/// `Payload::from(&b"..."[..])`, or `Payload::from(vec_of_u8)`.
pub use weir_core::Payload;

/// A record type carried through the drain pipeline.
///
/// The simplest implementation is `type Record = Payload` (opaque bytes). Richer
/// sinks can define a concrete record type that deserialises the payload.
///
/// # Footgun
///
/// The two dead-letter paths recover bytes differently, and a custom record type
/// can make them disagree:
///
/// - **Per-record** (a successful `commit` that returns records in
///   [`CommitResult::dead_lettered`]): the drain calls [`into_payload`] on each
///   rejected record to recover the bytes it writes to the dead-letter store.
/// - **Whole-batch `Err`** (`commit` returns a permanent error): the typed
///   records were moved into `commit` and are gone, so the drain dead-letters the
///   **original** raw segment payload bytes — the same bytes it passed to
///   [`from_payload`] — and never calls [`into_payload`] at all.
///
/// **Rule:** if `into_payload(from_payload(x))` is not byte-identical to `x`,
/// your dead-letter bytes differ between the per-record path (uses
/// [`into_payload`]) and the whole-batch-`Err` path (uses the original payload) —
/// make the round-trip byte-preserving, or carry the raw payload inside your
/// record. For the identity [`Payload`] record the round-trip is exact, so the
/// two paths always agree.
///
/// [`into_payload`]: SinkRecord::into_payload
/// [`from_payload`]: SinkRecord::from_payload
pub trait SinkRecord: Send + 'static {
    /// Build the record from the raw payload bytes handed over by the drain.
    fn from_payload(payload: Payload) -> Self;

    /// Recover the raw payload bytes for a record the sink returned in
    /// [`CommitResult::dead_lettered`].
    ///
    /// Called on the **per-record** dead-letter path only: when a `commit` call
    /// succeeds but reports some records as permanently rejected, the drain calls
    /// `into_payload` on each to recover the bytes it writes to the dead-letter
    /// store. It is **not** called when `commit` returns `Err` — see the
    /// [`SinkRecord`] type-level `# Footgun` for the whole-batch-`Err` divergence
    /// and the byte-preserving round-trip rule.
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

/// [`SinkError`] for a sink whose `commit` can never fail: use
/// `type Error = std::convert::Infallible` instead of hand-rolling a never-type.
/// `is_transient` is unreachable (an `Infallible` value cannot exist).
impl SinkError for std::convert::Infallible {
    fn is_transient(&self) -> bool {
        match *self {}
    }
}

/// A ready-made [`SinkError`] for sinks that don't need a bespoke error type —
/// a message plus a transient/permanent classification. Construct it with
/// [`BasicSinkError::transient`] (the drain retries the segment) or
/// [`BasicSinkError::permanent`] (the records are dead-lettered).
#[derive(Debug, Clone)]
pub struct BasicSinkError {
    message: String,
    transient: bool,
}

impl BasicSinkError {
    /// A transient failure — the drain retries the whole segment with backoff.
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: true,
        }
    }

    /// A permanent failure — the affected records are dead-lettered.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: false,
        }
    }
}

impl std::fmt::Display for BasicSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BasicSinkError {}

impl SinkError for BasicSinkError {
    fn is_transient(&self) -> bool {
        self.transient
    }
}

/// The result of a successful [`Sink::commit`].
///
/// Build one with [`CommitResult::new`]. The fields are public for reading, but
/// the type is `#[non_exhaustive]`, so a future release can add a field (or a
/// constructor variant) without a breaking change — construct it through `new`
/// rather than a struct literal.
///
/// Conceptually, every record handed to [`Sink::commit`] should appear in
/// exactly one of `committed` or `dead_lettered`. **Nothing enforces that set
/// partition.** This type does not (the record type `R` carries no identity the
/// constructor could check), and the drain only validates total **count**
/// coverage: it refuses to confirm a segment unless
/// `committed.len() + dead_lettered.len()` equals the batch length. That count
/// check catches a sink that simply drops a record, but it does **not** catch a
/// sink that emits the same record in *both* vectors while dropping a different
/// one — the counts still add up, so the segment is confirmed and the dropped
/// record is silently false-acked (never delivered, never dead-lettered).
///
/// **Author warning:** do not emit a record in both partitions, and do not drop
/// or duplicate records — a count-correct but identity-incorrect partition can
/// still cause a lost record. Built-in sinks always partition cleanly; the
/// burden is on custom sinks.
///
/// # Reading a `CommitResult`
///
/// Both fields are public, so inspect them directly. `committed` is a
/// `Vec<R>` of the accepted records; `dead_lettered` is a `Vec<(R, String)>`
/// pairing each permanently-rejected record with its human-readable reason.
///
/// ```
/// use weir_sink_sdk::{CommitResult, Payload};
///
/// // The drain hands you back a CommitResult; here we build one to read it.
/// let result = CommitResult::new(
///     vec![Payload::from(b"keep-1".as_ref()), Payload::from(b"keep-2".as_ref())],
///     vec![(Payload::from(b"reject".as_ref()), "400 bad request".to_string())],
/// );
///
/// // `committed`: the records the sink accepted.
/// assert_eq!(result.committed.len(), 2);
/// for record in &result.committed {
///     println!("committed {} bytes", record.len());
/// }
///
/// // `dead_lettered`: each rejected record paired with WHY it was rejected.
/// assert_eq!(result.dead_lettered.len(), 1);
/// for (record, reason) in &result.dead_lettered {
///     println!("dead-lettered {} bytes: {reason}", record.len());
/// }
/// let (rejected, reason) = &result.dead_lettered[0];
/// assert_eq!(&rejected[..], b"reject");
/// assert_eq!(reason, "400 bad request");
/// ```
///
/// # Driving an async `commit` from a sync test (no runtime)
///
/// `Sink::commit` is `async`, but you can unit-test a sink whose commit is
/// immediately ready (no real I/O await points) without pulling in a runtime,
/// by polling the future to completion with a no-op waker. This is the same
/// `block_on` helper the SDK's own tests use — copy it into your sink's tests:
///
/// ```
/// use weir_sink_sdk::{CommitResult, Payload, Sink, SinkError, SinkHealth};
///
/// /// Minimal std-only executor: drives a future to completion by polling with
/// /// a no-op waker. Enough for a sink whose `commit`/`health` are immediately
/// /// ready (no real I/O await points), without pulling a runtime into a test.
/// fn block_on<F: std::future::Future>(fut: F) -> F::Output {
///     use std::task::{Context, Poll};
///     let mut fut = std::pin::pin!(fut);
///     let waker = std::task::Waker::noop();
///     let mut cx = Context::from_waker(waker);
///     loop {
///         if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
///             return v;
///         }
///     }
/// }
///
/// // A sink that dead-letters anything equal to `b"reject"` and commits the rest.
/// struct MySink;
/// impl Sink for MySink {
///     type Record = Payload;
///     type Error = std::convert::Infallible;
///     async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, Self::Error> {
///         let (mut ok, mut dead) = (Vec::new(), Vec::new());
///         for r in batch {
///             if r.as_ref() == b"reject" {
///                 dead.push((r, "rejected by MySink".to_string()));
///             } else {
///                 ok.push(r);
///             }
///         }
///         Ok(CommitResult::new(ok, dead))
///     }
///     async fn health(&self) -> SinkHealth {
///         SinkHealth::Healthy
///     }
/// }
///
/// let batch = vec![
///     Payload::from(b"keep".as_ref()),
///     Payload::from(b"reject".as_ref()),
/// ];
/// // Drive the async commit to completion synchronously, then read the result.
/// let result = block_on(MySink.commit(batch)).unwrap();
/// assert_eq!(result.committed.len(), 1);
/// assert_eq!(result.dead_lettered.len(), 1);
/// assert_eq!(result.dead_lettered[0].1, "rejected by MySink");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct CommitResult<R> {
    /// Records the sink accepted.
    pub committed: Vec<R>,
    /// Records the sink permanently rejected, each with a human-readable reason.
    pub dead_lettered: Vec<(R, String)>,
}

impl<R> CommitResult<R> {
    /// Builds a commit result from the accepted and permanently-rejected records.
    ///
    /// Every record passed to [`Sink::commit`] should appear in exactly one of the
    /// two lists. Neither this constructor nor the drain enforces that partition
    /// by identity (only total count coverage is checked) — see the type-level
    /// note for the failure mode and the author warning.
    #[must_use]
    pub fn new(committed: Vec<R>, dead_lettered: Vec<(R, String)>) -> Self {
        Self {
            committed,
            dead_lettered,
        }
    }
}

/// Coarse health signal from [`Sink::health`].
///
/// `#[non_exhaustive]`: a finer health taxonomy may be added post-1.0, so
/// downstream matches must include a wildcard arm.
#[derive(Clone, Debug)]
#[non_exhaustive]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal std-only executor: drives a future to completion by polling with a
    /// no-op waker. Enough to test a sink whose `commit`/`health` are immediately
    /// ready (no real I/O await points), without pulling a runtime into the SDK.
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        use std::task::{Context, Poll};
        let mut fut = std::pin::pin!(fut);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    /// A trivial sink that counts committed records and dead-letters anything
    /// equal to `b"reject"`. Shows the pattern sink authors use to unit-test a
    /// `Sink` impl against the stable contract (no daemon, no runtime).
    #[test]
    fn a_custom_sink_can_be_driven_and_unit_tested() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct Never;
        impl std::fmt::Display for Never {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "never")
            }
        }
        impl std::error::Error for Never {}
        impl SinkError for Never {
            fn is_transient(&self) -> bool {
                true
            }
        }

        struct CountingSink {
            committed: AtomicUsize,
        }
        impl Sink for CountingSink {
            type Record = Payload;
            type Error = Never;
            async fn commit(&self, batch: Vec<Payload>) -> Result<CommitResult<Payload>, Never> {
                let (mut ok, mut dead) = (Vec::new(), Vec::new());
                for r in batch {
                    if r.as_ref() == b"reject" {
                        dead.push((r, "rejected by CountingSink".to_string()));
                    } else {
                        ok.push(r);
                    }
                }
                self.committed.fetch_add(ok.len(), Ordering::Relaxed);
                Ok(CommitResult::new(ok, dead))
            }
            async fn health(&self) -> SinkHealth {
                SinkHealth::Healthy
            }
        }

        let sink = CountingSink {
            committed: AtomicUsize::new(0),
        };
        let batch = vec![
            Payload::copy_from_slice(b"keep-1"),
            Payload::copy_from_slice(b"reject"),
            Payload::copy_from_slice(b"keep-2"),
        ];
        let result = block_on(sink.commit(batch)).unwrap();
        assert_eq!(result.committed.len(), 2);
        assert_eq!(result.dead_lettered.len(), 1);
        assert_eq!(&result.dead_lettered[0].0[..], b"reject");
        assert_eq!(sink.committed.load(Ordering::Relaxed), 2);
        assert!(matches!(block_on(sink.health()), SinkHealth::Healthy));
    }

    #[test]
    fn basic_sink_error_classifies_and_displays() {
        let t = BasicSinkError::transient("503 from upstream");
        assert!(t.is_transient());
        assert_eq!(t.to_string(), "503 from upstream");
        let p = BasicSinkError::permanent("400 bad request");
        assert!(!p.is_transient());
        // Usable as a SinkError trait object / std error.
        let _: &dyn SinkError = &p;
        let _: &dyn std::error::Error = &p;
    }

    #[test]
    fn commit_result_new_keeps_both_partitions() {
        let r = CommitResult::new(
            vec![Payload::from(b"a".as_ref())],
            vec![(Payload::from(b"b".as_ref()), "rejected".to_string())],
        );
        assert_eq!(r.committed.len(), 1);
        assert_eq!(r.dead_lettered.len(), 1);
        assert_eq!(&r.committed[0][..], b"a");
        assert_eq!(&r.dead_lettered[0].0[..], b"b");
        assert_eq!(r.dead_lettered[0].1, "rejected");
    }

    #[test]
    fn the_payload_record_is_an_identity_round_trip() {
        // The built-in pass-through record: from_payload/into_payload are inverses.
        let p = Payload::from(b"weir".as_ref());
        let recovered = <Payload as SinkRecord>::from_payload(p.clone()).into_payload();
        assert_eq!(&recovered[..], &p[..]);
    }
}
