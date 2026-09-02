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
//! A sink whose `commit` can never fail picks `type Error = std::convert::Infallible`
//! — no error type to hand-roll, no `is_transient` to write:
//!
//! ```
//! use weir_sink_sdk::{CommitResult, Payload, Sink, SinkBatch, SinkHealth};
//!
//! struct StdoutSink;
//!
//! impl Sink for StdoutSink {
//!     type Error = std::convert::Infallible; // this commit never returns Err
//!     async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, Self::Error> {
//!         let batch = batch.into_records();
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
//! A sink that *can* fail returns a [`SinkError`] that classifies the failure as
//! transient (retried) or permanent (dead-lettered). Reach for the ready-made
//! [`BasicSinkError`] before writing your own error type.
//!
//! # Idempotency contract
//!
//! The drain guarantees **at-least-once delivery per segment**, not per record.
//! If the daemon crashes after a partial commit but before recording the segment
//! as confirmed, `commit` is called again with the full segment — including
//! records already committed. The same whole-segment replay also occurs when a
//! segment is *stranded* (its transient-retry budget is exhausted while the sink
//! is unavailable) and later auto-resumed once the sink recovers: the resume
//! reprocesses from the start, not from the last durably-committed sub-batch.
//! Implementations **must** handle duplicates gracefully (upsert, `INSERT IGNORE`,
//! a content-derived dedup key, etc.). This is the explicit durability trade-off,
//! not a protocol weakness.
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
/// partition.** This type does not (a [`Payload`] carries no identity the
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
/// `Vec<Payload>` of the accepted records; `dead_lettered` is a
/// `Vec<(Payload, String)>`
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
/// <div class="warning">
///
/// **Only for sinks with no real I/O await points.** This `block_on` busy-polls
/// with a *no-op* waker — it never sleeps and never wakes. If the future ever
/// returns `Poll::Pending` (any real `await` that yields: a socket read, a timer,
/// `tokio::fs`, `sqlx`, `reqwest`), this loop spins the CPU at 100% forever and
/// **never makes progress** — the no-op waker can't reschedule it. It is fine
/// only when `commit`/`health` complete synchronously (in-memory work, a `Vec`
/// push, no yield point). For a sink that does real I/O, use a real runtime —
/// write a `#[tokio::test]` and `.await` the commit directly (see
/// `docs/getting-started/integrating.md` → "Unit-testing a real-I/O sink").
///
/// </div>
///
/// ```
/// use weir_sink_sdk::{CommitResult, DedupToken, Payload, Sink, SinkBatch, SinkError, SinkHealth};
///
/// /// Minimal std-only executor: drives a future to completion by polling with
/// /// a no-op waker. ONLY for a sink whose `commit`/`health` are immediately
/// /// ready (no real I/O await points) — it busy-spins forever on any future
/// /// that returns `Poll::Pending`. For real I/O use a `#[tokio::test]` instead.
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
///     type Error = std::convert::Infallible;
///     async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, Self::Error> {
///         let batch = batch.into_records();
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
/// let records = vec![
///     Payload::from(b"keep".as_ref()),
///     Payload::from(b"reject".as_ref()),
/// ];
/// let batch = SinkBatch::new(records.clone(), DedupToken::for_payloads(records.iter()));
/// // Drive the async commit to completion synchronously, then read the result.
/// let result = block_on(MySink.commit(batch)).unwrap();
/// assert_eq!(result.committed.len(), 1);
/// assert_eq!(result.dead_lettered.len(), 1);
/// assert_eq!(result.dead_lettered[0].1, "rejected by MySink");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct CommitResult {
    /// Records the sink accepted.
    pub committed: Vec<Payload>,
    /// Records the sink permanently rejected, each with a human-readable reason.
    pub dead_lettered: Vec<(Payload, String)>,
}

impl CommitResult {
    /// Builds a commit result from the accepted and permanently-rejected records.
    ///
    /// Every record passed to [`Sink::commit`] should appear in exactly one of the
    /// two lists. Neither this constructor nor the drain enforces that partition
    /// by identity (only total count coverage is checked) — see the type-level
    /// note for the failure mode and the author warning.
    #[must_use]
    pub fn new(committed: Vec<Payload>, dead_lettered: Vec<(Payload, String)>) -> Self {
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

/// A content-derived, batch-scoped idempotency handle for one [`Sink::commit`]
/// call.
///
/// # What it is
///
/// `sha256(len(p₀) ++ p₀ ++ len(p₁) ++ p₁ ++ …)`, each length an 8-byte
/// little-endian prefix, over the batch's payload bytes in delivery order.
///
/// The length prefix is load-bearing, not decoration. Concatenating payloads
/// without a delimiter makes `["ab", "c"]` and `["a", "bc"]` hash identically —
/// a dedup-capable sink would then drop the second, genuinely distinct batch as
/// a duplicate and **lose data**. The prefix restores a prefix-free framing.
///
/// # What it guarantees
///
/// A crash-replayed, byte-identical batch produces a byte-identical token, so a
/// downstream that deduplicates on it (ClickHouse's `insert_deduplication_token`,
/// an HTTP `Idempotency-Key`, a Postgres `ON CONFLICT` key) collapses the
/// re-delivery that weir's at-least-once contract permits.
///
/// # Precondition — keep `sink_max_batch_size` stable
///
/// The token covers exactly the sub-batch handed to `commit`, which the drain
/// sizes by `sink_max_batch_size`. If that config changes across a restart, a
/// replayed segment re-splits into differently-sized sub-batches whose tokens
/// differ from the originals, the downstream does not recognise them as
/// duplicates, and at-least-once becomes a double-insert. **The guarantee above
/// holds only while that setting is stable.**
///
/// # Stability
///
/// The digest is byte-identical to the token weir 1.x's ClickHouse sink computed
/// internally, so an operator upgrading mid-outage keeps deduplicating. It is
/// pinned by a known-answer test and must not change without a major version.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DedupToken([u8; 32]);

impl DedupToken {
    /// Derives the token from a batch's payloads, in delivery order.
    pub fn for_payloads<'a>(payloads: impl IntoIterator<Item = &'a Payload>) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for p in payloads {
            hasher.update((p.len() as u64).to_le_bytes());
            hasher.update(p.as_ref());
        }
        Self(hasher.finalize().into())
    }

    /// Rebuilds a token from a previously captured digest. Intended for sink
    /// authors' tests; the drain always uses [`DedupToken::for_payloads`].
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lower-hex, 64 characters, no prefix.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in self.0 {
            // Writing to a String is infallible.
            let _ = write!(out, "{b:02x}");
        }
        out
    }

    /// `sha256:<lower-hex>` — the form an HTTP `Idempotency-Key` wants, where
    /// the prefix tells the endpoint which digest it is looking at.
    pub fn to_prefixed_hex(&self) -> String {
        format!("sha256:{}", self.to_hex())
    }
}

impl std::fmt::Debug for DedupToken {
    /// Prints the hex digest rather than 32 raw bytes — a token shows up in
    /// drain error logs, where `[228, 91, ...]` is useless for correlating
    /// against a downstream's dedup table.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DedupToken({})", self.to_hex())
    }
}

/// A per-record identity: unique even across records carrying identical bytes.
///
/// # Why this exists separately from [`DedupToken`]
///
/// The two answer different questions and pull in opposite directions.
///
/// A `DedupToken` must be **stable across a retry**: the same batch, re-sent
/// after a transient failure, has to present the same value or the retry
/// deduplicates as new data. It is therefore a pure function of the payload
/// bytes, and two batches carrying identical bytes are — correctly — the same
/// token.
///
/// A per-record idempotency key must be **unique across records**. Derived from
/// payload bytes alone, a producer emitting repetitive records (a heartbeat, a
/// status ping, any fixed-shape event) hands the downstream the same key for
/// genuinely distinct events, and a correctly-implemented idempotent endpoint
/// keeps the first and discards the rest. weir acked those records and wrote
/// them to disk; they are then dropped downstream by weir's own header.
///
/// `RecordId` mixes the record's WAB coordinate — the segment it was read from
/// and its index within that segment — into the digest, so identical bytes at
/// different coordinates are different ids. The coordinate is stable across a
/// re-read of the same segment, so a retried delivery still presents the same
/// id and per-record retry dedup keeps working.
///
/// # Format
///
/// SHA-256 over `segment_len ++ segment ++ index ++ payload_len ++ payload`,
/// every length a little-endian `u64`. The lengths are what stop
/// `("ab", 1)` and `("a", 0xb...)` from colliding.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId([u8; 32]);

impl RecordId {
    /// Derives the id from a record's WAB coordinate and bytes.
    ///
    /// `segment` is the segment's file name and `index` the record's ordinal
    /// within it — together the record's address in the buffer, which is what
    /// makes this unique where a content hash is not.
    pub fn for_record(segment: &str, index: u64, payload: &Payload) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update((segment.len() as u64).to_le_bytes());
        hasher.update(segment.as_bytes());
        hasher.update(index.to_le_bytes());
        hasher.update((payload.len() as u64).to_le_bytes());
        hasher.update(payload.as_ref());
        Self(hasher.finalize().into())
    }

    /// Rebuilds an id from a previously captured digest. Intended for sink
    /// authors' tests.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lower-hex, 64 characters, no prefix.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in self.0 {
            // Writing to a String is infallible.
            let _ = write!(out, "{b:02x}");
        }
        out
    }

    /// `sha256:<lower-hex>` — the form an HTTP `Idempotency-Key` wants.
    pub fn to_prefixed_hex(&self) -> String {
        format!("sha256:{}", self.to_hex())
    }
}

impl std::fmt::Debug for RecordId {
    /// Prints the hex digest rather than 32 raw bytes, for the same reason
    /// [`DedupToken`] does: an id shows up in drain logs, where a byte array is
    /// useless for correlating against a downstream's dedup table.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordId({})", self.to_hex())
    }
}

/// One batch of records on its way to [`Sink::commit`], with the
/// [`DedupToken`] the drain derived from it.
///
/// The token is **batch-scoped, not segment-scoped**. The drain splits each
/// sealed segment into `sink_max_batch_size` chunks and calls `commit` once per
/// chunk; a token shared across those chunks would make a dedup-capable sink
/// discard every chunk after the first. See [`DedupToken`] for the stability
/// precondition that comes with it.
///
/// Records are [`Payload`] — opaque bytes. weir 1.x let a sink pick its own
/// record type via `Sink::Record`, but the only implementation was ever the
/// identity on `Payload`, so 2.0 drops the generic and its no-op conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkBatch {
    records: Vec<Payload>,
    dedup_token: DedupToken,
    /// Parallel to `records` when present. `None` from [`SinkBatch::new`], so a
    /// batch built by a 1.x-era caller or a sink author's test is unchanged.
    record_ids: Option<Vec<RecordId>>,
}

impl SinkBatch {
    /// Builds a batch. The drain calls this; sink authors need it to construct
    /// realistic input in their own tests.
    pub fn new(records: Vec<Payload>, dedup_token: DedupToken) -> Self {
        Self {
            records,
            dedup_token,
            record_ids: None,
        }
    }

    /// Builds a batch that also carries a [`RecordId`] per record, in the same
    /// order. The drain uses this; [`SinkBatch::new`] remains the plain form.
    ///
    /// # Panics
    ///
    /// If `record_ids.len() != records.len()`. They are read positionally, so a
    /// length mismatch would silently pair records with other records' ids —
    /// worse than the bug this type exists to fix.
    pub fn with_record_ids(
        records: Vec<Payload>,
        dedup_token: DedupToken,
        record_ids: Vec<RecordId>,
    ) -> Self {
        assert_eq!(
            records.len(),
            record_ids.len(),
            "SinkBatch::with_record_ids: {} records but {} ids",
            records.len(),
            record_ids.len()
        );
        Self {
            records,
            dedup_token,
            record_ids: Some(record_ids),
        }
    }

    /// The per-record ids, if the batch carries them, in record order.
    ///
    /// `None` means the batch was built without them — a sink should fall back
    /// to whatever key it used before rather than treat this as an error.
    pub fn record_ids(&self) -> Option<&[RecordId]> {
        self.record_ids.as_deref()
    }

    /// Consumes the batch for its records paired with their ids, where present.
    pub fn into_records_with_ids(self) -> (Vec<Payload>, Option<Vec<RecordId>>) {
        (self.records, self.record_ids)
    }

    /// The records, borrowed.
    pub fn records(&self) -> &[Payload] {
        &self.records
    }

    /// Consumes the batch for its records, discarding the token. This is the
    /// one-line migration for a sink written against weir 1.x:
    /// `let batch = batch.into_records();` at the top of `commit`.
    pub fn into_records(self) -> Vec<Payload> {
        self.records
    }

    /// Consumes the batch for both halves.
    pub fn into_parts(self) -> (Vec<Payload>, DedupToken) {
        (self.records, self.dedup_token)
    }

    /// The batch's idempotency handle.
    pub fn dedup_token(&self) -> &DedupToken {
        &self.dedup_token
    }

    /// Number of records in the batch.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch carries no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Builds a batch whose token is derived from its own records — the same thing
/// the drain does, and the convenient form for a sink author's tests.
///
/// Use [`SinkBatch::new`] instead when you need to pin a specific token, e.g. to
/// assert that your sink forwards it unchanged.
impl From<Vec<Payload>> for SinkBatch {
    fn from(records: Vec<Payload>) -> Self {
        let dedup_token = DedupToken::for_payloads(records.iter());
        Self {
            records,
            dedup_token,
            record_ids: None,
        }
    }
}

/// A downstream commit target for weir records.
///
/// The drain calls [`commit`](Sink::commit) with batches of records read from
/// sealed segments. Implementations may be async (tokio, sqlx, reqwest, …); they
/// run on a dedicated single-threaded tokio runtime in the drain thread.
pub trait Sink: Send + Sync + 'static {
    /// The error type this sink returns; must classify transient vs permanent.
    type Error: SinkError;

    /// Commit a batch of records. Returns the committed records and any
    /// permanently rejected ones. Return `Err(e)` with `e.is_transient() == true`
    /// to have the drain retry the whole segment.
    ///
    /// # Migrating from weir 1.x
    ///
    /// Two changes. `Sink::Record` is gone — records are always [`Payload`], as
    /// they always were in practice — and the parameter is now a [`SinkBatch`]
    /// carrying the batch's [`DedupToken`]. A sink that does not want the token
    /// needs one deleted line and one added line:
    ///
    /// ```ignore
    /// impl Sink for MySink {
    ///     // type Record = Payload;          <- delete this
    ///     type Error = MyError;
    ///
    ///     async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, MyError> {
    ///         let batch = batch.into_records();   // <- add this
    ///         // ... the rest of your 1.x body, unchanged
    ///     }
    /// }
    /// ```
    ///
    /// A sink whose downstream can deduplicate should instead read
    /// [`SinkBatch::dedup_token`] and pass it along — see [`DedupToken`].
    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, Self::Error>;

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

    /// The defect this type exists to fix: two distinct records that happen to
    /// carry identical bytes must not present the same idempotency key.
    #[test]
    fn identical_payloads_at_different_coordinates_get_different_ids() {
        let p = Payload::from(b"heartbeat".as_ref());
        let a = RecordId::for_record("seg-000042.wab", 7, &p);
        let b = RecordId::for_record("seg-000042.wab", 8, &p);
        let c = RecordId::for_record("seg-000043.wab", 7, &p);
        assert_ne!(a, b, "same segment, different index must differ");
        assert_ne!(a, c, "same index, different segment must differ");
        assert_ne!(b, c);
    }

    /// And the property that keeps per-record retry dedup working: a re-read of
    /// the same record from the same place is the same id.
    #[test]
    fn the_same_record_reread_gets_the_same_id() {
        let p = Payload::from(b"heartbeat".as_ref());
        assert_eq!(
            RecordId::for_record("seg-000042.wab", 7, &p),
            RecordId::for_record("seg-000042.wab", 7, &p)
        );
    }

    /// The length prefixes are load-bearing: without them a segment name and an
    /// index could be re-split to produce the same byte stream.
    #[test]
    fn field_boundaries_cannot_be_confused() {
        let p = Payload::from(b"x".as_ref());
        assert_ne!(
            RecordId::for_record("ab", 1, &p),
            RecordId::for_record("a", 1, &p),
            "a shorter segment name must not collide with a longer one"
        );
    }

    /// Payload bytes still participate, so a corrupted re-read is a different id
    /// rather than silently reusing the original record's key.
    #[test]
    fn payload_bytes_still_participate() {
        let a = RecordId::for_record("seg.wab", 0, &Payload::from(b"one".as_ref()));
        let b = RecordId::for_record("seg.wab", 0, &Payload::from(b"two".as_ref()));
        assert_ne!(a, b);
    }

    #[test]
    fn record_id_hex_forms_are_well_shaped() {
        let id = RecordId::for_record("seg.wab", 0, &Payload::from(b"x".as_ref()));
        assert_eq!(id.to_hex().len(), 64);
        assert!(id.to_hex().chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(id.to_prefixed_hex(), format!("sha256:{}", id.to_hex()));
        // Debug prints the digest, not a byte array — it lands in drain logs.
        assert!(format!("{id:?}").contains(&id.to_hex()));
    }

    #[test]
    fn with_record_ids_round_trips() {
        let records = vec![Payload::from(b"a".as_ref()), Payload::from(b"b".as_ref())];
        let ids: Vec<_> = records
            .iter()
            .enumerate()
            .map(|(i, p)| RecordId::for_record("seg.wab", i as u64, p))
            .collect();
        let batch = SinkBatch::with_record_ids(
            records.clone(),
            DedupToken::for_payloads(records.iter()),
            ids.clone(),
        );
        assert_eq!(batch.record_ids(), Some(&ids[..]));
        let (r, got) = batch.into_records_with_ids();
        assert_eq!(r, records);
        assert_eq!(got, Some(ids));
    }

    /// A batch built the plain way carries no ids, and a sink must be able to
    /// tell that apart from "ids that happen to be empty".
    #[test]
    fn plain_batches_carry_no_record_ids() {
        let records = vec![Payload::from(b"a".as_ref())];
        let batch = SinkBatch::new(records.clone(), DedupToken::for_payloads(records.iter()));
        assert_eq!(batch.record_ids(), None);
        assert_eq!(SinkBatch::from(records).record_ids(), None);
    }

    #[test]
    #[should_panic(expected = "2 records but 1 ids")]
    fn mismatched_id_count_panics_rather_than_mispairing() {
        let records = vec![Payload::from(b"a".as_ref()), Payload::from(b"b".as_ref())];
        let token = DedupToken::for_payloads(records.iter());
        SinkBatch::with_record_ids(records, token, vec![RecordId::from_bytes([0u8; 32])]);
    }

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
            type Error = Never;
            async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, Never> {
                let batch = batch.into_records();
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
        let records = vec![
            Payload::copy_from_slice(b"keep-1"),
            Payload::copy_from_slice(b"reject"),
            Payload::copy_from_slice(b"keep-2"),
        ];
        let batch = SinkBatch::new(records.clone(), DedupToken::for_payloads(records.iter()));
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

    // ── DedupToken ────────────────────────────────────────────────────────────

    fn p(bytes: &[u8]) -> Payload {
        Payload::from(bytes)
    }

    /// Captured by running weir 1.3.1's `clickhouse::dedup_token` on
    /// `[b"alpha", b"beta", b"gamma"]`. Changing this constant means breaking
    /// compatibility with every deployment that deduplicates on the token.
    /// Don't. See also the second, deliberately independent copy in
    /// `weir-server`'s clickhouse sink tests.
    const KNOWN_ANSWER_1_X: &str =
        "5bc1fe58cc34db881c67b2acd898651311f6dfc576285c906c8f97e049b15342";

    #[test]
    fn dedup_token_is_deterministic() {
        let batch = [p(b"a"), p(b"b")];
        assert_eq!(
            DedupToken::for_payloads(&batch).to_hex(),
            DedupToken::for_payloads(&batch).to_hex()
        );
    }

    #[test]
    fn dedup_token_changes_on_reorder() {
        let a = [p(b"a"), p(b"b")];
        let b = [p(b"b"), p(b"a")];
        assert_ne!(
            DedupToken::for_payloads(&a).to_hex(),
            DedupToken::for_payloads(&b).to_hex()
        );
    }

    #[test]
    fn dedup_token_distinguishes_different_batch_boundaries() {
        // Without the length prefix these two hash identically, and a
        // dedup-capable sink would drop the second as a duplicate — losing data.
        let a = [p(b"ab"), p(b"c")];
        let b = [p(b"a"), p(b"bc")];
        assert_ne!(
            DedupToken::for_payloads(&a).to_hex(),
            DedupToken::for_payloads(&b).to_hex()
        );
    }

    #[test]
    fn dedup_token_empty_batch_is_the_empty_sha256() {
        // sha256 of zero bytes. An empty batch never reaches a sink in practice
        // (the drain skips it), but the function must be total.
        assert_eq!(
            DedupToken::for_payloads(&[] as &[Payload]).to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn dedup_token_hex_forms_agree() {
        let t = DedupToken::for_payloads(&[p(b"x")]);
        assert_eq!(t.to_prefixed_hex(), format!("sha256:{}", t.to_hex()));
        assert_eq!(t.to_hex().len(), 64);
        assert!(
            t.to_hex()
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn dedup_token_round_trips_through_bytes() {
        let t = DedupToken::for_payloads(&[p(b"x")]);
        assert_eq!(DedupToken::from_bytes(*t.as_bytes()).to_hex(), t.to_hex());
    }

    #[test]
    fn dedup_token_debug_is_hex_not_a_byte_array() {
        let t = DedupToken::for_payloads(&[p(b"x")]);
        let d = format!("{t:?}");
        assert!(
            d.contains(&t.to_hex()),
            "Debug should show the hex digest: {d}"
        );
        assert!(
            !d.contains('['),
            "Debug should not dump the byte array: {d}"
        );
    }

    /// Known-answer vector. This exact digest is what weir 1.x's ClickHouse sink
    /// sent as `insert_deduplication_token` for this batch. If this test fails,
    /// the algorithm changed and every deployment that deduplicates on the token
    /// will stop recognising replays across the upgrade. Do not re-bless it —
    /// fix the code.
    #[test]
    fn dedup_token_matches_the_weir_1_x_digest() {
        let batch = [p(b"alpha"), p(b"beta"), p(b"gamma")];
        assert_eq!(
            DedupToken::for_payloads(&batch).to_hex(),
            KNOWN_ANSWER_1_X,
            "the dedup digest changed — see the doc comment"
        );
    }

    // ── SinkBatch ─────────────────────────────────────────────────────────────

    #[test]
    fn sink_batch_exposes_records_and_token() {
        let token = DedupToken::for_payloads(&[p(b"a")]);
        let batch = SinkBatch::new(vec![p(b"a"), p(b"b")], token);

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_eq!(batch.records(), &[p(b"a"), p(b"b")][..]);
        assert_eq!(batch.dedup_token().to_hex(), token.to_hex());
    }

    #[test]
    fn sink_batch_into_records_is_the_migration_path() {
        // The whole third-party migration: one line at the top of commit().
        let batch = SinkBatch::new(vec![p(b"a")], DedupToken::for_payloads(&[p(b"a")]));
        let records: Vec<Payload> = batch.into_records();
        assert_eq!(records, vec![p(b"a")]);
    }

    #[test]
    fn sink_batch_into_parts_yields_both() {
        let token = DedupToken::for_payloads(&[p(b"a")]);
        let (records, t) = SinkBatch::new(vec![p(b"a")], token).into_parts();
        assert_eq!(records, vec![p(b"a")]);
        assert_eq!(t.to_hex(), token.to_hex());
    }

    #[test]
    fn sink_batch_empty_is_empty() {
        let batch = SinkBatch::new(vec![], DedupToken::for_payloads(&[] as &[Payload]));
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }
}
