use tokio::sync::oneshot;
use weir_core::{Durability, Payload};

/// A single unit of work flowing from the socket layer through the queue to a
/// worker. The worker batches these by shard and forwards the batch to the WAB.
/// `ack_tx` is held intact through the batch; the WAB drain sends the ack after
/// the record is durably written.
/// Where the WAB put one record — the address half of a
/// [`RecordCoordinate`](weir_core::RecordCoordinate).
///
/// The digest half is deliberately absent: computing it needs the payload, and
/// the socket layer already holds an O(1) clone of that, so hashing there keeps
/// SHA-256 off the flusher thread that is serialised with disk writes.
#[derive(Debug)]
pub struct RecordSlot {
    /// The segment's address in the buffer, `<shard-dir>/<sealed-file-name>` —
    /// the string the drain will mix into this record's `RecordId`.
    pub segment: String,
    /// The record's 1-based ordinal within that segment.
    pub index: u64,
}

/// What the WAB flusher tells the socket handler about one record.
#[derive(Debug)]
pub struct AckOutcome {
    /// Whether the record reached the durability its tier promises. `false` is a
    /// write/fsync failure, and the handler turns it into `Nack(InternalError)`.
    pub durable: bool,
    /// Where the record landed. `Some` only when the producer asked for it with
    /// a `PushTracked` **and** the write actually reached a segment.
    ///
    /// Boxed because it rides in the flusher's per-batch pending-ack vectors:
    /// unboxed it would grow every element from 8 to 40 bytes for a field the
    /// overwhelming majority of records leave empty. The allocation happens only
    /// for a tracked push.
    pub coordinate: Option<Box<RecordSlot>>,
}

impl AckOutcome {
    /// The record is durable, at `coordinate` — `None` for an untracked push,
    /// which is every push that did not ask to be told.
    pub fn durable(coordinate: Option<Box<RecordSlot>>) -> Self {
        Self {
            durable: true,
            coordinate,
        }
    }

    /// The record is NOT durable. A failure never carries a coordinate: an
    /// address for a record that did not survive is an invitation to reconcile
    /// against something that is not there.
    pub fn failed() -> Self {
        Self {
            durable: false,
            coordinate: None,
        }
    }
}

pub struct WorkUnit {
    /// Target shard. Assigned by the socket layer's accept loop on a
    /// round-robin basis (`accept_counter % shard_count`) so every connection
    /// gets a single deterministic shard for its lifetime. With
    /// `shard_count = 1` every WorkUnit lands on shard 0.
    pub shard_id: u32,
    /// Opaque payload bytes from the wire envelope.
    pub payload: Payload,
    /// Durability tier requested by the producer.
    pub durability: Durability,
    /// Whether the producer used `PushTracked` and is waiting to be told where
    /// the record landed. The flusher builds a [`RecordSlot`] only when this is
    /// set, so an ordinary push pays nothing for the feature.
    pub wants_coordinate: bool,
    /// Oneshot back-channel to the async socket handler. The WAB drain resolves
    /// this with the record's [`AckOutcome`] — durable or not, plus the
    /// coordinate when one was asked for.
    pub ack_tx: oneshot::Sender<AckOutcome>,
    /// Wall-clock instant the unit was enqueued to the work queue. Present only
    /// under `bench-trace`; used to attribute per-stage latency in the load suite.
    #[cfg(feature = "bench-trace")]
    pub enqueued_at: std::time::Instant,
}

/// A flushed batch of work units for one shard, ready for the WAB to consume.
/// `ack_tx` inside each `WorkUnit` is carried intact; the WAB flusher resolves
/// it after the record is durably written.
pub struct Batch {
    /// Diagnostic tag — never read in production today, but the field is
    /// set by every batch-producing path so a test can assert routing
    /// correctness and a future per-shard tracing/metric story has the data
    /// it needs without re-plumbing. `#[allow(dead_code)]` so production
    /// builds stay quiet and test builds don't trip lint expectations.
    #[allow(dead_code)]
    pub shard_id: u32,
    pub records: Vec<WorkUnit>,
    /// Wall-clock instant the worker flushed this batch. Present only under
    /// `bench-trace`; used by the WAB flusher to attribute the worker-flush →
    /// flusher-dequeue (`bridge_wait`) stage delta.
    #[cfg(feature = "bench-trace")]
    pub flushed_at: std::time::Instant,
}
