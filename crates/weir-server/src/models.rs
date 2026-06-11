use tokio::sync::oneshot;
use weir_core::{Durability, Payload};

/// A single unit of work flowing from the socket layer through the queue to a
/// worker. The worker batches these by shard and forwards the batch to the WAB.
/// `ack_tx` is held intact through the batch; the WAB drain sends the ack after
/// the record is durably written.
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
    /// Oneshot back-channel to the async socket handler. The WAB drain resolves
    /// this with `true` on successful write, `false` on unrecoverable failure.
    pub ack_tx: oneshot::Sender<bool>,
    /// Wall-clock instant the unit was enqueued to the work queue. Present only
    /// under `bench-trace`; used to attribute per-stage latency in the load suite.
    #[cfg(feature = "bench-trace")]
    pub enqueued_at: std::time::Instant,
}
