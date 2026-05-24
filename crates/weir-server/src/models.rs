use tokio::sync::oneshot;
use weir_core::Payload;

/// A single unit of work flowing from the socket layer through the queue to a
/// worker. The worker batches these by shard and forwards the batch to the WAB.
/// `ack_tx` is held intact through the batch; the WAB drain sends the ack after
/// the record is durably written.
pub struct WorkUnit {
    /// Target shard, set by the socket layer (e.g. connection hash mod shard_count).
    pub shard_id: u32,
    /// Opaque payload bytes from the wire envelope.
    pub payload: Payload,
    /// Oneshot back-channel to the async socket handler. The WAB drain resolves
    /// this with `true` on successful write, `false` on unrecoverable failure.
    pub ack_tx: oneshot::Sender<bool>,
}
