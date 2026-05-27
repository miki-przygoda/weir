use std::time::Duration;

use crossbeam_channel::{Receiver, SendTimeoutError, Sender};

/// Total work-queue capacity, split evenly across partitions in [`new`].
pub const QUEUE_CAPACITY: usize = 65_536;

/// Send half of the work queue. Internally a fixed set of bounded sub-channels
/// — one per partition — selected at push time by a caller-supplied key. The
/// partition is the unit of FIFO ordering: any two records pushed with the
/// same key are delivered to the same receiver in submission order.
///
/// Cheap to clone — every clone shares the same underlying sub-channels.
/// Dropping all clones closes every sub-channel and propagates shutdown to
/// every worker `Receiver`.
pub struct QueueSender<T> {
    txs: Vec<Sender<T>>,
}

/// Receive half of the work queue. One `Receiver` per partition; each worker
/// owns one partition's receiver and never sees records from any other.
pub struct QueueReceiver<T> {
    rxs: Vec<Receiver<T>>,
}

/// Creates a bounded partitioned work queue with `partitions` independent
/// sub-channels totalling `QUEUE_CAPACITY` slots.
///
/// The sender routes each push to `partition_key % partitions`. The receiver
/// hands out one `Receiver` per partition via [`QueueReceiver::get`].
///
/// Dropping all `QueueSender` clones closes every sub-channel; every
/// `Receiver` will observe `Disconnected` once its in-flight items are
/// drained, propagating the shutdown signal to workers.
///
/// # Panics
///
/// Panics if `partitions == 0`. Callers must validate before calling.
pub fn new<T>(partitions: usize) -> (QueueSender<T>, QueueReceiver<T>) {
    assert!(partitions >= 1, "queue must have at least one partition");
    let per_partition = (QUEUE_CAPACITY / partitions).max(1);
    let mut txs = Vec::with_capacity(partitions);
    let mut rxs = Vec::with_capacity(partitions);
    for _ in 0..partitions {
        let (tx, rx) = crossbeam_channel::bounded(per_partition);
        txs.push(tx);
        rxs.push(rx);
    }
    (QueueSender { txs }, QueueReceiver { rxs })
}

impl<T> QueueSender<T> {
    /// Total in-flight count summed across every partition. Used by the
    /// `weir_queue_depth` metric poll task.
    pub fn len(&self) -> usize {
        self.txs.iter().map(|t| t.len()).sum()
    }

    /// Blocking push, test-only. Production code routes through
    /// `push_timeout` (below) so a saturated queue surfaces as
    /// `Nack(InternalError)` instead of an indefinite stall.
    #[cfg(test)]
    pub fn push(&self, partition_key: usize, unit: T) {
        let idx = partition_key % self.txs.len();
        self.txs[idx].send(unit).ok();
    }

    /// Attempts to push a work unit to the partition selected by
    /// `partition_key`, waiting at most `timeout`.
    ///
    /// Returns `Err(unit)` if the chosen partition is full for the entire
    /// duration or all of its receivers have been dropped. The socket layer
    /// uses this to nack rather than block indefinitely when a partition is
    /// saturated.
    ///
    /// **Per-shard ordering**: records produced with the same `partition_key`
    /// land on the same sub-channel, so a single producer's records on a
    /// given shard arrive at the worker in submission order even when
    /// other producers are concurrently pushing to the same shard.
    pub fn push_timeout(
        &self,
        partition_key: usize,
        unit: T,
        timeout: Duration,
    ) -> Result<(), T> {
        let idx = partition_key % self.txs.len();
        match self.txs[idx].send_timeout(unit, timeout) {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(u) | SendTimeoutError::Disconnected(u)) => Err(u),
        }
    }
}

impl<T> Clone for QueueSender<T> {
    fn clone(&self) -> Self {
        QueueSender {
            txs: self.txs.clone(),
        }
    }
}

impl<T> QueueReceiver<T> {
    /// Returns the `Receiver` for a specific partition.
    ///
    /// Each worker should call this once with its own worker index (typically
    /// `worker_idx == partition`). Records pushed with `partition_key == idx`
    /// arrive at the returned receiver in submission order.
    pub fn get(&self, partition: usize) -> Receiver<T> {
        self.rxs[partition].clone()
    }

    pub fn partitions(&self) -> usize {
        self.rxs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// Test-only constructor that lets a test override the per-partition
    /// capacity so blocking behaviour can be exercised without filling
    /// `QUEUE_CAPACITY / partitions` slots.
    fn new_with_capacity<T>(partitions: usize, per_partition_cap: usize) -> (QueueSender<T>, QueueReceiver<T>) {
        let mut txs = Vec::with_capacity(partitions);
        let mut rxs = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let (tx, rx) = crossbeam_channel::bounded(per_partition_cap);
            txs.push(tx);
            rxs.push(rx);
        }
        (QueueSender { txs }, QueueReceiver { rxs })
    }

    #[test]
    fn push_and_get_basic_send_receive() {
        let (tx, rx) = new::<u32>(1);
        let recv = rx.get(0);
        tx.push(0, 42u32);
        assert_eq!(recv.recv().unwrap(), 42);
    }

    #[test]
    fn len_reflects_in_flight_count_across_partitions() {
        let (tx, rx) = new::<u32>(2);
        let _r0 = rx.get(0);
        let _r1 = rx.get(1);
        assert_eq!(tx.len(), 0);
        tx.push(0, 1); // partition 0
        tx.push(1, 2); // partition 1
        tx.push(2, 3); // partition 0 (2 % 2)
        assert_eq!(tx.len(), 3);
    }

    #[test]
    fn dropping_sender_disconnects_receiver() {
        let (tx, rx) = new::<u32>(1);
        let recv = rx.get(0);
        drop(tx);
        assert!(recv.recv().is_err());
    }

    #[test]
    fn queue_blocks_producer_at_partition_capacity() {
        let (tx, rx) = new_with_capacity::<u32>(1, 2);
        let recv = rx.get(0);

        tx.push(0, 1);
        tx.push(0, 2); // partition now full

        let tx2 = tx.clone();
        let handle = thread::spawn(move || {
            tx2.push(0, 3); // blocks until consumer frees a slot
        });

        thread::sleep(Duration::from_millis(20));
        assert!(
            !handle.is_finished(),
            "producer should be blocked on a full partition"
        );

        assert_eq!(recv.recv().unwrap(), 1);
        handle
            .join()
            .expect("producer thread should complete after slot freed");
    }

    #[test]
    fn push_timeout_returns_unit_when_full_and_timeout_expires() {
        let (tx, _rx) = new_with_capacity::<u32>(1, 1);
        tx.push(0, 1);
        let result = tx.push_timeout(0, 2, Duration::from_millis(20));
        assert!(result.is_err(), "push_timeout should time out");
        assert_eq!(result.unwrap_err(), 2);
    }

    #[test]
    fn push_timeout_succeeds_when_slot_available() {
        let (tx, rx) = new_with_capacity::<u32>(1, 2);
        let _r = rx.get(0);
        assert!(tx.push_timeout(0, 42, Duration::from_millis(20)).is_ok());
    }

    #[test]
    fn push_timeout_returns_unit_when_disconnected() {
        let (tx, rx) = new_with_capacity::<u32>(1, 1);
        drop(rx);
        let result = tx.push_timeout(0, 99, Duration::from_millis(20));
        assert_eq!(result.unwrap_err(), 99);
    }

    #[test]
    fn partition_isolation_preserves_order_per_key() {
        // The whole point of partitioning: items pushed with the same key
        // arrive at the same receiver in submission order, regardless of
        // what concurrent producers are doing on other keys.
        let (tx, rx) = new::<u32>(4);
        let recv0 = rx.get(0);
        // Push 50 items to partition 0 interleaved with pushes to other
        // partitions. The partition-0 receiver must see them in submission order.
        for i in 0..50u32 {
            tx.push(0, i);                 // partition 0
            tx.push(1, 1000 + i);          // partition 1
            tx.push(2, 2000 + i);          // partition 2
        }
        let mut seen: Vec<u32> = Vec::new();
        while let Ok(v) = recv0.try_recv() {
            seen.push(v);
        }
        assert_eq!(
            seen,
            (0..50u32).collect::<Vec<_>>(),
            "partition 0 receiver must see exactly its own pushes in submission order"
        );
    }

    #[test]
    fn partition_key_modulus_routes_correctly() {
        let (tx, rx) = new::<u32>(3);
        let r0 = rx.get(0);
        let r1 = rx.get(1);
        let r2 = rx.get(2);
        tx.push(0, 10);  // partition 0
        tx.push(1, 20);  // partition 1
        tx.push(2, 30);  // partition 2
        tx.push(3, 40);  // partition 0 (3 % 3)
        tx.push(4, 50);  // partition 1 (4 % 3)
        assert_eq!(r0.try_recv().unwrap(), 10);
        assert_eq!(r0.try_recv().unwrap(), 40);
        assert_eq!(r1.try_recv().unwrap(), 20);
        assert_eq!(r1.try_recv().unwrap(), 50);
        assert_eq!(r2.try_recv().unwrap(), 30);
    }

    #[test]
    #[should_panic(expected = "at least one partition")]
    fn new_panics_with_zero_partitions() {
        let _ = new::<u32>(0);
    }
}
