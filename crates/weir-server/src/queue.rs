use crossbeam_channel::{Receiver, Sender};

/// Bounded MPMC queue capacity. The socket layer blocks on `push` when the
/// channel is full, providing backpressure that signals producers to slow
/// down rather than growing unboundedly or silently dropping records.
pub const QUEUE_CAPACITY: usize = 65_536;

/// Send half of the work queue. Cheap to clone — all clones share the same
/// underlying bounded channel. Drop all clones to close the channel and
/// propagate the shutdown signal to every worker `Receiver`.
pub struct QueueSender<T> {
    tx: Sender<T>,
}

/// Receive half of the work queue. Call `get()` once per worker thread to
/// hand each worker its own `Receiver` clone from the shared MPMC channel.
pub struct QueueReceiver<T> {
    rx: Receiver<T>,
}

/// Creates a bounded MPMC work queue with `QUEUE_CAPACITY` slots.
///
/// Dropping all `QueueSender` clones closes the channel; every `Receiver`
/// clone returned by `get()` will observe `RecvError::Disconnected` once the
/// in-flight items are drained, propagating the shutdown signal to workers.
pub fn new<T>() -> (QueueSender<T>, QueueReceiver<T>) {
    let (tx, rx) = crossbeam_channel::bounded(QUEUE_CAPACITY);
    (QueueSender { tx }, QueueReceiver { rx })
}

impl<T> QueueSender<T> {
    /// Pushes a work unit onto the queue, blocking the calling thread until a
    /// slot is available.
    ///
    /// This is the intentional backpressure point: the socket handler blocks
    /// here under load rather than allocating unboundedly or dropping records.
    /// If all `QueueReceiver` clones have been dropped (workers exited), the
    /// unit is silently discarded — the caller is in the shutdown path and the
    /// socket listener will be closed imminently.
    pub fn push(&self, unit: T) {
        self.tx.send(unit).ok();
    }

    /// Returns the number of items currently buffered in the channel.
    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx.is_empty()
    }
}

impl<T> Clone for QueueSender<T> {
    fn clone(&self) -> Self {
        QueueSender { tx: self.tx.clone() }
    }
}

impl<T> QueueReceiver<T> {
    /// Returns a `Receiver` clone for one worker thread.
    ///
    /// Each worker should call this once during setup. All returned receivers
    /// share the same MPMC channel — any sender push is delivered to exactly
    /// one receiver (fair work distribution with no duplication).
    pub fn get(&self) -> Receiver<T> {
        self.rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// Test-only constructor with a custom capacity so blocking behaviour can
    /// be exercised without filling 65_536 slots.
    fn new_with_capacity<T>(cap: usize) -> (QueueSender<T>, QueueReceiver<T>) {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        (QueueSender { tx }, QueueReceiver { rx })
    }

    #[test]
    fn push_and_get_basic_send_receive() {
        let (tx, rx) = new::<u32>();
        let recv = rx.get();
        tx.push(42u32);
        assert_eq!(recv.recv().unwrap(), 42);
    }

    #[test]
    fn len_reflects_in_flight_count() {
        let (tx, rx) = new::<u32>();
        let _recv = rx.get(); // keep receiver alive so push does not discard
        assert_eq!(tx.len(), 0);
        tx.push(1);
        tx.push(2);
        tx.push(3);
        assert_eq!(tx.len(), 3);
    }

    #[test]
    fn dropping_sender_disconnects_receiver() {
        let (tx, rx) = new::<u32>();
        let recv = rx.get();
        drop(tx);
        // No senders remain — recv must observe Disconnected.
        assert!(recv.recv().is_err());
    }

    #[test]
    fn queue_blocks_producer_at_capacity() {
        let (tx, rx) = new_with_capacity::<u32>(2);
        let recv = rx.get();

        tx.push(1);
        tx.push(2); // channel now full

        // A third push must block. Run it on a separate thread.
        let tx2 = tx.clone();
        let handle = thread::spawn(move || {
            tx2.push(3); // blocks until consumer frees a slot
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!handle.is_finished(), "producer should be blocked on a full queue");

        // Drain one slot — producer should unblock.
        assert_eq!(recv.recv().unwrap(), 1);
        handle.join().expect("producer thread should complete after slot freed");
    }

    #[test]
    fn multiple_receivers_share_channel() {
        let (tx, rx) = new::<u32>();
        let recv_a = rx.get();
        let recv_b = rx.get();

        tx.push(10);
        tx.push(20);

        // Drain both receivers. Each item is delivered to exactly one receiver
        // with no duplication and no loss — total must be exactly [10, 20].
        let mut items = Vec::new();
        while let Ok(v) = recv_a.try_recv() { items.push(v); }
        while let Ok(v) = recv_b.try_recv() { items.push(v); }
        items.sort_unstable();
        assert_eq!(items, vec![10, 20]);
    }
}
