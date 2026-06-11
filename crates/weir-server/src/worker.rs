use std::{thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tracing::warn;

use crate::{
    models::{Batch, WorkUnit},
    queue::QueueReceiver,
};

/// Workers are pinned starting at this logical core index, leaving cores 0–1
/// free for the OS scheduler and network interrupt handlers.
const WORKER_CORE_START: usize = 2;

/// Per-worker state. One `Worker` runs on one thread and holds independent
/// per-shard batch buffers — no lock contention on the hot path.
struct Worker {
    /// One pre-allocated buffer per shard. Swapped out on flush via
    /// `std::mem::replace` to avoid per-record allocation.
    buffers: Vec<Vec<WorkUnit>>,
    batch_size: usize,
    /// One sender per shard, shared (cloned) across all worker threads.
    shard_txs: Vec<Sender<Batch>>,
}

impl Worker {
    fn new(shard_count: usize, batch_size: usize, shard_txs: Vec<Sender<Batch>>) -> Self {
        let buffers = pretouched_buffers(shard_count, batch_size);
        Worker {
            buffers,
            batch_size,
            shard_txs,
        }
    }

    fn run(mut self, work_rx: Receiver<WorkUnit>, batch_deadline: Duration) {
        // Adaptive coalesce — predict the next batch's shape from the
        // previous one. Investigation (load.rs::investigate_herd_64_ceiling)
        // showed the worker alternating between tiny "I got here first"
        // batches and large "queue-was-piling-up-during-fsync" batches in
        // multi-producer Sync workloads. The wake-up record alone isn't
        // a reliable concurrency signal — by the time the worker sees the
        // first producer's next push, the other 15 producers' acks are
        // still completing their roundtrip (~50–100 μs).
        //
        // Use the PREVIOUS batch's size as the predictor instead:
        //   - last batch had ≥ 2 records → next one likely will too;
        //     pay the coalesce window so the staggered burst lands in
        //     ONE big batch instead of two unequal halves.
        //   - last batch was solo (1 record) → single-producer or
        //     sporadic; skip the coalesce window so we don't tax
        //     latency.
        //
        // 200 μs covers the worst-case stagger of ~16 producers' next-
        // cycle pushes after a 1 ms fsync. The recv_timeout loop EXTENDS
        // each time a record arrives, so the wait collapses naturally
        // once records stop coming.
        const COALESCE_WINDOW: Duration = Duration::from_micros(200);
        let mut expect_concurrent = false;

        loop {
            match work_rx.recv_timeout(batch_deadline) {
                Ok(unit) => {
                    let shard = (unit.shard_id as usize) % self.buffers.len();
                    self.buffers[shard].push(unit);
                    if self.buffers[shard].len() >= self.batch_size {
                        self.flush_shard(shard);
                    }
                    let mut total_drained: usize = 1; // the wake-up record
                    // Phase 1: wait-free drain.
                    while self.any_buffer_below_batch_size() {
                        match work_rx.try_recv() {
                            Ok(unit) => {
                                total_drained += 1;
                                let shard = (unit.shard_id as usize) % self.buffers.len();
                                self.buffers[shard].push(unit);
                                if self.buffers[shard].len() >= self.batch_size {
                                    self.flush_shard(shard);
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    // Phase 2: coalesce only when the PREVIOUS batch told us
                    // to expect more. This catches the multi-producer case
                    // even when "we got here first" — the first record's
                    // arrival hits this branch because the previous batch
                    // (large) set the flag.
                    if expect_concurrent {
                        while self.any_buffer_below_batch_size() {
                            match work_rx.recv_timeout(COALESCE_WINDOW) {
                                Ok(unit) => {
                                    total_drained += 1;
                                    let shard = (unit.shard_id as usize) % self.buffers.len();
                                    self.buffers[shard].push(unit);
                                    if self.buffers[shard].len() >= self.batch_size {
                                        self.flush_shard(shard);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                    // Self-correcting: if we mispredict, the next batch's
                    // size updates the flag.
                    expect_concurrent = total_drained >= 2;
                    self.flush_all();
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Idle wake-up — clear the prediction so we don't pay
                    // the window on the first record after a quiet period.
                    expect_concurrent = false;
                    self.flush_all();
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.on_disconnect();
                    break;
                }
            }
        }
    }

    /// Returns true if any shard's buffer has room left before hitting
    /// `batch_size`. Used as the inner-loop drain guard so we never overrun
    /// the batch-size ceiling — if every buffer is already at the ceiling,
    /// continuing the drain just wastes a try_recv.
    fn any_buffer_below_batch_size(&self) -> bool {
        self.buffers.iter().any(|b| b.len() < self.batch_size)
    }

    /// Flushes one shard's buffer. Swaps in a fresh pre-allocated buffer so
    /// the next batch pays no allocation cost on the first push.
    fn flush_shard(&mut self, shard: usize) {
        if self.buffers[shard].is_empty() {
            return;
        }
        let records = std::mem::replace(
            &mut self.buffers[shard],
            Vec::with_capacity(self.batch_size),
        );
        self.shard_txs[shard]
            .send(Batch {
                shard_id: shard as u32,
                records,
                #[cfg(feature = "bench-trace")]
                flushed_at: std::time::Instant::now(),
            })
            .ok(); // receiver gone means WAB is shutting down; discard silently
    }

    fn flush_all(&mut self) {
        for shard in 0..self.buffers.len() {
            self.flush_shard(shard);
        }
    }

    /// Called on queue disconnect (graceful shutdown). Marked `#[cold]` and
    /// `#[inline(never)]` to keep the code off the hot path and bias the
    /// branch predictor toward the `Ok(unit)` arm in the main loop.
    #[cold]
    #[inline(never)]
    fn on_disconnect(&mut self) {
        self.flush_all();
    }
}

/// Spawns `worker_count` worker threads. Each worker owns one queue partition
/// (`worker_idx == partition_idx`) and routes the units it pulls into
/// per-shard batch buffers.
///
/// **Per-shard ordering**: the connection layer pushes each record with
/// `partition_key = shard_id`, so every record destined for a given shard
/// lands in the same partition and is handled by a single worker. The
/// partition receiver is FIFO, the worker's intra-shard buffer is FIFO, and
/// the shard's flusher channel is FIFO — so a record acked to a producer is
/// guaranteed to reach the per-shard WAB writer ahead of any record acked
/// later for the same shard, even across multiple concurrent connections.
///
/// Accepts `shard_txs`: one `Sender<Batch>` per shard, owned by the WAB
/// (`wab::spawn` creates the channels and returns the senders via `WabHandle`).
/// Workers send `Batch`es directly to the flusher — no intermediate bridge
/// thread. Returns one `JoinHandle` per worker. Workers exit cleanly when all
/// `QueueSender` clones are dropped.
pub fn spawn_workers(
    queue_rx: &QueueReceiver<WorkUnit>,
    shard_txs: Vec<Sender<Batch>>,
    shard_count: usize,
    worker_count: usize,
    batch_size: usize,
    batch_deadline: Duration,
) -> Vec<thread::JoinHandle<()>> {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    assert_eq!(
        queue_rx.partitions(),
        worker_count,
        "worker count must equal queue partition count — every worker owns \
         exactly one partition so per-shard FIFO is preserved"
    );

    let mut handles = Vec::with_capacity(worker_count);
    for worker_idx in 0..worker_count {
        let work_rx = queue_rx.get(worker_idx);
        let txs = shard_txs.clone();
        let core_id = if core_ids.is_empty() {
            None
        } else {
            Some(core_ids[(WORKER_CORE_START + worker_idx) % core_ids.len()])
        };

        let handle = thread::Builder::new()
            .name(format!("weir-worker-{worker_idx}"))
            .spawn(move || {
                // ── Core affinity ────────────────────────────────────────────
                if let Some(id) = core_id
                    && !core_affinity::set_for_current(id)
                {
                    warn!(
                        worker = worker_idx,
                        "failed to set CPU affinity; continuing"
                    );
                }

                // ── Thread priority ──────────────────────────────────────────
                #[cfg(target_os = "linux")]
                {
                    let param = libc::sched_param { sched_priority: 1 };
                    let ret = unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) };
                    if ret == -1 {
                        warn!(
                            worker = worker_idx,
                            "SCHED_FIFO unavailable; continuing with default scheduler"
                        );
                    }
                }

                #[cfg(target_os = "macos")]
                {
                    // libc does not expose pthread_set_qos_class_self_np or
                    // QOS_CLASS_USER_INTERACTIVE (0x21); declare them directly.
                    unsafe extern "C" {
                        fn pthread_set_qos_class_self_np(
                            qos_class: libc::c_uint,
                            relative_priority: libc::c_int,
                        ) -> libc::c_int;
                    }
                    const QOS_CLASS_USER_INTERACTIVE: libc::c_uint = 0x21;
                    let ret =
                        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
                    if ret != 0 {
                        warn!(
                            worker = worker_idx,
                            "QOS_CLASS_USER_INTERACTIVE unavailable; continuing"
                        );
                    }
                }

                // ── Warmup ───────────────────────────────────────────────────
                simd_warmup();

                let worker = Worker::new(shard_count, batch_size, txs);
                worker.run(work_rx, batch_deadline);
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    handles
}

/// Allocates per-shard batch buffers and pre-touches their backing pages so
/// the first push on the hot path does not incur a page-fault penalty.
fn pretouched_buffers(shard_count: usize, batch_size: usize) -> Vec<Vec<WorkUnit>> {
    let mut buffers: Vec<Vec<WorkUnit>> = (0..shard_count)
        .map(|_| Vec::with_capacity(batch_size))
        .collect();

    let item_size = std::mem::size_of::<WorkUnit>();
    if item_size > 0 && batch_size > 0 {
        for buf in &mut buffers {
            let ptr = buf.as_mut_ptr() as *mut u8;
            let byte_cap = buf.capacity() * item_size;
            let mut offset = 0;
            while offset < byte_cap {
                // Safety: `offset < byte_cap` and the memory was allocated by
                // `Vec::with_capacity`, so this is within the owned allocation.
                unsafe {
                    ptr.add(offset).write_volatile(0u8);
                }
                offset = offset.saturating_add(4096);
            }
        }
    }

    buffers
}

// ── Platform SIMD / FP warmup ─────────────────────────────────────────────────
// 10 000 multiply-accumulate iterations prime the FP pipeline and instruction
// cache before the first real record arrives, amortising the cold-start penalty.

#[cfg(target_arch = "x86_64")]
fn simd_warmup() {
    use std::arch::x86_64::*;
    // Safety: SSE is baseline on x86_64; no runtime check required.
    unsafe {
        let mut acc = _mm_setzero_ps();
        let factor = _mm_set1_ps(1.0001_f32);
        let addend = _mm_set1_ps(0.001_f32);
        for _ in 0..10_000 {
            acc = _mm_mul_ps(acc, factor);
            acc = _mm_add_ps(acc, addend);
        }
        let _ = std::hint::black_box(acc);
    }
}

#[cfg(target_arch = "aarch64")]
fn simd_warmup() {
    use std::arch::aarch64::*;
    // Safety: NEON is mandatory on AArch64; no runtime check required.
    unsafe {
        let mut acc = vdupq_n_f32(0.0_f32);
        let factor = vdupq_n_f32(1.0001_f32);
        let addend = vdupq_n_f32(0.001_f32);
        for _ in 0..10_000 {
            acc = vmulq_f32(acc, factor);
            acc = vaddq_f32(acc, addend);
        }
        let _ = std::hint::black_box(acc);
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn simd_warmup() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue;
    use crossbeam_channel::Receiver;
    use tokio::sync::oneshot;

    fn make_unit(shard_id: u32, payload: &[u8]) -> (WorkUnit, oneshot::Receiver<bool>) {
        let (tx, rx) = oneshot::channel();
        (
            WorkUnit {
                shard_id,
                payload: payload.to_vec(),
                durability: weir_core::Durability::Buffered,
                ack_tx: tx,
                #[cfg(feature = "bench-trace")]
                enqueued_at: std::time::Instant::now(),
            },
            rx,
        )
    }

    /// Create a set of per-shard `(Sender<Batch>, Receiver<Batch>)` pairs for
    /// tests. The test holds the receivers; the senders are passed to
    /// `spawn_workers` (mirroring how `wab::spawn` owns them in production).
    fn make_shard_channels(shard_count: usize) -> (Vec<Sender<Batch>>, Vec<Receiver<Batch>>) {
        let mut txs = Vec::with_capacity(shard_count);
        let mut rxs = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = crossbeam_channel::bounded(64);
            txs.push(tx);
            rxs.push(rx);
        }
        (txs, rxs)
    }

    #[test]
    fn single_worker_batches_on_deadline() {
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let (shard_txs, batch_rxs) = make_shard_channels(1);
        let handles = spawn_workers(&queue_rx, shard_txs, 1, 1, 10, Duration::from_millis(20));

        let (unit, _ack) = make_unit(0, b"hello");
        queue_tx.push(0, unit);

        let batch = batch_rxs[0]
            .recv_timeout(Duration::from_millis(200))
            .expect("batch should arrive after deadline flush");
        assert_eq!(batch.shard_id, 0);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].payload.as_slice(), b"hello");

        drop(queue_tx);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn batch_flushes_at_batch_size() {
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let batch_size = 3;
        // 60s deadline — flush must be triggered by batch-full, not timeout.
        let (shard_txs, batch_rxs) = make_shard_channels(1);
        let handles =
            spawn_workers(&queue_rx, shard_txs, 1, 1, batch_size, Duration::from_secs(60));

        for _ in 0..batch_size {
            let (unit, _) = make_unit(0, b"x");
            queue_tx.push(0, unit);
        }

        let batch = batch_rxs[0]
            .recv_timeout(Duration::from_millis(500))
            .expect("batch should flush when batch_size is reached");
        assert_eq!(batch.records.len(), batch_size);

        drop(queue_tx);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn pending_batches_flushed_on_sender_drop() {
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        // 60s deadline — flush must be triggered by disconnect, not timeout.
        let (shard_txs, batch_rxs) = make_shard_channels(1);
        let handles = spawn_workers(&queue_rx, shard_txs, 1, 1, 100, Duration::from_secs(60));

        let (unit, _) = make_unit(0, b"pending");
        queue_tx.push(0, unit);
        drop(queue_tx); // triggers Disconnected in the worker

        let batch = batch_rxs[0]
            .recv_timeout(Duration::from_millis(500))
            .expect("pending records should be flushed on disconnect");
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].payload.as_slice(), b"pending");

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn spawn_workers_returns_correct_counts() {
        let (_queue_tx, queue_rx) = queue::new::<WorkUnit>(2);
        let (shard_txs, _batch_rxs) = make_shard_channels(3);
        let handles = spawn_workers(&queue_rx, shard_txs, 3, 2, 100, Duration::from_millis(10));
        // shard channel count is set by the caller (3 in this case)
        assert_eq!(_batch_rxs.len(), 3);
        assert_eq!(handles.len(), 2);

        drop(_queue_tx);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn worker_exits_cleanly_after_disconnect() {
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let (shard_txs, _batch_rxs) = make_shard_channels(1);
        let handles = spawn_workers(&queue_rx, shard_txs, 1, 1, 10, Duration::from_millis(10));

        drop(queue_tx);
        for h in handles {
            h.join()
                .expect("worker thread should exit cleanly after disconnect");
        }
    }
}
