# Phase 3 · Stream B — Bridge-Thread Removal — Implementation Plan

> **For agentic workers:** Implement task-by-task with TDD. Keep every existing test green on every commit (`--test-threads=1` locally for the known WAB/socket `$TMPDIR` flake). Build AND clippy must be clean **both** with and without `--features bench-trace`. Commit per task.

**Goal:** Eliminate the per-shard bridge thread. Today the hot path is `worker → Batch-channel → bridge thread → WabRecord-channel → flusher`; collapse it to `worker → Batch-channel → flusher`, removing one OS thread and one channel hop per shard. Pure topology change — no durability, ordering, or ack semantics change.

**Architecture:** Unify on a single set of per-shard `Batch` channels. `wab::spawn` creates them and returns the `Sender<Batch>`s; `worker::spawn_workers` accepts those senders and sends `Batch`es directly to the flushers. The flusher consumes `Batch` (a `Vec<WorkUnit>`), accumulating multiple `Batch`es per fsync to preserve today's cross-batch fsync coalescing. The `WabRecord` type is deleted (the flusher uses `WorkUnit` directly).

**Why measurement says this is a *simplification* win, not a latency win:** Stream A's baseline shows `bridge_wait` is only ~4 µs of Sync's 166 µs (fsync dominates at ~89%). The value here is a simpler topology (one fewer thread/channel per shard) and reduced context-switch pressure under herd load — proven by the `bench-trace` `stage_bridge_wait` histogram dropping after the change.

## Current wiring (verified)

- `worker.rs`: `Batch { shard_id, records: Vec<WorkUnit>, #[cfg(bench-trace)] flushed_at }`. `spawn_workers(queue_rx, shard_count, worker_count, batch_size, deadline)` **creates** its own `Vec<Sender<Batch>>` / returns `Vec<Receiver<Batch>>` (`worker.rs:204-210`), and `flush_shard` (`worker.rs:146`) sends `Batch`es into them.
- `wab/mod.rs`: `spawn` creates `bounded::<WabRecord>(batch_size*4)` per shard (`wab/mod.rs:224`), returns `WabHandle { shard_txs: Vec<Sender<WabRecord>>, join_handles }`. `flusher_thread` (`:333`) recv-loops `WabRecord`, `flush_batch` (`:434`) drains a `Vec<WabRecord>`.
- `main.rs:206-226`: bridge threads recv `Batch` from worker's `shard_batch_rxs`, convert each `WorkUnit → WabRecord`, send to `wab_handle.shard_txs`.
- Shutdown chain (`main.rs:575-599`): queue→workers→**bridge**→wab→drain, joined in that order.

---

### Task 1: Move `Batch` to `models.rs` and delete `WabRecord`

**Files:** `models.rs`, `worker.rs`, `wab/mod.rs`

- [ ] **Step 1:** Move the `Batch` struct (and its `#[allow(dead_code)]` `shard_id` doc-comment, plus the `#[cfg(feature = "bench-trace")] flushed_at` field) from `worker.rs` into `models.rs`, next to `WorkUnit`. Re-export or `use crate::models::Batch` in `worker.rs`. `Batch.records` stays `Vec<WorkUnit>`.
- [ ] **Step 2:** Delete the `WabRecord` struct from `wab/mod.rs` (including its `#[cfg(bench-trace)] enqueued_at`/`worker_flushed_at` fields). The flusher will consume `WorkUnit` via `Batch`. Keep `WorkUnit.enqueued_at` (still set at enqueue); the flusher reads `batch.flushed_at` for the worker-flush timestamp instead of a per-record `worker_flushed_at`.
- [ ] **Step 3:** Build (no feature) — `cargo build -p weir-server`. Expect compile errors only at the now-broken `wab`/`main` wiring (fixed in Tasks 2–3); this step is just to confirm the type moves themselves are sound. (It's fine if the crate doesn't fully build until Task 3 — verify the `models.rs` change compiles in isolation by checking the error list contains only wiring errors in `wab/mod.rs` and `main.rs`.)

---

### Task 2: Flusher consumes `Batch`; `wab::spawn` owns the channels

**Files:** `wab/mod.rs`

- [ ] **Step 1:** Change the per-shard channel in `spawn` to `crossbeam_channel::bounded::<Batch>(batch_size * 4)`. `WabHandle.shard_txs` becomes `Vec<Sender<Batch>>`. Keep the panic-supervision body factory (channel clones still share the same queue — now of `Batch`es; the "records buffered survive a flusher panic" property holds at Batch granularity).
- [ ] **Step 2:** Rewrite `flusher_thread`'s `work_rx` to `Receiver<Batch>` and its accumulation loop to gather **multiple `Batch`es per fsync** (preserving today's cross-batch coalescing):
```rust
let mut batches: Vec<Batch> = Vec::new();
let mut record_count = 0usize;
loop {
    match work_rx.recv_timeout(batch_deadline) {
        Ok(batch) => { record_count += batch.records.len(); batches.push(batch); }
        Err(RecvTimeoutError::Disconnected) => break,
        Err(RecvTimeoutError::Timeout) => {
            if !batches.is_empty() {
                flush_batch(&mut writer, &mut batches, &drain_tx, shard_id, &metrics);
                record_count = 0;
            }
            continue;
        }
    }
    while record_count < batch_size {
        match work_rx.try_recv() {
            Ok(batch) => { record_count += batch.records.len(); batches.push(batch); }
            Err(_) => break,
        }
    }
    flush_batch(&mut writer, &mut batches, &drain_tx, shard_id, &metrics);
    record_count = 0;
}
```
- [ ] **Step 3:** Rewrite `flush_batch` to take `batches: &mut Vec<Batch>` and iterate batch-by-batch so each record knows its `batch.flushed_at`:
```rust
for batch in batches.drain(..) {
    #[cfg(feature = "bench-trace")]
    let worker_flushed_at = batch.flushed_at;
    for unit in batch.records {
        // bench-trace: observe stage_queue (worker_flushed_at - unit.enqueued_at)
        //              + stage_bridge_wait (now - worker_flushed_at), capture flusher_recv_at
        // write_record(&unit.payload) ...; observe stage_write
        // dispatch on unit.durability into sync_acks / batched_acks / Buffered-immediate-ack
        //   (collect (enqueued_at) for stage_total as today)
    }
}
// group fsync if need_fsync; observe stage_total for sync+batched; ack all
```
Port the existing Stream A bench-trace observes verbatim, substituting `unit` for `record` and `batch.flushed_at` for `record.worker_flushed_at`. The Sync/Batched/Buffered dispatch, group-fsync, `wab_fsync_duration`, segment-rotation→drain notify, and ack logic are **unchanged**.
- [ ] **Step 4:** Build/clippy both ways — `cargo build -p weir-server` + `--features bench-trace`, `cargo clippy ... -- -D warnings` both. (Will still fail at `main.rs` wiring until Task 3 — confirm `wab/mod.rs` itself is clean.)

---

### Task 3: Wire worker→flusher directly; remove the bridge

**Files:** `worker.rs`, `main.rs`

- [ ] **Step 1:** Change `spawn_workers` to **accept** the flusher senders instead of creating its own:
  `pub fn spawn_workers(queue_rx, shard_txs: Vec<Sender<Batch>>, shard_count, worker_count, batch_size, batch_deadline) -> Vec<thread::JoinHandle<()>>` (no more returned `Vec<Receiver<Batch>>`). Each worker clones `shard_txs` as today. Delete the internal channel-creation block (`worker.rs:204-210`). Keep the `assert_eq!(queue_rx.partitions(), worker_count, ...)`.
- [ ] **Step 2:** In `main.rs`: spawn the WAB first (it already returns `wab_handle.shard_txs: Vec<Sender<Batch>>`), then call `worker::spawn_workers(&queue_rx, wab_handle.shard_txs, ...)`. **Delete the bridge-thread block** (`main.rs:201-226`) and the `bridge_handles` vector and its join loop (`main.rs:593-595`). Remove the now-unused `use wab::WabRecord;` import.
- [ ] **Step 3:** Update the shutdown-drain doc comment (`main.rs:575-585`) to drop the bridge step: queue→workers→wab→drain. The drop/seal ordering otherwise holds (worker `Disconnected` → flush remaining `Batch`es → drop `shard_txs` clones → flusher `Disconnected` → seal → drop drain_tx → drain exits).
- [ ] **Step 4:** Update worker unit tests that called the old `spawn_workers` signature (`worker.rs` tests create channels + assert on `batch_rxs`). They must now create the `Sender<Batch>`/`Receiver<Batch>` pair themselves, pass the sender in, and assert on the receiver — preserving each test's intent (`single_worker_batches_on_deadline`, `batch_flushes_at_batch_size`, `pending_batches_flushed_on_sender_drop`, `spawn_workers_returns_correct_counts`, `worker_exits_cleanly_after_disconnect`).
- [ ] **Step 5:** Full build/clippy/test:
  - `cargo build -p weir-server` and `--features bench-trace` — PASS.
  - `cargo clippy --all-targets -- -D warnings` and `... --features bench-trace ...` — PASS.
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1` — PASS.
- [ ] **Step 6:** Commit — `refactor(wab): remove per-shard bridge thread; worker feeds flusher directly`

---

### Task 4: Integration + ordering verification

- [ ] **Step 1:** Run the full system + load suites to confirm no regression in correctness or per-shard ordering:
  - `cargo test -p weir-server --test system -- --test-threads=1` (skip `#[ignore]` sink E2E) — PASS.
  - `cargo test -p weir-server --test load --release -- --test-threads=1` — all scenarios PASS (esp. the herd + ramp + compression-ratio, which exercise multi-shard concurrency and FIFO).
- [ ] **Step 2:** Run the recovery/fuzz-adjacent WAB tests (the flusher is on the crash-recovery path):
  - `cargo test -p weir-server --bin weir-server wab -- --test-threads=1` — PASS.
- [ ] **Step 3:** Commit any test fixups — `test(wab): adjust for bridge-free worker→flusher wiring [skip ci]`

---

### Task 5: Prove the win (before/after via bench-trace)

- [ ] **Step 1:** Capture the per-stage breakdown after the change and compare to the Stream A baseline (`docs/benchmarks/phase3-stage-baseline.md`):
```sh
: > /tmp/stage_b.jsonl
for d in 1 2; do
  WEIR_BENCH_DEADLINE=$d cargo test -p weir-server --test load --release --features bench-trace \
    latency_stage_breakdown -- --nocapture | grep '^BENCH_STAGE: ' >> /tmp/stage_b.jsonl
done
python3 deploy/avg_benchmarks.py /tmp/stage_b.jsonl docs/benchmarks/phase3-stage-after-bridge-removal.md
```
- [ ] **Step 2:** Confirm `stage_bridge_wait` dropped (it now measures worker-flush → flusher-dequeue with no intermediate bridge thread). Note the before/after `bridge_wait` numbers + the standard-suite throughput (herd/ramp) in the commit message. If `bridge_wait` did NOT drop, investigate before committing — that would mean the hop wasn't actually eliminated.
- [ ] **Step 3:** Commit — `docs(benchmarks): per-stage breakdown after bridge removal [skip ci]`

---

## Done criteria

- No bridge thread; `WabRecord` deleted; `worker → flusher` is one direct `Batch` channel per shard.
- Per-shard FIFO, group-fsync coalescing, ack ordering, panic supervision, and graceful-shutdown drain all unchanged and proven by the system + load + wab test suites.
- `bench-trace` `stage_bridge_wait` measurably lower than the Stream A baseline; one fewer thread per shard.
- Default and `bench-trace` builds clippy-clean; all `--bin weir-server` + `--test system` + `--test load` tests green.
