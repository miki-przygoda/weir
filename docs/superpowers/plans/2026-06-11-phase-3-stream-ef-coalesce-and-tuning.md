# Phase 3 · Streams E + F — Adaptive Coalesce Window + Tuning Defaults — Implementation Plan

> **For agentic workers:** Implement task-by-task with TDD. Build + clippy clean across default / `--features bench-trace` / `--all-features`. Keep all tests green (`--test-threads=1` locally). Commit per stream.

Two related worker/config changes. **Stream E** makes the worker's coalesce window adapt to observed fsync latency (instead of a fixed 200 µs). **Stream F** fixes a default-config inefficiency (an idle worker). The Prometheus counter-cache idea from the original Stream F is **dropped** — Stream A measured the whole push→enqueue path at ~2 µs, so caching a counter lookup there is immaterial (it would violate our own "prove it's material first" gate).

**Measurement honesty (Stream E):** the win shows on storage *slower* than the dev machine's NVMe (cloud volumes, spinning disks), where a fixed 200 µs window is too short and fragments batches → more fsyncs → lower throughput. On fast local NVMe (fsync ~150 µs) the adaptive window lands near the existing 200 µs, so the throughput gain is **not demonstrable on macOS**. Unit-test the EWMA logic here; validate the throughput effect on Linux/cloud storage later (alongside the io_uring spike). Do NOT claim a local throughput win.

---

## Stream E — Adaptive coalesce window (EWMA of fsync latency)

**Rationale:** The worker's coalesce window (`worker.rs:58`, `const COALESCE_WINDOW = 200µs`) should be long enough to gather the staggered burst of producers' next records, which arrive roughly one fsync-latency after the previous batch's acks. A fixed 200 µs assumes ~NVMe fsync; on slower storage it's too short. Track fsync latency with an EWMA and size the window from it.

**Design:** A shared `Arc<AtomicU64>` holding the EWMA fsync duration in microseconds. The flusher updates it after each fsync; the worker reads it each batch to size the coalesce window. Lock-free, `Relaxed` ordering (it's a heuristic, not a correctness signal).

### Task E1: Shared EWMA atomic + flusher updates it

**Files:** new tiny module or `wab/mod.rs`; `wab/mod.rs` (`fsync_observed`, `flusher_thread`, `spawn`); `main.rs` (wiring)

- [ ] **Step 1:** Define a small helper for the EWMA update (pure, unit-testable):
```rust
/// Exponential moving average update, fixed-point microseconds.
/// alpha = 1/4 (new sample weighted 25%). Pure fn so it's unit-tested.
pub(crate) fn ewma_update_us(current_us: u64, sample_us: u64) -> u64 {
    // current*3/4 + sample/4, integer math, no overflow for realistic µs.
    (current_us.saturating_mul(3) / 4).saturating_add(sample_us / 4)
}
```
- [ ] **Step 2:** Create `let coalesce_hint = Arc::new(AtomicU64::new(200));` in `main.rs` (initial = today's fixed 200 µs). Thread an `Arc<AtomicU64>` into `wab::spawn` → each `flusher_thread` → `fsync_observed` (add the param). In `fsync_observed` (wab/mod.rs:550), after `t.elapsed()`, do:
```rust
let sample_us = t.elapsed().as_micros() as u64;
let cur = coalesce_hint.load(Relaxed);
coalesce_hint.store(ewma_update_us(cur, sample_us), Relaxed);
```
(Keep the existing `wab_fsync_duration.observe(...)` unchanged.)
- [ ] **Step 3:** Unit-test `ewma_update_us`: converges toward a constant input; a single spike moves it only ~25%; clamping behaviour (see E2). Build + clippy clean both feature ways.
- [ ] **Step 4:** Commit — `feat(wab): track EWMA of fsync latency for adaptive coalescing`

### Task E2: Worker reads the hint to size its window

**Files:** `worker.rs` (`Worker`, `spawn_workers`, `run`); `main.rs` (pass the Arc)

- [ ] **Step 1:** Give `Worker` a `coalesce_hint: Arc<AtomicU64>` field; thread it through `spawn_workers` (new param) from `main.rs` (the same Arc created in E1).
- [ ] **Step 2:** In `Worker::run`, replace the `const COALESCE_WINDOW` usage (worker.rs:91) with a per-batch read:
```rust
// Adapt the coalesce window to observed fsync latency, clamped so it can
// neither collapse to nothing nor add unbounded latency. On fast NVMe this
// lands near the old fixed 200µs; on slow/cloud storage it widens so the
// staggered producer burst lands in one batch (fewer fsyncs, more throughput).
const COALESCE_MIN_US: u64 = 50;
const COALESCE_MAX_US: u64 = 2_000;
let window = Duration::from_micros(
    self.coalesce_hint.load(Relaxed).clamp(COALESCE_MIN_US, COALESCE_MAX_US),
);
```
Use `window` in the `recv_timeout(window)` coalesce call. (Read it once per outer-loop batch, not per inner record.)
- [ ] **Step 3:** Update worker unit tests for the new `spawn_workers` signature (they construct workers directly). Build + clippy clean both feature ways; `cargo test -p weir-server --bin weir-server -- --test-threads=1` PASS.
- [ ] **Step 4:** Run the load suite locally to confirm **no regression** on NVMe (the adaptive window should land near 200 µs here): `cargo test -p weir-server --test load --release -- --test-threads=1`. Do NOT claim a throughput win — note "validate on slower storage (Linux)" in the commit message.
- [ ] **Step 5:** Commit — `feat(worker): adaptive coalesce window from fsync EWMA (validate gain on slow storage)`

---

## Stream F — Tuning defaults (fix the idle worker)

**Rationale:** Default `shard_count=1`, `worker_count=2` (config/mod.rs:389,392). Records route by `shard_id`, so with 1 shard every record lands on worker 0's partition — **worker 1 is idle**. The balanced invariant is `worker_count == shard_count` (each shard → its own worker partition). Default `worker_count` to the resolved `shard_count` so there's no idle worker. Keep `shard_count` default at 1 (deterministic; the existing `advise_agent_count` advisory already nudges big-box operators to raise both — aggressive core-aware auto-scaling is intentionally left to the operator).

### Task F1: Default worker_count to shard_count

**Files:** `config/mod.rs`; config default tests; `docs/operations/configuration.md`; `CHANGELOG.md`

- [ ] **Step 1:** Change `worker_count` resolution (config/mod.rs:392): when unset, default to the already-resolved `shard_count` rather than the literal `2`:
```rust
let worker_count = merge!(worker_count).unwrap_or(shard_count);
```
(Confirm `shard_count` is resolved above this line — it is, at :389.)
- [ ] **Step 2:** Update any config unit test asserting the old `worker_count == 2` default (now equals `shard_count`, i.e. 1 by default). Add/adjust a test proving `worker_count` defaults to `shard_count` when only `shard_count` is set (e.g. set `shard_count=4`, assert resolved `worker_count==4`).
- [ ] **Step 3:** Update `docs/operations/configuration.md` (the `worker_count` default description) and add a `CHANGELOG.md` entry under the unreleased/0.8.0 section noting the default change (worker_count now defaults to shard_count — removes an idle worker thread in the default single-shard config).
- [ ] **Step 4:** Build + clippy + `cargo test -p weir-server --bin weir-server -- --test-threads=1` PASS.
- [ ] **Step 5:** Commit — `perf(config): default worker_count to shard_count (no idle worker)`

### Task F2: Sanity-confirm via the agent-count sweep (optional, local)

- [ ] **Step 1:** Run the existing investigation sweep on this machine to sanity-check the heuristic isn't pathological here (it's `#[ignore]`):
  `cargo test -p weir-server --test load --release sweep_agent_count_vs_throughput -- --ignored --nocapture`
  Record the peak agent_count + RPS in the commit/PR notes. This is confirmation only — no code change.
- [ ] **Step 2:** (No commit unless the sweep reveals the heuristic is badly wrong on this host, in which case STOP and report.)

---

## Done criteria

- **E:** coalesce window is sized from an EWMA of fsync latency (clamped 50 µs–2 ms); `ewma_update_us` unit-tested; no NVMe regression; throughput gain explicitly deferred to slow-storage (Linux) validation — not claimed locally.
- **F:** default `worker_count` follows `shard_count` (no idle worker in the default config); counter-cache dropped as immaterial; tests + docs + CHANGELOG updated.
- Default / `bench-trace` / `all-features` builds clippy-clean; full `--bin weir-server` + load suites green.
