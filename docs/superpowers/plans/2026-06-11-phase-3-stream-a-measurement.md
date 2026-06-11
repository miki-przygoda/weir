# Phase 3 · Stream A — Measurement Foundation — Implementation Plan

> **For agentic workers:** Implement task-by-task with TDD. Steps use checkbox (`- [ ]`) syntax. Keep every existing test green on every commit (`--test-threads=1` locally for the known WAB/socket `$TMPDIR` flake). Commit after each task.

**Goal:** Make write-path latency attributable to a pipeline *stage* (queue+coalesce, bridge+flusher-wait, write, fsync) and tighten the benchmark's statistical confidence — all behind a `bench-trace` cargo feature that costs nothing in a normal release build.

**Architecture:** weir-server runs as a child process under the testkit; the load suite scrapes `/metrics`. So per-stage timing is exposed as Prometheus histograms (built into the binary under `bench-trace`) and scraped by a new gated bench test. Timestamps are captured at queue-enqueue, worker-flush, and flusher-dequeue, carried through `WorkUnit → Batch → WabRecord`, and the per-stage deltas are observed in the WAB flusher (which already holds the metrics handle).

**Tech stack:** Rust, `prometheus-client` histograms, `std::time::Instant`, existing `weir-testkit` child-process harness, `deploy/avg_benchmarks.py`.

---

## Pipeline map (verified against current code)

- **Enqueue:** `socket/connection.rs:281` — `WorkUnit` is constructed in `handle_push`, then `try_push` at `:298`. Capture `enqueued_at` at construction.
- **Worker flush:** `worker.rs:146 flush_shard` builds a `Batch` and sends it. Capture `worker_flushed_at` here (per batch).
- **Bridge:** `main.rs:206-226` converts each `Batch` record → `WabRecord` and sends to the flusher. Carry both timestamps across.
- **Flusher:** `wab/mod.rs` flusher thread dequeues `WabRecord`s, `flush_batch` writes each, then one group `fsync_current` per batch, then fires `ack_tx`. Capture `flusher_recv_at` on dequeue and observe all stage deltas here.
- **Existing metric:** `metrics/mod.rs:205 wab_fsync_duration` (buckets `LATENCY_BUCKETS`, 1 ms–1 s) already times the group fsync.

## Zero-cost discipline

Every field, histogram, registration, observation, and bench test added here is `#[cfg(feature = "bench-trace")]`. A default `cargo build --release` compiles none of it. `bench-trace` pulls in **no** new dependencies (only `std::time::Instant`). Verify with `cargo build -p weir-server` (no feature) producing an unchanged `WorkUnit` size.

---

### Task 1: Add the `bench-trace` feature + thread stage timestamps through the pipeline

**Files:**
- Modify: `crates/weir-server/Cargo.toml` (features table)
- Modify: `crates/weir-server/src/models.rs` (`WorkUnit`)
- Modify: `crates/weir-server/src/worker.rs` (`Batch`, `flush_shard`, test ctor)
- Modify: `crates/weir-server/src/wab/mod.rs` (`WabRecord`)
- Modify: `crates/weir-server/src/socket/connection.rs` (`handle_push`)
- Modify: `crates/weir-server/src/main.rs` (bridge closure)

- [ ] **Step 1: Add the feature.** In `Cargo.toml` `[features]`, after `_sql-sink`:
```toml
# Per-stage latency instrumentation for the load suite. Off by default and
# carries ZERO cost in a normal build — adds Instant fields + histograms only
# when enabled. Build the binary with this to populate weir_stage_* metrics.
bench-trace = []
```

- [ ] **Step 2: Gated field on `WorkUnit`** (`models.rs`). Add, after `ack_tx`:
```rust
    /// Wall-clock instant the unit was enqueued to the work queue. Present only
    /// under `bench-trace`; used to attribute per-stage latency in the load suite.
    #[cfg(feature = "bench-trace")]
    pub enqueued_at: std::time::Instant,
```

- [ ] **Step 3: Gated field on `Batch`** (`worker.rs`, the `pub struct Batch`). Add:
```rust
    #[cfg(feature = "bench-trace")]
    pub flushed_at: std::time::Instant,
```
Set it in `flush_shard` when constructing the `Batch` (the `std::mem::replace` site):
```rust
        self.shard_txs[shard]
            .send(Batch {
                shard_id: shard as u32,
                records,
                #[cfg(feature = "bench-trace")]
                flushed_at: std::time::Instant::now(),
            })
            .ok();
```
Update the worker test helper `make_unit` (worker.rs:362) and any `WorkUnit { .. }` literal in tests to include `#[cfg(feature = "bench-trace")] enqueued_at: std::time::Instant::now()`.

- [ ] **Step 4: Gated fields on `WabRecord`** (`wab/mod.rs`). Add two:
```rust
    #[cfg(feature = "bench-trace")]
    pub enqueued_at: std::time::Instant,
    #[cfg(feature = "bench-trace")]
    pub worker_flushed_at: std::time::Instant,
```

- [ ] **Step 5: Set `enqueued_at` at enqueue** (`connection.rs:281`):
```rust
    let unit = WorkUnit {
        shard_id,
        payload,
        durability,
        ack_tx,
        #[cfg(feature = "bench-trace")]
        enqueued_at: std::time::Instant::now(),
    };
```

- [ ] **Step 6: Carry timestamps across the bridge** (`main.rs:211-220`). Inside the `for unit in batch.records` loop, populate the new `WabRecord` fields from `unit.enqueued_at` and `batch.flushed_at`:
```rust
                while let Ok(batch) = batch_rx.recv() {
                    #[cfg(feature = "bench-trace")]
                    let flushed_at = batch.flushed_at;
                    for unit in batch.records {
                        let record = WabRecord {
                            payload: unit.payload,
                            durability: unit.durability,
                            ack_tx: unit.ack_tx,
                            #[cfg(feature = "bench-trace")]
                            enqueued_at: unit.enqueued_at,
                            #[cfg(feature = "bench-trace")]
                            worker_flushed_at: flushed_at,
                        };
```

- [ ] **Step 7: Build both ways.**
  - `cargo build -p weir-server` — Expected: PASS, no warnings.
  - `cargo build -p weir-server --features bench-trace` — Expected: PASS.
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1` — Expected: PASS (worker tests still compile/run with the no-feature build).

- [ ] **Step 8: Commit** — `feat(bench-trace): thread per-stage timestamps through the pipeline [skip ci]`

---

### Task 2: Per-stage histograms + observe them in the flusher

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs`
- Modify: `crates/weir-server/src/wab/mod.rs` (flusher: capture `flusher_recv_at`, observe stage deltas)

- [ ] **Step 1: Fine-grained buckets + gated histogram fields** (`metrics/mod.rs`). Add a sub-millisecond bucket set near `LATENCY_BUCKETS`:
```rust
/// Finer buckets for the per-stage breakdown — queue/write stages are tens of
/// microseconds, far below LATENCY_BUCKETS' 1 ms floor. Only used under bench-trace.
#[cfg(feature = "bench-trace")]
const STAGE_BUCKETS: &[f64] = &[
    0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500,
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
];
```
Add gated fields to `Metrics` (each a `Histogram`):
```rust
    /// queue + coalesce wait: enqueue → worker flush. bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_queue: Histogram,
    /// bridge hop + flusher recv wait: worker flush → flusher dequeue. bench-trace only.
    /// This is where the Batched double-deadline anomaly will show up.
    #[cfg(feature = "bench-trace")]
    pub stage_bridge_wait: Histogram,
    /// record write: flusher dequeue → write_record done (pre-fsync). bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_write: Histogram,
    /// end-to-end server-side: enqueue → ack fired. bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_total: Histogram,
```
Register them in `Metrics::new` (gated `reg!` calls) with names `weir_stage_queue_seconds`, `weir_stage_bridge_wait_seconds`, `weir_stage_write_seconds`, `weir_stage_total_seconds`, and add them to the struct literal (gated). Match the existing `reg!` macro pattern.

- [ ] **Step 2: Observe in the flusher** (`wab/mod.rs`). Read the flusher thread + `flush_batch`. Then:
  - When a `WabRecord` is dequeued from the channel, capture `#[cfg(feature = "bench-trace")] let flusher_recv_at = Instant::now();` and observe `stage_bridge_wait = flusher_recv_at - record.worker_flushed_at` and `stage_queue = record.worker_flushed_at - record.enqueued_at`.
  - After `write_record` returns for that record (pre-fsync), observe `stage_write = Instant::now() - flusher_recv_at`.
  - Just before / at `ack_tx.send(..)` for the record, observe `stage_total = Instant::now() - record.enqueued_at`.
  - Use `.observe(d.as_secs_f64())`. All four observe calls are `#[cfg(feature = "bench-trace")]`. The flusher already has the `Arc<Metrics>` handle (it records `wab_fsync_duration`); reuse it.
  - Keep the existing per-batch group-fsync behaviour and `wab_fsync_duration` observation untouched.

- [ ] **Step 3: Build + test.**
  - `cargo build -p weir-server --features bench-trace` — PASS.
  - `cargo clippy -p weir-server --features bench-trace --all-targets -- -D warnings` — PASS.
  - `cargo build -p weir-server` and `cargo clippy -p weir-server --all-targets -- -D warnings` — PASS (no-feature build clean, no dead-code warnings).
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1` — PASS.

- [ ] **Step 4: Commit** — `feat(bench-trace): per-stage latency histograms observed in the flusher [skip ci]`

---

### Task 3: Tighten latency stats — wider samples + per-run σ

**Files:**
- Modify: `crates/weir-server/tests/load.rs` (`emit_latency`, three `SAMPLES` constants)
- Modify: `deploy/avg_benchmarks.py` (render σ in latency tables)

- [ ] **Step 1: Widen samples.** In `baseline_latency_percentiles_{sync,batched,buffered}` change `const SAMPLES: usize = 500;` → `2000`.

- [ ] **Step 2: Add σ to `emit_latency`.** Compute population stddev over `sorted_us` and add `"stddev_us":<n>` to the emitted JSON (integer µs). Keep all existing fields.

- [ ] **Step 3: Render σ in `avg_benchmarks.py`.** In the latency tables (both the multi-deadline and single-deadline branches), add a row or column for σ. Simplest: add a `("stddev_us", "σ")` entry to `LATENCY_FIELDS` so it renders as another metric row averaged across runs. Verify the script still runs: `python3 deploy/avg_benchmarks.py /dev/stdin /tmp/out.md` fed a sample line.

- [ ] **Step 4: Run the three latency tests** to confirm the new field emits:
  - `cargo test -p weir-server --test load --release baseline_latency -- --nocapture` — Expected: three `BENCH:` lines each containing `"stddev_us"`.

- [ ] **Step 5: Commit** — `test(bench): widen latency samples to 2000 + emit per-run σ [skip ci]`

---

### Task 4: Sync-tier saturation ramp

**Files:**
- Modify: `crates/weir-server/tests/load.rs` (parameterise `run_ramp_level` by `Durability`; add `ramp_to_saturation_sync`)
- Modify: `deploy/avg_benchmarks.py` (handle `ramp_sync_` scenarios)

- [ ] **Step 1: Parameterise `run_ramp_level`.** Add a `durability: Durability` parameter; use it in the `client.push(b"ramp", durability)` call (load.rs:170). Update the existing `ramp_to_saturation` caller to pass `Durability::Buffered`.

- [ ] **Step 2: Add `ramp_to_saturation_sync`.** Clone `ramp_to_saturation`, pass `Durability::Sync`, and emit scenario names `ramp_sync_{n}_threads_d{d}ms` (note the `sync` segment). Keep the same `MAX_CONN=48` and `LEVELS`. Add the same post-level health-check assertion.

- [ ] **Step 3: Teach `avg_benchmarks.py` about the Sync ramp.** The ramp grouping splits on `_`; `ramp_sync_8_threads_d1ms` must parse its thread count from the token after `sync`. Add a `RAMP_SYNC_PREFIX = "ramp_sync_"` and render a second "Saturation Ramp — Sync tier" table (mirror the existing ramp section, keyed off the new prefix; ensure the Buffered ramp parser still only matches `ramp_<digit>` not `ramp_sync_`).

- [ ] **Step 4: Run both ramps** to confirm they pass and emit:
  - `cargo test -p weir-server --test load --release ramp_to_saturation -- --nocapture` — Expected: both ramp tests pass; Sync ramp emits `ramp_sync_*` lines.

- [ ] **Step 5: Commit** — `test(bench): add Sync-tier saturation ramp [skip ci]`

---

### Task 5: Stage-breakdown bench test (gated) + averager section

**Files:**
- Modify: `crates/weir-server/tests/load.rs` (gated `latency_stage_breakdown_*`)
- Modify: `deploy/avg_benchmarks.py` (parse + render `BENCH_STAGE:` lines)

- [ ] **Step 1: Gated stage-breakdown test.** Add, behind `#[cfg(feature = "bench-trace")]`, a test `latency_stage_breakdown` that, for each tier (Sync, Batched, Buffered): starts a `bench_preset` server, pushes ~2000 records of that tier, then `srv.scrape_metrics()` and parses the four `weir_stage_*_seconds_sum` and `_count` values (reuse the `parse_load_metric`-style helper, matching the histogram `_sum`/`_count` lines). Emit one line per tier:
```
BENCH_STAGE: {"scenario":"stage_<tier>_d<N>ms","queue_us":<sum/count*1e6>,"bridge_wait_us":...,"write_us":...,"total_us":...}
```
(Mean µs per stage = `sum/count*1e6`.) This test only compiles/runs under `--features bench-trace`, so it does not affect the normal `load` CI job.

- [ ] **Step 2: Averager section.** In `avg_benchmarks.py`, parse `BENCH_STAGE: {json}` lines into their own group and render a "Per-stage latency breakdown (bench-trace)" table: rows = tiers, columns = queue / bridge_wait / write / total (µs). Only render the section if such lines are present.

- [ ] **Step 3: Run it under the feature** to confirm:
  - `cargo test -p weir-server --test load --release --features bench-trace latency_stage_breakdown -- --nocapture` — Expected: three `BENCH_STAGE:` lines with plausible µs (queue/write tens of µs, fsync-bearing tiers' total dominated by fsync).

- [ ] **Step 4: Commit** — `test(bench-trace): per-stage latency breakdown scenario + averager section [skip ci]`

---

### Task 6: Full verification across feature combos

- [ ] **Step 1: fmt** — `cargo fmt --all -- --check` — PASS.
- [ ] **Step 2: Default build/clippy/test:**
  - `cargo clippy --all-targets -- -D warnings` — PASS.
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1` — PASS.
- [ ] **Step 3: bench-trace build/clippy:**
  - `cargo clippy -p weir-server --features bench-trace --all-targets -- -D warnings` — PASS.
- [ ] **Step 4: all-features (ensure bench-trace composes with the sinks/tls):**
  - `cargo build -p weir-server --all-features` — PASS.
- [ ] **Step 5: Commit** any fmt/lint fixups — `chore(bench-trace): fmt + clippy across feature combos [skip ci]`

---

### Task 7: Capture the Phase-3 baseline

**Files:**
- Modify/create: `docs/benchmarks/latest.md` (regenerated), `docs/benchmarks/phase3-stage-baseline.md` (new)

- [ ] **Step 1: Run the standard load suite, both deadlines, a few passes**, appending to one JSONL (mirror the CI `load` job). Example:
```sh
: > /tmp/load_results.jsonl
for d in 1 2; do for pass in 1 2 3; do
  WEIR_BENCH_DEADLINE=$d cargo test -p weir-server --test load --release -- --nocapture \
    | grep '^BENCH: ' >> /tmp/load_results.jsonl
done; done
python3 deploy/avg_benchmarks.py /tmp/load_results.jsonl docs/benchmarks/latest.md
```
(Use `--test-threads=1` if the local `$TMPDIR` parallelism flake appears.)

- [ ] **Step 2: Capture the stage breakdown** under bench-trace into its own file:
```sh
: > /tmp/stage_results.jsonl
for d in 1 2; do
  WEIR_BENCH_DEADLINE=$d cargo test -p weir-server --test load --release --features bench-trace \
    latency_stage_breakdown -- --nocapture | grep '^BENCH_STAGE: ' >> /tmp/stage_results.jsonl
done
python3 deploy/avg_benchmarks.py /tmp/stage_results.jsonl docs/benchmarks/phase3-stage-baseline.md
```

- [ ] **Step 3: Eyeball the numbers.** Confirm the stage breakdown is sane (queue+write are tens of µs; Batched `bridge_wait` ≥ Sync `bridge_wait` — the suspected double-deadline). Note the headline figures in the commit message so Streams B–F have a reference.

- [ ] **Step 4: Commit** — `docs(benchmarks): Phase 3 baseline + per-stage breakdown [skip ci]`

---

## Done criteria

- `bench-trace` off → zero footprint (build identical, no new metrics, all tests green).
- `bench-trace` on → `weir_stage_{queue,bridge_wait,write,total}_seconds` exposed; the gated breakdown test emits `BENCH_STAGE:` lines.
- Latency scenarios run 2000 samples and emit σ; a Sync-tier ramp exists.
- A committed baseline (standard + per-stage) gives Streams B–F a measuring stick. In particular, the per-stage numbers either confirm or revise: the bridge-hop cost (Stream B), the Batched double-deadline (Stream B), and whether the Prometheus counter path is material (Stream F).
