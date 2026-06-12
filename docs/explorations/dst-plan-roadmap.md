# DST Incremental Delivery Roadmap

**Status:** Planning — no implementation.
**Depends on:** `phase4-dst-harness.md` (trait design), `phase4-dst-scenarios.md` (fault catalog).
**Goal:** Land deterministic simulation testing value in measured, shippable increments — no big-bang refactor, existing tests green at every step.

---

## Why incremental matters here

The harness doc proposes a sound end-state (injectable `Clock` + `WabBackend` + `SimExecutor` + turmoil). The risk is treating it as a single ~6-week project that yields zero test value until everything is wired together. Weir's architecture has a natural decomposition that makes a staircase approach safe:

- The WAB flusher is the deepest, most self-contained seam — it can be simulated in isolation without touching the worker, drain, or socket layers.
- The drain's state machine is already partially tested; the gaps are clock-injection and filesystem faults, not structural rewrites.
- The worker and async socket layers benefit from simulation but are not the source of the highest-severity unfixed bugs in the current gap list.

Each phase below ships a real test that can fail, be seeded, and be replayed. The production binary is never broken at a merge boundary.

---

## Severity rankings from the gap list (reference)

The scenarios doc ranks the following as Critical-severity gaps with no current test:

| Gap | Scenario | Why it is the highest yield |
|-----|----------|-----------------------------|
| G-WAB-1 | `EIO` on `fdatasync` | The fsync failure path (`fsync_observed()` → Nack) has *zero* integration test coverage despite being the primary durability guarantee. Any regression here is invisible. |
| G-WAB-3 | Crash between `sync_all` and `rename` in `seal()` | Mid-seal crash leaves a `.wab` with valid footer bytes; recovery must re-seal it. No targeted test exists; the crash-restart system test does not exercise seal timing. |
| G-QUEUE-1 | WAB flusher channel full / worker silent discard | `.ok()` on `shard_txs[shard].send(Batch)` in `Worker::flush_shard()` silently discards the batch when the flusher channel is full. The ack channels in that batch are dropped, and the path through `handle_connection` on `ack_rx` channel-close is unverified. This is a potential correctness hole (I-8 violation). |
| C-3 + D-3 | mid-seal crash / EIO | Both require a controllable injection point on `WabSegment::seal()` and `platform_fsync()` — the same seam unlocks both. |

These four guide the front-loading decision: the minimum seam to build first is `WabBackend` (covers G-WAB-1 and G-WAB-3) plus a `BlockingClock` (covers the panic supervisor sleep and batch deadline timer). G-QUEUE-1 can be exercised with just `BlockingClock` + a slow-flusher injection, no separate seam needed.

---

## Phase overview

```
Phase 1 (2–3 weeks)   Flusher sim seam
  │  WabBackend + BlockingClock traits
  │  SimFs + SimClock implementations
  │  3 seed-reproducible scenarios: EIO-fsync, mid-seal crash, panic-supervisor
  │  All existing tests stay green
  ▼
Phase 2 (2 weeks)     Flusher fault coverage expansion
  │  D-5 torn write, D-8/D-9 ENOSPC-at-seal, G-QUEUE-1 silent-discard analysis
  │  Remaining Batch A scenarios from scenarios doc
  ▼
Phase 3 (3–4 weeks)   Worker + pipeline integration
  │  SimExecutor, ThreadSpawner injection
  │  Full queue → worker → flusher single-threaded pipeline sim
  │  Coalesce timing, T-4 ACK_TIMEOUT on slow fsync
  ▼
Phase 4 (2 weeks)     Drain simulation
  │  SimSink, SimDeadLetterFs
  │  Batch B scenarios: hung sink, mid-segment crash, recovery-of-recovery
  ▼
Phase 5 (2–3 weeks)   Async socket layer via turmoil
  │  turmoil dev-dependency
  │  End-to-end DST: producer → socket → pipeline → WAB → drain
  │  Network fault scenarios (R-2, R-4, M-7, M-8)
  ▼
Phase 6 (ongoing)     Full 57-scenario catalog coverage
         Remaining scenarios wired in; seed-regression suite in CI
```

---

## Phase 1 — Minimum Viable DST (2–3 weeks)

### What it ships

Three deterministic, seed-reproducible scenarios for the WAB flusher running on a single OS thread. The production binary is unchanged except for two new trait bounds on `flusher_thread` and `ShardWriter`. All existing unit and system tests pass without modification.

### Scenarios to build first

**Scenario 1: G-WAB-1 — EIO on `fdatasync` (Critical)**

This is the highest-yield scenario in the entire catalog. The current `efbig_returns_nack_not_crash` system test fires `EFBIG` on `write_vectored`, not `EIO` on `fsync`. The path through `fsync_observed()` returning `false` → ack sends `false` → `wab_fsync_failures` increments → producer observes Nack has no test at all.

What makes it ideal as a first DST scenario:
- Injection point is `WabSegment::fsync()` → `platform_fsync(&self.file)`. One seam, one injected error.
- The postcondition is simple: ack booleans all come out `false`; no panic; the WAB file bytes written before the fsync error are intact (records are written, just not durably confirmed).
- The seed is trivial: `fsync_fail_on_call_n: usize`.

**Scenario 2: G-WAB-3 — Crash between `sync_all` and `rename` in `WabSegment::seal()` (High)**

`seal()` in `segment.rs` lines 207–224 has a three-step commit: write sentinel + footer → `platform_fsync` → `std::fs::rename`. A crash (or injected error) after the fsync but before the rename leaves a `.wab` with valid footer bytes that `recover_shard_dir()` must re-seal. No existing test targets this window.

DST advantage: Without simulation, testing this requires either OS-level fault injection (fragile, non-deterministic timing) or a targeted unit test that bypasses `seal()` to write the file bytes manually. The `WabBackend` seam lets us inject a fault at precisely the rename step and then run `recover_open_segments()` on the resulting in-memory filesystem, verifying recovery output against expected record content.

**Scenario 3: Panic supervisor timing — respawn sleep under `SimClock` (High)**

The current P-1/P-2/P-3 tests for the panic supervisor exist but use `thread::sleep(10 * attempt ms)`. Under real wall clock, the P-3 cap-out test sleeps up to 550ms. Under `SimClock`, the same test runs in microseconds because `sleep_blocking(d)` advances the simulated tick counter rather than blocking.

This is a pure clock-injection win: no filesystem changes needed, and it validates that the supervisor correctly increments backoff ticks. It also establishes the `BlockingClock` trait before Phase 2 uses it for batch deadline timing.

### The trait seam: minimum interface

Rather than immediately building the full `WabFs` interface from the harness doc, Phase 1 introduces the narrowest seam that unlocks the three scenarios above:

```rust
/// Abstracts WAB filesystem operations for simulation and fault injection.
/// Production code implements this via the real File-based path.
/// `#[cfg(any(test, feature = "dst"))]` gates the simulation implementation.
pub(crate) trait WabBackend: Send {
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()>;
    fn fsync(&self) -> io::Result<()>;
    fn seal(self: Box<Self>) -> io::Result<PathBuf>;
    fn bytes_written(&self) -> u64;
    fn is_poisoned(&self) -> bool;
}

/// Abstracts clock and blocking-wait operations for deterministic simulation.
pub(crate) trait BlockingClock: Send + Sync {
    fn sleep_blocking(&self, duration: Duration);
}
```

`WabSegment` becomes the `RealWabBackend` implementation. `SimWabBackend` wraps an in-memory `Vec<u8>` with a scriptable `FaultSchedule` (a small enum: `NoFault`, `FailFsync`, `FailRename`). `SimClock` wraps an `AtomicU64` tick counter; `sleep_blocking(d)` advances it by `d.as_nanos()`.

The `create_segment` factory is not yet abstracted in Phase 1 — `ShardWriter` still calls `WabSegment::create(path, shard_id)` directly but wraps the result in `Box<dyn WabBackend>`. The `SimFs` in-memory path is reached by passing a `SimWabBackend::new()` directly into the test harness without going through `ShardWriter::ensure_open()`.

This is the narrowest possible change: `ShardWriter` gains a `Box<dyn WabBackend>` field only in test/dst builds, and `flusher_thread` gains a `clock: Arc<dyn BlockingClock>` parameter.

### Refactor strategy: keep existing tests green

The rule is: **no change to a `pub` function signature that is called from `system.rs`, `wab/mod.rs` tests, or `drain/mod.rs` tests**. The injectable seam is always behind a `#[cfg(any(test, feature = "dst"))]` guard or uses `impl Trait` in new test-only entry points.

Concretely:

1. `WabSegment` gets a new `pub(crate) fn into_backend(self) -> impl WabBackend` method gated behind `#[cfg(any(test, feature = "dst"))]`. No change to the public `create`/`write_record`/`fsync`/`seal` API.
2. `flusher_thread` remains `pub(crate)` with its current signature. A new `flusher_thread_with_clock` twin function accepts the `BlockingClock` parameter; the original calls the twin with `Arc::new(RealClock)`.
3. `run_with_panic_supervision`'s `thread::sleep` is the only direct call site in Phase 1. It is wrapped: `clock.sleep_blocking(d)` in a version that is only exercised through the new `SimClock` path.

### TDD task breakdown for Phase 1

**Step 1 (Red): Write the failing sim test first**

Create `crates/weir-server/src/wab/sim_tests.rs` (gated `#[cfg(any(test, feature = "dst"))]`):

```rust
#[test]
fn eio_on_fsync_sends_nack_and_increments_metric() {
    // Build a flusher harness with SimWabBackend that fails on fsync #1.
    // Push one Sync record.
    // Assert: ack channel receives `false`.
    // Assert: wab_fsync_failures metric == 1.
    // Assert: no panic.
}

#[test]
fn mid_seal_rename_failure_leaves_recoverable_wab() {
    // Build a SimWabBackend configured to fail at the rename step of seal().
    // Flush one batch; trigger rotation.
    // Assert: the SimFs contains a .wab file with valid header + record bytes + sentinel + footer.
    // Run recover_segment() on the SimFs state.
    // Assert: the recovered sealed segment contains exactly the written records.
}

#[test]
fn panic_supervisor_sleep_uses_sim_clock() {
    // Build flusher with a body_factory that panics on attempts 1..3, succeeds on attempt 4.
    // Use SimClock that tracks total sleep duration.
    // After run_with_panic_supervision returns, assert:
    //   - SimClock.total_sleep() == (10+20+30) ms in simulated ticks.
    //   - Actual wall-clock elapsed < 10ms (no real sleeps).
    //   - wab_flusher_panics metric == 3.
}
```

These tests compile but do not pass because `WabBackend`, `SimWabBackend`, and `SimClock` do not yet exist (or `flusher_thread_with_clock` doesn't accept them). That is the red state.

**Step 2 (Green part A): Introduce the traits and sim types**

- `crates/weir-server/src/wab/backend.rs`: `WabBackend` trait + `RealWabBackend` wrapper around `WabSegment`. `RealWabBackend` is zero-overhead: it delegates to the existing `WabSegment` methods.
- `crates/weir-server/src/wab/sim.rs` (gated): `SimWabBackend`, `SimClock`, `FaultSchedule`, `SimFs` (a simple `HashMap<PathBuf, Vec<u8>>`).
- Run `cargo test --features dst`. The existing tests still pass because `RealWabBackend` is a transparent wrapper; the sim types are new code. The new sim tests still fail because `flusher_thread_with_clock` is not wired.

**Step 3 (Green part B): Wire the trait into `flusher_thread`**

- Add `flusher_thread_sim` (test/dst-only) that takes `Box<dyn WabBackend>` and `Arc<dyn BlockingClock>` instead of taking the full channel + shard dir setup.
- The three sim tests now drive `flusher_thread_sim` directly. Run `cargo test --features dst`. All three new tests pass. All existing tests still pass.

**Step 4 (Verify): Seed reproducibility**

Add a test that runs `eio_on_fsync_sends_nack_and_increments_metric` with two different seeds and asserts that with the same seed (including the fsync-fail injection schedule), the ack outcomes are identical; with a different schedule, they diverge as expected. This validates the seed-reproducibility property.

**Step 5 (CI gate): Add `cargo test --features dst` to the existing CI matrix**

The features gate ensures simulation code is compiled and tested in CI without being included in release builds. The Windows leg needs no special handling — `SimClock` and `SimFs` are pure Rust with no `libc` dependency.

### Risk assessment

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| `WabSegment` internal coupling makes `WabBackend` trait awkward | Medium | `WabSegment` is a single file (250 lines). The `RealWabBackend` wrapper is 30–40 lines and changes no existing semantics. |
| `flusher_thread` signature change breaks callers | Low | The original function is preserved; `flusher_thread_sim` is a new entry point only reachable from test/dst builds. |
| The `SimFs` rename injection does not accurately represent real OS behavior | Low | It doesn't need to. The goal is correctness of the *recovery logic*, not filesystem semantics. The sim's `rename` can return any `io::Error`; `recover_open_segments` must handle it. |
| Phase 1 sim doesn't cover real thread concurrency | Accepted | Phase 1 explicitly scopes to flusher-only, single-threaded. Concurrency is Phase 3's concern. |

### What Phase 1 unlocks

- Every fsync-fault scenario in the catalog (D-3, D-4 via delayed SimClock, parts of C-1, C-2) becomes injectable without OS tricks.
- The panic supervisor's real-time test cost drops from ~550ms to microseconds.
- The `WabBackend` and `BlockingClock` traits become the permanent foundation for all subsequent phases — they never need to be rewritten.
- A template (red → green → seed-verify) for all future DST scenario work.

---

## Phase 2 — Flusher fault coverage expansion (2 weeks)

**Builds on:** Phase 1 traits already wired into `flusher_thread_sim`.

### Scenarios added

- **D-5 torn write** (Critical): Inject a short `write_vectored` return (fewer bytes than requested) inside `SimWabBackend::write_record`. Assert: `WabSegment.poisoned` is set; flusher opens a fresh segment; recovery of the torn file truncates at the last valid CRC boundary.
- **D-8/D-9 ENOSPC at seal** (High): Inject an `ENOSPC` error from `SimWabBackend::seal()` at either the sentinel-write step or the rename step. Assert seal failure is handled without a panic; the segment either remains as `.wab` (for recovery) or the flusher logs and continues.
- **G-QUEUE-1 channel-full silent discard analysis** (Critical gap): This scenario does not require a new seam. Write a test that fills the `shard_txs[shard]` bounded channel to capacity (using a `SimWabBackend` configured to never complete an fsync, so the flusher never drains) and then forces `Worker::flush_shard()` to call `send(...).ok()`. Trace the dropped `ack_tx` senders through `handle_connection`'s `ack_rx.await` path. Document whether this is a safe Nack or a hang. If it is a hang or a panic, raise as a correctness issue and add the fix to Phase 2 scope.
- **T-2/T-4 EWMA and ACK_TIMEOUT timing** (Medium): Use `SimClock` to advance time precisely. Assert `ewma_update_us` with a zero-latency sample does not underflow. Assert the batch deadline timer fires at the correct tick without real wall-clock waits.

### What Phase 2 unlocks

Batch A of the scenarios doc's prioritized 10 is fully covered. CI now catches any regression in the write, fsync, seal, or poison paths without requiring OS-level fault injection or `#[ignore]` system tests.

---

## Phase 3 — Worker + pipeline integration (3–4 weeks)

**Builds on:** Phase 1+2 traits; adds `ThreadSpawner` and `SimExecutor`.

### New seam

```rust
pub(crate) trait ThreadSpawner: Send + Sync {
    fn spawn_named<F: FnOnce() + Send + 'static>(&self, name: &str, f: F);
}
```

`RealThreadSpawner` wraps `thread::Builder::new().spawn(...)`. `SimThreadSpawner` queues the closure as a task in `SimExecutor`. The `SimExecutor` is a cooperative scheduler driven by a seeded `SmallRng` (from the `rand` crate) for task ordering decisions.

### Scenarios added

- **Q-3 queue saturated + hung flusher** (Critical): Run the full worker → flusher pipeline under simulation. Inject a slow fsync (SimClock delay). Fill the queue partition while the flusher is blocked. Assert: `QUEUE_PUSH_TIMEOUT` fires before the fsync completes; producer observes Nack; no deadlock.
- **T-4 ACK_TIMEOUT fires before flusher completes** (High): Inject a 40-second SimClock delay in `SimWabBackend::fsync`. Assert: the ack timeout fires; the producer gets Nack(InternalError); the flusher eventually completes; no panic; the WAB file is consistent.
- **R-1 concurrent producer FIFO under sim** (Critical): Run two simulated producers on two `SimExecutor` tasks sending to the same shard. Assert per-producer record order is preserved in the flushed segment.
- **Coalesce window determinism**: With a fixed seed, the `expect_concurrent` prediction in `Worker::run` and the `coalesce_hint` EWMA read produce the same window on every replay. Assert the ack latency histogram (from `SimClock` elapsed) is identical across two runs with the same seed.

### What Phase 3 unlocks

The worker's batching and coalesce logic is now deterministically testable. The `SimExecutor` is the first infrastructure that enables full scheduling-order exhaustion for small scenarios. The seed format (`Sim::new(seed)`) crystallizes here.

---

## Phase 4 — Drain simulation (2 weeks)

**Builds on:** Phase 3 `SimExecutor`; adds `SimSink` and `SimDeadLetterFs`.

### New seam

```rust
pub(crate) trait SimSink: Send {
    fn commit(&mut self, records: Vec<SinkRecord>) -> Result<CommitResult, SinkError>;
}
```

(The existing `MockSink` in drain tests is already close to this; Phase 4 formalizes and seeds it.)

### Scenarios added

- **B-1 hung sink** (High): `HungSink::commit` blocks the drain thread. Assert segments accumulate in the drain channel; daemon does not crash; after unblocking, drain catches up. Under simulation, "blocks" means `SimClock::sleep_blocking` called with a very large duration.
- **B-2 mid-segment crash** (Critical): Crash after the first `commit_batch` but before the second within the same segment. Assert at-least-once on replay; duplicate records appear in the `DeduplicatingSink`'s received set.
- **B-3 recovery-of-recovery (C-6)** (High): Two successive `recover_open_segments` calls with a simulated crash between them. Assert the second recovery does not lose records that were valid before the first crash point.
- **B-4 at-least-once duplicate delivery (S-10)** (Critical contract): Full pipeline sim: push N records; crash at drain C-8 point; restart; replay. Assert `DeduplicatingSink` receives each record at least once and no record is silently dropped.
- **B-5 shutdown timeout + unsealed segment** (High): `shutdown_timeout_secs = 1`; inject a 5-second SimClock seal delay. Assert daemon exits within the budget; `.wab` survives on the sim filesystem; recovery seals and replays it.

### What Phase 4 unlocks

Batch B of the prioritized 10 is fully covered. The drain state machine has clock-exact retry and dead-letter tests that don't rely on real wall-clock `base_retry_delay: 1ms` hacks. The at-least-once contract has an explicit seed-reproducible regression test.

---

## Phase 5 — Async socket layer via turmoil (2–3 weeks)

**Builds on:** Phase 3+4 full pipeline sim; adds `turmoil` as a dev-dependency.

### What turmoil adds

The accept loop (`socket/mod.rs`), connection handler (`socket/connection.rs`), and the `spawn_blocking` boundary for `push_timeout` all run in the tokio async layer. turmoil intercepts `tokio::net` and provides deterministic message delivery and latency injection. This is the only layer where turmoil is needed — all blocking-thread behavior is already covered by Phases 1–4.

### Scenarios added

- **R-2 partial frame** (High): Under turmoil, simulate a TCP connection that sends a valid header then drops. Assert server reads partial payload, gets EOF, closes connection; next connection works normally.
- **G-1 SIGTERM during active Sync push** (Critical): Inject shutdown signal while a simulated fsync is in-flight under `SimClock`. Assert the in-flight ack fires after the fsync completes; the connection sees `Ok` or `Io(EOF)`; never silent Nack.
- **R-4 stalled producer holds semaphore permit** (High): Under turmoil, simulate a client that sends a frame but never reads the ack. Assert `read_timeout` eventually fires; the semaphore permit is returned; other connections work.
- **G-2/G-3 SIGTERM during drain processing** (High/Medium): Wire the Phase 4 drain sim into the turmoil host. Assert unconfirmed segments are replayed on restart.

### What Phase 5 unlocks

End-to-end DST: a producer-to-sink scenario with a single reproducible seed that covers the entire weir pipeline. Network-level fault injection. The `--dst-seed=0x...` replay mechanism described in the harness doc is fully operational.

---

## Phase 6 — Full 57-scenario catalog (ongoing)

**Scope:** Wire all remaining scenarios from `phase4-dst-scenarios.md` into the Phase 5 harness. Prioritize by severity, then by likelihood of silent regression.

### Remaining high-priority gaps

- **G-WAB-4** (C-6 recovery-of-recovery): Covered in Phase 4 B-3.
- **G-WAB-5** (D-8/D-9 ENOSPC at seal): Covered in Phase 2.
- **G-DRAIN-2** (record-level at-least-once): Covered in Phase 4 B-2.
- **G-FLUSHER-1** (P-5 worker panic unsupervised): Requires a Phase 3 decision on whether to add `run_with_panic_supervision` to `Worker::run`. Add the supervision wrapper and a Phase 6 test that verifies the metric increments and the queue partition transitions to Nack mode.
- **G-RECOVERY-1** (corrupt `.confirmed` with valid CRC32): Lower priority (security model boundary, not a correctness gap). Add a comment in `format.rs` and defer to Phase 6.
- **G-MULTI-1** (multi-shard partial recovery): Wire in Phase 5 or Phase 6 once the multi-shard sim infrastructure exists.
- **G-SHUTDOWN-1** (shutdown timeout): Covered in Phase 4 B-5.

### CI seed-regression suite

Each scenario that finds a real bug ships with a `#[test] fn regression_seed_XXXXXXXX()` that runs the exact failing seed. These are permanent CI fixtures — they run on every push to main and on every PR. Failed seeds cannot silently regress.

---

## Incremental delivery contract: what must stay true at every phase boundary

1. `cargo test` (all unit + system tests) passes without `--features dst`.
2. `cargo test --features dst` passes (adds sim scenarios).
3. The production binary has zero sim code in its binary (verified by symbol-strip + nm/objdump check in CI).
4. No `pub` function signature used by `system.rs` or `lib.rs` is changed without a corresponding bump to the calling site.
5. Every new DST test prints a seed on failure: `DST FAILURE: seed=0x... scenario=...`.

---

## Effort summary

| Phase | Duration | Key deliverable | Risk |
|-------|----------|-----------------|------|
| 1 | 2–3 weeks | `WabBackend` + `BlockingClock` traits; EIO-fsync + mid-seal-crash + panic-supervisor scenarios; seed-reproducible | Low — smallest possible seam; no public API changes |
| 2 | 2 weeks | Torn-write, ENOSPC-at-seal, QUEUE-1 analysis; Batch A complete | Low — builds on Phase 1 seam without new infrastructure |
| 3 | 3–4 weeks | `SimExecutor` + `ThreadSpawner`; full worker pipeline sim; coalesce + ACK_TIMEOUT scenarios | Medium — first multi-task cooperative scheduler |
| 4 | 2 weeks | `SimSink` + `SimDeadLetterFs`; Batch B complete; at-least-once regression tests | Low — drain seam is simpler than flusher; `MockSink` already exists |
| 5 | 2–3 weeks | turmoil integration; end-to-end network-level scenarios; `--dst-seed` CLI flag | Medium — turmoil adds new dependency; `spawn_blocking` boundary needs care |
| 6 | ongoing | Full 57-scenario catalog; CI seed-regression suite | Low per scenario — incremental additions to stable harness |

Total to a working end-to-end DST harness (Phases 1–5): approximately 11–14 weeks of focused engineering, with working tested value after each phase.

---

## Decision record: choices made in this plan

**Why `Box<dyn WabBackend>` over `ShardWriter<F: WabFs>`**

The harness doc (§4.1) identifies two options for making the file backend injectable. This plan recommends the `Box<dyn WabBackend>` option (dynamic dispatch over a single vtable) because: (a) the performance cost is negligible — `fdatasync` dominates, not a vtable lookup; (b) the generic approach propagates through `ShardWriter`, `flusher_thread`, `WabHandle`, and `spawn()`, adding visible complexity to production code that has nothing to do with production behavior. The `Box` approach confines the abstraction to the types that actually need it.

**Why start with three specific scenarios, not all of Batch A**

Batch A has five scenarios (A-1 through A-5). A-4 (G-QUEUE-1) and A-5 (ACK_TIMEOUT on slow fsync) both require `BlockingClock` but also benefit from the `SimExecutor` for full fidelity — they are deferred to Phase 2/3. Starting with the two pure-filesystem-injection scenarios (A-1 EIO, A-2 mid-seal crash) plus the clock-only scenario (panic supervisor) keeps Phase 1 tightly scoped and ensures the traits are validated against the simplest test cases before adding timing complexity.

**Why not madsim**

The harness doc (§3 Approach A) correctly identifies that madsim covers only the tokio async layer and cannot reach the `std::thread` flusher/worker/drain threads where the highest-severity bugs live. Turmoil (Phase 5) is the right async-layer tool because it focuses on network simulation without claiming to determinize all tokio task scheduling. The blocking threads are covered by the injectable traits in Phases 1–4.

**Feature flag: `--features dst`**

The harness doc (§6.5) recommends a `SimMode` feature flag. This plan uses `--features dst` specifically, matching the proposed CLI flag in the harness doc's §6.7 seed-replay design. The feature gates all sim types and sim entry points. Production builds produced by `cargo build --release` never include sim code.
