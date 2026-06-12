# DST (Deterministic Simulation Testing) for the WAB — Synthesized Plan

**Date:** 2026-06-12 · **For:** Phase 4 thread #3 · **Status:** ready to execute

Synthesis of four parallel planning routes (read for full detail):
`dst-plan-trait-seams.md` · `dst-plan-alternatives.md` · `dst-plan-roadmap.md` · `dst-plan-ergonomics.md`
(built on `phase4-dst-harness.md` + `phase4-dst-scenarios.md`).

## Goal

A **seed-reproducible** test harness for weir's durability-critical WAB flusher: inject faults (fsync `EIO`, crash points, timing) deterministically, assert the durability invariants, and replay any failure exactly from its seed. Front-loaded on the highest-severity, currently-*zero-coverage* scenarios. The production binary is untouched at every merge.

## The four routes converged

Independently, all four landed on the same shape: **injectable trait seams** (a clock + a segment store) + a **builder test API** + **phased delivery** starting with the flusher. The alternatives survey added the one new variable — `turmoil-fs` — and the build-vs-adopt call below.

## Build vs adopt — the decision

| Component | Decision | Why |
|-----------|----------|-----|
| `BlockingClock` + `SegmentStore` traits, `SimClock`, `SimSegmentStore`, `SimExecutor` | **Hand-roll** (~500–650 LOC) | Full control, no external maturity risk on a *durability-critical* test foundation; the trait seams are clean infrastructure regardless |
| Seed shrinking / minimization | **Adopt `proptest`** (already in the workspace) | Mature, free, exactly the right tool for fault-list/trace minimization |
| **`turmoil-fs`** (replace `SimSegmentStore`) | **Note as a later spike — do NOT bet the foundation on it now** | Saves ~1 week, but it's an `unstable` feature *and* has an unvalidated `install_host_accessor` integration for a flusher running as a bare `std::thread` outside tokio. The survey itself flags this as the thing that could force a fallback. Optionally spike it (~1 day) after Phase 1; adopt only if the bare-thread integration proves clean |
| `shuttle` (thread-interleaving permutation) | **Defer** | Can't wrap `crossbeam_channel` yet (open issue), and its `recv_timeout` ignores the timeout — the batch-deadline flush path would never fire. Revisit when the crossbeam wrapper lands |
| `loom` (memory-model) | **Defer / narrow** | Two targeted tests (the `coalesce_hint` Relaxed pair; the 2-thread ack protocol) *only if* a memory-ordering bug is ever suspected. Not a primary strategy |

**Net:** hand-rolled traits + `proptest`. `turmoil-fs` is a documented future optimization, not a Phase-1 dependency.

## The trait seams (from `dst-plan-trait-seams.md`)

- **`BlockingClock`** — generic param `C: BlockingClock` (zero hot-path vtable). Methods: `now()`, `recv_timeout(rx, d)`, `sleep(d)`, `unix_nanos()`. Injection points: flusher `recv_timeout` (wab/mod.rs:405), `fsync_observed` `Instant::now` (mod.rs:599), panic-supervisor `thread::sleep` (mod.rs:179), worker `recv_timeout` ×2 (worker.rs:80, 124).
- **`SegmentStore` / `SegmentHandle`** — `Arc<dyn>` / `Box<dyn>` (one vtable call per write/fsync, negligible vs real I/O; keeps the generic *out* of the public `wab::spawn` API). Methods: create / `write_record` / `fsync` / `seal` / `rename` / counter-scan. Injection points: `ShardWriter::{ensure_open, fsync_current, seal_current, scan_and_advance_counter}` (segment.rs).
- **Production = pure delegation.** `RealClock` calls std time/sleep/`recv_timeout`; `FsSegmentStore` wraps the current `WabSegment`. `ShardWriter::new` is preserved as a one-line wrapper over `new_with_store(FsSegmentStore)` → **all 30+ existing unit tests compile + pass unchanged**.
- **Feature gating:** trait *definitions* are always compiled (production references them); the sim *implementations* (`SimClock`, `SimSegmentStore`, the harness) are `#[cfg(any(test, feature = "dst"))]`. So `cargo test` always compiles the sim types (catches API drift), and release binaries contain **zero** sim code.

## The test API (from `dst-plan-ergonomics.md`)

```rust
// authoring surface — one seed drives scheduler order + fault offsets + payloads
Sim::new(seed)
    .fault(Fault::FsyncReturns { shard: 0, nth: 3 })
    .scenario(Scenario::CrashAndRecover)
    .run();
```

- **Faults + Scenarios are serde enums.** A failing seed serializes straight into `tests/dst_seeds/*.json` as a pinned regression.
- **Invariants = a `Model` oracle** with one `check_iN_*` method per invariant (the 10 from `phase4-dst-scenarios.md`: Sync-ack durability, no-torn-record, per-shard FIFO, at-least-once, crash-during-recovery survivability, …). The model is updated by sim hooks (ack send, fs fsync, sink commit, recovery read) and checked at `CheckPoint`s + a final `run_all_checks()`. A violation **panics with the invariant name, the seed, and a one-line repro**: `WEIR_DST_SEED=0x… cargo test --features dst`.
- **Shrinking** via `proptest`/ddmin over the event trace → minimal reproducer.

## Phase 1 scope — the first shippable deliverable (~1–2 weeks)

Three seed-reproducible scenarios, **front-loaded by bug severity** (the first two are Critical with *zero* current coverage):

1. **`EIO` on `fdatasync`** (G-WAB-1) — `SimSegmentStore::FailFsync(n)` → exercise the `fsync_observed()` → Nack path (no integration coverage today). Invariant: producers get Nack, **no false ack** for an unsynced record.
2. **Crash between `sync_all` and `rename` in `seal()`** (G-WAB-3) — inject `FailRename` → run `recover_open_segments()` against the resulting `SimFs` state → assert records replay correctly (no lost/torn record).
3. **Panic-supervisor sleep under `SimClock`** — collapses the ~550 ms real-clock respawn-cap test to microseconds; validates `BlockingClock` before Phase 2 needs it for batch-deadline timing.

**Staging (each stage leaves build + all existing tests green):**
- **4.0a** — trait defs (`BlockingClock`, `SegmentStore`) + `ShardWriter` → `new_with_store` refactor (prod-delegating).
- **4.0b** — flusher injection: `flusher_thread` takes the seams; production passes `Real*`.
- **4.0c** — sim impls: `SimClock`, `SimSegmentStore`, the `Sim` builder + `Model` oracle.
- **4.0d** — the 3 DST tests + a seed-reproducibility assertion (identical fault schedule → identical ack outcomes).

**TDD order:** write the 3 failing tests first (they name types that don't exist) → add the minimal seams → green → seed-verify.

## CI integration

- Add a `cargo test --features dst` leg to the matrix.
- **`dst-regression`** — replay every `tests/dst_seeds/*.json` on each push.
- **`dst-sweep-small`** — 100 random seeds × ~5 s on each push.
- **`dst-sweep-large`** — 5,000 seeds nightly.
- Any sweep failure **auto-shrinks** and logs the minimal repro.

## Beyond Phase 1 (roadmap)

- **Phase 2** — more fs faults (torn write, ENOSPC-at-seal); the worker/coalesce timing; analyse the `flush_shard` full-channel path (G-QUEUE-1).
- **Phase 3** — `SimExecutor`: cooperatively schedule multiple blocking "threads" deterministically (cross-shard ordering, the 2-thread ack protocol).
- **Phase 4** — the drain/sink: `SimSink` + the **hung-sink / commit-timeout** scenario (validates concern #2's fix), then the async/socket layer (turmoil for the network).
- Work toward the full **57-scenario catalog × 10 invariants**.

## The one decision to confirm at kickoff

**`SimSegmentStore` (hand-rolled, recommended) vs a `turmoil-fs` spike.** Default: hand-rolled, for a controlled durability-critical foundation. Revisit `turmoil-fs` as a ~1-day spike after Phase 1 lands, adopting it only if the bare-`std::thread` host-accessor integration is clean.
