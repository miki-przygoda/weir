# DST Ergonomics Plan — Author-Facing API, Seed Replay, Invariant Checking, Shrinking, and CI

> **Status:** Exploration / Design Proposal.  
> **Scope:** How a test *author* writes, runs, and debugs DST tests for the WAB.
> Architecture foundation is already defined in `phase4-dst-harness.md`
> (injectable traits, `SimExecutor`, `SimClock`, `SimFs`).  
> The fault catalog lives in `phase4-dst-scenarios.md` (10 invariants, 57 faults).  
> This document makes "using DST" concrete: API shape, invariant oracle,
> seed/replay semantics, shrinking, and CI integration.

---

## 1. Test-Authoring API — What the Developer Actually Writes

### 1.1 Design principles

Three pressures shape the API:

1. **The reproduce-first rule.** When a seed appears in CI output the developer
   must be able to reproduce the failure with a single copy-paste command.  No
   intermediate scaffolding, no environment setup.
2. **Minimal boilerplate per scenario.** A new fault scenario should require
   adding one variant to a Rust enum and one block of assertion logic.  No new
   files, no trait impls.
3. **Ordinary Rust test.** A DST test is a `#[test]` function in a `#[cfg(test)]`
   module gated by `--features dst`.  It runs under `cargo test`, is visible to
   IDEs and `rust-analyzer`, and does not require a custom test harness binary.

### 1.2 The builder API — `Sim`

The primary entry point is a builder that is seeded once and derives everything
else from that seed:

```rust
// crates/weir-server/src/sim/mod.rs  (compiled only with --features dst)

/// A single deterministic simulation run.  All nondeterminism (scheduler
/// ordering, fault injection timing, input sequence) derives from `seed`.
pub struct Sim {
    seed: u64,
    scenario: Scenario,
    config: SimConfig,
}

impl Sim {
    /// Create a simulation from an explicit seed.
    pub fn new(seed: u64) -> SimBuilder { SimBuilder::new(seed) }

    /// Create a simulation from the `WEIR_DST_SEED` environment variable if
    /// set, or from a freshly drawn random seed otherwise.  This is the
    /// call-site used in property-style random runs.
    pub fn from_env_or_random() -> SimBuilder { ... }

    /// Run the simulation to completion and return the final `SimOutcome`.
    /// Panics if any invariant fires; the panic message includes the repro line.
    pub fn run(self) -> SimOutcome { ... }
}
```

`SimBuilder` exposes ergonomic fault attachment and config overrides:

```rust
pub struct SimBuilder { ... }

impl SimBuilder {
    /// Attach a named fault schedule.  Multiple faults can be chained.
    pub fn fault(self, fault: Fault) -> Self { ... }

    /// Override the default pipeline config (shard count, batch size, etc.).
    pub fn config(self, config: SimConfig) -> Self { ... }

    /// Override the scenario (the logical sequence of pushes and events).
    pub fn scenario(self, scenario: Scenario) -> Self { ... }

    /// Seal the builder and run.  Equivalent to `.build().run()`.
    pub fn run(self) -> SimOutcome { self.build().run() }

    pub fn build(self) -> Sim { ... }
}
```

### 1.3 A worked example — EIO on fdatasync (gap G-WAB-1)

```rust
#[cfg(feature = "dst")]
#[test]
fn eio_on_nth_fsync_nacks_producer_and_preserves_prior_records() {
    // --- arrange ---------------------------------------------------------
    // Run with an explicit seed so this is a pinned regression test.
    // When run in the random loop (§5) no seed is passed; a failure prints
    // the seed for later pinning.
    Sim::new(0xC0FFEE_0000_0001)
        .fault(Fault::FsyncReturns {
            shard: 0,
            call_index: 3,           // fail the 4th fsync (0-indexed)
            error: FaultError::Eio,
        })
        .scenario(Scenario::SteadyPush {
            record_count: 20,
            durability: Durability::Sync,
            shard: 0,
        })
        .run();
    // SimOutcome is automatically checked against every applicable invariant
    // (see §2).  The call panics on the first violation with the repro line.
}
```

The `Fault` and `Scenario` types are plain Rust enums so they compose without
combinatorial explosion of builder methods.

### 1.4 The `Fault` enum

Every entry in the phase4-dst-scenarios.md fault catalog maps to one variant:

```rust
/// A deterministic fault to inject.  One `Sim` can carry zero or more.
/// The seed controls *when within the fault's window* the fault fires (for
/// faults that specify a call_index that is itself derived from the seed in
/// random-seed runs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Fault {
    // --- Disk faults (D-*) -----------------------------------------------
    FsyncReturns       { shard: usize, call_index: u32, error: FaultError },
    FsyncBlocksFor     { shard: usize, call_index: u32, delay_us: u64 },
    WriteRecordReturns { shard: usize, record_index: u32, error: FaultError },
    SealReturns        { shard: usize, segment_index: u32, at: SealPoint, error: FaultError },
    // --- Crash points (C-*) ----------------------------------------------
    KillAfterFsync     { shard: usize, call_index: u32 },
    KillAfterSeal      { shard: usize, segment_index: u32, at: SealPoint },
    KillDuringRecovery { shard: usize, at: RecoveryPoint },
    // --- Flusher panics (P-*) --------------------------------------------
    FlushPanic         { shard: usize, call_index: u32, times: u32 },
    WorkerPanic        { worker: usize, after_records: u32 },
    // --- Queue saturation (Q-*) ------------------------------------------
    StallFlusher       { shard: usize, stall_us: u64 },  // makes channel fill
    // --- Sink faults (S-*) -----------------------------------------------
    SinkReturns        { call_index: u32, response: SinkResponse },
    SinkHangs          { call_index: u32, unblock_after_ms: u64 },
    // --- Clock / timer (T-*) ---------------------------------------------
    ClockJitter        { jitter_us: u64 },  // adds simulated jitter to each tick
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultError { Eio, Enospc, Efbig, PermissionDenied }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SealPoint { AfterSyncAll, AfterSentinel, AfterFooter }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryPoint { AfterSetLen, AfterRename }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkResponse { Transient, Permanent, Ok }
```

`Fault` derives `Serialize`/`Deserialize` — this is the anchor for the seed
serialization scheme described in §3.

### 1.5 The `Scenario` enum

Scenarios describe the logical input sequence, not the scheduling:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Scenario {
    /// Push N records to one shard with fixed durability.
    SteadyPush { record_count: u32, durability: Durability, shard: u32 },

    /// N concurrent producers each pushing M records to the same shard.
    ConcurrentProducers { producers: u32, records_each: u32, shard: u32 },

    /// Push records then send SIGTERM while the flusher is mid-fsync.
    GracefulShutdownUnderLoad { records_before_shutdown: u32, shard: u32 },

    /// Push records, crash (via `KillAfterFsync`-style fault), then restart
    /// and recover.  The harness drives both the pre-crash and recovery runs.
    CrashAndRecover { pre_crash_records: u32, shard: u32 },

    /// Custom sequence: the test provides a list of `SimEvent`s directly.
    Custom(Vec<SimEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SimEvent {
    Push { payload: Vec<u8>, durability: Durability, shard: u32 },
    AdvanceClock { by_us: u64 },
    FlushAll,
    TriggerShutdown,
    RestartDaemon,
}
```

### 1.6 Pinned regression tests vs. random property tests

Two usage patterns:

**Pinned regression** — the seed is a literal `u64` and the test is checked in.
Used to prevent a known bug from silently re-emerging.  These run under
`cargo test --features dst`.

**Random sweep** — the seed comes from `Sim::from_env_or_random()`.  Running N
random sweeps per CI job is covered in §5.  A failure in a random sweep prints
the seed; the developer pastes it into a pinned test.

---

## 2. Invariant Checking — the `Model` Oracle

### 2.1 Oracle vs. inline assertions

There are two places to check invariants: *inline* during the simulation (after
each simulated event) and *post-run* against the final state.  The oracle uses
both:

- **Continuous checks** catch violations as early as possible and produce traces
  that are easier to shrink (§4 needs a small trace to shrink against).
- **Post-run checks** catch invariants that are only meaningful once the run is
  complete (e.g., "every Sync ack eventually corresponds to a durable record").

The `Model` struct accumulates the ground truth during the run:

```rust
/// Ground-truth model maintained by the harness alongside the simulated pipeline.
/// Every event that flows through the simulated pipeline is recorded here first;
/// invariant checks compare the pipeline's observed behaviour against this model.
pub struct Model {
    /// All records for which the simulated pipeline sent `ack=true` with
    /// Durability::Sync or Durability::Batched.
    synced_acks: Vec<Record>,

    /// Records observed in the in-memory WAB segments after the run (or after
    /// simulated recovery if the scenario includes a crash).
    wab_contents: Vec<Record>,

    /// Records delivered to the simulated sink.
    sink_received: Vec<Record>,

    /// Records for which the pipeline sent `ack=true` with Durability::Buffered.
    buffered_acks: Vec<Record>,

    /// Records for which the pipeline sent `ack=false` (Nack).
    nacks: Vec<Record>,

    /// Metric snapshots captured after the run.
    metrics_snapshot: MetricsSnapshot,
}

/// A record as seen by the model (payload + position in submission order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub payload: Vec<u8>,
    pub shard: u32,
    /// Submission sequence number within this shard (0-indexed).
    pub seq: u64,
}
```

### 2.2 Mapping all 10 invariants to executable checks

Each check is a named method on `Model`.  The harness calls all checks; any
failure panics with the invariant name and the repro seed.

```rust
impl Model {
    /// I-1: Every Sync-acked record must be in `wab_contents` after recovery.
    ///
    /// Checked post-run (or post-recovery if the scenario included a crash).
    pub fn check_i1_sync_acked_records_are_durable(&self) {
        for r in &self.synced_acks {
            assert!(
                self.wab_contents.contains(r),
                "[I-1 FAIL] Sync-acked record {:?} not in WAB after recovery",
                r
            );
        }
    }

    /// I-2: No Buffered-acked record is claimed to be durable.
    ///
    /// The harness never *requires* Buffered records to survive, but it asserts
    /// that the pipeline does not upgrade a Buffered ack to appear in a
    /// post-crash WAB when the fault was injected before the fsync.
    pub fn check_i2_buffered_acks_carry_no_durability_promise(&self) {
        // Nothing to assert structurally — the harness simply does not include
        // Buffered records in synced_acks.  This check validates the model
        // itself is consistent.
        let overlap: Vec<_> = self.buffered_acks.iter()
            .filter(|r| self.synced_acks.contains(r))
            .collect();
        assert!(overlap.is_empty(),
            "[I-2 FAIL] Records appear as both buffered and sync-acked: {:?}", overlap);
    }

    /// I-3: Every Sync/Batched acked record either reaches the sink or is
    /// dead-lettered.
    ///
    /// Checked post-drain (after the simulated drain completes or the scenario
    /// declares the run done).
    pub fn check_i3_no_acked_but_lost_record(&self) {
        for r in &self.synced_acks {
            let delivered = self.sink_received.contains(r)
                || self.metrics_snapshot.dead_lettered_payloads.contains(&r.payload);
            assert!(
                delivered,
                "[I-3 FAIL] Sync-acked record {:?} neither reached sink nor dead-letter",
                r
            );
        }
    }

    /// I-4: No torn record appears as valid in `wab_contents`.
    ///
    /// Checked after every simulated recovery.  The harness injects torn
    /// writes via `Fault::WriteRecordReturns { error: Enospc }` and records
    /// which records were partially written; those must not appear in
    /// wab_contents.
    pub fn check_i4_no_torn_record_treated_as_valid(&self, partially_written: &[Record]) {
        for r in partially_written {
            assert!(
                !self.wab_contents.contains(r),
                "[I-4 FAIL] Partially-written record {:?} returned by recovery as valid",
                r
            );
        }
    }

    /// I-5: Per-shard FIFO ordering is preserved in both WAB and sink delivery.
    pub fn check_i5_fifo_within_shard(&self) {
        let check_fifo = |records: &[Record], label: &str| {
            let mut by_shard: HashMap<u32, u64> = HashMap::new();
            for r in records {
                let last_seq = by_shard.entry(r.shard).or_insert(0);
                assert!(
                    r.seq >= *last_seq,
                    "[I-5 FAIL] {label}: out-of-order record on shard {}: \
                     saw seq {} after seq {}",
                    r.shard, r.seq, last_seq
                );
                *last_seq = r.seq;
            }
        };
        check_fifo(&self.wab_contents, "WAB");
        check_fifo(&self.sink_received, "Sink");
    }

    /// I-6: No segment is drained more than once.
    pub fn check_i6_no_double_drain(&self) {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for r in &self.sink_received {
            assert!(
                seen.insert(r.payload.clone()),
                "[I-6 FAIL] Record {:?} delivered to sink more than once \
                 (double-drain of segment)",
                r
            );
        }
    }

    /// I-7: After < MAX_FLUSHER_RESPAWNS panics, the shard recovers and
    /// subsequent Sync pushes succeed.
    ///
    /// The harness checks this by asserting that after the last injected panic
    /// resolves, at least one more Sync push to the same shard produces an ack.
    pub fn check_i7_flusher_panic_does_not_permanently_lose_shard(
        &self,
        shard: u32,
        panics_injected: u32,
    ) {
        assert!(
            panics_injected < crate::wab::MAX_FLUSHER_RESPAWNS,
            "test precondition: panics_injected must be < MAX_FLUSHER_RESPAWNS for I-7 to hold"
        );
        let post_panic_acks = self.synced_acks.iter()
            .filter(|r| r.shard == shard)
            .count();
        assert!(
            post_panic_acks > 0,
            "[I-7 FAIL] No Sync acks on shard {} after {} recoverable panics",
            shard, panics_injected
        );
    }

    /// I-8: Queue saturation produces Nack, not silent drop.
    pub fn check_i8_queue_saturation_is_nack_not_silent_drop(
        &self,
        records_pushed: usize,
    ) {
        let accounted = self.synced_acks.len()
            + self.buffered_acks.len()
            + self.nacks.len();
        assert_eq!(
            accounted, records_pushed,
            "[I-8 FAIL] {} records pushed but only {} accounted for \
             (synced + buffered + nacked); {} silently dropped",
            records_pushed, accounted, records_pushed - accounted
        );
    }

    /// I-9: Dead-letter-full blocks the drain, does not crash or drop.
    ///
    /// The harness verifies the segment that was pending when the DL dir filled
    /// is eventually confirmed (either after headroom is made available or
    /// after the scenario ends with an explicit unblock event).
    pub fn check_i9_dead_letter_full_blocks_not_crashes(
        &self,
        segment_pending_when_blocked: PathBuf,
    ) {
        // The segment must not be confirmed while blocked; after unblock it
        // must be confirmed.  The harness tracks confirm events.
        assert!(
            self.metrics_snapshot.drain_state_label == "blocked_dead_letter_full"
                || self.metrics_snapshot.segments_confirmed.contains(&segment_pending_when_blocked),
            "[I-9 FAIL] Segment {:?} neither confirmed nor properly blocked",
            segment_pending_when_blocked
        );
    }

    /// I-10: A crash during recovery does not corrupt records that were valid
    /// before the interrupted truncation.
    ///
    /// Checked by running two successive recovery passes with a crash injected
    /// between them and comparing wab_contents before and after.
    pub fn check_i10_crash_during_recovery_does_not_corrupt_surviving_records(
        &self,
        pre_crash_valid_records: &[Record],
    ) {
        for r in pre_crash_valid_records {
            assert!(
                self.wab_contents.contains(r),
                "[I-10 FAIL] Record {:?} valid before recovery crash but absent after \
                 second recovery pass",
                r
            );
        }
    }

    /// Run all checks that are always applicable at end-of-run.
    /// Individual checks may be called earlier (e.g., check_i4 after recovery).
    pub fn run_all_checks(&self, ctx: &SimContext) {
        self.check_i1_sync_acked_records_are_durable();
        self.check_i2_buffered_acks_carry_no_durability_promise();
        self.check_i3_no_acked_but_lost_record();
        self.check_i5_fifo_within_shard();
        self.check_i6_no_double_drain();
        self.check_i8_queue_saturation_is_nack_not_silent_drop(ctx.total_records_pushed);
    }
}
```

### 2.3 How the harness drives the oracle

The `SimExecutor` drives every simulated event through a shared `Arc<Mutex<Model>>`
oracle alongside the simulated pipeline.  Each simulated hook calls back into
`Model`:

- `SimFs::fsync(shard, seg)` — on success: records this fsync in the model;
  on EIO: marks the records in the batch as not-durable.
- `SimSink::commit(batch)` — on success: appends records to `model.sink_received`.
- `SimAck::send(ack)` — routes to `model.synced_acks` (true, Sync/Batched) or
  `model.buffered_acks` (true, Buffered) or `model.nacks` (false).
- `SimFs::recovery_read(seg)` — populates `model.wab_contents` so I-1 and I-4
  can compare.

The invariant checks are called at each `CheckPoint` (after recovery, after
drain, at end-of-run) inserted as `SimEvent::CheckInvariants` nodes in the
event graph.

---

## 3. Seed → Reproducible Run

### 3.1 The canonical seed type

A single `u64` is the external identity.  Internally the harness derives three
independent sub-streams from it via SipHash-style key derivation:

```rust
pub struct DerivedSeeds {
    /// Drives the SimExecutor task-ordering RNG.
    pub scheduler:   u64,
    /// Drives the fault-injection offset RNG (for faults whose `call_index` is
    /// "random within a range" rather than pinned to a literal).
    pub fault_rng:   u64,
    /// Drives the input-sequence payload generator.
    pub input_rng:   u64,
}

impl DerivedSeeds {
    pub fn from(master: u64) -> Self {
        // Each sub-seed is the master XOR-mixed with a domain constant so the
        // three streams never collide.
        Self {
            scheduler: mix(master, 0x517cc1b727220a95),
            fault_rng: mix(master, 0x9e3779b97f4a7c15),
            input_rng: mix(master, 0x6c62272e07bb0142),
        }
    }
}

fn mix(a: u64, b: u64) -> u64 {
    let x = a ^ b;
    // Finalizer from wyhash — avalanches well and is branch-free.
    let x = x.wrapping_mul(0x517cc1b727220a95);
    x ^ (x >> 32)
}
```

Both the scheduler stream (`SmallRng::seed_from_u64(seeds.scheduler)`) and the
fault-rng stream are initialized once at `Sim::run()` and never reset.  This
guarantees that replaying the same `u64` always produces the exact same run.

### 3.2 Serialized seed format — `SeedRecord`

For pinned regression tests the seed is stored as a `SeedRecord`:

```rust
/// Complete description of a DST run that can be checked in and replayed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedRecord {
    /// The master u64 seed.
    pub seed: u64,
    /// The scenario used (for human readability; the seed alone is sufficient
    /// for replay but the scenario makes the intent obvious).
    pub scenario: Scenario,
    /// Any pinned faults (faults with literal call_index values).
    pub faults: Vec<Fault>,
    /// Human-readable description of what this seed is supposed to exercise.
    pub description: String,
}
```

Seed records are JSON files checked into `tests/dst_seeds/`.  They are also
used to drive the regression suite (§5.3).

### 3.3 Failure output — the repro line

When any invariant check fires the harness emits:

```
=== DST FAILURE ===
Invariant : I-1 (Sync-acked records are durable)
Seed      : 0xC0FFEE_DEAD_0042
Scenario  : SteadyPush { record_count: 20, durability: Sync, shard: 0 }
Faults    : [FsyncReturns { shard: 0, call_index: 3, error: Eio }]
Repro     : WEIR_DST_SEED=0xC0FFEE_DEAD_0042 cargo test --features dst dst_

Pin this seed with:
  cargo test --features dst -- dst_seeds_print 0xC0FFEE_DEAD_0042
```

The `WEIR_DST_SEED` env var is read by `Sim::from_env_or_random()` so both the
property-sweep tests and individual named tests respond to it.

### 3.4 How the `SimExecutor` consumes the seed

The `SimExecutor` maintains a `SmallRng` initialized from `seeds.scheduler`.
When multiple tasks are ready at the same simulated tick it picks the next task
to run by calling `rng.gen_range(0..ready.len())`.  Because the same seed
always produces the same sequence of `gen_range` calls, and because no
real-time input enters the run, the interleaving is fully deterministic.

The only dependency is that tasks must be enqueued in a canonical order (sorted
by name) before the RNG is consulted — this prevents the OS thread pool from
introducing ordering variance during the setup phase.

---

## 4. Failure Minimization — Shrinking the Event Trace

### 4.1 Why event-trace shrinking rather than value shrinking

proptest shrinks values (e.g., reduce `n` from 1000 to 3).  DST failures are
caused by a specific interleaving of events, not by the size of the input.
Shrinking a DST failure means finding the *minimal prefix* of the event trace
that still triggers the invariant violation.

### 4.2 The shrinker algorithm

```
Given: seed S, scenario SC, faults F
       and the failing event trace T = [e_0, e_1, … e_N]

Goal: find the shortest prefix T' ⊆ T such that replaying with the same seed
      and the events in T' still violates the same invariant.

Algorithm (ddmin-style bisection):
  1. Start with i = N (full trace).
  2. Bisect: try prefix T[0..i/2].  If it still fails, set i = i/2 and repeat.
  3. If the half fails, try dropping individual events from the failing half.
  4. Repeat until no further reduction is possible.
  5. Report the minimal trace and the "minimal repro" SeedRecord.
```

The shrinker also tries:
- Removing individual `Fault` entries (which fault is load-bearing?)
- Reducing `record_count` in the scenario (how few records still trigger it?)
- Fixing the fault `call_index` to the smallest value that still fires it.

The output is a `SeedRecord` with a human-readable `description` and the
minimal fault + scenario combination.  The developer's job is to read that
record and understand the bug — not to understand a 10,000-event trace.

### 4.3 Shrinking API

Shrinking is exposed as a helper called automatically when a random-seed run
fails in CI (§5), and as a manual tool for debugging:

```rust
// In a dev shell after CI reports seed 0xDEAD:
cargo test --features dst -- dst_shrink 0xDEAD
```

This runs the shrinker, prints the minimal `SeedRecord` as JSON, and writes it
to `tests/dst_seeds/shrunk_<timestamp>.json`.

### 4.4 Shrinking budget

Shrinking is bounded by a configurable `ShrinkBudget`:

```rust
pub struct ShrinkBudget {
    /// Maximum number of candidate runs to try.
    pub max_candidates: u32,     // default: 500
    /// Time limit for the shrink session.
    pub time_limit: Duration,    // default: 30s
}
```

In CI the budget is generous (500 candidates).  Locally the developer can
override with `WEIR_DST_SHRINK_BUDGET=fast` (50 candidates, 5s) to get a quick
rough-cut minimiser.

---

## 5. CI Integration

### 5.1 The `dst` Cargo feature

All simulation code is behind `--features dst`.  This means:

- **Zero cost in release builds.** The `sim/` module, `SimClock`, `SimFs`,
  `SimExecutor`, and all `Model` oracle code compile out completely.
- **No API surface leakage.**  `pub(crate)` types inside `wab/segment.rs` that
  gain a `#[cfg(feature = "dst")]` injection hook are invisible to downstream
  crates.
- **Ordinary `cargo test` discipline.** `cargo test` (no flags) runs the
  existing test suite unchanged.  `cargo test --features dst` enables DST.

The feature gates are:

```toml
# crates/weir-server/Cargo.toml

[features]
# Deterministic Simulation Testing — injectable trait seams + SimExecutor.
# Adds ~800 lines of test-only code; zero cost in release builds.
# All dst_* integration tests require this feature.
dst = []
```

Internal gating in source:

```rust
// crates/weir-server/src/wab/segment.rs
#[cfg(feature = "dst")]
pub(crate) fn inject_fsync_error_next(&self) { ... }
```

### 5.2 Random seed loop in CI

A dedicated CI job runs N random seeds per push:

```yaml
# .github/workflows/dst.yml
name: DST random sweep
on: [push]
jobs:
  dst-sweep:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: DST random sweep (500 seeds)
        run: |
          cargo test --features dst --test dst_sweep -- \
            --seeds 500 --timeout-per-seed 10
        env:
          WEIR_DST_SEEDS: "500"
          WEIR_DST_TIMEOUT_PER_SEED: "10"  # seconds
```

The `dst_sweep` integration test (in `tests/dst_sweep.rs`) loops:

```rust
#[cfg(feature = "dst")]
#[test]
fn dst_random_sweep() {
    let n_seeds: u32 = std::env::var("WEIR_DST_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    for _ in 0..n_seeds {
        let builder = Sim::from_env_or_random();
        // Run; any invariant violation causes a panic with the repro line.
        // The test harness catches that panic, records the seed, and continues.
        // After the loop it re-panics with all failed seeds.
        let outcome = std::panic::catch_unwind(|| builder.run());
        if let Err(e) = outcome {
            // Record failed seed for the final report.
            ...
        }
    }
    assert!(failures.is_empty(), "DST sweep found {} failure(s):\n{}", failures.len(), report);
}
```

### 5.3 Regression seed suite

Every bug that was found via DST and fixed ships with a pinned `SeedRecord` in
`tests/dst_seeds/`.  A separate test loads and replays all files in that directory:

```rust
// tests/dst_regression.rs
#[cfg(feature = "dst")]
#[test]
fn dst_regression_seeds() {
    let seed_dir = std::path::Path::new("tests/dst_seeds");
    for entry in std::fs::read_dir(seed_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") { continue; }
        let record: SeedRecord = serde_json::from_str(
            &std::fs::read_to_string(&path).unwrap()
        ).unwrap();
        // This must reproduce the original failure (i.e., the test verifies
        // the fix is in place: if the fix reverted, the seed would fail again).
        //
        // Pinned seeds are therefore "must-pass" seeds — they document fixes,
        // not failures.  The harness runs them expecting success.
        Sim::new(record.seed)
            .scenario(record.scenario)
            .faults(record.faults)
            .run(); // panics if invariant violated
    }
}
```

The workflow: CI finds seed `0xDEAD` → developer runs shrinker → produces
`tests/dst_seeds/i1_eio_on_fsync.json` → fixes the bug → seed now passes →
checked in as regression guard.

### 5.4 Recommended CI schedule

| Job | Trigger | Seeds | Feature flag |
|-----|---------|-------|--------------|
| `dst-regression` | Every push | All files in `tests/dst_seeds/` | `--features dst` |
| `dst-sweep-small` | Every push | 100 seeds, 5 s each | `--features dst` |
| `dst-sweep-large` | Nightly + release tags | 5 000 seeds, 30 s each | `--features dst` |

The small sweep catches regressions quickly on every PR.  The large sweep
explores more of the state space in the background and is where new bugs are
most likely to appear.

---

## 6. What the Test-Author Experience Feels Like End to End

1. **Write a test** — copy the EIO example from §1.3.  Swap in the fault and
   scenario you want.  One `Sim::new(seed).fault(...).scenario(...).run()` call.
   No mock trait impls, no setup/teardown infrastructure.

2. **Run it** — `cargo test --features dst eio_on_nth_fsync`.  It either passes
   (green) or panics with `[I-1 FAIL]` + the repro line.

3. **Reproduce a CI failure** — copy the `WEIR_DST_SEED=0x...` line from the CI
   log, paste it into a shell, run the sweep test again.  Same failure, same
   trace.

4. **Shrink it** — `cargo test --features dst -- dst_shrink 0xDEAD`.  The
   shrinker produces a JSON file listing the minimal fault (e.g., one
   `FsyncReturns` on call 3) and the minimal scenario (e.g., 4 records not 20).
   That JSON becomes the test case.

5. **Fix and pin** — fix the bug, run the seed, verify it passes.  Rename the
   JSON file to something descriptive (`tests/dst_seeds/i1_eio_fsync.json`).
   `dst_regression_seeds` will replay it forever.

6. **Review** — `dst_regression_seeds` + `dst-sweep-small` run on every PR.
   The full description in `SeedRecord.description` explains what each pinned
   seed is testing.  New contributors can read the JSON files as a catalog of
   fixed bugs.

---

## 7. Open Questions and Constraints

**Q-A: Should `Scenario::CrashAndRecover` drive two `Sim` instances or one?**

The crash-and-recover pattern requires two separate runs (pre-crash state →
crash → recovery pass).  The cleanest model is one `Sim` that owns both phases:
the `SimFs` in-memory filesystem persists across the restart boundary, so the
second run sees the same file system state the first left behind.  This means
the `Sim` struct gets a `restart()` hook that resets the executor and clock
without resetting the `SimFs`.  The seed still drives both phases (post-restart
the seed continues from where it left off, not re-seeded).

**Q-B: How does continuous invariant checking interact with shrinking?**

Shrinking works by replaying prefixes.  Continuous checks may fire at different
prefix lengths for different seeds.  The shrinker's bisection must compare the
*same invariant* fired at different points — it uses the invariant name (e.g.,
`"I-1"`) as the bisection key, not the event index.

**Q-C: Can the `Fault` enum express the `G-QUEUE-1` silent-discard gap?**

Yes, via `Fault::StallFlusher` which stalls the flusher long enough for the
worker to fill the bounded channel and trigger the `.ok()` discard.  The oracle
then checks I-8 to confirm every pushed record is accounted for.  The current
hypothesis (per `phase4-dst-scenarios.md §5 Q-2`) is that the discard is safe
because acks fire in the flusher — but the DST check will confirm or refute
this.

**Q-D: `SmallRng` is not `no_std`-compatible.**

`rand::rngs::SmallRng` requires `std`.  Since `weir-server` already depends on
`std` throughout, this is not a concern.  If the simulation types are later
extracted into a `weir-sim` crate that targets `no_std`, switch to a portable
PRNG like `wyrand` or `xoshiro256**` with a hand-rolled implementation.

---

## Summary

- **Test API shape:** `Sim::new(seed).fault(Fault::...).scenario(Scenario::...).run()` — builder pattern, seeded, no per-test boilerplate beyond constructing the fault and scenario enums.

- **Invariants as executable oracle:** A `Model` struct accumulates ground truth alongside the simulated pipeline; all 10 invariants map to named `check_*` methods called at `CheckPoint` events and at end-of-run.

- **Single `u64` master seed:** Three sub-streams derived via mix function drive the scheduler RNG, the fault-offset RNG, and the input-payload generator independently; same master seed → identical run on any machine.

- **Failure repro line:** Any invariant violation prints `WEIR_DST_SEED=0x...` + a one-line `cargo test` invocation; the developer reproduces by copy-paste; the shrinker produces a minimal `SeedRecord` JSON that becomes the pinned regression file in `tests/dst_seeds/`.

- **Shrinking:** ddmin-style bisection over the event trace, bounded by a `ShrinkBudget`; also shrinks fault count and scenario record-count; output is a minimal `SeedRecord` that documents both the bug and its fix.

- **CI:** `--features dst` feature gate; `dst-regression` replays all `tests/dst_seeds/*.json` on every push; `dst-sweep-small` (100 random seeds) on every push; `dst-sweep-large` (5 000 seeds) nightly; failure in a sweep triggers the shrinker automatically and logs the minimal repro.
