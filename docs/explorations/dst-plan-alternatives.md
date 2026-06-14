# DST Tooling Landscape: Build vs Adopt vs Hybrid

**Status:** Research / recommendation — no implementation.
**Builds on:** `phase4-dst-harness.md` (recommended hand-rolled injectable traits +
single-threaded sim executor), `phase4-dst-scenarios.md` (invariants + fault catalog).
**Purpose:** Survey the current Rust DST/concurrency-testing ecosystem, evaluate each
tool against weir's specific hybrid architecture (tokio async + `std::thread` blocking +
`crossbeam_channel` + real `fdatasync`), and issue a clear build-vs-adopt recommendation.

---

## 1. The Constraint That Shapes Every Evaluation

weir is not a pure-async program. Its durability-critical hot path runs in
`std::thread` workers and flushers:

```
async accept loop (tokio) → spawn_blocking → crossbeam queue
  → std::thread worker → crossbeam shard channel
    → std::thread wab-flusher → fdatasync → ack_tx.send(true)
```

The bugs that matter (silent discard on flusher channel full, EIO ack path,
mid-seal crash, panic respawn timing, ACK_TIMEOUT races with a slow fsync) all
live in that blocking-thread slice. Any framework that controls only the async
layer covers at most the queue's entry point. The hard requirement is:
**controllable blocking-thread scheduling** and **injectable `fdatasync`**.

---

## 2. Crate-by-Crate Assessment

### 2.1 `madsim` — Deterministic tokio runtime replacement

**What it is.** madsim (v0.2.34, ~5.8M total downloads, last updated October
2025) is a drop-in tokio replacement that intercepts `tokio::task`, `tokio::time`,
`tokio::net`, and `tokio::fs`. It exposes a seeded RNG for task scheduling and
provides deterministic simulated time. It is the primary simulation substrate
for TiKV and related distributed databases.

**Coverage for weir.**

| Layer | Covered? |
|-------|----------|
| tokio task ordering (accept loop, connection handlers) | Yes |
| `tokio::time::sleep`, `timeout`, `interval` | Yes |
| `tokio::net` TCP | Yes |
| `std::thread` (worker, flusher, drain) | No — OS-scheduled |
| `crossbeam_channel::recv_timeout` | No — real wall-clock |
| `fdatasync` / `File` operations | No |
| `std::time::Instant::now()` in blocking threads | No |

**The blocking-thread problem.** madsim controls everything inside the tokio
runtime. The moment a `spawn_blocking` call or a `thread::Builder::new().spawn()`
runs, madsim loses visibility. The WAB flusher's `recv_timeout(batch_deadline)`,
its `fdatasync` call, and the ack's `send(true)` are all invisible to madsim.
This covers roughly 30% of weir's nondeterminism sources (the accept loop, queue
depth poller, disk scanner interval, `ACK_TIMEOUT` on the async side) while
leaving the critical 70% — the blocking hot path — fully nondeterministic.

**Maturity and maintenance.** Active (October 2025 release), large user base via
RisingWave and TiKV ecosystem. API is stable at 0.2.x. The hard requirement of
patching `tokio` is via a proc-macro that replaces imports at compile time
(`#[madsim::test]`). This means weir's Cargo.toml would need a feature-gated
tokio replacement, which is non-trivial. The tokio version pinning (madsim 0.2.34
wraps tokio 1.x but specific minor version alignment matters) creates upgrade
friction.

**Verdict for weir.** Insufficient on its own. Does not address the blocking-thread
problem at all. The import-patching mechanism adds non-trivial Cargo.toml
complexity. Only useful if combined with blocking-thread simulation — but in that
case, turmoil covers the same async ground with a simpler integration model and
also brings `turmoil-fs`.

---

### 2.2 `turmoil` — Deterministic async simulation + network + filesystem

**What it is.** turmoil (v0.7.2, ~15.5M total downloads, last updated April 2026,
Tokio project) is a multi-host simulation framework. Unlike madsim, it does not
replace the tokio runtime binary; instead it runs multiple "hosts" as async tasks
inside a single-threaded deterministic runtime. As of v0.7.1 (January 2026) it
ships two unstable-but-usable sub-crates:

- **`turmoil-net`** (0.1.0, published May 2026): Drop-in `tokio::net` replacement
  for deterministic network simulation.
- **`turmoil-fs`** (0.1.0, published alongside v0.7.1 on GitHub, not yet on
  crates.io as of this writing): Drop-in `std::fs` / `tokio::fs` replacement with
  an explicit pending-vs-durable durability model. `sync_data()` and `sync_all()`
  are fully shimmed and model the `fdatasync` semantic: writes land in "pending"
  state, a `sync_all()`/`sync_data()` call promotes them to durable, and a
  simulated crash discards pending state. Available via
  `turmoil = { features = ["unstable-fs"] }` in 0.7.2.

The `unstable-barriers` feature (available in 0.7.2) adds typed trigger points
that can be conditionally compiled into source code. `trigger_noop()` is
**synchronous** — callable from a `std::thread` — and fires a notification to
test code without suspending execution. `trigger()` is async-only but supports
full suspension (test code can halt a task at a specific code point).

**Coverage for weir.**

| Layer | Covered? |
|-------|----------|
| tokio task ordering (deterministic single-threaded sim) | Yes |
| `tokio::time::sleep`, `timeout`, `interval` | Yes |
| `tokio::net` TCP | Yes (turmoil-net) |
| `std::fs::File::sync_data()` (fdatasync shim) | Yes (unstable-fs) |
| `std::fs::File::write_all()`, `write_vectored()` | Yes (unstable-fs) |
| `std::fs::rename()` | Yes (unstable-fs) |
| `std::thread` scheduling | No — OS-scheduled |
| `crossbeam_channel::recv_timeout` timing | No — real wall-clock |
| `std::time::Instant::now()` in blocking threads | No |

**The blocking-thread problem remains.** turmoil-fs controls what happens when the
flusher thread calls `file.sync_data()` via the shim, but it does not control
*when* the flusher thread runs relative to the worker thread. The OS scheduler
still determines thread interleaving. turmoil-fs gives fault injection at
`fdatasync` call sites; it does not give deterministic scheduling of the threads
that make those calls.

**The `trigger_noop` bridge.** For scenarios that need to observe a specific code
point inside a blocking thread (e.g., "between `sync_all()` and `rename()` in
`seal()`" — scenario A-2 in the fault catalog), `trigger_noop()` can be
conditionally compiled in and fires synchronously. This gives test code a signal
that a known-sensitive point was reached, enabling coarse control (inject a fault
at the next `sync_data()` call, then check the result after recovery) without
requiring full thread scheduling control.

**Maturity and maintenance.** Active Tokio project, large user base, responsive
maintainers. The `unstable-fs` feature is newly stabilized (added January 2026,
marked experimental removed April 2026 in the CHANGELOG). The `turmoil-fs`
pending/durable model maps precisely to the WAB flusher's correctness property
(writes are only durable after `sync_data()` or `sync_all()`). The fs shim models
torn writes and crash-before-fsync natively.

**Verdict for weir.** The most relevant framework for weir's file I/O fault
scenarios. turmoil-fs directly addresses D-1 through D-9 in the disk fault
catalog without requiring a custom `WabBackend` trait at all — the shim intercepts
the real `std::fs::File` operations. Combined with barriers, it can cover mid-seal
crash scenarios (A-2, C-3/C-4) by injecting a panic or error at a specific code
point. Does NOT address blocking-thread scheduling nondeterminism or
`crossbeam_channel` timing.

---

### 2.3 `shuttle` — Randomized/PCT concurrency-interleaving testing

**What it is.** shuttle (v0.9.1, ~4.0M total downloads, last updated April 2026,
AWS Labs) is a randomized concurrency testing library implementing PCT
(Probabilistic Concurrency Testing) and DFS scheduling for `std::thread` programs.
It replaces concurrency primitives (`shuttle::thread::spawn`, `shuttle::sync::Mutex`,
`shuttle::sync::mpsc::channel`) with instrumented versions that the shuttle
scheduler controls. By controlling the scheduler, it makes thread interleavings
deterministic and replayable from a seed.

**Coverage for weir.**

| Primitive | Shuttle wrapper? | Notes |
|-----------|-----------------|-------|
| `std::thread::spawn` | `shuttle::thread::spawn` | Yes — core feature |
| `std::sync::Mutex` | `shuttle::sync::Mutex` | Yes |
| `std::sync::mpsc::Receiver::recv_timeout` | `shuttle::sync::mpsc::Receiver::recv_timeout` | **Partially** — see below |
| `crossbeam_channel::Receiver::recv_timeout` | None | **No** — open issue #crossbeam |
| `std::time::Instant::now()` / `thread::sleep` | None | **No** — real wall-clock |
| `fdatasync` / file I/O | None | No |

**Critical gap 1: crossbeam_channel.** weir's worker and flusher threads use
`crossbeam_channel::Receiver::recv_timeout`, not `std::sync::mpsc`. Shuttle wraps
`std::sync::mpsc` but not crossbeam. An open GitHub issue (#crossbeam) requests
this; as of May 2026 it is not implemented. Using shuttle on weir's actual flusher
and worker code would require either replacing crossbeam with `std::sync::mpsc`
(a meaningful dependency change with performance implications) or creating a
compatibility shim.

**Critical gap 2: `recv_timeout` is a no-op for time.** Even in the `std::sync::mpsc`
wrapper, shuttle's `recv_timeout` ignores the `_timeout` parameter completely
(source: `shuttle-std/src/sync/mpsc.rs`):

```rust
pub fn recv_timeout(&self, _timeout: Duration) -> Result<T, RecvTimeoutError> {
    // TODO support the timeout case -- this method never times out
    self.inner.recv().map_err(|_| RecvTimeoutError::Disconnected)
}
```

This means the `batch_deadline`-driven flush path (`recv_timeout(batch_deadline)`)
would NEVER fire in shuttle testing. Every flusher test would run without the
deadline-flush path, missing a whole class of timing-dependent bugs (T-5, T-6 in
the clock/timer catalog).

**What shuttle is still good for.** Despite these gaps, shuttle is the only tool
in this set that exercises `std::thread` interleavings with PCT. If weir were
migrated from crossbeam to `std::sync::mpsc` channels and the `recv_timeout`
timeout firing were unimportant, shuttle would cover the worker-flusher-drain
thread ordering bugs directly, without any trait injection. The PCT scheduler
empirically finds most real-world concurrency bugs. shuttle is actively maintained
(weekly commits, last release April 2026) and is produced by AWS Labs for
production use in the AWS SDK.

**Verdict for weir.** Partially applicable today; not plug-and-play given crossbeam
dependency. The `recv_timeout` gap means the batch-deadline flush path is untestable.
A hybrid where the `BlockingClock` trait (hand-rolled) wraps the `recv_timeout` call
and shuttle replaces `std::thread::spawn` could make the scheduling deterministic
while the clock injection handles deadline timing — but this requires both shuttle
integration AND the trait seam refactor.

---

### 2.4 `loom` — Exhaustive memory-model concurrency testing

**What it is.** loom (v0.7.2, ~51M total downloads, last updated April 2024,
Tokio project) systematically explores all thread execution orderings and memory
access patterns permitted by the C11 memory model. It replaces `loom::sync::atomic`
for atomics and `loom::thread` for threads, then re-runs the test body for every
reachable execution prefix.

**Coverage for weir.**

| Layer | Covered? |
|-------|----------|
| `AtomicU64` load/store on `coalesce_hint` (Relaxed ordering) | Yes |
| `std::thread::spawn` interleavings (exhaustive) | Yes |
| `crossbeam_channel` | No (loom does not wrap it) |
| Wall-clock time | No |
| File I/O | No |

**Practical limitation: scale.** loom is exhaustive — it explores every reachable
interleaving. For two threads sharing one atomic, this is tractable. For weir's 1
worker + 1 flusher + 1 drain thread, each with multi-step loops, the state space
explodes almost immediately. loom is most useful for small, isolated data structures
(a custom lock, the EWMA atomic update sequence, a 2-thread ack protocol).

**The `coalesce_hint` question.** The DST harness doc (§6.1) explicitly asks whether
loom is warranted for the `coalesce_hint` `AtomicU64`. The answer is: it is the
only tool that can verify the Relaxed load/store sequence. However, the harness
doc's tentative conclusion stands: `coalesce_hint` is a heuristic, the blast radius
of a bogus value is bounded by `clamp(COALESCE_MIN_US, COALESCE_MAX_US)`, and the
risk does not justify loom's integration overhead. loom would require conditionally
replacing `use std::sync::atomic::AtomicU64` with `use loom::sync::atomic::AtomicU64`
behind `cfg(loom)`, which is low overhead but adds a compile-time config dimension.

**Verdict for weir.** Narrow but precise. Warranted specifically for verifying the
`coalesce_hint` update/read protocol if a Relaxed-ordering bug is suspected, or for
unit-testing a custom channel or lock implementation. Not a substitute for
behavioral DST. Maximum two targeted loom tests; not a primary DST strategy.

---

### 2.5 `proptest` — Property-based generation and shrinking

**What it is.** proptest (v1.11.0, ~138M total downloads, last updated March 2026)
provides Strategy-based input generation and automatic shrinking. weir already uses
it in `weir-core` and `weir-client` for envelope protocol fuzzing.

**DST relevance: seed minimization and trace shrinking.** The harness doc (§6.7)
describes the desired seed format as a single `u64` that determines scheduling
order, fault injection, and input sequences. proptest's `Arbitrary` trait and
`prop_compose!` macro provide a clean way to generate structured `Scenario`
structs (a combination of record sequence + fault schedule + scheduling seed) and
shrink them. When a DST failure is found, proptest's shrinking reduces the `Scenario`
to the minimal one that still fails — smaller record count, fewer faults, simpler
scheduling seed.

**How it combines.** proptest does not provide scheduling or fault injection
itself. It generates the *inputs* to a DST harness. The integration looks like:

```rust
proptest! {
    #[test]
    fn dst_flusher_fault_scenarios(scenario in any::<FlusherScenario>()) {
        let mut sim = FlusherSim::new(scenario.seed);
        sim.inject_faults(scenario.fault_schedule);
        sim.run(scenario.records);
        sim.assert_invariants();
    }
}
```

proptest's shrinking then automatically finds the minimal `FlusherScenario` that
triggers an invariant violation.

**Verdict for weir.** Already present in the workspace. Should be used as the
outer test harness for DST scenario generation + shrinking. Not a substitute for
scheduling control or fault injection — a complement to both.

---

### 2.6 `quickcheck` — Randomized property testing

**What it is.** quickcheck (v1.1.0, ~57M total downloads, last updated February
2026) is the original Rust property-testing library, inspired by Haskell's
QuickCheck. It provides `Arbitrary`-based generation and shrinking with a simpler
API than proptest but less expressiveness for structured generation.

**Comparison with proptest.** For weir's DST use case, proptest is the better
choice. weir already has proptest in the workspace (used in `weir-core` and
`weir-client`). proptest's `Strategy` combinators allow generating structured
`FaultSchedule` and `RecordSequence` types more naturally than quickcheck's
`Arbitrary`. proptest also supports a stable, saved-to-disk seed corpus
(`proptest-regressions`) which aligns directly with the DST goal of persisting
failing seeds as regression fixtures.

**Verdict for weir.** No advantage over proptest given weir already uses proptest.
Do not add quickcheck.

---

### 2.7 `cargo-nextest` — Test runner

**What it is.** cargo-nextest (v0.9.137, ~10.8M downloads, last updated May 2026)
is a next-generation cargo test runner that runs each test in its own process,
provides better failure output, supports test partitioning for CI sharding, and
integrates with retry-on-failure policies.

**DST relevance.** nextest's per-test process isolation is useful for DST tests
that manipulate global state (e.g., `turmoil`'s thread-local barrier repo, or the
`SimClock` global tick counter). nextest's `--fail-fast` and
`--test-threads 1` flags allow running DST tests serially in CI without contention.
nextest also supports `junit` XML output, enabling CI to surface failing DST seeds
as structured artifacts.

**Integration overhead.** nextest is a dev toolchain addition, not a code change.
weir's CI already runs tests with cargo; adding nextest is a one-line `.cargo/config.toml`
change (`[alias] test = ["nextest", "run"]`) or explicit `cargo nextest run` in CI.

**Verdict for weir.** Worth adopting as the test runner, independent of which DST
strategy is chosen. Low-cost, improves DST test isolation, no code changes required.

---

### 2.8 `tokio::time` pause/advance

**What it is.** tokio provides `tokio::time::pause()` and `tokio::time::advance(d)`
(enabled by the `test-util` feature flag) for deterministic async time control in
`#[tokio::test]` tests. `start_paused = true` in `#[tokio::test(start_paused = true)]`
starts with time frozen; `advance()` moves simulated time forward without wall-clock
delay.

**Coverage for weir.** The tokio async layer in weir uses several `tokio::time`
constructs: `ACK_TIMEOUT` (30s), `read_timeout`, the 500ms queue-depth poll
interval, and the 5s WAB disk scanner interval. With `test-util` + `start_paused`,
all of these can be controlled deterministically in async tests.

**The blocking-thread boundary.** `tokio::time::pause()` affects only tokio's
async runtime clock. `std::time::Instant::now()` in blocking threads is
unaffected. `crossbeam_channel::recv_timeout` in the flusher is unaffected. The
pause/advance mechanism is therefore useful for async-layer tests (connection
handler timeout behavior, queue depth poll timing) but does not reach the
blocking-thread durability path.

**Verdict for weir.** Already available with a `test-util` feature flag addition
to the dev-dependencies. Worth using for async-layer timer tests (e.g., testing
that `ACK_TIMEOUT` fires correctly at the async boundary without waiting 30 real
seconds). Covered by Phase 4.3 in the roadmap doc. Not a DST strategy for the
blocking hot path.

---

### 2.9 `fail` — Named failure points

**What it is.** fail (v0.5.1, ~17.8M total downloads) provides named failure
points that can be configured at runtime via environment variables or a
`FailScenario`. A `fail_point!("name")` macro either no-ops or executes a
configured action (return an error, panic, sleep, print) depending on the active
scenario.

**Coverage for weir.** `fail` is the closest to a pure fault-injection library.
It would allow inserting `fail_point!("wab::fsync")` in `platform_fsync()` and
activating `EIO` injection from a test without replacing the `WabBackend` trait.
It works in blocking threads, requires no architectural refactor, and is
synchronous.

**Limitations for weir.**

1. **No scheduling control.** `fail` injects faults at named points but does not
   control thread interleaving. The "exactly between `sync_all()` and `rename()`"
   scenario requires both a fault at `rename()` AND control over when the drain
   thread runs next. `fail` covers the first; the second needs either shuttle or
   a custom executor.

2. **Not seed-reproducible.** `fail` configurations are process-global and set
   via environment variables or `FailScenario::setup()`. There is no built-in
   concept of a seed that deterministically reproduces a specific fault schedule
   across runs.

3. **Test pollution risk.** `fail` uses a process-global configuration table.
   Parallel tests that configure `fail_point!` for the same point interfere with
   each other. nextest's per-process isolation mitigates this.

**Verdict for weir.** Useful as a lightweight complement for adding controllable
fault injection to existing integration tests without the `WabBackend` trait
refactor. Specifically valuable for the disk-fault scenarios (D-3 EIO on fsync,
D-8/D-9 ENOSPC during seal) as an interim measure before the full trait seam
lands. Does not achieve DST goals of reproducibility or scheduling control.
Treat as a stopgap, not a primary strategy.

---

## 3. Capability Matrix

| Tool | Async scheduling | Blocking-thread scheduling | File I/O fault injection | Clock simulation (blocking) | `crossbeam_channel` recv_timeout | Seed-reproducible | Production-code invasiveness |
|------|-----------------|---------------------------|--------------------------|-----------------------------|---------------------------------|-------------------|------------------------------|
| madsim | Full (drops-in tokio) | None | None (unless via shim) | Async only | No | Yes | High (tokio replacement) |
| turmoil | Full (own runtime) | None | Yes (unstable-fs) | Async only | No | Yes | Medium (import swap in tests) |
| shuttle | Partial (wraps tokio) | Yes (std::thread) | None | No | No (no crossbeam wrapper) | Yes | High (replace all sync primitives) |
| loom | None | Yes (exhaustive, small scope) | None | No | No | Yes (model-guided) | High (replace atomics/sync) |
| proptest | None | None | None | None | No | Yes (saved seeds) | None |
| tokio::time pause | Partial (async timers) | None | None | Async only | No | Yes | Low (test feature flag) |
| fail | None | None | Partial (named points) | No | No | No (env-var config) | Low–Medium |
| **Hand-rolled traits** (from harness doc) | None directly | Yes (SimExecutor) | Yes (SimFs/SimBackend) | Yes (SimClock) | Yes (via Clock.recv_timeout) | Yes (seeded SimExecutor) | Medium–High (trait injection) |

---

## 4. Combination Analysis

### Option 1: Pure hand-rolled (the harness doc's conclusion)

Implement `BlockingClock`, `WabBackend`, `SimExecutor`, `SimFs`, `SimSink` as
described in `phase4-dst-harness.md`. No external framework dependencies in the
DST path.

**Pros:**
- Full coverage of every nondeterminism source, including `crossbeam_channel`
  `recv_timeout` timing (via `Clock.recv_timeout` wrapper) and thread scheduling
  (via `SimExecutor` cooperative scheduler).
- No upstream API instability risk.
- The `SimClock` advances simulated time explicitly, making batch-deadline
  testing correct (unlike shuttle's broken `recv_timeout` timeout handling).
- Maximum portability: works on macOS, Windows, and Linux without filesystem
  shim compatibility concerns.

**Cons:**
- ~400–500 lines of new simulation code to write and maintain.
- `SimFs` is custom work that turmoil-fs has already solved (pending/durable
  model, torn writes, crash recovery).
- The `SimExecutor` is custom work that is subtle to get right (deadlock
  detection, deterministic ordering, panic propagation).
- No built-in network fault simulation for the async accept loop.

---

### Option 2: turmoil-fs for file faults + hand-rolled for blocking-thread scheduling

Use `turmoil` with `unstable-fs` to replace `std::fs::File` in the WAB flusher
with the shim (intercepting `sync_data()`, `write_all()`, `rename()`), and use
the hand-rolled `BlockingClock` + `SimExecutor` for scheduling.

**Pros:**
- turmoil-fs's pending/durable model is exactly the WAB correctness model. A
  simulated crash discards pending writes, which directly tests invariant I-1.
- Torn write simulation (partial writes that survive a crash) is built into
  turmoil-fs.
- Eliminates the need to write a custom `SimFs` — the hardest part of the
  pure hand-rolled approach.
- turmoil-fs is stable enough to use (published 0.1.0, experimental label
  removed April 2026, used in Tokio's own tests).
- Barriers (`turmoil::trigger_noop`) can observe specific code points in
  blocking threads (e.g., between `sync_all()` and `rename()` in `seal()`),
  enabling mid-seal crash injection without fully replacing the file backend.

**Cons:**
- turmoil-fs is a synchronous `std::fs::File` shim. Its determinism depends on
  being inside a turmoil simulation's host context. Using it from a bare
  `std::thread` (outside a tokio runtime) requires careful setup — the shim
  uses a thread-local host accessor that turmoil installs. This is the
  published design (`install_host_accessor`), but it adds integration complexity.
- Still requires `BlockingClock` + `SimExecutor` for scheduling control (turmoil
  does not schedule `std::thread`s).
- turmoil-fs's crates.io publication lags the GitHub HEAD; pinning to git may
  be needed for some features.

---

### Option 3: shuttle for thread interleavings + hand-rolled WabBackend + proptest for shrinking

Use `shuttle::thread::spawn` to replace `thread::Builder::new().spawn()` in the
flusher and worker threads, inject fault points via a hand-rolled `WabBackend`
trait (not a full `SimFs`, just the fsync and write error path), and use proptest
to generate + shrink fault schedules.

**Pros:**
- shuttle's PCT scheduler directly exercises worker-flusher-drain thread
  interleavings deterministically, which is the scheduling nondeterminism source
  the pure hand-rolled approach addresses with `SimExecutor`.
- Reduces the custom executor code to zero.
- shuttle is actively maintained (weekly commits, AWS backing).

**Cons (blockers):**
- weir's channels are `crossbeam_channel`, not `std::sync::mpsc`. shuttle
  has no crossbeam wrapper (open issue, unimplemented as of May 2026). Using
  shuttle would require replacing all `crossbeam_channel::Receiver::recv_timeout`
  calls with `std::sync::mpsc`, which is a meaningful dependency change and may
  have performance implications.
- Even with `std::sync::mpsc`, shuttle's `recv_timeout` **never times out**
  (`// TODO support the timeout case -- this method never times out`). The
  batch-deadline flush path would be entirely untestable under shuttle: every
  flusher test would wait for explicit sends, never for the deadline timer.
- shuttle does not control `Instant::now()` or `thread::sleep`. The panic
  supervisor's `thread::sleep(10 * attempt ms)` would block real wall-clock
  time in tests.

**Conclusion on Option 3.** The crossbeam incompatibility and `recv_timeout`
stub are blockers, not limitations. shuttle cannot test weir's actual flusher
code without replacing crossbeam and custom-implementing timeout semantics —
at which point it provides less value than the hand-rolled approach.

---

### Option 4: Recommended hybrid — turmoil-fs + hand-rolled Clock/SimExecutor + proptest

This is the precise refinement of the harness doc's "Approach C" conclusion,
incorporating the newly available turmoil-fs.

**Components:**

| Responsibility | Implementation |
|----------------|----------------|
| `fdatasync` fault injection, torn writes, crash-before-fsync | turmoil-fs `unstable-fs` shim |
| Mid-operation observation points (between sync_all + rename) | turmoil `unstable-barriers` `trigger_noop` |
| `crossbeam_channel::recv_timeout` timing (batch deadline) | `BlockingClock` trait, `SimClock` impl |
| Flusher / worker / drain thread scheduling order | `SimExecutor` cooperative scheduler |
| Sink faults (transient/permanent/hang) | `SimSink` hand-rolled |
| Input generation + seed shrinking | proptest `FaultScenario` + prop_compose! |
| ACK_TIMEOUT / `tokio::time` async timers | `tokio::time::pause()` + `test-util` feature |
| Network fault simulation (Phase 4.3) | turmoil network layer |
| Test runner isolation | cargo-nextest |

**What this eliminates from the pure hand-rolled approach:**

- Custom `SimFs` is replaced by turmoil-fs. turmoil-fs models pending vs durable
  state, crash discard, and torn writes correctly. Writing a correct `SimFs` from
  scratch is several hundred lines of subtle code; turmoil-fs is battle-tested.
- The `unix_nanos_now()` timestamp in segment headers is automatically handled:
  turmoil-fs's simulated time provides the `SystemTime::now()` equivalent inside
  the simulation.

**What remains hand-rolled:**

- `BlockingClock` trait + `SimClock`: still needed because turmoil-fs does not
  control `crossbeam_channel::recv_timeout` timing or `Instant::now()` in
  blocking threads.
- `SimExecutor`: still needed because turmoil does not schedule `std::thread`s.
  However, the `SimExecutor` can be simpler than in the pure hand-rolled case:
  it does not need to model file state (turmoil-fs handles that); it only needs
  to control which blocking "thread" runs next.
- `SimSink`: simple scripted mock, ~50 lines.

**Integration path for turmoil-fs shim.** The shim requires an import swap in
test code:

```rust
#[cfg(not(feature = "dst"))]
use std::fs::{File, OpenOptions};
#[cfg(feature = "dst")]
use turmoil::fs::shim::std::fs::{File, OpenOptions};
```

This affects `wab/segment.rs` and `drain/confirmed.rs`. It is less invasive than
the `WabBackend` trait refactor: no signature changes to `ShardWriter`, no
dynamic dispatch overhead. The shim intercepts at the `File` level.

---

## 5. Honest Assessment: Build vs Adopt

### The harness doc's "build it" conclusion revisited

The harness doc concluded "Approach C (injectable traits + turmoil)" primarily
because turmoil, at the time of that analysis, was primarily a network simulation
tool. The intervening months have changed the picture: turmoil-fs (January 2026)
directly addresses the file I/O layer that was the main gap in the earlier
turmoil assessment. The `unstable-fs` feature is now in the stable 0.7.2 release.

The core conclusion — **that no off-the-shelf tool can replace the hand-rolled
blocking-thread simulation** — remains correct. shuttle cannot because of the
crossbeam incompatibility and broken `recv_timeout`. loom cannot because it is
exhaustive (not behavioral) and does not control clock time. madsim cannot because
it does not reach `std::thread`. turmoil cannot because it only schedules async
tasks. This remains an irreducible gap.

The change is that **the file I/O simulation component is now adoptable rather
than buildable**. turmoil-fs is a better `SimFs` than weir would write in-house.

### What this means for the Phase 4.0 plan

The Phase 4.0 plan in the harness doc is:
1. Extract `WabBackend` trait
2. Implement `SimWabBackend` (in-memory, scriptable faults)
3. Extract `BlockingClock` trait + `SimClock`
4. Refactor `flusher_thread` to accept both traits

The revised recommendation is:
1. Extract `BlockingClock` trait + `SimClock` (unchanged)
2. Refactor `flusher_thread` to accept `BlockingClock` (unchanged)
3. For file I/O fault injection: use **turmoil-fs shim** (import swap in test
   code, no trait extraction from `ShardWriter`) rather than a custom `WabBackend`
   trait
4. Keep `SimExecutor` for blocking-thread scheduling

This reduces Phase 4.0 effort by approximately one week (the `WabBackend` trait
design, `SimWabBackend` implementation, and `ShardWriter` generic/dyn refactor
are the most complex parts of the original plan). turmoil-fs does that work for
free.

The `WabBackend` trait extraction remains a long-term desideratum for other
reasons (testing recovery behavior with synthetic files, segment format
unit tests) but is not required for Phase 4.0's core goal of deterministic
fdatasync fault injection.

---

## 6. Final Recommendation

**Adopt a specific hybrid, with the heaviest lifting still hand-rolled.**

| Category | Tool | Adoption |
|----------|------|----------|
| File I/O simulation (fdatasync, torn writes, crash-before-sync) | `turmoil` + `unstable-fs` feature | Adopt |
| Blocking-thread scheduling determinism | `SimExecutor` (hand-rolled) | Build |
| `crossbeam_channel::recv_timeout` timing + clock | `BlockingClock` + `SimClock` (hand-rolled) | Build |
| Observation points in blocking threads | `turmoil` `unstable-barriers` `trigger_noop` | Adopt |
| Async timer determinism (`ACK_TIMEOUT`, intervals) | `tokio::time::pause()` + `test-util` | Adopt |
| Input generation + seed minimization | `proptest` (already in workspace) | Already present |
| Test runner isolation | `cargo-nextest` | Adopt |
| Thread interleaving concurrency | `shuttle` | Defer — blocked by crossbeam gap |
| Memory ordering (`coalesce_hint` AtomicU64) | `loom` | Defer — low risk, heuristic-only |
| madsim | Not recommended | Redundant with turmoil |

**Why not pure hand-rolled.** turmoil-fs is the better `SimFs`. It models
pending vs durable state correctly, handles torn writes, and has been validated
by the Tokio project. Writing an equivalent from scratch adds several hundred
lines of subtle code that weir does not need to own.

**Why not a framework-first approach.** No single framework covers weir's
blocking-thread hot path. shuttle is the closest, but the crossbeam
incompatibility and `recv_timeout` stub are hard blockers today. The `SimExecutor`
and `SimClock` are irreducible custom work.

**The irreducible hand-rolled core** is small (under 300 lines for `SimClock` +
`SimExecutor` + `SimSink`), well-bounded, and does not change after Phase 4.0.
It is not a framework; it is a thin adapter layer between real concurrency
primitives and the test harness.

**Reassess shuttle** when its crossbeam-channel wrapper is implemented and
`recv_timeout` is given time semantics. At that point, shuttle could replace
`SimExecutor` entirely, reducing the hand-rolled surface further.

---

## 7. Open Questions

**Q-1: Does turmoil-fs's `sync_data()` shim require the flusher to run inside
a turmoil host context?** The shim uses a thread-local host accessor. If the
flusher thread is a bare `std::thread` spawned outside a turmoil simulation,
the accessor may not be installed. This requires validation against turmoil-fs
0.1.0's `install_host_accessor` API before Phase 4.0 begins.

**Q-2: turmoil-fs publish status.** `turmoil-fs` 0.1.0 is in the turmoil GitHub
repository and referenced by `turmoil = { features = ["unstable-fs"] }` in
0.7.2, but its crates.io listing shows no stable version as of this writing.
The `unstable-fs` re-export in `turmoil` 0.7.2 should work; verify
`cargo build --features unstable-fs` compiles before committing to this path.

**Q-3: Does `SimExecutor` need to integrate with turmoil's event loop?** If
flusher threads use turmoil-fs for file operations and `SimExecutor` for
scheduling, both need to agree on the current simulated time. `turmoil-fs`
advances time independently. The `SimClock` advances a separate tick counter.
These two must be unified or explicitly kept separate (acceptable if file
operation latency is modeled as zero in simulation).

**Q-4: How does proptest interact with turmoil simulation seeds?** turmoil uses
its own seeded RNG for network and filesystem chaos decisions. proptest generates
its own seeds for input sequences. A `FaultScenario` struct wrapping both seeds
(`turmoil_seed: u64, proptest_seed: u64, records: Vec<Payload>`) can be
`Arbitrary`-derived and shrunk by proptest as a unit.

---

*Paired with `phase4-dst-harness.md` (trait design), `phase4-dst-scenarios.md`
(fault catalog), `dst-plan-trait-seams.md` (exact trait signatures), and
`dst-plan-roadmap.md` (delivery phases).*
