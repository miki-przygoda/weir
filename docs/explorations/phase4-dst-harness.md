# Exploration: Deterministic Simulation Testing (DST) Harness for weir

**Status:** Exploration — research + proposal only. No implementation.
**Scope:** Harness architecture. Fault/scenario catalog is handled by a sibling document.
**Pairing:** This document focuses on *how to get to determinism*; the fault catalog covers *what to test once there*.

---

## 1. Why DST for weir — and what it buys

### The core testing gap today

weir's current test pyramid has good coverage at the extremes:

- **Unit tests** (pure functions): `ewma_update_us`, WAB format parsing, CRC round-trips, queue routing — all deterministic and fast.
- **Integration / system tests** (`tests/system.rs`): spawn the real binary, push records over a real socket, assert bytes on disk, kill with SIGKILL and verify recovery. These are extremely valuable but inherently probabilistic. A test like `graceful_shutdown_under_load` concludes "no unexpected errors" — but whether those 8 threads interleaved in the pathological order (worker flush races flusher shutdown races drain confirmation) is undefined. The test passes 999 times and fails on attempt 1000 in CI on a loaded machine.

The gap in the middle is *protocol-level correctness under adversarial scheduling* — scenarios like:

- Worker flushes its last batch to shard channel *exactly* as flusher enters graceful-shutdown drain. Does the record land in the sealed segment?
- Drain reads the confirmed sidecar midway through a crash that kills the write. Which file is authoritative on restart?
- Coalesce EWMA update is written by flusher thread while worker is reading it with `Relaxed` ordering. Do the relaxed-store/load sequences produce a plausible coalesce window or a degenerate 0?
- Queue backpressure: `push_timeout` fires at exactly the same instant a worker drains the partition. Does the Nack fire before or after the drain?

None of these can be reliably reproduced by a system test that uses real time and real thread scheduling.

### What DST would buy

1. **Reproducibility.** Every failure carries a seed. Running the same seed always produces the same interleaving. Finding-in-CI → reproduce-on-laptop becomes trivial.
2. **Exhaustion of thin-slice scenarios.** A DST engine can step through every scheduling order for a 3-thread scenario in seconds. The same exhaustion takes hours or is impossible in real-wall-clock tests.
3. **Fault coverage without fragile mocks.** Fsync failures, clock jitter, network partitions, and torn writes can be injected at defined points rather than via RLIMIT hacks or tmpfs mounts (see `efbig_returns_nack_not_crash` in `system.rs`).
4. **Regression regression.** A fixed bug ships with a failing seed. The seed becomes a permanent CI fixture — the bug can never silently regress.
5. **Shrinking.** Good DST frameworks shrink the event trace to the minimal sequence that still triggers the bug, analogous to proptest's value shrinking but over execution traces.

### The hard constraint

weir is a **hybrid** system: tokio async for the accept loop, `std::thread` + blocking I/O for the worker pool, WAB flushers, and drain. The blocking threads call `recv_timeout`, `Instant::now()`, `fdatasync`, and `thread::sleep`. No existing off-the-shelf DST runtime (madsim, turmoil) covers this hybrid model out of the box. Getting to full DST requires either radical architectural changes or careful scoping of what "deterministic" means.

---

## 2. Nondeterminism sources — mapped to code

This section enumerates every source of nondeterminism and maps it to the specific code location.

### 2.1 OS thread scheduling

**All std::thread spawns.** The kernel schedules threads in an undefined order. Relevant spawn sites:

| Thread | Location | Nondeterminism |
|--------|----------|----------------|
| `weir-worker-N` | `worker.rs:spawn_workers` (`thread::Builder::new().spawn(...)`) | Order in which workers drain the queue partition; which worker gets the first record after a quiet period |
| `wab-flusher-N` | `wab/mod.rs:spawn` | Which flusher fires its `batch_deadline` timer first; order in which `recv_timeout` unblocks across shards |
| `weir-drain` | `drain/mod.rs:spawn` | When drain thread picks up a newly sealed segment vs. when flusher is still writing to it |
| Panic supervisor retries | `wab/mod.rs:run_with_panic_supervision` | `thread::sleep(10 * attempt ms)` — wall-clock timing of respawn |
| Tokio blocking pool | `socket/connection.rs:handle_connection` via `task::spawn_blocking` | `push_timeout` executes on a tokio-managed blocking pool thread; scheduling order with worker threads is undefined |

Core affinity (`core_affinity::set_for_current`) and SCHED_FIFO (`libc::sched_setscheduler` on Linux) reduce *variance* but do not eliminate it — the kernel can preempt SCHED_FIFO threads at any point between syscalls.

### 2.2 Tokio task scheduling

**Async task ordering.** The tokio multi-thread runtime (`new_multi_thread`) uses a work-stealing scheduler; the order in which async tasks are polled is undefined across test runs.

| Code | Nondeterminism |
|------|----------------|
| `socket/mod.rs:loop { tokio::select! { ... } }` | `select!` arm ordering when multiple events are ready simultaneously (biased toward shutdown but otherwise unordered) |
| `join_set.spawn(handle_connection(...))` | Which connection task is polled next; race between the accept loop and in-flight handlers |
| `metrics server::spawn(...)` | Parallel with accept loop; ordering of metric scrape vs. record ack is undefined |
| Queue depth poller (`tokio::time::interval(500ms)`) | Fires at real wall-clock ticks; not synchronized with record processing |
| WAB disk scanner (`tokio::time::interval(5s)` + `spawn_blocking`) | Same |

### 2.3 `Instant::now()` and real-time timers

**Every call to `Instant::now()` or `recv_timeout`/`timeout` captures real wall-clock time.** Key sites:

| Code | Usage |
|------|-------|
| `worker.rs:79` — `work_rx.recv_timeout(batch_deadline)` | Decides when worker flushes without a full batch |
| `worker.rs:91` — `self.coalesce_hint.load(Relaxed)` then `recv_timeout(window)` | The coalesce window size depends on the EWMA read, which is updated by flusher threads asynchronously |
| `wab/mod.rs:405` — `work_rx.recv_timeout(batch_deadline)` | Decides when flusher runs a batch even without batch_size records |
| `wab/mod.rs:599` — `let t = Instant::now(); writer.fsync_current(); t.elapsed()` | EWMA update input — fsync timing is real I/O latency |
| `drain/mod.rs:288` — `Instant::now() + config.dead_letter_check_interval` | BlockedDeadLetterFull wake-up timing |
| `drain/mod.rs:283` — `blocked_since.elapsed()` | `dead_letter_blocked_duration` gauge value |
| `connection.rs:ACK_TIMEOUT` — `tokio::time::timeout(30s, ack_rx.recv())` | Ack timeout fires at real time; under simulation, this means tests must wait 30s or the timeout never fires |
| `#[cfg(feature = "bench-trace")] enqueued_at: std::time::Instant::now()` | Stage latency attribution |

### 2.4 Fsync timing and fault injection

**`fdatasync` is a real OS call.** Its latency is:
- Variable (NVMe: ~100µs, spinning disk: ~5ms, tmpfs: ~0µs, VM block device: highly variable)
- Not reproducible across runs
- A source of latency that feeds back into the coalesce EWMA

**Fault injection** today requires OS-level tricks:
- `RLIMIT_FSIZE = 0` produces `EFBIG` (see `system.rs:efbig_returns_nack_not_crash`)
- A small tmpfs produces `ENOSPC` (see the `#[ignore]` test `enospc_returns_nack_not_crash`)

Neither is injectable at specific code points (e.g., "fail the fsync on the 7th call but not the 6th").

### 2.5 Crossbeam channel ordering under concurrent senders

**MPMC channels.** `crossbeam_channel::bounded` provides FIFO within a single sender, but when multiple senders compete, the interleaving of messages from different senders is defined by OS thread scheduling.

| Channel | Concurrent senders |
|---------|-------------------|
| `queue.rs` work queue (partitioned) | Multiple async tasks via `spawn_blocking` — effectively multiple threads |
| `wab/mod.rs` `shard_txs` (per-shard) | All worker threads send to the same `Sender<Batch>` clone for a given shard |
| `drain_tx: Sender<PathBuf>` | Flusher threads (on rotation) + flusher threads (on shutdown seal) |

### 2.6 `AtomicU64` EWMA update races

`coalesce_hint` (`Arc<AtomicU64>`) is updated by each flusher after every fsync and read by each worker before each coalesce window. The ordering is `Relaxed` by design ("heuristic, not a correctness signal"). Under DST this is fine — relaxed atomics are not a correctness source. But the *value* of the EWMA at any given point depends on the thread interleaving, making coalesce window sizes nondeterministic.

### 2.7 Panic supervisor `thread::sleep`

`wab/mod.rs:179` — `thread::sleep(Duration::from_millis(10 * u64::from(attempt)))`. Under real wall clock, `thread::sleep(10ms)` blocks for at least 10ms (often more). Under simulation, this sleep must be interceptable.

### 2.8 `unix_nanos_now()` in WAB segment headers

`wab/format.rs` — segment headers embed a timestamp (`unix_nanos_now()`). This is a real `SystemTime::now()` call. Under simulation this produces non-repeatable headers. The timestamp is currently used only for diagnostics, not correctness; but it is a nondeterminism source.

---

## 3. Architectural approaches

### Approach A: `madsim` (deterministic tokio runtime replacement)

[`madsim`](https://github.com/madsim-rs/madsim) is a drop-in replacement for tokio that intercepts `tokio::time`, `tokio::task`, and network I/O. It exposes a seeded random number generator for task scheduling decisions, making the async portion of a program deterministic and replayable.

**What madsim covers for weir:**
- Tokio task scheduling (accept loop, connection handlers, metrics polling)
- `tokio::time::sleep`, `tokio::time::interval`, `tokio::time::timeout`
- Network I/O on simulated transports (relevant for the TCP+mTLS path)

**What madsim does NOT cover for weir — the hard part:**

Weir's blocking threads (`weir-worker-N`, `wab-flusher-N`, `weir-drain`) are `std::thread`s. They call `crossbeam_channel::recv_timeout`, `std::time::Instant::now()`, `fdatasync`, and `thread::sleep`. madsim has no visibility into `std::thread` execution order. Thread scheduling remains at the mercy of the OS kernel.

**The threads-vs-determinism tension with madsim:**

madsim controls all tokio-runtime-internal scheduling. But the boundary is the `spawn_blocking` call in `connection.rs` — once the work unit crosses into the blocking pool, it is in an OS-scheduled thread. The queue send, worker batch accumulation, WAB write, fsync, and ack resolution all happen in `std::thread` context that madsim cannot intercept.

**Practical consequence:** madsim would give weir deterministic async scheduling (covering about 30% of the nondeterminism sources in section 2) but would leave the critical hot path — worker→WAB→ack — fully non-deterministic.

**Verdict:** Insufficient on its own. Useful if combined with blocking-thread simulation.

---

### Approach B: Custom injectable trait abstractions (`Clock`, `Filesystem`, `Executor`)

Define thin traits that abstract every nondeterminism source, provide real implementations for production and deterministic implementations for simulation.

#### Proposed trait surface

```rust
/// Abstracts Instant::now() and sleep/timeout.
trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep_blocking(&self, duration: Duration);
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration)
        -> Result<T, RecvTimeoutError>;
}

/// Abstracts the WAB filesystem operations.
trait WabFs: Send + Sync {
    fn create_segment(&self, path: &Path, shard_id: u16) -> io::Result<WabSegment>;
    fn fsync(&self, segment: &mut WabSegment) -> io::Result<()>;
    fn seal_segment(&self, segment: WabSegment) -> io::Result<PathBuf>;
    fn read_segment(&self, path: &Path) -> io::Result<Box<dyn Iterator<Item=io::Result<Payload>>>>;
}

/// Abstracts std::thread::spawn for the WAB flusher and worker threads.
trait ThreadSpawner {
    fn spawn<F: FnOnce() + Send + 'static>(&self, name: String, f: F);
}
```

#### Deterministic implementations

**`SimClock`:** Wraps a single global simulated time counter (an `AtomicU64` of nanoseconds). `now()` reads the counter. `sleep_blocking(d)` does not block — it atomically advances the counter by `d`. `recv_timeout(rx, timeout)` polls `rx.try_recv()` in a loop, advancing simulated time on each empty poll until timeout.

**`SimFs`:** In-memory file system backed by a `HashMap<PathBuf, Vec<u8>>`. `create_segment` returns an in-memory writer. `fsync` is a no-op (or can be injected to fail). `seal_segment` renames the in-memory key. All operations are synchronous and produce no latency variation. Fault injection is per-call: a `FaultSchedule` can specify "fail fsync #7 with ENOSPC".

**`SimExecutor`:** A single-threaded cooperative scheduler. `spawn(name, f)` enqueues `f` as a ready task. The executor runs tasks in a deterministic order (e.g., alphabetical by name, or seeded random from a fixed seed). Task switching happens only at explicit yield points — `recv_timeout` polling and `sleep_blocking`.

#### How this fits weir's hybrid model

The key insight: weir's blocking threads are not truly parallel for correctness purposes. The WAB segment write sequence within a single shard is:

1. Flusher receives `Batch` from shard channel.
2. Flusher calls `write_record` for each `WorkUnit`.
3. Flusher calls `fsync`.
4. Flusher sends `true` on each `ack_tx`.

Steps 1–4 form a **sequential state machine within a single shard**. Under simulation, we can run all shards interleaved on a single thread by the `SimExecutor`, yielding between steps at defined points. The absence of real parallelism does not compromise the usefulness of the simulation — the bugs we care about are in the *sequencing of state transitions*, not in CPU-level parallelism.

#### Invasiveness assessment

This is the most invasive approach. Every subsystem that currently takes `Instant::now()` or calls `fdatasync` directly must be refactored to take a `&dyn Clock` or `&dyn WabFs` reference. Estimated scope:

| File | Changes needed |
|------|---------------|
| `worker.rs` | `Worker::run` takes `&C: Clock`; `recv_timeout` → `clock.recv_timeout` |
| `wab/mod.rs` | `flusher_thread` takes `&F: WabFs`; `ShardWriter` calls `fs.fsync()` |
| `wab/segment.rs` | `WabSegment::create`, `write_record`, `seal` delegate to `WabFs` |
| `wab/format.rs` | `unix_nanos_now()` → `clock.unix_nanos()` |
| `drain/mod.rs` | `drain_thread` takes `&C: Clock`; `thread::sleep` → `clock.sleep_blocking` |
| `wab/mod.rs` | `run_with_panic_supervision` sleep → `clock.sleep_blocking` |
| `main.rs` | `spawn_workers`, `wab::spawn`, `drain::spawn` → pass `Arc<dyn Clock>` + `Arc<dyn WabFs>` |

Rough line count: ~200–300 changed lines across 6–8 files, plus the new trait definitions and simulation implementations (~400–500 lines).

This is a meaningful refactor but not a rewrite. The most delicate part is `WabSegment` — it currently owns a real `File` handle. Under simulation the `File` would be replaced by an in-memory `Vec<u8>`, which requires either boxing or generic dispatch.

**Verdict:** High invasiveness, maximum DST coverage. The right long-term target.

---

### Approach C: Hybrid — turmoil for network + lightweight sim for blocking threads

[`turmoil`](https://github.com/tokio-rs/turmoil) is a library for simulating networks in tokio-based programs. It intercepts `tokio::net` and provides deterministic message delivery and latency injection.

For weir specifically, the TCP+mTLS listener (`socket/tcp.rs`, `socket/tls.rs`) is the relevant turmoil target. The Unix socket path does not involve the network layer at all.

**What turmoil adds over madsim:**

turmoil provides network-layer fault injection (message reorder, delay, drop, partition) that madsim does not. For weir's current architecture the main use case would be:
- Simulating a slow client whose partial frame stalls `handle_connection` (currently tested via raw socket in `partial_frame_does_not_corrupt_next_connection`)
- Testing the TCP+mTLS path for client authentication failures and cert-reload timing

**The blocking-thread problem remains.** turmoil only controls `tokio::net`; it does not address the WAB flusher threads or the worker threads.

**Hybrid proposal:** Use turmoil for the network/socket layer + the injectable traits from Approach B for the blocking threads. The async layer becomes deterministic via turmoil; the blocking layer becomes deterministic via SimClock + SimFs + SimExecutor.

**Verdict:** This is the most realistic path to full DST coverage. It builds on both turmoil (well-maintained, used in production at Tokio) and a custom trait abstraction layer. The downside is that it requires the full refactor from Approach B for the blocking threads — turmoil does not reduce that work.

---

## 4. Required refactors and invasiveness

### 4.1 The `flusher_thread` is the hardest target

`flusher_thread` in `wab/mod.rs` is a blocking function that:
- Holds a `ShardWriter` (which owns a real `File`)
- Calls `recv_timeout` on a real clock
- Calls `fdatasync` via `writer.fsync_current()`
- Updates `coalesce_hint` after each fsync with `Instant::now().elapsed()`

To make this deterministic, all four of those must be injectable. The `ShardWriter` abstraction is the deepest change — it currently wraps `WabSegment` which owns a `File`. Making the file backend injectable requires either:

**Option 1:** Generic `ShardWriter<F: WabFs>` — zero-cost at runtime, but propagates the generic parameter through the entire type hierarchy.

**Option 2:** `Box<dyn WabBackend>` inside `ShardWriter` — dynamic dispatch, adds one vtable lookup per write/fsync, likely negligible compared to actual I/O cost.

Option 2 is recommended for initial implementation: the performance cost is negligible in the WAB hot path (fsync dominates), and it avoids the viral generic parameter.

### 4.2 The crossbeam channel is not injectable — and that is acceptable

`crossbeam_channel::Receiver::recv_timeout` is the main "wait for work" primitive in the worker and flusher threads. Replacing it with an injectable version would require either:
- A custom `SimChannel` that integrates with `SimClock`'s tick management
- Or wrapping `recv_timeout` in a Clock method: `clock.recv_timeout(&rx, timeout)`

The second option is simpler and sufficient. The `Clock` trait already encapsulates the timeout logic; the channel itself remains a real `crossbeam_channel` in simulation (the `SimFs` runs on a single thread, so there is no real inter-thread contention to simulate anyway).

### 4.3 The tokio runtime in the drain thread

`drain/mod.rs:182` — the drain thread creates its own single-threaded tokio runtime (`Builder::new_current_thread()`) to drive async sink calls. This `block_on` runtime is a nested tokio runtime within a `std::thread`.

Under madsim or turmoil, this nested runtime is partially covered (turmoil can intercept `tokio::net` calls inside it). Under Approach B, the drain thread's behavior is controlled by the `SimExecutor` at the coarse level (the drain thread itself is a task in `SimExecutor`) and `SimClock` for the retry sleeps. The `block_on` calls on the sink become synchronous calls on a `SimSink` that returns immediately.

This is the right decomposition: the drain thread's correctness properties are about *when* it calls the sink and *what* it does with the result — not about the async internals of the sink itself. A `SimSink` that returns `Ok(all_committed)` or `Err(Transient)` or `Err(Permanent)` on a scripted schedule fully covers the drain state machine.

### 4.4 Summary of invasiveness by component

| Component | Change needed | Invasiveness |
|-----------|--------------|-------------|
| `worker.rs` | `recv_timeout` → clock injection | Low |
| `wab/mod.rs` | Clock injection in flusher + supervisor sleep | Medium |
| `wab/segment.rs` | File backend abstraction | Medium-High |
| `drain/mod.rs` | Clock injection for retries; SimSink in tests | Medium |
| `wab/format.rs` | `unix_nanos_now` → clock | Low |
| `main.rs` | Wire-up clock/fs/sink overrides for sim mode | Medium |
| `models.rs` | `enqueued_at: Instant` → clock-injected | Low |
| New: `sim/` module | `SimClock`, `SimFs`, `SimExecutor`, `SimSink` | New code |

Total: invasive but tractable. The refactor can be done incrementally, subsystem by subsystem, without breaking the existing test suite at any step.

---

## 5. Pragmatic incremental recommendation

Full DST (every nondeterminism source controlled) is the long-term goal, but it is not the right first step. Here is a pragmatic phased plan.

### Phase 4.0 — Deterministic unit-level simulation of the WAB flusher (2–3 weeks)

**Target:** Make `flusher_thread` alone testable in a single-threaded, deterministic simulation.

**Steps:**
1. Extract a `WabBackend` trait covering `create_segment`, `write_record`, `fsync_current`, `seal`. Implement `RealWabBackend` (wraps existing `ShardWriter` + `WabSegment`).
2. Add `SimWabBackend`: in-memory Vec-backed segments, no fsync latency, scriptable fsync faults (nth call fails with `io::Error`).
3. Extract `BlockingClock` trait: `now()`, `sleep_blocking()`, `recv_timeout_batch(rx, deadline)`. Implement `RealClock` (calls real OS functions) and `SimClock` (advances a `u64` tick counter, returns from recv_timeout immediately or after N ticks).
4. Refactor `flusher_thread` to accept `&dyn WabBackend` + `&dyn BlockingClock`.
5. Write a `#[cfg(test)]` simulation harness for the flusher: feed it scripted `Batch`es via a channel, inject fsync failures, assert ack outcomes and segment file state.

**What this buys immediately:**
- Every fsync fault scenario (EFBIG, ENOSPC, partial write after 3 records) can be injected deterministically.
- The panic supervisor's respawn behavior can be tested without `thread::sleep(10ms)` multiplied by 10 attempts.
- Batch deadline behavior can be tested without wall-clock waits.

**What it does NOT yet cover:** Worker scheduling, queue ordering, drain state machine, async accept loop.

---

### Phase 4.1 — Deterministic drain simulation (1–2 weeks)

**Target:** Make the drain state machine testable without real filesystem or real clock.

The drain is already partially testable in unit tests (`drain/mod.rs` tests use a `MockSink`). The gaps are:
- `thread::sleep(next_delay)` in `RetryingTransient` — tests use `base_retry_delay: 1ms` as a workaround, but this is still real wall-clock time.
- `Instant::now()` for `dead_letter_blocked_duration` — the gauge value is nondeterministic under load.
- The dead-letter dir rescan uses real filesystem calls.

**Steps:**
1. Extend `SimClock` from Phase 4.0 to cover the drain's `thread::sleep` and `Instant::now()` calls.
2. Add `SimFs` extension: directory listing, file creation/deletion for confirmed sidecars and dead-letter files.
3. Refactor `drain_thread` to accept `&dyn BlockingClock` + `&dyn DeadLetterFs`.
4. Write a deterministic drain integration test: multiple segments, scripted sink responses (transient → permanent → success), clock advances confirm that retry delays fire at exactly the right simulated tick.

---

### Phase 4.2 — Worker simulation + pipeline integration (3–4 weeks)

**Target:** A single-threaded simulation that runs the full pipeline: queue → worker → flusher → drain, all on one `SimExecutor` thread.

**Steps:**
1. Implement `SimExecutor`: a cooperative scheduler with a deterministic run queue.
2. Refactor `spawn_workers` to accept a `&dyn ThreadSpawner`; in simulation, the `SimExecutor`-backed spawner enqueues the worker loop as a task.
3. Refactor `wab::spawn` to use the same `SimExecutor`.
4. Write a pipeline simulation test: seed an RNG, generate N records, feed them through the simulated pipeline, assert all records appear in the simulated WAB segments in the correct order, with the correct ack outcomes.

**The threading-determinism tension at this stage:**

The `SimExecutor` runs all tasks cooperatively on a single OS thread. This means there is no real parallelism — which is exactly what we want for determinism. The tradeoff is that the simulation does not stress real shared-memory races (like the `coalesce_hint` AtomicU64 load/store ordering). Those require property-based testing with loom (`tokio-rs/loom`) which is a separate concern.

---

### Phase 4.3 — Async socket layer integration with turmoil (2–3 weeks)

**Target:** Connect the simulated pipeline from Phase 4.2 to a simulated socket layer via turmoil.

**Steps:**
1. Introduce `turmoil` as a dev-dependency.
2. Write a simulation test that runs the accept loop on a turmoil-simulated network, drives it with simulated producers, and asserts end-to-end record delivery through the Phase 4.2 pipeline.
3. Add network fault scenarios: slow clients, partial frames, connection resets during ack.

---

### Recommended starting point: Phase 4.0 only

The highest bang-for-buck is Phase 4.0. It directly addresses the hardest-to-reproduce bugs (fsync failures, partial writes, panic respawn timing) with the least refactor. Phases 4.1–4.3 build on it incrementally and can be prioritized based on which category of bugs is most painful.

**Estimated Phase 4.0 effort:** 2–3 weeks of focused refactor + test writing.

---

## 6. Open questions

### 6.1 Loom vs SimExecutor for shared-memory races

[`loom`](https://github.com/tokio-rs/loom) is a tool for systematically exploring all memory-ordering interleavings of a multi-threaded program. It would directly address the `coalesce_hint` AtomicU64 question (§2.6). However, loom requires rewriting the program to use `loom::sync::atomic` etc., which is more invasive than the trait abstraction approach. The question is whether the relaxed-atomic bugs in `coalesce_hint` are worth the loom integration cost, or whether they are low-risk enough (heuristic, not correctness-critical) to leave as-is.

**Tentative answer:** The EWMA hint is explicitly documented as a heuristic. A buggy value (0 or u64::MAX) is clamped at the worker call site (`clamp(COALESCE_MIN_US, COALESCE_MAX_US)`), so the blast radius is bounded. Loom integration is probably not warranted for this specific atomic. Open for discussion.

### 6.2 How to simulate `spawn_blocking` in the DST context

In production, `handle_connection` uses `task::spawn_blocking` to move `queue_tx.push_timeout` off the async runtime. Under simulation, the question is whether `spawn_blocking` should:

- **Option A:** Execute synchronously within the turmoil task (turn it into a direct call). This is simpler but loses the concurrency model — the async handler blocks until the push completes.
- **Option B:** Schedule it as a simulated blocking task with a deterministic delay. This preserves the concurrency model but adds complexity.

For initial Phase 4.3 work, Option A is sufficient — the correctness property under test is protocol-level (ack/nack sequencing), not backpressure dynamics.

### 6.3 Segment file naming under simulation

WAB segment names embed a monotonic counter (`segment_path(&shard_dir, counter)`). Under simulation, the counter advances as a side effect of `create_segment` calls. This is already deterministic if the SimExecutor runs tasks in a fixed order. No special handling needed, but worth confirming once Phase 4.0 is implemented.

### 6.4 `unix_nanos_now()` in segment headers

The segment header timestamp is used only for diagnostics. The simplest fix under simulation is to have `SimClock::unix_nanos()` return the simulated tick counter directly. This means simulation-mode segment files have timestamps that are not real calendar times, which is fine for all tests but would confuse forensic tooling that parses real WAB files. A `#[cfg(test)]` guard on the simulated timestamp value is sufficient.

### 6.5 How invasive is the refactor path in practice?

The trait abstraction approach (§3 Approach B) requires passing `Arc<dyn BlockingClock>` and `Arc<dyn WabBackend>` through `spawn_workers`, `wab::spawn`, and `drain::spawn`. In production these are always the `Real*` implementations. The main concern is that this adds visible complexity to `main.rs` that has nothing to do with the production use case.

**Mitigation:** Use a `SimMode` feature flag (`--features dst`) that compiles in the simulation types. Production builds compile out all simulation code. The `main.rs` change is minimal: under `#[cfg(feature = "dst")]` use sim implementations, otherwise use real ones.

### 6.6 Interaction with the fuzz harness

The existing fuzz targets (`fuzz/fuzz_targets/`) test trust-boundary parsers (`Envelope::decode`, `wab_confirmed`). DST tests a different property: pipeline behavior over sequences of valid inputs under adversarial scheduling. The two are complementary and do not interfere. The fuzz harness can continue to run independently.

### 6.7 What does "replay from seed" look like in practice?

For a DST failure to be replay-worthy, the seed must fully determine:
1. The scheduling order of the `SimExecutor` tasks
2. Any injected faults (which fsync call fails, what the fault is)
3. The input sequence (which records are pushed, in what order)

If all three are seeded from a single `u64`, a failing test prints something like:

```
DST FAILURE: seed=0xDEADBEEF1234ABCD
To reproduce: cargo test --features dst dst_pipeline -- --dst-seed=0xDEADBEEF1234ABCD
```

The `SimExecutor` uses the seed to initialize a `SmallRng` for task ordering decisions. The fault schedule is also derived from the seed (deterministically pick which calls fail from a seeded RNG).

This requires designing the `SimExecutor` and `SimFs` to use the seed from the beginning, not mid-run. A natural API is:

```rust
let sim = Sim::new(seed);
sim.run_pipeline(N_records, fault_schedule);
```

where `Sim` encapsulates `SimExecutor`, `SimClock`, `SimFs`, and `SimSink`.

---

## Summary

- **Why DST:** weir's hybrid async+blocking architecture creates scheduling-dependent bugs that real-wall-clock system tests cannot reliably reproduce. DST with a fixed seed makes every failure replayable.

- **Nondeterminism map:** The five main sources are thread scheduling (`spawn_workers`, `wab::spawn`, `drain::spawn`), tokio task ordering, `Instant::now()`/`recv_timeout` timing, real `fdatasync` latency and faults, and crossbeam channel interleaving under concurrent senders. All are mapped to specific code locations in §2.

- **The hard part is the blocking threads:** madsim covers only the async layer; the WAB flushers, workers, and drain run on `std::thread` and call real OS functions that madsim cannot intercept. The only way to get DST coverage of those threads is Approach B (injectable `Clock`/`WabBackend` traits) or Approach C (B + turmoil).

- **Recommended approach:** Approach C (injectable traits + turmoil), implemented incrementally. Phase 4.0 (flusher simulation) is the first and highest-value step, requiring ~200–300 lines of refactor plus a `SimWabBackend` and `SimClock` implementation.

- **The threads-determinism tension resolved:** The `SimExecutor` runs all blocking "threads" cooperatively on one OS thread. This eliminates OS scheduling nondeterminism while preserving the logical concurrency model. Real parallelism bugs (CPU memory ordering) are a separate concern addressed by loom if needed.

- **Estimated first milestone:** Phase 4.0 in 2–3 weeks delivers deterministic fault injection for the WAB flusher — the component most responsible for hard-to-reproduce data-loss scenarios.
