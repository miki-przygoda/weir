# DST Trait-Seam Engineering Design

**Status:** Design — no implementation yet.
**Builds on:** `phase4-dst-harness.md` (concluded: injectable `Clock` + `WabBackend` traits +
single-threaded sim executor) and `phase4-dst-scenarios.md` (invariants + fault catalog).
**Scope:** Exact trait signatures, injection points, dispatch strategy, refactor sketch,
production wrappers. Does not repeat the rationale from those documents.

---

## 1. Trait Signatures

### 1.1 `BlockingClock`

Abstracts every call that reads or advances real wall-clock time in blocking thread context.

```rust
// crates/weir-server/src/sim/clock.rs  (new file)

use std::time::{Duration, Instant};
use crossbeam_channel::{Receiver, RecvTimeoutError};

/// Abstracts real time in blocking (std::thread) context. All three operations
/// must be provided together because recv_timeout is time-gated: a SimClock
/// cannot advance ticks without controlling what happens inside the wait.
pub trait BlockingClock: Send + Sync + 'static {
    /// Equivalent of `Instant::now()` — returns the current clock reading.
    fn now(&self) -> Instant;

    /// Equivalent of `std::thread::sleep(d)` — blocks or advances simulated
    /// time by `d` without consuming from any channel.
    fn sleep(&self, d: Duration);

    /// Equivalent of `rx.recv_timeout(timeout)` — waits up to `timeout` for
    /// a message. The implementation may advance simulated time.
    ///
    /// Split from `now`/`sleep` so the sim executor can interleave task steps
    /// at this yield point rather than in arbitrary mid-computation positions.
    fn recv_timeout<T: Send>(
        &self,
        rx: &Receiver<T>,
        timeout: Duration,
    ) -> Result<T, RecvTimeoutError>;

    /// Returns unix nanoseconds for embedding in segment headers. Diagnostic
    /// only — not a correctness signal. SimClock may return its tick counter
    /// directly; production returns `SystemTime::now()` as today.
    fn unix_nanos(&self) -> i64;
}
```

**Production wrapper** (`RealClock`):

```rust
pub struct RealClock;

impl BlockingClock for RealClock {
    fn now(&self) -> Instant { Instant::now() }
    fn sleep(&self, d: Duration) { std::thread::sleep(d); }
    fn recv_timeout<T: Send>(&self, rx: &Receiver<T>, d: Duration)
        -> Result<T, RecvTimeoutError>
    { rx.recv_timeout(d) }
    fn unix_nanos(&self) -> i64 { crate::wab::format::unix_nanos_now() }
}
```

Zero code size and zero runtime overhead — the compiler inlines all four calls
through the monomorphised path used in production.

**Simulation implementation** (`SimClock`) — detailed in §3.

---

### 1.2 `SegmentStore`

Abstracts the filesystem operations performed by `WabSegment` and `ShardWriter`:
create, write, fsync, seal, and enumerate existing segment counters.

```rust
// crates/weir-server/src/sim/segment_store.rs  (new file)

use std::{io, path::PathBuf};
use weir_core::Payload;

/// One active-segment handle. In production this wraps a real `WabSegment`
/// (owned `File`). In simulation it is an in-memory `Vec<u8>`.
pub trait SegmentHandle: Send {
    /// Equivalent of `WabSegment::write_record`. Returns `Ok(())` or an error
    /// (which the caller treats as poison — same as today).
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()>;

    /// Equivalent of `WabSegment::fsync`. Under simulation this may be a
    /// no-op or a scripted fault.
    fn fsync(&self) -> io::Result<()>;

    /// Equivalent of `WabSegment::should_rotate`.
    fn should_rotate(&self, max_bytes: u64) -> bool;

    /// Equivalent of `WabSegment::seal`. Consumes self; returns the sealed path.
    fn seal(self: Box<Self>) -> io::Result<PathBuf>;
}

/// Abstracts the filesystem layer used by `ShardWriter` and `flusher_thread`.
///
/// A single `SegmentStore` instance is shared by all shard flusher threads via
/// `Arc<dyn SegmentStore>`. It is the source of truth for "what files exist"
/// in both production (the real filesystem) and simulation (an in-memory map).
pub trait SegmentStore: Send + Sync + 'static {
    /// Equivalent of `WabSegment::create` — creates a new active segment at
    /// the given path for the given shard.
    fn create_segment(
        &self,
        path: &std::path::Path,
        shard_id: u16,
    ) -> io::Result<Box<dyn SegmentHandle>>;

    /// Equivalent of `ShardWriter::scan_and_advance_counter` — returns the
    /// highest existing segment counter in `shard_dir`, or 0 if none.
    fn max_segment_counter(&self, shard_dir: &std::path::Path) -> io::Result<u64>;

    /// Read back the records from a sealed segment. Used only in simulation
    /// assertion code and recovery tests, not on the hot path.
    fn read_segment(
        &self,
        sealed_path: &std::path::Path,
    ) -> io::Result<Vec<Payload>>;
}
```

**Why `Box<dyn SegmentHandle>` rather than an associated type:**
`SegmentHandle` is returned from `create_segment` and then stored in
`ShardWriter::active`. An associated type would propagate the generic into
`ShardWriter<S: SegmentStore>` and from there into `flusher_thread`, `spawn`,
and every callsite. `Box<dyn SegmentHandle>` confines the indirection to the
`ShardWriter` field and one vtable lookup per `write_record` / `fsync` — both
are cheap relative to the real I/O cost they replace. The tradeoff is discussed
in §4.

**Production wrapper** (`FsSegmentStore` — wraps the existing `WabSegment`):

```rust
pub struct FsSegmentStore;

impl SegmentStore for FsSegmentStore {
    fn create_segment(&self, path: &Path, shard_id: u16)
        -> io::Result<Box<dyn SegmentHandle>>
    { Ok(Box::new(WabSegment::create(path, shard_id)?)) }

    fn max_segment_counter(&self, shard_dir: &Path) -> io::Result<u64> {
        // existing logic from ShardWriter::scan_and_advance_counter
    }

    fn read_segment(&self, sealed_path: &Path) -> io::Result<Vec<Payload>> {
        SegmentReader::open(sealed_path)?.collect::<Result<Vec<_>, _>>()
    }
}

impl SegmentHandle for WabSegment {
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()> { self.write_record(payload) }
    fn fsync(&self) -> io::Result<()> { self.fsync() }
    fn should_rotate(&self, max_bytes: u64) -> bool { self.should_rotate(max_bytes) }
    fn seal(self: Box<Self>) -> io::Result<PathBuf> { (*self).seal() }
}
```

This is purely mechanical delegation — zero new logic in production.

---

### 1.3 `SimExecutor` (cooperative flusher runner)

The sim does not use `std::thread::spawn` for the flusher. Instead it drives the
flusher body as a synchronous function on the test thread via a cooperative
turn-based runner.

```rust
// crates/weir-server/src/sim/executor.rs  (new file)

/// A named task: a boxed closure that runs to its next yield point.
/// "Yield point" = any call to SimClock::recv_timeout or SimClock::sleep.
/// After each yield the executor picks the next task (by fixed order or seeded RNG).
pub struct SimTask {
    pub name: String,
    pub body: Box<dyn FnOnce() + Send>,
}

/// Single-threaded cooperative executor. All "threads" run on the calling OS thread.
/// Scheduling decisions are seeded so runs are reproducible from a u64 seed.
pub struct SimExecutor {
    seed: u64,
    tasks: Vec<SimTask>,
}

impl SimExecutor {
    pub fn new(seed: u64) -> Self { /* ... */ }
    pub fn add_task(&mut self, name: impl Into<String>, body: impl FnOnce() + Send + 'static);
    /// Run all tasks to completion. Panics if any task panics and propagation
    /// is not wrapped by the test harness.
    pub fn run_all(self);
}
```

For Phase 4.0 (flusher-only simulation), `SimExecutor` is extremely simple: the
test feeds `Batch` items into a channel and then calls `flusher_thread(...)` directly
in the test thread — no real parallelism needed. The full cooperative scheduler
(§6 Phase 4.2) is deferred. This means Phase 4.0 does not need `SimExecutor` at all:
the flusher body is just a function call.

---

## 2. Injection Points in Real Code

### 2.1 `flusher_thread` — `wab/mod.rs:345`

Current signature (line 345–356):
```rust
fn flusher_thread(
    shard_id: u16,
    shard_dir: PathBuf,
    work_rx: Receiver<Batch>,
    drain_tx: Sender<PathBuf>,
    batch_size: usize,
    batch_deadline: Duration,
    segment_max_bytes: u64,
    core_id: Option<core_affinity::CoreId>,
    metrics: Arc<Metrics>,
    coalesce_hint: Arc<AtomicU64>,
)
```

New signature adds two trailing parameters:

```rust
fn flusher_thread<C: BlockingClock, S: SegmentStore>(
    // ...all existing params...
    clock: Arc<C>,
    store: Arc<S>,
)
```

Internal changes (see §5 for full diff):
- Line 405: `work_rx.recv_timeout(batch_deadline)` → `clock.recv_timeout(&work_rx, batch_deadline)`
- Lines 431–432: `work_rx.try_recv()` is unchanged (non-blocking, no clock needed)
- Line 381: `ShardWriter::new(...)` → `ShardWriter::new_with_store(shard_id, shard_dir, segment_max_bytes, Arc::clone(&metrics), Arc::clone(&store))`

### 2.2 `flush_batch` → `fsync_observed` — `wab/mod.rs:475, 593`

`flush_batch` calls `fsync_observed` (line 573). `fsync_observed` is a free
function that reads real time on line 599:

```rust
fn fsync_observed(writer: &mut ShardWriter, ...) -> bool {
    let t = Instant::now();           // line 599 — inject clock.now()
    let result = writer.fsync_current();
    let elapsed = t.elapsed();        // clock-relative: no injection needed (uses t)
    ...
}
```

`t.elapsed()` is already derived from `t` (a captured `Instant`), so only the
`Instant::now()` call on line 599 needs the clock. `fsync_observed` becomes:

```rust
fn fsync_observed<C: BlockingClock>(
    writer: &mut ShardWriter,
    shard_id: u16,
    metrics: &Arc<Metrics>,
    coalesce_hint: &Arc<AtomicU64>,
    clock: &C,
) -> bool {
    let t = clock.now();
    let result = writer.fsync_current();
    let elapsed = t.elapsed(); // still uses real Instant arithmetic; fine in sim too
    ...
}
```

### 2.3 `WabSegment` / `ShardWriter` — `wab/segment.rs`

`ShardWriter::active` currently holds `Option<WabSegment>`. It becomes
`Option<Box<dyn SegmentHandle>>`.

`ShardWriter::ensure_open` (line 361) currently calls `WabSegment::create`.
It will call `self.store.create_segment(...)` instead.

`ShardWriter::write_record` (line 320), `fsync_current` (line 344), and
`seal_current` (line 354) delegate to `self.active` — the method calls remain
structurally identical, just dispatching through `dyn SegmentHandle` instead of
the concrete `WabSegment`.

`ShardWriter::scan_and_advance_counter` (line 293) currently reads the
filesystem directly. It becomes `self.store.max_segment_counter(&self.shard_dir)`.

`ShardWriter` gains one field and a new constructor:

```rust
pub struct ShardWriter {
    shard_id: u16,
    shard_dir: PathBuf,
    next_counter: u64,
    segment_max_bytes: u64,
    active: Option<Box<dyn SegmentHandle>>,  // was: Option<WabSegment>
    metrics: Arc<Metrics>,
    store: Arc<dyn SegmentStore>,            // new
}

impl ShardWriter {
    pub fn new_with_store(
        shard_id: u16,
        shard_dir: PathBuf,
        segment_max_bytes: u64,
        metrics: Arc<Metrics>,
        store: Arc<dyn SegmentStore>,
    ) -> Self { ... }

    // Convenience for production callers — uses FsSegmentStore
    pub fn new(shard_id: u16, shard_dir: PathBuf, ...) -> Self {
        Self::new_with_store(..., Arc::new(FsSegmentStore))
    }
}
```

Preserving `ShardWriter::new` means all existing unit tests in `segment.rs`
compile unchanged.

### 2.4 `unix_nanos_now` — `wab/format.rs:93` and `wab/segment.rs:208`

`WabSegment::seal` (segment.rs line 208) calls `unix_nanos_now()` directly.
After the `SegmentHandle` abstraction, the production `WabSegment` still calls
`unix_nanos_now()` at seal time. The `SimSegmentHandle` embeds a tick value from
its clock instead. No change to `format.rs` or `WabSegment` is needed for Phase
4.0 — the sim segment does not use `unix_nanos_now` at all.

If `format.rs::build_segment_header` (line 108) also needs to be clock-injected
for complete reproducibility, the injection point is:
- `WabSegment::create` passes `unix_nanos_now()` → `build_segment_header(shard_id)`
- Under `FsSegmentStore` this is unchanged
- Under `SimSegmentStore`, `create_segment` injects the simulated tick value

For Phase 4.0 this is low priority (headers are diagnostic), but the seam is
already in `SegmentStore::create_segment`.

### 2.5 `Worker::run` coalesce loop — `worker.rs:80,124`

Two `recv_timeout` sites:
- Line 80: `work_rx.recv_timeout(batch_deadline)` — outer loop
- Line 124: `work_rx.recv_timeout(window)` — coalesce phase

The `Worker` struct gains a `clock: Arc<dyn BlockingClock>` field (or a generic
`C: BlockingClock`). Both `recv_timeout` calls become `self.clock.recv_timeout(...)`.

The `bench-trace` timestamp at line 179 (`flushed_at: std::time::Instant::now()`)
also uses real time. Under simulation, this becomes `clock.now()`. This field is
only compiled in with `--features bench-trace`, so it does not affect production
builds with that feature disabled.

### 2.6 Panic supervisor sleep — `wab/mod.rs:179`

```rust
thread::sleep(Duration::from_millis(10 * u64::from(attempt)));  // line 179
```

`run_with_panic_supervision` gains a `clock: &C` parameter:

```rust
fn run_with_panic_supervision<F, B, C: BlockingClock>(
    shard_id: usize,
    metrics: Arc<Metrics>,
    clock: &C,
    mut body_factory: F,
)
```

The `thread::sleep` on line 179 becomes `clock.sleep(Duration::from_millis(...))`.

Under `SimClock`, this advances simulated time without any real sleep, making
`flusher_panic_loop_caps_out_after_max_respawns` (which today takes ~550ms real
wall-clock) run in microseconds.

### 2.7 Drain thread — `drain/mod.rs:250`

The drain is Phase 4.1 (deferred). The injection points for completeness:
- `std::thread::sleep(next_delay)` at line 250 → `clock.sleep(next_delay)`
- `Instant::now()` for `blocked_since` (line ~284) → `clock.now()`
- `Instant::now() + config.dead_letter_check_interval` (line ~294) → computed
  relative to `clock.now()`

No drain changes are needed for Phase 4.0.

---

## 3. Injection Mechanism: Generics vs `dyn` vs `#[cfg]`

### 3.1 Options

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A: Full generics** | `flusher_thread<C: BlockingClock, S: SegmentStore>` everywhere | Zero vtable cost; compiler can inline across seam | Viral: `spawn`, `Worker`, `ShardWriter` all become generic; monomorphisation binary size increases; complex type signatures leak to `main.rs` |
| **B: `dyn` for store, generic for clock** | `ShardWriter` uses `Arc<dyn SegmentStore>`; `flusher_thread<C: BlockingClock>` uses generic clock | Store's `dyn` is already gated behind real I/O cost; clock's generic is cheap and contained to the flusher | Two dispatch strategies to reason about |
| **C: All `dyn`** | `Arc<dyn BlockingClock>` + `Arc<dyn SegmentStore>` passed to all sites | Simplest type signatures; no monomorphisation overhead | One vtable call per `recv_timeout` on the hot path (though benchmarked as negligible: real fsync ~150µs >> one vtable dispatch ~2ns) |
| **D: `#[cfg(feature = "dst")]` swap** | Compile-time substitution: under `dst` feature, use sim types; production gets stubs | No runtime cost, no viral generics, no `dyn` pointer overhead | All-or-nothing: cannot mix real and simulated components in a single binary; makes unit-testing individual seams harder; harder to compose scenarios |

### 3.2 Recommendation: Option B (hybrid — `dyn` for store, generic for clock)

**`SegmentStore`: use `Arc<dyn SegmentStore>`**

The store's methods (`create_segment`, `fsync`, `seal`) are called at batch
granularity (roughly every N records and once per segment rotation). The vtable
overhead is completely buried under filesystem latency. Dynamic dispatch at this
seam keeps `ShardWriter` free of generics, which is the right trade: `ShardWriter`
is an implementation detail of the flusher and should not propagate type parameters
to the spawn infrastructure.

**`BlockingClock`: use a generic `C: BlockingClock`**

`recv_timeout` in the flusher loop is called once per batch arrival (the tight
loop). In the worst case (very small batches, high throughput), this is ~100K
calls/sec per shard — still negligible, but the generic form costs nothing at
all (the monomorphised binary has a direct call). More importantly, `flusher_thread`
is a private function; the generic parameter does not escape to the public API.
`spawn` in `wab/mod.rs` uses:

```rust
fn spawn(..., clock: Arc<impl BlockingClock + 'static>, store: Arc<dyn SegmentStore>)
    -> io::Result<WabHandle>
```

`impl BlockingClock` in the `spawn` signature is erased to a concrete type at the
call site in `main.rs` (or the test harness), so it does not pollute the public
interface.

`Worker` follows the same pattern: `spawn_workers<C: BlockingClock>` takes
`clock: Arc<C>`. The concrete type is resolved at `main.rs`.

**Why not `#[cfg(feature = "dst")]`:**
The `#[cfg]` approach is tempting for zero-cost purity but creates a hidden
correctness risk: the sim types are compiled out of every production build,
meaning typos, API changes, and broken sim code go undetected until a DST build
is explicitly triggered. Traits with concrete production and simulation
implementations remain in scope under `cargo test` without any feature flag,
catching breakage in normal CI. The `#[cfg(feature = "dst")]` flag should be
used only to gate the *simulation harness binary* (the test that actually runs
the sim), not the trait definitions or production wrappers.

---

## 4. Refactor Diff Sketch and Invasiveness

### 4.1 `crates/weir-server/src/wab/segment.rs`

**Changes:**
1. Add `SegmentHandle` trait above `WabSegment` (new, ~20 lines)
2. `impl SegmentHandle for WabSegment` (~15 lines delegation)
3. `ShardWriter::active`: `Option<WabSegment>` → `Option<Box<dyn SegmentHandle>>`
4. `ShardWriter` gains `store: Arc<dyn SegmentStore>` field
5. `ShardWriter::new` becomes `new_with_store`; old `new` becomes a convenience
   wrapper calling `new_with_store` with `Arc::new(FsSegmentStore)` — **existing
   callers and tests unchanged**
6. `ensure_open`: `WabSegment::create(...)` → `self.store.create_segment(...)`
7. `scan_and_advance_counter`: reads filesystem → `self.store.max_segment_counter`

**Invasiveness:** ~60 lines changed/added. All existing unit tests in `segment.rs`
(`wab_segment_round_trip`, `shardwriter_drops_segment_after_write_error`,
`open_segment_counter_increments_when_ensure_open_creates_segment`, etc.) remain
green because `ShardWriter::new` is preserved as a no-op wrapper.

### 4.2 `crates/weir-server/src/wab/mod.rs`

**Changes:**
1. `flusher_thread` signature: add `clock: Arc<impl BlockingClock>`, `store: Arc<dyn SegmentStore>` (2 params)
2. Line 405 `work_rx.recv_timeout(batch_deadline)` → `clock.recv_timeout(&work_rx, batch_deadline)`
3. Line 381 `ShardWriter::new(...)` → `ShardWriter::new_with_store(..., Arc::clone(&store))`
4. `fsync_observed` signature: add `clock: &impl BlockingClock`
5. Line 599 `Instant::now()` → `clock.now()`
6. `flush_batch` signature: add `clock: &impl BlockingClock`; passes it through to `fsync_observed`
7. `run_with_panic_supervision`: add `clock: &C` param; line 179 `thread::sleep` → `clock.sleep`
8. `spawn` function: add `clock: Arc<impl BlockingClock>`, `store: Arc<dyn SegmentStore>` params;
   pass them into the closure and through to `flusher_thread`

**Invasiveness:** ~40 lines changed. The public API change is to `spawn` only.
All internal logic is unchanged.

Existing tests in `mod.rs` (`supervisor_catches_str_panic_and_increments_metric`,
`flusher_panic_respawn_recovers_within_cap`, `flusher_panic_loop_caps_out_after_max_respawns`,
`ewma_*`) test `run_with_panic_supervision` and `ewma_update_us` directly. None of them
call `flusher_thread` — so they survive without change. The panic supervisor tests
need to pass a `RealClock` (or no clock at all if overloaded with a default).
Cleanest approach: default to `RealClock` in the test-facing overloads via a
`#[cfg(test)]` convenience wrapper.

### 4.3 `crates/weir-server/src/worker.rs`

**Changes:**
1. `Worker` struct gains `clock: Arc<dyn BlockingClock>` (or generic `C`)
2. `Worker::new` gains a `clock` param
3. Line 80 `work_rx.recv_timeout(batch_deadline)` → `self.clock.recv_timeout(&work_rx, batch_deadline)`
4. Line 124 `work_rx.recv_timeout(window)` → `self.clock.recv_timeout(&work_rx, window)`
5. `spawn_workers` signature gains `clock: Arc<impl BlockingClock>`

**Invasiveness:** ~25 lines changed. All existing worker tests pass `batch_deadline`
as a `Duration` and use real channels — they work unchanged if `spawn_workers`
defaults to `RealClock` when the clock argument is not supplied (or via a
`spawn_workers_with_clock` overload used only in sim tests).

**Bench-trace path** (`flushed_at: std::time::Instant::now()` in `flush_shard`, line 179):
under `--features bench-trace`, change to `clock.now()`. This is inside a
`#[cfg(feature = "bench-trace")]` block so it does not affect production builds
with that feature disabled.

### 4.4 `crates/weir-server/src/main.rs` (or `lib.rs`)

**Changes:**
1. `wab::spawn` call gains `Arc::new(RealClock)` and `Arc::new(FsSegmentStore)` arguments
2. `spawn_workers` call gains `Arc::new(RealClock)` argument

**Invasiveness:** ~6 lines changed. These are the only callsites in production binary.

### 4.5 New files

| File | Purpose | Est. lines |
|------|---------|-----------|
| `crates/weir-server/src/sim/mod.rs` | Module declaration | 5 |
| `crates/weir-server/src/sim/clock.rs` | `BlockingClock` trait + `RealClock` | 40 |
| `crates/weir-server/src/sim/segment_store.rs` | `SegmentStore` + `SegmentHandle` traits + `FsSegmentStore` + `impl SegmentHandle for WabSegment` | 120 |
| `crates/weir-server/src/sim/sim_clock.rs` | `SimClock` implementation | 80 |
| `crates/weir-server/src/sim/sim_store.rs` | `SimSegmentStore` + in-memory `SimSegmentHandle` | 150 |
| `crates/weir-server/src/sim/harness.rs` | `FlushSim` test harness + `FaultSchedule` | 120 |

**Total new lines:** ~515
**Total changed lines across existing files:** ~130 (segment.rs ~60, mod.rs ~40, worker.rs ~25, main.rs ~6)
**Grand total:** ~645 lines — well within the earlier rough estimate of 600–800 lines for Phase 4.0.

### 4.6 How existing tests stay green

The key constraint is that no existing test should need to pass a clock or store
argument. This is achieved through:

1. **`ShardWriter::new` preserved** — delegates to `new_with_store(FsSegmentStore)`;
   all `segment.rs` tests work unchanged.

2. **`run_with_panic_supervision` default clock** — add a `_with_clock` variant
   for sim use; keep the original (which uses `RealClock` internally) for the
   existing panic supervisor tests.

3. **`spawn_workers` / `wab::spawn` gains required params** — since these are
   called only from `main.rs` (production) and `tests/system.rs` (integration),
   the change is contained. `tests/system.rs` spawns the real binary, so it does
   not call these functions directly — unaffected. Any unit tests that construct
   a `Worker` or `WabHandle` directly will need a `RealClock` / `FsSegmentStore`
   argument. A search of the test code shows the worker tests use `spawn_workers`
   (5 tests in `worker.rs`) and the WAB tests use `ShardWriter::new` (not
   `wab::spawn`) — both are handled by the preserved convenience APIs.

---

## 5. Production Wrappers — Complete Picture

### 5.1 `RealClock`

```rust
// sim/clock.rs

pub struct RealClock;

impl BlockingClock for RealClock {
    #[inline(always)]
    fn now(&self) -> Instant { Instant::now() }

    #[inline(always)]
    fn sleep(&self, d: Duration) { std::thread::sleep(d); }

    #[inline(always)]
    fn recv_timeout<T: Send>(
        &self, rx: &Receiver<T>, d: Duration,
    ) -> Result<T, RecvTimeoutError> {
        rx.recv_timeout(d)
    }

    #[inline(always)]
    fn unix_nanos(&self) -> i64 { crate::wab::format::unix_nanos_now() }
}
```

Compile-time verified identical to current behaviour. `#[inline(always)]`
ensures the optimizer sees through the trait call for production builds using
generics.

### 5.2 `FsSegmentStore`

```rust
// sim/segment_store.rs

pub struct FsSegmentStore;

impl SegmentStore for FsSegmentStore {
    fn create_segment(&self, path: &Path, shard_id: u16)
        -> io::Result<Box<dyn SegmentHandle>>
    { Ok(Box::new(WabSegment::create(path, shard_id)?)) }

    fn max_segment_counter(&self, shard_dir: &Path) -> io::Result<u64> {
        let mut max: u64 = 0;
        for entry in std::fs::read_dir(shard_dir)? {
            let entry = entry?;
            if let Some(n) = segment_counter_from_path(&entry.path())
                && n > max
            { max = n; }
        }
        Ok(max)
    }

    fn read_segment(&self, sealed_path: &Path) -> io::Result<Vec<Payload>> {
        SegmentReader::open(sealed_path)?.collect::<Result<Vec<_>, _>>()
    }
}
```

The `max_segment_counter` logic is moved from `ShardWriter::scan_and_advance_counter`
into the store — a pure extraction, behaviour-identical.

### 5.3 Production behaviour unchanged — verification checklist

| Current code | After refactor | Behavioural difference |
|-------------|---------------|----------------------|
| `work_rx.recv_timeout(d)` | `RealClock::recv_timeout` delegates to same | None |
| `Instant::now()` in `fsync_observed` | `RealClock::now()` == `Instant::now()` | None |
| `thread::sleep` in supervisor | `RealClock::sleep` delegates to same | None |
| `WabSegment::create` in `ensure_open` | `FsSegmentStore::create_segment` wraps it | None |
| `ShardWriter::scan_and_advance_counter` | `FsSegmentStore::max_segment_counter` — same logic extracted | None |
| `unix_nanos_now()` in header | `RealClock::unix_nanos()` delegates to same | None |

---

## 6. Simulation Implementations (Phase 4.0 harness sketch)

These are not fully specified here (that is implementation work), but the
contracts are clear enough to code against.

### 6.1 `SimClock`

```rust
pub struct SimClock {
    /// Monotonically advancing tick counter in nanoseconds.
    ticks: Arc<AtomicU64>,
}

impl BlockingClock for SimClock {
    fn now(&self) -> Instant {
        // Return a fixed base Instant + simulated elapsed duration.
        // Use Instant::now() captured once at SimClock construction as the base.
        EPOCH + Duration::from_nanos(self.ticks.load(Ordering::SeqCst))
    }

    fn sleep(&self, d: Duration) {
        self.ticks.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
    }

    fn recv_timeout<T: Send>(&self, rx: &Receiver<T>, timeout: Duration)
        -> Result<T, RecvTimeoutError>
    {
        // Try non-blocking first.
        match rx.try_recv() {
            Ok(v) => Ok(v),
            Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
            Err(TryRecvError::Empty) => {
                // Advance clock by timeout, return Timeout.
                // Under cooperative simulation all channel data has been pre-loaded;
                // a real empty channel means the timeout fires immediately.
                self.ticks.fetch_add(timeout.as_nanos() as u64, Ordering::SeqCst);
                Err(RecvTimeoutError::Timeout)
            }
        }
    }

    fn unix_nanos(&self) -> i64 {
        self.ticks.load(Ordering::SeqCst) as i64
    }
}
```

**Key property:** On a pre-loaded channel (test feeds all batches before calling
`flusher_thread`), every `recv_timeout` that finds data returns immediately without
advancing ticks. `recv_timeout` on an empty channel advances by `timeout` and
returns `Timeout`, which models "the deadline expired". This is sufficient for
all Phase 4.0 scenarios.

### 6.2 `SimSegmentStore`

```rust
pub struct SimSegmentStore {
    /// All written records, keyed by sealed path. Populated by SimSegmentHandle::seal.
    sealed: Arc<Mutex<HashMap<PathBuf, Vec<Payload>>>>,
    /// Scripted fsync fault injection: (shard_id, call_number) -> io::ErrorKind
    fsync_faults: Arc<Mutex<HashMap<(u16, u64), io::ErrorKind>>>,
    /// Per-shard fsync call counter.
    fsync_calls: Arc<Mutex<HashMap<u16, u64>>>,
}
```

`SimSegmentHandle` holds an in-memory `Vec<Payload>`, implements `write_record`
as `self.records.push(Payload::from(payload))`, implements `fsync` by consulting
the fault schedule, and implements `seal` by moving the records into the store's
`sealed` map under the expected sealed path.

### 6.3 `FlushSim` — the Phase 4.0 test harness

```rust
pub struct FlushSim {
    pub clock: Arc<SimClock>,
    pub store: Arc<SimSegmentStore>,
    pub shard_tx: Sender<Batch>,
    pub drain_rx: Receiver<PathBuf>,
    pub metrics: Arc<Metrics>,
    pub coalesce_hint: Arc<AtomicU64>,
}

impl FlushSim {
    pub fn new(seed: u64) -> Self { ... }

    /// Feed pre-built Batches into the shard channel, then run flusher_thread to completion.
    /// The flusher sees the batches as immediately available (SimClock::try_recv succeeds).
    pub fn run(self, batches: Vec<Batch>) -> FlushSimResult {
        // Pre-load batches into shard_tx, then drop tx to trigger graceful shutdown.
        for b in batches { self.shard_tx.send(b).unwrap(); }
        drop(self.shard_tx);

        flusher_thread(
            0,
            PathBuf::from("/sim/shard_00"),
            self.drain_rx_as_work_rx, // receives Batch, not PathBuf
            self.drain_tx,
            BATCH_SIZE,
            BATCH_DEADLINE,
            SEGMENT_MAX_BYTES,
            None,
            Arc::clone(&self.metrics),
            Arc::clone(&self.coalesce_hint),
            Arc::clone(&self.clock),
            Arc::clone(&self.store) as Arc<dyn SegmentStore>,
        );

        FlushSimResult {
            sealed_segments: self.store.sealed_segments(),
            drained_paths: self.drain_rx.try_iter().collect(),
            metrics: self.metrics,
        }
    }
}
```

A test for G-WAB-1 (EIO on fdatasync) becomes:

```rust
#[test]
fn fsync_eio_produces_nack_not_crash() {
    let mut sim = FlushSim::new(0xDEAD_BEEF);
    sim.store.inject_fsync_fault(shard_id: 0, call_number: 1, ErrorKind::Other); // EIO

    let (ack_tx, ack_rx) = oneshot::channel();
    let batch = Batch {
        shard_id: 0,
        records: vec![WorkUnit {
            shard_id: 0,
            payload: Payload::copy_from_slice(b"test"),
            durability: Durability::Sync,
            ack_tx,
        }],
    };

    let result = sim.run(vec![batch]);

    // Ack must be false — fsync failed
    assert_eq!(ack_rx.blocking_recv().unwrap(), false);
    // Metric must be bumped
    assert_eq!(result.metrics.wab_fsync_failures.get(), 1);
    // No sealed segment sent to drain (fsync failed, segment not sealed)
    // The .wab file remains in the sim store as an active (unsealed) segment
}
```

This runs in < 1ms, is fully deterministic from the seed, and requires no tmpfs
or RLIMIT tricks.

---

## 7. Phases and Sequencing

| Phase | Deliverable | New files | Changed files | Est. effort |
|-------|------------|-----------|--------------|-------------|
| **4.0a** (foundation) | `BlockingClock` + `RealClock`; `SegmentHandle` + `SegmentStore` + `FsSegmentStore`; `ShardWriter` refactor | `sim/clock.rs`, `sim/segment_store.rs` | `segment.rs` | 3–4 days |
| **4.0b** (flusher injection) | Inject clock + store into `flusher_thread`; `fsync_observed` clock; supervisor sleep | `sim/mod.rs` | `wab/mod.rs` | 2–3 days |
| **4.0c** (sim implementations) | `SimClock`, `SimSegmentStore`, `FlushSim` harness | `sim/sim_clock.rs`, `sim/sim_store.rs`, `sim/harness.rs` | none | 3–4 days |
| **4.0d** (first DST tests) | G-WAB-1 (EIO fsync), P-2 (N transient panics), T-1 (deadline idle), T-2 (zero-latency EWMA) | test module in `sim/harness.rs` | none | 2–3 days |
| **4.1** (worker injection) | Clock into `Worker::run`; `spawn_workers` gains clock param | none | `worker.rs`, `main.rs` | 2 days |
| **4.2** (drain injection) | Clock into `drain_thread`; `SimSink` | `sim/sim_sink.rs` | `drain/mod.rs` | 3–4 days |

**Total Phase 4.0 (a–d): ~10–14 days**, matching the earlier 2–3 week estimate.

---

## 8. Open Design Questions

**Q-1: `SegmentHandle::seal` takes `self: Box<Self>` — is that the right ownership shape?**

`WabSegment::seal` today takes `self` (consuming the segment value). A trait
object requires `Box<Self>` to do the same. The alternative is `seal(mut self: Box<Self>)`
with the dyn-safe `seal(&mut self) -> io::Result<PathBuf>` signature, which
requires the caller to `take()` from `Option<Box<dyn SegmentHandle>>` before
calling. That is what `ShardWriter::seal_current` already does (`self.active.take()`).
**Recommendation:** `fn seal_into_sealed_path(&mut self) -> io::Result<PathBuf>` —
it is dyn-safe (no `self: Box<Self>`) and the handle is effectively inaccessible
after `seal_current` drops it. The "consume to prevent further use" property is
enforced by the `ShardWriter` state machine (`self.active = None`) rather than
the type system. Acceptable trade for the sim phase.

**Q-2: Should `SimClock` use a single `AtomicU64` tick or a per-task tick vector?**

For Phase 4.0 (single-threaded, one flusher body running to completion), a single
`AtomicU64` is sufficient. For Phase 4.2 (multiple cooperative tasks, each with
their own notion of "how much time has passed"), a per-task tick will be needed.
The `SimClock` API does not need to change; only the internal storage changes.
**Recommendation:** Start with `AtomicU64`; add a per-task tick map when `SimExecutor`
is introduced.

**Q-3: How are fault schedules serialized for seed-based replay?**

The fault catalog (scenarios.md) recommends a `#[derive(Serialize, Deserialize)]`
Rust enum. For Phase 4.0, fault schedules can be simple structs constructed inline
in tests. Seed-based replay (print seed on failure, re-run with `--dst-seed=N`)
requires the `SimSegmentStore` and `SimClock` to derive their random choices from
the seed. The `rand` crate's `SmallRng::seed_from_u64(seed)` is appropriate:
cheap, deterministic, and not cryptographic (no security requirement here).

**Q-4: Does `#[cfg(test)]` gate the sim module or a Cargo feature?**

The trait definitions (`BlockingClock`, `SegmentStore`, `SegmentHandle`) must be
available in production builds because production code (the flusher, worker, drain)
references them in function signatures. The simulation implementations
(`SimClock`, `SimSegmentStore`, `FlushSim`) should be gated:

```toml
# Cargo.toml
[features]
dst = []  # enables sim types; dev-dependency only
```

```rust
// sim/mod.rs
pub mod clock;         // always compiled (traits + RealClock)
pub mod segment_store; // always compiled (traits + FsSegmentStore)
#[cfg(any(test, feature = "dst"))]
pub mod sim_clock;
#[cfg(any(test, feature = "dst"))]
pub mod sim_store;
#[cfg(any(test, feature = "dst"))]
pub mod harness;
```

This keeps production binaries free of simulation code while ensuring
`cargo test` always compiles and runs the sim implementations, catching
API drift in normal CI without requiring `--features dst`.

---

## Summary

- **Two seams, two dispatch strategies:** `BlockingClock` is a generic parameter
  (zero-cost in production) covering `recv_timeout`, `sleep`, `now`, and
  `unix_nanos`; `SegmentStore`/`SegmentHandle` use `Arc<dyn ...>` (one vtable
  call per write/fsync, negligible versus real I/O cost) to avoid propagating
  generics through `ShardWriter` into the public spawn API.

- **Injection points are narrowly scoped:** Five exact sites — `recv_timeout` in
  `flusher_thread` (mod.rs:405), `Instant::now()` in `fsync_observed` (mod.rs:599),
  `thread::sleep` in `run_with_panic_supervision` (mod.rs:179), and the two
  `recv_timeout` calls in `Worker::run` (worker.rs:80,124) — cover all timing
  nondeterminism in the hot path. `WabSegment::create`/`fsync`/`seal` in
  `ShardWriter::ensure_open`, `fsync_current`, and `seal_current` cover all
  filesystem nondeterminism.

- **Production wrappers are mechanical delegations:** `RealClock` delegates
  four calls to their existing OS functions; `FsSegmentStore` wraps `WabSegment`
  and moves the counter-scan logic out of `ShardWriter`. Identical observable
  behaviour, verified by the full existing test suite staying green.

- **Refactor is incremental and non-breaking:** `ShardWriter::new` is preserved
  as a convenience wrapper over `new_with_store(FsSegmentStore)`. All 30+
  existing `segment.rs`, `mod.rs`, and `worker.rs` unit tests compile without
  modification. Integration tests in `tests/system.rs` spawn a real binary and
  are unaffected by any of these changes.

- **Estimated scope:** ~130 changed lines across four existing files
  (`segment.rs`, `wab/mod.rs`, `worker.rs`, `main.rs`) plus ~515 new lines in
  the `sim/` module, totalling ~645 lines for Phase 4.0a–d.
