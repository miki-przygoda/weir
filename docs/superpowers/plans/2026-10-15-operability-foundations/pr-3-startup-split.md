# Operability Foundations — PR 3: WAB startup split, bounded flusher hand-off, channel depth, socket bind/serve split and live-shard routing

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 3: WAB startup split, bounded flusher hand-off, channel depth, socket bind/serve split and live-shard routing

**Scope (spec §16 row 3).** `wab::recover` / `wab::scan_replay` / `wab::spawn_flushers` split; the `hand_off` loop with `BlockingClock::send_timeout` and deterministic bounded-channel DST seeds; drain-channel depth publication; socket bind/serve split; bind every producer listener before serving any; live-shard routing; `DrainSpawner` (sink selection seam); `refuse_if_live` (built on the safety floor's `probe_socket`); the umask arithmetic in the `bind_hardened` comment (spec cites socket/mod.rs:431, it is now socket/mod.rs:449-460).

**Depends on.** PR 1 (StatusBoard with `drain_depth`, ShardRegistry with `pick_live` / `all_settled` / the hand-off-blocked marker, `FlusherExit`, the PR-1 metric families, the poll tasks in `status/poll.rs`). Developed in parallel with PR 2 (nothing here touches `drain/`), but **merged after PR 2** (integrator decision): Task 3.7's `DrainSpawner` replaces `main`'s `build_and_spawn_drain`, whose signature PR 2 changes, so this PR rebases onto PR 2 before merge and the spawner carries PR 2's `DrainRuntime` and `JoinHandle<DrainExit>` from the start. **PR 3 runs Tasks 3.1-3.6 and 3.8-3.12 in its worktree. Task 3.7 starts only after PR 2 has merged and PR 3 is rebased onto main** (it consumes PR 2's `DrainRuntime`, `spawn_with` and `drain_runtime`, which do not exist on the PR 1 base); no other PR 3 task consumes Task 3.7, and the PR merge gate runs after it. PR 4 builds on it (it deletes `replay_unconfirmed`, hands `scan_replay`'s list to the drain as `initial_backlog`, swaps the `oneshot` shutdown receiver for `watch::Receiver<ShutdownLevel>`, sets the flushers' `shutting_down` flag at T0, and moves the settle wait into startup step 10).

**Behaviour change.** Routing (new connections go only to ingest-capable shards; none live means the connection is dropped and counted). Bind order (TCP and Unix producer listeners are both bound before either accept loop starts). Nothing else: the hand-off loop behaves exactly like a blocking send while `shutting_down` is false, and nothing in PR 3 sets it.

**Merge gate.**
- `cargo test -p weir-server --bins -- --test-threads=1` and the same with `--all-features`.
- DST: `WEIR_DST_SWEEP=300 cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`, including the three new pinned `bounded_handoff_*` seeds.
- `cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test load --test load_tls --test tls_client --test tls_listener` with default features and with `--all-features` (the routing system tests are behind `test-hooks`, so only the `--all-features` line runs them).
- Hand-off tests A and B (spec §15), Task 3.4.
- `cargo clippy -p weir-server --all-targets -- -D warnings` with default features, `--all-features` and `--no-default-features` (ci.yml's lint job).
- `act -W .github/workflows/ci.yml -j lint`, `-j test` and `-j dst` locally before pushing (memory: the cargo gate misses the container jobs).
- Owner-run on beast before merge: `tests/load.rs` Durable herd and Buffered, before and after, within ±3% (the hand-off loop is the only new branch next to the ack path, spec §19 risk 2).

**Reconciliations with the spec (code wins for current behaviour, spec wins for target).**
1. Spec line numbers predate the safety floor. The three flusher send sites are now wab/mod.rs:766 (timed seal, `seal_before_size_threshold`), :1014 (shutdown seal) and :1149 (rotation, inside `flush_batch`), not :595/:843/:978.
2. The per-flusher budget is the spec's `now - t0 >= FINAL_HANDOFF_TIMEOUT`, measured in wall time **on the injected clock** (README Q7): Task 3.1 adds `BlockingClock::now() -> Duration` (monotonic, arbitrary epoch). `RealClock` reads a process-wide `Instant`; `SimClock` and `SchedClock` return their virtual time, which a full-channel `send_timeout` advances by the slice, so the bounded-channel seed family stays deterministic. `t0` is the start of the first slice that timed out after `shutting_down` was seen (that slice already counts against the budget), so a channel that never drains gives up after exactly 10 slices of 100 ms, then one slice per later hand-off. `t0` lives in an `Arc<AtomicU64>` cloned into every panic-respawn of the same flusher, so the budget is per flusher thread as the spec requires.
3. Spec: `hand_off(clock, tx, sealed, sh)`. Plan: one extra `site: HandoffSite` argument, so the existing per-site log context survives the unification: rotation, shutdown seal, and the timed seal **with its `reason`** (`idle` or `lifetime`, which `seal_before_size_threshold` logs today), carried as `HandoffSite::TimedSeal { reason }`.
4. Spec: `spawn_flushers(dir, cfg, drain_tx, metrics, coalesce_hint, registry, drain_depth, shutting_down)`. Plan: `drain_depth` and `shutting_down` (plus the blocked-marker sink) travel together in one `HandoffConfig`; PR 1's registry parameter is kept exactly as PR 1 left it on `wab::spawn`.
5. Spec: `scan_replay(dir, shard_count, &metrics)`. The safety floor added `orphan_cleanup` to the scan (D7), so `scan_replay` takes it too.
6. Spec: `replay_unconfirmed` and `replay_tx` are deleted. In PR 3 the drain is still fed from the channel (PR 2's `initial_backlog` is not wired until PR 4), so `replay_unconfirmed` survives as a thin wrapper (`scan_replay` + blocking sends) and its three existing tests stay unchanged. PR 4 deletes it.
7. Spec: `socket::bind -> BoundUnix{listener, path, dev, ino, parent_dev_ino}` and `run(cfg, bound, ...)`. The safety floor already has `BoundSocket{listener, dev, ino, pin, ctime, birthtime}` and `remove_own_socket(path, &BoundSocket)`, which re-opens the parent and checks the socket's own identity, so `BoundUnix` wraps it as `{path, socket: BoundSocket}` with no `parent_dev_ino`. The bound-taking accept loop is named `socket::serve`; `socket::run` stays as a `#[cfg(test)]` bind-then-serve wrapper so the seven existing socket tests compile unchanged.
8. Spec: `tcp::bind`. `socket::tcp` is `tls`-gated and `bind_all` is not, so the TCP bind lives in `socket::bind_all` (error text from `preflight::tcp_bind_error` under `tls`).
9. Spec: `refuse_if_live` attempts a non-blocking connect "before `cleanup_existing_socket`". SP-A landed first and owns the probe: `bind_hardened` already refuses a live socket through `remove_stale_socket_at_startup` → `probe_socket`. `refuse_if_live(path, what)` is therefore a thin public helper over `probe_socket` for callers that want the refusal before `bind_hardened` with their own wording (PR 6's admin socket). The producer bind does not call it (it would double-connect a live daemon).
10. Routing and the hand-off marker reach PR 1's types through two one-method traits (`socket::route::LiveShards`, `wab::handoff::HandoffMarker`) implemented for `status::StatusBoard`. That keeps every unit test in this PR independent of PR 1's constructors, and makes "detached" (spec: "DST passes a detached registry") mean `marker: None`.
11. The settle wait (spec §6 step 10, "wait at most 10 s for `registry.all_settled()`") lands here, before the binds, because routing lands here: without it a connection accepted before the flushers finish their counter scan would see every shard `Starting` and be refused. Routing also needs the shard's worker running (PR 1's `ingest_capable`), so the wait also waits for every worker. PR 4 moves the call into its startup step 10.
12. **Listener marks, one writer each (README Q6).** `socket::bind` marks the Unix listener `Bound` or `Failed`; `socket::bind_all` marks the TCP listener `Bound` or `Failed`; `socket::serve` enters `Serving` and marks the Unix listener `Closed`; `main` marks no listener state. Until PR 4 Task 4.7 moves them into `tcp::run`, PR 3's `main` keeps only the TCP `Closed` / `Failed` marks around the spawned `tcp::run` (Task 3.11).
13. **`weir_wab_segments_not_handed_off_total{reason}` (README Q7).** PR 1 Task 1.18 registers it as `Family<NotHandedOffLabel, Counter<u64, AtomicU64>>` with `reason` ∈ {`budget`, `disconnected`}, both pre-created, plus `Metrics::wab_segments_not_handed_off_total()` (the sum). `hand_off` increments `budget` when the shutdown budget runs out and `disconnected` when the drain's Receiver is gone.

**Consumed from PR 1 and PR 2 (reconciled against their sections of this plan).**
- `crate::status::StatusBoard` held by `main` as `board: Arc<StatusBoard>`, with fields `pub(crate) shards: Arc<ShardRegistry>` (an `Arc`, PR 1 reconciliation 4; method calls are unchanged) and `pub(crate) drain_depth: Arc<AtomicUsize>` (initial value `status::board::DEPTH_UNPUBLISHED = usize::MAX` until the first store).
- `ShardRegistry::pick_live(&self, n: u64) -> Option<u32>`, `ShardRegistry::all_settled(&self) -> bool`, `ShardRegistry::set_handoff_blocked_since_ms(&self, shard: usize, since_unix_ms: u64)` (0 clears).
- `Metrics::wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>` (`weir_wab_segments_not_handed_off_total{reason}`), with `crate::metrics::{NotHandedOffLabel { pub reason: NotHandedOffReason }, NotHandedOffReason::{budget, disconnected}}` (both values pre-created) and `pub(crate) fn Metrics::wab_segments_not_handed_off_total(&self) -> u64` (the sum of both series), per README Q7; and `Metrics::connections_refused_no_live_shard: Counter<u64, AtomicU64>` (`weir_connections_refused_no_live_shard_total`). Both registered by PR 1 Task 1.18 with their docs/monitoring.md rows.
- `crate::status::unix_ms_now() -> u64` (PR 1 Task 1.11); the hand-off marker stamps with it (no local copy).
- PR 1's shard-state arguments (PR 1 Task 1.13): `wab::spawn(.., registry: Arc<ShardRegistry>)` and `flusher_thread(<16 existing params>, slot: &ShardSlot) -> FlusherExit`, with `ShardSlot::detached()` at every DST and test call site; `run_with_panic_supervision(shard_id, metrics, clock, slot: &ShardSlot, body_factory)` with the `B: FnOnce() -> FlusherExit` bound.
- PR 1's `SocketConfig.board: Option<Arc<StatusBoard>>` hook (PR 1 Task 1.23), which `socket::run` uses to mark the Unix listener and the serving phase; Tasks 3.10 and 3.11 move those marks across the bind/serve split to the README Q6 writers (reconciliation 12): `bind` / `bind_all` mark `Bound` or `Failed`, `serve` marks `Serving` and `Closed`, `main` does not mark.
- `ShardRegistry::{worker_count(), worker_running(usize)}` (PR 1) for the settle wait.
- PR 2 (merged first): `drain::spawn_with(rx, sink, config, metrics, heartbeat, runtime: DrainRuntime) -> JoinHandle<DrainExit>`, `drain::DrainRuntime { control, board, initial_backlog }` and `DrainRuntime::detached()`; `drain::spawn` is `#[cfg(test)]` after PR 2.
- PR 1's 500 ms poll task in `crates/weir-server/src/status/poll.rs` (the loop that sets `metrics.queue_depth`).
- PR 2's `wait_slice` stores `rx.len()` into the same `board.drain_depth` (spec §5.4); PR 3 does not touch the drain.

---

### Task 3.1: `BlockingClock::send_timeout` and `BlockingClock::now`

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/wab/clock.rs` (trait at lines 19-25, `RealClock` impl at 32-43; add a `#[cfg(test)] mod tests`)
- Modify: `crates/weir-server/src/wab/dst.rs` (imports at 39; `SimClock` impl at 159-169; `SchedClock` impl at 321-341; tests module from 1530)
- Test: the two files above

**Interfaces:**
- Consumes: `crossbeam_channel::{Sender, SendTimeoutError, TrySendError}` (crossbeam-channel is already a dependency).
- Produces:
  - `pub(crate) trait BlockingClock { fn send_timeout<T>(&self, tx: &Sender<T>, value: T, timeout: Duration) -> Result<(), SendTimeoutError<T>>; fn now(&self) -> Duration; }` (two new required methods beside `recv_timeout` and `sleep`). `now` is monotonic time since an arbitrary epoch; the hand-off measures its shutdown budget with it (README Q7).
  - `RealClock::send_timeout` = `tx.send_timeout(value, timeout)`; `RealClock::now` = elapsed time since a process-wide `OnceLock<Instant>` epoch.
  - `SimClock::send_timeout`: `try_send`; `Full` advances virtual time by `timeout` and returns `Timeout(value)` without blocking; `Disconnected` returns at once. `SimClock::now` = `self.virtual_elapsed()`.
  - `SchedClock::send_timeout`: `try_send`; `Full` advances virtual time, yields the turn (`sched.step`, never `block`, so the scheduler's "producers never block" liveness rule holds) and returns `Timeout(value)`. `SchedClock::now` = `Duration::from_nanos(self.virtual_nanos.load(SeqCst))`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/weir-server/src/wab/clock.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::SendTimeoutError;
    use std::time::Instant;

    #[test]
    fn real_send_timeout_hands_the_value_back_after_the_timeout_on_a_full_channel() {
        let (tx, _rx) = crossbeam_channel::bounded::<u32>(1);
        tx.send(1).unwrap();
        let t0 = Instant::now();
        match RealClock.send_timeout(&tx, 2, Duration::from_millis(100)) {
            Err(SendTimeoutError::Timeout(v)) => assert_eq!(v, 2, "the unsent value must come back"),
            other => panic!("expected Timeout(2), got {other:?}"),
        }
        let waited = t0.elapsed();
        assert!(
            waited >= Duration::from_millis(90) && waited < Duration::from_millis(600),
            "a full channel must wait out the timeout, waited {waited:?}"
        );
    }

    #[test]
    fn real_send_timeout_reports_a_closed_channel_at_once() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        drop(rx);
        let t0 = Instant::now();
        assert!(matches!(
            RealClock.send_timeout(&tx, 7, Duration::from_secs(5)),
            Err(SendTimeoutError::Disconnected(7))
        ));
        assert!(t0.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn real_send_timeout_delivers_when_there_is_room() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        RealClock
            .send_timeout(&tx, 3, Duration::from_millis(100))
            .unwrap();
        assert_eq!(rx.try_recv().unwrap(), 3);
    }

    #[test]
    fn real_now_is_monotonic_and_follows_wall_time() {
        let a = RealClock.now();
        std::thread::sleep(Duration::from_millis(50));
        let b = RealClock.now();
        assert!(b >= a + Duration::from_millis(45), "now() must advance with real time: {a:?} -> {b:?}");
        assert!(b - a < Duration::from_millis(1000), "{a:?} -> {b:?}");
    }
}
```

Append inside `mod tests` of `crates/weir-server/src/wab/dst.rs`:

```rust
    #[test]
    fn sim_send_timeout_on_a_full_channel_advances_virtual_time_without_blocking() {
        use crossbeam_channel::SendTimeoutError;
        let clock = SimClock::new();
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        tx.send(1).unwrap();
        let t0 = Instant::now();
        assert!(matches!(
            clock.send_timeout(&tx, 2, Duration::from_millis(100)),
            Err(SendTimeoutError::Timeout(2))
        ));
        assert!(
            t0.elapsed() < Duration::from_millis(50),
            "SimClock must never block the real thread"
        );
        assert_eq!(clock.virtual_elapsed(), Duration::from_millis(100));
        assert_eq!(clock.now(), clock.virtual_elapsed(), "SimClock's now() is its virtual time");

        // Room again: delivered, and no virtual time is charged for a send that
        // did not wait.
        assert_eq!(rx.try_recv().unwrap(), 1);
        clock.send_timeout(&tx, 3, Duration::from_millis(100)).unwrap();
        assert_eq!(clock.virtual_elapsed(), Duration::from_millis(100));

        drop(rx);
        assert!(matches!(
            clock.send_timeout(&tx, 4, Duration::from_millis(100)),
            Err(SendTimeoutError::Disconnected(4))
        ));
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server send_timeout -- --test-threads=1`
Expected: compile error `no method named `send_timeout` found for struct `RealClock`` (and the same for `SimClock`), and `no method named `now``.

- [ ] **Step 3: Implement**

`crates/weir-server/src/wab/clock.rs`: update the module doc's first paragraph to name three blocking points (waiting for the next batch, handing a sealed segment to the drain, the supervisor backoff) and one time source (the hand-off's shutdown budget), then:

```rust
use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender};

pub(crate) trait BlockingClock {
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration) -> Result<T, RecvTimeoutError>;

    /// Block for at most `timeout` trying to send `value` on `tx`. On a timeout
    /// or a closed channel the value comes back inside the error, so the caller
    /// still owns it. Mirrors [`crossbeam_channel::Sender::send_timeout`]. The
    /// flusher's hand-off to the drain is the only caller (`wab::handoff`).
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>>;

    fn sleep(&self, dur: Duration);

    /// Monotonic time since an arbitrary epoch. Only differences are
    /// meaningful. The hand-off measures its shutdown budget with it, so the
    /// DST clocks can make that budget virtual and deterministic.
    fn now(&self) -> Duration;
}

impl BlockingClock for RealClock {
    // recv_timeout and sleep unchanged

    #[inline]
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        tx.send_timeout(value, timeout)
    }

    fn now(&self) -> Duration {
        static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        EPOCH.get_or_init(std::time::Instant::now).elapsed()
    }
}
```

`crates/weir-server/src/wab/dst.rs`: change the import at line 39 to `use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError, TrySendError};`, then add to `impl BlockingClock for SimClock`:

```rust
    /// Never blocks: a full channel charges `timeout` of virtual time and hands
    /// the value back, exactly as a real `send_timeout` that waited it out.
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        match tx.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(v)) => Err(SendTimeoutError::Disconnected(v)),
            Err(TrySendError::Full(v)) => {
                self.virtual_nanos
                    .fetch_add(timeout.as_nanos() as u64, SeqCst);
                Err(SendTimeoutError::Timeout(v))
            }
        }
    }

    fn now(&self) -> Duration {
        self.virtual_elapsed()
    }
```

and to `impl BlockingClock for SchedClock`:

```rust
    /// A cooperative point. A full channel yields the turn (NOT blocked: a
    /// blocked thread is only rescheduled after progress, and nothing in the
    /// sim drains the drain channel) and reports a timeout.
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        match tx.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(v)) => Err(SendTimeoutError::Disconnected(v)),
            Err(TrySendError::Full(v)) => {
                self.virtual_nanos
                    .fetch_add(timeout.as_nanos() as u64, SeqCst);
                self.sched.step(self.id);
                Err(SendTimeoutError::Timeout(v))
            }
        }
    }

    /// Shared virtual time: other sim threads advance it too, which can only
    /// shorten a budget measured with it, never lengthen it.
    fn now(&self) -> Duration {
        Duration::from_nanos(self.virtual_nanos.load(SeqCst))
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server send_timeout -- --test-threads=1` and `cargo test -p weir-server --bin weir-server real_now -- --test-threads=1`
Expected: 4 passed, then 1 passed. Then `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`: all existing DST tests still pass (no call site uses the new methods yet).

- [ ] **Step 5: Commit**

`git commit -m "feat(wab): a timed send and a clock reading on the flusher's clock seam"` (body: the DST clock never blocks and charges virtual time; the scheduler clock yields rather than blocks so its liveness rule holds; `now()` lets the hand-off measure its shutdown budget on the injected clock.)

---

### Task 3.2: `wab/handoff.rs` — the bounded hand-off loop

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Create: `crates/weir-server/src/wab/handoff.rs`
- Modify: `crates/weir-server/src/wab/mod.rs:1-6` (add `pub(crate) mod handoff;`)
- Test: `crates/weir-server/src/wab/handoff.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `BlockingClock::{send_timeout, now}` (Task 3.1); `Metrics::wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>`, `NotHandedOffLabel`, `NotHandedOffReason::{budget, disconnected}` and `Metrics::wab_segments_not_handed_off_total()` (PR 1 Task 1.18, README Q7); `crate::status::unix_ms_now()` (PR 1 Task 1.11); `status::StatusBoard { shards: Arc<ShardRegistry> }` and `ShardRegistry::set_handoff_blocked_since_ms(&self, shard: usize, since_unix_ms: u64)` (PR 1).
- Produces:
  - `pub(crate) const HANDOFF_SLICE: Duration = Duration::from_millis(100);`
  - `pub(crate) const FINAL_HANDOFF_TIMEOUT: Duration = Duration::from_secs(1);` (wall time on the injected clock; no attempt count)
  - `pub(crate) trait HandoffMarker: Send + Sync { fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64); }` with `impl HandoffMarker for crate::status::StatusBoard`
  - `#[derive(Clone)] pub(crate) struct HandoffConfig { pub(crate) shutting_down: Arc<AtomicBool>, pub(crate) drain_depth: Arc<AtomicUsize>, pub(crate) marker: Option<Arc<dyn HandoffMarker>> }`, `fn for_shard(&self, shard_id: u16, metrics: Arc<Metrics>) -> HandoffState`, `#[cfg(any(test, feature = "dst"))] fn detached() -> HandoffConfig`
  - `#[derive(Clone)] pub(crate) struct HandoffState` (private fields: `shard_id`, `shutting_down`, `drain_depth`, `marker`, `metrics`, `shutdown_t0_ns: Arc<AtomicU64>` (`u64::MAX` = budget not started)), `#[cfg(any(test, feature = "dst"))] fn new(shard_id: u16, shutting_down: Arc<AtomicBool>, drain_depth: Arc<AtomicUsize>, marker: Option<Arc<dyn HandoffMarker>>, metrics: Arc<Metrics>) -> HandoffState`, `#[cfg(any(test, feature = "dst"))] fn detached(shard_id: u16, metrics: Arc<Metrics>) -> HandoffState`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum HandoffSite { Rotation, TimedSeal { reason: &'static str }, ShutdownSeal }`
  - `#[derive(Debug, PartialEq, Eq)] pub(crate) enum HandOff { Delivered, NotHandedOff(NotHandedOff) }`, `#[derive(Debug, PartialEq, Eq)] pub(crate) enum NotHandedOff { ShutdownBudget, Disconnected }`
  - `pub(crate) fn hand_off<C: BlockingClock>(clock: &C, tx: &Sender<PathBuf>, sealed: PathBuf, sh: &HandoffState, site: HandoffSite) -> HandOff`

- [ ] **Step 1: Write the failing tests**

The tests module at the bottom of the new file (written first; the file's non-test body arrives in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{Receiver, RecvTimeoutError, TrySendError};
    use std::sync::{Mutex, atomic::{AtomicU64, Ordering::SeqCst}};
    use std::time::Instant;

    use crate::wab::clock::RealClock;

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    /// One `reason` series of `weir_wab_segments_not_handed_off_total`.
    fn not_handed_off(m: &Metrics, reason: NotHandedOffReason) -> u64 {
        m.wab_segments_not_handed_off
            .get_or_create(&NotHandedOffLabel { reason })
            .get()
    }

    /// Records every marker write, so a test can see "set, then cleared".
    #[derive(Default)]
    struct RecordingMarker(Mutex<Vec<(u16, u64)>>);
    impl HandoffMarker for RecordingMarker {
        fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64) {
            self.0.lock().unwrap().push((shard, since_unix_ms));
        }
    }

    /// Counts send attempts and never waits; a full channel charges the
    /// timeout to a virtual clock, as `SimClock` does. Makes the wall-time
    /// budget exact in attempts.
    #[derive(Default)]
    struct CountingClock {
        attempts: AtomicU64,
        virtual_ms: AtomicU64,
    }
    impl BlockingClock for CountingClock {
        fn recv_timeout<T>(&self, rx: &Receiver<T>, t: Duration) -> Result<T, RecvTimeoutError> {
            rx.recv_timeout(t)
        }
        fn send_timeout<T>(
            &self,
            tx: &Sender<T>,
            v: T,
            t: Duration,
        ) -> Result<(), SendTimeoutError<T>> {
            self.attempts.fetch_add(1, SeqCst);
            match tx.try_send(v) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(v)) => {
                    self.virtual_ms.fetch_add(t.as_millis() as u64, SeqCst);
                    Err(SendTimeoutError::Timeout(v))
                }
                Err(TrySendError::Disconnected(v)) => Err(SendTimeoutError::Disconnected(v)),
            }
        }
        fn sleep(&self, _dur: Duration) {}
        fn now(&self) -> Duration {
            Duration::from_millis(self.virtual_ms.load(SeqCst))
        }
    }

    fn full_channel() -> (Sender<PathBuf>, Receiver<PathBuf>) {
        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
        tx.send(PathBuf::from("prefill")).unwrap();
        (tx, rx)
    }

    #[test]
    fn the_budget_is_one_second_per_flusher_then_one_slice_per_later_hand_off() {
        let m = metrics();
        let sh = HandoffState::new(
            3,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicUsize::new(0)),
            None,
            Arc::clone(&m),
        );
        let (tx, _rx) = full_channel();
        let clock = CountingClock::default();

        let first = hand_off(&clock, &tx, PathBuf::from("a"), &sh, HandoffSite::Rotation);
        assert_eq!(first, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert_eq!(clock.attempts.load(SeqCst), 10, "the budget is 1 s of clock time: ten 100 ms slices");
        assert_eq!(clock.now(), Duration::from_secs(1));

        // A clone shares the budget, as a panic-respawned flusher does.
        let respawned = sh.clone();
        let second = hand_off(&clock, &tx, PathBuf::from("b"), &respawned, HandoffSite::ShutdownSeal);
        assert_eq!(second, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert_eq!(clock.attempts.load(SeqCst), 11, "a spent budget allows exactly one more slice");
        assert_eq!(not_handed_off(&m, NotHandedOffReason::budget), 2);
        assert_eq!(not_handed_off(&m, NotHandedOffReason::disconnected), 0);
        assert_eq!(m.wab_segments_not_handed_off_total(), 2);
    }

    #[test]
    fn outside_shutdown_a_full_channel_never_gives_up() {
        // Backpressure is unchanged: no flag, so the loop keeps trying. A
        // consumer frees the slot after 250 ms and the segment is delivered.
        let marker = Arc::new(RecordingMarker::default());
        let depth = Arc::new(AtomicUsize::new(0));
        let sh = HandoffState::new(
            1,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&depth),
            Some(Arc::clone(&marker) as Arc<dyn HandoffMarker>),
            metrics(),
        );
        let (tx, rx) = full_channel();
        let consumer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            let first = rx.recv().unwrap();
            let second = rx.recv().unwrap();
            (first, second)
        });
        let t0 = Instant::now();
        let out = hand_off(&RealClock, &tx, PathBuf::from("seg"), &sh, HandoffSite::Rotation);
        assert_eq!(out, HandOff::Delivered);
        assert!(t0.elapsed() >= Duration::from_millis(200));
        let (first, second) = consumer.join().unwrap();
        assert_eq!((first, second), (PathBuf::from("prefill"), PathBuf::from("seg")));

        let writes = marker.0.lock().unwrap().clone();
        assert_eq!(writes.len(), 2, "marked once on the first timeout, cleared once: {writes:?}");
        assert_eq!(writes[0].0, 1);
        assert!(writes[0].1 > 0, "the mark is a wall-clock unix-ms stamp");
        assert_eq!(writes[1], (1, 0), "a delivered hand-off clears the marker");
        assert!(depth.load(SeqCst) <= 1, "depth is the channel length after the send");
    }

    #[test]
    fn a_flag_raised_mid_block_is_seen_within_one_slice_and_bounds_the_wait() {
        let flag = Arc::new(AtomicBool::new(false));
        let sh = HandoffState::new(0, Arc::clone(&flag), Arc::new(AtomicUsize::new(0)), None, metrics());
        let (tx, _rx) = full_channel();
        let raiser = {
            let flag = Arc::clone(&flag);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                flag.store(true, SeqCst);
            })
        };
        let t0 = Instant::now();
        let out = hand_off(&RealClock, &tx, PathBuf::from("seg"), &sh, HandoffSite::ShutdownSeal);
        let waited = t0.elapsed();
        raiser.join().unwrap();
        assert_eq!(out, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert!(
            waited >= Duration::from_millis(1200) && waited < Duration::from_millis(2000),
            "≈300 ms blocked + 1 s budget, got {waited:?}"
        );
    }

    #[test]
    fn a_closed_channel_gives_up_at_once_and_counts() {
        let m = metrics();
        let sh = HandoffState::detached(0, Arc::clone(&m));
        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drop(rx);
        let t0 = Instant::now();
        let out = hand_off(&RealClock, &tx, PathBuf::from("seg"), &sh, HandoffSite::TimedSeal { reason: "idle" });
        assert_eq!(out, HandOff::NotHandedOff(NotHandedOff::Disconnected));
        assert!(t0.elapsed() < Duration::from_millis(100));
        assert_eq!(not_handed_off(&m, NotHandedOffReason::disconnected), 1);
        assert_eq!(not_handed_off(&m, NotHandedOffReason::budget), 0, "a closed channel is not a spent budget");
    }

    #[test]
    fn the_site_carries_the_timed_seal_reason_into_the_log_fields() {
        assert_eq!(HandoffSite::TimedSeal { reason: "lifetime" }.as_str(), "timed_seal");
        assert_eq!(HandoffSite::TimedSeal { reason: "lifetime" }.reason(), Some("lifetime"));
        assert_eq!(HandoffSite::Rotation.reason(), None);
        assert_eq!(HandoffSite::ShutdownSeal.as_str(), "shutdown_seal");
    }

    #[test]
    fn a_delivered_hand_off_publishes_the_channel_depth() {
        let depth = Arc::new(AtomicUsize::new(0));
        let sh = HandoffState::new(0, Arc::new(AtomicBool::new(false)), Arc::clone(&depth), None, metrics());
        let (tx, _rx) = crossbeam_channel::bounded::<PathBuf>(4);
        tx.send(PathBuf::from("already")).unwrap();
        assert_eq!(hand_off(&RealClock, &tx, PathBuf::from("seg"), &sh, HandoffSite::Rotation), HandOff::Delivered);
        assert_eq!(depth.load(SeqCst), 2);
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server wab::handoff -- --test-threads=1`
Expected: compile error, `cannot find function `hand_off` in this scope` / `cannot find type `HandoffState``, because the module body does not exist yet.

- [ ] **Step 3: Implement**

`crates/weir-server/src/wab/mod.rs`, after `pub(crate) mod clock;`: `pub(crate) mod handoff;`

`crates/weir-server/src/wab/handoff.rs` (above the tests module):

```rust
//! The flusher → drain hand-off (spec §5.5).
//!
//! Every flusher send site (rotation, timed seal, shutdown seal) goes through
//! [`hand_off`]: a loop of 100 ms timed sends instead of one blocking send.
//!
//! # Invariant (spec §15, the only new branch next to the ack path)
//!
//! `sealed` is always a path returned by a successful `seal_current`, and the
//! seal includes the fsync. The acks for the segment's records are appended
//! after this returns (`flush_batch`'s `durable_acks.append`) and resolved
//! after that. So whatever this returns, those acks are true. A segment that is
//! NOT handed off is sealed, unconfirmed and replayed on the next start: the
//! same outcome as today's `SendError` arm. Never move the hand-off after the
//! ack resolution, and never call it with a path that was not sealed.
//!
//! # Why a loop
//!
//! A flusher that entered a send before SIGTERM re-reads `shutting_down` at
//! most one slice later; a plain `send` waits until the drain takes an item,
//! and the drain may be inside a commit lasting up to `commit_timeout`.
//! Outside shutdown the loop behaves exactly like a blocking send: the only
//! added cost is one Relaxed load per 100 ms *of blocking*.
//!
//! # Budget
//!
//! Once `shutting_down` is set, the budget is [`FINAL_HANDOFF_TIMEOUT`] of
//! wall time measured on the injected clock (`BlockingClock::now`, README
//! Q7): `t0` is the start of the first slice that timed out after the flag was
//! seen, and the hand-off gives up once `now - t0 >= FINAL_HANDOFF_TIMEOUT`.
//! The budget is per flusher thread (`t0` is an `Arc<AtomicU64>` shared by
//! every panic-respawn of that flusher): once it is spent, each later hand-off
//! makes one slice and gives up. Total shutdown hand-off time per flusher is
//! ≤ 1 s + 100 ms per remaining rotation. Under `RealClock` that is real time;
//! under the DST `SimClock` it is virtual time, which only a full channel or a
//! flusher wait advances, so the bounded-channel seeds are deterministic.
//!
//! # Counting
//!
//! Both ways of giving up leave a sealed, unconfirmed segment for replay and
//! are counted in `weir_wab_segments_not_handed_off_total{reason}` (README
//! Q7): `budget` when the shutdown budget ran out, `disconnected` when the
//! drain's Receiver was gone. The T3 shutdown summary reads the sum.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use crossbeam_channel::{SendTimeoutError, Sender};
use tracing::{info, warn};

use super::clock::BlockingClock;
use crate::metrics::{Metrics, NotHandedOffLabel, NotHandedOffReason};

/// One timed send. Also the latency with which a blocked flusher sees shutdown.
pub(crate) const HANDOFF_SLICE: Duration = Duration::from_millis(100);
/// The per-flusher shutdown budget (spec §13 constant), in wall time on the
/// injected clock.
pub(crate) const FINAL_HANDOFF_TIMEOUT: Duration = Duration::from_secs(1);

/// `shutdown_t0_ns` before the budget has started.
const BUDGET_NOT_STARTED: u64 = u64::MAX;

/// Where a hand-off reports "this shard's flusher is blocked on the drain".
/// Production: the StatusBoard's ShardRegistry (`handoff_blocked_since_ms`,
/// spec §5.3). `None` in [`HandoffConfig`] = detached (DST, unit tests).
pub(crate) trait HandoffMarker: Send + Sync {
    /// `since_unix_ms == 0` clears the marker.
    fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64);
}

impl HandoffMarker for crate::status::StatusBoard {
    fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64) {
        self.shards
            .set_handoff_blocked_since_ms(usize::from(shard), since_unix_ms);
    }
}

/// Shared by every flusher; turned into one [`HandoffState`] per shard.
#[derive(Clone)]
pub(crate) struct HandoffConfig {
    /// Set at T0 by the Lifecycle (PR 4). Nothing sets it in PR 3.
    pub(crate) shutting_down: Arc<AtomicBool>,
    /// `StatusBoard::drain_depth`: last-writer-wins `len()` snapshots.
    pub(crate) drain_depth: Arc<AtomicUsize>,
    pub(crate) marker: Option<Arc<dyn HandoffMarker>>,
}

impl HandoffConfig {
    #[cfg(any(test, feature = "dst"))]
    pub(crate) fn detached() -> Self {
        HandoffConfig {
            shutting_down: Arc::new(AtomicBool::new(false)),
            drain_depth: Arc::new(AtomicUsize::new(0)),
            marker: None,
        }
    }

    /// Called once per flusher THREAD (outside the panic-respawn factory), so
    /// the budget is shared by every respawn of that flusher.
    pub(crate) fn for_shard(&self, shard_id: u16, metrics: Arc<Metrics>) -> HandoffState {
        HandoffState {
            shard_id,
            shutting_down: Arc::clone(&self.shutting_down),
            drain_depth: Arc::clone(&self.drain_depth),
            marker: self.marker.clone(),
            metrics,
            shutdown_t0_ns: Arc::new(AtomicU64::new(BUDGET_NOT_STARTED)),
        }
    }
}

/// One flusher's view of the hand-off. Clones share the budget.
#[derive(Clone)]
pub(crate) struct HandoffState {
    shard_id: u16,
    shutting_down: Arc<AtomicBool>,
    drain_depth: Arc<AtomicUsize>,
    marker: Option<Arc<dyn HandoffMarker>>,
    metrics: Arc<Metrics>,
    /// `BlockingClock::now()` in ns at the end of the first slice that timed
    /// out while shutting down; [`BUDGET_NOT_STARTED`] until then.
    shutdown_t0_ns: Arc<AtomicU64>,
}

impl HandoffState {
    #[cfg(any(test, feature = "dst"))]
    pub(crate) fn new(
        shard_id: u16,
        shutting_down: Arc<AtomicBool>,
        drain_depth: Arc<AtomicUsize>,
        marker: Option<Arc<dyn HandoffMarker>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        HandoffConfig {
            shutting_down,
            drain_depth,
            marker,
        }
        .for_shard(shard_id, metrics)
    }

    /// Never shutting down, private depth cell, no marker.
    #[cfg(any(test, feature = "dst"))]
    pub(crate) fn detached(shard_id: u16, metrics: Arc<Metrics>) -> Self {
        HandoffConfig::detached().for_shard(shard_id, metrics)
    }

    fn mark_blocked(&self) {
        if let Some(m) = &self.marker {
            m.set_handoff_blocked_since_ms(self.shard_id, crate::status::unix_ms_now().max(1));
        }
    }

    fn clear_blocked(&self) {
        if let Some(m) = &self.marker {
            m.set_handoff_blocked_since_ms(self.shard_id, 0);
        }
    }

    /// Called after a slice timed out while shutting down, with the clock's
    /// `now()`. True once [`FINAL_HANDOFF_TIMEOUT`] has passed since the start
    /// of the first such slice (that slice counts against the budget, hence
    /// the `+ HANDOFF_SLICE`). Only the owning flusher thread calls this (a
    /// respawn runs only after the previous body ended), so load-then-store
    /// is race-free.
    fn shutdown_budget_spent(&self, now: Duration) -> bool {
        let now_ns = now.as_nanos() as u64;
        let t0 = match self.shutdown_t0_ns.load(Relaxed) {
            BUDGET_NOT_STARTED => {
                self.shutdown_t0_ns.store(now_ns, Relaxed);
                now_ns
            }
            t0 => t0,
        };
        now_ns.saturating_sub(t0) + HANDOFF_SLICE.as_nanos() as u64
            >= FINAL_HANDOFF_TIMEOUT.as_nanos() as u64
    }

    fn count_not_handed_off(&self, reason: NotHandedOffReason) {
        self.metrics
            .wab_segments_not_handed_off
            .get_or_create(&NotHandedOffLabel { reason })
            .inc();
    }
}

/// Which flusher site handed off; carried into the log line. A timed seal
/// keeps its `reason` (`idle` / `lifetime`), which the old send site logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandoffSite {
    Rotation,
    TimedSeal { reason: &'static str },
    ShutdownSeal,
}

impl HandoffSite {
    fn as_str(self) -> &'static str {
        match self {
            HandoffSite::Rotation => "rotation",
            HandoffSite::TimedSeal { .. } => "timed_seal",
            HandoffSite::ShutdownSeal => "shutdown_seal",
        }
    }

    /// The timed seal's reason; `None` for the other sites.
    fn reason(self) -> Option<&'static str> {
        match self {
            HandoffSite::TimedSeal { reason } => Some(reason),
            HandoffSite::Rotation | HandoffSite::ShutdownSeal => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HandOff {
    Delivered,
    NotHandedOff(NotHandedOff),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NotHandedOff {
    /// Shutting down and the per-flusher budget is spent.
    ShutdownBudget,
    /// The drain's Receiver is gone.
    Disconnected,
}

/// Hands one sealed segment to the drain. See the module docs for the
/// invariant that makes every return value safe for the acks.
pub(crate) fn hand_off<C: BlockingClock>(
    clock: &C,
    tx: &Sender<PathBuf>,
    sealed: PathBuf,
    sh: &HandoffState,
    site: HandoffSite,
) -> HandOff {
    let mut path = sealed;
    let mut marked = false;
    loop {
        match clock.send_timeout(tx, path, HANDOFF_SLICE) {
            Ok(()) => {
                if marked {
                    sh.clear_blocked();
                }
                sh.drain_depth.store(tx.len(), Relaxed);
                return HandOff::Delivered;
            }
            Err(SendTimeoutError::Timeout(back)) => {
                path = back;
                if !marked {
                    sh.mark_blocked();
                    marked = true;
                }
                if sh.shutting_down.load(Relaxed) && sh.shutdown_budget_spent(clock.now()) {
                    sh.clear_blocked();
                    sh.count_not_handed_off(NotHandedOffReason::budget);
                    warn!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %path.display(),
                        budget_ms = FINAL_HANDOFF_TIMEOUT.as_millis() as u64,
                        "shutting down and the drain channel stayed full; sealed segment not \
                         handed off. It is durable and will be delivered on restart"
                    );
                    return HandOff::NotHandedOff(NotHandedOff::ShutdownBudget);
                }
            }
            Err(SendTimeoutError::Disconnected(back)) => {
                if marked {
                    sh.clear_blocked();
                }
                sh.count_not_handed_off(NotHandedOffReason::disconnected);
                if sh.shutting_down.load(Relaxed) {
                    info!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %back.display(),
                        "drain channel closed on shutdown; sealed segment is durable and will \
                         be delivered on restart"
                    );
                } else {
                    warn!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %back.display(),
                        "drain channel closed; sealed segment is durable and will be delivered \
                         on restart"
                    );
                }
                return HandOff::NotHandedOff(NotHandedOff::Disconnected);
            }
        }
    }
}
```

Note: both causes are counted, under `reason` (README Q7): `budget` (spec §5.5's case) and `disconnected`. Both leave a sealed, unconfirmed segment for replay, the T3 shutdown summary (spec §7) reads the sum through `Metrics::wab_segments_not_handed_off_total()`, and an operator can still tell "the budget ran out" from "the drain was gone". The marker stamp uses PR 1's `crate::status::unix_ms_now()`; this module has no clock helper of its own.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server wab::handoff -- --test-threads=1`
Expected: 6 passed. `hand_off` has no production caller until Task 3.4, so a plain `cargo build` warns `dead_code` for this one commit. Do not add an `allow` to silence it: Task 3.4 (same PR) removes the warning, and the clippy run there is the check. If the branch must be warning-free at every commit, land Tasks 3.2 and 3.4 as one commit.

- [ ] **Step 5: Commit**

`git commit -m "feat(wab): a bounded hand-off from flusher to drain"`

---

### Task 3.3: Split `wab::spawn` into `recover`, `scan_replay` and `spawn_flushers`

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/wab/mod.rs:257-358` (`spawn`), `:616-658` (`replay_unconfirmed`), tests module from `:1299`
- Modify: `crates/weir-server/src/main.rs`: the `let wab_handle = wab::spawn(..)?;` call (:1087-1093 at 3adc5f5), the `// ── Replay recovery backlog` comment block above `wab::replay_unconfirmed(` (:1417-1432 at 3adc5f5), and the comment naming "recovery inside wab::spawn" (:963). PR 1 and PR 2 shift these lines; find them by the quoted text.
- Modify: `crates/weir-server/src/wab/recovery.rs:681-684` and `crates/weir-server/src/worker.rs:262,456` and `crates/weir-server/src/preflight.rs:13` (doc comments that name `wab::spawn`)
- Test: `crates/weir-server/src/wab/mod.rs` tests module

**Interfaces:**
- Consumes: `recovery::recover_open_segments(&Path, &Arc<Metrics>) -> io::Result<()>`; `scan_unconfirmed_sealed(&Path, Option<usize>, &Metrics, &dyn Fn(&Path) -> bool, bool) -> io::Result<SealedScan>`; `read_segment_record_count`; PR 1's `registry: Arc<ShardRegistry>` parameter on `wab::spawn`.
- Produces:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub(crate) struct RecoverySummary { pub(crate) shard_dirs: usize, pub(crate) segments_quarantined: u64, pub(crate) segments_failed: u64, pub(crate) elapsed: Duration }`
  - `pub(crate) fn recover(wab_dir: &Path, shard_count: usize, metrics: &Arc<Metrics>) -> io::Result<RecoverySummary>`
  - `pub(crate) fn scan_replay(wab_dir: &Path, shard_count: usize, metrics: &Arc<Metrics>, orphan_cleanup: bool) -> io::Result<Vec<PathBuf>>` (no sends; bumps `recovery_records_replayed`; same order and same fail-fast policy as today's replay)
  - `pub(crate) fn spawn_flushers(wab_dir: PathBuf, config: WabConfig, drain_tx: Sender<PathBuf>, metrics: Arc<Metrics>, coalesce_hint: Arc<AtomicU64>, registry: Arc<ShardRegistry>) -> io::Result<WabHandle>` (PR 1's parameter, moved unchanged; Task 3.4 appends `handoff: HandoffConfig`)
  - `replay_unconfirmed(wab_dir, shard_count, drain_tx, metrics, orphan_cleanup)` kept, body = `scan_replay` + blocking sends (deleted in PR 4)
  - `wab::spawn` removed.

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `crates/weir-server/src/wab/mod.rs`:

```rust
    /// `recover` is the first half of the old `spawn`: it makes the shard dirs
    /// private and seals what a crash left open, and it does nothing else.
    #[test]
    fn recover_creates_private_shard_dirs_and_seals_open_segments() {
        let dir = tmp_dir("recover_split");
        let sd0 = shard_dir_path(&dir, 0);
        crate::testutil::mkdir_p(&sd0);
        let open = segment_path(&sd0, 1);
        {
            let mut s = WabSegment::create(&open, 0, Compression::None).unwrap();
            s.write_record(b"left-open").unwrap();
            s.fsync().unwrap();
            // dropped unsealed: what a crash leaves
        }
        let metrics = Arc::new(Metrics::new().0);

        let summary = recover(&dir, 2, &metrics).unwrap();

        assert_eq!(summary.shard_dirs, 2);
        assert_eq!(summary.segments_quarantined, 0);
        assert!(!open.exists(), "recovery seals the open segment");
        let sealed = open.with_extension("wab.sealed");
        assert_eq!(read_segment_record_count(&sealed).unwrap(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for id in 0..2 {
                let mode = fs::metadata(shard_dir_path(&dir, id)).unwrap().permissions().mode();
                assert_eq!(mode & 0o777, 0o700, "shard_{id:02} must be private");
            }
        }
        fs::remove_dir_all(dir).ok();
    }

    /// `scan_replay` is the replay WITHOUT the sends: the list, in the order
    /// the drain will see it, and the replay metric, with no channel at all.
    #[test]
    fn scan_replay_lists_the_backlog_in_order_and_counts_its_records() {
        let dir = tmp_dir("scan_replay");
        let sd0 = shard_dir_path(&dir, 0);
        let sd1 = shard_dir_path(&dir, 1);
        let a = sealed_in_shard(&sd0, 1, b"a");
        let b = sealed_in_shard(&sd0, 2, b"b");
        let c = sealed_in_shard(&sd1, 1, b"c");
        let metrics = Arc::new(Metrics::new().0);

        let replay = scan_replay(&dir, 2, &metrics, false).unwrap();

        assert_eq!(replay, vec![a, b, c]);
        assert_eq!(metrics.recovery_records_replayed.get(), 3);
        fs::remove_dir_all(dir).ok();
    }

    /// Startup keeps the fail-fast policy: an unreadable shard aborts the scan
    /// rather than silently dropping its backlog.
    #[cfg(unix)]
    #[test]
    fn scan_replay_fails_on_an_unreadable_shard() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } == 0 {
            return; // root reads a 0o000 directory; the condition cannot be built
        }
        let dir = tmp_dir("scan_replay_unreadable");
        let sd0 = shard_dir_path(&dir, 0);
        sealed_in_shard(&sd0, 1, b"a");
        fs::set_permissions(&sd0, fs::Permissions::from_mode(0o000)).unwrap();
        let err = scan_replay(&dir, 1, &Arc::new(Metrics::new().0), false);
        fs::set_permissions(&sd0, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(err.is_err(), "startup replay must not skip a shard it cannot list");
        fs::remove_dir_all(dir).ok();
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server wab::tests:: -- --test-threads=1`
Expected: compile error `cannot find function `recover` in this scope` and `cannot find function `scan_replay` in this scope`.

- [ ] **Step 3: Implement**

In `crates/weir-server/src/wab/mod.rs`, replace `spawn` (257-358) with three functions:

```rust
/// What [`recover`] did. Logged once at startup ("recovered ... in ...").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    /// Shard directories ensured private (`0..shard_count`).
    pub(crate) shard_dirs: usize,
    /// Segments quarantined by this recovery pass.
    pub(crate) segments_quarantined: u64,
    /// Segments recovery could not process (`weir_recovery_segments_failed`).
    pub(crate) segments_failed: u64,
    pub(crate) elapsed: Duration,
}

/// Startup phase `recovering` (spec §6 step 8): `create_dir_private` for every
/// configured shard, then crash recovery (unsealed `.wab` → `.wab.sealed`).
/// Runs on the main thread before any flusher exists: the segment-counter
/// safety rule in `flusher_thread` relies on recovery having finished.
pub(crate) fn recover(
    wab_dir: &Path,
    shard_count: usize,
    metrics: &Arc<Metrics>,
) -> io::Result<RecoverySummary> {
    let t0 = Instant::now();
    let quarantined_before = metrics.recovery_segments_quarantined.get();
    let failed_before = metrics.recovery_segments_failed.get();
    for shard_id in 0..shard_count {
        create_dir_private(shard_dir_path(wab_dir, shard_id))?;
    }
    recover_open_segments(wab_dir, metrics)?;
    Ok(RecoverySummary {
        shard_dirs: shard_count,
        segments_quarantined: metrics.recovery_segments_quarantined.get() - quarantined_before,
        segments_failed: metrics.recovery_segments_failed.get() - failed_before,
        elapsed: t0.elapsed(),
    })
}

/// Startup phase `handoff` (spec §6 step 9): the sealed-but-unconfirmed
/// backlog, in the order the drain must deliver it, with the same footer
/// accounting the old `replay_unconfirmed` did, and NO sends. In PR 3 the
/// list is still pushed through the channel by [`replay_unconfirmed`]; from
/// PR 4 it moves into the drain in memory as `initial_backlog`.
///
/// `Some(shard_count)`: an unlistable shard fails startup (no later rescan is
/// guaranteed), exactly as before.
pub(crate) fn scan_replay(
    wab_dir: &Path,
    shard_count: usize,
    metrics: &Arc<Metrics>,
    orphan_cleanup: bool,
) -> io::Result<Vec<PathBuf>> {
    let scan = scan_unconfirmed_sealed(
        wab_dir,
        Some(shard_count),
        metrics,
        &|_| false,
        orphan_cleanup,
    )?;
    for sealed in &scan.segments {
        // A footer-read failure only undercounts recovery_records_replayed;
        // the segment is still delivered. Say so rather than report 0 silently.
        let record_count = match read_segment_record_count(sealed) {
            Ok(n) => n,
            Err(e) => {
                warn!(sealed = %sealed.display(), error = %e, "could not read record count for replay metric; reporting 0");
                0
            }
        };
        info!(sealed = %sealed.display(), records = record_count, "queuing segment for drain replay");
        metrics.recovery_records_replayed.inc_by(record_count);
    }
    Ok(scan.segments)
}

/// Startup phase `handoff`, last step (spec §6 step 10): one flusher thread
/// per shard. MUST run after [`recover`]: `flusher_thread` establishes its
/// segment counter from what recovery left on disk.
pub(crate) fn spawn_flushers(
    wab_dir: PathBuf,
    config: WabConfig,
    drain_tx: Sender<PathBuf>,
    metrics: Arc<Metrics>,
    coalesce_hint: Arc<AtomicU64>,
    // PR 1's parameter, moved here unchanged from `spawn`.
    registry: Arc<crate::status::ShardRegistry>,
) -> io::Result<WabHandle> {
    // Body: the old `spawn`'s block from the `// Phase 2: one flusher thread
    // per shard` comment through its final `Ok(WabHandle { .. })`, moved
    // verbatim, including PR 1's FlusherExit / registry wiring.
}
```

`replay_unconfirmed` (616-658) keeps its signature and doc (add: "PR 3 keeps this as the channel-fed replay; PR 4 deletes it"), and its body becomes:

```rust
    for sealed in scan_replay(wab_dir, shard_count, metrics, orphan_cleanup)? {
        drain_tx.send(sealed).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "drain channel closed during startup replay",
            )
        })?;
    }
    Ok(())
```

Update the `scan_unconfirmed_sealed` doc ("Shared by startup [`replay_unconfirmed`]" → "Shared by startup [`scan_replay`]"), and the doc comments in recovery.rs:681-684, worker.rs:262/456 and preflight.rs:13 that name `wab::spawn` (→ `wab::recover` / `wab::spawn_flushers` as appropriate).

`main`'s `let wab_handle = wab::spawn(..)?;` call (with PR 1's registry argument) becomes:

```rust
    // Recovery is now its own step (spec §6 step 8). PR 4 runs it while the
    // runtime already serves HTTP; here it keeps its old position.
    let recovery = wab::recover(&config.wab_dir, config.shard_count, &metrics)?;
    info!(
        shard_dirs = recovery.shard_dirs,
        quarantined = recovery.segments_quarantined,
        failed = recovery.segments_failed,
        elapsed_ms = recovery.elapsed.as_millis() as u64,
        "WAB recovery complete"
    );
    let wab_handle = wab::spawn_flushers(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
        Arc::clone(&coalesce_hint),
        Arc::clone(&board.shards), // PR 1's registry argument, unchanged
    )?;
```

The comment naming "recovery inside wab::spawn" (main.rs:963 at 3adc5f5) becomes "recovery in wab::recover". The `wab::replay_unconfirmed(..)` call is unchanged (it still goes through `replay_unconfirmed`); only its comment's "inside wab::spawn" becomes "inside wab::recover".

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server wab:: -- --test-threads=1`
Expected: the three new tests pass, and the existing `replay_needs_a_live_consumer_for_backlog_over_channel_capacity`, `replay_drains_shard_dirs_beyond_configured_count`, `replay_skips_already_confirmed_segment`, `an_orphan_is_deleted_at_startup_only_when_the_wab_dir_is_owned` and `an_unreadable_shard_is_skipped_by_the_rescan_but_fails_startup_replay` pass unchanged. Then `cargo test -p weir-server --test system -- --test-threads=1 server_restarts_after_sigkill wab_data_preserved_across_crash_restart acked_records_delivered_to_sink_and_confirmed_after_crash_restart` (one filter per invocation).

- [ ] **Step 5: Commit**

`git commit -m "refactor(wab): split startup into recover, scan_replay and spawn_flushers"`

---

### Task 3.4: Route every flusher send through `hand_off`; hand-off tests A and B

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/wab/mod.rs`: `seal_before_size_threshold` (751-788), `flusher_thread` (791-1028: signature, the two timed-seal calls at 938 and 998, the shutdown seal at 1005-1027), `flush_batch` (1030-1224: signature and the rotation send at 1149-1151), `spawn_flushers` (Task 3.3)
- Modify: `crates/weir-server/src/wab/dst.rs` call sites of `flusher_thread` at 924 (`drive_sync_flusher_with`), 1160 (`run_panic_during_flush`), 1474 (`run_interleaved_flush`), 1637 (`drive_rotating_batch`)
- Modify: `crates/weir-server/src/main.rs` (the `spawn_flushers` call from Task 3.3)
- Test: `crates/weir-server/src/wab/mod.rs` tests module

**Interfaces:**
- Consumes: `hand_off`, `HandoffState`, `HandoffConfig`, `HandoffSite`, `HandoffMarker` (Task 3.2); `StatusBoard::drain_depth` (PR 1).
- Produces:
  - `flusher_thread<C: BlockingClock>(<16 existing params>, slot: &ShardSlot, handoff: HandoffState) -> FlusherExit` (new LAST parameter, after PR 1's `slot`)
  - `#[allow(clippy::too_many_arguments)] flush_batch<C: BlockingClock>(writer, batches, drain_tx, shard_id, metrics, coalesce_hint, clock: &C, handoff: &HandoffState)` (8 parameters, over clippy's default limit of 7; the repo has no clippy.toml, and `flusher_thread` already carries the same allow)
  - `seal_before_size_threshold<C: BlockingClock>(writer, drain_tx, shard_id, metrics, reason, clock: &C, handoff: &HandoffState) -> bool`
  - `spawn_flushers(.., handoff: HandoffConfig) -> io::Result<WabHandle>` (new LAST parameter)
  - `main`: `let flushers_shutting_down: Arc<AtomicBool>` (false; PR 4's `Lifecycle` takes ownership and sets it at T0)

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `crates/weir-server/src/wab/mod.rs`:

```rust
    // ── Hand-off tests A and B (spec §15) ─────────────────────────────────────

    fn durable_unit(payload: Vec<u8>, ack_tx: oneshot::Sender<AckOutcome>) -> crate::models::WorkUnit {
        crate::models::WorkUnit {
            shard_id: 0,
            payload: weir_core::Payload::from(payload),
            durability: Durability::Durable,
            wants_coordinate: false,
            ack_tx,
            #[cfg(feature = "bench-trace")]
            enqueued_at: Instant::now(),
        }
    }

    /// One batch of `n` 32-byte Durable records, then the work channel closes,
    /// so the flusher writes, group-fsyncs, acks, then runs its shutdown seal
    /// and the final hand-off.
    fn one_batch_then_close(n: u8) -> (Receiver<Batch>, Vec<oneshot::Receiver<AckOutcome>>) {
        let (work_tx, work_rx) = crossbeam_channel::bounded::<Batch>(4);
        let mut acks = Vec::new();
        let mut units = Vec::new();
        for i in 0..n {
            let (tx, rx) = oneshot::channel();
            acks.push(rx);
            units.push(durable_unit(vec![i; 32], tx));
        }
        work_tx
            .send(Batch {
                shard_id: 0,
                records: units,
                #[cfg(feature = "bench-trace")]
                flushed_at: Instant::now(),
            })
            .unwrap();
        drop(work_tx);
        (work_rx, acks)
    }

    /// The real flusher on the real filesystem with `RealClock`. This is the
    /// only helper call site of `flusher_thread` PR 3 adds; PR 1's shard slot
    /// is `ShardSlot::detached()`, as at PR 1's DST call sites.
    fn run_real_flusher(
        shard_dir: PathBuf,
        work_rx: Receiver<Batch>,
        drain_tx: Sender<PathBuf>,
        metrics: Arc<Metrics>,
        handoff: handoff::HandoffState,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let _ = flusher_thread(
                0,
                shard_dir,
                work_rx,
                drain_tx,
                64,
                Duration::from_millis(5),
                64 * 1024 * 1024,
                None,
                None,
                None,
                metrics,
                Arc::new(AtomicU64::new(200)),
                &RealClock,
                Arc::new(FsSegmentStore),
                Compression::None,
                1,
                &crate::status::ShardSlot::detached(),
                handoff,
            );
        })
    }

    /// One `reason` series of `weir_wab_segments_not_handed_off_total` (README Q7).
    fn not_handed_off_for(metrics: &Metrics, reason: crate::metrics::NotHandedOffReason) -> u64 {
        metrics
            .wab_segments_not_handed_off
            .get_or_create(&crate::metrics::NotHandedOffLabel { reason })
            .get()
    }

    fn sealed_files(shard_dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = fs::read_dir(shard_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(EXT_SEALED))
            .collect();
        v.sort();
        v
    }

    /// Test A: a full bounded(1) channel nobody consumes, flag already set.
    /// The flusher returns within ~1 s; its segment is sealed with a valid
    /// footer; every ack resolves true (they were fsynced before the hand-off).
    #[test]
    fn hand_off_test_a_full_channel_and_flag_already_set_returns_within_the_budget() {
        let dir = tmp_dir("handoff_a");
        let sd = shard_dir_path(&dir, 0);
        crate::testutil::mkdir_p(&sd);
        let metrics = Arc::new(Metrics::new().0);
        let (drain_tx, drain_rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drain_tx.send(PathBuf::from("prefill")).unwrap();
        let handoff = handoff::HandoffState::new(
            0,
            Arc::new(AtomicBool::new(true)),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            None,
            Arc::clone(&metrics),
        );
        let (work_rx, acks) = one_batch_then_close(5);

        let t0 = Instant::now();
        run_real_flusher(sd.clone(), work_rx, drain_tx, Arc::clone(&metrics), handoff)
            .join()
            .unwrap();
        let took = t0.elapsed();

        assert!(
            took >= Duration::from_millis(900) && took < Duration::from_millis(1700),
            "the final hand-off must give up after the 1 s budget, took {took:?}"
        );
        for (i, mut rx) in acks.into_iter().enumerate() {
            assert!(rx.try_recv().unwrap().durable, "record {i}: acks precede and do not depend on the hand-off");
        }
        let sealed = sealed_files(&sd);
        assert_eq!(sealed.len(), 1);
        assert_eq!(read_segment_record_count(&sealed[0]).unwrap(), 5);
        assert_eq!(SegmentReader::open(&sealed[0]).unwrap().count(), 5);
        assert_eq!(not_handed_off_for(&metrics, crate::metrics::NotHandedOffReason::budget), 1);
        assert_eq!(metrics.wab_segments_not_handed_off_total(), 1, "budget only, nothing disconnected");
        assert_eq!(drain_rx.try_iter().collect::<Vec<_>>(), vec![PathBuf::from("prefill")]);
        fs::remove_dir_all(dir).ok();
    }

    /// Test B: the same channel, but the flag is raised 500 ms AFTER the
    /// flusher blocked: the blocked send re-reads it within one slice.
    #[test]
    fn hand_off_test_b_a_flag_raised_while_blocked_bounds_the_join() {
        let dir = tmp_dir("handoff_b");
        let sd = shard_dir_path(&dir, 0);
        crate::testutil::mkdir_p(&sd);
        let metrics = Arc::new(Metrics::new().0);
        let (drain_tx, _drain_rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drain_tx.send(PathBuf::from("prefill")).unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        let handoff = handoff::HandoffState::new(
            0,
            Arc::clone(&flag),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            None,
            Arc::clone(&metrics),
        );
        let (work_rx, acks) = one_batch_then_close(3);

        let t0 = Instant::now();
        let flusher = run_real_flusher(sd.clone(), work_rx, drain_tx, Arc::clone(&metrics), handoff);
        thread::sleep(Duration::from_millis(500));
        assert!(!flusher.is_finished(), "outside shutdown the hand-off must keep blocking");
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
        flusher.join().unwrap();
        let took = t0.elapsed();

        assert!(
            took >= Duration::from_millis(1400) && took < Duration::from_millis(2400),
            "≈500 ms + 1 s budget (+ ≤ one slice), took {took:?}"
        );
        for mut rx in acks {
            assert!(rx.try_recv().unwrap().durable);
        }
        assert_eq!(sealed_files(&sd).len(), 1);
        assert_eq!(not_handed_off_for(&metrics, crate::metrics::NotHandedOffReason::budget), 1);
        assert_eq!(metrics.wab_segments_not_handed_off_total(), 1);
        fs::remove_dir_all(dir).ok();
    }

    /// Rotation goes through the hand-off too, and the shard's blocked marker
    /// is set while it waits and cleared when the slow drain catches up.
    #[test]
    fn a_rotation_waits_out_a_slow_drain_and_marks_then_clears_the_shard() {
        #[derive(Default)]
        struct Marks(std::sync::Mutex<Vec<(u16, u64)>>);
        impl handoff::HandoffMarker for Marks {
            fn set_handoff_blocked_since_ms(&self, shard: u16, ms: u64) {
                self.0.lock().unwrap().push((shard, ms));
            }
        }
        let dir = tmp_dir("handoff_rotation");
        let sd = shard_dir_path(&dir, 0);
        crate::testutil::mkdir_p(&sd);
        let metrics = Arc::new(Metrics::new().0);
        let (drain_tx, drain_rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drain_tx.send(PathBuf::from("prefill")).unwrap();
        let marks = Arc::new(Marks::default());
        let handoff = handoff::HandoffState::new(
            0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            Some(Arc::clone(&marks) as Arc<dyn handoff::HandoffMarker>),
            Arc::clone(&metrics),
        );
        // 32-byte records over a 100-byte segment rotate every second record.
        let (work_rx, acks) = one_batch_then_close(4);
        let consumer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(350));
            drain_rx.iter().collect::<Vec<_>>()
        });
        let flusher = thread::spawn({
            let sd = sd.clone();
            let metrics = Arc::clone(&metrics);
            move || {
                let _ = flusher_thread(
                    0, sd, work_rx, drain_tx, 64, Duration::from_millis(5), 100,
                    None, None, None, metrics, Arc::new(AtomicU64::new(200)),
                    &RealClock, Arc::new(FsSegmentStore), Compression::None, 1,
                    &crate::status::ShardSlot::detached(), handoff,
                );
            }
        });
        flusher.join().unwrap();
        let delivered = consumer.join().unwrap();

        assert_eq!(metrics.wab_segments_not_handed_off_total(), 0, "outside shutdown nothing is given up");
        assert_eq!(delivered.len(), 1 + sealed_files(&sd).len(), "prefill + every sealed segment");
        for mut rx in acks {
            assert!(rx.try_recv().unwrap().durable);
        }
        let marks = marks.0.lock().unwrap().clone();
        assert!(marks.first().is_some_and(|(s, ms)| *s == 0 && *ms > 0), "{marks:?}");
        assert_eq!(marks.last(), Some(&(0, 0)), "the marker must end cleared: {marks:?}");
        fs::remove_dir_all(dir).ok();
    }
```

(`super::*` already brings `Sender`, `Receiver`, `AtomicBool`, `AtomicU64`, `Batch`, `AckOutcome`, `oneshot`, `Durability`, `thread`, `Instant`, `EXT_SEALED`, `SegmentReader`, `FsSegmentStore`, `RealClock` and the `handoff` module; `AtomicUsize` is spelled out in full.) Both calls pass PR 1's `&ShardSlot::detached()` before the new `handoff` argument.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server wab::tests::hand_off_test -- --test-threads=1`
Expected: compile error `this function takes 17 arguments but 18 arguments were supplied` at the `flusher_thread` calls (16 original parameters plus PR 1's `slot`).

- [ ] **Step 3: Implement**

`crates/weir-server/src/wab/mod.rs`:

```rust
use handoff::{HandoffConfig, HandoffSite, HandoffState, hand_off};
```

`seal_before_size_threshold` gains `clock: &C, handoff: &HandoffState` and becomes generic `<C: BlockingClock>` (7 parameters, within clippy's limit); its `Ok(Some(sealed))` arm replaces the `drain_tx.send` block (766-774) with the following. The `reason` (`TimedSeal::as_str()`, `"idle"` / `"lifetime"`) that the replaced `error!` logged travels in the site, so `hand_off`'s log lines keep it:

```rust
            // Sealed (and fsynced) above; see wab::handoff for why any outcome
            // of the hand-off leaves this segment's acks true.
            let _ = hand_off(
                clock,
                drain_tx,
                sealed,
                handoff,
                HandoffSite::TimedSeal { reason: reason.as_str() },
            );
            true
```

`flusher_thread` gains the final parameter `handoff: HandoffState`. The two `seal_before_size_threshold(...)` calls (938-944 and 998) append `clock, &handoff`. The two `flush_batch(...)` calls (917-924, 966-973) append `clock, &handoff`. The shutdown seal (1014-1016) becomes:

```rust
            let _ = hand_off(clock, &drain_tx, sealed, &handoff, HandoffSite::ShutdownSeal);
```

`flush_batch` (no allow today) gets the attribute and the two new parameters:

```rust
#[allow(clippy::too_many_arguments)]
fn flush_batch<C: BlockingClock>(
    writer: &mut ShardWriter,
    batches: &mut Vec<Batch>,
    drain_tx: &Sender<PathBuf>,
    shard_id: u16,
    metrics: &Arc<Metrics>,
    coalesce_hint: &Arc<AtomicU64>,
    clock: &C,
    handoff: &HandoffState,
) {
```

and the rotation block (1149-1151) becomes:

```rust
                // INVARIANT (wab::handoff): hand off BEFORE promoting the acks
                // below, and only a path the seal (with its fsync) returned.
                let _ = hand_off(clock, drain_tx, sealed, handoff, HandoffSite::Rotation);
                durable_acks.append(&mut pending_acks);
```

`spawn_flushers` gains the final parameter `handoff: HandoffConfig`. Inside the per-shard loop, before `thread::Builder::new()`:

```rust
        // One per flusher THREAD, outside the respawn factory: the shutdown
        // budget is per flusher, shared by every respawn (wab::handoff).
        let handoff_for_flusher = handoff.for_shard(shard_id as u16, Arc::clone(&metrics));
```

and inside the factory closure, beside the other per-attempt clones, `let handoff = handoff_for_flusher.clone();`, passed as the last `flusher_thread` argument.

DST call sites (`crates/weir-server/src/wab/dst.rs`), each appending one argument after the `&ShardSlot::detached()` PR 1 already passes there:
- `drive_sync_flusher_with` (924): before the `std::thread::spawn`, `let handoff = super::handoff::HandoffState::detached(0, Arc::clone(&env.metrics));`; pass `handoff`.
- `run_panic_during_flush` (1160): before the spawn, `let handoff = super::handoff::HandoffState::detached(0, Arc::clone(&env.metrics));`; in the factory, `let handoff = handoff.clone();`; pass `handoff`.
- `run_interleaved_flush` (1474): before the flusher spawn, `let handoff = super::handoff::HandoffState::detached(0, Arc::clone(&env.metrics));`; pass `handoff`.
- `drive_rotating_batch` (1637): `let handoff = crate::wab::handoff::HandoffState::detached(0, Arc::clone(&env.metrics));`; pass `handoff`.

All four keep their unbounded drain channels, so `try_send` always succeeds and their behaviour is unchanged.

`crates/weir-server/src/main.rs`, before the `spawn_flushers` call:

```rust
    // The flushers' shutdown flag (spec §5.5). The Lifecycle (PR 4) owns it and
    // sets it at T0; until then nothing sets it, so every hand-off behaves as
    // the blocking send it replaces.
    let flushers_shutting_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handoff = wab::handoff::HandoffConfig {
        shutting_down: Arc::clone(&flushers_shutting_down),
        drain_depth: Arc::clone(&board.drain_depth),
        marker: Some(Arc::clone(&board) as Arc<dyn wab::handoff::HandoffMarker>),
    };
```

and `handoff` is passed as the last argument of `wab::spawn_flushers`. (`board` is PR 1's `Arc<StatusBoard>`, created before the WAB in PR 1's main.)

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server wab:: -- --test-threads=1`
Expected: the three new tests pass, all existing `wab::` tests pass.
Run: `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`
Expected: every scenario, pinned seed and the sweep pass unchanged.
Run: `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` and `cargo clippy -p weir-server --all-targets --no-default-features -- -D warnings`
Expected: clean (no dead code left in `wab::handoff`, and no `too_many_arguments` on `flush_batch`).

- [ ] **Step 5: Commit**

`git commit -m "feat(wab): every flusher hand-off goes through the bounded hand-off loop"` (body: names the three sites, the invariant, tests A and B, and that nothing sets the flag until PR 4.)

---

### Task 3.5: Bounded-channel DST seed family

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/wab/dst.rs`: `SimClock` (143-169), `Scenario` (556-608), `SimSpec::run` (641-678), `SimReport` (726-756), new driver `run_bounded_handoff`, tests module (`replay_pinned_regression_seeds` unchanged; `sweep_random_seeds` 2011-2072 gains one run)
- Create: `crates/weir-server/tests/dst_seeds/bounded_handoff_shutdown_from_start.json`, `crates/weir-server/tests/dst_seeds/bounded_handoff_flip_mid_block.json`, `crates/weir-server/tests/dst_seeds/bounded_handoff_slow_drain.json`
- Test: `crates/weir-server/src/wab/dst.rs` tests module

**Interfaces:**
- Consumes: `SimClock::{send_timeout, now}` (Task 3.1); `HandoffState::new`, `HANDOFF_SLICE`, `FINAL_HANDOFF_TIMEOUT` (Task 3.2); `flusher_thread(.., handoff)` (Task 3.4); `Metrics::wab_segments_not_handed_off_total()` (PR 1 Task 1.18, the sum over both `reason` series).
- Produces:
  - `pub struct SimHandoffHook` (fields private) with `fn new(flip_after: Option<u64>, shutting_down: Arc<AtomicBool>, consume_every: Option<u64>, drain_rx: Receiver<PathBuf>) -> Arc<Self>`
  - `SimClock::with_handoff_hook(hook: Arc<SimHandoffHook>) -> SimClock`
  - `Scenario::BoundedHandoff { records: usize, capacity: usize, flip_after: Option<u64>, consume_every: Option<u64> }`
  - `SimReport` fields `handed_off: usize`, `not_handed_off: u64`, `handoff_timeouts_after_flip: u64`
  - invariants `i_handoff_acks_true`, `i_handoff_no_sealed_segment_lost`, `i_handoff_backpressure_unchanged`, `i_handoff_bounded`

- [ ] **Step 1: Write the failing tests and pin the seeds**

Append to `mod tests` in `dst.rs`:

```rust
    /// Spec §15: "a deterministic DST seed family with a bounded channel
    /// through SimClock::send_timeout". Flag set before the first send: every
    /// sealed segment is either handed off or counted, the acks are all true,
    /// and the hand-off gives up within the budget.
    #[test]
    fn bounded_handoff_with_shutdown_from_the_start_is_bounded_and_loses_nothing() {
        let r = Sim::new(0xB0_0001)
            .scenario(Scenario::BoundedHandoff { records: 6, capacity: 1, flip_after: Some(0), consume_every: None })
            .run();
        assert!(r.acks.iter().all(|a| *a));
        assert_eq!(r.handed_off, 0, "nothing consumes, so nothing is handed off");
        assert!(r.not_handed_off >= 3, "6 records rotate into ≥ 3 sealed segments");
        assert!(r.handoff_timeouts_after_flip <= super::budget_slices() + r.not_handed_off);
    }

    /// The flag flips after the flusher has already been blocked for 7 slices.
    #[test]
    fn bounded_handoff_flag_raised_mid_block_is_honoured() {
        let r = Sim::new(0xB0_0002)
            .scenario(Scenario::BoundedHandoff { records: 4, capacity: 2, flip_after: Some(7), consume_every: None })
            .run();
        assert!(r.acks.iter().all(|a| *a));
        assert!(r.not_handed_off >= 1);
    }

    /// No shutdown, a drain that takes one item every third slice: backpressure
    /// only, every segment delivered, nothing given up.
    #[test]
    fn bounded_handoff_without_shutdown_only_applies_backpressure() {
        let r = Sim::new(0xB0_0003)
            .scenario(Scenario::BoundedHandoff { records: 8, capacity: 1, flip_after: None, consume_every: Some(3) })
            .run();
        assert_eq!(r.not_handed_off, 0);
        assert!(r.handed_off >= 4);
        assert!(r.acks.iter().all(|a| *a));
    }

    #[test]
    fn bounded_handoff_is_seed_deterministic() {
        let spec = |seed| Sim::new(seed)
            .scenario(Scenario::BoundedHandoff { records: 7, capacity: 2, flip_after: Some(5), consume_every: Some(4) })
            .run();
        let (a, b) = (spec(0xB0_0004), spec(0xB0_0004));
        assert_eq!((a.handed_off, a.not_handed_off, a.handoff_timeouts_after_flip),
                   (b.handed_off, b.not_handed_off, b.handoff_timeouts_after_flip));
    }
```

In `sweep_random_seeds`, after the `InterleavedFlush` run, add:

```rust
            // Bounded drain channel through the timed hand-off: a shutdown that
            // is set from the start, raised mid-block, or never raised (then a
            // slow drain must exist, or the flusher correctly blocks forever).
            let capacity = 1 + ((seed >> 40) % 3) as usize;
            let flip_after = match (seed >> 44) % 3 {
                0 => None,
                1 => Some(0),
                _ => Some(1 + (seed >> 48) % 40),
            };
            let consume_every = match (flip_after, (seed >> 54) & 1) {
                (None, _) | (_, 1) => Some(1 + (seed >> 56) % 5),
                _ => None,
            };
            Sim::new(seed)
                .scenario(Scenario::BoundedHandoff { records: records + 1, capacity, flip_after, consume_every })
                .compressed(compressed)
                .run();
```

Create the three seed files:

`crates/weir-server/tests/dst_seeds/bounded_handoff_shutdown_from_start.json`
```json
{
  "seed": 11534337,
  "scenario": { "BoundedHandoff": { "records": 6, "capacity": 1, "flip_after": 0, "consume_every": null } },
  "faults": []
}
```

`crates/weir-server/tests/dst_seeds/bounded_handoff_flip_mid_block.json`
```json
{
  "seed": 11534338,
  "scenario": { "BoundedHandoff": { "records": 4, "capacity": 2, "flip_after": 7, "consume_every": null } },
  "faults": []
}
```

`crates/weir-server/tests/dst_seeds/bounded_handoff_slow_drain.json`
```json
{
  "seed": 11534339,
  "scenario": { "BoundedHandoff": { "records": 8, "capacity": 1, "flip_after": null, "consume_every": 3 } },
  "faults": [],
  "compressed": true
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`
Expected: compile error `no variant named `BoundedHandoff` found for enum `Scenario`` (and, once that compiles, `replay_pinned_regression_seeds` would fail to deserialise the three new JSON files).

- [ ] **Step 3: Implement**

`SimClock` gains an optional hook (still `Clone + Default`):

```rust
#[derive(Clone, Default)]
pub struct SimClock {
    virtual_nanos: Arc<AtomicU64>,
    handoff: Option<Arc<SimHandoffHook>>,
}

impl SimClock {
    /// A SimClock whose full-channel sends drive `hook`: a deterministic
    /// shutdown flip and a deterministic slow drain, both counted in slices.
    pub fn with_handoff_hook(hook: Arc<SimHandoffHook>) -> Self {
        SimClock { virtual_nanos: Arc::default(), handoff: Some(hook) }
    }
}
```

and in `SimClock::send_timeout`'s `Full` arm, after the virtual-time charge: `if let Some(hook) = &self.handoff { hook.on_timeout(); }`.

```rust
/// Drives a bounded-channel hand-off deterministically, in units of timed-out
/// slices: after `flip_after` slices it raises the flusher's `shutting_down`
/// flag (SIGTERM arriving while the flusher is blocked); every `consume_every`
/// slices it takes one item off the drain channel (a slow drain).
pub struct SimHandoffHook {
    timeouts: AtomicU64,
    flip_after: Option<u64>,
    /// Timeouts counted when the flag went up; u64::MAX = never.
    flipped_at: AtomicU64,
    shutting_down: Arc<AtomicBool>,
    consume_every: Option<u64>,
    drain_rx: Receiver<PathBuf>,
    consumed: Mutex<Vec<PathBuf>>,
}

impl SimHandoffHook {
    pub fn new(
        flip_after: Option<u64>,
        shutting_down: Arc<AtomicBool>,
        consume_every: Option<u64>,
        drain_rx: Receiver<PathBuf>,
    ) -> Arc<Self> {
        if flip_after == Some(0) {
            shutting_down.store(true, SeqCst);
        }
        Arc::new(SimHandoffHook {
            timeouts: AtomicU64::new(0),
            flip_after,
            flipped_at: AtomicU64::new(if flip_after == Some(0) { 0 } else { u64::MAX }),
            shutting_down,
            consume_every,
            drain_rx,
            consumed: Mutex::new(Vec::new()),
        })
    }

    fn on_timeout(&self) {
        let n = self.timeouts.fetch_add(1, SeqCst) + 1;
        if let Some(k) = self.flip_after
            && n >= k
            && !self.shutting_down.swap(true, SeqCst)
        {
            self.flipped_at.store(n, SeqCst);
        }
        if let Some(e) = self.consume_every
            && n % e == 0
            && let Ok(p) = self.drain_rx.try_recv()
        {
            self.consumed.lock().unwrap().push(p);
        }
    }

    fn timeouts_after_flip(&self) -> u64 {
        match self.flipped_at.load(SeqCst) {
            u64::MAX => 0,
            at => self.timeouts.load(SeqCst) - at,
        }
    }
}
```

Scenario variant (append to `enum Scenario`):

```rust
    /// The real flusher hands sealed segments to a BOUNDED drain channel of
    /// `capacity`, pre-filled to capacity, through `SimClock::send_timeout`.
    /// 50-byte records over a 100-byte segment rotate every second record.
    /// `flip_after`: slices until `shutting_down` is raised (`Some(0)` = set
    /// before the first send, `None` = never). `consume_every`: a slow drain
    /// takes one item every that many slices (`None` = nobody consumes).
    /// At least one of the two must be set: with neither, the flusher blocks
    /// forever, which is backpressure working as designed.
    BoundedHandoff {
        records: usize,
        capacity: usize,
        flip_after: Option<u64>,
        consume_every: Option<u64>,
    },
```

`SimSpec::run` arm:

```rust
            Scenario::BoundedHandoff { records, capacity, flip_after, consume_every } => {
                run_bounded_handoff(self.seed, *records, *capacity, *flip_after, *consume_every, self.compression())
            }
```

`SimReport` gains (documented as `BoundedHandoff:`) `pub handed_off: usize`, `pub not_handed_off: u64`, `pub handoff_timeouts_after_flip: u64`.

Driver (with the budget expressed in slices for the invariants; the hand-off itself measures wall time on `SimClock::now()`, which a full channel's slice charge, a flusher wait or a sleep advances, and any advance besides the slice charges only shortens the budget, so the bound below is an upper bound):

```rust
/// The 1 s shutdown budget in 100 ms slices: the most timed-out slices one
/// flusher can spend before its budget is gone.
fn budget_slices() -> u64 {
    (super::handoff::FINAL_HANDOFF_TIMEOUT.as_nanos() / super::handoff::HANDOFF_SLICE.as_nanos()) as u64
}

/// Scenario — the timed hand-off against a bounded drain channel (spec §15).
fn run_bounded_handoff(
    seed: u64,
    records: usize,
    capacity: usize,
    flip_after: Option<u64>,
    consume_every: Option<u64>,
    compression: Compression,
) -> SimReport {
    assert!(capacity >= 1, "a bounded channel has room for at least one item");
    assert!(
        flip_after.is_some() || consume_every.is_some(),
        "with neither a shutdown nor a consumer the flusher blocks forever (backpressure)"
    );
    let env = SimEnv::new("bounded_handoff");
    let ledger = Ledger::default();
    let store: Arc<dyn SegmentStore> =
        Arc::new(SimSegmentStore::new(SimFaults::from_faults(&[]), ledger.clone()));

    let (drain_tx, drain_rx) = crossbeam_channel::bounded::<PathBuf>(capacity);
    for i in 0..capacity {
        drain_tx.send(PathBuf::from(format!("prefill-{i}"))).unwrap();
    }
    let shutting_down = Arc::new(AtomicBool::new(false));
    let hook = SimHandoffHook::new(flip_after, Arc::clone(&shutting_down), consume_every, drain_rx.clone());
    let clock = SimClock::with_handoff_hook(Arc::clone(&hook));
    let handoff = super::handoff::HandoffState::new(
        0,
        Arc::clone(&shutting_down),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        None,
        Arc::clone(&env.metrics),
    );

    // Fixed 50-byte payloads (index prefix + seed fill) so rotation is
    // deterministic: two records per 100-byte segment.
    let mut rng = SplitMix64::new(seed);
    let (work_tx, work_rx) = crossbeam_channel::bounded::<Batch>(records.max(1) * 4);
    let mut payloads = Vec::with_capacity(records);
    let mut ack_rxs = Vec::with_capacity(records);
    let mut units = Vec::with_capacity(records);
    for i in 0..records {
        let mut p = (i as u64).to_be_bytes().to_vec();
        p.extend((0..42).map(|_| (rng.next_u64() & 0xFF) as u8));
        let (ack_tx, ack_rx) = oneshot::channel();
        ack_rxs.push(ack_rx);
        units.push(make_unit(p.clone(), Durability::Durable, ack_tx));
        payloads.push(p);
    }
    work_tx.send(make_batch(units)).expect("send batch");
    drop(work_tx);

    let shard_dir = env.shard_dir(0);
    let metrics = Arc::clone(&env.metrics);
    std::thread::spawn(move || {
        let _ = super::flusher_thread(
            0, shard_dir, work_rx, drain_tx, records.max(1), Duration::from_millis(5), 100,
            None, None, None, metrics, Arc::new(AtomicU64::new(200)), &clock, store,
            compression, 1, &crate::status::ShardSlot::detached(), handoff,
        );
    })
    .join()
    .expect("flusher join");

    let acks: Vec<bool> = ack_rxs
        .into_iter()
        .map(|mut rx| rx.try_recv().map(|o| o.durable).unwrap_or(false))
        .collect();
    for (p, &acked) in payloads.iter().zip(&acks) {
        if acked {
            assert_invariant(seed, "i1_acked_true_is_durable", ledger.is_durable_logical(p, compression));
        }
    }
    // No faults: every Durable record is fsynced before any hand-off, so the
    // hand-off outcome can never turn an ack false.
    assert_invariant(seed, "i_handoff_acks_true", acks.iter().all(|a| *a));

    let mut handed: Vec<PathBuf> = hook.consumed.lock().unwrap().clone();
    handed.extend(drain_rx.try_iter());
    handed.retain(|p| !p.to_string_lossy().starts_with("prefill-"));
    let mut on_disk: Vec<PathBuf> = std::fs::read_dir(env.shard_dir(0))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
        .collect();
    on_disk.sort();
    // Both reasons: a slot that never frees is `budget`; nothing here closes
    // the channel, so `disconnected` stays 0, but the invariant is about every
    // segment left for replay, whatever the reason.
    let not_handed_off = env.metrics.wab_segments_not_handed_off_total();

    // Every sealed segment is handed off exactly once, or counted as left for
    // replay: nothing durable is silently dropped, nothing is sent twice.
    let mut handed_sorted = handed.clone();
    handed_sorted.sort();
    handed_sorted.dedup();
    assert_invariant(
        seed,
        "i_handoff_no_sealed_segment_lost",
        handed_sorted.len() == handed.len()
            && handed.iter().all(|p| on_disk.contains(p))
            && handed.len() as u64 + not_handed_off == on_disk.len() as u64,
    );
    // Outside shutdown the loop is a blocking send: it never gives up.
    if flip_after.is_none() {
        assert_invariant(seed, "i_handoff_backpressure_unchanged", not_handed_off == 0);
    }
    // After the flag: ≤ the per-flusher budget, plus one slice per later
    // hand-off (spec §5.5 "≤ 1 s + 100 ms per remaining rotation").
    let after = hook.timeouts_after_flip();
    if flip_after.is_some() {
        assert_invariant(
            seed,
            "i_handoff_bounded",
            after <= budget_slices() + on_disk.len() as u64,
        );
    }

    let recovered = read_sealed_payloads(&env.shard_dir(0));
    env.cleanup();
    SimReport {
        seed,
        acks,
        written: payloads,
        recovered,
        handed_off: handed.len(),
        not_handed_off,
        handoff_timeouts_after_flip: after,
        ..Default::default()
    }
}
```

(`dst.rs` imports gain `std::sync::atomic::AtomicBool` — already imported — and `Mutex` — already imported.)

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1`
Expected: the four new tests pass; `replay_pinned_regression_seeds` replays 11 seeds (8 + 3); the sweep passes.
Run: `WEIR_DST_SWEEP=300 cargo test -p weir-server --bin weir-server --features dst "wab::dst::tests::sweep_random_seeds" -- --test-threads=1`
Expected: pass. Then mutate `hand_off` to `return HandOff::Delivered` on `Timeout` while shutting down (drop the segment): `i_handoff_no_sealed_segment_lost` must fire with a `WEIR_DST_SEED=` repro. Restore.

- [ ] **Step 5: Commit**

`git commit -m "test(dst): a bounded drain channel seed family for the hand-off loop"`

---

### Task 3.6: Drain-channel depth and capacity

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/wab/mod.rs` (new `pub(crate) const DRAIN_CHANNEL_CAPACITY: usize = 256;` beside `MAX_FLUSHER_RESPAWNS`)
- Modify: `crates/weir-server/src/metrics/mod.rs` (struct at 304-…, registrations in `Metrics::new` after `queue_depth` at 720-724, struct literal at ~987, `all_metric_names_present_in_output` at 1166-1205)
- Modify: `crates/weir-server/src/status/poll.rs` (PR 1's 500 ms task)
- Modify: `crates/weir-server/src/main.rs`: the `let (drain_tx, drain_rx) = crossbeam_channel::bounded::<std::path::PathBuf>(256);` line (:1046 at 3adc5f5)
- Modify: `docs/monitoring.md` (`### Drain / sink` table of `## Metric reference`)
- Modify: `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`, ~:795: its `expected` vec)
- Test: `crates/weir-server/src/metrics/mod.rs` tests; `crates/weir-server/tests/docs_drift.rs` (`every_registered_metric_is_in_the_monitoring_reference`, unchanged); `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`)

**Interfaces:**
- Consumes: `StatusBoard::drain_depth: Arc<AtomicUsize>` (PR 1); PR 1's 500 ms poll loop.
- Produces:
  - `pub(crate) const DRAIN_CHANNEL_CAPACITY: usize = 256;` in `crate::wab`
  - `Metrics::drain_channel_depth: Gauge<f64, AtomicU64>` (`weir_drain_channel_depth`), `Metrics::drain_channel_capacity: Gauge<f64, AtomicU64>` (`weir_drain_channel_capacity`)
  - `impl Metrics { pub(crate) fn publish_drain_channel_depth(&self, depth: usize) }`

- [ ] **Step 1: Write the failing test**

Append to the tests module of `crates/weir-server/src/metrics/mod.rs`:

```rust
    #[test]
    fn drain_channel_depth_and_capacity_are_exported() {
        let (m, reg) = Metrics::new();
        m.drain_channel_capacity
            .set(crate::wab::DRAIN_CHANNEL_CAPACITY as f64);
        m.publish_drain_channel_depth(7);
        let out = encode_to_string(&reg);
        assert!(out.contains("weir_drain_channel_depth 7"), "{out}");
        assert!(out.contains("weir_drain_channel_capacity 256"), "{out}");
        // PR 1's board holds DEPTH_UNPUBLISHED (usize::MAX) until the first
        // hand-off or drain slice stores a real len(): never export it.
        m.publish_drain_channel_depth(crate::status::board::DEPTH_UNPUBLISHED);
        let out = encode_to_string(&reg);
        assert!(out.contains("weir_drain_channel_depth 7"), "the unpublished sentinel leaked: {out}");
    }
```

and add `"weir_drain_channel_depth"` and `"weir_drain_channel_capacity"` to the `expected` array of `all_metric_names_present_in_output`.

`crates/weir-server/tests/system.rs`, `metrics_all_families_registered`: in its `expected` vec, under the `// Sink / drain` comment (after `"weir_queue_depth",`), add:

```rust
        // PR 3 (Task 3.6)
        "weir_drain_channel_depth",
        "weir_drain_channel_capacity",
```

That test asserts that the number of `# HELP weir_` lines equals `expected.len()`, so it fails without these two names once Step 3 registers the families (it is the system-level drift detector; the unit list above only checks presence).

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-server --bin weir-server metrics::tests::drain_channel -- --test-threads=1`
Expected: compile error `no field `drain_channel_capacity` on type `Metrics``.

- [ ] **Step 3: Implement**

`crates/weir-server/src/wab/mod.rs`:

```rust
/// Capacity of the flusher → drain channel. It is the implicit disk bound
/// between seal and delivery (spec §18 SP-H invariant): outside shutdown no
/// flusher sends without blocking, so at most this many sealed segments wait.
pub(crate) const DRAIN_CHANNEL_CAPACITY: usize = 256;
```

`crates/weir-server/src/metrics/mod.rs`, struct fields beside `queue_depth`:

```rust
    /// Sealed segments waiting in the flusher → drain channel. Last-writer-wins
    /// `len()` snapshots from the flushers (after each hand-off) and the drain
    /// (each wait slice), published by the 500 ms poll task.
    pub drain_channel_depth: Gauge<f64, AtomicU64>,
    /// Capacity of that channel; constant `wab::DRAIN_CHANNEL_CAPACITY`.
    pub drain_channel_capacity: Gauge<f64, AtomicU64>,
```

registrations after `queue_depth`:

```rust
        let drain_channel_depth = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_channel_depth",
            "Sealed segments waiting in the bounded flusher-to-drain channel (snapshot)"
        );
        let drain_channel_capacity = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_channel_capacity",
            "Capacity of the flusher-to-drain channel"
        );
```

both added to the `Metrics { .. }` literal, and:

```rust
impl Metrics {
    /// Called by the 500 ms poll task with `board.drain_depth`. Skips PR 1's
    /// `DEPTH_UNPUBLISHED` sentinel (usize::MAX, the value before the first
    /// flusher hand-off or drain slice), which is "unknown", not a depth.
    pub(crate) fn publish_drain_channel_depth(&self, depth: usize) {
        if depth != crate::status::board::DEPTH_UNPUBLISHED {
            self.drain_channel_depth.set(depth as f64);
        }
    }
}
```

`crates/weir-server/src/status/poll.rs`: in PR 1's 500 ms task body, directly after the line that sets `metrics.queue_depth`, add:

```rust
        metrics.publish_drain_channel_depth(board.drain_depth.load(std::sync::atomic::Ordering::Relaxed));
```

`crates/weir-server/src/main.rs`, the `bounded::<std::path::PathBuf>(256)` line:

```rust
    let (drain_tx, drain_rx) =
        crossbeam_channel::bounded::<std::path::PathBuf>(wab::DRAIN_CHANNEL_CAPACITY);
    metrics
        .drain_channel_capacity
        .set(wab::DRAIN_CHANNEL_CAPACITY as f64);
```

`docs/monitoring.md`, in the `### Drain / sink` table:

```markdown
| `weir_drain_channel_depth` | gauge | Sealed segments waiting in the bounded flusher-to-drain channel (capacity `weir_drain_channel_capacity`). A recent snapshot, written by each flusher after a hand-off and by the drain on every wait slice, published every 500 ms. Sustained at capacity means the drain is the bottleneck: flushers block at rotation, and producers can see ACK_TIMEOUT NACKs for records that are already durable. Check `weir_wab_handoff_blocked_seconds` and sink latency before suspecting storage. |
| `weir_drain_channel_capacity` | gauge | Capacity of the flusher-to-drain channel (256). Use it for a `depth / capacity` panel. |
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-server --bin weir-server metrics:: -- --test-threads=1`
Expected: pass, including `all_metric_names_present_in_output`.
Run: `cargo test -p weir-server --test docs_drift every_registered_metric_is_in_the_monitoring_reference`
Expected: pass (it fails with `missing: ["weir_drain_channel_depth", "weir_drain_channel_capacity"]` if the monitoring rows are skipped — check that by removing one row once, then restore).
Run: `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1`
Expected: pass.

- [ ] **Step 5: Commit**

`git commit -m "feat(metrics): drain channel depth and capacity"`

---

### Task 3.7: `sink/select.rs` — `DrainSpawner` (the SP-M seam)

PR 3 runs Tasks 3.1-3.6 and 3.8-3.12 in its worktree. Task 3.7 starts only after PR 2 has merged and PR 3 is rebased onto main.

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Create: `crates/weir-server/src/sink/select.rs`
- Modify: `crates/weir-server/src/sink/mod.rs:19-30` (add `pub(crate) mod select;`)
- Modify: `crates/weir-server/src/main.rs`: remove `require_sink_url` (the fn after `spawn_lock_integrity_thread`, with its doc and `#[cfg(any(..))]`) and `build_and_spawn_drain` (:849-881 at 3adc5f5); replace the `let drain_handle = match config.sink_type { .. };` block (:1162-1415 at 3adc5f5); imports: `use config::{Config, SinkType};` (:55) and the per-sink `use sink::..` lines (:58-66)
- Test: `crates/weir-server/src/sink/select.rs` tests module

**Interfaces:**
- Consumes: `crate::config::{Config, SinkType}`, `crate::drain::{self, DrainConfig, DrainHeartbeat, DrainRuntime, DrainExit}`, PR 2's `drain::spawn_with<S: Sink + 'static>(Receiver<PathBuf>, Arc<S>, DrainConfig, Arc<Metrics>, DrainHeartbeat, DrainRuntime) -> JoinHandle<DrainExit>` (PR 2 merges first; `drain::spawn` is `#[cfg(test)]` from PR 2 on), PR 2's `drain_runtime` local in `main` (PR 2 Task 2.17), every built-in sink constructor as main uses them today.
- Produces:
  - `pub(crate) struct DrainSpawnArgs { pub(crate) rx: crossbeam_channel::Receiver<PathBuf>, pub(crate) config: DrainConfig, pub(crate) metrics: Arc<Metrics>, pub(crate) heartbeat: DrainHeartbeat, pub(crate) runtime: drain::DrainRuntime }`
  - `pub(crate) struct DrainSpawner` with `fn from_sink<S: Sink + 'static>(sink_type: SinkType, sink: S) -> DrainSpawner`, `fn sink_type(&self) -> SinkType`, `fn spawn(self, args: DrainSpawnArgs) -> std::thread::JoinHandle<drain::DrainExit>`
  - `pub(crate) fn build_drain_spawner(config: &Config, metrics: &Arc<Metrics>) -> Result<DrainSpawner, Box<dyn std::error::Error>>`

- [ ] **Step 1: Write the failing test**

`crates/weir-server/src/sink/select.rs` tests module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PartialConfig;
    use std::time::{Duration, Instant};

    fn noop_config(label: &str) -> Config {
        let dir = crate::testutil::scratch_dir(&format!("select_{label}"));
        crate::testutil::mkdir_p(&dir);
        Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir),
                sink_type: Some("noop".to_string()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .map_err(|e| e.to_string())
        .unwrap()
    }

    fn drain_config(wab_dir: PathBuf) -> DrainConfig {
        DrainConfig {
            wab_dir,
            dead_letter_max_bytes: 1 << 20,
            dead_letter_check_interval: Duration::from_secs(1),
            base_retry_delay: Duration::from_millis(10),
            max_retries: 1,
            max_respawns: 1,
            commit_timeout: Duration::from_secs(60),
            health_poll_interval: Duration::from_secs(1),
            health_probe_timeout: Duration::from_secs(1),
            orphan_cleanup: false,
            local_read_escalation_min_age: drain::LOCAL_READ_ESCALATION_MIN_AGE,
        }
    }

    #[test]
    fn the_noop_spawner_names_its_sink_and_runs_a_drain_that_exits_on_disconnect() {
        let config = noop_config("noop");
        let metrics = Arc::new(Metrics::new().0);
        let spawner = build_drain_spawner(&config, &metrics).unwrap();
        assert_eq!(spawner.sink_type(), SinkType::Noop);

        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(crate::wab::DRAIN_CHANNEL_CAPACITY);
        let handle = spawner.spawn(DrainSpawnArgs {
            rx,
            config: drain_config(config.wab_dir.clone()),
            metrics,
            heartbeat: DrainHeartbeat::new(),
            runtime: drain::DrainRuntime::detached(),
        });
        drop(tx); // detached drain: Disconnected with nothing pending = exit
        let deadline = Instant::now() + Duration::from_secs(5);
        while !handle.is_finished() {
            assert!(Instant::now() < deadline, "the drain did not exit after its channel closed");
            std::thread::sleep(Duration::from_millis(10));
        }
        let exit = handle.join().unwrap();
        assert_eq!(exit.reason, crate::status::DrainExitReason::Stopped, "{exit:?}");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p weir-server --bin weir-server sink::select -- --test-threads=1`
Expected: compile error `unresolved import`/`cannot find function `build_drain_spawner`` (module body not written yet).

- [ ] **Step 3: Implement**

`crates/weir-server/src/sink/mod.rs`: `pub(crate) mod select;` beside `pub mod noop;`.

`crates/weir-server/src/sink/select.rs` (above the tests):

```rust
//! Sink selection (spec §5.12): config → a built sink → a [`DrainSpawner`].
//!
//! A mechanical move out of `main`. The sink is still built after recovery.
//! This is the SP-M (sink hot-swap) seam: a spawner is a value, so a later
//! sub-project can build a second one, stop the old drain and hand it the same
//! Receiver. Invariant (spec §18): the drain Receiver is never cloned; it moves
//! through `DrainSpawnArgs` into exactly one drain.

use std::{path::PathBuf, sync::Arc};

// Only the feature-gated sink arms use these; a noop-only build
// (`--no-default-features`) would otherwise fail clippy on unused imports.
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink",
    feature = "s3-sink"
))]
use std::time::Duration;
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink",
    feature = "s3-sink"
))]
use tracing::info;
use tracing::warn;

use crate::config::{Config, SinkType};
use crate::drain::{self, DrainConfig, DrainHeartbeat};
use crate::metrics::Metrics;
use crate::sink::Sink;
use crate::sink::noop::NoopSink;
#[cfg(feature = "clickhouse-sink")]
use crate::sink::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
#[cfg(feature = "http-sink")]
use crate::sink::http::{HttpBatchMode, HttpSink, HttpSinkConfig};
#[cfg(feature = "mysql-sink")]
use crate::sink::mysql::{MySqlSink, MySqlSinkConfig};
#[cfg(feature = "postgres-sink")]
use crate::sink::postgres::{PostgresSink, PostgresSinkConfig};

/// Everything a drain needs except the sink.
pub(crate) struct DrainSpawnArgs {
    pub(crate) rx: crossbeam_channel::Receiver<PathBuf>,
    pub(crate) config: DrainConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) heartbeat: DrainHeartbeat,
    /// PR 2's control, board and startup backlog. Detached until PR 4 flips
    /// production to `DrainControl::attached()`.
    pub(crate) runtime: drain::DrainRuntime,
}

type SpawnFn = Box<dyn FnOnce(DrainSpawnArgs) -> std::thread::JoinHandle<drain::DrainExit> + Send>;

/// A built sink, type-erased: `Sink` has an associated error type and async
/// methods, so it is not object-safe; the closure captures the concrete type.
pub(crate) struct DrainSpawner {
    sink_type: SinkType,
    spawn: SpawnFn,
}

impl DrainSpawner {
    pub(crate) fn from_sink<S: Sink + 'static>(sink_type: SinkType, sink: S) -> Self {
        let sink = Arc::new(sink);
        DrainSpawner {
            sink_type,
            spawn: Box::new(move |a: DrainSpawnArgs| {
                drain::spawn_with(a.rx, sink, a.config, a.metrics, a.heartbeat, a.runtime)
            }),
        }
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "read by SP-M's hot-swap; pinned by the unit test")
    )]
    pub(crate) fn sink_type(&self) -> SinkType {
        self.sink_type
    }

    /// Spawns the drain thread. Consumes the spawner: one sink, one drain.
    pub(crate) fn spawn(self, args: DrainSpawnArgs) -> std::thread::JoinHandle<drain::DrainExit> {
        (self.spawn)(args)
    }
}

#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink"
))]
fn require_sink_url(
    config: &Config,
    sink_label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // body moved verbatim from main's `require_sink_url`
}

/// Builds the configured sink. Every arm's log lines, warnings and error
/// texts are the ones `main` had; only the tail changes, from
/// `build_and_spawn_drain(sink, drain_rx, drain_config, metrics, hb, drain_runtime)` to
/// `Ok(DrainSpawner::from_sink(SinkType::X, sink))`.
pub(crate) fn build_drain_spawner(
    config: &Config,
    _metrics: &Arc<Metrics>,
) -> Result<DrainSpawner, Box<dyn std::error::Error>> {
    match config.sink_type {
        SinkType::Noop => {
            warn!(
                "sink: noop — records are acked and DISCARDED, not forwarded anywhere. \
                 This is for soak-testing the pipeline; set --sink-type to deliver downstream."
            );
            Ok(DrainSpawner::from_sink(SinkType::Noop, NoopSink))
        }
        #[cfg(feature = "http-sink")]
        SinkType::Http => {
            // main's `SinkType::Http => {` arm body up to, not including, its
            // `build_and_spawn_drain(` tail, moved verbatim (bearer token, batch
            // mode, concurrency display, the "sink: http" info line, HttpSink::new)
            Ok(DrainSpawner::from_sink(SinkType::Http, sink))
        }
        #[cfg(feature = "mysql-sink")]
        SinkType::Mysql => { /* main's `SinkType::Mysql => {` arm body up to, not including, its `build_and_spawn_drain(` tail, verbatim */ Ok(DrainSpawner::from_sink(SinkType::Mysql, sink)) }
        #[cfg(feature = "postgres-sink")]
        SinkType::Postgres => { /* main's `SinkType::Postgres => {` arm body up to, not including, its `build_and_spawn_drain(` tail, verbatim */ Ok(DrainSpawner::from_sink(SinkType::Postgres, sink)) }
        #[cfg(feature = "clickhouse-sink")]
        SinkType::ClickHouse => { /* main's `SinkType::ClickHouse => {` arm body up to, not including, its `build_and_spawn_drain(` tail, verbatim */ Ok(DrainSpawner::from_sink(SinkType::ClickHouse, sink)) }
        #[cfg(feature = "s3-sink")]
        SinkType::S3 => { /* main's `SinkType::S3 => {` arm body up to, not including, its `build_and_spawn_drain(` tail, verbatim, incl. the empty-bucket Err */ Ok(DrainSpawner::from_sink(SinkType::S3, sink)) }
    }
}
```

The arm bodies are the existing main.rs blocks with exactly two edits each: `&config` stays `config` (it is now a `&Config` parameter), and the `build_and_spawn_drain(...)` tail is replaced as shown. `_metrics` is the parameter the spec's signature names; nothing reads it in this move (the `weir_sink_info` set stays in `main`); keep the leading underscore until a sink needs it.

`crates/weir-server/src/main.rs`: delete `require_sink_url` (with its doc and `#[cfg(any(..))]`) and `build_and_spawn_drain`, and the per-sink `use sink::…` imports (`ClickHouseSink`, `HttpSink`, `MySqlSink`, `NoopSink`, `PostgresSink`, with their `#[cfg]` lines); change `use config::{Config, SinkType};` to `use config::Config;` (the moved match arms were `SinkType`'s only users in main, so leaving it fails clippy in every build); replace the `let drain_handle = match config.sink_type { … };` block with:

```rust
    // Sink selection (spec §5.12): builds the sink, still after recovery.
    let drain_spawner = sink::select::build_drain_spawner(&config, &metrics)?;
    let drain_handle = drain_spawner.spawn(sink::select::DrainSpawnArgs {
        rx: drain_rx,
        config: drain_config,
        metrics: Arc::clone(&metrics),
        heartbeat: drain_heartbeat.clone(),
        // PR 2 Task 2.17's local: DrainControl::detached(), board Some, no backlog.
        runtime: drain_runtime,
    });
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p weir-server --bin weir-server sink::select -- --test-threads=1` → pass.
Run: `cargo build -p weir-server --all-features` and `cargo build -p weir-server --no-default-features` → both compile (every `cfg` arm still matches its feature).
Run: `cargo clippy -p weir-server --all-targets --no-default-features -- -D warnings`, `cargo clippy -p weir-server --all-targets -- -D warnings` and `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` → all clean (ci.yml's lint job runs the same three; the noop-only build is the one that catches an unused `Duration` / `info` import, a leftover `SinkType` import in main, or a dead `sink_type()`).
Run: `cargo test -p weir-server --test system sink_health_shows_healthy_via_noop_sink -- --test-threads=1` and `cargo test -p weir-server --test system --features http-sink wab_cap_nack_is_recoverable_for_a_real_client -- --test-threads=1` → pass.

- [ ] **Step 5: Commit**

`git commit -m "refactor(sink): move sink selection out of main into a DrainSpawner"`

---

### Task 3.8: `refuse_if_live`, and the umask arithmetic in `bind_hardened`

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

Verified against 2491883: `refuse_if_live` calls the merged `probe_socket` (socket/mod.rs:810). On non-Linux that probe settles a stream ECONNREFUSED with a SOCK_DGRAM attached probe (`probe_socket_attached`, :857), so a full backlog is not misread as stale. Do not add a connect of its own.

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs` (new fn after `unknown_socket_refusal` at 769-777; the comment paragraph starting `// umask is process-global` at 449-460; tests module from 1163)
- Modify: `docs/security/socket-bind.md:383-395` (the "Known limitation — the umask window is process-global" paragraph: its `0o400` segment claim)
- Test: `crates/weir-server/src/socket/mod.rs` tests module

**Interfaces:**
- Consumes: `probe_socket(&Path) -> SocketProbe` (safety floor), `SocketProbe::{Absent, Stale, Live, Unknown(i32)}`.
- Produces: `#[cfg(unix)] pub(crate) fn refuse_if_live(path: &Path, what: &str) -> io::Result<()>` (`Live` → `ErrorKind::AddrInUse` "another daemon is serving `<path>`"; `Unknown(errno)` → error naming `what` and the errno; `Absent | Stale` → `Ok(())`). PR 6 calls it for the admin socket before `bind_hardened`.

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `socket/mod.rs`:

```rust
    #[test]
    #[cfg(unix)]
    fn refuse_if_live_refuses_a_live_socket_and_allows_absent_or_stale() {
        let dir = umask_immune_dir("refuse_if_live");
        let path = dir.join("admin.sock");

        refuse_if_live(&path, "admin socket").expect("nothing at the path");

        leave_stale_socket(&path);
        refuse_if_live(&path, "admin socket").expect("a stale socket is not a live daemon");
        std::fs::remove_file(&path).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _guard = rt.enter();
        let live = UnixListener::bind(&path).unwrap();
        let err = refuse_if_live(&path, "admin socket").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        let msg = err.to_string();
        assert!(msg.contains("another daemon is serving"), "{msg}");
        assert!(msg.contains(&path.display().to_string()), "{msg}");
        assert!(msg.contains("admin socket"), "{msg}");
        assert!(path.exists(), "refuse_if_live never unlinks");
        drop(live);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Pins the corrected comment in `bind_hardened`: under the 0o177 bind
    /// window a 0o600 file keeps 0o600 (owner rw is not in the mask), and a
    /// directory from `create_dir_private` keeps 0o700 (it chmods after mkdir).
    #[test]
    #[cfg(unix)]
    fn the_bind_umask_window_leaves_segment_files_and_private_dirs_intact() {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("umask_window");
        let file = dir.join("seg.wab");
        let sub = dir.join("shard_00");
        {
            let _umask = UmaskGuard(umask_set(0o177));
            std::fs::OpenOptions::new().create(true).write(true).mode(0o600).open(&file).unwrap();
            crate::wab::create_dir_private(sub.clone()).unwrap();
        }
        assert_eq!(std::fs::metadata(&file).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::metadata(&sub).unwrap().permissions().mode() & 0o777, 0o700);
        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run them to verify they fail / characterise**

Run: `cargo test -p weir-server --bin weir-server socket::tests::refuse_if_live -- --test-threads=1`
Expected: compile error `cannot find function `refuse_if_live``.
Run: `cargo test -p weir-server --bin weir-server socket::tests::the_bind_umask_window -- --test-threads=1` (after Step 3 makes the module compile)
Expected: PASS. This one is a characterisation test: the code is right, the comment is wrong (it claims a 0o600 segment file lands at 0o400 and a directory at 0o600). It pins the arithmetic the corrected comment states.

- [ ] **Step 3: Implement**

After `unknown_socket_refusal`:

```rust
/// Fails if something is listening at `path` (spec §5.7, §18). The probe is
/// the safety floor's connect-only [`probe_socket`]; nothing is unlinked.
///
/// `bind_hardened` already refuses a live socket for every caller (through
/// `remove_stale_socket_at_startup`), with the producer socket's wording. This
/// helper is for a caller that wants the refusal before the bind, worded for
/// its own socket: the admin socket (PR 6) passes `what = "admin socket"`.
#[cfg(unix)]
#[cfg_attr(not(test), allow(dead_code, reason = "PR 6's admin bind is the caller; PR 6 Task 6.14 removes this"))]
pub(crate) fn refuse_if_live(path: &Path, what: &str) -> io::Result<()> {
    match probe_socket(path) {
        SocketProbe::Absent | SocketProbe::Stale => Ok(()),
        SocketProbe::Live => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!(
                "another daemon is serving '{}'; refusing to bind the {what} there. Stop \
                 the other weir-server or choose another path.",
                path.display()
            ),
        )),
        SocketProbe::Unknown(errno) => Err(io::Error::other(format!(
            "cannot tell whether the {what} path '{}' is in use: the liveness probe's \
             connect failed with {} (errno {errno}); refusing to bind. If no weir-server \
             is using it, remove the file manually and start again.",
            path.display(),
            io::Error::from_raw_os_error(errno)
        ))),
    }
}
```

Replace the comment paragraph at 449-460 with:

```rust
    // umask is process-global and applies to other threads for the duration of
    // the bind(2) below. Requested mode bits are masked by it, so the window
    // only matters for bits the mask removes: 0o177 removes owner-x and every
    // group/other bit. A 0o600 file (segments, `.confirmed`) is therefore
    // unaffected: 0o600 & !0o177 = 0o600. A directory made with a plain
    // mkdir(0o700) would land at 0o700 & !0o177 = 0o600 (no execute bit,
    // nothing can be created inside it), which is why every WAB directory goes
    // through `wab::create_dir_private`, which chmods after mkdir. For today's
    // call sites the window is benign. It is still kept short, and from PR 3
    // no producer listener serves before every listener is bound (spec §5.7),
    // so no producer traffic overlaps it. Under `cargo test` other test
    // threads create directories without that helper, which is why
    // weir-server's bin unit tests run with --test-threads=1 — see
    // CONTRIBUTING.md and docs/security/socket-bind.md.
```

(The removed text also cited main.rs:292/306 for the flusher and worker spawns; those line numbers were stale.)

`docs/security/socket-bind.md` (lines 391-392 at 3adc5f5) makes the same wrong claim. Replace "a WAB segment lands at `0o600 & !0o177 = 0o400`." with "a 0o600 file (a WAB segment, a `.confirmed` sidecar) is unaffected (`0o600 & !0o177 = 0o600`); WAB directories go through `wab::create_dir_private`, which chmods after mkdir." Leave the directory sentence before it (`0o700 & !0o177 = 0o600`, for a plain mkdir) as it is: that arithmetic is right. Check with `grep -n "0o400" docs/security/socket-bind.md` (expect no output).

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1`
Expected: all socket tests pass, including the two new ones.

- [ ] **Step 5: Commit**

`git commit -m "feat(socket): refuse_if_live, and correct the umask arithmetic in bind_hardened"`

---

### Task 3.9: `socket/route.rs` — a shard router that can pick only live shards

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Create: `crates/weir-server/src/socket/route.rs`
- Modify: `crates/weir-server/src/socket/mod.rs:19-27` (`pub(crate) mod route;`)
- Test: `crates/weir-server/src/socket/route.rs` tests module

**Interfaces:**
- Consumes: `ShardRegistry::pick_live(&self, n: u64) -> Option<u32>` via `StatusBoard.shards` (PR 1); `Metrics::connections_refused_no_live_shard` (PR 1).
- Produces:
  - `pub(crate) trait LiveShards: Send + Sync { fn pick_live(&self, n: u64) -> Option<u32>; }` with `impl LiveShards for crate::status::StatusBoard`
  - `#[derive(Clone)] pub(crate) struct ShardRouter` with `fn round_robin(shard_count: usize) -> ShardRouter`, `fn live(shards: Arc<dyn LiveShards>) -> ShardRouter`, `fn pick(&self, n: u64) -> Option<u32>`, `fn refuse(&self, metrics: &Metrics, transport: &'static str)`
  - `pub(crate) const NO_LIVE_SHARD_WARN_INTERVAL: Duration = Duration::from_secs(10);`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Shards 0..k; the ones in `down` are not ingest-capable.
    struct FakeShards {
        k: u32,
        down: Vec<u32>,
    }
    impl LiveShards for FakeShards {
        fn pick_live(&self, n: u64) -> Option<u32> {
            let live: Vec<u32> = (0..self.k).filter(|s| !self.down.contains(s)).collect();
            (!live.is_empty()).then(|| live[(n % live.len() as u64) as usize])
        }
    }

    #[test]
    fn round_robin_is_todays_assignment() {
        let r = ShardRouter::round_robin(3);
        let picks: Vec<_> = (0..6).map(|n| r.pick(n)).collect();
        assert_eq!(picks, [0, 1, 2, 0, 1, 2].map(Some));
        assert_eq!(ShardRouter::round_robin(0).pick(5), Some(0), "shard_count 0 is clamped to 1");
    }

    #[test]
    fn live_routing_skips_an_offline_shard_and_reports_none_when_all_are_down() {
        let r = ShardRouter::live(Arc::new(FakeShards { k: 3, down: vec![1] }));
        let picks: Vec<_> = (0..100).filter_map(|n| r.pick(n)).collect();
        assert_eq!(picks.len(), 100);
        assert!(!picks.contains(&1));
        assert!(picks.contains(&0) && picks.contains(&2), "the live shards share the load");

        let none = ShardRouter::live(Arc::new(FakeShards { k: 2, down: vec![0, 1] }));
        assert_eq!(none.pick(0), None);
    }

    #[test]
    fn a_refusal_always_counts_but_warns_at_most_once_per_interval() {
        let m = Metrics::new().0;
        let r = ShardRouter::round_robin(1);
        let shared = r.clone();
        assert!(r.refusal_warn_due(), "the first refusal warns");
        assert!(!shared.refusal_warn_due(), "clones share the limiter (Unix and TCP)");
        r.refuse(&m, "unix");
        shared.refuse(&m, "tcp");
        assert_eq!(m.connections_refused_no_live_shard.get(), 2);
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server socket::route -- --test-threads=1`
Expected: compile error, `ShardRouter` not found (module body not written).

- [ ] **Step 3: Implement**

`crates/weir-server/src/socket/mod.rs`: `pub(crate) mod route;` after `mod peer;`.

`crates/weir-server/src/socket/route.rs`:

```rust
//! Connection → shard assignment (spec §5.7). Pinning at accept is unchanged,
//! so per-connection ordering is unchanged; only WHICH shard is picked moves
//! from `n % shard_count` to "the (n mod k)-th ingest-capable shard".

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use tracing::warn;

use crate::metrics::Metrics;

/// At most one "no live shard" WARN per this interval, across both listeners.
pub(crate) const NO_LIVE_SHARD_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// The one question routing asks of the shard registry.
pub(crate) trait LiveShards: Send + Sync {
    fn pick_live(&self, n: u64) -> Option<u32>;
}

impl LiveShards for crate::status::StatusBoard {
    fn pick_live(&self, n: u64) -> Option<u32> {
        self.shards.pick_live(n)
    }
}

#[derive(Clone)]
enum Kind {
    /// Today's assignment. The `#[cfg(test)]` `socket::run` wrapper uses it.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "the #[cfg(test)] socket::run wrapper is the only caller after Task 3.11")
    )]
    RoundRobin { shard_count: u64 },
    /// Production from PR 3: only shards that can accept ingest.
    Live(Arc<dyn LiveShards>),
}

/// Shared by the Unix and TCP accept loops (clone it): one refusal limiter.
#[derive(Clone)]
pub(crate) struct ShardRouter {
    kind: Kind,
    warn_limiter: Arc<WarnLimiter>,
}

impl ShardRouter {
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "the #[cfg(test)] socket::run wrapper is the only caller after Task 3.11")
    )]
    pub(crate) fn round_robin(shard_count: usize) -> Self {
        Self::with(Kind::RoundRobin { shard_count: shard_count.max(1) as u64 })
    }

    pub(crate) fn live(shards: Arc<dyn LiveShards>) -> Self {
        Self::with(Kind::Live(shards))
    }

    fn with(kind: Kind) -> Self {
        ShardRouter {
            kind,
            warn_limiter: Arc::new(WarnLimiter::new(NO_LIVE_SHARD_WARN_INTERVAL)),
        }
    }

    /// The shard for the `n`-th accepted connection, or `None` when no shard
    /// can take ingest (every one offline, or still starting).
    pub(crate) fn pick(&self, n: u64) -> Option<u32> {
        match &self.kind {
            Kind::RoundRobin { shard_count } => Some((n % shard_count) as u32),
            Kind::Live(shards) => shards.pick_live(n),
        }
    }

    /// Counts a refused connection and WARNs, rate-limited. The caller drops
    /// the stream: no frame has been read, so the producer sees a closed
    /// connection and retries, as with the connection cap.
    pub(crate) fn refuse(&self, metrics: &Metrics, transport: &'static str) {
        metrics.connections_refused_no_live_shard.inc();
        if self.refusal_warn_due() {
            warn!(
                transport,
                "refusing connection: no shard can accept ingest (every shard is offline or \
                 still starting); see weir_wab_shard_up and the flusher logs"
            );
        }
    }

    fn refusal_warn_due(&self) -> bool {
        self.warn_limiter.due()
    }
}

/// Lock-free "at most once per interval".
struct WarnLimiter {
    epoch: Instant,
    interval_ms: u64,
    /// Milliseconds since `epoch` before which no WARN is due; 0 = due now.
    next_ms: AtomicU64,
}

impl WarnLimiter {
    fn new(interval: Duration) -> Self {
        WarnLimiter { epoch: Instant::now(), interval_ms: interval.as_millis() as u64, next_ms: AtomicU64::new(0) }
    }

    fn due(&self) -> bool {
        let now = self.epoch.elapsed().as_millis() as u64;
        let next = self.next_ms.load(Relaxed);
        now >= next
            && self
                .next_ms
                .compare_exchange(next, now + self.interval_ms, Relaxed, Relaxed)
                .is_ok()
    }
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server socket::route -- --test-threads=1`
Expected: 3 passed. `ShardRouter` has no production caller until Task 3.10 (round-robin) and Task 3.11 (`live`), so a plain `cargo build` warns `dead_code` until then; as in Task 3.2, do not silence it, since Task 3.11's clippy runs are the check (land 3.9-3.11 as one commit if every commit must be warning-free).

- [ ] **Step 5: Commit**

`git commit -m "feat(socket): a shard router that can pick only live shards"`

---

### Task 3.10: Split Unix `bind` from `serve`; both accept loops take a `ShardRouter`

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs:91-305` (`run`), new `BoundUnix`, `bind`, `serve`
- Modify: `crates/weir-server/src/socket/tcp.rs` (`run` signature at :105, the local `shard_count` at :152 and the shard assignment at :195-196, at 3adc5f5)
- Test: `crates/weir-server/src/socket/mod.rs` tests module (existing tests unchanged; new tests added)

**Interfaces:**
- Consumes: `validate_socket_path`, `bind_hardened -> io::Result<BoundSocket>`, `remove_own_socket(&Path, &BoundSocket)` (safety floor); `ShardRouter` (Task 3.9).
- Produces:
  - `#[derive(Debug)] pub(crate) struct BoundUnix { pub(crate) path: PathBuf, pub(crate) socket: BoundSocket }`
  - `#[cfg(unix)] pub(crate) fn bind(config: &SocketConfig) -> io::Result<BoundUnix>` (marks `config.board`'s Unix listener `Bound` on success and `Failed` on error; README Q6)
  - `pub async fn serve(config: SocketConfig, bound: BoundUnix, queue_tx: QueueSender<WorkUnit>, shutdown_rx: oneshot::Receiver<()>, metrics: Arc<Metrics>, sem: Arc<Semaphore>, router: ShardRouter) -> io::Result<()>` (enters `Phase::Serving` at its start and marks the Unix listener `Closed` on return; never `Bound`. PR 4 replaces `shutdown_rx` with `watch::Receiver<ShutdownLevel>`)
  - `#[cfg(test)] pub async fn run(config, queue_tx, shutdown_rx, metrics, sem) -> io::Result<()>` = `bind` + `serve` with `ShardRouter::round_robin(config.shard_count)` (existing tests unchanged)
  - `tcp::run(config: TcpConfig, listener: TcpListener, tls: ReloadableServerConfig, queue_tx, sem, shutdown_rx, metrics, router: ShardRouter)`

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `socket/mod.rs`:

```rust
    /// `bind` binds and returns; nothing accepts until `serve` runs. The
    /// kernel backlog still completes a connect, so the test checks that no
    /// frame is answered rather than that the connect fails.
    #[tokio::test]
    #[cfg(unix)]
    async fn bind_returns_a_bound_socket_that_serve_then_serves() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_then_serve");
        let path = dir.join("weir.sock");
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let bound = bind(&cfg).unwrap();
        assert_eq!(bound.path, path);
        assert!(path.exists());
        // README Q6: `bind` is the one writer of the Unix Bound mark; Serving
        // is `serve`'s, and nothing serves yet.
        assert_eq!(board.listeners.unix.get(), weir_status::ListenerState::Bound);
        assert_ne!(board.life.phase(), weir_status::Phase::Serving);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let handle = tokio::spawn(serve(cfg, bound, queue_tx, shutdown_rx, test_metrics(), sem, route::ShardRouter::round_robin(1)));
        let _conn = UnixStream::connect(&path).await.unwrap();
        assert_eq!(board.life.phase(), weir_status::Phase::Serving);
        shutdown_tx.send(()).unwrap();
        time::timeout(Duration::from_secs(3), handle).await.unwrap().unwrap().unwrap();
        assert!(!path.exists(), "serve removes the socket it was handed");
        assert_eq!(board.listeners.unix.get(), weir_status::ListenerState::Closed);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// With no live shard the connection is dropped before any frame and
    /// counted; the accept loop keeps running.
    #[tokio::test]
    #[cfg(unix)]
    async fn serve_drops_and_counts_a_connection_when_no_shard_is_live() {
        struct NoneLive;
        impl route::LiveShards for NoneLive {
            fn pick_live(&self, _n: u64) -> Option<u32> {
                None
            }
        }
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("no_live_shard");
        let path = dir.join("weir.sock");
        let cfg = default_config(path.clone());
        let bound = bind(&cfg).unwrap();
        let metrics = test_metrics();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let router = route::ShardRouter::live(std::sync::Arc::new(NoneLive));
        let handle = tokio::spawn(serve(cfg, bound, queue_tx, shutdown_rx, std::sync::Arc::clone(&metrics), sem, router));

        for _ in 0..2 {
            let mut conn = UnixStream::connect(&path).await.unwrap();
            let mut buf = [0u8; 1];
            let n = time::timeout(Duration::from_secs(2), conn.read(&mut buf)).await.unwrap().unwrap_or(0);
            assert_eq!(n, 0, "the daemon must close the connection without a byte");
        }
        assert_eq!(metrics.connections_refused_no_live_shard.get(), 2);

        shutdown_tx.send(()).unwrap();
        time::timeout(Duration::from_secs(3), handle).await.unwrap().unwrap().unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server socket::tests -- --test-threads=1`
Expected: compile error `cannot find function `bind`` / `serve`.

- [ ] **Step 3: Implement**

In `socket/mod.rs`, after `BoundSocket`:

```rust
/// A bound producer (or admin) Unix socket: the path it was bound at plus the
/// identity of the inode it created (spec §5.7 `BoundUnix`; the identity is
/// the safety floor's `BoundSocket`, which `remove_own_socket` checks). Also
/// the SP-N seam: an inherited fd would produce one of these.
#[derive(Debug)]
pub(crate) struct BoundUnix {
    pub(crate) path: PathBuf,
    pub(crate) socket: BoundSocket,
}

/// Binds the producer socket: path validation plus the hardened bind, moved
/// out of the accept loop so startup can bind every listener before serving
/// any (spec §5.7). A live socket at the path is refused inside
/// `bind_hardened` (safety floor).
#[cfg(unix)]
pub(crate) fn bind(config: &SocketConfig) -> io::Result<BoundUnix> {
    // PR 1's board hook (Task 1.23): a validation or bind failure marks the
    // Unix listener Failed, exactly as `run` did before the split.
    // README Q6: `bind` is the only writer of the Unix listener's Bound /
    // Failed marks; `serve` writes Serving and Closed; `main` writes neither.
    let failed = |_: &io::Error| {
        if let Some(b) = &config.board {
            b.listeners.unix.set(weir_status::ListenerState::Failed);
        }
    };
    validate_socket_path(&config.socket_path).inspect_err(failed)?;
    let socket = bind_hardened(&config.socket_path).inspect_err(failed)?;
    if let Some(b) = &config.board {
        b.listeners.unix.set(weir_status::ListenerState::Bound);
    }
    info!(path = %config.socket_path.display(), "socket bound");
    Ok(BoundUnix { path: config.socket_path.clone(), socket })
}
```

Rename the current `pub async fn run(config, queue_tx, shutdown_rx, metrics, sem)` to `serve` with the signature above. Its first two lines (`validate_socket_path` / `bind_hardened`, with PR 1's `Failed` marks, now in `bind`) go; `let listener = &bound.socket.listener;` replaces `let listener = &bound.listener;`; the "socket listening" info stays. PR 1's block after it loses its `Bound` mark (now in `bind`, README Q6) and becomes `if let Some(b) = &config.board { b.enter_phase(weir_status::Phase::Serving, &metrics); }`. The `Closed` mark before the final `Ok(())` stays (use an inline `if let Some(b) = &config.board { b.listeners.unix.set(weir_status::ListenerState::Closed); }` there, since PR 1's `mark` closure no longer has a `Failed` use in `serve`). PR 1's tests `run_marks_the_unix_listener_bound_then_serving_then_closed` and `a_bind_failure_marks_the_unix_listener_failed` reach both through the `#[cfg(test)] run` wrapper below and pass unchanged. The round-robin state goes (`conn_counter` stays — the router takes `n`; `shard_count` local goes). In the accept arm, directly after the peer-uid block and BEFORE the permit:

```rust
                        let n = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(shard_id) = router.pick(n) else {
                            router.refuse(&metrics, "unix");
                            drop(stream);
                            continue;
                        };
```

and the later `let n = …; cfg.shard_id = (n % shard_count) as u32;` becomes `cfg.shard_id = shard_id;`. The shutdown removal becomes `remove_own_socket(&bound.path, &bound.socket);` then `drop(bound);`. Update the doc comment ("Accepts on an already-bound socket; see [`bind`]").

The test-compat wrapper:

```rust
/// bind + serve with today's round-robin routing. Test-only: production binds
/// every listener first (`bind_all`) and serves with live-shard routing.
#[cfg(test)]
pub async fn run(
    config: SocketConfig,
    queue_tx: QueueSender<WorkUnit>,
    shutdown_rx: oneshot::Receiver<()>,
    metrics: std::sync::Arc<Metrics>,
    sem: std::sync::Arc<Semaphore>,
) -> io::Result<()> {
    let bound = bind(&config)?;
    let router = route::ShardRouter::round_robin(config.shard_count);
    serve(config, bound, queue_tx, shutdown_rx, metrics, sem, router).await
}
```

`SocketConfig::shard_count` is now read only by the `#[cfg(test)] run` wrapper (main and PR 4 still write it), so give the field `#[cfg_attr(not(test), allow(dead_code, reason = "read only by the #[cfg(test)] run wrapper; production routing is the ShardRouter's"))]` (models.rs:93 and config/mod.rs:591 are the precedents for unread fields in this binary).

`socket/tcp.rs`: `run` gains the final parameter `router: crate::socket::route::ShardRouter`; delete the local `shard_count` (`let shard_count = config.shard_count.max(1) as u64;`, :152 at 3adc5f5, and the comment above it); in the accept arm, after the `try_join_next` reap and before the permit:

```rust
                        let n = conn_counter.fetch_add(1, Ordering::Relaxed);
                        let Some(shard_id) = router.pick(n) else {
                            router.refuse(&metrics, "tcp");
                            drop(stream);
                            continue;
                        };
```

and `cfg.shard_id = shard_id;` replaces the `cfg.shard_id = (n % shard_count) as u32;` assignment (:195-196 at 3adc5f5; the `let n = conn_counter.fetch_add(..)` line above it moves up into the snippet). `TcpConfig::shard_count` is then read by nothing (main and PR 4's literal still set it): update its doc ("informational; routing is the `ShardRouter`'s") and give it `#[allow(dead_code, reason = "informational; routing is the ShardRouter's")]`. Update the module doc ("round-robin shard assignment" → "shard assignment through the shared `ShardRouter`").

`main.rs` still calls `socket::run` here only if Task 3.11 has not landed; since `run` is now `#[cfg(test)]`, make main call `socket::bind` + `socket::serve(..., socket::route::ShardRouter::round_robin(config.shard_count))` in this commit, and `tcp::run(..., socket::route::ShardRouter::round_robin(config.shard_count))`; Task 3.11 replaces both with the shared live router.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1`
Expected: the two new tests and every existing socket test (`full_run_start_then_shutdown_removes_its_own_socket`, `connection_over_cap_is_refused`, `idle_connection_exits_promptly_on_shutdown`, `run_exits_cleanly_after_shutdown_signal`, `socket_bind_sets_mode_0600`, …) pass unchanged.
Run: `cargo test -p weir-server --bins --all-features -- --test-threads=1 socket::` → pass (tcp.rs compiles with the new parameter).
Run: `cargo test -p weir-server --bin weir-server socket::tests::run_marks_the_unix_listener_bound_then_serving_then_closed socket::tests::a_bind_failure_marks_the_unix_listener_failed -- --test-threads=1` (one filter per invocation) → both PR 1 tests pass unchanged: through the `#[cfg(test)] run` wrapper, `bind` marks `Bound` / `Failed` and `serve` enters `Serving` and marks `Closed`.

- [ ] **Step 5: Commit**

`git commit -m "feat(socket): split bind from serve, and refuse a connection when no shard is live"`

---

### Task 3.11: Bind every producer listener before serving any; route to live shards

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

Verified against 2491883: the lock-integrity check is its own thread (main.rs:825), started right after lock acquisition (main.rs:1000) and looping until exit. The settle wait and `bind_all` run on other threads and cannot pause it; keep passing the same `wab_ownership_lost` Arc into `SocketConfig`.

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs` (new `BoundListeners`, `bind_all`)
- Modify: `crates/weir-server/src/main.rs`: the settle wait after `drop(replay_tx)` and before `let rt = tokio::runtime::Builder`; inside `rt.block_on`, the `#[cfg(all(unix, feature = "tls"))] let (tcp_shutdown_tx, tcp_join) = { .. };` block (:1560-1633 at 3adc5f5) and the `// Socket accept loop` `#[cfg(unix)] { use socket::SocketConfig; .. }` block (:1661-1680 at 3adc5f5)
- Test: `crates/weir-server/src/socket/mod.rs` tests; `crates/weir-server/src/main.rs` `mod tests` (:2088 at 3adc5f5)

**Interfaces:**
- Consumes: `bind` / `serve` / `ShardRouter::live` / `LiveShards for StatusBoard` (Tasks 3.9-3.10); `StatusBoard.shards.{all_settled(), worker_count(), worker_running(usize)}` (PR 1); `preflight::tcp_bind_error` (tls builds).
- Produces:
  - `pub(crate) struct BoundListeners { pub(crate) unix: BoundUnix, pub(crate) tcp: Option<tokio::net::TcpListener> }`
  - `#[cfg(unix)] pub(crate) async fn bind_all(config: &SocketConfig, tcp: Option<std::net::SocketAddr>) -> io::Result<BoundListeners>` (TCP first, then Unix; any failure drops what was bound and returns the error). README Q6: it is the one writer of the TCP listener's `Bound` / `Failed` marks, on `config.board`; the Unix marks are `bind`'s.
  - `main.rs`: `const SHARD_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);`, `fn wait_until(timeout: Duration, poll: Duration, done: impl FnMut() -> bool) -> bool`

- [ ] **Step 1: Write the failing tests**

In `socket/mod.rs` tests:

```rust
    fn free_loopback_addr() -> std::net::SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bind_all_binds_tcp_and_unix_and_serves_neither() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_ok");
        let path = dir.join("weir.sock");
        let addr = free_loopback_addr();
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let bound = bind_all(&cfg, Some(addr)).await.unwrap();
        assert_eq!(bound.tcp.as_ref().unwrap().local_addr().unwrap(), addr);
        assert_eq!(bound.unix.path, path);
        // README Q6: bind_all / bind mark Bound; nothing enters Serving yet.
        assert_eq!(board.listeners.tcp.get(), weir_status::ListenerState::Bound);
        assert_eq!(board.listeners.unix.get(), weir_status::ListenerState::Bound);
        assert_ne!(board.life.phase(), weir_status::Phase::Serving);
        drop(bound);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A Unix bind failure after the TCP bind fails the whole step and leaves
    /// nothing listening: startup fails before any producer could be acked.
    #[tokio::test]
    #[cfg(unix)]
    async fn bind_all_releases_the_tcp_port_when_the_unix_bind_fails() {
        let _g = umask_test_lock().lock().unwrap_or_else(|e| e.into_inner());
        let dir = umask_immune_dir("bind_all_fail");
        let path = dir.join("weir.sock");
        std::fs::write(&path, b"not a socket").unwrap();
        let addr = free_loopback_addr();
        let board = crate::status::test_board(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.board = Some(std::sync::Arc::clone(&board));
        let err = bind_all(&cfg, Some(addr)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert_eq!(board.listeners.unix.get(), weir_status::ListenerState::Failed);
        assert_ne!(
            board.listeners.tcp.get(),
            weir_status::ListenerState::Bound,
            "a TCP listener dropped with the failed step is never reported Bound"
        );
        assert!(
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err(),
            "the TCP listener must be dropped with the failed step"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
```

In `main.rs` `mod tests`:

```rust
    #[test]
    fn wait_until_returns_true_as_soon_as_done_and_false_at_the_deadline() {
        let mut calls = 0;
        assert!(wait_until(Duration::from_secs(5), Duration::from_millis(1), || {
            calls += 1;
            calls == 3
        }));
        assert_eq!(calls, 3);

        let t0 = std::time::Instant::now();
        assert!(!wait_until(Duration::from_millis(60), Duration::from_millis(5), || false));
        let took = t0.elapsed();
        assert!(took >= Duration::from_millis(60) && took < Duration::from_millis(500), "{took:?}");
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --bin weir-server bind_all -- --test-threads=1`
Expected: compile error `cannot find function `bind_all``; and `cannot find function `wait_until`` for the main test.

- [ ] **Step 3: Implement**

`socket/mod.rs`:

```rust
/// Every producer listener, bound and not yet served (spec §6 step 11).
pub(crate) struct BoundListeners {
    pub(crate) unix: BoundUnix,
    /// `Some` only with `tcp_bind`, which config validation allows only in a
    /// `tls` build. Only main's `tls` block reads it, hence the same
    /// non-tls dead-code allowance `Config::tcp_bind` carries.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) tcp: Option<tokio::net::TcpListener>,
}

/// Binds TCP (if configured), then the Unix socket, and serves neither.
///
/// Why before any accept loop (spec §5.7): a bind failure then fails startup
/// before a single producer is acked, and no producer traffic overlaps the
/// `umask(0o177)` window inside `bind_hardened`. TCP goes first because a
/// failed Unix bind then only has to drop a TcpListener; the reverse would
/// leave a socket file to clean up.
///
/// README Q6: this is the one writer of the TCP listener's `Bound` / `Failed`
/// marks (`bind` writes the Unix ones). `Bound` is written only once the whole
/// step has succeeded, so a TCP listener dropped because the Unix bind failed
/// is never reported Bound.
#[cfg(unix)]
pub(crate) async fn bind_all(
    config: &SocketConfig,
    tcp: Option<std::net::SocketAddr>,
) -> io::Result<BoundListeners> {
    let mark_tcp = |state: weir_status::ListenerState| {
        if let Some(b) = &config.board {
            b.listeners.tcp.set(state);
        }
    };
    let tcp = match tcp {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| {
                    #[cfg(feature = "tls")]
                    {
                        crate::preflight::tcp_bind_error(addr, &e)
                    }
                    #[cfg(not(feature = "tls"))]
                    {
                        e
                    }
                })
                .inspect_err(|_| mark_tcp(weir_status::ListenerState::Failed))?;
            info!(addr = %listener.local_addr()?, "TCP+mTLS listener bound");
            Some(listener)
        }
        None => None,
    };
    let unix = bind(config)?;
    if tcp.is_some() {
        mark_tcp(weir_status::ListenerState::Bound);
    }
    Ok(BoundListeners { unix, tcp })
}
```

`main.rs`, new helper near the other startup helpers:

```rust
/// How long startup waits for every flusher to leave `Starting` before binding
/// the producer listeners (spec §6 step 10). Unsettled shards stay `Starting`
/// and are not routed to.
const SHARD_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Polls `done` every `poll` until it returns true (→ true) or `timeout`
/// passes (→ false).
fn wait_until(timeout: Duration, poll: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if done() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(poll);
    }
}
```

In `main`, after the replay (`drop(replay_tx)`) and before building the runtime:

```rust
    // Routing sends connections only to settled, ingest-capable shards, so the
    // listeners wait for the flushers' counter scans (spec §6 step 10; PR 4
    // moves this into its startup step 10).
    // Routing also needs the shard's worker running (PR 1's `ingest_capable`),
    // so wait for every worker too: a connection accepted before a worker has
    // entered its guard would otherwise be refused as no_live_shard.
    if !wait_until(SHARD_SETTLE_TIMEOUT, Duration::from_millis(10), || {
        board.shards.all_settled()
            && (0..board.shards.worker_count()).all(|w| board.shards.worker_running(w))
    }) {
        warn!(
            timeout_secs = SHARD_SETTLE_TIMEOUT.as_secs(),
            "not every WAB shard and worker settled before the producer listeners were \
             bound; unsettled shards receive no connections until they come up"
        );
    }
```

Inside `rt.block_on`, replace the `#[cfg(all(unix, feature = "tls"))] let (tcp_shutdown_tx, tcp_join) = { .. };` block and the `#[cfg(unix)] { use socket::SocketConfig; .. }` accept-loop block with this order — TLS material, then `bind_all`, then spawn TCP, then serve Unix:

```rust
        // One router for both transports: one refusal counter and limiter.
        let router = socket::route::ShardRouter::live(
            Arc::clone(&board) as Arc<dyn socket::route::LiveShards>,
        );

        // TLS material loads before any bind (unchanged position relative to
        // the TCP bind; PR 4 moves it to startup step 6).
        #[cfg(all(unix, feature = "tls"))]
        let tls = match config.tcp_bind {
            Some(_) => {
                let (cert, key, ca) = preflight::tls_paths(&config)?;
                let tls = socket::tls::ReloadableServerConfig::load(cert, key, ca)
                    .map_err(|e| preflight::tls_load_error(&e))?;
                reloader.register_tls(tls.clone());
                Some(tls)
            }
            None => None,
        };

        #[cfg(unix)]
        let socket_config = socket::SocketConfig { /* the `let socket_config = SocketConfig { .. }` literal inside `#[cfg(unix)] { use socket::SocketConfig; .. }`, including PR 1's `board` field, unchanged */ };

        // Bind EVERY producer listener, then serve (spec §5.7, §6 step 11).
        // PR 1's Binding phase moves here with the binds. `bind_all` and
        // `bind` write the listener marks (README Q6); main writes none here.
        board.enter_phase(weir_status::Phase::Binding, &metrics);
        #[cfg(unix)]
        let listeners = socket::bind_all(&socket_config, config.tcp_bind).await?;

        #[cfg(all(unix, feature = "tls"))]
        let (tcp_shutdown_tx, tcp_join) = match (listeners.tcp, tls) {
            (Some(listener), Some(tls)) => {
                let tcp_config = socket::tcp::TcpConfig { /* the `let tcp_config = TcpConfig { .. }` literal in the `Some(bind_addr)` arm, unchanged */ };
                let (tcp_shutdown_tx, tcp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                let tcp_queue_tx = queue_tx.clone();
                let tcp_metrics = Arc::clone(&metrics);
                let tcp_sem = Arc::clone(&conn_sem);
                let tcp_router = router.clone();
                let tcp_board = Arc::clone(&board);
                let tcp_join = tokio::spawn(async move {
                    // PR 1's Closed / Failed marks (Task 1.23). Interim: PR 4
                    // Task 4.7 moves them into `tcp::run` (README Q6).
                    match socket::tcp::run(
                        tcp_config, listener, tls, tcp_queue_tx, tcp_sem,
                        tcp_shutdown_rx, tcp_metrics, tcp_router,
                    )
                    .await
                    {
                        Ok(()) => tcp_board.listeners.tcp.set(weir_status::ListenerState::Closed),
                        Err(e) => {
                            tcp_board.listeners.tcp.set(weir_status::ListenerState::Failed);
                            tracing::error!(error = %e, "TCP+mTLS listener exited with error");
                        }
                    }
                });
                (Some(tcp_shutdown_tx), Some(tcp_join))
            }
            _ => (None, None),
        };
```

The signal-handler `tokio::spawn(async move { .. })` task between the two blocks (:1635-1659 at 3adc5f5) is unchanged. The Unix arm becomes:

```rust
        #[cfg(unix)]
        socket::serve(
            socket_config,
            listeners.unix,
            queue_tx,
            shutdown_rx,
            Arc::clone(&metrics),
            conn_sem,
            router,
        )
        .await?;
```

The `board.enter_phase(weir_status::Phase::Binding, &metrics)` line PR 1 put before the TCP block is the one shown above `bind_all` (delete the old one). PR 1's TCP block also marked the TCP listener `Bound` in `main` after its bind; that mark goes with the block and is not re-added (`bind_all` writes it now, README Q6). The `let conn_sem = ..` line (:1551 at 3adc5f5) stays above `bind_all`. The `if let Some(h) = tcp_join { let _ = h.await; }` block is unchanged. The old comment "Bind in the caller so a bad bind address fails startup (rather than the spawned accept task)" moves onto `bind_all`'s doc (above). In a non-tls unix build `listeners.tcp` is always `None` (config validation rejects `tcp_bind`) and nothing reads it; the `cfg_attr(not(feature = "tls"), allow(dead_code))` on the field covers that build, as it does for `Config::tcp_bind`. Run `cargo clippy -p weir-server --all-targets -- -D warnings` with default features, with `--all-features` and with `--no-default-features` to confirm (after this task `ShardRouter::round_robin`, `Kind::RoundRobin`, `SocketConfig::shard_count` and `TcpConfig::shard_count` are read only from test code or not at all; Tasks 3.9 and 3.10 gave each its allowance).

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --bin weir-server bind_all -- --test-threads=1` and `cargo test -p weir-server --bin weir-server wait_until -- --test-threads=1` → pass.
Run: `cargo test -p weir-server --test system -- --test-threads=1` → pass (in particular `concurrent_producers_all_acked_with_multiple_shards`: live routing over 4 healthy shards still fans out).
Run: `cargo test -p weir-server --test tls_listener --test tls_client --all-features -- --test-threads=1` → pass.
Run: `cargo test -p weir-server --test system --all-features -- --test-threads=1` → pass.

- [ ] **Step 5: Commit**

`git commit -m "feat(startup): bind every producer listener before serving any, and route to live shards"`

---

### Task 3.12: `test-hooks` feature, the counter-scan hook, and routing system tests (spec §16 system test 8)

*PR-level merge gate: PR 3's **Merge gate** (top of the PR 3 section), run after rebasing onto PR 2.*

**Files:**
- Modify: `crates/weir-server/Cargo.toml` `[features]` (add `test-hooks = []`)
- Create: `crates/weir-server/src/test_hooks.rs`
- Modify: `crates/weir-server/src/main.rs:1-22` (`#[cfg(feature = "test-hooks")] mod test_hooks;`) and right after the tracing subscriber is installed (call `test_hooks::warn_if_compiled_in()`)
- Modify: `crates/weir-server/src/wab/mod.rs` `flusher_thread` counter-scan loop (851-873)
- Test: `crates/weir-server/tests/system.rs` (two tests); `crates/weir-server/tests/docs_drift.rs` (one guard)

**Interfaces:**
- Consumes: PR 1's `CounterScanFailed` → `Offline(CounterScanFailed)` path; `ShardRouter::live` (Task 3.11); testkit `weir_server!`, `.shard_count`, `.env`, `.start`, `.client`, `.scrape_metrics`, `wab_dir`.
- Produces:
  - Cargo feature `test-hooks` (owned here; PR 4 adds `WEIR_TEST_STARTUP_DELAY_MS` to it)
  - `crate::test_hooks::{enabled() -> bool, fail_counter_scan(shard_id: u16) -> bool, warn_if_compiled_in()}` (env: `WEIR_TEST_HOOKS=1` gates every hook; `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN=<comma-separated shard ids>`)

- [ ] **Step 1: Write the failing tests**

`crates/weir-server/tests/system.rs`:

```rust
/// Spec §16 system test 8: a shard whose counter scan fails goes
/// Offline(CounterScanFailed), and new connections are routed around it.
#[cfg(feature = "test-hooks")]
#[test]
fn routing_avoids_a_shard_whose_counter_scan_failed() {
    let srv = weir_server!("route_offline")
        .shard_count(2)
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN", "1")
        .start();
    // One push per fresh connection: round-robin would put half on shard 1.
    for i in 0..20 {
        let mut c = srv.client();
        c.push(format!("r{i}").as_bytes(), Durability::Durable)
            .unwrap_or_else(|e| panic!("connection {i} was routed to the offline shard: {e}"));
    }
    assert!(wab_dir_bytes(&srv.wab_dir.join("shard_00")) > 0);
    assert_eq!(
        wab_dir_bytes(&srv.wab_dir.join("shard_01")),
        0,
        "nothing may be routed to the offline shard"
    );
}

/// With every shard offline, a connection is closed at accept and counted.
#[cfg(feature = "test-hooks")]
#[test]
fn no_live_shard_refuses_connections_and_counts_them() {
    let srv = weir_server!("route_none_live")
        .shard_count(1)
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN", "0")
        .start();
    let mut c = srv.client();
    assert!(c.push(b"x", Durability::Durable).is_err(), "no shard can take the record");
    let metrics = srv.scrape_metrics();
    let refused: f64 = metrics
        .lines()
        .find_map(|l| l.strip_prefix("weir_connections_refused_no_live_shard_total "))
        .expect("the refusal counter is exported")
        .trim()
        .parse()
        .unwrap();
    // wait_ready's own probe connection is refused too.
    assert!(refused >= 2.0, "refused = {refused}");
}
```

`crates/weir-server/tests/docs_drift.rs`:

```rust
/// `test-hooks` system tests compile only under that feature. The CI line that
/// runs the system tests with `--all-features` is what keeps them from being
/// silently skipped forever (spec §16 "a drift guard asserts the
/// --all-features line runs them").
#[test]
fn the_test_hooks_system_tests_run_in_ci() {
    let cargo = include_str!("../Cargo.toml");
    assert!(cargo.contains("\ntest-hooks = []"), "the test-hooks feature is gone from Cargo.toml");
    let ci = include_str!("../../../.github/workflows/ci.yml");
    assert!(
        ci.lines().any(|l| l.contains("cargo test -p weir-server")
            && l.contains("--test system")
            && l.contains("--all-features")),
        "no CI line runs tests/system.rs with --all-features, so every \
         #[cfg(feature = \"test-hooks\")] system test is skipped"
    );
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p weir-server --test system --features test-hooks routing_avoids -- --test-threads=1`
Expected: error `the package 'weir-server' does not contain this feature: test-hooks`.
Run: `cargo test -p weir-server --test docs_drift the_test_hooks_system_tests_run_in_ci`
Expected: FAIL, "the test-hooks feature is gone from Cargo.toml".

- [ ] **Step 3: Implement**

`crates/weir-server/Cargo.toml`, in `[features]`:

```toml
# Test-only fault hooks for the system tests (spec §16). Compiled in only with
# this feature, and active only when WEIR_TEST_HOOKS=1 is also set; a build
# with it logs a loud WARN at startup. Never enable it for a production build.
test-hooks = []
```

`crates/weir-server/src/test_hooks.rs`:

```rust
//! Test-only hooks (feature `test-hooks`). Each is inert unless the process
//! also has `WEIR_TEST_HOOKS=1`.

use tracing::warn;

pub(crate) fn enabled() -> bool {
    std::env::var("WEIR_TEST_HOOKS").as_deref() == Ok("1")
}

/// `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN=1,3`: those shards' flushers fail their
/// segment-counter scan, so they end `Offline(CounterScanFailed)`.
pub(crate) fn fail_counter_scan(shard_id: u16) -> bool {
    enabled()
        && std::env::var("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN")
            .map(|v| v.split(',').any(|s| s.trim().parse::<u16>() == Ok(shard_id)))
            .unwrap_or(false)
}

pub(crate) fn warn_if_compiled_in() {
    warn!(
        active = enabled(),
        "this weir-server was built with the test-hooks feature; it is for system tests only"
    );
}
```

`main.rs`: `#[cfg(feature = "test-hooks")] mod test_hooks;` with the other `mod` lines; after the config-warnings replay: `#[cfg(feature = "test-hooks")] test_hooks::warn_if_compiled_in();`.

`wab/mod.rs` `flusher_thread`, in the counter-scan loop, replace `match writer.scan_and_advance_counter() {` with:

```rust
            let scan = writer.scan_and_advance_counter();
            #[cfg(feature = "test-hooks")]
            let scan = if crate::test_hooks::fail_counter_scan(shard_id) {
                Err(io::Error::other("test hook: WEIR_TEST_FAIL_SHARD_COUNTER_SCAN"))
            } else {
                scan
            };
            match scan {
```

(The DST build does not enable `test-hooks`, so the harness is unaffected.)

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p weir-server --test system --features test-hooks routing_avoids_a_shard_whose_counter_scan_failed -- --test-threads=1` → pass.
Run: `cargo test -p weir-server --test system --features test-hooks no_live_shard_refuses_connections_and_counts_them -- --test-threads=1` → pass.
Run: `cargo test -p weir-server --test docs_drift the_test_hooks_system_tests_run_in_ci` → pass; mutate ci.yml's `--all-features` system line (drop `--test system`) and watch it fail; restore.
Once PR 2 has merged, PR 3 is rebased onto main and Task 3.7 has landed, run the full PR merge gate listed at the top of this section, then `act -W .github/workflows/ci.yml -j test`, `-j dst` and `-j lint` (the lint job's `--no-default-features` clippy line is the one that catches a dead or unused item in a noop-only build).

- [ ] **Step 5: Commit**

`git commit -m "test(system): routing avoids an offline shard, behind a test-hooks feature"`
