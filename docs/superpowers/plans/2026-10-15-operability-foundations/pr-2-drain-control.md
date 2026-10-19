# Operability Foundations — PR 2: Drain control — DrainControl/DrainRuntime with detached wrappers, live/backlog alternation, pause and Held, retry-stranded, bounded stop, CommitGuard, absorb mode, board publication, DrainExit

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 2: Drain control — DrainControl/DrainRuntime with detached wrappers, live/backlog alternation, pause and Held, retry-stranded, bounded stop, CommitGuard, absorb mode, board publication, DrainExit

**Scope (spec §16, PR table row 2).** `DrainControl` / `DrainRuntime` and the detached wrappers; the `backlog` queue and live/backlog alternation; pause including `Held` at commit-batch granularity; retry-stranded with coalescing; `Abandoned` and the stop deadline; `CommitGuard`; absorb-mode skips; publication to the status board; `DrainExit`. Spec sections 5.4, 8 and the drain parts of 15.

**Depends on.** PR 1 (the `StatusBoard` with its `DrainCell` and `SinkCell`, the board-side value types, the additive drain metrics, arc-swap unconditional). Runs in parallel with PR 3. Shared files: `wab/mod.rs` (this PR only makes `panic_message_str` `pub(crate)`, one line) and `main.rs`. PR 3's `DrainSpawner` (spec §5.12) replaces `build_and_spawn_drain`. **Integrator decision:** this PR merges first; PR 3 rebases onto it, and PR 3 Task 3.7's spawner already carries this PR's `runtime: DrainRuntime` (in `DrainSpawnArgs`) and `JoinHandle<DrainExit>` return type, and takes over the `drain_runtime` local from Task 2.17.

**Behaviour change.** None in production. The drain is wired **Detached** (today's "channel closed = deliver everything, then exit"), with the board `Some` and **no** drain-channel handle outside the pipeline. Nothing calls `request_stop`, `request_pause` or `request_requeue` in production until PR 4 (Lifecycle) and PR 6 (admin). The new behaviour is reachable only from tests. A few internal changes do run in production; each is called out in its task:
- The idle wait is sliced to 50 ms (it was one `recv_timeout` up to `health_poll_interval`), so the Draining head runs about 20 times a second when idle (Task 2.4).
- Rescan finds go to the `backlog` and alternate with live segments, instead of queueing behind `pending` (Task 2.6).
- The idle health-poll rescan waits for the `backlog` to empty as well as `pending` (Task 2.6).
- The drain runtime is shut down with `shutdown_timeout(100 ms)` on every way out of `drain_thread_with`, a panic unwinding included, instead of being dropped (Task 2.14).
- `drain_is_healthy` also reads `weir_drain_paused`, which stays 0 until PR 6 exposes pause (Task 2.16).

**Merge gate.**
1. The `mod tests` block of `crates/weir-server/src/drain/mod.rs` diffs as **additions only** against the merge base (command in Task 2.17).
2. Every existing drain test passes unchanged through the detached wrappers; the 19 new drain tests of spec §16 pass (map in Task 2.17).
3. `cargo test -p weir-server --bins -- --test-threads=1` and `--all-features` both green; `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` clean; `cargo fmt --check` clean.
4. The PR 1 system test "SIGTERM exits within N s" (spec §16 system test 13) still passes.

---

### Interfaces consumed from PR 1 (exact shapes this PR codes against)

The spec (§5.2) names the cells and their fields; PR 1 owns them. This PR only calls the writer methods below and, in tests, loads the named fields. *(Integrator check: every name below matches PR 1 Tasks 1.16-1.17 of this plan exactly.)* Nothing else in this PR depends on PR 1 internals.

```rust
// crate::status (crates/weir-server/src/status/board.rs), PR 1
pub(crate) struct StatusBoard {
    pub(crate) drain: DrainCell,
    pub(crate) sink: SinkCell,
    /// len() snapshots of the drain channel (flushers after each hand-off, drain each wait slice).
    pub(crate) drain_depth: Arc<AtomicUsize>,
    /* … other cells, untouched here … */
}
impl StatusBoard {
    #[cfg(test)] pub(crate) fn for_test() -> Arc<StatusBoard>;   // all cells at their initial values
}

pub(crate) struct DrainCell {                                    // single writer: the drain thread + its supervisor
    pub(crate) state_since_ms: AtomicU64,
    pub(crate) paused_since_ms: AtomicU64,          // 0 = not paused
    pub(crate) held: AtomicBool,
    pub(crate) observed_requeue: AtomicU64,
    pub(crate) generation: AtomicU64,
    pub(crate) in_flight: ArcSwapOption<InFlight>,
    pub(crate) commit_in_flight_since_ms: AtomicU64, // 0 = none
    pub(crate) last_progress_ms: AtomicU64,
    pub(crate) pending_len: AtomicUsize,
    pub(crate) backlog_len: AtomicUsize,
    pub(crate) replay_total: AtomicU64,
    pub(crate) replay_remaining: AtomicU64,
    pub(crate) blocked_since_ms: AtomicU64,         // 0 = not blocked
    pub(crate) stopping: AtomicBool,
    pub(crate) deadline_ms: AtomicU64,              // unix ms; 0 = none
    pub(crate) last_requeue: ArcSwapOption<RequeueResult>,
    pub(crate) exit: ArcSwapOption<DrainExitInfo>,
    /* state: AtomicU8 — encoded by PR 1, written only through set_state */
}
impl DrainCell {
    pub(crate) fn set_state(&self, state: DrainStateValue);       // also stamps state_since_ms on a change
    pub(crate) fn set_paused(&self, paused_since_ms: u64, held: bool);
    pub(crate) fn set_generation(&self, generation: u64);
    pub(crate) fn set_in_flight(&self, in_flight: Option<InFlight>);
    pub(crate) fn commit_started(&self);    // commit_in_flight_since_ms = now; last_progress_ms = now
    pub(crate) fn commit_finished(&self);   // commit_in_flight_since_ms = 0;  last_progress_ms = now
    pub(crate) fn set_queue_lens(&self, pending: usize, backlog: usize);
    pub(crate) fn set_replay(&self, total: u64, remaining: u64);
    pub(crate) fn set_blocked_since(&self, unix_ms: u64);         // 0 clears
    pub(crate) fn set_stopping(&self, deadline_unix_ms: Option<u64>); // stopping = true; deadline_ms = d or 0
    pub(crate) fn set_last_requeue(&self, result: RequeueResult);
    pub(crate) fn set_observed_requeue(&self, ticket: u64);
    pub(crate) fn set_exit(&self, info: DrainExitInfo);
    pub(crate) fn state(&self) -> Option<DrainStateValue>;        // test reads
}

pub(crate) struct SinkCell {                                     // single writer: the drain thread
    pub(crate) last_success_ms: AtomicU64,
    pub(crate) consecutive_failures: AtomicU64,
    pub(crate) last_error: ArcSwapOption<String>,                 // admin-only text
    pub(crate) probe_reason: ArcSwapOption<String>,               // admin-only text
    /* health, health_since_ms, last_failure_ms, last_failure_kind — PR 1 encoding */
}
impl SinkCell {
    pub(crate) fn set_health(&self, health: &SinkHealth, reason: Option<String>); // reason already sanitised
    pub(crate) fn record_success(&self);                         // last_success_ms = now; consecutive_failures = 0
    pub(crate) fn record_failure(&self, kind: SinkFailureKind, message: String); // message already sanitised
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InFlight { pub segment: String, pub attempt: u32, pub retries_left: u32,
                             pub processed: u64, pub next_retry_at_ms: Option<u64> }
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RequeueResult { pub ticket: u64, pub requeued: u64, pub already_queued: u64,
                                  pub scan_error: Option<String>, pub backoff_cut: bool,
                                  pub coalesced: bool, pub at_ms: u64 }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DrainExitReason { Stopped, DeadlineAbandoned, GaveUp }
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrainExitInfo { pub reason: DrainExitReason, pub abandoned: Option<(String, u64)>,
                                  pub pending_left: usize, pub backlog_left: usize,
                                  pub last_panic: Option<String>, pub at_ms: u64 }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SinkFailureKind { Transient, Permanent, Timeout, ContractViolation }
```

Additive metrics registered by PR 1 (`crate::metrics::Metrics` fields, following the existing "field = metric name without `weir_` and `_total`" convention):

```rust
pub drain_paused: Gauge<f64, AtomicU64>,                                  // weir_drain_paused
pub drain_generation: Gauge<f64, AtomicU64>,                              // weir_drain_generation
pub drain_pending_segments: Family<DrainQueueLabel, Gauge<f64, AtomicU64>>, // weir_drain_pending_segments{queue}
pub drain_replay_remaining_segments: Gauge<f64, AtomicU64>,               // weir_drain_replay_remaining_segments
pub drain_segments_abandoned: Counter<u64, AtomicU64>,                    // weir_drain_segments_abandoned_total
// label: pub struct DrainQueueLabel { pub queue: DrainQueue }   enum DrainQueue { live, backlog }
```

### Reconciliation: current code vs spec (the code wins for current behaviour, the spec for target behaviour)

1. **Wrapper signatures.** The spec quotes the pre-safety-floor shapes (`process_segment(seg, sink, cfg, metrics, dl, skip)`). The current tree has `process_segment(segment, sink, config, metrics, dead_letter, skip, last_local_read_chance, hb)` (drain/mod.rs:1953), `commit_batch(payloads, record_ids, segment_created_at, sink, config, metrics, dead_letter, hb)` (:2714), `drain_thread(rx, sink, config, metrics, initial_health_ok, heartbeat, ledger: &mut SegmentLedger)` (:945) and `probe_and_resume_stranded(rt, sink, config, metrics, pending, prev_health_ok, in_flight, exclude, hb)` (:593). The `mod tests` block reaches them through same-named shims (:3995-4112) that call `super::…` with exactly these current signatures. **Those current signatures are the ones the wrappers keep**, as `#[cfg(test)]` thin wrappers over the `_ctl` / `_with` functions. `spawn(rx, sink, cfg, metrics, hb) -> JoinHandle<()>` (:318) is kept the same way. `run_drain_supervised` and `attempt_segment` are never called by tests, so their signatures change in place.
2. **Line numbers.** The spec's drain/mod.rs line numbers predate the safety floor. Anchors at `3adc5f5`: `DrainState` 196-221; `ProcessResult` 224-266; `spawn` 318-329; `run_drain_supervised` 367-463; `probe_and_resume_stranded` 593-695; `drain_thread` 945-1543; health-poll block 1050-1087; segment selection `let next_segment = loop` 1094-1123, its idle `recv_timeout` 1112; spent-budget arm 1178; RetryingTransient arm 1262, its wait loop 1304-1339 (`recv_timeout` 1324), the M2 probe 1349-1360, its `on_transient` closure 1385; BlockedDeadLetterFull arm 1396-1533, its wait 1409-1434 (`recv_timeout` 1425), its probe 1466-1477, the unblock 1509, the shutdown-while-blocked exit 1516-1523; `drain_thread` tail 1535-1543; `next_state_after_process` 1558 (`confirm_and_delete` call 1575); `attempt_segment` 1673-1712; `transition_from_draining` 1862-1888; `process_segment` 1953-2286, its two `commit_batch` calls 2150 and 2170; `apply_batch` 2294; `commit_batch` 2714-2894, its timed block 2760-2763, timeout early return 2766-2785, contract-violation return 2799-2812. In `main.rs`: `build_and_spawn_drain` 869-881 (doc comment 869-872), `replay_tx` 1053, the sink-selection comment 1159-1161 and `match` 1162, the six `build_and_spawn_drain` calls 1168, 1235, 1275, 1309, 1340, 1407, `drain_handle.join()` 1722, `drain_is_healthy` 721-740, `drain_is_healthy_tests` 2235-2331. PR 1 Task 1.25 adds a tracing span at the two commit sites in `process_segment`, and PR 1 Task 1.19 moves a large part of `main.rs`; anchor every edit on the item and statement names quoted here, not on line numbers.
3. **Respawn while Held.** The spec says a respawn while `Held` re-delivers the held segment from record 0 (the H2 window). Since the safety floor, `SegmentLedger` (owned by the supervisor) keeps the resume cursor across a respawn (ledger.rs:359 `record_progress`, :275 `admit`). `Held` records its cursor on entry (Task 2.9), so a respawn while held resumes past the committed prefix. Target behaviour is therefore *better* than the spec states; Task 2.8's respawn test asserts only that the pause survives.
4. **`DrainControl` fields.** The spec's `Inner` lists `pause: AtomicBool, requeue_req: AtomicU64, generation: AtomicU64`. This PR adds three fields: `pause_seq` (so `request_pause` / `request_resume` can return the `u64` the spec's signatures promise), `requeue_seen` (the drain-observed ticket, needed inside the control for coalescing without a board in tests), and `epoch_unix_ms` (to publish the deadline as unix ms).
5. **`Wait::Stop`.** The spec's `wait_slice` returns `Stop`. Here it means exactly "stop requested AND deadline passed", the one stop condition that does not depend on the queues. Natural completion (`input_closed` and every queue empty) is checked where the queues are owned, at the Draining head.
6. **Board value types.** `InFlight`, `RequeueResult`, `DrainExitInfo`, `DrainExitReason` and `SinkFailureKind` live with the board (PR 1), because PR 1's evaluator reads them. This PR's own `DrainExit` (the supervisor's return value) reuses `DrainExitReason` and adds `last_panic`, which the status document's admin view exposes (§9.1 `exit … admin adds last_panic`).
7. **Sanitising admin text.** §5.2 says `last_error` and `probe_reason` are stored after redaction, control-character stripping and a 2048-byte cap. The drain is the writer, so this PR owns `drain::publish::sanitize_admin_text` and passes already-sanitised strings into the PR 1 cells.
8. **Sink and drain gauges.** The drain writes `drain_paused`, `drain_generation`, `drain_pending_segments{queue}`, `drain_replay_remaining_segments` and `drain_segments_abandoned_total` directly, because it is their single writer and the abandonment counter must work without a board (tests). Sink timestamp gauges and `weir_drain_commit_in_flight_seconds` stay the PR 1 poll task's job, read from the cells.
9. **Safety-floor fixes (merged in 2491883):** the (dev, ino) pin (`SegmentLedger::checked_skip`, called in `attempt_segment`), the 3-cycle + 10-minute escalation (`DrainConfig::local_read_escalation_min_age`) and the lock-integrity ticker (main.rs) are in the base; Tasks 2.6, 2.9 and 2.10 were checked against them.

### Shared test scaffolding (introduced in Task 2.3, used by every later task)

New drain tests live in `crates/weir-server/src/drain/tests/foundations.rs`, declared as `mod foundations;` on the last line inside `mod tests { … }` of drain/mod.rs. That one added line is the only change to the `mod tests` block. Being a child of `tests`, the file sees `MockSink` (including its private fields, for struct-update construction), `MockError`, `fast_config`, `tight_dl_config`, `noop_metrics`, `tmp_dir`, `wait_for`, `make_numbered_segment`, `committed_bytes`, `get_confirmed_path`, `drain_panics`, `drain_is_blocked`, `records_retried`, `assert_drain_reports_stopped`, `health_polls_left` and `fresh_ledger`. Run a single new test with:

```bash
cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::<name>
```

---

### Task 2.1: `DrainControl` — the intent cell

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Create: `crates/weir-server/src/drain/control.rs`
- Modify: `crates/weir-server/src/drain/mod.rs:43-44` (add `pub(crate) mod control;` after `pub(crate) mod ledger;`)
- Test: `crates/weir-server/src/drain/control.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: PR 1 `crate::status::unix_ms_now()` (the one wall-clock helper; CROSS-18), otherwise std + tokio.
- Produces:
  ```rust
  pub(crate) const REQUEUE_MIN_INTERVAL: Duration;   // 5 s
  pub(crate) const SLICE: Duration;                  // 50 ms: wait slice, pause floor, deadline poll
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum Lifetime { Attached, Detached }
  #[derive(Clone)] pub(crate) struct DrainControl(Arc<Inner>);
  impl DrainControl {
      pub(crate) fn attached() -> Self;  pub(crate) fn detached() -> Self;  pub(crate) fn mode(&self) -> Lifetime;
      // control side (Lifecycle PR 4, admin PR 6)
      pub(crate) fn begin_shutdown(&self);
      pub(crate) fn request_stop(&self, budget: Duration);
      pub(crate) fn force_stop(&self);
      pub(crate) fn request_pause(&self) -> u64;
      pub(crate) fn request_resume(&self) -> u64;
      pub(crate) fn request_requeue(&self) -> u64;
      // drain side
      pub(crate) fn absorbing(&self) -> bool;
      pub(crate) fn stop_requested(&self) -> bool;
      pub(crate) fn input_closed(&self) -> bool;
      pub(crate) fn deadline_passed(&self) -> bool;
      pub(crate) fn time_left(&self) -> Option<Duration>;
      pub(crate) fn deadline(&self) -> Option<Instant>;
      pub(crate) fn deadline_unix_ms(&self) -> Option<u64>;
      pub(crate) async fn deadline_reached(&self);
      pub(crate) fn pause_requested(&self) -> bool;
      pub(crate) fn pause_seq(&self) -> u64;
      pub(crate) fn requeue_requested(&self) -> u64;
      pub(crate) fn requeue_observed(&self) -> u64;
      pub(crate) fn observe_requeue(&self, ticket: u64);
      pub(crate) fn begin_generation(&self) -> u64;
      pub(crate) fn generation(&self) -> u64;
  }
  ```

- [ ] **Step 1: Write the failing test.** Create `crates/weir-server/src/drain/control.rs` with only the test module (the implementation goes above it in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*; // includes the module's `unix_ms_now` import

    #[test]
    fn a_fresh_control_carries_no_intent() {
        let c = DrainControl::detached();
        assert_eq!(c.mode(), Lifetime::Detached);
        assert_eq!(DrainControl::attached().mode(), Lifetime::Attached);
        assert!(!c.absorbing() && !c.stop_requested() && !c.input_closed());
        assert!(!c.pause_requested() && !c.deadline_passed());
        assert_eq!(c.time_left(), None);
        assert_eq!(c.deadline(), None);
        assert_eq!(c.deadline_unix_ms(), None);
        assert_eq!(
            (c.requeue_requested(), c.requeue_observed(), c.generation(), c.pause_seq()),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn begin_shutdown_turns_on_absorb_and_nothing_else() {
        let c = DrainControl::attached();
        c.begin_shutdown();
        assert!(c.absorbing());
        assert!(!c.stop_requested() && !c.input_closed());
        assert_eq!(c.time_left(), None, "T0 sets no deadline; T2's request_stop does");
    }

    #[test]
    fn request_stop_closes_input_and_sets_a_deadline() {
        let c = DrainControl::attached();
        c.request_stop(Duration::from_secs(10));
        assert!(c.stop_requested() && c.input_closed());
        assert!(!c.absorbing(), "request_stop does not imply absorb; begin_shutdown does");
        let left = c.time_left().expect("a deadline");
        assert!(left > Duration::from_secs(9) && left <= Duration::from_secs(10), "{left:?}");
        assert!(!c.deadline_passed());
    }

    #[test]
    fn force_stop_then_request_stop_keeps_the_forced_deadline() {
        let c = DrainControl::attached();
        c.force_stop();
        c.request_stop(Duration::from_secs(10));
        assert!(
            c.deadline_passed(),
            "fetch_min: main's T2 request_stop(10 s) must not raise a forced deadline"
        );
        assert!(c.absorbing() && c.stop_requested());
        assert_eq!(c.time_left(), Some(Duration::ZERO));
    }

    #[test]
    fn the_deadline_only_ever_moves_earlier() {
        let c = DrainControl::attached();
        c.request_stop(Duration::from_secs(10));
        c.request_stop(Duration::from_secs(1));
        assert!(c.time_left().unwrap() <= Duration::from_secs(1));
        c.request_stop(Duration::from_secs(60));
        assert!(c.time_left().unwrap() <= Duration::from_secs(1), "a longer budget never raises it");
    }

    #[test]
    fn a_huge_budget_saturates_instead_of_wrapping() {
        let c = DrainControl::attached();
        c.request_stop(Duration::MAX);
        assert!(c.stop_requested());
        assert!(!c.deadline_passed());
        assert!(c.time_left().is_some());
    }

    #[test]
    fn the_deadline_is_published_in_unix_milliseconds() {
        let c = DrainControl::attached();
        let before = unix_ms_now();
        c.request_stop(Duration::from_secs(10));
        let after = unix_ms_now();
        let d = c.deadline_unix_ms().expect("a deadline");
        assert!(d + 50 >= before + 10_000 && d <= after + 10_000 + 50, "{d} vs [{before}, {after}]");
    }

    #[test]
    fn deadline_reached_resolves_once_the_deadline_passes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let c = DrainControl::detached();
        c.request_stop(Duration::from_millis(120));
        let t = Instant::now();
        rt.block_on(c.deadline_reached());
        let took = t.elapsed();
        assert!(
            took >= Duration::from_millis(100) && took < Duration::from_millis(400),
            "{took:?}"
        );
    }

    #[test]
    fn deadline_reached_never_resolves_without_a_deadline() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let c = DrainControl::detached();
        let r = rt.block_on(async {
            tokio::time::timeout(Duration::from_millis(200), c.deadline_reached()).await
        });
        assert!(r.is_err(), "no deadline: the future must stay pending");
    }

    #[test]
    fn pause_and_resume_return_increasing_sequence_numbers() {
        let c = DrainControl::detached();
        let a = c.request_pause();
        assert!(c.pause_requested());
        let b = c.request_resume();
        assert!(!c.pause_requested());
        assert!(b > a);
        assert_eq!(c.pause_seq(), b);
        let again = c.request_resume();
        assert!(again > b, "idempotent in effect, but every request is sequenced");
    }

    #[test]
    fn requeue_requests_fold_into_the_outstanding_ticket() {
        let c = DrainControl::detached();
        assert_eq!(c.request_requeue(), 1);
        assert_eq!(c.request_requeue(), 1, "ticket 1 not yet observed: fold into it");
        assert_eq!(c.request_requeue(), 1);
        c.observe_requeue(1);
        assert_eq!(c.request_requeue(), 2, "after observation a request is a new ticket");
        c.observe_requeue(1);
        assert_eq!(c.requeue_observed(), 1, "observation is monotonic (fetch_max)");
        c.observe_requeue(2);
        assert_eq!(c.requeue_observed(), 2);
    }

    #[test]
    fn generations_count_drain_starts() {
        let c = DrainControl::detached();
        assert_eq!(c.begin_generation(), 1);
        assert_eq!(c.begin_generation(), 2);
        assert_eq!(c.generation(), 2);
    }

    #[test]
    fn clones_share_one_intent() {
        let a = DrainControl::attached();
        let b = a.clone();
        b.request_pause();
        b.begin_shutdown();
        assert!(a.pause_requested() && a.absorbing());
    }
}
```

and add the module declaration to `crates/weir-server/src/drain/mod.rs` after line 44:

```rust
pub(crate) mod control;
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::control::`
  Expected: compile failure, `error[E0433]: failed to resolve: use of undeclared type `DrainControl`` (and `Lifetime`, `Duration`, `Instant`, `unix_ms_now`).

- [ ] **Step 3: Implement.** Put this above the test module in `control.rs`:

```rust
//! Drain intent (spec §5.4): what the lifecycle or an operator WANTS the drain
//! to do. The drain reads it; nothing here observes the drain. What the drain
//! has actually done is published to the status board (`drain::publish`).
//!
//! Every field is an atomic on one `Arc`, so a control handle is cheap to clone
//! and never blocks the drain. There is no channel handle here, by design
//! (spec §4): the drain owns the only Receiver of the drain channel.
// Unconditional, not `cfg_attr(not(test), ..)`: `REQUEUE_MIN_INTERVAL` has no
// caller in either build until Task 2.7, and `clippy --all-targets` lints the
// test build too.
#![allow(dead_code, reason = "wired in Tasks 2.3-2.15; replaced by item-level allows in Task 2.17")]

use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
    },
    time::{Duration, Instant},
};

use crate::status::unix_ms_now;

/// At most one operator-driven stranded-segment rescan per this interval;
/// requests inside it are answered from the previous scan (`coalesced: true`).
pub(crate) const REQUEUE_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// The drain's wait slice, the floor on every wait while a pause is in force
/// (so a hold can never spin on `recv_timeout(0)`), and the deadline poll.
pub(crate) const SLICE: Duration = Duration::from_millis(50);

/// `deadline_ms` value meaning "no deadline".
const NO_DEADLINE: u64 = u64::MAX;

/// How the drain treats its channel disconnecting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lifetime {
    /// Production from PR 4: `Disconnected` is a non-event; the drain exits only
    /// on `request_stop` / `force_stop` (or when its panic budget is spent).
    Attached,
    /// Today's semantics, kept for the existing tests and for production until
    /// PR 4: `Disconnected` means "deliver what is queued, then exit".
    Detached,
}

struct Inner {
    mode: Lifetime,
    /// Origin of `deadline_ms`; monotonic, so a wall-clock step cannot move a deadline.
    epoch: Instant,
    /// Wall-clock reading taken with `epoch`, to publish the deadline as unix ms.
    epoch_unix_ms: u64,
    absorb: AtomicBool,
    stop: AtomicBool,
    input_closed: AtomicBool,
    /// Milliseconds since `epoch`; `NO_DEADLINE` = none. Only ever lowered (`fetch_min`).
    deadline_ms: AtomicU64,
    pause: AtomicBool,
    pause_seq: AtomicU64,
    /// The newest requeue ticket handed out.
    requeue_req: AtomicU64,
    /// The newest ticket the drain has resolved.
    requeue_seen: AtomicU64,
    /// One bump per `drain_thread_with` start.
    generation: AtomicU64,
}

/// Shared drain intent. Clone freely; every clone is the same cell.
#[derive(Clone)]
pub(crate) struct DrainControl(Arc<Inner>);

impl DrainControl {
    fn new(mode: Lifetime) -> Self {
        let epoch_unix_ms = unix_ms_now();
        Self(Arc::new(Inner {
            mode,
            epoch: Instant::now(),
            epoch_unix_ms,
            absorb: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            input_closed: AtomicBool::new(false),
            deadline_ms: AtomicU64::new(NO_DEADLINE),
            pause: AtomicBool::new(false),
            pause_seq: AtomicU64::new(0),
            requeue_req: AtomicU64::new(0),
            requeue_seen: AtomicU64::new(0),
            generation: AtomicU64::new(0),
        }))
    }

    pub(crate) fn attached() -> Self {
        Self::new(Lifetime::Attached)
    }

    pub(crate) fn detached() -> Self {
        Self::new(Lifetime::Detached)
    }

    pub(crate) fn mode(&self) -> Lifetime {
        self.0.mode
    }

    fn now_ms(&self) -> u64 {
        // Capped one below the sentinel so "now" can never read as "no deadline".
        u64::try_from(self.0.epoch.elapsed().as_millis())
            .unwrap_or(NO_DEADLINE - 1)
            .min(NO_DEADLINE - 1)
    }

    fn deadline_raw(&self) -> Option<u64> {
        let d = self.0.deadline_ms.load(Acquire);
        (d != NO_DEADLINE).then_some(d)
    }

    // ── Control side ─────────────────────────────────────────────────────────

    /// T0 (first SIGTERM): absorb mode — move channel arrivals into `pending`
    /// on every wait slice; skip probes, rescans and gauge walks.
    pub(crate) fn begin_shutdown(&self) {
        self.0.absorb.store(true, Release);
    }

    /// T2 (after the flusher joins): input is closed, deliver until `budget`
    /// from now, then abandon. The deadline only ever moves earlier, and is
    /// stored before `stop` so a drain that sees `stop` sees the deadline.
    pub(crate) fn request_stop(&self, budget: Duration) {
        self.0.input_closed.store(true, Release);
        let budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
        let at = self.now_ms().saturating_add(budget_ms).min(NO_DEADLINE - 1);
        self.0.deadline_ms.fetch_min(at, AcqRel);
        self.0.stop.store(true, Release);
    }

    /// Second signal: absorb, and a deadline of now. A later `request_stop`
    /// cannot raise it (both use `fetch_min`).
    pub(crate) fn force_stop(&self) {
        self.0.absorb.store(true, Release);
        self.0.deadline_ms.fetch_min(self.now_ms(), AcqRel);
        self.0.stop.store(true, Release);
    }

    /// Pause delivery before the next commit batch. Returns the request's sequence number.
    pub(crate) fn request_pause(&self) -> u64 {
        self.0.pause.store(true, Release);
        self.0.pause_seq.fetch_add(1, AcqRel) + 1
    }

    /// Resume delivery. Returns the request's sequence number.
    pub(crate) fn request_resume(&self) -> u64 {
        self.0.pause.store(false, Release);
        self.0.pause_seq.fetch_add(1, AcqRel) + 1
    }

    /// Ask for a stranded-segment rescan. A request made while an earlier
    /// ticket is still unobserved folds into it and returns that ticket, so a
    /// caller looping on this cannot queue more than one scan (spec §5.4).
    pub(crate) fn request_requeue(&self) -> u64 {
        loop {
            let req = self.0.requeue_req.load(Acquire);
            if req > self.0.requeue_seen.load(Acquire) {
                return req;
            }
            if self
                .0
                .requeue_req
                .compare_exchange(req, req + 1, AcqRel, Acquire)
                .is_ok()
            {
                return req + 1;
            }
        }
    }

    // ── Drain side ───────────────────────────────────────────────────────────

    pub(crate) fn absorbing(&self) -> bool {
        self.0.absorb.load(Acquire)
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.0.stop.load(Acquire)
    }

    pub(crate) fn input_closed(&self) -> bool {
        self.0.input_closed.load(Acquire)
    }

    pub(crate) fn deadline_passed(&self) -> bool {
        self.deadline_raw().is_some_and(|d| self.now_ms() >= d)
    }

    pub(crate) fn time_left(&self) -> Option<Duration> {
        self.deadline_raw()
            .map(|d| Duration::from_millis(d.saturating_sub(self.now_ms())))
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline_raw()
            .map(|d| self.0.epoch + Duration::from_millis(d))
    }

    pub(crate) fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_raw()
            .map(|d| self.0.epoch_unix_ms.saturating_add(d))
    }

    /// Resolves once the deadline has passed; pending forever without one.
    /// Polls every [`SLICE`] on the drain's current-thread runtime, so it is
    /// cheap to race against a sink commit in `select!`.
    pub(crate) async fn deadline_reached(&self) {
        loop {
            if self.deadline_passed() {
                return;
            }
            let nap = self
                .time_left()
                .map_or(SLICE, |left| left.min(SLICE))
                .max(Duration::from_millis(1));
            tokio::time::sleep(nap).await;
        }
    }

    pub(crate) fn pause_requested(&self) -> bool {
        self.0.pause.load(Acquire)
    }

    pub(crate) fn pause_seq(&self) -> u64 {
        self.0.pause_seq.load(Acquire)
    }

    pub(crate) fn requeue_requested(&self) -> u64 {
        self.0.requeue_req.load(Acquire)
    }

    pub(crate) fn requeue_observed(&self) -> u64 {
        self.0.requeue_seen.load(Acquire)
    }

    /// The drain resolved `ticket` (and every ticket before it).
    pub(crate) fn observe_requeue(&self, ticket: u64) {
        self.0.requeue_seen.fetch_max(ticket, AcqRel);
    }

    /// Called once per `drain_thread_with` start; returns the new generation.
    pub(crate) fn begin_generation(&self) -> u64 {
        self.0.generation.fetch_add(1, AcqRel) + 1
    }

    pub(crate) fn generation(&self) -> u64 {
        self.0.generation.load(Acquire)
    }
}
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::control::`
  Expected: `test result: ok. 13 passed`.

- [ ] **Step 5: Commit.**
  `git add crates/weir-server/src/drain/control.rs crates/weir-server/src/drain/mod.rs`
  `git commit -m "feat(drain): DrainControl, the drain's intent cell"` (body: names the fetch_min deadline, the pause sequence, ticket folding; no trailers).

---

### Task 2.2: `Publisher`, `CommitGuard` and `sanitize_admin_text`

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Create: `crates/weir-server/src/drain/publish.rs`
- Modify: `crates/weir-server/src/drain/mod.rs` (add `mod publish;` after the `control` line from Task 2.1)
- Test: `crates/weir-server/src/drain/publish.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes (PR 1): `crate::status::{StatusBoard, InFlight, RequeueResult, DrainExitInfo, SinkFailureKind}`, `StatusBoard::for_test()`, the `DrainCell` / `SinkCell` writer methods listed above; `crate::sink::redact_url_password` (sink/mod.rs:50); `crate::metrics::DrainStateValue`; `crate::sink::SinkHealth`; `super::DrainHeartbeat::bump`.
- Produces:
  ```rust
  pub(crate) const ADMIN_TEXT_MAX: usize;                           // 2048
  pub(crate) fn sanitize_admin_text(s: &str) -> String;
  // No wall-clock helper of its own: every unix-ms stamp in PR 2 is PR 1's
  // `crate::status::unix_ms_now()` (CROSS-18).
  pub(crate) struct Publisher { board: Option<Arc<StatusBoard>> }
  impl Publisher {
      pub(crate) fn new(board: Option<Arc<StatusBoard>>) -> Self;
      pub(crate) fn none() -> Self;
      pub(crate) fn state(&self, s: &DrainStateValue);
      pub(crate) fn paused(&self, paused_since_ms: u64, held: bool);
      pub(crate) fn generation(&self, g: u64);
      pub(crate) fn in_flight(&self, f: Option<InFlight>);
      pub(crate) fn queue_lens(&self, pending: usize, backlog: usize);
      pub(crate) fn channel_depth(&self, depth: usize);
      pub(crate) fn replay(&self, total: u64, remaining: u64);
      pub(crate) fn blocked_since(&self, unix_ms: u64);
      pub(crate) fn stopping(&self, deadline_unix_ms: Option<u64>);
      pub(crate) fn requeue(&self, r: &RequeueResult);
      pub(crate) fn exit(&self, info: DrainExitInfo);
      pub(crate) fn sink_health(&self, h: &SinkHealth);
      pub(crate) fn sink_success(&self);
      pub(crate) fn sink_failure(&self, kind: SinkFailureKind, msg: &str);
  }
  pub(crate) struct CommitGuard<'a> { .. }
  impl<'a> CommitGuard<'a> { pub(crate) fn new(publish: &'a Publisher, hb: &'a DrainHeartbeat) -> Self; }
  impl Drop for CommitGuard<'_>;
  ```

- [ ] **Step 1: Write the failing test.** Create `publish.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn sanitize_redacts_credentials_and_strips_control_characters() {
        let raw = "error sending request for url \
                   (https://user:hunter2@sink.example/ingest?token=s3cr3t&x=1): \
                   connection refused\n\u{1b}[31mspoofed log line\u{7}";
        let out = sanitize_admin_text(raw);
        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("s3cr3t"), "{out}");
        assert!(out.contains("sink.example"), "the host stays for diagnosis: {out}");
        assert!(!out.chars().any(char::is_control), "{out:?}");
    }

    #[test]
    fn sanitize_caps_at_2048_bytes_on_a_char_boundary() {
        let raw = "é".repeat(3000); // 6000 bytes, 2 bytes per char
        let out = sanitize_admin_text(&raw);
        assert!(out.len() <= ADMIN_TEXT_MAX);
        assert!(out.len() >= ADMIN_TEXT_MAX - 1);
        assert!(out.chars().all(|c| c == 'é'), "never cut inside a char");
    }

    #[test]
    fn a_publisher_without_a_board_accepts_every_write() {
        let p = Publisher::none();
        let hb = DrainHeartbeat::new();
        p.state(&DrainStateValue::draining);
        p.paused(1, true);
        p.generation(3);
        p.in_flight(None);
        p.queue_lens(1, 2);
        p.channel_depth(7);
        p.replay(3, 1);
        p.blocked_since(0);
        p.stopping(Some(1));
        p.sink_health(&SinkHealth::Down("x".into()));
        p.sink_success();
        p.sink_failure(SinkFailureKind::Transient, "x");
        {
            let _g = CommitGuard::new(&p, &hb);
        }
        assert!(hb.get() >= 2, "the heartbeat moves with or without a board");
    }

    #[test]
    fn a_commit_guard_marks_the_commit_in_flight_and_clears_it_on_drop() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let hb = DrainHeartbeat::new();
        let before = hb.get();
        {
            let _g = CommitGuard::new(&p, &hb);
            assert!(board.drain.commit_in_flight_since_ms.load(Relaxed) > 0);
            assert!(board.drain.last_progress_ms.load(Relaxed) > 0);
        }
        assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0);
        assert!(hb.get() >= before + 2, "bumped on construction and on drop");
    }

    #[test]
    fn a_commit_guard_clears_the_marker_when_the_commit_unwinds() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let hb = DrainHeartbeat::new();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = CommitGuard::new(&p, &hb);
            panic!("sink panicked mid-commit");
        }));
        assert!(r.is_err());
        assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0);
    }

    #[test]
    fn sink_text_is_sanitised_before_it_reaches_the_board() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        p.sink_failure(
            SinkFailureKind::Transient,
            &format!("https://u:hunter2@h/x?token=s3cr3t \u{7}{}", "z".repeat(5000)),
        );
        let e = board.sink.last_error.load_full().expect("last_error stored");
        assert!(!e.contains("hunter2") && !e.contains("s3cr3t"));
        assert!(!e.chars().any(char::is_control));
        assert!(e.len() <= ADMIN_TEXT_MAX);
        p.sink_health(&SinkHealth::Down("https://u:hunter2@h/ down".into()));
        let r = board.sink.probe_reason.load_full().expect("probe_reason stored");
        assert!(!r.contains("hunter2"));
    }

    #[test]
    fn requeue_publishes_the_result_and_the_observed_ticket() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let r = RequeueResult {
            ticket: 4,
            requeued: 2,
            already_queued: 1,
            scan_error: None,
            backoff_cut: true,
            coalesced: false,
            at_ms: 9,
        };
        p.requeue(&r);
        assert_eq!(board.drain.observed_requeue.load(Relaxed), 4);
        assert_eq!(board.drain.last_requeue.load_full().as_deref(), Some(&r));
    }

    #[test]
    fn channel_depth_goes_to_the_shared_depth_atomic() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        p.channel_depth(17);
        assert_eq!(board.drain_depth.load(Relaxed), 17);
    }
}
```

and in `drain/mod.rs`, under `pub(crate) mod control;`:

```rust
mod publish;
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::publish::`
  Expected: compile failure, `cannot find function `sanitize_admin_text`` / `cannot find struct `Publisher``.

- [ ] **Step 3: Implement.** Above the test module in `publish.rs`:

```rust
//! The drain's writes to the status board (spec §5.2, §5.4 "Publication").
//!
//! The drain thread (and its supervisor) is the single writer of the drain and
//! sink cells. Every write is a wait-free atomic or ArcSwap store; with no
//! board (the detached test wiring) every write is a no-op, so the drain's
//! logic never branches on whether it is observed.
// Unconditional, not `cfg_attr(not(test), ..)`: `Publisher::exit` has no caller
// in either build until Task 2.14, and `clippy --all-targets` lints the test
// build too.
#![allow(dead_code, reason = "wired in Tasks 2.3-2.15; replaced by item-level allows in Task 2.17")]

use std::sync::{Arc, atomic::Ordering::Relaxed};

use super::DrainHeartbeat;
use crate::{
    metrics::DrainStateValue,
    sink::SinkHealth,
    status::{DrainExitInfo, InFlight, RequeueResult, SinkFailureKind, StatusBoard},
};

/// Cap on any free text the drain stores for the admin view (spec §5.2).
pub(crate) const ADMIN_TEXT_MAX: usize = 2048;

/// Makes a sink or probe message safe to keep for the admin view: URL userinfo
/// passwords and credential query parameters redacted
/// (`sink::redact_url_password`, which also runs the query-credential pass),
/// control characters replaced with `.`, and the result capped at
/// [`ADMIN_TEXT_MAX`] bytes on a char boundary. Never reaches the public view:
/// the public status type has no field that can hold it (spec §5.1).
pub(crate) fn sanitize_admin_text(s: &str) -> String {
    let redacted = crate::sink::redact_url_password(s);
    let mut out: String = redacted
        .chars()
        .map(|c| if c.is_control() { '.' } else { c })
        .collect();
    if out.len() > ADMIN_TEXT_MAX {
        let mut cut = ADMIN_TEXT_MAX;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
    }
    out
}

/// The drain's handle on the status board. `None` = unobserved (tests).
pub(crate) struct Publisher {
    board: Option<Arc<StatusBoard>>,
}

impl Publisher {
    pub(crate) fn new(board: Option<Arc<StatusBoard>>) -> Self {
        Self { board }
    }

    pub(crate) fn none() -> Self {
        Self { board: None }
    }

    pub(crate) fn state(&self, s: &DrainStateValue) {
        if let Some(b) = &self.board {
            b.drain.set_state(s.clone());
        }
    }

    pub(crate) fn paused(&self, paused_since_ms: u64, held: bool) {
        if let Some(b) = &self.board {
            b.drain.set_paused(paused_since_ms, held);
        }
    }

    pub(crate) fn generation(&self, g: u64) {
        if let Some(b) = &self.board {
            b.drain.set_generation(g);
        }
    }

    pub(crate) fn in_flight(&self, f: Option<InFlight>) {
        if let Some(b) = &self.board {
            b.drain.set_in_flight(f);
        }
    }

    pub(crate) fn queue_lens(&self, pending: usize, backlog: usize) {
        if let Some(b) = &self.board {
            b.drain.set_queue_lens(pending, backlog);
        }
    }

    /// A `len()` snapshot of the drain channel. Last-writer-wins with the
    /// flushers' snapshots (spec §4): a store, never an inc/dec.
    pub(crate) fn channel_depth(&self, depth: usize) {
        if let Some(b) = &self.board {
            b.drain_depth.store(depth, Relaxed);
        }
    }

    pub(crate) fn replay(&self, total: u64, remaining: u64) {
        if let Some(b) = &self.board {
            b.drain.set_replay(total, remaining);
        }
    }

    pub(crate) fn blocked_since(&self, unix_ms: u64) {
        if let Some(b) = &self.board {
            b.drain.set_blocked_since(unix_ms);
        }
    }

    pub(crate) fn stopping(&self, deadline_unix_ms: Option<u64>) {
        if let Some(b) = &self.board {
            b.drain.set_stopping(deadline_unix_ms);
        }
    }

    /// Publishes the result BEFORE the observed ticket, so an admin client that
    /// polls for its ticket always finds the matching result.
    pub(crate) fn requeue(&self, r: &RequeueResult) {
        if let Some(b) = &self.board {
            b.drain.set_last_requeue(r.clone());
            b.drain.set_observed_requeue(r.ticket);
        }
    }

    pub(crate) fn exit(&self, info: DrainExitInfo) {
        if let Some(b) = &self.board {
            b.drain.set_exit(info);
        }
    }

    pub(crate) fn sink_health(&self, h: &SinkHealth) {
        if let Some(b) = &self.board {
            let reason = match h {
                SinkHealth::Degraded(r) | SinkHealth::Down(r) => Some(sanitize_admin_text(r)),
                _ => None,
            };
            b.sink.set_health(h, reason);
        }
    }

    pub(crate) fn sink_success(&self) {
        if let Some(b) = &self.board {
            b.sink.record_success();
        }
    }

    pub(crate) fn sink_failure(&self, kind: SinkFailureKind, msg: &str) {
        if let Some(b) = &self.board {
            b.sink.record_failure(kind, sanitize_admin_text(msg));
        }
    }
}

/// RAII marker for "a sink commit is in flight" (spec §5.4 CommitGuard).
///
/// Construction stores `commit_in_flight_since = now`, stamps
/// `last_progress_ms` and bumps the heartbeat; `Drop` clears the marker and does
/// the same stamp and bump. Taken immediately before the commit `select!` in
/// `commit_batch_ctl`, so every way out of a commit — success, the
/// `commit_timeout` early return, the contract-violation return, `Abandoned` at
/// the deadline, and a panic unwinding through it — clears the marker. A
/// marker left set would read as a wedged drain (`drain_wedged`) forever.
pub(crate) struct CommitGuard<'a> {
    publish: &'a Publisher,
    hb: &'a DrainHeartbeat,
}

impl<'a> CommitGuard<'a> {
    pub(crate) fn new(publish: &'a Publisher, hb: &'a DrainHeartbeat) -> Self {
        if let Some(b) = &publish.board {
            b.drain.commit_started();
        }
        hb.bump();
        Self { publish, hb }
    }
}

impl Drop for CommitGuard<'_> {
    fn drop(&mut self) {
        if let Some(b) = &self.publish.board {
            b.drain.commit_finished();
        }
        self.hb.bump();
    }
}
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::publish::`
  Expected: `test result: ok. 8 passed`.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): board publisher, CommitGuard and admin-text sanitising"` (after `git add crates/weir-server/src/drain/publish.rs`).

---
### Task 2.3: `DrainRuntime`, `DrainCtx`, `DrainExit` and the detached wrappers (no behaviour change)

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/control.rs` (add `DrainRuntime`, `DrainExit`)
- Modify: `crates/weir-server/src/drain/mod.rs` — `spawn` (318-329), `run_drain_supervised` (367-463), `drain_thread` (945-1543), `attempt_segment` (1673-1712), `process_segment` (1953-2286), `commit_batch` (2714-2894); add `mod foundations;` as the last line inside `mod tests` (before the closing brace at the end of the file)
- Modify: `crates/weir-server/src/main.rs:869-881` (`build_and_spawn_drain` and its doc comment), its six call sites (1168, 1235, 1275, 1309, 1340, 1407) and the sink-selection comment above the `match` (1159-1161)
- Create: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: Task 2.1 `DrainControl`; Task 2.2 `Publisher`, `CommitGuard`; PR 1 `crate::status::{StatusBoard, DrainExitReason}`.
- Produces:
  ```rust
  // drain/control.rs
  pub(crate) struct DrainRuntime { pub(crate) control: DrainControl,
                                   pub(crate) board: Option<Arc<StatusBoard>>,
                                   pub(crate) initial_backlog: Vec<PathBuf> }
  impl DrainRuntime { pub(crate) fn detached() -> Self; }
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub(crate) struct DrainExit { pub(crate) reason: DrainExitReason,
                                pub(crate) abandoned: Option<(String, u64)>, // (segment_identity, records processed)
                                pub(crate) pending_left: usize,              // pending + channel residue
                                pub(crate) backlog_left: usize,
                                pub(crate) last_panic: Option<String> }
  // drain/mod.rs
  pub(crate) use control::{DrainControl, DrainExit, DrainRuntime};
  pub(crate) fn spawn_with<S: Sink + 'static>(drain_rx: crossbeam_channel::Receiver<PathBuf>, sink: Arc<S>,
      config: DrainConfig, metrics: Arc<Metrics>, heartbeat: DrainHeartbeat, runtime: DrainRuntime)
      -> std::thread::JoinHandle<DrainExit>;
  pub(crate) struct DrainCtx<'a> { pub(crate) control: &'a DrainControl, pub(crate) publish: &'a Publisher,
                                   pub(crate) hb: &'a DrainHeartbeat }
  struct ThreadExit { reason: DrainExitReason, abandoned: Option<(PathBuf, u64)>, pending_left: usize, backlog_left: usize }
  fn run_drain_supervised<S: Sink>(drain_rx, sink: Arc<S>, config: DrainConfig, metrics: Arc<Metrics>,
      heartbeat: DrainHeartbeat, runtime: DrainRuntime) -> DrainExit;
  #[allow(clippy::too_many_arguments)] fn drain_thread_with<S: Sink>(drain_rx, sink: Arc<S>, config: DrainConfig, metrics: Arc<Metrics>,
      initial_health_ok: bool, heartbeat: DrainHeartbeat, ledger: &mut SegmentLedger,
      control: &DrainControl, publish: &Publisher, initial_backlog: Vec<PathBuf>) -> ThreadExit;
  fn attempt_segment<S: Sink>(rt, segment, sink, config, metrics, dead_letter, ledger, skip: u64, ctx: &DrainCtx<'_>) -> ProcessResult;
  async fn process_segment_ctl<S: Sink>(segment: &Path, sink: &S, config: &DrainConfig, metrics: &Metrics,
      dead_letter: &mut DeadLetterWriter, skip: u64, last_local_read_chance: bool, ctx: &DrainCtx<'_>) -> ProcessResult;
  async fn commit_batch_ctl<S: Sink>(payloads: &[Payload], record_ids: &[RecordId], segment_created_at: i64, sink: &S,
      config: &DrainConfig, metrics: &Metrics, dead_letter: &mut DeadLetterWriter, ctx: &DrainCtx<'_>) -> BatchResult;
  // kept, #[cfg(test)], current signatures, delegating with DrainRuntime::detached() / DrainControl::detached() + Publisher::none():
  pub fn spawn<S: Sink + 'static>(drain_rx, sink: Arc<S>, config: DrainConfig, metrics: Arc<Metrics>, heartbeat: DrainHeartbeat) -> std::thread::JoinHandle<()>;
  fn drain_thread<S: Sink>(drain_rx, sink, config, metrics, initial_health_ok: bool, heartbeat: DrainHeartbeat, ledger: &mut SegmentLedger);
  async fn process_segment<S: Sink>(segment, sink, config, metrics, dead_letter, skip: u64, last_local_read_chance: bool, hb: &DrainHeartbeat) -> ProcessResult;
  async fn commit_batch<S: Sink>(payloads, record_ids, segment_created_at: i64, sink, config, metrics, dead_letter, hb: &DrainHeartbeat) -> BatchResult;
  ```

- [ ] **Step 1: Write the failing test.** Add the module line as the last line inside `mod tests { … }` in drain/mod.rs:

```rust
    // ── Foundations: drain control (operability spec §5.4, §8) ───────────────
    mod foundations;
```

and create `crates/weir-server/src/drain/tests/foundations.rs`:

```rust
//! Foundations PR 2 drain tests (operability spec §16 "Drain", new tests 1-19).
//! A child of `drain::tests`, so the existing helpers and `MockSink` are in scope.

use super::*;
use crate::drain::control::{DrainControl, DrainRuntime};
use crate::drain::publish::Publisher;
use crate::drain::{DrainExit, spawn_with};
use crate::status::{DrainExitReason, StatusBoard};
use crate::wab::{
    format::Compression,
    segment::{WabSegment, segment_path},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// A sealed shard-0 segment with an explicit counter, so a test can make many.
fn seg(dir: &Path, counter: u64, payloads: &[&[u8]]) -> PathBuf {
    let shard_dir = dir.join("shard_00");
    crate::testutil::mkdir_p(&shard_dir);
    let mut s =
        WabSegment::create(&segment_path(&shard_dir, counter), 0, Compression::None).unwrap();
    for p in payloads {
        s.write_record(p).unwrap();
    }
    s.seal().unwrap()
}

fn rt_with(
    control: DrainControl,
    board: Option<Arc<StatusBoard>>,
    initial_backlog: Vec<PathBuf>,
) -> DrainRuntime {
    DrainRuntime { control, board, initial_backlog }
}

fn start<S: Sink + 'static>(
    rx: crossbeam_channel::Receiver<PathBuf>,
    sink: &Arc<S>,
    config: DrainConfig,
    metrics: &Arc<Metrics>,
    runtime: DrainRuntime,
) -> JoinHandle<DrainExit> {
    spawn_with(rx, Arc::clone(sink), config, Arc::clone(metrics), DrainHeartbeat::new(), runtime)
}

/// Joins `h`, failing the test if the drain has not exited within `limit`.
fn join_within(h: JoinHandle<DrainExit>, limit: Duration) -> DrainExit {
    let deadline = Instant::now() + limit;
    while !h.is_finished() {
        assert!(Instant::now() < deadline, "the drain did not exit within {limit:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
    h.join().expect("the drain thread panicked")
}

/// `fast_config` with the health poll pushed out of the test's way.
fn quiet(dir: PathBuf) -> DrainConfig {
    DrainConfig {
        health_poll_interval: Duration::from_secs(60),
        ..fast_config(dir)
    }
}

#[test]
fn a_detached_drain_reports_a_clean_stop_when_its_channel_closes() {
    let dir = tmp_dir("fnd_detached_exit");
    let sealed = seg(&dir, 1, &[b"one"]);
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(sealed.clone()).unwrap();
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(DrainControl::detached(), None, Vec::new()),
    );
    wait_for(5, "the segment is delivered", || !sealed.exists());
    drop(tx);
    let exit = join_within(h, Duration::from_secs(5));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(exit.abandoned, None);
    assert_eq!((exit.pending_left, exit.backlog_left), (0, 0));
    assert_eq!(exit.last_panic, None);
    assert_eq!(sink.call_count(), 1);
    assert_drain_reports_stopped(&metrics, "detached channel close");
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn every_drain_start_takes_a_new_generation() {
    let dir = tmp_dir("fnd_generation");
    let control = DrainControl::detached();
    let publish = Publisher::none();
    for _ in 0..2 {
        let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
        drop(tx);
        let mut ledger = fresh_ledger();
        let exit = crate::drain::drain_thread_with(
            rx,
            Arc::new(MockSink::with_responses([])),
            quiet(dir.clone()),
            noop_metrics(),
            true,
            DrainHeartbeat::new(),
            &mut ledger,
            &control,
            &publish,
            Vec::new(),
        );
        assert_eq!(exit.reason, DrainExitReason::Stopped);
    }
    assert_eq!(control.generation(), 2);
    std::fs::remove_dir_all(dir).ok();
}
```

(Imports are kept to what each task's tests use, so `clippy --all-targets -D warnings` stays clean at every commit. Later tasks name the imports they add in their Step 1. `MockSink`, `MockError`, `Mutex`, `VecDeque` and the helpers listed above already come in through `use super::*`.)

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `unresolved imports `crate::drain::DrainExit`, `crate::drain::spawn_with``, `cannot find function `drain_thread_with``.

- [ ] **Step 3: Implement.**

In `control.rs` (above the test module):

```rust
use std::path::PathBuf;
use crate::status::{DrainExitReason, StatusBoard};

/// Everything the drain is started with besides its sink and config (spec §5.4).
/// Kept out of `DrainConfig` so the existing `DrainConfig { .. }` test literals
/// compile unchanged.
pub(crate) struct DrainRuntime {
    pub(crate) control: DrainControl,
    /// `None` = unobserved (tests); production passes the board from PR 1.
    pub(crate) board: Option<Arc<StatusBoard>>,
    /// The startup replay list, handed over in memory (PR 3/4). Used on the
    /// drain's FIRST start only; a respawn recovers it through its rescan.
    pub(crate) initial_backlog: Vec<PathBuf>,
}

impl DrainRuntime {
    /// Today's wiring: Detached, no board, no backlog.
    pub(crate) fn detached() -> Self {
        Self {
            control: DrainControl::detached(),
            board: None,
            initial_backlog: Vec::new(),
        }
    }
}

/// Why and how the drain stopped. The input to main's T3 summary (PR 4),
/// built without new I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrainExit {
    pub(crate) reason: DrainExitReason,
    /// The segment left partly delivered (`segment_identity`, records processed).
    pub(crate) abandoned: Option<(String, u64)>,
    /// Paths still in `pending` plus what was left in the channel.
    pub(crate) pending_left: usize,
    pub(crate) backlog_left: usize,
    /// The last drain panic's message, sanitised (admin view only).
    pub(crate) last_panic: Option<String>,
}
```

In `drain/mod.rs`, after the module declarations:

```rust
pub(crate) use control::{DrainControl, DrainExit, DrainRuntime};
use crate::status::DrainExitReason;
use publish::Publisher;

/// What the drain's inner functions need besides their data: the intent to
/// obey, the board to publish to, the heartbeat to bump. Borrowed for one
/// drain incarnation.
#[allow(dead_code, reason = "control and publish are first read in Task 2.4, which removes this")]
pub(crate) struct DrainCtx<'a> {
    pub(crate) control: &'a DrainControl,
    pub(crate) publish: &'a Publisher,
    pub(crate) hb: &'a DrainHeartbeat,
}

/// How one `drain_thread_with` incarnation ended (a panic never returns one).
struct ThreadExit {
    reason: DrainExitReason,
    abandoned: Option<(PathBuf, u64)>,
    pending_left: usize,
    backlog_left: usize,
}
```

Replace `spawn` (318-329) with:

```rust
/// Spawns the supervised drain thread. The handle yields how the drain ended.
pub(crate) fn spawn_with<S: Sink + 'static>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    heartbeat: DrainHeartbeat,
    runtime: DrainRuntime,
) -> std::thread::JoinHandle<DrainExit> {
    std::thread::Builder::new()
        .name("weir-drain".into())
        .spawn(move || run_drain_supervised(drain_rx, sink, config, metrics, heartbeat, runtime))
        .expect("failed to spawn drain thread")
}

/// The pre-Foundations entry point, kept with its signature for the existing
/// tests: a Detached drain with no board (spec §5.4 "Entry points").
#[cfg(test)]
pub fn spawn<S: Sink + 'static>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    heartbeat: DrainHeartbeat,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("weir-drain".into())
        .spawn(move || {
            run_drain_supervised(drain_rx, sink, config, metrics, heartbeat, DrainRuntime::detached());
        })
        .expect("failed to spawn drain thread")
}
```

`run_drain_supervised` (367-463): new signature `(drain_rx, sink, config, metrics, heartbeat, runtime: DrainRuntime) -> DrainExit`. At the top:

```rust
    let DrainRuntime { control, board, initial_backlog } = runtime;
    let publish = Publisher::new(board);
    // The replay list goes to the FIRST incarnation only (spec §5.4 "Supervisor").
    let mut first_backlog = Some(initial_backlog);
```

The loop becomes `let exit = loop { … };`. Inside, before the `catch_unwind`:

```rust
        let backlog = first_backlog.take().unwrap_or_default();
        let (ctl, pb) = (&control, &publish);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drain_thread_with(rx, sink, cfg, m, first_start, hb, lent, ctl, pb, backlog)
        }));
        let _payload = match result {
            Ok(t) => {
                break DrainExit {
                    reason: t.reason,
                    abandoned: t.abandoned.map(|(p, n)| (segment_identity(&p), n)),
                    pending_left: t.pending_left,
                    backlog_left: t.backlog_left,
                    last_panic: None,
                };
            }
            Err(payload) => payload,
        };
```

(the existing `let Err(_) = result else { break };` line is what this replaces). The give-up branch becomes:

```rust
        if attempts >= config.max_respawns {
            error!(
                attempts,
                "drain thread panicked too many times; giving up — delivery is \
                 stopped and the WAB will accumulate on disk until restart"
            );
            break DrainExit {
                reason: DrainExitReason::GaveUp,
                abandoned: None,
                pending_left: drain_rx.len(),
                backlog_left: 0,
                last_panic: None,
            };
        }
```

After the loop, keep `set_drain_state(&metrics, DrainStateValue::stopped); info!(…);` and return `exit`.

Rename `drain_thread` to `drain_thread_with` with the produced signature, and put `#[allow(clippy::too_many_arguments)]` on the line above `fn drain_thread_with` (10 parameters; clippy's default limit is 7 and the repo has no `clippy.toml`). Body changes:

```rust
    let ctx = DrainCtx { control, publish, hb: &heartbeat };
    let generation = control.begin_generation();
    publish.generation(generation);
    metrics.drain_generation.set(generation as f64);
    // ...
    // Task 2.5 moves the replay list into its own `backlog` queue.
    let mut pending: VecDeque<PathBuf> = initial_backlog.into();
```

`'outer: loop {` becomes `let reason = 'outer: loop {`. The two existing `break 'outer;` (the Draining idle wait's `Err(RecvTimeoutError::Disconnected) => break 'outer,` and the Blocked arm's shutdown-while-blocked exit) become `break 'outer DrainExitReason::Stopped;`. Every `attempt_segment(…, &heartbeat)` call (Draining arm, RetryingTransient arm) passes `&ctx` instead. The tail (after `info!("drain thread exiting");`) ends with:

```rust
    ThreadExit {
        reason,
        abandoned: None,
        pending_left: pending.len() + drain_rx.len(),
        backlog_left: 0,
    }
```

The test-facing wrapper, directly below `drain_thread_with`:

```rust
/// The pre-Foundations entry, kept with its signature for the existing tests:
/// Detached, unobserved, no backlog.
#[cfg(test)]
fn drain_thread<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    initial_health_ok: bool,
    heartbeat: DrainHeartbeat,
    ledger: &mut SegmentLedger,
) {
    let control = DrainControl::detached();
    let publish = Publisher::none();
    drain_thread_with(
        drain_rx, sink, config, metrics, initial_health_ok, heartbeat, ledger, &control, &publish,
        Vec::new(),
    );
}
```

`attempt_segment`: replace the `hb: &DrainHeartbeat` parameter with `ctx: &DrainCtx<'_>`; first line of the body `let hb = ctx.hb;`; the inner call becomes `rt.block_on(process_segment_ctl(segment, sink, config, metrics, dead_letter, skip, last_chance, ctx))`.

Rename `process_segment` to `process_segment_ctl`, replacing its last parameter `hb: &DrainHeartbeat` with `ctx: &DrainCtx<'_>` and adding `let hb = ctx.hb;` as the first statement (so every inner `hb` use is unchanged; its existing `#[allow(clippy::too_many_arguments)]` stays). Both `commit_batch(…, hb)` calls (the in-loop full batch and the tail batch; PR 1 Task 1.25 chains `.instrument(segment_span.clone())` onto each, which stays) become `commit_batch_ctl(…, ctx)`. Rename `commit_batch` to `commit_batch_ctl` the same way (`ctx: &DrainCtx<'_>` last, `let hb = ctx.hb;` first). Add the two test-facing wrappers:

```rust
/// Pre-Foundations signature for the existing tests: Detached, unobserved.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn process_segment<S: Sink>(
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    skip: u64,
    last_local_read_chance: bool,
    hb: &DrainHeartbeat,
) -> ProcessResult {
    let control = DrainControl::detached();
    let publish = Publisher::none();
    let ctx = DrainCtx { control: &control, publish: &publish, hb };
    process_segment_ctl(segment, sink, config, metrics, dead_letter, skip, last_local_read_chance, &ctx)
        .await
}

/// Pre-Foundations signature for the existing tests: Detached, unobserved.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn commit_batch<S: Sink>(
    payloads: &[Payload],
    record_ids: &[RecordId],
    segment_created_at: i64,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    hb: &DrainHeartbeat,
) -> BatchResult {
    let control = DrainControl::detached();
    let publish = Publisher::none();
    let ctx = DrainCtx { control: &control, publish: &publish, hb };
    commit_batch_ctl(payloads, record_ids, segment_created_at, sink, config, metrics, dead_letter, &ctx)
        .await
}
```

In `main.rs`, `build_and_spawn_drain` and its doc comment:

```rust
/// Wraps a built sink and spawns the drain — the tail shared by every sink arm.
/// `drain::spawn_with` is generic over the sink but returns the same
/// `JoinHandle<drain::DrainExit>`, so each arm produces a uniform handle for
/// the join sequence.
fn build_and_spawn_drain<S: sink::Sink + 'static>(
    sink: S,
    drain_rx: crossbeam_channel::Receiver<std::path::PathBuf>,
    drain_config: DrainConfig,
    metrics: Arc<metrics::Metrics>,
    heartbeat: DrainHeartbeat,
    runtime: drain::DrainRuntime,
) -> std::thread::JoinHandle<drain::DrainExit> {
    drain::spawn_with(drain_rx, Arc::new(sink), drain_config, metrics, heartbeat, runtime)
}
```

and each of the six call sites gains a last argument `drain::DrainRuntime::detached(),` (Task 2.17 replaces it with the board-carrying runtime). The comment above `let drain_handle = match config.sink_type {` becomes:

```rust
    // Sink selection. `drain::spawn_with` is generic over the sink but returns
    // the same `JoinHandle<drain::DrainExit>`, so each arm produces a uniform
    // handle for the join sequence.
```

`drain_handle.join().ok();` is unchanged.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: every existing `drain::tests::*` test passes unchanged, plus the two new `drain::tests::foundations::*` tests and the Task 2.1/2.2 unit tests.

- [ ] **Step 5: Commit.**
  `git commit -am "refactor(drain): DrainRuntime and DrainCtx behind detached wrappers"` (after `git add crates/weir-server/src/drain/tests/foundations.rs`). Body: "No behaviour change; the existing tests reach the drain through wrappers with their current signatures."

---

### Task 2.4: `wait_slice`, the Attached lifetime, and the stop predicate

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — add `Wait`, `wait_slice`, `stop_exit` above `drain_thread_with`; replace the three `recv_timeout` sites (the Draining idle wait inside `let next_segment = loop`, 1112; the RetryingTransient wait loop, 1304-1339; the Blocked wait loop, 1409-1434); the Draining head
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainCtx`, `DrainControl::{mode, absorbing, stop_requested, deadline_passed, time_left, pause_requested, input_closed}`, `Publisher::{channel_depth, queue_lens}`, `control::SLICE`.
- Produces:
  ```rust
  #[derive(Debug, PartialEq, Eq)] enum Wait { Segment(PathBuf), TimedOut, Stop, Disconnected }
  fn wait_slice(rx: &crossbeam_channel::Receiver<PathBuf>, ctx: &DrainCtx<'_>, dur: Duration,
                pending: &mut VecDeque<PathBuf>, backlog_len: usize, channel_gone: &mut bool) -> Wait;
  fn stop_exit(control: &DrainControl, queues_empty: bool, rx: &crossbeam_channel::Receiver<PathBuf>) -> Option<DrainExitReason>;
  ```

- [ ] **Step 1: Write the failing test.** Extend the imports at the top of `foundations.rs` to `use crate::drain::{DrainCtx, DrainExit, Wait, spawn_with, wait_slice};` and add `use std::collections::VecDeque;`, then append:

```rust
/// Spec §16 drain test 1 (the zombie regression): an Attached drain whose every
/// flusher Sender is gone stays alive, keeps delivering what its rescan finds on
/// disk, and exits only on request_stop.
#[test]
fn an_attached_drain_outlives_its_flushers_and_stops_only_on_request() {
    let dir = tmp_dir("fnd_zombie");
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
    drop(tx); // shard_count = 1 and its flusher died: nothing else holds a Sender
    let control = DrainControl::attached();
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let h = start(rx, &sink, fast_config(dir.clone()), &metrics, rt_with(control.clone(), None, Vec::new()));

    std::thread::sleep(Duration::from_millis(200));
    assert!(!h.is_finished(), "an attached drain must not read Disconnected as shutdown");

    let late = seg(&dir, 1, &[b"late"]);
    wait_for(5, "the idle rescan delivers a segment found on disk", || sink.call_count() == 1);
    assert!(!h.is_finished());

    control.request_stop(Duration::from_secs(5));
    let exit = join_within(h, Duration::from_secs(2));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert!(!late.exists());
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 3: a stop with budget to spare delivers everything still
/// queued (here: all of it in the channel) and exits on natural completion.
#[test]
fn a_stop_with_budget_left_delivers_everything_queued_then_exits() {
    let dir = tmp_dir("fnd_stop_drains_queue");
    let segs: Vec<PathBuf> = (1..=5)
        .map(|n| seg(&dir, n, &[format!("s{n}").as_bytes()]))
        .collect();
    let (tx, rx) = crossbeam_channel::bounded(256);
    for s in &segs {
        tx.send(s.clone()).unwrap();
    }
    drop(tx);
    let control = DrainControl::attached();
    control.request_stop(Duration::from_millis(500)); // input already closed at start
    let sink = Arc::new(MockSink::with_responses([]));
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(control, None, Vec::new()));
    let exit = join_within(h, Duration::from_secs(2));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(sink.call_count(), 5);
    assert!(segs.iter().all(|s| !s.exists()), "all confirmed and deleted");
    assert_eq!((exit.pending_left, exit.backlog_left, exit.abandoned), (0, 0, None));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn wait_slice_reads_a_disconnect_by_lifetime() {
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    drop(tx);
    let hb = DrainHeartbeat::new();
    let publish = Publisher::none();
    let mut pending = VecDeque::new();
    let mut gone = false;

    let detached = DrainControl::detached();
    let ctx = DrainCtx { control: &detached, publish: &publish, hb: &hb };
    assert_eq!(
        wait_slice(&rx, &ctx, Duration::from_millis(10), &mut pending, 0, &mut gone),
        Wait::Disconnected
    );
    assert!(!gone);

    let attached = DrainControl::attached();
    let ctx = DrainCtx { control: &attached, publish: &publish, hb: &hb };
    assert_eq!(
        wait_slice(&rx, &ctx, Duration::from_millis(10), &mut pending, 0, &mut gone),
        Wait::TimedOut
    );
    assert!(gone, "attached: the disconnect is remembered");
    let t = Instant::now();
    assert_eq!(
        wait_slice(&rx, &ctx, Duration::from_millis(30), &mut pending, 0, &mut gone),
        Wait::TimedOut
    );
    assert!(t.elapsed() >= Duration::from_millis(25), "a gone channel sleeps the slice, never spins");
    assert!(hb.get() >= 3, "every slice bumps the heartbeat");
}

#[test]
fn wait_slice_returns_stop_once_the_deadline_has_passed() {
    let (_tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let control = DrainControl::attached();
    control.force_stop();
    let (hb, publish) = (DrainHeartbeat::new(), Publisher::none());
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let t = Instant::now();
    assert_eq!(
        wait_slice(&rx, &ctx, Duration::from_secs(1), &mut VecDeque::new(), 0, &mut false),
        Wait::Stop
    );
    assert!(t.elapsed() < Duration::from_millis(20), "Stop is returned without waiting");
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `unresolved imports `crate::drain::Wait`, `crate::drain::wait_slice``.

- [ ] **Step 3: Implement.** Extend the re-export to `pub(crate) use control::{DrainControl, DrainExit, DrainRuntime, Lifetime};` and delete the temporary `#[allow(dead_code, …)]` on `DrainCtx` (its fields are read from here on). Then, above `drain_thread_with` in drain/mod.rs:

```rust
/// What one [`wait_slice`] produced.
#[derive(Debug, PartialEq, Eq)]
enum Wait {
    /// A segment arrived on the channel.
    Segment(PathBuf),
    /// The slice elapsed (in Attached mode, also after the channel is gone).
    TimedOut,
    /// `stop` is set and its deadline has passed: stop waiting.
    Stop,
    /// The channel disconnected and the drain is Detached: today's shutdown.
    Disconnected,
}

/// The drain's single way to wait (spec §5.4 "Wait helper"), replacing the
/// three `recv_timeout` sites: the Draining idle wait, the retry backoff and the
/// dead-letter-full block.
///
/// One slice of at most [`control::SLICE`], with a [`control::SLICE`] FLOOR
/// while a pause is requested, so a paused hold whose deadline has already
/// elapsed can never spin on `recv_timeout(0)`; never past a stop deadline. In
/// absorb mode it first moves everything already in the channel into
/// `pending`. Every slice bumps the heartbeat (a waiting drain is alive) and
/// publishes the channel depth and the queue lengths.
///
/// Attached mode: a disconnected channel is a non-event. One WARN, then plain
/// sleeps; the drain keeps delivering its queues and rescans until stopped.
fn wait_slice(
    rx: &crossbeam_channel::Receiver<PathBuf>,
    ctx: &DrainCtx<'_>,
    dur: Duration,
    pending: &mut VecDeque<PathBuf>,
    backlog_len: usize,
    channel_gone: &mut bool,
) -> Wait {
    ctx.hb.bump();
    if ctx.control.absorbing() {
        while let Ok(p) = rx.try_recv() {
            pending.push_back(p);
        }
    }
    ctx.publish.channel_depth(rx.len());
    ctx.publish.queue_lens(pending.len(), backlog_len);
    if ctx.control.stop_requested() && ctx.control.deadline_passed() {
        return Wait::Stop;
    }
    let mut slice = dur.min(control::SLICE);
    if ctx.control.pause_requested() {
        slice = control::SLICE;
    }
    if let Some(left) = ctx.control.time_left() {
        slice = slice.min(left.max(Duration::from_millis(1)));
    }
    if *channel_gone {
        std::thread::sleep(slice);
        return Wait::TimedOut;
    }
    match rx.recv_timeout(slice) {
        Ok(p) => Wait::Segment(p),
        Err(RecvTimeoutError::Timeout) => Wait::TimedOut,
        Err(RecvTimeoutError::Disconnected) => match ctx.control.mode() {
            Lifetime::Detached => Wait::Disconnected,
            Lifetime::Attached => {
                warn!("drain: all flushers gone; continuing to deliver the on-disk backlog");
                *channel_gone = true;
                std::thread::sleep(slice);
                Wait::TimedOut
            }
        },
    }
}

/// The Draining head's exit check (spec §5.4 "Lifetime"). With `stop` set: exit
/// at the deadline, or once input is closed and nothing is left (no queued
/// path, nothing in the channel). Without `stop`: never. Held and in-flight
/// segments are the arms' own business; the head holds neither.
fn stop_exit(
    control: &DrainControl,
    queues_empty: bool,
    rx: &crossbeam_channel::Receiver<PathBuf>,
) -> Option<DrainExitReason> {
    if !control.stop_requested() {
        return None;
    }
    let nothing_left = queues_empty && rx.is_empty();
    if control.deadline_passed() {
        return Some(if nothing_left {
            DrainExitReason::Stopped
        } else {
            DrainExitReason::DeadlineAbandoned
        });
    }
    (control.input_closed() && nothing_left).then_some(DrainExitReason::Stopped)
}
```

In `drain_thread_with`, add `let mut channel_gone = false;` with the other locals. At the top of the Draining arm, after `set_drain_state(&metrics, DrainStateValue::draining);`:

```rust
                if let Some(reason) = stop_exit(control, pending.is_empty(), &drain_rx) {
                    break 'outer reason;
                }
```

In `let next_segment = loop { … }`, the idle wait (`let until_next_poll = …;` and `match drain_rx.recv_timeout(until_next_poll) { … }`) becomes:

```rust
                    let until_next_poll = config
                        .health_poll_interval
                        .saturating_sub(last_health_poll.elapsed())
                        .max(Duration::from_millis(1));
                    match wait_slice(&drain_rx, &ctx, until_next_poll, &mut pending, 0, &mut channel_gone) {
                        Wait::Segment(p) => match ledger.admit(&p, Instant::now()) {
                            Admit::Process { skip } => break Some((p, skip)),
                            Admit::Drop(owner) => {
                                debug!(path = %p.display(), owner, "drain: not admitting segment from the channel");
                                break None;
                            }
                        },
                        // Back to the head: the poll, the stop check and the
                        // verbs run there, at least once per slice.
                        Wait::TimedOut | Wait::Stop => break None,
                        Wait::Disconnected => break 'outer DrainExitReason::Stopped,
                    }
```

RetryingTransient wait loop (`let wait_end = Instant::now() + next_delay; loop { … }`): delete the `heartbeat.bump();` at the top of the loop body (wait_slice bumps) and replace the `recv_timeout` match with:

```rust
                    let remaining = wait_end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match wait_slice(&drain_rx, &ctx, remaining, &mut pending, 0, &mut channel_gone) {
                        Wait::Segment(new_seg) => pending.push_back(new_seg),
                        Wait::TimedOut => {}
                        // Task 2.11 turns this into an exit; until then the
                        // retry runs at once and the Draining head exits.
                        Wait::Stop => break,
                        // Detached shutdown: serve the clamped remainder (W2), unchanged.
                        Wait::Disconnected => {
                            let left = wait_end
                                .saturating_duration_since(Instant::now())
                                .min(SHUTDOWN_RETRY_BACKOFF_CAP)
                                .min(shutdown_backoff_left);
                            shutdown_backoff_left -= left;
                            std::thread::sleep(left);
                            break;
                        }
                    }
```

Blocked wait loop (after `let mut channel_closed = false;`): delete its `heartbeat.bump();` and use:

```rust
                    let remaining = check_end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match wait_slice(&drain_rx, &ctx, remaining, &mut pending, 0, &mut channel_gone) {
                        Wait::Segment(new_seg) => pending.push_back(new_seg),
                        Wait::TimedOut => {}
                        Wait::Stop | Wait::Disconnected => {
                            channel_closed = true;
                            break;
                        }
                    }
```

(Task 2.11 renames the Blocked arm's local `channel_closed` to `stop_seen` and gives it the stop semantics.)

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all existing drain tests and every `foundations` test so far pass. In particular `a_real_idle_drain_thread_advances_the_heartbeat`, `a_drain_in_a_long_retry_backoff_keeps_advancing_the_heartbeat` and `a_drain_blocked_on_dead_letter_headroom_keeps_advancing_the_heartbeat` stay green: `wait_slice` bumps the heartbeat on every slice.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): one wait helper; an attached drain survives its flushers"`

---

### Task 2.5: the `backlog` queue, live/backlog alternation and `initial_backlog`

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `drain_thread_with` locals and the Draining segment selection (`let next_segment = loop`, 1094-1123); `stop_exit` call and every `wait_slice(…, 0, …)` call (pass `backlog.len()`); `ThreadExit` construction
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainRuntime::initial_backlog` (via `run_drain_supervised`'s first start), `ledger.admit`.
- Produces:
  ```rust
  fn next_queued(pending: &mut VecDeque<PathBuf>, backlog: &mut VecDeque<PathBuf>,
                 rx: &crossbeam_channel::Receiver<PathBuf>, backlog_turn: &mut bool) -> Option<PathBuf>;
  ```

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// Spec §16 drain test 14 (alternation). The spec's figure is a 300-segment
/// backlog; 30 keeps the run short and still separates the two behaviours by
/// an order of magnitude: without alternation the second live send waits for
/// the whole backlog (~1.5 s), with it for at most one backlog segment and one
/// live segment.
#[test]
fn live_segments_alternate_with_a_long_backlog() {
    let dir = tmp_dir("fnd_alternate");
    let backlog: Vec<PathBuf> = (1..=30).map(|n| seg(&dir, n, &[b"b"])).collect();
    let per_commit = Duration::from_millis(50);
    let sink = Arc::new(MockSink::with_yielding_commit(per_commit, 1000));
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &noop_metrics(),
        rt_with(DrainControl::detached(), None, backlog),
    );
    wait_for(5, "replay under way", || sink.call_count() >= 1);
    let mut worst = Duration::ZERO;
    for n in 1001..=1006 {
        let live = seg(&dir, n, &[b"l"]);
        let t = Instant::now();
        tx.send(live).unwrap();
        worst = worst.max(t.elapsed());
    }
    // A send on a bounded(1) channel waits for the drain to take the previous
    // live segment: at most one backlog segment plus one live segment (2× the
    // segment time), plus 100 ms of scheduling and fsync slack.
    assert!(
        worst <= per_commit * 2 + Duration::from_millis(100),
        "a live sender waited {worst:?} behind the replay backlog"
    );
    drop(tx);
    join_within(h, Duration::from_secs(10));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 15: the replay list plus a panic on its first segment.
/// The respawn has no backlog (the panic took it), but its first healthy probe
/// is a recovery edge, so the rescan finds every segment on disk again.
#[test]
fn an_initial_backlog_survives_a_panic_on_its_first_segment() {
    let dir = tmp_dir("fnd_backlog_panic");
    let segs: Vec<PathBuf> = (1..=3).map(|n| seg(&dir, n, &[b"r"])).collect();
    let sink = Arc::new(MockSink::with_panic(1, []));
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(DrainControl::detached(), None, segs.clone()),
    );
    wait_for(10, "every replayed segment is delivered after the respawn", || {
        segs.iter().all(|s| !s.exists())
    });
    assert_eq!(drain_panics(&metrics), 1);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn next_queued_alternates_and_falls_back_to_whichever_side_has_work() {
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let p = |s: &str| PathBuf::from(s);
    let mut pending = VecDeque::from([p("l1"), p("l2")]);
    let mut backlog = VecDeque::from([p("b1"), p("b2"), p("b3")]);
    tx.send(p("c1")).unwrap();
    let mut turn = false;
    let mut got = Vec::new();
    while let Some(x) =
        crate::drain::next_queued(&mut pending, &mut backlog, &rx, &mut turn)
    {
        got.push(x);
    }
    assert_eq!(got, ["l1", "b1", "l2", "b2", "c1", "b3"].map(PathBuf::from));
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `cannot find function `next_queued``. (Before the function exists, `live_segments_alternate_with_a_long_backlog` would fail its assertion at ~1.5 s, since Task 2.3 put the replay list at the front of `pending`.)

- [ ] **Step 3: Implement.** Add above `drain_thread_with`:

```rust
/// Picks the next queued segment, alternating the live side (`pending`, then
/// the channel) with the `backlog` (spec §5.4 "Two queues"). While both have
/// work, the channel is served at least every other segment, so a long replay
/// or a large requeue never stalls the flushers' hand-off for more than one
/// backlog segment. Either side alone is drained in FIFO order.
fn next_queued(
    pending: &mut VecDeque<PathBuf>,
    backlog: &mut VecDeque<PathBuf>,
    rx: &crossbeam_channel::Receiver<PathBuf>,
    backlog_turn: &mut bool,
) -> Option<PathBuf> {
    if !(*backlog_turn && !backlog.is_empty()) {
        if let Some(p) = pending.pop_front().or_else(|| rx.try_recv().ok()) {
            *backlog_turn = true;
            return Some(p);
        }
    }
    let p = backlog.pop_front()?;
    *backlog_turn = false;
    Some(p)
}
```

In `drain_thread_with`, replace `let mut pending: VecDeque<PathBuf> = initial_backlog.into();` with:

```rust
    let mut pending: VecDeque<PathBuf> = VecDeque::new();
    // The startup replay list and every rescan's finds (Task 2.6). Alternates
    // with `pending` and the channel; see `next_queued`.
    let mut backlog: VecDeque<PathBuf> = initial_backlog.into();
    let mut backlog_turn = false;
```

The Draining head exit check becomes `stop_exit(control, pending.is_empty() && backlog.is_empty(), &drain_rx)`. The segment selection loop (`let next_segment = loop { … };`) becomes:

```rust
                let next_segment = loop {
                    if let Some(p) = next_queued(&mut pending, &mut backlog, &drain_rx, &mut backlog_turn) {
                        match ledger.admit(&p, Instant::now()) {
                            Admit::Process { skip } => break Some((p, skip)),
                            Admit::Drop(owner) => {
                                debug!(path = %p.display(), owner, "drain: not admitting queued segment");
                                continue;
                            }
                        }
                    }
                    // Idle: nothing queued on either side and the channel is
                    // empty. Wait no longer than the time left until the next
                    // health poll, then re-enter Draining so the poll runs.
                    let until_next_poll = config
                        .health_poll_interval
                        .saturating_sub(last_health_poll.elapsed())
                        .max(Duration::from_millis(1));
                    match wait_slice(&drain_rx, &ctx, until_next_poll, &mut pending, backlog.len(), &mut channel_gone) {
                        Wait::Segment(p) => match ledger.admit(&p, Instant::now()) {
                            Admit::Process { skip } => break Some((p, skip)),
                            Admit::Drop(owner) => {
                                debug!(path = %p.display(), owner, "drain: not admitting segment from the channel");
                                break None;
                            }
                        },
                        // Back to the head: the poll, the stop check and the
                        // verbs run there, at least once per slice.
                        Wait::TimedOut | Wait::Stop => break None,
                        Wait::Disconnected => break 'outer DrainExitReason::Stopped,
                    }
                };
```

Every other `wait_slice(…, &mut pending, 0, …)` (the RetryingTransient and Blocked wait loops) passes `backlog.len()` instead of `0`. `ThreadExit` gets `backlog_left: backlog.len()`.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass; the alternation test reports its worst send well under 200 ms.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): a backlog queue that alternates with live segments"`

---

### Task 2.6: `requeue_unconfirmed` — one rescan path, the backlog-aware idle gate

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `probe_and_resume_stranded` (593-695) becomes `probe_and_resume_stranded_with` plus a `#[cfg(test)]` wrapper with the current signature; add `RescanOutcome`, `requeue_unconfirmed`, `RESCAN_CALLS`; the three call sites (Draining health poll 1050-1087, RetryingTransient M2 1349-1360, Blocked 1466-1477)
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

Verified against 2491883: `wab::scan_unconfirmed_sealed(&config.wab_dir, None, metrics, exclude, config.orphan_cleanup)` (wab/mod.rs:400) and `ledger.excludes(p, now)` (ledger.rs:310) are unchanged by the safety floor's fixes; this task only moves the call.

**Interfaces:**
- Consumes: `crate::wab::{scan_unconfirmed_sealed, SealedScan}` (wab/mod.rs:400, :543), `BusyGuard`/`BusyKind::DirScan`, `Publisher::sink_health`.
- Produces:
  ```rust
  #[derive(Clone, Debug, Default, PartialEq, Eq)]
  pub(crate) struct RescanOutcome { pub(crate) requeued: u64, pub(crate) already_queued: u64,
                                    pub(crate) scan_error: Option<String>, pub(crate) incomplete: bool }
  #[allow(clippy::too_many_arguments)]
  fn requeue_unconfirmed(config: &DrainConfig, metrics: &Metrics, rx: Option<&crossbeam_channel::Receiver<PathBuf>>,
      pending: &mut VecDeque<PathBuf>, backlog: &mut VecDeque<PathBuf>, in_flight: Option<&Path>,
      exclude: &dyn Fn(&Path) -> bool, hb: &DrainHeartbeat) -> RescanOutcome;
  #[allow(clippy::too_many_arguments)]
  fn probe_and_resume_stranded_with<S: Sink>(rt: &tokio::runtime::Runtime, sink: &S, config: &DrainConfig,
      metrics: &Metrics, rx: Option<&crossbeam_channel::Receiver<PathBuf>>, pending: &mut VecDeque<PathBuf>,
      backlog: &mut VecDeque<PathBuf>, prev_health_ok: bool, in_flight: Option<&Path>,
      exclude: &dyn Fn(&Path) -> bool, ctx: &DrainCtx<'_>) -> bool;
  #[cfg(test)] static RESCAN_CALLS: AtomicU64;   // every requeue_unconfirmed scan
  ```

- [ ] **Step 1: Write the failing test.** Add `use std::sync::atomic::Ordering::Relaxed;` to the imports, then append to `foundations.rs`:

```rust
/// The rescan drains the channel into `pending` FIRST, so a path that is both
/// in the channel and on disk is counted as already queued, never queued twice.
#[test]
fn a_rescan_drains_the_channel_first_and_never_double_queues() {
    let dir = tmp_dir("fnd_rescan_dedup");
    let a = seg(&dir, 1, &[b"a"]);
    let b = seg(&dir, 2, &[b"b"]);
    let (tx, rx) = crossbeam_channel::bounded(256);
    tx.send(a.clone()).unwrap();
    let (mut pending, mut backlog) = (VecDeque::new(), VecDeque::new());
    let out = crate::drain::requeue_unconfirmed(
        &fast_config(dir.clone()),
        &noop_metrics(),
        Some(&rx),
        &mut pending,
        &mut backlog,
        None,
        &|_| false,
        &DrainHeartbeat::new(),
    );
    assert_eq!(pending, VecDeque::from([a.clone()]));
    assert_eq!(backlog, VecDeque::from([b.clone()]));
    assert_eq!((out.requeued, out.already_queued, out.scan_error), (1, 1, None));
    assert!(rx.is_empty());
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 13: auto-resume during replay. With the replay list
/// still in the backlog, a healthy health poll must not rescan (the old gate
/// looked only at `pending`, so it rescanned every poll for the whole replay
/// and double-queued whatever sat in the channel).
#[test]
fn the_idle_rescan_waits_for_the_backlog_to_empty() {
    crate::drain::RESCAN_CALLS.store(0, Relaxed);
    let dir = tmp_dir("fnd_idle_gate");
    let backlog: Vec<PathBuf> = (1..=5).map(|n| seg(&dir, n, &[b"b"])).collect();
    let sink = Arc::new(MockSink::with_yielding_commit(Duration::from_millis(100), 1000));
    let cfg = DrainConfig {
        health_poll_interval: Duration::from_millis(20),
        ..fast_config(dir.clone())
    };
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(DrainControl::detached(), None, backlog));
    std::thread::sleep(Duration::from_millis(300));
    assert!(sink.call_count() < 5, "the replay must still be running for this to prove anything");
    assert_eq!(
        crate::drain::RESCAN_CALLS.load(Relaxed),
        0,
        "a health poll during replay must not rescan"
    );
    wait_for(5, "replay finished", || sink.call_count() == 5);
    wait_for(2, "the idle rescan resumes once both queues are empty", || {
        crate::drain::RESCAN_CALLS.load(Relaxed) > 0
    });
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `cannot find function `requeue_unconfirmed``, `cannot find value `RESCAN_CALLS``.

- [ ] **Step 3: Implement.** Replace `probe_and_resume_stranded` with:

```rust
/// Test-only count of stranded-segment scans (every `requeue_unconfirmed` call
/// that reached the directory walk).
#[cfg(test)]
static RESCAN_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// What one stranded-segment rescan did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RescanOutcome {
    /// Paths found on disk and pushed onto the backlog.
    pub(crate) requeued: u64,
    /// Paths found on disk that were already queued (or in flight / held).
    pub(crate) already_queued: u64,
    /// The scan failed outright (already logged by the caller).
    pub(crate) scan_error: Option<String>,
    /// A shard could not be listed; the list is partial.
    pub(crate) incomplete: bool,
}

/// The single rescan path, shared by the automatic resume and the operator's
/// retry-stranded verb (spec §5.4).
///
/// 1. Moves whatever is already in the drain channel into `pending`, so the
///    up-to-256 sealed paths still in flight there are visible to the dedup.
/// 2. Scans for sealed-but-unconfirmed segments, skipping ledger-owned paths.
/// 3. Dedups against `pending ∪ backlog ∪ {in_flight}` (`in_flight` is the
///    segment the calling arm is retrying, blocked on, or holding).
/// 4. Pushes the rest onto `backlog` and counts them in
///    `weir_drain_segments_resumed_total`.
///
/// Any residual double-queue is absorbed by the ledger's admission check and
/// the NotFound no-op in `process_segment_ctl`.
#[allow(clippy::too_many_arguments)]
fn requeue_unconfirmed(
    config: &DrainConfig,
    metrics: &Metrics,
    rx: Option<&crossbeam_channel::Receiver<PathBuf>>,
    pending: &mut VecDeque<PathBuf>,
    backlog: &mut VecDeque<PathBuf>,
    in_flight: Option<&Path>,
    exclude: &dyn Fn(&Path) -> bool,
    hb: &DrainHeartbeat,
) -> RescanOutcome {
    if let Some(rx) = rx {
        while let Ok(p) = rx.try_recv() {
            pending.push_back(p);
        }
    }
    #[cfg(test)]
    RESCAN_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scan = {
        let _busy = BusyGuard::new(hb, BusyKind::DirScan);
        // `None` shard_count: skip the beyond-configured-count advisory, a
        // startup-replay concern.
        crate::wab::scan_unconfirmed_sealed(&config.wab_dir, None, metrics, exclude, config.orphan_cleanup)
    };
    match scan {
        Ok(crate::wab::SealedScan { segments, incomplete }) => {
            let (fresh, already_queued) = {
                let mut queued: std::collections::HashSet<&Path> = pending
                    .iter()
                    .chain(backlog.iter())
                    .map(PathBuf::as_path)
                    .collect();
                if let Some(seg) = in_flight {
                    queued.insert(seg);
                }
                let (dupes, fresh): (Vec<PathBuf>, Vec<PathBuf>) =
                    segments.into_iter().partition(|s| queued.contains(s.as_path()));
                (fresh, dupes.len() as u64)
            };
            let requeued = fresh.len() as u64;
            backlog.extend(fresh);
            if requeued > 0 {
                metrics.drain_segments_resumed.inc_by(requeued);
            }
            RescanOutcome { requeued, already_queued, scan_error: None, incomplete }
        }
        Err(e) => RescanOutcome { scan_error: Some(e.to_string()), ..RescanOutcome::default() },
    }
}

/// Probes sink health, publishes it (gauge and board), and — on a down→up
/// recovery edge, or while the drain is idle — runs [`requeue_unconfirmed`].
/// "Idle" now means BOTH queues are empty: during a replay the backlog is
/// non-empty, and rescanning on every poll would only find the backlog again
/// (spec §5.4). Returns the new health-ok state for `prev_health_ok`, held at
/// the previous value when the scan failed or was incomplete (H1), so the
/// next poll scans again.
///
/// (The doc comment's remaining paragraphs on `in_flight`, `exclude`,
/// `Degraded` and the edge-vs-idle cost control move here unchanged from the
/// old `probe_and_resume_stranded`.)
#[allow(clippy::too_many_arguments)]
fn probe_and_resume_stranded_with<S: Sink>(
    rt: &tokio::runtime::Runtime,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    rx: Option<&crossbeam_channel::Receiver<PathBuf>>,
    pending: &mut VecDeque<PathBuf>,
    backlog: &mut VecDeque<PathBuf>,
    prev_health_ok: bool,
    in_flight: Option<&Path>,
    exclude: &dyn Fn(&Path) -> bool,
    ctx: &DrainCtx<'_>,
) -> bool {
    let health = probe_health(rt, sink, config.health_probe_timeout, ctx.hb);
    let now_ok = !matches!(health, SinkHealth::Down(_));
    ctx.publish.sink_health(&health);
    set_sink_health(metrics, health);
    if now_ok && (!prev_health_ok || (pending.is_empty() && backlog.is_empty())) {
        let out = requeue_unconfirmed(config, metrics, rx, pending, backlog, in_flight, exclude, ctx.hb);
        if out.requeued > 0 {
            info!(resumed = out.requeued, "drain: sink recovered; re-queued stranded segment(s) for delivery");
        }
        if let Some(e) = &out.scan_error {
            warn!(error = %e, "drain: sink recovered but the stranded-segment rescan failed; will retry on the next recovery edge");
            return prev_health_ok;
        }
        if out.incomplete {
            return prev_health_ok;
        }
    }
    now_ok
}

/// Pre-Foundations signature for the existing tests: no channel, and the
/// rescan's finds land in `pending` as they used to.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn probe_and_resume_stranded<S: Sink>(
    rt: &tokio::runtime::Runtime,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    pending: &mut VecDeque<PathBuf>,
    prev_health_ok: bool,
    in_flight: Option<&Path>,
    exclude: &dyn Fn(&Path) -> bool,
    hb: &DrainHeartbeat,
) -> bool {
    let control = DrainControl::detached();
    let publish = Publisher::none();
    let ctx = DrainCtx { control: &control, publish: &publish, hb };
    let mut backlog = VecDeque::new();
    let ok = probe_and_resume_stranded_with(
        rt, sink, config, metrics, None, pending, &mut backlog, prev_health_ok, in_flight, exclude, &ctx,
    );
    pending.extend(backlog);
    ok
}
```

The three call sites in `drain_thread_with` switch to `probe_and_resume_stranded_with(&rt, &*sink, &config, &metrics, Some(&drain_rx), &mut pending, &mut backlog, prev_health_ok, <in_flight>, &|p| ledger.excludes(p, now), &ctx)`, with `<in_flight>` = `None` (Draining head), `Some(&segment)` (RetryingTransient M2, Blocked).

**Ordering note.** Rescan finds now go to `backlog`, so after a recovery edge they alternate with live segments instead of queueing behind `pending`. The existing tests do not assert cross-source order. If one does fail on order, stop and raise it: the `mod tests` block is additions-only, so the fix belongs in the code, not the test.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass, including every existing `*rescan*`, `*stranded*`, `*orphan*` and `same_path_in_channel_and_rescan_is_delivered_once` test through the wrapper.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): one rescan path that drains the channel first and fills the backlog"`

---

### Task 2.7: retry-stranded — ticket service, coalescing, one backoff cut per episode

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `DrainState::RetryingTransient` gains `backoff_cut: bool`; add `RequeueService`, `VERB_RESCANS`; service calls at the Draining head, in the RetryingTransient wait loop and in the Blocked wait loop; every `DrainState::RetryingTransient { … }` construction (`transition_from_draining` 1862-1888, the RetryingTransient `on_transient` closure 1385, the Blocked→RetryingTransient unblock 1509) and the two destructuring patterns (1178 spent-budget arm, 1262 retry arm) gain the field (`..` in the spent-budget pattern)
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainControl::{requeue_requested, observe_requeue, absorbing}`, `control::REQUEUE_MIN_INTERVAL`, `Publisher::requeue`, `publish::sanitize_admin_text`, PR 1 `RequeueResult` and `crate::status::unix_ms_now`, Task 2.6 `requeue_unconfirmed`.
- Produces:
  ```rust
  struct RequeueService { seen: u64, last_scan_at: Option<Instant>, last: Option<RequeueResult> }
  impl RequeueService {
      fn new(control: &DrainControl) -> Self;
      fn service(&mut self, ctx: &DrainCtx<'_>, scan: impl FnOnce() -> RescanOutcome, cut: impl FnOnce() -> bool);
  }
  #[cfg(test)] static VERB_RESCANS: AtomicU64;
  // DrainState::RetryingTransient { segment, retries_left, next_delay, processed, cause, backoff_cut: bool }
  ```

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// Spec §16 drain test 11: retry-stranded while the probe says Down. The
/// automatic resume never fires (no healthy probe), the verb bypasses that
/// gate, drains the channel first and dedups against it, and resolves the ticket.
#[test]
fn retry_stranded_rescans_while_the_probe_says_down_and_dedups_the_channel() {
    let dir = tmp_dir("fnd_retry_stranded");
    let c = seg(&dir, 1, &[b"c"]);
    let sink = Arc::new(MockSink {
        yield_for: Duration::from_millis(400),
        health_script: Mutex::new(
            std::iter::repeat_n(SinkHealth::Down("probe refused".into()), 10_000).collect(),
        ),
        ..MockSink::with_responses([])
    });
    let board = StatusBoard::for_test();
    let control = DrainControl::detached();
    let cfg = DrainConfig {
        health_poll_interval: Duration::from_millis(50),
        ..fast_config(dir.clone())
    };
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(c.clone()).unwrap();
    wait_for(5, "c's commit is in flight", || sink.call_count() == 1);

    // While c commits: `a` is stranded on disk AND sitting in the channel; `b` only on disk.
    let a = seg(&dir, 2, &[b"a"]);
    let b = seg(&dir, 3, &[b"b"]);
    tx.send(a.clone()).unwrap();
    let ticket = control.request_requeue();
    wait_for(5, "the ticket is observed", || control.requeue_observed() >= ticket);

    let r = board.drain.last_requeue.load_full().expect("a published result");
    assert_eq!(r.ticket, ticket);
    assert!(!r.coalesced && r.scan_error.is_none());
    assert_eq!(
        (r.requeued, r.already_queued),
        (1, 1),
        "b requeued; a already queued (moved out of the channel before the scan)"
    );
    assert_eq!(board.drain.observed_requeue.load(Relaxed), ticket);
    wait_for(10, "a and b delivered", || !a.exists() && !b.exists());
    assert_eq!(sink.call_count(), 3, "each segment reaches the sink exactly once");
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 12: 1,000 back-to-back requests produce at most one scan
/// per 5 s window and at most one backoff cut per retry episode, and a cut is
/// never below base_retry_delay (W2 preserved).
#[test]
fn a_flood_of_retry_stranded_requests_scans_once_and_cuts_one_backoff() {
    crate::drain::VERB_RESCANS.store(0, Relaxed);
    let dir = tmp_dir("fnd_requeue_flood");
    let s = seg(&dir, 1, &[b"x"]);
    let hint = Duration::from_secs(2);
    let sink = Arc::new(MockSink::with_responses([
        Err(MockError::TransientWithRetryAfter(hint)),
        Err(MockError::TransientWithRetryAfter(hint)),
    ]));
    let cfg = DrainConfig {
        base_retry_delay: Duration::from_millis(100),
        ..quiet(dir.clone())
    };
    let control = DrainControl::detached();
    let board = StatusBoard::for_test();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    wait_for(5, "the first attempt failed", || sink.call_count() == 1);

    let mut tickets = std::collections::BTreeSet::new();
    for i in 0..1000 {
        tickets.insert(control.request_requeue());
        if i % 100 == 99 {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    wait_for(10, "delivered on the third attempt", || !s.exists());

    let ts = sink.call_timestamps();
    assert_eq!(ts.len(), 3);
    let (first_gap, second_gap) = (ts[1] - ts[0], ts[2] - ts[1]);
    assert!(
        first_gap >= Duration::from_millis(100) && first_gap < Duration::from_millis(1000),
        "the first backoff is cut once, never below base_retry_delay: {first_gap:?}"
    );
    assert!(
        second_gap >= Duration::from_millis(1900),
        "at most one cut per retry episode: {second_gap:?}"
    );
    assert_eq!(crate::drain::VERB_RESCANS.load(Relaxed), 1, "one scan per 5 s window");
    assert!(tickets.len() < 1000, "requests fold into the outstanding ticket");
    let last = board.drain.last_requeue.load_full().unwrap();
    assert!(last.coalesced, "later requests are answered from the window's scan");
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `cannot find value `VERB_RESCANS``. (With only the static added, both tests time out on `requeue_observed() >= ticket`.)

- [ ] **Step 3: Implement.** Add near `requeue_unconfirmed`:

```rust
/// Test-only count of verb-driven scans (those [`RequeueService::service`] ran).
#[cfg(test)]
static VERB_RESCANS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Resolves operator retry-stranded tickets (spec §5.4 "Retry stranded").
///
/// At most one scan per [`control::REQUEUE_MIN_INTERVAL`]; a ticket inside the
/// window is answered from the previous scan with `coalesced: true`. A running
/// retry backoff is cut at most once per retry episode (`backoff_cut` travels
/// with the episode's `RetryingTransient` state), never below
/// `base_retry_delay`. In absorb mode the ticket is resolved without scanning,
/// so an admin client never waits on a drain that is shutting down.
struct RequeueService {
    seen: u64,
    last_scan_at: Option<Instant>,
    last: Option<RequeueResult>,
}

impl RequeueService {
    fn new(control: &DrainControl) -> Self {
        Self { seen: control.requeue_observed(), last_scan_at: None, last: None }
    }

    /// `scan` runs `requeue_unconfirmed` (bypassing the health-edge gate);
    /// `cut` shortens a running backoff and says whether it did.
    fn service(
        &mut self,
        ctx: &DrainCtx<'_>,
        scan: impl FnOnce() -> RescanOutcome,
        cut: impl FnOnce() -> bool,
    ) {
        let ticket = ctx.control.requeue_requested();
        if ticket <= self.seen {
            return;
        }
        let now = Instant::now();
        let result = match (&self.last, self.last_scan_at) {
            (Some(last), Some(at)) if now.duration_since(at) < control::REQUEUE_MIN_INTERVAL => {
                RequeueResult {
                    ticket,
                    backoff_cut: false,
                    coalesced: true,
                    at_ms: unix_ms_now(),
                    ..last.clone()
                }
            }
            _ => {
                let outcome = if ctx.control.absorbing() {
                    RescanOutcome {
                        scan_error: Some("skipped: the daemon is shutting down".into()),
                        ..RescanOutcome::default()
                    }
                } else {
                    #[cfg(test)]
                    VERB_RESCANS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    scan()
                };
                self.last_scan_at = Some(now);
                let backoff_cut = cut();
                info!(
                    ticket,
                    requeued = outcome.requeued,
                    already_queued = outcome.already_queued,
                    backoff_cut,
                    scan_failed = outcome.scan_error.is_some(),
                    "drain: retry-stranded: rescanned the WAB for stranded segments"
                );
                RequeueResult {
                    ticket,
                    requeued: outcome.requeued,
                    already_queued: outcome.already_queued,
                    scan_error: outcome.scan_error.as_deref().map(publish::sanitize_admin_text),
                    backoff_cut,
                    coalesced: false,
                    at_ms: unix_ms_now(),
                }
            }
        };
        self.seen = ticket;
        ctx.publish.requeue(&result);
        ctx.control.observe_requeue(ticket);
        self.last = Some(result);
    }
}
```

(add `use crate::status::{RequeueResult, unix_ms_now};` to the imports). In `drain_thread_with`, add `let mut requeue = RequeueService::new(control);` with the locals.

**Draining head**, right after the health-poll block:

```rust
                {
                    let now = Instant::now();
                    requeue.service(
                        &ctx,
                        || requeue_unconfirmed(&config, &metrics, Some(&drain_rx), &mut pending, &mut backlog, None, &|p| ledger.excludes(p, now), &heartbeat),
                        || false,
                    );
                }
```

**RetryingTransient** pattern binds `backoff_cut` (and `cause` instead of `cause: _`); before its wait loop:

```rust
                let wait_start = Instant::now();
                let mut wait_end = wait_start + next_delay;
                let mut backoff_cut = backoff_cut;
```

and first thing inside the loop:

```rust
                    {
                        let now = Instant::now();
                        let base = config.base_retry_delay;
                        requeue.service(
                            &ctx,
                            || requeue_unconfirmed(&config, &metrics, Some(&drain_rx), &mut pending, &mut backlog, Some(&segment), &|p| ledger.excludes(p, now), &heartbeat),
                            || {
                                if backoff_cut {
                                    return false;
                                }
                                // Never below base_retry_delay from the wait's start (W2).
                                let cut_to = wait_end.min(Instant::now().max(wait_start + base));
                                if cut_to < wait_end {
                                    wait_end = cut_to;
                                    backoff_cut = true;
                                    true
                                } else {
                                    false
                                }
                            },
                        );
                    }
```

The retry arm's `on_transient` closure builds `DrainState::RetryingTransient { …, backoff_cut }` (the same episode keeps its cut). `transition_from_draining` and the Blocked unblock build it with `backoff_cut: false` (a new episode). The spent-budget pattern (`retries_left: 0`) adds `..`.

**Blocked** wait loop, first thing inside: the same `requeue.service(&ctx, || requeue_unconfirmed(… Some(&segment) …), || false);`.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): retry-stranded tickets with coalescing and one backoff cut per episode"`

---
### Task 2.8: pause at the Draining head (and a pause that outlives a respawn)

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `drain_thread_with` locals; the Draining arm after the requeue service and before segment selection
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainControl::{pause_requested, stop_requested}`, `Publisher::paused`, `crate::status::unix_ms_now` (imported in Task 2.7), `Metrics::drain_paused` (PR 1), `wait_slice`.
- Produces: no new items. Behaviour: a pause in force at the Draining head starts no segment, keeps running the health poll and the verbs, and moves channel arrivals into `pending` on 50 ms slices. A stop while paused exits at once (owner decision: pause honoured at SIGTERM). The pause lives in `DrainControl`, so it can be set before the drain spawns and it survives a respawn.

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// Spec §16 drain test 8: pause in Draining. 300 timed sends into a small
/// channel never time out (the paused drain keeps emptying it), nothing is
/// committed, and resume delivers everything in arrival order.
#[test]
fn a_paused_drain_keeps_taking_from_the_channel_and_resumes_in_order() {
    let dir = tmp_dir("fnd_pause_head");
    let control = DrainControl::detached();
    control.request_pause(); // before spawn: the pause lives in DrainControl
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(16);
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), None, Vec::new()));
    let mut expected = Vec::new();
    for n in 1..=300u64 {
        let rec = format!("r{n:03}");
        let p = seg(&dir, n, &[rec.as_bytes()]);
        expected.push(rec.into_bytes());
        tx.send_timeout(p, Duration::from_millis(500))
            .expect("a paused drain must keep taking from the channel");
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(sink.call_count(), 0, "nothing is committed while paused");
    assert_eq!(metrics.drain_paused.get(), 1.0);
    control.request_resume();
    wait_for(30, "all 300 delivered", || sink.call_count() == 300);
    assert_eq!(committed_bytes(&sink), expected, "resume delivers in arrival order");
    assert_eq!(metrics.drain_paused.get(), 0.0);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 18: a respawn while paused stays paused.
#[test]
fn a_respawned_drain_is_still_paused() {
    let dir = tmp_dir("fnd_pause_respawn");
    let first = seg(&dir, 1, &[b"boom"]);
    let sink = Arc::new(MockSink::with_panic(1, []));
    let control = DrainControl::detached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, fast_config(dir.clone()), &metrics, rt_with(control.clone(), None, Vec::new()));
    tx.send(first.clone()).unwrap();
    wait_for(5, "the first commit panicked", || drain_panics(&metrics) == 1);
    control.request_pause(); // inside the supervisor's 100 ms respawn backoff
    // Respawn, then several 50 ms health polls whose rescan re-queues `first`.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(sink.call_count(), 1, "the respawned drain honours the pause it inherited");
    assert!(first.exists());
    assert_eq!(metrics.drain_paused.get(), 1.0);
    control.request_resume();
    wait_for(5, "delivered after resume", || !first.exists());
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// A stop while paused exits at once: the pause is honoured (owner decision).
#[test]
fn a_stop_while_paused_exits_without_delivering() {
    let dir = tmp_dir("fnd_pause_stop");
    let s = seg(&dir, 1, &[b"x"]);
    let control = DrainControl::attached();
    control.request_pause();
    let sink = Arc::new(MockSink::with_responses([]));
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(control.clone(), None, Vec::new()));
    std::thread::sleep(Duration::from_millis(100));
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_millis(500));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(exit.pending_left, 1, "the queued segment is left for replay");
    assert_eq!(sink.call_count(), 0);
    assert!(s.exists());
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::a_paused`
  Expected: FAIL — `a_paused_drain_keeps_taking_from_the_channel_and_resumes_in_order` panics at `assert_eq!(sink.call_count(), 0, …)` (left: 300), because nothing reads the pause yet.

- [ ] **Step 3: Implement.** In `drain_thread_with` add the local `let mut paused_since: Option<u64> = None; // unix ms; shared by every hold`. In the Draining arm, after the requeue service block, the rest of the arm becomes the `if`/`else` below, which is the arm's tail expression. The whole `let next_segment = loop { … };` (as Task 2.5 left it) and the `match next_segment { … }` move, unchanged, inside the `else` branch, with the `match` as that branch's tail expression:

```rust
                // Pause (spec §5.4): after the health poll and the verbs, so
                // probes and gauges keep running; before taking a segment.
                if control.pause_requested() {
                    if control.stop_requested() {
                        info!(
                            "drain: stop while paused; queued segments stay sealed and replay on \
                             the next start (pause honoured)"
                        );
                        break 'outer DrainExitReason::Stopped;
                    }
                    let since = *paused_since.get_or_insert_with(|| {
                        info!("drain: paused before the next segment; ingest keeps buffering on disk");
                        unix_ms_now()
                    });
                    publish.paused(since, false);
                    metrics.drain_paused.set(1.0);
                    match wait_slice(&drain_rx, &ctx, control::SLICE, &mut pending, backlog.len(), &mut channel_gone) {
                        Wait::Segment(p) => pending.push_back(p),
                        Wait::TimedOut | Wait::Stop => {}
                        // A Detached shutdown while paused honours the pause too.
                        Wait::Disconnected => break 'outer DrainExitReason::Stopped,
                    }
                    DrainState::Draining
                } else {
                    if paused_since.take().is_some() {
                        publish.paused(0, false);
                        metrics.drain_paused.set(0.0);
                        info!("drain: resumed");
                    }
                    let next_segment = loop {
                        if let Some(p) = next_queued(&mut pending, &mut backlog, &drain_rx, &mut backlog_turn) {
                            match ledger.admit(&p, Instant::now()) {
                                Admit::Process { skip } => break Some((p, skip)),
                                Admit::Drop(owner) => {
                                    debug!(path = %p.display(), owner, "drain: not admitting queued segment");
                                    continue;
                                }
                            }
                        }
                        // Idle: nothing queued on either side and the channel is
                        // empty. Wait no longer than the time left until the next
                        // health poll, then re-enter Draining so the poll runs.
                        let until_next_poll = config
                            .health_poll_interval
                            .saturating_sub(last_health_poll.elapsed())
                            .max(Duration::from_millis(1));
                        match wait_slice(&drain_rx, &ctx, until_next_poll, &mut pending, backlog.len(), &mut channel_gone) {
                            Wait::Segment(p) => match ledger.admit(&p, Instant::now()) {
                                Admit::Process { skip } => break Some((p, skip)),
                                Admit::Drop(owner) => {
                                    debug!(path = %p.display(), owner, "drain: not admitting segment from the channel");
                                    break None;
                                }
                            },
                            Wait::TimedOut | Wait::Stop => break None,
                            Wait::Disconnected => break 'outer DrainExitReason::Stopped,
                        }
                    };
                    match next_segment {
                        // Idle timeout with nothing queued (or a dropped channel
                        // copy): re-enter Draining so the head runs on schedule.
                        None => DrainState::Draining,
                        Some((segment, skip)) => {
                            let result = attempt_segment(
                                &rt,
                                &segment,
                                &*sink,
                                &config,
                                &metrics,
                                &mut dead_letter,
                                ledger,
                                skip,
                                &ctx,
                            );
                            transition_from_draining(
                                segment,
                                result,
                                &config,
                                &metrics,
                                &mut block_episode,
                                ledger,
                                &heartbeat,
                            )
                        }
                    }
                }
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): pause before the next segment, honoured through a respawn and a stop"`

---

### Task 2.9: pause mid-segment — `ProcessResult::Paused` and `DrainState::Held`

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — module doc's state diagram (1-22); `DrainState` (196-221) gains `Held` and `RetryingTransient.waited`; `ProcessResult` (224-266) gains `Paused`; add `HeldResume`, `batch_gate`; `process_segment_ctl` (both commit sites); `next_state_after_process` (1558) gains `resume: HeldResume`; `transition_from_draining`; the RetryingTransient arm; a new `Held` arm; `ThreadExit` construction
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

Verified against 2491883: both resume paths are pinned. `Held → Draining` re-enters through `ledger.admit` (ledger.rs:275), which discards a cursor whose (dev, ino) no longer matches. `Held → RetryingTransient{waited: true}` calls `attempt_segment`, whose first step is `ledger.checked_skip(segment, skip)` (drain/mod.rs:1688), the safety floor's pin check for a `processed` carried outside the ledger.

**Interfaces:**
- Consumes: `DrainControl::pause_requested`, `ledger.record_progress`, `Publisher::paused`, Task 2.8's `paused_since`.
- Produces:
  ```rust
  enum HeldResume { Fresh, Retry { retries_left: u32, next_delay: Duration, cause: TransientCause, backoff_cut: bool } }
  impl HeldResume { fn gauge_state(&self) -> DrainStateValue; }
  // DrainState::Held { segment: PathBuf, processed: u64, resume: HeldResume }
  // DrainState::RetryingTransient { …, backoff_cut: bool, waited: bool }
  // ProcessResult::Paused { processed: u64 }
  fn batch_gate(ctx: &DrainCtx<'_>, durable_through: u64) -> Option<ProcessResult>;
  fn next_state_after_process(segment: PathBuf, result: ProcessResult, resume: HeldResume, config: &DrainConfig,
      metrics: &Metrics, block_episode: &mut Option<Instant>, ledger: &mut SegmentLedger, hb: &DrainHeartbeat,
      on_transient: impl FnOnce(PathBuf, Option<Duration>, u64, TransientCause) -> DrainState) -> DrainState;
  ```

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// Spec §16 drain test 9: pause mid-segment at a commit-batch boundary. The
/// commit in flight finishes, the next is not started, `held` is published,
/// and resume re-sends nothing: every record reaches the sink exactly once.
#[test]
fn a_pause_mid_segment_holds_at_a_batch_boundary_and_resumes_without_duplicates() {
    let dir = tmp_dir("fnd_pause_held");
    let (sealed, records) = make_numbered_segment(&dir, 10);
    let sink = Arc::new(MockSink {
        yield_for: Duration::from_millis(100),
        max_batch: 2, // 5 commits
        ..MockSink::with_responses([])
    });
    let board = StatusBoard::for_test();
    let control = DrainControl::detached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "two commits", || sink.call_count() >= 2);

    control.request_pause();
    wait_for(5, "held is published", || board.drain.held.load(Relaxed));
    let held_at = sink.call_count();
    assert!(held_at < 5, "held before the segment finished");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(sink.call_count(), held_at, "no commit starts while held");
    assert!(sealed.exists(), "a held segment is neither confirmed nor deleted");
    assert!(board.drain.paused_since_ms.load(Relaxed) > 0);
    assert_eq!(metrics.drain_paused.get(), 1.0);

    control.request_resume();
    wait_for(5, "delivered", || !sealed.exists());
    assert_eq!(committed_bytes(&sink), records, "every record exactly once, in order");
    assert_eq!(sink.call_count(), 5);
    assert!(!board.drain.held.load(Relaxed));
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn process_segment_ctl_stops_before_a_batch_when_paused() {
    let dir = tmp_dir("fnd_batch_gate");
    let (sealed, _) = make_numbered_segment(&dir, 6);
    let sink = MockSink::with_batch_size(2, []);
    let control = DrainControl::detached();
    control.request_pause();
    let (publish, hb) = (Publisher::none(), DrainHeartbeat::new());
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let mut dl = DeadLetterWriter::open(&dir).unwrap();
    let r = block_on(crate::drain::process_segment_ctl(
        &sealed, &sink, &fast_config(dir.clone()), &noop_metrics(), &mut dl, 2, false, &ctx,
    ));
    assert!(matches!(r, ProcessResult::Paused { processed: 2 }), "the cursor is the skip, untouched");
    assert_eq!(sink.call_count(), 0, "no commit after a pause is seen");
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `no variant named `Paused` found for enum `ProcessResult``.

- [ ] **Step 3: Implement.**

```rust
// ProcessResult, new variant:
    /// A pause was seen before the next commit batch (spec §5.4). No commit is
    /// in flight and `processed` is exactly the committed prefix, so resuming
    /// past it re-sends nothing.
    Paused { processed: u64 },

// DrainState, new variant, and RetryingTransient's two new fields:
    /// Paused mid-segment between commit batches (spec §5.4, §8). Gauged as
    /// the state it came from; `weir_drain_paused` says it is paused.
    Held {
        segment: PathBuf,
        processed: u64,
        resume: HeldResume,
    },
//  RetryingTransient { …, backoff_cut: bool,
//      /// The backoff was already served (a hold resumed): retry at once.
//      waited: bool }

/// How a held segment continues once the pause lifts.
enum HeldResume {
    /// It was a first attempt: back through admission and the Draining arm.
    Fresh,
    /// It was a retry: continue the same episode without serving the backoff again.
    Retry {
        retries_left: u32,
        next_delay: Duration,
        cause: TransientCause,
        backoff_cut: bool,
    },
}

impl HeldResume {
    /// The one-hot `weir_drain_state` a hold keeps showing (spec §5.4 "Gauges").
    fn gauge_state(&self) -> DrainStateValue {
        match self {
            Self::Fresh => DrainStateValue::draining,
            Self::Retry { .. } => DrainStateValue::retrying_transient,
        }
    }
}

/// Checked before every `commit_batch_ctl` (spec §5.4). Nothing is in flight
/// here and `durable_through` is the committed prefix, so stopping here costs
/// no duplicate on resume.
fn batch_gate(ctx: &DrainCtx<'_>, durable_through: u64) -> Option<ProcessResult> {
    if ctx.control.pause_requested() {
        return Some(ProcessResult::Paused { processed: durable_through });
    }
    None
}
```

In `process_segment_ctl`, immediately before each of the two `let result = commit_batch_ctl(` statements (the in-loop full batch and the tail batch), insert:

```rust
            if let Some(stop) = batch_gate(ctx, durable_through) {
                return stop;
            }
```

`next_state_after_process` gains the `resume: HeldResume` parameter after `result`, and an arm:

```rust
        ProcessResult::Paused { processed } => {
            ledger.record_progress(&segment, processed);
            info!(
                path = %segment.display(),
                processed,
                "drain: paused between commit batches; holding the segment (nothing in flight, \
                 nothing re-sent on resume)"
            );
            DrainState::Held { segment, processed, resume }
        }
```

`transition_from_draining` passes `HeldResume::Fresh`. Every `DrainState::RetryingTransient { … }` construction gets `waited: false`. The RetryingTransient arm binds `cause`, `backoff_cut` and `waited`. `let mut backoff_cut = backoff_cut;` moves ABOVE a new `if !waited { … }` block, because the episode's cut flag is passed on after it. The block holds `wait_start`, `wait_end`, the wait loop and the M2 probe. After it the arm calls `attempt_segment` as before and passes `HeldResume::Retry { retries_left, next_delay, cause, backoff_cut }` to `next_state_after_process`.

Add the local `let mut abandoned: Option<(PathBuf, u64)> = None;`, and the new arm (after BlockedDeadLetterFull):

```rust
            // ── Held: paused mid-segment (spec §5.4, §8) ──────────────────────
            DrainState::Held { segment, processed, resume } => {
                set_drain_state(&metrics, resume.gauge_state());
                let since = *paused_since.get_or_insert_with(unix_ms_now);
                publish.paused(since, true);
                metrics.drain_paused.set(1.0);
                if control.stop_requested() {
                    info!(
                        path = %segment.display(),
                        processed,
                        "drain: stop while held; the segment stays sealed and unconfirmed and replays \
                         on the next start (pause honoured)"
                    );
                    abandoned = Some((segment, processed));
                    break 'outer DrainExitReason::Stopped;
                }
                {
                    let now = Instant::now();
                    requeue.service(
                        &ctx,
                        || requeue_unconfirmed(&config, &metrics, Some(&drain_rx), &mut pending, &mut backlog, Some(&segment), &|p| ledger.excludes(p, now), &heartbeat),
                        || false,
                    );
                }
                // Health while held (spec §5.4): the wall-clock poll keeps
                // running, with the held segment excluded from the rescan.
                if !control.absorbing() && last_health_poll.elapsed() >= config.health_poll_interval {
                    let now = Instant::now();
                    prev_health_ok = probe_and_resume_stranded_with(
                        &rt, &*sink, &config, &metrics, Some(&drain_rx), &mut pending, &mut backlog,
                        prev_health_ok, Some(&segment), &|p| ledger.excludes(p, now), &ctx,
                    );
                    last_health_poll = Instant::now();
                }
                if control.pause_requested() {
                    match wait_slice(&drain_rx, &ctx, control::SLICE, &mut pending, backlog.len(), &mut channel_gone) {
                        Wait::Segment(p) => pending.push_back(p),
                        Wait::TimedOut | Wait::Stop => {}
                        Wait::Disconnected => {
                            abandoned = Some((segment, processed));
                            break 'outer DrainExitReason::Stopped;
                        }
                    }
                    DrainState::Held { segment, processed, resume }
                } else {
                    paused_since = None;
                    publish.paused(0, false);
                    metrics.drain_paused.set(0.0);
                    info!(
                        path = %segment.display(),
                        processed,
                        "drain: resumed; continuing the held segment past the records already delivered"
                    );
                    match resume {
                        // Back through admission, which re-reads the cursor
                        // recorded on entry, pinned to the file's (dev, ino).
                        HeldResume::Fresh => {
                            pending.push_front(segment);
                            backlog_turn = false; // the held segment goes first
                            DrainState::Draining
                        }
                        HeldResume::Retry { retries_left, next_delay, cause, backoff_cut } => {
                            DrainState::RetryingTransient {
                                segment,
                                retries_left,
                                next_delay,
                                processed,
                                cause,
                                backoff_cut,
                                waited: true,
                            }
                        }
                    }
                }
            }
```

`ThreadExit` now carries `abandoned`, and one place counts it:

```rust
    if abandoned.is_some() {
        metrics.drain_segments_abandoned.inc();
    }
    ThreadExit { reason, abandoned, pending_left: pending.len() + drain_rx.len(), backlog_left: backlog.len() }
```

Module doc (1-22): add to the state diagram

```text
//! Draining / RetryingTransient ──(pause seen before a commit batch)──▶ Held
//!   Held ──(resume)──▶ Draining (same segment first, past its cursor)
//!                   or RetryingTransient (same episode, backoff already served)
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): pause between commit batches holds the segment with zero added duplicates"`

---

### Task 2.10: pause during a retry backoff, and health while holding

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — the RetryingTransient wait loop
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

Verified against 2491883: the local-read escalation (3 strand cycles AND `local_read_escalation_min_age`, 10 min in production) lives in `DrainConfig` and the ledger (`local_read_escalation_due`, drain/mod.rs:1838); `DrainState::RetryingTransient` carries no new field.

**Interfaces:**
- Consumes: `DrainControl::{pause_requested, stop_requested, absorbing}`, `probe_and_resume_stranded_with`, `paused_since`, `abandoned`.
- Produces: no new items. Behaviour: the backoff wait ends only when `remaining.is_zero() && !pause_requested()`. While paused it holds on 50 ms slices (the `wait_slice` floor), runs the wall-clock health poll on the `health_poll_interval` cadence with the segment excluded, and keeps `retries_left`, `processed` and the episode's `backoff_cut`. A stop or a Detached disconnect while paused exits at once.

- [ ] **Step 1: Write the failing test.** Add `use crate::drain::control::SLICE;` to the imports, then append to `foundations.rs`:

```rust
/// Spec §16 drain test 10: pause during RetryingTransient after the backoff
/// has elapsed. No spin (≤ 25 wait iterations in 1 s: the 50 ms floor), no
/// retry fires, and sink health refreshes on its cadence.
///
/// The failing commit lasts 200 ms, so the pause is requested while it is
/// still in flight: the backoff loop's first slice sees it, and the 10 ms
/// backoff then elapses while paused. (Requesting the pause after the commit
/// returned would race the 10 ms backoff against `wait_for`'s 5 ms poll.)
#[test]
fn a_pause_during_a_retry_backoff_holds_without_spinning_and_keeps_probing() {
    let dir = tmp_dir("fnd_pause_retry");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink {
        health_script: Mutex::new(std::iter::repeat_n(SinkHealth::Healthy, 1000).collect()),
        yield_for: Duration::from_millis(200),
        ..MockSink::with_responses([Err(MockError::Transient)])
    });
    let cfg = DrainConfig {
        base_retry_delay: Duration::from_millis(10),
        health_poll_interval: Duration::from_millis(200),
        ..fast_config(dir.clone())
    };
    let control = DrainControl::detached();
    let hb = DrainHeartbeat::new();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = spawn_with(
        rx,
        Arc::clone(&sink),
        cfg,
        noop_metrics(),
        hb.clone(),
        rt_with(control.clone(), None, Vec::new()),
    );
    tx.send(s.clone()).unwrap();
    wait_for(5, "the first attempt is in flight", || sink.call_count() == 1);
    control.request_pause(); // seen on the backoff loop's first slice; the 10 ms backoff then elapses while paused
    std::thread::sleep(Duration::from_millis(300));

    let (beats0, polls0) = (hb.get(), health_polls_left(&sink));
    std::thread::sleep(Duration::from_secs(1));
    let beats = hb.get() - beats0;
    let probes = polls0 - health_polls_left(&sink);
    assert!(beats <= 25, "{beats} wait iterations in 1 s: the paused hold must sleep {SLICE:?} slices");
    assert!(probes >= 3, "sink health must refresh on its cadence while held ({probes} probes)");
    assert_eq!(sink.call_count(), 1, "no retry fires while paused");

    control.request_resume();
    wait_for(5, "the retry delivers after resume", || !s.exists());
    assert_eq!(sink.call_count(), 2);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn a_paused_wait_slice_never_returns_early_on_a_zero_duration() {
    let (_tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let control = DrainControl::detached();
    control.request_pause();
    let (hb, publish) = (DrainHeartbeat::new(), Publisher::none());
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let t = Instant::now();
    assert_eq!(
        wait_slice(&rx, &ctx, Duration::ZERO, &mut VecDeque::new(), 0, &mut false),
        Wait::TimedOut
    );
    assert!(t.elapsed() >= SLICE - Duration::from_millis(5), "the pause floor: {:?}", t.elapsed());
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::a_pause_during_a_retry_backoff`
  Expected: FAIL at `beats <= 25` (about 40 per second): without the backoff-loop hold, the retry reaches the batch gate and holds in `Held`, which also bumps at the `'outer` head. (`a_paused_wait_slice_never_returns_early_on_a_zero_duration` already passes: the floor landed with `wait_slice` in Task 2.4, and this test pins it.)

- [ ] **Step 3: Implement.** In the RetryingTransient wait loop (inside `if !waited`), replace Task 2.4's two lines `let remaining = wait_end.saturating_duration_since(Instant::now()); if remaining.is_zero() { break; }` with the following block (it goes after the requeue service block and ends with the new, pause-aware break):

```rust
                        let paused = control.pause_requested();
                        if paused {
                            if control.stop_requested() {
                                abandoned = Some((segment.clone(), processed));
                                break 'outer DrainExitReason::Stopped; // pause honoured at stop
                            }
                            let since = *paused_since.get_or_insert_with(|| {
                                info!(path = %segment.display(), "drain: paused during a retry backoff; holding the segment");
                                unix_ms_now()
                            });
                            publish.paused(since, true);
                            metrics.drain_paused.set(1.0);
                            // Health while held: the M2 probe below is not reached
                            // until the hold ends, so the poll runs here on cadence.
                            if !control.absorbing() && last_health_poll.elapsed() >= config.health_poll_interval {
                                let now = Instant::now();
                                prev_health_ok = probe_and_resume_stranded_with(
                                    &rt, &*sink, &config, &metrics, Some(&drain_rx), &mut pending, &mut backlog,
                                    prev_health_ok, Some(&segment), &|p| ledger.excludes(p, now), &ctx,
                                );
                                last_health_poll = Instant::now();
                            }
                        } else if paused_since.take().is_some() {
                            publish.paused(0, false);
                            metrics.drain_paused.set(0.0);
                            info!(path = %segment.display(), "drain: resumed during a retry backoff");
                        }
                        let remaining = wait_end.saturating_duration_since(Instant::now());
                        if remaining.is_zero() && !paused {
                            break;
                        }
```

and in the same loop's `Wait::Disconnected` arm, first:

```rust
                            if control.pause_requested() {
                                abandoned = Some((segment.clone(), processed));
                                break 'outer DrainExitReason::Stopped; // Detached shutdown honours the pause
                            }
```

(the clamped-remainder code follows unchanged).

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): a pause holds a retry backoff without spinning and keeps probing"`

---

### Task 2.11: the stop deadline — `Abandoned` before a batch, inside a commit, in a backoff, while blocked

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `ProcessResult` (+`Abandoned`), `BatchResult` (+`Abandoned`), `DrainState` (+`Abandoned`), `batch_gate`, `apply_batch` (2294), `commit_batch_ctl` (the timed block, 2760-2763), `next_state_after_process`, the RetryingTransient wait loop, the BlockedDeadLetterFull arm (1396-1533), a new `Abandoned` arm
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainControl::{deadline_reached, deadline_passed, deadline, stop_requested}`.
- Produces:
  ```rust
  // ProcessResult::Abandoned { processed: u64 }
  // BatchResult::Abandoned
  // DrainState::Abandoned { segment: PathBuf, processed: u64 }
  ```
  Behaviour (spec §5.4 "Stop and deadline"): the Draining head exits at the deadline (Task 2.4); `process_segment_ctl` checks the deadline before each batch; `commit_batch_ctl` races the commit against `deadline_reached()` with the same drop semantics as `commit_timeout` but no retry bump and no backoff; RetryingTransient exits at once if its wait would end after the deadline; Blocked exits on any stop. `Abandoned` never reaches `confirm_and_delete`.

- [ ] **Step 1: Write the failing test.** Add `use crate::wab::segment::segment_identity;` to the imports, then append to `foundations.rs`:

```rust
/// Spec §16 drain test 2: budget 0 against a 2 s (yielding) sink commit. Exit
/// within 100 ms, nothing confirmed, one segment abandoned.
#[test]
fn a_zero_budget_abandons_a_commit_in_flight_within_100ms() {
    let dir = tmp_dir("fnd_budget_zero");
    let sealed = seg(&dir, 1, &[b"slow"]);
    let sink = Arc::new(MockSink::with_yielding_commit(Duration::from_secs(2), 1000));
    let control = DrainControl::attached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), None, Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "the commit is in flight", || sink.call_count() == 1);
    let t = Instant::now();
    control.request_stop(Duration::ZERO);
    let exit = join_within(h, Duration::from_secs(1));
    assert!(t.elapsed() < Duration::from_millis(100), "exit took {:?}", t.elapsed());
    assert_eq!(exit.reason, DrainExitReason::DeadlineAbandoned);
    assert_eq!(exit.abandoned, Some((segment_identity(&sealed), 0)));
    assert!(sealed.exists() && !get_confirmed_path(&sealed).exists(), "never confirmed");
    assert_eq!(metrics.drain_segments_abandoned.get(), 1);
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 4: stop in RetryingTransient with wait_end > deadline.
#[test]
fn a_backoff_that_would_outlast_the_deadline_exits_at_once() {
    let dir = tmp_dir("fnd_backoff_deadline");
    let sealed = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_responses([Err(MockError::TransientWithRetryAfter(
        Duration::from_secs(10),
    ))]));
    let control = DrainControl::attached();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(control.clone(), None, Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "the first attempt failed", || sink.call_count() == 1);
    std::thread::sleep(Duration::from_millis(50)); // inside the 10 s backoff
    let t = Instant::now();
    control.request_stop(Duration::from_secs(1));
    let exit = join_within(h, Duration::from_secs(1));
    assert!(t.elapsed() < Duration::from_millis(200), "{:?}", t.elapsed());
    assert_eq!(exit.reason, DrainExitReason::DeadlineAbandoned);
    assert_eq!(exit.abandoned, Some((segment_identity(&sealed), 0)));
    assert_eq!(sink.call_count(), 1, "the retry never fired");
    assert!(sealed.exists());
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 5: stop while BlockedDeadLetterFull.
#[test]
fn a_stop_while_blocked_on_dead_letter_headroom_exits() {
    let dir = tmp_dir("fnd_blocked_stop");
    let sealed = seg(&dir, 1, &[b"record"]);
    let dl_dir = dir.join("dead_letter");
    crate::testutil::mkdir_p(&dl_dir);
    std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();
    let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
    let cfg = DrainConfig {
        dead_letter_check_interval: Duration::from_millis(1500),
        health_poll_interval: Duration::from_secs(60),
        ..tight_dl_config(dir.clone(), 100)
    };
    let control = DrainControl::attached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &metrics, rt_with(control.clone(), None, Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "blocked on dead-letter headroom", || drain_is_blocked(&metrics));
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_millis(500));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(exit.abandoned, Some((segment_identity(&sealed), 0)));
    assert!(sealed.exists());
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 6: force_stop during a yielding commit is Abandoned,
/// with no `retried` bump (it is not a sink failure).
#[test]
fn force_stop_abandons_a_yielding_commit_without_counting_a_retry() {
    let dir = tmp_dir("fnd_force_commit");
    let sealed = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_yielding_commit(Duration::from_secs(2), 1000));
    let control = DrainControl::attached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), None, Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "the commit is in flight", || sink.call_count() == 1);
    control.force_stop();
    let exit = join_within(h, Duration::from_millis(200));
    assert_eq!(exit.reason, DrainExitReason::DeadlineAbandoned);
    assert_eq!(records_retried(&metrics), 0);
    assert!(sealed.exists());
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 7: force_stop then main's T2 request_stop(10 s): the
/// forced deadline stands (fetch_min), so the drain exits within the grace.
#[test]
fn force_stop_then_request_stop_still_exits_within_the_grace() {
    let dir = tmp_dir("fnd_force_then_stop");
    let sealed = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_yielding_commit(Duration::from_secs(5), 1000));
    let control = DrainControl::attached();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(control.clone(), None, Vec::new()));
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "the commit is in flight", || sink.call_count() == 1);
    control.force_stop();
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_secs(2)); // DRAIN_EXIT_GRACE
    assert_eq!(exit.reason, DrainExitReason::DeadlineAbandoned);
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn process_segment_ctl_abandons_before_a_batch_once_the_deadline_has_passed() {
    let dir = tmp_dir("fnd_gate_deadline");
    let (sealed, _) = make_numbered_segment(&dir, 4);
    let sink = MockSink::with_batch_size(2, []);
    let control = DrainControl::attached();
    control.force_stop();
    control.request_pause(); // the deadline outranks a pause
    let (publish, hb) = (Publisher::none(), DrainHeartbeat::new());
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let mut dl = DeadLetterWriter::open(&dir).unwrap();
    let r = block_on(crate::drain::process_segment_ctl(
        &sealed, &sink, &fast_config(dir.clone()), &noop_metrics(), &mut dl, 0, false, &ctx,
    ));
    assert!(matches!(r, ProcessResult::Abandoned { processed: 0 }));
    assert_eq!(sink.call_count(), 0);
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: compile failure, `no variant named `Abandoned` found for enum `ProcessResult``.

- [ ] **Step 3: Implement.**

```rust
// ProcessResult:
    /// The stop deadline passed before the next batch, or during a commit
    /// (spec §5.4). `processed` is the committed prefix. Maps to drain exit;
    /// never confirmed.
    Abandoned { processed: u64 },
// BatchResult:
    /// The stop deadline passed while the sink commit was in flight; the
    /// commit future was dropped (the `commit_timeout` drop semantics, with no
    /// retry bump and no backoff).
    Abandoned,
// DrainState:
    /// An attempt was abandoned at the deadline; the loop exits on its next pass.
    Abandoned { segment: PathBuf, processed: u64 },
```

`batch_gate` checks the deadline first:

```rust
fn batch_gate(ctx: &DrainCtx<'_>, durable_through: u64) -> Option<ProcessResult> {
    if ctx.control.stop_requested() && ctx.control.deadline_passed() {
        return Some(ProcessResult::Abandoned { processed: durable_through });
    }
    if ctx.control.pause_requested() {
        return Some(ProcessResult::Paused { processed: durable_through });
    }
    None
}
```

`apply_batch` gains `BatchResult::Abandoned => Err(ProcessResult::Abandoned { processed }),`.

`commit_batch_ctl`'s timed block (`let timed = { let _busy = BusyGuard::new(hb, BusyKind::SinkCommit); tokio::time::timeout(…).await };`) becomes:

```rust
    let timed = {
        let _busy = BusyGuard::new(hb, BusyKind::SinkCommit);
        tokio::select! {
            biased;
            r = tokio::time::timeout(config.commit_timeout, sink.commit(batch)) => Some(r),
            () = ctx.control.deadline_reached() => None,
        }
    };
    let Some(timed) = timed else {
        warn!(
            records = payloads.len(),
            "drain: the stop deadline passed during a sink commit; abandoning it (the segment stays \
             sealed and unconfirmed and replays on the next start; the sink may already have \
             applied this batch, which its idempotency absorbs)"
        );
        return BatchResult::Abandoned;
    };
```

(the existing `match timed { Ok(inner) => inner, Err(_elapsed) => … }` follows unchanged). Without a deadline `deadline_reached()` never resolves, so `biased` plus the 50 ms poll costs one timer wake per 50 ms of commit and nothing else.

`next_state_after_process` gains:

```rust
        ProcessResult::Abandoned { processed } => {
            ledger.record_progress(&segment, processed);
            DrainState::Abandoned { segment, processed }
        }
```

and `drain_thread_with` a new arm:

```rust
            DrainState::Abandoned { segment, processed } => {
                warn!(
                    path = %segment.display(),
                    processed,
                    "drain: stop deadline reached; abandoning the segment in flight (left sealed and \
                     unconfirmed; its first {processed} records re-deliver on replay)"
                );
                abandoned = Some((segment, processed));
                break 'outer DrainExitReason::DeadlineAbandoned;
            }
```

RetryingTransient wait loop, first statement of each iteration:

```rust
                        // Spec §5.4: a backoff that would end after the
                        // deadline is abandoned now, not served.
                        if control.stop_requested()
                            && (control.deadline_passed() || control.deadline().is_some_and(|d| wait_end > d))
                        {
                            info!(
                                path = %segment.display(),
                                processed,
                                "drain: stop requested and this retry would fire after the deadline; \
                                 leaving the segment unconfirmed"
                            );
                            abandoned = Some((segment.clone(), processed));
                            break 'outer DrainExitReason::DeadlineAbandoned;
                        }
```

and its `Wait::Stop => break,` (from Task 2.4) becomes:

```rust
                            Wait::Stop => {
                                abandoned = Some((segment.clone(), processed));
                                break 'outer DrainExitReason::DeadlineAbandoned;
                            }
```

BlockedDeadLetterFull: rename the local `channel_closed` to `stop_seen`; first statement inside its wait loop `if control.stop_requested() { stop_seen = true; break; }` (the requeue service follows); the `Wait::Stop | Wait::Disconnected` arm sets `stop_seen = true`. The no-headroom shutdown branch (`} else if channel_closed {`, the Blocked arm's shutdown-while-blocked exit) becomes:

```rust
                } else if stop_seen {
                    // No headroom and a stop (or a Detached disconnect): leave the
                    // segment unconfirmed; replay delivers it on the next start.
                    info!(path = %segment.display(), "drain: stop while blocked; segment not confirmed");
                    abandoned = Some((segment, processed));
                    break 'outer if control.deadline_passed() {
                        DrainExitReason::DeadlineAbandoned
                    } else {
                        DrainExitReason::Stopped
                    };
                } else {
```

The Blocked arm keeps its own `dead_letter.rescan()` on stop (bounded local I/O), so an operator freeing headroom still unblocks it.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass. `hung_sink_commit_times_out_then_segment_retried_and_confirmed` and `commit_timeout_on_a_later_sub_batch_resumes_past_the_committed_prefix` still pass: with no stop, the `select!` reduces to the old timeout.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): a stop deadline that abandons in-flight work without confirming it"`

---
### Task 2.12: `CommitGuard` around every commit, and the sink cell (success, failure, health)

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — `DrainCtx::commit_guard`; imports; `commit_batch_ctl` (the timed block and every outcome branch: the `Err(_elapsed)` timeout return 2766-2785, the `accounted != payloads.len()` contract-violation return 2799-2812, `Ok`, transient `Err`, permanent `Err`)
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: Task 2.2 `CommitGuard`, `Publisher::{sink_success, sink_failure}`; PR 1 `SinkFailureKind`, `SinkCell::{last_error, consecutive_failures, last_success_ms}`, `DrainCell::{commit_in_flight_since_ms, last_progress_ms}`.
- Produces:
  ```rust
  impl<'a> DrainCtx<'a> { pub(crate) fn commit_guard(&self) -> CommitGuard<'a>; }
  ```

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// A sink that records whether the commit marker is set WHILE it runs.
struct BoardSpySink {
    board: Arc<StatusBoard>,
    seen: Mutex<Vec<u64>>,
}

impl crate::sink::Sink for BoardSpySink {
    type Error = MockError;
    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, MockError> {
        self.seen
            .lock()
            .unwrap()
            .push(self.board.drain.commit_in_flight_since_ms.load(Relaxed));
        Ok(CommitResult::new(batch.into_records(), Vec::new()))
    }
    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

/// A transient error with a caller-chosen message.
#[derive(Debug)]
struct LoudError(String);
impl std::fmt::Display for LoudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for LoudError {}
impl crate::sink::SinkError for LoudError {
    fn is_transient(&self) -> bool {
        true
    }
}

/// Fails its first commit with `message`, then commits everything.
struct LoudSink {
    message: String,
    calls: std::sync::atomic::AtomicU64,
}
impl crate::sink::Sink for LoudSink {
    type Error = LoudError;
    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, LoudError> {
        if self.calls.fetch_add(1, Relaxed) == 0 {
            return Err(LoudError(self.message.clone()));
        }
        Ok(CommitResult::new(batch.into_records(), Vec::new()))
    }
    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

#[test]
fn the_commit_guard_spans_exactly_the_sink_call() {
    let dir = tmp_dir("fnd_guard_span");
    let board = StatusBoard::for_test();
    let sink = BoardSpySink { board: Arc::clone(&board), seen: Mutex::new(Vec::new()) };
    let control = DrainControl::detached();
    let publish = Publisher::new(Some(Arc::clone(&board)));
    let hb = DrainHeartbeat::new();
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let mut dl = DeadLetterWriter::open(&dir).unwrap();
    let payloads = vec![Payload::from(b"r".as_ref())];
    let ids = test_record_ids(&payloads);
    let r = block_on(crate::drain::commit_batch_ctl(
        &payloads, &ids, 0, &sink, &fast_config(dir.clone()), &noop_metrics(), &mut dl, &ctx,
    ));
    assert!(matches!(r, BatchResult::Ok));
    assert!(sink.seen.lock().unwrap()[0] > 0, "marked while the sink runs");
    assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0, "cleared after");
    assert!(board.sink.last_success_ms.load(Relaxed) > 0);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 16, first half: a commit that exceeds commit_timeout.
#[test]
fn a_commit_timeout_leaves_no_commit_marked_in_flight() {
    let dir = tmp_dir("fnd_guard_timeout");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_hang(1, []));
    let board = StatusBoard::for_test();
    let cfg = DrainConfig {
        commit_timeout: Duration::from_millis(100),
        base_retry_delay: Duration::from_secs(2),
        ..quiet(dir.clone())
    };
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &metrics, rt_with(DrainControl::detached(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    wait_for(5, "the commit timed out", || records_retried(&metrics) == 1);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0, "the timeout's early return clears it");
    assert!(board.drain.last_progress_ms.load(Relaxed) > 0);
    assert_eq!(board.sink.consecutive_failures.load(Relaxed), 1);
    assert!(board.sink.last_error.load_full().is_some_and(|e| e.contains("commit_timeout")));
    drop(tx); // Detached: the retry fires after the clamped remainder and succeeds
    join_within(h, Duration::from_secs(5));
    assert_eq!(board.sink.consecutive_failures.load(Relaxed), 0, "a success resets the streak");
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 16, second half: a sink that breaks the CommitResult contract.
#[test]
fn a_contract_violation_leaves_no_commit_marked_in_flight() {
    let dir = tmp_dir("fnd_guard_contract");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_responses([MockSink::ok(vec![])])); // accounts for 0 of 1
    let board = StatusBoard::for_test();
    let cfg = DrainConfig { base_retry_delay: Duration::from_secs(2), ..quiet(dir.clone()) };
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(DrainControl::detached(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    // Published after the guard dropped (the violation is detected after the
    // timed block), so this wait orders the two without a sleep.
    wait_for(5, "the violation is published", || board.sink.last_error.load_full().is_some());
    assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0);
    assert!(board.sink.last_error.load_full().is_some_and(|e| e.contains("accounts for 0 of 1")));
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 17: `last_error` is redacted and capped.
#[test]
fn the_sinks_last_error_is_redacted_and_capped() {
    let dir = tmp_dir("fnd_last_error");
    let s = seg(&dir, 1, &[b"x"]);
    let message = format!(
        "error sending request for url (https://user:hunter2@sink.example/ingest?token=s3cr3t): \
         \u{1b}[31m{}",
        "z".repeat(5000)
    );
    let sink = Arc::new(LoudSink { message, calls: std::sync::atomic::AtomicU64::new(0) });
    let board = StatusBoard::for_test();
    let cfg = DrainConfig { base_retry_delay: Duration::from_secs(2), ..quiet(dir.clone()) };
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(DrainControl::detached(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    wait_for(5, "the failure is published", || board.sink.last_error.load_full().is_some());
    let e = board.sink.last_error.load_full().unwrap();
    assert!(!e.contains("hunter2") && !e.contains("s3cr3t"), "{e}");
    assert!(!e.chars().any(char::is_control));
    assert!(e.len() <= crate::drain::publish::ADMIN_TEXT_MAX);
    assert_eq!(board.sink.consecutive_failures.load(Relaxed), 1);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: FAIL — `the_commit_guard_spans_exactly_the_sink_call` on `seen[0] > 0`; `a_commit_timeout_leaves_no_commit_marked_in_flight` on `last_progress_ms > 0`; the contract and last_error tests time out waiting for `last_error`.

- [ ] **Step 3: Implement.** Imports: `use publish::{CommitGuard, Publisher};` and `use crate::status::SinkFailureKind;`. Add to `impl DrainCtx`:

```rust
impl<'a> DrainCtx<'a> {
    /// See [`CommitGuard`]: taken immediately before the commit `select!`.
    pub(crate) fn commit_guard(&self) -> CommitGuard<'a> {
        CommitGuard::new(self.publish, self.hb)
    }
}
```

In `commit_batch_ctl`, the timed block takes the guard inside the busy guard:

```rust
    let timed = {
        let _busy = BusyGuard::new(hb, BusyKind::SinkCommit);
        // Every way out of this block — completion, commit_timeout, the
        // deadline, an unwinding panic — clears the commit marker (spec §5.4).
        let _commit = ctx.commit_guard();
        tokio::select! {
            biased;
            r = tokio::time::timeout(config.commit_timeout, sink.commit(batch)) => Some(r),
            () = ctx.control.deadline_reached() => None,
        }
    };
```

Outcome publication (one call per branch, before that branch's existing `return` / value):

```rust
        // Err(_elapsed) (commit_timeout):
        ctx.publish.sink_failure(
            SinkFailureKind::Timeout,
            &format!("sink commit exceeded commit_timeout ({}s)", config.commit_timeout.as_secs()),
        );
        // accounted != payloads.len():
        ctx.publish.sink_failure(
            SinkFailureKind::ContractViolation,
            &format!("sink CommitResult accounts for {accounted} of {} records", payloads.len()),
        );
        // Ok, right after the accounting check passes (the sink did its part,
        // whatever the dead-letter write that may follow does):
        ctx.publish.sink_success();
        // Err(e) if e.is_transient():
        ctx.publish.sink_failure(SinkFailureKind::Transient, &e.to_string());
        // Err(e) (permanent):
        ctx.publish.sink_failure(SinkFailureKind::Permanent, &e.to_string());
```

In `probe_and_resume_stranded_with`, `ctx.publish.sink_health(&health)` is already called before `set_sink_health` (Task 2.6); nothing more to add for health.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): truthful commit-in-flight marker and sink outcome publication"`

---

### Task 2.13: absorb mode skips probes, rescans and gauge walks

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — the Draining health-poll block (1050-1087), the RetryingTransient M2 probe (1349-1360), the Blocked probe (1466-1477)
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `DrainControl::absorbing`.
- Produces: no new items. Behaviour (spec §5.4 "Absorb mode"): from T0 (`begin_shutdown`) the drain skips `probe_health`, the stranded rescan and the Draining arm's dead-letter and quarantine gauge walks: none is useful during shutdown, and a probe can block for up to `health_probe_timeout`, past the exit grace. The Blocked arm keeps its local `dead_letter.rescan()`. `wait_slice` already moves the channel into `pending` while absorbing (Task 2.4); the holds already skip their polls (Tasks 2.9, 2.10); `RequeueService` already resolves tickets without scanning (Task 2.7).

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// A sink whose health probe takes 5 s (yielding).
struct SlowHealthSink;
impl crate::sink::Sink for SlowHealthSink {
    type Error = MockError;
    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, MockError> {
        Ok(CommitResult::new(batch.into_records(), Vec::new()))
    }
    async fn health(&self) -> SinkHealth {
        tokio::time::sleep(Duration::from_secs(5)).await;
        SinkHealth::Healthy
    }
}

/// Spec §16 drain test 19: absorb mode skips probe_health, so a sink whose
/// health() takes 5 s cannot delay the exit.
#[test]
fn absorb_mode_skips_the_health_probe_so_a_slow_probe_cannot_delay_exit() {
    let dir = tmp_dir("fnd_absorb_probe");
    let control = DrainControl::attached();
    let cfg = DrainConfig {
        health_poll_interval: Duration::from_millis(300),
        health_probe_timeout: Duration::from_secs(10),
        ..fast_config(dir.clone())
    };
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let h = start(rx, &Arc::new(SlowHealthSink), cfg, &noop_metrics(), rt_with(control.clone(), None, Vec::new()));
    std::thread::sleep(Duration::from_millis(50));
    control.begin_shutdown(); // T0, before the first poll falls due at 300 ms
    std::thread::sleep(Duration::from_millis(350)); // the poll is now due
    drop(tx); // T1: flushers joined
    let t = Instant::now();
    control.request_stop(Duration::from_secs(10)); // T2
    let exit = join_within(h, Duration::from_secs(2));
    assert!(t.elapsed() < Duration::from_millis(300), "a probe ran during shutdown: {:?}", t.elapsed());
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    std::fs::remove_dir_all(dir).ok();
}

/// Pins the absorb half of wait_slice (landed in Task 2.4).
#[test]
fn an_absorbing_wait_slice_empties_the_channel_into_pending() {
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(16);
    for n in 0..10 {
        tx.send(PathBuf::from(format!("/wab/shard_00/seg_{n:08}.wab.sealed"))).unwrap();
    }
    let control = DrainControl::attached();
    control.begin_shutdown();
    let (hb, publish) = (DrainHeartbeat::new(), Publisher::none());
    let ctx = DrainCtx { control: &control, publish: &publish, hb: &hb };
    let mut pending = VecDeque::new();
    let w = wait_slice(&rx, &ctx, Duration::from_millis(10), &mut pending, 0, &mut false);
    assert_eq!(w, Wait::TimedOut);
    assert_eq!(pending.len(), 10);
    assert!(rx.is_empty());
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::absorb_mode`
  Expected: FAIL — `the drain did not exit within 2s`: the 300 ms health poll starts a 5 s probe.

- [ ] **Step 3: Implement.**
  - Draining health-poll block: `if last_health_poll.elapsed() >= config.health_poll_interval {` becomes `if !control.absorbing() && last_health_poll.elapsed() >= config.health_poll_interval {`. The dead-letter and quarantine gauge walks are inside that block, so they are skipped too.
  - RetryingTransient M2 probe (inside `if !waited`, after the wait loop): wrap in `if !control.absorbing() { … }`.
  - BlockedDeadLetterFull probe after `dead_letter.rescan()`: wrap the `probe_and_resume_stranded_with` call in `if !control.absorbing() { … }`; `dead_letter.rescan()` and the `dead_letter_bytes_on_disk` gauge set stay unconditional.
  - Comment at the poll block: `// Absorb mode (T0, spec §5.4): skip the probe, the rescan and the gauge walks; a probe can block for health_probe_timeout and overrun the exit grace.`

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): absorb mode skips probes and rescans during shutdown"`

---

### Task 2.14: the supervisor — `DrainExit`, no respawn after stop, the panic message, `shutdown_timeout(100 ms)`

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/wab/mod.rs:123` — `fn panic_message_str` becomes `pub(crate) fn panic_message_str` (the only line PR 2 changes in that file; PR 3 edits other parts of it in parallel)
- Modify: `crates/weir-server/src/drain/mod.rs` — `run_drain_supervised` (367-463); add `BoundedRuntime` above `drain_thread_with`; the runtime construction at the top of `drain_thread_with`
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `crate::wab::panic_message_str(&Box<dyn Any + Send>) -> &str`, `publish::sanitize_admin_text`, `Publisher::{exit, in_flight, state}`, PR 1 `DrainExitInfo` and `crate::status::unix_ms_now`.
- Produces: `DrainExit.last_panic` populated; `DrainExitReason::GaveUp` on a spent panic budget; no respawn once `stop_requested()`; `publish.exit(DrainExitInfo { .. })` on every exit; and
  ```rust
  /// Owns the drain's runtime; its Drop is `shutdown_timeout(100 ms)`, on a normal return and on unwind.
  struct BoundedRuntime(Option<tokio::runtime::Runtime>);
  impl std::ops::Deref for BoundedRuntime { type Target = tokio::runtime::Runtime; }
  impl Drop for BoundedRuntime;
  ```

- [ ] **Step 1: Write the failing test.** Append to `foundations.rs`:

```rust
/// Commits after `delay`, then panics.
struct PanicAfterSink {
    delay: Duration,
    calls: std::sync::atomic::AtomicU64,
}
impl crate::sink::Sink for PanicAfterSink {
    type Error = MockError;
    async fn commit(&self, _batch: SinkBatch) -> Result<CommitResult, MockError> {
        self.calls.fetch_add(1, Relaxed);
        tokio::time::sleep(self.delay).await;
        panic!("sink exploded after the stop request");
    }
    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

/// Leaves a 3 s blocking task behind on every commit, like a DNS lookup on
/// tokio's blocking pool that outlives the request that started it.
struct LingeringTaskSink;
impl crate::sink::Sink for LingeringTaskSink {
    type Error = MockError;
    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, MockError> {
        drop(tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(3))));
        Ok(CommitResult::new(batch.into_records(), Vec::new()))
    }
    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

/// Starts a 3 s blocking task, lets it start running, then panics. tokio
/// cancels a blocking task that has not started by shutdown, so the sleep
/// before the panic is what makes the leftover work real; it also leaves the
/// test time to request the stop while the commit is in flight.
struct LingeringTaskThenPanicSink {
    calls: std::sync::atomic::AtomicU64,
}
impl crate::sink::Sink for LingeringTaskThenPanicSink {
    type Error = MockError;
    async fn commit(&self, _batch: SinkBatch) -> Result<CommitResult, MockError> {
        drop(tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(3))));
        self.calls.fetch_add(1, Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        panic!("sink exploded with a blocking task still running");
    }
    async fn health(&self) -> SinkHealth {
        SinkHealth::Healthy
    }
}

#[test]
fn a_drain_that_panics_after_stop_is_not_respawned() {
    let dir = tmp_dir("fnd_panic_after_stop");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(PanicAfterSink {
        delay: Duration::from_millis(300),
        calls: std::sync::atomic::AtomicU64::new(0),
    });
    let control = DrainControl::attached();
    let metrics = noop_metrics();
    let board = StatusBoard::for_test();
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()));
    wait_for(5, "the commit is in flight", || sink.calls.load(Relaxed) == 1);
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_secs(2));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert!(exit.last_panic.as_deref().is_some_and(|m| m.contains("sink exploded")));
    assert_eq!(drain_panics(&metrics), 1);
    assert_eq!(sink.calls.load(Relaxed), 1, "no respawn after stop");
    assert!(s.exists());
    let info = board.drain.exit.load_full().expect("exit published");
    assert_eq!(info.reason, DrainExitReason::Stopped);
    assert!(info.last_panic.is_some());
    assert_drain_reports_stopped(&metrics, "panic after stop");
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn a_drain_that_spends_its_panic_budget_exits_gave_up() {
    let dir = tmp_dir("fnd_gave_up");
    let s = seg(&dir, 1, &[b"poison"]);
    let sink = Arc::new(MockSink::panicking_on(b"poison"));
    let cfg = DrainConfig { max_respawns: 2, ..fast_config(dir.clone()) };
    let board = StatusBoard::for_test();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(rx, &sink, cfg, &metrics, rt_with(DrainControl::detached(), Some(Arc::clone(&board)), Vec::new()));
    let exit = join_within(h, Duration::from_secs(5));
    assert_eq!(exit.reason, DrainExitReason::GaveUp);
    assert!(exit.last_panic.as_deref().is_some_and(|m| m.contains("poison payload")));
    assert_eq!(board.drain.exit.load_full().unwrap().reason, DrainExitReason::GaveUp);
    assert_drain_reports_stopped(&metrics, "gave up");
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Engineering question (e): a sink's leftover spawn_blocking work must not
/// hold the drain's exit. Runtime::drop waits for the blocking pool without
/// limit; shutdown_timeout(100 ms) does not.
#[test]
fn a_sinks_leftover_blocking_task_cannot_hold_the_drain_exit() {
    let dir = tmp_dir("fnd_shutdown_timeout");
    let s = seg(&dir, 1, &[b"x"]);
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(rx, &Arc::new(LingeringTaskSink), quiet(dir.clone()), &noop_metrics(), rt_with(DrainControl::detached(), None, Vec::new()));
    wait_for(5, "delivered", || !s.exists());
    drop(tx);
    join_within(h, Duration::from_secs(1));
    std::fs::remove_dir_all(dir).ok();
}

/// The same bound on the unwinding path: a sink that panics with blocking work
/// still running must not hold the drain's exit (or, before a stop, its
/// respawn) for as long as that work runs. Runtime::drop during the unwind
/// would wait ~3 s here.
#[test]
fn a_sinks_leftover_blocking_task_cannot_hold_a_panicking_drain() {
    let dir = tmp_dir("fnd_shutdown_timeout_panic");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(LingeringTaskThenPanicSink { calls: std::sync::atomic::AtomicU64::new(0) });
    let control = DrainControl::attached();
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(control.clone(), None, Vec::new()));
    wait_for(5, "the commit is in flight", || sink.calls.load(Relaxed) == 1);
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_secs(1));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert!(exit.last_panic.as_deref().is_some_and(|m| m.contains("blocking task still running")));
    assert!(s.exists(), "a panicked commit is never confirmed");
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: FAIL — `a_drain_that_panics_after_stop_is_not_respawned` asserts on `last_panic` (None); `a_sinks_leftover_blocking_task_cannot_hold_the_drain_exit` panics with `the drain did not exit within 1s` (the runtime drop waits ~3 s); `a_sinks_leftover_blocking_task_cannot_hold_a_panicking_drain` panics with `the drain did not exit within 1s` (the unwind drops the runtime, which waits ~3 s, and the supervisor then respawns).

- [ ] **Step 3: Implement.** Rename Task 2.3's `let _payload = match result {` to `let payload = match result {` in `run_drain_supervised`. `wab/mod.rs:123`: `pub(crate) fn panic_message_str(…)`. Above `drain_thread_with`:

```rust
/// The drain's runtime, shut down with `shutdown_timeout(100 ms)` on every way
/// out of `drain_thread_with`, a sink panic unwinding through it included.
///
/// Not a plain `Runtime`: dropping one waits, without limit, for its
/// spawn_blocking pool, and a sink can leave work there (a DNS lookup:
/// hyper-util's GaiResolver, tokio::net::lookup_host). On a normal return that
/// would turn a clean exit into PR 4's orphan path; on a panic it would hold
/// the respawn (or, after stop, the exit) for as long as that work runs. The
/// abandoned blocking thread holds no drain state and finishes on its own
/// (spec §5.4, engineering question (e)).
struct BoundedRuntime(Option<tokio::runtime::Runtime>);

impl std::ops::Deref for BoundedRuntime {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &tokio::runtime::Runtime {
        self.0.as_ref().expect("present until drop")
    }
}

impl Drop for BoundedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_timeout(Duration::from_millis(100));
        }
    }
}
```

At the top of `drain_thread_with`, the runtime construction becomes:

```rust
    let rt = BoundedRuntime(Some(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("drain tokio runtime"),
    ));
```

Every `&rt` argument (the `probe_and_resume_stranded_with` and `attempt_segment` calls) coerces to `&tokio::runtime::Runtime` through `Deref`, so no call site changes. Keep `let rt` the first statement of the function: locals drop in reverse order, so it drops last, after the dead-letter writer and the queues.

`run_drain_supervised`: add `let mut last_panic: Option<String> = None;` and `let replay_total = first_backlog.as_ref().map_or(0, |b| b.len() as u64);` before the loop. The `Ok(t)` break sets `last_panic: last_panic.clone()`. In the `Err(payload)` path, right after `metrics.drain_panics.inc();`:

```rust
        let message = publish::sanitize_admin_text(crate::wab::panic_message_str(&payload));
        error!(attempts, panic = %message, "drain thread panicked");
        last_panic = Some(message);
        // A panic took the replay list with it; the respawn's rescan recovers
        // it into the backlog, where backlog_len counts it.
        if replay_total > 0 {
            publish.replay(replay_total, 0);
            metrics.drain_replay_remaining_segments.set(0.0);
        }
        // Never respawn after stop (spec §5.4 "Supervisor"): main is waiting
        // on a bounded exit, and a fresh incarnation would restart delivery.
        if control.stop_requested() {
            warn!("drain: panicked after stop was requested; not respawning");
            break DrainExit {
                reason: if control.deadline_passed() {
                    DrainExitReason::DeadlineAbandoned
                } else {
                    DrainExitReason::Stopped
                },
                abandoned: None,
                pending_left: drain_rx.len(),
                backlog_left: 0,
                last_panic: last_panic.clone(),
            };
        }
```

(the existing ledger panic accounting, the give-up check and the backoff follow unchanged; the give-up `DrainExit` carries `last_panic: last_panic.clone()`). After the loop:

```rust
    set_drain_state(&metrics, DrainStateValue::stopped);
    publish.state(&DrainStateValue::stopped);
    publish.in_flight(None);
    publish.exit(DrainExitInfo {
        reason: exit.reason,
        abandoned: exit.abandoned.clone(),
        pending_left: exit.pending_left,
        backlog_left: exit.backlog_left,
        last_panic: exit.last_panic.clone(),
        at_ms: unix_ms_now(),
    });
    info!(reason = ?exit.reason, pending_left = exit.pending_left, backlog_left = exit.backlog_left, "drain supervisor exiting");
    exit
```

(import `crate::status::DrainExitInfo`). Remove the old bare `info!("drain supervisor exiting");`.

**Engineering question (e) audit (spec §20).** Run:

```bash
grep -rln "spawn_blocking\|lookup_host" ~/.cargo/registry/src/*/{hyper-util,reqwest,tokio-postgres,mysql_async}-*/src 2>/dev/null
```

Record the result in the doc comment on `BoundedRuntime`, naming each built-in sink whose client uses the blocking pool (expected: http, clickhouse and s3 through reqwest/hyper-util's resolver; postgres and mysql through `lookup_host`). None of them holds a lock, a file or drain state across that blocking call, so abandoning the thread after 100 ms only skips waiting for a lookup whose answer nobody reads.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass, including the existing `*panic*` and `*respawn*` tests and `assert_drain_reports_stopped` users.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): DrainExit from the supervisor; no respawn after stop; bounded runtime shutdown"`

---

### Task 2.15: publication — arm state, in-flight, queue lengths, replay progress, stopping, generation

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs` — add `enter_arm`, `publish_queues`, `ReplayProgress`; arm heads (Draining, RetryingTransient, BlockedDeadLetterFull, Held); the `'outer` loop head; segment selection; `wait_slice` (stopping); RetryingTransient wait (in-flight with `next_retry_at_ms`, re-published after a backoff cut)
- Test: `crates/weir-server/src/drain/tests/foundations.rs`

**Interfaces:**
- Consumes: `Publisher::{state, blocked_since, queue_lens, in_flight, replay, stopping}`, `DrainControl::deadline_unix_ms`, PR 1 `InFlight` and `crate::status::unix_ms_now`, `Metrics::{drain_pending_segments, drain_replay_remaining_segments}`, `crate::metrics::{DrainQueueLabel, DrainQueue}`.
- Produces:
  ```rust
  fn enter_arm(publish: &Publisher, metrics: &Metrics, state: DrainStateValue, blocked_since_ms: u64);
  fn publish_queues(publish: &Publisher, metrics: &Metrics, pending: usize, backlog: usize);
  struct ReplayProgress { total: u64, taken: u64 }   // took_one(), remaining()
  ```

- [ ] **Step 1: Write the failing test.** Add `use crate::metrics::{DrainQueue, DrainQueueLabel};` to the imports, then append to `foundations.rs`:

```rust
#[test]
fn the_drain_publishes_its_progress_to_the_board() {
    let dir = tmp_dir("fnd_publication");
    let backlog: Vec<PathBuf> = (1..=3).map(|n| seg(&dir, n, &[b"r"])).collect();
    let sink = Arc::new(MockSink::with_yielding_commit(Duration::from_millis(200), 1000));
    let board = StatusBoard::for_test();
    let control = DrainControl::attached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
    let h = start(rx, &sink, quiet(dir.clone()), &metrics, rt_with(control.clone(), Some(Arc::clone(&board)), backlog.clone()));
    wait_for(5, "the first replay segment is in flight", || sink.call_count() == 1);

    assert_eq!(board.drain.generation.load(Relaxed), 1);
    assert_eq!(metrics.drain_generation.get(), 1.0);
    assert_eq!(board.drain.replay_total.load(Relaxed), 3);
    assert_eq!(board.drain.replay_remaining.load(Relaxed), 2);
    assert_eq!(metrics.drain_replay_remaining_segments.get(), 2.0);
    assert_eq!(board.drain.backlog_len.load(Relaxed), 2);
    assert_eq!(
        metrics
            .drain_pending_segments
            .get_or_create(&DrainQueueLabel { queue: DrainQueue::backlog })
            .get(),
        2.0
    );
    let f = board.drain.in_flight.load_full().expect("a segment in flight");
    assert_eq!(f.segment, segment_identity(&backlog[0]));
    assert_eq!((f.attempt, f.processed, f.next_retry_at_ms), (1, 0, None));
    assert_eq!(board.drain.state(), Some(DrainStateValue::draining));

    control.request_stop(Duration::from_secs(30));
    wait_for(2, "stopping is published", || board.drain.stopping.load(Relaxed));
    assert_eq!(Some(board.drain.deadline_ms.load(Relaxed)), control.deadline_unix_ms());

    let exit = join_within(h, Duration::from_secs(5));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(board.drain.replay_remaining.load(Relaxed), 0);
    assert_eq!(board.drain.state(), Some(DrainStateValue::stopped));
    assert!(board.drain.in_flight.load_full().is_none());
    assert_eq!(board.drain.exit.load_full().unwrap().reason, DrainExitReason::Stopped);
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn a_retry_publishes_its_attempt_and_when_it_fires() {
    let dir = tmp_dir("fnd_publication_retry");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_responses([Err(MockError::TransientWithRetryAfter(
        Duration::from_secs(2),
    ))]));
    let board = StatusBoard::for_test();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, quiet(dir.clone()), &noop_metrics(), rt_with(DrainControl::detached(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    // The arm state is published a moment before the retry's in-flight record,
    // so wait on the record itself.
    wait_for(5, "the retry is published", || {
        board.drain.in_flight.load_full().is_some_and(|f| f.attempt == 2)
    });
    assert_eq!(board.drain.state(), Some(DrainStateValue::retrying_transient));
    let f = board.drain.in_flight.load_full().unwrap();
    assert_eq!(f.retries_left, MAX_RETRIES);
    assert!(f.next_retry_at_ms.is_some_and(|t| t > crate::status::unix_ms_now()));
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// A retry-stranded backoff cut (Task 2.7) moves the retry earlier; the board's
/// `next_retry_at_ms` must move with it. A 10 s hint cut to the 1 s
/// base_retry_delay floor leaves the test ~900 ms to see the new value before
/// the retry fires.
#[test]
fn a_backoff_cut_republishes_when_the_retry_fires() {
    let dir = tmp_dir("fnd_publication_cut");
    let s = seg(&dir, 1, &[b"x"]);
    let sink = Arc::new(MockSink::with_responses([Err(MockError::TransientWithRetryAfter(
        Duration::from_secs(10),
    ))]));
    let cfg = DrainConfig { base_retry_delay: Duration::from_secs(1), ..quiet(dir.clone()) };
    let control = DrainControl::detached();
    let board = StatusBoard::for_test();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(rx, &sink, cfg, &noop_metrics(), rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()));
    tx.send(s.clone()).unwrap();
    let retry_at = || board.drain.in_flight.load_full().and_then(|f| f.next_retry_at_ms);
    wait_for(5, "the 10 s retry is published", || {
        retry_at().is_some_and(|t| t > crate::status::unix_ms_now() + 5_000)
    });
    control.request_requeue();
    wait_for(5, "the cut retry time is published", || {
        retry_at().is_some_and(|t| t <= crate::status::unix_ms_now() + 1_000)
    });
    assert!(board.drain.last_requeue.load_full().is_some_and(|r| r.backoff_cut));
    wait_for(5, "delivered on the cut retry", || !s.exists());
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::tests::foundations::`
  Expected: FAIL — `the_drain_publishes_its_progress_to_the_board` at `assert_eq!(board.drain.replay_total.load(Relaxed), 3)` (left 0; the generation assertions already pass, Task 2.3); `a_retry_publishes_its_attempt_and_when_it_fires` times out on `the retry is published`; `a_backoff_cut_republishes_when_the_retry_fires` times out on `the 10 s retry is published`.

- [ ] **Step 3: Implement.**

```rust
/// Publishes the arm the drain is in: the one-hot gauge, the board state, and
/// the blocked-since stamp (0 outside BlockedDeadLetterFull). Called wherever
/// `set_drain_state` was called at an arm head (spec §5.4 "Publication").
fn enter_arm(publish: &Publisher, metrics: &Metrics, state: DrainStateValue, blocked_since_ms: u64) {
    publish.state(&state);
    publish.blocked_since(blocked_since_ms);
    set_drain_state(metrics, state);
}

/// Queue lengths to the board and to `weir_drain_pending_segments{queue}`.
fn publish_queues(publish: &Publisher, metrics: &Metrics, pending: usize, backlog: usize) {
    publish.queue_lens(pending, backlog);
    for (queue, n) in [(DrainQueue::live, pending), (DrainQueue::backlog, backlog)] {
        metrics
            .drain_pending_segments
            .get_or_create(&DrainQueueLabel { queue })
            .set(n as f64);
    }
}

/// Startup-replay progress. The replay list sits at the FRONT of the FIFO
/// backlog and rescans append behind it, so the first `total` backlog pops are
/// exactly the replay list.
struct ReplayProgress {
    total: u64,
    taken: u64,
}

impl ReplayProgress {
    fn took_one(&mut self) {
        if self.taken < self.total {
            self.taken += 1;
        }
    }
    fn remaining(&self) -> u64 {
        self.total - self.taken
    }
}
```

In `drain_thread_with`:
- after the backlog is built: `let mut replay = ReplayProgress { total: backlog.len() as u64, taken: 0 }; if replay.total > 0 { publish.replay(replay.total, replay.total); metrics.drain_replay_remaining_segments.set(replay.total as f64); }`
- `'outer` loop head, after `publish_ledger_counts`: `publish_queues(publish, &metrics, pending.len(), backlog.len());`
- arm heads: Draining `enter_arm(publish, &metrics, DrainStateValue::draining, 0)`; RetryingTransient `enter_arm(publish, &metrics, DrainStateValue::retrying_transient, 0)`; Held `enter_arm(publish, &metrics, resume.gauge_state(), 0)`; BlockedDeadLetterFull:
  ```rust
  let blocked_ms = unix_ms_now()
      .saturating_sub(u64::try_from(blocked_since.elapsed().as_millis()).unwrap_or(u64::MAX));
  enter_arm(publish, &metrics, DrainStateValue::blocked_dead_letter_full, blocked_ms);
  ```
  (`enter_blocked` keeps its own `set_drain_state` call; its test-facing signature is unchanged.)
- Draining head, before `stop_exit`: `if control.stop_requested() { publish.stopping(control.deadline_unix_ms()); }`; and the same line at the top of `wait_slice` (via `ctx`), so every hold publishes it too.
- segment selection: wrap the `next_queued` call:
  ```rust
  let backlog_before = backlog.len();
  let picked = next_queued(&mut pending, &mut backlog, &drain_rx, &mut backlog_turn);
  if backlog.len() < backlog_before {
      replay.took_one();
      publish.replay(replay.total, replay.remaining());
      metrics.drain_replay_remaining_segments.set(replay.remaining() as f64);
  }
  ```
  and when the idle wait is reached (nothing queued): `publish.in_flight(None);`.
- before the Draining `attempt_segment`: `publish_queues(publish, &metrics, pending.len(), backlog.len());` and
  ```rust
  publish.in_flight(Some(InFlight {
      segment: segment_identity(&segment),
      attempt: 1,
      retries_left: config.max_retries,
      processed: skip,
      next_retry_at_ms: None,
  }));
  ```
- RetryingTransient, before the wait loop (inside `if !waited`), and again with `next_retry_at_ms: None` just before its `attempt_segment`:
  ```rust
  publish.in_flight(Some(InFlight {
      segment: segment_identity(&segment),
      attempt: config.max_retries.saturating_sub(retries_left).saturating_add(2),
      retries_left,
      processed,
      next_retry_at_ms: Some(unix_ms_now().saturating_add(u64::try_from(next_delay.as_millis()).unwrap_or(u64::MAX))),
  }));
  ```
- RetryingTransient wait loop, around Task 2.7's `requeue.service(…)` block: a backoff cut re-publishes the record with the new `next_retry_at_ms`. Before the block put `let cut_before = backoff_cut;`, and after it:
  ```rust
  if backoff_cut && !cut_before {
      publish.in_flight(Some(InFlight {
          segment: segment_identity(&segment),
          attempt: config.max_retries.saturating_sub(retries_left).saturating_add(2),
          retries_left,
          processed,
          next_retry_at_ms: Some(unix_ms_now().saturating_add(
              u64::try_from(wait_end.saturating_duration_since(Instant::now()).as_millis())
                  .unwrap_or(u64::MAX),
          )),
      }));
  }
  ```
  (The `cut` closure borrowed `backoff_cut` and `wait_end` mutably; the borrow ends when `service` returns.)

(import `crate::metrics::{DrainQueue, DrainQueueLabel}` and `crate::status::InFlight`.)

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain::`
  Expected: all pass.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): publish state, in-flight segment, queues and replay progress to the board"`

---

### Task 2.16: the WAB-growth warning treats a paused or held drain as not delivering

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `drain_is_healthy` — where PR 1 placed it (spec §5.11 moves the poll tasks to `crates/weir-server/src/status/poll.rs`; at `3adc5f5`, `crates/weir-server/src/main.rs:721-740`)
- Test: `drain_is_healthy_tests` beside it (at `3adc5f5`, `crates/weir-server/src/main.rs:2235-2331`; PR 1 Task 1.19 moves it with the function)

**Interfaces:**
- Consumes: `Metrics::drain_paused` (PR 1).
- Produces: `drain_is_healthy(metrics) == sink_healthy && draining && !paused` (spec §9.3 condition 11: "`drain_is_healthy` also returns false while paused or held").

- [ ] **Step 1: Write the failing test.** In `mod drain_is_healthy_tests`:

```rust
    /// A held segment keeps `drain_state{draining}` (or retrying) at 1, since
    /// Held shows the state it came from, so the pause must be read from
    /// `weir_drain_paused`. A paused drain delivers nothing, and the WAB-growth
    /// warning must be able to fire on it (spec §9.3 condition 11).
    #[test]
    fn a_paused_or_held_drain_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::draining);
        m.drain_paused.set(1.0);
        assert!(!drain_is_healthy(&m), "paused must not compose to healthy");
        m.drain_paused.set(0.0);
        assert!(drain_is_healthy(&m));
    }
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain_is_healthy_tests::a_paused_or_held_drain_is_not_healthy`
  Expected: FAIL, `paused must not compose to healthy`.

- [ ] **Step 3: Implement.** In `drain_is_healthy`, before the return:

```rust
    // Paused or held (Foundations): the drain is alive but delivers nothing,
    // and Held keeps its source state's gauge, so read the pause directly.
    let paused = metrics.drain_paused.get() > 0.0;
    sink_healthy && draining && !paused
```

and extend its doc comment with the third input.

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server -- --test-threads=1 drain_is_healthy_tests`
  Expected: all 6 pass.

- [ ] **Step 5: Commit.**
  `git commit -am "fix(status): a paused drain no longer silences the WAB growth warning"`

---

### Task 2.17: wire the board into production (still Detached), close the dead-code allowances, run the gates

*PR-level merge gate: PR 2's **Merge gate** (top of the PR 2 section; the `mod tests` additions-only diff is part of it).*

**Files:**
- Modify: `crates/weir-server/src/main.rs` — before the sink `match` (`let drain_handle = match config.sink_type {`, 1162 at `3adc5f5`) and its six `build_and_spawn_drain(…, drain::DrainRuntime::detached())` arguments
- Modify: `crates/weir-server/src/drain/publish.rs` (remove the module-level `allow`; `#[cfg(test)]` on `Publisher::none`)
- Modify: `crates/weir-server/src/drain/control.rs` (replace the module-level `allow` with item-level ones; `#[cfg(test)]` on `DrainRuntime::detached`)
- Test: the whole crate, plus the gate commands below

**Interfaces:**
- Consumes: PR 1's `board: Arc<status::StatusBoard>` in `main` (built before the drain spawn, spec §6 step 3).
- Produces: production drain = `DrainRuntime { control: DrainControl::detached(), board: Some(board), initial_backlog: vec![] }`. No drain-channel handle outside the pipeline.

- [ ] **Step 1: Write the failing test (the gate).** Save as `$TMPDIR/pr2-gate.sh` (scratch, not committed):

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
base=$(git merge-base HEAD main)
# 1. Additions-only: no line of the drain `mod tests` block may be removed or changed.
removed=$(diff <(git show "$base":crates/weir-server/src/drain/mod.rs | sed -n '/^mod tests {$/,$p') \
               <(sed -n '/^mod tests {$/,$p' crates/weir-server/src/drain/mod.rs) | grep -c '^<' || true)
[ "$removed" -eq 0 ] || { echo "FAIL: $removed line(s) removed from drain mod tests"; exit 1; }
# 2. No drain-channel handle outside the pipeline: main names drain_rx only to move it into the drain.
uses=$(grep -c 'drain_rx' crates/weir-server/src/main.rs)
echo "drain_rx mentions in main.rs: $uses (expect: creation, flusher spawn arm(s), build_and_spawn_drain param + call sites)"
# replay_tx is the startup replay sender, dropped before serving; PR 4 deletes it.
grep -n 'drain_rx\.clone()\|drain_tx\.clone()' crates/weir-server/src/main.rs | grep -v 'let replay_tx = drain_tx\.clone();' && { echo "FAIL: a drain channel handle is cloned in main"; exit 1; } || true
# 3. Production wiring carries the board.
grep -q 'board: Some(Arc::clone(&board))' crates/weir-server/src/main.rs || { echo "FAIL: drain not wired to the board"; exit 1; }
echo "OK: additions-only, no extra channel handle, board wired"
```

- [ ] **Step 2: Run it to verify it fails.**
  `bash "$TMPDIR/pr2-gate.sh"`
  Expected: `FAIL: drain not wired to the board` (checks 1 and 2 already pass; check 2 skips the `let replay_tx = drain_tx.clone();` startup replay sender, which lives until PR 4 Task 4.10).

- [ ] **Step 3: Implement.** In `main.rs`, before the sink `match`:

```rust
    // Foundations PR 2: the drain publishes to the status board. It stays
    // Detached ("channel closed = deliver what is queued, then exit") until
    // PR 4 flips it to Attached together with the lifecycle that calls
    // request_stop. No drain-channel handle is created here: the drain owns
    // the only Receiver, the flushers the only Senders.
    let drain_runtime = drain::DrainRuntime {
        control: drain::DrainControl::detached(),
        board: Some(Arc::clone(&board)),
        initial_backlog: Vec::new(),
    };
```

and each of the six arms passes `drain_runtime` (moved; only one arm runs) in place of `drain::DrainRuntime::detached()`.

`publish.rs`: delete the module-level `#![allow(dead_code, …)]` line (and the comment above it); put `#[cfg(test)]` on `Publisher::none` (only the test-facing wrappers use it).

`control.rs`: put `#[cfg(test)]` on `DrainRuntime::detached`; from here on only tests construct a detached runtime (PR 3's DrainSpawner test included). Delete the module-level `#![allow(dead_code, …)]` (and the comment above it) and put this on each item that only PR 4 (lifecycle) or PR 6 (admin) calls in production — `enum Lifetime` (its `Attached` variant), `attached`, `begin_shutdown`, `request_stop`, `force_stop`, `request_pause`, `request_resume`, `request_requeue`, `pause_seq`, `generation`:

```rust
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "called by PR 4 (lifecycle) / PR 6 (admin); tests exercise it now")
    )]
```

Then run `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` and add the same attribute to any further control-side item it names, and to nothing else. A dead-code warning on a drain-side item is a wiring bug to fix, not to allow.

- [ ] **Step 4: Run to verify it passes.**
  ```bash
  bash "$TMPDIR/pr2-gate.sh"
  cargo fmt --all -- --check
  cargo clippy -p weir-server --all-targets --all-features -- -D warnings
  cargo test -p weir-server --bins -- --test-threads=1
  cargo test -p weir-server --bins --all-features -- --test-threads=1
  cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test tls_listener -- --test-threads=1
  cargo test -p weir-server --test load_drain --release -- --nocapture --test-threads=1   # delivery-side load + no-loss gate
  act -W .github/workflows/ci.yml -j lint    # the real CI jobs, per the repo's CI discipline
  act -W .github/workflows/ci.yml -j msrv
  act -W .github/workflows/ci.yml -j test
  act -W .github/workflows/ci.yml -j drain
  ```
  Expected: `OK: additions-only, no extra channel handle, board wired`; fmt and clippy clean; every test green, including PR 1's "SIGTERM exits within N s" system test.

  Spec §16 new-drain-test map (all in `drain::tests::foundations` unless noted): 1 `an_attached_drain_outlives_its_flushers_and_stops_only_on_request` · 2 `a_zero_budget_abandons_a_commit_in_flight_within_100ms` · 3 `a_stop_with_budget_left_delivers_everything_queued_then_exits` · 4 `a_backoff_that_would_outlast_the_deadline_exits_at_once` · 5 `a_stop_while_blocked_on_dead_letter_headroom_exits` · 6 `force_stop_abandons_a_yielding_commit_without_counting_a_retry` · 7 `force_stop_then_request_stop_still_exits_within_the_grace` (+ `drain::control::tests::force_stop_then_request_stop_keeps_the_forced_deadline`) · 8 `a_paused_drain_keeps_taking_from_the_channel_and_resumes_in_order` · 9 `a_pause_mid_segment_holds_at_a_batch_boundary_and_resumes_without_duplicates` · 10 `a_pause_during_a_retry_backoff_holds_without_spinning_and_keeps_probing` · 11 `retry_stranded_rescans_while_the_probe_says_down_and_dedups_the_channel` · 12 `a_flood_of_retry_stranded_requests_scans_once_and_cuts_one_backoff` · 13 `the_idle_rescan_waits_for_the_backlog_to_empty` + `a_rescan_drains_the_channel_first_and_never_double_queues` · 14 `live_segments_alternate_with_a_long_backlog` · 15 `an_initial_backlog_survives_a_panic_on_its_first_segment` · 16 `a_commit_timeout_leaves_no_commit_marked_in_flight` + `a_contract_violation_leaves_no_commit_marked_in_flight` (+ `the_commit_guard_spans_exactly_the_sink_call`, `drain::publish::tests::a_commit_guard_clears_the_marker_when_the_commit_unwinds`) · 17 `the_sinks_last_error_is_redacted_and_capped` (+ `drain::publish::tests::sanitize_*`) · 18 `a_respawned_drain_is_still_paused` · 19 `absorb_mode_skips_the_health_probe_so_a_slow_probe_cannot_delay_exit`.

- [ ] **Step 5: Commit.**
  `git commit -am "feat(drain): publish the drain to the status board in production, still detached"`
