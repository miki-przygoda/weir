# Operability Foundations — PR 4: Lifecycle, observable startup and bounded shutdown (the drain goes ATTACHED)

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 4: Lifecycle, observable startup and bounded shutdown (the drain goes ATTACHED)

**Scope** (spec §16, PR row 4; size M-L): `lifecycle.rs` (signals plus a `ShutdownLevel` watch); the startup restructure (lock → preflight → runtime → metrics/health listener → [admin slot for PR 6] → recovery → replay handed to the drain in memory → producer listeners); bounded shutdown with `drain_shutdown_budget_secs`; lame duck; the second signal; flipping the production drain to `DrainControl::attached()`; the grace-period docs. Spec sections: §5.6, §6, §7, §15 (lifecycle parts), plus the PR-4 rows of §13 (the config key) and §17 (behaviour changes 1, 2, 3, 8).

**Depends on:** PR 2 (DrainControl, DrainRuntime, `spawn_with`, DrainExit, the Attached/Detached semantics and absorb mode) and PR 3 (`wab::recover` / `scan_replay` / `spawn_flushers`, the `hand_off` loop with the per-flusher 1 s budget, `socket::bind` / `socket::bind_all` and the bind/serve split, `DrainSpawner`, live-shard routing, the `test-hooks` module). Both sit on PR 1 (StatusBoard, ShardRegistry, `weir-status::Phase`, the poll tasks in `status/poll.rs`, the synchronous first scan, the §13 metric families).

**Behaviour change:** this is *the* behaviour PR. (1) Shutdown is bounded: after the flushers join, the drain gets `drain_shutdown_budget_secs` (default 10 s), and whatever is left is replayed at the next start. (2) Startup no longer waits on the sink. `/metrics` answers before recovery, and the replay backlog is handed to the drain in memory. (3) Lame duck: SIGTERM unlinks the producer socket and closes the TCP listener at once. (4) A second SIGTERM/SIGINT forces the exit; the worker and flusher joins still run. (5) No zombie drain: a drain whose flushers are all gone keeps delivering the on-disk backlog. (6) SIGHUP is now served from before recovery, because the runtime and its handler exist earlier; it used to be dropped during recovery.

**Merge gate:**
- System tests 1-5, 11 and 13 of spec §16 pass in CI (test 3 without its `/readyz` half, which PR 5 Task 5.10 adds as `readyz_goes_503_within_200ms_of_sigterm_while_metrics_stay_up`), plus `sigterm_during_recovery_exits_zero_without_ever_serving` and `lock_stolen_during_startup_recovery_fails_closed_before_serving` (Task 4.10). Test 2, those two and the zombie test are `#[cfg(feature = "test-hooks")]` and run on the `--all-features` line (ci.yml:224).
- `cargo test -p weir-server --bins --all-features -- --test-threads=1` and `cargo test -p weir-server --bins -- --test-threads=1` both pass.
- The clippy matrix (ci.yml:23-27) passes.
- `act -W .github/workflows/ci.yml -j <job>` runs locally for the test and container jobs before pushing.
- `cargo test -p weir-server --test system --test tls_listener --all-features` runs three times with the default thread count (as CI does, ci.yml:219 and :224; only the `--bins` lines are serial there) before pushing, because the new wall-clock asserts (unlink within 200 ms, `/metrics` within 2 s, ready within 3 s) run alongside other daemons only in that mode.
- The new docs_drift guards pass and are verified by mutation.
- PRs 5 and 6 start only after this merges.
- Chaos campaigns run between this PR and PR 9 must set `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS=3600`, because the harness oracle (chaos run.py:418-436) still assumes SIGTERM delivers everything.

### Interfaces this PR consumes (reconciled against the PR 1-3 sections of this plan)

| From | Item (exact signature) |
|---|---|
| safety floor | `wab::lock::acquire_wab_dir_lock(&Path, LockPolicy, &HolderInfo) -> Result<LockOutcome, LockError>` (wab/lock.rs:305; `LockPolicy` is at :50). The lock fd lives in the process-lifetime static `WAB_DIR_LOCK` (wab/lock.rs:138); there is no guard object to drop. |
| safety floor | `preflight::preflight(&Config) -> io::Result<()>`, `preflight::{metrics_bind_error, tls_paths, tls_load_error, tcp_bind_error}` |
| safety floor | `reload::Reloader::new(Arc<Metrics>, bool) -> Arc<Reloader>`, `Reloader::register_tls(&self, ReloadableServerConfig)`, `reload::spawn_reload_loop(Arc<Reloader>) -> mpsc::Sender<ReloadRequest>`, `reload::spawn_sighup_task(mpsc::Sender<ReloadRequest>, Arc<Metrics>)` |
| safety floor | `socket::BoundSocket { listener, dev, ino, .. }`, `socket::remove_own_socket(&Path, &BoundSocket)` (socket/mod.rs:1001); `main::spawn_lock_integrity_thread(PathBuf, Arc<Metrics>, Arc<AtomicBool>) -> io::Result<()>` (main.rs:825; a std thread spawned at :999 right after the lock, before preflight, running until exit; PR 4 neither calls nor moves it) |
| PR 1 | `weir_status::Phase { Initializing, Recovering, Handoff, Binding, Serving, ShuttingDown }` (`Copy`, `Eq`, `Debug`), `Phase::as_u8` / `Phase::from_u8(u8) -> Option<Phase>` |
| PR 1 | `status::StatusBoard` (`Arc`), with `life: LifecycleCell` and `LifecycleCell::{enter(&self, Phase, now_ms: u64) -> bool /* refused once ShuttingDown; sets ever_served on Serving */, phase(&self) -> Phase, begin_shutdown(&self, now_ms: u64) -> bool, mark_forced(&self), is_shutting_down(&self) -> bool, shutting_down_flag(&self) -> Arc<AtomicBool>, forced(&self) -> bool, ever_served(&self) -> bool, set_drain_deadline(&self, deadline_unix_ms: u64, budget_secs: u64)}`; and on the board `StatusBoard::enter_phase(&self, Phase, &Metrics) -> bool` (moves the phase and publishes the one-hot `weir_startup_phase`) and `StatusBoard::begin_shutdown(&self, &Metrics) -> bool` (T0 a-b: flag, phase, gauge; true for the first caller) |
| PR 1 | `StatusBoard.listeners: ListenerCells { unix: ListenerCell, tcp: ListenerCell }`, `ListenerCell::{set(&self, ListenerState), get(&self) -> ListenerState}`, `weir_status::ListenerState { NotConfigured, Pending, Bound, Closed, Failed }`; PR 1's `SocketConfig.board: Option<Arc<StatusBoard>>` hook, with one writer per mark (README Q6, applied in PR 3 Tasks 3.10-3.11): `bind`/`bind_all` mark `Bound`/`Failed`; `serve` marks `Serving` at its start and, from Task 4.7, `Closed` at the lame duck; from Task 4.7 `tcp::run` marks the TCP listener `Closed` (lame duck) or `Failed` (an `Err` return); `main` marks nothing except in `discard_listeners`, the pre-serving path where `serve` never runs |
| PR 1 | `StatusBoard.shards: Arc<ShardRegistry>` with `ShardRegistry::all_settled(&self) -> bool`; `StatusBoard.wab: WabCell` with `last_scan_ok_ms(&self) -> Option<u64>`; `StatusBoard.wab_bytes_now: Arc<AtomicU64>` (the last successful size scan) |
| PR 1 | `#[cfg(test)] StatusBoard::for_test_with(shards: usize, workers: usize) -> Arc<StatusBoard>` and `#[cfg(test)] status::test_board(shard_count, worker_count)` |
| PR 1 | `StatusBoard::new(StaticInfo, shard_count, worker_count, tcp_configured: bool, wab_cap_rejecting: Arc<AtomicBool>, wab_bytes_now: Arc<AtomicU64>) -> Arc<StatusBoard>`, `status::board::StaticInfo::capture(&Config, Arc<ConfigView>)`; `Metrics::{init_topology, publish_build_info, publish_config}` (PR 1 Task 1.23's metrics block) |
| PR 1 | `status::poll::spawn_queue_depth_task(QueueSender<WorkUnit>, Arc<Metrics>, Arc<StatusBoard>) -> tokio::task::JoinHandle<()>`; `status::poll::startup_cap_gate(&Path, wab_max_bytes: u64, &StatusBoard, &Metrics) -> Option<u64>` (the synchronous first scan plus `wab::cap::evaluate`, fail-closed); `status::poll::{WabPoll::new(Arc<Metrics>, Arc<AtomicU64>, u64, DrainHeartbeat, DrainLiveness), WabPoll::with_status(PollStatus), WabPoll::tick, DrainLiveness::new, PollStatus::new(Arc<StatusBoard>, PathBuf, Arc<AtomicU64>), compute_wab_bytes_on_disk}` (the 5 s task body PR 1 Task 1.23 spawns inline) |
| PR 1 | `Metrics.startup_phase: Family<StartupPhaseLabel, Gauge<f64, AtomicU64>>`, with `StartupPhaseLabel { phase: StartupPhaseValue }` and `StartupPhaseValue { initializing, recovering, handoff, binding, serving, shutting_down }` (`ALL`, `of(Phase)`), all pre-created. **Not** registered by PR 1 (PR 1 Task 1.18 assigns them here): `weir_config_drain_shutdown_budget_seconds` and `weir_drain_shutdown_deadline_seconds`, which Task 4.1 registers. |
| PR 1 | `status/config_view.rs::ConfigView::from(&Config)`, an exhaustive destructure with no `..`, built with the private `Entries::{public, admin, redacted}` helpers and `u(..)`/`b(..)`/`s(..)`; PR 1's test `public_entries_are_exactly_the_spec_9_1_subset` carries `doc.retain(|k| k != "drain_shutdown_budget_secs")` until Task 4.1 removes it |
| PR 2 | `drain::control::DrainControl::{attached() -> Self, detached() -> Self, begin_shutdown(&self), request_stop(&self, Duration), force_stop(&self), deadline_passed(&self) -> bool, time_left(&self) -> Option<Duration>, deadline_unix_ms(&self) -> Option<u64>, absorbing(&self) -> bool, stop_requested(&self) -> bool}` (`Clone`) |
| PR 2 | `drain::DrainRuntime { control: DrainControl, board: Option<Arc<StatusBoard>>, initial_backlog: Vec<PathBuf> }` and `drain::DrainExit { reason: status::DrainExitReason, abandoned: Option<(String /* segment identity */, u64)>, pending_left: usize, backlog_left: usize, last_panic: Option<String> }` |
| PR 2 / PR 1 | `StatusBoard.drain.in_flight: ArcSwapOption<status::InFlight>`, with `InFlight { segment: String, processed: u64, .. }`. The Attached drain logs the WARN `"all flushers gone; continuing to deliver the on-disk backlog"`. |
| PR 3 | `wab::recover(&Path, usize, &Arc<Metrics>) -> io::Result<RecoverySummary>`, with `RecoverySummary { shard_dirs: usize, segments_quarantined: u64, segments_failed: u64, elapsed: Duration }` |
| PR 3 | `wab::scan_replay(&Path, usize, &Arc<Metrics>, orphan_cleanup: bool) -> io::Result<Vec<PathBuf>>`. Reconciled: the code's `replay_unconfirmed` takes `orphan_cleanup` (safety floor, wab/mod.rs:620-626), and the spec's signature predates it. PR 3 keeps `replay_unconfirmed` + main's `replay_tx`; Task 4.10 deletes both. |
| PR 3 | `wab::spawn_flushers(wab_dir: PathBuf, config: WabConfig, drain_tx: Sender<PathBuf>, metrics: Arc<Metrics>, coalesce_hint: Arc<AtomicU64>, registry: Arc<ShardRegistry>, handoff: wab::handoff::HandoffConfig) -> io::Result<WabHandle>`, with `HandoffConfig { shutting_down: Arc<AtomicBool>, drain_depth: Arc<AtomicUsize>, marker: Option<Arc<dyn wab::handoff::HandoffMarker>> }`; `wab::DRAIN_CHANNEL_CAPACITY` |
| PR 1 / PR 3 | `Metrics.wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>` (`weir_wab_segments_not_handed_off_total{reason}`, `NotHandedOffReason::{budget, disconnected}`, both pre-created; README Q7) and `pub(crate) fn Metrics::wab_segments_not_handed_off_total(&self) -> u64` (the sum of both series), registered by PR 1 Task 1.18 and incremented by PR 3's `hand_off`; `Metrics.{drain_channel_depth, drain_channel_capacity}` (PR 3 Task 3.6) |
| PR 3 | `socket::bind(&SocketConfig) -> io::Result<BoundUnix>`, `socket::BoundUnix { path: PathBuf, socket: BoundSocket }`, `socket::bind_all(&SocketConfig, Option<SocketAddr>) -> io::Result<BoundListeners>` (async; TCP first, then Unix), `socket::BoundListeners { unix: BoundUnix, tcp: Option<TcpListener> }`, `socket::route::{ShardRouter::live(Arc<dyn LiveShards>), LiveShards}` |
| PR 3 | `socket::serve(SocketConfig, BoundUnix, QueueSender<WorkUnit>, oneshot::Receiver<()>, Arc<Metrics>, Arc<Semaphore>, ShardRouter) -> io::Result<()>`, `#[cfg(test)] socket::run(SocketConfig, QueueSender<WorkUnit>, oneshot::Receiver<()>, Arc<Metrics>, Arc<Semaphore>)`, and `socket::tcp::run(TcpConfig, TcpListener, ReloadableServerConfig, QueueSender<WorkUnit>, Arc<Semaphore>, oneshot::Receiver<()>, Arc<Metrics>, ShardRouter) -> io::Result<()>`. This PR replaces the `oneshot::Receiver<()>` with `watch::Receiver<ShutdownLevel>` and adds `board: Arc<StatusBoard>` as `tcp::run`'s last parameter. |
| PR 3 | `sink::select::build_drain_spawner(&Config, &Arc<Metrics>) -> Result<DrainSpawner, Box<dyn Error>>`, and `DrainSpawner::spawn(self, DrainSpawnArgs { rx, config, metrics, heartbeat, runtime }) -> std::thread::JoinHandle<DrainExit>` |
| PR 3 | the `test-hooks` feature and `crate::test_hooks::{enabled() -> bool, fail_counter_scan(shard_id: u16) -> bool, warn_if_compiled_in()}` (PR 3 Task 3.12; Task 4.2 extends it); `main`'s `fn wait_until(Duration, Duration, impl FnMut() -> bool) -> bool` and `SHARD_SETTLE_TIMEOUT` (PR 3 Task 3.11; Task 4.9 moves them into `lifecycle.rs`) |

### Reconciliations with the current code (code wins for current behaviour, spec for target)

1. **SIGHUP.** Spec §5.6 has `Lifecycle::spawn_signal_task(reloader, rt)` loop on TERM/INT/HUP and "replace `spawn_tls_reload_task` (main.rs:356-394)". That function no longer exists: the safety floor replaced it with `reload.rs` (`Reloader`, `spawn_reload_loop`, `spawn_sighup_task`, `request_reload`), and the spec says the admin `reload` verb reuses that queue.
   - In this PR, the Lifecycle signal task handles **TERM and INT only**. `reload::spawn_sighup_task` stays the single HUP path.
   - Both are installed at startup step 4, before recovery. A HUP during recovery is therefore **served** as a reload: today it is dropped, because the handler is installed only when the runtime starts (main.rs:1417-1429).
   - Task 4.10 deletes one and rewrites the other of the two safety-floor tests that pinned the old window (README Q4 keeps this split of TERM/INT and HUP). `spawn_signal_task` takes neither `reloader` nor `rt`; it spawns onto the runtime entered at step 4.
2. **The lock guard.** Spec §6 step 2 and §7 T3 talk about dropping `wab_lock`. The safety floor holds the lock fd in a process-lifetime static (wab/lock.rs:107) and never releases it explicitly. Both exit paths (clean `return`, orphan `process::exit(0)`) therefore release it only when the kernel reaps the process, which is exactly the spec's requirement.
   - Verified at 3adc5f5: the integrity check is a thread (`spawn_lock_integrity_thread`, main.rs:825) started right after acquisition (:999) and running until exit, so it already spans recovery; PR 4 neither moves nor duplicates it, and keeps the one `wab_ownership_lost` flag that thread writes.
3. **Socket identity.** Spec `BoundUnix{listener, path, dev, ino, parent_dev_ino}` is the code's `BoundSocket{listener, dev, ino, pin|ctime|birthtime}` (socket/mod.rs:406-418), and the inode-checked unlink is `remove_own_socket` (socket/mod.rs:1001).
   - **Order at lame duck:** the spec says "drop the listener, then unlink". This plan unlinks first and then drops the `BoundSocket`. While the listener (and on Linux the `O_PATH` pin) is still open, `names_our_socket` proves the identity with no window; on macOS the ctime/birthtime tie-breaker is the same either way.
   - Both happen before the connection drain, which is what the lame duck needs. Verified at 3adc5f5: the stricter probe changes only startup probing (socket/mod.rs:857); remove_own_socket (:1001) and names_our_socket (:1045) are unchanged, so unlink-then-drop stands.
4. **"Accepted connections are untouched" (spec §7).** Today's handlers already exit between frames when `handler_shutdown_tx` fires (connection.rs:147-159, 287, 310; pinned by `idle_connection_exits_promptly_on_shutdown`). This PR keeps that unchanged.
   - "Untouched" means "not aborted before `shutdown_timeout_secs` or a forced shutdown".
   - System test 3 therefore asserts a clean EOF on the idle connection. The "acked pushes are durable, the rest see Io" contract stays with `graceful_shutdown_under_load`. (Decided: README Q5.)
5. **The queue sender count (spec §5.7: "a debug assertion checks the sender count is 0").**
   - crossbeam exposes no sender count. Task 4.8 adds one (`QueueReceiver::live_senders`, an `Arc`/`Weak` pair, with nothing on the push path).
   - An aborted connection task may still own a sender inside its `spawn_blocking` push for up to `QUEUE_PUSH_TIMEOUT` (5 s, connection.rs:31, 610-611). So T1 waits up to `QUEUE_SENDERS_GRACE` (6 s) for the count to reach 0, logs an ERROR if it does not, and only then hits the `debug_assert`.
6. **`/readyz` and `/startupz` do not exist until PR 5.** System tests 2 and 3 assert on `weir_startup_phase{phase=..}` and the socket file. PR 5 adds the probe assertions to the same tests.
7. **The T3 summary's "D dead-lettered records".** `DrainExit` (spec §5.4) carries no dead-letter count, so the summary states the re-dead-letter window without a number (decided: README Q5).
8. **Line numbers.** Spec line numbers are against 4.0.1, and the code has moved. The current sites are:
   - main.rs (at 3adc5f5): `// ── Metrics` at 976, `// ── Queue` at 1037, the blocking replay at 1425, the runtime block from 1440, the metrics bind at 1460, the SIGTERM task at 1634-1657, `drop(rt)` at 1712, and the joins at 1716-1722;
   - socket/mod.rs: the shutdown unlink is at 300 and the connection drain at 273-291;
   - tcp.rs: the connection drain is at 288-309.

   PR 3 moves these again; find them by symbol, not by line.
9. **Who deletes the blocking replay.** Spec §5.5 deletes `wab::replay_unconfirmed` and main's `replay_tx` as part of the PR 3 split. PR 3 keeps both (its Task 3.3, because PR 2's `initial_backlog` is not wired until here), so Task 4.10 deletes them and system test 1 fails first on PR 3's tree (resolved; was open question 1).
10. **Integrator reconciliations applied to this section.** PR 1 already makes `ShuttingDown` terminal in `LifecycleCell::enter` and publishes the phase gauge through `StatusBoard::enter_phase` / `begin_shutdown`, so Task 4.4 reuses them instead of re-editing `board.rs`; PR 3 owns the `test-hooks` module, `bind_all` and `wait_until`, so Tasks 4.2, 4.9 and 4.10 extend or move them instead of re-creating them; PR 3's accept loop is `socket::serve` (the `run` name is a `#[cfg(test)]` wrapper), so Task 4.7 edits `serve`; the two PR 4 metric families PR 1 left to this PR are registered in Task 4.1.

---

### Task 4.1: `drain_shutdown_budget_secs` config key

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/config/mod.rs`:
  - `PartialConfig`, ~line 243: add the field next to `shutdown_timeout_secs`;
  - `Config`, ~line 457;
  - the merge block after `shutdown_timeout_secs`, ~line 912;
  - the `Config { .. }` literal, ~line 1241;
  - the tests module.
- Modify: `crates/weir-server/src/config/env.rs` (~line 24), `crates/weir-server/src/config/cli.rs` (HELP ~line 49; parse ~line 207; tests), `crates/weir-server/src/config/file.rs` (`RawServer` ~line 32; `BASE_SERVER_KEYS` ~line 117; mapping ~line 243; tests)
- Modify: `crates/weir-server/src/status/config_view.rs` (PR 1's exhaustive destructure: classify the new field `Public`)
- Modify: `crates/weir-server/src/metrics/mod.rs` (register `weir_config_drain_shutdown_budget_seconds` and `weir_drain_shutdown_deadline_seconds`, which PR 1 Task 1.18 leaves to this PR; extend PR 1's `Metrics::publish_config`; `all_metric_names_present_in_output`) and `docs/monitoring.md` (two rows in PR 1's `### Status, lifecycle and conditions` table)
- Modify: `docs/operations/configuration.md` (a new `#### \`drain_shutdown_budget_secs\`` under `### Shutdown`, ~line 1006), `deploy/docker/weir.toml.example` (the `shutdown_timeout_secs` comment ~line 118 and the new key after it), `deploy/systemd/weir.toml` (~line 57)
- Modify: `crates/weir-server/tests/system.rs` (`metrics_all_families_registered`: the two new families in its `expected` vec, which its `# HELP weir_` count check requires)
- Test: `crates/weir-server/src/config/mod.rs`, `config/cli.rs`, `config/file.rs`, `crates/weir-server/tests/docs_drift.rs`, `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes: `check_range<T: PartialOrd + Display>(&'static str, T, T, T) -> Result<(), ConfigError>` (config/mod.rs:1419); `ConfigView::from(&Config)` (PR 1).
- Produces:
  - `PartialConfig::drain_shutdown_budget_secs: Option<u64>` and `Config::drain_shutdown_budget_secs: u64` (default 10, range 0..=3600);
  - CLI `--drain-shutdown-budget-secs <secs>`, env `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`, TOML `[server] drain_shutdown_budget_secs`;
  - `ConfigView` field `drain_shutdown_budget_secs: u64` classified `Public` (spec §9.1 lists it in the public subset; PR 1's `PublicConfig` already has the `drain_shutdown_budget_secs: Option<u64>` field);
  - `Metrics.config_drain_shutdown_budget_seconds: Gauge<f64, AtomicU64>` (`weir_config_drain_shutdown_budget_seconds`, set by `publish_config`) and `Metrics.drain_shutdown_deadline_seconds: Gauge<f64, AtomicU64>` (`weir_drain_shutdown_deadline_seconds`, unix seconds, 0 until Task 4.11's T2 sets it).

- [ ] **Write the failing tests.** Append to `mod tests` in `config/mod.rs`:

```rust
    #[test]
    fn drain_shutdown_budget_defaults_to_ten_seconds() {
        let dir = tmp_dir("budget_default");
        let c = layers_with_wab(dir.clone()).unwrap();
        // Spec §13: 10 s, counted from the flusher joins (owner decision).
        assert_eq!(c.drain_shutdown_budget_secs, 10);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn drain_shutdown_budget_accepts_zero_and_an_hour_and_rejects_more() {
        for ok in [0u64, 3600] {
            let dir = tmp_dir(&format!("budget_ok_{ok}"));
            let c = Config::from_layers(
                PartialConfig {
                    wab_dir: Some(dir.clone()),
                    drain_shutdown_budget_secs: Some(ok),
                    ..PartialConfig::empty()
                },
                PartialConfig::empty(),
                PartialConfig::empty(),
            )
            .unwrap();
            // 0 is legal: "seal, hand off, abandon whatever is in flight".
            assert_eq!(c.drain_shutdown_budget_secs, ok);
            fs::remove_dir_all(dir).ok();
        }
        assert_knob_rejected("rng_drain_budget", "drain_shutdown_budget_secs", |p| {
            p.drain_shutdown_budget_secs = Some(3601)
        });
    }
```

Append to `mod tests` in `crates/weir-server/src/metrics/mod.rs`:

```rust
    #[test]
    fn the_shutdown_budget_and_deadline_gauges_are_exported() {
        let (m, reg) = Metrics::new();
        m.config_drain_shutdown_budget_seconds.set(10.0);
        m.drain_shutdown_deadline_seconds.set(1_800_000_000.5);
        let out = encode_to_string(&reg);
        assert!(out.contains("weir_config_drain_shutdown_budget_seconds 10"), "{out}");
        assert!(out.contains("weir_drain_shutdown_deadline_seconds 1800000000.5"), "{out}");
    }
```

and add `"weir_config_drain_shutdown_budget_seconds"` and `"weir_drain_shutdown_deadline_seconds"` to the `expected` array of `all_metric_names_present_in_output`.

In `crates/weir-server/tests/system.rs`, `metrics_all_families_registered`, append to the `expected` vec (after the last entry, before `];`):

```rust
        // Bounded shutdown (Foundations PR 4)
        "weir_config_drain_shutdown_budget_seconds",
        "weir_drain_shutdown_deadline_seconds",
```

That test asserts the number of `# HELP weir_` lines equals the vec's length, so registering the two families without this edit fails it.

Append to `mod tests` in `config/cli.rs`:

```rust
    #[test]
    fn drain_shutdown_budget_flag_is_parsed() {
        let (partial, _) =
            parse_from(args(&["--drain-shutdown-budget-secs", "42"])).unwrap();
        assert_eq!(partial.drain_shutdown_budget_secs, Some(42));
        assert!(
            HELP.contains("--drain-shutdown-budget-secs"),
            "the flag must be in --help"
        );
    }
```

Append to `mod tests` in `config/file.rs`:

```rust
    #[test]
    fn drain_shutdown_budget_is_a_known_key() {
        let dir = crate::testutil::scratch_dir("filecfg_budget");
        crate::testutil::mkdir_p(&dir);
        let path = dir.join("weir.toml");
        std::fs::write(&path, "[server]\ndrain_shutdown_budget_secs = 7\n").unwrap();
        let (partial, warnings) = read(&path).unwrap().expect("the file exists");
        assert_eq!(partial.drain_shutdown_budget_secs, Some(7));
        assert!(
            warnings.iter().all(|w| !w.contains("drain_shutdown_budget_secs")),
            "drain_shutdown_budget_secs must not be reported as unknown: {warnings:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }
```

Append to `crates/weir-server/tests/docs_drift.rs` (the same shape as `the_wab_dir_lock_key_is_in_the_configuration_reference`, line 796):

```rust
/// The bounded-shutdown budget (Foundations §7) is an operator-facing knob
/// whose default decides how much a SIGTERM delivers; it must be findable in
/// the reference and in the example that claims to list every option.
#[test]
fn the_drain_shutdown_budget_key_is_in_the_configuration_reference() {
    assert!(
        CONFIG_ENV_SRC.contains("\"WEIR_DRAIN_SHUTDOWN_BUDGET_SECS\""),
        "src/config/env.rs no longer reads WEIR_DRAIN_SHUTDOWN_BUDGET_SECS; update \n\
         this guard and the docs to the new spelling"
    );
    assert!(
        CONFIG_CLI_SRC.contains("\"--drain-shutdown-budget-secs\""),
        "src/config/cli.rs no longer parses --drain-shutdown-budget-secs"
    );
    // Scoped to the entry's own section: "**Default**: `10`" also appears
    // under two other keys, so a whole-file search would stay green with
    // this entry's default deleted.
    let entry = CONFIGURATION
        .split("#### `drain_shutdown_budget_secs`")
        .nth(1)
        .expect("docs/operations/configuration.md must have a #### `drain_shutdown_budget_secs` entry")
        .split("\n#### ")
        .next()
        .unwrap();
    for needle in [
        "**Default**: `10`",
        "**Range**: 0-3600",
        "`WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`",
        "`--drain-shutdown-budget-secs",
    ] {
        assert!(
            entry.contains(needle),
            "the drain_shutdown_budget_secs entry is missing {needle:?}"
        );
    }
    for needle in ["drain_shutdown_budget_secs", "WEIR_DRAIN_SHUTDOWN_BUDGET_SECS"] {
        assert!(
            CONFIG_EXAMPLE.contains(needle),
            "deploy/docker/weir.toml.example claims to list every option; it is \n\
             missing {needle:?}"
        );
    }
}
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server config:: -- --test-threads=1`
  Expected: a compile failure, `error[E0560]: struct \`PartialConfig\` has no field named \`drain_shutdown_budget_secs\``.
  `cargo test -p weir-server --test docs_drift the_drain_shutdown_budget_key`
  Expected: `FAILED`, with the panic text `src/config/env.rs no longer reads WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`.

- [ ] **Implement.**

`config/mod.rs`, `PartialConfig` (after `shutdown_timeout_secs`):

```rust
    pub shutdown_timeout_secs: Option<u64>,
    pub drain_shutdown_budget_secs: Option<u64>,
```

`Config` (after `shutdown_timeout_secs`):

```rust
    pub shutdown_timeout_secs: u64,
    /// How long the drain may keep delivering after every flusher has joined
    /// (spec §7 T2). At the deadline the in-flight segment is abandoned without
    /// a `.confirmed` and everything left replays on the next start. 0 =
    /// abandon at once. Default 10.
    pub drain_shutdown_budget_secs: u64,
```

Merge block (directly after the `shutdown_timeout_secs == 0` guard):

```rust
        // Bounded shutdown (Foundations §7): the drain's budget after the
        // flusher joins. 0 is legal (seal, hand off, abandon what is in
        // flight); the hour cap keeps a typo from turning SIGTERM back into
        // "deliver the whole backlog".
        let drain_shutdown_budget_secs = merge!(drain_shutdown_budget_secs).unwrap_or(10);
        check_range("drain_shutdown_budget_secs", drain_shutdown_budget_secs, 0, 3600)?;
```

`Config { .. }` literal: add `drain_shutdown_budget_secs,` after `shutdown_timeout_secs,`.

`config/env.rs` (after the `shutdown_timeout_secs` line):

```rust
        drain_shutdown_budget_secs: env_parse("WEIR_DRAIN_SHUTDOWN_BUDGET_SECS")?,
```

`config/cli.rs`, HELP (after the `--shutdown-timeout-secs` line, same column layout):

```text
    --drain-shutdown-budget-secs <secs>      Drain delivery budget after SIGTERM, once
                                               ingest is sealed (0-3600) [default: 10]
```

and in `parse_from` (after `shutdown_timeout_secs`):

```rust
        drain_shutdown_budget_secs: pargs
            .opt_value_from_str("--drain-shutdown-budget-secs")
            .map_err(pico_err)?,
```

`config/file.rs`: add `drain_shutdown_budget_secs: Option<u64>,` to `RawServer` after `shutdown_timeout_secs`; add `"drain_shutdown_budget_secs",` to `BASE_SERVER_KEYS` after `"shutdown_timeout_secs"`; add `drain_shutdown_budget_secs: s.drain_shutdown_budget_secs,` in the `PartialConfig` mapping.

`status/config_view.rs` (PR 1): in the exhaustive `let Config { .. } = cfg;` destructure add `drain_shutdown_budget_secs,`, and beside PR 1's `v.public("shutdown_timeout_secs", u(*shutdown_timeout_secs));` add:

```rust
        v.public("drain_shutdown_budget_secs", u(*drain_shutdown_budget_secs));
```

The build fails until this is done; that is the guard working. In the same file's tests, delete PR 1's line `doc.retain(|k| k != "drain_shutdown_budget_secs"); // the key arrives with PR 4` from `public_entries_are_exactly_the_spec_9_1_subset`: the public entries now equal `PublicConfig`'s keys exactly.

`metrics/mod.rs`, struct fields beside PR 1's `config_shutdown_timeout_seconds`:

```rust
    /// Effective `drain_shutdown_budget_secs` (Foundations §7).
    pub config_drain_shutdown_budget_seconds: Gauge<f64, AtomicU64>,
    /// Unix time (seconds) at which a bounded shutdown abandons the drain's
    /// in-flight work. 0 until T2 (`request_stop`) sets it.
    #[cfg_attr(not(test), allow(dead_code))] // read from PR 4 Task 4.11 (T2); remove there
    pub drain_shutdown_deadline_seconds: Gauge<f64, AtomicU64>,
```

registrations beside PR 1's `config_shutdown_timeout_seconds`, both added to the `Metrics { .. }` literal:

```rust
        let config_drain_shutdown_budget_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
            "weir_config_drain_shutdown_budget_seconds", "Effective drain_shutdown_budget_secs");
        let drain_shutdown_deadline_seconds = reg!(Gauge::<f64, AtomicU64>::default(),
            "weir_drain_shutdown_deadline_seconds",
            "Unix time at which a bounded shutdown abandons the drain's in-flight work (0 = not shutting down)");
```

and in PR 1's `Metrics::publish_config`, after the `config_shutdown_timeout_seconds` line:

```rust
        self.config_drain_shutdown_budget_seconds.set(c.drain_shutdown_budget_secs as f64);
```

`main.rs` needs no change: PR 1's metrics block already calls `publish_config(&config)` right after `Metrics::new`.

`docs/monitoring.md`, PR 1's `### Status, lifecycle and conditions` table, after the `weir_config_shutdown_timeout_seconds` row:

```markdown
| `weir_config_drain_shutdown_budget_seconds` | gauge | Effective `drain_shutdown_budget_secs`: how long the drain keeps delivering after SIGTERM once everything is sealed. |
| `weir_drain_shutdown_deadline_seconds` | gauge | Unix time at which the current bounded shutdown abandons the drain's in-flight work; 0 when not shutting down. `weir_drain_shutdown_deadline_seconds - time()` is the budget left. |
```

`docs/operations/configuration.md`, under `### Shutdown`, after the `shutdown_timeout_secs` entry:

```markdown
#### `drain_shutdown_budget_secs`

- **Type**: u64 (seconds)
- **Default**: `10`
- **Range**: 0-3600
- **CLI**: `--drain-shutdown-budget-secs <n>`
- **Env**: `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`
- **TOML**: `drain_shutdown_budget_secs`

How long the drain keeps delivering after SIGTERM, counted from the moment
every flusher has sealed and handed off its last segment. At the deadline the
drain abandons the segment it is delivering (no `.confirmed` is written) and
exits; that segment and every segment still queued stay sealed and unconfirmed
on disk and are delivered by the next start. `0` abandons at once.

**Effect**: shutdown no longer waits for the whole backlog. A sink that is slow
or down cannot hold `systemctl stop` or a pod termination hostage. The cost is
at-least-once re-delivery of the abandoned segment's committed prefix on the
next start (see "Shutdown grace period" below).

**When to tune**: raise it (up to 3600) to drain more on a planned stop; to
decommission a host, wait for `weir_wab_undelivered_segments == 0` first
rather than relying on the budget. Whatever you choose, keep the
orchestrator grace at `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`.
```

`deploy/docker/weir.toml.example`: in the comment above `shutdown_timeout_secs = 30`, replace the line `# Set <= the orchestrator's terminationGracePeriodSeconds minus a few seconds.` with `# Keep the orchestrator grace >= shutdown_timeout_secs + drain_shutdown_budget_secs + 5.`; then, after `shutdown_timeout_secs = 30`:

```toml

# Bounded shutdown: how long the drain keeps delivering after SIGTERM, counted
# from the moment ingest is sealed. What is left replays on the next start.
# Keep the orchestrator grace >= shutdown_timeout_secs + this + 5 (45 s at the
# defaults). Range 0-3600.
# Env: WEIR_DRAIN_SHUTDOWN_BUDGET_SECS
drain_shutdown_budget_secs = 10
```

`deploy/systemd/weir.toml`, after `shutdown_timeout_secs = 30`:

```toml
# Drain budget after the flushers join; the rest replays at the next start.
# TimeoutStopSec must stay >= shutdown_timeout_secs + this + 5 (45s).
drain_shutdown_budget_secs = 10
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server config:: -- --test-threads=1` passes, including `drain_shutdown_budget_defaults_to_ten_seconds`, `drain_shutdown_budget_accepts_zero_and_an_hour_and_rejects_more`, `drain_shutdown_budget_flag_is_parsed` and `drain_shutdown_budget_is_a_known_key`.
  `cargo test -p weir-server --bin weir-server metrics:: status::config_view -- --test-threads=1` passes, including `the_shutdown_budget_and_deadline_gauges_are_exported`, `all_metric_names_present_in_output` and PR 1's `public_entries_are_exactly_the_spec_9_1_subset` without its `retain`.
  `cargo test -p weir-server --test docs_drift every_registered_metric_is_in_the_monitoring_reference` passes (it fails naming both families if the two monitoring rows are skipped).
  `cargo test -p weir-server --test system metrics_all_families_registered -- --test-threads=1` passes (it fails with `metric family count mismatch` if the `expected` vec edit is skipped).
  `cargo test -p weir-server --test docs_drift` passes.
  Mutation check: delete the `- **Default**: \`10\`` line of the `drain_shutdown_budget_secs` entry in configuration.md, watch the guard fail with `the drain_shutdown_budget_secs entry is missing "**Default**: \`10\`"`, then restore it.
  Then `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` and `cargo fmt --check`.

- [ ] **Commit.** `feat(config): drain_shutdown_budget_secs, the drain's delivery budget after SIGTERM`

---

### Task 4.2: Extend PR 3's `test-hooks` module: the startup delay, an unconditional module, and a never-ships guard

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

*(Integrator reconciliation: PR 3 Task 3.12 created the `test-hooks` feature, `src/test_hooks.rs` with `enabled()`, `fail_counter_scan(shard_id: u16)` and `warn_if_compiled_in()`, the counter-scan hook in `flusher_thread`, and the docs_drift guard `the_test_hooks_system_tests_run_in_ci`. This task no longer creates a second module or feature; it extends PR 3's. `fail_counter_scan` keeps PR 3's `u16` parameter, because `flusher_thread`'s `shard_id` is a `u16`.)*

**Files:**
- Modify: `crates/weir-server/src/test_hooks.rs` (PR 3's file: pure parsers, `startup_delay`, inlined no-ops when the feature is off, the louder startup WARN)
- Modify: `crates/weir-server/src/main.rs` (PR 3's `#[cfg(feature = "test-hooks")] mod test_hooks;` and `#[cfg(feature = "test-hooks")] test_hooks::warn_if_compiled_in();` lose their `cfg`; `startup_delay` is called from Task 4.10's step 8)
- Modify: `crates/weir-server/src/wab/mod.rs` (`flusher_thread`'s counter-scan loop: PR 3's `#[cfg(feature = "test-hooks")] let scan = …` shadowing becomes one unconditional `and_then`)
- Modify: `crates/weir-server/Cargo.toml` (PR 3's `test-hooks = []` comment gains the startup delay)
- Test: `crates/weir-server/src/test_hooks.rs` (unit), `crates/weir-server/tests/docs_drift.rs` (extend PR 3's guard), `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes: PR 3's `test_hooks::{enabled, fail_counter_scan, warn_if_compiled_in}` and its counter-scan call site; `ShardWriter::scan_and_advance_counter(&mut self) -> io::Result<()>` (wab/segment.rs:614); PR 1's `FlusherExit::CounterScanFailed`.
- Produces:
  - `test_hooks::startup_delay() -> Option<Duration>` (env `WEIR_TEST_STARTUP_DELAY_MS=<ms>`, armed only with `WEIR_TEST_HOOKS=1`);
  - `test_hooks::{enabled() -> bool, fail_counter_scan(shard_id: u16) -> bool, warn_if_compiled_in()}` unchanged in signature, now compiled in every build (inlined no-ops without the feature), so production code calls them without `cfg`;
  - the startup WARN text `THIS BINARY CONTAINS TEST HOOKS (built with --features test-hooks); never run it in production`;
  - the docs_drift const `CI_YML` (PR 8 reuses it; PR 3's guard switches from its inline `include_str!` to it) and the release-never-ships assertion inside PR 3's `the_test_hooks_system_tests_run_in_ci`.

  Hook-dependent system tests carry `#[cfg(feature = "test-hooks")]`.

- [ ] **Write the failing tests.** Append to PR 3's `src/test_hooks.rs` (it has no test module yet):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn delay_needs_the_master_switch_and_a_number() {
        assert_eq!(parse_delay(Some("1"), Some("250")), Some(Duration::from_millis(250)));
        assert_eq!(parse_delay(None, Some("250")), None, "not armed");
        assert_eq!(parse_delay(Some("0"), Some("250")), None, "armed means exactly 1");
        assert_eq!(parse_delay(Some("1"), Some("soon")), None, "garbage is ignored");
        assert_eq!(parse_delay(Some("1"), None), None);
    }

    #[test]
    fn counter_scan_list_matches_exact_shard_ids() {
        assert!(parse_shard_list(Some("1"), Some("0"), 0));
        assert!(parse_shard_list(Some("1"), Some("3, 7"), 7));
        assert!(!parse_shard_list(Some("1"), Some("3,7"), 1));
        assert!(!parse_shard_list(None, Some("0"), 0), "not armed");
        assert!(!parse_shard_list(Some("1"), Some("x"), 0));
    }
}
```

In `tests/docs_drift.rs`, add after the existing `const CONFIG_CLI_SRC` line:

```rust
const CI_YML: &str = include_str!("../../../.github/workflows/ci.yml");
```

and extend PR 3's `the_test_hooks_system_tests_run_in_ci` in place: replace its `let ci = include_str!("../../../.github/workflows/ci.yml");` line with `let ci = CI_YML;`, and append before its closing brace:

```rust
    // And no release build may carry the hooks (Foundations §16).
    assert!(
        !RELEASE_YML.contains("test-hooks") && !RELEASE_YML.contains("--all-features"),
        "the release workflow must never build weir-server with the test hooks"
    );
```

Append to `tests/system.rs`:

```rust
/// A binary built with the hooks says so loudly at startup, armed or not.
#[cfg(feature = "test-hooks")]
#[test]
fn a_binary_with_test_hooks_announces_them() {
    let srv = weir_server!("hooks_announce").log_level("info").start();
    let log = read_log(&srv);
    assert!(
        log.contains("THIS BINARY CONTAINS TEST HOOKS"),
        "the startup WARN is missing\nlog:\n{log}"
    );
    srv.shutdown();
}
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server --features test-hooks test_hooks:: -- --test-threads=1`
  Expected: `error[E0425]: cannot find function \`parse_delay\` in this scope` (and `parse_shard_list`).
  `cargo test -p weir-server --test system --all-features a_binary_with_test_hooks_announces_them -- --test-threads=1`
  Expected: it panics with `the startup WARN is missing` (PR 3's text is "this weir-server was built with the test-hooks feature").
  `cargo test -p weir-server --test docs_drift the_test_hooks_system_tests_run_in_ci` passes (release.yml carries neither string); the release half is a tripwire, checked by mutation below.

- [ ] **Implement.** `Cargo.toml`, replace PR 3's comment above `test-hooks = []` with:

```toml
# System-test fault hooks (Foundations §16): a startup delay inside the
# recovering phase and a forced segment-counter-scan failure. Compiled only
# with this feature AND inert unless WEIR_TEST_HOOKS=1 is set at runtime; the
# release workflow and the Dockerfile build `--features tls` only. CI's
# `--all-features` test line runs the `#[cfg(feature = "test-hooks")]` tests.
test-hooks = []
```

`src/test_hooks.rs`, replace PR 3's body (everything above the new test module) with:

```rust
//! Fault hooks for the system tests (Foundations §16).
//!
//! Two layers of opt-in: the `test-hooks` Cargo feature (absent from every
//! release build) and the `WEIR_TEST_HOOKS=1` environment variable. With the
//! feature off, every function here is an inlined no-op, so production code
//! calls them unconditionally and pays nothing.

use std::time::Duration;

/// `WEIR_TEST_HOOKS` value that arms the hooks.
const ARMED: &str = "1";

/// Pure parser behind [`startup_delay`]; separate so the tests need no
/// process-global environment mutation.
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn parse_delay(armed: Option<&str>, ms: Option<&str>) -> Option<Duration> {
    if armed != Some(ARMED) {
        return None;
    }
    ms?.trim().parse::<u64>().ok().map(Duration::from_millis)
}

/// Pure parser behind [`fail_counter_scan`].
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn parse_shard_list(armed: Option<&str>, list: Option<&str>, shard_id: u16) -> bool {
    armed == Some(ARMED)
        && list.is_some_and(|l| {
            l.split(',')
                .filter_map(|s| s.trim().parse::<u16>().ok())
                .any(|s| s == shard_id)
        })
}

#[cfg(feature = "test-hooks")]
fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// True when the hooks are compiled in AND armed with `WEIR_TEST_HOOKS=1`.
#[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
pub(crate) fn enabled() -> bool {
    #[cfg(feature = "test-hooks")]
    {
        env("WEIR_TEST_HOOKS").as_deref() == Some(ARMED)
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        false
    }
}

/// Logs, once at startup, that this binary carries the hooks.
pub(crate) fn warn_if_compiled_in() {
    #[cfg(feature = "test-hooks")]
    tracing::warn!(
        armed = enabled(),
        "THIS BINARY CONTAINS TEST HOOKS (built with --features test-hooks); never run it \
         in production"
    );
}

/// `WEIR_TEST_STARTUP_DELAY_MS`: how long main sleeps at the start of the
/// `recovering` phase, so a test can observe it.
pub(crate) fn startup_delay() -> Option<Duration> {
    #[cfg(feature = "test-hooks")]
    {
        parse_delay(
            env("WEIR_TEST_HOOKS").as_deref(),
            env("WEIR_TEST_STARTUP_DELAY_MS").as_deref(),
        )
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        None
    }
}

/// `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN=1,3`: those shards' flushers fail their
/// segment-counter scan every attempt, so they end `Offline(CounterScanFailed)`
/// exactly as a persistent `read_dir` failure would.
pub(crate) fn fail_counter_scan(shard_id: u16) -> bool {
    #[cfg(feature = "test-hooks")]
    {
        parse_shard_list(
            env("WEIR_TEST_HOOKS").as_deref(),
            env("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN").as_deref(),
            shard_id,
        )
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = shard_id;
        false
    }
}
```

`main.rs`: drop the `#[cfg(feature = "test-hooks")]` from PR 3's `mod test_hooks;` (unconditional, so the DST build sees it too) and from PR 3's `test_hooks::warn_if_compiled_in();` call; move that call to directly after the `"weir starting"` `info!`.

`wab/mod.rs`, `flusher_thread`. Replace PR 3's two-step `let scan = …; #[cfg(feature = "test-hooks")] let scan = if … else { scan };` with:

```rust
            let scan = writer.scan_and_advance_counter().and_then(|()| {
                if crate::test_hooks::fail_counter_scan(shard_id) {
                    Err(io::Error::other("test hook: WEIR_TEST_FAIL_SHARD_COUNTER_SCAN"))
                } else {
                    Ok(())
                }
            });
            match scan {
```

The retry arms, the 3 attempts and the give-up return (`FlusherExit::CounterScanFailed`, from PR 1) are unchanged.

Mutation check for the extended guard (the guard `include_str!`s the real file, so a scratch copy proves nothing): temporarily edit `.github/workflows/release.yml`'s weir-server build line to `--features tls,test-hooks`, run `cargo test -p weir-server --test docs_drift the_test_hooks_system_tests_run_in_ci` and see it fail with `the release workflow must never build weir-server with the test hooks`, then `git checkout -- .github/workflows/release.yml`.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server --features test-hooks test_hooks:: -- --test-threads=1` passes.
  `cargo test -p weir-server --test system --all-features a_binary_with_test_hooks_announces_them routing_avoids_a_shard_whose_counter_scan_failed no_live_shard_refuses_connections_and_counts_them -- --test-threads=1` passes (one filter per invocation; PR 3's two routing tests still pass through the reworked hook).
  `cargo test -p weir-server --test docs_drift` passes.
  The clippy matrix (default, `--all-features`, `--no-default-features`) passes.

- [ ] **Commit.** `test(server): the test-hooks startup delay, compiled-out no-ops and a never-ships guard`

---

### Task 4.3: testkit: a slow recording sink, a non-blocking signal, a fallible scrape

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-testkit/src/recording_sink.rs` (`start_mode(hang: bool)` becomes `start_mode(Mode)`; `serve` takes a delay)
- Modify: `crates/weir-testkit/src/harness.rs` (add `WeirServer::signal` and `WeirServer::try_scrape_metrics` beside `scrape_metrics`, ~line 161)
- Test: `crates/weir-testkit/src/recording_sink.rs` (its existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: none new.
- Produces:
  - `RecordingSink::start_slow(per_request: std::time::Duration) -> RecordingSink`;
  - `WeirServer::signal(&self, sig: i32)` (sends, does not wait);
  - `WeirServer::try_scrape_metrics(&self) -> Option<String>` (the body, or `None` on any connect/read failure, with a 2 s I/O timeout).

- [ ] **Write the failing test.** `recording_sink.rs` already has a `#[cfg(test)] mod tests { use super::*; .. }` (with `records_post_bodies_over_one_keep_alive_connection` and `a_hanging_sink_never_answers_and_closes_on_drop`); a second `mod tests` would be `E0428`. Inside that existing module, add `use std::time::{Duration, Instant};` directly under its `use super::*;` (the module spells `std::time::Duration` out today, so nothing conflicts), and append after `a_hanging_sink_never_answers_and_closes_on_drop`:

```rust
    #[test]
    fn start_slow_delays_every_answer_and_still_records() {
        let sink = RecordingSink::start_slow(Duration::from_millis(200));
        let mut s = TcpStream::connect(sink.addr).unwrap();
        let t0 = Instant::now();
        s.write_all(b"POST /sink HTTP/1.1\r\ncontent-length: 3\r\n\r\nabc")
            .unwrap();
        let mut buf = [0u8; 64];
        let n = s.read(&mut buf).unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        assert!(
            t0.elapsed() >= Duration::from_millis(200),
            "the answer must wait the configured delay, took {:?}",
            t0.elapsed()
        );
        assert_eq!(sink.bodies(), vec![b"abc".to_vec()]);
    }
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-testkit start_slow`
  Expected: `error[E0599]: no function or associated item named \`start_slow\` found for struct \`RecordingSink\``.

- [ ] **Implement.** In `recording_sink.rs`:

```rust
/// How the fixture answers.
#[derive(Clone, Copy)]
enum Mode {
    /// Answer every request, after `delay`.
    Answer { delay: std::time::Duration },
    /// Accept and never answer (the black hole).
    Hang,
}

impl RecordingSink {
    pub fn start() -> RecordingSink {
        Self::start_mode(Mode::Answer { delay: std::time::Duration::ZERO })
    }

    pub fn start_hanging() -> RecordingSink {
        Self::start_mode(Mode::Hang)
    }

    /// A slow but healthy endpoint: every request (the `HEAD` probe
    /// included) is answered `200` after `per_request`, and POST bodies are
    /// recorded as with [`start`](Self::start). Models a sink whose commit
    /// latency, not its availability, is the bottleneck.
    pub fn start_slow(per_request: std::time::Duration) -> RecordingSink {
        Self::start_mode(Mode::Answer { delay: per_request })
    }
}
```

In `start_mode`, the per-connection closure becomes:

```rust
                        .spawn(move || match mode {
                            Mode::Hang => hold(conn, &s),
                            Mode::Answer { delay } => serve(conn, &b, delay),
                        });
```

`serve(conn, bodies, delay: std::time::Duration)`: immediately before the `writer.write_all(b"HTTP/1.1 200 OK...")` line, add:

```rust
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
```

Update the module doc's second paragraph to name `start_slow`.

In `harness.rs`, after `scrape_metrics`:

```rust
    /// Sends `sig` to the daemon and returns at once (unlike
    /// [`sigterm`](Self::sigterm), which waits for the exit). For tests that
    /// act on the daemon WHILE it shuts down: a second signal, a scrape
    /// during the drain budget, a lame-duck check.
    #[cfg(unix)]
    pub fn signal(&self, sig: i32) {
        let pid = self.child_pid();
        // SAFETY: kill(2) on our own child, which has not been reaped (the
        // handle still owns it).
        let rc = unsafe { libc::kill(pid as libc::pid_t, sig) };
        assert_eq!(
            rc,
            0,
            "kill -{sig} {pid}: {}",
            std::io::Error::last_os_error()
        );
    }

    /// Like [`scrape_metrics`](Self::scrape_metrics), but `None` instead of a
    /// panic when the endpoint is not (or no longer) reachable, with a 2 s
    /// I/O bound. For polling during startup and shutdown.
    pub fn try_scrape_metrics(&self) -> Option<String> {
        use std::io::{Read, Write};
        use std::net::{SocketAddr, TcpStream};
        let addr = SocketAddr::from(([127, 0, 0, 1], self.metrics_port));
        let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").ok()?;
        let mut response = String::new();
        s.read_to_string(&mut response).ok()?;
        let idx = response.find("\r\n\r\n")?;
        Some(response[idx + 4..].to_string())
    }
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-testkit` passes.
  `cargo test -p weir-server --test system --no-run` still compiles.
  `cargo clippy -p weir-testkit --all-targets -- -D warnings` passes.

- [ ] **Commit.** `test(testkit): a slow recording sink, a non-blocking signal and a fallible scrape`

---

### Task 4.4: `lifecycle.rs`: shutdown level, one-way escalation, phase publication

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Create: `crates/weir-server/src/lifecycle.rs`
- Modify: `crates/weir-server/src/main.rs` (add `mod lifecycle;`)
- Test: `crates/weir-server/src/lifecycle.rs`

*(Integrator reconciliation: the draft also edited PR 1's `LifecycleCell::enter` to make `ShuttingDown` terminal and published the phase gauge itself. PR 1 Task 1.11 already refuses every transition out of `ShuttingDown` (`enter` returns `false`) and PR 1 Task 1.18 publishes `weir_startup_phase` through `StatusBoard::enter_phase` / `StatusBoard::begin_shutdown`, so this task reuses those and does not touch `board.rs`.)*

**Interfaces:**
- Consumes:
  - PR 1: `StatusBoard::{enter_phase(&self, Phase, &Metrics) -> bool, begin_shutdown(&self, &Metrics) -> bool}`, `LifecycleCell::{phase, is_shutting_down, forced, mark_forced, ever_served}`, `weir_status::Phase`, `Metrics.startup_phase`, `StartupPhaseLabel`, `StartupPhaseValue` (tests only), `StatusBoard::for_test_with`.
  - PR 2: `DrainControl::{attached, begin_shutdown, request_stop, force_stop, deadline_passed, absorbing, stop_requested}`.
- Produces (all `pub(crate)` in `crate::lifecycle`):
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)] enum ShutdownLevel { Running, Graceful, Forced }`;
  - `struct Lifecycle`;
  - `Lifecycle::new(board: Arc<StatusBoard>, metrics: Arc<Metrics>, drain: DrainControl, flushers_shutting_down: Arc<AtomicBool>) -> Lifecycle`;
  - `Lifecycle::subscribe(&self) -> watch::Receiver<ShutdownLevel>`;
  - `Lifecycle::level(&self) -> ShutdownLevel`;
  - `Lifecycle::request_shutdown(&self, cause: &'static str) -> ShutdownLevel`;
  - `Lifecycle::publish_phase(&self, phase: Phase) -> Phase`;
  - `async fn Lifecycle::wait_shutdown(&self) -> ShutdownLevel`;
  - `async fn shutdown_requested(rx: &mut watch::Receiver<ShutdownLevel>) -> ShutdownLevel`;
  - `async fn forced(rx: &mut watch::Receiver<ShutdownLevel>)`.

  `Lifecycle::publish_phase` returns the phase actually in force (`board.life.phase()` after PR 1's `enter_phase`): once `ShuttingDown` is stored, nothing else can be. Main publishes `Recovering`, `Handoff` and `Binding` through it (PR 1's board block already enters `Initializing`); there is no `mark_serving`, because `Serving` has exactly one writer, `socket::serve` (README Q6).

- [ ] **Write the failing tests.** In `lifecycle.rs` (test module first):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{StartupPhaseLabel, StartupPhaseValue};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    struct Fx {
        lc: Arc<Lifecycle>,
        board: Arc<StatusBoard>,
        drain: DrainControl,
        flag: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
    }

    fn fx() -> Fx {
        let board = StatusBoard::for_test_with(1, 1);
        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        let drain = DrainControl::attached();
        let flag = Arc::new(AtomicBool::new(false));
        let lc = Arc::new(Lifecycle::new(
            Arc::clone(&board),
            Arc::clone(&metrics),
            drain.clone(),
            Arc::clone(&flag),
        ));
        Fx { lc, board, drain, flag, metrics }
    }

    fn gauge(m: &Metrics, v: StartupPhaseValue) -> f64 {
        m.startup_phase.get_or_create(&StartupPhaseLabel { phase: v }).get()
    }

    #[test]
    fn first_request_is_graceful_and_sets_every_t0_flag() {
        let f = fx();
        let rx = f.lc.subscribe();
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Graceful);
        assert_eq!(*rx.borrow(), ShutdownLevel::Graceful, "T0 (e): the watch");
        assert!(f.board.life.is_shutting_down(), "T0 (a)");
        assert_eq!(f.board.life.phase(), Phase::ShuttingDown, "T0 (b)");
        assert!(f.drain.absorbing(), "T0 (c): absorb mode");
        assert!(
            !f.drain.stop_requested(),
            "T0 must not stop the drain: its budget starts after the flusher joins"
        );
        assert!(f.flag.load(Ordering::Relaxed), "T0 (d): the flushers' flag");
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::shutting_down), 1.0);
        assert!(!f.board.life.forced());
    }

    #[test]
    fn second_request_forces_and_third_only_logs() {
        let f = fx();
        let rx = f.lc.subscribe();
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Graceful);
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Forced);
        assert!(f.drain.stop_requested(), "force_stop sets stop");
        assert!(f.drain.deadline_passed(), "force_stop sets the deadline to now");
        assert!(f.board.life.forced());
        assert_eq!(*rx.borrow(), ShutdownLevel::Forced);
        assert_eq!(f.lc.request_shutdown("SIGINT"), ShutdownLevel::Forced);
        assert_eq!(f.lc.level(), ShutdownLevel::Forced);
    }

    #[test]
    fn a_forced_deadline_survives_main_s_later_request_stop() {
        let f = fx();
        f.lc.request_shutdown("SIGTERM");
        f.lc.request_shutdown("SIGTERM");
        // Main's T2 runs after the joins, i.e. after the second signal here.
        f.drain.request_stop(Duration::from_secs(10));
        assert!(
            f.drain.deadline_passed(),
            "fetch_min: request_stop must not raise a forced deadline"
        );
    }

    #[test]
    fn the_level_is_set_even_with_no_receiver_alive() {
        let f = fx();
        // watch::Sender::send fails (and does not store) with no receivers;
        // request_shutdown must use send_replace.
        f.lc.request_shutdown("SIGTERM");
        assert_eq!(f.lc.level(), ShutdownLevel::Graceful);
        assert_eq!(*f.lc.subscribe().borrow(), ShutdownLevel::Graceful);
    }

    #[tokio::test]
    async fn a_receiver_subscribed_after_the_signal_still_sees_it() {
        let f = fx();
        f.lc.request_shutdown("SIGTERM");
        let mut rx = f.lc.subscribe();
        let level = tokio::time::timeout(Duration::from_millis(50), shutdown_requested(&mut rx))
            .await
            .expect("wait_for checks the current value first; a late subscriber must not hang");
        assert_eq!(level, ShutdownLevel::Graceful);
    }

    #[tokio::test]
    async fn forced_resolves_only_on_the_second_signal() {
        let f = fx();
        let mut rx = f.lc.subscribe();
        f.lc.request_shutdown("SIGTERM");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), forced(&mut rx))
                .await
                .is_err(),
            "Graceful is not Forced"
        );
        f.lc.request_shutdown("SIGTERM");
        tokio::time::timeout(Duration::from_millis(50), forced(&mut rx))
            .await
            .expect("Forced must resolve");
    }

    #[test]
    fn a_phase_published_after_shutdown_cannot_overwrite_it() {
        let f = fx();
        assert_eq!(f.lc.publish_phase(Phase::Recovering), Phase::Recovering);
        f.lc.request_shutdown("SIGTERM");
        assert_eq!(f.lc.publish_phase(Phase::Handoff), Phase::ShuttingDown);
        assert_eq!(f.board.life.phase(), Phase::ShuttingDown);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::handoff), 0.0);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::shutting_down), 1.0);
        assert_eq!(
            f.lc.publish_phase(Phase::Serving),
            Phase::ShuttingDown,
            "serving must be refused once shutting down"
        );
    }

    #[test]
    fn startup_phase_gauge_is_one_hot() {
        let f = fx();
        for p in [Phase::Initializing, Phase::Recovering, Phase::Handoff, Phase::Binding] {
            f.lc.publish_phase(p);
            let ones = [
                StartupPhaseValue::initializing,
                StartupPhaseValue::recovering,
                StartupPhaseValue::handoff,
                StartupPhaseValue::binding,
                StartupPhaseValue::serving,
                StartupPhaseValue::shutting_down,
            ]
            .into_iter()
            .filter(|&v| gauge(&f.metrics, v) == 1.0)
            .count();
            assert_eq!(ones, 1, "exactly one phase series is 1 after {p:?}");
        }
        assert_eq!(f.lc.publish_phase(Phase::Serving), Phase::Serving);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::serving), 1.0);
        assert!(f.board.life.ever_served(), "PR 1's enter(Serving) sets ever_served");
    }
}
```

(PR 1 Task 1.11 already pins "nothing leaves `ShuttingDown`" on `LifecycleCell` itself; `a_phase_published_after_shutdown_cannot_overwrite_it` above pins it through the Lifecycle.)

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server lifecycle:: -- --test-threads=1`
  Expected: `error[E0583]: file not found for module \`lifecycle\``, or once the file exists with only tests, `error[E0412]: cannot find type \`Lifecycle\` in this scope`.

- [ ] **Implement.** `src/lifecycle.rs` (full, above the tests):

```rust
//! Process lifecycle: the shutdown level and the signals that raise it
//! (Foundations §5.6). Not an actor: `request_shutdown` does its work inline
//! on whichever task calls it (the signal task), and everything else only
//! reads the level from a `watch` channel.
//!
//! The level only ever rises: Running -> Graceful (first SIGTERM/SIGINT) ->
//! Forced (second). A third signal only logs. SIGHUP is NOT handled here; it
//! stays on `reload::spawn_sighup_task`, the one reload path the safety floor
//! built and the admin `reload` verb reuses.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

use tokio::sync::watch;
use tracing::{info, warn};
use weir_status::Phase;

use crate::drain::control::DrainControl;
use crate::metrics::Metrics;
use crate::status::StatusBoard;

/// How far shutdown has gone. Ordered: a later level implies the earlier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ShutdownLevel {
    Running,
    /// First SIGTERM/SIGINT: lame duck, bounded drain (§7 T0).
    Graceful,
    /// Second signal: connection drains abort, the drain deadline is now
    /// (§7 "second signal"). The worker and flusher joins still run.
    Forced,
}

pub(crate) struct Lifecycle {
    level: watch::Sender<ShutdownLevel>,
    /// How many shutdown requests arrived; picks the escalation step.
    requests: AtomicU32,
    board: Arc<StatusBoard>,
    metrics: Arc<Metrics>,
    drain: DrainControl,
    /// Read by every flusher's hand-off loop (PR 3's `hand_off`), at most
    /// once per 100 ms of blocking.
    flushers_shutting_down: Arc<AtomicBool>,
}

impl Lifecycle {
    pub(crate) fn new(
        board: Arc<StatusBoard>,
        metrics: Arc<Metrics>,
        drain: DrainControl,
        flushers_shutting_down: Arc<AtomicBool>,
    ) -> Self {
        let (level, _rx) = watch::channel(ShutdownLevel::Running);
        Self {
            level,
            requests: AtomicU32::new(0),
            board,
            metrics,
            drain,
            flushers_shutting_down,
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ShutdownLevel> {
        self.level.subscribe()
    }

    pub(crate) fn level(&self) -> ShutdownLevel {
        *self.level.borrow()
    }

    /// One shutdown request (a signal). The first is Graceful, the second
    /// Forced, the rest only log. Returns the level now in force.
    pub(crate) fn request_shutdown(&self, cause: &'static str) -> ShutdownLevel {
        match self.requests.fetch_add(1, Ordering::AcqRel) {
            0 => {
                info!(signal = cause, "shutdown requested: lame duck, then a bounded drain");
                // §7 T0, in order: (a) readiness and (b) phase plus its gauge
                // (PR 1's StatusBoard::begin_shutdown), (c) drain absorb,
                // (d) flushers, (e) the broadcast that starts the lame duck.
                self.board.begin_shutdown(&self.metrics);
                self.drain.begin_shutdown();
                self.flushers_shutting_down.store(true, Ordering::Release);
                // send_replace, not send: send stores nothing when no
                // receiver is alive, and main may subscribe later.
                self.level.send_replace(ShutdownLevel::Graceful);
                ShutdownLevel::Graceful
            }
            1 => {
                warn!(
                    signal = cause,
                    "forced shutdown: aborting open connections and the drain now; the \
                     worker and flusher joins still run, so every acked record stays on disk"
                );
                self.drain.force_stop();
                self.board.life.mark_forced();
                self.level.send_replace(ShutdownLevel::Forced);
                ShutdownLevel::Forced
            }
            _ => {
                info!(signal = cause, "shutdown already forced; signal ignored");
                ShutdownLevel::Forced
            }
        }
    }

    /// Publishes a startup phase on the board and the one-hot
    /// `weir_startup_phase` gauge. Returns the phase actually in force:
    /// `ShuttingDown` once shutdown began, whatever was asked.
    pub(crate) fn publish_phase(&self, phase: Phase) -> Phase {
        // PR 1: moves the phase and publishes the one-hot gauge, or refuses
        // (and publishes nothing) once ShuttingDown is stored.
        self.board.enter_phase(phase, &self.metrics);
        self.board.life.phase()
    }

    /// Resolves once the level leaves `Running`.
    pub(crate) async fn wait_shutdown(&self) -> ShutdownLevel {
        let mut rx = self.subscribe();
        shutdown_requested(&mut rx).await
    }
}

/// Resolves with the level once it is past `Running`. Uses `wait_for`, which
/// checks the current value first, never `changed()`: a signal that lands
/// between a caller's "already shutting down?" check and its first poll is
/// never missed (§5.6). A dropped sender reads as Graceful, which matches the
/// `oneshot` it replaces ("fires or is dropped").
pub(crate) async fn shutdown_requested(rx: &mut watch::Receiver<ShutdownLevel>) -> ShutdownLevel {
    match rx.wait_for(|l| *l != ShutdownLevel::Running).await {
        Ok(l) => *l,
        Err(_) => ShutdownLevel::Graceful,
    }
}

/// Resolves once the level is `Forced`. A dropped sender never forces: it
/// pends, so a connection drain then ends by its own timeout.
pub(crate) async fn forced(rx: &mut watch::Receiver<ShutdownLevel>) {
    if rx.wait_for(|l| *l == ShutdownLevel::Forced).await.is_err() {
        std::future::pending::<()>().await;
    }
}

```

`main.rs`: add `mod lifecycle;` to the module list. It has no `main` caller yet; add `#[allow(dead_code)]` on the module declaration with a comment that Task 4.11 removes it (Task 4.10 calls part of the module, Task 4.11 the rest).

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server lifecycle:: -- --test-threads=1` passes (8 tests).
  `cargo test -p weir-server --bin weir-server status::board:: -- --test-threads=1` still passes (no change to `board.rs`).
  Clippy `--all-features` and `--no-default-features` pass.

- [ ] **Commit.** `feat(lifecycle): a one-way shutdown level, and a startup phase shutdown cannot overwrite`

---

### Task 4.5: The signal task (SIGTERM and SIGINT, every time, not once)

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/lifecycle.rs`
- Test: `crates/weir-server/src/lifecycle.rs`

**Interfaces:**
- Consumes: `tokio::signal::unix::{signal, SignalKind}` (tokio feature `signal`, already enabled).
- Produces: `Lifecycle::spawn_signal_task(self: &Arc<Self>) -> std::io::Result<()>`, which must be called inside the runtime. It replaces the one-shot task at main.rs:1634-1657 (3adc5f5). A second signal is no longer swallowed.

- [ ] **Write the failing test.** Append to `lifecycle::tests`:

```rust
    /// Two SIGTERMs to this very process: Graceful, then Forced. The handler
    /// is installed (synchronously) before the first kill, so the test binary
    /// is never terminated by the default disposition.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_sigterm_escalates_graceful_then_forced() {
        let f = fx();
        f.lc.spawn_signal_task().expect("install the SIGTERM/SIGINT handlers");
        let mut rx = f.lc.subscribe();
        // SAFETY: kill(2) on our own pid with a handler installed above.
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
        let level = tokio::time::timeout(Duration::from_secs(1), shutdown_requested(&mut rx))
            .await
            .expect("the first SIGTERM must reach the lifecycle");
        assert_eq!(level, ShutdownLevel::Graceful);
        // tokio coalesces signals that arrive before the task is polled.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // SAFETY: as above.
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
        tokio::time::timeout(Duration::from_secs(1), forced(&mut rx))
            .await
            .expect("a second SIGTERM must force, not be swallowed");
    }
```

(SIGINT is not raised in-process; the INT arm shares the `select!`. Note that installing the handlers makes this test binary ignore SIGTERM/SIGINT for the rest of the run (tokio never uninstalls them); CI kills hung jobs with SIGKILL, so that is acceptable.)

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server lifecycle::tests::repeated_sigterm -- --test-threads=1`
  Expected: `error[E0599]: no method named \`spawn_signal_task\` found for struct \`Arc<Lifecycle>\``.

- [ ] **Implement.** Add to `impl Lifecycle`:

```rust
    /// Installs the SIGTERM and SIGINT handlers (synchronously, so they are in
    /// place when this returns) and spawns the task that turns EVERY delivery
    /// into a `request_shutdown`. Replaces the one-shot task that took the first
    /// signal and then exited, after which a second SIGTERM was swallowed
    /// (main.rs:1634-1657 before this change). Must be called inside the
    /// runtime. tokio coalesces deliveries that arrive before the task is
    /// polled, so two signals sent microseconds apart may count as one.
    #[cfg(unix)]
    pub(crate) fn spawn_signal_task(self: &Arc<Self>) -> std::io::Result<()> {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())?;
        let mut int = signal(SignalKind::interrupt())?;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let cause = tokio::select! {
                    s = term.recv() => match s { Some(()) => "SIGTERM", None => break },
                    s = int.recv() => match s { Some(()) => "SIGINT", None => break },
                };
                me.request_shutdown(cause);
            }
        });
        Ok(())
    }

    /// Non-Unix builds have no producer listener; Ctrl-C still escalates.
    #[cfg(not(unix))]
    pub(crate) fn spawn_signal_task(self: &Arc<Self>) -> std::io::Result<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            while tokio::signal::ctrl_c().await.is_ok() {
                me.request_shutdown("Ctrl-C");
            }
        });
        Ok(())
    }
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server lifecycle:: -- --test-threads=1` passes.

- [ ] **Commit.** `feat(lifecycle): every SIGTERM and SIGINT escalates; the second one forces`

---

### Task 4.6: One bounded connection drain for both listeners

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs` (new `drain_connections` and `ConnDrain` next to `drain_join_set`, ~line 307)
- Test: `crates/weir-server/src/socket/mod.rs` (`mod tests`)

**Interfaces:**
- Consumes: `lifecycle::{ShutdownLevel, forced}` (Task 4.4); `Metrics.connections_aborted_at_shutdown: Counter` (metrics/mod.rs:334).
- Produces:
  - `#[derive(Debug, PartialEq, Eq)] pub(crate) enum ConnDrain { Clean, TimedOut { aborted: usize }, Forced { aborted: usize } }`;
  - `pub(crate) async fn drain_connections(join_set: &mut JoinSet<()>, timeout: Duration, level: &mut watch::Receiver<ShutdownLevel>, metrics: &Metrics, who: &'static str) -> ConnDrain`.

  This is the spec §5.7 `select!{drain, sleep(shutdown_timeout), wait_for(Forced)}`. It awaits the aborted tasks, so every handler's queue sender is dropped when it returns.

- [ ] **Write the failing tests.** Append to `socket::tests`:

```rust
    // ── Bounded connection drain (Foundations §5.7) ───────────────────────────

    use crate::lifecycle::ShutdownLevel;

    #[tokio::test]
    async fn drain_connections_aborts_at_forced_without_waiting_out_the_timeout() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.send_replace(ShutdownLevel::Forced);
            // Keep the sender alive past the abort.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let t0 = Instant::now();
        let out = drain_connections(&mut js, Duration::from_secs(30), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Forced { aborted: 1 });
        assert!(t0.elapsed() < Duration::from_millis(500), "took {:?}", t0.elapsed());
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        assert!(js.is_empty(), "aborted handlers must be reaped before returning");
    }

    #[tokio::test]
    async fn drain_connections_aborts_at_the_timeout() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        let out =
            drain_connections(&mut js, Duration::from_millis(100), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::TimedOut { aborted: 1 });
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
    }

    #[tokio::test]
    async fn drain_connections_is_clean_when_handlers_finish() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(async { tokio::time::sleep(Duration::from_millis(20)).await });
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Graceful);
        let out = drain_connections(&mut js, Duration::from_secs(5), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Clean);
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 0);
    }

    #[tokio::test]
    async fn drain_connections_aborts_at_once_when_already_forced() {
        let metrics = test_metrics();
        let mut js: JoinSet<()> = JoinSet::new();
        js.spawn(std::future::pending::<()>());
        let (_tx, mut rx) = tokio::sync::watch::channel(ShutdownLevel::Forced);
        let t0 = Instant::now();
        let out = drain_connections(&mut js, Duration::from_secs(30), &mut rx, &metrics, "test").await;
        assert_eq!(out, ConnDrain::Forced { aborted: 1 });
        assert!(t0.elapsed() < Duration::from_millis(100));
    }
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server socket::tests::drain_connections -- --test-threads=1`
  Expected: `error[E0425]: cannot find function \`drain_connections\` in this scope`.

- [ ] **Implement.** In `socket/mod.rs`, beside `drain_join_set`:

```rust
/// How a listener's connection drain ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConnDrain {
    Clean,
    TimedOut { aborted: usize },
    Forced { aborted: usize },
}

/// Waits for in-flight connections after the accept loop broke: up to
/// `timeout` (`shutdown_timeout_secs`), or until the level becomes `Forced`,
/// whichever comes first; then aborts what is left and counts it in
/// `weir_connections_aborted_at_shutdown`. Shared by the Unix and TCP
/// listeners (Foundations §5.7).
///
/// The aborted tasks are awaited before returning, so every handler's
/// `QueueSender` clone is dropped by then. Only a push already parked in
/// `spawn_blocking` can outlive that, for at most `QUEUE_PUSH_TIMEOUT`, which
/// is why main waits on the queue's live-sender count (Task 4.8) rather than
/// asserting it.
pub(crate) async fn drain_connections(
    join_set: &mut JoinSet<()>,
    timeout: Duration,
    level: &mut tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>,
    metrics: &Metrics,
    who: &'static str,
) -> ConnDrain {
    enum Why {
        Done,
        Timeout,
        Forced,
    }
    // The select's futures borrow `join_set`; decide first, act after they drop.
    let why = tokio::select! {
        biased;
        _ = crate::lifecycle::forced(level) => Why::Forced,
        r = time::timeout(timeout, drain_join_set(join_set)) => {
            if r.is_ok() { Why::Done } else { Why::Timeout }
        }
    };
    let remaining = join_set.len();
    let out = match why {
        Why::Done => {
            info!("{who}: all connections drained cleanly");
            return ConnDrain::Clean;
        }
        Why::Timeout => {
            error!(
                remaining,
                timeout_secs = timeout.as_secs(),
                "{who}: shutdown timeout reached; aborting remaining connections — producers \
                 of those records may see no ack/nack response"
            );
            ConnDrain::TimedOut { aborted: remaining }
        }
        Why::Forced => {
            warn!(remaining, "{who}: forced shutdown; aborting remaining connections now");
            ConnDrain::Forced { aborted: remaining }
        }
    };
    metrics.connections_aborted_at_shutdown.inc_by(remaining as u64);
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
    out
}
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server socket::tests::drain_connections -- --test-threads=1` passes (4 tests).

- [ ] **Commit.** `feat(socket): a connection drain that a forced shutdown cuts short`

---

### Task 4.7: Listeners follow the shutdown level, and go lame-duck at once

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs`:
  - `serve` (PR 3 Task 3.10's accept loop): replace the `oneshot::Receiver<()>` parameter; rewrite the post-loop block (the old handler broadcast, timeout drain and late `remove_own_socket`);
  - the `#[cfg(test)] run` wrapper (PR 3): same parameter change;
  - the module docs on `serve`;
  - `mod tests`: every `oneshot::channel()` site that feeds `run` or `serve` (PR 1's and PR 3's new tests included), found with `grep -n 'oneshot::channel' crates/weir-server/src/socket/mod.rs`.
- Modify: `crates/weir-server/src/socket/mod.rs` top-level imports (`sync::{Semaphore, oneshot}` becomes `sync::Semaphore`: after this task nothing outside the tests uses `oneshot`)
- Modify: `crates/weir-server/src/socket/tcp.rs` (`run` signature: the shutdown parameter and a new last `board` parameter; the `Failed` mark on its one `Err` path; accept-loop `select!`; post-loop block; imports)
- Modify: `crates/weir-server/src/main.rs` (the `socket::serve` and `socket::tcp::run` call sites PR 3 Task 3.11 wrote, and the TCP `Closed` / `Failed` marks PR 3 left in main's spawn wrapper; minimal, Task 4.10 replaces them)
- Test: `crates/weir-server/src/socket/mod.rs` (new Unix lame-duck test), `crates/weir-server/src/socket/tcp.rs` (`mod tests`: new TCP lame-duck test). The TCP system test (`sigterm_closes_the_tcp_listener_at_once`) is Task 4.11's: its bounded exit needs the attached drain.

**Interfaces:**
- Consumes:
  - `lifecycle::{ShutdownLevel, shutdown_requested}` (Task 4.4) and `drain_connections` (Task 4.6);
  - `remove_own_socket(&Path, &BoundSocket)` (safety floor);
  - PR 1: `StatusBoard.listeners.{unix, tcp}.set(weir_status::ListenerState::{Closed, Failed})`, reached on the Unix side through PR 1's `SocketConfig.board` hook (PR 3's `bind` already marks the Unix listener `Bound`, `bind_all` the TCP one, and `serve` enters `Serving`; README Q6);
  - PR 3: `socket::{bind, serve, BoundUnix, route::ShardRouter}` and `socket::tcp::run(.., router: ShardRouter)`.
- Produces:
  - `socket::serve(config: SocketConfig, bound: BoundUnix, queue_tx: QueueSender<WorkUnit>, level_rx: watch::Receiver<ShutdownLevel>, metrics: Arc<Metrics>, sem: Arc<Semaphore>, router: ShardRouter) -> io::Result<()>` (marks the Unix listener `Closed` through `config.board` at the lame duck);
  - `#[cfg(test)] socket::run(config: SocketConfig, queue_tx: QueueSender<WorkUnit>, level_rx: watch::Receiver<ShutdownLevel>, metrics: Arc<Metrics>, sem: Arc<Semaphore>) -> io::Result<()>` (bind + serve with `ShardRouter::round_robin`);
  - `socket::tcp::run(config: TcpConfig, listener: TcpListener, tls: ReloadableServerConfig, queue_tx: QueueSender<WorkUnit>, sem: Arc<Semaphore>, level_rx: watch::Receiver<ShutdownLevel>, metrics: Arc<Metrics>, router: ShardRouter, board: Arc<StatusBoard>) -> io::Result<()>` (the single writer of the TCP listener's `Closed` mark at the lame duck and of `Failed` on an `Err` return; README Q6).

  PR 6's `admin::run` uses the same `level_rx` convention.

- [ ] **Write the failing tests.** Append to `socket::tests`:

```rust
    /// Lame duck (Foundations §5.7, §7): on Graceful the socket file is gone
    /// and new connects fail at once, BEFORE the connection drain finishes,
    /// and a later Forced cuts that drain short.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(unix)]
    async fn lame_duck_unlinks_before_the_connection_drain_and_forced_cuts_it_short() {
        let dir = umask_immune_dir("lame_duck");
        let path = dir.join("weir.sock");
        let board = crate::status::StatusBoard::for_test_with(1, 1);
        let mut cfg = default_config(path.clone());
        cfg.shutdown_timeout_secs = 30; // the drain is NOT what removes the socket
        cfg.board = Some(std::sync::Arc::clone(&board));
        let (level_tx, level_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        // Kept alive and never consumed: a push waits for an ack that never comes,
        // so one handler (and therefore the drain) stays busy.
        let held = queue_rx.get(0);
        let metrics = test_metrics();
        let sem = std::sync::Arc::new(Semaphore::new(cfg.max_connections));
        let bound = bind(&cfg).expect("bind");
        let handle = tokio::spawn(serve(
            cfg,
            bound,
            queue_tx,
            level_rx,
            std::sync::Arc::clone(&metrics),
            sem,
            route::ShardRouter::round_robin(1),
        ));

        let p = path.clone();
        let pusher = tokio::task::spawn_blocking(move || {
            let mut c = weir_client::WeirClient::connect(&p).expect("connect");
            c.push(b"in-flight", weir_core::Durability::Durable)
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while held.is_empty() {
            assert!(Instant::now() < deadline, "the push never reached the queue");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Graceful);
        let t0 = Instant::now();
        while path.exists() {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "lame duck: the socket must be unlinked within 200 ms"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            UnixStream::connect(&path).await.is_err(),
            "a new connect must fail fast once the lame duck began"
        );
        assert!(
            !handle.is_finished(),
            "the connection drain must still be waiting on the in-flight push"
        );
        // The lame duck marks the listener Closed right after the unlink and
        // before the connection drain.
        let t1 = Instant::now();
        while board.listeners.unix.get() != weir_status::ListenerState::Closed {
            assert!(t1.elapsed() < Duration::from_millis(200), "the listener was never marked Closed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Forced);
        time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("Forced must end the 30 s drain at once")
            .expect("task should not panic")
            .expect("run() should return Ok");
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        let _ = pusher.await; // the aborted connection fails the push with Io
        std::fs::remove_dir_all(&dir).ok();
    }
```

Then update the existing `run` and `serve` call sites in this test module (PR 1's `run_marks_the_unix_listener_bound_then_serving_then_closed` and `a_bind_failure_marks_the_unix_listener_failed`, PR 3's `bind_returns_a_bound_socket_that_serve_then_serves` and `serve_drops_and_counts_a_connection_when_no_shard_is_live`, and the pre-existing ones). Each `let (shutdown_tx, shutdown_rx) = oneshot::channel();` (or `oneshot::channel::<()>()`) becomes:

```rust
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownLevel::Running);
```

and each `shutdown_tx.send(()).unwrap();` / `.ok();` becomes `shutdown_tx.send_replace(ShutdownLevel::Graceful);`. Drop the now-unused `sync::oneshot` from the tests' `use tokio::{..}`.

Append to `mod tests` in `socket/tcp.rs` (it runs with `tls`, i.e. on the `--bins --all-features` line):

```rust
    /// Lame duck on the TCP side (Foundations §5.7, §7): on Graceful the
    /// listener is closed and marked `Closed` at once, BEFORE the connection
    /// drain, which is still waiting on an in-flight Durable push; Forced then
    /// cuts that 30 s drain short. Mirrors the Unix
    /// `lame_duck_unlinks_before_the_connection_drain_and_forced_cuts_it_short`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_lame_duck_closes_the_listener_before_the_connection_drain() {
        use crate::lifecycle::ShutdownLevel;

        let fx = weir_testkit::tls::TlsFixture::generate("tcp_lame_duck");
        let tls = ReloadableServerConfig::load(
            fx.server_cert_path.clone(),
            fx.server_key_path.clone(),
            fx.ca_cert_path.clone(),
        )
        .expect("load the fixture's TLS material");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let board = crate::status::StatusBoard::for_test_with(1, 1);
        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        let (queue_tx, queue_rx) = crate::queue::new::<WorkUnit>(1);
        // Kept alive and never consumed: the push waits for an ack that never
        // comes, so one handler (and therefore the drain) stays busy.
        let held = queue_rx.get(0);
        let config = TcpConfig {
            max_connections: 4,
            max_payload_bytes: 1 << 20,
            max_batch_records: 64,
            shard_count: 1,
            shutdown_timeout_secs: 30, // the drain is NOT what closes the listener
            connection_read_timeout_secs: 30,
            handshake_timeout_secs: 5,
            wab_max_bytes: 0,
            wab_bytes_now: Arc::new(AtomicU64::new(0)),
            wab_cap_rejecting: Arc::new(AtomicBool::new(false)),
            wab_ownership_lost: Arc::new(AtomicBool::new(false)),
        };
        let (level_tx, level_rx) = watch::channel(ShutdownLevel::Running);
        let handle = tokio::spawn(run(
            config,
            listener,
            tls,
            queue_tx,
            Arc::new(Semaphore::new(4)),
            level_rx,
            Arc::clone(&metrics),
            crate::socket::route::ShardRouter::round_robin(1),
            Arc::clone(&board),
        ));

        let (ca, cert, key) = (
            fx.ca_cert_path.clone(),
            fx.client_cert_path.clone(),
            fx.client_key_path.clone(),
        );
        let pusher = tokio::task::spawn_blocking(move || {
            let mut c = weir_client::WeirClient::connect_tls(
                addr,
                weir_client::ClientTlsConfig {
                    client_cert: &cert,
                    client_key: &key,
                    ca_cert: &ca,
                    server_name: "weir-server",
                    default_durability: None,
                },
            )
            .expect("mTLS connect");
            c.push(b"in-flight", weir_core::Durability::Durable)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while held.is_empty() {
            assert!(Instant::now() < deadline, "the push never reached the queue");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        level_tx.send_replace(ShutdownLevel::Graceful);
        let t0 = Instant::now();
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "lame duck: the TCP listener must refuse new connects within 200 ms"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while board.listeners.tcp.get() != weir_status::ListenerState::Closed {
            assert!(
                t0.elapsed() < Duration::from_millis(200),
                "the TCP listener was never marked Closed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !handle.is_finished(),
            "the connection drain must still be waiting on the in-flight push"
        );

        level_tx.send_replace(ShutdownLevel::Forced);
        time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("Forced must end the 30 s drain at once")
            .expect("task should not panic")
            .expect("run() should return Ok");
        assert_eq!(metrics.connections_aborted_at_shutdown.get(), 1);
        let _ = pusher.await; // the aborted connection fails the push
    }
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server socket::tests::lame_duck -- --test-threads=1`
  Expected: `error[E0308]: mismatched types: expected \`Receiver<()>\`, found \`Receiver<ShutdownLevel>\`` (oneshot vs watch).
  `cargo test -p weir-server --bin weir-server --all-features socket::tcp::tests::tcp_lame_duck -- --test-threads=1`
  Expected: `error[E0061]: this function takes 8 arguments but 9 arguments were supplied` (PR 3's `tcp::run` has no `board` and takes a `oneshot`). Behaviourally the old `run` would fail too: its listener is a by-value parameter dropped only when `run` returns, i.e. after the 30 s connection drain, so the 200 ms refusal assertion cannot hold.

- [ ] **Implement.** `socket/mod.rs`, `serve` (PR 3). Parameter `shutdown_rx: oneshot::Receiver<()>` becomes `mut level_rx: tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>`. Delete `let mut shutdown = std::pin::pin!(shutdown_rx);` and PR 3's `let listener = &bound.socket.listener;`. PR 3's `enter_phase(Serving)` at the start of `serve` stays (the Unix `Bound` mark is in `socket::bind`, README Q6). The accept-loop shutdown arm becomes:

```rust
            biased;
            _ = crate::lifecycle::shutdown_requested(&mut level_rx) => {
                info!("socket manager: shutdown requested, stopping accept loop");
                break;
            }
            res = bound.socket.listener.accept() => {
```

Replace everything after the loop (the old handler broadcast, the timeout drain, PR 3's late `remove_own_socket(&bound.path, &bound.socket)` and PR 3's final `Closed` mark) with:

```rust
    // Lame duck (Foundations §5.7, §7): stop being reachable BEFORE the
    // connection drain, so new connects fail fast instead of piling up in the
    // backlog for up to shutdown_timeout_secs. The unlink runs first, while
    // the listener (and on Linux its O_PATH pin) still holds the inode, so the
    // identity check in remove_own_socket has no window; then the listener
    // closes. A successor that replaced our socket keeps its file.
    #[cfg(unix)]
    remove_own_socket(&bound.path, &bound.socket);
    #[cfg(not(unix))]
    let _ = std::fs::remove_file(&bound.path);
    drop(bound);
    if let Some(b) = &config.board {
        b.listeners.unix.set(weir_status::ListenerState::Closed);
    }
    info!(
        path = %config.socket_path.display(),
        "socket manager: lame duck: socket unlinked and listener closed; draining connections"
    );

    // Handlers exit between frames (after acking a push they already read).
    let _ = handler_shutdown_tx.send(true);
    drain_connections(
        &mut join_set,
        Duration::from_secs(config.shutdown_timeout_secs),
        &mut level_rx,
        &metrics,
        "socket manager",
    )
    .await;
    Ok(())
```

Update the doc comment's `# Shutdown removal` paragraph to describe the lame duck: unlink and close on the first Graceful, a bounded drain, and an abort on Forced. The `#[cfg(test)] run` wrapper's `shutdown_rx: oneshot::Receiver<()>` parameter becomes `level_rx: tokio::sync::watch::Receiver<crate::lifecycle::ShutdownLevel>`, passed through to `serve`. In the module's top-level imports, `sync::{Semaphore, oneshot},` becomes `sync::Semaphore,` (otherwise `oneshot` is an unused import under `-D warnings`).

`socket/tcp.rs`, `run`. Parameter `shutdown_rx: oneshot::Receiver<()>` becomes `mut level_rx: watch::Receiver<crate::lifecycle::ShutdownLevel>`, and a new last parameter `board: std::sync::Arc<crate::status::StatusBoard>` follows PR 3's `router: ShardRouter`. Remove `oneshot` from the `tokio::sync` import and delete the `pin!`. Its one `Err` path (`listener.local_addr()?`, the first line) marks the listener `Failed` first, so `tcp::run` is the TCP listener's only post-bind writer (README Q6):

```rust
    // The only `?` in `run`: a later one must mark `Failed` the same way.
    let local_addr = listener.local_addr().inspect_err(|_| {
        board.listeners.tcp.set(weir_status::ListenerState::Failed);
    })?;
```

Accept arm:

```rust
            biased;
            _ = crate::lifecycle::shutdown_requested(&mut level_rx) => {
                info!("TCP manager: shutdown requested, stopping accept loop");
                break;
            }
```

After the loop:

```rust
    // Lame duck: close the TCP listener before the connection drain, so new
    // connects are refused at once (Foundations §7).
    drop(listener);
    board.listeners.tcp.set(weir_status::ListenerState::Closed);
    info!(addr = %local_addr, "TCP manager: lame duck: listener closed; draining connections");
    let _ = handler_shutdown_tx.send(true);
    super::drain_connections(
        &mut join_set,
        Duration::from_secs(config.shutdown_timeout_secs),
        &mut level_rx,
        &metrics,
        "TCP manager",
    )
    .await;
    Ok(())
```

Delete tcp.rs's now-unused `drain_join_set`. In main.rs, PR 3's `socket::serve` and `socket::tcp::run` call sites still pass `oneshot` receivers; so the tree builds before Task 4.10, change them minimally:

- `let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();` becomes `let (level_tx, level_rx) = tokio::sync::watch::channel(lifecycle::ShutdownLevel::Running);`;
- the TCP side gets `level_tx.subscribe()` in place of `tcp_shutdown_rx` (drop the `tcp_shutdown_tx`/`tcp_shutdown_rx` pair) and `Arc::clone(&board)` after `tcp_router`;
- in PR 3's TCP spawn wrapper, delete the `tcp_board.listeners.tcp.set(..Closed)` and `..set(..Failed)` marks (and `tcp_board`): `tcp::run` now writes both. The wrapper keeps only `if let Err(e) = socket::tcp::run(..).await { tracing::error!(error = %e, "TCP+mTLS listener exited with error"); }`;
- the signal task's `shutdown_tx.send(())` / `tx.send(())` become one `level_tx.send_replace(lifecycle::ShutdownLevel::Graceful);`.

Task 4.10 replaces all of this with the Lifecycle.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server socket:: -- --test-threads=1` passes, and so does the same command with `--all-features`. The pre-existing `idle_connection_exits_promptly_on_shutdown`, `full_run_start_then_shutdown_removes_its_own_socket`, `run_exits_cleanly_after_shutdown_signal` and `socket_bind_sets_mode_0600` pass with only their channel lines changed.
  `cargo test -p weir-server --bin weir-server --all-features socket::tcp:: -- --test-threads=1` passes, including `tcp_lame_duck_closes_the_listener_before_the_connection_drain`.
  `cargo test -p weir-server --test tls_listener --all-features -- --test-threads=1` passes (no new test there yet).
  `cargo test -p weir-server --test system --all-features shutdown_leaves_a_successor_socket_alone -- --test-threads=1` passes: the earlier unlink is still identity-checked.
  The clippy matrix passes (an unused `oneshot` import fails it).

- [ ] **Commit.** `feat(socket): lame duck: unlink the socket and close the TCP listener at the first signal`

---

### Task 4.8: The work queue can say whether any sender is still alive

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/queue.rs`:
  - `QueueSender` (line 16), `QueueReceiver` (line 22);
  - `new` (line 39), `Clone` (line 120);
  - the test-only `new_with_capacity` (~line 151).
- Test: `crates/weir-server/src/queue.rs`

**Interfaces:**
- Consumes: none.
- Produces: `QueueReceiver::<T>::live_senders(&self) -> usize`. This is the number of `QueueSender` clones alive. It adds nothing to the push path, because the count is an `Arc` strong count that changes only on clone and drop.

- [ ] **Write the failing test.** Append to `queue::tests`:

```rust
    #[test]
    fn live_senders_counts_clones_and_reaches_zero() {
        let (tx, rx) = new::<u32>(2);
        assert_eq!(rx.live_senders(), 1);
        let tx2 = tx.clone();
        let tx3 = tx2.clone();
        assert_eq!(rx.live_senders(), 3);
        drop(tx);
        drop(tx3);
        assert_eq!(rx.live_senders(), 1);
        drop(tx2);
        assert_eq!(
            rx.live_senders(),
            0,
            "0 is what main waits for before joining the workers (Foundations §5.7)"
        );
    }
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server queue::tests::live_senders -- --test-threads=1`
  Expected: `error[E0599]: no method named \`live_senders\` found for struct \`QueueReceiver\``.

- [ ] **Implement.**

```rust
pub struct QueueSender<T> {
    txs: Vec<Sender<T>>,
    /// One strong count per live sender; the receiver holds the `Weak`.
    /// Touched only on clone/drop, never per push.
    alive: std::sync::Arc<()>,
}

pub struct QueueReceiver<T> {
    rxs: Vec<Receiver<T>>,
    alive: std::sync::Weak<()>,
}
```

In `new` (and identically in the test-only `new_with_capacity`):

```rust
    let alive = std::sync::Arc::new(());
    let weak = std::sync::Arc::downgrade(&alive);
    (QueueSender { txs, alive }, QueueReceiver { rxs, alive: weak })
```

`Clone`:

```rust
        QueueSender {
            txs: self.txs.clone(),
            alive: std::sync::Arc::clone(&self.alive),
        }
```

`impl<T> QueueReceiver<T>`:

```rust
    /// How many `QueueSender` clones are alive. Main waits for 0 before
    /// joining the workers: a sender that outlives ingest keeps every worker
    /// in `recv` and the join never returns (the deadlock main.rs documents
    /// above its pipeline joins).
    #[cfg_attr(not(test), allow(dead_code))] // read from PR 4 Task 4.11 (T1); remove there
    pub fn live_senders(&self) -> usize {
        self.alive.strong_count()
    }
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server queue:: -- --test-threads=1` passes.
  `cargo clippy -p weir-server --all-targets --all-features -- -D warnings` passes (the allowance covers the non-test build until Task 4.11 calls it).

- [ ] **Commit.** `feat(queue): count live senders so shutdown can prove ingest has let go`

---

### Task 4.9: The bounded drain wait and the no-I/O exit summary

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/lifecycle.rs`
- Modify: `crates/weir-server/src/main.rs` (delete PR 3's `SHARD_SETTLE_TIMEOUT`, `wait_until` and its test `wait_until_returns_true_as_soon_as_done_and_false_at_the_deadline`, which move into `lifecycle.rs`; PR 3's one call site becomes `lifecycle::wait_until(lifecycle::SETTLE_WAIT, Duration::from_millis(10), || board.shards.all_settled())`)
- Test: `crates/weir-server/src/lifecycle.rs`

*(Integrator reconciliation: PR 3 Task 3.11 added `main.rs`'s `wait_until(timeout, poll, done: impl FnMut() -> bool)` and `SHARD_SETTLE_TIMEOUT` (10 s). This task moves them into `lifecycle.rs` under the names below instead of adding a second copy; the `FnMut` bound is kept, since `Fn` closures satisfy it too.)*

**Interfaces:**
- Consumes: PR 2's `DrainControl::{request_stop, force_stop, time_left}` and `DrainExit { abandoned: Option<(String, u64)>, pending_left, backlog_left, .. }`; PR 3's `main.rs` `wait_until` and `SHARD_SETTLE_TIMEOUT` (moved).
- Produces:
  - `pub(crate) const DRAIN_EXIT_GRACE: Duration = 2 s`, `DRAIN_JOIN_POLL: Duration = 50 ms`, `SETTLE_WAIT: Duration = 10 s`, `QUEUE_SENDERS_GRACE: Duration = 6 s`;
  - `pub(crate) fn wait_until(max: Duration, poll: Duration, done: impl FnMut() -> bool) -> bool` (PR 3's function, moved; `SETTLE_WAIT` replaces PR 3's `SHARD_SETTLE_TIMEOUT`);
  - `#[derive(Debug)] pub(crate) enum DrainWait<T> { Joined(Option<T>), Orphaned }`;
  - `pub(crate) fn wait_for_drain<T>(handle: std::thread::JoinHandle<T>, ctl: &DrainControl, grace: Duration, poll: Duration) -> DrainWait<T>`;
  - `pub(crate) struct OnDisk { pub bytes: u64, pub as_of_unix_ms: u64 }`;
  - `pub(crate) struct ShutdownFacts { pub drain_joined: bool, pub pending_left: usize, pub backlog_left: usize, pub abandoned: Option<(String, u64)>, pub not_handed_off: u64, pub on_disk: Option<OnDisk> }`, with `ShutdownFacts::from_exit(Option<&DrainExit>, u64, Option<OnDisk>) -> Self`, `ShutdownFacts::orphaned(Option<(String, u64)>, u64, Option<OnDisk>) -> Self` and `ShutdownFacts::left_for_replay(&self) -> u64`;
  - `pub(crate) fn shutdown_summary(f: &ShutdownFacts) -> String`.

- [ ] **Write the failing tests.** Append to `lifecycle::tests`:

```rust
    use std::time::Instant;

    #[test]
    fn wait_for_drain_returns_as_soon_as_the_drain_ends() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_secs(5));
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            7u32
        });
        let t0 = Instant::now();
        match wait_for_drain(h, &ctl, Duration::from_secs(2), Duration::from_millis(10)) {
            DrainWait::Joined(Some(7)) => {}
            other => panic!("expected Joined(Some(7)), got {other:?}"),
        }
        assert!(t0.elapsed() < Duration::from_millis(400));
    }

    #[test]
    fn wait_for_drain_gives_up_one_grace_after_the_deadline() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_millis(200));
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(10)); // a sink call that never yields
            0u32
        });
        let t0 = Instant::now();
        let w = wait_for_drain(h, &ctl, Duration::from_millis(300), Duration::from_millis(10));
        let took = t0.elapsed();
        assert!(matches!(w, DrainWait::Orphaned), "{w:?}");
        assert!(
            took >= Duration::from_millis(450) && took < Duration::from_millis(900),
            "deadline (200 ms) + grace (300 ms), took {took:?}"
        );
    }

    #[test]
    fn a_force_stop_during_the_wait_shrinks_it() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_secs(10));
        let c2 = ctl.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            c2.force_stop(); // the second SIGTERM, during T2
        });
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(10));
            0u32
        });
        let t0 = Instant::now();
        let w = wait_for_drain(h, &ctl, Duration::from_millis(300), Duration::from_millis(10));
        assert!(matches!(w, DrainWait::Orphaned));
        assert!(
            t0.elapsed() < Duration::from_millis(700),
            "force_stop must cut the 10 s wait to ~grace, took {:?}",
            t0.elapsed()
        );
    }

    #[test]
    fn wait_until_reports_success_and_timeout() {
        let t0 = Instant::now();
        assert!(wait_until(Duration::from_secs(1), Duration::from_millis(5), || {
            t0.elapsed() > Duration::from_millis(30)
        }));
        assert!(!wait_until(Duration::from_millis(50), Duration::from_millis(5), || false));
    }

    /// PR 3's test, moved with the function from main.rs.
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

    #[test]
    fn summary_names_what_is_left_and_the_duplicate_window() {
        let f = ShutdownFacts {
            drain_joined: true,
            pending_left: 2,
            backlog_left: 3,
            abandoned: Some(("shard_00/seg_00000007".to_string(), 41)),
            not_handed_off: 1,
            on_disk: Some(OnDisk { bytes: 1_048_576, as_of_unix_ms: 1_700_000_000_000 }),
        };
        assert_eq!(f.left_for_replay(), 7, "2 pending + 3 backlog + 1 not handed off + 1 abandoned");
        let s = shutdown_summary(&f);
        for needle in [
            "7 segment(s)",
            "left sealed+unconfirmed for replay",
            "~1048576 bytes",
            "as of 1700000000000",
            "abandoned shard_00/seg_00000007 after 41 records",
            "re-delivered on replay",
            "dead-lettered again",
            "1 segment(s) were not handed off",
        ] {
            assert!(s.contains(needle), "missing {needle:?} in: {s}");
        }
    }

    #[test]
    fn a_complete_drain_says_nothing_is_left() {
        let f = ShutdownFacts {
            drain_joined: true,
            pending_left: 0,
            backlog_left: 0,
            abandoned: None,
            not_handed_off: 0,
            on_disk: None,
        };
        assert_eq!(f.left_for_replay(), 0);
        assert_eq!(
            shutdown_summary(&f),
            "shutdown: every handed-off segment was delivered; nothing is left for replay"
        );
    }
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --bin weir-server lifecycle:: -- --test-threads=1`
  Expected: `error[E0425]: cannot find function \`wait_for_drain\` in this scope`, plus the same for `wait_until`, `ShutdownFacts` and `shutdown_summary`.

- [ ] **Implement.** Append to `lifecycle.rs` (above the tests):

```rust
use std::time::{Duration, Instant};

use crate::drain::DrainExit;

/// How long main waits for the drain thread past its deadline before taking
/// the orphan exit (§7 T2).
pub(crate) const DRAIN_EXIT_GRACE: Duration = Duration::from_secs(2);
/// `is_finished` poll cadence during that wait.
pub(crate) const DRAIN_JOIN_POLL: Duration = Duration::from_millis(50);
/// Startup step 10: how long to wait for every shard to leave `Starting`.
pub(crate) const SETTLE_WAIT: Duration = Duration::from_secs(10);
/// T1: how long a work-queue sender may outlive ingest. A connection task
/// aborted mid-push can hold one inside `spawn_blocking` for up to
/// `QUEUE_PUSH_TIMEOUT` (5 s, connection.rs:31).
pub(crate) const QUEUE_SENDERS_GRACE: Duration = Duration::from_secs(6);

/// Polls `done` every `poll` until it holds or `max` passes. Blocking; for
/// main's thread only. (Moved from PR 3's main.rs.)
pub(crate) fn wait_until(max: Duration, poll: Duration, mut done: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + max;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(poll);
    }
}

#[derive(Debug)]
pub(crate) enum DrainWait<T> {
    /// The drain thread ended; `None` if it panicked past its supervisor.
    Joined(Option<T>),
    /// Still running one grace past its deadline: a sink call that never
    /// yields. The handle is dropped (the thread is detached) and main takes
    /// the orphan exit.
    Orphaned,
}

/// Waits for the drain thread: until it ends, or until `grace` after the
/// DrainControl deadline. The deadline is re-read on every pass, because a
/// second signal lowers it (`force_stop`'s `fetch_min`) while main is already
/// waiting, and the wait must shrink with it (§7 "second signal").
pub(crate) fn wait_for_drain<T>(
    handle: std::thread::JoinHandle<T>,
    ctl: &DrainControl,
    grace: Duration,
    poll: Duration,
) -> DrainWait<T> {
    let mut deadline_seen: Option<Instant> = None;
    loop {
        if handle.is_finished() {
            return DrainWait::Joined(handle.join().ok());
        }
        // None means no stop was requested, which main never does here; treat
        // it as an expired deadline so the wait stays bounded regardless.
        let left = ctl.time_left().unwrap_or(Duration::ZERO);
        if left.is_zero() {
            let at = *deadline_seen.get_or_insert_with(Instant::now);
            if at.elapsed() >= grace {
                return DrainWait::Orphaned;
            }
        }
        // Inside the grace window poll at `poll`; before the deadline, never
        // oversleep it.
        std::thread::sleep(if left.is_zero() { poll } else { poll.min(left) });
    }
}

/// When the last WAB size scan ran, and what it found.
pub(crate) struct OnDisk {
    pub bytes: u64,
    pub as_of_unix_ms: u64,
}

/// Everything the T3 exit line reports, gathered WITHOUT new I/O (§7 T3): no
/// `scan_unconfirmed_sealed`, which can quarantine through `check_confirmed`.
pub(crate) struct ShutdownFacts {
    pub drain_joined: bool,
    pub pending_left: usize,
    pub backlog_left: usize,
    /// The segment abandoned at the deadline and its committed record count.
    pub abandoned: Option<(String, u64)>,
    /// `weir_wab_segments_not_handed_off_total` at exit.
    pub not_handed_off: u64,
    pub on_disk: Option<OnDisk>,
}

impl ShutdownFacts {
    pub(crate) fn from_exit(exit: Option<&DrainExit>, not_handed_off: u64, on_disk: Option<OnDisk>) -> Self {
        Self {
            drain_joined: true,
            pending_left: exit.map_or(0, |e| e.pending_left),
            backlog_left: exit.map_or(0, |e| e.backlog_left),
            abandoned: exit
                .and_then(|e| e.abandoned.as_ref())
                .map(|(seg, n)| (seg.to_string(), *n)),
            not_handed_off,
            on_disk,
        }
    }

    pub(crate) fn orphaned(in_flight: Option<(String, u64)>, not_handed_off: u64, on_disk: Option<OnDisk>) -> Self {
        Self {
            drain_joined: false,
            pending_left: 0,
            backlog_left: 0,
            abandoned: in_flight,
            not_handed_off,
            on_disk,
        }
    }

    pub(crate) fn left_for_replay(&self) -> u64 {
        (self.pending_left + self.backlog_left) as u64
            + self.not_handed_off
            + u64::from(self.abandoned.is_some())
    }
}

/// The one exit line (§7 T3). States the duplicate window; never a count of
/// dead-lettered records (DrainExit does not carry one).
pub(crate) fn shutdown_summary(f: &ShutdownFacts) -> String {
    if f.left_for_replay() == 0 {
        return "shutdown: every handed-off segment was delivered; nothing is left for replay"
            .to_string();
    }
    let mut s = format!(
        "shutdown: {} segment(s) left sealed+unconfirmed for replay",
        f.left_for_replay()
    );
    if let Some(d) = &f.on_disk {
        s.push_str(&format!(" (WAB ~{} bytes, as of {})", d.bytes, d.as_of_unix_ms));
    }
    if let Some((seg, n)) = &f.abandoned {
        s.push_str(&format!(
            "; abandoned {seg} after {n} records: those records will be re-delivered on replay, \
             and any of them the sink rejected permanently will be dead-lettered again"
        ));
    }
    if f.not_handed_off > 0 {
        s.push_str(&format!(
            "; {} segment(s) were not handed off before the 1 s budget and are durable on disk",
            f.not_handed_off
        ));
    }
    s
}
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --bin weir-server lifecycle:: -- --test-threads=1` passes (16 tests: 8 from Task 4.4, 1 from Task 4.5, 7 here).
  `cargo test -p weir-server --bin weir-server -- --test-threads=1` still builds and passes with PR 3's main.rs copy deleted.

- [ ] **Commit.** `feat(lifecycle): a drain wait bounded by the deadline, and an exit summary that does no I/O`

---

### Task 4.10: Observable startup: listeners before recovery, replay handed to the drain

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/main.rs`:
  - `fn main`, from `// ── Queue` (line 1037 at 3adc5f5; PRs 1-3 move it) to the end of the function, as PRs 1-3 left it. Everything above `// ── Queue` is kept exactly as the safety floor and PR 1 left it (see Implement);
  - keep `#[allow(dead_code)]` on `mod lifecycle;` (Task 4.9's shutdown items are first used in Task 4.11, which removes it);
  - add the private helpers `discard_listeners` and `log_listener_exit`.
- Modify: `crates/weir-server/src/wab/mod.rs` (delete `replay_unconfirmed`, which PR 3 Task 3.3 kept as a thin wrapper, with all five of its test callers switched to `scan_replay` or deleted; see reconciliation 9)
- Modify: `crates/weir-server/tests/system.rs`:
  - delete `sighup_during_startup_replay_is_ignored` (~3283-3355);
  - rename and amend `sighup_during_startup_recovery_is_ignored` (~3357);
  - replace `lock_stolen_during_startup_replay_fails_closed_before_serving` (~3063) with a recovery-phase variant;
  - add three tests.
- Test: `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes:
  - everything in the "consumed" table above, in particular PR 1's `StatusBoard::new` / `StaticInfo::capture` / `Metrics::{init_topology, publish_build_info, publish_config}` / `status::poll::{startup_cap_gate, spawn_queue_depth_task, WabPoll, PollStatus, DrainLiveness, compute_wab_bytes_on_disk}`, PR 2's `drain::{DrainRuntime, DrainControl}`, PR 3's `wab::{recover, scan_replay, spawn_flushers, handoff::HandoffConfig, DRAIN_CHANNEL_CAPACITY}`, `socket::{bind_all, BoundListeners, serve, route::ShardRouter}` and `sink::select::{build_drain_spawner, DrainSpawnArgs}`;
  - Tasks 4.2 (`test_hooks::startup_delay`), 4.4 and 4.5 (`Lifecycle`), 4.7 (the `serve` / `tcp::run` signatures) and 4.9 (`wait_until`, `SETTLE_WAIT`).
- Produces:
  - the §6 startup sequence: phases `initializing → recovering → handoff → binding → serving`, each published on the board and in `weir_startup_phase`;
  - the step 7 admin slot and the `reload_tx` clone PR 6 needs;
  - `main.rs` private helpers: `fn discard_listeners(listeners: socket::BoundListeners, board: &status::StatusBoard)` (the pre-serving skip path) and `fn log_listener_exit(r: Result<std::io::Result<()>, tokio::task::JoinError>, which: &str)`. The producer `SocketConfig` is built inline at the bind site, so main keeps exactly two `wab_ownership_lost: Arc::clone(&wab_ownership_lost),` fields (Unix and TCP), which the safety floor's source check `ownership_flag_is_one_arc_shared_by_the_integrity_thread_and_the_listeners` (main.rs:1848) requires.

  *(Integrator reconciliation: the draft added its own `ProducerListeners` / `bind_producer_listeners`; PR 3 Task 3.11's `socket::bind_all` / `BoundListeners` already bind TCP then Unix before any accept loop, so this task uses them and keeps only the pre-serving `discard_listeners`.)*

  The drain is still wired **detached** in this task, with `initial_backlog: replay`. Task 4.11 flips it.

- [ ] **Write the failing tests.** In `tests/system.rs`, add near the SIGHUP section:

```rust
/// Spec §16 system test 2: the metrics listener answers DURING recovery,
/// before the producer socket exists, and reports the phase.
#[cfg(feature = "test-hooks")]
#[test]
fn metrics_answer_during_recovery_before_the_socket_binds() {
    let mut srv = weir_server!("startup_recovering")
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_STARTUP_DELAY_MS", "3000")
        .spawn();
    let t0 = Instant::now();
    let body = loop {
        if let Some(b) = srv.try_scrape_metrics() {
            break b;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "/metrics must answer within 2 s of start, during recovery\nlog:\n{}",
            read_log(&srv)
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        parse_metric_f64(&body, "weir_startup_phase{phase=\"recovering\"} "),
        1.0,
        "{body}"
    );
    assert!(
        !srv.socket_path.exists(),
        "the producer socket must not exist before recovery has finished"
    );
    srv.wait_ready(Duration::from_secs(15));
    let body = srv.scrape_metrics();
    assert_eq!(parse_metric_f64(&body, "weir_startup_phase{phase=\"serving\"} "), 1.0);
    srv.shutdown();
}

/// Spec §16 system test 1: a 300-segment backlog behind a 100 ms/POST sink
/// (README Q13) does not hold the producer socket: it accepts within 3 s while
/// nearly the whole backlog is still undelivered (`sink_http_concurrency = 1`
/// and the sink's delay is a floor: ~3 records per segment is ~300 ms per
/// segment, so at most ~10 segments can be delivered in those 3 s on any
/// machine), a live push is acked, and a SIGHUP in that window is served (the
/// runtime now exists before recovery). Everything is delivered eventually
/// (~90 s).
#[test]
fn a_slow_sink_backlog_does_not_delay_the_producer_socket() {
    const TOTAL: usize = 300;
    let wab = fresh_wab_dir("backlog_socket");
    seed_sealed_backlog("backlog_socket_seed", &wab);
    pad_sealed_backlog(&wab, TOTAL);
    let sink = RecordingSink::start_slow(Duration::from_millis(100));
    let mut srv = weir_server!("backlog_socket")
        .wab_dir(&wab)
        .extra_config("sink_type = \"http\"\nsink_http_concurrency = 1")
        .env("WEIR_SINK_URL", sink.url())
        .log_level("info")
        .spawn();
    let t0 = Instant::now();
    srv.wait_ready(Duration::from_secs(3));
    let ready_after = t0.elapsed();
    let undelivered = count_wab_files(&wab, ".wab.sealed");
    assert!(
        undelivered > 250,
        "the socket must accept while the backlog is still undelivered: only {undelivered} \
         of {TOTAL} left after {ready_after:?}"
    );
    srv.client()
        .push(b"live-during-backlog", Durability::Durable)
        .expect("a live push must ack while the backlog drains");
    send_sighup(srv.child_pid());
    assert!(
        wait_for_sighup_reloads(&srv, "noop", 1) >= 1,
        "a HUP during the backlog must be served as a reload, not dropped"
    );
    let deadline = Instant::now() + Duration::from_secs(150);
    while count_wab_files(&wab, ".wab.sealed") > 0 {
        assert!(
            Instant::now() < deadline,
            "the backlog was not delivered: {} sealed left",
            count_wab_files(&wab, ".wab.sealed")
        );
        thread::sleep(Duration::from_millis(250));
    }
    srv.shutdown();
    assert!(
        sink.bodies().iter().any(|b| b.as_slice() == b"live-during-backlog"),
        "the live record must be delivered too"
    );
    let _ = fs::remove_dir_all(&wab);
}

/// A fresh, empty wab_dir outside the harness temp dir, so it survives a
/// restart with a second `WeirServer`.
fn fresh_wab_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("weir_sys_{tag}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// Spec §6 "A signal before serving": SIGTERM while recovery runs lets the
/// step finish, never binds or serves the producer socket, and exits 0
/// within the bound. Before this test only the chaos campaign (PR 9,
/// `sigterm_in_recovery`) exercised this path.
#[cfg(feature = "test-hooks")]
#[test]
fn sigterm_during_recovery_exits_zero_without_ever_serving() {
    let mut srv = weir_server!("sigterm_recovering")
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_STARTUP_DELAY_MS", "2000")
        .log_level("info")
        .spawn();
    let t0 = Instant::now();
    loop {
        if let Some(b) = srv.try_scrape_metrics()
            && parse_metric_f64(&b, "weir_startup_phase{phase=\"recovering\"} ") == 1.0
        {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "never observed phase recovering\nlog:\n{}",
            read_log(&srv)
        );
        thread::sleep(Duration::from_millis(20));
    }
    srv.signal(libc::SIGTERM);
    // §6: a SECOND signal during recovery is logged and forces, but recovery
    // still finishes and the joins still run.
    thread::sleep(Duration::from_millis(200));
    srv.signal(libc::SIGTERM);
    let status = srv
        .wait_for_exit(Duration::from_secs(2 + 10 + 6))
        .unwrap_or_else(|| panic!("a pre-serving shutdown must exit\nlog:\n{}", read_log(&srv)));
    assert!(status.success(), "{status}");
    assert!(
        !srv.socket_path.exists(),
        "the producer socket was never served, so no socket file may remain"
    );
    let log = read_log(&srv);
    assert!(!log.contains("weir serving"), "{log}");
    assert!(log.contains("shutdown requested during startup"), "{log}");
    assert!(log.contains("forced shutdown"), "the second signal must force\n{log}");
    assert!(
        log.contains("WAB recovery complete"),
        "a forced shutdown must not interrupt recovery\n{log}"
    );
}

/// conf-durability-3, after Foundations §6: the lock-integrity thread runs
/// from acquisition to exit, so a steal while recovery is still running (A is
/// held in `recovering` by the test-hooks startup delay) is seen within a
/// tick: A exits 3 and never binds or serves its producer socket. Replaces
/// `lock_stolen_during_startup_replay_fails_closed_before_serving`, whose
/// blocking replay no longer exists. B gets its own socket and metrics port
/// (`steal_wab_dir_lock`), so A's early `/metrics` bind does not refuse it.
#[cfg(feature = "test-hooks")]
#[test]
fn lock_stolen_during_startup_recovery_fails_closed_before_serving() {
    let wab = fresh_wab_dir("lock_recovery");
    let mut srv_a = weir_server!("lock_recovery_a")
        .wab_dir(&wab)
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_STARTUP_DELAY_MS", "10000")
        .log_level("info")
        .spawn();
    let t0 = Instant::now();
    loop {
        if let Some(b) = srv_a.try_scrape_metrics()
            && parse_metric_f64(&b, "weir_startup_phase{phase=\"recovering\"} ") == 1.0
        {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "never observed phase recovering\nlog:\n{}",
            read_log(&srv_a)
        );
        thread::sleep(Duration::from_millis(20));
    }

    let (mut child_b, b_tmp) = steal_wab_dir_lock("lock_recovery_b", &wab);
    let b_pid = child_b.id();
    let status = srv_a.wait_for_exit(Duration::from_secs(10));
    let _ = child_b.kill();
    let _ = child_b.wait();
    let log_a = read_log(&srv_a);
    let Some(status) = status else {
        panic!(
            "A is still running 10 s after B took its wab_dir during A's recovery \
             (the integrity thread is not running yet)\nA log:\n{log_a}"
        );
    };
    assert_eq!(
        status.code(),
        Some(3),
        "proven ownership loss during recovery exits 3 (got {status})\nA log:\n{log_a}"
    );
    assert!(
        !log_a.contains("weir serving"),
        "A must exit before it serves anything\nA log:\n{log_a}"
    );
    assert!(
        !srv_a.socket_path.exists(),
        "A never bound its producer socket\nA log:\n{log_a}"
    );
    assert!(
        log_a.contains("CRITICAL")
            && log_a.contains("ownership lost")
            && log_a.contains(&format!("pid {b_pid}")),
        "A's CRITICAL line must name B (pid {b_pid})\nA log:\n{log_a}"
    );

    drop(srv_a);
    let _ = fs::remove_dir_all(&b_tmp);
    let _ = fs::remove_dir_all(&wab);
}
```

Amend the safety-floor recovery test (rename it `sighup_during_startup_recovery_is_served`). Replace its comment and its last block (after `srv.wait_ready(..)`):

```rust
/// spec §3.2, the recovery-phase variant, after Foundations §6: the runtime
/// and its SIGHUP handler now exist BEFORE recovery, so a HUP while
/// `wab::recover` is sealing a pile of unsealed segments is served as a
/// (noop) reload instead of being dropped, and recovery is not disturbed.
```

```rust
    srv.wait_ready(Duration::from_secs(60));
    assert_alive_and_acking(&mut srv, "the SIGHUP during recovery");
    let log = read_log(&srv);
    assert_eq!(
        log.matches("recovery complete").count(),
        TOTAL,
        "every unsealed segment must be recovered\nlog:\n{log}"
    );
    assert_eq!(
        wait_for_sighup_reloads(&srv, "noop", 1),
        1,
        "the HUP during recovery is served exactly once (it used to be dropped)"
    );
```

Its inline comment "An ignored signal is discarded when it is sent" becomes "A log read now that still shows recovery unfinished proves the HUP landed during it". Delete `sighup_during_startup_replay_is_ignored` and its doc comment. Its premise, a replay blocked before the runtime exists, is gone; `a_slow_sink_backlog_does_not_delay_the_producer_socket` covers the HUP during a backlog.

Delete `lock_stolen_during_startup_replay_fails_closed_before_serving` (system.rs ~3053-3135, doc comment included); `lock_stolen_during_startup_recovery_fails_closed_before_serving` above replaces it. It is an unconditional `#[test]` that waits on `"queuing segment for drain replay"` lines and asserts `queued(&log_a) < TOTAL` and `!log_a.contains("metrics endpoint listening")`. After this task `scan_replay` logs that line for all 300 segments without blocking and `/metrics` binds before recovery, so it would fail on the default-features line.

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --test system --all-features metrics_answer_during_recovery -- --test-threads=1`
  Expected: it panics at `assert_eq!(parse_metric_f64(.. recovering ..), 1.0)` (left `0.0`): the delay hook is not called before this task, so the daemon is already past recovery, and serving, by the time `/metrics` answers.
  `cargo test -p weir-server --test system --all-features a_slow_sink_backlog -- --test-threads=1`
  Expected: `weir-server did not become ready within 3s`, because PR 3's tree still runs the blocking replay before the runtime (reconciliation 9).
  `cargo test -p weir-server --test system --all-features sighup_during_startup_recovery_is_served -- --test-threads=1`
  Expected: it panics with `the HUP during recovery is served exactly once`, because the counter is 0: the HUP was dropped.
  `cargo test -p weir-server --test system --all-features sigterm_during_recovery_exits_zero_without_ever_serving -- --test-threads=1`
  Expected: it panics with `never observed phase recovering`: on PR 3's tree `/metrics` binds only after recovery, so the phase is never scraped in time.
  `cargo test -p weir-server --test system --all-features lock_stolen_during_startup_recovery -- --test-threads=1`
  Expected: it panics with `never observed phase recovering`, for the same reason.

- [ ] **Implement.** Restructure `main` from `// ── Queue` onward (the line after `advise_agent_count(config.shard_count, config.worker_count);`). Everything above it stays exactly as the safety floor and PR 1 left it, in this order: umask + SIG_IGN, `Config::load`, tracing, the source line, the macOS note, `config_view` (PR 1 Task 1.24) and `"weir starting"` followed by Task 4.2's `test_hooks::warn_if_compiled_in()`; the `wab_dir` lock; `// ── Metrics` (`Metrics::new`, PR 1 Task 1.23's `init_topology` / `publish_build_info` / `publish_config`, which also sets Task 4.1's budget gauge, the `registry` Arc and `wab_dir_lock_held`); PR 1 Task 1.23's status-board block (`tcp_configured`, `wab_bytes_now`, `wab_cap_rejecting`, `board`, `board.enter_phase(Initializing)`); `let wab_ownership_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));` and `if lock_outcome == Held { spawn_lock_integrity_thread(config.wab_dir.clone(), Arc::clone(&metrics), Arc::clone(&wab_ownership_lost))?; }`; preflight; `advise_agent_count`. Do not re-declare any of those bindings below: a second `metrics`, `board`, `wab_cap_rejecting` or `wab_ownership_lost` would shadow the one the integrity thread writes and the probes read. Checked against 3adc5f5: the integrity thread (main.rs:825, spawned at :999) runs from acquisition to exit, so recovery is covered without any hand-over.

```rust
    // ── Step 3: in-memory setup (Foundations §6) ──────────────────────────────
    // Metrics, the status board, the ownership flag and its integrity thread
    // already exist (above preflight). This step adds the queue, the drain
    // channel, the drain's control and the Lifecycle.

    // One queue partition per worker (see worker::spawn_workers_with).
    let (queue_tx, queue_rx) = queue::new::<WorkUnit>(config.worker_count);
    // Held in an Option so every path names where it is dropped (§5.7).
    let mut queue_tx = Some(queue_tx);
    // ONE Receiver (the drain), Senders only in the flushers (§4). No replay_tx.
    let (drain_tx, drain_rx) =
        crossbeam_channel::bounded::<std::path::PathBuf>(wab::DRAIN_CHANNEL_CAPACITY);
    metrics
        .drain_channel_capacity
        .set(wab::DRAIN_CHANNEL_CAPACITY as f64);

    // Task 4.11 flips this to `attached()` together with the bounded shutdown.
    let drain_ctl = drain::control::DrainControl::detached();
    // PR 3's flushers' flag; the Lifecycle sets it at T0.
    let flushers_shutting_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lifecycle = Arc::new(lifecycle::Lifecycle::new(
        Arc::clone(&board),
        Arc::clone(&metrics),
        drain_ctl.clone(),
        Arc::clone(&flushers_shutting_down),
    ));
    // PR 1's board block already entered `initializing`; nothing re-publishes it.
    let started = std::time::Instant::now();

    // ── Step 4: runtime, signals, reload ──────────────────────────────────────
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // Held for the rest of main: the sync binds (bind_hardened's
    // UnixListener::from_std) and every spawn below need a reactor. Recovery,
    // the scans and the joins run on this thread OUTSIDE any block_on, while
    // the runtime's workers serve HTTP.
    let _rt_guard = rt.enter();

    let reloader = reload::Reloader::new(Arc::clone(&metrics), tcp_configured);
    let reload_tx = reload::spawn_reload_loop(Arc::clone(&reloader));
    // [PR 6 slot] `reloader.attach_board(..)` and `let admin_reload_tx =
    // reload_tx.clone();` for the admin `reload` verb, which reuses this queue.
    #[cfg(unix)]
    reload::spawn_sighup_task(reload_tx, Arc::clone(&metrics));
    #[cfg(not(unix))]
    drop(reload_tx);
    // SIGTERM/SIGINT are handled from here (§6 step 4). A signal before this
    // line takes the default disposition: nothing is in flight yet.
    lifecycle.spawn_signal_task()?;
    // (The lock-integrity thread has run since right after the lock; nothing
    // to start here.)

    // ── Step 5: metrics/health listener ───────────────────────────────────────
    let metrics_listener = rt
        .block_on(tokio::net::TcpListener::bind((
            config.metrics_bind,
            config.metrics_port,
        )))
        .map_err(|e| preflight::metrics_bind_error(&config, &e))?;
    // PR 5 swaps this for the router (HttpState); the bind point stays here.
    metrics::server::spawn(
        metrics_listener,
        Arc::clone(&registry),
        config.metrics_max_connections,
    );
    info!(
        bind = %config.metrics_bind,
        port = config.metrics_port,
        max_connections = config.metrics_max_connections,
        "metrics endpoint listening"
    );

    // ── Step 6: TLS material, before a long recovery ──────────────────────────
    #[cfg(all(unix, feature = "tls"))]
    let tls = match config.tcp_bind {
        Some(_) => {
            let (cert, key, ca) = preflight::tls_paths(&config)?;
            let t = socket::tls::ReloadableServerConfig::load(cert, key, ca)
                .map_err(|e| preflight::tls_load_error(&e))?;
            reloader.register_tls(t.clone());
            Some(t)
        }
        None => None,
    };

    // ── Step 7: [PR 6 slot] admin socket ──────────────────────────────────────
    // admin path → refuse_if_live → bind_hardened_with → admin::spawn(.., lifecycle.subscribe()).
    // `drain.pause` is accepted from here on.

    // ── Step 8: recovery, on this thread, while HTTP is served ────────────────
    lifecycle.publish_phase(weir_status::Phase::Recovering);
    if let Some(d) = test_hooks::startup_delay() {
        warn!(delay_ms = d.as_millis() as u64, "test hook: delaying the recovering phase");
        std::thread::sleep(d);
    }
    let recovery = wab::recover(&config.wab_dir, config.shard_count, &metrics)?;
    info!(
        shard_dirs = recovery.shard_dirs,
        quarantined = recovery.segments_quarantined,
        failed = recovery.segments_failed,
        elapsed_ms = recovery.elapsed.as_millis() as u64,
        "WAB recovery complete"
    );

    // ── Step 9: hand-off: the replay list in memory, then the first cap scan ──
    lifecycle.publish_phase(weir_status::Phase::Handoff);
    let orphan_cleanup = lock_outcome == wab::lock::LockOutcome::Held;
    let replay = wab::scan_replay(&config.wab_dir, config.shard_count, &metrics, orphan_cleanup)?;
    let replay_len = replay.len();
    // Synchronous, before any producer listener exists: a restart over the cap
    // must not read wab_bytes_now = 0 and admit pushes (§6 step 9; PR 1).
    status::poll::startup_cap_gate(&config.wab_dir, config.wab_max_bytes, &board, &metrics);

    // ── Step 10: pipeline ─────────────────────────────────────────────────────
    let coalesce_hint = Arc::new(AtomicU64::new(200));
    let coalesce_hint_for_poll = Arc::clone(&coalesce_hint);
    let wab_config = wab::WabConfig {
        shard_count: config.shard_count,
        batch_size: config.batch_size,
        batch_deadline: Duration::from_millis(config.batch_deadline_ms),
        segment_max_bytes: config.wab_segment_max_bytes,
        segment_max_age: (config.wab_segment_max_age_secs > 0)
            .then(|| Duration::from_secs(config.wab_segment_max_age_secs)),
        segment_max_lifetime: (config.wab_segment_max_lifetime_secs > 0)
            .then(|| Duration::from_secs(config.wab_segment_max_lifetime_secs)),
        compression: match config.wab_compression.as_str() {
            "zstd" => weir_wab::format::Compression::Zstd,
            _ => weir_wab::format::Compression::None,
        },
        compression_level: config.wab_compression_level,
    };
    let wab_handle = wab::spawn_flushers(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
        Arc::clone(&coalesce_hint),
        Arc::clone(&board.shards),
        wab::handoff::HandoffConfig {
            shutting_down: Arc::clone(&flushers_shutting_down),
            drain_depth: Arc::clone(&board.drain_depth),
            marker: Some(Arc::clone(&board) as Arc<dyn wab::handoff::HandoffMarker>),
        },
    )?;
    let worker_handles = worker::spawn_workers_with(
        &queue_rx,
        wab_handle.shard_txs,
        config.shard_count,
        config.worker_count,
        config.batch_size,
        Duration::from_millis(config.batch_deadline_ms),
        coalesce_hint,
        Some(Arc::clone(&board.shards)),
    );
    let drain_config = DrainConfig {
        wab_dir: config.wab_dir.clone(),
        dead_letter_max_bytes: config.dead_letter_max_bytes,
        dead_letter_check_interval: Duration::from_secs(config.dead_letter_check_interval_secs),
        base_retry_delay: Duration::from_millis(config.sink_retry_base_delay_ms),
        max_retries: config.sink_max_retries,
        max_respawns: drain::MAX_DRAIN_RESPAWNS,
        commit_timeout: config.commit_timeout(),
        health_poll_interval: Duration::from_secs(config.health_poll_interval_secs),
        health_probe_timeout: Duration::from_secs(config.sink_timeout_secs.min(5)),
        orphan_cleanup,
        // The safety floor's escalation floor (3 strand cycles AND 10 minutes).
        local_read_escalation_min_age: drain::LOCAL_READ_ESCALATION_MIN_AGE,
    };
    let commit_timeout_for_stall = drain_config.commit_timeout;
    let drain_heartbeat = DrainHeartbeat::new();
    metrics
        .sink_info
        .get_or_create(&metrics::SinkInfoLabel {
            sink_type: config.sink_type.as_str().to_string(),
        })
        .set(1.0);
    let spawner = sink::select::build_drain_spawner(&config, &metrics)?;
    let drain_handle = spawner.spawn(sink::select::DrainSpawnArgs {
        rx: drain_rx,
        config: drain_config,
        metrics: Arc::clone(&metrics),
        heartbeat: drain_heartbeat.clone(),
        runtime: drain::DrainRuntime {
            control: drain_ctl.clone(),
            board: Some(Arc::clone(&board)),
            // Replay is in memory now: no blocking sends, no replay_tx (§5.5).
            initial_backlog: replay,
        },
    });
    let queue_depth_task = status::poll::spawn_queue_depth_task(
        queue_tx.as_ref().expect("queue_tx is still owned by main").clone(),
        Arc::clone(&metrics),
        Arc::clone(&board),
    );
    // PR 1's 5 s task, spawned on the entered runtime instead of inside the
    // old block_on.
    let mut poll = status::poll::WabPoll::new(
        Arc::clone(&metrics),
        Arc::clone(&wab_bytes_now),
        config.wab_max_bytes,
        drain_heartbeat,
        status::poll::DrainLiveness::new(
            std::time::Instant::now(),
            Duration::from_secs(config.health_poll_interval_secs),
            commit_timeout_for_stall,
        ),
    )
    .with_status(status::poll::PollStatus::new(
        Arc::clone(&board),
        config.wab_dir.clone(),
        coalesce_hint_for_poll,
    ));
    let wab_dir_bg = config.wab_dir.clone();
    let _wab_poll_task = rt.spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let wab_dir = wab_dir_bg.clone();
            poll.tick(std::time::Instant::now(), move || {
                tokio::task::spawn_blocking(move || status::poll::compute_wab_bytes_on_disk(&wab_dir))
            })
            .await;
        }
    });
    if !lifecycle::wait_until(lifecycle::SETTLE_WAIT, Duration::from_millis(10), || {
        board.shards.all_settled()
    }) {
        warn!(
            timeout_secs = lifecycle::SETTLE_WAIT.as_secs(),
            "not every WAB shard settled before the producer listeners were bound; \
             unsettled shards stay `starting` and take no connections"
        );
    }

    // ── Steps 11-12: bind ALL producer listeners, then serve ─────────────────
    // A signal during steps 4-10 set Graceful; main finished the step it was in
    // and skips the producer side from here (recovery is never interrupted).
    #[cfg(unix)]
    let mut unix_join: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;
    #[cfg(all(unix, feature = "tls"))]
    let mut tcp_join: Option<tokio::task::JoinHandle<std::io::Result<()>>> = None;
    #[cfg(unix)]
    if lifecycle.level() == lifecycle::ShutdownLevel::Running {
        lifecycle.publish_phase(weir_status::Phase::Binding);
        // The producer socket's config (PR 1's `board` hook included), built
        // once: `bind_all` reads it, then it moves into `serve`. Inline, so main
        // keeps exactly two cloned ownership fields (this one and the TCP one).
        let socket_config = socket::SocketConfig {
            socket_path: config.socket_path.clone(),
            max_connections: config.max_connections,
            max_payload_bytes: config.max_payload_bytes,
            max_batch_records: config.max_batch_records,
            shutdown_timeout_secs: config.shutdown_timeout_secs,
            connection_read_timeout_secs: config.connection_read_timeout_secs,
            shard_count: config.shard_count,
            peer_uid_check: config.peer_uid_check,
            wab_max_bytes: config.wab_max_bytes,
            wab_bytes_now: Arc::clone(&wab_bytes_now),
            wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
            wab_ownership_lost: Arc::clone(&wab_ownership_lost),
            board: Some(Arc::clone(&board)),
        };
        // TCP first, then Unix; nothing accepts until both are bound (PR 3).
        // `bind` / `bind_all` mark the listeners Bound or Failed (README Q6);
        // main marks nothing here.
        let listeners = rt.block_on(socket::bind_all(&socket_config, config.tcp_bind))?;
        if lifecycle.level() == lifecycle::ShutdownLevel::Running {
            let conn_sem = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
            let router = socket::route::ShardRouter::live(
                Arc::clone(&board) as Arc<dyn socket::route::LiveShards>,
            );
            let socket::BoundListeners { unix, tcp } = listeners;
            #[cfg(feature = "tls")]
            if let (Some(listener), Some(tls)) = (tcp, tls.clone()) {
                let tcp_config = socket::tcp::TcpConfig {
                    max_connections: config.max_connections,
                    max_payload_bytes: config.max_payload_bytes,
                    max_batch_records: config.max_batch_records,
                    shard_count: config.shard_count,
                    shutdown_timeout_secs: config.shutdown_timeout_secs,
                    connection_read_timeout_secs: config.connection_read_timeout_secs,
                    handshake_timeout_secs: config.tls_handshake_timeout_secs,
                    wab_max_bytes: config.wab_max_bytes,
                    wab_bytes_now: Arc::clone(&wab_bytes_now),
                    wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
                    wab_ownership_lost: Arc::clone(&wab_ownership_lost),
                };
                tcp_join = Some(rt.spawn(socket::tcp::run(
                    tcp_config,
                    listener,
                    tls,
                    queue_tx.as_ref().expect("queue_tx").clone(),
                    Arc::clone(&conn_sem),
                    lifecycle.subscribe(),
                    Arc::clone(&metrics),
                    router.clone(),
                    Arc::clone(&board),
                )));
            }
            #[cfg(not(feature = "tls"))]
            drop(tcp); // always None: config validation rejects tcp_bind without tls
            unix_join = Some(rt.spawn(socket::serve(
                socket_config,
                unix,
                queue_tx.take().expect("queue_tx is served exactly once"),
                lifecycle.subscribe(),
                Arc::clone(&metrics),
                conn_sem,
                router,
            )));
            // `serve` enters Serving at its start, the phase's only writer
            // (README Q6). Log once it has; a signal that wins that race makes
            // `enter` refuse, and the line is skipped.
            if lifecycle::wait_until(Duration::from_secs(1), Duration::from_millis(1), || {
                board.life.ever_served()
            }) {
                info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    recovery_shard_dirs = recovery.shard_dirs,
                    recovery_quarantined = recovery.segments_quarantined,
                    replay_segments = replay_len,
                    "weir serving"
                );
            }
        } else {
            // A signal landed between the binds and the spawns: unlink what was
            // bound (inode-checked) and serve nothing.
            discard_listeners(listeners, &board);
        }
    }
    if queue_tx.is_some() {
        warn!("shutdown requested during startup; the producer listeners were not served");
        drop(queue_tx.take());
    }

    // ── Shutdown (interim; Task 4.11 replaces this with §7 T0-T3) ─────────────
    let level = rt.block_on(lifecycle.wait_shutdown());
    info!(?level, "shutdown requested; draining producer connections");
    rt.block_on(async {
        #[cfg(unix)]
        if let Some(h) = unix_join {
            log_listener_exit(h.await, "unix socket");
        }
        #[cfg(all(unix, feature = "tls"))]
        if let Some(h) = tcp_join {
            log_listener_exit(h.await, "TCP+mTLS");
        }
    });
    queue_depth_task.abort();
    let _ = rt.block_on(queue_depth_task);
    for h in worker_handles {
        h.join().ok();
    }
    for h in wab_handle.join_handles {
        h.join().ok();
    }
    let _ = drain_handle.join();
    drop(_rt_guard);
    rt.shutdown_timeout(std::time::Duration::from_secs(1));
    info!("weir shut down cleanly");
    Ok(())
}
```

Add these helpers at module level in main.rs (PR 3 already deleted `build_and_spawn_drain`):

```rust
/// The pre-serving shutdown path (spec §6 "A signal before serving"): unlink
/// the Unix socket we bound (inode-checked), close every listener, and serve
/// nothing. The one place main marks listener state: `serve` / `tcp::run`,
/// which own the `Closed` marks otherwise, never run on this path.
#[cfg(unix)]
fn discard_listeners(listeners: socket::BoundListeners, board: &status::StatusBoard) {
    socket::remove_own_socket(&listeners.unix.path, &listeners.unix.socket);
    drop(listeners.unix);
    board.listeners.unix.set(weir_status::ListenerState::Closed);
    if let Some(l) = listeners.tcp {
        drop(l);
        board.listeners.tcp.set(weir_status::ListenerState::Closed);
    }
}

/// Logs a listener task's exit if it was not a clean `Ok`.
#[cfg(unix)]
fn log_listener_exit(r: Result<std::io::Result<()>, tokio::task::JoinError>, which: &str) {
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "{which} listener exited with an error"),
        Err(e) => tracing::error!(error = %e, "{which} listener task failed"),
    }
}
```

Then:
- Delete `wab::replay_unconfirmed` (PR 3 kept it as `scan_replay` + blocking sends) and `replay_tx`. It has five callers in `wab::tests` at 3adc5f5 (wab/mod.rs:1480, :1528, :1574, :1729, :1909):
  - delete `replay_needs_a_live_consumer_for_backlog_over_channel_capacity` outright, because its premise (a blocking send) is gone;
  - `replay_drains_shard_dirs_beyond_configured_count` and `replay_skips_already_confirmed_segment` call `scan_replay(&dir, shard_count, &metrics, orphan_cleanup)` and assert on the returned `Vec<PathBuf>` instead of draining a channel;
  - `an_orphan_is_deleted_at_startup_only_when_the_wab_dir_is_owned`: replace the channel, the `replay_unconfirmed(..)` call and `drop(tx)` with `let replay = scan_replay(&dir, 1, &metrics, orphan_cleanup).unwrap();`, and its first assertion with `assert_eq!(replay, vec![pending], "an orphan is never replayed, deleted or not");`. The orphan-existence, tombstone and quarantine-count assertions are unchanged;
  - `a_legacy_torn_sidecar_is_quarantined_once_at_startup`: the loop body becomes `let replay = scan_replay(&dir, 1, &metrics, true).unwrap(); assert!(replay.is_empty(), "a quarantined segment is not replayed");`. The quarantine count of 1, the `!sealed.exists()` and the `quarantine/` assertions are unchanged.
  Both take `&Arc<Metrics>`, which is what PR 3's `scan_replay` takes.
- Delete the leftover pieces of PR 1-3's runtime block that this code replaces: PR 1's inline 500 ms task call, PR 1's signal task with `board_sig.begin_shutdown`, PR 3's TCP block (with the spawn wrapper Task 4.7 stripped of its marks) and `socket::serve` call, PR 3's `Binding` mark, and the `rt.block_on(async { … })` wrapper itself. Leave the lock, metrics, board, ownership-flag, integrity-thread and preflight lines above `// ── Queue` untouched.
- Rewrite the comment above the old replay (main.rs:883-884 in the spec's numbering, "queued ahead of any newly-accepted traffic") as consumption policy: "the replay list is the drain's `backlog`; it alternates with live segments, which share no connection with it, so per-connection order is unchanged".
- Keep `#[allow(dead_code)]` on `mod lifecycle;`: `wait_for_drain`, `DrainWait`, `OnDisk`, `ShutdownFacts`, `shutdown_summary`, `DRAIN_EXIT_GRACE`, `DRAIN_JOIN_POLL` and `QUEUE_SENDERS_GRACE` have no caller until Task 4.11, which removes the allowance.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --test system --all-features metrics_answer_during_recovery -- --test-threads=1`, then the same for `a_slow_sink_backlog`, `sighup_during_startup_recovery_is_served`, `sigterm_during_recovery_exits_zero_without_ever_serving` and `lock_stolen_during_startup_recovery_fails_closed_before_serving` (one filter per invocation) all pass.
  `cargo test -p weir-server --test system -- --test-threads=1` (default features) passes in full, with `lock_stolen_during_startup_replay_fails_closed_before_serving` deleted. In particular the lock, preflight and duplicate-refusal tests still exit 1 before recovery, because the lock and preflight are unchanged steps 2.
  `cargo test -p weir-server --bin weir-server ownership_flag_is_one_arc -- --test-threads=1` passes: one `let wab_ownership_lost = `, exactly two `wab_ownership_lost: Arc::clone(&wab_ownership_lost),` fields, and the `spawn_lock_integrity_thread(..)?` call.
  `cargo test -p weir-server --bins --all-features -- --test-threads=1` passes (including the five reworked or deleted `wab::tests` replay tests).
  The clippy matrix passes, including `--no-default-features`, where `tls` blocks are compiled out.

- [ ] **Commit.** `feat(startup): metrics and signals before recovery; the replay goes to the drain in memory`

---

### Task 4.11: Bounded shutdown (T0-T3) and the drain flipped to ATTACHED

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `crates/weir-server/src/main.rs` (the drain control at step 3; the whole shutdown tail written in Task 4.10; the `#[allow(dead_code)]` on `mod lifecycle;`)
- Modify: the PR 4 dead-code allowances: `crates/weir-server/src/metrics/mod.rs` (`drain_shutdown_deadline_seconds`, Task 4.1), `crates/weir-server/src/queue.rs` (`live_senders`, Task 4.8), `crates/weir-server/src/status/board.rs` (PR 1's on `LifecycleCell::{mark_forced, set_drain_deadline}`), `crates/weir-server/src/drain/control.rs` (PR 2's on `Lifetime`, `attached`, `begin_shutdown`, `request_stop`, `force_stop`)
- Test: `crates/weir-server/tests/system.rs` (spec §16 system tests 3, 4, 5), `crates/weir-server/tests/tls_listener.rs` (the TCP lame duck with a bounded exit, moved here from Task 4.7)

**Interfaces:**
- Consumes:
  - Task 4.8: `QueueReceiver::live_senders`;
  - Task 4.9: `wait_for_drain`, `DrainWait`, `ShutdownFacts`, `OnDisk`, `shutdown_summary`, and the constants;
  - PR 2: `DrainControl::{attached(), request_stop, deadline_unix_ms}`, and `StatusBoard.drain.in_flight` (`InFlight { segment: String, processed: u64, .. }`);
  - PR 1 / PR 3: `Metrics::wab_segments_not_handed_off_total(&self) -> u64` (the sum over `reason`, README Q7);
  - PR 1: `StatusBoard.wab.last_scan_ok_ms()`, `StatusBoard.wab_bytes_now`, `LifecycleCell::set_drain_deadline(deadline_unix_ms, budget_secs)`;
  - Task 4.1: `Metrics.drain_shutdown_deadline_seconds`.
- Produces: the §7 contract.
  - T0 runs in the signal task.
  - T1 awaits the lame-duck listeners, aborts and awaits the 500 ms task, waits up to `QUEUE_SENDERS_GRACE` for the senders to reach 0, then joins the workers and flushers.
  - T2 is `request_stop(budget)`, publishing the deadline (`board.life.set_drain_deadline`, `weir_drain_shutdown_deadline_seconds`), then `wait_for_drain(.., DRAIN_EXIT_GRACE, DRAIN_JOIN_POLL)`.
  - T3 is the summary line, `rt.shutdown_timeout(1 s)`, the [PR 6 slot] admin unlink, and exit 0. The orphan path is `process::exit(0)`.
  - Exit status is 0 for clean, forced and orphaned exits alike (owner decision).

- [ ] **Write the failing tests.** Append to `tests/system.rs`:

```rust
/// Every tag in `wanted` (a fixed-length prefix of each payload) shows up in
/// `sink`'s POST bodies within `within`. At-least-once: duplicates allowed.
fn assert_all_delivered(sink: &RecordingSink, wanted: &[String], within: Duration) {
    let len = wanted.first().map_or(0, String::len);
    let deadline = Instant::now() + within;
    loop {
        let got: std::collections::HashSet<String> = sink
            .bodies()
            .iter()
            .filter_map(|b| std::str::from_utf8(b.get(..len)?).ok().map(str::to_owned))
            .collect();
        let missing: Vec<&String> = wanted.iter().filter(|w| !got.contains(*w)).collect();
        if missing.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {} acked records were never delivered, e.g. {:?}",
            missing.len(),
            wanted.len(),
            &missing[..missing.len().min(5)]
        );
        thread::sleep(Duration::from_millis(250));
    }
}

/// Spec §16 system test 3: SIGTERM with an idle open connection. Within 200 ms
/// the socket is gone and new connects fail; the idle connection is closed
/// cleanly (EOF); /metrics keeps answering, reporting `shutting_down`, until
/// the process exits. (PR 5 adds the `/readyz` 503 assertion here.)
#[test]
fn sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit() {
    use std::io::Read;
    let hole = RecordingSink::start_hanging();
    let mut srv = weir_server!("lame_duck")
        .extra_config(
            "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
             drain_shutdown_budget_secs = 4",
        )
        .env("WEIR_SINK_URL", hole.url())
        .log_level("info")
        .start();
    let mut c = srv.client();
    for i in 0..8 {
        let mut p = format!("ld-{i}-").into_bytes();
        p.resize(2048, b'l');
        c.push(p, Durability::Durable).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while hole.connections() == 0 {
        assert!(Instant::now() < deadline, "the drain never reached the sink");
        thread::sleep(Duration::from_millis(50));
    }
    let idle = std::os::unix::net::UnixStream::connect(&srv.socket_path).unwrap();
    idle.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let t0 = Instant::now();
    srv.signal(libc::SIGTERM);
    while srv.socket_path.exists() {
        assert!(
            t0.elapsed() < Duration::from_millis(200),
            "lame duck: the socket must be unlinked within 200 ms of SIGTERM"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        std::os::unix::net::UnixStream::connect(&srv.socket_path).is_err(),
        "new connects must fail fast"
    );
    let mut buf = [0u8; 1];
    assert_eq!(
        (&idle).read(&mut buf).expect("a clean close, not a reset or a hang"),
        0,
        "the idle connection must see EOF"
    );
    let body = srv.try_scrape_metrics().expect("/metrics must answer during shutdown");
    assert_eq!(
        parse_metric_f64(&body, "weir_startup_phase{phase=\"shutting_down\"} "),
        1.0,
        "{body}"
    );
    thread::sleep(Duration::from_secs(1));
    let body = srv
        .try_scrape_metrics()
        .expect("/metrics must stay up while the drain spends its budget");
    // T2 publishes the deadline in force (spec §13), once the joins are done:
    // the connection drain here is clean (both connections are idle).
    assert!(
        parse_metric_f64(&body, "weir_drain_shutdown_deadline_seconds ") > 0.0,
        "T2 must publish the drain deadline\n{body}"
    );
    let status = srv
        .wait_for_exit(Duration::from_secs(3 + 4 + 6))
        .unwrap_or_else(|| panic!("shutdown not bounded\nlog:\n{}", read_log(&srv)));
    assert!(status.success(), "{status}");
    drop(c);
}

/// Spec §16 system test 4: bounded shutdown under a black-holed sink WITH THE
/// DRAIN CHANNEL FULL at SIGTERM (a flusher is blocked in its hand-off). Exit
/// within shutdown_timeout + budget + 6 s; nothing is confirmed; a restart
/// against a working sink delivers every acked record.
#[test]
fn bounded_shutdown_with_a_full_drain_channel_loses_no_acked_record() {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    let wab = fresh_wab_dir("bounded_full");
    let hole = RecordingSink::start_hanging();
    let mut srv = weir_server!("bounded_full")
        .wab_dir(&wab)
        .extra_config(
            "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
             drain_shutdown_budget_secs = 2",
        )
        .env("WEIR_SINK_URL", hole.url())
        .log_level("info")
        .start();
    let acked = Arc::new(Mutex::new(Vec::<String>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let pusher = {
        let (path, acked, stop) = (srv.socket_path.clone(), Arc::clone(&acked), Arc::clone(&stop));
        thread::spawn(move || {
            let Ok(mut c) = WeirClient::connect(&path) else { return };
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let tag = format!("full-{i:06}");
                let mut p = tag.clone().into_bytes();
                p.resize(1024, b'f');
                match c.push(p, Durability::Durable) {
                    Ok(()) => acked.lock().unwrap().push(tag),
                    Err(_) => break,
                }
                i += 1;
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let depth = parse_metric_f64(&srv.scrape_metrics(), "weir_drain_channel_depth ");
        if depth >= 256.0 {
            break;
        }
        assert!(Instant::now() < deadline, "the drain channel never filled (depth {depth})");
        thread::sleep(Duration::from_millis(100));
    }

    srv.signal(libc::SIGTERM);
    let status = srv
        .wait_for_exit(Duration::from_secs(3 + 2 + 6))
        .unwrap_or_else(|| panic!("shutdown not bounded\nlog:\n{}", read_log(&srv)));
    stop.store(true, Ordering::Relaxed);
    pusher.join().unwrap();
    assert!(status.success(), "{status}");
    let log = read_log(&srv);
    assert!(log.contains("left sealed+unconfirmed for replay"), "{log}");
    assert_eq!(
        count_wab_files(&wab, ".confirmed"),
        0,
        "nothing reached the sink, so nothing may be confirmed"
    );
    drop(srv);
    drop(hole);

    let acked = acked.lock().unwrap().clone();
    assert!(acked.len() > 256, "the channel cannot fill with {} records", acked.len());
    let sink = RecordingSink::start();
    let srv2 = weir_server!("bounded_full_2")
        .wab_dir(&wab)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .start();
    assert_all_delivered(&sink, &acked, Duration::from_secs(120));
    drop(srv2);
    let _ = fs::remove_dir_all(&wab);
}

/// Spec §16 system test 5: with a 120 s budget one SIGTERM keeps waiting on a
/// black-holed sink; a second forces the exit within the drain-exit grace, exit
/// status 0, and a restart still delivers every acked record.
#[test]
fn a_second_sigterm_forces_a_prompt_exit_and_loses_nothing() {
    let wab = fresh_wab_dir("second_sigterm");
    let hole = RecordingSink::start_hanging();
    let mut srv = weir_server!("second_sigterm")
        .wab_dir(&wab)
        .extra_config(
            "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
             drain_shutdown_budget_secs = 120",
        )
        .env("WEIR_SINK_URL", hole.url())
        .log_level("info")
        .start();
    let mut c = srv.client();
    let mut acked = Vec::new();
    for i in 0..20 {
        let tag = format!("force-{i:04}");
        let mut p = tag.clone().into_bytes();
        p.resize(1024, b'q');
        c.push(p, Durability::Durable).unwrap();
        acked.push(tag);
    }
    drop(c);
    let deadline = Instant::now() + Duration::from_secs(15);
    while hole.connections() == 0 {
        assert!(Instant::now() < deadline, "the drain never reached the sink");
        thread::sleep(Duration::from_millis(50));
    }

    srv.signal(libc::SIGTERM);
    assert!(
        srv.wait_for_exit(Duration::from_secs(5)).is_none(),
        "with a 120 s budget one SIGTERM must not exit within 5 s"
    );
    srv.signal(libc::SIGTERM);
    let status = srv
        .wait_for_exit(Duration::from_secs(2 + 1 + 3))
        .unwrap_or_else(|| panic!("the second SIGTERM did not force an exit\nlog:\n{}", read_log(&srv)));
    assert!(status.success(), "a forced exit is still exit 0 (owner decision): {status}");
    assert!(read_log(&srv).contains("forced shutdown"));
    assert_eq!(count_wab_files(&wab, ".confirmed"), 0);
    drop(srv);
    drop(hole);

    let sink = RecordingSink::start();
    let srv2 = weir_server!("second_sigterm_2")
        .wab_dir(&wab)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .start();
    assert_all_delivered(&sink, &acked, Duration::from_secs(60));
    drop(srv2);
    let _ = fs::remove_dir_all(&wab);
}
```

Append to `tests/tls_listener.rs` (moved from Task 4.7: the 200 ms half already holds on the Task 4.10 tree, because `tcp::run` drops its listener at the lame duck since Task 4.7; the bounded exit needs this task):

```rust
/// Lame duck on the TCP side (Foundations §7): within 200 ms of SIGTERM the
/// TCP listener refuses new connections, while the process is still alive
/// (its drain is spending the budget on a black-holed sink), and the exit is
/// bounded by shutdown_timeout + budget + 6 s. The 200 ms half is pinned
/// in-process by Task 4.7's `tcp_lame_duck_closes_the_listener_before_the_connection_drain`.
#[test]
fn sigterm_closes_the_tcp_listener_at_once() {
    let fx = TlsFixture::generate("tcp_lame_duck");
    let hole = weir_testkit::RecordingSink::start_hanging();
    let mut srv = weir_server!("tcp_lame_duck")
        .tls(&fx)
        .extra_config(
            "sink_type = \"http\"\nsink_timeout_secs = 300\nwab_segment_max_bytes = 4096\n\
             drain_shutdown_budget_secs = 4",
        )
        .env("WEIR_SINK_URL", hole.url())
        .start();
    let addr = srv.tcp_addr();
    // A sealed segment for the drain to hang on.
    let cfg = client_config_with_cert(&fx.ca_cert_path, &fx.client_cert_path, &fx.client_key_path);
    let mut stream = tls_connect(addr, cfg).expect("handshake");
    for i in 0..8 {
        let mut p = format!("tcp-ld-{i}-").into_bytes();
        p.resize(2048, b't');
        assert_eq!(push_one(&mut stream, &p).unwrap(), MessageType::Ack);
    }
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(15);
    while hole.connections() == 0 {
        assert!(Instant::now() < deadline, "the drain never reached the sink");
        std::thread::sleep(Duration::from_millis(50));
    }

    let t0 = Instant::now();
    srv.signal(libc::SIGTERM);
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(50)) {
            Err(_) => break,
            Ok(_) => assert!(
                t0.elapsed() < Duration::from_millis(200),
                "the TCP listener must close within 200 ms of SIGTERM"
            ),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        srv.wait_for_exit(Duration::from_millis(100)).is_none(),
        "the process must still be in its drain budget"
    );
    let status = srv
        .wait_for_exit(Duration::from_secs(3 + 4 + 6))
        .expect("shutdown not bounded");
    assert!(status.success());
}
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --test system --all-features bounded_shutdown_with_a_full_drain_channel a_second_sigterm_forces sigterm_unlinks_the_socket_at_once -- --test-threads=1`
  Expected:
  - `bounded_shutdown_…` panics `shutdown not bounded`. With the interim detached drain, the drain is still inside a 600 s `commit_timeout` on the black hole, and `drain_handle.join()` never returns.
  - `a_second_sigterm_…` panics `the second SIGTERM did not force an exit`.
  - `sigterm_unlinks_…` panics `T2 must publish the drain deadline` (nothing sets `weir_drain_shutdown_deadline_seconds` before this task; without that assertion it would panic `shutdown not bounded`).
  `cargo test -p weir-server --test tls_listener --all-features sigterm_closes_the_tcp_listener_at_once -- --test-threads=1`
  Expected: it panics `shutdown not bounded`: the detached drain sits in a 600 s `commit_timeout` on the black hole, and `drain_shutdown_budget_secs` is not wired yet.

- [ ] **Implement.** Step 3: `let drain_ctl = drain::control::DrainControl::attached();` with this comment:

```rust
    // ATTACHED (Foundations §5.4): the drain's lifetime is this control, not the
    // channel. A closed channel (every flusher gone, e.g. shard_count=1 and the
    // only flusher offline) no longer ends delivery; only request_stop /
    // force_stop do. Every path past the drain spawn therefore reaches
    // request_stop below, or exits the process.
```

Replace the interim shutdown tail with:

```rust
    // ── T0 ran in the signal task (Lifecycle::request_shutdown): readiness off,
    //    phase shutting_down, drain absorb, flushers' flag, the lame duck. ──
    let level = rt.block_on(lifecycle.wait_shutdown());
    info!(?level, "shutdown: lame duck; draining producer connections");

    // ── T1: ingest closed. The runtime stays up: /metrics, the poll tasks and
    //    (PR 6) admin `status` keep answering through the joins. ──
    rt.block_on(async {
        #[cfg(unix)]
        if let Some(h) = unix_join {
            log_listener_exit(h.await, "unix socket");
        }
        #[cfg(all(unix, feature = "tls"))]
        if let Some(h) = tcp_join {
            log_listener_exit(h.await, "TCP+mTLS");
        }
    });
    // Its queue_tx clone must go before the worker join (abort + await, which
    // replaces the old `drop(rt)`).
    queue_depth_task.abort();
    let _ = rt.block_on(queue_depth_task);
    if !lifecycle::wait_until(
        lifecycle::QUEUE_SENDERS_GRACE,
        std::time::Duration::from_millis(10),
        || queue_rx.live_senders() == 0,
    ) {
        tracing::error!(
            live = queue_rx.live_senders(),
            "a work-queue sender outlived ingest; the worker join waits for it"
        );
    }
    debug_assert_eq!(queue_rx.live_senders(), 0, "a QueueSender outlived ingest");
    info!("shutdown: ingest closed; joining workers and flushers (each flusher seals, fsyncs and hands off)");
    for h in worker_handles {
        h.join().ok();
    }
    // Bounded in every drain state: PR 3's hand_off gives up after the
    // per-flusher 1 s budget once `flushers_shutting_down` is set.
    for h in wab_handle.join_handles {
        h.join().ok();
    }

    // ── T2: the drain's budget starts now that everything ingested is sealed. ──
    let budget = std::time::Duration::from_secs(config.drain_shutdown_budget_secs);
    drain_ctl.request_stop(budget); // fetch_min: a forced deadline stays forced
    // The deadline actually in force (a forced one stays earlier), for
    // /status, lifecycle.drain_deadline_ms and the gauge.
    if let Some(deadline_ms) = drain_ctl.deadline_unix_ms() {
        board.life.set_drain_deadline(deadline_ms, config.drain_shutdown_budget_secs);
        metrics
            .drain_shutdown_deadline_seconds
            .set(deadline_ms as f64 / 1000.0);
    }
    info!(
        budget_secs = config.drain_shutdown_budget_secs,
        "shutdown: everything ingested is sealed; delivering until the budget ends"
    );
    let waited = lifecycle::wait_for_drain(
        drain_handle,
        &drain_ctl,
        lifecycle::DRAIN_EXIT_GRACE,
        lifecycle::DRAIN_JOIN_POLL,
    );

    // ── T3: exit. The summary does no I/O (no scan_unconfirmed_sealed). ──
    // Both `reason` series (budget, disconnected; README Q7).
    let not_handed_off = metrics.wab_segments_not_handed_off_total();
    // The last successful size scan (PR 1's poll task writes both).
    let on_disk = board.wab.last_scan_ok_ms().map(|as_of| lifecycle::OnDisk {
        bytes: board.wab_bytes_now.load(std::sync::atomic::Ordering::Relaxed),
        as_of_unix_ms: as_of,
    });
    match waited {
        lifecycle::DrainWait::Joined(exit) => {
            let facts = lifecycle::ShutdownFacts::from_exit(exit.as_ref(), not_handed_off, on_disk);
            if facts.left_for_replay() == 0 {
                info!("{}", lifecycle::shutdown_summary(&facts));
            } else {
                warn!("{}", lifecycle::shutdown_summary(&facts));
            }
            drop(_rt_guard);
            rt.shutdown_timeout(std::time::Duration::from_secs(1));
            // [PR 6 slot] unlink the admin socket here (remove_own_socket on its
            // BoundSocket: inode-checked).
            // The wab_dir lock (wab::lock's process-lifetime static) is released by
            // the kernel at exit, after everything above.
            info!("weir shut down cleanly");
            Ok(())
        }
        lifecycle::DrainWait::Orphaned => {
            let in_flight = board
                .drain
                .in_flight
                .load_full()
                .map(|f| (f.segment.clone(), f.processed));
            let facts = lifecycle::ShutdownFacts::orphaned(in_flight, not_handed_off, on_disk);
            tracing::error!(
                "drain did not stop within budget; exiting with its in-flight segment \
                 unconfirmed. {}",
                lifecycle::shutdown_summary(&facts)
            );
            // Without dropping the runtime or the lock: the kernel releases the
            // flock only once the whole process, orphaned drain thread included,
            // is gone. Exit status 0 (owner decision).
            std::process::exit(0);
        }
    }
}
```

Delete the old `// ── Graceful pipeline drain` comment block, which described `drop(rt)` and the channel-disconnect cascade. Replace it with a doc comment on `main` that lists the T0-T3 order and points to spec §7.

Remove the dead-code allowances that existed only until this wiring (every item below now has a production caller, through `main` or through `Lifecycle::request_shutdown` from the signal task):
- `#[allow(dead_code)]` on `mod lifecycle;` (main.rs);
- `#[cfg_attr(not(test), allow(dead_code))]` on `Metrics.drain_shutdown_deadline_seconds` (Task 4.1) and on `QueueReceiver::live_senders` (Task 4.8);
- PR 1's PR-4 allowances on `LifecycleCell::{mark_forced, set_drain_deadline}`;
- PR 2's allowances on `DrainControl::{attached, begin_shutdown, request_stop, force_stop}` and `Lifetime` (its `Attached` variant). Keep PR 2's on `request_pause`, `request_resume`, `request_requeue`, `pause_seq` and `generation`: PR 6 calls those.

Then run the clippy matrix. A dead-code warning on any item in this list is a missing wiring, not an allowance to restore.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --test system --all-features bounded_shutdown_with_a_full_drain_channel a_second_sigterm_forces sigterm_unlinks_the_socket_at_once -- --test-threads=1` passes.
  The full `cargo test -p weir-server --test system -- --test-threads=1` and `--all-features` runs pass. Watch `second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking` and `graceful_shutdown_under_load` in particular: both deliver well within the 10 s default budget.
  `cargo test -p weir-server --test tls_listener --all-features -- --test-threads=1` passes, including `sigterm_closes_the_tcp_listener_at_once`.
  `cargo test -p weir-server --bins --all-features -- --test-threads=1` passes.
  The clippy matrix (default, `--all-features`, `--no-default-features`, `--features s3-sink`) passes with the allowances above removed.

- [ ] **Commit.** `feat(shutdown): bounded shutdown with a lame duck and a forcing second signal; the drain is attached`

---

### Task 4.12: No zombie drain, and SIGTERM under a tight budget

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Test: `crates/weir-server/tests/system.rs`

**Interfaces:**
- Consumes: PR 3 Task 3.12's `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN` hook (as Task 4.2 reworked it); Task 4.11 (attached wiring); PR 2's attached-mode WARN text; PR 1's `FlusherExit::CounterScanFailed` → `Offline(CounterScanFailed)`.
- Produces: the system-level regression tests for behaviour change 5 ("no more zombie drain") and spec §16 system test 11. This plan interprets test 11 ("existing SIGTERM tests with a tight budget") as `sigterm_with_a_zero_budget_is_prompt_and_loses_nothing` plus the existing SIGTERM tests (`server_shuts_down_cleanly_on_sigterm`, `graceful_shutdown_under_load`, system.rs:959, :1871), which run under the 10 s default budget from Task 4.11 on and are not duplicated with a 0 budget. Spec system test 13 is PR 1's "SIGTERM exits within N s". It stays in the gate unchanged: its bound (shutdown_timeout + 5 s, noop sink) is unaffected because a noop drain finishes at once.

- [ ] **Write the failing tests.**

```rust
/// Behaviour change "no more zombie drain" (Foundations §5.4, §17 item 8):
/// shard_count = 1 and its only flusher goes offline at startup (the
/// counter-scan hook), so every drain-channel Sender is dropped. The attached
/// drain must keep delivering the on-disk backlog and stay alive afterwards;
/// before, it exited on Disconnected and reported `stopped` while the daemon
/// kept running.
#[cfg(feature = "test-hooks")]
#[test]
fn a_dead_only_flusher_does_not_stop_the_drain() {
    let wab = fresh_wab_dir("zombie");
    seed_sealed_backlog("zombie_seed", &wab);
    let sink = RecordingSink::start();
    let mut srv = weir_server!("zombie")
        .wab_dir(&wab)
        .shard_count(1)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .env("WEIR_TEST_HOOKS", "1")
        .env("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN", "0")
        .log_level("info")
        .spawn();
    let deadline = Instant::now() + Duration::from_secs(30);
    while count_wab_files(&wab, ".wab.sealed") > 0 {
        assert!(
            Instant::now() < deadline,
            "the backlog must be delivered with the only flusher offline\nlog:\n{}",
            read_log(&srv)
        );
        thread::sleep(Duration::from_millis(100));
    }
    // The WARN lands at the drain's next wait slice, which can be after the
    // last segment went; read the log only after the 2 s settle.
    thread::sleep(Duration::from_secs(2));
    let log = read_log(&srv);
    assert!(log.contains("segment-counter scan failed repeatedly"), "the hook did not fire\n{log}");
    assert!(
        log.contains("all flushers gone; continuing to deliver the on-disk backlog"),
        "{log}"
    );
    let body = srv.try_scrape_metrics().expect("metrics up");
    assert_eq!(
        parse_metric_f64(&body, "weir_drain_state{state=\"stopped\"} "),
        0.0,
        "zombie: the drain stopped while the daemon runs\n{body}"
    );
    srv.signal(libc::SIGTERM);
    let status = srv
        .wait_for_exit(Duration::from_secs(3 + 10 + 6))
        .expect("an attached drain with no flushers must still stop on SIGTERM");
    assert!(status.success());
    let _ = fs::remove_dir_all(&wab);
}

/// Spec §16 system test 11: the ordinary SIGTERM path with a ZERO budget is
/// prompt and loses nothing: what the drain abandons replays on the next start.
/// `sink_http_concurrency = 1` serializes the POSTs (60 records at 500 ms is
/// ~30 s), so a drain that ignores the budget cannot pass by luck.
#[test]
fn sigterm_with_a_zero_budget_is_prompt_and_loses_nothing() {
    let wab = fresh_wab_dir("budget_zero");
    let slow = RecordingSink::start_slow(Duration::from_millis(500));
    let mut srv = weir_server!("budget_zero")
        .wab_dir(&wab)
        .extra_config(
            "sink_type = \"http\"\nwab_segment_max_bytes = 4096\nsink_http_concurrency = 1\n\
             drain_shutdown_budget_secs = 0",
        )
        .env("WEIR_SINK_URL", slow.url())
        .start();
    let mut c = srv.client();
    let tags: Vec<String> = (0..60).map(|i| format!("zero-budget-{i:03}")).collect();
    for t in &tags {
        let mut p = t.clone().into_bytes();
        p.resize(1024, b'z');
        c.push(p, Durability::Durable).unwrap();
    }
    drop(c);
    let took = srv.sigterm();
    assert!(
        took < Duration::from_secs(3 + 0 + 6),
        "budget 0: SIGTERM must not wait on a 500 ms/POST sink, took {took:?}"
    );
    drop(srv);

    let sink = RecordingSink::start();
    let srv2 = weir_server!("budget_zero_2")
        .wab_dir(&wab)
        .extra_config("sink_type = \"http\"")
        .env("WEIR_SINK_URL", sink.url())
        .start();
    // Delivered across both runs: whatever run 1 confirmed is gone from disk.
    let mut seen: Vec<Vec<u8>> = slow.bodies();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        seen.extend(sink.bodies());
        let missing = tags
            .iter()
            .filter(|t| !seen.iter().any(|b| b.starts_with(t.as_bytes())))
            .count();
        if missing == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{missing} acked records were lost");
        thread::sleep(Duration::from_millis(250));
    }
    drop(srv2);
    let _ = fs::remove_dir_all(&wab);
}
```

- [ ] **Run it to verify it fails, against Task 4.11's parent commit.**
  Check out `HEAD~1` (Task 4.10, the drain still detached) into a scratch worktree and run `cargo test -p weir-server --test system --all-features a_dead_only_flusher_does_not_stop_the_drain sigterm_with_a_zero_budget -- --test-threads=1`. Expected:
  - the zombie test panics on the missing `all flushers gone; continuing to deliver the on-disk backlog` WARN (the Detached drain exits on `Disconnected` and never logs it), or earlier on `the backlog must be delivered with the only flusher offline` if that exit comes before the backlog is done;
  - the zero-budget test panics with the `budget 0: SIGTERM must not wait` message: with one POST at a time at 500 ms each, the detached drain needs about 30 s to deliver the 60 records, well past the 9 s bound.

  On the current HEAD both pass. This task adds regression tests for Task 4.11's behaviour, and the fail-first check is against the pre-flip tree. Record the two observed failure messages in the PR description.

- [ ] **Implement.** No production change; the behaviour landed in Task 4.11. If the zombie test fails on HEAD, the attached-mode `Disconnected` handling in PR 2's `wait_slice` is not wired. Fix it there (drain/mod.rs, PR 2's `Lifetime::Attached` branch), not here.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --test system --all-features a_dead_only_flusher_does_not_stop_the_drain sigterm_with_a_zero_budget -- --test-threads=1` passes. Then run the full system suite on both CI lines.

- [ ] **Commit.** `test(shutdown): no zombie drain without flushers, and a zero budget loses nothing`

---

### Task 4.13: Docs: the new startup and shutdown order, the duplicate window, SIGHUP during startup

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `docs/architecture.md`:
  - `### Startup order`, lines 49-81;
  - the socket-layer bullet at line 110, which still names a `CancellationToken`;
  - the WAB ownership bullet at line 138 ("so through recovery and the startup replay too");
  - the testing paragraph at line 267 ("during a blocked startup replay", "SIGHUP surviving at every startup phase").
- Modify: `docs/operations/configuration.md`:
  - `### Shutdown` (the `shutdown_timeout_secs` "When to tune" paragraph, ~lines 999-1005; a new subsection after Task 4.1's entry);
  - `## Limitations` (the SIGHUP sentence, ~line 2436).
- Modify: `docs/operations/tcp-mtls.md` (the "SIGHUP during startup" paragraph, ~line 207)
- Modify: `deploy/docker/docker-compose.yml` (healthcheck comment, lines 46-47), `deploy/docker/Dockerfile` (the STOPSIGNAL comment, lines 118-122)
- Modify: `deploy/systemd/README.md` (`## SIGHUP and why there is no ExecReload=`, the "During startup" bullet, lines 63-65; PR 8 Task 8.14 greps for the stale text)
- Modify: `CHANGELOG.md` (`## [Unreleased]`: a `### Changed` set for this PR's behaviour changes and an **Upgrading from 4.0.x** line; PR 8 Task 8.13 merges into it)
- Test: `crates/weir-server/tests/docs_drift.rs`

**Interfaces:**
- Consumes: the behaviour of Tasks 4.10 and 4.11.
- Produces: docs only. The phase names in architecture.md are guarded against drift.

- [ ] **Write the failing test.** Append to `tests/docs_drift.rs`:

```rust
const ARCHITECTURE: &str = include_str!("../../../docs/architecture.md");

/// The startup order in architecture.md names the six lifecycle phases in the
/// order the daemon publishes them (Foundations §6), and no longer claims the
/// metrics endpoint binds after recovery.
#[test]
fn the_startup_order_doc_lists_the_phases_in_order() {
    let section = ARCHITECTURE
        .split("### Startup order")
        .nth(1)
        .and_then(|s| s.split("\n## ").next())
        .expect("docs/architecture.md must keep a `### Startup order` section");
    let mut at = 0;
    for phase in ["initializing", "recovering", "handoff", "binding", "serving", "shutting_down"] {
        let pos = section[at..].find(phase).unwrap_or_else(|| {
            panic!("`### Startup order` must name phase `{phase}` after the previous one")
        });
        at += pos + phase.len();
    }
    assert!(
        !section.contains("replay of sealed-but-unconfirmed segments into the drain channel"),
        "the blocking replay is gone; the replay list is handed to the drain in memory"
    );
    assert!(
        !ARCHITECTURE.contains("CancellationToken"),
        "the socket layer takes a watch<ShutdownLevel>, not a CancellationToken"
    );
}
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift the_startup_order_doc`
  Expected: `FAILED`, with `\`### Startup order\` must name phase \`initializing\``.

- [ ] **Implement.** `docs/architecture.md`, replace the code block and the paragraph under `### Startup order` with:

````markdown
Ownership first, then every refuse-to-start check that can run early, then the
health surface, and only then the WAB (`src/main.rs`; Foundations §6). Each
step publishes a phase in `weir_startup_phase{phase}`:

```
umask 0o077 + SIGHUP ignored
→ Config::load → tracing → "config source", "weir starting"
→ wab_dir lock         flock(LOCK_EX|LOCK_NB) on <wab_dir>/.weir.lock   (src/wab/lock.rs)
→ initializing         Metrics::new, StatusBoard
                       → lock-integrity thread (1 s, from here to exit)
→ preflight            socket_path checks, connect-only liveness probe, metrics
                       test-bind, [tls + tcp_bind: TLS load + TCP test-bind]
→ queue, drain channel, DrainControl
→ tokio runtime        reload loop + SIGHUP handler, SIGTERM/SIGINT task,
                       metrics bind (answers from here), [TLS load], [admin socket]
→ recovering           wab::recover on the main thread (HTTP keeps answering)
→ handoff              wab::scan_replay (in memory, no sends), first WAB size + cap scan
→ pipeline             flushers, workers, drain (replay list = its backlog), poll tasks,
                       wait ≤ 10 s for every shard to settle
→ binding              bind TCP, then the Unix socket (all listeners before any accept loop)
→ serving              accept loops start; "weir serving"
→ shutting_down        on SIGTERM/SIGINT; see "Shutdown order"
```

Any refusal up to and including preflight exits 1 before recovery has touched a
segment. Preflight test-binds and releases; the real binds stay late and
authoritative, so they can still fail, but only on a race. Two late failures
remain outside preflight: a scan error in `wab::scan_replay` and a `bind_all`
error other than a live socket; both exit before any producer is served. A
slow or unreachable sink no longer delays the producer socket: the
replay backlog is handed to the drain in memory and alternates with live
segments (they share no connection, so per-connection order is unchanged). A
signal before `serving` lets the current step finish (recovery is never
interrupted) and skips the producer listeners.

### Shutdown order

```
T0 first SIGTERM/SIGINT   phase shutting_down; drain absorb mode; flushers' flag;
                          lame duck: socket unlinked (inode-checked), TCP listener closed
   connection drain       ≤ shutdown_timeout_secs (aborted at once on a second signal)
T1 ingest closed          queue-depth task stopped; workers joined; flushers joined
                          (each seals, fsyncs, hands off; ≤ 1 s if the drain is stuck)
T2 drain budget           drain_shutdown_budget_secs, then the in-flight segment is
                          abandoned (no .confirmed); main waits ≤ budget + 2 s
T3 exit                   one summary line (no I/O), runtime stops, exit 0
second signal (any time)  forced: connection drains abort, the drain deadline is now;
                          the worker and flusher joins still run
```

`/metrics` answers until T3. If the drain is inside a sink call that never
yields, the daemon exits 0 at budget + 2 s with the segment unconfirmed; it
replays on the next start.
````

Line 110: replace "the socket layer receives a `CancellationToken` and drains the `JoinSet` before returning" with "the socket layer follows a `watch<ShutdownLevel>` from `src/lifecycle.rs`: on the first signal it unlinks its socket and closes the listener at once, then drains the `JoinSet` for at most `shutdown_timeout_secs`, and aborts it on a second signal". Line 138: "(so through recovery and the startup replay too)" becomes "(so through recovery too)". Line 267: "at runtime or during a blocked startup replay" becomes "at runtime or during recovery", and "SIGHUP surviving at every startup phase" becomes "SIGHUP served as a reload from before recovery onward, bounded shutdown with a full drain channel, a forcing second SIGTERM, and a drain that outlives its flushers".

`docs/operations/configuration.md`, `shutdown_timeout_secs` "When to tune": replace the sentence about "≤ the orchestrator grace minus a few seconds" with "See [Shutdown grace period](#shutdown-grace-period) below for the orchestrator grace this implies." Then add, after the `drain_shutdown_budget_secs` entry from Task 4.1 (PR 8 Task 8.1 extends this section rather than adding a second one):

```markdown
#### Shutdown grace period

SIGTERM (or SIGINT) runs a bounded sequence:

1. **Lame duck, at once.** The producer socket is unlinked and the TCP listener
   closed, so new connects fail fast; open connections finish the frame they
   are on.
2. **Connection drain**, at most `shutdown_timeout_secs`.
3. **Seal.** Workers and flushers join; every flusher seals, fsyncs and hands
   off its active segment. If the drain is stuck, a flusher gives its hand-off
   at most 1 s; that segment is durable and replays on the next start.
4. **Drain budget**, at most `drain_shutdown_budget_secs`, then the in-flight
   segment is abandoned without a `.confirmed`.
5. **Exit 0**, with one log line naming how many segments are left for replay.

A **second** SIGTERM at any point forces: open connections are aborted and the
drain deadline becomes "now". The seal (step 3) still runs, so no acked record
is lost. A third signal only logs. Exit status is 0 in every case.

**Grace period.** Give the orchestrator at least
`shutdown_timeout_secs + drain_shutdown_budget_secs + 5` seconds: **45 s** at
the defaults (30 + 10 + 5). systemd `TimeoutStopSec=45`, compose
`stop_grace_period: 45s`, Kubernetes `terminationGracePeriodSeconds: 45`.

**Duplicates.** For the one segment abandoned at the deadline (at most one per
shutdown, forced or not): its committed prefix is re-delivered on replay; a
commit cancelled at the deadline may already have been applied by the sink;
and records the sink rejected permanently in that prefix are dead-lettered
again, counting against `dead_letter_max_bytes`. A segment a flusher did not
hand off was never started and adds no duplicates. This is the same window as
a crash.

**Decommissioning** a host: stop the producers, wait for
`weir_wab_undelivered_segments` to reach 0, then stop the daemon; or raise
`drain_shutdown_budget_secs` for that one stop.
```

`## Limitations`: replace "it is ignored during recovery and the startup replay, and on a daemon with nothing to reload" with "it is ignored only before the runtime starts (configuration load, lock and preflight); from before WAB recovery onward it is served, and on a daemon with nothing to reload".

`docs/operations/tcp-mtls.md`, replace the **SIGHUP during startup** paragraph with:

```markdown
**SIGHUP during startup.** SIGHUP is ignored from the first line of `main`
until the runtime starts, which now happens before WAB recovery (lock and
preflight come first). A HUP in that short window is dropped. From then on it
is served: during recovery the TLS material has already been loaded (it loads
right after the metrics listener, before recovery), so a HUP reloads it as
usual. Preflight also validates the TLS material before recovery. A daemon
whose cert, key or CA does not load refuses to start (`failed to load TLS
config: …`) before it touches the WAB.
```

`deploy/systemd/README.md`, replace the "**During startup**" bullet of `## SIGHUP and why there is no ExecReload=` (it says the signal "stays ignored through WAB recovery and the startup replay") with:

```markdown
- **During startup** it is ignored only until the runtime starts. `main` sets
  SIGHUP to `SIG_IGN` before it reads the config; the runtime, and with it the
  SIGHUP handler, starts right after the `wab_dir` lock and preflight, before
  WAB recovery. A HUP before that point is dropped; from then on, recovery
  included, it is served.
```

`deploy/docker/docker-compose.yml`, lines 46-47: change "It binds after WAB recovery and the startup replay, shortly before the producer socket." to "It binds before WAB recovery, so it answers during a long recovery too." `deploy/docker/Dockerfile` STOPSIGNAL comment: replace "waits for in-flight connections to drain (shutdown_timeout_secs)." with "goes lame duck, waits for in-flight connections (shutdown_timeout_secs), seals, and gives the drain drain_shutdown_budget_secs; a second SIGTERM forces it.".

`CHANGELOG.md`, under `## [Unreleased]`, `### Changed` (after PR 1's entries; PR 8 Task 8.13 merges its own into the same lists):

```markdown
- **Shutdown is bounded.** After SIGTERM the daemon goes lame duck, waits at
  most `shutdown_timeout_secs` for open connections, seals and hands off every
  active segment, then gives the drain `drain_shutdown_budget_secs` (new key,
  default 10, range 0-3600). Whatever is still undelivered stays sealed and
  unconfirmed on disk and is delivered by the next start; the exit status is
  0. The segment abandoned at the deadline re-delivers its committed prefix on
  replay (at-least-once, the same window as a crash).
- **Lame duck at the first signal.** The producer socket is unlinked
  (inode-checked) and the TCP listener closed at once, so new connects fail
  fast instead of queueing for `shutdown_timeout_secs`. Idle connections still
  get EOF; a frame already read is still acked.
- **A second SIGTERM/SIGINT forces the exit.** Open connections are aborted and
  the drain deadline becomes "now"; the worker and flusher joins still run, so
  no acked record is lost. A third signal only logs. It used to be swallowed.
- **Startup no longer waits on the sink.** `/metrics` answers before WAB
  recovery and reports `weir_startup_phase`; the replay backlog is handed to
  the drain in memory, so a slow or unreachable sink no longer delays the
  producer socket.
- **SIGHUP is served from before WAB recovery**, because the runtime and its
  handler now start right after the lock and preflight. It used to be ignored
  through recovery and the startup replay.
- **The shipped grace periods are 45 s** (`shutdown_timeout_secs +
  drain_shutdown_budget_secs + 5`): systemd `TimeoutStopSec=45`, compose
  `stop_grace_period: 45s`.
```

and under `### Upgrading from 4.0.x`:

```markdown
- **SIGTERM no longer delivers the whole backlog.** It delivers for at most
  `drain_shutdown_budget_secs` (10 s) once ingest is sealed; the rest is
  delivered by the next start. Before decommissioning a host, stop the
  producers and wait for `weir_wab_undelivered_segments` to reach 0, or raise
  `drain_shutdown_budget_secs` for that stop. Raise your orchestrator's grace
  period to at least 45 s.
```

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --test docs_drift` passes.
  Mutation check: swap `binding` and `handoff` in the architecture block, watch the guard fail, then restore.
  Run `cargo test -p weir-server --test demo_drift` too, in case demo text quotes the startup order.

- [ ] **Commit.** `docs: the startup and shutdown order, the shutdown duplicate window, SIGHUP during startup`

---

### Task 4.14: Deploy grace periods follow the new bound (45 s), guarded

*PR-level merge gate: PR 4's **Merge gate** (top of the PR 4 section; system tests 1-5, 11 and 13 in CI).*

**Files:**
- Modify: `deploy/systemd/weir.service` (lines 65-71)
- Modify: `deploy/systemd/README.md` (`## Graceful shutdown: …`, lines 127-141)
- Modify: `deploy/docker/docker-compose.yml` (add `stop_grace_period: 45s` to the `weir` service, after `restart: unless-stopped`)
- Modify: `deploy/systemd/weir.toml` (the stale `TimeoutStopSec (35s)` comment above `shutdown_timeout_secs`, lines 55-56)
- Test: `crates/weir-server/tests/docs_drift.rs`

**Interfaces:**
- Consumes: Task 4.1's defaults: 30 (`shutdown_timeout_secs`) and 10 (`drain_shutdown_budget_secs`), read from the CLI help text.
- Produces: the grace formula guard and the docs_drift consts `SYSTEMD_UNIT`, `SYSTEMD_README`, `COMPOSE` (named as PR 8 Task 8.1 names them, so PR 8 reuses them instead of redeclaring them). Spec §16 lists the grace-formula drift guard under PR 8. It is here because this PR changes the bound, and shipping 35 s against a 43 s worst case would leave main unsafe until PR 8. PR 8 Task 8.1 replaces this guard with its stricter `the_shutdown_grace_formula_holds_in_every_deploy_file` (per-unit config, every file, stale figures) and deletes `help_default`.

- [ ] **Write the failing test.** Append to `tests/docs_drift.rs`:

```rust
const SYSTEMD_UNIT: &str = include_str!("../../../deploy/systemd/weir.service");
const SYSTEMD_README: &str = include_str!("../../../deploy/systemd/README.md");
const COMPOSE: &str = include_str!("../../../deploy/docker/docker-compose.yml");

/// `[default: N]` from the CLI help line naming `flag`.
fn help_default(flag: &str) -> u64 {
    let line = CONFIG_CLI_SRC
        .lines()
        .skip_while(|l| !l.contains(flag))
        .take(3)
        .collect::<String>();
    line.split("[default: ")
        .nth(1)
        .and_then(|s| s.split(']').next())
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or_else(|| panic!("no `[default: N]` for {flag} in src/config/cli.rs HELP"))
}

/// Foundations §7: the orchestrator grace must be at least
/// shutdown_timeout_secs + drain_shutdown_budget_secs + 5, derived here from
/// the defaults the binary actually ships, and stated identically in every
/// deploy file and doc that states it.
#[test]
fn the_shutdown_grace_follows_the_shutdown_bound_everywhere() {
    let grace = help_default("--shutdown-timeout-secs")
        + help_default("--drain-shutdown-budget-secs")
        + 5;
    assert!(
        SYSTEMD_UNIT.lines().any(|l| l.trim() == format!("TimeoutStopSec={grace}")),
        "deploy/systemd/weir.service must set TimeoutStopSec={grace}"
    );
    assert!(
        COMPOSE.contains(&format!("stop_grace_period: {grace}s")),
        "deploy/docker/docker-compose.yml must set stop_grace_period: {grace}s"
    );
    for (name, doc) in [("configuration.md", CONFIGURATION), ("deploy/systemd/README.md", SYSTEMD_README)] {
        assert!(
            doc.contains("shutdown_timeout_secs + drain_shutdown_budget_secs + 5"),
            "{name} must state the grace formula"
        );
        assert!(doc.contains(&format!("{grace} s")) || doc.contains(&format!("**{grace}**")),
            "{name} must state the resulting grace ({grace} s)");
    }
}
```

- [ ] **Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift the_shutdown_grace`
  Expected: `FAILED`, with `deploy/systemd/weir.service must set TimeoutStopSec=45`.

- [ ] **Implement.** `deploy/systemd/weir.service`, replace lines 65-71 with:

```ini
# Graceful shutdown (bounded): on SIGTERM weir goes lame duck (unlinks its
# socket), waits up to shutdown_timeout_secs (default 30s) for open connections,
# seals every active WAB segment, then gives the drain drain_shutdown_budget_secs
# (default 10s) before exiting; what is left replays at the next start. Keep
# TimeoutStopSec >= shutdown_timeout_secs + drain_shutdown_budget_secs + 5 (45s
# at the defaults) so systemd never SIGKILLs a shutdown that is still bounded.
# Raise it with either knob.
KillSignal=SIGTERM
TimeoutStopSec=45
```

`deploy/systemd/README.md`, rename the section to `## Graceful shutdown: \`TimeoutStopSec\`, \`shutdown_timeout_secs\` and \`drain_shutdown_budget_secs\`` and replace its body:

```markdown
On `systemctl stop` (or a host shutdown), systemd sends `SIGTERM`. weir unlinks
its socket at once (new connects fail fast), lets open connections finish,
seals every active WAB segment, gives the drain a bounded budget, and exits.
Three settings bound that:

- **`shutdown_timeout_secs`** (in `weir.toml`, default **30**): how long open
  connections may take to finish.
- **`drain_shutdown_budget_secs`** (in `weir.toml`, default **10**): how long the
  drain keeps delivering once everything is sealed; the rest replays at the
  next start.
- **`TimeoutStopSec`** (in `weir.service`, **45**): how long *systemd* waits
  before `SIGKILL`.

Keep `TimeoutStopSec` at least
`shutdown_timeout_secs + drain_shutdown_budget_secs + 5`: **45 s** at the
defaults. A second `systemctl kill --signal=SIGTERM weir` forces a prompt exit
(open connections aborted, the drain stopped); the seal still runs, so no
acked record is lost.
```

`deploy/docker/docker-compose.yml`, under the `weir` service after `restart: unless-stopped`:

```yaml
    # Bounded shutdown: shutdown_timeout_secs (30) + drain_shutdown_budget_secs
    # (10) + 5 = 45 s. Docker's default of 10 s would SIGKILL a shutdown that is
    # still within its bound (crash-safe, but it skips the seal and the summary).
    stop_grace_period: 45s
```

`deploy/systemd/weir.toml`, replace the two comment lines above `shutdown_timeout_secs = 30` ("Graceful-shutdown budget. Keep this BELOW the unit's TimeoutStopSec (35s) so / the daemon finishes its own drain before systemd SIGKILLs it.") with:

```toml
# Connection-drain budget at shutdown. Keep TimeoutStopSec >=
# shutdown_timeout_secs + drain_shutdown_budget_secs + 5 (45s).
```

`docs/operations/configuration.md` already carries the formula and "**45 s**" from Task 4.13. Make sure the text reads exactly `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` and `**45 s**`.

- [ ] **Run to verify it passes.**
  `cargo test -p weir-server --test docs_drift` passes.
  Mutation check: set `TimeoutStopSec=40` and watch the guard fail; change `[default: 10]` in the CLI help to `[default: 15]` and watch it ask for 50 s; restore both.
  `act -W .github/workflows/ci.yml -j <the compose/container job>` passes locally, because the compose file is exercised there.

- [ ] **Commit.** `deploy: a 45 s stop grace for the bounded shutdown, in systemd and compose`

---

### Open questions for the integrator / owner

Resolved by the integrator (kept for the record): 1 (PR 3 keeps the blocking replay; Task 4.10 deletes it), 5 (one `test-hooks` module, owned by PR 3 and extended in Task 4.2), 6 (every PR 1-3 name in this section was renamed to the merged plan's; see the consumed table) and 7 (Task 4.14 keeps the guard; PR 8 Task 8.1 supersedes it).

Resolved by the README decisions: 2 (Q4: `reload::spawn_sighup_task` stays the only HUP handler; the Lifecycle takes TERM and INT), 3 and 4 (Q5: idle connections get EOF at the lame duck; the T3 line states the re-dead-letter window without a count and `DrainExit` is unchanged), 8 (Q13: system test 1 runs at 100 ms per POST).

Still open for the owner (not in the README's Owner questions; listed here):

9. **Lame-duck order.** Spec §7 says drop the listener, then unlink; this plan unlinks first (inode-checked, while the listener and the Linux `O_PATH` pin still hold the inode), then drops the listener. Both happen before the connection drain. Verified safe against `remove_own_socket` / `names_our_socket` at 3adc5f5 (socket/mod.rs:1001, :1045).
