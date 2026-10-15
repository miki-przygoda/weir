# Safety Floor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it impossible for two weir daemons to share a `wab_dir`, stop SIGHUP from killing the daemon, never unlink a live socket, and make every drain resend loop bounded — without touching the wire protocol or the ack path beyond one atomic load.

**Architecture:** SP-A adds an exclusive `flock` on `<wab_dir>/.weir.lock` taken before anything reads the WAB, a preflight that runs every refuse-to-start check before recovery, a single `Reloader` that SIGHUP (and later the admin socket) goes through, and probe-before-unlink socket handling with own-inode-only removal. SP-B adds a drain-owned in-memory `SegmentLedger` with one admission filter, class-based escalation for read errors, atomic `.confirmed` sidecars, dead-letter carry, plain-mode duplicate-key handling in the SQL sinks, and a truthful busy/heartbeat liveness signal.

**Tech Stack:** Rust 2024 (MSRV 1.88), tokio, crossbeam-channel, libc, prometheus-client; system tests via `weir-testkit`.

**Spec:** `docs/superpowers/specs/2026-10-04-safety-floor-design.md` (section numbers below refer to it). The spec carries the exact current-code line references, the error-class tables and the invariant analysis; every task's implementer reads the cited spec section in full before writing code.

## Global Constraints

- MSRV 1.88: do not use `std::fs::File::try_lock` (stable 1.89); use `libc::flock`. `libc` is already a dependency.
- No new crate dependencies in `weir-server` or `weir-ctl`.
- Wire protocol untouched; ownership loss NACKs with the existing `WireNack::InternalError`.
- Metrics and config changes are **additive only**: no renames, no label-set changes on existing series.
- Ack path: at most one `Relaxed` `AtomicBool` load per push is added. No lock, no I/O, no allocation.
- weir-ctl never calls `flock`, and its `--json` output shapes do not change.
- Commit messages: conventional prefix (`fix(...)`, `feat(...)`, `test(...)`, `docs(...)`), an explanatory body, **no `Co-Authored-By` or `Claude-Session` trailers**, and never the literal CI-skip token.
- weir-server bin unit tests and system tests run with `--test-threads=1`.
- Every new doc table that a reader relies on gets a `docs_drift` guard, verified by mutation (break it, watch it fail, restore).
- Exit codes: 1 = startup refusal (unchanged), 3 = WAB ownership lost (new).

## Review Focus

These are the inputs the spec implies but none of its tests exercise. Each now has a test in the owning task.

1. **systemd restarts immediately after SIGKILL.** A daemon started right after the previous one was SIGKILLed must acquire the lock, because the kernel releases flock at process death. The test is in Task 1 (`lock_is_released_when_the_holder_is_sigkilled`).
2. **Reading the WAB beside a live daemon.** An operator running `weir-ctl segments` / `dl list` against a live daemon's `wab_dir` must neither see `.weir.lock` nor disturb the lock. Task 1 (walker tests plus a weir-ctl scan test).
3. **The socket's parent directory is missing at start.** systemd's `RuntimeDirectory` may remove it between runs. The probe reports `Absent` and startup proceeds to the existing parent-missing error path. Task 3.
4. **Upgrading from 4.0.1.** A 4.0.1-style `wab_dir` with sealed segments, a 0-byte torn `.confirmed`, and a stale socket file left by SIGKILL must start, recover and deliver. Task 3 (stale socket) and Task 10 (torn legacy sidecar quarantined once, as today).
5. **SIGHUP floods.** Sending 100 SIGHUPs in a burst must not kill the daemon, wedge reloads or grow memory; the reload queue is bounded at 8 and the extras are dropped and counted. Task 5.

---

## Execution layout

The spec splits cleanly by file ownership, so the work runs in **three parallel tracks**, each in its own git worktree and `CARGO_TARGET_DIR`, then an integration track.

| Track | Branch | Tasks | Files mostly touched |
|---|---|---|---|
| A (ownership and signals) | `sf/track-a` | 1-6 | `wab/lock.rs`, `preflight.rs`, `reload.rs`, `socket/mod.rs`, `socket/connection.rs`, `main.rs` startup, `config/*`, `weir-testkit` |
| B (drain) | `sf/track-b` | 7-13, 15, 16 | `drain/mod.rs`, `drain/ledger.rs`, `drain/confirmed.rs`, `drain/dead_letter.rs`, `wab/mod.rs` (scan), `main.rs` stall verdict/poll |
| C (SQL sinks) | `sf/track-c` | 14 | `sink/mysql.rs`, `sink/postgres.rs`, `sink/sql_common.rs` |
| Integration | `fix/safety-floor` | 17-18 | merge A, B, C; docs; CI step; full gate |

Every track starts from `fix/safety-floor` at the spec commit. The integration track merges them in the order A, B, C and resolves `main.rs` and `metrics/mod.rs` conflicts. Both tracks add fields to `Metrics`, so keep each new field and registration in one contiguous block, to keep the conflicts mechanical.

Per task, the loop is: write the failing test, run it to see it fail, implement, run it to see it pass, run the crate's full test suite on the track's `CARGO_TARGET_DIR`, `cargo clippy -p weir-server --all-targets --all-features -- -D warnings`, `cargo fmt`, then commit.

---

## Track A — ownership and signals

### Task 1: wab_dir lock (spec §3.1, acquisition half)

**Files:**
- Create: `crates/weir-server/src/wab/lock.rs`
- Modify: `crates/weir-server/src/wab/mod.rs` (add `pub(crate) mod lock;`)
- Modify: `crates/weir-server/src/main.rs` (call it after the `weir starting` log, before `Metrics::new` at ~506)
- Modify: `crates/weir-server/src/config/{mod.rs,cli.rs,env.rs,file.rs}` (new key `wab_dir_lock`)
- Modify: `crates/weir-server/src/metrics/mod.rs` (gauge `weir_wab_dir_lock_held`)
- Modify: `crates/weir-testkit/src/harness.rs` (add `WeirServerBuilder::spawn_expecting_exit`)
- Create: `crates/weir-testkit/src/recording_sink.rs` (a tiny threaded HTTP/1.1 server that answers 200 to `HEAD` and `POST`, and records each POST body)
- Test: `crates/weir-server/src/wab/lock.rs` (unit), `crates/weir-server/tests/system.rs`

**Interfaces:**
- Produces:
  ```rust
  // wab/lock.rs
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) enum LockPolicy { Required, BestEffort }
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum LockOutcome { Held, Unsupported { errno: i32 } }
  #[derive(Debug)]
  pub(crate) enum LockError {
      Held { holder: Option<HolderInfo>, path: PathBuf },
      Unsupported { errno: i32, path: PathBuf },
      Io { source: std::io::Error, path: PathBuf },
  }
  #[derive(Clone, Debug, Default, PartialEq, Eq)]
  pub(crate) struct HolderInfo { pub pid: u32, pub host: String, pub started_unix: u64,
      pub boot_id: Option<String>, pub version: String, pub socket_path: String, pub metrics: String }
  impl HolderInfo { pub(crate) fn current(config: &Config) -> Self; pub(crate) fn render(&self) -> String;
      pub(crate) fn parse(text: &str) -> Option<Self>; }
  pub(crate) const LOCK_FILE_NAME: &str = ".weir.lock";
  pub(crate) fn acquire_wab_dir_lock(wab_dir: &Path, policy: LockPolicy, holder: &HolderInfo)
      -> Result<LockOutcome, LockError>;
  pub(crate) fn held_lock_fd() -> Option<std::os::fd::RawFd>; // for Task 2
  ```
  `static WAB_DIR_LOCK: std::sync::Mutex<Option<std::fs::File>>` holds the fd for the process lifetime; it is never released explicitly and never re-opened.
- Config: `Config.wab_dir_lock: LockPolicy`, TOML `wab_dir_lock = "required" | "best-effort"`, env `WEIR_WAB_DIR_LOCK`, CLI `--wab-dir-lock`, default `required`.
- Testkit: `WeirServerBuilder::spawn_expecting_exit(self, timeout: Duration) -> (std::process::ExitStatus, String)` spawns the binary with its own tmp dir unless `wab_dir()` was set, captures stderr+stdout, waits up to `timeout`, panics if still running (after killing it). `RecordingSink::start() -> RecordingSink` with `url() -> String`, `bodies() -> Vec<Vec<u8>>`.

- [ ] **Step 1: Write the failing unit tests** in `wab/lock.rs` `mod tests`:
  - `second_acquire_in_same_process_reports_holder`: the first acquire returns `Held`; a second `acquire_wab_dir_lock` call (it opens a new file description, so flock conflicts) returns `Err(LockError::Held{ holder: Some(h), .. })`, with `h.pid == std::process::id()`. Use an inner helper `try_lock_path(path) -> Result<File, LockError>` that bypasses the static, so tests do not poison the process-wide slot.
  - `symlinked_lock_file_is_refused` (`ELOOP` from `O_NOFOLLOW`).
  - `directory_at_lock_path_is_refused`.
  - `holder_record_round_trips` (`render` then `parse` gives an equal value).
  - `refused_contender_does_not_modify_holder_record`: byte-compare the file before and after a refused second acquire.
  - `unsupported_errno_refuses_when_required_and_warns_when_best_effort`: use a `#[cfg(test)] thread_local! { static FLOCK_HOOK: Cell<Option<i32>> }` consulted before `libc::flock`, injecting `ENOLCK` and then `ENOTSUP`.
- [ ] **Step 2: Write the failing walker tests**: one test each placing a `.weir.lock` regular file at the `wab_dir` root and asserting it is ignored by `recover_open_segments`, `scan_unconfirmed_sealed`, `compute_wab_bytes_on_disk` (counts its bytes or not, so assert the current behaviour and document it), `segment_counters`, and weir-ctl `scan_segments` / `attest::shard_dirs` (`crates/weir-ctl/src`).
- [ ] **Step 3: Run them**: `cargo test -p weir-server --bin weir-server lock -- --test-threads=1`. Expect compile failure or FAIL.
- [ ] **Step 4: Implement `lock.rs`** per spec §3.1:
  - open with `read+write+create`, `mode(0o600)`, `custom_flags(libc::O_NOFOLLOW)`;
  - `fstat`, and refuse unless `S_IFREG`; WARN when mode != 0600;
  - `libc::flock(fd, LOCK_EX|LOCK_NB)`;
  - on success `ftruncate(0)` then `pwrite` the rendered record at 0;
  - map `EWOULDBLOCK` to `Held` (read the record best-effort);
  - map `ENOLCK|EOPNOTSUPP|ENOTSUP|ENOSYS|EINVAL` to `Unsupported`, honouring the policy;
  - map anything else to `Io`.

  Add the error `Display` with the exact refusal message from §3.1.
- [ ] **Step 5: Wire config and main.** Parse the key in all three layers and in the file-layer unknown-key allowlist (config/file.rs ~115). In `main`:
  ```rust
  let holder = wab::lock::HolderInfo::current(&config);
  let lock_outcome = wab::lock::acquire_wab_dir_lock(&config.wab_dir, config.wab_dir_lock, &holder)
      .map_err(|e| { tracing::error!("{e}"); e })?;
  ```
  Placement: after the `info!("weir starting")` (~485), before `advise_agent_count`/`Metrics::new`. After `Metrics::new`, set `weir_wab_dir_lock_held` to 1 for `Held` and 0 for `Unsupported`. `?` returns exit code 1 through `main`'s `Result`.
- [ ] **Step 6: System tests** in `tests/system.rs`:
  - `second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking`: the owner's reproduction as a regression, using the spec §3.1 test steps 1-7 with `RecordingSink`, 20 + 50 Durable pushes, and `spawn_expecting_exit`.
  - `lock_is_released_when_the_holder_is_sigkilled` (Review Focus 1): start A, `kill_ungracefully()`, then immediately start B on the same `wab_dir`; B becomes ready.
- [ ] **Step 7: Run** `cargo test -p weir-server --bin weir-server -- --test-threads=1`, then `cargo test -p weir-server --test system -- --test-threads=1`, and `cargo test -p weir-ctl`. All PASS.
- [ ] **Step 8: Commit** as `fix(wab): lock wab_dir so a second daemon cannot recover a live WAB`, with a body that cites the reproduction.

### Task 2: lock integrity and fail-closed on proven loss (spec §3.1 integrity, D4)

**Files:** Modify `wab/lock.rs`, `main.rs` (spawn the 1 s integrity task in `rt.block_on`), `socket/connection.rs` (check before both `over_wab_cap` call sites ~356/~404), `socket/tcp.rs` (the same handler is shared, so verify), `metrics/mod.rs` (`weir_wab_dir_lock_integrity_failures_total`). Tests: `lock.rs` unit, `connection.rs` unit, `system.rs`.

**Interfaces:**
- Consumes `held_lock_fd()` and `LOCK_FILE_NAME` from Task 1.
- Produces:
  ```rust
  pub(crate) enum LockIntegrity { Ok, Relocked, LostToOther { holder: Option<HolderInfo> }, Error(i32) }
  pub(crate) fn check_wab_dir_lock(wab_dir: &Path) -> LockIntegrity;
  pub(crate) const EXIT_WAB_OWNERSHIP_LOST: i32 = 3;
  ```
  `ConnectionConfig` gains `wab_ownership_lost: Arc<AtomicBool>`.

- [ ] **Step 1: Failing unit tests.**
  - `unlink_and_recreate_relocks`: holding the lock, unlink the file; the check gives `Relocked`, and the static now holds the new fd.
  - `unlink_then_other_holder_gives_lost_to_other`: unlink, then another fd in-process opens and flocks the new file; the check gives `LostToOther`.
  - `ownership_lost_nacks_before_queue`: in connection.rs tests, with `wab_ownership_lost=true`, a Durable push gets `NackReason::InternalError` and the queue receiver gets nothing.
  - `lock_module_never_reopens_path`: read `lock.rs` source with `include_str!`, count `OpenOptions` occurrences outside `mod tests`, and assert 2: acquire and the Relocked re-create.
- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement.** Per §3.1, `check_wab_dir_lock` stats the path (dev, ino) against `fstat(held)`, re-issues `flock(held, LOCK_EX|LOCK_NB)`, and classifies the result. The integrity task runs every 1 s via `spawn_blocking`:
  - `Relocked`: ERROR log and bump the counter.
  - `LostToOther`: store `wab_ownership_lost=true`, set the gauge to 0, log the CRITICAL line with the holder, then `std::process::exit(EXIT_WAB_OWNERSHIP_LOST)`. Do not seal.
  - `Error`: ERROR log, bump the counter, retry on the next tick.

  Under `LockOutcome::Unsupported` the task is not spawned.
- [ ] **Step 4: System test** `lock_file_stolen_by_second_daemon_fails_closed`, following spec §3.1 steps 1-4: A's exit status is 3, and any push in the window is NACKed, never acked.
- [ ] **Step 5: Run the full server unit and system suites. Commit** as `fix(wab): fail closed when another instance provably owns the wab_dir`.

**Revised in review (2026-10-05 adversarial review; the spec's §3.1 has the detail).** As built, Task 2 differs from the steps above:
- The check runs on a std thread (`weir-lock-integrity`) spawned in `main` right after `acquire_wab_dir_lock`, not a task in `rt.block_on`, so it covers preflight, recovery and the startup replay (conf-durability-3; system test `lock_stolen_during_startup_replay_fails_closed_before_serving`).
- The re-create goes through one opener, `open_lock_path` (`lock_module_never_reopens_path` pins it): `O_EXCL` for a missing file; for a replaced one, a same-`fstat`-id check first (`Ok` plus a kept alias fd, conf-regressions-12), then `flock` and a read of the record before any write (another daemon's record is `LostToOther`, conf-durability-2), and only then the hard-link and owner vet as the gate on writing (conf-security-5, cont-security-2). The proof precedes the vet so a second daemon running as another uid is `LostToOther`, not an error retried while acks continue.
- A source-check unit test in `main.rs` pins that the integrity thread and both listener configs share one ownership `Arc` (conf-tests-14).

### Task 3: socket ownership: probe, BoundSocket, own-inode removal (spec §3.3)

**Files:** Modify `socket/mod.rs` (split `cleanup_existing_socket` into the two functions, add `probe_socket`, have `bind_hardened` return `BoundSocket`, update `run`'s shutdown path at ~286 and `remove_socket_hardened` ~298); `tests/system.rs` (replace `socket_takeover_does_not_corrupt_wab_data` at ~2170-2256).

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, PartialEq, Eq)]
  pub(crate) enum SocketProbe { Absent, Stale, Live, Unknown(i32) }
  pub(crate) fn probe_socket(path: &Path) -> SocketProbe;          // connect-only, never unlinks
  pub(crate) struct BoundSocket { pub listener: tokio::net::UnixListener, pub dev: u64, pub ino: u64,
      #[cfg(target_os = "linux")] pin: Option<std::os::fd::OwnedFd> }
  pub(crate) fn bind_hardened(path: &Path) -> io::Result<BoundSocket>;
  pub(crate) fn remove_own_socket(path: &Path, bound: &BoundSocket);
  ```
  Task 4 (preflight) uses `probe_socket`. Foundations' admin socket reuses all four.

- [ ] **Step 1: Failing unit tests** (extend the existing tests at socket/mod.rs:943-971):
  - `startup_cleanup_unlinks_a_dropped_listener_socket` (ECONNREFUSED is stale, so it unlinks);
  - `startup_cleanup_refuses_a_live_listener`;
  - `probe_reports_absent_when_parent_missing` (Review Focus 3);
  - `remove_own_socket_keeps_a_replaced_socket` (mismatched (dev, ino));
  - `full_run_start_then_shutdown_removes_its_own_socket`, the self-probe trap regression;
  - `#[cfg(target_os="linux")] o_path_pin_opens_on_socket_inode`.
- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement** per the §3.3 probe table:
  - use a non-blocking `socket(AF_UNIX, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC)` and `connect`;
  - on non-Linux, re-probe ECONNREFUSED 3× at 50 ms;
  - `EACCES`/`EPERM`/other → `Unknown`, which refuses (fail closed);
  - the refusal message is the exact §3.3 text.

  The Linux pin is `open(path, O_PATH|O_NOFOLLOW|O_CLOEXEC)` immediately after the existing late-swap check. Non-Linux identity adds `st_ctime` (and `st_birthtime` on macOS) to the (dev, ino) comparison.
- [ ] **Step 4: System tests:**
  - replace the takeover test with `second_daemon_refuses_a_live_socket` (B has its own `wab_dir` and metrics port but A's socket; B exits non-zero, and A still acks);
  - add `shutdown_leaves_a_successor_socket_alone`;
  - add `starts_cleanly_over_a_stale_socket_left_by_sigkill` (Review Focus 4: `kill_ungracefully`, then start a new daemon on the same paths).
- [ ] **Step 5: Run all suites, including the existing `restart_in_place` crash tests. Commit** as `fix(socket): never unlink a live socket, and remove only the socket we bound`.

### Task 4: preflight (spec §3.0)

**Files:** Create `crates/weir-server/src/preflight.rs`; modify `main.rs` (call it right after `acquire_wab_dir_lock`); `tests/system.rs`.

**Interfaces:**
- Consumes `socket::validate_socket_path`, `socket::probe_socket` (Task 3), and `socket::tls::ReloadableServerConfig::load` (under `cfg(all(unix, feature="tls"))`).
- Produces `pub(crate) fn preflight(config: &Config) -> io::Result<()>`.

- [ ] **Step 1: Failing unit tests:**
  - `preflight_refuses_live_socket` (bind a listener at the path);
  - `preflight_refuses_taken_metrics_port` (bind a `TcpListener` on `127.0.0.1:0` and point the config at its port);
  - `preflight_passes_on_clean_paths`;
  - `#[cfg(feature="tls")] preflight_refuses_bad_tls_material`.
- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement** the steps in the §3.0 order, test-binding and dropping each listener, with error messages that name the conflicting resource.
- [ ] **Step 4: System test** `refused_duplicate_never_delivers`:
  - socket variant and metrics-port variant;
  - B is pre-seeded with 3 sealed segments in its own `wab_dir` and uses `RecordingSink`;
  - B exits non-zero within 5 s, the recording sink sees 0 requests, and B's log has no `recovering` line.

  To seed sealed segments, start a daemon on that `wab_dir` with a noop sink, push, then `kill_ungracefully` so the segments stay unconfirmed. Recovery seals them on the next start, so for "sealed and unconfirmed" use `restart_in_place` with the HTTP sink pointed at a closed port, then stop. If that proves awkward, write segments directly with `weir_wab` format helpers and document the method in the test.
- [ ] **Step 5: Run all suites. Commit** as `fix(startup): refuse a duplicate before recovery, not after delivery begins`.

### Task 5: SIGHUP: ignore early, one reload path (spec §3.2)

**Files:** Create `crates/weir-server/src/reload.rs`; modify `main.rs`:
- add `libc::signal(SIGHUP, SIG_IGN)` in the umask block (~447-450);
- delete `spawn_tls_reload_task` (~348-400) and replace its call (~1143) with `reloader.register_tls(tls.clone())`;
- spawn `reload_loop` and the SIGHUP task first inside `rt.block_on`.

Also modify `metrics/mod.rs` (`weir_config_reloads_total{trigger,outcome}`). Tests go in `reload.rs` (unit), `tests/system.rs`, and `tests/tls_listener.rs`.

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum ReloadTrigger { Sighup, Admin }
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum ReloadOutcome { Ok, Noop, Failed }
  #[derive(Debug, Clone)] pub(crate) struct ReloadReport { pub trigger: ReloadTrigger, pub outcome: ReloadOutcome,
      pub changed: Vec<&'static str>, pub rejected: Vec<(&'static str, String)>, pub note: Option<&'static str> }
  pub(crate) struct Reloader { /* tls: OnceLock<ReloadableServerConfig> under cfg(tls), metrics: Arc<Metrics> */ }
  impl Reloader { pub(crate) fn new(metrics: Arc<Metrics>, tls_configured: bool) -> Arc<Self>;
      #[cfg(all(unix, feature="tls"))] pub(crate) fn register_tls(&self, t: ReloadableServerConfig);
      pub(crate) fn reload(&self, t: ReloadTrigger) -> ReloadReport; }
  pub(crate) struct ReloadRequest { pub trigger: ReloadTrigger, pub reply: Option<tokio::sync::oneshot::Sender<ReloadReport>> }
  pub(crate) fn spawn_reload_loop(r: Arc<Reloader>) -> tokio::sync::mpsc::Sender<ReloadRequest>; // channel(8)
  pub(crate) fn spawn_sighup_task(tx: tokio::sync::mpsc::Sender<ReloadRequest>, metrics: Arc<Metrics>);
  ```
  The SIGHUP task uses `try_send`; when the queue is full it drops the request and bumps `weir_config_reloads_total{trigger="sighup",outcome="dropped"}` (Review Focus 5). Label values: `ok`, `failed`, `noop`, `dropped`. The `admin` trigger value is reserved.

- [ ] **Step 1: Failing unit tests:**
  - `reload_without_tls_is_a_counted_noop`;
  - `tls_configured_but_not_registered_is_noop_with_note`;
  - `burst_of_requests_never_blocks_and_counts_drops`: 100 `try_send` calls against a stalled loop leave at most 8 queued, and the drops are counted.
- [ ] **Step 2: Run them to see them fail.**
- [ ] **Step 3: Implement** per §3.2. The TLS branch keeps `weir_tls_config_reloads_total{outcome}` exactly as today.
- [ ] **Step 4: System tests:**
  - `sighup_is_a_counted_noop_on_a_default_daemon`;
  - `sighup_flood_does_not_kill_or_wedge` (Review Focus 5: 100 HUPs, then the process is alive and a push acks);
  - `sighup_during_startup_replay_is_ignored`, following spec §3.2 test 3 with a black-hole HTTP listener in `RecordingSink` mode `hang`, plus the recovery-phase variant;
  - in `tests/tls_listener.rs`: `sighup_reloads_tls_when_configured`.
- [ ] **Step 5: Run all suites, including `--features tls`. Commit** as `fix(signals): SIGHUP no longer kills a daemon without TLS`.

### Task 6: missing config file rider (spec §3.4, D10)

**Files:** Modify `config/cli.rs` (track whether `--config` was passed), `config/mod.rs` (`Config::load` warnings), and `config/file.rs:194-199`.

- [ ] **Step 1: Failing tests:**
  - `missing_explicit_config_warns_naming_path`;
  - `missing_default_config_warns`;
  - `missing_config_with_noop_sink_warns_loudly`.

  These assert on the returned warnings vector, which `Config::load` already returns (main.rs:452).
- [ ] **Step 2: Implement** the warnings and the INFO log naming the config source. Do not make it an error.
- [ ] **Step 3: Run the config tests. Commit** as `fix(config): say so when the config file is missing`.

---

## Track B — drain

Track B rewrites the drain in place. Read the spec's §4.0 first. All of Track B's tasks mutate `drain/mod.rs`, so they run strictly in order. **Regression gate for every Track B task:** every pre-existing test in `drain/mod.rs`'s `mod tests` still passes, and the diff of that block is additions-only, unless the spec explicitly changes the asserted behaviour. Where it does, the task names the test and the spec section.

### Task 7: SegmentLedger, admission filter, cursor across strands, cause classification (spec §4.0)

**Files:** Create `crates/weir-server/src/drain/ledger.rs`; modify `drain/mod.rs`:
- `run_drain_supervised` ~235 owns the ledger;
- the Draining arm ~679/~690 calls admission on both sources;
- `ProcessResult::Transient` gains `cause`;
- the strand branch ~822-842 splits bookkeeping by cause.

Also modify `wab/mod.rs`, adding the `scan_unconfirmed_sealed` `exclude` parameter at ~376 (startup replay passes `|_| false`), and `metrics/mod.rs` (`weir_drain_local_io_strands_total{cause}`, `weir_drain_segments_parked{reason}`, `weir_drain_segments_disposition_pending{kind}`).

**Interfaces:**
- Produces:
  ```rust
  pub(crate) struct SegmentLedger { /* fields exactly as spec §4.0 */ }
  pub(crate) enum Admit { Process { skip: u64 }, Drop(&'static str) }
  pub(crate) enum ParkReason { LocalReadFailed, UnsupportedVersion, DrainPanic }
  pub(crate) enum ErrClass { Process, Access, Media }
  pub(crate) enum TransientCause { Sink, LocalRead { class: ErrClass, errno: Option<i32> }, LocalWrite }
  pub(crate) struct ResumeCursor { pub processed: u64, pub dev: u64, pub ino: u64 }
  pub(crate) struct LedgerCounts { /* AtomicUsize/AtomicU64 mirrors per spec §5 seam 2 */ }
  impl SegmentLedger {
      pub(crate) fn new(counts: Arc<LedgerCounts>) -> Self;
      pub(crate) fn admit(&mut self, path: &Path, now: Instant) -> Admit;
      pub(crate) fn excludes(&self, path: &Path, now: Instant) -> bool;
      pub(crate) fn record_progress(&mut self, path: &Path, processed: u64);   // writes resume_from with (dev,ino)
      pub(crate) fn forget(&mut self, path: &Path);                           // Confirmed / Quarantined / parked
      pub(crate) fn park(&mut self, path: &Path, why: ParkReason);
      pub(crate) fn note_local_read_failure(&mut self, path: &Path, class: ErrClass) -> u32;
      pub(crate) fn set_not_before(&mut self, path: &Path, at: Instant);
      pub(crate) fn retry_dispositions(&mut self, /* filled in Tasks 10-11 */) ;
  }
  pub(crate) fn classify(err: &std::io::Error) -> ErrClass;   // table in spec §4.0
  ```
- [ ] **Step 1: Failing tests** (ledger unit tests plus drain tests):
  - `admit_skips_parked_confirm_and_quarantine_pending`;
  - `admit_resumes_from_cursor_when_inode_matches`;
  - `stale_cursor_for_replaced_file_resets_to_zero`;
  - `classify_maps_errnos_per_table`;
  - **`same_path_in_channel_and_rescan_is_delivered_once`**: put the same path in both the channel and the rescan, fail the first delivery's sidecar (use the Task 10 hook once it exists; for now, park it), and assert a sink count of 1;
  - `cursor_survives_a_strand_within_the_run`: a sink that is transient on batch 3 of 5 for longer than `max_retries`, then healthy; the sink never re-sees batches 1-2.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement** per §4.0, including the health-poll ordering (`retry_dispositions`, then `probe_and_resume_stranded`, then gauges) and the best-effort disposition retry on `'outer` exit.
- [ ] **Step 4: Run** the full `drain` test module and the whole server bin suite. **Commit** as `fix(drain): one admission filter, and a resume cursor that survives a strand`.

### Task 8: a corrupt or newer-format header at open (spec §4.1)

**Files:** `drain/mod.rs` (`process_segment` open arm ~1116-1136; fix the false comment ~1127-1130), `drain/ledger.rs`.

- [ ] **Step 1: Failing tests:**
  - bad magic is quarantined on the first attempt, `stranded` is 0, and the file is in `quarantine/`;
  - version `FORMAT_VERSION_MAX_SUPPORTED+1` is parked as `unsupported_version` and not moved;
  - EISDIR (a directory at the sealed path) parks after 3 strand cycles and is not re-queued, with `stranded` unchanged;
  - the EMFILE hook for 30 cycles parks nothing, and everything is delivered after the hook clears.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement** the version-byte read and the escalation threshold `(max_retries+1) × LOCAL_READ_STRAND_LIMIT` (`const LOCAL_READ_STRAND_LIMIT: u32 = 3`).
- [ ] **Step 4: Run the suites. Commit** as `fix(drain): quarantine a corrupt sealed header instead of retrying it forever`.

### Task 9: transient I/O mid-segment and at the footer (spec §4.2)

**Files:** `drain/mod.rs` (~1170-1180, ~1255-1303). Add a `#[cfg(test)]` reader-fault hook: an `Option<(u64, io::ErrorKind, u32 /*cycles*/)>` consulted by the record loop.

- [ ] **Step 1: Failing tests:**
  - EIO at record N held for more than K cycles: each record below N reaches the sink exactly once, and the segment ends in `quarantine/`;
  - footer EIO for 2 cycles, then cleared: the sink count equals the record count, and the segment is confirmed;
  - the existing CRC-mismatch quarantine test stays unchanged.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement** the kind split. The partial in-memory batch is never committed on a non-corruption error. Footer `NotFound` counts as consumed.
- [ ] **Step 4: Run the suites. Commit** as `fix(drain): an I/O blip mid-segment no longer quarantines a good segment`.

### Task 10: atomic sidecar, confirm_pending, orphan deletion (spec §4.3, D7)

**Files:**
- `drain/confirmed.rs`: `write_confirmed_durably` becomes tmp + fsync + rename + dir fsync; `confirm_and_delete` returns `ConfirmOutcome`.
- `drain/ledger.rs`: `confirm_pending` and the `retry_dispositions` sidecar and delete stages.
- `wab/mod.rs`: in `scan_unconfirmed_sealed`, the `Ok(true)` arm deletes orphans only when the lock is held, and stale `*.confirmed.tmp` files are deleted.
- `metrics/mod.rs`: `weir_drain_confirm_failures_total{stage}`.
- Walkers that must ignore `.tmp`: `recovery.rs:80-82`, `:117-118`, `main.rs:111`, and weir-ctl `main.rs:530-535`.

**Interfaces:** Produces `pub(crate) enum ConfirmOutcome { Done, SidecarFailed, DeleteFailed }`. The lock state comes from Task 1: `wab::lock::LockOutcome` is passed into `DrainConfig` as `orphan_cleanup: bool`. If Task 1 is not yet merged on this track, add the field defaulting to `false`, and the integration track sets it from `lock_outcome`.

- [ ] **Step 1: Failing tests** (spec §4.3 test list):
  - a directory at `seg_X.wab.confirmed.tmp`: the sink count stays at 1 across polls, then the next poll succeeds after removal;
  - a short-write ENOSPC hook leaves no final-name sidecar and causes no quarantine;
  - a `sync_all` failure hook means no rename and no delete;
  - an orphan is removed at startup and in the rescan when `orphan_cleanup` is set, and kept when it is not;
  - a delete-failure hook leaves the segment in `confirm_pending{Delete}` and never delivers it again;
  - **legacy torn sidecar** (Review Focus 4): a 0-byte `.confirmed` written by 4.0 is quarantined once, exactly as today.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): write .confirmed atomically, and retry the confirm, not the delivery`.

### Task 11: quarantine_pending (spec §4.4)

**Files:** `drain/mod.rs` (`quarantine_segment` returns `bool`; new `ProcessResult::QuarantinePending`), `drain/ledger.rs`, `metrics/mod.rs` (`weir_drain_quarantine_failures_total`).

- [ ] **Step 1: Failing test:** with a regular file named `<wab_dir>/quarantine`, a CRC-error segment's prefix is delivered once across 5 polls and the failure counter is 1. After the file is removed, the next poll completes the move.
- [ ] **Step 2: Run it to see it fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): a failed quarantine move no longer re-delivers the prefix every poll`.

### Task 12: dead-letter headroom, carry, rescan NotFound, write failure (spec §4.5, §4.11, §4.9 DL rider)

**Files:**
- `drain/mod.rs`: `BatchResult::Blocked{needed}`, `ProcessResult::BlockedDeadLetter{processed, needed, carry}`, the `DrainState::BlockedDeadLetterFull` unblock predicate at ~951, and the `commit_batch` DL write failure ~1545-1555.
- `drain/dead_letter.rs`: `rescan` on NotFound; `write_records` on ENOENT.
- `drain/ledger.rs`: `dl_carry`, with `const DL_CARRY_MAX_BYTES: u64 = 64 * 1024 * 1024`.
- `metrics/mod.rs`: `weir_drain_dead_letter_write_failures_total`.

- [ ] **Step 1: Failing tests** (spec §4.5 and §4.11):
  - a total of `cap - estimate/2` over 10 check intervals gives exactly 1 sink commit, then frees space, then resumes once, writes the carry, and does not re-commit;
  - deleting `dead_letter/` while blocked resumes within one interval;
  - the DL write ENOSPC hook for 10 polls gives a sink commit count of 1 and the DL holds each rejected record exactly once;
  - a carry over the cap falls back to re-commit once, with a WARN.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): the dead-letter block re-commits nothing it already committed`.

### Task 13: strand before the final wait; per-segment not_before (spec §4.6)

**Files:** `drain/mod.rs`: move the `retries_left == 0` branch from ~822 to the top of the RetryingTransient arm (~727) and set `ledger.not_before`.

- [ ] **Step 1: Failing tests:**
  - with `base=200ms`, `max_retries=2` and an always-transient sink, the time from the first failure to the `stranded` increment is under 1.0 s;
  - with `Retry-After: 2s`, the segment is not re-processed before 2 s, even when a copy of its path is in the channel.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): strand without serving a final backoff nobody waits for`.

### Task 15: truthful drain liveness (spec §4.8)

**Files:**
- `drain/mod.rs`: `DrainHeartbeat` gains `busy_since: AtomicU64` and `busy_kind: AtomicU8`; add `BusyKind` and `BusyGuard`; bump the heartbeat after every `commit_batch` and every 1,024 records read.
- Wrap these sites with guards: the commit timeout (outside `tokio::time::timeout`), `probe_health`, the reader loop, `write_confirmed_durably`, `dead_letter.write_records`, `quarantine_segment`, and the scans.
- `main.rs`: `drain_stall_verdict` takes `busy` and `commit_timeout`, with new `SlowSinkCall`/`SlowLocalIo` variants. Move the verdict and the gauge before the scan `else { continue }` at ~990-994.
- `metrics/mod.rs`: `weir_drain_busy_seconds{kind}`.

**Interfaces:** Produces
```rust
#[repr(u8)] pub(crate) enum BusyKind { SinkCommit=1, SinkProbe=2, SegmentRead=3, LocalFsync=4, DeadLetterWrite=5, QuarantineMove=6, DirScan=7 }
impl DrainHeartbeat { pub(crate) fn busy(&self) -> Option<(BusyKind, Duration)>; }
pub(crate) struct BusyGuard<'a> { /* sets on new(), clears on Drop (incl. unwind) */ }
impl<'a> BusyGuard<'a> { pub(crate) fn new(hb: &'a DrainHeartbeat, kind: BusyKind) -> Self; }
```
Foundations' StatusBoard reads `busy()`.

- [ ] **Step 1: Failing tests:**
  - a mock sink that sleeps 2 s per commit (yielding) across a 10-batch segment with thresholds scaled down: the heartbeat advances and the verdict is never `Wedged`;
  - a thread-blocking sink past `commit_timeout + grace` gives `Wedged`;
  - verdict table rows, including a 60 s sidecar fsync giving `SlowLocalIo`;
  - `BusyGuard` clears on a panic unwind.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): a slow sink or a slow disk no longer reads as a wedged drain`.

### Task 16: riders and the deterministic drain panic (spec §4.9, §4.12)

**Files:**
- `drain/mod.rs`: the supervisor `Err` arm increments `panic_count[in_flight]` and parks at 2.
- `wab/mod.rs`: `scan_unconfirmed_sealed` skips an unreadable shard for the rescan caller only and returns `incomplete`.
- `main.rs`: `compute_wab_bytes_on_disk` matches dirent errors explicitly instead of `.flatten()`.
- Stale comments, all listed in §4.9: drain/mod.rs:22-27, :78, :221-223, :826-842; confirmed.rs:11-15; metrics/server.rs:41-43; metrics/mod.rs:333-338.

- [ ] **Step 1: Failing tests:**
  - a sink that panics on one payload: that segment is parked after 2 panics, the others deliver, and `drain_state` is never `stopped`;
  - an unreadable shard dir does not block resume of the other shards in the rescan, while startup replay still fails fast;
  - a dirent error makes `compute_wab_bytes_on_disk` return `None`.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run the suites. Commit** as `fix(drain): park a segment that crashes the drain twice instead of stopping delivery`.

---

## Track C — SQL sinks

### Task 14: plain-mode duplicate-key handling (spec §4.7, D6)

**Files:** `crates/weir-server/src/sink/mysql.rs` (~319-330, ~403), `sink/postgres.rs` (~452-465, ~509), `sink/sql_common.rs` (the shared bisection driver), `config/mod.rs` (the config-load WARN for plain mode without `id_column`), `deploy/run-sink-integration-tests.sh` and the `#[ignore]` sink tests in `tests/system.rs`.

**Interfaces:** Produces, in `sql_common.rs`:
```rust
pub(crate) async fn insert_with_dup_fallback<F, Fut>(rows: &[Payload], ids: Option<&[RecordId]>, deadline: tokio::time::Instant,
    insert: F, lookup_present: impl ...) -> Result<CommitResult, SinkError>
```
Shape it so MySQL and Postgres share the bisection and id-lookup driver, and only the statement building is per-sink.

- [ ] **Step 1: Failing unit tests**, using a fake executor: a closure that reports dup-key for configured rows, for both paths.
  - id path: `[a,b]`, then `[a,b,c]` gives `committed=[a,b,c]`.
  - bisection path: gives `committed=[c]` and `dead_lettered=[a,b]` with the reason text.
  - deadline passed: transient.
  - another permanent error dead-letters only that row.
  - another transient error makes the whole batch transient.
  - the `committed ∪ dead_lettered = batch` partition always holds.
- [ ] **Step 2: Run them to see them fail. Step 3: Implement. Step 4: Run** the unit tests and clippy. The sink-integration tests (§4.7 list, MySQL and Postgres) are run with `deploy/run-sink-integration-tests.sh` if Docker is available. Otherwise record that they were not run.
- [ ] **Step 5: Commit** as `fix(sql): a plain-mode duplicate key no longer livelocks the drain`.

---

## Integration track (on `fix/safety-floor`)

### Task 17: merge, docs, CI step

**Files:** the docs listed in spec §8 ("Docs to update"); `.github/workflows/ci.yml` (seccomp check step: `systemd-analyze syscall-filter @system-service | grep -qw flock`, skipped when `systemd-analyze` is absent); `CHANGELOG.md` `[Unreleased]` (Fixed / Added / Changed with the behaviour-change list from §8); `crates/weir-server/tests/docs_drift.rs` (a guard that the new metric names appear in `docs/monitoring.md`, and that `wab_dir_lock` appears in `docs/operations/configuration.md`).

- [ ] **Step 1:** Merge `sf/track-a`, `sf/track-b` and `sf/track-c` into `fix/safety-floor` (no fast-forward). Resolve the conflicts and wire Task 10's `orphan_cleanup` from Task 1's `lock_outcome`.
- [ ] **Step 2:** Run the whole workspace build and tests.
- [ ] **Step 3:** Write the docs and the CHANGELOG. Add the docs_drift guards and verify each by mutation.
- [ ] **Step 4: Commit** as `docs: safety floor -- ownership, signals, socket and drain behaviour changes`.

### Task 18: the full gate and the reproduction

- [ ] **Step 1:** Run the CONTRIBUTING.md pre-PR gate for real and save the output:
  - fmt;
  - the four clippy feature sets CI runs;
  - tests on default and all-features (server bins at `--test-threads=1`);
  - conformance;
  - `cargo deny fetch && cargo deny check`;
  - `cargo +1.88 check --all-features --locked`;
  - `WEIR_DST_SWEEP=300 cargo test -p weir-server --features dst dst`;
  - `mdbook build`.
- [ ] **Step 2:** `act -W .github/workflows/ci.yml -j lint` and `-j test`, where the local Docker supports them. Record what could not run and why.
- [ ] **Step 3:** Rebuild and re-run `spike_two_daemons.sh` against the branch binary. Expect B refused with `locked by`, A's 50 post-start acks all delivered, and SIGHUP survived.
- [ ] **Step 4:** Commit any fixes the gate forced, each with its own message.
