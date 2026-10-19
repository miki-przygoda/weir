# Operability Foundations Implementation Plan

This plan is split into this index and one file per PR (it is about 1.9 MB in total):

| PR | File | Tasks |
|---|---|---|
| 1 | [pr-1-status-foundations.md](pr-1-status-foundations.md) | 1.1-1.28 |
| 2 | [pr-2-drain-control.md](pr-2-drain-control.md) | 2.1-2.17 |
| 3 | [pr-3-startup-split.md](pr-3-startup-split.md) | 3.1-3.12 |
| 4 | [pr-4-lifecycle.md](pr-4-lifecycle.md) | 4.1-4.14 |
| 5 | [pr-5-http-health.md](pr-5-http-health.md) | 5.1-5.13 |
| 6 | [pr-6-admin-socket.md](pr-6-admin-socket.md) | 6.1-6.17 |
| 7 | [pr-7-weir-ctl.md](pr-7-weir-ctl.md) | 7.1-7.16 |
| 8 | [pr-8-deploy-and-docs.md](pr-8-deploy-and-docs.md) | 8.1-8.14 |
| 9 | [pr-9-chaos-harness.md](pr-9-chaos-harness.md) | 9.1-9.23 (9.22-9.23 owner-run) |

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship weir "Foundations" (SP-D + SP-F + SP-E): a wait-free status board and one shared Conditions evaluator (`weir-status`), an explicit drain lifetime with pause / resume / retry-stranded, observable non-blocking startup, bounded shutdown with a lame duck, `/livez` `/readyz` `/startupz` `/status` on the metrics listener, a private admin Unix socket, `weir-ctl status|reload|drain`, deploy defaults and alerts, and the chaos harness that gates the release.

**Architecture:** Every writer stores into single-writer atomics / `ArcSwap` cells on a `status::StatusBoard`; every reader (HTTP probes, `/status`, admin `status`, the 5 s poll task, offline `weir-ctl`) only loads and calls the one pure `weir_status::evaluate`, so a stalled reader can never delay an ack, a flusher or the drain. Control is intent (`DrainControl` atomics, the `ShutdownLevel` watch) and the board is observation; the drain owns the drain channel's only Receiver and the flushers its only Senders. Startup binds health and admin before recovery and hands the replay list to the drain in memory; shutdown is lame duck → bounded connection drain → joins → bounded drain budget → exit 0.

**Tech Stack:** Rust (edition 2024, MSRV 1.88), tokio 1.52 (`watch::Receiver::wait_for`), crossbeam-channel 0.5, prometheus-client 0.22, arc-swap 1.9 (now unconditional), serde 1 / serde_json 1, tracing / tracing-subscriber 0.3, libc 0.2 (weir-ctl, `cfg(unix)`); a new published crate `weir-status`; Python 3 stdlib for the chaos orchestrator; promtool (prom/prometheus v3.14.0) and `act` for the CI gates.

**Spec:** `docs/superpowers/specs/2026-10-04-operability-foundations-design.md` (revision 2; read its Owner review status block, §§1-5, the sections named in each PR, §16 and §18). Base: branch `feat/operability-foundations`, which already carries the complete safety floor (`wab/lock.rs`, `preflight.rs`, `reload.rs`, `socket::{probe_socket, BoundSocket, bind_hardened, remove_own_socket}`, `drain/ledger.rs`, the drain heartbeat `busy()` / `BusyGuard` / `BusyKind`, `drain_stall_verdict` with `SlowSinkCall` / `SlowLocalIo`). Reuse it; do not re-plan it.

## Global Constraints

- MSRV is **1.88** (`rust-version` in the workspace `Cargo.toml`); only `JoinHandle::is_finished`, `IsTerminal`, `OnceLock`, `AtomicU64::fetch_min` and tokio 1.52's `watch::Receiver::wait_for` are newer APIs the spec relies on (§13).
- Dependencies (§13): `arc-swap` becomes unconditional in weir-server; `serde_json` becomes a normal weir-server dependency; **no new third-party crates**; the new crate `weir-status` depends on `serde` and `serde_json` only (§5.1); weir-ctl gains `weir-status` and `libc = "0.2"` under `cfg(unix)` (already in the lock tree).
- Everything is additive (§2 G7, §13): no metric renames, no labels added to existing families, every new family pre-creates its full label set; the wire protocol, on-disk format, `HealthCheck`, existing metrics and existing `--json` shapes (including `attest`) are unchanged.
- Constants, not knobs (§13): `DRAIN_EXIT_GRACE` 2 s, `FINAL_HANDOFF_TIMEOUT` 1 s, the hand-off slice 100 ms, the settle wait 10 s, `REQUEUE_MIN_INTERVAL` 5 s, `HEAD_READ_SLOTS` 64, `HEAD_READ_TIMEOUT` 1 s, `PROBE_SLOTS` 4, request head ≤ 4096 bytes; the drain channel stays `bounded(256)`.
- `drain_shutdown_budget_secs`: default **10**, range **0..=3600**, CLI `--drain-shutdown-budget-secs`, env `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`; the budget starts after the flusher joins.
- `admin_socket_enabled`: default **true**, bool, no CLI, env `WEIR_ADMIN_SOCKET_ENABLED`.
- `admin_socket_path`: default `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`; absolute, no `..`, ≤ 103 B, not under `wab_dir`, ≠ `socket_path` (normalized + (dev, ino)); CLI `--admin-socket-path`, env `WEIR_ADMIN_SOCKET_PATH`.
- `livez_checks`: default `[]`, a subset of {`no_live_shard`, `listener_down`, `worker_exited`, `drain_stopped`, `drain_wedged`}; CLI `--livez-checks a,b`, env `WEIR_LIVEZ_CHECKS`.
- Orchestrator grace ≥ `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` = **45 s** at the defaults: systemd `TimeoutStopSec=45`, compose `stop_grace_period: 45s`, k8s `terminationGracePeriodSeconds: 45` (§7).
- Readiness = ingest-capable: recovery done, producer socket bound, not shutting down, not WAB-cap-rejecting, ≥ 1 shard up; sink and drain health never gate readiness; a failed TCP listener while Unix is up is not ready; `/livez` fails only on selected restart-fixable checks (§3, §12).
- Status document schema `weir.status/v1`; `ConditionCode` is snake_case with `#[serde(other)] Unknown`; the public document is a separate type with no free text (§5.1, §9.1).
- Admin socket: mode 0600, peer = daemon euid or root, JSON lines protocol v1 (`{"v":1,"id","verb","args"}`), 64 KiB lines, 5 s per request, 10 s idle, 64 requests and 4 connections per socket (§11).
- Exit status is **0** for a clean, forced or orphaned-drain exit (owner decision).
- Pause is in-memory only and takes effect before the next commit batch; a pause in force at SIGTERM is honoured (owner decisions).
- weir-server bin and system tests run with `--test-threads=1` (`bind_hardened` swaps the process umask).
- Commits: conventional subjects, **no trailers** (no Co-Authored-By, no session lines), never the CI-skip token in a message; PR bodies end with the Claude Code footer.
- Before pushing: run the real workflows with `act -W .github/workflows/ci.yml -j <job>` for every job, container jobs included; before merging: wait for every remote check, Windows included.
- Release: `weir-status` is published right after weir-core; `version` then appears **8** times in the root `Cargo.toml`; three lockfiles (root, chaos, fuzz) under `--locked`; reserve the `weir-status` crate name under the `miki-przygoda` account before the tag (a 404 is availability, not ownership).
- The safety-floor review fixes have landed on this branch's base (`2491883`): the lock-integrity ticker runs from lock acquisition through startup; drain resume paths check a (dev, ino) pin; local read errors escalate only after 3 strand cycles **and** 10 minutes; the non-Linux socket probe is stricter. The 2026-10-15 plan review re-checked every task that touches those areas against that code and rewrote Task 4.10 for the lock-integrity thread. A note that still says **re-check against the merged safety floor** means the implementer checks again before writing code.

## Review Focus

The five input classes or failure modes the spec implies that no drafted task tested, most likely first. Each now has a test in the task named.

1. **A restart while the WAB is over `wab_max_bytes`, with no producer pushing.** Expected: the first `/readyz` past startup is 503 `wab_cap_rejecting` (never a 200), and readiness returns on its own once the drain brings the WAB under the low-water mark (spec §16 system test 10, which no PR owned). Test: `a_restart_over_the_cap_is_not_ready_at_once_and_recovers_with_zero_pushes`, **Task 5.13**.
2. **A scrape before the first flusher hand-off or drain wait slice.** Expected: `weir_drain_channel_depth` reads 0 / its last real value, never PR 1's `DEPTH_UNPUBLISHED` sentinel (`usize::MAX` ≈ 1.8e19) on every daemon start. Test: the sentinel assertion in `drain_channel_depth_and_capacity_are_exported`, **Task 3.6**.
3. **SIGTERM while recovery is still running** (a long recovery under a k8s rollout). Expected: recovery finishes, the producer socket is never bound or served, no socket file remains, exit 0 within the bound. Only the chaos campaign exercised this path. Test: `sigterm_during_recovery_exits_zero_without_ever_serving`, **Task 4.10**.
4. **An operator who sets `livez_checks`.** Expected: `weir_live` (and so dashboards and alerts) agrees with `/livez`; PR 1's poll task hard-coded `[]` and no PR threaded the configured list into it. Test: `weir_live_follows_the_configured_livez_checks`, **Task 5.12**.
5. **The same daemon asked three ways** (admin `status`, `/status`, `/readyz`). Expected: one verdict (G2); PR 5 and PR 6 each planned their own renderer for the admin document. Test: `render_admin_doc_shares_readyz_s_predicate_and_carries_the_livez_checks` on the single shared `status::render::render_admin_doc`, **Task 1.17**.

## Execution layout

**PR dependency order** (spec §16 "PR sequence" and "Dependencies"; each PR leaves `main` green and releasable):

| Step | PRs | Worktrees | Merge order and why |
|---|---|---|---|
| 1 | PR 1 | one | Everything else codes against its types, board, registry, metrics and poll tasks. |
| 2 | PR 2 ∥ PR 3 | two parallel worktrees, both from PR 1 | **Merge PR 2 first, then rebase PR 3 onto it.** PR 3 runs Tasks 3.1-3.6 and 3.8-3.12 in its worktree; Task 3.7 starts only after PR 2 has merged and PR 3 is rebased. PR 3 Task 3.7's `DrainSpawner` replaces `main`'s `build_and_spawn_drain`, whose signature PR 2 Task 2.17 changes; the plan writes Task 3.7 against PR 2's `DrainRuntime` / `JoinHandle<DrainExit>`. Shared files: `crates/weir-server/src/status/poll.rs` (PR 2 Task 2.16 `drain_is_healthy` vs PR 3 Task 3.6 `spawn_queue_depth_task`; different functions), `crates/weir-server/src/main.rs` (PR 2 Task 2.17 vs PR 3 Tasks 3.3, 3.4, 3.6, 3.7, 3.11, 3.12) and `crates/weir-server/src/wab/mod.rs` (PR 2 makes `panic_message_str` `pub(crate)`, one line; PR 3 Tasks 3.2-3.6, 3.12). `drain/` is PR 2's alone; `socket/`, `sink/select.rs`, `wab/handoff.rs`, `wab/dst.rs` are PR 3's alone. |
| 3 | PR 4 | one, from PR 2 + PR 3 | **The** behaviour PR; system tests 1-5, 11 and 13 gate it in CI. |
| 4 | PR 5 ∥ PR 6 | two parallel worktrees, both from PR 4 | **Merge PR 5 first, then rebase PR 6 onto it.** PR 6 runs Tasks 6.1-6.13 in its worktree; Tasks 6.14-6.17 start only after PR 5 has merged and PR 6 is rebased. PR 6 Task 6.14 reads PR 5's `Config::livez_checks`. Shared files: `docs/security/threat-model.md` (5.11 vs 6.17), `crates/weir-server/src/main.rs` (5.10, 5.12 vs 6.8 step 4, 6.14 steps 7 and T3), the config layer `config/{mod,env,cli,file}.rs` and `status/config_view.rs` (5.3 vs 6.2), `metrics/mod.rs` and `docs/monitoring.md` (5.4 vs 6.3), `docs/operations/configuration.md` (5.11 vs 6.17), `crates/weir-status/src/lib.rs` (5.1 `mod public` vs 6.1 `pub mod admin`), `crates/weir-server/tests/system.rs` (5.10, 5.13 vs 6.14-6.15). Both consume PR 1's `render_admin_doc`, so neither depends on the other's renderer. |
| 5 | PR 7 | one, from PR 5 + PR 6 | weir-ctl over the admin socket, `/status` and disk. |
| 6 | PR 8 | one, from PR 7 | Deploy defaults, alerts, runbooks, docs_drift guards ("PR 8 after all"). |
| — | PR 9 | its own worktree | **Owner-run gate.** Tasks 9.1-9.4 (crash-path post-mortem) depend on nothing and can start any time; Tasks 9.5-9.21 after PR 4 (their contract tests pass once PR 4 and PR 6 are merged; rebase onto that `main` before review rather than skipping them); Tasks 9.22-9.23 are the owner on beast (root, dm-flakey, a `--features test-hooks` build) and must pass before the release tag. PR 9 touches only `chaos/`, so it shares no file with PRs 1-8. |

**Every task names its PR-level merge gate** (an italic line under its heading). A task's own "Run to verify it passes" step is its test cycle; the PR's **Merge gate** list (top of each PR section) is what the last task of the PR must leave green.

## Integrator reconciliations

The nine PR sections were drafted in parallel. Where they disagreed, the code in this tree wins for current behaviour, the spec for target behaviour, and an earlier PR's plan wins for any name a later PR consumes. Each fix below is applied in the PR text that follows (look for *Integrator reconciliation* notes):

1. **Merge order inside the parallel pairs:** PR 2 before PR 3; PR 5 before PR 6 (see Execution layout).
2. **PR 3 ← PR 1:** `board.shards` is `Arc<ShardRegistry>`; PR 1's shard-state parameters are `wab::spawn(.., registry: Arc<ShardRegistry>)` and `flusher_thread(<16>, slot: &ShardSlot)`. Task 3.3 moves `registry` onto `spawn_flushers`; Tasks 3.4 / 3.5 pass `&ShardSlot::detached()` before `handoff` (18 arguments; the expected compile error now reads "takes 17 arguments but 18").
3. **PR 3 ← PR 2:** Task 3.7's `DrainSpawnArgs` gains `runtime: drain::DrainRuntime`; `DrainSpawner::spawn` returns `JoinHandle<DrainExit>` and calls `drain::spawn_with` (`drain::spawn` is `#[cfg(test)]` after PR 2); `main` passes PR 2's `drain_runtime`.
4. **PR 3 ← PR 1:** PR 1's `SocketConfig.board` hook survives the bind/serve split, with one writer per mark (decision Q6): `socket::bind` marks the Unix listener `Bound` or `Failed`; `socket::bind_all` marks TCP `Bound` or `Failed`; `socket::serve` enters `Serving` (the phase's only writer) and marks Unix `Closed`; `socket::tcp::run` marks TCP `Closed` (lame duck) or `Failed`. `main` marks no listener, except `discard_listeners` on the pre-serving shutdown path, where `serve` never runs. Task 3.11 keeps PR 1's `Binding` phase.
5. **PR 3:** `publish_drain_channel_depth` skips PR 1's `DEPTH_UNPUBLISHED` sentinel (Review Focus 2); `refuse_if_live` carries a dead-code allowance until PR 6 calls it.
6. **PR 4 consumed-interfaces table** rewritten to the merged names: `LifecycleCell::{enter(Phase, now_ms) -> bool, forced(), ever_served()}`, `StatusBoard::{enter_phase, begin_shutdown, for_test_with}`, `listeners.{unix,tcp}.set(..)`, `wab.last_scan_ok_ms()` + `wab_bytes_now`, `status::poll::startup_cap_gate` (was `first_scan`), the 5 s task built from `WabPoll` / `PollStatus` (there is no `spawn_wab_poll_task`), `RecoverySummary { shard_dirs, segments_quarantined, segments_failed, elapsed }` (no `recovered_segments`), `spawn_flushers(.., registry, handoff: HandoffConfig)`, `socket::{bind_all, BoundListeners, serve}`, `DrainSpawnArgs`, `DrainExit.abandoned: Option<(String, u64)>`, `InFlight.segment: String`.
7. **PR 4 Task 4.1** registers `weir_config_drain_shutdown_budget_seconds` and `weir_drain_shutdown_deadline_seconds` (PR 1 Task 1.18 assigns them to PR 4; the draft assumed PR 1 registered them), sets the first through `publish_config`, classifies the key with PR 1's `v.public(..)` builder and drops PR 1's temporary `retain` in the ConfigView test.
8. **Duplicate removed — `test-hooks`:** PR 3 Task 3.12 owns the feature and module; PR 4 Task 4.2 now extends it (startup delay, compiled-out no-ops, the louder WARN, the release-never-ships assertion added to PR 3's guard) and keeps `fail_counter_scan(shard_id: u16)` (the draft's `usize` would not have matched `flusher_thread`'s `u16`).
9. **Duplicate removed — sticky `ShuttingDown` and the phase gauge:** PR 1 already has both; PR 4 Task 4.4 drops its `board.rs` edit and its own gauge writer and calls `StatusBoard::enter_phase` / `begin_shutdown`.
10. **Duplicate removed — `wait_until`:** PR 3's `main.rs` copy moves into `lifecycle.rs` (Task 4.9), keeping `FnMut`; `SETTLE_WAIT` replaces `SHARD_SETTLE_TIMEOUT`.
11. **Duplicate removed — producer binding:** PR 4 Task 4.10 uses PR 3's `bind_all` / `BoundListeners` instead of its own `ProducerListeners` / `bind_producer_listeners`; the main code block was rewritten concretely against PR 1-3 names (no `/* as PR n left it */` placeholders), and `socket_config_template` carries PR 1's `board` field.
12. **PR 4 Task 4.7:** the lame duck edits PR 3's `socket::serve` (not the test-only `run`), with `serve(config, BoundUnix, queue_tx, level_rx, metrics, sem, router: ShardRouter)` and `tcp::run(.., router, board)`; the Unix `Closed` mark goes through `config.board`.
13. **PR 4 Task 4.11:** the T3 `OnDisk` comes from `wab.last_scan_ok_ms()` + `wab_bytes_now`; T2 publishes the deadline (`LifecycleCell::set_drain_deadline`, `weir_drain_shutdown_deadline_seconds`).
14. **PR 4 Task 4.13** also fixes the systemd README's "stays ignored through WAB recovery" bullet (PR 8 Task 8.14 greps for it).
15. **Duplicate removed — grace guard:** PR 4 Task 4.14 names its consts `SYSTEMD_UNIT` / `SYSTEMD_README` / `COMPOSE` (PR 8's names); PR 8 Task 8.1 reuses them, replaces PR 4's guard with its stricter one and deletes `help_default`. `CI_YML` is declared once (PR 4 Task 4.2) and reused by PR 8 Task 8.2.
16. **One admin renderer:** `status::render::render_admin_doc(board, thresholds, livez_checks, now)` moves into PR 1 Task 1.17; PR 5's `public_doc` and PR 6's `status` verb both call it (Review Focus 5).
17. **PR 5 Task 5.1 ← PR 1:** the public projection uses PR 1's enum names (`DrainStateCode`, `DrainExitCode`, `SinkHealthCode`, `SinkFailureKind`, `ReloadTriggerCode`, `ReloadOutcomeCode`, `ReloadItemOutcome`, `AdminState`) and maps PR 1's `Option` fields to `Option` (`drain`, `sink`, `backlog`, `channel_depth`, `health`, `kind`, `state`); `PublicEvidenceValue` is `#[serde(untagged)]` in PR 1's variant order; `MAX_PUBLIC_*` alias PR 1's `MAX_DOC_*`.
18. **PR 5 Task 5.9 ← PR 1:** tests drive the board through `mark_up`, `mark_exit(SupervisorExit)`, `WorkerGuard`, `life.enter(Phase, now_ms)` and `SinkCell::set_health(&SinkHealth, Option<String>)`.
19. **PR 5 Task 5.12 (new):** `PollStatus::with_livez_checks` so `weir_live` follows the configured checks (Review Focus 4). **PR 5 Task 5.13 (new):** spec system test 10 (Review Focus 1).
20. **PR 6 ← PR 1:** `StatusBoard::for_test()` (was `for_tests`); `ReloadCell::publish(report, at_unix_ms)` / `evidence()` (was an `ArcSwapOption`), plus `at_unix_ms: 0` in PR 1's `ReloadReport` test literal; `AdminCell::{set_disabled, set_bound(String), set_unavailable(AdminReason)}` via a new `admin_reason` mapper (was `AdminCellState`); the pause uid goes through PR 1's `DrainCell::set_pause_requested(bool, Option<u32>)` (the draft's `AdminCell.paused_by_uid` / `NO_UID` and its `board.rs` / `render.rs` edits are dropped); `paused_since_ms`, `InFlight.segment`, `crate::status::{RequeueResult (at_ms), DrainExitInfo (no Default), DrainExitReason}`.
21. **PR 6 Task 6.1:** `weir-status` moves `serde_json` to `[dependencies]` (the envelope uses `serde_json::Value`); `PROTOCOL_VERSION` is `crate::ADMIN_PROTOCOL`, and a test pins `Verb::names()` to PR 1's `ADMIN_VERBS`.
22. **PR 6 Task 6.6** calls PR 3's `refuse_if_live(path, "admin socket")` before `bind_hardened_with` (spec §5.9 reuse; gives PR 3's helper its caller); **Task 6.14** builds `thresholds` / `livez_checks` in `main` from PR 5's config key and removes `refuse_if_live`'s dead-code allowance.
23. **PR 7 ← PR 1 / PR 6:** `weir_status::admin::default_socket_path` (PR 6's name; the draft said `derived_socket_path`); `StatusExit::code()` returns PR 1's `EXIT_*` constants and the parity test asserts it equals PR 1's `exit_code`; PR 1's `NoLatches` is re-exported instead of redefined; `recovery_pending` uses the catalogue's `affects: durability`; `drain.resume` "in effect" is PR 6's `resumed` key (no `observed` key needed); the parity fixtures also glob PR 1's `metrics_*.txt`.
24. **PR 8 ← PR 1:** `catalogue_row` reads `weir_status::entry(code)` fields (there are no `ConditionCode::{severity, affects, gates}` methods).
25. **Duplicate removed — threat-model rows:** PR 5 Task 5.11 and PR 6 Task 6.17 write their rows, actors, root bullet and daemon-uid qualification under the exact titles and phrases PR 8 Task 8.12's guard pins; PR 8 Task 8.12 keeps the guard and rewrites only the "Stuck pipeline on shutdown" row.
26. **Metric ownership:** `weir_drain_channel_depth` / `_capacity` (PR 3 Task 3.6), `weir_config_drain_shutdown_budget_seconds` / `weir_drain_shutdown_deadline_seconds` (PR 4 Task 4.1), `weir_http_requests_dropped_total` (PR 5 Task 5.4), `weir_admin_requests_total` / `weir_admin_socket_up` (PR 6 Task 6.3); PR 1 registers the other 34 families. Each registering task adds its `docs/monitoring.md` rows.

## Decisions on the integrator's open questions

Taken autonomously on 2026-10-15 (the owner's AFK grant covers planning and implementation). Each is applied in the PR files. The owner can reverse any of them; the PR file named is where the change would go.

| # | Question | Decision | Why | Applied in |
|---|---|---|---|---|
| Q1 | Merge order inside the parallel pairs | PR 2 before PR 3; PR 5 before PR 6. | Task 3.7 is written against PR 2's `DrainRuntime`; Task 6.14 reads PR 5's `Config::livez_checks`. The spec is silent. | Execution layout |
| Q3 | `AdminReason` has four values; PR 6 maps three bind-time causes onto them | Keep the four values and PR 6's mapping. The WARN log line carries the precise cause. | Config validation already refuses an `admin_socket_path` under `wab_dir` or equal to `socket_path`, so those reasons are reachable only through races; widening a closed set would also change PR 1's golden file. | PR 6 |
| Q4 | One TERM/INT/HUP loop (spec §5.6) or the safety floor's own SIGHUP task | Keep the safety floor's SIGHUP task as the only HUP handler. `Lifecycle` handles TERM and INT. | That task is reviewed and tested, it sets `SIG_IGN` before the runtime starts, and one handler per signal rules out a double reload. | PR 4, PR 6 |
| Q5 | Lame duck and the T3 summary | Idle accepted connections still get EOF at lame duck (today's behaviour). The T3 summary states the re-dead-letter window without a count; `DrainExit` is unchanged. | Holding idle connections open adds a code path for no ack-safety gain. A dead-letter count can be added later without changing anything else. | PR 4 |
| Q6 | Listener marks written twice (by `socket::serve` and by `main`) | One writer per mark: `socket::bind` marks Unix `Bound` / `Failed`; `socket::bind_all` marks TCP `Bound` / `Failed`; `socket::serve` enters `Serving` (its only writer; there is no `mark_serving` in `main`) and marks Unix `Closed`; `socket::tcp::run` marks TCP `Closed` / `Failed`. `main` marks nothing, except `discard_listeners` on the pre-serving shutdown path. | Two writers for one fact is how a status board starts to disagree with itself. | PR 1, PR 3, PR 4 |
| Q7 | What `weir_wab_segments_not_handed_off_total` counts | Count both causes under a `reason` label (`budget`, `disconnected`), with both label values created up front. Measure the budget in wall time from the injected clock, not in counted 100 ms attempts. | The family is new, so the label is additive. An operator needs to tell "the budget ran out" from "the drain was gone". | PR 3 |
| Q8 | PR 6 is stricter than spec §11 | Keep both: refuse a parent directory owned by another non-root uid, and leave `instance` out of the `forbidden` reply. **Flagged for the owner's security review** as deviations from §11. | Both reduce what an unauthorised peer can do or learn. | PR 6 |
| Q9 | `weir-ctl status` exit code when no daemon answered | Follow spec §14: exit 2 only when no daemon answered **and** there are no offline facts. When disk facts are printed, the exit code comes from evaluating them. An identity mismatch stays terminal, with no fallback to `/status`. | The spec is explicit. Answering for a different daemon is worse than not answering. | PR 7 |
| Q11 | PR 2's counting and test sizes | Accepted: `weir_drain_segments_abandoned_total` counts every exit that leaves a segment partly delivered. The alternation test uses 30 segments. A respawn while `Held` resumes past the committed prefix. PR 8's docs state the last point. | Each is equal to or stricter than the spec. 30 segments prove the alternation at a CI-friendly cost. | PR 2, PR 8 |
| Q12 | PR 5's literal failure-line formats | Follow spec §10 literally. Rewrite `endpoint_caps_concurrent_connections`. | The spec fixes the format, and §5.8 removes the idle-peer premise of the old test. | PR 5 |
| Q13 | System test 1 timing | 100 ms per POST, not 200 ms. | It still proves backlog and live traffic alternate, at about half the CI time. | PR 4 |
| Q15 | Lame-duck order (PR 4): spec §5.7 says drop the listener, then unlink | Unlink the socket path first (inode-checked through `remove_own_socket`), then drop the listener. | A new connection then fails at once with `ENOENT` instead of reaching a listener that is about to close. The review checked this order against `remove_own_socket` at `3adc5f5`. | PR 4 |
| Q14 | Tasks touching the safety floor's late fixes | Re-checked in the 2026-10-15 plan review against `2491883`. | The fixes have landed. | all |

## Owner questions (not decided here)

- **Q2: the `weir-status` crate name.** A 404 on crates.io means unclaimed, not owned. Reserve it under `miki-przygoda` before the release tag. `version` then appears 8 times in the root `Cargo.toml`. PR 1 Task 1.1 updates `RELEASING.md`: "Nine crates" becomes "Ten crates", and the publish order is the one the RELEASING.md snippet prints once weir-ctl and weir-server depend on weir-status.
- **Q10: the chaos campaign's reading of the spec.** It treats every lifecycle trigger as ending in a dm-flakey power cut, fails the gate on any anomaly unless `--allow-anomalies` is passed, and stalls the sink with SIGSTOP on the recorder. The owner runs it on beast (Tasks 9.22-9.23), so the owner decides.
- **Versions.** 4.1.0 for the safety floor and 4.2.0 for Foundations are recommended, not decided.
- **The spec's 16 autonomous decisions** (its Owner review status table) still need the owner's confirmation.
