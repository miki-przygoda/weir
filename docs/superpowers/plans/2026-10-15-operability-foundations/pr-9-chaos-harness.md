# Operability Foundations — PR 9: Chaos harness for the lifecycle — oracle fix, crash-path post-mortem, four lifecycle fault kinds, schedules, campaign gate (OWNER-RUN campaign)

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 9: Chaos harness for the lifecycle — oracle fix, crash-path post-mortem, four lifecycle fault kinds, schedules, campaign gate (OWNER-RUN campaign)

**Scope (spec §16, "Chaos", items 1-3, plus the schedules and the gate):**
1. **Oracle fix.** Today the final stop relies on SIGTERM running a full drain (`final_pass` docstring, chaos/orchestrator/run.py:404-451; `Daemon.stop`, run.py:788-808). After PR 4 the drain is bounded by `drain_shutdown_budget_secs` (default 10 s), so the `frontier_slack=0` verify would report records sitting intact on disk as lost. This PR does both remedies the spec names. It pins `--drain-shutdown-budget-secs 3600` on every daemon the harness starts. Every schedule that sets a smaller budget then gets a drain-out restart that waits for `weir_wab_undelivered_segments == 0` before the zero-slack verify.
2. **New fault kinds:** `sigterm_at`, `double_sigterm`, `sigterm_in_recovery`, `admin_pause_resume`, each with orchestrator unit tests.
3. **Prerequisite:** today `stack.teardown()` destroys the WAB post-mortem on the crash path (MEMORY: "`stack.teardown()` runs on the crash path but the WAB post-mortem does not (and no SIGTERM handler)").
4. **Schedules:** `shutdown-bounded`, `shutdown-forced`, `startup-signal`, `pause-resume-cut`.
5. **Gate:** at least 1,000 cuts in total and at least 200 per schedule, 0 oracle violations, the canary firing on every episode, and the duplicate ratio reported against the phase-2 baseline.

**Depends on**
- **PR 4:** `drain_shutdown_budget_secs` and `--drain-shutdown-budget-secs`, bounded shutdown, lame duck, the second-signal Forced path, the log lines "weir serving" (§6 step 12) and "forced shutdown" (§7), the `test-hooks` feature with `WEIR_TEST_HOOKS` / `WEIR_TEST_STARTUP_DELAY_MS`, and exit 0 on a forced or orphaned exit.
- **PR 1 / PR 2 metrics:** `weir_startup_phase{phase}`, `weir_wab_undelivered_segments`, `weir_drain_paused`.
- **PR 6:** the admin socket, `--admin-socket-path`, protocol v1 `drain.pause` / `drain.resume`.
- **Ordering:**
  - Tasks 9.1-9.4 (the teardown prerequisite) depend on nothing and can land first.
  - Tasks 9.5 onward can be written after PR 4, but `TestDaemonCliContract` (Task 9.5; both its flag tests, needing PR 4 and PR 6) and `TestDaemonLogContract` (Task 9.11; needing PRs 1, 2 and 4) only pass once those PRs have merged.
  - The campaign (Tasks 9.22-9.23) must pass before the 4.1.0 tag (see the ship-order note in the spec's owner-review block: it may be 4.2.0).

**Behaviour change:** none in weir. This is harness-only: Python stdlib, no new dependency, and the chaos Rust crate and its lockfile are unchanged. Every pre-existing schedule keeps its fault, argv and records byte-identical, except that `--drain-shutdown-budget-secs 3600` is now always passed. That flag restores the 4.0 full-drain stop which those schedules' final pass was written against.

**Merge gate**
- **PR merge (agent-checkable):** the orchestrator suite is green (`cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`), `cd chaos && cargo check --all-targets --locked` passes, and `act -W .github/workflows/ci.yml -j harnesses` passes locally.
- **Release gate (OWNER on beast, root plus dm-flakey):** `python3 orchestrator/gate.py <4 run dirs>` exits 0, followed by the write-up. Chaos is a manual pre-release gate, not a per-PR CI gate (owner decision, spec owner-review block).

**Who does what**

| Tasks | Who | Where |
|---|---|---|
| 9.1-9.21 | **Agent, locally** (macOS or Linux, no root; stdlib `unittest`) | worktree |
| 9.22-9.23 | **OWNER on beast** (root, `dm-flakey`, release build with `--features test-hooks`) | beast |

**Reconciliations (code wins for current behaviour; spec wins for target behaviour)**
- **Pinning the budget.** The spec suggests the env var `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS=3600`. The harness passes **every** weir knob as a CLI flag, and `TestDaemonCliContract` (test_run.py:513-564) pins those flags against cli.rs. An env var would escape that contract, so the pin is the CLI flag `--drain-shutdown-budget-secs` (spec §13 defines both).
- **Where the tests live.** The spec places the orchestrator unit tests in `test_run.py`. Fault-kind dispatch, schedule validation, `Daemon` and `final_pass` tests stay in `test_run.py`. The trigger choreography moves to a new `faults.py` module because run.py is already 1,437 lines, so its tests go in `test_faults.py`, with `test_admin.py` and `test_gate.py` alongside. CI's discover glob (ci.yml:201) already picks all three up.
- **What "cut" and "canary" mean.** A "cut" is a dm-flakey power cut, as in the phase-2 corpus ("6,129 cuts"). "The canary firing every episode" is I6 (`canary_verdict`, run.py:338-362). Every lifecycle trigger therefore **ends in a power cut**, at a seeded point inside or after the shutdown or startup it provoked.
- **How a lifecycle episode is recorded.**
  - The record's `fault` field carries the **oracle** fault: `power_loss` when the cut is a power cut.
  - A new `trigger` field carries the lifecycle kind.
  - Because `fault` stays `power_loss`, `verify.check_counts`'s tier exemption, `report.powerloss_verdict` and the quarantine exemption in `final_pass` (run.py:635-638) keep working with no change.
- **Log text.** The spec's log strings ("weir serving", "forced shutdown") and the metric names come from PRs 1, 2, 4 and 6. Each is held in exactly one constant, and a contract test scrapes `crates/weir-server/src` for it. If the merged text differs, change the constant, not the daemon.
- **Checked against the merged safety floor (2491883), 2026-10-15:** (1) the lock-integrity thread (main.rs:817-845, spawned at main.rs:998-1004 right after acquisition) runs through preflight, recovery and the replay, and exits only with `EXIT_WAB_OWNERSHIP_LOST = 3` (wab/lock.rs:171) on `LostToOther`. `sigterm_in_recovery` reaps the SIGKILLed daemon (`Daemon.kill9` waits) before it spawns the next, so the flock is free and no `LostToOther` can arise. An exit 3 in any trigger episode is still an anomaly (`shutdown_verdict`). (2) The drain's `(dev, ino)` pins (drain/ledger.rs) are in-memory, per process. Every harness cut restarts the daemon, and pause/resume never respawns the drain, so no trigger exercises them. (3) The stricter probe (socket/mod.rs:810-822) is non-Linux only. The campaign runs on Linux, and admin.py's tests use a Python server. (4) A stale producer or admin socket file left by a SIGKILL or a power cut is removed by `bind_hardened`'s `remove_stale_socket_at_startup` (socket/mod.rs:433-435).

All paths below are relative to the worktree root `/Users/miki_przygoda/Projects/GitHub-Projects/weir/.claude/worktrees/fnd`. Commits carry **no** trailers. Never write the CI-skip token in a message.

---

### Task 9.1: Crash-path WAB post-mortem before teardown

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

The prerequisite. Today an exception that escapes the episode loop goes straight to the `finally` (run.py:1308-1381). That block stops the processes, calls `stack.teardown()` (which unmounts the device and deletes `wab.img`), and only then renders the report. `final_pass` step 5 never ran, so nothing records what weir left on disk.

**Files:**
- Modify: `chaos/orchestrator/run.py`:
  - new constant and two functions after `cumulative_deltas`, around line 386;
  - `main()`: `wab_dir = None` and `final_pass_ran = False` before `try:` (line 898);
  - an `except BaseException` before `finally:` (line 1308);
  - the leftovers check (lines 855-860);
  - the `finally` block, between the recorder stop and `stack.teardown()` (lines 1337-1354).
- Test: `chaos/orchestrator/test_run.py` (new class `TestCrashPostmortem`, appended).

**Interfaces:**
- Consumes:
  - `quiescence.scan_wab_residue(wab_dir) -> Residue`;
  - `Residue(unconfirmed_sealed, nonempty_active, quarantined, dead_letter, survivors)`.
- Produces:
  - `run.CRASH_POSTMORTEM_FILE: str = "crash-postmortem.json"`
  - `run.crash_postmortem(wab_dir: str | None, reason: str, residue_fn=None) -> dict` — keys `reason`, `wab_dir`, and then either `wab_residue{unconfirmed_sealed,nonempty_active,quarantined,dead_letter}`, `wab_survivor_count`, `wab_survivors[:50]` or `wab_scan_error`
  - `run.write_crash_postmortem(run_dir: str, record: dict) -> str` (returns the path)

- [ ] **Write the failing test** (append to `chaos/orchestrator/test_run.py`)

```python
class TestCrashPostmortem(unittest.TestCase):
    """The prerequisite (spec §16 chaos item 3): an exception escaping the
    episode loop went straight to stack.teardown(), which unmounted the WAB
    and deleted wab.img with no post-mortem. The one piece of evidence that
    explains a crash was destroyed unread."""

    SURVIVORS = (
        {"kind": "unconfirmed_sealed",
         "path": "/w/shard_0/seg_00000003.wab.sealed", "size": 4096},
        {"kind": "nonempty_active",
         "path": "/w/shard_1/seg_00000007.wab", "size": 900},
    )

    def test_survivors_are_recorded_with_the_reason(self):
        residue = quiescence.Residue(1, 1, 0, 0, self.SURVIVORS)
        rec = run.crash_postmortem(
            "/w", "RuntimeError: boom", residue_fn=lambda _d: residue)
        self.assertEqual(rec["reason"], "RuntimeError: boom")
        self.assertEqual(rec["wab_dir"], "/w")
        self.assertEqual(rec["wab_residue"], {
            "unconfirmed_sealed": 1, "nonempty_active": 1,
            "quarantined": 0, "dead_letter": 0,
        })
        self.assertEqual(rec["wab_survivor_count"], 2)
        self.assertEqual(
            rec["wab_survivors"][0]["path"], "/w/shard_0/seg_00000003.wab.sealed")

    def test_a_failing_scan_is_recorded_not_raised(self):
        def boom(_d):
            raise OSError("EIO")
        rec = run.crash_postmortem("/w", "x", residue_fn=boom)
        self.assertEqual(rec["wab_scan_error"], "OSError: EIO")
        self.assertNotIn("wab_residue", rec)

    def test_a_run_that_died_before_the_wab_existed_says_so(self):
        rec = run.crash_postmortem(None, "x", residue_fn=lambda _d: 1 / 0)
        self.assertIn("no WAB directory", rec["wab_scan_error"])

    def test_the_survivor_list_is_capped_at_fifty_but_counted_in_full(self):
        many = tuple(
            {"kind": "unconfirmed_sealed", "path": f"/w/s/{n}", "size": 1}
            for n in range(80)
        )
        rec = run.crash_postmortem(
            "/w", "x", residue_fn=lambda _d: quiescence.Residue(80, 0, 0, 0, many))
        self.assertEqual(rec["wab_survivor_count"], 80)
        self.assertEqual(len(rec["wab_survivors"]), 50)

    def test_it_is_written_beside_the_episode_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = run.write_crash_postmortem(tmp, {"reason": "x", "wab_dir": None})
            self.assertEqual(path, os.path.join(tmp, run.CRASH_POSTMORTEM_FILE))
            with open(path) as f:
                self.assertEqual(json.load(f)["reason"], "x")

    def _main_source(self):
        run_py = os.path.join(os.path.dirname(os.path.abspath(run.__file__)), "run.py")
        with open(run_py) as f:
            source = f.read()
        return source[source.index("def main("):]

    def test_main_runs_it_before_teardown_and_only_when_the_final_pass_did_not(self):
        body = self._main_source()
        fin = body[body.index("\n    finally:\n"):]
        self.assertIn("crash_postmortem(", fin)
        # Anchored on the 12-space-indented CALL line: the finally block's own
        # comments mention `stack.teardown()` before the call does.
        self.assertLess(
            fin.index("crash_postmortem("), fin.index("\n            stack.teardown("),
            "the post-mortem must read the WAB before teardown unmounts it",
        )
        self.assertIn("if not final_pass_ran", fin)
        self.assertIn("final_pass_ran = True", body)

    def test_a_leftover_crash_file_blocks_a_rerun_of_the_seed(self):
        body = self._main_source()
        self.assertIn(
            "(ledger_path, delivered_path, episodes_path, crash_path)", body,
            "a stale crash-postmortem.json would be rendered into the NEXT "
            "run's report as though that run had crashed",
        )
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestCrashPostmortem -v`
  - Expected: `AttributeError: module 'run' has no attribute 'crash_postmortem'` for the unit tests, and `AssertionError` for the two source tests.

- [ ] **Implement.** In `run.py`, after `cumulative_deltas`:

```python
#: Where the crash-path post-mortem lands, beside episodes.jsonl. A separate
#: file and NOT an episodes.jsonl row: report.py and main()'s closing count
#: treat every non-"final" row as an episode, and this is not one.
CRASH_POSTMORTEM_FILE = "crash-postmortem.json"


def crash_postmortem(wab_dir, reason, residue_fn=None):
    """The WAB post-mortem for a run that ended on an exception.

    `final_pass` step 5 is the only other place the WAB is scanned before
    `stack.teardown()` unmounts the device and deletes `wab.img`. A run that
    raised out of the episode loop never reaches it, so the evidence of what
    weir left on disk was destroyed unread, on exactly the runs where it is
    most needed. Same scanner and the same 50-row cap as the final pass, so
    the two records read alike. Never raises: a failed scan is recorded.
    """
    residue_fn = residue_fn or quiescence.scan_wab_residue
    record = {"reason": reason, "wab_dir": wab_dir}
    if wab_dir is None:
        record["wab_scan_error"] = (
            "no WAB directory was created before the run failed"
        )
        return record
    try:
        residue = residue_fn(wab_dir)
    except Exception as exc:
        record["wab_scan_error"] = f"{type(exc).__name__}: {exc}"
        return record
    record["wab_residue"] = {
        "unconfirmed_sealed": residue.unconfirmed_sealed,
        "nonempty_active": residue.nonempty_active,
        "quarantined": residue.quarantined,
        "dead_letter": residue.dead_letter,
    }
    survivors = list(residue.survivors)
    record["wab_survivor_count"] = len(survivors)
    record["wab_survivors"] = survivors[:50]
    return record


def write_crash_postmortem(run_dir, record):
    """Writes `record` to `run_dir/crash-postmortem.json`; returns the path."""
    path = os.path.join(run_dir, CRASH_POSTMORTEM_FILE)
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    return path
```

  In `main()`, make these edits:
  1. **Leftovers check.** Add `crash_path = os.path.join(run_dir, CRASH_POSTMORTEM_FILE)` next to `episodes_path`, and change the loop at line 855 to `for path in (ledger_path, delivered_path, episodes_path, crash_path):`.
  2. **State before the `try`.** Just before `try:` (line 898), add `wab_dir = None`, `final_pass_ran = False` and `crash_reason = None`. Line 905 still assigns `wab_dir` inside the `try`.
  3. **Mark the final pass.** Directly after `final_record, v, a = final_pass(...)` (line 1291), add `final_pass_ran = True`.
  4. **Record the crash.** Between the end of the `try` body and `finally:`, add:
     ```python
         except BaseException as exc:
             # Recorded for the crash post-mortem in `finally`, then re-raised
             # unchanged: this is evidence capture, not error handling.
             crash_reason = f"{type(exc).__name__}: {exc}"
             raise
     ```
  5. **Run the post-mortem.** In `finally:`, after the recorder `try` block and immediately before the `stack.teardown()` `try` block, add:
     ```python
         if not final_pass_ran:
             try:
                 pm = crash_postmortem(
                     wab_dir, crash_reason or "the run ended before its final pass")
                 pm_path = write_crash_postmortem(run_dir, pm)
                 print(f"crash path: WAB post-mortem written to {pm_path}",
                       file=sys.stderr)
             except Exception as exc:
                 print(f"cleanup: crash post-mortem failed: {exc}", file=sys.stderr)
     ```
     (Task 9.3 adds `image_kept` together with `keep_image=not final_pass_ran`.)

- [ ] **Run to verify it passes**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestCrashPostmortem -v`, then the full suite `python3 -m unittest discover -p 'test_*.py'`.
  - Expected: `OK`.
- [ ] **Commit:** `fix(chaos): post-mortem the WAB on the crash path before teardown`

---

### Task 9.2: Render the crash post-mortem into report.md

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/report.py`:
  - new `render_crash_postmortem` after `render_final_pass` (line 47);
  - `render(episodes, meta)` gains `crash=None` and inserts the section just before `if final is not None:` (line 538);
  - `write_report` (lines 742-754) loads the file.
- Test: `chaos/orchestrator/test_report.py` (new class `TestCrashPostmortemSection`).

**Interfaces:**
- Consumes: `run.CRASH_POSTMORTEM_FILE`. This is duplicated as `report.CRASH_POSTMORTEM_FILE`, because report.py must not import run.py, which imports report.
- Produces:
  - `report.CRASH_POSTMORTEM_FILE = "crash-postmortem.json"`
  - `report.render_crash_postmortem(crash: dict) -> list[str]`
  - `report.render(episodes, meta, crash=None) -> str`
  - `report.load_crash_postmortem(run_dir) -> dict | None`

- [ ] **Write the failing test**

```python
CRASH = {
    "reason": "RuntimeError: boom", "wab_dir": "/mnt/weir-wab/wab",
    "image_kept": "/chaos/runs/7/wab.img",
    "wab_residue": {"unconfirmed_sealed": 1, "nonempty_active": 0,
                    "quarantined": 0, "dead_letter": 0},
    "wab_survivor_count": 1,
    "wab_survivors": [{"kind": "unconfirmed_sealed",
                       "path": "/mnt/weir-wab/wab/shard_0/seg_00000003.wab.sealed",
                       "size": 4096}],
}


class TestCrashPostmortemSection(unittest.TestCase):
    def test_no_crash_renders_no_section(self):
        self.assertNotIn("## Crash post-mortem", report.render(EPISODES, {}))

    def test_the_section_names_the_reason_the_image_and_every_survivor(self):
        out = report.render(EPISODES, {}, crash=CRASH)
        self.assertIn("## Crash post-mortem", out)
        self.assertIn("RuntimeError: boom", out)
        self.assertIn("/chaos/runs/7/wab.img", out)
        self.assertIn("seg_00000003.wab.sealed", out)
        self.assertIn("4096", out)

    def test_a_scan_error_is_shown_rather_than_an_empty_table(self):
        out = report.render(EPISODES, {}, crash={
            "reason": "x", "wab_dir": None,
            "wab_scan_error": "no WAB directory was created before the run failed"})
        self.assertIn("no WAB directory", out)

    def test_write_report_picks_up_the_crash_file(self):
        import json, os, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "episodes.jsonl"), "w") as f:
                f.write(json.dumps(EPISODES[0]) + "\n")
            with open(os.path.join(tmp, report.CRASH_POSTMORTEM_FILE), "w") as f:
                json.dump(CRASH, f)
            with open(report.write_report(tmp)) as f:
                self.assertIn("## Crash post-mortem", f.read())
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_report.TestCrashPostmortemSection -v`
  - Expected: `TypeError: render() got an unexpected keyword argument 'crash'`, and `AttributeError: module 'report' has no attribute 'CRASH_POSTMORTEM_FILE'`.

- [ ] **Implement**

```python
#: Mirrors run.CRASH_POSTMORTEM_FILE (report.py must not import run.py).
CRASH_POSTMORTEM_FILE = "crash-postmortem.json"


def render_crash_postmortem(crash):
    """The WAB as `stack.teardown()` was about to unmount it, for a run that
    ended on an exception before its final pass."""
    lines = ["## Crash post-mortem\n"]
    lines.append(
        "The run ended on an exception before its final pass. This is what weir "
        "left on disk at that moment: the only record of it, because teardown "
        "unmounts the device next.\n"
    )
    lines.append(f"- **Reason:** {crash.get('reason')}")
    lines.append(f"- **WAB dir:** `{crash.get('wab_dir')}`")
    if crash.get("image_kept"):
        lines.append(
            f"- **Image kept for forensics:** `{crash['image_kept']}` "
            "(loop-mount it read-only to inspect)")
    if crash.get("wab_scan_error"):
        lines.append(f"- **Scan error:** {crash['wab_scan_error']}")
    residue = crash.get("wab_residue") or {}
    for key in ("unconfirmed_sealed", "nonempty_active", "quarantined", "dead_letter"):
        lines.append(f"- {key}: {_dash(residue.get(key))}")
    survivors = crash.get("wab_survivors") or []
    if survivors:
        lines.append("")
        lines.append("| Kind | Path | Size (bytes) |")
        lines.append("|---|---|---|")
        for s in survivors[:MAX_SURVIVORS_SHOWN]:
            lines.append(f"| {s.get('kind')} | `{s.get('path')}` | {s.get('size')} |")
        hidden = crash.get("wab_survivor_count", len(survivors)) - min(
            len(survivors), MAX_SURVIVORS_SHOWN)
        if hidden > 0:
            lines.append(f"\n…and {hidden} more (see {CRASH_POSTMORTEM_FILE}).")
    lines.append("")
    return lines


def load_crash_postmortem(run_dir):
    path = os.path.join(run_dir, CRASH_POSTMORTEM_FILE)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)
```

  - `render(episodes, meta, crash=None)`: immediately before `if final is not None:` (line 538), add `if crash is not None: lines.extend(render_crash_postmortem(crash))`.
  - `write_report`: `f.write(render(episodes, meta, crash=load_crash_postmortem(run_dir)))`.

- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_report -v`. Expected: `OK`, with the existing tests unchanged.
- [ ] **Commit:** `feat(chaos): render the crash post-mortem in report.md`

---

### Task 9.3: Keep `wab.img` when a run crashes

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify:
  - `chaos/orchestrator/dm_stack.py`: `StorageStack.teardown` (lines 202-252);
  - `chaos/orchestrator/run.py`: the `stack.teardown()` call in `main()`'s `finally` (line 1352), and a new `image_kept` line added to Task 9.1's crash post-mortem block.
- Test:
  - `chaos/orchestrator/test_dm_stack.py`: class `TestTeardownStackOrder`, line 459;
  - `chaos/orchestrator/test_run.py`: one assertion added to `TestCrashPostmortem`.

**Interfaces:**
- Produces: `StorageStack.teardown(self, keep_image: bool = False) -> None`.

- [ ] **Write the failing test** (add to `TestTeardownStackOrder` in test_dm_stack.py)

```python
    def test_keep_image_detaches_but_leaves_the_backing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            open(img, "wb").close()
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt)
            s.loop_device = "/dev/loop7"
            fake = _fake_run({})
            with mock.patch.object(dm_stack, "_run", fake):
                s.teardown(keep_image=True)
            self.assertEqual([c[0] for c in fake.calls], ["umount", "losetup"])
            self.assertTrue(os.path.exists(img), "the crash evidence must survive")
            self.assertIsNone(s.loop_device)

    def test_default_teardown_still_removes_the_image(self):
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            open(img, "wb").close()
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt)
            s.loop_device = "/dev/loop7"
            with mock.patch.object(dm_stack, "_run", _fake_run({})):
                s.teardown()
            self.assertFalse(os.path.exists(img))
```

  Also add to `TestCrashPostmortem` in test_run.py:

```python
    def test_teardown_keeps_the_image_on_the_crash_path(self):
        fin = self._main_source()
        fin = fin[fin.index("\n    finally:\n"):]
        # The call line, not the comment at the top of `finally` that names it.
        call = fin[fin.index("\n            stack.teardown("):][:80]
        self.assertIn("keep_image=not final_pass_ran", call)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_dm_stack.TestTeardownStackOrder test_run.TestCrashPostmortem -v`
  - Expected: `TypeError: teardown() got an unexpected keyword argument 'keep_image'`, and an `AssertionError` from the source test.
- [ ] **Implement**
  - dm_stack.py: change the signature to `def teardown(self, keep_image=False):`. Extend the docstring: "`keep_image=True` (the crash path) unmounts and detaches but leaves the backing file, so the WAB a crashed run left behind can be loop-mounted and read afterwards." Replace the final two lines with:
    ```python
        if keep_image:
            print(
                f"teardown: kept {self.backing_file} for forensics "
                f"(`losetup -fP --show -r {self.backing_file}` then mount it read-only)",
                file=sys.stderr,
            )
            return
        if os.path.exists(self.backing_file):
            os.remove(self.backing_file)
    ```
  - run.py `finally`: `stack.teardown(keep_image=not final_pass_ran)`.
  - run.py `finally`, in the crash post-mortem block from Task 9.1, right after `pm = crash_postmortem(...)`:
    ```python
                 pm["image_kept"] = stack.backing_file if os.path.exists(stack.backing_file) else None
    ```
    The path is recorded only if the file exists: an exception before or inside `stack.setup()` never created it, and report.md must not point the operator at an image that is not there.
- [ ] **Run to verify it passes:** the same command, then the full suite. Expected: `OK`.
- [ ] **Commit:** `fix(chaos): keep the WAB image when a run crashes`

---

### Task 9.4: A SIGTERM to run.py unwinds through teardown

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

Today Python's default SIGTERM disposition kills run.py **without** running `finally`. That strands the loop device, the dm mapping and the mount, and skips the post-mortem; this is why run-soak.sh:41-43 uses `timeout -s INT`. After this task, SIGTERM and SIGHUP raise an exception like SIGINT already does.

**Files:**
- Modify: `chaos/orchestrator/run.py` (a new exception class and function after `LOADGEN_STOP_TIMEOUT_SECS`, line 57; one call at the top of `main()` after the euid check, line 817).
- Test: `chaos/orchestrator/test_run.py` (new class `TestInterruptHandlers`).

**Interfaces:**
- Produces:
  - `class run.HarnessInterrupted(BaseException)`
  - `run.install_interrupt_handlers(signal_fn=signal.signal, getsignal_fn=signal.getsignal) -> list[signal.Signals]`

- [ ] **Write the failing test**

```python
class TestInterruptHandlers(unittest.TestCase):
    def _install(self, current):
        installed = {}

        def signal_fn(sig, handler):
            installed[sig] = handler

        def getsignal_fn(sig):
            return current.get(sig, signal.SIG_DFL)

        sigs = run.install_interrupt_handlers(
            signal_fn=signal_fn, getsignal_fn=getsignal_fn)
        return sigs, installed

    def test_sigterm_raises_so_the_finally_block_runs(self):
        sigs, installed = self._install({})
        self.assertIn(signal.SIGTERM, sigs)
        with self.assertRaises(run.HarnessInterrupted):
            installed[signal.SIGTERM](signal.SIGTERM, None)

    def test_it_is_a_BaseException_so_no_except_Exception_swallows_it(self):
        # final_pass wraps acc.check in `except Exception`; a SIGTERM landing
        # there must not be recorded as "final_check_failed" and carry on.
        self.assertFalse(issubclass(run.HarnessInterrupted, Exception))

    def test_a_second_signal_during_cleanup_is_ignored(self):
        _, installed = self._install({})
        with self.assertRaises(run.HarnessInterrupted):
            installed[signal.SIGTERM](signal.SIGTERM, None)
        self.assertIsNone(installed[signal.SIGTERM](signal.SIGTERM, None))
        self.assertIsNone(installed[signal.SIGHUP](signal.SIGHUP, None))

    def test_a_nohup_ignored_hangup_stays_ignored(self):
        sigs, installed = self._install({signal.SIGHUP: signal.SIG_IGN})
        self.assertNotIn(signal.SIGHUP, installed)
        self.assertNotIn(signal.SIGHUP, sigs)

    def test_main_installs_them_first(self):
        run_py = os.path.join(os.path.dirname(os.path.abspath(run.__file__)), "run.py")
        with open(run_py) as f:
            body = f.read()
        body = body[body.index("def main("):]
        self.assertLess(
            body.index("install_interrupt_handlers()"), body.index("stack.setup()"))
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestInterruptHandlers -v`
  - Expected: `AttributeError: module 'run' has no attribute 'install_interrupt_handlers'`.
- [ ] **Implement**

```python
class HarnessInterrupted(BaseException):
    """A termination signal reached run.py.

    Raised FROM the signal handler so `main()`'s `finally` runs: the WAB
    post-mortem, the teardown of the loop device, dm mapping and mount, and
    the report. With Python's default SIGTERM disposition none of that ran.
    A BaseException, like KeyboardInterrupt, so that the `except Exception`
    guards inside the episode loop and `final_pass` cannot swallow it.
    """


def install_interrupt_handlers(signal_fn=signal.signal, getsignal_fn=signal.getsignal):
    """Maps SIGTERM and SIGHUP onto HarnessInterrupted. Returns the signals it took.

    One-shot: a second signal while `finally` is cleaning up is logged and
    ignored, because raising again mid-teardown would strand exactly the
    devices the first one is releasing. A SIGHUP that is already SIG_IGN
    (run under nohup) is left ignored, so a dropped SSH session does not stop a
    run the operator deliberately detached.
    """
    fired = []

    def _raise(signum, _frame):
        name = signal.Signals(signum).name
        if fired:
            print(f"run.py: {name} again during cleanup; ignored so teardown "
                  "can finish", file=sys.stderr)
            return None
        fired.append(signum)
        raise HarnessInterrupted(f"received {name}")

    taken = []
    for sig in (signal.SIGTERM, signal.SIGHUP):
        if getsignal_fn(sig) == signal.SIG_IGN:
            continue
        signal_fn(sig, _raise)
        taken.append(sig)
    return taken
```

  In `main()`, directly after the `os.geteuid() != 0` check, add `install_interrupt_handlers()`.
- [ ] **Run to verify it passes:** the same command, then the full suite. Expected: `OK`.
- [ ] **Commit:** `fix(chaos): SIGTERM to run.py unwinds through teardown`

---

### Task 9.5: Pin the shutdown drain budget (oracle fix, part 1)

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/run.py`:
  - constants after `DEFAULT_MIN_FREE_DEVICE_BYTES` (line 86);
  - `SCHEDULE_KEYS["weir"]` (lines 151-154);
  - a new `validate_weir_table` called from `load_schedule`;
  - `Daemon.__init__` (lines 696-708), a new `Daemon.argv`, `Daemon.start` (lines 710-781) and `Daemon.stop` (lines 788-808);
  - the `"http://127.0.0.1:9900/ingest"` literals in `main()` (lines 937, 1125) become `SINK_URL`.
- Test: `chaos/orchestrator/test_run.py` (new `TestDrainBudgetPin`; extend `TestDaemonCliContract`).

**Interfaces:**
- Consumes (PR 4): the weir-server CLI `--drain-shutdown-budget-secs <0..=3600>`. Existing flags: `--shutdown-timeout-secs` (cli.rs:207-208, default 30, 0 refused at config/mod.rs:912-913). PR 6: `--admin-socket-path`.
- Produces:
  - `run.FINAL_DRAIN_BUDGET_SECS = 3600`, `run.WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS = 30`, `run.SHUTDOWN_GRACE_EXTRA_SECS = 5`, `run.LEGACY_STOP_TIMEOUT_SECS = 300`, `run.STOP_SLACK_SECS = 10`
  - `run.SINK_URL = "http://127.0.0.1:9900/ingest"`
  - `run.stop_timeout_for(budget_secs: int, shutdown_timeout_secs: int) -> int`
  - `run.validate_weir_table(weir: dict) -> None` (raises `ValueError`)
  - `run.Daemon.__init__(self, binary, wab_dir, socket_path, metrics_port, cfg, log_file, log_path=None, admin_socket_path=None)`
  - attributes `Daemon.drain_budget_secs`, `Daemon.shutdown_timeout_secs`, `Daemon.active_budget_secs`, `Daemon.admin_socket_path`, `Daemon.log_path`, `Daemon.stop_timeout_secs`
  - `run.Daemon.argv(self, sink_url: str, budget_override: int | None = None) -> list[str]`
  - `run.Daemon.start(self, sink_url, budget_override=None)`

- [ ] **Write the failing test**

```python
def load_schedule_text(text):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "sched.toml")
        with open(path, "w") as f:
            f.write(text)
        return run.load_schedule(path)


class TestDrainBudgetPin(unittest.TestCase):
    """Oracle fix (spec §16 chaos item 1). final_pass verifies at
    frontier_slack=0 on the premise that SIGTERM runs a FULL drain. From
    Foundations on, weir bounds that drain at drain_shutdown_budget_secs
    (default 10 s), so every historical schedule would report records left
    sealed-and-intact on disk as I1 violations: a false P0. Pinning 3600 s
    restores the stop those schedules were written against."""

    CFG = {"shard_count": 4, "batch_size": 64, "batch_deadline_ms": 2,
           "wab_segment_max_bytes": 8388608}

    def _daemon(self, **over):
        return run.Daemon("/fake/weir-server", "/fake/wab", "/fake/weir.sock",
                          19185, dict(self.CFG, **over), None)

    @staticmethod
    def _flag(argv, flag):
        return argv[argv.index(flag) + 1]

    def test_a_schedule_without_a_budget_pins_the_full_drain(self):
        argv = self._daemon().argv(run.SINK_URL)
        self.assertEqual(self._flag(argv, "--drain-shutdown-budget-secs"), "3600")
        self.assertNotIn("--shutdown-timeout-secs", argv)
        self.assertNotIn("--admin-socket-path", argv)

    def test_the_schedule_budget_reaches_the_daemon(self):
        argv = self._daemon(drain_shutdown_budget_secs=2).argv(run.SINK_URL)
        self.assertEqual(self._flag(argv, "--drain-shutdown-budget-secs"), "2")

    def test_an_override_wins_for_the_drain_out_restart(self):
        argv = self._daemon(drain_shutdown_budget_secs=2).argv(
            run.SINK_URL, budget_override=run.FINAL_DRAIN_BUDGET_SECS)
        self.assertEqual(self._flag(argv, "--drain-shutdown-budget-secs"), "3600")

    def test_shutdown_timeout_is_passed_only_when_the_schedule_sets_it(self):
        argv = self._daemon(shutdown_timeout_secs=2).argv(run.SINK_URL)
        self.assertEqual(self._flag(argv, "--shutdown-timeout-secs"), "2")

    def test_stop_timeout_keeps_the_legacy_300s_for_a_full_drain(self):
        self.assertEqual(run.stop_timeout_for(3600, 30), 300)
        self.assertEqual(self._daemon().stop_timeout_secs, 300)

    def test_stop_timeout_covers_the_documented_grace_for_a_bounded_budget(self):
        # spec §7: grace >= shutdown_timeout + budget + 5; plus 10 s slack.
        self.assertEqual(run.stop_timeout_for(10, 30), 55)
        d = self._daemon(drain_shutdown_budget_secs=2, shutdown_timeout_secs=2)
        self.assertEqual(d.stop_timeout_secs, 19)

    def _valid(self, weir_extra):
        return TestUnknownScheduleKeysRefused.VALID.replace(
            "wab_segment_max_bytes = 1048576\n",
            "wab_segment_max_bytes = 1048576\n" + weir_extra)

    def test_the_two_new_weir_keys_load(self):
        s = load_schedule_text(self._valid(
            "drain_shutdown_budget_secs = 2\nshutdown_timeout_secs = 2\n"))
        self.assertEqual(s["weir"]["drain_shutdown_budget_secs"], 2)

    def test_a_budget_outside_weirs_range_is_refused_here_not_at_daemon_start(self):
        with self.assertRaises(ValueError):
            load_schedule_text(self._valid("drain_shutdown_budget_secs = 3601\n"))
        with self.assertRaises(ValueError):
            load_schedule_text(self._valid("drain_shutdown_budget_secs = -1\n"))

    def test_a_zero_shutdown_timeout_is_refused(self):
        with self.assertRaises(ValueError):
            load_schedule_text(self._valid("shutdown_timeout_secs = 0\n"))
```

  Add to `TestDaemonCliContract`:

```python
    def test_the_foundations_flags_are_real_weir_server_flags(self):
        """Needs PR 4 (--drain-shutdown-budget-secs) and PR 6
        (--admin-socket-path) merged; red until then by design."""
        cfg = {"shard_count": 4, "batch_size": 64, "batch_deadline_ms": 2,
               "wab_segment_max_bytes": 8388608,
               "drain_shutdown_budget_secs": 2, "shutdown_timeout_secs": 2}
        daemon = run.Daemon("/fake/weir-server", "/fake/wab", "/fake/weir.sock",
                            19185, cfg, None,
                            admin_socket_path="/run/weir-chaos/weir.admin.sock")
        flags = [t for t in daemon.argv(run.SINK_URL) if t.startswith("--")]
        cli_rs_path = os.path.join(
            run.WEIR_ROOT, "crates", "weir-server", "src", "config", "cli.rs")
        with open(cli_rs_path) as f:
            cli_source = f.read()
        for flag in ("--drain-shutdown-budget-secs", "--shutdown-timeout-secs",
                     "--admin-socket-path"):
            self.assertIn(flag, flags)
            self.assertIn(flag, cli_source, f"{flag} is not a weir-server flag")
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestDrainBudgetPin test_run.TestDaemonCliContract -v`
  - Expected: `AttributeError: module 'run' has no attribute 'SINK_URL'` (and `'Daemon' object has no attribute 'argv'`).
- [ ] **Implement.** First the constants and helpers:

```python
#: The drain budget the harness pins on every daemon unless the schedule sets
#: one. weir's own default since Foundations is 10 s (spec §13); every schedule
#: written before that was written against a SIGTERM that drained EVERYTHING,
#: and its final pass verifies at frontier_slack=0 on that premise. 3600 is the
#: top of weir's accepted range (0..=3600), and it puts the old behaviour back.
FINAL_DRAIN_BUDGET_SECS = 3600
#: weir's `shutdown_timeout_secs` default (config/mod.rs:912).
WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS = 30
#: spec §7: the documented grace is shutdown_timeout + budget + 5.
SHUTDOWN_GRACE_EXTRA_SECS = 5
#: Daemon.stop's wait before SIGKILL for a full-drain stop. Unchanged from 4.0.
LEGACY_STOP_TIMEOUT_SECS = 300
STOP_SLACK_SECS = 10
SINK_URL = "http://127.0.0.1:9900/ingest"


def stop_timeout_for(budget_secs, shutdown_timeout_secs):
    """How long Daemon.stop waits before SIGKILL. A full-drain budget keeps the
    4.0-era 300 s, so every historical schedule's stop is unchanged. A bounded
    budget gets weir's documented grace plus slack, because overrunning THAT is
    itself a finding (spec §7's bound)."""
    if budget_secs >= FINAL_DRAIN_BUDGET_SECS:
        return LEGACY_STOP_TIMEOUT_SECS
    return shutdown_timeout_secs + budget_secs + SHUTDOWN_GRACE_EXTRA_SECS + STOP_SLACK_SECS


def validate_weir_table(weir):
    """Refuses `[weir]` values weir itself would refuse at startup, so the
    error comes at schedule load and not minutes into a run."""
    budget = weir.get("drain_shutdown_budget_secs")
    if budget is not None and not (0 <= budget <= FINAL_DRAIN_BUDGET_SECS):
        raise ValueError(
            f"[weir].drain_shutdown_budget_secs={budget} is outside weir's "
            f"accepted range 0..={FINAL_DRAIN_BUDGET_SECS}")
    timeout = weir.get("shutdown_timeout_secs")
    if timeout is not None and timeout < 1:
        raise ValueError("[weir].shutdown_timeout_secs must be >= 1 (weir refuses 0)")
```

  The remaining edits:
  - **Schedule keys.** Add `"drain_shutdown_budget_secs", "shutdown_timeout_secs"` to `SCHEDULE_KEYS["weir"]`.
  - **load_schedule.** After `reject_unknown_schedule_keys(sched)`, call `validate_weir_table(sched.get("weir") or {})`.
  - **`Daemon.__init__`.** Add the keyword arguments `log_path=None, admin_socket_path=None`, and set:
    ```python
        self.log_path = log_path
        self.admin_socket_path = admin_socket_path
        self.drain_budget_secs = cfg.get("drain_shutdown_budget_secs", FINAL_DRAIN_BUDGET_SECS)
        self.shutdown_timeout_secs = cfg.get("shutdown_timeout_secs", WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS)
        self.active_budget_secs = self.drain_budget_secs
        self.stop_timeout_secs = stop_timeout_for(self.drain_budget_secs, self.shutdown_timeout_secs)
    ```
    These replace the line `self.stop_timeout_secs = 300`.
  - **`Daemon.argv(sink_url, budget_override=None)`.** Move the whole `cmd = [...]` list, including the compression block, out of `start()` into `argv()`, which returns it. Append the following right after `"--wab-segment-max-age-secs", "2",` and before the compression block:
    ```python
        budget = self.drain_budget_secs if budget_override is None else budget_override
        cmd += ["--drain-shutdown-budget-secs", str(budget)]
        if "shutdown_timeout_secs" in self.cfg:
            cmd += ["--shutdown-timeout-secs", str(self.cfg["shutdown_timeout_secs"])]
        if self.admin_socket_path:
            cmd += ["--admin-socket-path", self.admin_socket_path]
    ```
  - **`start(sink_url, budget_override=None)`.** Set `self.active_budget_secs` and recompute `self.stop_timeout_secs` from it. `Popen` gets `self.argv(sink_url, budget_override)`; the socket-wait loop is unchanged.
  - **`stop()` docstring.** Replace "weir's SIGTERM path runs a FULL drain…" with: "Since Foundations weir's SIGTERM drain is bounded by `drain_shutdown_budget_secs`; the harness pins 3600 unless a schedule sets a budget, and `final_pass` runs a drain-out restart when it does."
  - **`main()`.** Use `daemon.start(SINK_URL)` at both sites, and pass `log_path=os.path.join(run_dir, "weir-server.log")` to `Daemon(...)`.
- [ ] **Run to verify it passes**
  - Command: the same command, then the full suite.
  - Expected: `OK` once PR 4 and PR 6 are in this branch's base. Before PR 4, both `test_every_flag_daemon_start_emits_is_a_real_weir_server_flag` and `test_the_foundations_flags_are_real_weir_server_flags` fail on `--drain-shutdown-budget-secs`; before PR 6, the second also fails on `--admin-socket-path`.
- [ ] **Commit:** `fix(chaos): pin the shutdown drain budget so the final pass still drains`

---

### Task 9.6: Quiescence also waits on undelivered segments and an unpaused drain

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/quiescence.py`: constants after `RESUMED` (line 110); the condition block in `wait_for_quiescence` (lines 412-447).
- Test: `chaos/orchestrator/test_quiescence.py` (new class `TestFoundationsGauges`).

**Interfaces:**
- Consumes:
  - **PR 1:** gauge `weir_wab_undelivered_segments` (5 s poll task).
  - **PR 2:** gauge `weir_drain_paused` (0/1).
- Produces: `quiescence.UNDELIVERED = "weir_wab_undelivered_segments"` and `quiescence.PAUSED = "weir_drain_paused"`. Neither changes the signature of `wait_for_quiescence`.

- [ ] **Write the failing test**

```python
class TestFoundationsGauges(unittest.TestCase):
    """weir_wab_undelivered_segments is the spec's own drain-out condition
    (§16 chaos item 1); weir_drain_paused must block quiescence because a
    paused drain delivers nothing. Both are ABSENT on a pre-Foundations daemon,
    and absence must not block, or every historical schedule would hang."""

    def _wait(self, reading):
        return quiescence.wait_for_quiescence(
            "unused", timeout_s=0.5, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: reading, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=2)

    def test_undelivered_segments_block(self):
        ok, reason = self._wait(healthy_reading(**{quiescence.UNDELIVERED: 3.0}))
        self.assertFalse(ok)
        self.assertIn("undelivered_segments=3", reason)

    def test_zero_undelivered_quiesces(self):
        ok, reason = self._wait(healthy_reading(**{quiescence.UNDELIVERED: 0.0}))
        self.assertTrue(ok, reason)

    def test_absent_gauges_do_not_block_a_pre_foundations_daemon(self):
        ok, reason = self._wait(healthy_reading())
        self.assertTrue(ok, reason)

    def test_a_paused_drain_blocks(self):
        ok, reason = self._wait(healthy_reading(**{quiescence.PAUSED: 1.0}))
        self.assertFalse(ok)
        self.assertIn("drain is paused", reason)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_quiescence.TestFoundationsGauges -v`
  - Expected: `AttributeError: module 'quiescence' has no attribute 'UNDELIVERED'`.
- [ ] **Implement**

```python
#: Foundations gauges (spec §13). ABSENT on a pre-4.1 daemon, and absence is
#: not a condition: every historical schedule runs against binaries that do
#: not export them. Present and non-zero blocks quiescence.
UNDELIVERED = "weir_wab_undelivered_segments"
PAUSED = "weir_drain_paused"
```

  In the poll body, after `sink_down = ...`:

```python
        undelivered = m.get(UNDELIVERED)
        nothing_undelivered = undelivered is None or undelivered == 0.0
        not_paused = m.get(PAUSED, 0.0) != 1.0
```

  Add `if not nothing_undelivered: unmet.append(f"undelivered_segments={undelivered:.0f}")` and `if not not_paused: unmet.append("drain is paused")` to the `unmet` list. Add `and nothing_undelivered and not_paused` to the all-conditions `if`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_quiescence -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): quiescence waits on undelivered segments and an unpaused drain`

---

### Task 9.7: Drain-out restart before the zero-slack final verify (oracle fix, part 2)

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

A schedule that sets a bounded budget (all four Foundations schedules do) exits on its final SIGTERM with sealed, unconfirmed segments **by design**. `final_pass` therefore restarts the daemon with the budget pinned at 3600, waits for quiescence (which now includes `weir_wab_undelivered_segments == 0`), and stops it, all before the `frontier_slack=0` check.

**Files:**
- Modify:
  - `chaos/orchestrator/run.py`: a new `DRAIN_OUT_TIMEOUT_SECS`, `needs_drain_out` and `drain_out` before `final_pass`; `final_pass` (lines 404-648) gets a `drain_out_fn` parameter and a step 3b after step 3 (line 541);
  - `chaos/orchestrator/report.py`: `render_final_pass` (line 47) prose and two table rows;
  - `chaos/README.md`: "The final pass" step 3 (lines 215-217).
- Test: `chaos/orchestrator/test_run.py`: `FakeDaemon` (lines 59-72) gets `drain_budget_secs` and `start`; `final_pass_fixture` (lines 93-129) gets `drain_out`; new class `TestFinalPassDrainOut`.

**Interfaces:**
- Consumes: `Daemon.start(sink_url, budget_override)`, `Daemon.stop() -> bool`, `quiescence.wait_for_quiescence(url, timeout_s, wab_dir=...)`.
- Produces:
  - `run.DRAIN_OUT_TIMEOUT_SECS = 600`
  - `run.needs_drain_out(daemon) -> bool`
  - `run.drain_out(daemon, sink_url=SINK_URL, timeout_secs=DRAIN_OUT_TIMEOUT_SECS, quiesce_fn=None) -> dict` with keys `ran: bool`, `quiesced: bool`, `reason: str`, `clean_stop: bool`
  - `run.final_pass(..., drain_out_fn=None)`
  - the record key `drain_out: dict | None`, and the anomaly reason `"drain_out_not_quiesced"`.

- [ ] **Write the failing test.** First the fixture edits in test_run.py:

```python
class FakeDaemon:
    def __init__(self, alive=True, clean_stop=True, log=None,
                 drain_budget_secs=None):
        self.proc = FakeProc(returncode=None if alive else 0)
        self.metrics_url = "http://127.0.0.1:19185/metrics"
        self.wab_dir = "/unused/wab"
        self._clean_stop = clean_stop
        self.log = log if log is not None else []
        self.drain_budget_secs = (run.FINAL_DRAIN_BUDGET_SECS
                                  if drain_budget_secs is None else drain_budget_secs)
        self.starts = []

    def start(self, sink_url, budget_override=None):
        self.log.append("daemon.start")
        self.starts.append(budget_override)
        self.proc.returncode = None

    def stop(self):
        self.log.append("daemon.stop")
        self.proc.returncode = 0 if self._clean_stop else -9
        return self._clean_stop
```

  `final_pass_fixture` gains the parameter `drain_out=None`, plus:

```python
    def drain_out_fn(daemon):
        log.append("drain_out")
        if isinstance(drain_out, Exception):
            raise drain_out
        return drain_out or {"ran": True, "quiesced": True, "reason": "",
                             "clean_stop": True}
```

  It returns `drain_out_fn=drain_out_fn` in the kwargs dict. Then the tests:

```python
class TestFinalPassDrainOut(unittest.TestCase):
    def test_a_bounded_budget_gets_a_drain_out_before_the_zero_slack_check(self):
        kwargs, log = final_pass_fixture(daemon=FakeDaemon(drain_budget_secs=2))
        kwargs["daemon"].log = log
        record, v, a = run.final_pass(**kwargs)
        self.assertEqual(
            log, ["loadgen.wait", "scrape", "daemon.stop", "drain_out",
                  "ledger.read_new", "delivered.read_new", "residue_scan"])
        self.assertTrue(record["drain_out"]["quiesced"])
        self.assertEqual(record["frontier_slack"], 0)
        self.assertEqual((v, a), (0, 0))

    def test_a_full_drain_budget_needs_no_drain_out(self):
        kwargs, log = final_pass_fixture()
        record, _, _ = run.final_pass(**kwargs)
        self.assertNotIn("drain_out", log)
        self.assertIsNone(record["drain_out"])

    def test_a_drain_out_that_never_quiesced_makes_the_check_advisory(self):
        kwargs, _ = final_pass_fixture(
            daemon=FakeDaemon(drain_budget_secs=2),
            drain_out={"ran": True, "quiesced": False,
                       "reason": "timeout after 600s", "clean_stop": True},
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"])
        record, v, a = run.final_pass(**kwargs)
        self.assertTrue(record["advisory"])
        self.assertIn("drain_out_not_quiesced", record["anomaly_reasons"])
        self.assertEqual((v, a), (0, 1),
                         "segments left on disk must not read as lost records")

    def test_a_drain_out_stop_that_needed_a_kill_is_an_anomaly(self):
        kwargs, _ = final_pass_fixture(
            daemon=FakeDaemon(drain_budget_secs=2),
            drain_out={"ran": True, "quiesced": True, "reason": "", "clean_stop": False})
        record, v, a = run.final_pass(**kwargs)
        self.assertIn("drain_out_kill_at_stop", record["anomaly_reasons"])
        self.assertEqual((v, a), (0, 1))

    def test_a_drain_out_that_raises_is_recorded_and_the_post_mortem_still_runs(self):
        kwargs, log = final_pass_fixture(
            daemon=FakeDaemon(drain_budget_secs=2),
            drain_out=RuntimeError("weir-server exited during startup"))
        kwargs["daemon"].log = log
        record, _, a = run.final_pass(**kwargs)
        self.assertIn("RuntimeError", record["drain_out"]["reason"])
        self.assertEqual(log[-1], "residue_scan")
        self.assertEqual(a, 1)

    def test_no_drain_out_into_a_dead_recorder(self):
        kwargs, log = final_pass_fixture(
            daemon=FakeDaemon(drain_budget_secs=2), recorder_alive=False)
        run.final_pass(**kwargs)
        self.assertNotIn("drain_out", log)

    def test_drain_out_restarts_with_the_full_budget_waits_then_stops(self):
        d = FakeDaemon(drain_budget_secs=2)
        seen = []

        def quiesce(url, timeout, wab_dir=None):
            seen.append((url, timeout, wab_dir))
            return True, ""

        out = run.drain_out(d, quiesce_fn=quiesce, timeout_secs=42)
        self.assertEqual(d.starts, [run.FINAL_DRAIN_BUDGET_SECS])
        self.assertEqual(d.log, ["daemon.start", "daemon.stop"])
        self.assertEqual(seen, [(d.metrics_url, 42, d.wab_dir)])
        self.assertEqual(out, {"ran": True, "quiesced": True, "reason": "",
                               "clean_stop": True})
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestFinalPassDrainOut test_run.TestFinalPass -v`
  - Expected: `TypeError: final_pass() got an unexpected keyword argument 'drain_out_fn'` (and `AttributeError: ... 'drain_out'`).
- [ ] **Implement.** In run.py:

```python
#: How long the drain-out restart may take to deliver what a bounded shutdown
#: left behind. The quiescence timeout of a soak schedule is 180 s for ONE
#: episode's backlog; the final drain-out may hold several abandoned segments.
DRAIN_OUT_TIMEOUT_SECS = 600


def needs_drain_out(daemon):
    """True when the daemon's shutdown drain was bounded below a full drain,
    so its SIGTERM may legitimately leave sealed, unconfirmed segments."""
    return getattr(daemon, "drain_budget_secs", FINAL_DRAIN_BUDGET_SECS) < FINAL_DRAIN_BUDGET_SECS


def drain_out(daemon, sink_url=SINK_URL, timeout_secs=DRAIN_OUT_TIMEOUT_SECS,
              quiesce_fn=None):
    """Restart, deliver everything on disk, stop. Spec §16 chaos item 1:
    "restart and wait for weir_wab_undelivered_segments == 0 before the
    frontier_slack=0 verify". The restart replays every sealed, unconfirmed
    segment the bounded shutdown left. The budget is pinned so this stop is
    itself a full drain."""
    quiesce_fn = quiesce_fn or quiescence.wait_for_quiescence
    daemon.start(sink_url, budget_override=FINAL_DRAIN_BUDGET_SECS)
    ok, reason = quiesce_fn(daemon.metrics_url, timeout_secs, wab_dir=daemon.wab_dir)
    clean = daemon.stop()
    return {"ran": True, "quiesced": ok, "reason": reason, "clean_stop": clean}
```

  `final_pass` changes:
  - Add the parameter `drain_out_fn=None` and `drain_out_fn = drain_out_fn or drain_out`.
  - In the docstring's first bullet, replace "weir's SIGTERM path runs a FULL drain, not a seal-and-exit, so it delivers" with "the final stop runs a full drain (budget pinned at 3600 s, or a bounded stop plus the drain-out restart of step 3b), so it delivers". Step 4 is unchanged. Replace step 3 with:
    ```text
    3. `daemon.stop()`: the SIGTERM stop, bounded by the daemon's
       `drain_shutdown_budget_secs`. Having to `kill9` is an anomaly.
    3b. When `needs_drain_out(daemon)` holds (a bounded budget), restart with
       the budget at 3600, wait for quiescence, stop again (`drain_out`), so
       step 4's zero slack is not judged against segments the bounded stop
       left on disk.
    ```
  - After the step-3 block (line 541), insert:

```python
    # 3b. Drain-out. A bounded shutdown (Foundations, spec §7) leaves sealed,
    # unconfirmed segments ON PURPOSE; verifying at zero slack on top of that
    # would report records sitting intact on disk as lost — a false P0.
    record["drain_out"] = None
    if daemon is not None and needs_drain_out(daemon) and recorder_alive:
        try:
            out = drain_out_fn(daemon)
        except Exception as exc:
            out = {"ran": True, "quiesced": False,
                   "reason": f"{type(exc).__name__}: {exc}", "clean_stop": False}
        record["drain_out"] = out
        if not out["quiesced"]:
            anomaly_reasons.append("drain_out_not_quiesced")
            advisory_reasons.append(
                f"the drain-out restart did not quiesce ({out['reason']}), so "
                "acked records may still be on disk, undelivered")
        elif not out["clean_stop"]:
            # Quiesced, but its own full-budget stop overran and was SIGKILLed:
            # the same finding as a killed shutdown drain at step 3.
            anomaly_reasons.append("drain_out_kill_at_stop")
```

  report.py `render_final_pass`:
  - Replace "weir's SIGTERM drain — a **full** drain, not a seal-and-exit — given a real chance to finish." with "weir's SIGTERM drain given a real chance to finish: a full drain (budget pinned at 3600 s), or, for a schedule with a bounded `drain_shutdown_budget_secs`, the bounded stop followed by a drain-out restart that delivers whatever it left."
  - Directly after the `| Shutdown drain completed without a kill | … |` row (report.py:95-98), still inside the `| Teardown step | Result |` table, add two rows (a bullet here would end the table and the rows after it would render as plain text):
    ```python
    do = final.get("drain_out")
    if do:
        lines.append(
            f"| Drain-out restart quiesced | {_yes_no(do.get('quiesced'))}"
            f"{' (' + do['reason'] + ')' if do.get('reason') else ''} |")
        lines.append(
            f"| Drain-out stop completed without a kill | {_yes_no(do.get('clean_stop'))} |")
    ```

  README "The final pass", step 3 (chaos/README.md:215-217): replace the whole item with:
    ```markdown
    3. the daemon is asked to shut down. Since Foundations, weir bounds its
       SIGTERM drain at `drain_shutdown_budget_secs` (default 10 s), so run.py
       passes `--drain-shutdown-budget-secs 3600` unless the schedule sets a
       budget, which restores the full drain this check needs. A schedule with
       a smaller budget gets a **drain-out restart** after this stop: the
       daemon is restarted with the budget at 3600, run.py waits for
       quiescence (including `weir_wab_undelivered_segments == 0`), then stops
       it again. A drain-out that does not quiesce makes step 4 advisory
       (`drain_out_not_quiesced`);
    ```
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK` (every existing `TestFinalPass` order test is unchanged, because `FakeDaemon` defaults to the 3600 budget).
- [ ] **Commit:** `fix(chaos): drain-out restart before the zero-slack final verify`

---

### Task 9.8: Lifecycle fault kinds and their schedule validation

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Create: `chaos/orchestrator/faults.py`.
- Modify: `chaos/orchestrator/run.py`:
  - `fault_kind` (lines 60-71);
  - `SCHEDULE_KEYS["faults"]` (line 156);
  - `load_schedule`'s power-loss guard (lines 270-284);
  - `import faults` with the module imports (line 35).
- Test: `chaos/orchestrator/test_run.py` (new class `TestTriggerFaultKinds`).

**Interfaces:**
- Produces in `faults.py`:
  - `LEGACY_KINDS = ("kill_random", "power_loss")`
  - `TRIGGER_KINDS = ("sigterm_at", "double_sigterm", "sigterm_in_recovery", "admin_pause_resume")`
  - `SHUTDOWN_TRIGGERS = ("sigterm_at", "double_sigterm")`
  - `CUT_MODES = ("power_loss", "kill")`
  - `TRIGGER_FAULT_KEYS: dict[str, frozenset[str]]`, `REQUIRED_TRIGGER_KEYS: dict[str, frozenset[str]]`, `ALL_FAULT_KEYS: frozenset[str]`
  - `validate_faults(faults: dict) -> None` (raises `ValueError`)
  - `cut_mode(sched: dict) -> str` (one of `"power_loss" | "kill"`)
  - `oracle_fault(kind: str, cut: str) -> str` (one of `"power_loss" | "kill_random"`)
- Produces in `run.py`: `fault_kind(sched) -> str` now also returns the four trigger kinds.

- [ ] **Write the failing test**

```python
TRIGGER_FAULTS = (
    '[faults]\nkind = "sigterm_at"\ncut = "power_loss"\n'
    "cut_lo_ms = 0\ncut_hi_ms = 12000\nstall_sink_probability = 0.5\n"
)


class TestTriggerFaultKinds(unittest.TestCase):
    def _sched(self, faults_toml, dm_target='"flakey"'):
        base = TestUnknownScheduleKeysRefused.VALID
        base = base[: base.index("[faults]")] + faults_toml
        return base.replace('dm_target = "flakey"', f"dm_target = {dm_target}")

    def test_each_trigger_kind_is_a_fault_kind(self):
        for kind in faults.TRIGGER_KINDS:
            self.assertEqual(run.fault_kind({"faults": {"kind": kind}}), kind)

    def test_a_valid_trigger_schedule_loads(self):
        s = load_schedule_text(self._sched(TRIGGER_FAULTS))
        self.assertEqual(faults.cut_mode(s), "power_loss")

    def test_a_power_cut_without_a_real_flakey_layer_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            load_schedule_text(self._sched(TRIGGER_FAULTS, dm_target='"linear"'))
        self.assertIn("flakey", str(cm.exception))

    def test_a_kill_cut_needs_no_dm_layer(self):
        text = self._sched(TRIGGER_FAULTS.replace('"power_loss"', '"kill"'))
        text = text.replace('dm_target = "flakey"\n', "")
        self.assertEqual(faults.cut_mode(load_schedule_text(text)), "kill")

    def test_a_key_that_belongs_to_another_trigger_is_refused(self):
        # Nothing reads recovery_delay_ms under sigterm_at: silently ignoring
        # it would report a run as having measured something it did not.
        with self.assertRaises(ValueError) as cm:
            load_schedule_text(self._sched(TRIGGER_FAULTS + "recovery_delay_ms = 3000\n"))
        self.assertIn("recovery_delay_ms", str(cm.exception))

    def test_a_missing_required_key_is_refused(self):
        with self.assertRaises(ValueError) as cm:
            load_schedule_text(self._sched(TRIGGER_FAULTS.replace("cut_hi_ms = 12000\n", "")))
        self.assertIn("cut_hi_ms", str(cm.exception))

    def test_an_inverted_range_is_refused(self):
        with self.assertRaises(ValueError):
            load_schedule_text(self._sched(TRIGGER_FAULTS.replace("cut_lo_ms = 0", "cut_lo_ms = 20000")))

    def test_a_probability_outside_0_1_is_refused(self):
        with self.assertRaises(ValueError):
            load_schedule_text(self._sched(TRIGGER_FAULTS.replace("= 0.5", "= 1.5")))

    def test_an_unknown_cut_mode_is_refused(self):
        with self.assertRaises(ValueError):
            load_schedule_text(self._sched(TRIGGER_FAULTS.replace('"power_loss"', '"powerloss"')))

    def test_a_legacy_kind_refuses_trigger_keys(self):
        with self.assertRaises(ValueError):
            faults.validate_faults({"kind": "power_loss", "cut": "kill"})

    def test_the_oracle_sees_a_power_cut_as_power_loss(self):
        # Keeps verify's tier exemption, report.powerloss_verdict and the
        # quarantine exemption working for trigger episodes, unchanged.
        self.assertEqual(faults.oracle_fault("sigterm_at", "power_loss"), "power_loss")
        self.assertEqual(faults.oracle_fault("sigterm_at", "kill"), "kill_random")
        self.assertEqual(faults.oracle_fault("power_loss", "power_loss"), "power_loss")
        self.assertEqual(faults.oracle_fault("kill_random", "kill"), "kill_random")
```

  (Add `import faults` to test_run.py's imports.)
- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestTriggerFaultKinds -v`
  - Expected: `ModuleNotFoundError: No module named 'faults'`.
- [ ] **Implement** `chaos/orchestrator/faults.py` (validation part):

```python
"""Lifecycle fault kinds for the Foundations chaos schedules (spec §16).

Phase 1 cut the daemon with SIGKILL and Phase 2 with a dm-flakey power cut.
These kinds first PROVOKE a lifecycle path weir only has since Foundations —
a bounded SIGTERM shutdown, a forced second signal, a signal during
recovery, an admin pause — and then cut at a seeded point inside or after it.
The cut is what the oracle judges; the trigger decides what state it lands
on. Every trigger records whether it actually landed (its own canary), and
every power cut records I6, the injector's.

Stdlib only, like the rest of the orchestrator.
"""
import random
import signal

LEGACY_KINDS = ("kill_random", "power_loss")
TRIGGER_KINDS = ("sigterm_at", "double_sigterm", "sigterm_in_recovery",
                 "admin_pause_resume")
SHUTDOWN_TRIGGERS = ("sigterm_at", "double_sigterm")
CUT_MODES = ("power_loss", "kill")

_COMMON = frozenset({"kind", "cut", "cut_lo_ms", "cut_hi_ms"})
#: Every [faults] key a trigger reads. A key outside its kind's set is
#: refused — the same "nothing reads it, so the run lies" rule as
#: run.reject_unknown_schedule_keys, applied per kind.
TRIGGER_FAULT_KEYS = {
    "sigterm_at": _COMMON | {"stall_sink_probability"},
    "double_sigterm": _COMMON | {"stall_sink_probability",
                                 "second_signal_lo_ms", "second_signal_hi_ms"},
    "sigterm_in_recovery": _COMMON | {"recovery_delay_ms"},
    "admin_pause_resume": _COMMON | {"pause_hold_lo_ms", "pause_hold_hi_ms",
                                     "pause_cut_while_paused_probability"},
}
REQUIRED_TRIGGER_KEYS = {
    "sigterm_at": frozenset({"cut", "cut_lo_ms", "cut_hi_ms"}),
    "double_sigterm": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                 "second_signal_lo_ms", "second_signal_hi_ms"}),
    "sigterm_in_recovery": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                      "recovery_delay_ms"}),
    "admin_pause_resume": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                     "pause_hold_lo_ms", "pause_hold_hi_ms"}),
}
ALL_FAULT_KEYS = frozenset().union(*TRIGGER_FAULT_KEYS.values())
_RANGES = (("cut_lo_ms", "cut_hi_ms"),
           ("second_signal_lo_ms", "second_signal_hi_ms"),
           ("pause_hold_lo_ms", "pause_hold_hi_ms"))
_PROBABILITIES = ("stall_sink_probability", "pause_cut_while_paused_probability")


def validate_faults(faults):
    """Refuses an incoherent [faults] table. Raises ValueError."""
    kind = faults.get("kind", "kill_random")
    if kind in LEGACY_KINDS:
        extra = set(faults) - {"kind"}
        if extra:
            raise ValueError(
                f"[faults] keys {sorted(extra)} are lifecycle-trigger keys; "
                f"fault kind {kind!r} reads none of them")
        return
    if kind not in TRIGGER_KINDS:
        raise ValueError(f"unknown fault kind {kind!r}")
    unknown = set(faults) - TRIGGER_FAULT_KEYS[kind]
    if unknown:
        raise ValueError(
            f"[faults] key(s) {sorted(unknown)} are not read by {kind!r}: the run "
            f"would silently ignore them. Accepted: {sorted(TRIGGER_FAULT_KEYS[kind])}")
    missing = REQUIRED_TRIGGER_KEYS[kind] - set(faults)
    if missing:
        raise ValueError(f"[faults] for {kind!r} is missing {sorted(missing)}")
    if faults["cut"] not in CUT_MODES:
        raise ValueError(f"[faults].cut must be one of {CUT_MODES}, got {faults['cut']!r}")
    for lo, hi in _RANGES:
        if lo in faults and not (0 <= faults[lo] <= faults[hi]):
            raise ValueError(f"[faults] needs 0 <= {lo} <= {hi}")
    for p in _PROBABILITIES:
        if p in faults and not (0.0 <= faults[p] <= 1.0):
            raise ValueError(f"[faults].{p} must be within 0..1")
    if kind == "sigterm_in_recovery" and faults["recovery_delay_ms"] <= 0:
        raise ValueError("[faults].recovery_delay_ms must be > 0: without the "
                         "startup delay hook the recovering phase is too short to hit")


def cut_mode(sched):
    """How an episode ends: a dm-flakey power cut or a SIGKILL."""
    f = sched.get("faults") or {}
    kind = f.get("kind", "kill_random")
    if kind == "power_loss":
        return "power_loss"
    if kind in TRIGGER_KINDS:
        return f["cut"]
    return "kill"


def oracle_fault(kind, cut):
    """The fault the ORACLE must judge an episode under. A trigger that ends in
    a power cut is power loss to verify.check_counts, report.powerloss_verdict
    and final_pass's quarantine exemption; the trigger itself is recorded
    separately, under `trigger`."""
    if kind in LEGACY_KINDS:
        return kind
    return "power_loss" if cut == "power_loss" else "kill_random"
```

  Then the run.py edits:
  - **`fault_kind`.** Accept `faults.LEGACY_KINDS + faults.TRIGGER_KINDS`. The error lists both.
  - **Schedule keys.** `SCHEDULE_KEYS["faults"] = faults.ALL_FAULT_KEYS`. It is a superset, and `validate_faults` enforces the per-kind sets.
  - **`load_schedule`.** After `validate_weir_table`, call `faults.validate_faults(sched.get("faults") or {})`. Widen the guard to `if fault_kind(sched) == "power_loss" or faults.cut_mode(sched) == "power_loss":`, keeping its message.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK`. `test_every_shipped_schedule_still_loads` stays green.
- [ ] **Commit:** `feat(chaos): lifecycle fault kinds and their schedule validation`

---

### Task 9.9: Seeded per-episode fault plan

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/faults.py` (append).
- Test: `chaos/orchestrator/test_faults.py` (create).

**Interfaces:**
- Produces:
  - `faults.FAULT_PLAN_SALT = 0xF0D5`
  - `faults.fault_plan(seed: int, count: int, kind: str, faults_cfg: dict) -> list[dict]`. Each step holds `cut_after_secs: float`, plus:
    - `stall_sink: bool` for shutdown triggers;
    - `second_signal_secs: float` for `double_sigterm`;
    - `pause_hold_secs: float` and `cut_while_paused: bool` for `admin_pause_resume`;
    - `recovery_delay_ms: int` for `sigterm_in_recovery` (copied from `[faults]`, not drawn; `_sigterm_in_recovery` reads it from the step).

- [ ] **Write the failing test** (`chaos/orchestrator/test_faults.py`)

```python
"""Tests for the Foundations lifecycle triggers (faults.py).

Every collaborator a trigger touches goes through an `ops` object, so the
choreography — signal, watch, cut, release — is testable on a virtual clock
with no root, no daemon and no device.
"""
import os
import signal
import tempfile
import unittest

import faults
import run

SIGTERM_AT = {"kind": "sigterm_at", "cut": "power_loss", "cut_lo_ms": 0,
              "cut_hi_ms": 12000, "stall_sink_probability": 0.5}
DOUBLE = {"kind": "double_sigterm", "cut": "power_loss", "cut_lo_ms": 0,
          "cut_hi_ms": 12000, "second_signal_lo_ms": 100,
          "second_signal_hi_ms": 5000, "stall_sink_probability": 0.0}
ADMIN = {"kind": "admin_pause_resume", "cut": "power_loss", "cut_lo_ms": 0,
         "cut_hi_ms": 5000, "pause_hold_lo_ms": 2000, "pause_hold_hi_ms": 8000,
         "pause_cut_while_paused_probability": 1.0}


class TestFaultPlan(unittest.TestCase):
    def test_reproducible_from_the_seed(self):
        a = faults.fault_plan(7, 50, "sigterm_at", SIGTERM_AT)
        self.assertEqual(a, faults.fault_plan(7, 50, "sigterm_at", SIGTERM_AT))
        self.assertNotEqual(a, faults.fault_plan(8, 50, "sigterm_at", SIGTERM_AT))

    def test_it_does_not_disturb_the_kill_delay_sequence(self):
        # kill_delays draws from Random(seed); the plan must use its own
        # stream, or adding a trigger would reshuffle every steady-state wait.
        before = run.kill_delays(7, 20, 4.0, 8.0)
        faults.fault_plan(7, 20, "sigterm_at", SIGTERM_AT)
        self.assertEqual(before, run.kill_delays(7, 20, 4.0, 8.0))

    def test_cut_times_stay_inside_the_schedule_window(self):
        for step in faults.fault_plan(1, 500, "sigterm_at", SIGTERM_AT):
            self.assertGreaterEqual(step["cut_after_secs"], 0.0)
            self.assertLessEqual(step["cut_after_secs"], 12.0)

    def test_a_half_probability_stalls_roughly_half(self):
        plan = faults.fault_plan(1, 1000, "sigterm_at", SIGTERM_AT)
        stalled = sum(s["stall_sink"] for s in plan)
        self.assertTrue(400 < stalled < 600, stalled)

    def test_double_sigterm_steps_carry_a_second_signal_time(self):
        for step in faults.fault_plan(3, 100, "double_sigterm", DOUBLE):
            self.assertTrue(0.1 <= step["second_signal_secs"] <= 5.0)
            self.assertFalse(step["stall_sink"])

    def test_admin_steps_carry_a_hold_and_the_cut_while_paused_choice(self):
        for step in faults.fault_plan(3, 100, "admin_pause_resume", ADMIN):
            self.assertTrue(2.0 <= step["pause_hold_secs"] <= 8.0)
            self.assertTrue(step["cut_while_paused"])
            self.assertNotIn("stall_sink", step)

    def test_sigterm_in_recovery_steps_carry_the_recovery_delay(self):
        cfg = {"kind": "sigterm_in_recovery", "cut": "power_loss", "cut_lo_ms": 0,
               "cut_hi_ms": 8000, "recovery_delay_ms": 3000}
        for step in faults.fault_plan(3, 20, "sigterm_in_recovery", cfg):
            self.assertEqual(step["recovery_delay_ms"], 3000)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestFaultPlan -v`
  - Expected: `AttributeError: module 'faults' has no attribute 'fault_plan'`.
- [ ] **Implement** (append to faults.py)

```python
#: XORed into the schedule seed so the plan has its OWN random stream:
#: run.kill_delays draws from Random(seed), and sharing it would reshuffle
#: every historical steady-state sequence the moment a trigger draws.
FAULT_PLAN_SALT = 0xF0D5


def fault_plan(seed, count, kind, faults_cfg):
    """One step per episode: when to cut and what the trigger varies.

    Draw order is fixed (second signal, pause hold, cut-while-paused, stall,
    cut), so a seed reproduces its plan exactly. Pure.
    """
    rng = random.Random(seed ^ FAULT_PLAN_SALT)

    def secs(lo, hi):
        return rng.uniform(faults_cfg[lo], faults_cfg[hi]) / 1000.0

    plan = []
    for _ in range(count):
        step = {}
        if kind == "double_sigterm":
            step["second_signal_secs"] = secs("second_signal_lo_ms", "second_signal_hi_ms")
        if kind == "admin_pause_resume":
            step["pause_hold_secs"] = secs("pause_hold_lo_ms", "pause_hold_hi_ms")
            step["cut_while_paused"] = (
                rng.random() < faults_cfg.get("pause_cut_while_paused_probability", 0.0))
        if kind in SHUTDOWN_TRIGGERS:
            step["stall_sink"] = rng.random() < faults_cfg.get("stall_sink_probability", 0.0)
        if kind == "sigterm_in_recovery":
            # A constant, not a draw, so the draw order is unchanged.
            step["recovery_delay_ms"] = int(faults_cfg["recovery_delay_ms"])
        step["cut_after_secs"] = secs("cut_lo_ms", "cut_hi_ms")
        plan.append(step)
    return plan
```

- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_faults -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): seeded per-episode fault plan`

---

### Task 9.10: Extract the power cut from the episode loop

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify:
  - `chaos/orchestrator/faults.py` (append `power_cut`, `cut`);
  - `chaos/orchestrator/run.py`: `main()`'s `if kind == "power_loss":` branch (lines 1066-1121) becomes one call.
- Test: `chaos/orchestrator/test_faults.py` (new classes `TestPowerCut`, `TestPowerCutWiring`).

**Interfaces:**
- Consumes: `run.canary_verdict(before, during, after)` (run.py:338). `faults` must not import `run` at module import time, because run imports faults; it imports `run` lazily inside `power_cut`. The `StorageStack` methods `write_canary`, `engage_fault`, `drop_and_remount(wab_dir=)`, `disengage_fault`, `read_canary`, and `Daemon.kill9`, `Daemon.wab_dir`.
- Produces:
  - `faults.power_cut(stack, daemon, episode: int) -> str` (one of `"bit" | "did_not_bite" | "unexpected"`)
  - `faults.cut(mode: str, stack, daemon, episode: int) -> str | None`

- [ ] **Write the failing test**

```python
class FakeStack:
    def __init__(self, readback=None, drop_raises=None):
        self.calls = []
        self.readback = readback
        self.drop_raises = drop_raises
        self.written = []

    def write_canary(self, content):
        self.calls.append("write_canary")
        self.written.append(content)

    def engage_fault(self):
        self.calls.append("engage_fault")

    def drop_and_remount(self, wab_dir=None):
        self.calls.append("drop_and_remount")
        if self.drop_raises:
            raise self.drop_raises

    def disengage_fault(self):
        self.calls.append("disengage_fault")

    def read_canary(self):
        self.calls.append("read_canary")
        return self.written[0] if self.readback is None else self.readback


class CutDaemon:
    wab_dir = "/mnt/weir-wab/wab"

    def __init__(self, calls):
        self.calls = calls

    def kill9(self):
        self.calls.append("kill9")


class TestPowerCut(unittest.TestCase):
    def test_kill_first_then_drop_and_remount_exactly_as_phase_2(self):
        stack = FakeStack()
        verdict = faults.power_cut(stack, CutDaemon(stack.calls), 3)
        self.assertEqual(stack.calls, [
            "write_canary", "kill9", "engage_fault", "write_canary",
            "drop_and_remount", "disengage_fault", "read_canary"])
        self.assertEqual(verdict, "bit")
        self.assertIn(b"ep=3", stack.written[0])

    def test_a_failed_remount_still_disengages(self):
        stack = FakeStack(drop_raises=RuntimeError("umount busy"))
        with self.assertRaises(RuntimeError):
            faults.power_cut(stack, CutDaemon(stack.calls), 0)
        self.assertEqual(stack.calls[-1], "disengage_fault")

    def test_a_kill_cut_only_kills(self):
        calls = []
        self.assertIsNone(faults.cut("kill", FakeStack(), CutDaemon(calls), 0))
        self.assertEqual(calls, ["kill9"])

    def test_an_unknown_cut_mode_raises(self):
        with self.assertRaises(ValueError):
            faults.cut("bogus", FakeStack(), CutDaemon([]), 0)


class TestPowerCutWiring(unittest.TestCase):
    def test_main_uses_power_cut_and_no_longer_inlines_it(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(run.__file__)), "run.py")) as f:
            body = f.read()
        body = body[body.index("def main("):]
        self.assertIn("faults.power_cut(stack, daemon, i)", body)
        self.assertNotIn("stack.engage_fault()", body)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestPowerCut test_faults.TestPowerCutWiring -v`
  - Expected: `AttributeError: module 'faults' has no attribute 'power_cut'`.
- [ ] **Implement.** Append to faults.py, moving the comments from run.py:1067-1118 into the docstring:

```python
def power_cut(stack, daemon, episode):
    """One dm-flakey power cut, KILL FIRST (the Phase 2 protocol; see the spec's
    "Protocol: kill first, then drop and remount" and README "The protocol is
    kill-first"). Moved verbatim from run.py's episode loop so the lifecycle
    triggers end in exactly the cut the 6,129-cut corpus was measured with.
    Returns the I6 canary verdict."""
    import run  # lazy: run imports faults
    before = f"weir-chaos-canary ep={episode} phase=pre-fault\n".encode()
    during = f"weir-chaos-canary ep={episode} phase=engaged\n".encode()
    stack.write_canary(before)
    daemon.kill9()
    stack.engage_fault()
    try:
        stack.write_canary(during)
        stack.drop_and_remount(wab_dir=daemon.wab_dir)
    finally:
        # A device left engaged corrupts every later episode; weir is
        # already dead, so this redundant disengage costs nothing.
        stack.disengage_fault()
    return run.canary_verdict(before, during, stack.read_canary())


def cut(mode, stack, daemon, episode):
    """Ends an episode: a power cut (returns the canary) or SIGKILL (None)."""
    if mode == "power_loss":
        return power_cut(stack, daemon, episode)
    if mode == "kill":
        daemon.kill9()
        return None
    raise ValueError(f"unknown cut mode {mode!r}")
```

  In run.py `main()`, replace lines 1066-1121 (the `if kind == "power_loss":` body down to `canary_result = canary_verdict(...)`) with `canary_result = faults.power_cut(stack, daemon, i)`. Leave a two-line comment pointing at `faults.power_cut` for the protocol. The `else:` branch (`daemon.kill9(); canary_result = None`) is unchanged.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK`.
- [ ] **Commit:** `refactor(chaos): extract the power cut from the episode loop`

---

### Task 9.11: Daemon lifecycle primitives for signalled shutdowns

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/run.py`, class `Daemon`: split `start` into `spawn` and `wait_serving`; add `signal`, `exit_code`, `log_since_spawn` and `bound_secs`; add a new `DAEMON_STARTUP_TIMEOUT_SECS`.
- Test:
  - `chaos/orchestrator/test_run.py` (new class `TestDaemonLifecycle`);
  - `chaos/orchestrator/test_faults.py` (new class `TestDaemonLogContract`).

**Interfaces:**
- Consumes (PR 4):
  - the log line text "weir serving" (§6 step 12) and "forced shutdown" (§7);
  - the env vars `WEIR_TEST_HOOKS`, `WEIR_TEST_STARTUP_DELAY_MS` (the `test-hooks` feature);
  - metric `weir_startup_phase{phase="recovering"}` (PR 1).
- Produces:
  - `run.DAEMON_STARTUP_TIMEOUT_SECS = 30`
  - `run.Daemon.spawn(self, sink_url, budget_override=None, extra_env=None) -> None` (sets `self.log_offset`)
  - `run.Daemon.wait_serving(self, timeout_secs=DAEMON_STARTUP_TIMEOUT_SECS) -> None` (raises `RuntimeError`)
  - `run.Daemon.signal(self, sig) -> bool` (True if delivered to a live process)
  - `run.Daemon.exit_code(self) -> int | None`
  - `run.Daemon.log_since_spawn(self) -> bytes`
  - `run.Daemon.bound_secs(self) -> int` (= `shutdown_timeout_secs + active_budget_secs + 5`)
  - `faults.SERVING_LOG_LINE = b"weir serving"`, `faults.FORCED_LOG_LINE = b"forced shutdown"`, `faults.RECOVERING_SERIES = 'weir_startup_phase{phase="recovering"}'`, `faults.PAUSED_SERIES = "weir_drain_paused"`, `faults.TEST_HOOK_ENV = ("WEIR_TEST_HOOKS", "WEIR_TEST_STARTUP_DELAY_MS")`

- [ ] **Write the failing test** (test_run.py)

```python
class TestDaemonLifecycle(unittest.TestCase):
    CFG = {"shard_count": 1, "batch_size": 8, "batch_deadline_ms": 2,
           "wab_segment_max_bytes": 1048576,
           "drain_shutdown_budget_secs": 2, "shutdown_timeout_secs": 2}

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log_path = os.path.join(self.tmp, "weir-server.log")
        with open(self.log_path, "wb") as f:
            f.write(b"previous process\n")
        self.d = run.Daemon("/fake/weir-server", "/fake/wab",
                            os.path.join(self.tmp, "weir.sock"), 19185,
                            dict(self.CFG), None, log_path=self.log_path)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _spawn(self, **kw):
        captured = {}

        def fake_popen(cmd, **kwargs):
            captured.update(cmd=cmd, **kwargs)
            proc = mock.Mock()
            proc.poll.return_value = None
            return proc

        with mock.patch.object(run.subprocess, "Popen", fake_popen):
            self.d.spawn(run.SINK_URL, **kw)
        return captured

    def test_spawn_returns_without_waiting_and_merges_extra_env(self):
        c = self._spawn(extra_env={"WEIR_TEST_HOOKS": "1"})
        self.assertEqual(c["env"]["WEIR_TEST_HOOKS"], "1")
        self.assertEqual(c["env"]["NO_COLOR"], "1")

    def test_log_since_spawn_is_only_this_processs_output(self):
        self._spawn()
        with open(self.log_path, "ab") as f:
            f.write(b"weir serving\n")
        self.assertEqual(self.d.log_since_spawn(), b"weir serving\n")

    def test_signal_is_not_sent_to_a_dead_process(self):
        self.d.proc = FakeProc(returncode=0)
        self.assertFalse(self.d.signal(signal.SIGTERM))
        self.assertEqual(self.d.proc.signals, [])

    def test_signal_reaches_a_live_process(self):
        self.d.proc = FakeProc()
        self.assertTrue(self.d.signal(signal.SIGTERM))
        self.assertEqual(self.d.proc.signals, [signal.SIGTERM])
        self.assertIsNone(self.d.exit_code())

    def test_bound_secs_is_the_documented_grace(self):
        self.assertEqual(self.d.bound_secs(), 2 + 2 + 5)

    def test_bound_secs_follows_a_budget_override(self):
        self._spawn(budget_override=run.FINAL_DRAIN_BUDGET_SECS)
        self.assertEqual(self.d.bound_secs(), 2 + 3600 + 5)
        self.assertEqual(self.d.stop_timeout_secs, 300)

    def test_wait_serving_reports_a_crash_on_startup(self):
        self.d.proc = FakeProc(returncode=1)
        with self.assertRaises(RuntimeError) as cm:
            self.d.wait_serving(timeout_secs=1)
        self.assertIn("exited during startup", str(cm.exception))

    def test_wait_serving_returns_once_the_socket_exists(self):
        self.d.proc = FakeProc()
        open(self.d.socket_path, "w").close()
        self.d.wait_serving(timeout_secs=1)
```

  test_faults.py:

```python
class TestDaemonLogContract(unittest.TestCase):
    """The triggers judge weir by two log lines, two env vars and two series
    that PR 1, PR 2 and PR 4 introduce. Pin them against the source so a
    rename cannot turn every sigterm_in_recovery episode into a silent pass.
    Red until PR 1, PR 2 and PR 4 merge."""

    def _server_source(self):
        root = os.path.join(run.WEIR_ROOT, "crates", "weir-server", "src")
        chunks = []
        for dirpath, _d, files in os.walk(root):
            for name in files:
                if name.endswith(".rs"):
                    with open(os.path.join(dirpath, name), encoding="utf-8") as f:
                        chunks.append(f.read())
        return "\n".join(chunks)

    def test_every_string_the_triggers_rely_on_exists_in_weir_server(self):
        src = self._server_source()
        for needle in (faults.SERVING_LOG_LINE.decode(),
                       faults.FORCED_LOG_LINE.decode(),
                       *faults.TEST_HOOK_ENV,
                       "weir_startup_phase", "weir_drain_paused",
                       "weir_wab_undelivered_segments"):
            with self.subTest(needle=needle):
                self.assertIn(needle, src)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestDaemonLifecycle test_faults.TestDaemonLogContract -v`
  - Expected: `AttributeError: 'Daemon' object has no attribute 'spawn'` and `AttributeError: module 'faults' has no attribute 'SERVING_LOG_LINE'`.
- [ ] **Implement.** In faults.py:

```python
#: Text and series the triggers judge weir by. One constant each, pinned
#: against weir-server's source by TestDaemonLogContract. If the merged
#: Foundations text differs, change these, never the daemon.
SERVING_LOG_LINE = b"weir serving"            # spec §6 step 12
FORCED_LOG_LINE = b"forced shutdown"          # spec §7, second signal
RECOVERING_SERIES = 'weir_startup_phase{phase="recovering"}'
PAUSED_SERIES = "weir_drain_paused"
TEST_HOOK_ENV = ("WEIR_TEST_HOOKS", "WEIR_TEST_STARTUP_DELAY_MS")
```

  run.py `Daemon`:

```python
#: Socket-appearance wait. 10 s before Foundations; Foundations adds a settle
#: wait of up to 10 s before the producer bind (spec §6 step 10), so a healthy
#: restart can legitimately take longer. A crash on startup is still caught
#: on the next 50 ms poll, not at this timeout.
DAEMON_STARTUP_TIMEOUT_SECS = 30

    def spawn(self, sink_url, budget_override=None, extra_env=None):
        """Starts weir-server and returns at once. `start()` is spawn+wait."""
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        self.active_budget_secs = (self.drain_budget_secs if budget_override is None
                                   else budget_override)
        self.stop_timeout_secs = stop_timeout_for(
            self.active_budget_secs, self.shutdown_timeout_secs)
        self.log_offset = (os.path.getsize(self.log_path)
                           if self.log_path and os.path.exists(self.log_path) else 0)
        env = dict(os.environ, NO_COLOR="1")
        if extra_env:
            env.update(extra_env)
        self.proc = subprocess.Popen(
            self.argv(sink_url, budget_override),
            stdout=self.log_file, stderr=self.log_file, env=env)

    def wait_serving(self, timeout_secs=DAEMON_STARTUP_TIMEOUT_SECS):
        deadline = time.monotonic() + timeout_secs
        while True:
            if os.path.exists(self.socket_path):
                return
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"weir-server exited during startup (code {self.proc.returncode}); "
                    f"see the daemon log in the run directory")
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"weir-server did not create its socket within {timeout_secs:g}s")
            time.sleep(0.05)

    def start(self, sink_url, budget_override=None):
        self.spawn(sink_url, budget_override)
        self.wait_serving()

    def signal(self, sig):
        if self.proc is None or self.proc.poll() is not None:
            return False
        self.proc.send_signal(sig)
        return True

    def exit_code(self):
        return None if self.proc is None else self.proc.poll()

    def log_since_spawn(self):
        if not self.log_path:
            return b""
        try:
            with open(self.log_path, "rb") as f:
                f.seek(getattr(self, "log_offset", 0))
                return f.read()
        except FileNotFoundError:
            return b""

    def bound_secs(self):
        """spec §7's documented grace for THIS process's budget."""
        return self.shutdown_timeout_secs + self.active_budget_secs + SHUTDOWN_GRACE_EXTRA_SECS
```

  Delete the old body of `start()`; the stale-socket comment moves into `spawn`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK`, except `TestDaemonLogContract`, which stays red until PRs 1, 2 and 4 merge; note this in the PR body.
- [ ] **Commit:** `feat(chaos): daemon lifecycle primitives for signalled shutdowns`

---

### Task 9.12: Stdlib admin-socket client

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Create: `chaos/orchestrator/admin.py`.
- Test: `chaos/orchestrator/test_admin.py` (create).

**Interfaces:**
- Consumes (PR 6, spec §11):
  - JSON lines;
  - request `{"v":1,"id","verb","args"}`;
  - response `{"v":1,"id","instance":{...},"ok":bool,"result"|"error":{"code","message","retryable"}}`;
  - socket mode 0600 in a parent that is not group- or world-writable;
  - 64 KiB line limit;
  - peer is the daemon euid or root.
- Produces:
  - `admin.PROTOCOL_VERSION = 1`, `admin.MAX_LINE_BYTES = 65536`
  - `class admin.AdminError(Exception)`
  - `admin.check_socket_path(path: str) -> None`
  - `admin.peer_uid(sock) -> int | None`
  - `admin.call(path: str, verb: str, args: dict | None = None, request_id: int = 1, timeout_secs: float = 10.0, expected_uids: set[int] | None = None) -> dict`. It returns the **envelope**: `ok:false` is returned, not raised. It raises `AdminError` only on transport, framing, identity or envelope faults.

- [ ] **Write the failing test** (`chaos/orchestrator/test_admin.py`)

```python
"""Tests for the harness's admin-socket client, against a real AF_UNIX server
in a thread (works on macOS and Linux; no root, no daemon)."""
import json
import os
import shutil
import socket
import tempfile
import threading
import unittest

import admin

INSTANCE = {"pid": 1, "started_at_ms": 0, "wab_dir": "/w", "socket_path": "/s",
            "wab_dir_dev": 1, "wab_dir_ino": 2}


class Server:
    """Accepts one connection, records the request line, replies `reply`."""

    def __init__(self, reply, mode=0o600):
        # /tmp keeps the path well under sun_path's 104-byte limit on macOS.
        self.dir = tempfile.mkdtemp(prefix="wa", dir="/tmp")
        self.path = os.path.join(self.dir, "a.sock")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.path)
        os.chmod(self.path, mode)
        self.sock.listen(1)
        # A client refused before connecting (mode, parent) never arrives;
        # the timeout ends this thread instead of leaving it in accept().
        self.sock.settimeout(5.0)
        self.reply = reply
        self.request = None
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        try:
            conn, _ = self.sock.accept()
        except OSError:
            return
        with conn:
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            self.request = json.loads(buf.split(b"\n")[0])
            if self.reply is not None:
                try:
                    conn.sendall(self.reply)
                except OSError:
                    pass  # the client hung up mid-reply (the oversize test)

    def close(self):
        self.sock.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def envelope(**kw):
    base = {"v": 1, "id": 1, "instance": INSTANCE, "ok": True, "result": {}}
    base.update(kw)
    return (json.dumps(base) + "\n").encode()


class TestAdminCall(unittest.TestCase):
    def _with(self, reply, mode=0o600):
        s = Server(reply, mode)
        self.addCleanup(s.close)
        return s

    def test_a_request_is_one_v1_json_line_and_the_envelope_comes_back(self):
        s = self._with(envelope(result={"paused": True}))
        resp = admin.call(s.path, "drain.pause", {"wait_ms": 5000})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["result"], {"paused": True})
        self.assertEqual(s.request, {"v": 1, "id": 1, "verb": "drain.pause",
                                     "args": {"wait_ms": 5000}})

    def test_an_error_response_is_returned_not_raised(self):
        s = self._with(envelope(ok=False, error={"code": "shutting_down",
                                                 "message": "m", "retryable": False}))
        resp = admin.call(s.path, "drain.pause")
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "shutting_down")

    def test_a_response_without_an_instance_block_is_refused(self):
        bad = (json.dumps({"v": 1, "id": 1, "ok": True, "result": {}}) + "\n").encode()
        s = self._with(bad)
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_mismatched_id_is_refused(self):
        s = self._with(envelope(id=99))
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_an_oversized_line_is_refused_without_buffering_forever(self):
        s = self._with(b"x" * (admin.MAX_LINE_BYTES + 4096))
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_connection_closed_before_a_full_line_is_refused(self):
        s = self._with(b'{"v":1')
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_socket_that_is_not_0600_is_refused_before_connecting(self):
        s = self._with(envelope(), mode=0o666)
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status")
        self.assertIn("mode", str(cm.exception))

    def test_a_group_writable_parent_is_refused(self):
        s = self._with(envelope())
        os.chmod(s.dir, 0o770)
        self.addCleanup(os.chmod, s.dir, 0o700)
        with self.assertRaises(admin.AdminError):
            admin.call(s.path, "status")

    def test_a_regular_file_is_not_a_socket(self):
        d = tempfile.mkdtemp(prefix="wa", dir="/tmp")
        self.addCleanup(shutil.rmtree, d, True)
        p = os.path.join(d, "f")
        open(p, "w").close()
        os.chmod(p, 0o600)
        with self.assertRaises(admin.AdminError):
            admin.call(p, "status")

    @unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "Linux peer credentials")
    def test_a_peer_with_an_unexpected_uid_is_an_identity_mismatch(self):
        s = self._with(envelope())
        with self.assertRaises(admin.AdminError) as cm:
            admin.call(s.path, "status", expected_uids={os.geteuid() + 1})
        self.assertIn("identity mismatch", str(cm.exception))
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_admin -v`
  - Expected: `ModuleNotFoundError: No module named 'admin'`.
- [ ] **Implement** `chaos/orchestrator/admin.py`

```python
"""Minimal client for weir's admin socket (Foundations spec §11), for the
chaos harness's admin_pause_resume trigger. Stdlib only.

It authenticates the SERVER the way weir-ctl does (§11 "Client-side server
authentication"): the path must be a 0600 socket in a parent that is not
group- or world-writable, and on Linux the peer must be root or our own
euid. A harness that sent `drain.pause` to an impostor and believed the
reply would report a pause that never happened.
"""
import json
import os
import socket
import stat
import struct

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 64 * 1024


class AdminError(Exception):
    """A transport, framing, identity or envelope fault, never an `ok:false`."""


def check_socket_path(path):
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        raise AdminError(f"no admin socket at {path}") from None
    if not stat.S_ISSOCK(st.st_mode):
        raise AdminError(f"{path} is not a socket")
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        raise AdminError(f"{path} has mode {mode:o}, expected 600")
    parent = os.lstat(os.path.dirname(path) or ".")
    if parent.st_mode & 0o022:
        raise AdminError(f"the parent of {path} is group- or world-writable")


def peer_uid(sock):
    """The peer's uid via SO_PEERCRED, or None where the platform has none."""
    opt = getattr(socket, "SO_PEERCRED", None)
    if opt is None:
        return None
    raw = sock.getsockopt(socket.SOL_SOCKET, opt, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", raw)
    return uid


def _read_line(sock, limit=MAX_LINE_BYTES):
    buf = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise AdminError("admin socket closed before a full response line")
        buf += chunk
        nl = buf.find(b"\n")
        if nl >= 0:
            return bytes(buf[:nl])
        if len(buf) > limit:
            raise AdminError(f"admin response exceeds {limit} bytes without a newline")


def call(path, verb, args=None, request_id=1, timeout_secs=10.0, expected_uids=None):
    """One request, one response. Returns the parsed envelope."""
    check_socket_path(path)
    allowed = expected_uids if expected_uids is not None else {0, os.geteuid()}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_secs)
        try:
            s.connect(path)
        except OSError as exc:
            raise AdminError(f"connect {path}: {exc}") from None
        uid = peer_uid(s)
        if uid is not None and uid not in allowed:
            raise AdminError(f"identity mismatch: peer uid {uid} not in {sorted(allowed)}")
        req = {"v": PROTOCOL_VERSION, "id": request_id, "verb": verb, "args": args or {}}
        s.sendall(json.dumps(req).encode() + b"\n")
        try:
            line = _read_line(s)
        except socket.timeout:
            raise AdminError(f"no response within {timeout_secs:g}s") from None
    try:
        resp = json.loads(line)
    except ValueError as exc:
        raise AdminError(f"response is not JSON: {exc}") from None
    if resp.get("v") != PROTOCOL_VERSION:
        raise AdminError(f"unexpected protocol version {resp.get('v')!r}")
    if resp.get("id") != request_id:
        raise AdminError(f"response id {resp.get('id')!r} != request id {request_id!r}")
    if "instance" not in resp:
        raise AdminError("response has no `instance` block (spec §11: every response carries one)")
    return resp
```

- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_admin -v`. Expected: `OK` (the uid test is skipped on macOS).
- [ ] **Commit:** `feat(chaos): stdlib admin-socket client`

---

### Task 9.13: `sigterm_at` and `double_sigterm` triggers

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/faults.py` (append constants, helpers, `shutdown_verdict`, `_sigterm_at`, `_double_sigterm`, `run_trigger`).
- Test: `chaos/orchestrator/test_faults.py` (add `FakeOps`, `TestSigtermAt`, `TestDoubleSigterm`).

**Interfaces:**
- Consumes the ops protocol, which `FakeOps` implements here and `LiveOps` in Task 9.16:

```
now() -> float                     sleep(secs: float) -> None
bound_secs() -> float              signal_daemon(sig) -> bool
daemon_exit_code() -> int | None   kill_daemon() -> None
spawn_daemon(extra_env: dict) -> None
scrape() -> dict                   daemon_log_since_spawn() -> bytes
producer_socket_exists() -> bool   stall_sink() -> None    unstall_sink() -> None
admin(verb: str, args: dict) -> dict                       delivered_bytes() -> int
cut() -> str | None
```

- Produces:
  - `faults.FORCED_BOUND_SECS = 8.0`, `faults.EXIT_POLL_SECS = 0.05`
  - `faults.shutdown_verdict(exit_code: int | None, alive_for_secs: float, bound_secs: float, label: str = "shutdown") -> list[str]`
  - `faults.run_trigger(kind: str, step: dict, ops) -> dict`
  - The result dict for `sigterm_at` has the keys `trigger`, `stall_sink`, `cut_after_secs`, `bound_secs`, `signal_sent`, `exit_code`, `exit_after_secs`, `cut_phase` (`"during_shutdown" | "after_exit"`), `canary` and `anomaly_reasons`.
  - `double_sigterm` adds `second_signal_secs`, `second_signal_sent`, `forced_observed` and `forced_bound_secs`.

- [ ] **Write the failing test**

```python
class FakeOps:
    """The ops protocol on a virtual clock. `exit_at` is the virtual time the
    daemon exits by itself; `on_signal(ops)` lets a test script how a signal
    changes that."""

    def __init__(self, bound=9.0, exit_at=None, exit_code=0, on_signal=None,
                 scrapes=None, admin_replies=None, log=b"", socket_left=False,
                 delivered=None, cut_raises=None):
        self.t = 0.0
        self.calls = []
        self.bound = bound
        self.exit_at = exit_at
        self.code = exit_code
        self.on_signal = on_signal
        self.scrapes = list(scrapes or [{}])
        self.admin_replies = admin_replies or {}
        self.log = log
        self.socket_left = socket_left
        self.delivered = list(delivered or [0])
        self.cut_raises = cut_raises
        self.signals = 0

    def now(self):
        return self.t

    def sleep(self, secs):
        self.t += max(0.0, secs)

    def bound_secs(self):
        return self.bound

    def daemon_exit_code(self):
        if self.exit_at is not None and self.t >= self.exit_at:
            return self.code
        return None

    def signal_daemon(self, sig):
        alive = self.daemon_exit_code() is None
        self.calls.append(("signal", round(self.t, 2)))
        if alive:
            self.signals += 1
            if self.on_signal:
                self.on_signal(self)
        return alive

    def kill_daemon(self):
        self.calls.append(("kill", round(self.t, 2)))
        self.exit_at, self.code = self.t, -9

    def spawn_daemon(self, extra_env):
        self.calls.append(("spawn", dict(extra_env)))
        self.exit_at, self.code = None, 0

    def scrape(self):
        self.calls.append(("scrape", round(self.t, 2)))
        item = self.scrapes.pop(0) if len(self.scrapes) > 1 else self.scrapes[0]
        if isinstance(item, Exception):
            raise item
        return item

    def daemon_log_since_spawn(self):
        return self.log

    def producer_socket_exists(self):
        return self.socket_left

    def stall_sink(self):
        self.calls.append(("stall_sink", round(self.t, 2)))

    def unstall_sink(self):
        self.calls.append(("unstall_sink", round(self.t, 2)))

    def admin(self, verb, args):
        self.calls.append(("admin", verb, round(self.t, 2)))
        reply = self.admin_replies[verb]
        if isinstance(reply, Exception):
            raise reply
        return reply

    def delivered_bytes(self):
        return self.delivered.pop(0) if len(self.delivered) > 1 else self.delivered[0]

    def cut(self):
        self.calls.append(("cut", round(self.t, 2)))
        if self.cut_raises:
            raise self.cut_raises
        return "bit"

    def names(self):
        return [c[0] for c in self.calls]


def exits_after(secs, on_nth=1, code=0):
    def on_signal(ops):
        if ops.signals == on_nth:
            ops.exit_at, ops.code = ops.t + secs, code
    return on_signal


class TestSigtermAt(unittest.TestCase):
    def step(self, cut_after, stall=False):
        return {"cut_after_secs": cut_after, "stall_sink": stall}

    def test_a_cut_during_shutdown_lands_at_the_planned_time(self):
        ops = FakeOps()
        r = faults.run_trigger("sigterm_at", self.step(3.0), ops)
        self.assertEqual(ops.names(), ["signal", "cut"])
        self.assertAlmostEqual(ops.calls[-1][1], 3.0, delta=0.06)
        self.assertEqual(r["cut_phase"], "during_shutdown")
        self.assertIsNone(r["exit_code"])
        self.assertEqual(r["canary"], "bit")
        self.assertEqual(r["anomaly_reasons"], [])

    def test_a_daemon_that_exits_first_is_cut_after_exit(self):
        ops = FakeOps(on_signal=exits_after(1.0))
        r = faults.run_trigger("sigterm_at", self.step(3.0), ops)
        self.assertEqual(r["cut_phase"], "after_exit")
        self.assertEqual(r["exit_code"], 0)
        self.assertAlmostEqual(r["exit_after_secs"], 1.0, delta=0.06)
        self.assertAlmostEqual(ops.calls[-1][1], 3.0, delta=0.06)
        self.assertEqual(r["anomaly_reasons"], [])

    def test_a_nonzero_exit_is_an_anomaly(self):
        # Owner decision: a forced or orphaned exit is still exit 0. Exit 3 is
        # the safety floor's EXIT_WAB_OWNERSHIP_LOST and is never a pass here.
        ops = FakeOps(on_signal=exits_after(1.0, code=3))
        r = faults.run_trigger("sigterm_at", self.step(3.0), ops)
        self.assertIn("daemon_exit_code=3", r["anomaly_reasons"])

    def test_alive_past_the_documented_bound_is_an_anomaly(self):
        r = faults.run_trigger("sigterm_at", self.step(12.0), FakeOps(bound=9.0))
        self.assertEqual(len(r["anomaly_reasons"]), 1)
        self.assertTrue(r["anomaly_reasons"][0].startswith("shutdown_overran_bound"))

    def test_alive_inside_the_bound_is_not(self):
        r = faults.run_trigger("sigterm_at", self.step(8.0), FakeOps(bound=9.0))
        self.assertEqual(r["anomaly_reasons"], [])

    def test_alive_exactly_at_the_bound_is_not_an_anomaly(self):
        self.assertEqual(faults.shutdown_verdict(None, 9.0, 9.0), [])

    def test_a_stalled_sink_is_released_only_after_the_cut(self):
        ops = FakeOps()
        faults.run_trigger("sigterm_at", self.step(2.0, stall=True), ops)
        self.assertEqual(ops.names(), ["stall_sink", "signal", "cut", "unstall_sink"])

    def test_the_sink_is_released_even_when_the_cut_raises(self):
        # A recorder left SIGSTOPped makes every later acked record look lost.
        ops = FakeOps(cut_raises=RuntimeError("umount busy"))
        with self.assertRaises(RuntimeError):
            faults.run_trigger("sigterm_at", self.step(1.0, stall=True), ops)
        self.assertEqual(ops.names()[-1], "unstall_sink")

    def test_a_daemon_already_dead_is_an_anomaly_not_a_shutdown(self):
        r = faults.run_trigger("sigterm_at", self.step(1.0),
                               FakeOps(exit_at=0.0, exit_code=-9))
        self.assertFalse(r["signal_sent"])
        self.assertEqual(r["anomaly_reasons"], ["daemon_dead_before_signal"])

    def test_an_unknown_kind_is_refused(self):
        with self.assertRaises(ValueError):
            faults.run_trigger("power_loss", {}, FakeOps())


class TestDoubleSigterm(unittest.TestCase):
    def step(self, second, cut_after):
        return {"second_signal_secs": second, "cut_after_secs": cut_after,
                "stall_sink": False}

    def test_the_second_signal_forces_the_exit_and_is_seen_in_the_log(self):
        ops = FakeOps(on_signal=exits_after(0.4, on_nth=2),
                      log=b"WARN forced shutdown\n")
        r = faults.run_trigger("double_sigterm", self.step(0.5, 2.0), ops)
        self.assertEqual(ops.names(), ["signal", "signal", "cut"])
        self.assertTrue(r["second_signal_sent"])
        self.assertTrue(r["forced_observed"])
        self.assertEqual(r["exit_code"], 0)
        self.assertEqual(r["cut_phase"], "after_exit")
        self.assertAlmostEqual(ops.calls[-1][1], 2.5, delta=0.06)
        self.assertEqual(r["anomaly_reasons"], [])

    def test_a_forced_shutdown_past_its_bound_is_an_anomaly(self):
        r = faults.run_trigger("double_sigterm", self.step(0.5, 10.0), FakeOps())
        self.assertTrue(r["anomaly_reasons"][0].startswith("forced_shutdown_overran_bound"))

    def test_no_second_signal_when_the_first_already_finished_it(self):
        ops = FakeOps(on_signal=exits_after(0.2))
        r = faults.run_trigger("double_sigterm", self.step(0.5, 2.0), ops)
        self.assertEqual(ops.names(), ["signal", "cut"])
        self.assertFalse(r["second_signal_sent"])
        self.assertAlmostEqual(ops.calls[-1][1], 2.5, delta=0.06)

    def test_forced_not_seen_in_the_log_is_recorded_not_alarmed(self):
        # The daemon may exit between our poll and the second signal.
        ops = FakeOps(on_signal=exits_after(0.4, on_nth=2), log=b"")
        r = faults.run_trigger("double_sigterm", self.step(0.5, 2.0), ops)
        self.assertFalse(r["forced_observed"])
        self.assertEqual(r["anomaly_reasons"], [])
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestSigtermAt test_faults.TestDoubleSigterm -v`
  - Expected: `AttributeError: module 'faults' has no attribute 'run_trigger'`.
- [ ] **Implement** (append to faults.py)

```python
#: After a SECOND signal: connections are aborted, the drain wait shrinks to
#: DRAIN_EXIT_GRACE (2 s), and the joins stay bounded at about 1 s plus 100 ms
#: per rotation (spec §7). 8 s is that plus generous slack; being alive past
#: it is a finding.
FORCED_BOUND_SECS = 8.0
EXIT_POLL_SECS = 0.05


def _watch_until(ops, deadline):
    """Sleeps until `deadline` (virtual or real), noting when the daemon exits.
    Returns (exit_code, exited_at), or (None, None) if it was still running."""
    while True:
        code = ops.daemon_exit_code()
        if code is not None:
            exited_at = ops.now()
            ops.sleep(deadline - ops.now())
            return code, exited_at
        remaining = deadline - ops.now()
        if remaining <= 0:
            return None, None
        ops.sleep(min(EXIT_POLL_SECS, remaining))


def _exit_fields(code, exited_at, t0):
    return {"exit_code": code,
            "exit_after_secs": None if exited_at is None else round(exited_at - t0, 3)}


def _cut_phase(code):
    return "after_exit" if code is not None else "during_shutdown"


def shutdown_verdict(exit_code, alive_for_secs, bound_secs, label="shutdown"):
    """Anomaly reasons for one signalled shutdown. Pure.

    A non-zero exit is never a pass (owner decision: forced and orphaned
    exits are 0). Still running at the cut, later than the bound weir
    documents, is the bounded-shutdown promise broken.
    """
    reasons = []
    if exit_code is not None and exit_code != 0:
        reasons.append(f"daemon_exit_code={exit_code}")
    if exit_code is None and alive_for_secs > bound_secs:
        reasons.append(f"{label}_overran_bound(alive_at={alive_for_secs:.1f}s,"
                       f"bound={bound_secs:.1f}s)")
    return reasons


def _sigterm_at(step, ops):
    stall = step["stall_sink"]
    result = {"trigger": "sigterm_at", "stall_sink": stall,
              "cut_after_secs": round(step["cut_after_secs"], 3),
              "bound_secs": ops.bound_secs()}
    if stall:
        ops.stall_sink()
    try:
        t0 = ops.now()
        sent = ops.signal_daemon(signal.SIGTERM)
        code, exited_at = _watch_until(ops, t0 + step["cut_after_secs"])
        result["signal_sent"] = sent
        result.update(_exit_fields(code, exited_at, t0))
        result["cut_phase"] = _cut_phase(code)
        result["canary"] = ops.cut()
    finally:
        if stall:
            ops.unstall_sink()
    result["anomaly_reasons"] = (
        ["daemon_dead_before_signal"] if not sent
        else shutdown_verdict(code, step["cut_after_secs"], result["bound_secs"]))
    return result


def _double_sigterm(step, ops):
    stall = step["stall_sink"]
    result = {"trigger": "double_sigterm", "stall_sink": stall,
              "second_signal_secs": round(step["second_signal_secs"], 3),
              "cut_after_secs": round(step["cut_after_secs"], 3),
              "bound_secs": ops.bound_secs(), "forced_bound_secs": FORCED_BOUND_SECS}
    if stall:
        ops.stall_sink()
    try:
        t0 = ops.now()
        sent = ops.signal_daemon(signal.SIGTERM)
        t1 = t0 + step["second_signal_secs"]
        code, exited_at = _watch_until(ops, t1)
        second = False
        if code is None:
            second = ops.signal_daemon(signal.SIGTERM)
            code, exited_at = _watch_until(ops, t1 + step["cut_after_secs"])
        else:
            ops.sleep(t1 + step["cut_after_secs"] - ops.now())
        result["signal_sent"] = sent
        result["second_signal_sent"] = second
        result.update(_exit_fields(code, exited_at, t0))
        result["forced_observed"] = bool(
            second and FORCED_LOG_LINE in ops.daemon_log_since_spawn())
        result["cut_phase"] = _cut_phase(code)
        result["canary"] = ops.cut()
    finally:
        if stall:
            ops.unstall_sink()
    if not sent:
        reasons = ["daemon_dead_before_signal"]
    elif second:
        reasons = shutdown_verdict(code, step["cut_after_secs"], FORCED_BOUND_SECS,
                                   label="forced_shutdown")
    else:
        reasons = shutdown_verdict(
            code, step["second_signal_secs"] + step["cut_after_secs"], result["bound_secs"])
    result["anomaly_reasons"] = reasons
    return result


def run_trigger(kind, step, ops):
    """Runs one lifecycle trigger and the cut that ends it. Returns its record."""
    handlers = {"sigterm_at": _sigterm_at, "double_sigterm": _double_sigterm}
    handler = handlers.get(kind)
    if handler is None:
        raise ValueError(f"not a lifecycle trigger: {kind!r}")
    return handler(step, ops)
```

- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_faults -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): sigterm_at and double_sigterm triggers`

---

### Task 9.14: `sigterm_in_recovery` trigger

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/faults.py` (add `_wait_for_series`, `_sigterm_in_recovery`, and register it in `run_trigger`).
- Test: `chaos/orchestrator/test_faults.py` (new class `TestSigtermInRecovery`).

**Interfaces:**
- Consumes:
  - **PR 4 §6.** "A signal before `serving`": recovery completes, steps 11-12 are skipped, any bound-but-unserved listener is unlinked, and the process exits 0.
  - **The `test-hooks` delay** sleeps inside `recovering`: PR 4 Task 4.10 step 8 publishes `Phase::Recovering`, then sleeps `test_hooks::startup_delay()` before `wab::recover`, while the HTTP listener is served (pr-4-lifecycle.md, Task 4.10 step 8). So `RECOVERING_SERIES = 'weir_startup_phase{phase="recovering"}'` is the series to poll.
- Produces:
  - `faults.RECOVERY_PHASE_TIMEOUT_SECS = 10.0`, `faults.SERIES_POLL_SECS = 0.05`
  - The result dict has the keys `trigger`, `recovery_delay_ms`, `recovery_phase_observed`, `signal_sent`, `exit_code`, `exit_after_secs`, `served`, `producer_socket_left`, `cut_after_secs`, `cut_phase`, `canary` and `anomaly_reasons`.

- [ ] **Write the failing test**

```python
class TestSigtermInRecovery(unittest.TestCase):
    STEP = {"recovery_delay_ms": 3000, "cut_after_secs": 1.0}
    R = faults.RECOVERING_SERIES

    def test_kill_then_spawn_with_hooks_then_signal_inside_recovery_then_cut(self):
        ops = FakeOps(scrapes=[OSError("refused"), {self.R: 0.0}, {self.R: 1.0}],
                      on_signal=exits_after(0.5), log=b"recovery complete\n")
        r = faults.run_trigger("sigterm_in_recovery", self.STEP, ops)
        self.assertEqual(ops.names(),
                         ["kill", "spawn", "scrape", "scrape", "scrape", "signal", "cut"])
        self.assertEqual(ops.calls[1][1], {"WEIR_TEST_HOOKS": "1",
                                           "WEIR_TEST_STARTUP_DELAY_MS": "3000"})
        self.assertTrue(r["recovery_phase_observed"])
        self.assertFalse(r["served"])
        self.assertEqual(r["exit_code"], 0)
        self.assertEqual(r["anomaly_reasons"], [])

    def test_serving_after_a_pre_serving_signal_is_an_anomaly(self):
        ops = FakeOps(scrapes=[{self.R: 1.0}], on_signal=exits_after(0.5),
                      log=b"weir serving elapsed=3.1s\n")
        r = faults.run_trigger("sigterm_in_recovery", self.STEP, ops)
        self.assertIn("served_after_pre_serving_signal", r["anomaly_reasons"])

    def test_a_producer_socket_left_after_the_exit_is_an_anomaly(self):
        ops = FakeOps(scrapes=[{self.R: 1.0}], on_signal=exits_after(0.5),
                      socket_left=True)
        r = faults.run_trigger("sigterm_in_recovery", self.STEP, ops)
        self.assertIn("producer_socket_left_after_pre_serving_exit", r["anomaly_reasons"])

    def test_never_seeing_recovering_is_an_anomaly_not_a_silent_pass(self):
        # e.g. the binary was built without --features test-hooks.
        ops = FakeOps(scrapes=[{self.R: 0.0}], on_signal=exits_after(0.5))
        r = faults.run_trigger("sigterm_in_recovery", self.STEP, ops)
        self.assertFalse(r["recovery_phase_observed"])
        self.assertIn("recovery_phase_not_observed", r["anomaly_reasons"])
        self.assertEqual(ops.names()[-2:], ["signal", "cut"], "it still cuts")

    def test_a_cut_while_recovery_still_runs_is_during_shutdown(self):
        ops = FakeOps(scrapes=[{self.R: 1.0}])
        r = faults.run_trigger("sigterm_in_recovery", self.STEP, ops)
        self.assertEqual(r["cut_phase"], "during_shutdown")
        self.assertIsNone(r["producer_socket_left"])
        self.assertEqual(r["anomaly_reasons"], [],
                         "recovery has no bound of its own, so alive is not an anomaly")
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestSigtermInRecovery -v`
  - Expected: `ValueError: not a lifecycle trigger: 'sigterm_in_recovery'`.
- [ ] **Implement**

```python
RECOVERY_PHASE_TIMEOUT_SECS = 10.0
SERIES_POLL_SECS = 0.05


def _wait_for_series(ops, series, timeout_secs):
    deadline = ops.now() + timeout_secs
    while ops.now() < deadline:
        try:
            if ops.scrape().get(series) == 1.0:
                return True
        except Exception:
            pass  # the metrics listener binds at startup step 5; early polls fail
        ops.sleep(min(SERIES_POLL_SECS, max(0.0, deadline - ops.now())))
    return False


def _sigterm_in_recovery(step, ops):
    """SIGKILL (so recovery has work), restart held in `recovering` by the
    test-hooks delay, SIGTERM inside recovery, then cut. spec §6: recovery is
    never interrupted, steps 11-12 are skipped, exit 0, no producer socket."""
    delay_ms = step["recovery_delay_ms"]
    result = {"trigger": "sigterm_in_recovery", "recovery_delay_ms": delay_ms,
              "cut_after_secs": round(step["cut_after_secs"], 3)}
    ops.kill_daemon()
    ops.spawn_daemon({TEST_HOOK_ENV[0]: "1", TEST_HOOK_ENV[1]: str(delay_ms)})
    observed = _wait_for_series(
        ops, RECOVERING_SERIES, delay_ms / 1000.0 + RECOVERY_PHASE_TIMEOUT_SECS)
    t0 = ops.now()
    sent = ops.signal_daemon(signal.SIGTERM)
    code, exited_at = _watch_until(ops, t0 + step["cut_after_secs"])
    served = SERVING_LOG_LINE in ops.daemon_log_since_spawn()
    socket_left = ops.producer_socket_exists() if code is not None else None
    result.update(recovery_phase_observed=observed, signal_sent=sent,
                  served=served, producer_socket_left=socket_left,
                  cut_phase=_cut_phase(code))
    result.update(_exit_fields(code, exited_at, t0))
    result["canary"] = ops.cut()
    reasons = []
    if not observed:
        reasons.append("recovery_phase_not_observed")
    if not sent:
        reasons.append("daemon_dead_before_signal")
    else:
        if observed and served:
            reasons.append("served_after_pre_serving_signal")
        if code is not None and code != 0:
            reasons.append(f"daemon_exit_code={code}")
    if socket_left:
        reasons.append("producer_socket_left_after_pre_serving_exit")
    result["anomaly_reasons"] = reasons
    return result
```

  Register `"sigterm_in_recovery": _sigterm_in_recovery` in `run_trigger`'s `handlers`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_faults -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): sigterm_in_recovery trigger`

---

### Task 9.15: `admin_pause_resume` trigger

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/faults.py` (add `_admin`, `_admin_pause_resume`, and register it).
- Test: `chaos/orchestrator/test_faults.py` (new classes `TestAdminPauseResume` and `TestPlanFeedsTriggers`).

**Interfaces:**
- Consumes (PR 6, §11):
  - `drain.pause {"wait_ms"}` returns `{"generation","pause_requested","paused","held",...}`;
  - `drain.resume {"wait_ms"}` returns `{"was_paused",...}`;
  - `weir_drain_paused` (spec §13).
- Produces:
  - `faults.ADMIN_WAIT_MS = 5000`, `faults.PAUSE_SETTLE_SECS = 1.0`
  - The result dict has the keys `trigger`, `action` (`"resume" | "cut_while_paused"`), `pause_hold_secs`, `cut_after_secs`, `pause_observed`, `paused`, `held`, `generation`, `paused_gauge`, `delivered_while_paused_bytes`, `was_paused`, `canary` and `anomaly_reasons`.

- [ ] **Write the failing test**

```python
def ok_reply(result):
    return {"v": 1, "id": 1, "instance": {}, "ok": True, "result": result}


PAUSED = ok_reply({"generation": 1, "pause_requested": True,
                   "paused": True, "held": False})
RESUMED = ok_reply({"generation": 1, "pause_requested": False, "was_paused": True})


class TestAdminPauseResume(unittest.TestCase):
    def step(self, cut_while_paused=False):
        return {"pause_hold_secs": 4.0, "cut_while_paused": cut_while_paused,
                "cut_after_secs": 1.0}

    def ops(self, **kw):
        kw.setdefault("admin_replies", {"drain.pause": PAUSED, "drain.resume": RESUMED})
        kw.setdefault("scrapes", [{faults.PAUSED_SERIES: 1.0}])
        kw.setdefault("delivered", [100, 100])
        return FakeOps(**kw)

    def test_pause_hold_resume_then_cut(self):
        ops = self.ops()
        r = faults.run_trigger("admin_pause_resume", self.step(), ops)
        self.assertEqual(ops.names(), ["admin", "scrape", "admin", "cut"])
        self.assertEqual(ops.calls[0][1], "drain.pause")
        self.assertAlmostEqual(ops.calls[2][2], 4.0, delta=0.01)
        self.assertAlmostEqual(ops.calls[3][1], 5.0, delta=0.01)
        self.assertEqual(r["action"], "resume")
        self.assertTrue(r["was_paused"])
        self.assertEqual(r["delivered_while_paused_bytes"], 0)
        self.assertEqual(r["anomaly_reasons"], [])

    def test_a_cut_while_paused_never_resumes(self):
        ops = self.ops()
        r = faults.run_trigger("admin_pause_resume", self.step(True), ops)
        self.assertEqual(ops.names(), ["admin", "scrape", "cut"])
        self.assertEqual(r["action"], "cut_while_paused")

    def test_deliveries_while_paused_are_an_anomaly(self):
        r = faults.run_trigger("admin_pause_resume", self.step(),
                               self.ops(delivered=[100, 250]))
        self.assertIn("delivered_while_paused(bytes=150)", r["anomaly_reasons"])

    def test_held_counts_as_paused(self):
        held = ok_reply({"generation": 1, "pause_requested": True,
                         "paused": False, "held": True})
        r = faults.run_trigger("admin_pause_resume", self.step(), self.ops(
            admin_replies={"drain.pause": held, "drain.resume": RESUMED}))
        self.assertTrue(r["pause_observed"])
        self.assertEqual(r["anomaly_reasons"], [])

    def test_requested_but_not_observed_is_an_anomaly_and_no_leak_is_judged(self):
        late = ok_reply({"generation": 1, "pause_requested": True,
                         "paused": False, "held": False})
        r = faults.run_trigger("admin_pause_resume", self.step(), self.ops(
            admin_replies={"drain.pause": late,
                           "drain.resume": ok_reply({"was_paused": False})},
            delivered=[100, 999]))
        self.assertEqual(r["anomaly_reasons"], ["pause_not_observed"])

    def test_a_refused_pause_is_an_anomaly(self):
        refused = {"v": 1, "id": 1, "instance": {}, "ok": False,
                   "error": {"code": "shutting_down", "message": "m", "retryable": False}}
        r = faults.run_trigger("admin_pause_resume", self.step(), self.ops(
            admin_replies={"drain.pause": refused, "drain.resume": RESUMED}))
        self.assertIn("admin_pause_refused(shutting_down)", r["anomaly_reasons"])

    def test_an_identity_mismatch_is_an_anomaly_and_the_episode_still_cuts(self):
        import admin
        ops = self.ops(admin_replies={
            "drain.pause": admin.AdminError("identity mismatch: peer uid 1000"),
            "drain.resume": RESUMED})
        r = faults.run_trigger("admin_pause_resume", self.step(), ops)
        self.assertTrue(r["anomaly_reasons"][0].startswith("admin_error(drain.pause"))
        self.assertEqual(ops.names()[-1], "cut")

    def test_a_resume_that_did_not_see_the_pause_is_an_anomaly(self):
        r = faults.run_trigger("admin_pause_resume", self.step(), self.ops(
            admin_replies={"drain.pause": PAUSED,
                           "drain.resume": ok_reply({"was_paused": False})}))
        self.assertIn("resume_did_not_see_pause", r["anomaly_reasons"])

    def test_the_paused_gauge_must_agree(self):
        r = faults.run_trigger("admin_pause_resume", self.step(),
                               self.ops(scrapes=[{faults.PAUSED_SERIES: 0.0}]))
        self.assertIn("paused_gauge_not_set(0.0)", r["anomaly_reasons"])


class TestPlanFeedsTriggers(unittest.TestCase):
    """Task 9.17 passes `fault_plan`'s steps straight to `run_trigger`. The
    trigger tests above build their steps by hand, so this is the test that
    catches a key a trigger reads but the plan never writes."""

    def test_every_kind_runs_on_its_own_plan_step(self):
        rec = {"kind": "sigterm_in_recovery", "cut": "power_loss", "cut_lo_ms": 0,
               "cut_hi_ms": 8000, "recovery_delay_ms": 3000}
        for kind, cfg in (("sigterm_at", SIGTERM_AT), ("double_sigterm", DOUBLE),
                          ("sigterm_in_recovery", rec), ("admin_pause_resume", ADMIN)):
            with self.subTest(kind):
                step = faults.fault_plan(1, 1, kind, cfg)[0]
                ops = FakeOps(scrapes=[{faults.RECOVERING_SERIES: 1.0, faults.PAUSED_SERIES: 1.0}],
                              admin_replies={"drain.pause": PAUSED, "drain.resume": RESUMED})
                faults.run_trigger(kind, step, ops)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestAdminPauseResume test_faults.TestPlanFeedsTriggers -v`
  - Expected: `ValueError: not a lifecycle trigger: 'admin_pause_resume'` (in `TestPlanFeedsTriggers`, only the `admin_pause_resume` subtest fails; the other three pass).
- [ ] **Implement**

```python
ADMIN_WAIT_MS = 5000
#: Covers the recorder finishing the response that was in flight when the
#: pause took effect (it fsyncs before its 200), so growth after this point
#: is a delivery the paused drain started.
PAUSE_SETTLE_SECS = 1.0


def _admin(ops, verb, args):
    try:
        return ops.admin(verb, args), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _error_code(reply):
    return (reply.get("error") or {}).get("code", "unknown")


def _admin_pause_resume(step, ops):
    """drain.pause under live load, hold, then either drain.resume and cut, or
    cut while still paused. Pause is in-memory only, so the restart after the
    cut resumes delivery. spec §8/§15: pause adds no duplicates and loses
    nothing; the cut's own window is the only duplicate source."""
    result = {"trigger": "admin_pause_resume",
              "action": "cut_while_paused" if step["cut_while_paused"] else "resume",
              "pause_hold_secs": round(step["pause_hold_secs"], 3),
              "cut_after_secs": round(step["cut_after_secs"], 3)}
    reasons = []
    t0 = ops.now()
    reply, err = _admin(ops, "drain.pause", {"wait_ms": ADMIN_WAIT_MS})
    observed = False
    if err:
        reasons.append(f"admin_error(drain.pause: {err})")
    elif not reply.get("ok"):
        reasons.append(f"admin_pause_refused({_error_code(reply)})")
    else:
        r = reply.get("result") or {}
        result.update(paused=bool(r.get("paused")), held=bool(r.get("held")),
                      generation=r.get("generation"))
        observed = result["paused"] or result["held"]
        if not observed:
            reasons.append("pause_not_observed")
    result["pause_observed"] = observed
    ops.sleep(PAUSE_SETTLE_SECS)
    before = ops.delivered_bytes()
    try:
        gauge = ops.scrape().get(PAUSED_SERIES)
    except Exception:
        gauge = None
    result["paused_gauge"] = gauge
    if observed and gauge != 1.0:
        reasons.append(f"paused_gauge_not_set({gauge})")
    ops.sleep(t0 + step["pause_hold_secs"] - ops.now())
    leak = ops.delivered_bytes() - before
    result["delivered_while_paused_bytes"] = leak
    if observed and leak > 0:
        reasons.append(f"delivered_while_paused(bytes={leak})")
    if not step["cut_while_paused"]:
        reply, err = _admin(ops, "drain.resume", {"wait_ms": ADMIN_WAIT_MS})
        if err:
            reasons.append(f"admin_error(drain.resume: {err})")
        elif not reply.get("ok"):
            reasons.append(f"admin_resume_refused({_error_code(reply)})")
        else:
            was = bool((reply.get("result") or {}).get("was_paused"))
            result["was_paused"] = was
            if observed and not was:
                reasons.append("resume_did_not_see_pause")
    ops.sleep(step["cut_after_secs"])
    result["canary"] = ops.cut()
    result["anomaly_reasons"] = reasons
    return result
```

  Register `"admin_pause_resume": _admin_pause_resume` in `run_trigger`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_faults -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): admin_pause_resume trigger`

---

### Task 9.16: Live trigger operations

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/faults.py` (append `class LiveOps`).
- Test: `chaos/orchestrator/test_faults.py` (new class `TestLiveOps`).

**Interfaces:**
- Consumes:
  - `run.Daemon.{spawn, signal, exit_code, kill9, log_since_spawn, bound_secs, metrics_url, socket_path}`;
  - `quiescence.scrape(url)`;
  - `admin.call(path, verb, args)`;
  - `faults.cut(mode, stack, daemon, episode)`.
- Produces: `faults.LiveOps(daemon, recorder, stack, cut_mode: str, episode: int, sink_url: str, admin_path: str | None = None, delivered_path: str | None = None, scrape_fn=None, admin_fn=None, sleep_fn=time.sleep, now_fn=time.monotonic)`. It implements every method of the ops protocol in Task 9.13.

- [ ] **Write the failing test**

```python
class LiveDaemon:
    metrics_url = "http://127.0.0.1:19185/metrics"
    wab_dir = "/w"

    def __init__(self, socket_path="/nonexistent/weir.sock"):
        self.socket_path = socket_path
        self.calls = []

    def spawn(self, sink_url, extra_env=None):
        self.calls.append(("spawn", sink_url, extra_env))

    def signal(self, sig):
        self.calls.append(("signal", sig))
        return True

    def exit_code(self):
        return None

    def kill9(self):
        self.calls.append(("kill9",))

    def log_since_spawn(self):
        return b"x"

    def bound_secs(self):
        return 9


class TestLiveOps(unittest.TestCase):
    def _ops(self, recorder=None, **kw):
        import test_run
        return faults.LiveOps(LiveDaemon(), recorder or test_run.FakeProc(), None,
                              "kill", 4, "http://sink", **kw)

    def test_stall_and_unstall_stop_and_continue_a_live_recorder(self):
        import test_run
        rec = test_run.FakeProc()
        ops = self._ops(recorder=rec)
        ops.stall_sink()
        ops.unstall_sink()
        self.assertEqual(rec.signals, [signal.SIGSTOP, signal.SIGCONT])

    def test_a_dead_recorder_is_not_signalled(self):
        import test_run
        rec = test_run.FakeProc(returncode=0)
        ops = self._ops(recorder=rec)
        ops.stall_sink()
        ops.unstall_sink()
        self.assertEqual(rec.signals, [])

    def test_delivered_bytes_is_the_delivery_logs_size(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"7 1\n7 2\n")
        self.addCleanup(os.unlink, f.name)
        self.assertEqual(self._ops(delivered_path=f.name).delivered_bytes(), 8)

    def test_spawn_signal_cut_and_admin_delegate(self):
        seen = []
        ops = self._ops(admin_path="/run/weir-chaos/weir.admin.sock",
                        admin_fn=lambda p, v, a: seen.append((p, v, a)) or {"ok": True},
                        scrape_fn=lambda url: {"u": url})
        ops.spawn_daemon({"WEIR_TEST_HOOKS": "1"})
        self.assertTrue(ops.signal_daemon(signal.SIGTERM))
        self.assertIsNone(ops.cut())
        ops.admin("drain.pause", {"wait_ms": 1})
        self.assertEqual(ops.daemon.calls, [
            ("spawn", "http://sink", {"WEIR_TEST_HOOKS": "1"}),
            ("signal", signal.SIGTERM), ("kill9",)])
        self.assertEqual(seen, [("/run/weir-chaos/weir.admin.sock", "drain.pause", {"wait_ms": 1})])
        self.assertEqual(ops.scrape(), {"u": LiveDaemon.metrics_url})
        self.assertEqual(ops.bound_secs(), 9)
        self.assertFalse(ops.producer_socket_exists())
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_faults.TestLiveOps -v`
  - Expected: `AttributeError: module 'faults' has no attribute 'LiveOps'`.
- [ ] **Implement** (add `import os`, `import time` to the faults.py imports)

```python
class LiveOps:
    """The ops protocol against the real daemon, recorder and storage stack.
    Thin by design: every decision lives in the trigger functions above,
    which are tested on a virtual clock."""

    def __init__(self, daemon, recorder, stack, cut_mode, episode, sink_url,
                 admin_path=None, delivered_path=None, scrape_fn=None,
                 admin_fn=None, sleep_fn=time.sleep, now_fn=time.monotonic):
        import admin as admin_mod
        import quiescence
        self.daemon, self.recorder, self.stack = daemon, recorder, stack
        self.cut_mode, self.episode, self.sink_url = cut_mode, episode, sink_url
        self.admin_path, self.delivered_path = admin_path, delivered_path
        self.scrape_fn = scrape_fn or quiescence.scrape
        self.admin_fn = admin_fn or admin_mod.call
        self.sleep_fn, self.now_fn = sleep_fn, now_fn

    def now(self):
        return self.now_fn()

    def sleep(self, secs):
        if secs > 0:
            self.sleep_fn(secs)

    def bound_secs(self):
        return self.daemon.bound_secs()

    def signal_daemon(self, sig):
        return self.daemon.signal(sig)

    def daemon_exit_code(self):
        return self.daemon.exit_code()

    def kill_daemon(self):
        self.daemon.kill9()

    def spawn_daemon(self, extra_env):
        self.daemon.spawn(self.sink_url, extra_env=extra_env)

    def scrape(self):
        return self.scrape_fn(self.daemon.metrics_url)

    def daemon_log_since_spawn(self):
        return self.daemon.log_since_spawn()

    def producer_socket_exists(self):
        return os.path.exists(self.daemon.socket_path)

    def stall_sink(self):
        # SIGSTOP the recorder: weir's sink calls hang exactly like a
        # blackholed sink (spec §16 system test 4), so the drain budget binds.
        if self.recorder is not None and self.recorder.poll() is None:
            self.recorder.send_signal(signal.SIGSTOP)

    def unstall_sink(self):
        if self.recorder is not None and self.recorder.poll() is None:
            self.recorder.send_signal(signal.SIGCONT)

    def admin(self, verb, args):
        return self.admin_fn(self.admin_path, verb, args)

    def delivered_bytes(self):
        return os.path.getsize(self.delivered_path)

    def cut(self):
        return cut(self.cut_mode, self.stack, self.daemon, self.episode)
```

  Note: a SIGSTOPped recorder still `poll()`s as alive, so `unstall_sink` sends SIGCONT to it. `final_pass`'s recorder-alive check is therefore not fooled by a stall, because the trigger's `finally` always unstalls.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_faults -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): live trigger operations`

---

### Task 9.17: Wire the lifecycle triggers into the episode loop

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/run.py`:
  - new constant `ADMIN_SOCKET_PATH` and pure functions `trigger_record_fields` and `episode_counts` (after `exit_code_for`, line 383);
  - in `main()`: the `Daemon(...)` construction (lines 931-936); plan and oracle setup before the loop (after line 941);
  - the abort record (lines 1058-1062), the fault branch (lines 1066-1124), `acc.check` (lines 1154-1157), the counting (lines 1191-1194), the record dict (lines 1196-1262), and the `final_pass` call (lines 1291-1295).
- Test: `chaos/orchestrator/test_run.py` (new classes `TestTriggerRecordFields`, `TestEpisodeCounts`, `TestTriggerWiring`).

**Interfaces:**
- Consumes: `faults.fault_plan`, `faults.cut_mode`, `faults.oracle_fault`, `faults.LiveOps`, `faults.run_trigger`, `faults.power_cut`, `faults.TRIGGER_KINDS`.
- Produces:
  - `run.ADMIN_SOCKET_PATH = "/run/weir-chaos/weir.admin.sock"`
  - `run.trigger_record_fields(kind: str, cut: str, trigger_result: dict | None) -> dict`
  - `run.episode_counts(result_ok: bool, advisory: bool, quiesced: bool, no_progress: bool, trigger_anomaly_reasons: list) -> tuple[int, int]` (violations, anomalies)
  - New record keys:
    - `trigger` (str);
    - `cut` (`"power_loss" | "kill"`);
    - `trigger_result` (the trigger dict minus `canary` and `anomaly_reasons`);
    - `anomaly_reasons` (list).
  - Changed record key: `fault` is now the **oracle** fault. For legacy kinds that is the same value as before.

- [ ] **Write the failing test**

```python
class TestTriggerRecordFields(unittest.TestCase):
    def test_legacy_kinds_add_nothing_so_records_stay_byte_identical(self):
        self.assertEqual(run.trigger_record_fields("power_loss", "power_loss", None), {})
        self.assertEqual(run.trigger_record_fields("kill_random", "kill", None), {})

    def test_a_trigger_records_its_kind_its_cut_and_its_findings_separately(self):
        tr = {"trigger": "sigterm_at", "cut_phase": "during_shutdown",
              "canary": "bit", "anomaly_reasons": ["daemon_exit_code=3"]}
        f = run.trigger_record_fields("sigterm_at", "power_loss", tr)
        self.assertEqual(f["trigger"], "sigterm_at")
        self.assertEqual(f["cut"], "power_loss")
        self.assertEqual(f["anomaly_reasons"], ["daemon_exit_code=3"])
        self.assertEqual(f["trigger_result"], {"trigger": "sigterm_at",
                                               "cut_phase": "during_shutdown"})
        json.dumps(f)

    def test_the_record_renders(self):
        tr = {"trigger": "sigterm_at", "canary": "bit", "anomaly_reasons": ["x_y"]}
        rec = {"episode": 0, "fault": "power_loss", "ok": True, "quiesced": True,
               "canary": "bit", "seed": 1,
               **run.trigger_record_fields("sigterm_at", "power_loss", tr)}
        out = report.render([rec], {})
        self.assertIn("x_y", out)


class TestEpisodeCounts(unittest.TestCase):
    def test_a_clean_episode_counts_nothing(self):
        self.assertEqual(run.episode_counts(True, False, True, False, []), (0, 0))

    def test_a_trigger_anomaly_is_an_anomaly_not_a_violation(self):
        self.assertEqual(run.episode_counts(True, False, True, False, ["x"]), (0, 1))

    def test_the_pre_existing_rules_are_unchanged(self):
        self.assertEqual(run.episode_counts(False, False, True, False, []), (1, 0))
        self.assertEqual(run.episode_counts(False, True, True, False, []), (0, 1))
        self.assertEqual(run.episode_counts(True, False, False, False, []), (0, 1))
        self.assertEqual(run.episode_counts(True, False, True, True, []), (0, 1))


class TestTriggerWiring(unittest.TestCase):
    def _main(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(run.__file__)), "run.py")) as f:
            src = f.read()
        return src[src.index("def main("):]

    def test_the_loop_dispatches_triggers_and_judges_by_the_oracle_fault(self):
        body = self._main()
        for needle in ("faults.run_trigger(kind, plan[i], ops)",
                       "fault=oracle",
                       "episode_counts(",
                       "trigger_record_fields(kind, cut, trigger_result)",
                       '"fault": oracle'):
            self.assertIn(needle, body)
        self.assertNotIn('"fault": kind', body,
                         "a trigger record must carry the ORACLE fault")

    def test_only_the_admin_trigger_gets_an_admin_socket_path(self):
        body = self._main()
        self.assertIn(
            'admin_socket_path=ADMIN_SOCKET_PATH if kind == "admin_pause_resume" else None',
            body)

    def test_the_admin_socket_lives_off_the_device_under_test(self):
        self.assertTrue(run.ADMIN_SOCKET_PATH.startswith("/run/weir-chaos/"))
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestTriggerRecordFields test_run.TestEpisodeCounts test_run.TestTriggerWiring -v`
  - Expected: `AttributeError: module 'run' has no attribute 'trigger_record_fields'`.
- [ ] **Implement.** The pure part:

```python
#: Off the device under test for the admin trigger, so its control-plane
#: socket is not on the filesystem being cut. Other schedules pass no path,
#: and weir binds its default `<parent(wab_dir)>/wab.admin.sock` on the
#: device. That is harmless: every cut SIGKILLs first, and `bind_hardened`
#: removes the stale file on restart. /run/weir-chaos is 0700 (main), which
#: the admin bind requires (spec §11).
ADMIN_SOCKET_PATH = "/run/weir-chaos/weir.admin.sock"


def trigger_record_fields(kind, cut, trigger_result):
    """Record fields a lifecycle trigger adds. {} for the legacy kinds, so every
    Phase 1/2 record stays byte-identical."""
    if trigger_result is None:
        return {}
    return {
        "trigger": kind,
        "cut": cut,
        "trigger_result": {k: v for k, v in trigger_result.items()
                           if k not in ("canary", "anomaly_reasons")},
        "anomaly_reasons": list(trigger_result.get("anomaly_reasons", [])),
    }


def episode_counts(result_ok, advisory, quiesced, no_progress, trigger_anomaly_reasons):
    """(violations, anomalies) one episode adds. Same split as I5: a trigger
    that did not land, or a shutdown that broke its bound, is an anomaly, never
    a durability violation."""
    violation = 1 if (not result_ok and not advisory) else 0
    anomaly = 1 if (not quiesced or no_progress or advisory or trigger_anomaly_reasons) else 0
    return violation, anomaly
```

  `main()` edits:
  1. **Daemon.** `Daemon(weir_bin, wab_dir, os.path.join(socket_dir, "weir.sock"), 19185, sched["weir"], daemon_log, log_path=os.path.join(run_dir, "weir-server.log"), admin_socket_path=ADMIN_SOCKET_PATH if kind == "admin_pause_resume" else None)`.
  2. **Plan and oracle.** After `delays = kill_delays(...)`:
     ```python
         cut = faults.cut_mode(sched)
         oracle = faults.oracle_fault(kind, cut)
         plan = (faults.fault_plan(seed, sched["episodes"], kind, sched.get("faults") or {})
                 if kind in faults.TRIGGER_KINDS else None)
     ```
  3. **Abort record.** `{"episode": i, "fault": oracle, **({"trigger": kind} if plan else {}), "ok": True, ...}`.
  4. **Fault branch.**
     ```python
                 trigger_result = None
                 if kind == "power_loss":
                     canary_result = faults.power_cut(stack, daemon, i)
                 elif kind in faults.TRIGGER_KINDS:
                     ops = faults.LiveOps(daemon, recorder, stack, cut, i, SINK_URL,
                                          admin_path=daemon.admin_socket_path,
                                          delivered_path=delivered_path)
                     trigger_result = faults.run_trigger(kind, plan[i], ops)
                     canary_result = trigger_result.get("canary")
                 else:
                     daemon.kill9()
                     canary_result = None
                 daemon.start(SINK_URL)
     ```
  5. **`acc.check`.** `acc.check(frontier_slack=frontier_slack, tier=sched["load"]["tier"], fault=oracle)`.
  6. **Counting.** Replace the two `if` lines (1191-1194) with:
     ```python
                 tf = trigger_record_fields(kind, cut, trigger_result)
                 v, a = episode_counts(result.ok, advisory, ok, no_progress,
                                       tf.get("anomaly_reasons", []))
                 violations += v
                 anomalies += a
     ```
  7. **Record dict.** `"fault": oracle,`, and after the dict literal add `record.update(tf)`, before the `if advisory:` line (1261). If both are present, the recorder-advisory `advisory_reasons` stays separate from `anomaly_reasons`.
  8. **`final_pass`.** Call it with `fault=oracle`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK`, with every pre-existing wiring test (device floor, power-cut wiring) still green.
- [ ] **Commit:** `feat(chaos): wire lifecycle triggers into the episode loop`

---

### Task 9.18: The four Foundations schedules and their README section

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Create: `chaos/schedules/shutdown-bounded.toml`, `chaos/schedules/shutdown-forced.toml`, `chaos/schedules/startup-signal.toml`, `chaos/schedules/pause-resume-cut.toml`.
- Modify: `chaos/README.md` (a new section "Foundations lifecycle schedules (pre-release gate)" after "The batched ack path (A6)").
- Test: `chaos/orchestrator/test_run.py` (new class `TestFoundationsSchedules`).

**Interfaces:**
- Consumes: `run.load_schedule`, `run.fault_kind`, `faults.FORCED_BOUND_SECS`, `run.SHUTDOWN_GRACE_EXTRA_SECS`, `run.WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS`.
- Produces: the four schedule files (keys per the Task 9.8 sets).

- [ ] **Write the failing test**

```python
class TestFoundationsSchedules(unittest.TestCase):
    SCHEDULES = {"shutdown-bounded.toml": "sigterm_at",
                 "shutdown-forced.toml": "double_sigterm",
                 "startup-signal.toml": "sigterm_in_recovery",
                 "pause-resume-cut.toml": "admin_pause_resume"}

    def _path(self, name):
        return os.path.join(os.path.dirname(os.path.abspath(run.__file__)),
                            "..", "schedules", name)

    def _load(self, name):
        return run.load_schedule(self._path(name))

    def test_each_ends_every_episode_in_a_real_power_cut_on_durable(self):
        for name, kind in self.SCHEDULES.items():
            with self.subTest(name):
                s = self._load(name)
                self.assertEqual(run.fault_kind(s), kind)
                self.assertEqual(s["faults"]["cut"], "power_loss")
                self.assertEqual(s["storage"]["dm_target"], "flakey")
                self.assertEqual(s["load"]["tier"], "D")
                self.assertGreaterEqual(s["episodes"], 250,
                                        "the gate needs >= 200 cuts per schedule")
                self.assertIn("drain_shutdown_budget_secs", s["weir"],
                              "omitting it pins 3600 and tests the 4.0 shutdown")

    def test_seeds_are_distinct_so_run_dirs_never_collide(self):
        seeds = [self._load(n)["seed"] for n in self.SCHEDULES]
        self.assertEqual(len(set(seeds)), len(seeds))

    def test_the_bounded_schedule_cuts_past_its_own_bound_sometimes(self):
        s = self._load("shutdown-bounded.toml")
        bound = (s["weir"]["shutdown_timeout_secs"]
                 + s["weir"]["drain_shutdown_budget_secs"] + run.SHUTDOWN_GRACE_EXTRA_SECS)
        self.assertGreater(s["faults"]["cut_hi_ms"] / 1000.0, bound)
        self.assertGreater(s["faults"]["stall_sink_probability"], 0.0)

    def test_the_forced_schedule_cuts_past_the_forced_bound_sometimes(self):
        s = self._load("shutdown-forced.toml")
        self.assertGreater(s["faults"]["cut_hi_ms"] / 1000.0, faults.FORCED_BOUND_SECS)

    def test_the_readme_documents_each_and_the_test_hooks_build(self):
        with open(os.path.join(os.path.dirname(self._path("x")), "..", "README.md")) as f:
            readme = f.read()
        for name in self.SCHEDULES:
            self.assertIn(name, readme)
        self.assertIn("--features test-hooks", readme)
        self.assertIn("orchestrator/gate.py", readme)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_run.TestFoundationsSchedules -v`
  - Expected: `FileNotFoundError: ... schedules/shutdown-bounded.toml`.
- [ ] **Implement.** `chaos/schedules/shutdown-bounded.toml`:

```toml
# Foundations pre-release gate (spec §16): BOUNDED SHUTDOWN under a power cut.
#
# Each episode: steady Durable load, then SIGTERM while the producer is still
# pushing. Half the episodes also SIGSTOP the recorder at the signal (a
# blackholed sink, spec §16 system test 4), so the 2 s drain budget really
# binds and in-flight segments are ABANDONED; the other half let the budget
# complete naturally. The power cut lands at a seeded 0-12 s after the
# signal: mid-lame-duck, mid-hand-off, mid-absorb, mid-commit, after the
# abandon, or after the exit. Durable: zero loss is the pass condition.
#
# The bound under test: shutdown_timeout 2 + budget 2 + 5 = 9 s. Cuts after
# 9 s (about a quarter of them) find the daemon gone, or record
# shutdown_overran_bound.
#
# shutdown_timeout_secs is 2, not weir's 30: the producer keeps its
# connections open and busy, so the connection drain would otherwise take the
# whole 30 s every episode.

seed = 0xF0D51
episodes = 300
max_duration_secs = 21600  # 6h ceiling; ~30-40 s per episode
steady_lo_secs = 4.0
steady_hi_secs = 8.0
quiescence_timeout_secs = 180.0

[load]
threads = 8
record_size = 256
tier = "D"
min_acked_per_episode = 100
min_delivered_per_episode = 100

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
wab_segment_max_bytes = 8388608
drain_shutdown_budget_secs = 2
shutdown_timeout_secs = 2

[storage]
size_mb = 2048
dm_target = "flakey"

[faults]
kind = "sigterm_at"
cut = "power_loss"
cut_lo_ms = 0
cut_hi_ms = 12000
stall_sink_probability = 0.5
```

  `chaos/schedules/shutdown-forced.toml`:

```toml
# Foundations pre-release gate: a SECOND SIGTERM (Forced, spec §7) under a
# power cut. weir's real defaults (budget 10 s, shutdown_timeout 30 s, so a
# graceful bound of 45 s) make the second signal the only way out in time:
# it lands 0.1-5 s after the first, and the cut lands 0-12 s after it. The
# forced bound is faults.FORCED_BOUND_SECS (8 s); later cuts probe it.

seed = 0xF0D52
episodes = 300
max_duration_secs = 21600  # 6h ceiling
steady_lo_secs = 4.0
steady_hi_secs = 8.0
quiescence_timeout_secs = 180.0

[load]
threads = 8
record_size = 256
tier = "D"
min_acked_per_episode = 100
min_delivered_per_episode = 100

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
wab_segment_max_bytes = 8388608
drain_shutdown_budget_secs = 10

[storage]
size_mb = 2048
dm_target = "flakey"

[faults]
kind = "double_sigterm"
cut = "power_loss"
second_signal_lo_ms = 100
second_signal_hi_ms = 5000
cut_lo_ms = 0
cut_hi_ms = 12000
stall_sink_probability = 0.5
```

  `chaos/schedules/startup-signal.toml`:

```toml
# Foundations pre-release gate: SIGTERM DURING RECOVERY (spec §6, "A signal
# before serving"). Needs weir-server built with `--features test-hooks`: the
# harness sets WEIR_TEST_HOOKS=1 and WEIR_TEST_STARTUP_DELAY_MS for the
# triggered start only, so recovery lasts long enough to hit. An episode that
# never sees phase=recovering is an ANOMALY (recovery_phase_not_observed),
# never a silent pass. Each episode SIGKILLs first so recovery has real work
# (sealing the active segments, truncating torn tails), then cuts power 0-8 s
# after the signal: mid-recovery, mid-shutdown or after the exit.

seed = 0xF0D53
episodes = 300
max_duration_secs = 21600  # 6h ceiling
steady_lo_secs = 4.0
steady_hi_secs = 8.0
quiescence_timeout_secs = 180.0

[load]
threads = 8
record_size = 256
tier = "D"
min_acked_per_episode = 100
min_delivered_per_episode = 100

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
wab_segment_max_bytes = 8388608
drain_shutdown_budget_secs = 10

[storage]
size_mb = 2048
dm_target = "flakey"

[faults]
kind = "sigterm_in_recovery"
cut = "power_loss"
recovery_delay_ms = 3000
cut_lo_ms = 0
cut_hi_ms = 8000
```

  `chaos/schedules/pause-resume-cut.toml`:

```toml
# Foundations pre-release gate: drain.pause / drain.resume over the admin
# socket under load, then a power cut, half while still paused (pause is
# in-memory: the restart resumes delivery) and half 0-5 s after resume. Pause
# stops at commit-batch granularity (spec §5.4, Held); delivery growth while
# paused is an anomaly (delivered_while_paused). The admin socket lives at
# run.ADMIN_SOCKET_PATH, off the device under test.

seed = 0xF0D54
episodes = 300
max_duration_secs = 21600  # 6h ceiling
steady_lo_secs = 4.0
steady_hi_secs = 8.0
quiescence_timeout_secs = 180.0

[load]
threads = 8
record_size = 256
tier = "D"
min_acked_per_episode = 100
min_delivered_per_episode = 100

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
wab_segment_max_bytes = 8388608
drain_shutdown_budget_secs = 10

[storage]
size_mb = 2048
dm_target = "flakey"

[faults]
kind = "admin_pause_resume"
cut = "power_loss"
pause_hold_lo_ms = 2000
pause_hold_hi_ms = 8000
pause_cut_while_paused_probability = 0.5
cut_lo_ms = 0
cut_hi_ms = 5000
```

  README section: insert this text in `chaos/README.md` directly before `## Phase 1 exit gate`, i.e. after the end of "The batched ack path (A6)" section. It is a `##` heading because the A6 section it follows is one; a `###` would nest it under A6.

````markdown
## Foundations lifecycle schedules (pre-release gate)

Four schedules gate the Foundations release. Each one provokes a lifecycle
path weir only has since Foundations, then ends the episode in the same
dm-flakey power cut the Phase 2 corpus was measured with, at a seeded point
inside or after that path.

| Schedule | Trigger | Lands the cut on | Anomaly reasons |
|---|---|---|---|
| `shutdown-bounded.toml` | `sigterm_at` | mid-lame-duck to after-exit, half with a stalled sink | `shutdown_overran_bound`, `daemon_exit_code=N`, `daemon_dead_before_signal` |
| `shutdown-forced.toml` | `double_sigterm` | after the second signal | `forced_shutdown_overran_bound`, `daemon_exit_code=N` |
| `startup-signal.toml` | `sigterm_in_recovery` | mid-recovery or after the pre-serving exit | `recovery_phase_not_observed`, `served_after_pre_serving_signal`, `producer_socket_left_after_pre_serving_exit` |
| `pause-resume-cut.toml` | `admin_pause_resume` | while paused or 0-5 s after resume | `pause_not_observed`, `paused_gauge_not_set`, `delivered_while_paused`, `resume_did_not_see_pause`, `admin_error`, `admin_pause_refused` |

Build weir-server with the test hooks, from the weir repo root:

```bash
cargo build --release -p weir-server --features test-hooks
```

`startup-signal.toml` needs them: the harness sets `WEIR_TEST_HOOKS=1` and
`WEIR_TEST_STARTUP_DELAY_MS` on the triggered start only, which holds the
daemon in `recovering` long enough to signal it there. The hooks are inert
without `WEIR_TEST_HOOKS=1`, and the daemon logs a loud WARN when they are
compiled in.

Since Foundations, weir bounds its SIGTERM drain at
`drain_shutdown_budget_secs`, so run.py passes
`--drain-shutdown-budget-secs 3600` to every daemon unless the schedule sets
a budget, and a schedule that sets a smaller one gets a drain-out restart
before the final pass's zero-slack verify (see "The final pass" below).

In these runs' records, `fault = power_loss` is the fault the oracle judges
(so the tier exemption, the power-loss verdict and the quarantine exemption
all apply unchanged), and `trigger = <kind>` names the lifecycle path.

run.py now unwinds on SIGTERM as well as SIGINT. A run that ends on an
exception or a signal before its final pass keeps `wab.img` for forensics and
writes `crash-postmortem.json` beside `episodes.jsonl`, which `report.md`
renders as "Crash post-mortem".

When all four runs are done, the gate reads their run directories:

```bash
python3 orchestrator/gate.py runs/<id> runs/<id> runs/<id> runs/<id> [--allow-anomalies]
```

It passes only with ≥1,000 power cuts in total and ≥200 per schedule, 0
oracle violations, the canary bit on every cut, 0 anomalies unless
`--allow-anomalies` (each allowed one explained in the write-up), and a final
pass in every run.
````
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`. Expected: `OK` (`test_every_shipped_schedule_still_loads` now covers the four files too).
- [ ] **Commit:** `test(chaos): Foundations lifecycle schedules`

---

### Task 9.19: Lifecycle trigger section and duplicate-rate baseline in report.md

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Modify: `chaos/orchestrator/report.py`: constants after `MAX_SURVIVORS_SHOWN` (line 35); a new `render_triggers`; in `render`, insert it immediately before `lines.append("## Episodes\n")` (line 541).
- Test: `chaos/orchestrator/test_report.py` (new class `TestTriggerSection`).

**Interfaces:**
- Produces:
  - `report.PHASE2_DUPLICATE_RATE = 1.000`
  - `report.PHASE2_DUPLICATE_RATE_SOURCE = "docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md"`
  - `report.render_triggers(episodes: list[dict]) -> list[str]`

- [ ] **Write the failing test**

```python
def trig_ep(i, kind, canary="bit", anomalies=(), **tr):
    return {"episode": i, "fault": "power_loss", "trigger": kind, "cut": "power_loss",
            "ok": True, "quiesced": True, "canary": canary, "duplicate_rate": 1.002,
            "trigger_result": tr, "anomaly_reasons": list(anomalies), "seed": 1}


class TestTriggerSection(unittest.TestCase):
    def test_a_run_without_triggers_has_no_section(self):
        self.assertNotIn("## Lifecycle triggers", report.render(EPISODES, {}))

    def test_one_row_per_trigger_with_where_the_cuts_landed(self):
        eps = [trig_ep(0, "sigterm_at", cut_phase="during_shutdown", stall_sink=True),
               trig_ep(1, "sigterm_at", cut_phase="during_shutdown"),
               trig_ep(2, "sigterm_at", cut_phase="after_exit")]
        out = report.render(eps, {})
        self.assertIn("## Lifecycle triggers", out)
        self.assertIn("| sigterm_at | 3 | 2 | 1 | 1 | 0 | 0 | 3/3 | 0 |", out)

    def test_a_missed_canary_is_counted_against_the_trigger(self):
        out = report.render([trig_ep(0, "admin_pause_resume", canary="did_not_bite",
                                     paused=True)], {})
        self.assertIn("| admin_pause_resume | 1 | 0 | 0 | 0 | 0 | 1 | 0/1 | 0 |", out)

    def test_the_duplicate_rate_is_stated_against_the_phase_2_baseline(self):
        out = report.render([trig_ep(0, "sigterm_at")], {})
        self.assertIn("phase-2 baseline 1.000", out)
        self.assertIn("+0.0020", out)
        self.assertIn(report.PHASE2_DUPLICATE_RATE_SOURCE, out)

    def test_a_trigger_anomaly_reaches_the_episode_notes(self):
        out = report.render([trig_ep(0, "sigterm_at",
                                     anomalies=["shutdown_overran_bound(alive_at=12.0s,bound=9.0s)"])], {})
        self.assertIn("shutdown_overran_bound", out)
        self.assertIn("| sigterm_at | 1 | 0 | 0 | 0 | 0 | 0 | 1/1 | 1 |", out)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_report.TestTriggerSection -v`
  - Expected: `AssertionError: '## Lifecycle triggers' not found`.
- [ ] **Implement**

```python
#: The phase-2 corpus's duplicate rate, which the Foundations gate reports
#: against (spec §16). Exactly 1.000 across 706 Buffered power-loss episodes.
PHASE2_DUPLICATE_RATE = 1.000
PHASE2_DUPLICATE_RATE_SOURCE = (
    "docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md")


def render_triggers(episodes):
    """Foundations lifecycle triggers: where each cut landed, whether the
    trigger bit, and the duplicate cost against the phase-2 baseline."""
    trig = [e for e in episodes
            if e.get("trigger") and isinstance(e.get("episode"), int)
            and not e.get("abort_reason")]
    if not trig:
        return []
    lines = ["## Lifecycle triggers\n"]
    lines.append(
        "Each episode provoked a lifecycle path, then cut power at a seeded point "
        "inside or after it. `Canary bit` is I6 for that cut.\n")
    lines.append("| Trigger | Episodes | Cut during shutdown | Cut after exit | "
                 "Sink stalled | Forced observed | Pause observed | Canary bit | "
                 "Trigger anomalies |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    by_kind = {}
    for e in trig:
        by_kind.setdefault(e["trigger"], []).append(e)
    for kind in sorted(by_kind):
        es = by_kind[kind]
        tr = [e.get("trigger_result") or {} for e in es]
        cells = (
            len(es),
            sum(1 for t in tr if t.get("cut_phase") == "during_shutdown"),
            sum(1 for t in tr if t.get("cut_phase") == "after_exit"),
            sum(1 for t in tr if t.get("stall_sink")),
            sum(1 for t in tr if t.get("forced_observed")),
            sum(1 for t in tr if t.get("paused") or t.get("held")),
        )
        bit = sum(1 for e in es if e.get("canary") == "bit")
        anomalous = sum(1 for e in es if e.get("anomaly_reasons"))
        lines.append(f"| {kind} | " + " | ".join(str(c) for c in cells)
                     + f" | {bit}/{len(es)} | {anomalous} |")
    rated = [e for e in episodes if "duplicate_rate" in e]
    if rated:
        rate = rated[-1]["duplicate_rate"]
        lines.append(
            f"\nDuplicate rate {rate:.4f} against the phase-2 baseline "
            f"{PHASE2_DUPLICATE_RATE:.3f} (Δ {rate - PHASE2_DUPLICATE_RATE:+.4f}; "
            f"source `{PHASE2_DUPLICATE_RATE_SOURCE}`). Bounded shutdown adds at "
            "most one abandoned segment's committed prefix per shutdown (spec §7), "
            "so a rise here is expected and stated, not a failure.")
    lines.append("")
    return lines
```

  In `render`, add `lines.extend(render_triggers(episodes))` immediately before `lines.append("## Episodes\n")`.
- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_report -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): lifecycle trigger section in report.md`

---

### Task 9.20: The campaign gate

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:**
- Create: `chaos/orchestrator/gate.py`.
- Test: `chaos/orchestrator/test_gate.py` (create).

**Interfaces:**
- Consumes: `report.load_episodes(run_dir)`, `report.PHASE2_DUPLICATE_RATE`, `report.CRASH_POSTMORTEM_FILE` (Task 9.2).
- Produces:
  - `gate.GATE_TRIGGERS = {"sigterm_at": "shutdown-bounded", "double_sigterm": "shutdown-forced", "sigterm_in_recovery": "startup-signal", "admin_pause_resume": "pause-resume-cut"}`
  - `gate.MIN_CUTS_TOTAL = 1000`, `gate.MIN_CUTS_PER_SCHEDULE = 200`
  - `gate.campaign_gate(runs: dict[str, list[dict]], allow_anomalies: bool = False) -> dict`, with the keys `ok`, `cuts{kind:n}`, `total_cuts`, `violations[list[str]]`, `canary_misses[list[str]]`, `anomalies[list[str]]`, `duplicate_rate{kind:float}`, `baseline_duplicate_rate`, `runs_without_final_pass[list[str]]`, `failures[list[str]]`
  - `gate.render_gate(result: dict) -> str`
  - `gate.main(argv: list[str] | None = None) -> int`
  - CLI: `python3 gate.py <run_dir>... [--allow-anomalies]`, exiting 0 on pass and 1 on fail.

- [ ] **Write the failing test** (`chaos/orchestrator/test_gate.py`)

```python
"""Tests for the Foundations pre-release chaos gate (spec §16)."""
import json
import os
import tempfile
import unittest

import gate
import report


def cut(i, kind, **over):
    e = {"episode": i, "fault": "power_loss", "trigger": kind, "cut": "power_loss",
         "ok": True, "quiesced": True, "canary": "bit", "duplicate_rate": 1.01,
         "anomaly_reasons": [], "seed": 1}
    e.update(over)
    return e


def full_campaign(per=250):
    return {f"runs/{k}": [cut(i, k) for i in range(per)]
            + [{"episode": "final", "fault": "power_loss", "ok": True,
                "advisory": False, "anomaly_reasons": [], "duplicate_rate": 1.012}]
            for k in gate.GATE_TRIGGERS}


class TestCampaignGate(unittest.TestCase):
    def test_a_full_clean_campaign_passes(self):
        r = gate.campaign_gate(full_campaign())
        self.assertTrue(r["ok"], r["failures"])
        self.assertEqual(r["total_cuts"], 1000)
        self.assertEqual(r["duplicate_rate"]["sigterm_at"], 1.012)
        self.assertEqual(r["baseline_duplicate_rate"], 1.0)

    def test_too_few_cuts_in_one_schedule_fails_and_names_it(self):
        runs = full_campaign()
        runs["runs/sigterm_in_recovery"] = runs["runs/sigterm_in_recovery"][:150]
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertTrue(any("startup-signal" in f for f in r["failures"]))

    def test_too_few_cuts_in_total_fails(self):
        r = gate.campaign_gate(full_campaign(per=210))
        self.assertFalse(r["ok"])
        self.assertTrue(any("1000" in f for f in r["failures"]))

    def test_one_missed_canary_fails(self):
        runs = full_campaign()
        runs["runs/sigterm_at"][7]["canary"] = "did_not_bite"
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertEqual(r["canary_misses"], ["runs/sigterm_at#7"])

    def test_one_violation_fails(self):
        runs = full_campaign()
        runs["runs/double_sigterm"][3].update(ok=False, i1_missing=[42])
        self.assertFalse(gate.campaign_gate(runs)["ok"])

    def test_an_advisory_failure_is_an_anomaly_not_a_violation(self):
        runs = full_campaign()
        runs["runs/double_sigterm"][3].update(ok=False, advisory=True)
        r = gate.campaign_gate(runs)
        self.assertEqual(r["violations"], [])
        self.assertFalse(r["ok"], "anomalies fail the gate unless allowed")
        self.assertTrue(gate.campaign_gate(runs, allow_anomalies=True)["ok"])

    def test_aborted_and_kill_cut_episodes_are_not_cuts(self):
        runs = full_campaign()
        runs["runs/sigterm_at"].append(cut(999, "sigterm_at", cut="kill"))
        runs["runs/sigterm_at"].append(cut(998, "sigterm_at", abort_reason="loadgen_exited"))
        self.assertEqual(gate.campaign_gate(runs)["cuts"]["sigterm_at"], 250)

    def test_legacy_power_loss_runs_do_not_count_towards_the_gate(self):
        runs = full_campaign()
        runs["runs/legacy"] = [{"episode": i, "fault": "power_loss", "ok": True,
                                "canary": "bit"} for i in range(500)]
        self.assertEqual(gate.campaign_gate(runs)["total_cuts"], 1000)

    def test_a_run_without_a_final_pass_fails(self):
        runs = full_campaign()
        runs["runs/sigterm_at"] = runs["runs/sigterm_at"][:-1]
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertEqual(r["runs_without_final_pass"], ["runs/sigterm_at"])

    def _write_campaign(self, tmp):
        dirs = []
        for name, records in full_campaign().items():
            d = os.path.join(tmp, os.path.basename(name))
            os.makedirs(d)
            with open(os.path.join(d, "episodes.jsonl"), "w") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
            dirs.append(d)
        return dirs

    def test_the_cli_reads_run_dirs_and_exits_by_the_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            dirs = self._write_campaign(tmp)
            self.assertEqual(gate.main(dirs), 0)
            self.assertEqual(gate.main(dirs[:3]), 1)

    def test_the_cli_fails_a_run_dir_holding_a_crash_post_mortem(self):
        with tempfile.TemporaryDirectory() as tmp:
            dirs = self._write_campaign(tmp)
            with open(os.path.join(dirs[0], report.CRASH_POSTMORTEM_FILE), "w") as f:
                json.dump({"reason": "x", "wab_dir": None}, f)
            self.assertEqual(gate.main(dirs), 1)
```

- [ ] **Run it to verify it fails**
  - Command: `cd chaos/orchestrator && python3 -m unittest test_gate -v`
  - Expected: `ModuleNotFoundError: No module named 'gate'`.
- [ ] **Implement** `chaos/orchestrator/gate.py`

```python
#!/usr/bin/env python3
"""The Foundations pre-release chaos gate (spec §16).

PASS needs ALL of:
- at least 1,000 power cuts across the four lifecycle schedules, and at least
  200 in each;
- 0 oracle violations (a non-advisory I1/I2/ledger-conflict failure, in any
  episode or final pass);
- the I6 canary "bit" on every cut;
- 0 anomalies, unless --allow-anomalies (each allowed one must then be
  explained in the write-up);
- a final pass in every run (its zero-slack verify and WAB post-mortem), and
  no crash-postmortem.json in any run dir.
The duplicate rate is REPORTED against the phase-2 baseline, not gated.

Usage: python3 orchestrator/gate.py runs/<id> [runs/<id> ...] [--allow-anomalies]
"""
import argparse
import os
import sys

import report

GATE_TRIGGERS = {
    "sigterm_at": "shutdown-bounded",
    "double_sigterm": "shutdown-forced",
    "sigterm_in_recovery": "startup-signal",
    "admin_pause_resume": "pause-resume-cut",
}
MIN_CUTS_TOTAL = 1000
MIN_CUTS_PER_SCHEDULE = 200


def _is_cut(e):
    return (isinstance(e.get("episode"), int) and e.get("trigger") in GATE_TRIGGERS
            and e.get("cut") == "power_loss" and not e.get("abort_reason"))


def _is_anomaly(e):
    return bool(e.get("anomaly_reasons") or e.get("abort_reason") or e.get("advisory")
                or not e.get("quiesced", True) or e.get("no_progress"))


def campaign_gate(runs, allow_anomalies=False):
    cuts = {k: 0 for k in GATE_TRIGGERS}
    canary_misses, violations, anomalies, dup = [], [], [], {}
    no_final = []
    for run_dir, records in sorted(runs.items()):
        # The final pass is the run's only zero-slack verify and its in-run WAB
        # post-mortem. A run that crashed, was SIGTERMed or hit run-soak.sh's
        # backstop has no `{"episode": "final"}` record and never had either.
        if not any(e.get("episode") == "final" for e in records):
            no_final.append(run_dir)
        for e in records:
            label = f"{run_dir}#{e.get('episode')}"
            if _is_cut(e):
                cuts[e["trigger"]] += 1
                if e.get("canary") != "bit":
                    canary_misses.append(label)
            if not e.get("ok", True) and not e.get("advisory"):
                violations.append(label)
            if _is_anomaly(e):
                anomalies.append(label)
        kinds = {e.get("trigger") for e in records if e.get("trigger") in GATE_TRIGGERS}
        rated = [e for e in records if "duplicate_rate" in e]
        if len(kinds) == 1 and rated:
            dup[kinds.pop()] = rated[-1]["duplicate_rate"]
    total = sum(cuts.values())
    failures = []
    if total < MIN_CUTS_TOTAL:
        failures.append(f"{total} cuts in total < {MIN_CUTS_TOTAL}")
    for kind, n in cuts.items():
        if n < MIN_CUTS_PER_SCHEDULE:
            failures.append(f"{GATE_TRIGGERS[kind]}: {n} cuts < {MIN_CUTS_PER_SCHEDULE}")
    if violations:
        failures.append(f"{len(violations)} oracle violation(s): {violations[:10]}")
    if canary_misses:
        failures.append(f"{len(canary_misses)} cut(s) where the canary did not bite: "
                        f"{canary_misses[:10]}")
    if anomalies and not allow_anomalies:
        failures.append(f"{len(anomalies)} anomal(y/ies): {anomalies[:10]} "
                        "(explain each in the write-up, then --allow-anomalies)")
    if no_final:
        failures.append(f"{len(no_final)} run(s) without a final pass "
                        f"(crashed or interrupted): {no_final}")
    return {"ok": not failures, "cuts": cuts, "total_cuts": total,
            "violations": violations, "canary_misses": canary_misses,
            "anomalies": anomalies, "duplicate_rate": dup,
            "baseline_duplicate_rate": report.PHASE2_DUPLICATE_RATE,
            "runs_without_final_pass": no_final,
            "failures": failures}


def render_gate(result):
    lines = ["Foundations chaos gate: " + ("PASS" if result["ok"] else "FAIL"), ""]
    lines.append(f"{'schedule':<20} {'cuts':>6} {'dup rate':>9} {'vs 1.000':>9}")
    for kind, sched in GATE_TRIGGERS.items():
        rate = result["duplicate_rate"].get(kind)
        rate_s = "—" if rate is None else f"{rate:.4f}"
        delta = "—" if rate is None else f"{rate - result['baseline_duplicate_rate']:+.4f}"
        lines.append(f"{sched:<20} {result['cuts'][kind]:>6} {rate_s:>9} {delta:>9}")
    lines.append(f"{'total':<20} {result['total_cuts']:>6}")
    lines.append("")
    lines.append(f"violations: {len(result['violations'])}  canary misses: "
                 f"{len(result['canary_misses'])}  anomalies: {len(result['anomalies'])}")
    for f in result["failures"]:
        lines.append(f"FAIL: {f}")
    return "\n".join(lines)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("run_dirs", nargs="+")
    p.add_argument("--allow-anomalies", action="store_true")
    args = p.parse_args(argv)
    # run.py writes crash-postmortem.json only when the final pass did not
    # run; its presence is a crashed run whatever episodes.jsonl says.
    crashed = [d for d in args.run_dirs
               if os.path.exists(os.path.join(d, report.CRASH_POSTMORTEM_FILE))]
    if crashed:
        print(f"FAIL: crash post-mortem present in {crashed}")
        return 1
    runs = {d: report.load_episodes(d) for d in args.run_dirs}
    result = campaign_gate(runs, allow_anomalies=args.allow_anomalies)
    print(render_gate(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Run to verify it passes:** `cd chaos/orchestrator && python3 -m unittest test_gate -v`. Expected: `OK`.
- [ ] **Commit:** `feat(chaos): Foundations campaign gate`

---

### Task 9.21: Local verification and the PR (agent)

*PR-level merge gate: PR 9's PR-merge gate (orchestrator suite, `cargo check --all-targets --locked` in `chaos/`, `act -j harnesses`).*

**Files:** none new.

**Interfaces:** none.

- [ ] **Run the whole oracle suite.**
  - Command: `cd chaos/orchestrator && python3 -m unittest discover -p 'test_*.py'`
  - Expected: `OK`. If PR 1, PR 2, PR 4 or PR 6 has not merged into this branch's base yet, three tests are expected to fail and are listed in the PR body: `TestDaemonCliContract.test_every_flag_daemon_start_emits_is_a_real_weir_server_flag` (needs PR 4), `TestDaemonCliContract.test_the_foundations_flags_are_real_weir_server_flags` (needs PR 4 and PR 6) and `TestDaemonLogContract` (needs PRs 1, 2 and 4). Rebase onto the merged main before review.
- [ ] **Mutation-check the guards the gate relies on** (project rule: a guard is theatre until verified by mutation). Each mutation must turn its named test red; revert each one after checking.
  - `faults.shutdown_verdict`: change `>` to `>=`. Expect `test_alive_exactly_at_the_bound_is_not_an_anomaly` to fail.
  - `LiveOps.unstall_sink` / `_sigterm_at`: delete the `finally:` unstall. Expect `test_the_sink_is_released_even_when_the_cut_raises` to fail.
  - `faults.fault_plan`: delete the `step["recovery_delay_ms"] = ...` line. Expect the `sigterm_in_recovery` subtest of `test_every_kind_runs_on_its_own_plan_step` to fail with `KeyError`.
  - `faults.oracle_fault`: return `"kill_random"` for power-cut triggers. Expect `test_the_oracle_sees_a_power_cut_as_power_loss` to fail.
  - `run.final_pass`: remove step 3b. Expect `test_a_bounded_budget_gets_a_drain_out_before_the_zero_slack_check` to fail.
  - `gate._is_cut`: drop the `cut == "power_loss"` clause. Expect `test_aborted_and_kill_cut_episodes_are_not_cuts` to fail.
  - `gate.campaign_gate`: delete the `if no_final:` failure. Expect `test_a_run_without_a_final_pass_fails` to fail.
  - `admin.check_socket_path`: drop the mode check. Expect `test_a_socket_that_is_not_0600_is_refused_before_connecting` to fail.
- [ ] **Compile the chaos crate** (unchanged, but CI gates it): `cd chaos && cargo check --all-targets --locked`. Expected: exit 0.
- [ ] **Run the real CI job locally** (MEMORY: run the real workflow with `act` before pushing): `act -W .github/workflows/ci.yml -j harnesses`. Expected: every step green, including "Chaos orchestrator unit tests (the oracle)".
- [ ] **Open the PR** against main. Do not merge it before the campaign.
  - The body states that this is PR 9 of Foundations, harness-only, with no weir behaviour change, and that its merge does not need the campaign. The 4.1.0 tag does.
  - It lists the three contract tests that depend on earlier PRs being merged: `TestDaemonCliContract.test_every_flag_daemon_start_emits_is_a_real_weir_server_flag` (needs PR 4), `TestDaemonCliContract.test_the_foundations_flags_are_real_weir_server_flags` (needs PR 4 and PR 6) and `TestDaemonLogContract` (needs PRs 1, 2 and 4).
  - It ends with the footer `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
  - Commits have no trailers.

---

### Task 9.22: OWNER on beast — build and pre-flight smoke (root, dm-flakey)

*Gate: PR 9's release gate (owner on beast: `python3 orchestrator/gate.py <4 run dirs>` exits 0, then the write-up); not a per-PR CI gate.*

**Agent cannot do this:** it needs root, device-mapper and beast (run.py:816-817; the README requirements).

**Files:** none committed.

**Interfaces:** none.

- [ ] **Sync and build** on beast, from the weir repo root, on the merged main that contains PRs 1-9:
  - `cargo build --release -p weir-server --features test-hooks`
  - `cd chaos && cargo build --release`
  - Confirm the daemon log carries the loud test-hooks WARN on its first start.
- [ ] **Load the fault module:** `sudo modprobe dm-flakey`.
- [ ] **Smoke each schedule for 5 episodes, under a distinct seed** so the real run's `runs/<id>` is never pre-empted (run.py refuses a non-empty run dir). From `chaos/`:
  ```bash
  for s in shutdown-bounded shutdown-forced startup-signal pause-resume-cut; do
    sed -e 's/^episodes = 300/episodes = 5/' \
        -e "s/^seed = 0xF0D5/seed = 0x5E0D5/" \
        "schedules/$s.toml" > "/tmp/smoke-$s.toml"
    sudo python3 orchestrator/run.py "/tmp/smoke-$s.toml" || echo "SMOKE FAILED: $s"
  done
  ```
  Expected, per smoke run:
  - exit 0;
  - `report.md` has a "Lifecycle triggers" row with `5/5` canary bit;
  - `startup-signal` shows no `recovery_phase_not_observed`. If it does, the binary was built without `test-hooks`; rebuild with `--features test-hooks` first;
  - `pause-resume-cut` shows pause observed in 5/5;
  - the final pass shows `drain_out` quiesced.
- [ ] **Check the crash path once:** start any smoke schedule and `sudo kill -TERM <run.py pid>` mid-episode. Expected:
  - the loop device and mount are released;
  - `runs/<id>/crash-postmortem.json` exists;
  - `report.md` shows "## Crash post-mortem";
  - `wab.img` is kept.
- [ ] **Delete the smoke run dirs and images** before the campaign.

---

### Task 9.23: OWNER on beast — the campaign, the gate and the write-up

*Gate: PR 9's release gate (owner on beast: `python3 orchestrator/gate.py <4 run dirs>` exits 0, then the write-up); not a per-PR CI gate.*

**Agent cannot run this.** The agent may draft the write-up from the owner's `runs/` output afterwards.

**Files:**
- Create: `docs/benchmarks/chaos-foundations/<YYYY-MM-DD>-lifecycle-campaign.md`.
- Modify: `chaos/README.md` (a status block at the top of the Foundations section).

**Interfaces:** none.

- [ ] **Run the four schedules**, one at a time, each unattended under tmux (chaos/docs-unattended-runs.md):
  - `./run-soak.sh schedules/shutdown-bounded.toml fnd-shutdown-bounded 25200`
  - `./run-soak.sh schedules/shutdown-forced.toml fnd-shutdown-forced 25200`
  - `./run-soak.sh schedules/startup-signal.toml fnd-startup-signal 25200`
  - `./run-soak.sh schedules/pause-resume-cut.toml fnd-pause-resume-cut 25200`

  The backstop of 7 h sits above each schedule's 6 h `max_duration_secs`. Hold a sleep inhibitor for the run, as that doc says.
- [ ] **Run the gate.**
  - Command: `cd chaos && python3 orchestrator/gate.py runs/<id-bounded> runs/<id-forced> runs/<id-startup> runs/<id-pause>`
  - Expected: `Foundations chaos gate: PASS`, exit 0:
    - at least 1,000 cuts, and at least 200 per schedule;
    - 0 violations;
    - 0 canary misses;
    - 0 anomalies, or each one explained and the gate re-run with `--allow-anomalies`;
    - a final pass in every run, and no `crash-postmortem.json` in any run dir (a run that crashed or hit the 7 h backstop is re-run, not allowed).
  - Any violation is treated as an oracle defect until proven otherwise (README "The gate is zero FALSE violations"). Check `recorder.log`, quiescence and the drain-out record before blaming weir.
- [ ] **Write it up** in `docs/benchmarks/chaos-foundations/<date>-lifecycle-campaign.md`. Include:
  - the gate output verbatim;
  - the per-trigger table from each `report.md`;
  - where the cuts landed (during shutdown vs after exit; stalled vs not);
  - forced observed, pause observed, and the duplicate rate per schedule against the phase-2 baseline of 1.000, explaining any rise with the §7 abandoned-prefix window;
  - every allowed anomaly with its cause;
  - the weir commit (with `-dirty` if so), kernel and hardware from `SETUP.md`.

  Also add a status block to the README's Foundations section.
- [ ] **Commit** (on a docs branch, then a PR): `docs(chaos): Foundations lifecycle campaign, <N> cuts, 0 violations`, with no trailers. This campaign passing is the precondition for the Foundations release tag.
