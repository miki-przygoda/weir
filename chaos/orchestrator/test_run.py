"""Tests for schedule parsing, the seeded random killer, the no-progress
floor, the Rust<->Python CLI contract, and the teardown-time final pass.

The episode loop itself needs root and a real daemon; it is exercised by the
Task 9 end-to-end gate, not here. `final_pass` and `stop_loadgen` are written
against injected collaborators precisely so they do NOT need either.
"""
import json
import os
import re
import signal
import subprocess
import tempfile
import unittest
from unittest import mock

import quiescence
import report
import run
import verify


class FakeProc:
    """A `subprocess.Popen` stand-in that records the signals it receives.

    `wait_raises` makes `wait()` raise `TimeoutExpired` once, so the escalation
    path (kill, then report the loss) is reachable without a real process.
    """

    def __init__(self, returncode=None, wait_raises=False, log=None):
        self.returncode = returncode
        self.signals = []
        self.wait_raises = wait_raises
        self.log = log if log is not None else []

    def poll(self):
        return self.returncode

    def send_signal(self, sig):
        self.signals.append(sig)

    def terminate(self):
        self.signals.append(signal.SIGTERM)

    def kill(self):
        self.signals.append(signal.SIGKILL)
        self.returncode = -9

    def wait(self, timeout=None):
        self.log.append("loadgen.wait")
        if self.wait_raises:
            self.wait_raises = False
            raise subprocess.TimeoutExpired("loadgen", timeout)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class FakeDaemon:
    """A `run.Daemon` stand-in whose shutdown is scripted."""

    def __init__(self, alive=True, clean_stop=True, log=None):
        self.proc = FakeProc(returncode=None if alive else 0)
        self.metrics_url = "http://127.0.0.1:19185/metrics"
        self.wab_dir = "/unused/wab"
        self._clean_stop = clean_stop
        self.log = log if log is not None else []

    def stop(self):
        self.log.append("daemon.stop")
        self.proc.returncode = 0 if self._clean_stop else -9
        return self._clean_stop


class FakeTailer:
    """A `verify.LogTailer` stand-in. `raises` reproduces the refusal a real
    tailer issues when an append-only log shrank."""

    def __init__(self, name, lines=(), log=None, raises=None):
        self.name = name
        self.lines = list(lines)
        self.log = log if log is not None else []
        self.raises = raises

    def read_new(self):
        self.log.append(f"{self.name}.read_new")
        if self.raises:
            raise self.raises
        lines, self.lines = self.lines, []
        return lines


def final_pass_fixture(
    log=None, loadgen=None, daemon=None, recorder_alive=True, ledger=(),
    delivered=(), ledger_raises=None, residue=None, scrape_raises=None,
    frontier_slack=2048, run_id=7,
):
    """Builds the collaborators `run.final_pass` needs, all fake but for the
    accumulator — the real oracle, so these tests exercise the real I1."""
    log = [] if log is None else log
    residue = residue if residue is not None else quiescence.Residue(0, 0)

    def scrape_fn(url):
        log.append("scrape")
        if scrape_raises:
            raise scrape_raises
        return {"weir_queue_depth": 0.0, quiescence.STRANDED: 2.0,
                quiescence.RESUMED: 2.0}

    def residue_fn(wab_dir):
        log.append("residue_scan")
        if isinstance(residue, Exception):
            raise residue
        return residue

    return dict(
        loadgen=FakeProc(log=log) if loadgen is None else loadgen,
        daemon=FakeDaemon(log=log) if daemon is None else daemon,
        recorder=FakeProc(returncode=None if recorder_alive else 0),
        acc=verify.Accumulator(delivered_run_id=run_id),
        ledger_tail=FakeTailer("ledger", ledger, log, raises=ledger_raises),
        delivered_tail=FakeTailer("delivered", delivered, log),
        wab_dir="/unused/wab",
        frontier_slack=frontier_slack,
        seed=0x5EED,
        scrape_fn=scrape_fn,
        residue_fn=residue_fn,
        sleep_fn=lambda _s: None,
    ), log


class TestSchedule(unittest.TestCase):
    def test_parses_the_smoke_schedule(self):
        s = run.load_schedule("../schedules/smoke.toml")
        self.assertEqual(s["seed"], 0x5EED)
        self.assertGreater(s["episodes"], 0)

    def test_the_invocation_the_readme_documents_actually_works(self):
        # The first real container run died here, before doing any work:
        # load_schedule resolved a relative path against THIS file's directory,
        # so `cd chaos && python3 orchestrator/run.py schedules/smoke.toml`
        # looked for orchestrator/schedules/smoke.toml.
        #
        # The test above did not catch it because `../schedules/smoke.toml`
        # from inside orchestrator/ resolves correctly under BOTH rules — it
        # pinned the bug instead of finding it. This one runs from the working
        # directory the README tells the operator to use, with the exact path
        # the README tells them to pass.
        prev = os.getcwd()
        os.chdir(run.CHAOS_ROOT)
        try:
            s = run.load_schedule("schedules/smoke.toml")
        finally:
            os.chdir(prev)
        self.assertEqual(s["seed"], 0x5EED)

    def test_run_id_is_derived_from_the_seed(self):
        # Same seed must give the same run_id, so a replay cannot collide with
        # or be confused for a different run.
        self.assertEqual(run.run_id_from_seed(0x5EED), run.run_id_from_seed(0x5EED))
        self.assertNotEqual(run.run_id_from_seed(1), run.run_id_from_seed(2))

    def test_schedule_has_progress_floors(self):
        # C2: these gate the no-progress check, so their absence would mean
        # every episode's floor check silently KeyErrors instead of running.
        s = run.load_schedule("../schedules/smoke.toml")
        self.assertGreater(s["load"]["min_acked_per_episode"], 0)
        self.assertGreater(s["load"]["min_delivered_per_episode"], 0)


class TestFaultDispatch(unittest.TestCase):
    def test_an_empty_faults_table_still_means_kill_random(self):
        # Every Phase 1 schedule ships `[faults]` empty. They must keep doing
        # exactly what they did before, or five banked soaks stop being
        # comparable to anything run after this.
        self.assertEqual(run.fault_kind({"faults": {}}), "kill_random")
        self.assertEqual(run.fault_kind({}), "kill_random")

    def test_power_loss_is_selected_explicitly(self):
        self.assertEqual(
            run.fault_kind({"faults": {"kind": "power_loss"}}), "power_loss")

    def test_an_unknown_fault_kind_is_refused_loudly(self):
        # Falling back to kill_random would silently run a Phase 1 episode
        # under a Phase 2 schedule and report it as power loss.
        with self.assertRaises(ValueError):
            run.fault_kind({"faults": {"kind": "typo"}})


class TestScheduleCoherence(unittest.TestCase):
    """A `linear` dm target is dm-flakey's pass-through stand-in (see
    dm_stack.py) — it builds a real dm layer but injects nothing. Paired with
    `fault.kind = "power_loss"` it would run an episode that injects nothing,
    loses nothing, and reports green: indistinguishable from a genuine pass,
    and outside the negative control's reach too, since that only fires on
    the Buffered tier (report.powerloss_verdict, Task 7).

    Refused HERE, at schedule-load time, via `load_schedule` — the real
    entry point every invocation goes through — rather than only once an
    episode reaches `engage_fault()`, so a misconfigured schedule fails
    before steady-state load even starts.
    """

    def _write(self, tmp, faults_kind=None, dm_target=None):
        lines = []
        if dm_target is not None:
            lines += ["[storage]", f'dm_target = "{dm_target}"']
        if faults_kind is not None:
            lines += ["[faults]", f'kind = "{faults_kind}"']
        path = os.path.join(tmp, "sched.toml")
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        return path

    def test_linear_paired_with_power_loss_is_refused_at_load_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, faults_kind="power_loss", dm_target="linear")
            with self.assertRaises(ValueError):
                run.load_schedule(path)

    def test_flakey_paired_with_power_loss_loads_fine(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, faults_kind="power_loss", dm_target="flakey")
            s = run.load_schedule(path)
            self.assertEqual(s["faults"]["kind"], "power_loss")

    def test_linear_paired_with_kill_random_loads_fine(self):
        # linear's only legitimate use is validating plumbing on a machine
        # without dm-flakey (the Pi), which only makes sense under
        # kill_random — dm-flakey is what power_loss actually needs.
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, faults_kind="kill_random", dm_target="linear")
            s = run.load_schedule(path)
            self.assertEqual(s["storage"]["dm_target"], "linear")

    def test_no_dm_target_at_all_paired_with_power_loss_loads_fine(self):
        # The guard is specifically about `linear` — a real, deliberately
        # built pass-through. An absent dm_target is Phase 1's ordinary
        # default and is caught elsewhere (StorageStack.engage_fault raises
        # loudly if the flakey layer was never built); it is not this
        # guard's job to duplicate that check.
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, faults_kind="power_loss")
            s = run.load_schedule(path)
            self.assertEqual(s["faults"]["kind"], "power_loss")


class TestSeededKiller(unittest.TestCase):
    def test_kill_delays_are_reproducible_from_the_seed(self):
        a = run.kill_delays(seed=42, count=10, lo=1.0, hi=5.0)
        b = run.kill_delays(seed=42, count=10, lo=1.0, hi=5.0)
        c = run.kill_delays(seed=43, count=10, lo=1.0, hi=5.0)
        self.assertEqual(a, b, "same seed must reproduce the schedule")
        self.assertNotEqual(a, c)
        self.assertTrue(all(1.0 <= d <= 5.0 for d in a))


class TestProgressFloor(unittest.TestCase):
    """C2: a run where weir refuses or delivers nothing must not pass. These
    pin the pure predicate without needing a live daemon."""

    def test_breached_when_either_delta_is_below_its_floor(self):
        self.assertTrue(
            run.progress_floor_breached(0, 500, min_acked=100, min_delivered=100)
        )
        self.assertTrue(
            run.progress_floor_breached(500, 0, min_acked=100, min_delivered=100)
        )

    def test_not_breached_when_both_deltas_clear_their_floor(self):
        self.assertFalse(
            run.progress_floor_breached(500, 500, min_acked=100, min_delivered=100)
        )

    def test_exactly_at_the_floor_does_not_breach(self):
        self.assertFalse(
            run.progress_floor_breached(100, 100, min_acked=100, min_delivered=100)
        )


class TestDaemonCliContract(unittest.TestCase):
    """`Daemon.start()`'s argv is a pure function of its config, so pinning
    every flag it emits against weir-server's real CLI parser catches a
    Rust<->Python drift without needing root, a real binary, or a real
    socket — run.py has ~25 of 358 lines under test otherwise, and this is
    the single highest-value test available for a file that can't
    otherwise run here (macOS, no root)."""

    def test_every_flag_daemon_start_emits_is_a_real_weir_server_flag(self):
        captured = {}

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            proc = mock.Mock()
            proc.poll.return_value = None
            return proc

        cfg = {
            "shard_count": 4,
            "batch_size": 64,
            "batch_deadline_ms": 2,
            "wab_segment_max_bytes": 8388608,
        }
        daemon = run.Daemon(
            binary="/fake/weir-server",
            wab_dir="/fake/wab",
            socket_path="/fake/weir.sock",
            metrics_port=19185,
            cfg=cfg,
            log_file=None,
        )

        with mock.patch.object(run.subprocess, "Popen", fake_popen), \
             mock.patch("os.path.exists", return_value=True):
            daemon.start("http://127.0.0.1:9900/ingest")

        flags = [tok for tok in captured["cmd"] if tok.startswith("--")]
        self.assertTrue(flags, "Daemon.start() emitted no flags to check")

        cli_rs_path = os.path.join(
            run.WEIR_ROOT, "crates", "weir-server", "src", "config", "cli.rs"
        )
        with open(cli_rs_path) as f:
            cli_source = f.read()

        for flag in flags:
            self.assertIn(
                flag, cli_source,
                f"{flag!r} is emitted by Daemon.start() but does not appear in "
                f"weir-server's CLI parser ({cli_rs_path}) — the Rust<->Python "
                "CLI contract has drifted",
            )


class TestFrontierSlackContract(unittest.TestCase):
    """`frontier_slack = threads * LEDGER_FLUSH_THRESHOLD` hard-codes loadgen's
    per-thread ledger-flush threshold as a bare Python constant, with nothing
    pinning the two together. Pin them the same way TestDaemonCliContract
    pins the Rust<->Python CLI flags, so a change to loadgen.rs's constant
    without a matching update here doesn't silently reopen I3 (the frontier
    exemption would then be computed against the wrong bound)."""

    def test_ledger_flush_threshold_matches_loadgen_rs(self):
        loadgen_rs_path = os.path.join(run.CHAOS_ROOT, "src", "bin", "loadgen.rs")
        with open(loadgen_rs_path) as f:
            source = f.read()

        match = re.search(
            r"const\s+LEDGER_FLUSH_THRESHOLD\s*:\s*usize\s*=\s*(\d+)\s*;", source
        )
        self.assertIsNotNone(
            match, f"could not find LEDGER_FLUSH_THRESHOLD in {loadgen_rs_path}"
        )
        rust_value = int(match.group(1))
        self.assertEqual(
            run.LEDGER_FLUSH_THRESHOLD, rust_value,
            "run.py's LEDGER_FLUSH_THRESHOLD has drifted from loadgen.rs's "
            "constant of the same name — this bounds the I3 frontier "
            "exemption, so a mismatch silently changes how much in-flight "
            "work is excused from the orphan/I1 checks",
        )


class TestStopLoadgen(unittest.TestCase):
    """D2: the producer must be resumed, asked to stop, and REAPED — in that
    order — before the daemon is sent its own SIGTERM."""

    def test_resumes_then_terminates_then_waits(self):
        proc = FakeProc()
        code, forced = run.stop_loadgen(proc)
        self.assertEqual(
            proc.signals, [signal.SIGCONT, signal.SIGTERM],
            "SIGCONT must come first: loadgen CATCHES SIGTERM now, and a caught "
            "signal is not delivered to a SIGSTOPped process until it resumes",
        )
        self.assertIn("loadgen.wait", proc.log, "an unreaped loadgen races weir's own SIGTERM")
        self.assertEqual((code, forced), (0, False))

    def test_an_already_exited_loadgen_is_not_signalled_again(self):
        proc = FakeProc(returncode=1)
        code, forced = run.stop_loadgen(proc)
        self.assertEqual(proc.signals, [])
        self.assertEqual(
            (code, forced), (1, False),
            "exit code 1 means loadgen lost ledger entries — it must reach the "
            "caller intact, not be normalised away",
        )

    def test_a_loadgen_that_will_not_stop_is_killed_and_reported_as_forced(self):
        proc = FakeProc(wait_raises=True)
        code, forced = run.stop_loadgen(proc, timeout_secs=0.01)
        self.assertIn(signal.SIGKILL, proc.signals)
        self.assertTrue(
            forced,
            "a killed loadgen lost its ledger tail; swallowing that would let "
            "the final pass run frontier_slack=0 against a truncated ledger",
        )
        self.assertEqual(code, -9)

    def test_no_loadgen_at_all_is_not_an_error(self):
        self.assertEqual(run.stop_loadgen(None), (None, False))


class TestCumulativeDeltas(unittest.TestCase):
    def test_deltas_are_taken_against_the_previous_cumulative_totals(self):
        first = verify.check({1: ("ACK", ""), 2: ("NACK", "")}, [1])
        deltas, totals = run.cumulative_deltas(first, {})
        self.assertEqual(deltas, totals)
        self.assertEqual(deltas["acked"], 1)
        self.assertEqual(deltas["nacked"], 1)
        self.assertEqual(deltas["pushed"], 2)

        second = verify.check(
            {1: ("ACK", ""), 2: ("NACK", ""), 3: ("ACK", "")}, [1, 3]
        )
        deltas, _ = run.cumulative_deltas(second, totals)
        self.assertEqual(
            deltas, {"acked": 1, "delivered": 1, "nacked": 0, "pushed": 1},
            "nacked/pushed must be per-episode deltas like acked/delivered — a "
            "running total under the same heading is the mislabelling I1 fixed",
        )


class TestFinalPass(unittest.TestCase):
    """D1: the one verification pass that runs with the producer stopped and
    the drain given a real chance to finish. Every collaborator is injected,
    so the ordering and the post-mortem are testable without root."""

    def test_tier_and_fault_reach_the_accumulator_and_expected_loss_lands_in_the_record(self):
        # Folded item 1 (Task 7): without this wiring, acc.check() never
        # learns the tier/fault, expected_loss stays 0 forever, and
        # report.powerloss_verdict would read "inconclusive" on every real
        # run regardless of what actually happened.
        kwargs, _ = final_pass_fixture(
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
        )
        record, violations, anomalies = run.final_pass(
            **kwargs, tier="U", fault="power_loss",
        )
        self.assertTrue(
            record["ok"],
            "a Buffered ack lost under power_loss is the exempted case, not "
            "a violation",
        )
        self.assertEqual(record["i1_missing"], [])
        self.assertEqual(record["expected_loss"], 1)
        self.assertEqual(record["tier"], "U")
        self.assertEqual((violations, anomalies), (0, 0))

    def test_no_tier_or_fault_is_exactly_phase_1_behaviour(self):
        # Defaults must reproduce the pre-Task-7 result byte for byte: the
        # same missing seq is a violation, not an exemption, and
        # expected_loss stays 0.
        kwargs, _ = final_pass_fixture(
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertFalse(record["ok"])
        self.assertEqual(record["i1_missing"], [2])
        self.assertEqual(record["expected_loss"], 0)
        self.assertIsNone(record["tier"])
        self.assertEqual((violations, anomalies), (1, 0))

    def test_it_runs_its_steps_in_the_only_order_that_works(self):
        kwargs, log = final_pass_fixture()
        run.final_pass(**kwargs)
        self.assertEqual(
            log,
            ["loadgen.wait", "scrape", "daemon.stop",
             "ledger.read_new", "delivered.read_new", "residue_scan"],
            "the producer must be reaped before the daemon is asked to drain; "
            "/metrics must be scraped while the daemon is still alive; the WAB "
            "post-mortem must happen before the caller's stack.teardown()",
        )

    def test_a_clean_shutdown_verifies_with_zero_frontier_slack(self):
        kwargs, _ = final_pass_fixture()
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertEqual(
            record["frontier_slack"], 0,
            "the producer is stopped and both logs are complete: this is the "
            "one instant where 'no exemption' is a true statement",
        )
        self.assertFalse(record["advisory"])
        self.assertEqual((violations, anomalies), (0, 0))
        self.assertEqual(record["episode"], "final")

    def test_an_acked_but_undelivered_record_after_a_clean_shutdown_is_a_violation(self):
        kwargs, _ = final_pass_fixture(
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertFalse(record["ok"])
        self.assertEqual(record["i1_missing"], [2])
        self.assertEqual(
            (violations, anomalies), (1, 0),
            "with zero slack and a clean shutdown, an undelivered acked record "
            "is a durability violation, not an anomaly",
        )

    def test_a_dirty_loadgen_exit_falls_back_to_the_normal_slack_and_is_advisory(self):
        # loadgen exit code 1 is its own "ledger entries lost — verification
        # cannot be trusted". A zero-slack pass on a truncated ledger is
        # unsound, so the same missing record must NOT read as a violation.
        kwargs, _ = final_pass_fixture(
            loadgen=FakeProc(returncode=1),
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertTrue(record["advisory"])
        self.assertEqual(record["frontier_slack"], 2048)
        self.assertEqual(record["loadgen_exit_code"], 1)
        self.assertEqual(record["i1_exempt"], 1)
        self.assertEqual((violations, anomalies), (0, 1))
        self.assertTrue(
            any("loadgen_dirty_exit" in r for r in record["anomaly_reasons"])
        )

    def test_a_killed_loadgen_is_dirty_too(self):
        kwargs, _ = final_pass_fixture(loadgen=FakeProc(wait_raises=True))
        record, _, anomalies = run.final_pass(**kwargs)
        self.assertTrue(record["loadgen_forced_kill"])
        self.assertTrue(record["advisory"])
        self.assertEqual(anomalies, 1)

    def test_a_shutdown_drain_that_had_to_be_killed_is_an_anomaly_not_a_violation(self):
        # An un-drained segment after a forced kill is not a durability
        # failure: weir was never given time to finish. Holding it to
        # frontier_slack=0 would manufacture tens of thousands of I1 misses.
        kwargs, _ = final_pass_fixture(
            daemon=FakeDaemon(clean_stop=False),
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertFalse(record["daemon_clean_stop"])
        self.assertTrue(record["advisory"])
        self.assertIn("daemon_kill_at_stop", record["anomaly_reasons"])
        self.assertEqual((violations, anomalies), (0, 1))

    def test_a_daemon_that_was_already_dead_is_not_read_as_a_clean_shutdown(self):
        # Daemon.stop() returns True for an already-exited process, so without
        # an explicit liveness check the run would credit weir with a drain it
        # never performed and then hold it to I1 for the result.
        kwargs, log = final_pass_fixture(daemon=FakeDaemon(alive=False))
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertFalse(record["daemon_alive_before_stop"])
        self.assertIn("daemon_not_running_at_final_pass", record["anomaly_reasons"])
        self.assertTrue(record["advisory"])
        self.assertNotIn("scrape", log, "nothing to scrape from a dead daemon")
        self.assertEqual((violations, anomalies), (0, 1))

    def test_a_dead_recorder_makes_the_shutdown_drain_sinkless_and_the_check_advisory(self):
        kwargs, _ = final_pass_fixture(recorder_alive=False)
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertFalse(record["recorder_alive"])
        self.assertIn("recorder_not_running", record["anomaly_reasons"])
        self.assertTrue(record["advisory"])
        self.assertEqual((violations, anomalies), (0, 1))

    def test_the_last_metrics_scrape_is_kept_in_the_run_record(self):
        kwargs, _ = final_pass_fixture()
        record, _, _ = run.final_pass(**kwargs)
        self.assertEqual(record["final_metrics"]["stranded"], 2.0)
        self.assertEqual(record["final_metrics"]["resumed"], 2.0)
        self.assertEqual(record["final_metrics"]["queue_depth"], 0.0)

    def test_a_failed_final_scrape_is_reported_not_swallowed(self):
        kwargs, _ = final_pass_fixture(scrape_raises=OSError("connection refused"))
        record, _, anomalies = run.final_pass(**kwargs)
        self.assertIn("final_metrics_scrape_failed", record["anomaly_reasons"])
        self.assertIn("connection refused", record["final_metrics_error"])
        self.assertEqual(anomalies, 1)

    def test_surviving_wab_files_are_an_anomaly_with_paths_and_sizes(self):
        # This evidence used to be deleted, unread, by stack.teardown().
        survivors = (
            {"kind": "unconfirmed_sealed",
             "path": "/mnt/weir-wab/wab/shard_00/seg_00000007.wab.sealed",
             "size": 8388608},
            {"kind": "dead_letter",
             "path": "/mnt/weir-wab/wab/dead_letter/dl_1.wab", "size": 512},
        )
        kwargs, _ = final_pass_fixture(
            residue=quiescence.Residue(1, 0, 0, 1, survivors)
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertEqual(record["wab_survivor_count"], 2)
        self.assertEqual(record["wab_residue"]["unconfirmed_sealed"], 1)
        self.assertEqual(record["wab_residue"]["dead_letter"], 1)
        self.assertIn("seg_00000007.wab.sealed", record["wab_survivors"][0]["path"])
        self.assertEqual(record["wab_survivors"][0]["size"], 8388608)
        self.assertIn("wab_survivors=2", record["anomaly_reasons"])
        self.assertEqual((violations, anomalies), (0, 1))

    def test_a_verification_that_blows_up_still_gets_the_post_mortem(self):
        # A LogTailer refusal is a real finding, but the WAB directory is what
        # teardown is about to destroy — losing it to an exception would be the
        # same disappearing act D1 exists to stop.
        kwargs, log = final_pass_fixture(
            ledger_raises=RuntimeError("ledger.log shrank"),
            residue=quiescence.Residue(2, 1),
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertIn("ledger.log shrank", record["final_check_error"])
        self.assertIn("final_check_failed_to_run", record["anomaly_reasons"])
        self.assertIn("residue_scan", log, "the post-mortem must still run")
        self.assertEqual(record["wab_residue"]["unconfirmed_sealed"], 2)
        self.assertEqual((violations, anomalies), (0, 1))

    def test_a_failed_post_mortem_scan_is_itself_reported(self):
        kwargs, _ = final_pass_fixture(residue=PermissionError("EACCES"))
        record, _, anomalies = run.final_pass(**kwargs)
        self.assertIn("wab_postmortem_scan_failed", record["anomaly_reasons"])
        self.assertEqual(anomalies, 1)

    def test_the_record_is_json_serialisable(self):
        # run.py appends it to episodes.jsonl; anything non-serialisable here
        # would take the whole run's report down with it at the last step.
        kwargs, _ = final_pass_fixture(
            residue=quiescence.Residue(1, 0, 0, 0, (
                {"kind": "unconfirmed_sealed", "path": "/x", "size": 1},
            )),
            ledger=["1 S 10 20 ACK"], delivered=["7 1"],
        )
        record, _, _ = run.final_pass(**kwargs)
        round_tripped = json.loads(json.dumps(record))
        self.assertEqual(round_tripped["episode"], "final")

    def test_the_record_it_produces_actually_renders_in_the_report(self):
        # The seam between D1 and D3. A finding that lands in episodes.jsonl
        # and never reaches report.md is the same disappearing act the
        # stranded segments already performed, so pin the two ends together
        # rather than testing each against its own idea of the record shape.
        kwargs, _ = final_pass_fixture(
            loadgen=FakeProc(returncode=1),
            daemon=FakeDaemon(clean_stop=False),
            ledger=["1 S 10 20 ACK", "2 S 11 21 ACK"], delivered=["7 1"],
            residue=quiescence.Residue(1, 0, 0, 1, (
                {"kind": "unconfirmed_sealed",
                 "path": "/mnt/weir-wab/wab/shard_00/seg_00000041.wab.sealed",
                 "size": 8388608},
                {"kind": "dead_letter",
                 "path": "/mnt/weir-wab/wab/dead_letter/dl_1.wab", "size": 512},
            )),
        )
        record, _, _ = run.final_pass(**kwargs)
        out = report.render([record], {})
        self.assertIn("| final |", out)
        self.assertIn("seg_00000041.wab.sealed", out)
        self.assertIn("daemon_kill_at_stop", out)
        self.assertIn("loadgen_dirty_exit", out)
        self.assertIn("Shutdown drain completed without a kill | **NO**", out)
        self.assertIn("0 violations", out)
        self.assertIn("1 anomaly", out)

    def test_at_most_one_violation_and_one_anomaly_come_from_the_final_pass(self):
        # It is ONE row in episodes.jsonl, and report.py counts rows. Emitting
        # a count per problem here would drift run.py's tally away from the
        # report's headline.
        kwargs, _ = final_pass_fixture(
            loadgen=FakeProc(returncode=1),
            daemon=FakeDaemon(clean_stop=False),
            recorder_alive=False,
            residue=quiescence.Residue(3, 2, 1, 1, ()),
            scrape_raises=OSError("gone"),
        )
        record, violations, anomalies = run.final_pass(**kwargs)
        self.assertGreater(len(record["anomaly_reasons"]), 3)
        self.assertEqual((violations, anomalies), (0, 1))


if __name__ == "__main__":
    unittest.main()
