"""Tests for schedule parsing, the seeded random killer, the no-progress
floor, the Rust<->Python CLI contract, and the teardown-time final pass.

The episode loop itself needs root and a real daemon; it is exercised by the
Task 9 end-to-end gate, not here. `final_pass` and `stop_loadgen` are written
against injected collaborators precisely so they do NOT need either.
"""
import os
import re
import signal
import subprocess
import unittest
from unittest import mock

import run


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


if __name__ == "__main__":
    unittest.main()
