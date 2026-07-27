"""Tests for schedule parsing, the seeded random killer, the no-progress
floor, and the Rust<->Python CLI contract.

The episode loop itself needs root and a real daemon; it is exercised by the
Task 9 end-to-end gate, not here.
"""
import os
import unittest
from unittest import mock

import run


class TestSchedule(unittest.TestCase):
    def test_parses_the_smoke_schedule(self):
        s = run.load_schedule("../schedules/smoke.toml")
        self.assertEqual(s["seed"], 0x5EED)
        self.assertGreater(s["episodes"], 0)

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


if __name__ == "__main__":
    unittest.main()
