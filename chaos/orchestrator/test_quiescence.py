"""Tests for drain-quiescence detection.

Parsing is tested against real OpenMetrics text; the wait loop is tested with
an injected scrape function so no daemon is required.
"""
import unittest

import quiescence

SAMPLE = """\
# HELP weir_queue_depth Work queue occupancy
# TYPE weir_queue_depth gauge
weir_queue_depth 0.0
# HELP weir_wab_bytes_on_disk WAB directory size
# TYPE weir_wab_bytes_on_disk gauge
weir_wab_bytes_on_disk 4096.0
# HELP weir_drain_state Drain state
# TYPE weir_drain_state gauge
weir_drain_state{state="draining"} 1.0
weir_drain_state{state="retrying_transient"} 0.0
weir_drain_state{state="blocked_dead_letter_full"} 0.0
"""


class TestParse(unittest.TestCase):
    def test_parses_plain_and_labelled_gauges(self):
        m = quiescence.parse(SAMPLE)
        self.assertEqual(m["weir_queue_depth"], 0.0)
        self.assertEqual(m["weir_wab_bytes_on_disk"], 4096.0)
        self.assertEqual(m['weir_drain_state{state="draining"}'], 1.0)

    def test_ignores_help_and_type_lines(self):
        m = quiescence.parse(SAMPLE)
        self.assertNotIn("# HELP", m)
        self.assertEqual(len([k for k in m if k.startswith("weir_drain_state")]), 3)


class TestWait(unittest.TestCase):
    def test_quiesces_when_all_three_signals_settle(self):
        # Bytes must be STABLE across consecutive polls, not merely low.
        readings = [
            {"weir_queue_depth": 5.0, "weir_wab_bytes_on_disk": 900000.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
        ]
        # Five readings, not four: stability is measured BETWEEN consecutive
        # polls, so the first stable reading only establishes the baseline.
        # Poll 1 sets last_bytes, poll 2 changes it, polls 3-5 are the three
        # stable comparisons `stable_polls=3` requires.
        it = iter(readings)
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: next(it),
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_reports_stuck_when_drain_is_blocked(self):
        blocked = {
            "weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
            'weir_drain_state{state="draining"}': 0.0,
            'weir_drain_state{state="blocked_dead_letter_full"}': 1.0,
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: blocked,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("blocked", reason)

    def test_reports_stuck_when_bytes_never_settle(self):
        counter = {"n": 0}

        def growing(_):
            counter["n"] += 1
            return {
                "weir_queue_depth": 0.0,
                "weir_wab_bytes_on_disk": 1000.0 * counter["n"],
                'weir_drain_state{state="draining"}': 1.0,
            }

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=growing,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("timeout", reason)


if __name__ == "__main__":
    unittest.main()
