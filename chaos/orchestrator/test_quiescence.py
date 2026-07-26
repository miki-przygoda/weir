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

    def test_a_missing_wab_bytes_gauge_never_counts_as_quiesced(self):
        # A partial scrape must not read as "drained". Every default has to be
        # the conservative one, because a false True here becomes a phantom
        # durability violation two steps downstream.
        partial = {
            "weir_queue_depth": 0.0,
            'weir_drain_state{state="draining"}': 1.0,
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent wab_bytes must never satisfy quiescence")
        self.assertIn("timeout", reason)

    def test_a_missing_queue_depth_never_counts_as_quiesced(self):
        partial = {
            "weir_wab_bytes_on_disk": 8192.0,
            'weir_drain_state{state="draining"}': 1.0,
        }
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent queue_depth must default to non-quiesced")

    def test_an_always_failing_scrape_is_reported_as_a_harness_problem(self):
        # A wrong URL and a genuinely stuck drain must not produce the same
        # reason string — on a multi-day run that ambiguity costs hours.
        def boom(_):
            raise ConnectionRefusedError("connection refused")

        ok, reason = quiescence.wait_for_quiescence(
            "http://127.0.0.1:1/metrics", timeout_s=0.01, scrape_fn=boom,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("harness", reason)
        self.assertIn("ConnectionRefusedError", reason)

    def test_the_poll_interval_cannot_overshoot_the_timeout(self):
        import time as _time

        readings = {"weir_queue_depth": 5.0, "weir_wab_bytes_on_disk": 1.0,
                    'weir_drain_state{state="draining"}': 1.0}
        start = _time.monotonic()
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.2, scrape_fn=lambda _: readings,
            poll_interval_s=3.0, stable_polls=3,
        )
        elapsed = _time.monotonic() - start
        self.assertFalse(ok)
        self.assertLess(
            elapsed, 1.0,
            f"a 3s poll interval must not overshoot a 0.2s timeout; took {elapsed:.2f}s",
        )


class TestParse2(unittest.TestCase):
    def test_a_trailing_timestamp_does_not_corrupt_the_key_or_value(self):
        # OpenMetrics allows `name value timestamp`. Reading the timestamp as
        # the value would also fold the real value into the key, so the
        # canonical key vanishes and quiescence silently reads a default.
        m = quiescence.parse("weir_wab_bytes_on_disk 4096.0 1721990400000\n")
        self.assertEqual(m["weir_wab_bytes_on_disk"], 4096.0)

    def test_a_label_value_containing_a_space_survives(self):
        m = quiescence.parse('foo{note="a b"} 7.0\n')
        self.assertEqual(m['foo{note="a b"}'], 7.0)

    def test_lines_without_a_value_are_skipped(self):
        m = quiescence.parse("garbage\nweir_queue_depth 0.0\n")
        self.assertEqual(m, {"weir_queue_depth": 0.0})


if __name__ == "__main__":
    unittest.main()
