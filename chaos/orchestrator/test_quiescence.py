"""Tests for drain-quiescence detection.

Parsing is tested against real OpenMetrics text; the wait loop is tested with
an injected scrape function so no daemon is required.
"""
import time
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


def healthy_reading(**overrides):
    """A fully-quiesced snapshot: every signal in its settled state.

    Individual tests override just the field(s) they want to exercise, so
    each test isolates the one condition it is about.
    """
    base = {
        "weir_queue_depth": 0.0,
        quiescence.DRAINING: 1.0,
        quiescence.SINK_DOWN: 0.0,
        quiescence.SEALED: 10.0,
        quiescence.CONFIRMED: 9.0,
        quiescence.QUARANTINED: 1.0,
        quiescence.STRANDED: 2.0,
        quiescence.RESUMED: 2.0,
    }
    base.update(overrides)
    return base


class TestWait(unittest.TestCase):
    def test_quiesces_when_all_signals_settle(self):
        # Every condition here is a snapshot property of a single scrape (no
        # delta against the previous poll, unlike the bytes-gauge approach
        # this replaced) — so the first reading fails on queue_depth, and the
        # next three consecutive passing readings satisfy stable_polls=3.
        readings = [
            {**healthy_reading(), "weir_queue_depth": 5.0},
            healthy_reading(),
            healthy_reading(),
            healthy_reading(),
        ]
        it = iter(readings)
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: next(it),
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_reports_stuck_when_drain_is_blocked(self):
        blocked = healthy_reading(**{
            quiescence.DRAINING: 0.0,
            'weir_drain_state{state="blocked_dead_letter_full"}': 1.0,
        })
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: blocked,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("blocked", reason)

    def test_reports_stuck_when_sealed_never_resolves(self):
        # Continuous load: sealed_total keeps climbing, confirmed_total (the
        # only terminal state reached in this scenario) always trails by a
        # fixed backlog. This must time out, not quiesce.
        counter = {"n": 0}

        def rising_backlog(_):
            counter["n"] += 1
            return healthy_reading(**{
                quiescence.SEALED: float(10 + counter["n"]),
                quiescence.CONFIRMED: float(counter["n"]),
                quiescence.QUARANTINED: 0.0,
            })

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=rising_backlog,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("timeout", reason)

    def test_a_missing_queue_depth_never_counts_as_quiesced(self):
        partial = {k: v for k, v in healthy_reading().items() if k != "weir_queue_depth"}
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent queue_depth must default to non-quiesced")

    def test_a_missing_sink_down_key_prevents_quiescence(self):
        # Absent means we cannot tell — the conservative reading is "not
        # quiesced", so a missing key must default to blocking, not passing.
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.SINK_DOWN}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent SINK_DOWN must default to 'down', not quiesced")

    def test_a_down_sink_prevents_quiescence(self):
        down = healthy_reading(**{quiescence.SINK_DOWN: 1.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: down,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "a down sink must block quiescence even if drain_state reads draining")

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

    def test_a_failed_scrape_resets_the_stability_counter(self):
        # Minor 6: without a reset, two passing polls separated by a failed
        # scrape would still add up to "consecutive" stability, even though
        # the window straddled an observability gap. A scrape that fails
        # every 3rd call can never accumulate 3 consecutive passes UNLESS a
        # failure is silently skipped rather than resetting the counter — if
        # it were skipped, pass/pass/FAIL/pass would read as stable=3 on the
        # 4th call (2 from before the gap, 1 after) and quiesce wrongly. With
        # the reset in place this must run out the clock instead.
        calls = {"n": 0}

        def periodic_gap(_):
            calls["n"] += 1
            if calls["n"] % 3 == 0:
                raise ConnectionResetError("mid-restart hiccup")
            return healthy_reading()

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=periodic_gap,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(
            ok, "a failed scrape must reset stability, not be silently skipped"
        )

    def test_the_poll_interval_cannot_overshoot_the_timeout(self):
        start = time.monotonic()
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.2, scrape_fn=lambda _: {"weir_queue_depth": 5.0},
            poll_interval_s=3.0, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertFalse(ok)
        self.assertLess(
            elapsed, 1.0,
            f"a 3s poll interval must not overshoot a 0.2s timeout; took {elapsed:.2f}s",
        )


class TestCounterDefaults(unittest.TestCase):
    """Missing-key defaults differ by metric TYPE, and getting it backwards
    reintroduces C1: counters (sealed/confirmed/quarantined/stranded/resumed)
    default to 0.0 because prometheus-client only emits a Family member once
    it has been incremented — absent genuinely means zero. Gauges keep the
    conservative blocking defaults (covered in TestWait above)."""

    def test_absent_counters_default_to_zero_and_can_quiesce_trivially(self):
        # A freshly-started daemon that has sealed nothing yet: every
        # weir_wab_segments_total / weir_drain_segments_*_total series is
        # absent because nothing has incremented them. 0 == 0 + 0 and
        # 0 == 0 are both correctly "nothing to drain", not "cannot tell".
        fresh = {
            "weir_queue_depth": 0.0,
            quiescence.DRAINING: 1.0,
            quiescence.SINK_DOWN: 0.0,
            # SEALED/CONFIRMED/QUARANTINED/STRANDED/RESUMED all absent.
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: fresh,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_gauges_absent_still_blocks_even_though_counters_default_to_zero(self):
        # An entirely empty scrape must not quiesce: the counters defaulting
        # to 0 make conditions 1 and 2 trivially true, but the GAUGES
        # (queue_depth, draining, sink_down) still default to their
        # conservative blocking values, so quiescence must not follow.
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: {},
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "an empty scrape must never read as quiesced")


class TestStrandedResumed(unittest.TestCase):
    """C1 Round 2: `stranded_total == resumed_total` is an EQUALITY check,
    not a stability check. The previous code tested STRANDED for stability
    (unchanging across polls), which only catches a counter that is RISING —
    never one that has already risen. An already-stranded segment, stranded
    before polling ever started and never resumed, satisfied every other
    condition while acked-undelivered records sat on disk."""

    def test_an_already_stranded_segment_that_never_resumes_blocks_forever(self):
        # stranded=5, resumed=0, held perfectly constant across every poll —
        # exactly the scenario that fooled the old "is STRANDED stable"
        # check, because 5 == 5 on every comparison. Equality against RESUMED
        # catches it: 5 != 0, so this must never quiesce.
        stuck = healthy_reading(**{quiescence.STRANDED: 5.0, quiescence.RESUMED: 0.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: stuck,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "a stranded segment that never resumes must never quiesce")
        self.assertIn("timeout", reason)

    def test_an_advancing_stranded_counter_with_no_resume_prevents_quiescence(self):
        counter = {"n": 0}

        def growing_stranded(_):
            counter["n"] += 1
            return healthy_reading(**{
                quiescence.STRANDED: float(counter["n"]), quiescence.RESUMED: 0.0,
            })

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=growing_stranded,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "segments still being abandoned to the sink outage must block quiescence")

    def test_stranded_caught_up_to_resumed_does_not_block(self):
        # The positive case: once every stranded segment has resumed, the
        # equality holds and this condition no longer blocks quiescence.
        caught_up = healthy_reading(**{quiescence.STRANDED: 4.0, quiescence.RESUMED: 4.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: caught_up,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)


class TestSealedConfirmedConvergence(unittest.TestCase):
    """Replaces a tautological test that ran the module defaults
    (poll_interval_s=2.0, stable_polls=4 under the PREVIOUS fix) against
    timeout_s=3.0 — an 8s window measured against a 3s budget, which only
    proved 3 < 8 and could never fail regardless of what the drain was
    doing. These exercise the actual C1 fix: `sealed_total ==
    confirmed_total + quarantined_total`, the real "drain caught up" test,
    with a GENEROUS budget so a false pass can't hide behind a tight one."""

    def test_a_persistent_sealed_confirmed_gap_never_quiesces(self):
        # Simulates continuous load: sealed_total rises every poll, confirmed
        # (the only terminal state reached here) always trails by a fixed
        # backlog of several segments — never converging. Must NOT quiesce
        # even given a budget 100x the poll interval.
        counter = {"n": 0}

        def continuous_load(_):
            counter["n"] += 1
            return healthy_reading(**{
                quiescence.SEALED: float(50 + counter["n"]),
                quiescence.CONFIRMED: float(counter["n"]),
                quiescence.QUARANTINED: 0.0,
                # Other values keep moving too, as they would under load —
                # the sealed/confirmed gap alone must be sufficient to block.
                quiescence.STRANDED: float(counter["n"] % 3),
                quiescence.RESUMED: float(counter["n"] % 3),
            })

        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=1.0, scrape_fn=continuous_load,
            poll_interval_s=0.01, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertFalse(ok, "a persistent sealed/confirmed gap must never quiesce")
        self.assertIn("timeout", reason)

    def test_sealed_confirmed_convergence_quiesces_promptly(self):
        # Same load shape, but confirmed+quarantined has now caught up to
        # sealed on every poll. Other counters keep changing value between
        # polls (as they would under real continuous load) but the identity
        # sealed == confirmed + quarantined holds throughout, so this must
        # quiesce well inside a realistic (30s) timeout — and quickly, not
        # near the deadline.
        counter = {"n": 0}

        def caught_up(_):
            counter["n"] += 1
            return healthy_reading(**{
                quiescence.SEALED: 500.0,
                quiescence.CONFIRMED: 480.0,
                quiescence.QUARANTINED: 20.0,
                quiescence.STRANDED: float(counter["n"] % 5),
                quiescence.RESUMED: float(counter["n"] % 5),
            })

        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=30.0, scrape_fn=caught_up,
            poll_interval_s=0.01, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertTrue(ok, reason)
        self.assertLess(
            elapsed, 5.0, f"convergence must quiesce promptly, took {elapsed:.2f}s"
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
