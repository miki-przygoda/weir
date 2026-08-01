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
    each test isolates the one condition it is about. WAB_BYTES is fixed at
    a constant value here; `wait_for_quiescence` judges it by comparing each
    poll against the PREVIOUS poll's value, so returning this same dict
    (or a dict with the same WAB_BYTES value) on every call is what makes a
    scenario read as byte-stable. A test that wants the "still draining"
    case instead overrides WAB_BYTES to something that changes call to call.
    """
    base = {
        "weir_queue_depth": 0.0,
        quiescence.DRAINING: 1.0,
        quiescence.SINK_DOWN: 0.0,
        quiescence.WAB_BYTES: 4096.0,
        quiescence.STRANDED: 2.0,
        quiescence.RESUMED: 2.0,
    }
    base.update(overrides)
    return base


class TestWait(unittest.TestCase):
    def test_quiesces_when_all_signals_settle(self):
        # WAB_BYTES stability is a delta against the PREVIOUS poll, so the
        # very first reading can never itself count as stable -- there is
        # nothing to compare it to yet. N stable comparisons therefore need
        # N+1 readings. Here queue_depth is what fails reading 1 (which also
        # plants the WAB_BYTES baseline of 4096.0); readings 2-4 then match
        # that baseline and are otherwise fully healthy, giving exactly the
        # 3 consecutive stable comparisons stable_polls=3 requires.
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

    def test_stable_polls_requires_one_more_reading_than_the_count(self):
        # Pins down the N+1 arithmetic directly: with a constant healthy
        # reading on every call, stable_polls=3 must take exactly 4 scrapes
        # -- the 1st plants the WAB_BYTES baseline (and can never itself
        # count as stable), and the following 3 each match it.
        calls = {"n": 0}

        def counting(_):
            calls["n"] += 1
            return healthy_reading()

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=counting,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)
        self.assertEqual(
            calls["n"], 4,
            "3 stable comparisons need 4 readings: the first plants the "
            "WAB_BYTES baseline and can never itself count as stable",
        )

    def test_reports_stuck_when_drain_is_blocked(self):
        blocked = healthy_reading(**{
            quiescence.DRAINING: 0.0,
            'weir_drain_state{state="blocked_dead_letter_full"}': 1.0,
        })
        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: blocked,
            poll_interval_s=0, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertFalse(ok)
        self.assertIn("blocked", reason)
        self.assertLess(
            elapsed, 1.0,
            "BLOCKED must return immediately on the first poll, not wait "
            "out a 10s timeout",
        )

    def test_a_missing_queue_depth_never_counts_as_quiesced(self):
        partial = {k: v for k, v in healthy_reading().items() if k != "weir_queue_depth"}
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent queue_depth must default to non-quiesced")

    def test_a_missing_draining_key_prevents_quiescence(self):
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.DRAINING}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent DRAINING must default to 0.0 (not draining), not quiesced")

    def test_a_missing_sink_down_key_prevents_quiescence(self):
        # Absent means we cannot tell -- the conservative reading is "not
        # quiesced", so a missing key must default to blocking, not passing.
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.SINK_DOWN}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent SINK_DOWN must default to 'down', not quiesced")

    def test_a_missing_wab_bytes_never_counts_as_quiesced(self):
        # WAB_BYTES is the primary signal now. Absent must not be treated as
        # "stable" -- a false True here is a phantom durability violation
        # two steps downstream, which is the entire reason this module
        # exists.
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.WAB_BYTES}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: partial,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent WAB_BYTES must never read as stable")
        self.assertIn("wab_bytes_on_disk still changing", reason)

    def test_a_down_sink_prevents_quiescence(self):
        down = healthy_reading(**{quiescence.SINK_DOWN: 1.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: down,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "a down sink must block quiescence even if drain_state reads draining")

    def test_an_always_failing_scrape_is_reported_as_a_harness_problem(self):
        # A wrong URL and a genuinely stuck drain must not produce the same
        # reason string -- on a multi-day run that ambiguity costs hours.
        def boom(_):
            raise ConnectionRefusedError("connection refused")

        ok, reason = quiescence.wait_for_quiescence(
            "http://127.0.0.1:1/metrics", timeout_s=0.05, scrape_fn=boom,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("harness", reason)
        self.assertIn("ConnectionRefusedError", reason)

    def test_a_failed_scrape_resets_the_stability_counter(self):
        # Without a reset, two passing polls separated by a failed scrape
        # would still add up to "consecutive" stability even though the
        # window straddled an observability gap. A scrape that fails every
        # 3rd call can only ever reach stable=1 (plant the WAB_BYTES
        # baseline, match it once) before the next failure wipes it out
        # again -- it must never reach stable_polls=3.
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

    def test_a_failed_scrape_resets_the_wab_bytes_baseline(self):
        # Differential test for the byte-baseline reset specifically (not
        # just the stable counter). Sequence: success, FAIL, success,
        # success, then the fake source runs dry and every further call
        # fails too (a `next()` on an exhausted iterator raises
        # StopIteration, which the retry path swallows like any other
        # scrape failure).
        #
        # WITHOUT resetting last_bytes on failure, call 1's baseline would
        # silently survive the call-2 failure, so call 3 would already read
        # as matching it (drained=True) and call 4 would push stable to 2,
        # quiescing right there with stable_polls=2.
        #
        # WITH the reset (the current, correct behaviour), the call-2
        # failure wipes the baseline, so call 3 has to re-plant it (drained
        # stays False) and call 4 only reaches stable=1 -- a second matching
        # reading is still needed, which this sequence deliberately never
        # supplies, so it must run out the clock instead.
        readings = [healthy_reading(), None, healthy_reading(), healthy_reading()]
        it = iter(readings)

        def flaky(_):
            r = next(it)
            if r is None:
                raise ConnectionResetError("mid-restart hiccup")
            return r

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=flaky,
            poll_interval_s=0, stable_polls=2,
        )
        self.assertFalse(
            ok, "the WAB_BYTES baseline must not survive a failed scrape"
        )
        self.assertIn(
            "3 successful scrapes", reason,
            "exactly 3 scrapes should have succeeded (calls 1, 3, 4) before "
            "quiescence should have been reachable at 2 stable polls, yet it "
            "wasn't -- proving the reset, not a fluke of timing, is what "
            "blocked it",
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


class TestWabBytesStability(unittest.TestCase):
    """`weir_wab_bytes_on_disk` is the primary quiescence signal again (3rd
    iteration): it's a gauge of CURRENT state (open active segment + sealed
    segments awaiting drain, excluding confirmed, which are deleted), so
    unlike a restart-reset counter it stays valid across weir's per-episode
    restarts. `run.py` SIGSTOPs the load generator before waiting, so the
    active segment stops growing and the gauge is expected to hold steady
    once the drain has actually caught up."""

    def test_a_changing_wab_bytes_gauge_never_quiesces_even_with_a_generous_budget(self):
        # The drain is still actively moving segments: bytes-on-disk changes
        # on every poll. Every OTHER condition is healthy throughout, so
        # only the bytes-stability check can be what's blocking this -- and
        # it must, even given a budget many times the poll interval.
        counter = {"n": 0}

        def still_draining(_):
            counter["n"] += 1
            return healthy_reading(**{quiescence.WAB_BYTES: float(10_000 - counter["n"])})

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.3, scrape_fn=still_draining,
            poll_interval_s=0.01, stable_polls=3,
        )
        self.assertFalse(ok, "a changing WAB_BYTES gauge must never quiesce")
        self.assertIn("timeout", reason)

    def test_a_stable_wab_bytes_gauge_quiesces_promptly(self):
        # Same shape of scenario, but the load generator has been SIGSTOPped
        # (as run.py does before waiting), so the active segment stops
        # growing and the gauge holds steady. All other conditions healthy
        # -> must quiesce well inside a realistic timeout, and quickly, not
        # near the deadline.
        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=30.0, scrape_fn=lambda _: healthy_reading(),
            poll_interval_s=0.01, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertTrue(ok, reason)
        self.assertLess(
            elapsed, 5.0, f"a stable gauge must quiesce promptly, took {elapsed:.2f}s"
        )


class TestStrandedResumed(unittest.TestCase):
    """`stranded_total == resumed_total` is an EQUALITY check, not a
    stability check. Testing STRANDED for mere stability (unchanging across
    polls) would only catch a counter that is RISING -- never one that has
    already risen and stopped. An already-stranded segment, stranded before
    polling ever started and never resumed, must still block forever: a
    persistent gap, not a rising one, is what weir's own HELP text for
    weir_drain_segments_resumed describes."""

    def test_an_already_stranded_segment_that_never_resumes_blocks_forever(self):
        # stranded=5, resumed=0, held perfectly constant across every poll
        # -- the scenario a mere stability check on STRANDED would miss,
        # because 5 == 5 on every comparison. Equality against RESUMED
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


class TestCounterDefaults(unittest.TestCase):
    """Missing-key defaults differ by metric TYPE: counters (stranded/
    resumed) default to 0.0 because prometheus-client only emits a Family
    member once it has been incremented -- absent genuinely means zero.
    Gauges (queue_depth, draining, sink_down, and WAB_BYTES) keep the
    conservative blocking defaults instead (covered in TestWait above)."""

    def test_absent_stranded_and_resumed_default_to_zero_and_can_quiesce_trivially(self):
        # A freshly-started daemon that has stranded nothing yet: both
        # weir_drain_segments_stranded_total and _resumed_total are absent
        # because nothing has incremented them. 0 == 0 correctly reads as
        # "nothing stranded", not "cannot tell".
        fresh = {
            "weir_queue_depth": 0.0,
            quiescence.DRAINING: 1.0,
            quiescence.SINK_DOWN: 0.0,
            quiescence.WAB_BYTES: 0.0,
            # STRANDED / RESUMED both absent.
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: fresh,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_gauges_absent_still_blocks_even_though_counters_default_to_zero(self):
        # An entirely empty scrape must not quiesce: STRANDED/RESUMED
        # defaulting to 0 make that one condition trivially true, but the
        # GAUGES (queue_depth, draining, sink_down, wab_bytes) still default
        # to their conservative blocking values, so quiescence must not
        # follow.
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: {},
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "an empty scrape must never read as quiesced")


class TestTimeoutDiagnostics(unittest.TestCase):
    """A timeout reason must name WHICH conditions were unmet, with their
    actual values -- not just that some were. Without this, "waiting for
    drain quiescence" sends the operator to read metrics by hand, the same
    diagnostic dead end the harness-vs-finding distinction below also
    fixes."""

    def test_timeout_reason_reports_the_stranded_resumed_gap_with_values(self):
        gap = healthy_reading(**{quiescence.STRANDED: 3.0, quiescence.RESUMED: 1.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: gap,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("stranded(3) != resumed(1)", reason)

    def test_timeout_reason_reports_the_queue_depth_value(self):
        backed_up = healthy_reading(**{"weir_queue_depth": 5.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=lambda _: backed_up,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("queue_depth=5", reason)

    def test_timeout_reason_reports_wab_bytes_still_changing(self):
        counter = {"n": 0}

        def still_draining(_):
            counter["n"] += 1
            return healthy_reading(**{quiescence.WAB_BYTES: float(counter["n"])})

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, scrape_fn=still_draining,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("wab_bytes_on_disk still changing", reason)


class TestGaugeRefreshGuard(unittest.TestCase):
    """`poll_interval_s * stable_polls` must exceed GAUGE_REFRESH_SECS
    (5.0s) when talking to a real daemon (scrape_fn=None), or an unchanged
    WAB_BYTES reading could mean "not yet recomputed" rather than "drain
    caught up" -- the module's very first bug, reintroduced. Every other
    test in this file injects a fake scrape_fn, so the guard must NOT apply
    to them; only scrape_fn=None triggers it."""

    def test_guard_raises_for_a_real_scrape_with_too_short_a_window(self):
        # 1.0 * 3 = 3.0s, which does not exceed the 5.0s gauge refresh.
        # scrape_fn=None means "use the real HTTP scrape function", so this
        # must raise before ever attempting a network call (no daemon is
        # running at "unused").
        # timeout_s is deliberately small: if the guard is doing its job it
        # raises before the polling loop ever starts, so the timeout value
        # is irrelevant to a passing run -- it only bounds how long this
        # test takes if the guard fails to fire and execution falls through
        # into a real (doomed to fail, "unused" is not a URL) scrape loop.
        with self.assertRaises(ValueError):
            quiescence.wait_for_quiescence(
                "unused", timeout_s=0.05, scrape_fn=None,
                poll_interval_s=1.0, stable_polls=3,
            )

    def test_guard_does_not_raise_for_an_injected_scrape_with_an_equally_short_window(self):
        # 0.01 * 3 = 0.03s -- shorter still than the real-scrape case above
        # -- but with an injected scrape_fn there is no real 5s-refreshed
        # gauge to outrun, so the guard must not fire, and this should
        # simply quiesce normally.
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=1.0, scrape_fn=lambda _: healthy_reading(),
            poll_interval_s=0.01, stable_polls=3,
        )
        self.assertTrue(ok, reason)


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
