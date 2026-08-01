"""Tests for drain-quiescence detection.

Parsing is tested against real OpenMetrics text; the wait loop is tested with
an injected scrape function AND an injected residue function so no daemon and
no real mount is required. `TestWabResidueScanRealFs` is the one exception:
it exercises `scan_wab_residue` against an actual directory tree built with
`tempfile`, because the shard-directory / extension / sidecar-naming logic is
exactly the kind of "did I get the on-disk layout right" question a fake
can't answer.
"""
import os
import tempfile
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
    """A fully-quiesced metrics snapshot: every signal in its settled state.

    Individual tests override just the field(s) they want to exercise, so
    each test isolates the one condition it is about.
    """
    base = {
        "weir_queue_depth": 0.0,
        quiescence.DRAINING: 1.0,
        quiescence.SINK_DOWN: 0.0,
        quiescence.STRANDED: 2.0,
        quiescence.RESUMED: 2.0,
    }
    base.update(overrides)
    return base


def healthy_residue(**overrides):
    """A fully-drained WAB directory: nothing sealed-without-a-sidecar, no
    buffered records sitting in an open active segment."""
    base = {"unconfirmed_sealed": 0, "nonempty_active": 0}
    base.update(overrides)
    return quiescence.Residue(**base)


#: Most tests care about exactly one condition and want everything else to
#: read as healthy. `wab_dir` is a dummy value here -- the default
#: `scan_wab_residue` is never reached because every test below injects its
#: own `residue_fn`, so nothing ever actually touches the filesystem at this
#: path. It only needs to be non-None to satisfy the constructor guard (see
#: TestWabDirRequired).
UNUSED_WAB_DIR = "/unused"


def healthy_residue_fn(_wab_dir):
    return healthy_residue()


class TestWait(unittest.TestCase):
    def test_quiesces_when_all_signals_settle(self):
        # Every condition here (residue counts, stranded/resumed equality,
        # queue depth, drain_state, sink_health) is a snapshot property of a
        # single poll, not a delta against the previous poll -- there is no
        # baseline to plant, unlike the bytes-gauge approach this module used
        # to carry. So stable_polls=3 needs exactly 3 GOOD readings, no more.
        # Reading 1 here is bad (queue_depth=5) to prove a leading failure
        # doesn't get counted or otherwise confuse the window; readings 2-4
        # are healthy and are exactly the 3 consecutive stable polls needed.
        readings = [
            {**healthy_reading(), "weir_queue_depth": 5.0},
            healthy_reading(),
            healthy_reading(),
            healthy_reading(),
        ]
        it = iter(readings)
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: next(it), residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_stable_polls_requires_exactly_that_many_readings(self):
        # Pins down the arithmetic directly: with a constant healthy reading
        # on every call, stable_polls=3 must take EXACTLY 3 scrapes -- not 4.
        # The old bytes-gauge design needed N+1 readings because the first
        # one only planted a delta baseline and could never itself count as
        # stable; this design has no baseline (every poll independently
        # either meets every condition or it doesn't), so N readings quiesce
        # N stable polls with nothing left over.
        calls = {"n": 0}

        def counting(_):
            calls["n"] += 1
            return healthy_reading()

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=counting, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)
        self.assertEqual(
            calls["n"], 3,
            "3 stable polls need exactly 3 readings -- each poll stands "
            "alone, there is no baseline-planting read to add on top",
        )

    def test_an_iterator_exhausted_one_read_short_of_stable_polls_times_out(self):
        # A trap that has bitten repeatedly: supply one fewer healthy reading
        # than stable_polls requires. The next scrape call raises
        # StopIteration (an exhausted iterator's next()), which the retry
        # path must swallow like any other scrape failure -- reset
        # stability, keep polling -- not propagate and crash the run, and
        # not spuriously count towards stable_polls either. With only 2
        # readings available and 3 required, this must run out the clock and
        # report a timeout, not hang and not raise.
        stable_polls = 3
        readings = [healthy_reading() for _ in range(stable_polls - 1)]
        it = iter(readings)
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: next(it), residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=stable_polls,
        )
        self.assertFalse(ok)
        self.assertIn("timeout", reason)

    def test_reports_stuck_when_drain_is_blocked(self):
        blocked = healthy_reading(**{
            quiescence.DRAINING: 0.0,
            'weir_drain_state{state="blocked_dead_letter_full"}': 1.0,
        })
        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: blocked, residue_fn=healthy_residue_fn,
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
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: partial, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent queue_depth must default to non-quiesced")

    def test_a_missing_draining_key_prevents_quiescence(self):
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.DRAINING}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: partial, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent DRAINING must default to 0.0 (not draining), not quiesced")

    def test_a_missing_sink_down_key_prevents_quiescence(self):
        # Absent means we cannot tell -- the conservative reading is "not
        # quiesced", so a missing key must default to blocking, not passing.
        partial = {k: v for k, v in healthy_reading().items() if k != quiescence.SINK_DOWN}
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: partial, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "absent SINK_DOWN must default to 'down', not quiesced")

    def test_a_down_sink_prevents_quiescence(self):
        down = healthy_reading(**{quiescence.SINK_DOWN: 1.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: down, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "a down sink must block quiescence even if drain_state reads draining")

    def test_an_always_failing_scrape_is_reported_as_a_harness_problem(self):
        # A wrong URL and a genuinely stuck drain must not produce the same
        # reason string -- on a multi-day run that ambiguity costs hours.
        def boom(_):
            raise ConnectionRefusedError("connection refused")

        ok, reason = quiescence.wait_for_quiescence(
            "http://127.0.0.1:1/metrics", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=boom, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("harness", reason)
        self.assertIn("ConnectionRefusedError", reason)

    def test_a_failed_scrape_resets_the_stability_counter(self):
        # Without a reset, two passing polls separated by a failed scrape
        # would still add up to "consecutive" stability even though the
        # window straddled an observability gap. A scrape that fails every
        # 3rd call can reach stable=2 at most (two healthy polls) before the
        # next failure wipes it out again -- it must never reach
        # stable_polls=3.
        calls = {"n": 0}

        def periodic_gap(_):
            calls["n"] += 1
            if calls["n"] % 3 == 0:
                raise ConnectionResetError("mid-restart hiccup")
            return healthy_reading()

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=periodic_gap, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(
            ok, "a failed scrape must reset stability, not be silently skipped"
        )

    def test_the_poll_interval_cannot_overshoot_the_timeout(self):
        start = time.monotonic()
        ok, _ = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.2, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: {"weir_queue_depth": 5.0}, residue_fn=healthy_residue_fn,
            poll_interval_s=3.0, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertFalse(ok)
        self.assertLess(
            elapsed, 1.0,
            f"a 3s poll interval must not overshoot a 0.2s timeout; took {elapsed:.2f}s",
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
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: stuck, residue_fn=healthy_residue_fn,
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
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=growing_stranded, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok, "segments still being abandoned to the sink outage must block quiescence")

    def test_stranded_caught_up_to_resumed_does_not_block(self):
        # The positive case: once every stranded segment has resumed, the
        # equality holds and this condition no longer blocks quiescence.
        caught_up = healthy_reading(**{quiescence.STRANDED: 4.0, quiescence.RESUMED: 4.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: caught_up, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)


class TestCounterDefaults(unittest.TestCase):
    """Missing-key defaults differ by metric TYPE: counters (stranded/
    resumed) default to 0.0 because prometheus-client only emits a Family
    member once it has been incremented -- absent genuinely means zero.
    Gauges (queue_depth, draining, sink_down) keep the conservative blocking
    defaults instead (covered in TestWait above)."""

    def test_absent_stranded_and_resumed_default_to_zero_and_can_quiesce_trivially(self):
        # A freshly-started daemon that has stranded nothing yet: both
        # weir_drain_segments_stranded_total and _resumed_total are absent
        # because nothing has incremented them. 0 == 0 correctly reads as
        # "nothing stranded", not "cannot tell". Paired with a clean (empty)
        # WAB directory, this must quiesce.
        fresh = {
            "weir_queue_depth": 0.0,
            quiescence.DRAINING: 1.0,
            quiescence.SINK_DOWN: 0.0,
            # STRANDED / RESUMED both absent.
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: fresh, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_gauges_absent_still_blocks_even_though_counters_default_to_zero(self):
        # An entirely empty scrape must not quiesce: STRANDED/RESUMED
        # defaulting to 0 make that one condition trivially true, but the
        # GAUGES (queue_depth, draining, sink_down) still default to their
        # conservative blocking values, so quiescence must not follow --
        # even though the WAB directory itself is clean.
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: {}, residue_fn=healthy_residue_fn,
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
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: gap, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("stranded(3) != resumed(1)", reason)

    def test_timeout_reason_reports_the_queue_depth_value(self):
        backed_up = healthy_reading(**{"weir_queue_depth": 5.0})
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: backed_up, residue_fn=healthy_residue_fn,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("queue_depth=5", reason)

    def test_timeout_reason_reports_unconfirmed_sealed_count(self):
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(),
            residue_fn=lambda _: healthy_residue(unconfirmed_sealed=2),
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("unconfirmed_sealed=2", reason)

    def test_timeout_reason_reports_nonempty_active_segment_count(self):
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(),
            residue_fn=lambda _: healthy_residue(nonempty_active=1),
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("nonempty_active_segments=1", reason)


class TestWabDirRequired(unittest.TestCase):
    """The module used to carry a runtime guard (poll_interval_s * stable_polls
    must exceed the bytes-gauge's 5s refresh period) that turned out to be
    dead code: it reassigned `scrape_fn = scrape_fn or scrape` BEFORE
    checking `if scrape_fn is None`, so the check could never see None and
    never fired. That guard and the gauge it protected are both gone now, but
    the same reassign-then-check mistake would be just as fatal here, so this
    pins down that the wab_dir/residue_fn guard actually fires -- unlike its
    predecessor."""

    def test_omitting_both_wab_dir_and_residue_fn_raises_immediately(self):
        # Not `assertFalse(ok)` -- this must not even reach the polling
        # loop. Silently falling back to `os.scandir`'s default (the
        # orchestrator's current working directory, NOT the WAB mount) would
        # be a wrong-but-plausible-looking answer; failing loud is safer.
        with self.assertRaises(ValueError):
            quiescence.wait_for_quiescence(
                "unused", timeout_s=0.05, scrape_fn=lambda _: healthy_reading(),
                poll_interval_s=0, stable_polls=3,
            )

    def test_supplying_only_residue_fn_does_not_raise(self):
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: healthy_reading(),
            residue_fn=healthy_residue_fn, poll_interval_s=0, stable_polls=2,
        )
        self.assertTrue(ok, reason)

    def test_supplying_only_wab_dir_uses_the_real_scanner(self):
        # No residue_fn injected: this exercises the real `scan_wab_residue`
        # against an actual empty temp directory (no shard dirs at all yet,
        # e.g. a daemon that hasn't created any) -- an empty directory has
        # nothing sealed and nothing buffered, so it reads as clean.
        with tempfile.TemporaryDirectory() as tmp:
            ok, reason = quiescence.wait_for_quiescence(
                tmp, timeout_s=10, wab_dir=tmp,
                scrape_fn=lambda _: healthy_reading(),
                poll_interval_s=0, stable_polls=2,
            )
            self.assertTrue(ok, reason)


class TestResidueBlocksQuiescence(unittest.TestCase):
    """The two on-disk conditions from `scan_wab_residue`, exercised through
    the wait loop via an injected `residue_fn` (no real mount needed). Every
    metric condition is healthy throughout each of these, so only the
    residue condition under test can be what's blocking -- and it must,
    even given a budget many times the poll interval."""

    def test_outstanding_sealed_without_confirmed_never_quiesces(self):
        counter = {"n": 0}

        def stuck_residue(_):
            counter["n"] += 1
            return healthy_residue(unconfirmed_sealed=1)

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.3, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(), residue_fn=stuck_residue,
            poll_interval_s=0.01, stable_polls=3,
        )
        self.assertFalse(
            ok, "a sealed segment with no .confirmed sidecar must never quiesce"
        )
        self.assertIn("timeout", reason)
        self.assertGreater(
            counter["n"], 3, "must have kept polling across the generous budget"
        )

    def test_nonempty_active_segment_never_quiesces(self):
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.3, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(),
            residue_fn=lambda _: healthy_residue(nonempty_active=1),
            poll_interval_s=0.01, stable_polls=3,
        )
        self.assertFalse(
            ok, "a non-empty active segment (buffered, undelivered records) must never quiesce"
        )
        self.assertIn("timeout", reason)

    def test_clean_residue_and_healthy_metrics_quiesces_promptly(self):
        start = time.monotonic()
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=30.0, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(), residue_fn=healthy_residue_fn,
            poll_interval_s=0.01, stable_polls=3,
        )
        elapsed = time.monotonic() - start
        self.assertTrue(ok, reason)
        self.assertLess(
            elapsed, 5.0, f"a clean WAB dir must quiesce promptly, took {elapsed:.2f}s"
        )


class TestResidueScanFailure(unittest.TestCase):
    """A filesystem scan can fail for the same mundane reasons a metrics
    scrape can (a transient dirent error mid-write, a directory disappearing
    mid-scan on an unmounting device) and must be handled exactly the same
    way: reset stability, keep polling, and never propagate out of
    `wait_for_quiescence` and crash the run."""

    def test_a_raising_residue_scan_is_handled_not_propagated(self):
        def boom(_):
            raise OSError("simulated transient dirent error")

        # Must not raise -- this call itself is the assertion.
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(), residue_fn=boom,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("harness", reason, "every poll failed -- a harness problem, not a stuck drain")
        self.assertIn("OSError", reason)

    def test_a_residue_scan_failure_is_distinguishable_from_a_scrape_failure(self):
        # Both are "failed polls" for stability-counting purposes, but the
        # reason string must say WHICH stage failed -- otherwise a flaky
        # metrics endpoint and a flaky filesystem look identical in the log,
        # and an operator debugging one wastes time on the other.
        def boom(_):
            raise OSError("simulated transient dirent error")

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(), residue_fn=boom,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("residue scan", reason)

    def test_a_residue_scan_failure_resets_the_stability_counter(self):
        # Mirrors test_a_failed_scrape_resets_the_stability_counter: a
        # residue scan that fails every 3rd call can reach stable=2 at most
        # before the next failure wipes it out -- it must never reach
        # stable_polls=3.
        calls = {"n": 0}

        def periodic_gap(_):
            calls["n"] += 1
            if calls["n"] % 3 == 0:
                raise OSError("simulated transient dirent error")
            return healthy_residue()

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.05, wab_dir=UNUSED_WAB_DIR,
            scrape_fn=lambda _: healthy_reading(), residue_fn=periodic_gap,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(
            ok, "a failed residue scan must reset stability, not be silently skipped"
        )


class TestWabResidueScanRealFs(unittest.TestCase):
    """`scan_wab_residue` against a real directory tree built with
    `tempfile` -- the shard-directory naming, extension matching, and
    sidecar-naming logic is exactly the kind of "did I get the on-disk
    layout right" question an injected fake can't answer. Mirrors the exact
    naming weir itself uses: `crates/weir-wab/src/format.rs` for the
    extensions and `confirmed_path_for`'s suffix-swap, and
    `crates/weir-server/src/wab/mod.rs::shard_dir_path` for `shard_{id:02}`.
    """

    def _write(self, path, size):
        with open(path, "wb") as f:
            f.write(b"\0" * size)

    def test_scan_counts_correctly_across_a_realistic_tree(self):
        with tempfile.TemporaryDirectory() as wab_dir:
            shard0 = os.path.join(wab_dir, "shard_00")
            shard1 = os.path.join(wab_dir, "shard_01")
            os.makedirs(shard0)
            os.makedirs(shard1)

            header_only = quiescence.SEGMENT_HEADER_LEN

            # shard_00: one sealed segment WITH a confirmed sidecar (drained,
            # not yet garbage-collected) -- must NOT count.
            self._write(os.path.join(shard0, "seg_00000001.wab.sealed"), header_only + 5)
            self._write(os.path.join(shard0, "seg_00000001.wab.confirmed"), 36)

            # shard_00: one sealed segment with NO sidecar -- genuine
            # backlog, MUST count as unconfirmed_sealed.
            self._write(os.path.join(shard0, "seg_00000002.wab.sealed"), header_only + 5)

            # shard_00: an active segment holding just the header, zero
            # records written yet -- must NOT count as non-empty.
            self._write(os.path.join(shard0, "seg_00000003.wab"), header_only)

            # shard_01: an active segment with at least one buffered record
            # (larger than the bare header) -- MUST count as nonempty_active.
            self._write(os.path.join(shard1, "seg_00000001.wab"), header_only + 12)

            # shard_01: a second sealed-without-sidecar segment, to prove
            # counts accumulate across shard directories, not just within one.
            self._write(os.path.join(shard1, "seg_00000002.wab.sealed"), header_only + 5)

            # Reserved subdirectories: a sealed-without-sidecar file and a
            # non-empty active-looking file in EACH, which would inflate both
            # counts if the scan didn't skip them entirely.
            quarantine = os.path.join(wab_dir, "quarantine")
            dead_letter = os.path.join(wab_dir, "dead_letter")
            os.makedirs(quarantine)
            os.makedirs(dead_letter)
            self._write(os.path.join(quarantine, "seg_00000099.wab.sealed"), header_only + 5)
            self._write(os.path.join(dead_letter, "seg_00000099.wab"), header_only + 5)

            residue = quiescence.scan_wab_residue(wab_dir)

            self.assertEqual(
                residue.unconfirmed_sealed, 2,
                "the confirmed segment and both reserved-subdir segments "
                "must be excluded; only the two genuinely undrained ones count",
            )
            self.assertEqual(
                residue.nonempty_active, 1,
                "only the shard_01 active segment has records past the bare "
                "header; the shard_00 one and the dead_letter one must be excluded",
            )

    def test_a_completely_empty_wab_dir_scans_clean(self):
        with tempfile.TemporaryDirectory() as wab_dir:
            residue = quiescence.scan_wab_residue(wab_dir)
            self.assertEqual(residue.unconfirmed_sealed, 0)
            self.assertEqual(residue.nonempty_active, 0)

    def test_a_missing_wab_dir_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "does-not-exist")
            with self.assertRaises(OSError):
                quiescence.scan_wab_residue(missing)


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
