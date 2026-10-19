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


if __name__ == "__main__":
    unittest.main()
