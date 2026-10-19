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


if __name__ == "__main__":
    unittest.main()
