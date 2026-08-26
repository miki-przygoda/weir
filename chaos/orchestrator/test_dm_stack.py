"""Tests for the device-mapper stack plumbing.

The setup/teardown test needs root and Linux; it skips otherwise so the file
is still runnable on a dev machine.
"""
import os
import platform
import subprocess
import tempfile
import unittest
from collections import namedtuple
from unittest import mock

import dm_flakey
import dm_stack

# Shared fake-`_run` result shape. Named here (rather than only in Task 4's
# async-remove tests) so both files can use it without disagreeing on fields.
_Result = namedtuple("_Result", ["returncode", "stdout", "stderr"])


def _needs_root_linux():
    return platform.system() != "Linux" or os.geteuid() != 0


def _fake_run(responses):
    """Builds a fake `_run(cmd, check=True)` that records every call it sees
    and returns a canned `_Result` keyed by the command's first two tokens.

    `responses` maps `(argv0, argv1)` -> `_Result`. Anything not in the map
    gets a bland `_Result(0, "", "")` so tests only need to stub the calls
    they actually care about.
    """
    calls = []

    def fake(cmd, check=True):
        calls.append(list(cmd))
        return responses.get(tuple(cmd[:2]), _Result(0, "", ""))

    fake.calls = calls
    return fake


class TestStorageStack(unittest.TestCase):
    def test_rejects_a_size_too_small_for_ext4(self):
        with self.assertRaises(ValueError):
            dm_stack.StorageStack("/tmp/x.img", size_mb=1, mount_point="/mnt/x")

    @unittest.skipIf(_needs_root_linux(), "needs root on Linux")
    def test_setup_then_teardown_leaves_nothing_behind(self):
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt)
            s.setup()
            try:
                self.assertTrue(s.is_mounted())
                probe = os.path.join(mnt, "probe")
                with open(probe, "w") as f:
                    f.write("x")
                self.assertTrue(os.path.exists(probe))
            finally:
                s.teardown()
            self.assertFalse(s.is_mounted())
            out = subprocess.run(
                ["losetup", "-j", img], capture_output=True, text=True
            ).stdout
            self.assertEqual(out.strip(), "", "loop device must be detached")
            self.assertFalse(
                os.path.exists(img),
                "backing image must be removed — the test name promises nothing is left",
            )


class TestDmTargetValidation(unittest.TestCase):
    def test_dm_target_defaults_to_none(self):
        s = dm_stack.StorageStack("/tmp/x.img", 512, "/mnt/x")
        self.assertIsNone(s.dm_target)
        self.assertIsNone(s.dm_name)

    def test_rejects_an_unknown_dm_target(self):
        with self.assertRaises(ValueError):
            dm_stack.StorageStack("/tmp/x.img", 512, "/mnt/x", dm_target="bogus")


class TestFaultDevice(unittest.TestCase):
    def test_without_a_dm_target_the_stack_targets_the_loop_device(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x")
        s.loop_device = "/dev/loop7"
        self.assertEqual(s.fault_device, "/dev/loop7")

    def test_with_flakey_target_the_stack_targets_the_mapper_device(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="flakey")
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-flakey-1234"
        self.assertEqual(s.fault_device, "/dev/mapper/weir-chaos-flakey-1234")

    def test_with_linear_target_the_stack_targets_the_mapper_device(self):
        # fault_device is keyed on dm_name existing, not on which dm_target
        # built it — flakey and linear stack identically.
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="linear")
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-linear-1234"
        self.assertEqual(s.fault_device, "/dev/mapper/weir-chaos-linear-1234")


class TestEngageFaultRefusal(unittest.TestCase):
    def test_engage_is_refused_when_the_layer_was_never_built(self):
        # Silently doing nothing here would produce a "power loss" episode
        # with no power loss in it, and a green run proving nothing.
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x")
        with self.assertRaises(RuntimeError):
            s.engage_fault()

    def test_engage_is_refused_before_setup_even_for_a_flakey_target(self):
        # dm_target="flakey" alone doesn't mean the layer has been built yet.
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="flakey")
        with self.assertRaises(RuntimeError):
            s.engage_fault()

    def test_engage_is_refused_on_a_linear_stack(self):
        # Linear is a pass-through. Engaging it is meaningless and must fail
        # loudly rather than silently appear to work.
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="linear")
        s.dm_name = "weir-chaos-linear-1234"
        with self.assertRaises(RuntimeError):
            s.engage_fault()

    def test_disengage_is_refused_when_the_layer_was_never_built(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x")
        with self.assertRaises(RuntimeError):
            s.disengage_fault()

    def test_disengage_is_refused_on_a_linear_stack(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="linear")
        s.dm_name = "weir-chaos-linear-1234"
        with self.assertRaises(RuntimeError):
            s.disengage_fault()


class TestFlakeyCreateVerification(unittest.TestCase):
    DISENGAGED = "0 65536 flakey 7:19 0 60 0 1 drop_writes"
    ENGAGED = "0 65536 flakey 7:19 0 0 60 1 drop_writes"
    ERRORING_DEFAULT = "0 65536 flakey 7:19 0 60 0 2 error_reads error_writes"

    def _stack(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="flakey")
        s.dm_name = "weir-chaos-flakey-1234"
        return s

    def test_accepts_a_disengaged_table(self):
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            s._dm_create(s.dm_name, "table text")
        self.assertIn(
            ["dmsetup", "create", s.dm_name, "--table", "table text"], fake.calls
        )
        self.assertIn(["dmsetup", "table", s.dm_name], fake.calls)

    def test_raises_if_the_device_came_up_engaged(self):
        # A device that came up engaged would corrupt the filesystem before
        # the first episode ever ran.
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.ENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(RuntimeError):
                s._dm_create(s.dm_name, "table text")

    def test_raises_if_the_kernel_substituted_erroring_defaults(self):
        # What a table submitted without feature args comes back as — an
        # ERRORING device, not a pass-through. dm_flakey.flakey_table never
        # produces this itself, but read-back must still catch it if a stale
        # table (or some other bug) ever installs one.
        s = self._stack()
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, self.ERRORING_DEFAULT, "")}
        )
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(dm_flakey.UnexpectedTable):
                s._dm_create(s.dm_name, "table text")


class TestLinearCreateVerification(unittest.TestCase):
    def _stack(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="linear")
        s.dm_name = "weir-chaos-linear-1234"
        return s

    def test_accepts_a_linear_table(self):
        s = self._stack()
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 65536 linear 7:19 0", "")}
        )
        with mock.patch.object(dm_stack, "_run", fake):
            s._dm_create(s.dm_name, "0 65536 linear 7:19 0")

    def test_raises_if_a_stale_table_of_the_wrong_kind_survived(self):
        # A create that failed EBUSY leaves the PREVIOUS table installed
        # rather than raising — read-back is the only way to know what
        # actually got installed.
        s = self._stack()
        fake = _fake_run(
            {
                ("dmsetup", "table"): _Result(
                    0, "0 65536 flakey 7:19 0 0 60 1 drop_writes", ""
                )
            }
        )
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(RuntimeError):
                s._dm_create(s.dm_name, "0 65536 linear 7:19 0")


class TestFlakeyEngageDisengage(unittest.TestCase):
    DISENGAGED = "0 65536 flakey 7:19 0 60 0 1 drop_writes"
    ENGAGED = "0 65536 flakey 7:19 0 0 60 1 drop_writes"

    def _stack(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="flakey")
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-flakey-1234"
        s._sectors = 65536
        return s

    def test_engage_suspends_reloads_and_resumes_in_order(self):
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.ENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            s.engage_fault()
        subcommands = [c[1] for c in fake.calls if c[0] == "dmsetup"]
        self.assertEqual(
            [c for c in subcommands if c in ("suspend", "reload", "resume")],
            ["suspend", "reload", "resume"],
        )

    def test_engage_raises_if_the_reload_did_not_actually_engage(self):
        # Read the table back after every reload — a reload that silently
        # no-opped would go green while injecting nothing.
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(RuntimeError):
                s.engage_fault()

    def test_disengage_suspends_reloads_and_resumes_in_order(self):
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            s.disengage_fault()
        subcommands = [c[1] for c in fake.calls if c[0] == "dmsetup"]
        self.assertEqual(
            [c for c in subcommands if c in ("suspend", "reload", "resume")],
            ["suspend", "reload", "resume"],
        )

    def test_disengage_raises_if_the_reload_left_it_engaged(self):
        s = self._stack()
        fake = _fake_run({("dmsetup", "table"): _Result(0, self.ENGAGED, "")})
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(RuntimeError):
                s.disengage_fault()


class TestSetupStackOrder(unittest.TestCase):
    """Proves the actual command sequence for each dm_target — in particular
    that dm_target=None is byte-identical to Phase 1's losetup/mkfs/mount,
    with no dm command anywhere in it.
    """

    def _fake_for(self, dm_target):
        responses = {
            ("losetup", "--find"): _Result(0, "/dev/loop7\n", ""),
            ("blockdev", "--getsz"): _Result(0, "65536\n", ""),
        }
        if dm_target == "flakey":
            responses[("dmsetup", "table")] = _Result(
                0, "0 65536 flakey 7:19 0 60 0 1 drop_writes", ""
            )
        elif dm_target == "linear":
            responses[("dmsetup", "table")] = _Result(0, "0 65536 linear 7:19 0", "")
        return _fake_run(responses)

    def _setup_stack(self, tmp, dm_target):
        img = os.path.join(tmp, "wab.img")
        mnt = os.path.join(tmp, "mnt")
        os.makedirs(mnt)
        return dm_stack.StorageStack(img, size_mb=128, mount_point=mnt, dm_target=dm_target)

    def test_setup_is_byte_identical_with_no_dm_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            s = self._setup_stack(tmp, None)
            fake = self._fake_for(None)
            with mock.patch.object(dm_stack, "_run", fake):
                s.setup()
            # Exactly losetup, mkfs.ext4, mount — nothing dm-flavoured.
            self.assertEqual([c[0] for c in fake.calls], ["losetup", "mkfs.ext4", "mount"])
            self.assertIn("/dev/loop7", fake.calls[1], "mkfs must target the loop device")
            self.assertIn("/dev/loop7", fake.calls[2], "mount must target the loop device")

    def test_setup_inserts_dmsetup_between_losetup_and_mkfs_for_flakey(self):
        with tempfile.TemporaryDirectory() as tmp:
            s = self._setup_stack(tmp, "flakey")
            fake = self._fake_for("flakey")
            with mock.patch.object(dm_stack, "_run", fake):
                s.setup()
            self.assertEqual(
                [c[0] for c in fake.calls],
                ["losetup", "blockdev", "dmsetup", "dmsetup", "mkfs.ext4", "mount"],
            )
            mapper = f"/dev/mapper/{s.dm_name}"
            self.assertIn(mapper, fake.calls[4], "mkfs must target the mapper device")
            self.assertIn(mapper, fake.calls[5], "mount must target the mapper device")

    def test_setup_inserts_dmsetup_between_losetup_and_mkfs_for_linear(self):
        with tempfile.TemporaryDirectory() as tmp:
            s = self._setup_stack(tmp, "linear")
            fake = self._fake_for("linear")
            with mock.patch.object(dm_stack, "_run", fake):
                s.setup()
            self.assertEqual(
                [c[0] for c in fake.calls],
                ["losetup", "blockdev", "dmsetup", "dmsetup", "mkfs.ext4", "mount"],
            )
            mapper = f"/dev/mapper/{s.dm_name}"
            self.assertIn(mapper, fake.calls[4], "mkfs must target the mapper device")
            self.assertIn(mapper, fake.calls[5], "mount must target the mapper device")


class TestTeardownStackOrder(unittest.TestCase):
    def test_teardown_is_byte_identical_with_no_dm_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            open(img, "wb").close()
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt)
            s.loop_device = "/dev/loop7"
            fake = _fake_run({})
            with mock.patch.object(dm_stack, "_run", fake):
                s.teardown()
            self.assertEqual([c[0] for c in fake.calls], ["umount", "losetup"])

    def test_teardown_removes_the_dm_mapping_between_umount_and_detach(self):
        # _dm_remove is Task 4's polling guard, not yet defined. `create=True`
        # lets us patch a module attribute that doesn't exist yet — this test
        # only pins the CALL SITE and its position in the sequence, not the
        # (not-yet-written) polling behaviour itself.
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            open(img, "wb").close()
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt, dm_target="flakey")
            s.loop_device = "/dev/loop7"
            s.dm_name = "weir-chaos-flakey-1234"

            order = []

            def fake_run(cmd, check=True):
                order.append(cmd[0])
                return _Result(0, "", "")

            def fake_remove(name):
                order.append("dm_remove")

            with mock.patch.object(dm_stack, "_run", fake_run), mock.patch.object(
                dm_stack, "_dm_remove", fake_remove, create=True
            ):
                s.teardown()

            self.assertEqual(order, ["umount", "dm_remove", "losetup"])
            self.assertIsNone(s.dm_name, "dm_name must be cleared once removed")


if __name__ == "__main__":
    unittest.main()
