"""Tests for the device-mapper stack plumbing.

The setup/teardown test needs root and Linux; it skips otherwise so the file
is still runnable on a dev machine.
"""
import os
import platform
import stat
import subprocess
import tempfile
import time
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

    # MINOR (final review): _verify_linear_target used to check only the
    # target TYPE, not sector count or backing device — so a stale LINEAR
    # table from an unrelated, differently-sized or differently-backed
    # device would pass just as easily as the real one.

    def test_raises_if_the_sector_count_does_not_match(self):
        s = self._stack()
        s._sectors = 65536
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 32768 linear 7:19 0", "")}
        )
        with mock.patch.object(dm_stack, "_run", fake):
            with self.assertRaises(RuntimeError):
                s._dm_create(s.dm_name, "0 65536 linear 7:19 0")

    def test_a_matching_sector_count_passes(self):
        s = self._stack()
        s._sectors = 65536
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 65536 linear 7:19 0", "")}
        )
        with mock.patch.object(dm_stack, "_run", fake):
            s._dm_create(s.dm_name, "0 65536 linear 7:19 0")  # must not raise

    def test_raises_if_the_backing_device_does_not_match(self):
        s = self._stack()
        s._sectors = 65536
        s.loop_device = "/dev/loop7"
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 65536 linear 8:3 0", "")}
        )
        with mock.patch.object(dm_stack, "_run", fake), mock.patch.object(
            dm_stack, "_device_major_minor", lambda p: "7:19"
        ):
            with self.assertRaises(RuntimeError):
                s._dm_create(s.dm_name, "0 65536 linear 8:3 0")

    def test_a_matching_backing_device_passes(self):
        s = self._stack()
        s._sectors = 65536
        s.loop_device = "/dev/loop7"
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 65536 linear 7:19 0", "")}
        )
        with mock.patch.object(dm_stack, "_run", fake), mock.patch.object(
            dm_stack, "_device_major_minor", lambda p: "7:19"
        ):
            s._dm_create(s.dm_name, "0 65536 linear 7:19 0")  # must not raise

    def test_an_unresolvable_loop_device_path_does_not_crash_verification(self):
        # When the device node can't be stat'ed, the device check must degrade
        # to a no-op rather than raising an unrelated OSError out of a routine
        # whose whole job is raising the RIGHT one.
        #
        # The OSError is INJECTED, not assumed. This test used to rely on
        # "/dev/loop7 doesn't exist on a dev machine" — true on the macOS
        # laptop it was written on, false on every Linux box with loop
        # devices, i.e. exactly the machines this harness runs on. There
        # /dev/loop7 resolves to 7:7, mismatches the fixture's 7:19, and the
        # test exercised the raising branch while claiming to prove the
        # opposite. Caught on beast, 2026-08-28.
        s = self._stack()
        s._sectors = 65536
        s.loop_device = "/dev/loop7"
        fake = _fake_run(
            {("dmsetup", "table"): _Result(0, "0 65536 linear 7:19 0", "")}
        )

        def _unresolvable(path):
            raise OSError(2, "No such file or directory", path)

        with mock.patch.object(dm_stack, "_run", fake), mock.patch.object(
            dm_stack, "_device_major_minor", _unresolvable
        ):
            s._dm_create(s.dm_name, "0 65536 linear 7:19 0")  # must not raise


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
            # _fake_for's linear table names 7:19 as the backing device, so
            # the resolver must agree or _verify_linear_target correctly
            # rejects the mapping. Stubbed rather than left to the host: on
            # Linux the faked "/dev/loop7" is a REAL node resolving to 7:7,
            # which failed this test on beast while it passed on macOS purely
            # because macOS has no loop devices to resolve.
            with mock.patch.object(dm_stack, "_run", fake), mock.patch.object(
                dm_stack, "_device_major_minor", lambda p: "7:19"
            ):
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

    def test_teardown_continues_to_detach_even_when_dm_remove_raises(self):
        # I5 (final review): a stuck mapping used to raise straight out of
        # teardown(), skipping the loop-device detach entirely — leaking the
        # loop device AND the backing image behind it, contradicting
        # teardown's documented idempotence (which Phase 1 had).
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
                raise RuntimeError("mapping still present 10s after remove --retry")

            with mock.patch.object(dm_stack, "_run", fake_run), mock.patch.object(
                dm_stack, "_dm_remove", fake_remove, create=True
            ):
                s.teardown()  # must not raise

            self.assertEqual(order, ["umount", "dm_remove", "losetup"])
            self.assertEqual(
                s.dm_name, "weir-chaos-flakey-1234",
                "a mapping that failed to remove must stay visible, not be "
                "silently forgotten",
            )


class TestAsyncRemoveGuard(unittest.TestCase):
    def test_remove_polls_until_the_mapping_is_really_gone(self):
        # `dmsetup remove` returns before udev releases the node. If the next
        # create runs too early it fails EBUSY, the PREVIOUS table stays
        # installed, and the next episode measures the old mapping — a
        # confident result about a device that was never created.
        calls = []
        def fake_run(cmd, check=True):
            calls.append(cmd)
            if cmd[:2] == ["dmsetup", "info"]:
                # Present twice, then gone.
                n = sum(1 for c in calls if c[:2] == ["dmsetup", "info"])
                return _Result(0 if n <= 2 else 1, "", "")
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", fake_run):
            dm_stack._dm_remove("weir-chaos-flakey", timeout_s=5)
        infos = [c for c in calls if c[:2] == ["dmsetup", "info"]]
        self.assertGreaterEqual(len(infos), 3, "must poll, not fire once")

    def test_remove_raises_if_the_mapping_never_goes_away(self):
        def always_present(cmd, check=True):
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", always_present):
            with self.assertRaises(RuntimeError):
                dm_stack._dm_remove("stuck", timeout_s=0.3)

    # I4 (final review): _dm_remove used to fire `dmsetup remove` once with
    # no retry and discard its stderr — so the likeliest real failure (a
    # preceding `umount` that itself failed silently) never reached the
    # operator.

    def test_remove_calls_dmsetup_remove_with_retry(self):
        calls = []
        def fake_run(cmd, check=True):
            calls.append(cmd)
            if cmd[:2] == ["dmsetup", "info"]:
                return _Result(1, "", "")
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", fake_run):
            dm_stack._dm_remove("weir-chaos-flakey")
        self.assertIn(["dmsetup", "remove", "--retry", "weir-chaos-flakey"], calls)

    def test_remove_timeout_message_includes_the_removes_stderr(self):
        def always_present(cmd, check=True):
            if cmd[:2] == ["dmsetup", "remove"]:
                return _Result(1, "", "device-mapper: remove ioctl failed: "
                                      "Device or resource busy")
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", always_present):
            with self.assertRaises(RuntimeError) as ctx:
                dm_stack._dm_remove("stuck", timeout_s=0.3)
        self.assertIn("Device or resource busy", str(ctx.exception))


class TestReloadResumesEvenOnFailure(unittest.TestCase):
    DISENGAGED = "0 65536 flakey 7:19 0 60 0 1 drop_writes"

    def test_a_failed_reload_still_resumes_the_device(self):
        # A device left suspended blocks all I/O to the filesystem, so the
        # daemon hangs rather than crashes — and a hang surfaces as a
        # quiescence timeout, which the harness would misattribute to weir
        # rather than to itself.
        calls = []
        def fake_run(cmd, check=True):
            calls.append(cmd[1] if cmd[0] == "dmsetup" else cmd[0])
            if cmd[0] == "dmsetup" and cmd[1] == "reload":
                raise RuntimeError("simulated reload failure")
            return _Result(0, self.DISENGAGED, "")
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x", dm_target="flakey")
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-flakey-1234"
        s._sectors = 65536
        with mock.patch.object(dm_stack, "_run", fake_run):
            with self.assertRaises(RuntimeError):
                s.engage_fault()
        self.assertIn("resume", calls, "resume must fire even when reload fails")


class TestDropAndRemount(unittest.TestCase):
    """C2 (final review): converts an engaged fault into actual filesystem
    loss. `drop_writes` sits below the page cache, so a dropped write leaves
    a correct, resident page behind — nothing is at risk until this method's
    umount forces writeback (discarded by the lying disk) and the following
    mount rebuilds the filesystem from whatever the disk actually kept.
    """
    DISENGAGED = "0 65536 flakey 7:19 0 60 0 1 drop_writes"

    def _stack(self, mount_point):
        s = dm_stack.StorageStack("/tmp/img", 512, mount_point, dm_target="flakey")
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-flakey-1234"
        s._sectors = 65536
        return s

    def test_refuses_without_a_built_flakey_layer(self):
        s = dm_stack.StorageStack("/tmp/img", 512, "/mnt/x")
        with self.assertRaises(RuntimeError):
            s.drop_and_remount()

    def test_umount_disengage_then_mount_run_in_that_order(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
            with mock.patch.object(dm_stack, "_run", fake):
                s.drop_and_remount()
            order = [
                c[0] if c[0] != "dmsetup" else f"dmsetup {c[1]}" for c in fake.calls
            ]
            self.assertEqual(
                order,
                ["umount", "dmsetup suspend", "dmsetup reload", "dmsetup resume",
                 "dmsetup table", "mount"],
            )

    def test_a_failed_remount_raises_loudly_not_a_warning(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            responses = {
                ("dmsetup", "table"): _Result(0, self.DISENGAGED, ""),
                ("mount", "-t"): _Result(32, "", "wrong fs type, bad option, "
                                                  "bad superblock"),
            }
            fake = _fake_run(responses)
            with mock.patch.object(dm_stack, "_run", fake):
                with self.assertRaises(RuntimeError) as ctx:
                    s.drop_and_remount()
            self.assertIn("wrong fs type", str(ctx.exception))

    def test_wab_dir_inside_the_mount_is_recreated_with_0700_if_missing(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            wab_dir = os.path.join(mnt, "wab")  # never created — didn't survive
            fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
            with mock.patch.object(dm_stack, "_run", fake):
                s.drop_and_remount(wab_dir=wab_dir)
            self.assertTrue(os.path.isdir(wab_dir))
            self.assertEqual(stat.S_IMODE(os.stat(wab_dir).st_mode), 0o700)

    def test_wab_dir_mode_is_reapplied_even_if_it_survived(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            wab_dir = os.path.join(mnt, "wab")
            os.makedirs(wab_dir)
            os.chmod(wab_dir, 0o755)  # simulate the wrong mode surviving
            fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
            with mock.patch.object(dm_stack, "_run", fake):
                s.drop_and_remount(wab_dir=wab_dir)
            self.assertEqual(stat.S_IMODE(os.stat(wab_dir).st_mode), 0o700)

    def test_wab_dir_outside_the_mount_is_left_untouched(self):
        with tempfile.TemporaryDirectory() as outside, \
             tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            not_wab = os.path.join(outside, "elsewhere")
            fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
            with mock.patch.object(dm_stack, "_run", fake):
                s.drop_and_remount(wab_dir=not_wab)
            self.assertFalse(
                os.path.exists(not_wab),
                "a path outside this stack's mount point is none of "
                "drop_and_remount's business",
            )

    def test_wab_dir_none_is_a_no_op(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = self._stack(mnt)
            fake = _fake_run({("dmsetup", "table"): _Result(0, self.DISENGAGED, "")})
            with mock.patch.object(dm_stack, "_run", fake):
                s.drop_and_remount()  # wab_dir defaults to None; must not raise


class TestCanary(unittest.TestCase):
    """I6: a canary block, written before the fault and overwritten while
    it's engaged, is what turns "did the injector bite" from an inference
    into a measurement. Pure filesystem I/O — no `_run`/subprocess involved."""

    def test_write_then_read_round_trips(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = dm_stack.StorageStack("/tmp/img", 512, mnt)
            s.write_canary(b"hello")
            self.assertEqual(s.read_canary(), b"hello")

    def test_a_second_write_truncates_the_first(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = dm_stack.StorageStack("/tmp/img", 512, mnt)
            s.write_canary(b"a much longer first value")
            s.write_canary(b"short")
            self.assertEqual(s.read_canary(), b"short")

    def test_read_before_any_write_is_none(self):
        with tempfile.TemporaryDirectory() as mnt:
            s = dm_stack.StorageStack("/tmp/img", 512, mnt)
            self.assertIsNone(s.read_canary())


if __name__ == "__main__":
    unittest.main()
