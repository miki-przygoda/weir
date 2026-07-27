"""Tests for the device-mapper stack plumbing.

The setup/teardown test needs root and Linux; it skips otherwise so the file
is still runnable on a dev machine.
"""
import os
import platform
import subprocess
import tempfile
import unittest

import dm_stack


def _needs_root_linux():
    return platform.system() != "Linux" or os.geteuid() != 0


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


if __name__ == "__main__":
    unittest.main()
