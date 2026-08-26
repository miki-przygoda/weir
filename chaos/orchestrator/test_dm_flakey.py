"""Tests for dm-flakey table construction.

This module is pure on purpose. The two ways a flakey device silently ends up
configured differently than you asked — omitted feature args, and a stale table
surviving an async remove — are both invisible until a run reports a violation
that never happened. Table construction is the half that can be tested on a
laptop with no root, so it is tested exhaustively here.
"""
import unittest

import dm_flakey


class TestTableConstruction(unittest.TestCase):
    def test_engaged_table_is_down_for_the_whole_interval(self):
        # up_interval=0, down_interval=N -> the fault is always active.
        t = dm_flakey.flakey_table("/dev/loop7", 65536, engaged=True, down_secs=60)
        self.assertEqual(t, "0 65536 flakey /dev/loop7 0 0 60 1 drop_writes")

    def test_disengaged_table_is_up_for_the_whole_interval(self):
        # up_interval=N, down_interval=0 -> the feature is inert.
        t = dm_flakey.flakey_table("/dev/loop7", 65536, engaged=False, down_secs=60)
        self.assertEqual(t, "0 65536 flakey /dev/loop7 0 60 0 1 drop_writes")

    def test_feature_args_are_never_omitted(self):
        # A table with NO feature args comes back from the kernel as
        # `2 error_reads error_writes` — an ERRORING device, not a pass-through,
        # and it fails towards false violations against weir.
        for engaged in (True, False):
            t = dm_flakey.flakey_table("/dev/loop0", 1024, engaged=engaged)
            self.assertIn("1 drop_writes", t)
            self.assertNotIn("error_writes", t)

    def test_rejects_a_zero_or_negative_sector_count(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                dm_flakey.flakey_table("/dev/loop0", bad, engaged=True)


class TestTableParsing(unittest.TestCase):
    def test_reads_back_an_engaged_table(self):
        # `dmsetup table` reports the device as major:minor, not by path.
        line = "0 65536 flakey 7:19 0 0 60 1 drop_writes"
        self.assertTrue(dm_flakey.table_is_engaged(line))

    def test_reads_back_a_disengaged_table(self):
        line = "0 65536 flakey 7:19 0 60 0 1 drop_writes"
        self.assertFalse(dm_flakey.table_is_engaged(line))

    def test_detects_the_kernel_substituting_erroring_defaults(self):
        # This is what you get back if you submit no feature args.
        line = "0 32768 flakey 7:19 0 60 0 2 error_reads error_writes"
        with self.assertRaises(dm_flakey.UnexpectedTable):
            dm_flakey.table_is_engaged(line)

    def test_rejects_a_table_that_is_not_flakey_at_all(self):
        with self.assertRaises(dm_flakey.UnexpectedTable):
            dm_flakey.table_is_engaged("0 65536 linear 7:19 0")


if __name__ == "__main__":
    unittest.main()
