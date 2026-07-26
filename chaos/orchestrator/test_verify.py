"""Tests for the three durability invariants."""
import unittest

import verify


def ledger(**kw):
    """Builds a ledger dict: seq -> (tag, reason)."""
    return {int(k): v for k, v in kw.items()}


class TestInvariants(unittest.TestCase):
    def test_clean_run_passes(self):
        r = verify.check(
            ledger(**{"1": ("ACK", ""), "2": ("ACK", ""), "3": ("NACK", "PayloadTooLarge")}),
            [1, 2],
        )
        self.assertTrue(r.ok)
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.i2_leaked, [])
        self.assertEqual(r.acked_count, 2)

    def test_i1_catches_an_acked_record_that_never_arrived(self):
        r = verify.check(ledger(**{"1": ("ACK", ""), "2": ("ACK", "")}), [1])
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [2])

    def test_i2_catches_a_nacked_record_that_was_delivered(self):
        r = verify.check(ledger(**{"1": ("NACK", "EmptyPayload")}), [1])
        self.assertFalse(r.ok)
        self.assertEqual(r.i2_leaked, [1])

    def test_duplicates_are_conformant_and_measured(self):
        # At-least-once: delivering seq 1 three times is legal.
        r = verify.check(ledger(**{"1": ("ACK", "")}), [1, 1, 1])
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 3.0)

    def test_unknown_records_are_counted_not_constrained(self):
        # Delivered-or-not, both conform. Neither may fail the run.
        delivered_yes = verify.check(ledger(**{"1": ("UNK", "")}), [1])
        delivered_no = verify.check(ledger(**{"1": ("UNK", "")}), [])
        self.assertTrue(delivered_yes.ok)
        self.assertTrue(delivered_no.ok)
        self.assertEqual(delivered_yes.unknown_count, 1)
        self.assertEqual(delivered_no.unknown_count, 1)

    def test_duplicate_rate_is_zero_when_nothing_delivered(self):
        r = verify.check(ledger(), [])
        self.assertEqual(r.duplicate_rate, 0.0)


class TestLogTailer(unittest.TestCase):
    def test_returns_only_newly_appended_lines(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "log")
            with open(p, "w") as f:
                f.write("a\nb\n")
            t = verify.LogTailer(p)
            self.assertEqual(t.read_new(), ["a", "b"])
            self.assertEqual(t.read_new(), [], "no new data means no lines")
            with open(p, "a") as f:
                f.write("c\n")
            self.assertEqual(t.read_new(), ["c"])

    def test_withholds_a_partial_trailing_line_until_it_completes(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "log")
            with open(p, "w") as f:
                f.write("full\npar")
            t = verify.LogTailer(p)
            # "par" has no newline yet: the writer is mid-append. Consuming it
            # would corrupt the oracle with a truncated record.
            self.assertEqual(t.read_new(), ["full"])
            with open(p, "a") as f:
                f.write("tial\n")
            self.assertEqual(t.read_new(), ["partial"])

    def test_missing_file_yields_nothing_rather_than_raising(self):
        t = verify.LogTailer("/nonexistent/path/log")
        self.assertEqual(t.read_new(), [])


class TestAccumulator(unittest.TestCase):
    def test_accumulates_across_episodes_without_rereading(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.acked_count, 1)

        # Second episode adds more; earlier state must persist.
        acc.ingest(["2 S 11 21 ACK"], ["7 2"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.acked_count, 2)
        self.assertEqual(r.delivered_distinct, 2)

    def test_filters_delivered_lines_by_run_id(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1", "9 1", "junk"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1, "run 9's record must be ignored")

    def test_detects_a_violation_that_spans_episodes(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], [])
        self.assertFalse(acc.check().ok, "acked but undelivered")
        # Delivery arrives late — which is legal, and the violation clears.
        acc.ingest([], ["7 1"])
        self.assertTrue(acc.check().ok)

    def test_preserves_duplicate_counts_across_episodes(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1"])
        acc.ingest([], ["7 1", "7 1"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 3.0)


if __name__ == "__main__":
    unittest.main()
