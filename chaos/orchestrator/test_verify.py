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

    def test_a_conflicting_tag_for_one_seq_fails_and_keeps_the_first(self):
        # loadgen allocates seq from a single monotonic counter, so a repeat
        # means corruption or cross-run pollution. Silently overwriting would
        # RECLASSIFY an outcome, which this module's contract forbids.
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["5 S 1 1 UNK"], [])
        acc.ingest(["5 S 2 2 ACK"], [])
        r = acc.check()
        self.assertFalse(r.ok, "verification against a corrupt ledger is meaningless")
        self.assertEqual(r.ledger_conflicts, [(5, "UNK", "ACK")])
        self.assertEqual(r.unknown_count, 1, "the first observation must stand")
        self.assertEqual(r.i1_missing, [], "and it must not become an I1 violation")


class TestOrphansAndProvenance(unittest.TestCase):
    def test_a_delivered_record_with_no_ledger_entry_is_reported_not_absorbed(self):
        # Folding it into the duplicate rate would distort a headline metric
        # with a record that is not a redelivery of anything real.
        r = verify.check({1: ("ACK", "")}, [1, 999])
        self.assertTrue(r.ok, "an orphan is not a durability violation")
        self.assertEqual(r.orphaned_delivered, [999])
        self.assertEqual(r.delivered_distinct, 1, "orphans excluded from the count")
        self.assertAlmostEqual(r.duplicate_rate, 1.0, msg="and from the rate")

    def test_an_unknown_record_that_arrives_is_not_an_orphan(self):
        r = verify.check({1: ("UNK", "")}, [1])
        self.assertTrue(r.ok)
        self.assertEqual(r.orphaned_delivered, [], "UNK is provenance, not an orphan")


class TestLedgerLineStrictness(unittest.TestCase):
    def test_rejects_what_the_rust_encoder_would_never_emit(self):
        # Mirrors LedgerEntry::from_line. Divergence between the two parsers of
        # one contract is a latent way for corrupt input to enter the oracle.
        self.assertIsNone(verify.parse_ledger_line("42 S 1 2 NACK"), "truncated NACK")
        self.assertIsNone(verify.parse_ledger_line("42 S 1 2 ACK junk"), "ACK + trailing")
        self.assertIsNone(verify.parse_ledger_line("42 S 1 2 UNK junk"), "UNK + trailing")
        self.assertIsNone(verify.parse_ledger_line("42 S 1 2 WAT"), "unknown tag")
        self.assertIsNone(verify.parse_ledger_line("notanint S 1 2 ACK"), "bad seq")

    def test_accepts_exactly_what_the_encoder_emits(self):
        self.assertEqual(verify.parse_ledger_line("42 S 1 2 ACK"), (42, "ACK"))
        self.assertEqual(verify.parse_ledger_line("42 U 1 2 UNK"), (42, "UNK"))
        # NACK always carries a reason field, even an empty one (trailing space).
        self.assertEqual(verify.parse_ledger_line("42 B 1 2 NACK "), (42, "NACK"))
        self.assertEqual(
            verify.parse_ledger_line("42 B 1 2 NACK reason with spaces"), (42, "NACK")
        )


class TestTailerSafety(unittest.TestCase):
    def test_a_shrinking_log_raises_rather_than_silently_skipping(self):
        # Seeking past a truncated file's EOF returns b"" forever, so every
        # later line would vanish with no signal — the oracle losing evidence
        # silently, which is worse than a false alarm.
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "log")
            with open(path, "w") as f:
                f.write("a\nb\nc\n")
            tailer = verify.LogTailer(path)
            self.assertEqual(tailer.read_new(), ["a", "b", "c"])
            with open(path, "w") as f:  # truncate + rewrite shorter
                f.write("x\n")
            with self.assertRaises(RuntimeError) as ctx:
                tailer.read_new()
            self.assertIn("shrank", str(ctx.exception))

    def test_a_form_feed_in_a_reason_does_not_fabricate_a_line(self):
        # splitlines() would split on \x0c; the Rust side escapes only \\, \n, \r.
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "log")
            with open(path, "w") as f:
                f.write("1 S 2 3 NACK err\x0cmore\n")
            self.assertEqual(
                verify.LogTailer(path).read_new(), ["1 S 2 3 NACK err\x0cmore"]
            )


if __name__ == "__main__":
    unittest.main()
