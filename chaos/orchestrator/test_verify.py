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


class TestPushedAndNacked(unittest.TestCase):
    """C2: acked and nacked are BOTH vacuous when acked is empty — a run where
    weir refuses or delivers everything must not read as a healthy 20/20.
    """

    def test_a_fully_nacked_run_is_distinguishable_from_a_healthy_one(self):
        # Demonstrated finding: Nack(InternalError) is recoverable, so loadgen
        # keeps pushing at full rate while every record is refused. acked=0
        # makes I1 and I2 vacuously true; nothing about ok/i1_missing/i2_leaked
        # told this apart from an idle daemon. nacked_count and pushed do.
        ledger_dict = {i: ("NACK", "InternalError") for i in range(1, 100001)}
        r = verify.check(ledger_dict, [])
        self.assertEqual(r.acked_count, 0)
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.i2_leaked, [])
        self.assertEqual(r.nacked_count, 100000, "the missing figure the review flagged")
        self.assertEqual(r.pushed, 100000, "so 'nothing happened' is no longer silent")

    def test_pushed_counts_every_outcome_regardless_of_tag(self):
        r = verify.check({1: ("ACK", ""), 2: ("NACK", "x"), 3: ("UNK", "")}, [1])
        self.assertEqual(r.pushed, 3)
        self.assertEqual(r.acked_count, 1)
        self.assertEqual(r.nacked_count, 1)
        self.assertEqual(r.unknown_count, 1)

    def test_summary_surfaces_pushed_and_nacked(self):
        r = verify.check({1: ("NACK", "x")}, [])
        self.assertIn("pushed=1", r.summary())
        self.assertIn("nacked=1", r.summary())

    def test_accumulator_exposes_nacked_and_pushed(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 NACK oops", "2 S 11 21 ACK"], ["7 2"])
        r = acc.check()
        self.assertEqual(r.nacked_count, 1)
        self.assertEqual(r.pushed, 2)


class TestFrontier(unittest.TestCase):
    """I3: continuous load means there is always in-flight work at check
    time. `frontier_slack` exempts records right at the edge of the ledger's
    coverage rather than misreading them as violations — but the default
    (0) must be a complete no-op, since every pre-existing caller never heard
    of a frontier.
    """

    def test_default_frontier_slack_is_a_no_op(self):
        r = verify.check({1: ("ACK", ""), 2: ("ACK", "")}, [1, 999])
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [2], "unexempted: identical to pre-frontier behaviour")
        self.assertEqual(r.i1_exempt, 0)
        self.assertEqual(r.orphaned_delivered, [999], "a plain orphan, not reclassified")
        self.assertEqual(r.pending_provenance, 0)

    def test_an_acked_seq_above_the_frontier_is_exempt_from_i1_not_failed(self):
        # ledger_hwm=2, frontier_slack=1 -> frontier=1. Seq 2 (>1) is acked but
        # undelivered: it may simply not have arrived yet, not lost.
        r = verify.check({1: ("ACK", ""), 2: ("ACK", "")}, [1], frontier_slack=1)
        self.assertTrue(r.ok, "an exempted seq must not fail the episode")
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.i1_exempt, 1, "the exemption must still be visible")

    def test_a_delivered_seq_above_the_frontier_is_pending_not_orphaned(self):
        # ledger_hwm=1, frontier_slack=1 -> frontier=0. Seq 2 (>0) has no
        # ledger entry yet because the ledger hasn't flushed that far, not
        # because it is a stale/foreign record.
        r = verify.check({1: ("ACK", "")}, [1, 2], frontier_slack=1)
        self.assertEqual(r.orphaned_delivered, [])
        self.assertEqual(r.pending_provenance, 1)
        self.assertTrue(r.ok)

    def test_pending_provenance_is_still_excluded_from_the_duplicate_rate(self):
        # Hiding the exemption inside the duplicate rate would replace one
        # silent distortion with another; it stays excluded, just relabelled.
        r = verify.check({1: ("ACK", "")}, [1, 1, 2], frontier_slack=1)
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 2.0)
        self.assertEqual(r.pending_provenance, 1)

    def test_a_seq_at_or_below_the_frontier_is_a_real_i1_violation(self):
        # ledger_hwm=3, frontier_slack=1 -> frontier=2. Seq 1 (<=2) is below
        # the frontier: the ledger has genuinely caught up past it, so a
        # missing delivery there is real, not a timing artefact.
        r = verify.check(
            {1: ("ACK", ""), 2: ("ACK", ""), 3: ("ACK", "")}, [2, 3], frontier_slack=1
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [1])
        self.assertEqual(r.i1_exempt, 0)

    def test_accumulator_check_passes_frontier_slack_through(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK", "2 S 11 21 ACK"], ["7 1"])
        self.assertEqual(acc.ledger_hwm, 2)

        r = acc.check(frontier_slack=1)
        self.assertTrue(r.ok, "seq 2 is within slack of the ledger high-water seq")
        self.assertEqual(r.i1_exempt, 1)

        r0 = acc.check()  # default frontier_slack=0
        self.assertFalse(r0.ok, "no slack means seq 2 is an unexempted miss")


class TestBitPacking(unittest.TestCase):
    def test_tag_codes_round_trip(self):
        for name in ("ACK", "NACK", "UNK"):
            code = verify._TAG_CODE[name]
            self.assertEqual(verify._TAG_NAME[code], name)

    def test_absent_is_zero_so_a_fresh_cell_means_no_tag(self):
        # bytearray() zero-fills, so "absent" MUST be 0 or every new cell
        # would claim a tag it was never given.
        self.assertEqual(verify._TAG_ABSENT, 0)
        self.assertNotIn(verify._TAG_ABSENT, verify._TAG_NAME)

    def test_tag_and_count_share_a_byte_without_collision(self):
        for tag in (verify._TAG_ACK, verify._TAG_NACK, verify._TAG_UNK):
            for count in (0, 1, 62, verify._COUNT_OVERFLOW):
                cell = (count << verify._COUNT_SHIFT) | tag
                self.assertLessEqual(cell, 255)
                self.assertEqual(cell & verify._TAG_MASK, tag)
                self.assertEqual(cell >> verify._COUNT_SHIFT, count)

    def test_overflow_sentinel_is_above_the_literal_ceiling(self):
        self.assertEqual(verify._COUNT_OVERFLOW, verify._COUNT_MAX + 1)


if __name__ == "__main__":
    unittest.main()
