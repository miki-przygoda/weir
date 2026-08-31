"""Differential test: DenseAccumulator must agree with ReferenceAccumulator.

This is the oracle-for-the-oracle. A representation change to the verifier is
only safe if it is observably behaviour-preserving, and "we unit-tested it" is
not that claim — a broken oracle fails in the dangerous direction, reporting
success. So the two implementations are driven with identical randomised event
streams and every field of every VerifyResult is compared.

The generator deliberately produces the awkward cases: deliveries before their
ledger line, orphans with no ledger line at all, tag conflicts, duplicates past
the count-overflow boundary, and lines from a foreign run_id that must be
filtered out.
"""
import dataclasses
import random
import unittest

import verify

RUN_ID = 7
FOREIGN_RUN_ID = 9


def ledger_line(seq, tag):
    """A ledger line in the format parse_ledger_line accepts.

    The sixth field is present iff the tag is NACK — the strict contract the
    Rust decoder mirrors, so a NACK without one is malformed and must be
    dropped by BOTH implementations identically.
    """
    if tag == "NACK":
        return f"{seq} 1700000000 t0 256 NACK PayloadTooLarge"
    return f"{seq} 1700000000 t0 256 {tag}"


def delivered_line(seq, run_id=RUN_ID):
    return f"{run_id} {seq}"


def random_batches(rng, n_batches=12, universe=400):
    """Yields (ledger_lines, delivered_lines) pairs.

    State is intentionally NOT tracked across batches: repeats, conflicts and
    out-of-order arrivals are the point, and both implementations must handle
    them the same way.
    """
    for _ in range(n_batches):
        ledger, delivered = [], []
        for _ in range(rng.randint(0, 40)):
            seq = rng.randrange(universe)
            tag = rng.choices(["ACK", "NACK", "UNK"], weights=[80, 10, 10])[0]
            ledger.append(ledger_line(seq, tag))
        for _ in range(rng.randint(0, 40)):
            seq = rng.randrange(universe)
            if rng.random() < 0.05:
                delivered.append(delivered_line(seq, FOREIGN_RUN_ID))
            else:
                delivered.append(delivered_line(seq))
        # Occasionally hammer one seq past the count-overflow boundary.
        if rng.random() < 0.15:
            hot = rng.randrange(universe)
            delivered.extend(delivered_line(hot) for _ in range(rng.randint(63, 80)))
        yield ledger, delivered


class TestDifferential(unittest.TestCase):
    def assert_same(self, ref, dense, ctx):
        a = dataclasses.asdict(ref)
        b = dataclasses.asdict(dense)
        rate_a, rate_b = a.pop("duplicate_rate"), b.pop("duplicate_rate")
        # asdict comparison is deliberate: it fails on any field added later
        # that the dense implementation forgot to populate.
        self.assertEqual(a, b, ctx)
        # Both sides compute known_total / known_distinct from identical
        # Python ints, so the result is bit-identical, not merely close —
        # confirmed across 3,000 fuzz seeds. assertEqual is strictly
        # stronger than an approximate comparison here, with no downside.
        self.assertEqual(rate_a, rate_b, ctx)

    def assert_accumulators_agree(self, ref, dense, ctx):
        """Compares the accumulators' own documented public surface, not just
        the VerifyResult they produce — delivered_total and conflicts are
        otherwise exercised only indirectly (via delivered_distinct/duplicate_rate
        and ledger_conflicts), and ledger_hwm isn't a VerifyResult field at all.
        """
        self.assertEqual(ref.delivered_total, dense.delivered_total, ctx)
        self.assertEqual(ref.conflicts, dense.conflicts, ctx)
        self.assertEqual(ref.ledger_hwm, dense.ledger_hwm, ctx)

    def test_agrees_across_many_random_streams(self):
        for seed in range(60):
            rng = random.Random(seed)
            ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
            dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
            for i, (lg, dl) in enumerate(random_batches(rng)):
                ref.ingest(lg, dl)
                dense.ingest(lg, dl)
                for slack in (0, 1, 25, 10_000):
                    ctx = f"seed={seed} batch={i} slack={slack}"
                    self.assert_same(
                        ref.check(frontier_slack=slack),
                        dense.check(frontier_slack=slack),
                        ctx,
                    )
                self.assert_accumulators_agree(ref, dense, f"seed={seed} batch={i}")

    def test_agrees_on_an_empty_stream(self):
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        self.assert_same(ref.check(), dense.check(), "empty")
        self.assert_accumulators_agree(ref, dense, "empty")

    def test_agrees_when_every_record_is_nacked_and_leaked(self):
        lg = [ledger_line(s, "NACK") for s in range(50)]
        dl = [delivered_line(s) for s in range(50)]
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        ref.ingest(lg, dl)
        dense.ingest(lg, dl)
        r = ref.check()
        self.assert_same(r, dense.check(), "all-nacked")
        self.assert_accumulators_agree(ref, dense, "all-nacked")
        self.assertEqual(len(r.i2_leaked), 50)

    def test_agrees_on_sparse_far_apart_seqs(self):
        # loadgen's seq is dense, but the array must not misbehave if it is not.
        lg = [ledger_line(s, "ACK") for s in (0, 5, 100_000)]
        dl = [delivered_line(s) for s in (0, 100_000)]
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        ref.ingest(lg, dl)
        dense.ingest(lg, dl)
        self.assert_same(ref.check(), dense.check(), "sparse")
        self.assert_same(ref.check(frontier_slack=10), dense.check(frontier_slack=10), "sparse+slack")
        self.assert_accumulators_agree(ref, dense, "sparse")

    def test_tier_awareness_does_not_change_phase_1_results(self):
        # The new exemption must be reachable ONLY by Buffered + power_loss.
        # Any Phase 1 input must produce byte-identical results with or without
        # the new arguments, or a legitimate exemption has become a licence to
        # weaken I1 generally.
        rng = random.Random(99)
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        for lg, dl in random_batches(rng):
            ref.ingest(lg, dl); dense.ingest(lg, dl)
            plain = dense.check()
            tiered = dense.check(tier="D", fault="kill_random")
            self.assert_same(plain, tiered, "tier-aware must not alter Phase 1")
            self.assert_same(ref.check(), tiered, "and must still match the reference")

    def test_agrees_on_the_buffered_power_loss_exemption(self):
        # MINOR (final review): the differential oracle never drove the two
        # implementations together with the exemption ACTIVE — only Phase 1
        # coverage (tier-blind, and tier-aware-but-inert above) existed.
        # ReferenceAccumulator.check and DenseAccumulator.check compute
        # expected_loss/i1_missing via two separate code paths for
        # tier="U", fault="power_loss", and neither had a cross-check.
        for seed in range(20):
            rng = random.Random(seed)
            ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
            dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
            for i, (lg, dl) in enumerate(random_batches(rng)):
                ref.ingest(lg, dl)
                dense.ingest(lg, dl)
                ctx = f"seed={seed} batch={i}"
                self.assert_same(
                    ref.check(tier="U", fault="power_loss"),
                    dense.check(tier="U", fault="power_loss"),
                    ctx,
                )

    def test_ledger_hwm_matches(self):
        rng = random.Random(1234)
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        for lg, dl in random_batches(rng):
            ref.ingest(lg, dl)
            dense.ingest(lg, dl)
            self.assertEqual(ref.ledger_hwm, dense.ledger_hwm)

    def test_agrees_on_a_negative_delivered_seq_after_the_array_has_grown(self):
        # Finding 1: the array is WARMED to 1024 cells by one legitimate
        # record first, then fed a corrupt line with seq=-1. Python's negative
        # indexing makes DenseAccumulator's self._cells[-1] alias cell 1023 —
        # a REAL cell — rather than raising, so a phantom delivery count
        # planted there can hide a genuinely lost record. Warming first is
        # required: on a still-empty array the same input raises IndexError
        # instead, which would not exercise the aliasing form of the bug.
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        warm = ([ledger_line(0, "ACK")], [delivered_line(0)])
        ref.ingest(*warm)
        dense.ingest(*warm)
        corrupt = ([], [delivered_line(-1)])
        ref.ingest(*corrupt)
        dense.ingest(*corrupt)
        # seq 1023 is acked but never delivered: this must stay a real I1
        # miss, not be masked by the phantom count the corrupt line planted.
        lost = ([ledger_line(1023, "ACK")], [])
        ref.ingest(*lost)
        dense.ingest(*lost)
        self.assert_same(ref.check(), dense.check(), "negative delivered seq")
        self.assert_accumulators_agree(ref, dense, "negative delivered seq")

    def test_agrees_on_a_negative_ledger_seq_after_the_array_has_grown(self):
        # Same hazard from the ledger side: a negative seq aliases onto cell
        # 1023 and plants a stray NACK tag there, so the real ACK that later
        # legitimately owns seq 1023 collides with it on ingest — fabricating
        # both a ledger conflict and an I2 leak for a perfectly healthy,
        # delivered record.
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        warm = ([ledger_line(0, "ACK")], [delivered_line(0)])
        ref.ingest(*warm)
        dense.ingest(*warm)
        corrupt = ([ledger_line(-1, "NACK")], [])
        ref.ingest(*corrupt)
        dense.ingest(*corrupt)
        healthy = ([ledger_line(1023, "ACK")], [delivered_line(1023)])
        ref.ingest(*healthy)
        dense.ingest(*healthy)
        self.assert_same(ref.check(), dense.check(), "negative ledger seq")
        self.assert_accumulators_agree(ref, dense, "negative ledger seq")


class TestActiveImplementation(unittest.TestCase):
    def test_the_harness_runs_the_dense_accumulator(self):
        self.assertIs(verify.Accumulator, verify.DenseAccumulator)


if __name__ == "__main__":
    unittest.main()
