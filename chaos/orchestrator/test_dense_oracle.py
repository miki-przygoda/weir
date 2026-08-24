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
            delivered.extend(delivered_line(hot) for _ in range(rng.randint(60, 75)))
        yield ledger, delivered


class TestDifferential(unittest.TestCase):
    def assert_same(self, ref, dense, ctx):
        a = dataclasses.asdict(ref)
        b = dataclasses.asdict(dense)
        rate_a, rate_b = a.pop("duplicate_rate"), b.pop("duplicate_rate")
        # asdict comparison is deliberate: it fails on any field added later
        # that the dense implementation forgot to populate.
        self.assertEqual(a, b, ctx)
        self.assertAlmostEqual(rate_a, rate_b, places=12, msg=ctx)

    def test_agrees_across_many_random_streams(self):
        for seed in range(60):
            rng = random.Random(seed)
            ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
            dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
            for i, (lg, dl) in enumerate(random_batches(rng)):
                ref.ingest(lg, dl)
                dense.ingest(lg, dl)
                for slack in (0, 1, 25, 10_000):
                    self.assert_same(
                        ref.check(frontier_slack=slack),
                        dense.check(frontier_slack=slack),
                        f"seed={seed} batch={i} slack={slack}",
                    )

    def test_agrees_on_an_empty_stream(self):
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        self.assert_same(ref.check(), dense.check(), "empty")

    def test_agrees_when_every_record_is_nacked_and_leaked(self):
        lg = [ledger_line(s, "NACK") for s in range(50)]
        dl = [delivered_line(s) for s in range(50)]
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        ref.ingest(lg, dl)
        dense.ingest(lg, dl)
        r = ref.check()
        self.assert_same(r, dense.check(), "all-nacked")
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

    def test_ledger_hwm_matches(self):
        rng = random.Random(1234)
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        for lg, dl in random_batches(rng):
            ref.ingest(lg, dl)
            dense.ingest(lg, dl)
            self.assertEqual(ref.ledger_hwm, dense.ledger_hwm)


if __name__ == "__main__":
    unittest.main()
