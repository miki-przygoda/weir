# Dense Oracle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the chaos verifier's dict-based accumulator with a dense byte-array representation, proven equivalent by a randomised differential test, so soak length stops being bounded by RAM.

**Architecture:** One `bytearray` indexed by `seq` (2-bit tag + 6-bit saturating delivery count, overflow dict for the rare count > 62). The three sets the invariants need — unresolved-acked, leaked, no-provenance — are the *unresolved working set* and are maintained incrementally at ingest, so `check()` becomes O(unresolved) instead of O(all history). The existing dict implementation is retained as `ReferenceAccumulator` and is the differential test's oracle-for-the-oracle.

**Tech Stack:** Python 3.14, `unittest` (the orchestrator's existing style), pytest 9.1.1 as the runner. No new dependencies — `bytearray` is stdlib.

**Spec:** `docs/superpowers/specs/2026-08-24-dense-oracle-design.md`

## Global Constraints

- **Behaviour-preserving.** This is a representation change. Every field of `VerifyResult` must be bit-identical to the current implementation for the same input. Equivalence is the acceptance criterion; performance is the motivation, not the bar.
- **Do not modify `check_counts()` or `check()`.** They are pure, dict-based, and now serve as the reference. Changing them destroys the differential test's independence.
- **`pushed` counts DISTINCT seqs.** A repeated identical ledger line overwrites in the reference (`self.ledger[seq] = (tag, "")`), leaving `len(ledger)` unchanged. The dense version must only increment `_pushed` on the first tag for a seq.
- **On tag conflict, keep the FIRST observation** and append `(seq, prior_tag_string, new_tag)` to `conflicts`. Do not overwrite.
- **Never use a probabilistic structure.** No Bloom filters, no sampling. An oracle with false positives is worse than no oracle.
- **Preserve the public surface:** `Accumulator(delivered_run_id=...)`, `.ingest(ledger_lines, delivered_lines)`, `.check(frontier_slack=0)`, `.ledger_hwm`, `.delivered_total`, `.conflicts`. `run.py:606`, `test_run.py:119` and `test_verify.py` depend on these.
- Python 3.14, tests in `unittest` style to match the existing orchestrator suite.
- Run the suite with: `cd chaos/orchestrator && python3 -m pytest -q`

## File Structure

| File | Responsibility |
|---|---|
| `chaos/orchestrator/verify.py` (modify) | Add bit-packing constants + `DenseAccumulator`; rename existing `Accumulator` to `ReferenceAccumulator`; alias `Accumulator` to the active implementation. `check_counts`/`check` untouched. |
| `chaos/orchestrator/test_verify.py` (modify) | Existing 30 tests keep passing throughout; add unit tests per task. |
| `chaos/orchestrator/test_dense_oracle.py` (create) | The randomised differential test. Separate file because it is the acceptance gate, not a unit test, and should be readable as such. |

Everything lives in `verify.py` because the accumulator and the invariant core must stay in one place — splitting them would put the reference and the implementation-under-test in different files and make the equivalence argument harder to see.

---

### Task 1: Preserve the reference implementation

Lock in the current behaviour under a stable name before anything changes, so later tasks always have something to diff against.

**Files:**
- Modify: `chaos/orchestrator/verify.py:188` (class `Accumulator`)
- Test: `chaos/orchestrator/test_verify.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ReferenceAccumulator` — the current class, unchanged in behaviour. `Accumulator` remains a working name via alias.

- [ ] **Step 1: Rename the class and add the alias**

In `verify.py`, change the class statement and append the alias immediately after the class body ends (just before `def check(`):

```python
class ReferenceAccumulator:
    """Accumulated verification state across episodes — dict-based reference.

    Retained as the differential test's oracle-for-the-oracle: the dense
    implementation must prove it agrees with this before replacing it. Do not
    optimise this class. Its value is that it is obviously correct and never
    changes.
    """
```

Then after the class:

```python
#: The accumulator the harness actually runs. Flipped to DenseAccumulator in
#: Task 7, once the differential test proves the two agree.
Accumulator = ReferenceAccumulator
```

- [ ] **Step 2: Run the suite to verify nothing broke**

Run: `cd chaos/orchestrator && python3 -m pytest -q`
Expected: PASS, same test count as before (30 in `test_verify.py`).

- [ ] **Step 3: Commit**

```bash
git add chaos/orchestrator/verify.py
git commit -F - <<'MSG'
refactor(chaos): name the dict accumulator as the reference

It is about to become the thing a dense implementation is diffed against,
so it needs a name that says so and a docstring that tells the next person
not to optimise it.
MSG
```

---

### Task 2: Bit-packing primitives

**Files:**
- Modify: `chaos/orchestrator/verify.py` (add constants near the top, after the imports)
- Test: `chaos/orchestrator/test_verify.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `_TAG_ABSENT`, `_TAG_ACK`, `_TAG_NACK`, `_TAG_UNK`, `_TAG_MASK`, `_COUNT_SHIFT`, `_COUNT_MAX`, `_COUNT_OVERFLOW`, `_TAG_CODE` (str->int), `_TAG_NAME` (int->str).

- [ ] **Step 1: Write the failing test**

Add to `test_verify.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k BitPacking`
Expected: FAIL with `AttributeError: module 'verify' has no attribute '_TAG_CODE'`

- [ ] **Step 3: Write the implementation**

In `verify.py`, after `from dataclasses import dataclass, field`:

```python
# ── Dense oracle cell layout ─────────────────────────────────────────────────
# One byte per seq: bits 0-1 the ledger tag, bits 2-7 the delivery count.
# ABSENT must be 0 — bytearray zero-fills, so any other choice would make a
# never-seen seq claim a tag it was never given.
_TAG_ABSENT = 0
_TAG_ACK = 1
_TAG_NACK = 2
_TAG_UNK = 3

_TAG_MASK = 0b11
_COUNT_SHIFT = 2
#: Highest delivery count stored literally in the cell.
_COUNT_MAX = 62
#: Sentinel meaning "the true count lives in the overflow dict". Counts this
#: high need a crash loop redelivering one record 63 times; the dict keeps the
#: total exact if that ever happens rather than silently saturating.
_COUNT_OVERFLOW = 63

_TAG_CODE = {"ACK": _TAG_ACK, "NACK": _TAG_NACK, "UNK": _TAG_UNK}
_TAG_NAME = {v: k for k, v in _TAG_CODE.items()}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k BitPacking`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py
git commit -F - <<'MSG'
feat(chaos): cell layout for the dense oracle

Tag and delivery count in one byte. ABSENT is 0 because bytearray zero-fills
and any other choice would let an untouched cell claim a tag.
MSG
```

---

### Task 3: DenseAccumulator — ledger ingest

**Files:**
- Modify: `chaos/orchestrator/verify.py` (new class after `ReferenceAccumulator`)
- Test: `chaos/orchestrator/test_verify.py`

**Interfaces:**
- Consumes: Task 2's constants; `parse_ledger_line`, `parse_delivered_line`.
- Produces: `DenseAccumulator(delivered_run_id)` with `_grow(seq)`, `_tag(seq)`, `_count(seq)`, `_ingest_ledger(seq, tag)`, and attributes `ledger_hwm`, `conflicts`, `_pushed`, `_acked`, `_nacked`, `_unknown`, `_unresolved_acked`, `_leaked`, `_no_provenance`, `_cells`, `_overflow`, `delivered_total`, `_delivered_distinct`.

- [ ] **Step 1: Write the failing test**

```python
class TestDenseLedgerIngest(unittest.TestCase):
    def acc(self):
        return verify.DenseAccumulator(delivered_run_id=7)

    def test_records_tag_and_counts_one_push(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        self.assertEqual(a._tag(3), verify._TAG_ACK)
        self.assertEqual(a._pushed, 1)
        self.assertEqual(a._acked, 1)
        self.assertEqual(a.ledger_hwm, 3)

    def test_a_repeated_identical_line_is_not_a_second_push(self):
        # The reference overwrites the dict entry, so len(ledger) is unchanged.
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        a._ingest_ledger(3, "ACK")
        self.assertEqual(a._pushed, 1)
        self.assertEqual(a._acked, 1)
        self.assertEqual(a.conflicts, [])

    def test_conflicting_tags_keep_the_first_and_record_the_conflict(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        a._ingest_ledger(3, "NACK")
        self.assertEqual(a._tag(3), verify._TAG_ACK)
        self.assertEqual(a.conflicts, [(3, "ACK", "NACK")])
        self.assertEqual(a._pushed, 1)
        self.assertEqual(a._nacked, 0)

    def test_an_acked_seq_with_no_delivery_is_unresolved(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        self.assertEqual(a._unresolved_acked, {3})

    def test_hwm_tracks_the_maximum_not_the_latest(self):
        a = self.acc()
        a._ingest_ledger(9, "ACK")
        a._ingest_ledger(2, "ACK")
        self.assertEqual(a.ledger_hwm, 9)

    def test_the_array_grows_to_cover_a_distant_seq(self):
        a = self.acc()
        a._ingest_ledger(100_000, "ACK")
        self.assertEqual(a._tag(100_000), verify._TAG_ACK)
        self.assertEqual(a._tag(50_000), verify._TAG_ABSENT)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseLedgerIngest`
Expected: FAIL with `AttributeError: module 'verify' has no attribute 'DenseAccumulator'`

- [ ] **Step 3: Write the implementation**

```python
class DenseAccumulator:
    """Accumulated verification state, one byte per seq.

    Same contract as `ReferenceAccumulator`, ~336x smaller. `seq` comes from a
    single shared AtomicU64 in loadgen, so it is dense from 0 and an array
    indexed by it needs no key storage.

    The three sets below are the UNRESOLVED working set, not history: each is
    defined by a state transition visible at ingest time, and each empties as
    records resolve. Maintaining them here is what makes `check()` O(unresolved)
    instead of O(everything ever seen).
    """

    def __init__(self, delivered_run_id):
        self.run_id = delivered_run_id
        self._cells = bytearray()
        #: seq -> true count, for the rare seq whose count exceeds _COUNT_MAX.
        self._overflow = {}
        self.delivered_total = 0
        self.ledger_hwm = 0
        self.conflicts = []
        self._pushed = 0
        self._acked = 0
        self._nacked = 0
        self._unknown = 0
        self._delivered_distinct = 0
        #: acked, never delivered — this IS i1_absent.
        self._unresolved_acked = set()
        #: nacked and delivered anyway — this IS i2_leaked. Should stay empty.
        self._leaked = set()
        #: delivered with no ledger entry yet.
        self._no_provenance = set()

    def _grow(self, seq):
        if seq < len(self._cells):
            return
        # Doubling keeps growth amortised O(1); the max() floor stops a long
        # run reallocating on every new seq once the array is large.
        new_len = max(seq + 1, len(self._cells) * 2, 1024)
        self._cells.extend(bytes(new_len - len(self._cells)))

    def _tag(self, seq):
        if seq >= len(self._cells):
            return _TAG_ABSENT
        return self._cells[seq] & _TAG_MASK

    def _count(self, seq):
        if seq >= len(self._cells):
            return 0
        packed = self._cells[seq] >> _COUNT_SHIFT
        return self._overflow[seq] if packed == _COUNT_OVERFLOW else packed

    def _ingest_ledger(self, seq, tag):
        self.ledger_hwm = max(self.ledger_hwm, seq)
        self._grow(seq)
        code = _TAG_CODE[tag]
        prior = self._cells[seq] & _TAG_MASK
        if prior != _TAG_ABSENT:
            if prior != code:
                # Two different tags for one seq. loadgen allocates seq from a
                # single monotonic counter, so no legitimate retry reuses one.
                # Keep the FIRST observation, exactly as the reference does —
                # silently reclassifying an outcome is what an oracle must not do.
                self.conflicts.append((seq, _TAG_NAME[prior], tag))
            return
        packed = self._cells[seq] >> _COUNT_SHIFT
        self._cells[seq] = (packed << _COUNT_SHIFT) | code
        self._pushed += 1
        delivered = self._count(seq)
        if code == _TAG_ACK:
            self._acked += 1
            if delivered == 0:
                self._unresolved_acked.add(seq)
        elif code == _TAG_NACK:
            self._nacked += 1
            if delivered:
                self._leaked.add(seq)
        else:
            self._unknown += 1
        if delivered:
            # Provenance has arrived for something already delivered.
            self._no_provenance.discard(seq)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseLedgerIngest`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py
git commit -F - <<'MSG'
feat(chaos): dense accumulator, ledger side

Tags into the byte array, and the unresolved-acked set that check() will read
instead of rebuilding from all history.
MSG
```

---

### Task 4: DenseAccumulator — delivery ingest and count overflow

**Files:**
- Modify: `chaos/orchestrator/verify.py` (add to `DenseAccumulator`)
- Test: `chaos/orchestrator/test_verify.py`

**Interfaces:**
- Consumes: Task 3's `DenseAccumulator`.
- Produces: `_bump_count(seq)`, `_ingest_delivered(seq)`, `ingest(ledger_lines, delivered_lines)`.

- [ ] **Step 1: Write the failing test**

```python
class TestDenseDeliveryIngest(unittest.TestCase):
    def acc(self):
        return verify.DenseAccumulator(delivered_run_id=7)

    def test_first_delivery_resolves_an_acked_seq(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        a._ingest_delivered(3)
        self.assertEqual(a._unresolved_acked, set())
        self.assertEqual(a._delivered_distinct, 1)
        self.assertEqual(a.delivered_total, 1)

    def test_duplicates_raise_total_but_not_distinct(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        for _ in range(5):
            a._ingest_delivered(3)
        self.assertEqual(a.delivered_total, 5)
        self.assertEqual(a._delivered_distinct, 1)
        self.assertEqual(a._count(3), 5)

    def test_delivery_before_ledger_is_no_provenance_until_the_tag_arrives(self):
        a = self.acc()
        a._ingest_delivered(3)
        self.assertEqual(a._no_provenance, {3})
        a._ingest_ledger(3, "ACK")
        self.assertEqual(a._no_provenance, set())

    def test_a_delivered_nacked_seq_leaks_in_either_order(self):
        ledger_first = self.acc()
        ledger_first._ingest_ledger(3, "NACK")
        ledger_first._ingest_delivered(3)
        self.assertEqual(ledger_first._leaked, {3})

        delivery_first = self.acc()
        delivery_first._ingest_delivered(3)
        delivery_first._ingest_ledger(3, "NACK")
        self.assertEqual(delivery_first._leaked, {3})

    def test_count_stays_exact_past_the_literal_ceiling(self):
        a = self.acc()
        a._ingest_ledger(3, "ACK")
        for _ in range(verify._COUNT_MAX + 10):
            a._ingest_delivered(3)
        self.assertEqual(a._count(3), verify._COUNT_MAX + 10)
        self.assertEqual(a.delivered_total, verify._COUNT_MAX + 10)
        # The tag must survive the overflow transition intact.
        self.assertEqual(a._tag(3), verify._TAG_ACK)

    def test_ingest_parses_lines_and_filters_other_runs(self):
        a = self.acc()
        a.ingest(["3 t x 256 ACK"], ["7 3", "9 3"])
        self.assertEqual(a._acked, 1)
        self.assertEqual(a.delivered_total, 1)  # the run_id 9 line is not ours
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseDeliveryIngest`
Expected: FAIL with `AttributeError: 'DenseAccumulator' object has no attribute '_ingest_delivered'`

- [ ] **Step 3: Write the implementation**

Add to `DenseAccumulator`:

```python
    def _bump_count(self, seq):
        cell = self._cells[seq]
        packed = cell >> _COUNT_SHIFT
        if packed == _COUNT_OVERFLOW:
            self._overflow[seq] += 1
            return
        if packed == _COUNT_MAX:
            # Move into the overflow dict and flip the sentinel, preserving the
            # tag bits — the count leaves the cell, the tag does not.
            self._overflow[seq] = _COUNT_MAX + 1
            self._cells[seq] = (_COUNT_OVERFLOW << _COUNT_SHIFT) | (cell & _TAG_MASK)
            return
        self._cells[seq] = ((packed + 1) << _COUNT_SHIFT) | (cell & _TAG_MASK)

    def _ingest_delivered(self, seq):
        self._grow(seq)
        first = self._count(seq) == 0
        self._bump_count(seq)
        self.delivered_total += 1
        if not first:
            return
        self._delivered_distinct += 1
        tag = self._cells[seq] & _TAG_MASK
        if tag == _TAG_ACK:
            self._unresolved_acked.discard(seq)
        elif tag == _TAG_NACK:
            self._leaked.add(seq)
        elif tag == _TAG_ABSENT:
            self._no_provenance.add(seq)

    def ingest(self, ledger_lines, delivered_lines):
        """Folds newly-read lines into the accumulated state."""
        for line in ledger_lines:
            parsed = parse_ledger_line(line)
            if not parsed:
                continue
            self._ingest_ledger(*parsed)
        for line in delivered_lines:
            seq = parse_delivered_line(line, self.run_id)
            if seq is not None:
                self._ingest_delivered(seq)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseDeliveryIngest`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py
git commit -F - <<'MSG'
feat(chaos): dense accumulator, delivery side

Counts saturate into an overflow dict rather than capping, so totals stay
exact even if a crash loop redelivers one record more than 62 times.
MSG
```

---

### Task 5: DenseAccumulator.check()

**Files:**
- Modify: `chaos/orchestrator/verify.py` (add to `DenseAccumulator`)
- Test: `chaos/orchestrator/test_verify.py`

**Interfaces:**
- Consumes: Tasks 3-4.
- Produces: `DenseAccumulator.check(frontier_slack=0) -> VerifyResult`.

- [ ] **Step 1: Write the failing test**

```python
class TestDenseCheck(unittest.TestCase):
    def acc(self):
        return verify.DenseAccumulator(delivered_run_id=7)

    def test_clean_run_passes(self):
        a = self.acc()
        a._ingest_ledger(1, "ACK"); a._ingest_ledger(2, "ACK")
        a._ingest_delivered(1); a._ingest_delivered(2)
        r = a.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.acked_count, 2)
        self.assertEqual(r.pushed, 2)
        self.assertEqual(r.delivered_distinct, 2)

    def test_i1_reports_an_acked_record_that_never_arrived(self):
        a = self.acc()
        a._ingest_ledger(1, "ACK"); a._ingest_ledger(2, "ACK")
        a._ingest_delivered(1)
        r = a.check()
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [2])

    def test_frontier_slack_exempts_rather_than_fails(self):
        a = self.acc()
        a._ingest_ledger(1, "ACK"); a._ingest_ledger(2, "ACK")
        a._ingest_delivered(1)
        r = a.check(frontier_slack=5)   # frontier = 2 - 5 = -3, so 2 is exempt
        self.assertTrue(r.ok)
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.i1_exempt, 1)

    def test_orphans_are_excluded_from_the_duplicate_rate(self):
        a = self.acc()
        a._ingest_ledger(1, "ACK")
        a._ingest_delivered(1); a._ingest_delivered(1)
        a._ingest_delivered(99); a._ingest_delivered(99)  # no provenance
        r = a.check()
        self.assertEqual(r.orphaned_delivered, [99])
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 2.0)

    def test_a_conflict_fails_the_episode(self):
        a = self.acc()
        a._ingest_ledger(1, "ACK"); a._ingest_ledger(1, "NACK")
        r = a.check()
        self.assertFalse(r.ok)
        self.assertEqual(r.ledger_conflicts, [(1, "ACK", "NACK")])

    def test_duplicate_rate_is_zero_when_nothing_delivered(self):
        self.assertEqual(self.acc().check().duplicate_rate, 0.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseCheck`
Expected: FAIL with `AttributeError: 'DenseAccumulator' object has no attribute 'check'`

- [ ] **Step 3: Write the implementation**

Add to `DenseAccumulator`. This mirrors `check_counts` line for line; the only
difference is that the four sets are read rather than rebuilt.

```python
    def check(self, frontier_slack=0):
        """Runs I1/I2/I3 against everything accumulated so far.

        Mirrors `check_counts` exactly. The difference is only where the sets
        come from: maintained incrementally here, rebuilt from the whole ledger
        there.
        """
        if frontier_slack:
            frontier = self.ledger_hwm - frontier_slack
            i1_exempt_seqs = {s for s in self._unresolved_acked if s > frontier}
            pending_provenance_seqs = {s for s in self._no_provenance if s > frontier}
        else:
            i1_exempt_seqs = set()
            pending_provenance_seqs = set()

        i1_missing = sorted(self._unresolved_acked - i1_exempt_seqs)
        i2_leaked = sorted(self._leaked)
        orphaned = sorted(self._no_provenance - pending_provenance_seqs)

        # The exclusion set is always the full no-provenance set: the frontier
        # only changes which LABEL an excluded seq gets, never the rate.
        known_total = self.delivered_total - sum(
            self._count(s) for s in self._no_provenance
        )
        known_distinct = self._delivered_distinct - len(self._no_provenance)
        dup_rate = (known_total / known_distinct) if known_distinct else 0.0

        return VerifyResult(
            ok=not i1_missing and not i2_leaked and not self.conflicts,
            i1_missing=i1_missing,
            i2_leaked=i2_leaked,
            unknown_count=self._unknown,
            acked_count=self._acked,
            nacked_count=self._nacked,
            pushed=self._pushed,
            delivered_distinct=known_distinct,
            duplicate_rate=dup_rate,
            orphaned_delivered=orphaned,
            ledger_conflicts=list(self.conflicts),
            i1_exempt=len(i1_exempt_seqs),
            pending_provenance=len(pending_provenance_seqs),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k DenseCheck`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py
git commit -F - <<'MSG'
feat(chaos): dense check() reads the unresolved sets instead of rebuilding them

check_counts rebuilt four full sets over all history on every episode — by the
end of a 10h run, four 25M-element constructions, 438 times.
MSG
```

---

### Task 6: The differential test

The acceptance gate. Everything before this is unit-tested; this is what
licenses the swap.

**Files:**
- Create: `chaos/orchestrator/test_dense_oracle.py`

**Interfaces:**
- Consumes: `verify.ReferenceAccumulator`, `verify.DenseAccumulator`.
- Produces: nothing (test-only).

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run it**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dense_oracle.py`
Expected: PASS (5 tests). If any fails, the dense implementation is wrong — fix `DenseAccumulator`, never the reference and never the test's expectations.

- [ ] **Step 3: Verify the test can actually fail (mutation check)**

A differential test that passes against a broken implementation is worthless.
Temporarily break one thing and confirm it is caught:

```bash
cd chaos/orchestrator
# Mutant: miscount pushes. The reference is untouched, so the two must diverge.
sed -i '' 's/^        self\._pushed += 1$/        self._pushed += 2/' verify.py
python3 -m pytest -q test_dense_oracle.py
```

Expected: FAIL. Then revert and confirm green again:

```bash
sed -i '' 's/^        self\._pushed += 2$/        self._pushed += 1/' verify.py
python3 -m pytest -q test_dense_oracle.py
```

Expected: PASS.

Repeat for one more mutation of your choosing — e.g. change `_COUNT_MAX` to
`6` (which must NOT change any result, because overflow is exact) and confirm
the suite still passes. That one is a *negative* check: it proves the overflow
path is exercised and correct rather than merely unused.

- [ ] **Step 4: Commit**

```bash
git add chaos/orchestrator/test_dense_oracle.py
git commit -F - <<'MSG'
test(chaos): prove the dense oracle agrees with the dict one

Randomised streams through both implementations, every VerifyResult field
compared, at four frontier-slack values. Covers delivery-before-ledger,
orphans, conflicts, foreign run_ids and the count-overflow boundary.
MSG
```

---

### Task 7: Swap the implementation and measure

**Files:**
- Modify: `chaos/orchestrator/verify.py` (the `Accumulator` alias and module docstring)

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: `Accumulator is DenseAccumulator`.

- [ ] **Step 1: Write the failing test**

Add to `test_dense_oracle.py`:

```python
class TestActiveImplementation(unittest.TestCase):
    def test_the_harness_runs_the_dense_accumulator(self):
        self.assertIs(verify.Accumulator, verify.DenseAccumulator)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dense_oracle.py -k ActiveImplementation`
Expected: FAIL — `Accumulator` is still `ReferenceAccumulator`.

- [ ] **Step 3: Flip the alias**

In `verify.py`, replace the Task 1 alias with:

```python
#: The accumulator the harness runs. DenseAccumulator is ~336x smaller than the
#: reference (1.00 vs 335.9 bytes/record measured), which is what makes a soak
#: longer than ~39h possible at all. ReferenceAccumulator stays as the
#: differential test's oracle — see test_dense_oracle.py.
Accumulator = DenseAccumulator
```

- [ ] **Step 4: Run the FULL orchestrator suite**

Run: `cd chaos/orchestrator && python3 -m pytest -q`
Expected: PASS — every pre-existing test, now running against the dense
implementation via the alias. This is the real regression gate: `test_verify.py`'s
`TestAccumulator` and `test_run.py`'s episode-loop test both go through it.

- [ ] **Step 5: Measure the real reduction**

```bash
cd chaos/orchestrator && python3 - <<'EOF'
import sys, verify
N = 2_000_000
for cls in (verify.ReferenceAccumulator, verify.DenseAccumulator):
    a = cls(delivered_run_id=7)
    a.ingest([f"{s} 1700000000 t0 256 ACK" for s in range(N)],
             [f"7 {s}" for s in range(N)])
    if cls is verify.DenseAccumulator:
        b = sys.getsizeof(a._cells) + sys.getsizeof(a._overflow)
    else:
        b = sys.getsizeof(a.ledger) + sys.getsizeof(a.delivered_counts)
        b += sum(sys.getsizeof(k) + sys.getsizeof(v) + sum(sys.getsizeof(x) for x in v)
                 for k, v in a.ledger.items())
        b += sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in a.delivered_counts.items())
    print(f"{cls.__name__:22} {b/N:8.2f} bytes/record")
EOF
```

Expected: `DenseAccumulator` at roughly 1 byte/record against `ReferenceAccumulator`'s ~336.

- [ ] **Step 6: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_dense_oracle.py
git commit -F - <<'MSG'
feat(chaos): run the dense oracle

336x less memory per record, so run length stops being a memory question:
the 39h ceiling on beast becomes roughly four months. The dict implementation
stays as the reference the differential test diffs against.
MSG
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| Byte layout, 2-bit tag + 6-bit count | 2 |
| Overflow dict keeps counts exact | 4 |
| Dense array indexed by seq, geometric growth | 3 |
| Incremental unresolved/leaked/no-provenance sets | 3, 4 |
| `check()` O(unresolved) | 5 |
| `check_counts`/`check` untouched | Global constraint; no task modifies them |
| Reference implementation preserved | 1 |
| Randomised differential test, all fields | 6 |
| Equivalence is the acceptance criterion | 6, 7 |
| mmap out of scope | Not planned — correctly absent |

**Placeholder scan:** No TBDs. Every code step carries runnable code; every
test step names the exact command and expected outcome.

**Type consistency:** `_TAG_CODE`/`_TAG_NAME`/`_TAG_MASK`/`_COUNT_SHIFT`/
`_COUNT_MAX`/`_COUNT_OVERFLOW` defined in Task 2, used with those exact names in
3-5. `_grow`/`_tag`/`_count` defined in Task 3, used in 4-5. `_bump_count`/
`_ingest_delivered`/`ingest` defined in Task 4. `_unresolved_acked`/`_leaked`/
`_no_provenance` created in Task 3, mutated in 4, read in 5. `ReferenceAccumulator`
named in Task 1, used in Task 6.

**Known risk, accepted:** Task 6's generator does not model the frontier the way
a real run does (continuous load, monotonic seq). It deliberately generates
harsher input — sparse seqs, conflicts, out-of-order arrival — because the point
is equivalence under *any* input, not realism. A realism-matched generator would
test less.
