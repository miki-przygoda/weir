"""The oracle: three invariants checked against the ledger and delivery log.

I1 - every Acked record was delivered. Set CONTAINMENT, not equality:
     at-least-once delivery makes duplicates conformant. The duplicate rate is
     measured and reported, because "your sink must dedupe" is documented but
     "how much redelivery a crash costs" is not.

I2 - no Nacked record was ever delivered. A record weir refused must not
     silently appear downstream.

I3 - Unknown records are unconstrained but counted. Either outcome conforms.
     An oracle that quietly reclassifies its awkward cases is not an oracle.

`acked` and `nacked` are BOTH vacuous when empty: `nacked_count` and `pushed`
(every ledger entry, regardless of outcome) exist so a run where weir refuses
or delivers nothing cannot silently read as clean.

Continuous load means there is always in-flight work at check time: records
acked but not yet ledger-flushed look like orphans, and records
ledger-flushed but not yet delivered look like I1 violations. `check_counts`'s
`frontier_slack` bounds this: seqs above `ledger_hwm - frontier_slack` are
exempted rather than failed (`i1_exempt`, `pending_provenance`), and both
exemption counts are reported rather than silently absorbed.

Phase 1 treats all tiers alike (`tier`/`fault` omitted, or any combination
other than Buffered+power_loss). Phase 2 adds one tier- and fault-aware I1
exemption: a Buffered ack, `tier="U"`, is permitted to go missing ONLY under
simulated power loss, `fault="power_loss"` — never under `kill_random`, since
`kill -9` does not lose the page cache. The lost count is reported as
`expected_loss`, not silently dropped.
"""
import os
from dataclasses import dataclass, field

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
#: Highest delivery count stored literally in the cell. This value is a TUNABLE:
#: any value whose sentinel still fits a byte (_COUNT_OVERFLOW << 2 <= 255) gives
#: identical results, because _bump_count spills to _overflow at whatever ceiling
#: is set and _count reads it back exactly. Verified by mutating it to 6 and to 61
#: with no change to any VerifyResult field. Do not "correct" it in either
#: direction without changing _COUNT_OVERFLOW in lockstep.
_COUNT_MAX = 62
#: Sentinel meaning "the true count lives in the overflow dict". Counts this
#: high need a crash loop redelivering one record 63 times; the dict keeps the
#: total exact if that ever happens rather than silently saturating.
_COUNT_OVERFLOW = 63

#: A seq this far past the ledger high-water mark is not a real record; it is a
#: spliced or stale log line — e.g. a kill -9 mid-write_all running two 8-digit
#: seqs together into one that still parses cleanly (chaos/src/lib.rs:294 notes
#: the same hazard on the Rust decoder's side). Refuse rather than allocating an
#: array proportional to it: 1 << 40 is ~1.1e12, far above the largest real run
#: observed (~2.5e7), so this cannot fire on legitimate traffic. Becomes
#: load-bearing, not just a safety margin, once _cells is file-backed via mmap
#: (see the design spec's Out of scope) — a sparse file would hide the
#: allocation until the page is touched.
_MAX_SEQ = 1 << 40

_TAG_CODE = {"ACK": _TAG_ACK, "NACK": _TAG_NACK, "UNK": _TAG_UNK}
_TAG_NAME = {v: k for k, v in _TAG_CODE.items()}


@dataclass
class VerifyResult:
    """Outcome of one episode's verification."""
    ok: bool
    i1_missing: list = field(default_factory=list)
    i2_leaked: list = field(default_factory=list)
    unknown_count: int = 0
    acked_count: int = 0
    delivered_distinct: int = 0
    duplicate_rate: float = 0.0
    #: Every ledger entry tagged NACK. `acked_count` alone cannot distinguish
    #: "weir refuses or delivers nothing" from a healthy run — both read as
    #: I1=0, I2=0 when acked is empty (I1/I2 are vacuously true on an empty
    #: set). Surfacing this alongside `pushed` is what makes a stalled or
    #: fully-refusing run visible instead of a silent 20/20 pass.
    nacked_count: int = 0
    #: Every ledger entry ingested, whatever its outcome (ACK/NACK/UNK) — the
    #: denominator against which acked_count/nacked_count are judged. Without
    #: it, acked_count == 0 is indistinguishable from "loadgen pushed nothing".
    pushed: int = 0
    #: Delivered seqs with NO ledger provenance — the sink saw something the
    #: producer never recorded pushing. Not a durability violation (more likely
    #: a stale log from a previous run of the same seed, since run_id derives
    #: from it), but they must not be folded into the duplicate rate as though
    #: they were redeliveries of something real.
    orphaned_delivered: list = field(default_factory=list)
    #: Seqs the ledger reports under two different tags. Ledger corruption:
    #: verification against corrupt input is meaningless, so this fails the
    #: episode — but it is reported distinctly so it is never mistaken for a
    #: durability violation by weir.
    ledger_conflicts: list = field(default_factory=list)
    #: Acked seqs above the frontier (`ledger_hwm - frontier_slack`) exempted
    #: from I1 because they may simply not have been delivered yet — continuous
    #: load means there is always in-flight work at check time. Reported
    #: separately so the exemption is visible, not a second silent distortion
    #: replacing the one it fixes.
    i1_exempt: int = 0
    #: Delivered seqs above the frontier: ahead of the ledger's flush point,
    #: so NOT counted as orphans (their provenance may simply not have been
    #: flushed to the ledger yet).
    pending_provenance: int = 0
    #: Acked seqs never delivered, EXEMPTED from I1 because this run is
    #: Buffered under simulated power loss — Buffered acks after the
    #: in-memory write, before any fsync, so power loss may legitimately eat
    #: an acked record. Counted and reported, never silently discarded: the
    #: whole point of Phase 2 is measuring how much, not waving it away.
    #: Zero for every Phase 1 caller (tier/fault omitted), and zero for any
    #: other tier/fault combination — see `check_counts`.
    expected_loss: int = 0

    def summary(self):
        extra = ""
        if self.orphaned_delivered:
            extra += f" orphaned={len(self.orphaned_delivered)}"
        if self.ledger_conflicts:
            extra += f" LEDGER_CONFLICTS={len(self.ledger_conflicts)}"
        if self.i1_exempt:
            extra += f" i1_exempt={self.i1_exempt}"
        if self.pending_provenance:
            extra += f" pending_provenance={self.pending_provenance}"
        if self.ok:
            return (
                f"PASS  pushed={self.pushed} acked={self.acked_count} "
                f"nacked={self.nacked_count} distinct_delivered="
                f"{self.delivered_distinct} dup_rate={self.duplicate_rate:.3f} "
                f"unknown={self.unknown_count}{extra}"
            )
        return (
            f"FAIL  I1_missing={len(self.i1_missing)} I2_leaked={len(self.i2_leaked)} "
            f"pushed={self.pushed} acked={self.acked_count} nacked={self.nacked_count} "
            f"unknown={self.unknown_count}{extra}"
        )


class LogTailer:
    """Reads only what has been appended to a file since the last call.

    Verification runs after every episode, so re-reading the whole log each
    time is O(n^2) — millions of records re-parsed twenty times. This reads
    each byte exactly once.

    A trailing line without a newline is WITHHELD and re-read next time: the
    writer is mid-append, and consuming a truncated record would corrupt the
    oracle. A missing file yields nothing rather than raising, because the
    recorder may not have received its first batch yet.
    """

    def __init__(self, path):
        self.path = path
        self.offset = 0

    def read_new(self):
        try:
            size = os.path.getsize(self.path)
        except FileNotFoundError:
            return []
        if size < self.offset:
            # The log shrank. Seeking past a shrunken file's EOF returns b""
            # indefinitely, so every line written after the truncation would be
            # skipped SILENTLY — the oracle losing evidence without saying so,
            # which is the one failure mode worse than a false alarm. These logs
            # are append-only for the life of one run, so this can only mean the
            # file was truncated, rotated or recreated. Refuse rather than guess.
            raise RuntimeError(
                f"{self.path} shrank from {self.offset} to {size} bytes. This log is "
                "append-only, so it was truncated, rotated or recreated mid-run. "
                "Refusing to continue rather than silently skipping records."
            )
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = f.read()
        if not chunk:
            return []
        # Keep only up to the last complete line; leave the remainder for later.
        cut = chunk.rfind(b"\n")
        if cut == -1:
            return []
        complete, self.offset = chunk[: cut + 1], self.offset + cut + 1
        # split("\n"), NOT splitlines(): the latter also splits on \x0b, \x0c,
        # \x1c-\x1e and U+2028/9, none of which the Rust side escapes — so a form
        # feed inside a sink error message would fabricate an extra line.
        text = complete.decode("utf-8", errors="replace")
        return [ln for ln in text.split("\n") if ln]


def parse_ledger_line(line):
    """Parses one ledger line into (seq, tag). Returns None if malformed.

    Strict, mirroring the Rust decoder (`LedgerEntry::from_line`): the sixth
    field is present iff the tag is `NACK`. The two implementations parse the
    same contract, so a line one accepts and the other rejects is a latent
    divergence — and for an oracle, wrongly accepting a corrupt line is worse
    than rejecting it.
    """
    parts = line.rstrip("\n").split(" ", 5)
    if len(parts) < 5:
        return None
    try:
        seq = int(parts[0])
    except ValueError:
        return None
    if seq < 0:
        # The Rust encoder emits seq as a u64, so a negative value is
        # corruption, never a legitimate record — and unlike under the old
        # dict accumulator, accepting it here is not merely a wasted slot.
        # DenseAccumulator indexes self._cells directly by seq, and Python
        # silently reinterprets a negative index as counting from the end of
        # the array — so a negative seq would ALIAS onto a real cell instead
        # of erroring, corrupting that cell's tag/count and potentially
        # hiding a genuine I1/I2 violation on the record that legitimately
        # owns it. Rejecting here keeps both accumulators' behaviour
        # identical and restores agreement with the Rust `u64` contract.
        return None
    tag = parts[4]
    has_reason = len(parts) > 5
    if tag == "NACK":
        # `to_line` always emits a reason field for NACK, even an empty one.
        return (seq, tag) if has_reason else None
    if tag in ("ACK", "UNK"):
        # Neither carries a reason; trailing content means corruption.
        return None if has_reason else (seq, tag)
    return None


def parse_delivered_line(line, run_id):
    """Parses one delivery line, keeping it only if it belongs to `run_id`."""
    parts = line.split()
    if len(parts) != 2:
        return None
    try:
        r, s = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if r < 0 or s < 0:
        # Same hazard as parse_ledger_line: `s` indexes DenseAccumulator's
        # cell array directly, so a negative value would silently alias a
        # real cell (Python's negative-index semantics) rather than error,
        # instead of being the harmless wasted dict slot it was before. Both
        # a negative run_id and a negative seq are impossible under the Rust
        # u64 contract, so either one is corruption; reject the line rather
        # than let it corrupt an unrelated cell.
        return None
    return s if r == run_id else None


class ReferenceAccumulator:
    """Accumulated verification state across episodes — dict-based reference.

    Retained as the differential test's oracle-for-the-oracle: the dense
    implementation must prove it agrees with this before replacing it. Do not
    optimise this class. Its value is that it is obviously correct and never
    changes.
    """

    def __init__(self, delivered_run_id):
        self.run_id = delivered_run_id
        self.ledger = {}
        self.delivered_counts = {}
        # Running total, maintained incrementally. Materialising the full
        # delivery list on every check() would be O(everything delivered so
        # far) per episode — reintroducing, in memory, the same accumulating
        # cost LogTailer exists to eliminate on disk.
        self.delivered_total = 0
        self.conflicts = []
        # The ledger high-water seq: the largest seq ever ingested, maintained
        # incrementally for the same reason delivered_total is — recomputing
        # max(self.ledger) from scratch every check() would rescan everything
        # accumulated so far. This is the frontier's basis: continuous load
        # means there is always in-flight work at check time, and the frontier
        # is how far the ledger can be trusted to have caught up.
        self.ledger_hwm = 0

    def ingest(self, ledger_lines, delivered_lines):
        """Folds newly-read lines into the accumulated state."""
        for line in ledger_lines:
            parsed = parse_ledger_line(line)
            if not parsed:
                continue
            seq, tag = parsed
            self.ledger_hwm = max(self.ledger_hwm, seq)
            prior = self.ledger.get(seq)
            if prior is not None and prior[0] != tag:
                # Two different tags for one seq. loadgen allocates seq from a
                # single monotonic counter, so no legitimate retry reuses one —
                # this means corruption or cross-run pollution. Keep the FIRST
                # observation and record the conflict; silently overwriting
                # would reclassify an outcome, which this module's own contract
                # forbids.
                self.conflicts.append((seq, prior[0], tag))
                continue
            self.ledger[seq] = (tag, "")
        for line in delivered_lines:
            seq = parse_delivered_line(line, self.run_id)
            if seq is not None:
                self.delivered_counts[seq] = self.delivered_counts.get(seq, 0) + 1
                self.delivered_total += 1

    def check(self, frontier_slack=0, tier=None, fault=None):
        """Runs I1/I2/I3 against everything accumulated so far.

        `frontier_slack` bounds the I1/orphan frontier exemption — see
        `check_counts`. Passed through, not recomputed, so the accumulator
        stays the single source of truth for the ledger high-water seq.
        `tier`/`fault` are forwarded untouched, also to `check_counts`.
        """
        return check_counts(
            self.ledger, self.delivered_counts, self.delivered_total,
            self.conflicts, frontier_slack, ledger_hwm=self.ledger_hwm,
            tier=tier, fault=fault,
        )


class DenseAccumulator:
    """Accumulated verification state, one byte per seq.

    Same contract as `ReferenceAccumulator`, ~302x smaller (measured at N=2,000,000;
    see the alias comment). `seq` comes from a single shared AtomicU64 in loadgen,
    so it is dense from 0 and an array indexed by it needs no key storage.

    Input contract: `seq` is a `u64` from that single monotonic counter — the
    array representation makes an out-of-domain `seq` a memory or correctness
    hazard rather than a wasted dict slot (see the design spec's Input
    contract section). The parsers enforce non-negativity, and `_grow`
    refuses a `seq` past `_MAX_SEQ` rather than allocating proportionally to
    it.

    The three sets below are the UNRESOLVED working set, not history: each is
    defined by a state transition visible at ingest time, and each empties as
    records resolve. Maintaining them here is what makes `check()`
    O(unresolved + leaked + conflicts) instead of O(everything ever seen) —
    the last two are monotonic (`sorted(self._leaked)`, `list(self.conflicts)`)
    but empty in a conforming run, same as the reference, so this is not a
    regression.

    `ingest()` processes all of a batch's ledger lines before any of its
    delivery lines, so `_unresolved_acked` transiently holds one whole
    episode's ledger batch mid-call (measured: 58,000 entries / 2.1 MB at Run
    A's rate) before the delivery lines resolve most of it back down. It
    self-heals within the same call, but the transient scales with episode
    duration, not record count overall — worth knowing before assuming this
    set is always small.
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
        if seq > _MAX_SEQ:
            raise RuntimeError(
                f"seq {seq} exceeds {_MAX_SEQ}; this log line is spliced or "
                "from another run. Refusing to allocate an array proportional to "
                "a corrupt seq."
            )
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
        # prior is ABSENT (0) here, so the tag bits are clear — OR the code straight in.
        self._cells[seq] |= code
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

    def check(self, frontier_slack=0, tier=None, fault=None):
        """Runs I1/I2/I3 against everything accumulated so far.

        Mirrors `check_counts` exactly. The difference is only where the sets
        come from: maintained incrementally here, rebuilt from the whole ledger
        there. `tier`/`fault` gate the same Buffered-under-power_loss I1
        exemption — see `check_counts`.
        """
        if frontier_slack:
            frontier = self.ledger_hwm - frontier_slack
            i1_exempt_seqs = {s for s in self._unresolved_acked if s > frontier}
            pending_provenance_seqs = {s for s in self._no_provenance if s > frontier}
        else:
            i1_exempt_seqs = set()
            pending_provenance_seqs = set()

        buffered_powerloss = (tier == "U" and fault == "power_loss")
        if buffered_powerloss:
            expected_loss = len(self._unresolved_acked - i1_exempt_seqs)
            i1_missing = []
        else:
            expected_loss = 0
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
            expected_loss=expected_loss,
        )


#: The accumulator the harness runs. DenseAccumulator is ~302x smaller than the
#: reference — 1.05 vs 316.89 bytes/record measured at N=2,000,000. The ratio is
#: sample-size sensitive (Python dict table sizing and bytearray doubling both
#: shift with N), so re-measuring at a different N and getting a different ratio
#: is expected, not a regression. This is what makes a soak longer than ~39h
#: possible at all. ReferenceAccumulator stays as the differential test's oracle
#: — see test_dense_oracle.py.
#:
#: Methodology, so a future re-measurement doesn't read as a third correction to
#: this comment: the 1.05 B/rec figure covers `_cells` + `_overflow` only, the
#: resolved history. The three UNRESOLVED sets (`_unresolved_acked`, `_leaked`,
#: `_no_provenance`) are not included — they add roughly another 0.3 B/rec, and
#: are bounded by one episode's ingest batch rather than by total record count
#: (see `DenseAccumulator`'s docstring), so they don't change the asymptotic
#: picture, only the constant.
Accumulator = DenseAccumulator


def check(ledger, delivered, frontier_slack=0, tier=None, fault=None):
    """Runs I1, I2 and I3. `ledger` is {seq: (tag, reason)}, `delivered` a list
    of seq values (duplicates intact).

    Convenience wrapper over [`check_counts`] for callers holding a plain list.
    `tier`/`fault` are forwarded untouched — see `check_counts`.
    """
    counts = {}
    for s in delivered:
        counts[s] = counts.get(s, 0) + 1
    return check_counts(
        ledger, counts, len(delivered), [], frontier_slack, tier=tier, fault=fault,
    )


def check_counts(
    ledger, delivered_counts, delivered_total, conflicts=(), frontier_slack=0,
    ledger_hwm=None, tier=None, fault=None,
):
    """The invariant core. `delivered_counts` is {seq: times_delivered}.

    Takes counts rather than a list so an accumulating caller never has to
    rebuild the whole delivery history to re-check.

    `frontier_slack` bounds the frontier exemption: continuous load means
    there is always in-flight work at check time, so records right at the
    edge of the ledger's coverage cannot be judged yet either way. It defaults
    to 0, which disables the exemption entirely (not "a frontier of exactly
    ledger_hwm") — every existing caller that never heard of a frontier keeps
    its exact prior behaviour.

    When non-zero, `frontier = ledger_hwm - frontier_slack`, where `ledger_hwm`
    is the ledger's high-water seq (the caller's, if it tracks one
    incrementally — e.g. `Accumulator` — else derived here from `ledger`
    itself). Acked seqs above the frontier are exempt from I1 (they may
    simply not have been delivered yet, not lost); delivered seqs above it are
    "pending provenance" rather than orphans (the ledger may simply not have
    flushed that far yet). Both exemptions are counted and reported, never
    silently dropped — replacing one silent distortion with another would
    defeat the purpose.

    `tier`/`fault` gate a SEPARATE exemption, tier-aware I1: Buffered acks
    after the in-memory write, before any fsync, so power loss may
    legitimately eat an acked record — that is its documented contract, not a
    defect. The exemption is keyed on tier AND fault, never tier alone: kill
    -9 does not lose the page cache, so a Buffered ack must still survive it.
    Both default to `None`, under which this is exactly Phase 1 behaviour —
    every existing caller that never heard of a tier or fault keeps its exact
    prior result, `expected_loss` included (always 0).
    """
    delivered_set = set(delivered_counts)

    acked = {s for s, (tag, _) in ledger.items() if tag == "ACK"}
    nacked = {s for s, (tag, _) in ledger.items() if tag == "NACK"}
    unknown = {s for s, (tag, _) in ledger.items() if tag == "UNK"}

    # I1 is set containment, not equality: at-least-once delivery makes
    # duplicates conformant.
    i1_absent = acked - delivered_set
    # Delivered with no ledger provenance (yet). Excluded from the duplicate
    # rate rather than absorbed into it: the rate is a headline deliverable
    # ("how much redelivery does a crash cost"), and a record that is not a
    # redelivery of anything real would quietly distort it.
    no_provenance = delivered_set - acked - nacked - unknown

    if frontier_slack:
        if ledger_hwm is None:
            ledger_hwm = max(ledger) if ledger else 0
        frontier = ledger_hwm - frontier_slack
        # Above the frontier, "missing" is exempted rather than failed — it
        # may simply not have arrived yet.
        i1_exempt_seqs = {s for s in i1_absent if s > frontier}
        # Above the frontier, "no provenance" is "pending", not an orphan —
        # the ledger may simply not have flushed that far yet.
        pending_provenance_seqs = {s for s in no_provenance if s > frontier}
    else:
        i1_exempt_seqs = set()
        pending_provenance_seqs = set()

    # Buffered acks after the in-memory write, before any fsync, so power loss
    # may legitimately eat an acked record. That exemption is keyed on tier
    # AND fault, never tier alone: `kill -9` does not lose the page cache, so
    # a Buffered ack must survive it, and that is the Phase 1 contract this
    # phase must not weaken.
    buffered_powerloss = (tier == "U" and fault == "power_loss")
    if buffered_powerloss:
        expected_loss = len(i1_absent - i1_exempt_seqs)
        i1_missing = []
    else:
        expected_loss = 0
        i1_missing = sorted(i1_absent - i1_exempt_seqs)
    i2_leaked = sorted(nacked & delivered_set)
    orphaned = sorted(no_provenance - pending_provenance_seqs)

    # The exclusion set for the duplicate rate is always the full
    # no_provenance set — pending-provenance seqs are still unconfirmed as a
    # redelivery of anything real, exactly like an orphan, so the frontier
    # only changes which LABEL an excluded seq gets, never the rate itself.
    known_total = delivered_total - sum(delivered_counts[s] for s in no_provenance)
    known_distinct = len(delivered_set) - len(no_provenance)
    dup_rate = (known_total / known_distinct) if known_distinct else 0.0

    return VerifyResult(
        # A ledger conflict fails the episode: verification against corrupt
        # input is meaningless. Reported separately from I1/I2 so it is never
        # read as a durability violation by weir.
        ok=not i1_missing and not i2_leaked and not conflicts,
        i1_missing=i1_missing,
        i2_leaked=i2_leaked,
        unknown_count=len(unknown),
        acked_count=len(acked),
        nacked_count=len(nacked),
        pushed=len(ledger),
        delivered_distinct=known_distinct,
        duplicate_rate=dup_rate,
        orphaned_delivered=orphaned,
        ledger_conflicts=list(conflicts),
        i1_exempt=len(i1_exempt_seqs),
        pending_provenance=len(pending_provenance_seqs),
        expected_loss=expected_loss,
    )
