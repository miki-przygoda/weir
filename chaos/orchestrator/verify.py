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

Phase 1 treats all tiers alike. Tier- and fault-aware I1 (where Buffered may
lose records under simulated power loss) arrives in Phase 2 with dm-flakey.
"""
import os
from dataclasses import dataclass, field


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
    return s if r == run_id else None


class Accumulator:
    """Accumulated verification state across episodes.

    Holds the ledger outcome per seq and the delivery count per seq, both
    growing monotonically. `check()` runs the same pure invariants as the
    standalone `check()` function on the accumulated state.
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

    def check(self, frontier_slack=0):
        """Runs I1/I2/I3 against everything accumulated so far.

        `frontier_slack` bounds the I1/orphan frontier exemption — see
        `check_counts`. Passed through, not recomputed, so the accumulator
        stays the single source of truth for the ledger high-water seq.
        """
        return check_counts(
            self.ledger, self.delivered_counts, self.delivered_total,
            self.conflicts, frontier_slack, ledger_hwm=self.ledger_hwm,
        )


def check(ledger, delivered, frontier_slack=0):
    """Runs I1, I2 and I3. `ledger` is {seq: (tag, reason)}, `delivered` a list
    of seq values (duplicates intact).

    Convenience wrapper over [`check_counts`] for callers holding a plain list.
    """
    counts = {}
    for s in delivered:
        counts[s] = counts.get(s, 0) + 1
    return check_counts(ledger, counts, len(delivered), [], frontier_slack)


def check_counts(
    ledger, delivered_counts, delivered_total, conflicts=(), frontier_slack=0,
    ledger_hwm=None,
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
    )
