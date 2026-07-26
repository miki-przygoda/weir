"""The oracle: three invariants checked against the ledger and delivery log.

I1 - every Acked record was delivered. Set CONTAINMENT, not equality:
     at-least-once delivery makes duplicates conformant. The duplicate rate is
     measured and reported, because "your sink must dedupe" is documented but
     "how much redelivery a crash costs" is not.

I2 - no Nacked record was ever delivered. A record weir refused must not
     silently appear downstream.

I3 - Unknown records are unconstrained but counted. Either outcome conforms.
     An oracle that quietly reclassifies its awkward cases is not an oracle.

Phase 1 treats all tiers alike. Tier- and fault-aware I1 (where Buffered may
lose records under simulated power loss) arrives in Phase 2 with dm-flakey.
"""
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

    def summary(self):
        if self.ok:
            return (
                f"PASS  acked={self.acked_count} distinct_delivered="
                f"{self.delivered_distinct} dup_rate={self.duplicate_rate:.3f} "
                f"unknown={self.unknown_count}"
            )
        return (
            f"FAIL  I1_missing={len(self.i1_missing)} I2_leaked={len(self.i2_leaked)} "
            f"acked={self.acked_count} unknown={self.unknown_count}"
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
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                chunk = f.read()
        except FileNotFoundError:
            return []
        if not chunk:
            return []
        # Keep only up to the last complete line; leave the remainder for later.
        cut = chunk.rfind(b"\n")
        if cut == -1:
            return []
        complete, self.offset = chunk[: cut + 1], self.offset + cut + 1
        return complete.decode("utf-8", errors="replace").splitlines()


def parse_ledger_line(line):
    """Parses one ledger line into (seq, tag). Returns None if malformed."""
    parts = line.rstrip("\n").split(" ", 5)
    if len(parts) < 5:
        return None
    try:
        seq = int(parts[0])
    except ValueError:
        return None
    return seq, parts[4]


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

    def ingest(self, ledger_lines, delivered_lines):
        """Folds newly-read lines into the accumulated state."""
        for line in ledger_lines:
            parsed = parse_ledger_line(line)
            if parsed:
                seq, tag = parsed
                self.ledger[seq] = (tag, "")
        for line in delivered_lines:
            seq = parse_delivered_line(line, self.run_id)
            if seq is not None:
                self.delivered_counts[seq] = self.delivered_counts.get(seq, 0) + 1

    def check(self):
        """Runs I1/I2/I3 against everything accumulated so far."""
        delivered = []
        for seq, count in self.delivered_counts.items():
            delivered.extend([seq] * count)
        return check(self.ledger, delivered)


def check(ledger, delivered):
    """Runs I1, I2 and I3. `ledger` is {seq: (tag, reason)}, `delivered` a list
    of seq values (duplicates intact)."""
    delivered_set = set(delivered)

    acked = {s for s, (tag, _) in ledger.items() if tag == "ACK"}
    nacked = {s for s, (tag, _) in ledger.items() if tag == "NACK"}
    unknown = {s for s, (tag, _) in ledger.items() if tag == "UNK"}

    i1_missing = sorted(acked - delivered_set)
    i2_leaked = sorted(nacked & delivered_set)

    distinct = len(delivered_set)
    dup_rate = (len(delivered) / distinct) if distinct else 0.0

    return VerifyResult(
        ok=not i1_missing and not i2_leaked,
        i1_missing=i1_missing,
        i2_leaked=i2_leaked,
        unknown_count=len(unknown),
        acked_count=len(acked),
        delivered_distinct=distinct,
        duplicate_rate=dup_rate,
    )
