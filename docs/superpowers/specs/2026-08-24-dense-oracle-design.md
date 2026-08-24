# Dense oracle: making soak length a choice rather than a memory limit

## Problem

The chaos verifier holds its entire I1/I2 oracle in RAM. `Accumulator` keeps two
dicts that grow monotonically for the life of a run:

```python
self.ledger = {}            # seq -> (tag, "")
self.delivered_counts = {}  # seq -> times delivered
```

Measured cost, on the actual structures:

```
ledger bytes/rec    = 227.4
delivered bytes/rec = 108.4
TOTAL bytes/record  = 335.9
```

Projected against beast's 31 GiB. The projections below use 1.00 byte/record as the
information-content basis; the measured allocated figure at N=2,000,000 is 1.05 bytes/record,
so real allocation runs about 5% above the table values.

| Run | Records | Verifier RSS |
|---|---|---|
| Run B, 5h | 13.4M | 4.5 GB |
| Run A, 10h | 25.3M | 8.5 GB |
| 24h | ~60.6M | 20.4 GB |
| 48h | ~121.2M | 40.7 GB |
| 72h | ~181.7M | 61.0 GB |

**The ceiling is ~99M records, about 39 hours.** A weekend soak would exhaust
memory around hour 39 — and Run A was already holding 8.5 GB without anyone
noticing.

This is not a data-volume problem. It is a representation problem.

## What is actually stored

Per record, the oracle needs:

- `tag` — exactly one of `ACK` / `NACK` / `UNK` → **2 bits**
- delivery count — almost always 1 → **6 bits is generous**

The `reason` field is parsed and immediately discarded: `parse_ledger_line`
returns `(seq, tag)`, and `Accumulator.ingest` stores `(tag, "")`. So no
information is lost by dropping it — it is already gone.

That is ~1 byte of real information stored in 336 bytes: a dict entry, a boxed
integer key, and a 2-tuple, per record. Roughly **672x overhead**, and the dict
separately stores a key that is nothing but the array index.

## Why an array is the right shape

`seq` comes from one shared counter in the load generator:

```rust
let seq = Arc::new(AtomicU64::new(0));            // loadgen.rs:239
let my_seq = seq.fetch_add(1, Ordering::Relaxed); // loadgen.rs:280
```

Every thread draws from the same `AtomicU64`, so `seq` is dense from 0 with no
gaps. An array indexed by `seq` needs no key storage at all.

### Input contract

That density claim is an assumption about well-formed input, and the array
representation changes what happens when it is violated. Under a dict,
violation was free: an out-of-domain key is just another hash bucket. Under
an array indexed directly by the value, it is not — it is a memory or
correctness hazard.

So the contract is now explicit: **`seq` is a `u64` drawn from a single
monotonic counter — non-negative, and never astronomically larger than the
run's actual record count.** Two things can violate it in practice, both from
corrupt input rather than legitimate traffic:

- **A negative `seq`.** `int("-1")` parses successfully even though the Rust
  encoder only ever emits a `u64` (`chaos/src/lib.rs`'s `Record::seq`), so a
  negative value is definitionally corruption. Left unchecked, Python's
  negative-index semantics make it alias onto a real cell from the end of the
  array instead of erroring — silently corrupting that cell's tag/count and
  potentially hiding a genuine I1/I2 violation on the record that legitimately
  owns it.
- **An absurdly large `seq`.** A spliced or truncated log line (e.g. a
  `kill -9` mid-`write_all`, which `chaos/src/lib.rs` already anticipates)
  can parse to a seq many orders of magnitude past the ledger's high-water
  mark. The array's cost is proportional to the largest seq it has seen, so
  this becomes an allocation proportional to a corrupt number — multi-GB at
  intermediate magnitudes, an instant `MemoryError` at extreme ones.

Both are enforced at the boundary rather than inside the array logic: the
parsers (`parse_ledger_line`, `parse_delivered_line`) reject a negative `seq`
outright, and `DenseAccumulator._grow` refuses a `seq` past a fixed ceiling
(`_MAX_SEQ = 1 << 40`, far above any observed run) rather than allocating
proportionally to it. Both failure modes drop the offending line the same way
for both accumulators, which is what keeps equivalence intact — this is a
domain restriction on the shared input, not a divergence between the two
implementations.

## Design

One `bytearray` indexed by `seq`, one byte per record:

```
bits 0-1 : tag    0=absent 1=ACK 2=NACK 3=UNK
bits 2-7 : delivery count, 0..62 literal, 63 = "consult overflow dict"
```

Measured: **1.05 bytes/record (at N=2,000,000), a 302x reduction** vs 316.89 bytes/record
(also at N=2,000,000). Earlier measurement at N=200,000 showed 1.00 vs 335.9 (336x);
the ratio varies with sample size due to Python dict table sizing and bytearray doubling.
72h becomes ~191 MB; 30 days becomes ~1.9 GB. Run length stops being a memory question.

Counts above 62 spill to a small `{seq: true_count}` dict so totals stay
exact. `delivered_total` is already a separate running integer, so only the
per-seq count needs the overflow path, and only a pathological redelivery
would ever take it.

### The second win: `check()` stops being O(all history)

`check_counts` currently rebuilds four full Python sets on **every episode**:

```python
delivered_set = set(delivered_counts)
acked   = {s for s, (tag, _) in ledger.items() if tag == "ACK"}
nacked  = {s for s, (tag, _) in ledger.items() if tag == "NACK"}
unknown = {s for s, (tag, _) in ledger.items() if tag == "UNK"}
```

By the end of Run A that is four 25M-element set constructions per episode,
438 times.

Every one of those sets is derivable incrementally, because each is defined by
a state transition already visible at ingest time. Critically, the three sets
the invariants actually need are the **unresolved working set**, which stays
small:

- `_unresolved_acked` — acked with count 0. Enters on ack-before-delivery,
  leaves on first delivery. This is `i1_absent`.
- `_leaked` — nacked with count > 0. Enters on either order. Should stay empty.
  This is `i2_leaked`.
- `_no_provenance` — delivered with tag absent. Enters on delivery-before-ledger,
  leaves when a tag arrives.

Maintaining these on ingest makes `check()` O(unresolved) instead of
O(everything), while the byte array carries the resolved history that only the
final totals need.

## Why not spill to SSD

Considered and rejected as the first move, for two reasons:

1. It optimises the wrong layer — paying I/O to store 61 GB of bits that
   compress to 182 MB of information.
2. **The instrument would perturb the experiment.** The soak workload is
   fsync-latency-bound, not compute-bound (851 acked/s x 256 B = 213 KiB/s).
   A key-value store doing random I/O on the same host, likely the same disk as
   the loop file, injects exactly the latency the harness exists to measure.

Once the representation is dense, `mmap`-ing the array to a file becomes cheap
and gives restart survival plus automatic page-cache residency. That is the
right place for a storage layer, and it is deliberately **out of scope here** —
it is a follow-on, valuable only after the representation is fixed.

For the record, the verifier's footprint does *not* explain the unexplained
4.5% throughput gap between Runs A and B: if page-cache starvation were the
cause, Run A's Q1 (~2 GB held) should have matched Run B's, but Run A sat at
843/s in Q1 and 851/s in Q4 — slightly faster as memory grew. Within-run
flatness rules that hypothesis out.

## The constraint that governs everything

**This is the oracle.** If it breaks, every future "0 violations" is worthless,
and wrong in the dangerous direction: a broken oracle reports success. So the
dense implementation does not replace the dict one on the strength of unit
tests. It must **prove** it agrees with it:

- `check_counts()` and `check()` stay exactly as they are — pure, dict-based,
  and now serving as the reference implementation.
- The current `Accumulator` is preserved as `ReferenceAccumulator`.
- A randomised differential test drives both with the same event streams and
  asserts every field of `VerifyResult` matches, across orderings, duplicates,
  orphans, conflicts, and frontier-slack values.

Equivalence is the acceptance criterion. Performance is the motivation, not
the bar.

## Out of scope

- `mmap` / on-disk backing (follow-on). Note for that follow-on: `_MAX_SEQ`
  (the `_grow` ceiling that refuses to allocate proportionally to a corrupt
  seq — see Input contract, above) stops being a nice-to-have and becomes
  load-bearing the day `_cells` is file-backed via `mmap`. A sparse file
  would hide an absurd allocation until the corresponding page is actually
  touched, instead of failing fast the way an in-memory `bytearray.extend`
  does today.
- Any change to the invariant semantics. This is a representation change and
  must be observably behaviour-preserving.
- Any change to the ledger or delivery log formats.
