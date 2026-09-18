# A6 — wire-level batching

**Status:** design, not approved. **Date:** 2026-09-13. **Target:** 4.0.0.

Verified by a four-agent adversarial sweep against `main @ f3e685e`, briefed to
falsify rather than confirm. Where a claim survived, the evidence is cited.
Where one did not, it is marked and corrected in place — several carried
forward from earlier work and were wrong.

---

## 1. What A6 is, and the question that precedes it

A producer sending N records pays N socket round trips and, at the `Durable`
tier, participates in a group fsync it cannot influence. A6 proposes a
`PushBatch` frame carrying N records, answered once.

**The premise is unmeasured.** A6's case is that real deployments do *not*
already coalesce records per fsync. Nothing in the tree observes that:

```
$ grep -rn "group_commit\|records_per_fsync\|pending_acks.len()" crates/weir-server/src/
(no output)
```

`wab/mod.rs` resolves `pending_acks` after one group fsync; observing
`pending_acks.len()` there is one line. **That line should be written and run
before this spec is implemented**, because it is the only cheap experiment that
can return "no". If deployments already coalesce near `batch_size`, A6's central
claim is gone.

What is known: coalescing begins only once connections-per-shard exceeds 1
(measured 0.97–0.99 records/fsync at 1, 2 and 4 connections across 4 shards),
and `shard_count` **defaults to 1** (`config/mod.rs:635`) — the `4` quoted
throughout earlier notes is `bench_preset`, not a default. So a single-producer
deployment coalesces nothing today, which favours A6. Whether multi-producer
deployments already saturate is unknown.

## 2. The alternative this spec must beat

`docs/wire_protocol.md:356-363` already says:

> A client **may** write multiple Push frames back-to-back without waiting for
> each Ack (pipelining at the kernel level), but the server still processes them
> in order and emits one response per request, in submission order.

**The wire already permits the thing A6 wants.** The constraint is server-side:
`handle_connection` awaits `handle_push` before reading the next frame
(`connection.rs:376-388`), and `handle_push` awaits the ack oneshot
(`connection.rs:506`). One record per connection is in flight, so nothing
accumulates for the group fsync to amortise.

A server-side pipelined read ("L1") is costed elsewhere at 2–3 weeks for a
claimed 8–30× on `Durable`, with **no wire change, no client change, and all
five polyglot clients benefiting on day one**. A6 is 3–5 weeks, a new message
type, a new response shape, and five client implementations.

Two honest arguments for A6 over L1:

- **Coalescing depth.** A6 reaches `min(N, batch_size)` records per fsync from
  one connection. L1's depth is a window `W` bounded by
  `max_connections × W × max_payload_bytes`, so large `W` is not reachable.
- **Invariant surface.** L1 rewrites the socket-side half of the crown invariant
  across five `send_nack(...); return` paths, any one of which yields a false
  ack. A6 preserves "one frame in flight per connection".

One honest argument against A6 that L1 does not share, and it is the strongest
objection in this document — see §7.1.

**This spec does not settle A6 vs L1.** It specifies A6 well enough to compare
them on equal footing. The comparison is a decision for the maintainer.

## 3. Wire format

### 3.1 Additivity

`PushBatch = 0x08`, `AckBatch = 0x09`. `WIRE_VERSION` stays **1**.
`docs/wire_protocol.md:609-622` reserves message-type bytes `0x08`–`0xFF` for
exactly this, and 3.0 set the precedent with `PushTracked`/`AckTracked`.

Verified: byte 5 of all 30 frozen vectors is one of `0x01`–`0x05` or `0xFF`.
Both vector files regenerate byte-identical from their generators. All six
conformance suites pass at baseline.

**`0xFF` is permanently unassignable** — `reject_unknown_message_type` pins it,
and flipping that vector's expectation to `ok` was demonstrated to fail the
suite (`29/30`, all six implementations read the same file). `0xFF` is also
pinned in the *durability* namespace by two further vectors. `0x08`/`0x09` are
genuinely clear.

**But "additive" is false one layer up.** `MessageType` is `pub` and not
`#[non_exhaustive]` (`envelope.rs:35-37`), so adding variants is a **semver-major
break of the published Rust API** with zero wire bytes changed — the same reason
3.0 was a major. `nack.rs:18-20` records the now-obsolete rationale
("changing them requires a `WIRE_VERSION` bump"), which 3.0 already disproved.

**Decision (maintainer, 2026-09-12):** this release makes `MessageType`
`#[non_exhaustive]`, so 4.0.0 is the last major a new message type ever forces.
`Durability` is a separate call and is **left exhaustive**: `0x02` is retired, a
downstream `match` on a config string is a legitimate exhaustive use, and
closing it buys nothing.

**Guard gap:** `the_frozen_v1_vectors_contain_no_tracked_frame` asserts only
`mt != 0x06 && mt != 0x07`. It must be widened, or the freeze rests on
convention.

### 3.2 `PushBatch` body

```
Offset  Size  Field           Notes
 0       1    batch_version   0x01. Leads, so the body can grow inside wire v1.
 1       2    record_count    u16 LE, 1..=effective cap
 3     var    entries         record_count × { u32 LE record_len (>=1); record_len bytes }
```

Validation order, **after** the frame's payload CRC passes and before any
allocation keyed off a peer-chosen number:

1. `body.len() >= 3`, else `BadBatchFraming`
2. `batch_version == 0x01`, else `BadBatchFraming` — an unknown version is a
   rejection, never a prefix-parse
3. `record_count != 0`, else `BadBatchFraming`
4. `record_count <= min(max_batch_records, MAX_BATCH_RECORDS_HARD_CAP)`, else
   `Nack(PayloadTooLarge)` — **before** any `Vec::with_capacity`
5. walk entries: `record_len == 0` → `Nack(EmptyPayload)`;
   `record_len > max_payload_bytes` → `Nack(PayloadTooLarge)`; truncation →
   `BadBatchFraming`
6. cursor must equal `body.len()` **exactly**, else `BadBatchFraming`
7. the walked count must equal the declared `record_count`, else
   `BadBatchFraming`

Step 7 exists because an earlier draft carried a contradiction: a "16 MiB body →
3.35 million records" hazard is only reachable if the parser *ignores*
`record_count`, which a `u16` caps at 65,535. Declared and actual must agree, and
disagreement is a rejection rather than a choice of which to believe.

**The CRC is not the safety argument.** An earlier draft said a corrupted
`record_count` can never be acted on because the payload CRC is verified first.
That ordering is real (`connection.rs:311-329` verifies, `:332` dispatches) but
CRC32 detects accident, not intent: a hostile peer computes a valid CRC over a
body declaring 65,535 records in three bytes. Full bounds checking is mandatory
and the CRC contributes nothing to it. `coordinate.rs:35` already states this
principle for `MAX_SEGMENT_NAME_LEN` — "a small, fixed pre-allocation bound
rather than trusting a peer-chosen `u16`". The batch body reintroduces exactly
that `u16`.

**No per-record CRC.** The frame CRC already detects any corruption in the body;
a per-record CRC would say *which* record was corrupt, and the only correct
response to a failed frame CRC is `Nack(BadPayloadCrc)` + close, because the
stream has desynced. Cost: 4 bytes × N for no detection gain.

### 3.3 `AckBatch` — unresolved, see §6

The leading candidate is a positional bitmap:

```
Offset  Size          Field          Notes
 0       1            ack_version    0x01
 1       2            record_count   u16 LE — MUST equal the request's count
 3       ceil(N/8)    accepted       bit i = record i is durable at the requested tier
```

At N=1024 that is 131 bytes. §6 explains why this is not yet settled.

### 3.4 New `NackReason`

`BadBatchFraming = 0x0B`, **not `0x0A`**. `0x0A` is the tree's worked example of
an unassigned byte: `nack.rs:229-231` asserts `try_from(0x0A).unwrap_err()`
(assigning it panics the test), `weir-client/src/unix.rs:664-684` asserts it
surfaces as `UnknownNack(0x0A)`, four polyglot clients branch on `>= 0x0A`, and
the frozen vector `nack_reserved_reason` *is* reason byte `0x0A` — regenerating
it on another byte would change a frozen vector.

Blast radius either way is ~30 files, including a **parallel private label enum**
in `metrics/mod.rs:49-59` kept in sync with `weir-core`'s by a comment, not by a
test. That divergence risk should be closed by a test in this release.

## 4. Safety — what must ship with A6

Each of these is a bug on day one, not a follow-up.

1. **`max_batch_records`**, default **1024**, hard cap 65,535, validated against
   `QUEUE_CAPACITY / shard_count`. Required for safety, not tuning: at the u16
   maximum a single ~320 KiB frame declares 65,535 records — **99.998% of the
   global `QUEUE_CAPACITY`** (65,536, `queue.rs:6`), and every record of one
   connection lands on **one** partition, because `shard_id` is fixed per
   connection at accept.

2. **Bounded speculative allocation.** `Vec::with_capacity(record_count)` would
   allocate ~3.7 MB from a 3-byte declaration — a worse ratio than the
   `payload_len` bug 2.0.3 fixed, and with no cap in front of it. Grow as records
   are parsed, mirroring `connection.rs:261`.

3. **A whole-batch deadline.** Serially, each record can cost
   `QUEUE_PUSH_TIMEOUT` (5 s) + `ACK_TIMEOUT` (30 s) = 35 s
   (`connection.rs:31,43`). N=1024 → **9.96 hours** holding one of 256 semaphore
   permits. One budget for enqueue-plus-await; records unresolved at the deadline
   report as not-durable.

4. **Per-record validation at ingest** — see §5, where omitting it makes a
   latent bug reachable.

5. **`inc_by(n)` on the three per-record counters.** They are incremented once
   per frame today (twelve `.inc()` sites in `connection.rs`), and `inc_by` is
   already idiomatic in the tree. Beyond undercounting rate panels, the
   **fsync-amortisation dashboard panel divides by `records_accepted`** and
   `weir-readiness.sh` gates on it. The nastier failure is inconsistent
   granularity: `monitoring.md` documents `accepted − ack` as in-flight work, and
   mixing per-batch with per-record makes that gap go negative.

6. **`is_push()` must learn `PushBatch`** (`connection.rs:211`, `:432-434`), or a
   zero-length `PushBatch` skips the empty-payload guard entirely.

**Also true, and not A6's fault:** `docs/security/threat-model.md:80` claims the
4 GiB worst case "requires genuinely streaming that much data". It does not —
`payload_buf.resize(payload_len, 0)` commits the full declared length once the
first 64 KiB arrives, so the cost is 64 KiB per 16 MiB, a 256:1 amplification of
*resident* memory. That wording should be corrected independently of A6.

## 5. A latent bug A6 would unlock

`flush_batch`'s failure arm nacks every record in `pending_acks` on the premise
stated in its own comment — *"The active segment was dropped"*
(`wab/mod.rs:901-906`). **That premise is false for two paths.** Both
`payload.is_empty()` (`segment.rs:602-607`) and zstd compression failure
(`segment.rs:614-621`) return `Err` *before* `self.active = None`, and the zstd
comment says so outright:

> A failure here returns before anything is written and before any accounting
> moves, so the segment stays clean and the producer gets a Nack.

The segment stays clean. So those `pending_acks` records are written, get
group-fsynced, and **are delivered to the sink** — after their producers were
told they were not durable and retried. Duplicate delivery, and because
`pending_acks` carries no connection identity, the victims can be other
connections.

Unreachable today only because ingest rejects zero-length payloads. **A6 makes it
reachable the moment a sub-record is not validated at ingest.** Two independent
fixes, both required: validate every sub-record before enqueueing any of them,
and give `ShardWriter::write_record` a typed error distinguishing "record
rejected, segment intact" from "segment dropped".

## 6. Open design questions

These are not settled. §6.1 is the one most likely to cause a silent false ack.

### 6.1 Bit order and padding — a silent false ack the protocol cannot detect

`AckBatch` would be weir's **first indexed, bit-addressed wire field**. Every
multi-byte field is explicitly little-endian, but no bit-order convention exists
to inherit — the only bit-level language anywhere in the protocol docs is a
single flag in the *file* format.

For a one-record batch, `0b00000001` means "record 0 succeeded" under LSB-first
and "record 0 failed" under MSB-first. **The frame is valid under both**: header
CRC passes, payload CRC passes, `payload_len` is correct. The failure mode is a
producer believing a record is durable when the daemon said it was not — the one
outcome weir exists to prevent, and nothing in the protocol detects it.

Requirements if the bitmap is adopted: state bit order explicitly with a worked
byte; mandate padding bits are zero and that a receiver **rejects** a nonzero
padding bit (following the `ReservedFlagsSet` precedent); and ship a conformance
vector at **N=9 with an asymmetric pattern** — any vector with N≤8 or a
symmetric pattern passes under both readings and guards nothing.

### 6.2 A `0` bit is three-valued

A `1` bit inherits the crown invariant. A `0` bit conflates:

- **(a)** definitely never written — WAB cap, queue refusal;
- **(b)** written, unconfirmed, and quite possibly delivered anyway — the
  fsync-failed path leaves bytes on disk that recovery picks up at restart;
- **(c)** unknown — `ACK_TIMEOUT`, where "the record may still be durably
  written by the eventual flusher completion".

`wire_protocol.md:396` already documents this ambiguity for a single-record Nack
and tells producers to treat all causes alike. A bitmap *looks* like per-record
truth and will be read that way; client authors will build exactly-once retry on
the `0` bits. The wire doc must state the asymmetry: **bit 1 ⇒ durable; bit 0 ⇒
not-durable-as-of-this-reply, retry, expect a possible duplicate.**

### 6.3 The bitmap discards `NackReason`, which is the retry decision

A bitmap answers "did it fail" and discards *why*. That reason is the most
operationally load-bearing field in the protocol: `is_recoverable` is
`Nack(InternalError) => true, Nack(_) => false`, and `monitoring.md` says
`internal_error` is the only transient reason. A producer holding a clear bit
cannot tell a transient `InternalError` (retry) from a permanent `EmptyPayload`
(retrying loops forever). Per §5 this is reachable, not hypothetical.

Options: a parallel reason-byte array for clear bits only; or all-or-nothing,
where a failure is a plain `Nack` carrying its existing reason.

### 6.4 The permanent-error contract conflicts with partial reporting

Today permanent protocol errors **close the connection**; transient runtime
conditions keep it open, and `weir-client` encodes exactly that
(`client.rs:144-166`). A per-record *validation* failure inside a batch is
permanent at record granularity. Either every sub-record is validated at ingest
and a violation closes the frame and the connection — preserving today's
contract — or the reply needs a "permanent, do not retry this record" signal the
client taxonomy has no room for. Pick one explicitly.

### 6.5 Is the bitmap even required?

**Not for correctness.** An all-or-nothing single result is correctness-legal:
weir already false-*Nacks* routinely and documents it as harmless at-least-once;
only false *acks* are forbidden. The bitmap's justification is
duplicate-minimisation — all-or-nothing forces up to N−1 unnecessary redeliveries
per failure, which at N=256 is a real cost. **That is a cost argument, not a
correctness argument, and the spec must not overclaim it.**

Among *fixed-size* encodings the bitmap is forced, and this is proven by executed
tests rather than argued. `dst.rs:1693` pins `[F,F,F,T]` and `dst.rs:1533` pins
`[T,T,F]`. An accepted-count is not merely lossy against the first — it is wrong
in **both** directions, naming record 0 accepted when it was nacked. A
`(first_failed, last_failed)` range dies to a three-block pattern that rotation
makes reachable.

### 6.6 The `u16` is 28× wider than the constraint it must satisfy

`record_count` permits N = 65,535 → an `AckBatch` of 8,195 bytes. The largest N
keeping it within the existing 298-byte response cap is **2,360**. Nothing on the
wire encodes the real limit.

And the cap is not free as assumed: **no client applies 298 to anything but
`0x07`** — all six return **2** for every other type, so a 131-byte `AckBatch`
is rejected as oversize by all of them until each changes. C is worse than a
constant: its response struct has a fixed 2-byte member and a test asserting it
stayed 2. The five tracked drift guards iterate a hardcoded
`{0x01…0x06, 0xFF}`, so `0x09` silently escapes the invariant they exist to
protect.

## 7. The honest case against

### 7.1 Producers without N records in hand — the strongest objection

A6 requires batchable arrival. A request-scoped handler pushing one record per
HTTP request cannot batch at all. To batch, it must accumulate records in memory
first — and `README.md` names that exact thing as the failure mode weir exists to
eliminate: *"unsafe (in-memory queue, lost on crash)"*.

A producer buffering 256 records client-side to earn A6's amortisation has
**reintroduced a 256-record loss window on producer crash**, strictly worse than
the one-record window it has today. For this population A6 does not merely buy
nothing — **taking it is a durability regression**, and no server-side work fixes
it, because the batch must be assembled somewhere.

L1 does not share this objection: it helps producers with one record in hand.

### 7.2 Fsync amortisation saturates at `batch_size`, not N

Three independent ceilings, all at `batch_size` (default **256**): the worker
flushes at `>= batch_size` in all three drain phases; the flusher's coalescing
loop checks `record_count < batch_size` before `try_recv`; and there is exactly
one fsync per `flush_batch`. A 1024-record batch pays **3–4** fdatasyncs.
**"N records, one fsync" is false above N≈256** and must not appear in any
published claim.

Worse, the published beast numbers were taken at `batch_size = 64`, so
benchmarking A6 with the existing preset would understate the win ~4× relative to
a default deployment.

### 7.3 A pessimisation A6 turns permanently on

The worker's coalesce window is gated on `expect_concurrent` (`total_drained >= 2`).
Any `PushBatch` with N ≥ 2 sets that true for a *single* producer. The window is
an EWMA of observed fsync latency clamped to 50–2000 µs — ~1,450 µs on beast. So
a `PushBatch` whose N is not a multiple of `batch_size` holds its remainder
~1.45 ms waiting for a producer that is blocked on its own acks and cannot send.
`worker.rs:114-117` already records that enabling this window when connections
co-locate "collapses single-shard `Buffered` throughput ~8–12×" — and at the
default `shard_count = 1`, every connection co-locates.

### 7.4 Latency regresses

An `AckBatch` cannot ship until every record resolves, so the *first* record of a
`PushBatch(1024)` waits for four serial fdatasyncs — ~6 ms on beast against
1.5 ms today, a 4× p50 regression. A6 is a throughput feature that degrades the
metric the README leads with.

### 7.5 On shipped defaults, A6 buys nothing end-to-end

`sink_http_batch` defaults to `"none"` (per-record), which delivers ~135 rec/s at
50 ms RTT. `min(653, 135) = 135` today; `min(~70k, 135) = 135` after A6 —
**1.0×**. The end-to-end win is a curve from 1.0× to ~107× keyed on sink mode and
RTT, not a single number. The "2.7×" in earlier notes is one point on it.

**Correction to earlier notes:** the claim that the docs falsely exempt the HTTP
sink from the `sink_max_batch_size` re-split hazard is **stale**. It was fixed on
2026-09-06 in `e25f45b`, one day *after* the sweep that reported it. The hazard
is real, permanent and now correctly documented.

## 8. What A6 is actually for

Not steady-state throughput against a slow sink. **Burst handoff.** Time-to-ack
for 100,000 `Durable` records goes from ~153 s to ~1.4 s, independent of the
sink, which is precisely the case `README.md` says the product exists for: absorb
a burst faster than the downstream can take it.

That reframe carries an obligation. There is **one** drain thread for the whole
daemon; delivery does not scale with `shard_count` while ingest does. A6
multiplies the rate at which the WAB outruns the drain by ~100×, against a
`wab_max_bytes` that **defaults to 0 (disabled)**. A6 must not ship without a
recommended value and a sizing rule.

## 9. Estimated win, with its error bars

Modelled on beast (i9-9900K, ext4 on a Samsung 850 EVO, honest `fdatasync`,
`isolcpus=2-7,10-15` with weir's workers pinned into the isolated range),
`batch_size = 256`, one connection: per-record cost ≈ 256 writes at 5–6 µs plus
one 1.4–1.5 ms fdatasync ⇒ **10.5–11.9 µs/record ⇒ ~84–95k rec/s**. A
beast-native cross-check at 64 records/fsync rescales to ~72k. Call it
**~70,000 rec/s, roughly 100× over 653, ±40%** — and refuse a second significant
figure until measured.

Three caveats on that number. It exceeds the `Buffered` figure of 49,725, which
earlier notes called a ceiling — wrongly: that figure is itself one round trip
per record, a latency reciprocal, not a throughput bound. **Batched `Buffered`
has never been measured.** Second, a beast number is a best case: the daemon's
threads get cores the scheduler holds empty for them while the harness is
squeezed onto four, and A6's win is a pipeline win that degrades on contended
cores. Third, the per-record write cost is Mac-measured; no beast equivalent is
published.

## 10. Work breakdown

**Before any of it:** the `records_per_fsync` observation (§1).

- `weir-core`: two `MessageType` variants + `#[non_exhaustive]`; `BadBatchFraming
  = 0x0B`; batch body codec; `AckBatch` codec; `MAX_BATCH_RECORDS_HARD_CAP`;
  proptest + fuzz coverage.
- `weir-server`: dispatch arm; body parse after CRC; the six safety items in §4;
  `max_batch_records` config; the §5 typed-error fix; new metrics.
- `weir-client`: `push_batch`; response cap arm; a `BatchOutcome` that is `Ok` on
  partial and `#[must_use]`.
- Conformance: `wire_v1_batch_vectors.json` (frozen set untouched), including the
  N=9 asymmetric bitmap vector; widen the `0x08`/`0x09` freeze guard; update the
  five hardcoded client cap-scope lists.
- Five polyglot clients: response cap for `0x09`, batch codec, tests.
- DST: a `Scenario` rotation variant so the three-block pattern is pinnable as a
  seed regression, and `SimFaults` holding a *set* of `ShortWriteOn` so
  multi-failure patterns are expressible. **Not** a two-connection scenario —
  `flush_batch` is single-threaded over a `Vec<Batch>`, so interleaved units in
  one batch observe the same thing, and a real-daemon two-connections-one-shard
  test already exists (`system.rs:2434`), merely without fault injection.
- Docs: wire protocol, conformance, configuration, threat model, monitoring,
  CHANGELOG.

## 11. Decisions required before implementation

1. **A6 or L1?** §2. A6 is 3–5 weeks and abandons single-record producers; L1 is
   2–3 weeks, no wire change, and serves them — but touches the crown invariant.
2. **Run the `records_per_fsync` metric first?** §1. Recommended: yes.
3. **`AckBatch` shape** — bitmap, bitmap plus reason array, or all-or-nothing.
   §6.2–6.5.
4. **Bit order and padding**, if a bitmap. §6.1.
5. **`max_batch_records` default** — 1024 proposed; interacts with the 2,360
   ceiling implied by the response cap. §6.6.
6. **Per-record permanent failures** — close, or report and continue. §6.4.

---

## 12. What implementation settled (added 2026-09-15, after merge)

This document is the design as written *before* the work. It is kept as the
record of the reasoning — including the case against A6 in §2 — rather than
rewritten to match the result. Where the two differ, the code wins and the
divergence is listed here.

**The six decisions in §11, as resolved:**

1. **A6, not L1.** A6 shipped; L1 was not attempted.
2. **Yes — the metric ran first, and the gate did not fire.**
   `weir_wab_group_commit_records` measured 1.00 records per fsync at one
   producer and 6.40 median at 8 producers / 1 shard, against a gate of
   `0.6 × 256 ≈ 154`. Nothing came close, so the premise survived its own
   falsification test.

   **Measured on beast, 2026-09-18** (`docs/benchmarks/wire-batching.md`): the
   macOS figures were *not* a lower bound in the direction assumed. Beast reads
   5.22 records per fsync at 8 producers and 16.58 at 32, against macOS's 7.89
   and 30.91 — worse, not better, because the ceiling is structural rather than
   timing-dependent. One record per connection is in flight at a time, so a
   group fsync covers at most `connections-per-shard` records and beast attains
   only 52-65% of even that. Reaching 154 by concurrency needs ~300 producers on
   one shard; one producer using `push_batch` reaches 232.

   The throughput number §9 refused to state without measurement is
   **165,546 rec/s** at a 1024-record frame, against a re-measured same-box
   control of 878 — **189x**. At the `batch_size = 256` configuration §9
   actually modelled it is 108,868 rec/s, which is **outside** that section's
   own ±40% band of 42,000-98,000. The model was conservative. Batched
   `Buffered`, which §9 recorded as never measured, is 975,492 rec/s against
   49,360 unbatched — 19.8x, confirming that the 49,725 figure earlier notes
   called a ceiling was a latency reciprocal and low by an order of magnitude.
3. **Bitmap**, as proposed in §6.2.
4. **LSB-first, padding bits must be zero**, and a decoder must reject a payload
   where they are not — otherwise `popcount(bitmap) == N` is silently wrong.
5. **`max_batch_records` default 1024 — but the hard cap is 2048, not 65,535.**
   This is the one substantive change from the spec. §6.6 left the ceiling open;
   2048 is the largest value for which the `AckBatch` reply (3 + `ceil(N/8)` =
   259 bytes) still fits under the 298-byte `MAX_TRACKED_ACK_PAYLOAD_LEN` weir
   already published. Choosing it means **batching introduces no new largest
   response**, so no client gains a new allocation bound and no published size
   claim changes. `record_count` remains a u16 on the wire; the cap is enforced
   against the *declared* count before anything is sized by it.
6. **Report and continue.** A per-record *runtime* failure clears its bit and
   the connection stays open. A per-record *validation* failure (empty record,
   over `max_payload_bytes`, framing disagreement) still rejects the whole frame
   with a plain `Nack` and closes, under the existing permanent-error contract —
   which is what makes a bare bitmap sufficient, since every failure a bitmap
   can report is then transient.

**Also diverged:**

- **DST.** §10 asked for a `Scenario` rotation variant and `SimFaults` holding a
  *set* of `ShortWriteOn`. Neither was built. What was built instead is
  `RejectedRecordInBatch`, because implementation surfaced a latent bug that
  mattered more: `flush_batch` drained every pending ack as a failure on *any*
  write error, so one rejected record nacked every innocent record sharing its
  group commit. The scenario drives a real rejection (an empty payload, refused
  by `ShardWriter` before the segment is touched) rather than an injected fault,
  so it exercises the production classification. The multi-fault set remains
  unbuilt and unneeded so far.
- **`MessageType` became `#[non_exhaustive]`**, as §5 anticipated — which makes
  the next release **4.0.0**, since adding that attribute is itself a breaking
  Rust-API change. §5's reasoning holds: it is the last major a new message type
  ever forces.
- **`BadBatchFraming` is `0x0B`, not `0x0A`.** `0x0A` is the frozen
  `nack_reserved_reason` vector's worked example of an unassigned byte; taking it
  would have changed a frozen vector's meaning.

**What the spec under-weighted.** §6.1 called bit order a detail to be decided.
It is the single most dangerous under-specification in the protocol: a reader
using the opposite convention produces a *well-formed* frame — header CRC valid,
payload CRC valid, `payload_len` correct — that reports failures as successes,
and no checksum, length check or cap detects it. Every vector with N ≤ 8, and
every symmetric pattern at any N, encodes identically under both conventions.
`ack_bitmap_asymmetric_n9` exists solely to discriminate, and six independent
implementations were written to agree on it.
