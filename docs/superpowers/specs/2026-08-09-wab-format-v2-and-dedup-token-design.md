# weir 2.0 — WAB format v2, transparent compression, and the dedup token

**Status:** Design, approved. No implementation yet.
**Date:** 2026-08-09
**Branch context:** `feat/chaos-fault-injection`, off `main` @ 1.3.1.

---

## 1. Why this exists

`docs/explorations/v1-feature-directions.md` §0 named four decisions it called
*irreversible*, on the grounds that a 1.0 semver freeze would put them out of
reach without a major release. Two were taken before the freeze:

- **#3** — language-neutral wire conformance vectors and a frozen `wire/v1`.
  Shipped: `docs/conformance/wire_v1_vectors.json`, 30 vectors, five polyglot
  decoders in `demos/`.
- **#4** — a reserved Nack-reason byte. Shipped: `NackReason` is
  `#[non_exhaustive]` with `0x0A`–`0xFF` reserved.

Two were not:

- **#1 — WAB on-disk format headroom.** `crates/weir-wab/src/format.rs:68` is
  still `FORMAT_VERSION = 1`.
- **#2 — the `Sink::commit` signature.** Still
  `commit(&self, batch: Vec<Self::Record>)`
  (`crates/weir-sink-sdk/src/lib.rs:376`), never became a batch type carrying a
  dedup token.

Everything downstream of those two — a first-class dedup token, payload
compression — has been blocked ever since. This design closes both, as
**weir 2.0.0**.

### The 2.0 is narrow on purpose

An audit for "what else is only fixable at a major" came back nearly empty. The
1.0 freeze work was thorough: `Payload` is already a newtype
(`crates/weir-core/src/payload.rs:20`), MSRV is pinned at 1.88, and
`CommitResult` / `SinkHealth` / `NackReason` / the public error enums are all
`#[non_exhaustive]`, so `BoxSink`, the backpressure Nack reason, and the rest of
the parked backlog stay additive whether or not a major happens.

`Sink::commit` is the only public-API item that genuinely requires the bump.
The 2.0 must not sprawl looking for justification: three changes ship, listed in
§2, and nothing else rides along.

---

## 2. Scope

1. **WAB format v2** — header byte `[5]` becomes a flags byte; transparent
   per-record zstd compression, opt-in, off by default.
2. **`Sink::commit(SinkBatch<Record>)`** replacing `commit(Vec<Record>)`, with
   `SinkBatch` carrying a content-derived batch dedup token.
3. **The ClickHouse and HTTP sinks migrated** onto the shared token.

### Explicitly out of scope

- **The wire protocol does not change.** Producers send plaintext; compression
  begins and ends inside the daemon. `weir-core` and `weir-client` are
  untouched — no producer and none of the five polyglot demo clients needs
  recompiling.
- **No footer change.** See §4.3.
- **No encryption.** Not even a reserved slot. See §4.3.
- **No new sinks, no `BoxSink`, no async client, no `/status`.** All additive,
  all independent of this work.

The workspace version moves as one (`version.workspace = true` already binds
every crate), so all eight crates go to 2.0.0 even though only `weir-sink-sdk`
and `weir-wab` change meaningfully.

### The two halves are independent

A consequence of §3.1 worth stating up front: because the dedup token needs no
on-disk field, **the format/compression work and the `SinkBatch` work touch
disjoint code and can be built, reviewed, and merged separately.** They are
coupled only by shipping in the same release.

The implementation plan should sequence them as two independent tracks rather
than one interleaved sequence. Recommended order is `SinkBatch` first: it is
smaller, it is the part that actually forces the major version, and landing it
first means the format work is never blocked behind an API debate.

---

## 3. Two premises from the exploration docs that did not survive review

Both are recorded here because they are the load-bearing reasons this design
differs from what `phase4-standout-architecture.md` Bet A and
`phase4-wab-optimization.md` §2 proposed. Re-opening either should require new
evidence, not a re-reading of the old docs.

### 3.1 The dedup token cannot be segment-scoped

Bet A proposed a seal-time `content_sha256` stored in the segment footer and
surfaced to the sink.

`process_segment` (`crates/weir-server/src/drain/mod.rs:933-976`) splits each
segment into `sink.max_batch_size()` chunks and calls `commit` **once per
chunk**. A segment-scoped token handed to every batch from that segment would be
identical across all of them, and a dedup-capable sink would drop batches
`2..N` as duplicates — silent data loss, caused by the feature meant to prevent
duplicates.

The token must be **batch-scoped**. Once it is batch-scoped it must be derived
from the batch's own contents, which the drain has in hand at commit time. It
therefore needs no on-disk storage at all.

### 3.2 The correct primitive already exists in the tree, twice

`crates/weir-server/src/sink/clickhouse.rs:88` computes exactly the right thing:

```rust
fn dedup_token(batch: &[Payload]) -> String {
    let mut hasher = Sha256::new();
    for p in batch {
        hasher.update((p.len() as u64).to_le_bytes());
        hasher.update(p);
    }
    // → lower-hex
}
```

8-byte little-endian length prefix per payload, sha256, lower-hex. Its rustdoc
already explains why the prefix is load-bearing (without it `["ab","c"]` and
`["a","bc"]` hash identically and ClickHouse drops a genuinely distinct block),
and it carries three tests: `dedup_token_is_deterministic`,
`dedup_token_changes_on_reorder`, `dedup_token_distinguishes_different_batch_boundaries`.

`crates/weir-server/src/sink/http.rs:303` independently reinvented
`sha256:<hex>` for `Idempotency-Key`.

Two sinks, the same primitive, derived independently. This design **promotes the
existing implementation** rather than inventing one: the shared code reproduces
the ClickHouse bytes exactly (§6.1).

### 3.3 Compression does not buy latency

`phase4-wab-optimization.md` argued that `fdatasync` cost scales with dirty
bytes, so fewer bytes means a faster fsync. At weir's batch sizes the measured
floors — ~150 µs on NVMe, ~1.4 ms on SATA — are dominated by **per-call** cost,
not per-byte transfer. Meanwhile zstd level 1 runs ~800 MB/s–1 GB/s, so a 1 KiB
record costs ~1.3 µs and a 256-record batch adds ~300 µs of CPU to a path whose
fsync is 150 µs. **On fast storage, compression can make the ack path slower.**

What compression actually buys, in priority order:

1. **WAB capacity.** weir's central use case is absorbing a sink outage. A 4×
   ratio is 4× the outage duration survivable in the same disk budget. This is a
   direct improvement to what weir is for, and it is not latency-sensitive.
2. **Fewer seals.** Each seal is fsync + rename + parent-dir fsync; a compressed
   segment fills more slowly.
3. **Less I/O on drain and on recovery replay.**

Consequences that are binding on the implementation:

- The default is `none`.
- **No documentation may claim a latency benefit.**
- No latency claim ships without a measurement on the i9.
- The chaos harness gets a compressed variant of the Phase 1 schedule (§8).

---

## 4. WAB format v2

### 4.1 Layout

```text
Segment header — SEGMENT_HEADER_LEN (24) bytes, unchanged
[0..4]   SEGMENT_MAGIC   b"WEIR"
[4]      format_version  u8 ∈ {1, 2}
[5]      flags           u8   — v1: MUST be 0
                                v2: bit 0 = ZSTD; bits 1-7 reserved, MUST be 0
[6..8]   shard_id        u16 LE
[8..16]  created_at      i64 LE — unix nanoseconds
[16..24] reserved        [u8; 8] — zero on write
```

Record framing, the end-of-records sentinel, the segment footer, and the
`.confirmed` sidecar are **byte-identical to v1**. `SEGMENT_HEADER_LEN` stays 24
and `SEGMENT_FOOTER_LEN` stays 32.

`payload_len` is the length of the **stored** bytes and `crc32` covers the
**stored** bytes. In a compressed segment, "stored bytes" is a complete zstd
frame.

### 4.2 Why the CRC covers stored bytes

This is the decision that makes the change cheap. Because integrity is checked
against what is physically on disk:

- `recovery.rs::recover_segment`'s torn-tail scan works unchanged.
- `verify_sealed_segment`'s whole-file CRC works unchanged.
- The 1.3.0 footer cross-check (`record_count` / `data_bytes` vs records walked)
  works unchanged.

None of them decompress anything. Decompression happens in exactly one place —
`SegmentReader::next` — after the CRC has already passed. The most
durability-critical and most defect-prone code in the tree needs no compression
awareness at all.

### 4.3 Why the footer does not grow

Given §3.1, no on-disk field is needed for the token. The remaining argument for
growing the footer is reserving headroom for a future segment-level
`content_sha256`. Rejected, for three reasons:

1. **It contradicts a stated design position.**
   `crates/weir-wab/src/format.rs:50-56` documents CRC32-not-a-hash as
   deliberate: *"A forged WAB segment or confirmation file with a valid CRC32
   will be accepted... This is an explicit assumption, not a weakness to be
   fixed."* A footer digest half-reverses that without closing it.
2. **It charges the hot path.** An incremental sha256 over every payload, in
   front of the fsync, for a field nothing consumes.
3. **The bet has already been lost once.** v1 reserved 4 footer bytes; a digest
   needs 32. Reserving a different guess without a concrete consumer is the same
   bet again.

**The durable asset is the version-routing reader built here, not reserved
bytes.** It makes a future v3 cheap, which is what headroom was supposed to buy.

### 4.4 The version bump is load-bearing

`parse_segment_header` (`crates/weir-wab/src/format.rs:314`) reads
`format_version: buf[4]` and **never inspects byte `[5]`**. A 1.x reader handed a
compressed segment would not see the flag, would read each zstd frame as an
opaque payload, and would hand compressed bytes to the sink as if they were a
record.

Bumping the version turns that silent corruption into `SegmentReader::open`
failing with `InvalidData`, which the drain already handles correctly at
`drain/mod.rs:918-930` ("cannot open segment; preserving for retry") and which
recovery quarantines. **Data is stranded and loud, never lost and quiet.**

### 4.5 Version routing and rollback

Reader rules, centralised in the header parse:

| Input | Behaviour |
|---|---|
| v1, `flags == 0` | Accept — weir has only ever written zero there |
| v1, `flags != 0` | Reject as corrupt |
| v2, only known flag bits | Accept |
| v2, any unknown flag bit | Reject rather than risk misreading |
| version > 2 | Reject (existing behaviour, preserved) |

**Writer rule: a segment is written as v1 whenever compression is off, and v2
only when it is on.** With `wab_compression = none` — the default — a 2.0 daemon
produces byte-identical v1 segments, so upgrade-then-rollback to 1.x is a
complete non-event. Enabling compression is the moment an operator opts into the
one-way door, and the docs say so in those words. The 2.0 reader accepts both
versions regardless of the setting.

### 4.6 Constants

`FORMAT_VERSION` currently means both "what we write" and "what we accept" and
can no longer mean both. It is **removed** and replaced by:

```rust
pub const FORMAT_VERSION_V1: u8 = 1;
pub const FORMAT_VERSION_V2: u8 = 2;
/// The highest version this build can *read*. Not necessarily what it writes.
pub const FORMAT_VERSION_MAX_SUPPORTED: u8 = FORMAT_VERSION_V2;
```

Note there is deliberately **no** "the version we write" constant. Per §4.5 the
writer picks the *lowest version that can express the segment's features* — v1
when compression is off, v2 when it is on — so that value is a function of
config, not a constant. Naming it would invite exactly the confusion this
rename exists to remove.

Removal of `FORMAT_VERSION` rather than redefinition is deliberate: it is a
compile error for every consumer, forcing each to state which meaning it
intended. On a 2.0 that is the point.

### 4.7 The stored-length cap

`SegmentReader::next` (`crates/weir-wab/src/lib.rs`) rejects any
`payload_len > MAX_PAYLOAD_HARD_CAP` (16 MiB) before allocating. Correct today;
wrong under compression, because zstd's worst-case bound is `n + n/255 + 16`, so
a 16 MiB incompressible payload can legitimately store as ~16.06 MiB and would
be rejected as corrupt.

- **v1 segments:** cap stays `MAX_PAYLOAD_HARD_CAP`.
- **v2 ZSTD segments:** cap is `compress_bound(MAX_PAYLOAD_HARD_CAP)`, defined
  as `n + n / 255 + 16` — written out as a `const fn` in `weir-wab` rather than
  taken from whichever zstd crate §12.1 selects, so the on-disk contract does
  not move if the dependency does.
- **Decompression** runs under an explicit **output** limit of
  `MAX_PAYLOAD_HARD_CAP`. A frame declaring more is rejected as corrupt.

That output limit is the decompress-bomb guard the exploration doc asked for,
and it is an exact bound rather than a guess, because the real cap is known.

---

## 5. The write path

### 5.1 The codec sits above the DST seam

`SegmentStore` / `SegmentHandle`
(`crates/weir-server/src/wab/segment.rs:356-384`) is the seam the DST harness
swaps to inject fsync and seal faults on a seeded schedule. If compression lived
inside `WabSegment::write_record` — below the seam — every existing DST seed
would keep exercising only the uncompressed path, and crash-during-compressed-write
would go untested.

Therefore:

- `weir-wab` gains `pub enum Compression { None, Zstd }` — the on-disk flag,
  and the *only* compression type the format layer knows about.
- `SegmentStore::create(path, shard_id, compression: Compression)` — the store
  writes the header, so it owns the version byte and the flags byte. It receives
  the variant, **not** the level.
- `ShardWriter` holds the full codec configuration (variant **and** level) and
  compresses in **its** `write_record`, then passes stored bytes down.
- `SegmentHandle::write_record(&mut self, stored: &[u8])` now unambiguously
  means "the bytes as they go on disk."

**`ShardWriter` owns the codec; `WabSegment` owns the framing.** The DST harness
drives real compressed byte streams through its fault schedules for free.

The compression *level* is a writer-only concern and is **not** stored on disk —
zstd frames are self-describing for decompression, so a reader never needs it.
It lives in server config only, which is why it stops at `ShardWriter` and never
reaches `SegmentStore`.

### 5.2 The empty-payload trap

`segment.rs:109` rejects empty payloads because a zero `payload_len` **is** the
end-of-records sentinel — storing one would truncate the segment on the next
read.

Compressing an empty payload yields a ~9-byte frame that writes and reads back
without complaint, silently defeating that guard. **The plaintext-empty check
must move up into `ShardWriter::write_record`, before the codec.** The existing
check stays below as defence in depth.

### 5.3 Compression failure

A compression error (realistically only OOM) returns `Err` before any bytes are
written and before any accounting moves, so the segment stays clean and the
producer receives a Nack.

There is no per-record flag, so falling back to storing plaintext is not
available — and that is the correct outcome regardless: failing the ack is
right, silently storing bytes the reader will try to decompress is not.

### 5.4 Accounting

`bytes_written` and the footer's `data_bytes` both track **stored** bytes. That
is what keeps the footer cross-check and `verify_sealed_segment`
decompression-free, with two visible consequences:

- `data_bytes` stops meaning "how much data is in here" for a compressed
  segment. `weir-ctl segments` must label it as on-disk bytes.
- Rotation is driven by stored bytes, so a compressed segment holds
  proportionally more records before reaching the 256 MiB threshold. For
  low-rate streams `wab_segment_max_age_secs` becomes the effective bound
  instead. Documented, not configurable around.

### 5.5 Metrics

Two counters, because a ratio that cannot be seen cannot be tuned:

- `weir_wab_record_logical_bytes_total`
- `weir_wab_record_stored_bytes_total`

Equal when compression is off; their quotient is the live compression ratio.

---

## 6. The sink API

### 6.1 `SinkBatch` and `DedupToken`

```rust
pub struct SinkBatch<R> { /* records + token */ }

impl<R> SinkBatch<R> {
    pub fn new(records: Vec<R>, dedup_token: DedupToken) -> Self;
    pub fn records(&self) -> &[R];
    pub fn into_records(self) -> Vec<R>;
    pub fn into_parts(self) -> (Vec<R>, DedupToken);
    pub fn dedup_token(&self) -> &DedupToken;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

pub struct DedupToken([u8; 32]);   // Debug prints hex, not a byte array

impl DedupToken {
    /// sha256(len(p₀) ++ p₀ ++ len(p₁) ++ p₁ ++ …), lengths as u64 LE.
    pub fn for_payloads<'a>(p: impl IntoIterator<Item = &'a Payload>) -> Self;
    pub fn as_bytes(&self) -> &[u8; 32];
    pub fn to_hex(&self) -> String;
    pub fn to_prefixed_hex(&self) -> String;   // "sha256:<hex>"
}
```

`SinkBatch::new` is public so sink authors can construct one in their own tests.

The digest is byte-identical to `clickhouse.rs:88` (§3.2). That is a
**requirement, not a coincidence**: an operator upgrading mid-outage must keep
deduplicating.

### 6.2 The trait change

```rust
async fn commit(&self, batch: SinkBatch<Self::Record>)
    -> Result<CommitResult<Self::Record>, Self::Error>;
```

The complete third-party migration is one line at the top of an existing
`commit`:

```rust
let batch = batch.into_records();
```

That snippet goes verbatim into the CHANGELOG and the sink-integration guide.

### 6.3 Where the token is computed

The drain computes it from the `Vec<Payload>` **before** converting to
`S::Record`, on the drain thread — off the fsync path entirely. Roughly 170 µs
per 256 KiB batch against a sink commit measured in milliseconds.

Computed eagerly, not lazily: a lazy token would have to retain the payloads
past the `SinkRecord::from_payload` conversion, and the complexity is not worth
it. The `noop` sink in the load suite has no network latency to hide the cost
behind, so **those baselines must be re-run rather than assumed unchanged**.

### 6.4 The F35 caveat is promoted with the primitive

`clickhouse.rs:81-87` already warns that a token covers exactly the sub-batch
`sink_max_batch_size` produced, so if that config changes across a restart a
replayed segment re-splits into differently-sized sub-batches whose tokens do
not match the originals — and at-least-once then double-inserts.

That warning currently lives in one sink. Once every sink receives a token it
belongs on `DedupToken` itself, stated as a **precondition of the guarantee**:
*keep `sink_max_batch_size` stable, or the dedup guarantee does not hold across
a restart.*

### 6.5 In-tree sink migrations

**ClickHouse** — delete the local `dedup_token`, call
`batch.dedup_token().to_hex()`. Its three existing tests move to
`weir-sink-sdk` beside the shared implementation, joined by a **known-answer
vector** asserting a fixed hex string, so no future refactor can silently change
the token and quietly break every ClickHouse deployment's idempotency.

**HTTP** — ndjson mode switches from hashing the joined NDJSON body to
`batch.dedup_token().to_prefixed_hex()`. This is a **different header value**:
an endpoint deduplicating on the old `Idempotency-Key` will not recognise a
retry that spans the upgrade. It ships as a called-out behaviour change in the
CHANGELOG, not a footnote. Per-record mode keeps its per-record key — a batch
token is meaningless there.

---

## 7. Read-path inventory

Every reader routes through the version-aware header parse.

| Reader | Change |
|---|---|
| `SegmentReader::next` | Decompresses after the CRC passes — **the only decompression site in the codebase** |
| `verify_sealed_segment` | None — operates on stored bytes |
| `recovery.rs::recover_segment` | Accepts a v2 header. The torn-tail scan and CRC walk are unchanged; recovery finalises an *existing* file and never rewrites the header, so the flags byte survives untouched |
| `drain::process_segment` | None — receives plaintext from `SegmentReader` |
| `weir-ctl dl list` / `dl requeue` | None |
| `weir-ctl segments` | On-disk-bytes labelling for `data_bytes` (§5.4) |
| `parse_segment_header` | Returns `compression` on `SegmentHeaderMeta`, which becomes `#[non_exhaustive]` |
| `list_segment_files` | None — filename-based |

**Dead-letter segments are always written v1/uncompressed.**
`drain/dead_letter.rs` writes dead-lettered records as valid WAB segments; they
are a small forensic artifact whose purpose is being readable later, possibly by
tooling that is not this daemon. Compressing them trades nothing for a
readability risk.

---

## 8. Testing

Ordered by risk, heaviest where the crown invariant lives.

### 8.1 Durability paths

- Torn tail in a compressed segment truncates at the last valid record, exactly
  as v1.
- Mid-file corruption in a compressed segment **quarantines** rather than
  truncates (the 1.1.0 behaviour).
- `verify_sealed_segment` passes on v2 and still catches a flipped bit.
- The crash window between `finalize_to_disk` and rename, on a compressed
  segment.
- **DST:** compression added to the matrix (the reason the codec sits above the
  `SegmentStore` seam, §5.1). Every existing seed must still pass.

### 8.2 Format

- v1 and v2 header round-trip.
- v1 with nonzero `[5]` → rejected.
- v2 with an unknown flag bit → rejected.
- Version 3 → still rejected.
- A v1 segment and a v2 segment in the same WAB dir both drain.

### 8.3 Compression

- Round-trip: compress → CRC → read → decompress → identical bytes.
- Incompressible input (random bytes).
- A 16 MiB incompressible payload whose stored form exceeds
  `MAX_PAYLOAD_HARD_CAP`, accepted by the v2 stored-length cap and round-tripped
  (§4.7).
- A hand-crafted bomb frame rejected at the output limit rather than allocating.
- Empty plaintext rejected **before** the codec (§5.2).

### 8.4 Rollback

- A v1-only reader handed a v2 segment fails with `InvalidData` — asserting
  stranded-and-loud rather than silently-misread (§4.4).

### 8.5 Token

- The three migrated ClickHouse tests.
- A known-answer vector pinning byte-identity with 1.x (§6.5).

### 8.6 Sinks

- ClickHouse sends the same token as 1.x for the same batch.
- HTTP ndjson sends the new token; per-record mode unchanged.
- All existing sink tests migrated to `SinkBatch`.

### 8.7 Fuzz and chaos

- Existing WAB fuzz targets extended to generate v2/compressed segments.
- A compressed variant of the Phase 1 chaos schedule, on this branch.

### 8.8 Benchmarks

- Load-suite baselines re-run (§6.3) — the drain's per-batch sha256 is not
  hidden by the `noop` sink.
- Compression's ack-path cost measured on the i9 before any doc claims anything
  about latency (§3.3).

---

## 9. Configuration

| Knob | Type | Default | Range |
|---|---|---|---|
| `wab_compression` | `none` \| `zstd` | `none` | — |
| `wab_compression_level` | i32 | `1` | `1..=19` |

Level 1 for hot-path speed; capped at 19 to stay clear of zstd's ultra levels
and their memory demands. Both get CLI, env, and TOML plumbing plus range
validation, matching the guard-every-bounded-scalar convention established in
1.2.0.

---

## 10. Documentation

| File | Change |
|---|---|
| `docs/wab_format.md` | The v2 layout and the version-routing rules |
| `docs/monitoring.md` | The two ratio counters (§5.5) |
| `docs/operations/configuration.md` | The two knobs (§9) |
| Sink-integration guide | The `SinkBatch` migration one-liner (§6.2) |
| `CHANGELOG.md` | The migration snippet, the HTTP header change, the rollback rule (§4.5) |
| `README.md` | Format version, if stated |
| `docs/explorations/parked-future-directions.md` | Mark the dedup token and compression closed; record that encryption-at-rest no longer has a reserved slot waiting and will need its own format bump |

---

## 11. Risks

| Risk | Mitigation |
|---|---|
| A compression bug corrupts records in a way the CRC cannot catch (the CRC covers the compressed bytes, so a wrong-but-valid frame round-trips a checksum) | The decompression output limit, the round-trip tests in §8.3, and fuzz coverage in §8.7. Default-off means no operator is exposed until they opt in |
| The HTTP `Idempotency-Key` change causes duplicate deliveries across the upgrade | Called out in the CHANGELOG as a behaviour change; unavoidable given one shared primitive |
| Third-party sinks break | One-line migration, published in the CHANGELOG; this is the acknowledged cost of the 2.0 |
| Compression regresses ack latency | Default-off; no latency claim without an i9 measurement; the ack-path cost is measured before release (§8.8) |
| A 2.0 six weeks after 1.0 reads as instability | Narrow scope (§1), no wire change, no producer-side recompile, and a rollback story that is a non-event for anyone who leaves compression off |

---

## 12. Open questions

None blocking. Two to settle during implementation, both local:

1. **zstd crate choice** — `zstd` (bindgen to libzstd) versus `ruzstd`
   (pure-Rust decoder only). The C binding adds a non-Rust build dependency to a
   project that currently has none, which affects the `cargo install` story;
   `ruzstd` cannot compress. Resolve against `deny.toml` and the existing
   dependency-reduction posture before the plan's first task.
2. **Exact frame configuration** — whether to write the content-size field and
   the frame checksum. The content size is redundant with the output limit and
   the frame checksum is redundant with weir's own CRC, so both are candidates
   for omission to reclaim bytes per record. Measure the per-record overhead
   both ways.
