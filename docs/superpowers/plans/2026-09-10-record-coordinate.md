# Record Coordinate — Implementation Plan

**Goal:** give a producer, at ack time, the **durable coordinate** of the record it just pushed —
the WAB segment that record landed in, its index inside that segment, and the `RecordId` derived
from those two plus the payload. That `RecordId` is byte-identical to the value the drain hands the
sink as `Idempotency-Key`, so a producer can finally correlate what it sent with what the
downstream saw.

**Explicitly out of scope:** a *delivery watermark* ("which of my records are confirmed
downstream"). That needs a per-producer cursor, a query path, and durable state weir does not have.
This plan builds the coordinate only.

**Wire constraint:** wire v1 is frozen under SemVer. Today an Ack is exactly 20 bytes with
`payload_len = 0`, `docs/conformance/wire_v1_vectors.json` freezes 30 vectors, and CI runs five
polyglot demo clients (Python, Go, C, Java, TypeScript) against that file. Nothing here may change
a byte any existing client reads or writes.

---

## Design

### Chosen: two new `MessageType` bytes — `PushTracked` (0x06) → `AckTracked` (0x07)

A producer that wants a coordinate sends `PushTracked` instead of `Push`. The daemon replies with
`AckTracked`, whose payload carries the coordinate. Everything else — durability tiers, CRCs, cap
checks, the Nack taxonomy, the connection lifecycle — is identical to `Push`.

```
Byte  Name                 Direction         Response
0x06  PushTracked          client → daemon   AckTracked, or Nack (same reasons as Push)
0x07  AckTracked           daemon → client   —
```

### `AckTracked` payload — the coordinate

```
Offset  Size  Field
──────  ────  ────────────────────────────────────────────────────────────
 0       1    coordinate_version   0x01
 1       8    index                u64 LE — 1-based ordinal within the segment
 9      32    record_id            SHA-256 digest (the sink's Idempotency-Key)
41       2    segment_len          u16 LE — length of `segment` in bytes
43     var    segment              UTF-8 segment identity, e.g. "shard_00/seg_00000001.wab.sealed"
```

`coordinate_version` leads so the payload can grow inside wire v1 without another message type:
a client that does not recognise the version byte rejects the frame instead of mis-parsing it.
`segment_len` is a `u16` although the identity is under 64 bytes by construction
(`shard_{id}` + `/` + `seg_{counter:08}.wab.sealed`, ≤ 48 bytes even at `u64::MAX` counters) —
one spare byte buys immunity to a future naming scheme rather than a silent truncation.

`record_id` is redundant with `(segment, index, payload)` — the producer holds the payload and
could recompute it. It is sent anyway because recomputing means reimplementing the exact digest
framing (`segment_len ++ segment ++ index ++ payload_len ++ payload`, every length a LE u64) in
every producer language. The 32 bytes are cheaper than five re-implementations that can silently
disagree.

### Why this shape and not the alternatives

| Rejected | Why |
|---|---|
| **Longer `Ack` unconditionally** | Every deployed client reads a 20-byte Ack; a longer one desyncs the stream and the *next* response is mis-read as this one's — a false ack. Non-starter. |
| **Capability handshake at connect time** | `docs/wire_protocol.md` states "There is no in-band handshake"; adding one makes the connection stateful, so what an `Ack` *means* depends on invisible prior state. A mis-implemented (or reconnecting) client then desyncs silently. It also costs a round-trip per connection for a feature most pushes will not use. The message-type approach negotiates per frame, statelessly, and an old daemon rejects it with an already-specified permanent error. |
| **A bit in the reserved `flags` byte** | `flags` must be zero in v1; F52 made a nonzero byte a hard `ReservedFlagsSet` rejection *specifically* so "introducing flag semantics in a future version is an explicit, `WIRE_VERSION`-gated change" (`docs/wire_protocol.md`). Spending a flag bit here would contradict a deliberate, documented decision — and worse, it would make the *same* message type (`Ack`) have two different lengths, which is exactly the desync hazard the whole design is avoiding. |
| **`Ack` + a follow-up coordinate frame** | Two frames per push breaks the one-request-one-response invariant the connection lifecycle rests on; a client that reads only the first frame leaves the second in the buffer and mis-reads it as the *next* push's reply. |
| **Side lookup via `weir-ctl` / the metrics port** | No way to correlate a coordinate back to a specific push without a token the push does not carry. Circular. |

### Wire-compatibility argument

An existing client never sends `0x06`, so a new daemon never sends it a `0x07`; its `Push` still
gets the same 20 constant bytes from the same memoised `ack_frame_bytes()` buffer. A new client
pointed at an old daemon sends `0x06`, which fails `MessageType::try_from` in `Header::decode` and
comes back as `Nack(UnknownMessage 0x08)` followed by a close — the already-specified permanent
error for exactly this case (version skew), so the negotiation needs no new failure mode. The
frozen vectors are untouched: `reject_unknown_message_type` pins `0xff`, not `0x06`, and no vector
mentions `0x07`. `wire_v1_vectors.json` stays byte-identical, so all five polyglot clients keep
passing unchanged. `WIRE_VERSION` stays 1: adding a message type is additive within v1, the same
way `NackReason` bytes `0x0A..0xFF` are reserved for additive growth.

The one published statement this weakens is the producer checklist's "every weir response payload
is ≤ 2 bytes". Its operational content — *cap the response payload before allocating* — survives
intact and gets sharper: the cap is now per response type, and only a client that opted into
`PushTracked` needs the larger one.

### Where the coordinate comes from

`RecordId::for_record(segment, index, payload)` (weir-sink-sdk) already exists, and the drain
already computes it per record from `segment_identity(sealed_path)` and a **1-based** `read_index`
(`drain/mod.rs`). The write side must produce the identical string and index or the whole feature
is a lie, so:

- `segment_identity` moves out of `drain/mod.rs` into `wab::segment` as the single definition, and
  both sides call it. A test pins write-side output against drain-side output for the same record.
- At write time the segment is not sealed yet, so the coordinate names the **predicted sealed
  path** — `sealed_path_for(active)`. That prediction is exact on every path that can deliver the
  record: rotation (`WabSegment::seal`), idle/shutdown seal (`seal_current`), and crash recovery
  (`recovery.rs:655`) all derive the sealed name with `sealed_path_for`. Recovery truncates a torn
  tail, which never renumbers the surviving prefix.
- `ShardWriter::write_record` gains the 1-based index in its return value (a `u64`, free) and a
  borrowing `active_segment_path()`. No `PathBuf` is cloned for an untracked record.
- The `RecordId` (SHA-256) is computed in the **socket layer**, not the flusher thread: `Payload`
  is `Bytes`, so the socket keeps an O(1) clone for tracked pushes only and hashes off the WAB's
  critical path.

### Honest limits (these go in the docs, not just here)

1. **Not a gap-free sequence.** The coordinate is a *buffer address*, not a per-producer counter.
   Records from other producers interleave, so one producer's indices have holes. `(shard, segment
   counter, index)` is a total order per shard, which is enough to sort and to detect *duplicates* —
   it is not enough for a statute that mandates gap-free sequential numbering. Say so plainly.
2. **`Buffered` returns a coordinate too**, and that record can still be lost on power loss. The
   coordinate says where the record *is*, not that it survived — the durability tier still governs
   that.
3. **A quarantined segment breaks the prediction's usefulness.** If crash recovery quarantines the
   segment, the records in it never drain, so the coordinate names an address nothing delivers.
   Consistent with at-least-once, but it must be written down.
4. **At-least-once still means duplicates.** Two coordinates can point at the same logical event if
   the producer retried; the coordinate deduplicates *deliveries*, not *intents*.

---

## Task list

- [ ] **T1 — `weir-core`: the types.** `MessageType::PushTracked = 0x06` / `AckTracked = 0x07`;
      a `RecordCoordinate { segment: String, index: u64, record_id: [u8; 32] }` with
      `encode()`/`decode()` and a `CoordinateDecodeError`. No new dependencies (the digest is
      carried, never computed, here). Tests: round-trip, every rejection arm, `MessageType`
      byte pinning.
- [ ] **T2 — Regression fence.** Confirm `cargo test -p weir-core --test conformance` and
      `python3 docs/conformance/run_vectors.py` still pass with T1 landed and the JSON untouched.
- [ ] **T3 — `weir-server`: shared `segment_identity`.** Move it to `wab::segment`, have `drain`
      call it, add the drift-guard test (write N records → capture write-side coordinates → seal →
      re-derive with `SegmentReader` + the drain's own code path → assert equal).
- [ ] **T4 — `weir-server`: plumb the coordinate.** `WorkUnit.ack_tx` becomes
      `oneshot::Sender<AckOutcome>` carrying `durable: bool` + `coordinate: Option<Box<...>>`;
      `flush_batch` fills it in only when the unit asked for it.
- [ ] **T5 — `weir-server`: the socket path.** Dispatch `PushTracked`, compute the `RecordId`,
      send `AckTracked`. A tracked push that acks durable but arrives with no coordinate is a
      loud `Nack(InternalError)` + `error!`, never a fabricated coordinate.
- [ ] **T6 — `weir-client`: `push_tracked`.** Returns `Result<RecordCoordinate, ClientError>`.
      Response-payload cap becomes per-message-type so `Ack`/`Nack` keep the existing 2-byte
      bound exactly.
- [ ] **T7 — Tracked conformance vectors.** A *separate* `wire_v1_tracked_vectors.json` plus
      generator, checked by `run_vectors.py` and by weir-core's conformance test. The five
      polyglot clients keep reading only `wire_v1_vectors.json`.
- [ ] **T8 — System test.** Real daemon via testkit: `push_tracked` twice, assert the second
      index is the first + 1 in the same segment, and that a plain `push` on the same connection
      still gets a bare Ack.
- [ ] **T9 — Docs + CHANGELOG.** `wire_protocol.md` (message table, payload layout, worked byte
      example, revised response cap), `conformance.md`, and a self-contained `## [Unreleased]`
      entry.

## Gate

`cargo fmt --all --check`; `cargo clippy --all-targets --all-features -- -D warnings`;
`cargo test --workspace`; `python3 docs/conformance/run_vectors.py`; and the five polyglot demo
clients if any file they read changed (expected: none).
