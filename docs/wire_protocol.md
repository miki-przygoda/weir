# weir Wire Protocol — v1

## Overview

weir uses a binary framing protocol over Unix sockets. Each frame is a fixed 16-byte header followed by a variable-length payload and a 4-byte payload CRC32.

The `WIRE_VERSION` constant in `weir-core::version` is the single source of truth. Version negotiation is strict-equality: no forward or backward compatibility across versions. A future-version frame is not silently parsed as v1 — only `version == WIRE_VERSION` is accepted. This is intentional: silently parsing a v2 frame as v1 would corrupt the record when the v2 layout differs.

The payload is opaque bytes to weir. `weir-core` exposes `Payload`, a newtype over ref-counted `bytes::Bytes` that derefs to `[u8]` (so clones through the drain are O(1)). Producers choose their own serialisation format (protobuf, bincode, raw, etc.).

---

## Frame layout

```
Offset  Size  Field           Description
──────  ────  ─────────────── ────────────────────────────────────────────────
 0       4    magic           b"WEIR" — identifies a weir frame; fixed offset
                              allows stream resync by scanning for this pattern
 4       1    version         WIRE_VERSION (currently 1)
 5       1    message_type    MessageType variant (see below)
 6       1    durability      Durability tier (see below)
 7       1    flags           Reserved; must be zero on write
 8       4    payload_len     u32 little-endian; payload byte count (excl. CRC)
12       4    header_crc32    CRC32 of bytes [0..12], little-endian
16     var    payload         payload_len bytes
16+n    4    payload_crc32   CRC32 of payload bytes, little-endian
```

Total frame size: `16 + payload_len + 4` bytes.

---

## Message types

| Byte | Name                  | Direction        |
|------|-----------------------|------------------|
| 0x01 | Push                  | client → daemon  |
| 0x02 | Ack                   | daemon → client  |
| 0x03 | Nack                  | daemon → client  |
| 0x04 | HealthCheck           | client → daemon  |
| 0x05 | HealthCheckResponse   | daemon → client  |
| 0x06 | PushTracked           | client → daemon  |
| 0x07 | AckTracked            | daemon → client  |

`PushTracked` / `AckTracked` are an **optional, additive** pair — see
[Tracked push](#tracked-push--pushtracked--acktracked). A client that does not
implement them is fully conformant and is unaffected by their existence: it
never sends `0x06`, so it never receives an `0x07`, and its `Push` still gets
the same 20-byte `Ack` it always did.

Message-type bytes `0x08`–`0xFF` are unassigned. A daemon rejects an
unrecognised one with `Nack(UnknownMessage 0x08)` and closes the connection —
which is also how a client discovers that a daemon predates a type it wanted to
use.

---

## Durability tiers

| Byte | Name      | Guarantee                                                            |
|------|-----------|------------------------------------------------------------------------|
| 0x01 | Durable   | Group fdatasync at the batch boundary before Ack — on stable storage  |
| 0x02 | *retired* | Formerly `Batched`. Still **decodes** to `Durable`; never emitted.    |
| 0x03 | Buffered  | Ack after memory write; fsync is deferred                             |

> **`0x02` is retired and permanently reserved.** It was `Batched`, which
> fsynced once per batch while `Sync` fsynced once per record; both moved to the
> batch-boundary group fsync and the distinction stopped existing. A daemon
> still accepts `0x02` so producers built against weir 1.x keep working, and
> canonicalises it to `Durable` — the encoder never emits `0x02`. It will not be
> reassigned to a future tier, because that would silently mis-tier every 1.x
> client.

**The tier is per record, and a connection may mix them freely.** It is a
header byte on each envelope, not a handshake value, a connection mode, or a
daemon setting. One producer can send `Durable` for the record it cannot lose
and `Buffered` for the telemetry line beside it, on the same socket, in any
order, with no reconnect and no negotiation.

The daemon dispatches per record inside a single flush batch: a `Buffered`
record is acked as soon as its memory write lands, while a `Durable` record in
that same batch waits for the batch-boundary group fsync. If a batch happens to
contain no `Durable` record, the fsync is skipped entirely. A client
implementing this protocol therefore needs no per-connection state for
durability — read the byte the caller asked for, write it, move on.

---

## Nack payload format

Every Nack frame carries at least one byte in its payload:

```
Byte 0: NackReason
Byte 1: (VersionMismatch only) daemon's WIRE_VERSION
```

| Byte | NackReason        | Extra bytes                          |
|------|-------------------|--------------------------------------|
| 0x01 | BadMagic          | none                                 |
| 0x02 | VersionMismatch   | daemon WIRE_VERSION (1 byte)         |
| 0x03 | BadHeaderCrc      | none                                 |
| 0x04 | PayloadTooLarge   | none                                 |
| 0x05 | BadPayloadCrc     | none                                 |
| 0x06 | InternalError     | none                                 |
| 0x07 | EmptyPayload      | none                                 |
| 0x08 | UnknownMessage    | none                                 |
| 0x09 | ReservedFlagsSet  | none                                 |

Reason bytes `0x0A`–`0xFF` are reserved for future use; a client that receives
an unrecognised reason byte should surface it (e.g. log the raw byte) rather
than assume a specific meaning.

`UnknownMessage` (`0x08`) is sent when a frame's header passes magic / version /
header-CRC validation but the daemon will not act on the message: either the
`message_type` or `durability` byte is unrecognised (typically version skew), **or
the `message_type` is a valid daemon→client type** (`Ack` `0x02`, `Nack` `0x03`,
`HealthCheckResponse` `0x05`, or `AckTracked` `0x07`) that a client must only
ever *receive*, never *send*. All of these are **permanent** protocol errors. It is distinct from
`InternalError`: the daemon **closes** the connection after an `UnknownMessage`,
and retrying the identical frame will not succeed (so a client must not retry on
the same connection).

`ReservedFlagsSet` (`0x09`) is sent when a frame's header is otherwise valid but
sets one or more bits in the reserved `flags` byte (byte 7), which **must be
zero** in wire v1. The daemon rejects such a frame rather than silently ignoring
the flag — a producer must never believe an unrecognised flag took effect. Like
`UnknownMessage` this is **permanent** and the daemon **closes** the connection
after it. Introducing flag semantics in a future version is therefore an
explicit, `WIRE_VERSION`-gated change.

The `VersionMismatch` second byte lets a client produce a specific error:
> "daemon is on wire protocol v1; this client is built against v2 — upgrade the daemon or downgrade the client."

---

## Tracked push — `PushTracked` / `AckTracked`

A `Push` tells a producer its record was accepted. It does not say **which**
record it was. `PushTracked` asks that question, and `AckTracked` answers it with
the record's **coordinate**: the segment the record landed in, its index within
that segment, and the `record_id` derived from those two plus the payload.

`PushTracked` is a `Push` in every other respect — same durability tiers, same
payload cap, same empty-payload rejection, same Nack reasons, same connection
effects. The only difference on the wire is the message-type byte and the shape
of the success reply.

### Why a new message type rather than a longer Ack

Because the alternatives all desync a deployed client:

- A **longer `Ack`** would be read as 20 bytes by every existing client, leaving
  the coordinate's bytes in the buffer to be mis-read as the *next* reply — a
  false ack.
- A **connect-time handshake** would make what an `Ack` means depend on
  invisible prior state, which is the same desync with more moving parts (and
  this protocol deliberately has no in-band handshake).
- A **bit in the reserved `flags` byte** would give one message type two
  lengths, and would spend a flag whose semantics `wire_v1` reserves for an
  explicit, `WIRE_VERSION`-gated change (see `ReservedFlagsSet` above).

A distinct message type keeps the invariant that makes the protocol safe to
extend at all: **the message type determines the response shape.**

### Negotiation

There is none, and none is needed. A client sends `PushTracked`; a daemon that
does not know the byte answers `Nack(UnknownMessage 0x08)` and closes — the
protocol's existing permanent-error path for version skew. Receiving that Nack
from a `PushTracked` means "this daemon does not support tracked pushes"; fall
back to `Push` on a fresh connection.

### `AckTracked` payload — the record coordinate

```
Offset  Size  Field                Description
──────  ────  ───────────────────  ─────────────────────────────────────────────
 0       1    coordinate_version   currently 0x01
 1       8    index                u64 LE — 1-based ordinal within the segment
 9      32    record_id            SHA-256 digest (see below)
41       2    segment_len          u16 LE — byte length of `segment`
43     var    segment              UTF-8 segment address
```

Total payload: `43 + segment_len` bytes, capped at **298** (`segment_len <= 255`).

`coordinate_version` leads so the coordinate can grow inside wire v1 without a
new message type: a reader that meets a version it does not know **must reject
the frame**, not parse a layout it has never seen. The buffer must be *exactly*
one coordinate — a trailing byte means the two ends disagree about the layout,
and quietly using the prefix is how that disagreement becomes a wrong address.

`record_id` is `SHA-256` over
`segment_len ++ segment ++ index ++ payload_len ++ payload`, every length a
little-endian `u64`. It is byte-identical to the per-record idempotency key the
daemon's drain hands a sink (the HTTP sink's `Idempotency-Key: sha256:<hex>`),
which is what lets a producer correlate what it sent with what the downstream
received. The length framing is what stops `("ab", 1)` and `("a", 0xb…)` from
colliding.

`segment` is `<shard-dir>/<file-name>`, e.g. `shard_00/seg_00000001.wab.sealed`.
Treat it as an **opaque, stable address**, not a filesystem path: it names the
segment the drain will read the record from. At ack time that segment is still
open, so the name is the daemon's *prediction* of its sealed form — exact on
every path that can deliver the record.

### What a coordinate does and does not tell you

- It is **not a delivery receipt.** An `AckTracked`, like an `Ack`, means the
  record is durably buffered at the requested tier — not that it has reached the
  sink. The coordinate says where it will be read from, not that it was read.
- It is **not a gap-free sequence.** The coordinate is a buffer address. Records
  from other producers interleave, so one producer's indices have holes.
  `(segment, index)` gives a total order per shard and identifies a record
  uniquely; it does not number a producer's own records consecutively.
- It is **not a durability upgrade.** A `Buffered` push receives a coordinate
  and can still be lost on power loss. The tier still decides that.
- **At-least-once still applies.** Two coordinates can point at the same logical
  event if the producer retried. The coordinate deduplicates *deliveries*, not
  *intents*.
- If crash recovery **quarantines** the segment, its records never drain, and
  the coordinate names an address nothing will deliver from.

### Worked example — `PushTracked` of `"hello"`, Durable

Byte-for-byte a `Push` frame except byte 5 and the header CRC that covers it.

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      06                                                 message_type = PushTracked
 6      01                                                 durability = Durable
 7      00                                                 flags
 8      05 00 00 00                                        payload_len = 5
12      e8 93 da f9                                        header_crc32
16      68 65 6c 6c 6f                                     payload = "hello"
21      86 a6 10 36                                        payload_crc32
```

Total: 25 bytes.

### Worked example — the `AckTracked` reply

For a record at index 7 of `shard_00/seg_00000001.wab.sealed`:

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      07                                                 message_type = AckTracked
 6      01                                                 durability = Durable (filler)
 7      00                                                 flags
 8      4b 00 00 00                                        payload_len = 75
12      52 b7 66 24                                        header_crc32
16      01                                                 coordinate_version = 1
17      07 00 00 00 00 00 00 00                            index = 7
25      ce 1e d3 cf ba 2b d8 f4 …  (32 bytes)              record_id
57      20 00                                              segment_len = 32
59      73 68 61 72 64 5f 30 30 …  (32 bytes)              "shard_00/seg_00000001.wab.sealed"
91      7b 44 b9 cb                                        payload_crc32
```

Total: 95 bytes. As on a plain `Ack`, the `durability` byte is fixed filler
(`0x01`) regardless of the request's tier.

These bytes are asserted against the encoder by
[`conformance/wire_v1_tracked_vectors.json`](conformance/wire_v1_tracked_vectors.json)
— a **separate** file from the frozen v1 vectors, for the reason given in
[`conformance.md`](conformance.md#the-tracked-push-extension--a-second-separate-file).

---

## Frame decode order (server-side)

The server decodes in this order to minimise DoS surface. **This order is mandatory and must not be changed.**

1. **Length, then magic** — a buffer shorter than the 16-byte header is `TruncatedFrame` **regardless of its leading bytes**; magic is only interpreted once a complete header is present. So a 1–15 byte buffer is *always* `TruncatedFrame` — never `BadMagic`, even if its bytes differ from `WEIR`. Once a full header is present, magic is the cheapest check and eliminates non-weir traffic before any further work. (Length-before-magic is the reference `Envelope::decode` order in `weir-core`; conformance vector `reject_partial_magic_short` pins it.)
2. **Version** — checked before header CRC so a v2 client gets `VersionMismatch` (actionable) rather than `HeaderCrcMismatch` (confusing) when the frame layout has shifted between versions.
3. **Header CRC** — validates the remaining header bytes are uncorrupted.
4. **Header field parsing** — only after the header CRC passes are the `message_type`, `durability`, and reserved `flags` bytes interpreted. An unknown `message_type` (or `durability`) yields `UnknownMessage`; a nonzero reserved `flags` byte yields `ReservedFlagsSet`. Both close the connection. (The sub-header length check in step 1 lives in `Envelope::decode`, which slices the buffer; magic through field parsing — the rest of steps 1–4 — are `Header::decode`, which takes a fixed 16-byte header.)
5. **Payload length cap** — `min(config.max_payload_bytes, MAX_PAYLOAD_HARD_CAP)` checked **before any heap allocation**. Exceeding the cap returns `PayloadTooLarge` and closes the connection without reading the payload bytes. A zero-length `Push` payload is rejected here with `EmptyPayload` (a `HealthCheck` legitimately carries none).
6. **Payload read** — only after the cap check passes.
7. **Payload CRC** — validates the payload bytes before the record is queued.

The daemon reads the header and payload separately (it knows `payload_len` from
the header before reading the payload), so it always consumes exactly one frame
off the wire. Framing is the reader's responsibility.

### Reference codec: one buffer, one frame

The `weir-core` reference codec (`Envelope::decode`, the executable definition of
this format) requires its input buffer to be **exactly one frame** —
`HEADER_LEN + payload_len + 4` bytes:

- a shorter buffer is rejected as `TruncatedFrame`;
- a longer buffer is rejected as `TrailingBytes` (carrying the excess length).

It does **not** decode the first frame and discard the remainder. An
implementation that reads from a stream must therefore frame the bytes itself
(read the 16-byte header, take `payload_len`, then read exactly
`payload_len + 4` more bytes) rather than handing a multi-frame buffer to a
single decode call. This keeps a desynced or concatenated stream from silently
losing records (G18).

`TruncatedFrame` (and `TrailingBytes`) are therefore **decoder-only verdicts** —
they describe the reference codec's *exactly-one-frame* contract. A streaming
reader never observes them: a short read is just "read more bytes" (or, past
`connection_read_timeout_secs`, a timeout), and an over-long buffer never arises
because the reader takes exactly `payload_len + 4` bytes. The daemon itself reads
the header and payload separately and so never produces `TruncatedFrame` or
`TrailingBytes` on the wire — there is no Nack reason for either (see the
[decoder-tag → wire-Nack mapping](conformance.md#decoder-tag--wire-nack-byte)).

---

## CRC32 algorithm

The two CRC fields use the **CRC-32 / ISO 3309** variant — the same
algorithm as zlib, PNG, and Ethernet. Concretely:

| Parameter | Value |
|-----------|-------|
| Width | 32 bits |
| Polynomial | `0x04C11DB7` |
| Initial value | `0xFFFFFFFF` |
| Reflect input bytes | yes |
| Reflect output | yes |
| Final XOR | `0xFFFFFFFF` |

This is the algorithm exposed by the Rust [`crc32fast`](https://crates.io/crates/crc32fast)
crate (`crc32fast::hash(bytes) -> u32`), Python's `zlib.crc32`,
Go's `hash/crc32.IEEETable`, Java's `java.util.zip.CRC32`, and Node's
`node:zlib.crc32` (Node >= 22.2). It is
**not** CRC-32C (Castagnoli, polynomial `0x1EDC6F41`) — using
CRC-32C will produce frames the daemon rejects with
`BadHeaderCrc` or `BadPayloadCrc`.

The CRC's empty-input value is `0x00000000` (a CRC over zero bytes
returns the all-zero post-XOR value). A `HealthCheckResponse` frame
therefore ends with the four bytes `00 00 00 00`.

**CRC32 guards against accidental corruption, not malicious tampering.**
Anyone with read/write access to the daemon's Unix socket can craft a
frame with a valid header and payload CRC; the algorithm has no key
and is publicly defined. The trust boundary is the socket file's mode
(`0o600` — see [Socket setup](#socket-setup) below), not the CRC. Treat
producers as authenticated by filesystem permissions, not by any
property of the wire frame itself.

## Connection lifecycle

Each connection is a serial request/response stream — the server reads
one frame at a time on a given connection, processes it, and writes
the response before reading the next frame. A client **may** write
multiple Push frames back-to-back without waiting for each Ack
(pipelining at the kernel level), but the server still processes them
in order and emits one response per request, in submission order. The
in-flight depth is bounded by the kernel socket buffer.

When the server closes a connection mid-stream the client should treat
in-flight Pushes (those without a matching Ack/Nack received yet) as
**unknown outcomes** — they may or may not have been durably written
depending on where in the pipeline the close happened. Retry on a
fresh connection.

**Cap the response `payload_len` before allocating, using the cap for the type
the header declares.** When reading a *response* header, reject (and close the
connection) any declared `payload_len` larger than that type's cap *before*
allocating a buffer — a larger length is a desync or a non-weir peer, and
honouring an attacker-chosen length would allocate an arbitrary buffer.

| Response type          | Max payload | Note                                  |
|------------------------|-------------|---------------------------------------|
| `Ack`                  | 0           |                                       |
| `HealthCheckResponse`  | 0           |                                       |
| `Nack`                 | 2           | 1 reason byte; 2 for `VersionMismatch`|
| `AckTracked`           | 298         | one record coordinate                 |

A client that does not implement tracked pushes never sends a `PushTracked`, so
it can never receive an `AckTracked` — **≤ 2 bytes remains the correct cap for
every response it can see**, exactly as before. This is the read-side mirror of
the send-path cap and is also enumerated in the
[producer checklist](#minimum-producer-checklist).

### When the server keeps the connection open

| Event | Connection |
|-------|-----------|
| Push → Ack | open |
| PushTracked → AckTracked | open |
| Push → Nack(InternalError) | open — covers transient daemon-side conditions: queue saturation, ack timeout, a non-durable write (write/fsync error), or the daemon's `wab_max_bytes` cap rejecting because its write-ahead buffer is full. The record's durable outcome is **unknown** for the first three (the producer should retry); a cap rejection is definitively *not* durable, but it is not distinguishable on the wire — the daemon's `weir_wab_cap_rejections_total` metric is what separates it, so a producer treats all four the same way and retries. |
| HealthCheck → HealthCheckResponse | open |

### When the server closes the connection

| Event | Connection |
|-------|-----------|
| Push with bad magic | closed after Nack(BadMagic) |
| Push with unknown version | closed after Nack(VersionMismatch) |
| Push with bad header CRC | closed after Nack(BadHeaderCrc) |
| Push with `payload_len > cap` | closed after Nack(PayloadTooLarge) |
| Push (or PushTracked) with a zero-length payload | closed after Nack(EmptyPayload) |
| Push with bad payload CRC | closed after Nack(BadPayloadCrc) |
| Push with unknown message_type / durability | closed after Nack(UnknownMessage) |
| Client sends a daemon→client message type (Ack / Nack / HealthCheckResponse / AckTracked) | closed after Nack(UnknownMessage) |
| Push with a nonzero reserved `flags` byte | closed after Nack(ReservedFlagsSet) |
| Idle past `connection_read_timeout_secs` mid-frame | closed silently (slowloris guard); no Nack |

Validation-failure closes are deliberate — once the framing has
desynced (bad magic, bad CRC) the server cannot trust subsequent
bytes on the same connection.

## Socket setup

- The daemon binds an `AF_UNIX SOCK_STREAM` socket at the path in
  `socket_path` (default `/run/weir/weir.sock`).
- The socket file is created with mode `0o600` — only the daemon's
  uid can connect. Producers must run as the same uid (or root).
- The parent directory must exist before the daemon starts; the
  daemon does not create it.
- **`sun_path` length cap (non-Rust producers).** A `sockaddr_un.sun_path` is a
  fixed-size buffer — roughly **104 bytes on macOS/BSD and 108 bytes on Linux,
  including the NUL terminator**. A socket path at or over that length fails with
  `ENAMETOOLONG` at `connect()` (or `bind()` on the daemon side). Keep the socket
  path comfortably short, or `chdir` near it and connect via a relative path.
- There is no in-band handshake. Producers connect and immediately
  send a Push (or HealthCheck) frame.

## Worked examples

The byte sequences below are real — they're asserted against the
encoder in `crates/weir-core/tests/reference_frames.rs`.
A client implementation that produces these exact bytes is
wire-compatible with the daemon.

### Push of a 5-byte payload (`"hello"`), Durable durability

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      01                                                 message_type = Push
 6      01                                                 durability = Durable
 7      00                                                 flags
 8      05 00 00 00                                        payload_len = 5
12      66 ad 7d 3c                                        header_crc32
16      68 65 6c 6c 6f                                     payload = "hello"
21      86 a6 10 36                                        payload_crc32
```

Total: 25 bytes on the wire.

The header CRC covers bytes `[0..12]` (everything from the magic
through the payload-length field). The payload CRC covers exactly
the payload bytes (5 in this case).

### Ack response

The daemon's Ack frame is fixed: zero-length payload, all-zero
payload CRC, header CRC computed over the same first 12 bytes.

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      02                                                 message_type = Ack
 6      01                                                 durability = Durable (filler)
 7      00                                                 flags
 8      00 00 00 00                                        payload_len = 0
12      c9 47 4b 3a                                        header_crc32
16      00 00 00 00                                        payload_crc32 (empty payload)
```

Total: 20 bytes. The `durability` byte on the response is always
`0x01` (Durable) regardless of the original request's tier — the server
populates it to a fixed value.

> **Do not read it as confirmation of the tier.** A client that asserts
> `ack.durability == request.durability` passes every `Durable` test it writes
> and then rejects every `Buffered` ack in production. Ignore the field on
> responses; an Ack means the record was accepted under the tier *you* sent,
> and the guarantee that tier carries is described above.

### Nack response — `PayloadTooLarge`

Nack payloads carry the `NackReason` byte (see table above). For
`PayloadTooLarge` the payload is exactly one byte (`0x04`).

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      03                                                 message_type = Nack
 6      01                                                 durability = Durable (filler)
 7      00                                                 flags
 8      01 00 00 00                                        payload_len = 1
12      18 2b 80 24                                        header_crc32
16      04                                                 NackReason::PayloadTooLarge
17      94 2b 6f d5                                        payload_crc32 (of [0x04])
```

Total: 21 bytes.

For `VersionMismatch` the payload is two bytes:
`[0x02, daemon_wire_version]`. All other Nack reasons send a
single-byte payload.

### HealthCheck request and response

A HealthCheck request has a zero-length payload. The daemon does not
*act* on the `durability` field for a HealthCheck — but it still **must
be a valid durability byte** (`0x01` Durable, `0x02` *retired* — still
decodes to Durable, or `0x03` Buffered), because the daemon validates the
entire header (including durability) before it dispatches on the message
type. A HealthCheck carrying an out-of-range durability byte (e.g. `0x00`)
is rejected with `UnknownDurability` → `Nack(UnknownMessage)` and the
connection is closed, exactly as any other frame with a bad durability
byte would be. Set it to `0x01` (Durable) by convention; the canonical
`healthcheck` [conformance vector](conformance.md) does. The
HealthCheckResponse mirrors the shape — zero payload, all-zero payload CRC.

The canonical HealthCheck carries a **zero-length** payload. The daemon is
currently **lenient** on this point: a HealthCheck frame that declares a
non-empty payload (with a matching, CRC-valid `payload_crc32`) is still answered
with a HealthCheckResponse rather than rejected — the daemon reads and CRC-checks
the payload bytes, then dispatches on the message type and ignores the payload
contents (the zero-length empty-payload guard applies only to `Push`). Do not
rely on this: send a zero-length payload, and treat the non-empty case as
unspecified — a future version may reject it.

## Minimum producer checklist

A non-Rust client that satisfies the following is wire-compatible:

- [ ] Writes 16-byte header with little-endian multi-byte fields.
- [ ] Computes header CRC32 over bytes `[0..12]` using the CRC-32
      variant above (not CRC-32C).
- [ ] Writes `payload_len` raw payload bytes after the header.
- [ ] Writes 4-byte payload CRC32 (same algorithm) after the payload.
- [ ] Reads 16-byte response header, then `header.payload_len`
      response-payload bytes, then 4 response-CRC bytes.
- [ ] **Caps the response `payload_len` before allocating, per response type.**
      `Ack`/`HealthCheckResponse` = 0; `Nack` = 1, except `VersionMismatch` = 2.
      If — and only if — you send `PushTracked`, an `AckTracked` may carry up to
      298 bytes; a client that does not send `PushTracked` keeps **≤ 2 bytes** as
      the cap for every response it can receive. A larger declared length on a
      *response* is a desync or a non-weir peer — treat it as a protocol error
      and close the connection rather than allocating an attacker-chosen buffer.
      (This mirrors the send-path cap below.)
- [ ] Verifies the response header magic, version, and CRC before
      consuming the payload.
- [ ] Treats response `message_type == Nack` as failure; decodes the
      first byte of the response payload as `NackReason`.
- [ ] Sends a **non-empty** payload on every `Push`: a zero-length `Push`
      is rejected with `Nack(EmptyPayload 0x07)` and the connection is closed.
      To probe liveness without a payload, send a `HealthCheck` (the correct
      no-payload frame) — **not** an empty `Push`.
- [ ] Caps its **send** `payload_len` at the **effective** cap —
      `min(configured max_payload_bytes, MAX_PAYLOAD_HARD_CAP)` — not just at
      the 16 MiB hard cap. The daemon's `max_payload_bytes` (default 16 MiB,
      but lower in many deployments) may be **below** the hard cap, so the
      effective boundary is the smaller of the two and a client cannot know it
      a priori. Treat a `Nack(PayloadTooLarge 0x04)` as **authoritative**: the
      frame exceeded the daemon's effective cap regardless of the client's own
      estimate. Sending larger frames is wasted I/O; the daemon closes the
      connection after the Nack without reading the payload.
- [ ] Handles connection close mid-stream as "in-flight requests had
      unknown outcomes; retry on a fresh connection."

## Test vectors

The `tests/reference_frames.rs` test module in `weir-core` exports
the byte sequences from the [Worked examples](#worked-examples)
section as Rust constants and asserts the encoder produces them.
Run `cargo test -p weir-core --test reference_frames` to confirm a
local build matches the published wire format. Implementers of
non-Rust clients can copy the constants directly to verify their
encoders.

For a complete, **language-neutral** suite — covering every message
type, all nine Nack reason bytes, and one rejection vector per decode
error (bad magic, version mismatch, bad CRC, oversize payload,
truncation, reserved flags, trailing bytes, …) — see
[`conformance.md`](conformance.md) and the machine-readable
[`conformance/wire_v1_vectors.json`](conformance/wire_v1_vectors.json).
weir's own decoder is checked against that file by
`cargo test -p weir-core --test conformance`.

---

## Version history

### v1 (current)

- Initial implementation.
- 16-byte fixed header with little-endian multi-byte fields.
- Header CRC covers bytes `[0..12]`; version is checked before the CRC (see decode order above).
- `VersionMismatch` Nack carries `[0x02, WIRE_VERSION]` so clients can report both sides of the mismatch.
- `MAX_PAYLOAD_HARD_CAP = 16 MiB` — absolute ceiling across all code paths.

#### Additive extensions within v1

`WIRE_VERSION` stays 1 for anything that a v1 client can ignore without being
wrong. Two such growth points exist, both already reserved by this document:

- **Nack reason bytes `0x0A`–`0xFF`** — a client surfaces an unrecognised reason
  rather than assuming a meaning.
- **Message-type bytes `0x08`–`0xFF`** — a daemon (or client) that does not know
  a type rejects it with `UnknownMessage`, which is an actionable, permanent
  error rather than a misparse.

`PushTracked` (`0x06`) / `AckTracked` (`0x07`) were added this way. No byte a v1
client reads or writes changed, and the frozen vectors in
`conformance/wire_v1_vectors.json` are untouched.
