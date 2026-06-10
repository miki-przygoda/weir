# weir Wire Protocol — v1

## Overview

weir uses a binary framing protocol over Unix sockets. Each frame is a fixed 16-byte header followed by a variable-length payload and a 4-byte payload CRC32.

The `WIRE_VERSION` constant in `weir-core::version` is the single source of truth. Version negotiation is strict-equality: no forward or backward compatibility across versions. A future-version frame is not silently parsed as v1 — only `version == WIRE_VERSION` is accepted. This is intentional: silently parsing a v2 frame as v1 would corrupt the record when the v2 layout differs.

The payload is opaque bytes to weir. `weir-core` exposes `pub type Payload = Vec<u8>`. Producers choose their own serialisation format (protobuf, bincode, raw, etc.).

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

---

## Durability tiers

| Byte | Name      | Guarantee                                              |
|------|-----------|--------------------------------------------------------|
| 0x01 | Sync      | fdatasync before Ack — record on stable storage        |
| 0x02 | Batched   | Group fdatasync at batch boundary before Ack           |
| 0x03 | Buffered  | Ack after memory write; fsync is deferred              |

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

The `VersionMismatch` second byte lets a client produce a specific error:
> "daemon is on wire protocol v1; this client is built against v2 — upgrade the daemon or downgrade the client."

---

## Frame decode order (server-side)

The server decodes in this order to minimise DoS surface. **This order is mandatory and must not be changed.**

1. **Magic** — cheapest check; eliminates non-weir traffic before any further work.
2. **Version** — checked before header CRC so a v2 client gets `VersionMismatch` (actionable) rather than `HeaderCrcMismatch` (confusing) when the frame layout has shifted between versions.
3. **Header CRC** — validates the remaining header bytes are uncorrupted.
4. **Payload length cap** — `min(config.max_payload_bytes, MAX_PAYLOAD_HARD_CAP)` checked **before any heap allocation**. Exceeding the cap returns `PayloadTooLarge` and closes the connection without reading the payload bytes.
5. **Payload read** — only after the cap check passes.
6. **Payload CRC** — validates the payload bytes before the record is queued.

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
Go's `hash/crc32.IEEETable`, and Java's `java.util.zip.CRC32`. It is
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

### When the server keeps the connection open

| Event | Connection |
|-------|-----------|
| Push → Ack | open |
| Push → Nack(InternalError) | open — queue saturation, transient |
| HealthCheck → HealthCheckResponse | open |

### When the server closes the connection

| Event | Connection |
|-------|-----------|
| Push with bad magic | closed after Nack(BadMagic) |
| Push with unknown version | closed after Nack(VersionMismatch) |
| Push with bad header CRC | closed after Nack(BadHeaderCrc) |
| Push with `payload_len > cap` | closed after Nack(PayloadTooLarge) |
| Push with bad payload CRC | closed after Nack(BadPayloadCrc) |
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
- There is no in-band handshake. Producers connect and immediately
  send a Push (or HealthCheck) frame.

## Worked examples

The byte sequences below are real — they're asserted against the
encoder in `crates/weir-core/tests/reference_frames.rs`.
A client implementation that produces these exact bytes is
wire-compatible with the daemon.

### Push of a 5-byte payload (`"hello"`), Sync durability

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      01                                                 message_type = Push
 6      01                                                 durability = Sync
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
 6      01                                                 durability = Sync (filler)
 7      00                                                 flags
 8      00 00 00 00                                        payload_len = 0
12      c9 47 4b 3a                                        header_crc32
16      00 00 00 00                                        payload_crc32 (empty payload)
```

Total: 20 bytes. The `durability` byte on the response is always
`0x01` (Sync) regardless of the original request's tier — the server
populates it to a fixed value. Clients reading responses can ignore
it.

### Nack response — `PayloadTooLarge`

Nack payloads carry the `NackReason` byte (see table above). For
`PayloadTooLarge` the payload is exactly one byte (`0x04`).

```text
Offset  Hex bytes                                          Field
──────  ─────────────────────────────────────────────────  ───────────────────
 0      57 45 49 52                                        magic = "WEIR"
 4      01                                                 version = 1
 5      03                                                 message_type = Nack
 6      01                                                 durability = Sync (filler)
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

A HealthCheck request has a zero-length payload. The `durability`
field is unused (set to anything; the server doesn't read it). The
HealthCheckResponse mirrors the shape — zero payload, all-zero
payload CRC.

## Minimum producer checklist

A non-Rust client that satisfies the following is wire-compatible:

- [ ] Writes 16-byte header with little-endian multi-byte fields.
- [ ] Computes header CRC32 over bytes `[0..12]` using the CRC-32
      variant above (not CRC-32C).
- [ ] Writes `payload_len` raw payload bytes after the header.
- [ ] Writes 4-byte payload CRC32 (same algorithm) after the payload.
- [ ] Reads 16-byte response header, then `header.payload_len`
      response-payload bytes, then 4 response-CRC bytes.
- [ ] Verifies the response header magic, version, and CRC before
      consuming the payload.
- [ ] Treats response `message_type == Nack` as failure; decodes the
      first byte of the response payload as `NackReason`.
- [ ] Caps `payload_len` at the daemon's `MAX_PAYLOAD_HARD_CAP`
      (16 MiB) — sending larger frames is wasted I/O; the daemon
      closes the connection after the Nack.
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

---

## Version history

### v1 (current)

- Initial implementation.
- 16-byte fixed header with little-endian multi-byte fields.
- Header CRC covers bytes `[0..12]`; version is checked before the CRC (see decode order above).
- `VersionMismatch` Nack carries `[0x02, WIRE_VERSION]` so clients can report both sides of the mismatch.
- `MAX_PAYLOAD_HARD_CAP = 16 MiB` — absolute ceiling across all code paths.
