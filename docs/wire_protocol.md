# weir Wire Protocol — v1

## Overview

weir uses a binary framing protocol over Unix sockets. Each frame is a fixed 16-byte header followed by a variable-length payload and a 4-byte payload CRC32.

The `WIRE_VERSION` constant in `weir-core::version` is the single source of truth. Version negotiation is strict-equality: no forward or backward compatibility across versions.

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

## Version history

### v1 (current)

- Initial implementation.
- 16-byte fixed header with little-endian multi-byte fields.
- Header CRC covers bytes `[0..12]`; version is checked before the CRC (see decode order above).
- `VersionMismatch` Nack carries `[0x02, WIRE_VERSION]` so clients can report both sides of the mismatch.
- `MAX_PAYLOAD_HARD_CAP = 16 MiB` — absolute ceiling across all code paths.
