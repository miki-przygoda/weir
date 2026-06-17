#!/usr/bin/env python3
"""Language-neutral conformance runner for the weir wire protocol (v1).

`gen_vectors.py` *produces* `wire_v1_vectors.json`; this script *checks a codec
against* it. It ships a small pure-Python reference codec (stdlib only —
`zlib`, `struct`) and runs it over every vector:

  * every vector's bytes are decoded and the result is compared to the
    expected `decode` outcome ("ok" or a specific rejection reason); and
  * every `decode == "ok"` vector is re-encoded and must reproduce `hex`
    byte-for-byte (catches endianness / CRC-coverage mistakes).

Run it:

    python3 docs/conformance/run_vectors.py

Exit code is non-zero on any mismatch, so it works as a CI gate.

**Validating your own (non-Rust) client:** replace the two functions in the
"REFERENCE CODEC" section below — `decode_frame()` and `encode_frame()` — with
thin adapters over your implementation, and run this harness unchanged. The
canonical CRC is `zlib.crc32` (IEEE / ISO-3309, the same polynomial as Go's
`hash/crc32.IEEETable` and Java's `java.util.zip.CRC32`) — NOT CRC-32C.
"""

import json
import pathlib
import struct
import sys
import zlib

VECTORS = pathlib.Path(__file__).with_name("wire_v1_vectors.json")

MAGIC = b"WEIR"
WIRE_VERSION = 1
HEADER_LEN = 16

MT = {0x01: "Push", 0x02: "Ack", 0x03: "Nack", 0x04: "HealthCheck", 0x05: "HealthCheckResponse"}
DUR = {0x01: "Sync", 0x02: "Batched", 0x03: "Buffered"}
MT_REV = {v: k for k, v in MT.items()}
DUR_REV = {v: k for k, v in DUR.items()}


# ── REFERENCE CODEC (replace these two functions to test your own client) ──────

def decode_frame(buf: bytes, max_payload_bytes: int):
    """Decode exactly one frame. Returns ("ok", fields) or (reason, None).

    Follows the mandatory decode order from docs/wire_protocol.md. `reason` is
    the rejection tag used in the vectors' `decode` field.
    """
    # 1. Magic — valid magic but a short buffer is TruncatedFrame, not BadMagic.
    n = min(len(buf), 4)
    if buf[:n] != MAGIC[:n]:
        return "BadMagic", None
    if len(buf) < HEADER_LEN:
        return "TruncatedFrame", None

    # 2. Version (before header CRC, so a v2 client gets an actionable error).
    if buf[4] != WIRE_VERSION:
        return "VersionMismatch", None

    # 3. Header CRC over bytes [0..12].
    (header_crc,) = struct.unpack_from("<I", buf, 12)
    if zlib.crc32(buf[:12]) & 0xFFFFFFFF != header_crc:
        return "HeaderCrcMismatch", None

    # 4. Header fields — only interpreted after the header CRC passes.
    message_type = buf[5]
    durability = buf[6]
    flags = buf[7]
    if message_type not in MT:
        return "UnknownMessageType", None
    if durability not in DUR:
        return "UnknownDurability", None
    if flags != 0:
        return "ReservedFlagsSet", None

    # 5. Payload length cap — checked before any allocation / payload read.
    (payload_len,) = struct.unpack_from("<I", buf, 8)
    if payload_len > max_payload_bytes:
        return "PayloadTooLarge", None

    # Exactly-one-frame: shorter buffer = TruncatedFrame, longer = TrailingBytes.
    expected = HEADER_LEN + payload_len + 4
    if len(buf) < expected:
        return "TruncatedFrame", None
    if len(buf) > expected:
        return "TrailingBytes", None

    # 6/7. Payload + payload CRC.
    payload = buf[HEADER_LEN:HEADER_LEN + payload_len]
    (payload_crc,) = struct.unpack_from("<I", buf, HEADER_LEN + payload_len)
    if zlib.crc32(payload) & 0xFFFFFFFF != payload_crc:
        return "PayloadCrcMismatch", None

    return "ok", {
        "message_type": MT[message_type],
        "durability": DUR[durability],
        "flags": flags,
        "payload_hex": payload.hex(),
    }


def encode_frame(message_type: str, durability: str, flags: int, payload: bytes) -> bytes:
    """Encode one frame. Inverse of decode_frame for `decode == "ok"` vectors."""
    header = bytearray(HEADER_LEN)
    header[0:4] = MAGIC
    header[4] = WIRE_VERSION
    header[5] = MT_REV[message_type]
    header[6] = DUR_REV[durability]
    header[7] = flags
    struct.pack_into("<I", header, 8, len(payload))
    struct.pack_into("<I", header, 12, zlib.crc32(bytes(header[:12])) & 0xFFFFFFFF)
    return bytes(header) + payload + struct.pack("<I", zlib.crc32(payload) & 0xFFFFFFFF)


# ── HARNESS (no need to touch) ─────────────────────────────────────────────────

def main() -> int:
    doc = json.loads(VECTORS.read_text())
    cap = doc["max_payload_hard_cap"]
    vectors = doc["vectors"]
    passed = failed = 0

    for v in vectors:
        name = v["name"]
        buf = bytes.fromhex(v["hex"])
        reason, fields = decode_frame(buf, cap)

        if reason != v["decode"]:
            print(f"FAIL {name}: decode = {reason!r}, expected {v['decode']!r}")
            failed += 1
            continue

        if v["decode"] == "ok":
            mismatch = next(
                (
                    f"{k}={fields[k]!r} != {v[k]!r}"
                    for k in ("message_type", "durability", "flags", "payload_hex")
                    if fields[k] != v[k]
                ),
                None,
            )
            if mismatch:
                print(f"FAIL {name}: decoded field {mismatch}")
                failed += 1
                continue
            payload = bytes.fromhex(v["payload_hex"])
            re_encoded = encode_frame(v["message_type"], v["durability"], v["flags"], payload).hex()
            if re_encoded != v["hex"]:
                print(f"FAIL {name}: re-encode\n  got {re_encoded}\n  exp {v['hex']}")
                failed += 1
                continue

        passed += 1

    total = passed + failed
    print(f"\n{passed}/{total} vectors passed" + (f", {failed} FAILED" if failed else " — all good"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
