#!/usr/bin/env python3
"""Language-neutral conformance runner for the weir wire protocol (v1).

`gen_vectors.py` *produces* `wire_v1_vectors.json`; this script *checks a codec
against* it. It ships a small pure-Python reference codec (stdlib only —
`zlib`, `struct`) and runs it over every vector:

  * every vector's bytes are decoded and the result is compared to the
    expected `decode` outcome ("ok" or a specific rejection reason); and
  * every `decode == "ok"` vector is re-encoded and must reproduce `hex`
    byte-for-byte (catches endianness / CRC-coverage mistakes) — UNLESS the
    vector is marked `"round_trip": false`, meaning its bytes use a
    non-canonical wire value (the retired 0x02 durability byte) that decodes
    fine but a conformant encoder never re-emits.

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
# The tracked-push extension lives in its own file so a decoder that implements
# only message types 0x01..0x05 stays conformant by ignoring it. See
# `wire_v1_tracked_vectors.json` and the TRACKED section near the bottom.
TRACKED_VECTORS = pathlib.Path(__file__).with_name("wire_v1_tracked_vectors.json")

MAGIC = b"WEIR"
WIRE_VERSION = 1
HEADER_LEN = 16
COORDINATE_VERSION = 1
COORDINATE_FIXED_LEN = 1 + 8 + 32 + 2
MAX_SEGMENT_NAME_LEN = 255

MT = {0x01: "Push", 0x02: "Ack", 0x03: "Nack", 0x04: "HealthCheck", 0x05: "HealthCheckResponse"}
# The tracked-push types. Kept OUT of `MT` above so the v1 pass over
# `wire_v1_vectors.json` decodes exactly what a v1-only client decodes; the
# tracked pass below merges the two tables.
TRACKED_MT = {0x06: "PushTracked", 0x07: "AckTracked"}
# Decode-side: the wire byte's canonical tier name. 0x01 and 0x02 both
# canonicalise to "Durable" — 0x02 is the retired `Batched` byte, permissively
# accepted per docs/wire_protocol.md and crates/weir-core/src/durability.rs.
DUR = {0x01: "Durable", 0x02: "Durable", 0x03: "Buffered"}
MT_REV = {v: k for k, v in MT.items()}
# Encode-side table covering both passes. `MT_REV` stays v1-only for anyone who
# copies it as the definition of the frozen set.
ALL_MT_REV = {**MT_REV, **{v: k for k, v in TRACKED_MT.items()}}
# Encode-side: canonical tier name back to its ONE canonical wire byte. NOT
# derived from DUR (which is many-to-one for 0x01/0x02) — a conformant encoder
# only ever emits 0x01 for Durable, never the retired 0x02. Vectors that decode
# from the non-canonical 0x02 byte are marked "round_trip": false and skip the
# re-encode check below rather than routing through this map.
DUR_REV = {"Durable": 0x01, "Buffered": 0x03}


# ── REFERENCE CODEC (replace these two functions to test your own client) ──────

def decode_frame(buf: bytes, max_payload_bytes: int, message_types=None):
    """Decode exactly one frame. Returns ("ok", fields) or (reason, None).

    Follows the mandatory decode order from docs/wire_protocol.md. `reason` is
    the rejection tag used in the vectors' `decode` field.

    `message_types` is the table of types this decoder implements; it defaults to
    the v1 set. A decoder that does not implement tracked pushes passes the
    default and is CORRECT to reject 0x06/0x07 as UnknownMessageType.
    """
    if message_types is None:
        message_types = MT
    # 1. Length, then magic — a buffer shorter than the 16-byte header is
    #    TruncatedFrame regardless of its leading bytes; magic is only interpreted
    #    once a full header is present (length-before-magic, matching weir-core).
    if len(buf) < HEADER_LEN:
        return "TruncatedFrame", None
    if buf[:4] != MAGIC:
        return "BadMagic", None

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
    if message_type not in message_types:
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
        "message_type": message_types[message_type],
        "durability": DUR[durability],
        "flags": flags,
        "payload_hex": payload.hex(),
    }


def encode_frame(message_type: str, durability: str, flags: int, payload: bytes) -> bytes:
    """Encode one frame. Inverse of decode_frame for `decode == "ok"` vectors."""
    header = bytearray(HEADER_LEN)
    header[0:4] = MAGIC
    header[4] = WIRE_VERSION
    header[5] = ALL_MT_REV[message_type]
    header[6] = DUR_REV[durability]
    header[7] = flags
    struct.pack_into("<I", header, 8, len(payload))
    struct.pack_into("<I", header, 12, zlib.crc32(bytes(header[:12])) & 0xFFFFFFFF)
    return bytes(header) + payload + struct.pack("<I", zlib.crc32(payload) & 0xFFFFFFFF)


# ── TRACKED EXTENSION: the RecordCoordinate payload codec ─────────────────────

def decode_coordinate(buf: bytes):
    """Decode one AckTracked payload. Returns ("ok", fields) or (reason, None).

    Layout: version(1) ++ index u64 LE(8) ++ record_id(32) ++ segment_len u16
    LE(2) ++ segment. Version leads so a reader meeting a layout it does not know
    rejects it rather than mis-parsing it, and the buffer must be exactly one
    coordinate — a trailing byte means the two ends disagree, and quietly using
    the prefix is how that becomes a wrong address.
    """
    if len(buf) < COORDINATE_FIXED_LEN:
        return "Truncated", None
    if buf[0] != COORDINATE_VERSION:
        return "UnsupportedVersion", None
    (index,) = struct.unpack_from("<Q", buf, 1)
    record_id = buf[9:41]
    (segment_len,) = struct.unpack_from("<H", buf, 41)
    if segment_len > MAX_SEGMENT_NAME_LEN:
        return "SegmentTooLong", None
    if len(buf) != COORDINATE_FIXED_LEN + segment_len:
        return "LengthMismatch", None
    try:
        segment = buf[COORDINATE_FIXED_LEN:].decode("utf-8")
    except UnicodeDecodeError:
        return "SegmentNotUtf8", None
    return "ok", {"segment": segment, "index": index, "record_id_hex": record_id.hex()}


def encode_coordinate(segment: str, index: int, record_id_hex: str) -> bytes:
    seg = segment.encode()
    return (
        bytes([COORDINATE_VERSION])
        + struct.pack("<Q", index)
        + bytes.fromhex(record_id_hex)
        + struct.pack("<H", len(seg))
        + seg
    )


# ── HARNESS (no need to touch) ─────────────────────────────────────────────────

def check_tracked() -> tuple:
    """Runs the tracked-extension vectors. Returns (passed, failed)."""
    if not TRACKED_VECTORS.exists():
        return 0, 0
    doc = json.loads(TRACKED_VECTORS.read_text())
    cap = 16 * 1024 * 1024
    types = {**MT, **TRACKED_MT}
    passed = failed = 0

    for v in doc["frame_vectors"]:
        buf = bytes.fromhex(v["hex"])
        reason, fields = decode_frame(buf, cap, types)
        if reason != v["decode"]:
            print(f"FAIL {v['name']}: decode = {reason!r}, expected {v['decode']!r}")
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
                print(f"FAIL {v['name']}: decoded field {mismatch}")
                failed += 1
                continue
            payload = bytes.fromhex(v["payload_hex"])
            re_encoded = encode_frame(
                v["message_type"], v["durability"], v["flags"], payload
            ).hex()
            if re_encoded != v["hex"]:
                print(f"FAIL {v['name']}: re-encode\n  got {re_encoded}\n  exp {v['hex']}")
                failed += 1
                continue
            # A v1-ONLY decoder must reject these, and must do so as an unknown
            # message type. This is the compatibility claim, checked rather than
            # asserted: nothing here asks an existing client to change.
            v1_reason, _ = decode_frame(buf, cap)
            if v1_reason != "UnknownMessageType":
                print(
                    f"FAIL {v['name']}: a v1-only decoder returned {v1_reason!r}, "
                    "expected 'UnknownMessageType'"
                )
                failed += 1
                continue
        passed += 1

    for v in doc["coordinate_vectors"]:
        buf = bytes.fromhex(v["hex"])
        reason, fields = decode_coordinate(buf)
        if reason != v["decode"]:
            print(f"FAIL {v['name']}: coordinate decode = {reason!r}, expected {v['decode']!r}")
            failed += 1
            continue
        if v["decode"] == "ok":
            mismatch = next(
                (
                    f"{k}={fields[k]!r} != {v[k]!r}"
                    for k in ("segment", "index", "record_id_hex")
                    if fields[k] != v[k]
                ),
                None,
            )
            if mismatch:
                print(f"FAIL {v['name']}: decoded field {mismatch}")
                failed += 1
                continue
            re_encoded = encode_coordinate(v["segment"], v["index"], v["record_id_hex"]).hex()
            if re_encoded != v["hex"]:
                print(f"FAIL {v['name']}: re-encode\n  got {re_encoded}\n  exp {v['hex']}")
                failed += 1
                continue
        passed += 1

    return passed, failed


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
            # A vector marked "round_trip": false decodes a non-canonical wire
            # byte (the retired 0x02 durability tier) to its canonical field
            # value, so re-encoding it does NOT reproduce `hex` by design — skip
            # the byte-identical check rather than weakening it for every vector.
            if v.get("round_trip", True):
                payload = bytes.fromhex(v["payload_hex"])
                re_encoded = encode_frame(v["message_type"], v["durability"], v["flags"], payload).hex()
                if re_encoded != v["hex"]:
                    print(f"FAIL {name}: re-encode\n  got {re_encoded}\n  exp {v['hex']}")
                    failed += 1
                    continue

        passed += 1

    total = passed + failed
    print(f"\n{passed}/{total} vectors passed" + (f", {failed} FAILED" if failed else " — all good"))

    t_passed, t_failed = check_tracked()
    if t_passed or t_failed:
        t_total = t_passed + t_failed
        print(
            f"{t_passed}/{t_total} tracked-extension vectors passed"
            + (f", {t_failed} FAILED" if t_failed else " — all good")
        )
    return 1 if (failed or t_failed) else 0


if __name__ == "__main__":
    sys.exit(main())
