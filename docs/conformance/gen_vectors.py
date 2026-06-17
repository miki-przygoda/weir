#!/usr/bin/env python3
"""Generate canonical conformance vectors for the weir v1 wire protocol.

Python's zlib.crc32 is the IEEE / ISO-3309 CRC-32 — byte-identical to the Rust
crc32fast crate weir uses. Generating the vectors here and verifying them with
weir-core's own decoder (tests/conformance.rs) cross-checks two independent CRC
implementations, so a vector can only pass if both agree on the wire bytes.
"""
import json
import zlib

WIRE_VERSION = 1
MAX_PAYLOAD_HARD_CAP = 16 * 1024 * 1024
MAGIC = b"WEIR"

# message_type bytes
MT = {"Push": 0x01, "Ack": 0x02, "Nack": 0x03,
      "HealthCheck": 0x04, "HealthCheckResponse": 0x05}
# durability bytes
DUR = {"Sync": 0x01, "Batched": 0x02, "Buffered": 0x03}


def crc32(b: bytes) -> int:
    return zlib.crc32(b) & 0xFFFFFFFF


def header_bytes(mt: int, dur: int, flags: int, payload_len: int) -> bytes:
    h = bytearray(16)
    h[0:4] = MAGIC
    h[4] = WIRE_VERSION
    h[5] = mt
    h[6] = dur
    h[7] = flags
    h[8:12] = payload_len.to_bytes(4, "little")
    h[12:16] = crc32(bytes(h[0:12])).to_bytes(4, "little")
    return bytes(h)


def frame(mt: int, dur: int, payload: bytes, flags: int = 0) -> bytes:
    h = header_bytes(mt, dur, flags, len(payload))
    return h + payload + crc32(payload).to_bytes(4, "little")


def hx(b: bytes) -> str:
    return b.hex()


vectors = []


def ok(name, notes, raw, mt_name, dur_name, flags, payload):
    vectors.append({
        "name": name,
        "notes": notes,
        "hex": hx(raw),
        "decode": "ok",
        "message_type": mt_name,
        "durability": dur_name,
        "flags": flags,
        "payload_hex": hx(payload),
    })


def err(name, notes, raw, tag):
    vectors.append({
        "name": name,
        "notes": notes,
        "hex": hx(raw),
        "decode": tag,
    })


# ── Valid message frames (decode ok + encode round-trip) ──────────────────────
ok("push_hello_sync", "Push of \"hello\" at Sync durability.",
   frame(MT["Push"], DUR["Sync"], b"hello"), "Push", "Sync", 0, b"hello")
ok("push_batched", "Push at Batched durability exercises durability byte 0x02.",
   frame(MT["Push"], DUR["Batched"], b"wb"), "Push", "Batched", 0, b"wb")
ok("push_buffered", "Push at Buffered durability exercises durability byte 0x03.",
   frame(MT["Push"], DUR["Buffered"], b"x"), "Push", "Buffered", 0, b"x")
ok("ack", "Ack response: empty payload, payload CRC of zero bytes is 0x00000000.",
   frame(MT["Ack"], DUR["Sync"], b""), "Ack", "Sync", 0, b"")
ok("healthcheck", "HealthCheck request: empty payload.",
   frame(MT["HealthCheck"], DUR["Sync"], b""), "HealthCheck", "Sync", 0, b"")
ok("healthcheck_response", "HealthCheckResponse: empty payload.",
   frame(MT["HealthCheckResponse"], DUR["Sync"], b""),
   "HealthCheckResponse", "Sync", 0, b"")

# ── Nack frames, one per reason byte (all decode ok as Nack messages) ─────────
NACK_REASONS = [
    (0x01, "bad_magic", "BadMagic"),
    (0x02, "version_mismatch", "VersionMismatch — payload carries the daemon's "
                               "WIRE_VERSION as a second byte"),
    (0x03, "bad_header_crc", "BadHeaderCrc"),
    (0x04, "payload_too_large", "PayloadTooLarge"),
    (0x05, "bad_payload_crc", "BadPayloadCrc"),
    (0x06, "internal_error", "InternalError (transient; connection kept open)"),
    (0x07, "empty_payload", "EmptyPayload"),
    (0x08, "unknown_message", "UnknownMessage (permanent; connection closed)"),
    (0x09, "reserved_flags_set", "ReservedFlagsSet (permanent; connection closed)"),
]
for byte, slug, desc in NACK_REASONS:
    if byte == 0x02:
        payload = bytes([byte, WIRE_VERSION])
    else:
        payload = bytes([byte])
    ok(f"nack_{slug}", f"Nack reason {desc}.",
       frame(MT["Nack"], DUR["Sync"], payload), "Nack", "Sync", 0, payload)

# ── Forward-compat / response-filler coverage (decode ok) ─────────────────────
# A Nack carrying a reserved reason byte still decodes as a Nack frame — the
# reason's *meaning* is forward-compatible (a client surfaces the raw byte). This
# pins a non-Rust decoder's forward-compat path, which the per-reason vectors above
# (0x01–0x09) don't exercise.
ok("nack_reserved_reason",
   "Nack with a reserved reason byte (0x0A): decodes as a Nack frame; the reason's "
   "meaning is forward-compatible and clients surface the raw byte.",
   frame(MT["Nack"], DUR["Sync"], bytes([0x0A])), "Nack", "Sync", 0, bytes([0x0A]))
# Responses carry a durability *filler* byte that clients must ignore. Pin that a
# non-Sync filler still decodes (here an Ack with Buffered, 0x03).
ok("ack_nonsync_durability_filler",
   "Ack response whose durability filler is non-Sync (Buffered, 0x03): clients must "
   "ignore the durability field on responses.",
   frame(MT["Ack"], DUR["Buffered"], b""), "Ack", "Buffered", 0, b"")

# ── Rejection vectors (decode must error with the named variant) ──────────────
# bad magic
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data"))
raw[0:4] = b"XXXX"
err("reject_bad_magic", "First four bytes are not \"WEIR\".", bytes(raw), "BadMagic")

# version mismatch (future version, CRC recomputed so version path fires, not CRC)
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data"))
raw[4] = WIRE_VERSION + 1
raw[12:16] = crc32(bytes(raw[0:12])).to_bytes(4, "little")
err("reject_version_mismatch", "Version byte 0x02 with a valid CRC.",
    bytes(raw), "VersionMismatch")

# unknown message type (valid CRC)
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data"))
raw[5] = 0xFF
raw[12:16] = crc32(bytes(raw[0:12])).to_bytes(4, "little")
err("reject_unknown_message_type", "message_type 0xFF with a valid CRC.",
    bytes(raw), "UnknownMessageType")

# unknown durability (valid CRC)
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data"))
raw[6] = 0xFF
raw[12:16] = crc32(bytes(raw[0:12])).to_bytes(4, "little")
err("reject_unknown_durability", "durability 0xFF with a valid CRC.",
    bytes(raw), "UnknownDurability")

# bad header CRC
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data"))
raw[12] ^= 0xFF
err("reject_bad_header_crc", "Header CRC field corrupted.", bytes(raw),
    "HeaderCrcMismatch")

# reserved flags set (valid CRC)
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"data", flags=0x01))
err("reject_reserved_flags_set",
    "flags byte 0x01 (reserved; must be zero in v1).", bytes(raw),
    "ReservedFlagsSet")

# payload too large: 16-byte header only, payload_len = cap + 1, valid CRC
oversize = MAX_PAYLOAD_HARD_CAP + 1
raw = bytearray(header_bytes(MT["Push"], DUR["Sync"], 0, oversize))
err("reject_payload_too_large",
    "Header alone declares payload_len = MAX_PAYLOAD_HARD_CAP + 1; rejected "
    "before any allocation, before the frame-length check.",
    bytes(raw), "PayloadTooLarge")

# truncated header (< 16 bytes)
raw = frame(MT["Push"], DUR["Sync"], b"data")[:15]
err("reject_truncated_header", "Buffer shorter than the 16-byte header.",
    bytes(raw), "TruncatedFrame")

# truncated payload (declares 5, frame cut short)
full = frame(MT["Push"], DUR["Sync"], b"hello")
err("reject_truncated_payload",
    "Header declares a 5-byte payload but the buffer is cut short of the full "
    "frame.", full[:-1], "TruncatedFrame")

# bad payload CRC (last byte flipped)
raw = bytearray(frame(MT["Push"], DUR["Sync"], b"hello"))
raw[-1] ^= 0xFF
err("reject_bad_payload_crc", "Trailing payload CRC corrupted.", bytes(raw),
    "PayloadCrcMismatch")

# trailing bytes (G18): a complete frame followed by extra bytes
raw = frame(MT["Push"], DUR["Sync"], b"hi") + b"\xde\xad\xbe\xef"
err("reject_trailing_bytes",
    "A complete frame followed by 4 extra bytes; the buffer must be exactly "
    "one frame (G18).", raw, "TrailingBytes")

doc = {
    "wire_version": WIRE_VERSION,
    "max_payload_hard_cap": MAX_PAYLOAD_HARD_CAP,
    "description": (
        "Canonical conformance vectors for the weir v1 wire protocol. Each "
        "vector is a hex-encoded byte buffer plus the result a conformant "
        "decoder MUST produce. 'decode' is \"ok\" for a buffer that is exactly "
        "one valid frame (the message_type / durability / flags / payload_hex "
        "fields give the decoded header and payload), or the name of the "
        "rejection error otherwise. CRCs are IEEE / ISO-3309 CRC-32 (zlib / "
        "crc32fast). See docs/wire_protocol.md for the format and "
        "docs/conformance.md for how to use these vectors."
    ),
    "vectors": vectors,
}

print(json.dumps(doc, indent=2))
