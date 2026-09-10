#!/usr/bin/env python3
"""Generate conformance vectors for the tracked-push extension to wire v1.

These live in their OWN file, not in `wire_v1_vectors.json`. That file is frozen
and is read by five polyglot demo clients whose decoders know message types
0x01..0x05 only; adding a `PushTracked` (0x06) vector there would make every one
of them fail on a frame they are correct to reject. The split is the point: a
client that does not implement tracked pushes stays conformant by ignoring this
file entirely.

Python's `zlib.crc32` is the IEEE / ISO-3309 CRC-32 weir uses, and `hashlib`'s
SHA-256 is an implementation independent of Rust's `sha2` crate — so a vector
here can only pass in `weir-core` if both sides agree on every byte of the frame
AND of the record-id digest.
"""
import hashlib
import json
import struct
import zlib

WIRE_VERSION = 1
COORDINATE_VERSION = 1
COORDINATE_FIXED_LEN = 1 + 8 + 32 + 2
MAX_SEGMENT_NAME_LEN = 255
MAX_TRACKED_ACK_PAYLOAD_LEN = COORDINATE_FIXED_LEN + MAX_SEGMENT_NAME_LEN
MAGIC = b"WEIR"

MT = {"PushTracked": 0x06, "AckTracked": 0x07}
DUR = {"Durable": 0x01, "Buffered": 0x03}


def crc32(b: bytes) -> int:
    return zlib.crc32(b) & 0xFFFFFFFF


def frame(mt: int, dur: int, payload: bytes) -> bytes:
    h = bytearray(16)
    h[0:4] = MAGIC
    h[4] = WIRE_VERSION
    h[5] = mt
    h[6] = dur
    h[7] = 0
    h[8:12] = len(payload).to_bytes(4, "little")
    h[12:16] = crc32(bytes(h[0:12])).to_bytes(4, "little")
    return bytes(h) + payload + crc32(payload).to_bytes(4, "little")


def record_id(segment: str, index: int, payload: bytes) -> bytes:
    """sha256(segment_len ++ segment ++ index ++ payload_len ++ payload).

    Every length a little-endian u64 — the framing is what stops ("ab", 1) and
    ("a", 0xb...) from colliding. Mirrors `weir_sink_sdk::RecordId::for_record`.
    """
    h = hashlib.sha256()
    seg = segment.encode()
    h.update(struct.pack("<Q", len(seg)))
    h.update(seg)
    h.update(struct.pack("<Q", index))
    h.update(struct.pack("<Q", len(payload)))
    h.update(payload)
    return h.digest()


def coordinate(segment: str, index: int, rid: bytes) -> bytes:
    seg = segment.encode()
    return (
        bytes([COORDINATE_VERSION])
        + struct.pack("<Q", index)
        + rid
        + struct.pack("<H", len(seg))
        + seg
    )


frames = []
coordinates = []


def ok_frame(name, notes, raw, mt_name, dur_name, payload):
    frames.append(
        {
            "name": name,
            "notes": notes,
            "hex": raw.hex(),
            "decode": "ok",
            "message_type": mt_name,
            "durability": dur_name,
            "flags": 0,
            "payload_hex": payload.hex(),
        }
    )


def ok_coordinate(name, notes, raw, segment, index, rid):
    coordinates.append(
        {
            "name": name,
            "notes": notes,
            "hex": raw.hex(),
            "decode": "ok",
            "segment": segment,
            "index": index,
            "record_id_hex": rid.hex(),
        }
    )


def bad_coordinate(name, notes, raw, tag):
    coordinates.append(
        {"name": name, "notes": notes, "hex": raw.hex(), "decode": tag}
    )


# ── Frames ────────────────────────────────────────────────────────────────────

ok_frame(
    "push_tracked_hello",
    'PushTracked of "hello" at Durable durability. Byte-for-byte a Push frame '
    "except for the message_type byte (0x06 instead of 0x01) and the header CRC "
    "that covers it.",
    frame(MT["PushTracked"], DUR["Durable"], b"hello"),
    "PushTracked",
    "Durable",
    b"hello",
)

ok_frame(
    "push_tracked_buffered",
    "PushTracked at Buffered durability. Every tier is available to a tracked "
    "push; the tier still decides durability, the coordinate only says where the "
    "record went.",
    frame(MT["PushTracked"], DUR["Buffered"], b"hello"),
    "PushTracked",
    "Buffered",
    b"hello",
)

SEGMENT = "shard_00/seg_00000001.wab.sealed"
INDEX = 7
RID = record_id(SEGMENT, INDEX, b"hello")
COORD = coordinate(SEGMENT, INDEX, RID)

ok_frame(
    "ack_tracked_hello",
    "The daemon's reply to `push_tracked_hello`: an AckTracked whose payload is "
    "the record's coordinate. The durability byte is fixed filler (0x01) exactly "
    "as it is on a plain Ack.",
    frame(MT["AckTracked"], DUR["Durable"], COORD),
    "AckTracked",
    "Durable",
    COORD,
)

# ── Coordinate payloads ───────────────────────────────────────────────────────

ok_coordinate(
    "coordinate_hello",
    "The coordinate carried by `ack_tracked_hello`. The record_id is "
    "sha256(segment_len ++ segment ++ index ++ payload_len ++ payload) over the "
    'payload "hello" — the same value the drain hands the sink as its '
    "per-record idempotency key.",
    COORD,
    SEGMENT,
    INDEX,
    RID,
)

ok_coordinate(
    "coordinate_empty_segment",
    "Boundary: a zero-length segment name is structurally legal (the daemon "
    "never emits one, but a decoder must not special-case it into a rejection).",
    coordinate("", 0, b"\x00" * 32),
    "",
    0,
    b"\x00" * 32,
)

ok_coordinate(
    "coordinate_max_segment",
    f"Boundary: a segment name at the {MAX_SEGMENT_NAME_LEN}-byte cap, which "
    f"makes the payload exactly {MAX_TRACKED_ACK_PAYLOAD_LEN} bytes — the "
    "largest AckTracked a conformant daemon sends, and the number a reader caps "
    "its allocation at.",
    coordinate("z" * MAX_SEGMENT_NAME_LEN, 2**64 - 1, b"\xff" * 32),
    "z" * MAX_SEGMENT_NAME_LEN,
    2**64 - 1,
    b"\xff" * 32,
)

bad_coordinate(
    "reject_coordinate_truncated",
    "Shorter than the fixed prefix every coordinate has. Rejected before any "
    "field offset is interpreted.",
    COORD[: COORDINATE_FIXED_LEN - 1],
    "Truncated",
)

bad_coordinate(
    "reject_coordinate_unknown_version",
    "Leading version byte is 0x02. The version leads the payload so a reader "
    "meeting a layout it does not know rejects it instead of mis-parsing it.",
    bytes([0x02]) + COORD[1:],
    "UnsupportedVersion",
)

bad_coordinate(
    "reject_coordinate_trailing_bytes",
    "One byte past the declared length. The buffer must be EXACTLY one "
    "coordinate — quietly using the prefix is how a layout disagreement becomes "
    "a wrong address.",
    COORD + b"\x00",
    "LengthMismatch",
)

bad_coordinate(
    "reject_coordinate_segment_too_long",
    f"Declares a segment name over the {MAX_SEGMENT_NAME_LEN}-byte cap. Rejected "
    "on the declared length, before the buffer is consulted.",
    COORD[:41]
    + struct.pack("<H", MAX_SEGMENT_NAME_LEN + 1)
    + COORD[43:],
    "SegmentTooLong",
)

bad_coordinate(
    "reject_coordinate_not_utf8",
    "Segment name bytes are not valid UTF-8.",
    COORD[:-1] + b"\xff",
    "SegmentNotUtf8",
)

doc = {
    "wire_version": WIRE_VERSION,
    "coordinate_version": COORDINATE_VERSION,
    "coordinate_fixed_len": COORDINATE_FIXED_LEN,
    "max_segment_name_len": MAX_SEGMENT_NAME_LEN,
    "max_tracked_ack_payload_len": MAX_TRACKED_ACK_PAYLOAD_LEN,
    "description": (
        "Conformance vectors for the tracked-push extension to the weir v1 wire "
        "protocol: message types PushTracked (0x06) and AckTracked (0x07), and "
        "the RecordCoordinate payload an AckTracked carries. Kept apart from "
        "wire_v1_vectors.json, which is frozen and is read by decoders that know "
        "only message types 0x01..0x05 — such a decoder is CORRECT to reject a "
        "0x06 frame, and stays conformant by ignoring this file. 'frame_vectors' "
        "are complete frames; 'coordinate_vectors' are bare AckTracked payloads."
    ),
    "frame_vectors": frames,
    "coordinate_vectors": coordinates,
}

print(json.dumps(doc, indent=2))
