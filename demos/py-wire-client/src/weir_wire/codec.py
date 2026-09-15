"""Pure-Python codec for the weir v1 wire protocol.

Implemented from docs/wire_protocol.md ONLY (no Rust, no peeking at the
reference codec). Frame layout, decode order, CRC parameters and the Nack
reason table all come from that document.

    Offset  Size  Field
     0       4    magic = b"WEIR"
     4       1    version = 1
     5       1    message_type
     6       1    durability
     7       1    flags (reserved, must be 0)
     8       4    payload_len   u32 little-endian
    12       4    header_crc32  CRC-32 of bytes [0..12], little-endian
    16     var    payload
    16+n    4    payload_crc32 CRC-32 of payload, little-endian

CRC is IEEE / ISO-3309 CRC-32 == Python's zlib.crc32 (NOT CRC-32C).
"""

from __future__ import annotations

import enum
import struct
import zlib
from dataclasses import dataclass

MAGIC = b"WEIR"
WIRE_VERSION = 1
HEADER_LEN = 16
MAX_PAYLOAD_HARD_CAP = 16 * 1024 * 1024  # 16 MiB, from the spec

# Cap on a RESPONSE payload, which is not the cap on a record being sent.
# Every response a client that never sends PushTracked (0x06) can receive is at
# most two bytes: Ack carries none, Nack one or two, HealthCheckResponse one
# (docs/wire_protocol.md, "Response sizes"). Reading a response against
# MAX_PAYLOAD_HARD_CAP -- or against nothing at all, as this client did -- lets
# a desynced or hostile peer choose an allocation off one header field.
#
# This client now sends PushTracked, so the cap is a per-type lookup -- see
# max_response_payload().
MAX_RESPONSE_PAYLOAD = 2

# RecordCoordinate wire layout (docs/wire_protocol.md, "AckTracked payload"):
#   0   1   coordinate_version (0x01)
#   1   8   index        u64 LE, 1-based within the segment
#   9  32   record_id    SHA-256
#  41   2   segment_len  u16 LE
#  43 var   segment      UTF-8, <= 255 bytes
COORDINATE_VERSION = 1
COORDINATE_FIXED_LEN = 1 + 8 + 32 + 2
MAX_SEGMENT_NAME_LEN = 255
MAX_TRACKED_ACK_PAYLOAD = COORDINATE_FIXED_LEN + MAX_SEGMENT_NAME_LEN  # 298

# AckBatch wire layout (docs/wire_protocol.md, "AckBatch payload"):
#   0   1   ack_batch_version (0x01)
#   1   2   record_count  u16 LE, echoing the PushBatch it answers
#   3 var   bitmap        ceil(N/8) bytes, LSB-first within each byte
#
# The hard cap is 2048 so this payload stays under MAX_TRACKED_ACK_PAYLOAD:
# batching introduces no new largest response, so this client's biggest
# allocation is unchanged by implementing it.
BATCH_VERSION = 1
ACK_BATCH_VERSION = 1
BATCH_HEADER_LEN = 1 + 2
ACK_BATCH_HEADER_LEN = 1 + 2
MAX_BATCH_RECORDS_HARD_CAP = 2048
MAX_ACK_BATCH_PAYLOAD = ACK_BATCH_HEADER_LEN + (MAX_BATCH_RECORDS_HARD_CAP + 7) // 8  # 259


def max_response_payload(message_type: int) -> int:
    """Cap for a response of this type, checked before any allocation.

    Widened for exactly the two response types whose payload can exceed two
    bytes, and for nothing else -- a desynced peer must not be able to use a
    stray type byte to unlock a bigger read, because an unexpected type is a
    desync the caller rejects anyway.
    """
    if message_type == MessageType.ACK_TRACKED:
        return MAX_TRACKED_ACK_PAYLOAD
    if message_type == MessageType.ACK_BATCH:
        return MAX_ACK_BATCH_PAYLOAD
    return MAX_RESPONSE_PAYLOAD


class MessageType(enum.IntEnum):
    PUSH = 0x01
    ACK = 0x02
    NACK = 0x03
    HEALTH_CHECK = 0x04
    HEALTH_CHECK_RESPONSE = 0x05
    # Additive within wire v1: message-type bytes 0x08-0xFF are reserved for
    # exactly this, so WIRE_VERSION stays 1 and the 30 frozen vectors are
    # untouched. A daemon that predates these answers Nack(UnknownMessage).
    PUSH_TRACKED = 0x06
    ACK_TRACKED = 0x07
    PUSH_BATCH = 0x08
    ACK_BATCH = 0x09


class Durability(enum.IntEnum):
    DURABLE = 0x01
    # Retired: still a valid wire byte that permissively decodes to DURABLE,
    # but a conformant encoder never emits it. See docs/wire_protocol.md
    # "Durability tiers".
    BATCHED = 0x02
    BUFFERED = 0x03


class NackReason(enum.IntEnum):
    BAD_MAGIC = 0x01
    VERSION_MISMATCH = 0x02
    BAD_HEADER_CRC = 0x03
    PAYLOAD_TOO_LARGE = 0x04
    BAD_PAYLOAD_CRC = 0x05
    INTERNAL_ERROR = 0x06
    EMPTY_PAYLOAD = 0x07
    UNKNOWN_MESSAGE = 0x08
    RESERVED_FLAGS_SET = 0x09

    @classmethod
    def describe(cls, byte: int) -> str:
        try:
            return cls(byte).name
        except ValueError:
            # Spec: 0x0A-0xFF reserved; surface the raw byte rather than guess.
            return f"reserved/unknown reason 0x{byte:02x}"


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def encode_frame(
    message_type: MessageType,
    durability: Durability,
    payload: bytes,
    flags: int = 0,
) -> bytes:
    """Encode exactly one frame. Mirrors the layout table above."""
    if len(payload) > 0xFFFFFFFF:
        raise ValueError("payload_len does not fit in u32")
    header = bytearray(HEADER_LEN)
    header[0:4] = MAGIC
    header[4] = WIRE_VERSION
    header[5] = int(message_type)
    header[6] = int(durability)
    header[7] = flags
    struct.pack_into("<I", header, 8, len(payload))
    struct.pack_into("<I", header, 12, _crc32(bytes(header[:12])))
    return bytes(header) + payload + struct.pack("<I", _crc32(payload))


@dataclass
class Frame:
    message_type: MessageType
    durability: int  # raw byte; responses may carry filler durability
    flags: int
    payload: bytes


class DecodeError(Exception):
    """Raised by decode_frame. `.tag` matches the conformance vector names."""

    def __init__(self, tag: str, detail: str = ""):
        self.tag = tag
        super().__init__(detail or tag)


@dataclass(frozen=True)
class RecordCoordinate:
    """Where a tracked record landed in the buffer.

    An address, not a sequence. Other producers interleave in the same segment,
    so one producer's indices have holes by construction -- the spec is explicit
    that this does not provide gap-free numbering. `segment` is an opaque,
    stable address rather than a filesystem path, and `record_id` is the same
    digest the daemon hands a sink as its per-record idempotency key, which is
    what lets a producer correlate what it sent with what arrived.
    """

    segment: str
    index: int  # u64, 1-based within the segment
    record_id: bytes  # 32-byte SHA-256

    def record_id_hex(self) -> str:
        return self.record_id.hex()


class CoordinateError(Exception):
    """Raised by decode_coordinate. `.tag` matches the conformance vector names."""

    def __init__(self, tag: str, detail: str = ""):
        self.tag = tag
        super().__init__(detail or tag)


def decode_coordinate(buf: bytes) -> RecordCoordinate:
    """Decode exactly one RecordCoordinate.

    The version byte leads so the layout can grow inside wire v1: a reader that
    meets a version it does not know must REJECT, never parse a prefix it has
    never seen. The buffer must be exactly one coordinate for the same reason --
    a trailing byte means the two ends disagree about the layout, and quietly
    using the prefix is how that disagreement becomes a wrong address.
    """
    if len(buf) < COORDINATE_FIXED_LEN:
        raise CoordinateError(
            "Truncated", f"need at least {COORDINATE_FIXED_LEN} bytes, got {len(buf)}"
        )
    version = buf[0]
    if version != COORDINATE_VERSION:
        raise CoordinateError("UnsupportedVersion", f"coordinate version {version}")

    (index,) = struct.unpack_from("<Q", buf, 1)
    record_id = bytes(buf[9:41])
    (segment_len,) = struct.unpack_from("<H", buf, 41)

    if segment_len > MAX_SEGMENT_NAME_LEN:
        raise CoordinateError(
            "SegmentTooLong", f"segment_len {segment_len} > {MAX_SEGMENT_NAME_LEN}"
        )
    expected = COORDINATE_FIXED_LEN + segment_len
    if len(buf) < expected:
        raise CoordinateError("Truncated", f"need {expected} bytes, got {len(buf)}")
    if len(buf) != expected:
        raise CoordinateError(
            "LengthMismatch", f"{len(buf) - expected} trailing byte(s) after the coordinate"
        )

    raw = buf[COORDINATE_FIXED_LEN:expected]
    try:
        segment = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CoordinateError("SegmentNotUtf8", str(exc)) from exc

    return RecordCoordinate(segment=segment, index=index, record_id=record_id)


def decode_frame(buf: bytes, max_payload_bytes: int = MAX_PAYLOAD_HARD_CAP) -> Frame:
    """Decode exactly one frame following the mandatory decode order.

    Raises DecodeError(tag) on rejection, where `tag` is one of the
    conformance rejection tags (BadMagic, VersionMismatch, ...).
    """
    # 1. Length, then magic. A buffer shorter than the 16-byte header is
    #    TruncatedFrame regardless of its leading bytes; magic is only interpreted
    #    once a full header is present (length-before-magic, matching weir-core).
    if len(buf) < HEADER_LEN:
        raise DecodeError("TruncatedFrame")
    if buf[:4] != MAGIC:
        raise DecodeError("BadMagic")

    # 2. Version, before the header CRC.
    if buf[4] != WIRE_VERSION:
        raise DecodeError("VersionMismatch")

    # 3. Header CRC over bytes [0..12].
    (header_crc,) = struct.unpack_from("<I", buf, 12)
    if _crc32(buf[:12]) != header_crc:
        raise DecodeError("HeaderCrcMismatch")

    # 4. Header fields (only after header CRC passes).
    mt_byte = buf[5]
    dur_byte = buf[6]
    flags = buf[7]
    try:
        message_type = MessageType(mt_byte)
    except ValueError:
        raise DecodeError("UnknownMessageType")
    if dur_byte not in (d.value for d in Durability):
        raise DecodeError("UnknownDurability")
    if flags != 0:
        raise DecodeError("ReservedFlagsSet")

    # 5. Payload length cap, before any allocation / frame-length check.
    (payload_len,) = struct.unpack_from("<I", buf, 8)
    if payload_len > max_payload_bytes:
        raise DecodeError("PayloadTooLarge")

    # Exactly-one-frame discipline.
    expected = HEADER_LEN + payload_len + 4
    if len(buf) < expected:
        raise DecodeError("TruncatedFrame")
    if len(buf) > expected:
        raise DecodeError("TrailingBytes", f"{len(buf) - expected} extra bytes")

    # 6/7. Payload + payload CRC.
    payload = buf[HEADER_LEN : HEADER_LEN + payload_len]
    (payload_crc,) = struct.unpack_from("<I", buf, HEADER_LEN + payload_len)
    if _crc32(payload) != payload_crc:
        raise DecodeError("PayloadCrcMismatch")

    return Frame(message_type=message_type, durability=dur_byte, flags=flags, payload=payload)


# ── Batch extension (PushBatch 0x08 / AckBatch 0x09) ──────────────────────────
#
# Implemented from docs/wire_protocol.md and checked against
# docs/conformance/wire_v1_batch_vectors.json. Nothing here is ported from the
# Rust reference: an independent implementation is the only thing that catches
# an under-specified format, and this one has a bug class no checksum detects --
# a bitmap written with the opposite bit order is a well-formed frame, valid
# CRCs and correct length, that reports failures as successes.
#
# Bit i lives in byte i // 8 at mask 1 << (i % 8): LSB-first.


class BatchError(DecodeError):
    """A PushBatch body or AckBatch payload that could not be decoded.

    The message is the conformance-vector tag, so a failure names the vector
    that pins it.
    """


def encode_batch_body(records: "list[bytes]") -> bytes:
    """A PushBatch body: version, u16 count, then u32-length-prefixed records."""
    out = bytearray([BATCH_VERSION])
    out += len(records).to_bytes(2, "little")
    for r in records:
        out += len(r).to_bytes(4, "little")
        out += r
    return bytes(out)


def decode_batch_body(
    body: bytes,
    max_records: int = MAX_BATCH_RECORDS_HARD_CAP,
    max_record_len: int = MAX_PAYLOAD_HARD_CAP,
) -> "list[bytes]":
    """Parse a PushBatch body.

    The check ORDER is part of the contract. The declared count is validated
    against the cap BEFORE anything is sized by it, and a record's declared
    length is checked against the cap BEFORE it is added to the cursor. The
    frame's payload CRC has already passed by this point and proves nothing
    here: a hostile peer computes a perfectly valid CRC over a body declaring
    65,535 records in three bytes.
    """
    if len(body) < BATCH_HEADER_LEN:
        raise BatchError("Truncated")
    if body[0] != BATCH_VERSION:
        raise BatchError("UnsupportedVersion")
    declared = int.from_bytes(body[1:3], "little")
    if declared == 0:
        raise BatchError("EmptyBatch")
    cap = min(max_records, MAX_BATCH_RECORDS_HARD_CAP)
    if declared > cap:
        raise BatchError("TooManyRecords")

    record_cap = min(max_record_len, MAX_PAYLOAD_HARD_CAP)
    records: "list[bytes]" = []
    cursor = BATCH_HEADER_LEN
    while cursor < len(body):
        if cursor + 4 > len(body):
            raise BatchError("TruncatedRecord")
        n = int.from_bytes(body[cursor:cursor + 4], "little")
        cursor += 4
        if n == 0:
            raise BatchError("EmptyRecord")
        if n > record_cap:
            raise BatchError("RecordTooLarge")
        if cursor + n > len(body):
            raise BatchError("TruncatedRecord")
        # Stop before overrunning the declared count, so a body carrying more
        # records than it declares is a mismatch rather than a silent drop.
        if len(records) == declared:
            raise BatchError("LengthMismatch")
        records.append(body[cursor:cursor + n])
        cursor += n

    if len(records) != declared or cursor != len(body):
        raise BatchError("LengthMismatch")
    return records


def encode_ack_batch(accepted: "list[bool]") -> bytes:
    """An AckBatch payload from per-record outcomes.

    Padding bits in the final byte are left zero, which the decoder requires.
    """
    bits = bytearray((len(accepted) + 7) // 8)
    for i, ok in enumerate(accepted):
        if ok:
            bits[i // 8] |= 1 << (i % 8)
    return bytes([ACK_BATCH_VERSION]) + len(accepted).to_bytes(2, "little") + bytes(bits)


def decode_ack_batch(payload: bytes, expected: int) -> "list[bool]":
    """Parse an AckBatch payload into per-record outcomes.

    `expected` is the count this client sent. A bitmap is the first weir
    response whose meaning depends on client-held state -- an Ack says "your
    last record" and an AckTracked carries its own coordinate, but a bitmap is
    meaningless without knowing which batch it answers. ceil(N/8) is not
    injective (N of 1017 through 1024 all give 131 bytes), so the echoed count
    is the only thing that can catch a desync.

    A set bit means the record is durable at the requested tier and inherits
    weir's crown invariant. A CLEAR bit is the weak statement: not durable as of
    this reply, retry it, and expect it may nonetheless have been written.
    """
    if len(payload) < ACK_BATCH_HEADER_LEN:
        raise BatchError("Truncated")
    if payload[0] != ACK_BATCH_VERSION:
        raise BatchError("UnsupportedVersion")
    declared = int.from_bytes(payload[1:3], "little")
    if declared == 0:
        raise BatchError("EmptyBatch")
    if declared != expected:
        raise BatchError("LengthMismatch")
    if len(payload) != ACK_BATCH_HEADER_LEN + (declared + 7) // 8:
        raise BatchError("LengthMismatch")
    # Padding bits must be zero. Ignoring them would make popcount == N -- the
    # obvious way to ask "did the whole batch succeed" -- silently wrong.
    used = declared % 8
    if used and payload[-1] & ~((1 << used) - 1) & 0xFF:
        raise BatchError("PaddingNotZero")
    bits = payload[ACK_BATCH_HEADER_LEN:]
    return [bool(bits[i // 8] & (1 << (i % 8))) for i in range(declared)]
