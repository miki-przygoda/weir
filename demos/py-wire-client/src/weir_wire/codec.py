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


def max_response_payload(message_type: int) -> int:
    """Cap for a response of this type, checked before any allocation.

    AckTracked is the only weir response whose payload exceeds two bytes, so the
    bound is widened for exactly that frame and for nothing else -- a desynced
    peer cannot use a stray type byte to unlock a bigger read, because an
    unexpected type is a desync the caller rejects anyway.
    """
    if message_type == MessageType.ACK_TRACKED:
        return MAX_TRACKED_ACK_PAYLOAD
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
