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


class MessageType(enum.IntEnum):
    PUSH = 0x01
    ACK = 0x02
    NACK = 0x03
    HEALTH_CHECK = 0x04
    HEALTH_CHECK_RESPONSE = 0x05


class Durability(enum.IntEnum):
    SYNC = 0x01
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
