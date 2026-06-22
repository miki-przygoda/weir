"""weir_wire — a pure-Python weir wire-protocol producer (v1)."""

from .client import (
    ConnectionClosed,
    NackError,
    PushResult,
    WeirClient,
    WeirError,
)
from .codec import (
    MAX_PAYLOAD_HARD_CAP,
    WIRE_VERSION,
    DecodeError,
    Durability,
    Frame,
    MessageType,
    NackReason,
    decode_frame,
    encode_frame,
)

__all__ = [
    "WeirClient",
    "WeirError",
    "NackError",
    "ConnectionClosed",
    "PushResult",
    "encode_frame",
    "decode_frame",
    "DecodeError",
    "Frame",
    "MessageType",
    "Durability",
    "NackReason",
    "WIRE_VERSION",
    "MAX_PAYLOAD_HARD_CAP",
]
