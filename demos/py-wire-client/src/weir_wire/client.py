"""A pure-Python weir producer over an AF_UNIX SOCK_STREAM socket.

Built from docs/wire_protocol.md:
  - No in-band handshake: connect and send a Push/HealthCheck immediately.
  - Each connection is a serial request/response stream.
  - Frame the response ourselves: read 16-byte header, take payload_len,
    read payload_len + 4 more bytes.
  - A Nack first payload byte is the NackReason.
  - On mid-stream close, treat in-flight pushes as unknown-outcome; retry on a
    fresh connection.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass

from .codec import (
    HEADER_LEN,
    MAX_PAYLOAD_HARD_CAP,
    DecodeError,
    Durability,
    Frame,
    MessageType,
    NackReason,
    decode_frame,
    encode_frame,
)


class WeirError(Exception):
    pass


class NackError(WeirError):
    """The daemon rejected the frame with a Nack."""

    def __init__(self, reason_byte: int):
        self.reason_byte = reason_byte
        self.reason = NackReason.describe(reason_byte)
        # The spec marks these as permanent (connection closed); the rest are
        # transient/retryable on a fresh connection.
        permanent = {
            NackReason.BAD_MAGIC,
            NackReason.VERSION_MISMATCH,
            NackReason.BAD_HEADER_CRC,
            NackReason.PAYLOAD_TOO_LARGE,
            NackReason.BAD_PAYLOAD_CRC,
            NackReason.EMPTY_PAYLOAD,
            NackReason.UNKNOWN_MESSAGE,
            NackReason.RESERVED_FLAGS_SET,
        }
        self.retryable = reason_byte == NackReason.INTERNAL_ERROR or reason_byte not in {
            r.value for r in permanent
        }
        super().__init__(f"Nack: {self.reason}")


class ConnectionClosed(WeirError):
    """The daemon closed the connection mid-frame; outcome unknown."""


@dataclass
class PushResult:
    acked: bool
    durability_used: Durability


class WeirClient:
    def __init__(self, socket_path: str, connect_timeout: float = 5.0):
        self.socket_path = socket_path
        self.connect_timeout = connect_timeout
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.connect_timeout)
        s.connect(self.socket_path)
        self._sock = s

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def __enter__(self) -> "WeirClient":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _recv_exactly(self, n: int) -> bytes:
        assert self._sock is not None
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionClosed(
                    f"connection closed after {len(buf)}/{n} bytes"
                )
            buf.extend(chunk)
        return bytes(buf)

    def _read_response_frame(self) -> Frame:
        """Frame one response off the wire per the spec's read recipe."""
        header = self._recv_exactly(HEADER_LEN)
        # payload_len lives at bytes [8..12], little-endian u32.
        payload_len = int.from_bytes(header[8:12], "little")
        rest = self._recv_exactly(payload_len + 4)
        # Hand the codec exactly one frame.
        return decode_frame(header + rest)

    def _request(self, frame_bytes: bytes) -> Frame:
        assert self._sock is not None, "call connect() first"
        self._sock.sendall(frame_bytes)
        return self._read_response_frame()

    def push(
        self, payload: bytes, durability: Durability = Durability.SYNC
    ) -> PushResult:
        if not payload:
            raise ValueError("weir rejects zero-length Push payloads (EmptyPayload)")
        if len(payload) > MAX_PAYLOAD_HARD_CAP:
            raise ValueError(
                f"payload {len(payload)} > MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"
            )
        frame = encode_frame(MessageType.PUSH, durability, payload)
        resp = self._request(frame)
        if resp.message_type == MessageType.ACK:
            return PushResult(acked=True, durability_used=durability)
        if resp.message_type == MessageType.NACK:
            reason = resp.payload[0] if resp.payload else NackReason.INTERNAL_ERROR
            raise NackError(reason)
        raise WeirError(f"unexpected response message_type {resp.message_type!r}")

    def health_check(self) -> bool:
        frame = encode_frame(MessageType.HEALTH_CHECK, Durability.SYNC, b"")
        resp = self._request(frame)
        return resp.message_type == MessageType.HEALTH_CHECK_RESPONSE
