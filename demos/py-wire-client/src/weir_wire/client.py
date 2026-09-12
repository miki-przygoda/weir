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
    MAX_RESPONSE_PAYLOAD,
    CoordinateError,
    RecordCoordinate,
    decode_coordinate,
    max_response_payload,
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

    @property
    def means_no_tracked_support(self) -> bool:
        """True when this Nack is a daemon that predates PushTracked (0x06).

        The wire cannot distinguish that from any other unknown type, so the
        caller has to: it is only meaningful on the reply to a push_tracked().
        Do not retry on this connection -- the daemon closes it. Open a new one
        and fall back to push().
        """
        return self.reason == NackReason.UNKNOWN_MESSAGE


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
        # payload_len lives at bytes [8..12], little-endian u32. Bound it BEFORE
        # reading the body: this client had no bound at all, so a peer declaring
        # 8 MiB got an 8 MiB read that only the socket timeout ended.
        payload_len = int.from_bytes(header[8:12], "little")
        # Cap by the type the header declares: AckTracked carries a coordinate
        # of up to 298 bytes, every other response at most two. Widening the
        # bound for one frame type is not the same as removing it.
        cap = max_response_payload(header[5])
        if payload_len > cap:
            raise DecodeError(
                "PayloadTooLarge",
                f"response declared payload_len {payload_len} > "
                f"{cap} for message_type {header[5]:#04x} (desync or hostile peer)",
            )
        rest = self._recv_exactly(payload_len + 4)
        # Hand the codec exactly one frame, with the same cap, so the two checks
        # cannot disagree.
        return decode_frame(header + rest, max_payload_bytes=cap)

    def _request(self, frame_bytes: bytes) -> Frame:
        assert self._sock is not None, "call connect() first"
        self._sock.sendall(frame_bytes)
        return self._read_response_frame()

    def _send_record(self, message_type: MessageType, payload: bytes, durability: Durability):
        """Shared guards + round trip for Push and PushTracked.

        Rejected locally rather than on the wire: an empty payload and an
        over-cap one are both certain Nacks, and a Nack closes the connection,
        so catching them here keeps a usable client.
        """
        if not payload:
            raise ValueError("weir rejects zero-length Push payloads (EmptyPayload)")
        if len(payload) > MAX_PAYLOAD_HARD_CAP:
            raise ValueError(
                f"payload {len(payload)} > MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"
            )
        resp = self._request(encode_frame(message_type, durability, payload))
        if resp.message_type == MessageType.NACK:
            reason = resp.payload[0] if resp.payload else NackReason.INTERNAL_ERROR
            raise NackError(reason)
        return resp

    def push(
        self, payload: bytes, durability: Durability = Durability.DURABLE
    ) -> PushResult:
        resp = self._send_record(MessageType.PUSH, payload, durability)
        if resp.message_type == MessageType.ACK:
            return PushResult(acked=True, durability_used=durability)
        raise WeirError(f"unexpected response message_type {resp.message_type!r}")

    def push_tracked(
        self, payload: bytes, durability: Durability = Durability.DURABLE
    ) -> RecordCoordinate:
        """Push a record and learn where it landed.

        Identical to push() in every respect but the reply: same tiers, same
        caps, same Nack reasons. A daemon that predates the type answers
        Nack(UnknownMessage) and closes, which surfaces as NackError with
        .means_no_tracked_support set -- open a new connection and use push().

        A bare Ack in reply is an error, not a success. The request determines
        the response shape, so an Ack here means the peer did not understand
        what was asked and treating it as success would report a push as
        tracked when no coordinate exists.
        """
        resp = self._send_record(MessageType.PUSH_TRACKED, payload, durability)
        if resp.message_type == MessageType.ACK_TRACKED:
            return decode_coordinate(resp.payload)
        if resp.message_type == MessageType.ACK:
            raise WeirError(
                "daemon answered a PushTracked with a bare Ack; the record may be "
                "durable but its coordinate is unknown"
            )
        raise WeirError(f"unexpected response message_type {resp.message_type!r}")

    def health_check(self) -> bool:
        frame = encode_frame(MessageType.HEALTH_CHECK, Durability.DURABLE, b"")
        resp = self._request(frame)
        return resp.message_type == MessageType.HEALTH_CHECK_RESPONSE
