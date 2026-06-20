"""Validate the pure-Python producer against a RUNNING weir daemon.

Assumes a daemon is already listening on the socket path given as argv[1]
(default: ./weir.sock next to the project). Exercises happy path + the
rejection paths the spec says the daemon enforces, building each malformed
frame by hand to confirm the producer surfaces the right Nack reason.
"""

import socket
import sys
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire import (  # noqa: E402
    ConnectionClosed,
    Durability,
    MessageType,
    NackError,
    WeirClient,
    encode_frame,
)
from weir_wire.codec import HEADER_LEN, _crc32  # noqa: E402

SOCKET = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent / "weir.sock")


def _raw_roundtrip(frame_bytes: bytes):
    """Send raw bytes on a fresh connection, return decoded response Frame or
    raise ConnectionClosed if the daemon hangs up without responding."""
    from weir_wire.codec import decode_frame

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect(SOCKET)
    try:
        s.sendall(frame_bytes)
        header = bytearray()
        while len(header) < HEADER_LEN:
            chunk = s.recv(HEADER_LEN - len(header))
            if not chunk:
                raise ConnectionClosed("closed before full response header")
            header.extend(chunk)
        payload_len = int.from_bytes(header[8:12], "little")
        rest = bytearray()
        while len(rest) < payload_len + 4:
            chunk = s.recv(payload_len + 4 - len(rest))
            if not chunk:
                raise ConnectionClosed("closed before full response payload")
            rest.extend(chunk)
        return decode_frame(bytes(header) + bytes(rest))
    finally:
        s.close()


def check(name, fn):
    try:
        fn()
        print(f"PASS  {name}")
        return True
    except AssertionError as e:
        print(f"FAIL  {name}: {e}")
        return False
    except Exception as e:
        print(f"ERROR {name}: {type(e).__name__}: {e}")
        return False


def t_health():
    with WeirClient(SOCKET) as c:
        assert c.health_check() is True, "health check did not respond"


def t_push_sync():
    with WeirClient(SOCKET) as c:
        r = c.push(b"hello from python", Durability.SYNC)
        assert r.acked, "push not acked"


def t_push_all_tiers():
    with WeirClient(SOCKET) as c:
        for dur in (Durability.SYNC, Durability.BATCHED, Durability.BUFFERED):
            r = c.push(f"tier-{dur.name}".encode(), dur)
            assert r.acked, f"push at {dur.name} not acked"


def t_pipelined():
    # Spec: a client may write multiple Push frames back-to-back; the server
    # responds in submission order, one response per request.
    with WeirClient(SOCKET) as c:
        payloads = [f"pipe-{i}".encode() for i in range(50)]
        frames = b"".join(
            encode_frame(MessageType.PUSH, Durability.BUFFERED, p) for p in payloads
        )
        c._sock.sendall(frames)
        for i in range(len(payloads)):
            resp = c._read_response_frame()
            assert resp.message_type == MessageType.ACK, f"frame {i} not acked"


def t_empty_payload_nack():
    # Build a zero-length Push by hand (our client refuses to, by design).
    frame = encode_frame(MessageType.PUSH, Durability.SYNC, b"")
    try:
        resp = _raw_roundtrip(frame)
    except ConnectionClosed:
        raise AssertionError("expected a Nack(EmptyPayload) before close, got bare close")
    assert resp.message_type == MessageType.NACK, f"got {resp.message_type}"
    assert resp.payload[0] == 0x07, f"expected EmptyPayload 0x07, got 0x{resp.payload[0]:02x}"


def t_bad_payload_crc_nack():
    frame = bytearray(encode_frame(MessageType.PUSH, Durability.SYNC, b"corruptme"))
    frame[-1] ^= 0xFF  # flip a byte in the payload CRC
    try:
        resp = _raw_roundtrip(bytes(frame))
    except ConnectionClosed:
        raise AssertionError("expected a Nack(BadPayloadCrc) before close")
    assert resp.message_type == MessageType.NACK
    assert resp.payload[0] == 0x05, f"expected BadPayloadCrc 0x05, got 0x{resp.payload[0]:02x}"


def t_bad_magic_nack():
    frame = bytearray(encode_frame(MessageType.PUSH, Durability.SYNC, b"x"))
    frame[0:4] = b"XXXX"
    try:
        resp = _raw_roundtrip(bytes(frame))
    except ConnectionClosed:
        raise AssertionError("expected a Nack(BadMagic) before close")
    assert resp.message_type == MessageType.NACK
    assert resp.payload[0] == 0x01, f"expected BadMagic 0x01, got 0x{resp.payload[0]:02x}"


def t_reserved_flags_nack():
    frame = bytearray(encode_frame(MessageType.PUSH, Durability.SYNC, b"x"))
    frame[7] = 0x01  # set a reserved flag bit
    # header CRC now stale; recompute so we exercise the flags path, not BadHeaderCrc
    frame[12:16] = _crc32(bytes(frame[:12])).to_bytes(4, "little")
    try:
        resp = _raw_roundtrip(bytes(frame))
    except ConnectionClosed:
        raise AssertionError("expected a Nack(ReservedFlagsSet) before close")
    assert resp.message_type == MessageType.NACK
    assert resp.payload[0] == 0x09, f"expected ReservedFlagsSet 0x09, got 0x{resp.payload[0]:02x}"


def t_client_sends_ack_nack():
    # Spec: client sending a daemon->client type gets Nack(UnknownMessage), close.
    frame = encode_frame(MessageType.ACK, Durability.SYNC, b"")
    try:
        resp = _raw_roundtrip(frame)
    except ConnectionClosed:
        raise AssertionError("expected a Nack(UnknownMessage) before close")
    assert resp.message_type == MessageType.NACK
    assert resp.payload[0] == 0x08, f"expected UnknownMessage 0x08, got 0x{resp.payload[0]:02x}"


def t_oversize_payload_nack():
    # Declare payload_len > hard cap WITHOUT sending the bytes (spec: rejected
    # before any allocation / payload read). Build header by hand.
    header = bytearray(HEADER_LEN)
    header[0:4] = b"WEIR"
    header[4] = 1
    header[5] = MessageType.PUSH
    header[6] = Durability.SYNC
    header[7] = 0
    header[8:12] = (16 * 1024 * 1024 + 1).to_bytes(4, "little")
    header[12:16] = _crc32(bytes(header[:12])).to_bytes(4, "little")
    try:
        resp = _raw_roundtrip(bytes(header))  # header only, no payload
    except ConnectionClosed:
        raise AssertionError("expected a Nack(PayloadTooLarge) before close")
    assert resp.message_type == MessageType.NACK
    assert resp.payload[0] == 0x04, f"expected PayloadTooLarge 0x04, got 0x{resp.payload[0]:02x}"


def t_nack_then_close():
    # After a permanent Nack the daemon must close. Verify the socket is dead.
    frame = bytearray(encode_frame(MessageType.PUSH, Durability.SYNC, b"x"))
    frame[0:4] = b"XXXX"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect(SOCKET)
    try:
        s.sendall(bytes(frame))
        # read the Nack (21 bytes) then expect EOF
        got = b""
        while len(got) < 21:
            chunk = s.recv(21 - len(got))
            if not chunk:
                break
            got += chunk
        assert len(got) == 21, f"expected a 21-byte Nack, got {len(got)} bytes"
        tail = s.recv(1)
        assert tail == b"", "daemon did not close the connection after a permanent Nack"
    finally:
        s.close()


def main():
    tests = [
        ("health_check", t_health),
        ("push_sync", t_push_sync),
        ("push_all_durability_tiers", t_push_all_tiers),
        ("pipelined_50_pushes", t_pipelined),
        ("empty_payload -> Nack(EmptyPayload)", t_empty_payload_nack),
        ("bad_payload_crc -> Nack(BadPayloadCrc)", t_bad_payload_crc_nack),
        ("bad_magic -> Nack(BadMagic)", t_bad_magic_nack),
        ("reserved_flags -> Nack(ReservedFlagsSet)", t_reserved_flags_nack),
        ("client_sends_Ack -> Nack(UnknownMessage)", t_client_sends_ack_nack),
        ("oversize_payload -> Nack(PayloadTooLarge)", t_oversize_payload_nack),
        ("permanent_nack_then_close", t_nack_then_close),
    ]
    results = [check(name, fn) for name, fn in tests]
    ok = sum(results)
    print(f"\n{ok}/{len(results)} live tests passed")
    sys.exit(0 if ok == len(results) else 1)


if __name__ == "__main__":
    main()
