"""A Nack frame with an empty payload is a stream desync, not a transient Nack.

The spec gives a Nack one reason byte. `_send_record` used to read a missing
byte as INTERNAL_ERROR, so the caller got a retryable NackError and kept using
a stream whose framing it could no longer trust. The Rust reference client
reports it as a protocol error and poisons (crates/weir-client/src/unix.rs,
empty_nack_payload_is_protocol_desync_and_poisons).

`health_check` had the same gap and a wider one: it read an empty Nack, and a
reply of the wrong kind (an Ack), as an unhealthy daemon, returned False and
kept the stream open. The Rust client's `health_check` poisons on both.

Run: python3 demos/py-wire-client/tests/test_empty_nack.py
"""

import os
import pathlib
import socket
import struct
import sys
import threading
import zlib

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire.client import NackError, WeirClient, WeirError  # noqa: E402
from weir_wire.codec import Durability  # noqa: E402


def reply_frame(message_type: int) -> bytes:
    """A well-formed reply frame (valid header and payload CRCs), payload_len 0."""
    h = bytearray(b"WEIR")
    h += bytes([1, message_type, 0x01, 0])  # version, type, Durable, flags
    h += struct.pack("<I", 0)
    h += struct.pack("<I", zlib.crc32(bytes(h)) & 0xFFFFFFFF)
    return bytes(h) + struct.pack("<I", zlib.crc32(b"") & 0xFFFFFFFF)


NACK = 0x03
ACK = 0x02

# (label, reply the fake peer sends, call under test)
CASES = [
    ("push answered with an empty Nack", NACK, lambda c: c.push(b"x", Durability.DURABLE)),
    ("health_check answered with an empty Nack", NACK, lambda c: c.health_check()),
    ("health_check answered with an Ack", ACK, lambda c: c.health_check()),
]


def check(label: str, reply: bytes, call, failures: list) -> str:
    # A short path: AF_UNIX caps sun_path near 104 bytes.
    sock_path = f"/tmp/wpynack{os.getpid()}.sock"
    pathlib.Path(sock_path).unlink(missing_ok=True)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(2)
    stop = threading.Event()

    def serve() -> None:
        try:
            conn, _ = srv.accept()
        except OSError:
            return
        try:
            conn.recv(65536)
            conn.sendall(reply)
            # Hold it open: a client that keeps the connection must not be
            # rescued by an EOF.
            stop.wait(3.0)
        except OSError:
            pass
        finally:
            conn.close()

    threading.Thread(target=serve, daemon=True).start()

    c = WeirClient(sock_path, connect_timeout=3.0)
    c.connect()
    try:
        result = call(c)
        outcome = f"returned {result!r}"
        failures.append(f"{label}: {outcome}; want a desync WeirError")
    except NackError as exc:
        outcome = f"NackError({exc.reason}, retryable={exc.retryable})"
        failures.append(f"{label}: surfaced as {outcome}; want a desync WeirError")
    except WeirError as exc:
        outcome = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 - any other failure is a failure
        outcome = f"{type(exc).__name__}: {exc}"
        failures.append(f"{label}: want a WeirError, got {outcome}")
    if c._sock is not None:
        failures.append(f"{label}: the client kept the desynced connection open")
    c.close()

    stop.set()
    srv.close()
    pathlib.Path(sock_path).unlink(missing_ok=True)
    return outcome


def run() -> int:
    failures: list = []
    for label, message_type, call in CASES:
        outcome = check(label, reply_frame(message_type), call, failures)
        print(f"{label}: {outcome}")
    for f in failures:
        print(f"FAIL: {f}")
    total = 2 * len(CASES)
    print(f"{total}/{total} checks passed" if not failures else f"\n{len(failures)} FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())
