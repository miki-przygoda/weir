"""A peer's declared payload_len must not choose this client's allocation.

`_read_response_frame` used to read `payload_len` out of the 16-byte response
header and pass it straight to `_recv_exactly` with no bound of any kind, so a
desynced or hostile daemon could declare 8 MiB and get an 8 MiB read that only
the socket timeout ended. The published checklist
(docs/wire_protocol.md, "Response sizes") requires a client to bound the
declared length by the message type; every response this client can receive
carries at most two bytes, because it never sends PushTracked (0x06).

Run: python3 demos/py-wire-client/tests/test_response_cap.py
"""

import os
import pathlib
import socket
import struct
import sys
import threading
import time
import tracemalloc
import zlib

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire.client import WeirClient  # noqa: E402
from weir_wire.codec import MAX_RESPONSE_PAYLOAD, DecodeError, Durability  # noqa: E402

DECLARED = 8 * 1024 * 1024  # well under the 16 MiB send cap, so only the
# response cap can reject it


def hostile_header() -> bytes:
    """A well-formed Nack header declaring DECLARED bytes. No body follows."""
    h = bytearray(b"WEIR")
    h += bytes([1, 0x03, 0x01, 0])  # version, Nack, Durable, flags
    h += struct.pack("<I", DECLARED)
    return bytes(h) + struct.pack("<I", zlib.crc32(bytes(h)) & 0xFFFFFFFF)


def run() -> int:
    # A short path: AF_UNIX caps sun_path near 104 bytes and the usual temp
    # directories on macOS exceed it.
    sock_path = f"/tmp/wpycap{os.getpid()}.sock"
    pathlib.Path(sock_path).unlink(missing_ok=True)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(2)
    blob = hostile_header()
    stop = threading.Event()

    def serve() -> None:
        try:
            conn, _ = srv.accept()
        except OSError:
            return
        try:
            conn.recv(65536)
            conn.sendall(blob)
            # Hold it open, so a client that trusts the length blocks rather
            # than getting an EOF that would mask the defect.
            stop.wait(3.0)
        except OSError:
            pass
        finally:
            conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    tracemalloc.start()
    started = time.monotonic()
    outcome: str
    try:
        with WeirClient(sock_path, connect_timeout=3.0) as c:
            c.push(b"x", Durability.DURABLE)
        outcome = "accepted"
    except DecodeError as exc:
        outcome = f"DecodeError:{exc.tag}"
    except Exception as exc:  # noqa: BLE001 - any other failure is a failure
        outcome = f"{type(exc).__name__}"
    elapsed = time.monotonic() - started
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    stop.set()
    srv.close()
    pathlib.Path(sock_path).unlink(missing_ok=True)

    failures = []
    if outcome != "DecodeError:PayloadTooLarge":
        failures.append(
            f"want DecodeError(PayloadTooLarge) for a {DECLARED}-byte declared "
            f"response, got {outcome}"
        )
    # The rejection must happen before the body read, not after: returning an
    # error having already allocated 8 MiB would still be the bug.
    if peak > 256 * 1024:
        failures.append(
            f"rejected the response but allocated {peak:,} bytes doing it; the "
            f"cap must be checked before _recv_exactly, not after"
        )
    if elapsed > 1.0:
        failures.append(
            f"took {elapsed:.2f}s to reject; it must not wait for a body it was "
            f"never going to accept"
        )

    print(f"cap = {MAX_RESPONSE_PAYLOAD} bytes; declared = {DECLARED:,}")
    print(f"outcome = {outcome}, elapsed = {elapsed:.3f}s, peak alloc = {peak:,} B")
    for f in failures:
        print(f"FAIL: {f}")
    print("1/1 checks passed — all good" if not failures else f"\n{len(failures)} FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())
