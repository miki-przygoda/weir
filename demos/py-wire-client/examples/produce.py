#!/usr/bin/env python3
"""Tiny end-to-end demo: produce a few records to a running weir daemon.

    python3 examples/produce.py [socket_path]

Run scripts/run_daemon.sh first (default socket: ./weir.sock).
"""

import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from weir_wire import Durability, NackError, WeirClient, ConnectionClosed

SOCKET = sys.argv[1] if len(sys.argv) > 1 else "weir.sock"


def main():
    with WeirClient(SOCKET) as c:
        if not c.health_check():
            print("daemon is not healthy")
            return 1
        print("daemon healthy")

        for i in range(5):
            payload = f'{{"event": "signup", "user": {i}}}'.encode()
            try:
                r = c.push(payload, Durability.SYNC)
                print(f"  push #{i}: acked={r.acked} tier={r.durability_used.name}")
            except NackError as e:
                # retryable tells the caller whether a fresh-connection retry is sane
                print(f"  push #{i}: NACK {e.reason} (retryable={e.retryable})")
            except ConnectionClosed:
                print(f"  push #{i}: connection closed mid-stream; outcome UNKNOWN, retry on fresh conn")
                c.connect()  # reconnect for subsequent pushes
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
