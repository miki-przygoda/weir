"""Run MY codec against the shipped conformance vectors.

This points at the real docs/conformance/wire_v1_vectors.json in the weir repo.
For each vector: hex-decode, run my decoder, assert outcome matches `decode`,
and for "ok" vectors assert the decoded fields AND that re-encoding round-trips
byte-for-byte.

Adapter mapping: the vectors use string names for message_type/durability;
my codec uses IntEnums. I bridge them here.
"""

import json
import os
import pathlib
import sys

# Make the src/ package importable without installing.
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire.codec import (  # noqa: E402
    DecodeError,
    Durability,
    MessageType,
    decode_frame,
    encode_frame,
)

# The canonical conformance vectors live in the weir repo at
# docs/conformance/wire_v1_vectors.json. Resolve them relative to this file
# (demos/py-wire-client/tests/ -> repo root is three parents up), or override with
# the WEIR_CONFORMANCE_VECTORS env var. No vendored copy — the docs file is the
# single source of truth.
_REPO_ROOT = HERE.parents[2]
VECTORS = pathlib.Path(
    os.environ.get(
        "WEIR_CONFORMANCE_VECTORS",
        str(_REPO_ROOT / "docs" / "conformance" / "wire_v1_vectors.json"),
    )
)

MT_NAME = {
    MessageType.PUSH: "Push",
    MessageType.ACK: "Ack",
    MessageType.NACK: "Nack",
    MessageType.HEALTH_CHECK: "HealthCheck",
    MessageType.HEALTH_CHECK_RESPONSE: "HealthCheckResponse",
}
MT_FROM_NAME = {v: k for k, v in MT_NAME.items()}
DUR_NAME = {
    Durability.SYNC: "Sync",
    Durability.BATCHED: "Batched",
    Durability.BUFFERED: "Buffered",
}
DUR_FROM_NAME = {v: k for k, v in DUR_NAME.items()}


def run() -> int:
    doc = json.loads(VECTORS.read_text())
    cap = doc["max_payload_hard_cap"]
    passed = failed = 0

    for v in doc["vectors"]:
        name = v["name"]
        buf = bytes.fromhex(v["hex"])
        try:
            frame = decode_frame(buf, cap)
            got = "ok"
        except DecodeError as e:
            frame = None
            got = e.tag

        if got != v["decode"]:
            print(f"FAIL {name}: decode = {got!r}, expected {v['decode']!r}")
            failed += 1
            continue

        if v["decode"] == "ok":
            got_mt = MT_NAME[frame.message_type]
            got_dur = DUR_NAME[Durability(frame.durability)]
            got_payload = frame.payload.hex()
            problem = None
            if got_mt != v["message_type"]:
                problem = f"message_type={got_mt!r} != {v['message_type']!r}"
            elif got_dur != v["durability"]:
                problem = f"durability={got_dur!r} != {v['durability']!r}"
            elif frame.flags != v["flags"]:
                problem = f"flags={frame.flags!r} != {v['flags']!r}"
            elif got_payload != v["payload_hex"]:
                problem = f"payload_hex={got_payload!r} != {v['payload_hex']!r}"
            if problem:
                print(f"FAIL {name}: {problem}")
                failed += 1
                continue

            re_encoded = encode_frame(
                MT_FROM_NAME[v["message_type"]],
                DUR_FROM_NAME[v["durability"]],
                bytes.fromhex(v["payload_hex"]),
                flags=v["flags"],
            ).hex()
            if re_encoded != v["hex"]:
                print(f"FAIL {name}: re-encode\n  got {re_encoded}\n  exp {v['hex']}")
                failed += 1
                continue

        passed += 1

    total = passed + failed
    tail = f", {failed} FAILED" if failed else " — all good"
    print(f"\n{passed}/{total} vectors passed{tail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run())
