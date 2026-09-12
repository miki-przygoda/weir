"""Run MY codec against the tracked-extension conformance vectors.

The frozen `wire_v1_vectors.json` is deliberately untouched by the tracked
extension — `PushTracked`/`AckTracked` are additive message-type bytes within
wire v1, so an implementation that ignores them stays fully conformant
(docs/conformance.md). This file exercises the separate
`wire_v1_tracked_vectors.json` for a client that does implement them.

Why it matters that this is a from-spec implementation: until now the tracked
vectors were checked by exactly two things — `weir-core` and the reference
runner in `docs/conformance/` — both written by the same author from the same
understanding. An independent decoder agreeing with them is a stronger claim
than either making it alone.

Run: python3 demos/py-wire-client/tests/test_tracked.py
"""

import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire.codec import (  # noqa: E402
    MAX_PAYLOAD_HARD_CAP,
    MAX_RESPONSE_PAYLOAD,
    MAX_TRACKED_ACK_PAYLOAD,
    CoordinateError,
    DecodeError,
    MessageType,
    decode_coordinate,
    decode_frame,
    encode_frame,
    max_response_payload,
)

REPO = HERE.parents[2]

# The vectors use the spec's CamelCase names for message types.
SPEC_NAME = {
    MessageType.PUSH: "Push",
    MessageType.ACK: "Ack",
    MessageType.NACK: "Nack",
    MessageType.HEALTH_CHECK: "HealthCheck",
    MessageType.HEALTH_CHECK_RESPONSE: "HealthCheckResponse",
    MessageType.PUSH_TRACKED: "PushTracked",
    MessageType.ACK_TRACKED: "AckTracked",
}

VECTORS = REPO / "docs" / "conformance" / "wire_v1_tracked_vectors.json"


def run() -> int:
    doc = json.loads(VECTORS.read_text())
    passed = failed = 0

    def check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal passed, failed
        if cond:
            passed += 1
        else:
            failed += 1
            print(f"  FAIL {name}: {detail}")

    # ── Frames ────────────────────────────────────────────────────────────
    for v in doc["frame_vectors"]:
        raw = bytes.fromhex(v["hex"])
        name = v["name"]
        try:
            # Frame vectors include both directions. The response cap bounds
            # what a client will READ from a peer; a PushTracked is a request
            # carrying a record, bounded by the send-side cap instead.
            frame = decode_frame(raw, max_payload_bytes=MAX_PAYLOAD_HARD_CAP)
        except DecodeError as exc:
            check(name, v["decode"] != "ok", f"unexpected DecodeError({exc.tag})")
            continue

        want_type = v["message_type"]
        got_type = SPEC_NAME[MessageType(frame.message_type)]
        check(
            f"{name}: message_type",
            got_type == want_type,
            f"want {want_type}, got {got_type}",
        )
        check(
            f"{name}: payload",
            frame.payload.hex() == v["payload_hex"],
            "payload bytes differ",
        )
        # Re-encoding a vector must reproduce it byte-for-byte, or the encoder
        # and decoder disagree about a layout one of them is guessing at.
        if v["decode"] == "ok":
            again = encode_frame(
                MessageType(frame.message_type),
                frame.durability,
                frame.payload,
                flags=frame.flags,
            )
            check(f"{name}: re-encode round-trips", again == raw, "bytes differ")

    # ── Coordinates ───────────────────────────────────────────────────────
    for v in doc["coordinate_vectors"]:
        raw = bytes.fromhex(v["hex"])
        name = v["name"]
        want = v["decode"]
        try:
            coord = decode_coordinate(raw)
            got = "ok"
        except CoordinateError as exc:
            got = exc.tag
            coord = None
        check(f"{name}: verdict", got == want, f"want {want}, got {got}")
        if want == "ok" and coord is not None:
            check(f"{name}: segment", coord.segment == v["segment"], "segment differs")
            check(f"{name}: index", coord.index == v["index"], "index differs")
            check(
                f"{name}: record_id",
                coord.record_id_hex() == v["record_id_hex"],
                "record_id differs",
            )

    # ── The constants the file declares about itself ──────────────────────
    check(
        "max_tracked_ack_payload_len agrees with the vectors",
        MAX_TRACKED_ACK_PAYLOAD == doc["max_tracked_ack_payload_len"],
        f"{MAX_TRACKED_ACK_PAYLOAD} vs {doc['max_tracked_ack_payload_len']}",
    )

    # ── The response cap, which is what tracked push actually changes ─────
    #
    # AckTracked is the only weir response whose payload exceeds two bytes, so
    # the read-side bound is widened for exactly that type. Widening it for
    # anything else would hand a desynced peer a bigger allocation.
    for v in doc["frame_vectors"]:
        if v["message_type"] != "AckTracked":
            continue
        payload_len = len(v["payload_hex"]) // 2
        check(
            f"{v['name']}: exceeds the default response cap",
            payload_len > MAX_RESPONSE_PAYLOAD,
            f"{payload_len} bytes does not exercise the widened cap",
        )
        check(
            f"{v['name']}: fits the tracked cap",
            payload_len <= MAX_TRACKED_ACK_PAYLOAD,
            f"{payload_len} > {MAX_TRACKED_ACK_PAYLOAD}",
        )
    check(
        "the cap is widened for AckTracked only",
        max_response_payload(int(MessageType.ACK_TRACKED)) == MAX_TRACKED_ACK_PAYLOAD
        and all(
            max_response_payload(mt) == MAX_RESPONSE_PAYLOAD
            for mt in (0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0xFF)
        ),
        "a non-AckTracked type unlocked the wider bound",
    )

    # ── The compatibility claim, made executable ──────────────────────────
    #
    # The extension is called additive because a decoder that knows only
    # 0x01-0x05 rejects a tracked frame as an unknown type rather than
    # misparsing it. Asserting the type bytes sit outside that range is what
    # stops that being merely a promise.
    for v in doc["frame_vectors"]:
        mt = bytes.fromhex(v["hex"])[5]
        check(
            f"{v['name']}: type {mt:#04x} is outside the v1-only table",
            mt > 0x05,
            "a v1-only decoder would have accepted this as a known type",
        )

    total = passed + failed
    tail = f", {failed} FAILED" if failed else " — all good"
    print(f"\n{passed}/{total} tracked-extension checks passed{tail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run())
