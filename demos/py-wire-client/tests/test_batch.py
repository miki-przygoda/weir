"""Run MY batch codec against the batch-extension conformance vectors.

The frozen `wire_v1_vectors.json` is deliberately untouched by this extension —
`PushBatch`/`AckBatch` are additive message-type bytes within wire v1, so an
implementation that ignores them stays fully conformant (docs/conformance.md).
This file exercises the separate `wire_v1_batch_vectors.json` for a client that
does implement them.

The bitmap is why an independent implementation matters more here than
anywhere else in the protocol. A bitmap written with the opposite bit order is a
WELL-FORMED frame — header CRC valid, payload CRC valid, `payload_len` correct
— that reports failures as successes. No checksum, length check or cap detects
it; only a vector with an asymmetric pattern at N > 8 does.

Run: python3 demos/py-wire-client/tests/test_batch.py
"""

import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "src"))

from weir_wire.codec import (  # noqa: E402
    ACK_BATCH_VERSION,
    MAX_ACK_BATCH_PAYLOAD,
    MAX_BATCH_RECORDS_HARD_CAP,
    MAX_PAYLOAD_HARD_CAP,
    MAX_RESPONSE_PAYLOAD,
    MAX_TRACKED_ACK_PAYLOAD,
    BatchError,
    MessageType,
    decode_ack_batch,
    decode_batch_body,
    decode_frame,
    encode_ack_batch,
    encode_batch_body,
    max_response_payload,
)

VECTORS = HERE.parent.parent.parent / "docs" / "conformance" / "wire_v1_batch_vectors.json"

# The vectors spell message types in the spec's CamelCase; this client's enum
# uses Python's SCREAMING_SNAKE. Mapping the two explicitly beats transforming
# one into the other, which silently starts matching the wrong member the first
# time a type name contains a word boundary the transform does not know about.
BY_NAME = {
    "PushBatch": MessageType.PUSH_BATCH,
    "AckBatch": MessageType.ACK_BATCH,
}

passed = 0
failed = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
    else:
        failed += 1
        print(f"FAIL {label}" + (f": {detail}" if detail else ""))


def run() -> int:
    doc = json.loads(VECTORS.read_text())

    # If the vectors were generated against different constants than this client
    # compiles in, every byte comparison below is meaningless.
    check(
        "hard cap matches the vectors",
        doc["max_batch_records_hard_cap"] == MAX_BATCH_RECORDS_HARD_CAP,
        f'{doc["max_batch_records_hard_cap"]} != {MAX_BATCH_RECORDS_HARD_CAP}',
    )
    check(
        "ack payload bound matches the vectors",
        doc["max_ack_batch_payload_len"] == MAX_ACK_BATCH_PAYLOAD,
        f'{doc["max_ack_batch_payload_len"]} != {MAX_ACK_BATCH_PAYLOAD}',
    )
    # The reason the hard cap is 2048: the reply stays inside the bound this
    # client already had, so batching adds no new allocation maximum.
    check(
        "AckBatch introduces no new largest response",
        MAX_ACK_BATCH_PAYLOAD <= MAX_TRACKED_ACK_PAYLOAD,
        f"{MAX_ACK_BATCH_PAYLOAD} > {MAX_TRACKED_ACK_PAYLOAD}",
    )

    # ── Frames ────────────────────────────────────────────────────────────
    for v in doc["frame_vectors"]:
        raw = bytes.fromhex(v["hex"])
        try:
            frame = decode_frame(raw)
        except Exception as e:  # noqa: BLE001
            check(f'{v["name"]}: decodes', False, repr(e))
            continue
        check(
            f'{v["name"]}: message_type',
            frame.message_type == BY_NAME[v["message_type"]],
            f'{frame.message_type!r} vs {v["message_type"]}',
        )
        check(
            f'{v["name"]}: payload',
            frame.payload.hex() == v["payload_hex"],
            f'{frame.payload.hex()} != {v["payload_hex"]}',
        )
        # The response cap must admit this payload. An AckBatch at the hard cap
        # is 259 bytes; a client still using the 2-byte default would refuse its
        # own daemon's reply.
        if v["message_type"] == "AckBatch":
            n = len(frame.payload)
            check(
                f'{v["name"]}: fits this client\'s AckBatch cap',
                n <= max_response_payload(int(MessageType.ACK_BATCH)),
                f"{n} > {max_response_payload(int(MessageType.ACK_BATCH))}",
            )

    # ── PushBatch bodies ──────────────────────────────────────────────────
    ok_seen = reject_seen = 0
    for v in doc["body_vectors"]:
        raw = bytes.fromhex(v["hex"])
        try:
            records = decode_batch_body(raw, MAX_BATCH_RECORDS_HARD_CAP, MAX_PAYLOAD_HARD_CAP)
        except BatchError as e:
            check(
                f'{v["name"]}: rejection tag',
                v["decode"] == str(e),
                f'rejected as {e}, expected {v["decode"]}',
            )
            reject_seen += 1
            continue
        check(f'{v["name"]}: expected rejection', v["decode"] == "ok", "it decoded")
        if v["decode"] != "ok":
            continue
        check(
            f'{v["name"]}: records',
            [r.hex() for r in records] == v["records_hex"],
            f'{[r.hex() for r in records]} != {v["records_hex"]}',
        )
        check(
            f'{v["name"]}: re-encode is byte-identical',
            encode_batch_body(records).hex() == v["hex"],
        )
        ok_seen += 1
    check(
        "body vectors cover both outcomes",
        ok_seen > 0 and reject_seen > 0,
        f"{ok_seen} ok, {reject_seen} rejected",
    )

    # ── AckBatch payloads ─────────────────────────────────────────────────
    ok_seen = reject_seen = 0
    for v in doc["ack_vectors"]:
        raw = bytes.fromhex(v["hex"])
        try:
            accepted = decode_ack_batch(raw, v["expected"])
        except BatchError as e:
            check(
                f'{v["name"]}: rejection tag',
                v["decode"] == str(e),
                f'rejected as {e}, expected {v["decode"]}',
            )
            reject_seen += 1
            continue
        check(f'{v["name"]}: expected rejection', v["decode"] == "ok", "it decoded")
        if v["decode"] != "ok":
            continue
        want = v.get("accepted")
        if want is None:
            want = [v["accepted_all"]] * v["expected"]
        check(
            f'{v["name"]}: verdicts',
            accepted == want,
            "if this is the N=9 asymmetric vector, this client's bitmap bit "
            "order is inverted",
        )
        check(
            f'{v["name"]}: re-encode is byte-identical',
            encode_ack_batch(accepted).hex() == v["hex"],
        )
        ok_seen += 1
    check(
        "ack vectors cover both outcomes",
        ok_seen > 0 and reject_seen > 0,
        f"{ok_seen} ok, {reject_seen} rejected",
    )

    # ── The bitmap convention, against a hand-written literal ─────────────
    #
    # Every other assertion here compares this client to the vectors. This one
    # compares it to bytes spelled out in the source, so the convention is
    # visible without running anything.
    literal = encode_ack_batch([True, False, True, False, False, False, False, False, True])
    check(
        "the bitmap is LSB-first",
        literal == bytes([ACK_BATCH_VERSION, 9, 0, 0b0000_0101, 0b0000_0001]),
        f"{literal.hex()} — MSB-first would be 010900a080",
    )

    # ── The compatibility claim, made executable ──────────────────────────
    #
    # The extension is additive because a decoder that knows only 0x01-0x05
    # rejects a batch frame as an unknown type rather than misparsing it.
    for v in doc["frame_vectors"]:
        mt = bytes.fromhex(v["hex"])[5]
        check(
            f'{v["name"]}: type {mt:#04x} is outside the v1-only table',
            mt > 0x05,
            "a v1-only decoder would have accepted this as a known type",
        )

    total = passed + failed
    tail = f", {failed} FAILED" if failed else " — all good"
    print(f"\n{passed}/{total} batch-extension checks passed{tail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run())
