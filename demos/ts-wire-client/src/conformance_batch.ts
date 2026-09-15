/**
 * Batch-extension conformance: runs wire_v1_batch_vectors.json against this
 * client's batch codec and its response-cap policy.
 *
 * The frozen wire_v1_vectors.json is deliberately untouched by this extension.
 * PushBatch/AckBatch are additive message-type bytes within wire v1, so a client
 * that ignores them stays fully conformant (docs/conformance.md); this file is
 * for one that does not.
 *
 * Why an independent implementation matters more here than anywhere else in the
 * protocol: a bitmap written with the opposite bit order is a WELL-FORMED frame
 * — header CRC valid, payload CRC valid, payload_len correct — that reports
 * failures as successes. No checksum, length check or cap detects it; only an
 * asymmetric vector at N > 8 does.
 *
 * Run:  node src/conformance_batch.ts [path/to/wire_v1_batch_vectors.json]
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  ACK_BATCH_VERSION,
  BatchError,
  MAX_ACK_BATCH_PAYLOAD,
  MAX_BATCH_RECORDS_HARD_CAP,
  MAX_PAYLOAD_HARD_CAP,
  MAX_RESPONSE_PAYLOAD,
  MAX_TRACKED_ACK_PAYLOAD,
  MessageType,
  decodeAckBatch,
  decodeBatchBody,
  decodeFrame,
  encodeAckBatch,
  encodeFrame,
  encodeBatchBody,
  maxResponsePayload,
  messageTypeName,
} from "./wire.ts";

const DEFAULT_VECTORS = fileURLToPath(
  new URL("../../../docs/conformance/wire_v1_batch_vectors.json", import.meta.url),
);

interface FrameVector {
  name: string;
  hex: string;
  decode: string;
  message_type: string;
  payload_hex: string;
}

interface BodyVector {
  name: string;
  hex: string;
  decode: string;
  records_hex?: string[];
}

interface AckVector {
  name: string;
  hex: string;
  decode: string;
  expected: number;
  accepted?: boolean[];
  accepted_all?: boolean;
}

interface BatchDoc {
  max_batch_records_hard_cap: number;
  max_ack_batch_payload_len: number;
  frame_vectors: FrameVector[];
  body_vectors: BodyVector[];
  ack_vectors: AckVector[];
}

let passed = 0;
let failed = 0;

function check(label: string, cond: boolean, detail = ""): void {
  if (cond) {
    passed++;
  } else {
    failed++;
    console.log(`  FAIL  ${label}${detail ? `: ${detail}` : ""}`);
  }
}

/** The tag a rejection is expected to carry, or null if it decoded. */
function rejectionTag(fn: () => unknown): string | null {
  try {
    fn();
    return null;
  } catch (e) {
    if (e instanceof BatchError) return e.kind;
    throw e;
  }
}

function main(): number {
  const path = process.argv[2] ?? DEFAULT_VECTORS;
  const doc: BatchDoc = JSON.parse(readFileSync(path, "utf8"));

  // If the vectors were generated against different constants than this client
  // holds, every byte comparison below is meaningless.
  check(
    "hard cap matches the vectors",
    doc.max_batch_records_hard_cap === MAX_BATCH_RECORDS_HARD_CAP,
    `${doc.max_batch_records_hard_cap} != ${MAX_BATCH_RECORDS_HARD_CAP}`,
  );
  check(
    "ack payload bound matches the vectors",
    doc.max_ack_batch_payload_len === MAX_ACK_BATCH_PAYLOAD,
    `${doc.max_ack_batch_payload_len} != ${MAX_ACK_BATCH_PAYLOAD}`,
  );
  // The reason the hard cap is 2048: the reply stays inside the bound this
  // client already had, so batching adds no new allocation maximum.
  check(
    "AckBatch introduces no new largest response",
    MAX_ACK_BATCH_PAYLOAD <= MAX_TRACKED_ACK_PAYLOAD,
    `${MAX_ACK_BATCH_PAYLOAD} > ${MAX_TRACKED_ACK_PAYLOAD}`,
  );

  // ── Frames ────────────────────────────────────────────────────────────
  for (const v of doc.frame_vectors) {
    const raw = Buffer.from(v.hex, "hex");
    const frame = decodeFrame(raw);
    check(
      `${v.name}: message_type`,
      messageTypeName(frame.messageType) === v.message_type,
      `${messageTypeName(frame.messageType)} != ${v.message_type}`,
    );
    check(
      `${v.name}: payload`,
      frame.payload.toString("hex") === v.payload_hex,
      `${frame.payload.toString("hex")} != ${v.payload_hex}`,
    );
    check(
      `${v.name}: re-encode is byte-identical`,
      encodeFrame(frame.payload, {
        messageType: frame.messageType,
        durability: frame.durability,
        flags: frame.flags,
      })
        .toString("hex") === v.hex,
    );
    // Additivity, made executable: the type byte must sit outside the v1-only
    // table, so a decoder knowing 0x01-0x05 rejects rather than misparses.
    check(`${v.name}: type byte is outside the v1-only table`, raw[5] > 0x05);

    if (v.message_type === "AckBatch") {
      const n = frame.payload.length;
      check(
        `${v.name}: exceeds the default response cap`,
        n > MAX_RESPONSE_PAYLOAD,
        `${n} bytes does not exercise the widening`,
      );
      check(
        `${v.name}: fits this client's AckBatch cap`,
        n <= maxResponsePayload(MessageType.AckBatch),
        `${n} > ${maxResponsePayload(MessageType.AckBatch)}`,
      );
    }
  }

  // ── PushBatch bodies ──────────────────────────────────────────────────
  let okSeen = 0;
  let rejectSeen = 0;
  for (const v of doc.body_vectors) {
    const raw = Buffer.from(v.hex, "hex");
    const tag = rejectionTag(() =>
      decodeBatchBody(raw, MAX_BATCH_RECORDS_HARD_CAP, MAX_PAYLOAD_HARD_CAP),
    );
    if (v.decode !== "ok") {
      check(`${v.name}: rejection tag`, tag === v.decode, `rejected as ${tag}, want ${v.decode}`);
      rejectSeen++;
      continue;
    }
    if (tag !== null) {
      check(`${v.name}: expected ok`, false, `rejected as ${tag}`);
      continue;
    }
    const records = decodeBatchBody(raw, MAX_BATCH_RECORDS_HARD_CAP, MAX_PAYLOAD_HARD_CAP);
    const got = records.map((r) => r.toString("hex"));
    check(
      `${v.name}: records`,
      JSON.stringify(got) === JSON.stringify(v.records_hex),
      `${JSON.stringify(got)} != ${JSON.stringify(v.records_hex)}`,
    );
    check(
      `${v.name}: re-encode is byte-identical`,
      encodeBatchBody(records).toString("hex") === v.hex,
    );
    okSeen++;
  }
  check(
    "body vectors cover both outcomes",
    okSeen > 0 && rejectSeen > 0,
    `${okSeen} ok, ${rejectSeen} rejected`,
  );

  // ── AckBatch payloads ─────────────────────────────────────────────────
  okSeen = 0;
  rejectSeen = 0;
  for (const v of doc.ack_vectors) {
    const raw = Buffer.from(v.hex, "hex");
    const tag = rejectionTag(() => decodeAckBatch(raw, v.expected));
    if (v.decode !== "ok") {
      check(`${v.name}: rejection tag`, tag === v.decode, `rejected as ${tag}, want ${v.decode}`);
      rejectSeen++;
      continue;
    }
    if (tag !== null) {
      check(`${v.name}: expected ok`, false, `rejected as ${tag}`);
      continue;
    }
    const accepted = decodeAckBatch(raw, v.expected);
    const want = v.accepted ?? new Array(v.expected).fill(v.accepted_all);
    check(
      `${v.name}: verdicts`,
      JSON.stringify(accepted) === JSON.stringify(want),
      "if this is the N=9 asymmetric vector, this client's bitmap bit order is inverted",
    );
    check(
      `${v.name}: re-encode is byte-identical`,
      encodeAckBatch(accepted).toString("hex") === v.hex,
    );
    okSeen++;
  }
  check(
    "ack vectors cover both outcomes",
    okSeen > 0 && rejectSeen > 0,
    `${okSeen} ok, ${rejectSeen} rejected`,
  );

  // ── The bitmap convention, against a hand-written literal ─────────────
  //
  // Every other assertion here compares this client to the vectors. This one
  // compares it to bytes spelled out in the source, so the convention is
  // visible without running anything.
  const literal = encodeAckBatch([true, false, true, false, false, false, false, false, true]);
  check(
    "the bitmap is LSB-first",
    literal.equals(Buffer.from([ACK_BATCH_VERSION, 9, 0, 0b0000_0101, 0b0000_0001])),
    `${literal.toString("hex")} — MSB-first would be 010900a080`,
  );

  // ── The response cap, swept over the whole type space ─────────────────
  //
  // Not over a list of the types that exist today: a list stops covering the
  // space the moment a byte is assigned.
  const widened = new Map<number, number>([
    [MessageType.AckTracked, MAX_TRACKED_ACK_PAYLOAD],
    [MessageType.AckBatch, MAX_ACK_BATCH_PAYLOAD],
  ]);
  const wrong: string[] = [];
  for (let b = 0; b <= 0xff; b++) {
    const want = widened.get(b) ?? MAX_RESPONSE_PAYLOAD;
    if (maxResponsePayload(b) !== want) wrong.push(`${b.toString(16)}: ${maxResponsePayload(b)} != ${want}`);
  }
  check("the cap is widened for exactly the types that need it", wrong.length === 0, wrong.join(", "));

  const total = passed + failed;
  console.log(
    `\n${passed}/${total} batch-extension checks passed` +
      (failed ? `, ${failed} FAILED` : " — all good"),
  );
  return failed ? 1 : 0;
}

process.exit(main());
