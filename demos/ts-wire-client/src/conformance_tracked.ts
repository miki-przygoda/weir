/**
 * Tracked-extension conformance: runs wire_v1_tracked_vectors.json against this
 * client's coordinate codec and its response-cap policy.
 *
 * The frozen wire_v1_vectors.json is deliberately untouched by this extension.
 * PushTracked/AckTracked are additive message-type bytes within wire v1, so a
 * client that ignores them stays fully conformant (docs/conformance.md); this
 * file is for one that does not.
 *
 * Run:  node src/conformance_tracked.ts [path/to/wire_v1_tracked_vectors.json]
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  CoordinateError,
  MAX_RESPONSE_PAYLOAD,
  MAX_TRACKED_ACK_PAYLOAD,
  MessageType,
  decodeCoordinate,
  decodeFrame,
  encodeFrame,
  maxResponsePayload,
  messageTypeName,
  recordIdHex,
} from "./wire.ts";

const DEFAULT_VECTORS = fileURLToPath(
  new URL("../../../docs/conformance/wire_v1_tracked_vectors.json", import.meta.url),
);

interface FrameVector {
  name: string;
  hex: string;
  decode: string;
  message_type: string;
  payload_hex: string;
}

interface CoordinateVector {
  name: string;
  hex: string;
  decode: string;
  segment?: string;
  index?: string; // kept as a string — see parseVectors
  record_id_hex?: string;
}

/**
 * Parse the vector file WITHOUT losing the top of the u64 range.
 *
 * JSON.parse turns every number into a double, and coordinate_max_segment's
 * index is u64::MAX. Measured: it comes back as 18446744073709552000 rather
 * than 18446744073709551615. A reviver does not help — it receives the already
 * lossy value. So quote the token before parsing and convert with BigInt.
 *
 * A bigint-aware JSON parser would be cleaner and is deliberately not used:
 * this client has zero runtime dependencies on purpose (see README), and a
 * five-line regex is a smaller price than breaking that.
 */
function parseVectors(text: string): {
  frame_vectors: FrameVector[];
  coordinate_vectors: CoordinateVector[];
  max_tracked_ack_payload_len: number;
} {
  return JSON.parse(text.replace(/"index":\s*(\d+)/g, '"index":"$1"'));
}

function run(path: string): number {
  const doc = parseVectors(readFileSync(path, "utf8"));
  const failures: string[] = [];
  let pass = 0;

  const check = (name: string, cond: boolean, detail = "") => {
    if (cond) pass++;
    else failures.push(`  FAIL ${name}${detail ? `: ${detail}` : ""}`);
  };

  // ── Frames ────────────────────────────────────────────────────────────
  for (const v of doc.frame_vectors) {
    const raw = Buffer.from(v.hex, "hex");
    let frame;
    try {
      frame = decodeFrame(raw);
    } catch (e) {
      check(v.name, v.decode !== "ok", `unexpected ${(e as Error).message}`);
      continue;
    }
    check(
      `${v.name}: message_type`,
      messageTypeName(frame.messageType) === v.message_type,
      `got ${messageTypeName(frame.messageType)}, want ${v.message_type}`,
    );
    check(
      `${v.name}: payload`,
      frame.payload.toString("hex") === v.payload_hex,
      "payload bytes differ",
    );
    // Re-encoding must reproduce the vector byte-for-byte, or the encoder and
    // decoder disagree about a layout one of them is guessing at.
    const again = encodeFrame(frame.payload, {
      messageType: frame.messageType,
      durability: frame.durability,
      flags: frame.flags,
    });
    check(`${v.name}: re-encode round-trips`, again.toString("hex") === v.hex, "bytes differ");
  }

  // ── Coordinates ───────────────────────────────────────────────────────
  for (const v of doc.coordinate_vectors) {
    const raw = Buffer.from(v.hex, "hex");
    let got = "ok";
    let coord;
    try {
      coord = decodeCoordinate(raw);
    } catch (e) {
      got = e instanceof CoordinateError ? e.tag : `unexpected:${(e as Error).message}`;
    }
    check(`${v.name}: verdict`, got === v.decode, `got ${got}, want ${v.decode}`);
    if (v.decode === "ok" && coord) {
      check(`${v.name}: segment`, coord.segment === v.segment, "segment differs");
      check(
        `${v.name}: index`,
        coord.index === BigInt(v.index!),
        `got ${coord.index}, want ${v.index}`,
      );
      check(`${v.name}: record_id`, recordIdHex(coord) === v.record_id_hex, "record_id differs");
    }
  }

  // ── The precision hazard, demonstrated rather than remembered ─────────
  const lossy = JSON.parse('{"index": 18446744073709551615}').index as number;
  check(
    "JSON.parse loses precision on u64::MAX",
    String(lossy) !== "18446744073709551615",
    `JSON.parse returned ${lossy} exactly, so the BigInt workaround is no longer needed ` +
      `— but leaving it costs nothing and removing it would be a silent regression`,
  );

  // ── The response cap, which is what tracked push changes on the read path ──
  check(
    "the cap is widened for AckTracked",
    maxResponsePayload(MessageType.AckTracked) === MAX_TRACKED_ACK_PAYLOAD,
  );
  for (const mt of [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0xff]) {
    check(
      `the cap stays ${MAX_RESPONSE_PAYLOAD} for type 0x${mt.toString(16)}`,
      maxResponsePayload(mt) === MAX_RESPONSE_PAYLOAD,
      "a non-AckTracked type unlocked the wider bound",
    );
  }
  for (const v of doc.frame_vectors) {
    if (v.message_type !== "AckTracked") continue;
    const n = v.payload_hex.length / 2;
    check(
      `${v.name}: exceeds the default response cap`,
      n > MAX_RESPONSE_PAYLOAD,
      `${n} bytes does not exercise the widened cap`,
    );
    check(`${v.name}: fits the tracked cap`, n <= MAX_TRACKED_ACK_PAYLOAD, `${n} bytes`);
  }

  // ── Additivity, made executable ───────────────────────────────────────
  // A decoder knowing only 0x01-0x05 must reject a tracked frame as an unknown
  // message type rather than misparsing it. That is the whole basis for calling
  // the extension additive.
  for (const v of doc.frame_vectors) {
    const mt = Buffer.from(v.hex, "hex").readUInt8(5);
    check(
      `${v.name}: type 0x${mt.toString(16)} is outside the v1-only table`,
      mt > 0x05,
      "a v1-only decoder would have accepted this as a known type",
    );
  }

  check(
    "max_tracked_ack_payload_len agrees with the vectors",
    MAX_TRACKED_ACK_PAYLOAD === doc.max_tracked_ack_payload_len,
    `${MAX_TRACKED_ACK_PAYLOAD} vs ${doc.max_tracked_ack_payload_len}`,
  );

  const total = pass + failures.length;
  console.log(
    `tracked conformance: ${pass}/${total} checks passed` +
      (failures.length ? `, ${failures.length} FAILED` : " — all good"),
  );
  if (failures.length) console.log(failures.join("\n"));
  return failures.length;
}

const path = process.argv[2] ?? DEFAULT_VECTORS;
process.exit(run(path) === 0 ? 0 : 1);
