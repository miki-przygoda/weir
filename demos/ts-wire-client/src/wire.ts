/**
 * weir v1 wire protocol — frame encode/decode.
 *
 * Pure, dependency-free implementation built from docs/wire_protocol.md and the
 * conformance vectors. Uses only Node stdlib `node:zlib` (for CRC-32).
 *
 * Frame layout (16-byte header + payload + 4-byte payload CRC):
 *
 *   0  4  magic        "WEIR"
 *   4  1  version      WIRE_VERSION (1)
 *   5  1  message_type MessageType
 *   6  1  durability   Durability
 *   7  1  flags        reserved; must be 0
 *   8  4  payload_len  u32 LE
 *  12  4  header_crc32 CRC-32 of bytes [0..12], LE
 *  16  n  payload      payload_len bytes
 *  16+n 4 payload_crc32 CRC-32 of payload bytes, LE
 */
import { crc32 } from "node:zlib";

export const WIRE_VERSION = 1;
export const MAGIC = Buffer.from("WEIR", "ascii"); // 57 45 49 52
export const HEADER_LEN = 16;
export const MAX_PAYLOAD_HARD_CAP = 16 * 1024 * 1024; // 16 MiB

/**
 * NOTE: these are `const` objects, not `enum`s, on purpose. Node's native
 * TypeScript support (>= 22) is *type-stripping only* — it removes annotations
 * but cannot emit the runtime code a TS `enum` needs, so `enum` throws
 * ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX under `node file.ts`. `as const` objects are
 * erasable syntax and run flag-free. (See README "Friction log".)
 */
export const MessageType = {
  Push: 0x01,
  Ack: 0x02,
  Nack: 0x03,
  HealthCheck: 0x04,
  HealthCheckResponse: 0x05,
  // Additive within wire v1: message-type bytes 0x08-0xFF are reserved for
  // exactly this, so WIRE_VERSION stays 1 and the 30 frozen vectors are
  // untouched. A daemon predating these answers Nack(UnknownMessage).
  PushTracked: 0x06,
  AckTracked: 0x07,
} as const;
export type MessageType = (typeof MessageType)[keyof typeof MessageType];

export const Durability = {
  Durable: 0x01,
  // Retired: still a valid wire byte that permissively decodes to Durable
  // (see durabilityName's override below), but a conformant encoder never
  // emits it. See docs/wire_protocol.md "Durability tiers".
  Batched: 0x02,
  Buffered: 0x03,
} as const;
export type Durability = (typeof Durability)[keyof typeof Durability];

export const NackReason = {
  BadMagic: 0x01,
  VersionMismatch: 0x02,
  BadHeaderCrc: 0x03,
  PayloadTooLarge: 0x04,
  BadPayloadCrc: 0x05,
  InternalError: 0x06,
  EmptyPayload: 0x07,
  UnknownMessage: 0x08,
  ReservedFlagsSet: 0x09,
} as const;
export type NackReason = (typeof NackReason)[keyof typeof NackReason];

const MESSAGE_TYPE_NAMES = new Map<number, string>(
  Object.entries(MessageType).map(([k, v]) => [v, k]),
);
const DURABILITY_NAMES = new Map<number, string>(
  Object.entries(Durability).map(([k, v]) => [v, k]),
);
// 0x02 (retired Batched) permissively decodes to the same canonical label as
// 0x01 — see docs/wire_protocol.md "Durability tiers" and
// crates/weir-core/src/durability.rs's permissive TryFrom.
DURABILITY_NAMES.set(Durability.Batched, "Durable");
const NACK_REASON_NAMES = new Map<number, string>(
  Object.entries(NackReason).map(([k, v]) => [v, k]),
);

export function messageTypeName(byte: number): string | undefined {
  return MESSAGE_TYPE_NAMES.get(byte);
}
export function durabilityName(byte: number): string | undefined {
  return DURABILITY_NAMES.get(byte);
}

export function nackReasonName(byte: number): string {
  const known = NACK_REASON_NAMES.get(byte);
  if (known !== undefined) return known;
  if (byte >= 0x0a) return `Reserved(0x${byte.toString(16).padStart(2, "0")})`;
  return `Unknown(0x${byte.toString(16).padStart(2, "0")})`;
}

/** CRC-32 / ISO-3309 (zlib). Returns an unsigned 32-bit int. */
export function crc(buf: Buffer): number {
  return crc32(buf) >>> 0;
}

export interface EncodeOpts {
  // Widened to `number` so the encoder can re-emit any decoded header byte
  // (e.g. round-tripping a conformance vector) — the wire is just a byte here.
  // Callers building fresh frames should pass MessageType / Durability values.
  messageType?: MessageType | number;
  durability?: Durability | number;
  flags?: number;
}

/**
 * Encode a frame. Defaults to a Push at Durable durability.
 *
 * Does NOT enforce a non-empty payload — that is a daemon policy (EmptyPayload
 * Nack), and tests need to be able to encode a zero-length Push to exercise it.
 */
export function encodeFrame(payload: Buffer, opts: EncodeOpts = {}): Buffer {
  const messageType = opts.messageType ?? MessageType.Push;
  const durability = opts.durability ?? Durability.Durable;
  const flags = opts.flags ?? 0;

  const frame = Buffer.allocUnsafe(HEADER_LEN + payload.length + 4);
  MAGIC.copy(frame, 0);
  frame.writeUInt8(WIRE_VERSION, 4);
  frame.writeUInt8(messageType, 5);
  frame.writeUInt8(durability, 6);
  frame.writeUInt8(flags, 7);
  frame.writeUInt32LE(payload.length, 8);
  const headerCrc = crc(frame.subarray(0, 12));
  frame.writeUInt32LE(headerCrc, 12);
  payload.copy(frame, HEADER_LEN);
  const payloadCrc = crc(payload);
  frame.writeUInt32LE(payloadCrc, HEADER_LEN + payload.length);
  return frame;
}

export interface DecodedFrame {
  version: number;
  messageType: number;
  durability: number;
  flags: number;
  payloadLen: number;
  payload: Buffer;
}

/**
 * Decode-error tags. These mirror the reference codec's verdicts. A streaming
 * reader never produces TruncatedFrame / TrailingBytes (it frames byte-exactly),
 * but the offline decoder needs them to match the conformance vectors.
 */
export type DecodeErrorTag =
  | "BadMagic"
  | "VersionMismatch"
  | "UnknownMessageType"
  | "UnknownDurability"
  | "HeaderCrcMismatch"
  | "ReservedFlagsSet"
  | "PayloadTooLarge"
  | "TruncatedFrame"
  | "PayloadCrcMismatch"
  | "TrailingBytes";

/**
 * Coordinate rejection tags, a separate union from DecodeErrorTag because a
 * coordinate is a payload shape rather than a frame: the same bytes are a
 * perfectly valid frame whose payload happens not to be a coordinate.
 */
export type CoordinateErrorTag =
  | "Truncated"
  | "UnsupportedVersion"
  | "SegmentTooLong"
  | "LengthMismatch"
  | "SegmentNotUtf8";

export class CoordinateError extends Error {
  tag: CoordinateErrorTag;
  constructor(tag: CoordinateErrorTag, detail?: string) {
    super(detail !== undefined ? `${tag}: ${detail}` : tag);
    this.name = "CoordinateError";
    this.tag = tag;
  }
}

export class DecodeError extends Error {
  // Explicit fields, not constructor parameter properties: parameter properties
  // are non-erasable TS syntax and throw under Node strip-only mode.
  tag: DecodeErrorTag;
  detail?: number;
  constructor(tag: DecodeErrorTag, detail?: number) {
    super(detail !== undefined ? `${tag}(${detail})` : tag);
    this.name = "DecodeError";
    this.tag = tag;
    this.detail = detail;
  }
}

const VALID_MESSAGE_TYPES = new Set<number>([
  MessageType.Push,
  MessageType.Ack,
  MessageType.Nack,
  MessageType.HealthCheck,
  MessageType.HealthCheckResponse,
  MessageType.PushTracked,
  MessageType.AckTracked,
]);

const VALID_DURABILITY = new Set<number>([
  Durability.Durable,
  Durability.Batched,
  Durability.Buffered,
]);

/**
 * Reference decoder: input buffer MUST be exactly one frame.
 *
 * Follows the mandatory server-side decode order from the spec:
 *   magic -> version -> header CRC -> field parse -> payload-len cap ->
 *   payload read -> payload CRC. Then the exactly-one-frame contract
 *   (TruncatedFrame / TrailingBytes).
 */
export function decodeFrame(
  buf: Buffer,
  maxPayload = MAX_PAYLOAD_HARD_CAP,
): DecodedFrame {
  // A buffer shorter than the 16-byte header is TruncatedFrame regardless of its
  // leading bytes — magic is only interpreted once a complete header is present
  // (length-before-magic, matching weir-core's Envelope::decode). So a 1–15 byte
  // buffer never returns BadMagic even if its bytes differ from "WEIR".
  if (buf.length < HEADER_LEN) throw new DecodeError("TruncatedFrame");
  if (!buf.subarray(0, 4).equals(MAGIC)) {
    throw new DecodeError("BadMagic");
  }

  // 2. version (before header CRC, so v2 -> VersionMismatch not HeaderCrcMismatch)
  const version = buf.readUInt8(4);
  if (version !== WIRE_VERSION) throw new DecodeError("VersionMismatch", version);

  // 3. header CRC
  const headerCrc = buf.readUInt32LE(12);
  if (crc(buf.subarray(0, 12)) !== headerCrc) {
    throw new DecodeError("HeaderCrcMismatch");
  }

  // 4. field parse
  const messageType = buf.readUInt8(5);
  const durability = buf.readUInt8(6);
  const flags = buf.readUInt8(7);
  if (!VALID_MESSAGE_TYPES.has(messageType)) {
    throw new DecodeError("UnknownMessageType", messageType);
  }
  if (!VALID_DURABILITY.has(durability)) {
    throw new DecodeError("UnknownDurability", durability);
  }
  if (flags !== 0) throw new DecodeError("ReservedFlagsSet");

  // 5. payload-len cap (before allocation)
  const payloadLen = buf.readUInt32LE(8);
  if (payloadLen > maxPayload) throw new DecodeError("PayloadTooLarge");

  // exactly-one-frame contract
  const totalLen = HEADER_LEN + payloadLen + 4;
  if (buf.length < totalLen) throw new DecodeError("TruncatedFrame");
  if (buf.length > totalLen) {
    throw new DecodeError("TrailingBytes", buf.length - totalLen);
  }

  // 6 + 7. payload read + CRC
  const payload = buf.subarray(HEADER_LEN, HEADER_LEN + payloadLen);
  const payloadCrc = buf.readUInt32LE(HEADER_LEN + payloadLen);
  if (crc(payload) !== payloadCrc) throw new DecodeError("PayloadCrcMismatch");

  return { version, messageType, durability, flags, payloadLen, payload };
}

// ---- RecordCoordinate (the AckTracked payload) ----

/**
 * Layout (docs/wire_protocol.md, "AckTracked payload"):
 *   0   1   coordinate_version (0x01)
 *   1   8   index        u64 LE, 1-based within the segment
 *   9  32   record_id    SHA-256
 *  41   2   segment_len  u16 LE
 *  43 var   segment      UTF-8, <= 255 bytes
 */
export const COORDINATE_VERSION = 1;
export const COORDINATE_FIXED_LEN = 1 + 8 + 32 + 2;
export const MAX_SEGMENT_NAME_LEN = 255;
export const MAX_TRACKED_ACK_PAYLOAD = COORDINATE_FIXED_LEN + MAX_SEGMENT_NAME_LEN; // 298

/**
 * Where a tracked record landed in the buffer.
 *
 * An address, not a sequence: other producers interleave in the same segment,
 * so one producer's indices have holes by construction. `segment` is an opaque,
 * stable address rather than a filesystem path, and `recordId` is the same
 * digest the daemon hands a sink as its per-record idempotency key.
 *
 * `index` is a bigint, not a number. It is a u64, and Number.MAX_SAFE_INTEGER
 * is 2^53-1 -- a plain number silently rounds the top of the range.
 */
export interface RecordCoordinate {
  segment: string;
  index: bigint;
  recordId: Buffer;
}

export function recordIdHex(c: RecordCoordinate): string {
  return c.recordId.toString("hex");
}

/**
 * Cap for a response that carries no coordinate. Every response a client that
 * never sends PushTracked (0x06) can receive is at most two bytes: Ack carries
 * none, Nack one or two, HealthCheckResponse one.
 */
export const MAX_RESPONSE_PAYLOAD = 2;

/**
 * Cap for a response of this type, applied before any allocation.
 *
 * AckTracked is the only weir response whose payload exceeds two bytes, so the
 * bound is widened for exactly that type. Widening it for anything else would
 * let a desynced peer use a stray type byte to unlock a bigger read.
 */
export function maxResponsePayload(messageType: number): number {
  return messageType === MessageType.AckTracked ? MAX_TRACKED_ACK_PAYLOAD : MAX_RESPONSE_PAYLOAD;
}

/**
 * Decode exactly one RecordCoordinate.
 *
 * The version byte leads so the layout can grow inside wire v1, which only
 * works if a reader meeting an unknown version REJECTS rather than parsing a
 * prefix it has never seen. The buffer must be exactly one coordinate for the
 * same reason: a trailing byte means the two ends disagree about the layout,
 * and quietly using the prefix is how that becomes a wrong address.
 */
export function decodeCoordinate(buf: Buffer): RecordCoordinate {
  if (buf.length < COORDINATE_FIXED_LEN) {
    throw new CoordinateError("Truncated", `${buf.length} < ${COORDINATE_FIXED_LEN}`);
  }
  const version = buf.readUInt8(0);
  if (version !== COORDINATE_VERSION) {
    throw new CoordinateError("UnsupportedVersion", `version ${version}`);
  }

  const index = buf.readBigUInt64LE(1);
  const recordId = Buffer.from(buf.subarray(9, 41));
  const segmentLen = buf.readUInt16LE(41);

  if (segmentLen > MAX_SEGMENT_NAME_LEN) {
    throw new CoordinateError("SegmentTooLong", `${segmentLen} > ${MAX_SEGMENT_NAME_LEN}`);
  }
  const want = COORDINATE_FIXED_LEN + segmentLen;
  if (buf.length < want) {
    throw new CoordinateError("Truncated", `${buf.length} < ${want}`);
  }
  if (buf.length !== want) {
    throw new CoordinateError("LengthMismatch", `${buf.length - want} trailing byte(s)`);
  }

  const raw = buf.subarray(COORDINATE_FIXED_LEN, want);
  // Buffer.toString("utf8") SUBSTITUTES U+FFFD for invalid sequences rather
  // than throwing, so it cannot be used to detect a non-UTF-8 segment. A fatal
  // TextDecoder is what actually rejects.
  let segment: string;
  try {
    segment = new TextDecoder("utf-8", { fatal: true }).decode(raw);
  } catch (e) {
    throw new CoordinateError("SegmentNotUtf8", (e as Error).message);
  }

  return { segment, index, recordId };
}
