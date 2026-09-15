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
  PushBatch: 0x08,
  AckBatch: 0x09,
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
  MessageType.PushBatch,
  MessageType.AckBatch,
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
 * AckBatch wire layout (docs/wire_protocol.md, "AckBatch payload"):
 *
 *     0   1   ack_batch_version (0x01)
 *     1   2   record_count  u16 LE, echoing the PushBatch it answers
 *     3 var   bitmap        ceil(N/8) bytes, LSB-first within each byte
 *
 * The 2048 hard cap is chosen so this payload stays under
 * MAX_TRACKED_ACK_PAYLOAD: batching introduces no new largest response, so this
 * client's biggest allocation is unchanged by implementing it.
 */
export const BATCH_VERSION = 1;
export const ACK_BATCH_VERSION = 1;
export const BATCH_HEADER_LEN = 1 + 2;
export const ACK_BATCH_HEADER_LEN = 1 + 2;
export const MAX_BATCH_RECORDS_HARD_CAP = 2048;
export const MAX_ACK_BATCH_PAYLOAD =
  ACK_BATCH_HEADER_LEN + Math.ceil(MAX_BATCH_RECORDS_HARD_CAP / 8); // 259

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
 * Widened for exactly the two response types whose payload can exceed two
 * bytes, and for nothing else: a desynced peer must not be able to use a stray
 * type byte to unlock a bigger read.
 */
export function maxResponsePayload(messageType: number): number {
  if (messageType === MessageType.AckTracked) return MAX_TRACKED_ACK_PAYLOAD;
  if (messageType === MessageType.AckBatch) return MAX_ACK_BATCH_PAYLOAD;
  return MAX_RESPONSE_PAYLOAD;
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

// ── Batch extension (PushBatch 0x08 / AckBatch 0x09) ─────────────────────────
//
// Implemented from docs/wire_protocol.md and checked against
// docs/conformance/wire_v1_batch_vectors.json. Nothing here is ported from the
// Rust reference: an independent implementation is the only thing that catches
// an under-specified format, and this one has a bug class no checksum detects
// — a bitmap written with the opposite bit order is a well-formed frame, valid
// CRCs and correct length, that reports failures as successes.
//
// Bit i lives in byte i >> 3 at mask 1 << (i & 7): LSB-first.

/** Why a PushBatch body or AckBatch payload could not be decoded. */
export type BatchErrorKind =
  | "Truncated"
  | "UnsupportedVersion"
  | "EmptyBatch"
  | "TooManyRecords"
  | "EmptyRecord"
  | "RecordTooLarge"
  | "TruncatedRecord"
  | "LengthMismatch"
  | "PaddingNotZero";

/** `kind` matches the conformance-vector tags, so a failure names its vector. */
export class BatchError extends Error {
  // Explicit fields, not constructor parameter properties: parameter properties
  // are non-erasable TS syntax and throw under Node strip-only mode, which is
  // how CI runs this file.
  kind: BatchErrorKind;
  detail?: string;
  constructor(kind: BatchErrorKind, detail?: string) {
    super(detail ? `${kind}: ${detail}` : kind);
    this.name = "BatchError";
    this.kind = kind;
    this.detail = detail;
  }
}

/** A PushBatch body: version, u16 count, then u32-length-prefixed records. */
export function encodeBatchBody(records: readonly Buffer[]): Buffer {
  let n = BATCH_HEADER_LEN;
  for (const r of records) n += 4 + r.length;
  const out = Buffer.alloc(n);
  out.writeUInt8(BATCH_VERSION, 0);
  out.writeUInt16LE(records.length, 1);
  let p = BATCH_HEADER_LEN;
  for (const r of records) {
    out.writeUInt32LE(r.length, p);
    p += 4;
    r.copy(out, p);
    p += r.length;
  }
  return out;
}

/**
 * Parse a PushBatch body into views of `body`.
 *
 * The check ORDER is part of the contract. The declared count is validated
 * against the cap BEFORE anything is sized by it, and a record's declared
 * length is checked against the cap BEFORE it is added to the cursor. The
 * frame's payload CRC has already passed by this point and proves nothing here:
 * a hostile peer computes a perfectly valid CRC over a body declaring 65,535
 * records in three bytes.
 */
export function decodeBatchBody(
  body: Buffer,
  maxRecords: number = MAX_BATCH_RECORDS_HARD_CAP,
  maxRecordLen: number = MAX_PAYLOAD_HARD_CAP,
): Buffer[] {
  if (body.length < BATCH_HEADER_LEN) {
    throw new BatchError("Truncated", `${body.length} < ${BATCH_HEADER_LEN}`);
  }
  if (body.readUInt8(0) !== BATCH_VERSION) {
    throw new BatchError("UnsupportedVersion", `version ${body.readUInt8(0)}`);
  }
  const declared = body.readUInt16LE(1);
  if (declared === 0) throw new BatchError("EmptyBatch");
  const cap = Math.min(maxRecords, MAX_BATCH_RECORDS_HARD_CAP);
  if (declared > cap) throw new BatchError("TooManyRecords", `${declared} > ${cap}`);

  const recordCap = Math.min(maxRecordLen, MAX_PAYLOAD_HARD_CAP);
  const records: Buffer[] = [];
  let cursor = BATCH_HEADER_LEN;
  while (cursor < body.length) {
    if (cursor + 4 > body.length) throw new BatchError("TruncatedRecord", `record ${records.length}`);
    const n = body.readUInt32LE(cursor);
    cursor += 4;
    if (n === 0) throw new BatchError("EmptyRecord", `record ${records.length}`);
    if (n > recordCap) throw new BatchError("RecordTooLarge", `record ${records.length}: ${n} > ${recordCap}`);
    if (cursor + n > body.length) throw new BatchError("TruncatedRecord", `record ${records.length}`);
    // Stop before overrunning the declared count, so a body carrying more
    // records than it declares is a mismatch rather than a silent drop.
    if (records.length === declared) {
      throw new BatchError("LengthMismatch", `more than the declared ${declared}`);
    }
    records.push(body.subarray(cursor, cursor + n));
    cursor += n;
  }
  if (records.length !== declared || cursor !== body.length) {
    throw new BatchError("LengthMismatch", `declared ${declared}, found ${records.length}`);
  }
  return records;
}

/**
 * An AckBatch payload from per-record outcomes.
 * Padding bits in the final byte are left zero, which the decoder requires.
 */
export function encodeAckBatch(accepted: readonly boolean[]): Buffer {
  const out = Buffer.alloc(ACK_BATCH_HEADER_LEN + Math.ceil(accepted.length / 8));
  out.writeUInt8(ACK_BATCH_VERSION, 0);
  out.writeUInt16LE(accepted.length, 1);
  for (let i = 0; i < accepted.length; i++) {
    if (accepted[i]) out[ACK_BATCH_HEADER_LEN + (i >> 3)] |= 1 << (i & 7);
  }
  return out;
}

/**
 * Parse an AckBatch payload into per-record outcomes.
 *
 * `expected` is the count this client sent. A bitmap is the first weir response
 * whose meaning depends on client-held state — an Ack says "your last record"
 * and an AckTracked carries its own coordinate, but a bitmap is meaningless
 * without knowing which batch it answers. ceil(N/8) is not injective (N of 1017
 * through 1024 all give 131 bytes), so the echoed count is the only thing that
 * can catch a desync.
 *
 * A set bit means the record is durable at the requested tier and inherits
 * weir's crown invariant. A CLEAR bit is the weak statement: not durable as of
 * this reply, retry it, and expect it may nonetheless have been written.
 */
export function decodeAckBatch(payload: Buffer, expected: number): boolean[] {
  if (payload.length < ACK_BATCH_HEADER_LEN) {
    throw new BatchError("Truncated", `${payload.length} < ${ACK_BATCH_HEADER_LEN}`);
  }
  if (payload.readUInt8(0) !== ACK_BATCH_VERSION) {
    throw new BatchError("UnsupportedVersion", `version ${payload.readUInt8(0)}`);
  }
  const declared = payload.readUInt16LE(1);
  if (declared === 0) throw new BatchError("EmptyBatch");
  if (declared !== expected) {
    throw new BatchError("LengthMismatch", `reply answers ${declared}, we sent ${expected}`);
  }
  const want = ACK_BATCH_HEADER_LEN + Math.ceil(declared / 8);
  if (payload.length !== want) {
    throw new BatchError("LengthMismatch", `${payload.length} bytes, need ${want}`);
  }
  // Padding bits must be zero. Ignoring them would make popcount === N — the
  // obvious way to ask "did the whole batch succeed" — silently wrong.
  const used = declared % 8;
  if (used !== 0) {
    const last = payload[payload.length - 1];
    if ((last & ~((1 << used) - 1) & 0xff) !== 0) {
      throw new BatchError("PaddingNotZero", `final byte ${last.toString(16)}`);
    }
  }
  const bits = payload.subarray(ACK_BATCH_HEADER_LEN);
  const out: boolean[] = new Array(declared);
  for (let i = 0; i < declared; i++) {
    out[i] = (bits[i >> 3] & (1 << (i & 7))) !== 0;
  }
  return out;
}
