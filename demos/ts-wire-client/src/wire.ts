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
} as const;
export type MessageType = (typeof MessageType)[keyof typeof MessageType];

export const Durability = {
  Sync: 0x01,
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
 * Encode a frame. Defaults to a Push at Sync durability.
 *
 * Does NOT enforce a non-empty payload — that is a daemon policy (EmptyPayload
 * Nack), and tests need to be able to encode a zero-length Push to exercise it.
 */
export function encodeFrame(payload: Buffer, opts: EncodeOpts = {}): Buffer {
  const messageType = opts.messageType ?? MessageType.Push;
  const durability = opts.durability ?? Durability.Sync;
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
]);

const VALID_DURABILITY = new Set<number>([
  Durability.Sync,
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
  // A buffer that starts with valid magic but is shorter than the 16-byte
  // header is TruncatedFrame, not BadMagic — a full header is required before
  // any field is interpreted.
  const magicLen = Math.min(4, buf.length);
  if (!buf.subarray(0, magicLen).equals(MAGIC.subarray(0, magicLen))) {
    throw new DecodeError("BadMagic");
  }
  if (buf.length < HEADER_LEN) throw new DecodeError("TruncatedFrame");

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
