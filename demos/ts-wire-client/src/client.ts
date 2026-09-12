/**
 * weir producer client — Unix-socket, async, stdlib only (`node:net`).
 *
 * Serial request/response over one connection. Each push/health call resolves
 * when the matching response frame has been fully read and validated. The
 * framing reader reads the 16-byte header, takes payload_len, then reads exactly
 * payload_len + 4 more bytes (per the spec — never hands a multi-frame buffer to
 * a single decode).
 */
import net from "node:net";
import {
  Durability,
  HEADER_LEN,
  MAGIC,
  MessageType,
  NackReason,
  WIRE_VERSION,
  crc,
  encodeFrame,
  nackReasonName,
  maxResponsePayload,
  decodeCoordinate,
  MAX_TRACKED_ACK_PAYLOAD,
  type RecordCoordinate,
} from "./wire.ts";

// The cap now moves with the message type (see maxResponsePayload in wire.ts):
// this client sends PushTracked, so AckTracked's up-to-298-byte coordinate is
// in scope for exactly that one frame type and for nothing else.

export class WireError extends Error {
  // Explicit field (parameter properties don't survive Node strip-only mode).
  closesConnection: boolean;
  constructor(message: string, closesConnection: boolean) {
    super(message);
    this.name = "WireError";
    this.closesConnection = closesConnection;
  }
}

export class NackError extends WireError {
  reason: number;
  daemonVersion: number | undefined;
  constructor(reason: number, daemonVersion: number | undefined, closesConnection: boolean) {
    super(
      `Nack(${nackReasonName(reason)})` +
        (daemonVersion !== undefined ? ` daemon wire v${daemonVersion}` : ""),
      closesConnection,
    );
    this.name = "NackError";
    this.reason = reason;
    this.daemonVersion = daemonVersion;
  }

  /** Transient per the spec: connection kept open, record outcome unknown, retry. */
  get isTransient(): boolean {
    return this.reason === NackReason.InternalError;
  }

  /**
   * True when this Nack is a daemon that predates PushTracked (0x06).
   *
   * The wire cannot distinguish that from any other unknown message type, so
   * only the caller knows which question it asked -- this is meaningful solely
   * on the reply to a pushTracked(). Do not retry on this connection: the
   * daemon closes it. Open a new one and use push().
   */
  get meansNoTrackedSupport(): boolean {
    return this.reason === NackReason.UnknownMessage;
  }
}

export interface PushResult {
  acked: true;
}

/**
 * A queued request, discriminated by what it asked for.
 *
 * This used to be one shape whose resolve took a PushResult, and dispatch
 * resolved an Ack and a HealthCheckResponse identically with `{acked:true}`.
 * There was nowhere to put a coordinate, and no way to express "this pending
 * wanted one and got a bare Ack".
 *
 * The alternative -- widening PushResult to carry an optional coordinate --
 * was rejected: it makes a tracked caller null-check something that is never
 * absent on success, which is exactly the shape that invites ignoring a missing
 * coordinate. A distinct message type exists because the REQUEST determines the
 * response shape; the queue should encode the same invariant.
 */
type Pending =
  | { kind: "push"; resolve: (r: PushResult) => void; reject: (e: Error) => void }
  | { kind: "tracked"; resolve: (c: RecordCoordinate) => void; reject: (e: Error) => void };

export interface ClientOpts {
  socketPath: string;
  /** Per-request response timeout. */
  timeoutMs?: number;
}

export class WeirClient {
  private sock: net.Socket | null = null;
  private buf: Buffer = Buffer.alloc(0);
  private readonly queue: Pending[] = [];
  private connClosed = false;
  private closeErr: Error | null = null;
  private readonly opts: ClientOpts;

  constructor(opts: ClientOpts) {
    this.opts = opts;
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection(this.opts.socketPath);
      this.sock = sock;
      sock.once("connect", () => {
        sock.removeListener("error", reject);
        resolve();
      });
      sock.once("error", reject);
      sock.on("data", (chunk) => this.onData(chunk));
      sock.on("close", () => this.onClose());
    });
  }

  private onClose(): void {
    this.connClosed = true;
    const err = this.closeErr ?? new WireError("connection closed by daemon", true);
    // In-flight pushes had unknown outcomes (spec: retry on a fresh connection).
    while (this.queue.length) this.queue.shift()!.reject(err);
  }

  private fail(err: Error): void {
    this.closeErr = err;
    this.sock?.destroy();
  }

  private onData(chunk: Buffer): void {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    // Frame as many complete responses as the buffer holds.
    for (;;) {
      if (this.buf.length < HEADER_LEN) return;

      // Validate the response header before consuming the payload.
      if (!this.buf.subarray(0, 4).equals(MAGIC)) {
        return this.fail(new WireError("response: bad magic (desync)", true));
      }
      const version = this.buf.readUInt8(4);
      if (version !== WIRE_VERSION) {
        return this.fail(new WireError(`response: wire v${version} != v${WIRE_VERSION}`, true));
      }
      if (crc(this.buf.subarray(0, 12)) !== this.buf.readUInt32LE(12)) {
        return this.fail(new WireError("response: bad header CRC (desync)", true));
      }
      const payloadLen = this.buf.readUInt32LE(8);
      // Cap before allocating (spec checklist), by the type the header
      // declares. The header CRC has already been verified above, so the type
      // byte is trustworthy at this point -- checking the cap against an
      // unverified byte would let a flipped bit widen the bound.
      const declaredType = this.buf.readUInt8(5);
      const cap = maxResponsePayload(declaredType);
      if (payloadLen > cap) {
        return this.fail(
          new WireError(
            `response: payload_len ${payloadLen} > ${cap} for message_type ` +
              `0x${declaredType.toString(16)} (desync)`,
            true,
          ),
        );
      }
      const total = HEADER_LEN + payloadLen + 4;
      if (this.buf.length < total) return; // need more bytes

      const messageType = this.buf.readUInt8(5);
      const payload = this.buf.subarray(HEADER_LEN, HEADER_LEN + payloadLen);
      const payloadCrc = this.buf.readUInt32LE(HEADER_LEN + payloadLen);
      this.buf = this.buf.subarray(total);

      if (crc(payload) !== payloadCrc) {
        return this.fail(new WireError("response: bad payload CRC (desync)", true));
      }

      this.dispatch(messageType, payload);
      if (this.connClosed) return;
    }
  }

  private dispatch(messageType: number, payload: Buffer): void {
    const pending = this.queue.shift();
    if (!pending) {
      return this.fail(new WireError("unsolicited response from daemon", true));
    }
    switch (messageType) {
      case MessageType.Ack:
      case MessageType.HealthCheckResponse:
        if (pending.kind === "tracked") {
          // A bare Ack in reply to a PushTracked is an error, not a success.
          // The request determines the response shape, so an Ack here means the
          // peer did not understand what was asked -- and resolving it would
          // report a coordinate that does not exist.
          pending.reject(
            new WireError(
              "daemon answered a PushTracked with a bare Ack; the record may be " +
                "durable but its coordinate is unknown",
              true,
            ),
          );
          return;
        }
        pending.resolve({ acked: true });
        return;
      case MessageType.AckTracked: {
        if (pending.kind !== "tracked") {
          // An AckTracked for a request that never asked to be tracked is a
          // desync: the queue and the wire disagree about what is in flight.
          return this.fail(
            new WireError("response: AckTracked for an untracked request (desync)", true),
          );
        }
        try {
          pending.resolve(decodeCoordinate(payload));
        } catch (e) {
          pending.reject(e as Error);
        }
        return;
      }
      case MessageType.Nack: {
        const reason = payload.length > 0 ? payload.readUInt8(0) : NackReason.InternalError;
        const daemonVersion =
          reason === NackReason.VersionMismatch && payload.length > 1
            ? payload.readUInt8(1)
            : undefined;
        // Transient (InternalError) keeps the connection open; everything else closes it.
        const closes = reason !== NackReason.InternalError;
        pending.reject(new NackError(reason, daemonVersion, closes));
        return;
      }
      default:
        return this.fail(
          new WireError(`response: unexpected message_type 0x${messageType.toString(16)}`, true),
        );
    }
  }

  /**
   * Queue a request whose reply is a plain Ack (or HealthCheckResponse).
   *
   * `kind` is what lets dispatch tell an expected Ack from one that answers a
   * question it was not asked.
   */
  private send(frame: Buffer): Promise<PushResult> {
    return this.enqueue<PushResult>(frame, (resolve, reject) => ({
      kind: "push",
      resolve,
      reject,
    }));
  }

  /** Queue a request whose reply carries a coordinate. */
  private sendTracked(frame: Buffer): Promise<RecordCoordinate> {
    return this.enqueue<RecordCoordinate>(frame, (resolve, reject) => ({
      kind: "tracked",
      resolve,
      reject,
    }));
  }

  private enqueue<T>(
    frame: Buffer,
    make: (resolve: (v: T) => void, reject: (e: Error) => void) => Pending,
  ): Promise<T> {
    if (this.connClosed || !this.sock) {
      return Promise.reject(this.closeErr ?? new WireError("not connected", true));
    }
    return new Promise<T>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      if (this.opts.timeoutMs) {
        timer = setTimeout(() => {
          this.fail(new WireError(`request timed out after ${this.opts.timeoutMs}ms`, true));
        }, this.opts.timeoutMs);
      }
      const clear = <A,>(f: (a: A) => void) => (a: A) => {
        if (timer) clearTimeout(timer);
        f(a);
      };
      this.queue.push(make(clear(resolve), clear(reject)));
      this.sock!.write(frame);
    });
  }

  /** Push a non-empty payload. Rejects with NackError on daemon rejection. */
  push(payload: Buffer | string, durability: Durability = Durability.Durable): Promise<PushResult> {
    const body = typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
    return this.send(encodeFrame(body, { messageType: MessageType.Push, durability }));
  }

  /**
   * Push a record and learn where it landed.
   *
   * Identical to push() in every other respect -- same tiers, same caps, same
   * Nack reasons -- but answered with an AckTracked carrying the coordinate.
   * A daemon predating the type answers Nack(UnknownMessage) and closes the
   * connection; `NackError.meansNoTrackedSupport` names that case. Open a new
   * connection and use push().
   */
  pushTracked(
    payload: Buffer | string,
    durability: Durability = Durability.Durable,
  ): Promise<RecordCoordinate> {
    const body = typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
    return this.sendTracked(
      encodeFrame(body, { messageType: MessageType.PushTracked, durability }),
    );
  }

  /** Liveness probe — zero-length HealthCheck frame. */
  healthCheck(): Promise<PushResult> {
    return this.send(
      encodeFrame(Buffer.alloc(0), {
        messageType: MessageType.HealthCheck,
        durability: Durability.Durable,
      }),
    );
  }

  close(): void {
    this.sock?.end();
  }
}
