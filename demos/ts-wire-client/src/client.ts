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
} from "./wire.ts";

/** Every weir response payload is <= 2 bytes; a larger declared len is a desync. */
const MAX_RESPONSE_PAYLOAD = 2;

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
}

export interface PushResult {
  acked: true;
}

interface Pending {
  resolve: (r: PushResult) => void;
  reject: (e: Error) => void;
}

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
      // Cap the response payload before allocating (spec checklist).
      if (payloadLen > MAX_RESPONSE_PAYLOAD) {
        return this.fail(
          new WireError(`response: payload_len ${payloadLen} > ${MAX_RESPONSE_PAYLOAD} (desync)`, true),
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
        pending.resolve({ acked: true });
        return;
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

  private send(frame: Buffer): Promise<PushResult> {
    if (this.connClosed || !this.sock) {
      return Promise.reject(this.closeErr ?? new WireError("not connected", true));
    }
    return new Promise<PushResult>((resolve, reject) => {
      const pending: Pending = { resolve, reject };
      let timer: NodeJS.Timeout | undefined;
      if (this.opts.timeoutMs) {
        timer = setTimeout(() => {
          this.fail(new WireError(`request timed out after ${this.opts.timeoutMs}ms`, true));
        }, this.opts.timeoutMs);
      }
      const wrap: Pending = {
        resolve: (r) => {
          if (timer) clearTimeout(timer);
          resolve(r);
        },
        reject: (e) => {
          if (timer) clearTimeout(timer);
          reject(e);
        },
      };
      this.queue.push(wrap);
      this.sock!.write(frame);
    });
  }

  /** Push a non-empty payload. Rejects with NackError on daemon rejection. */
  push(payload: Buffer | string, durability: Durability = Durability.Sync): Promise<PushResult> {
    const body = typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
    return this.send(encodeFrame(body, { messageType: MessageType.Push, durability }));
  }

  /** Liveness probe — zero-length HealthCheck frame. */
  healthCheck(): Promise<PushResult> {
    return this.send(
      encodeFrame(Buffer.alloc(0), {
        messageType: MessageType.HealthCheck,
        durability: Durability.Sync,
      }),
    );
  }

  close(): void {
    this.sock?.end();
  }
}
