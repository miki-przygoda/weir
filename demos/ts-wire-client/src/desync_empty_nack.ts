/**
 * A Nack frame with an empty payload is a stream desync, not a transient Nack.
 *
 * The spec gives a Nack one reason byte. This client used to read a missing
 * byte as InternalError, the one reason that keeps the connection open, so a
 * caller kept writing to a stream whose framing it could no longer trust. The
 * Rust reference client reports it as a protocol error and poisons
 * (crates/weir-client/src/unix.rs, empty_nack_payload_is_protocol_desync_and_poisons).
 *
 * Run: node src/desync_empty_nack.ts   (from demos/ts-wire-client)
 */
import fs from "node:fs";
import net from "node:net";
import { NackError, WeirClient, WireError } from "./client.ts";
import { HEADER_LEN, MAGIC, MessageType, WIRE_VERSION, crc } from "./wire.ts";

/** A well-formed Nack frame (valid header and payload CRCs) with payload_len 0. */
function emptyNack(): Buffer {
  const h = Buffer.alloc(HEADER_LEN);
  MAGIC.copy(h, 0);
  h.writeUInt8(WIRE_VERSION, 4);
  h.writeUInt8(MessageType.Nack, 5);
  h.writeUInt8(0x01, 6); // Durable
  h.writeUInt8(0, 7);
  h.writeUInt32LE(0, 8);
  h.writeUInt32LE(crc(h.subarray(0, 12)), 12);
  const payloadCrc = Buffer.alloc(4);
  payloadCrc.writeUInt32LE(crc(Buffer.alloc(0)), 0);
  return Buffer.concat([h, payloadCrc]);
}

const sockPath = `/tmp/wtsnack${process.pid}.sock`;
try {
  fs.unlinkSync(sockPath);
} catch {}

const server = net.createServer((conn) => {
  // Answer the first request with the empty Nack, then hold the connection
  // open: a client that keeps it must not be rescued by an EOF.
  conn.once("data", () => conn.write(emptyNack()));
  conn.on("error", () => {});
});
await new Promise<void>((r) => server.listen(sockPath, r));

const failures: string[] = [];
const client = new WeirClient({ socketPath: sockPath, timeoutMs: 2000 });
await client.connect();

let first: unknown;
try {
  await client.push("x");
  first = "accepted";
} catch (e) {
  first = e;
}
if (first instanceof NackError) {
  failures.push(`empty Nack surfaced as NackError(${first.message}); want a desync WireError`);
} else if (!(first instanceof WireError)) {
  failures.push(`empty Nack: want a WireError, got ${String(first)}`);
} else if (!first.closesConnection) {
  failures.push("empty Nack: the error must close the connection (closesConnection = false)");
}

// The client must have dropped the connection: a second push fails at once
// instead of being written to the desynced stream.
let second: unknown;
const t0 = Date.now();
try {
  await client.push("y");
  second = "accepted";
} catch (e) {
  second = e;
}
if (!(second instanceof WireError) || second instanceof NackError || Date.now() - t0 > 500) {
  failures.push(
    `push after an empty Nack: want an immediate closed-connection WireError, got ` +
      `${second instanceof Error ? second.message : String(second)} after ${Date.now() - t0}ms`,
  );
}

server.close();
try {
  fs.unlinkSync(sockPath);
} catch {}

console.log(`first = ${first instanceof Error ? `${first.name}: ${first.message}` : String(first)}`);
for (const f of failures) console.log(`FAIL: ${f}`);
console.log(failures.length ? `\n${failures.length} FAILED` : "empty-Nack desync: 2/2 checks passed");
process.exit(failures.length ? 1 : 0);
