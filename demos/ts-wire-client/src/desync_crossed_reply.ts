/**
 * A reply of the wrong kind is a stream desync, not a success.
 *
 * Ack answers a Push and HealthCheckResponse answers a HealthCheck. This client
 * used to accept either reply for either request, so a Push answered with a
 * HealthCheckResponse resolved `{acked:true}` for a record nobody acked. The
 * Rust reference client treats both crossings as a protocol error and poisons
 * (crates/weir-client/src/client.rs: "expected Ack or Nack" in push,
 * "expected HealthCheckResponse" in health_check, "expected AckTracked or
 * Nack" in push_tracked).
 *
 * Run: node src/desync_crossed_reply.ts   (from demos/ts-wire-client)
 */
import fs from "node:fs";
import net from "node:net";
import { NackError, WeirClient, WireError } from "./client.ts";
import { HEADER_LEN, MAGIC, MessageType, WIRE_VERSION, crc } from "./wire.ts";

/** A well-formed empty-payload frame of the given type (valid CRCs). */
function emptyFrame(messageType: number): Buffer {
  const h = Buffer.alloc(HEADER_LEN);
  MAGIC.copy(h, 0);
  h.writeUInt8(WIRE_VERSION, 4);
  h.writeUInt8(messageType, 5);
  h.writeUInt8(0x01, 6); // Durable
  h.writeUInt8(0, 7);
  h.writeUInt32LE(0, 8);
  h.writeUInt32LE(crc(h.subarray(0, 12)), 12);
  const payloadCrc = Buffer.alloc(4);
  payloadCrc.writeUInt32LE(crc(Buffer.alloc(0)), 0);
  return Buffer.concat([h, payloadCrc]);
}

const failures: string[] = [];
let checks = 0;

/**
 * Serve one connection that answers its first request with `reply`, then hold
 * it open (a client that keeps it must not be rescued by an EOF). Run `ask`
 * against it and require a desync WireError, then an immediate failure on the
 * next call.
 */
async function crossed(
  label: string,
  reply: number,
  ask: (c: WeirClient) => Promise<unknown>,
): Promise<void> {
  const sockPath = `/tmp/wtsx${process.pid}${reply}.sock`;
  try {
    fs.unlinkSync(sockPath);
  } catch {}
  const server = net.createServer((conn) => {
    conn.once("data", () => conn.write(emptyFrame(reply)));
    conn.on("error", () => {});
  });
  await new Promise<void>((r) => server.listen(sockPath, r));
  const client = new WeirClient({ socketPath: sockPath, timeoutMs: 2000 });
  await client.connect();

  let first: unknown;
  try {
    first = await ask(client);
  } catch (e) {
    first = e;
  }
  checks++;
  if (!(first instanceof WireError) || first instanceof NackError) {
    failures.push(
      `${label}: want a desync WireError, got ` +
        (first instanceof Error ? `${first.name}: ${first.message}` : JSON.stringify(first)),
    );
  } else if (!first.closesConnection) {
    failures.push(`${label}: the error must close the connection (closesConnection = false)`);
  }

  // The client must have dropped the connection: the next call fails at once
  // instead of being written to the desynced stream.
  let second: unknown;
  const t0 = Date.now();
  try {
    second = await client.push("y");
  } catch (e) {
    second = e;
  }
  checks++;
  if (!(second instanceof WireError) || second instanceof NackError || Date.now() - t0 > 500) {
    failures.push(
      `${label}, then push: want an immediate closed-connection WireError, got ` +
        `${second instanceof Error ? second.message : JSON.stringify(second)} after ${Date.now() - t0}ms`,
    );
  }

  client.close();
  server.close();
  try {
    fs.unlinkSync(sockPath);
  } catch {}
}

await crossed("Push answered with HealthCheckResponse", MessageType.HealthCheckResponse, (c) =>
  c.push("x"),
);
await crossed("HealthCheck answered with Ack", MessageType.Ack, (c) => c.healthCheck());
await crossed("PushTracked answered with a bare Ack", MessageType.Ack, (c) => c.pushTracked("x"));
await crossed("PushTracked answered with HealthCheckResponse", MessageType.HealthCheckResponse, (c) =>
  c.pushTracked("x"),
);

for (const f of failures) console.log(`FAIL: ${f}`);
console.log(
  failures.length ? `\n${failures.length} FAILED` : `crossed-reply desync: ${checks}/${checks} checks passed`,
);
process.exit(failures.length ? 1 : 0);
