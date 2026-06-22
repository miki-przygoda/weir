/**
 * Demo: a web backend that durably logs every HTTP request to weir.
 *
 * An async Node `http` handler builds a structured JSON log event per request
 * and Pushes it to the weir daemon over the v1 wire (Buffered durability for
 * low handler latency). weir is the write-ahead buffer; a real deployment would
 * point the daemon's sink at ClickHouse / Postgres for the access log.
 *
 * Stdlib only: node:http + the wire client (node:net + node:zlib).
 *
 * Run (daemon must be listening on the socket):
 *   node examples/request-logger.ts <socket-path> [http-port]
 */
import http from "node:http";
import { WeirClient, NackError } from "../src/client.ts";
import { Durability } from "../src/wire.ts";

const socketPath = process.argv[2];
const httpPort = Number(process.argv[3] ?? 8787);
if (!socketPath) {
  console.error("usage: node examples/request-logger.ts <socket-path> [http-port]");
  process.exit(2);
}

const weir = new WeirClient({ socketPath, timeoutMs: 5000 });
await weir.connect();
console.log(`[logger] connected to weir at ${socketPath}`);

let seq = 0;
let logged = 0;
let dropped = 0;

async function logRequest(req: http.IncomingMessage, status: number, latMs: number): Promise<void> {
  const event = {
    seq: ++seq,
    ts: new Date().toISOString(),
    method: req.method,
    path: req.url,
    status,
    latency_ms: latMs,
    ua: req.headers["user-agent"] ?? null,
    remote: req.socket.remoteAddress ?? null,
  };
  try {
    // Buffered: ack after the memory write — keeps the request hot-path fast.
    await weir.push(JSON.stringify(event), Durability.Buffered);
    logged++;
  } catch (e) {
    dropped++;
    if (e instanceof NackError && e.isTransient) {
      // Transient (InternalError): connection stays open; a real app would retry.
      console.warn(`[logger] transient nack for seq=${event.seq}, would retry`);
    } else {
      console.error(`[logger] log failed for seq=${event.seq}: ${(e as Error).message}`);
    }
  }
}

const server = http.createServer((req, res) => {
  const t0 = performance.now();
  // Trivial routing so there's something to log.
  let status = 200;
  let body = JSON.stringify({ ok: true, path: req.url });
  if (req.url === "/health") {
    body = JSON.stringify({ status: "up", logged, dropped });
  } else if (req.url === "/teapot") {
    status = 418;
    body = JSON.stringify({ error: "I'm a teapot" });
  } else if (req.url?.startsWith("/notfound")) {
    status = 404;
    body = JSON.stringify({ error: "not found" });
  }
  res.writeHead(status, { "content-type": "application/json" });
  res.end(body);
  // Fire-and-forget the durable log after responding (don't block the client).
  const latMs = Math.round((performance.now() - t0) * 1000) / 1000;
  void logRequest(req, status, latMs);
});

server.listen(httpPort, "127.0.0.1", () => {
  console.log(`[logger] http listening on http://127.0.0.1:${httpPort}`);
});

function shutdown(): void {
  console.log(`[logger] shutting down (logged=${logged} dropped=${dropped})`);
  server.close();
  weir.close();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
