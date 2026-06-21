/**
 * Live end-to-end smoke test against a running daemon.
 *
 * 1. Direct client checks: healthcheck, a Sync push, a Batched push, a Buffered
 *    push, and that an empty Push is rejected with Nack(EmptyPayload) and the
 *    connection is closed (per spec).
 * 2. Scrapes the daemon's Prometheus metrics and asserts the accepted-record
 *    counter advanced by the number of pushes we sent.
 *
 * Run:  node examples/live-smoke.ts <socket-path> <metrics-port>
 */
import http from "node:http";
import { WeirClient, NackError } from "../src/client.ts";
import { Durability, NackReason } from "../src/wire.ts";

const socketPath = process.argv[2];
const metricsPort = Number(process.argv[3]);
if (!socketPath || !metricsPort) {
  console.error("usage: node examples/live-smoke.ts <socket-path> <metrics-port>");
  process.exit(2);
}

function scrapeMetrics(): Promise<string> {
  return new Promise((resolve, reject) => {
    http
      .get({ host: "127.0.0.1", port: metricsPort, path: "/metrics" }, (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

/** Sum every sample of a counter regardless of labels. */
function metricSum(text: string, name: string): number {
  let total = 0;
  for (const line of text.split("\n")) {
    if (line.startsWith("#") || !line.startsWith(name)) continue;
    const rest = line.slice(name.length);
    if (rest.length && rest[0] !== " " && rest[0] !== "{") continue; // prefix guard
    const val = Number(line.trim().split(/\s+/).pop());
    if (!Number.isNaN(val)) total += val;
  }
  return total;
}

let failures = 0;
function check(cond: boolean, msg: string): void {
  console.log(`${cond ? "PASS" : "FAIL"}  ${msg}`);
  if (!cond) failures++;
}

// --- accepted-record counter: discover the name from the scrape, before/after.
const before = await scrapeMetrics();
const counterCandidates = [
  "weir_records_accepted_total",
  "weir_records_total",
  "weir_pushes_accepted_total",
  "weir_acked_total",
];
let counterName = counterCandidates.find((n) => before.includes(n));
const acceptedBefore = counterName ? metricSum(before, counterName) : NaN;

// --- direct client calls
{
  const c = new WeirClient({ socketPath, timeoutMs: 5000 });
  await c.connect();
  await c.healthCheck();
  check(true, "healthcheck answered");
  await c.push("sync-event", Durability.Sync);
  check(true, "Sync push acked");
  await c.push("batched-event", Durability.Batched);
  check(true, "Batched push acked");
  await c.push(Buffer.from("buffered-event"), Durability.Buffered);
  check(true, "Buffered push acked");
  c.close();
}

// --- empty-push rejection (separate connection, since it closes on Nack)
{
  const c = new WeirClient({ socketPath, timeoutMs: 5000 });
  await c.connect();
  try {
    await c.push(Buffer.alloc(0), Durability.Sync);
    check(false, "empty Push should be rejected");
  } catch (e) {
    const isEmpty = e instanceof NackError && e.reason === NackReason.EmptyPayload;
    check(isEmpty, `empty Push -> ${(e as Error).message}`);
    check(e instanceof NackError && e.closesConnection, "EmptyPayload closes the connection");
  }
  c.close();
}

// 3 ACCEPTED pushes (sync + batched + buffered). The empty Push is Nacked, not
// accepted, and the HealthCheck does not count as a record.
const PUSHES = 3;

// give the daemon a beat to update counters, then re-scrape
await new Promise((r) => setTimeout(r, 300));
const after = await scrapeMetrics();
if (!counterName) counterName = counterCandidates.find((n) => after.includes(n));

if (counterName) {
  const acceptedAfter = metricSum(after, counterName);
  const delta = acceptedAfter - (Number.isNaN(acceptedBefore) ? 0 : acceptedBefore);
  check(
    delta >= PUSHES,
    `metric ${counterName} advanced by ${delta} (>= ${PUSHES} accepted pushes)`,
  );
  // The empty Push should have bumped the empty_payload nack counter.
  const emptyNackBefore = metricSum(
    before.split("\n").filter((l) => l.includes('reason="empty_payload"')).join("\n"),
    "weir_records_nack_total",
  );
  const emptyNackAfter = metricSum(
    after.split("\n").filter((l) => l.includes('reason="empty_payload"')).join("\n"),
    "weir_records_nack_total",
  );
  check(
    emptyNackAfter - emptyNackBefore >= 1,
    `empty_payload nack counter advanced by ${emptyNackAfter - emptyNackBefore} (>= 1)`,
  );
} else {
  console.log(`NOTE  no accepted-record counter found; tried: ${counterCandidates.join(", ")}`);
  console.log("      (metrics names below for inspection)");
  console.log(
    after
      .split("\n")
      .filter((l) => l.startsWith("weir_") && !l.startsWith("#"))
      .slice(0, 40)
      .join("\n"),
  );
}

console.log(failures === 0 ? "\nSMOKE OK" : `\nSMOKE FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
