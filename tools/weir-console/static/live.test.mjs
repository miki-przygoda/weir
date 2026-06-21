import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// live.js is a plain <script> (no exports) that runs document.querySelectorAll(...) and
// main() at load. Strip the auto-main, wrap so its declarations are returnable, and
// inject document/fetch/setInterval/requestAnimationFrame stubs. The document stub
// memoises one element per selector and captures the last innerHTML/textContent.
function loadLive(documentStub, fetchStub) {
  let src = readFileSync(new URL("./live.js", import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, "");
  const factory = new Function(
    "document",
    "fetch",
    "setInterval",
    "requestAnimationFrame",
    src +
      "\nreturn { computeRates, makeRing, fmtRate, fmtBytes, sparkline, countersHtml, renderCounters, poll, frame };"
  );
  return factory(documentStub, fetchStub, () => 0, () => 0);
}

function makeDom() {
  const els = {};
  const makeEl = () => ({ innerHTML: "", textContent: "", querySelector: () => null, querySelectorAll: () => [] });
  return {
    els,
    document: {
      querySelector: (sel) => (els[sel] ??= makeEl()),
      querySelectorAll: () => [],
    },
  };
}

function makeFetch(body) {
  return () => Promise.resolve({ ok: true, statusText: "OK", json: () => Promise.resolve(body) });
}

const SUMMARY = (accepted, ack, nack, queue) => ({
  daemon: "up",
  metrics_addr: "127.0.0.1:9185",
  summary: {
    accepted, ack, nack, queue_depth: queue, fsync_avg_ms: 0.4,
    wab_bytes_on_disk: 4096, dead_letter_bytes_on_disk: 2048,
    sink_type: "http", sink_health: "healthy", flusher_panics: 0, fsync_failures: 0,
  },
});

test("computeRates: deltas, first-sample zeros, and reset guard", () => {
  const app = loadLive(makeDom().document, makeFetch({}));
  // first sample (no prev) -> zeros
  assert.deepEqual(app.computeRates(null, { accepted: 100, ack: 100, nack: 0 }, 1.5), { accepted: 0, ack: 0, nack: 0 });
  // 150 more accepted over 1.5s -> 100/s
  const r = app.computeRates({ accepted: 100, ack: 100, nack: 0 }, { accepted: 250, ack: 250, nack: 0 }, 1.5);
  assert.equal(r.accepted, 100);
  assert.equal(r.ack, 100);
  // counter reset (daemon restarted): cur < prev -> 0, never negative
  const reset = app.computeRates({ accepted: 1000 }, { accepted: 5 }, 1.5);
  assert.equal(reset.accepted, 0);
});

test("makeRing caps and drops the oldest", () => {
  const app = loadLive(makeDom().document, makeFetch({}));
  const ring = app.makeRing(3);
  ring.push(1); ring.push(2); ring.push(3); ring.push(4);
  assert.deepEqual(ring.values(), [2, 3, 4]);
});

test("formatters", () => {
  const app = loadLive(makeDom().document, makeFetch({}));
  assert.equal(app.fmtRate(100), "100/s");
  assert.equal(app.fmtRate(1500), "1.5k/s");
  assert.equal(app.fmtBytes(2048), "2.0 KiB");
});

test("poll renders counters after two up-samples (real rate appears)", async () => {
  const dom = makeDom();
  let body = SUMMARY(100, 100, 0, 7);
  const fetchStub = () => Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
  const app = loadLive(dom.document, fetchStub);
  await app.poll(); // baseline
  body = SUMMARY(250, 250, 0, 7); // +150 over 1.5s -> 100/s
  await app.poll();
  const html = dom.els["#live-counters"].innerHTML;
  assert.match(html, /acc 100\/s/);
  assert.match(html, /queue 7/);
  assert.match(html, /healthy/);
});

test("daemon down -> waiting state", async () => {
  const dom = makeDom();
  const app = loadLive(dom.document, makeFetch({ daemon: "down", metrics_addr: "127.0.0.1:9185" }));
  await app.poll();
  assert.match(dom.els["#live-counters"].innerHTML, /waiting for daemon at 127\.0\.0\.1:9185/);
});

test("frame() runs without throwing using a stubbed 2d context", () => {
  const dom = makeDom();
  const noop = () => {};
  const ctx2d = { clearRect: noop, fillText: noop, beginPath: noop, moveTo: noop, lineTo: noop, stroke: noop, arc: noop, fill: noop, fillStyle: "", strokeStyle: "", font: "" };
  dom.els["#cv"] = { width: 1040, height: 180, getContext: () => ctx2d };
  const app = loadLive(dom.document, makeFetch({}));
  assert.doesNotThrow(() => app.frame(16));
});
