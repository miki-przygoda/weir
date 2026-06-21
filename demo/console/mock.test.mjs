import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// Run mock.js against a fake globalThis and capture the fetch shim it installs.
function loadMock() {
  const src = readFileSync(new URL("./mock.js", import.meta.url), "utf8");
  const g = {};
  new Function("globalThis", src)(g);
  return g.fetch;
}
async function j(f, url, opts) {
  return (await f(url, opts)).json();
}

test("explorer routes return real-shaped JSON incl. a corrupt segment", async () => {
  const f = loadMock();
  const segs = await j(f, "/api/wab/segments");
  assert.equal(segs.totals.sealed, 2);
  assert.ok(segs.segments.some((s) => s.integrity && s.integrity.kind === "CrcMismatch"));
  const rec = await j(f, "/api/wab/segment?path=shard_00/seg_00000001.wab.sealed&limit=200");
  assert.equal(rec.records[0].utf8_preview, "order-1");
  const corrupt = await j(f, "/api/wab/segment?path=shard_00/seg_00000002.wab.sealed");
  assert.ok(corrupt.records.some((r) => r.error));
  const v = await j(f, "/api/wab/verify?path=shard_00/seg_00000002.wab.sealed");
  assert.equal(v.ok, false);
  assert.equal(v.kind, "CrcMismatch");
});

test("ops status increments accepted (so Live animates)", async () => {
  const f = loadMock();
  const a = (await j(f, "/api/ops/status")).summary.accepted;
  const b = (await j(f, "/api/ops/status")).summary.accepted;
  assert.ok(b > a, `accepted should increase: ${a} -> ${b}`);
});

test("ops drop empties the store; preview does not", async () => {
  const f = loadMock();
  assert.equal((await j(f, "/api/ops/dead-letter")).count, 2);
  const prev = await j(f, "/api/ops/drop/preview", { method: "POST" });
  assert.equal(prev.candidates, 2);
  assert.equal((await j(f, "/api/ops/dead-letter")).count, 2, "preview must not mutate");
  const done = await j(f, "/api/ops/drop", { method: "POST" });
  assert.equal(done.dropped, 2);
  assert.equal((await j(f, "/api/ops/dead-letter")).count, 0, "drop empties the store");
});

test("ops requeue empties the store and reports records", async () => {
  const f = loadMock();
  const prev = await j(f, "/api/ops/requeue/preview?durability=sync", { method: "POST" });
  assert.equal(prev.requeuable_records, 4);
  const done = await j(f, "/api/ops/requeue?durability=sync", { method: "POST" });
  assert.equal(done.requeued_records, 4);
  assert.equal((await j(f, "/api/ops/dead-letter")).count, 0);
});
