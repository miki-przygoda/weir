import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// ops.js is a plain <script> (no exports) that runs document.querySelectorAll(...) and
// main() at load. We strip the auto-main, wrap the source so its declarations are
// returnable, and inject minimal document/fetch stubs (memoised elements; recorded fetch).
function loadOps(documentStub, fetchStub) {
  let src = readFileSync(new URL("./ops.js", import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, "");
  const factory = new Function(
    "document",
    "fetch",
    "setInterval",
    src +
      "\nreturn { statusLine, dlSummary, dropConfirmMatches, requeuePreviewText, refreshStatus, refreshDeadLetter, requeueFlow, dropFlow };"
  );
  // setInterval is referenced in main (stripped) but keep a no-op for safety.
  return factory(documentStub, fetchStub, () => 0);
}

function makeDom() {
  const els = {};
  // Get-or-create the memoised stub element for a selector.
  const el = (sel) => (els[sel] ??= makeEl());
  function makeEl() {
    let _innerHTML = "";
    const node = {
      textContent: "", value: "", disabled: false, checked: false,
      onclick: null, oninput: null, style: {},
      querySelector: () => null, querySelectorAll: () => [],
    };
    // Model a browser parsing assigned innerHTML: any markup with id="X" becomes a
    // querySelector-addressable element, and an element carrying the `disabled`
    // boolean attribute starts disabled (ops.js renders `<button id="dr-go" disabled>`
    // and reads `<select id="rq-dur">` only later, inside its click handler).
    Object.defineProperty(node, "innerHTML", {
      get: () => _innerHTML,
      set: (html) => {
        _innerHTML = html;
        const re = /<(\w+)\b([^>]*)\bid="([^"]+)"([^>]*)>/g;
        let m;
        while ((m = re.exec(html)) !== null) {
          const child = el("#" + m[3]);
          if (/(^|\s)disabled(\s|=|>|\/|$)/.test(m[2] + m[4])) child.disabled = true;
        }
      },
    });
    return node;
  }
  return {
    els,
    document: {
      querySelector: (sel) => el(sel),
      querySelectorAll: () => [],
    },
  };
}

function makeFetch(routes) {
  const calls = [];
  const fetchStub = (url, opts) => {
    calls.push({ url, opts });
    const key = Object.keys(routes).find((k) => url.startsWith(k));
    const body = key ? routes[key] : {};
    return Promise.resolve({ ok: true, statusText: "OK", json: () => Promise.resolve(body) });
  };
  return { fetchStub, calls };
}

const UP = {
  daemon: "up",
  summary: { accepted: 5, ack: 4, nack: 1, fsync_avg_ms: 50.0, queue_depth: 7, wab_bytes_on_disk: 4096, dead_letter_bytes_on_disk: 2048, sink_type: "http", sink_health: "healthy", flusher_panics: 0, fsync_failures: 0 },
};
const DL = { dead_letter_dir: "/x", count: 2, total_bytes: 1024, segments: [{ segment: "dl_00000001.wab.sealed", bytes: 512 }, { segment: "dl_00000002.wab.sealed", bytes: 512 }] };

const tick = () => new Promise((r) => setTimeout(r, 0));

test("pure helpers format status / dl / previews / drop gate", () => {
  // NOTE: ordering — these are pure and don't touch the DOM, so any stub works.
  const dom = makeDom();
  const app = loadOps(dom.document, () => Promise.resolve({ ok: true, json: () => Promise.resolve({}) }));
  assert.match(app.statusLine(UP), /daemon/);
  assert.match(app.statusLine(UP), /healthy/);
  assert.match(app.statusLine({ daemon: "down" }), /down/);
  assert.equal(app.dlSummary(DL), "2 dead-letter segment(s) · 1024 bytes");
  assert.ok(app.dropConfirmMatches("2", 2));
  assert.ok(!app.dropConfirmMatches("1", 2));
  assert.match(app.requeuePreviewText({ segments: 2, readable_segments: 2, unreadable_segments: 0, requeuable_records: 5 }), /would requeue 5 record\(s\) from 2 segment\(s\)/);
});

test("status header + dead-letter panel render from mock JSON", async () => {
  const dom = makeDom();
  const { fetchStub } = makeFetch({ "/api/ops/status": UP, "/api/ops/dead-letter": DL });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus();
  await app.refreshDeadLetter();
  assert.match(dom.els["#ops-status"].innerHTML, /daemon/);
  assert.match(dom.els["#ops-status"].innerHTML, /queue 7/);
  assert.match(dom.els["#dl-body"].innerHTML, /dl_00000001\.wab\.sealed/);
});

test("drop flow: confirm button stays disabled until the typed count matches", async () => {
  const dom = makeDom();
  const { fetchStub } = makeFetch({
    "/api/ops/status": { daemon: "down" },
    "/api/ops/drop/preview": { dry_run: true, candidates: 2, candidate_bytes: 1024, dropped: 0, dropped_bytes: 0 },
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus(); // daemonLive = false
  await app.dropFlow();
  const inp = dom.els["#dr-count"], go = dom.els["#dr-go"];
  assert.equal(go.disabled, true, "starts disabled");
  inp.value = "9"; inp.oninput();
  assert.equal(go.disabled, true, "wrong count keeps it disabled");
  inp.value = "2"; inp.oninput();
  assert.equal(go.disabled, false, "matching count enables it");
});

test("requeue flow: confirm calls the commit endpoint", async () => {
  const dom = makeDom();
  const { fetchStub, calls } = makeFetch({
    "/api/ops/status": { daemon: "down" },
    "/api/ops/requeue/preview": { dry_run: true, segments: 2, readable_segments: 2, unreadable_segments: 0, requeuable_records: 5 },
    "/api/ops/requeue": { dry_run: false, segments: 2, requeued_records: 5, segments_cleared: 2, skipped_segments: 0, delete_failures: 0, durability: "batched" },
    "/api/ops/dead-letter": DL,
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus();
  await app.requeueFlow();
  dom.els["#rq-dur"].value = "batched";
  await dom.els["#rq-go"].onclick();
  await tick();
  const committed = calls.find((c) => c.url.startsWith("/api/ops/requeue?") && c.opts && c.opts.method === "POST");
  assert.ok(committed, "a POST to the requeue commit endpoint was made");
  assert.match(dom.els["#rq-result"].innerHTML, /requeued 5/);
});

test("live daemon gates the action until the box is checked", async () => {
  const dom = makeDom();
  const { fetchStub, calls } = makeFetch({
    "/api/ops/status": UP, // daemon live
    "/api/ops/drop/preview": { dry_run: true, candidates: 2, candidate_bytes: 1024, dropped: 0, dropped_bytes: 0 },
    "/api/ops/drop": { dropped: 2 },
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus(); // daemonLive = true
  await app.dropFlow();
  const inp = dom.els["#dr-count"], go = dom.els["#dr-go"];
  inp.value = "2"; inp.oninput();
  // live-ok checkbox not checked -> clicking go must NOT call the drop endpoint
  await go.onclick();
  await tick();
  assert.ok(!calls.some((c) => c.url === "/api/ops/drop"), "must not drop until the live box is ticked");
  assert.match(dom.els["#dr-result"].innerHTML, /tick the box/);
});
