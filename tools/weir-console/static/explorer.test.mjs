import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// explorer.js is a plain browser <script> (no exports) that runs
// `document.querySelectorAll(...)` and `main()` at load time. To exercise its
// render helpers under node we (1) strip the trailing auto-`main()` call,
// (2) wrap the source in a Function so its top-level `function`/`const`
// declarations become returnable, and (3) inject minimal `document`/`fetch`
// stubs. The document stub memoises one element per selector and captures the
// last innerHTML assigned, so we can assert on the produced HTML.

function loadExplorer(documentStub, fetchStub) {
  let src = readFileSync(new URL("./explorer.js", import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, ""); // drop the auto-run; the test drives main()
  const factory = new Function(
    "document",
    "fetch",
    src + "\nreturn { main, renderTopbar, renderTree, showSegment, showDeadLetter, recordRow };",
  );
  return factory(documentStub, fetchStub);
}

function makeDom() {
  const els = {};
  const makeEl = () => ({
    innerHTML: "",
    textContent: "",
    onclick: null,
    dataset: {},
    querySelector: () => null,
    querySelectorAll: () => [],
  });
  return {
    els,
    document: {
      querySelector: (sel) => (els[sel] ??= makeEl()),
      querySelectorAll: () => [],
    },
  };
}

const INV = {
  wab_dir: "/tmp/wab",
  totals: { segments: 1, sealed: 1, active: 0, confirmed: 0, dead_letter: 1, total_bytes: 128 },
  segments: [
    {
      shard: "00",
      file: "shard_00/seg_00000001.wab.sealed",
      state: "sealed",
      header: { shard_id: 0, created_at: 1700000000000000000, format_version: 1 },
      footer: { record_count: 2, data_bytes: 9, file_crc32: "0x0000abcd" },
      integrity: { ok: true },
    },
  ],
};

function fetchStub(url) {
  let body;
  if (url.startsWith("/api/wab/segments")) {
    body = INV;
  } else if (url.startsWith("/api/wab/segment")) {
    body = {
      header: INV.segments[0].header,
      terminated_cleanly: true,
      records: [
        { index: 0, len: 5, crc_ok: true, utf8_preview: "alpha", hex_preview: "616c706861" },
      ],
    };
  } else if (url.startsWith("/api/wab/dead-letter")) {
    body = {
      segments: [
        {
          file: "dead_letter/dl_00000001.wab.sealed",
          records: [
            { index: 0, len: 10, crc_ok: true, utf8_preview: "rejected-1", hex_preview: "72656a6563746564" },
          ],
        },
      ],
    };
  } else {
    body = {};
  }
  return Promise.resolve({ ok: true, statusText: "OK", json: () => Promise.resolve(body) });
}

const tick = () => new Promise((r) => setTimeout(r, 0));

test("tree renders a segment row with state chip + integrity badge", async () => {
  const dom = makeDom();
  const app = loadExplorer(dom.document, fetchStub);
  await app.main();
  const tree = dom.els["#tree"].innerHTML;
  assert.match(tree, /seg_00000001\.wab\.sealed/);
  assert.match(tree, /wc-chip/); // a state chip rendered
  assert.match(tree, /✓/); // integrity ok badge
  assert.match(dom.els["#topbar"].innerHTML, /1 segments/);
});

test("record viewer shows utf8 preview + crc✓, and the hex toggle re-renders as hex", async () => {
  const dom = makeDom();
  const app = loadExplorer(dom.document, fetchStub);
  await app.main();
  await app.showSegment("shard_00/seg_00000001.wab.sealed", INV);
  let detail = dom.els["#detail"].innerHTML;
  assert.match(detail, /alpha/); // utf8 preview
  assert.match(detail, /crc✓/);
  assert.ok(!/616c706861/.test(detail), "hex must not show in utf8 mode");

  // flip via the wired toggle button's onclick, then let the async re-render settle
  assert.equal(typeof dom.els["#toggle"].onclick, "function");
  dom.els["#toggle"].onclick();
  await tick();
  detail = dom.els["#detail"].innerHTML;
  assert.match(detail, /616c706861/); // hex preview now shown
  assert.ok(!/alpha/.test(detail), "utf8 must not show in hex mode");
});

test("a torn/Err record renders as an explicit error row", () => {
  const dom = makeDom();
  const app = loadExplorer(dom.document, fetchStub);
  const row = app.recordRow({ index: 3, error: "CrcMismatch" });
  assert.match(row, /#3/);
  assert.match(row, /ERROR: CrcMismatch/);
  assert.match(row, /wc-bad/);
});
