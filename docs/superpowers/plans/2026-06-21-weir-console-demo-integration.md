# weir-console → demo site integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an interactive, no-backend mock of all three weir-console views to the static demo bundle under `demo/console/`, wired into the demo nav + the examples `#admin` row.

**Architecture:** `demo/console/mock.js` installs a `globalThis.fetch` shim returning canned JSON in the real `/api/*` shapes; **verbatim copies** of `tools/weir-console/static/{explorer,ops,live}.js` render it unchanged; three adapted HTML pages load `mock.js` before the view script and reuse `../weir.css` + `../version.js`. The real `tools/weir-console/` is untouched.

**Tech Stack:** vanilla HTML/CSS/JS; `node --test`. No backend, no Rust, no deps.

**Canonical spec:** `docs/superpowers/specs/2026-06-21-weir-console-demo-integration-design.md`

**Real `/api/*` shapes (verified against the live binary — the mock MUST match these):**
- `/api/wab/segments` → `{wab_dir, totals:{segments,sealed,active,confirmed,dead_letter,total_bytes}, segments:[{shard,file,state,size_bytes,header,footer,integrity,confirmed}]}`
- `/api/wab/segment?path=&offset=&limit=` → `{file, header, records:[{index,len,crc_ok,hex_preview,utf8_preview}|{index,crc_ok,error}], terminated_cleanly}`
- `/api/wab/verify?path=` → `{ok:true}` | `{ok:false,kind,expected,computed}`
- `/api/wab/dead-letter` (Explorer) → `{segments:[{file, records:[…]}]}`
- `/api/ops/status` → `{daemon:"up", metrics_addr, summary:{accepted,ack,nack,fsync_avg_ms,queue_depth,wab_bytes_on_disk,dead_letter_bytes_on_disk,sink_type,sink_health,flusher_panics,fsync_failures}}`
- `/api/ops/dead-letter` (ops dl list) → `{dead_letter_dir,count,total_bytes,segments:[{segment,bytes}]}`
- `/api/ops/requeue/preview` → `{dry_run:true,segments,readable_segments,unreadable_segments,requeuable_records}`; `/api/ops/requeue` → `{dry_run:false,segments,requeued_records,segments_cleared,skipped_segments,delete_failures,durability}`
- `/api/ops/drop/preview` → `{dry_run:true,candidates,candidate_bytes,dropped:0,dropped_bytes:0}`; `/api/ops/drop` → `{dry_run:false,candidates,dropped,dropped_bytes,failures}`

---

### Task 1: `mock.js` + its unit test

**Files:**
- Create: `demo/console/mock.js`
- Create: `demo/console/mock.test.mjs`

- [ ] **Step 1: Create `mock.js`**

`demo/console/mock.js`:

```js
// Static-demo backend shim for weir-console. Installs globalThis.fetch so the REAL view
// scripts (explorer.js / ops.js / live.js — verbatim copies) run with NO backend, against
// canned JSON in the verified real /api/* shapes. Loaded BEFORE each view script.
(function () {
  // ── Explorer: WAB inventory (incl. one CrcMismatch segment) ──
  const SEGMENTS = {
    wab_dir: "/var/lib/weir/wab  (demo · sample data)",
    totals: { segments: 3, sealed: 2, active: 1, confirmed: 1, dead_letter: 2, total_bytes: 8192 },
    segments: [
      {
        shard: "00", file: "shard_00/seg_00000001.wab.sealed", state: "sealed", size_bytes: 4096,
        header: { format_version: 1, shard_id: 0, created_at: 1782000000000000000 },
        footer: { record_count: 3, data_bytes: 60, file_crc32: "0x1a2b3c4d", sealed_at: 1782000001000000000 },
        integrity: { ok: true },
        confirmed: { sealed_at: 1782000001000000000, record_count: 3, drained_at: 1782000002000000000 },
      },
      {
        shard: "00", file: "shard_00/seg_00000002.wab.sealed", state: "sealed", size_bytes: 4096,
        header: { format_version: 1, shard_id: 0, created_at: 1782000003000000000 },
        footer: { record_count: 2, data_bytes: 40, file_crc32: "0xdeadbeef", sealed_at: 1782000004000000000 },
        integrity: { ok: false, kind: "CrcMismatch", expected: "0xdeadbeef", computed: "0x0badf00d" },
      },
      {
        shard: "00", file: "shard_00/seg_00000003.wab", state: "active", size_bytes: 512,
        header: { format_version: 1, shard_id: 0, created_at: 1782000005000000000 },
      },
    ],
  };

  function recordsFor(path) {
    if (path.indexOf("seg_00000002") !== -1) {
      // corrupt: one good record, then an error row (the reader stops there)
      return {
        file: path, header: SEGMENTS.segments[1].header,
        records: [
          { index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 31", utf8_preview: "order-1" },
          { index: 1, crc_ok: false, error: "CrcMismatch" },
        ],
        terminated_cleanly: false,
      };
    }
    if (path.indexOf("seg_00000003") !== -1) {
      return {
        file: path, header: SEGMENTS.segments[2].header,
        records: [{ index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 39", utf8_preview: "order-9" }],
        terminated_cleanly: null,
      };
    }
    return {
      file: path, header: SEGMENTS.segments[0].header,
      records: [
        { index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 31", utf8_preview: "order-1" },
        { index: 1, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 32", utf8_preview: "order-2" },
        { index: 2, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 33", utf8_preview: "order-3" },
      ],
      terminated_cleanly: true,
    };
  }

  function verifyFor(path) {
    if (path.indexOf("seg_00000002") !== -1) {
      return { ok: false, kind: "CrcMismatch", expected: "0xdeadbeef", computed: "0x0badf00d" };
    }
    return { ok: true };
  }

  const WAB_DEAD_LETTER = {
    segments: [
      {
        file: "dead_letter/dl_00000001.wab.sealed",
        records: [
          { index: 0, len: 8, crc_ok: true, hex_preview: "72 65 6a 65 63 74 2d 31", utf8_preview: "reject-1" },
          { index: 1, len: 8, crc_ok: true, hex_preview: "72 65 6a 65 63 74 2d 32", utf8_preview: "reject-2" },
        ],
      },
    ],
  };

  // ── Ops: mutable dead-letter store ──
  let dlStore = [
    { segment: "dl_00000001.wab.sealed", bytes: 512 },
    { segment: "dl_00000002.wab.sealed", bytes: 490 },
  ];
  const dlCount = () => dlStore.length;
  const dlBytes = () => dlStore.reduce((a, s) => a + s.bytes, 0);
  const dlRecords = () => dlStore.length * 2;

  // ── Live: counters that increment so the animation/sparklines have motion ──
  let accepted = 12840, ack = 12835;
  const nack = 7;
  function opsStatus() {
    accepted += 9 + Math.floor(Math.random() * 24); // strictly increasing
    ack = accepted - Math.floor(Math.random() * 4);
    return {
      daemon: "up", metrics_addr: "127.0.0.1:9185  (demo)",
      summary: {
        accepted, ack, nack,
        fsync_avg_ms: 0.42, queue_depth: 2 + Math.floor(Math.random() * 6),
        wab_bytes_on_disk: 8192, dead_letter_bytes_on_disk: dlBytes(),
        sink_type: "http", sink_health: "healthy", flusher_panics: 0, fsync_failures: 0,
      },
    };
  }

  function opsDlList() {
    return { dead_letter_dir: "/var/lib/weir/wab/dead_letter  (demo)", count: dlCount(), total_bytes: dlBytes(), segments: dlStore.slice() };
  }
  function dropPreview() { return { dry_run: true, candidates: dlCount(), candidate_bytes: dlBytes(), dropped: 0, dropped_bytes: 0 }; }
  function dropCommit() { const c = dlCount(), b = dlBytes(); dlStore = []; return { dry_run: false, candidates: c, dropped: c, dropped_bytes: b, failures: 0 }; }
  function requeuePreview() { return { dry_run: true, segments: dlCount(), readable_segments: dlCount(), unreadable_segments: 0, requeuable_records: dlRecords() }; }
  function requeueCommit(durability) { const s = dlCount(), r = dlRecords(); dlStore = []; return { dry_run: false, segments: s, requeued_records: r, segments_cleared: s, skipped_segments: 0, delete_failures: 0, durability: durability || "batched" }; }

  function respond(body, ok) {
    return Promise.resolve({ ok: ok !== false, status: ok === false ? 500 : 200, statusText: "OK", json: () => Promise.resolve(body) });
  }

  function mockFetch(url, opts) {
    const u = new URL(url, "http://demo.local");
    const p = u.pathname;
    const method = (opts && opts.method) || "GET";
    if (p === "/api/wab/segments") return respond(SEGMENTS);
    if (p === "/api/wab/segment") return respond(recordsFor(u.searchParams.get("path") || ""));
    if (p === "/api/wab/verify") return respond(verifyFor(u.searchParams.get("path") || ""));
    if (p === "/api/wab/dead-letter") return respond(WAB_DEAD_LETTER);
    if (p === "/api/ops/status") return respond(opsStatus());
    if (p === "/api/ops/dead-letter") return respond(opsDlList());
    if (p === "/api/ops/requeue/preview") return respond(requeuePreview());
    if (p === "/api/ops/requeue") return respond(requeueCommit(u.searchParams.get("durability")));
    if (p === "/api/ops/drop/preview") return respond(dropPreview());
    if (p === "/api/ops/drop") return respond(dropCommit());
    return respond({ error: "mock: unhandled " + method + " " + p }, false);
  }

  globalThis.fetch = mockFetch;
})();
```

- [ ] **Step 2: Create `mock.test.mjs`**

`demo/console/mock.test.mjs`:

```js
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
```

- [ ] **Step 3: Run it**

Run: `node --check demo/console/mock.js && echo "OK: mock.js parses"`
Run: `node --test demo/console/mock.test.mjs`
Expected: `OK: mock.js parses`, then `# pass 4`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add demo/console/mock.js demo/console/mock.test.mjs
git commit -m "feat(demo): weir-console mock backend shim (canned /api/* JSON) + tests"
```

---

### Task 2: View-JS copies + HTML pages + demo wiring

**Files:**
- Create: `demo/console/explorer.js`, `demo/console/ops.js`, `demo/console/live.js` (verbatim copies)
- Create: `demo/console/index.html`, `demo/console/ops.html`, `demo/console/live.html` (adapted copies)
- Modify: `demo/index.html`, `demo/crates.html`, `demo/examples.html` (nav + `#admin` row)
- Modify: `demo/README.md`

- [ ] **Step 1: Copy the three view scripts verbatim**

```bash
mkdir -p demo/console
cp tools/weir-console/static/explorer.js demo/console/explorer.js
cp tools/weir-console/static/ops.js demo/console/ops.js
cp tools/weir-console/static/live.js demo/console/live.js
```

Verify identical: `diff tools/weir-console/static/explorer.js demo/console/explorer.js && diff tools/weir-console/static/ops.js demo/console/ops.js && diff tools/weir-console/static/live.js demo/console/live.js && echo "VERBATIM OK"`

- [ ] **Step 2: Create the three adapted HTML pages**

For each page, copy the console original and apply the SAME four edits. Do it with this exact transform for all three:

```bash
for v in index ops live; do cp "tools/weir-console/static/$v.html" "demo/console/$v.html"; done
```

Then edit each of `demo/console/{index,ops,live}.html`:

1. **CSS path** — change `<link rel="stylesheet" href="weir.css" />` to:
   ```html
   <link rel="stylesheet" href="../weir.css" />
   <script src="../version.js"></script>
   ```
   (version.js fills `data-weir-version` on DOMContentLoaded — after the body scripts — so the canonical demo version wins.)

2. **Statusbar back-link + sample-data note** — replace the page's trailing statusbar dim span (it differs per page: `<span class="sb-dim">read-only</span>` in index.html, `<span class="sb-dim">dead-letter management</span>` in ops.html, `<span class="sb-dim">live pipeline</span>` in live.html) with:
   ```html
   <a class="sb-dim" href="../index.html">← weir demo</a> · <span class="sb-dim">sample data · no backend</span>
   ```

3. **Load `mock.js` before the view script** — change the closing script tag:
   - `index.html`: `<script src="explorer.js"></script>` → `<script src="mock.js"></script>\n<script src="explorer.js"></script>`
   - `ops.html`: `<script src="ops.js"></script>` → `<script src="mock.js"></script>\n<script src="ops.js"></script>`
   - `live.html`: `<script src="live.js"></script>` → `<script src="mock.js"></script>\n<script src="live.js"></script>`

(The console's own Explorer/Ops/Live nav links — `href="index.html"`/`"ops.html"`/`"live.html"` — stay as-is; they resolve within `demo/console/`.)

- [ ] **Step 3: Add a `Console` link to the demo nav (all three demo pages)**

In `demo/index.html`, `demo/crates.html`, and `demo/examples.html`, the nav block is:
```html
  <a href="examples.html">Examples</a>
  <span class="nav-ext">
```
Insert a Console link before `<span class="nav-ext">` in each:
```html
  <a href="examples.html">Examples</a>
  <a href="console/index.html">Console</a>
  <span class="nav-ext">
```
(Each page marks its own tab `class="active"`; Console is not active on these three.)

- [ ] **Step 4: Wire the examples `#admin` card to the console**

In `demo/examples.html`, the `#admin` card ends with:
```html
    <p style="margin:0"><a href="https://miki-przygoda.github.io/weir/monitoring.html">→ monitoring &amp; ops</a></p>
  </div>
```
Add a console link line before the closing `</div>`:
```html
    <p style="margin:0"><a href="https://miki-przygoda.github.io/weir/monitoring.html">→ monitoring &amp; ops</a></p>
    <p style="margin:0"><a href="console/index.html">→ try the interactive console demo (Explorer · Ops · Live)</a></p>
  </div>
```

- [ ] **Step 5: README note**

Append to `demo/README.md`:

```markdown
## console/

`console/` is an **interactive, no-backend mock** of `weir-console` (the local tool under
`tools/weir-console/`): the three real views — **Explorer**, **Ops**, **Live** — running
on canned sample data via `mock.js` (a `fetch` shim). The `*.js` view files are committed
copies of `tools/weir-console/static/*.js`; `mock.js` serves JSON in the real `/api/*`
shapes (including a deliberately-corrupt segment, a mutable dead-letter store so
Drop/Requeue empty it, and an incrementing counter so Live animates). For the real tool,
run `cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <dir>`.
```

- [ ] **Step 6: Validate (static — no server needed)**

```bash
for v in explorer ops live; do node --check demo/console/$v.js; done && echo "view JS OK"
# mock loaded before each view script:
for v in index:explorer ops:ops live:live; do
  page="demo/console/${v%%:*}.html"; js="${v##*:}.js"
  grep -q 'src="mock.js"' "$page" && grep -q "src=\"$js\"" "$page" && echo "$page wires mock+view OK"
done
# nav + admin wiring:
grep -lq 'href="console/index.html"' demo/index.html demo/crates.html demo/examples.html && echo "nav Console OK"
grep -q 'console/index.html' demo/examples.html && echo "admin row OK"
# css/version rewired:
grep -q '../weir.css' demo/console/ops.html && grep -q '../version.js' demo/console/ops.html && echo "css/version OK"
```

Expected: all the `OK` lines print.

- [ ] **Step 7: Commit**

```bash
git add demo/console/*.js demo/console/*.html demo/index.html demo/crates.html demo/examples.html demo/README.md
git commit -m "feat(demo): wire the interactive weir-console (Explorer/Ops/Live) into the demo bundle"
```

---

### Task 3: Integration smoke + final gate

**Files:**
- Create: `demo/console/console.test.mjs`

- [ ] **Step 1: Write the integration smoke**

`demo/console/console.test.mjs` — loads `mock.js` (installs the shim), then drives each REAL view script (verbatim copy) with a DOM stub and asserts it renders from mock data.

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

function mockFetch() {
  const g = {};
  new Function("globalThis", readFileSync(new URL("./mock.js", import.meta.url), "utf8"))(g);
  return g.fetch;
}
function loadView(file, documentStub, fetchStub) {
  let src = readFileSync(new URL("./" + file, import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, "");
  // expose whatever the view defines that we drive; tolerate names not present per-view.
  const exposed = "main, renderTree, renderTopbar, refreshStatus, refreshDeadLetter, poll";
  const ret = "\ntry { return { " + exposed.split(", ").map((n) => n + ": typeof " + n + "!=='undefined'?" + n + ":undefined").join(", ") + " }; } catch (e) { return {}; }";
  return new Function("document", "fetch", "setInterval", "requestAnimationFrame", src + ret)(documentStub, fetchStub, () => 0, () => 0);
}
function makeDom() {
  const els = {};
  const makeEl = () => ({ innerHTML: "", textContent: "", value: "", disabled: false, checked: false, onclick: null, oninput: null, style: {}, querySelector: () => null, querySelectorAll: () => [] });
  return { els, document: { querySelector: (s) => (els[s] ??= makeEl()), querySelectorAll: () => [] } };
}

test("Explorer renders the tree (incl. the corrupt segment) from the mock", async () => {
  const dom = makeDom();
  const app = loadView("explorer.js", dom.document, mockFetch());
  await app.main();
  const tree = dom.els["#tree"].innerHTML;
  assert.match(tree, /seg_00000001\.wab\.sealed/);
  assert.match(tree, /CrcMismatch/); // the corrupt segment's integrity badge
});

test("Ops renders the status header + dead-letter panel from the mock", async () => {
  const dom = makeDom();
  const app = loadView("ops.js", dom.document, mockFetch());
  await app.refreshStatus();
  await app.refreshDeadLetter();
  assert.match(dom.els["#ops-status"].innerHTML, /healthy/);
  assert.match(dom.els["#dl-body"].innerHTML, /dl_00000001\.wab\.sealed/);
});

test("Live renders counters from the mock after two polls", async () => {
  const dom = makeDom();
  const app = loadView("live.js", dom.document, mockFetch());
  await app.poll();
  await app.poll();
  assert.match(dom.els["#live-counters"].innerHTML, /healthy/);
  assert.match(dom.els["#live-counters"].innerHTML, /acc /);
});
```

- [ ] **Step 2: Run the integration smoke**

Run: `node --test demo/console/console.test.mjs`
Expected: `# pass 3`, `# fail 0`. If the Explorer tree assertion fails because the mock + view disagree on a field name, fix the **mock** to match the real shape (never the verbatim view copy) — but the shapes here were verified against the live binary, so it should pass as written.

- [ ] **Step 3: Final gate**

```bash
# all demo-console JS parses
for f in mock explorer ops live; do node --check demo/console/$f.js; done && echo "node --check OK"
# all demo-console node tests
node --test demo/console/mock.test.mjs && node --test demo/console/console.test.mjs
# the REAL tool is untouched: its files match the committed copies and its tests still pass
diff tools/weir-console/static/explorer.js demo/console/explorer.js >/dev/null && echo "explorer copy verbatim"
diff tools/weir-console/static/ops.js demo/console/ops.js >/dev/null && echo "ops copy verbatim"
diff tools/weir-console/static/live.js demo/console/live.js >/dev/null && echo "live copy verbatim"
git status --short tools/weir-console | head        # expect EMPTY (tool untouched)
node --test tools/weir-console/static/explorer.test.mjs && node --test tools/weir-console/static/ops.test.mjs && node --test tools/weir-console/static/live.test.mjs
# demo version drift-check unaffected
./scripts/sync-demo-version.sh && git diff --exit-code demo/version.js && echo "version.js clean"
```

Expected: `node --check OK`; mock 4/4 + console 3/3 pass; the three `copy verbatim` lines print; `git status --short tools/weir-console` is empty; the tool's smokes (3+5+6) pass; `version.js clean`.

- [ ] **Step 4: Commit**

```bash
git add demo/console/console.test.mjs
git commit -m "test(demo): integration smoke — real weir-console views render against the mock"
```

---

## Self-review

**Spec coverage:** mock fetch shim with real shapes — Task 1 (`mock.js`); mutable store (Drop/Requeue empty it) + incrementing Live counter + corrupt segment — Task 1 + tested in Task 1/3; verbatim view-JS copies — Task 2 Step 1 (+ verified in Task 3); adapted HTML (../weir.css, ../version.js, banner+back-link, mock.js first) — Task 2 Step 2; nav Console + `#admin` wiring + README — Task 2 Steps 3–5; integration smoke (real views render mock data) — Task 3; real tool untouched / version drift unaffected — Task 3 Step 3. ✓ all covered.

**Placeholder scan:** no TBD/TODO; `mock.js`, both test files, and every transform/wiring edit are given in full (the HTML adaptations are exact find/replace edits against named source files).

**Type consistency:** the mock's JSON keys exactly match the verified real `/api/*` shapes and the keys the view JS reads (`s.integrity.kind`, `r.error`, `summary.sink_health`, `dl.segments[].segment`, `candidates`/`candidate_bytes`, `requeuable_records`/`segments`); the integration harness drives only functions that exist in each view (`main`/`renderTree` for Explorer, `refreshStatus`/`refreshDeadLetter` for Ops, `poll` for Live) and tolerates absent names.
