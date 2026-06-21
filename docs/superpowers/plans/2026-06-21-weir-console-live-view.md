# weir-console Live view (view G) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the Live view to `weir-console` — a canvas pipeline animation driven by a real running daemon's polled metrics (rates from counter deltas), with live counters + sparklines.

**Architecture:** Frontend-only. **Zero new Rust.** Live is `static/live.html` + `static/live.js`, served by the existing `ServeDir`, polling the **existing `/api/ops/status`** endpoint (which already shells out to `weir-ctl metrics --json`). Plus activating the `Live` nav link in `index.html` and `ops.html`, a `node --test` smoke, and a README update.

**Tech Stack:** vanilla HTML/CSS/JS + canvas; `node --test`. No backend, no deps, no CLI args.

**Canonical spec:** `docs/superpowers/specs/2026-06-21-weir-console-live-view-design.md`

**Data source — `/api/ops/status` (already implemented):**
```json
{ "daemon": "up" | "down", "metrics_addr": "127.0.0.1:9185",
  "summary": { "accepted", "ack", "nack", "fsync_avg_ms", "queue_depth",
               "wab_bytes_on_disk", "dead_letter_bytes_on_disk", "sink_type",
               "sink_health", "flusher_panics", "fsync_failures" } }
```
`accepted`/`ack`/`nack` are monotonic counters (→ rates via deltas); the rest are gauges.

**Existing nav (each has a disabled `Live` placeholder to activate):**
- `static/index.html`: `<a href="#" class="wc-disabled" title="coming soon">Live</a>`
- `static/ops.html`: `<a href="#" class="wc-disabled" title="coming soon">Live</a>`

---

### Task 1: Live page + script (`live.html`, `live.js`) + nav activation

**Files:**
- Create: `tools/weir-console/static/live.html`
- Create: `tools/weir-console/static/live.js`
- Modify: `tools/weir-console/static/index.html` (activate Live nav)
- Modify: `tools/weir-console/static/ops.html` (activate Live nav)

- [ ] **Step 1: Create `live.html`**

`tools/weir-console/static/live.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>weir-console · Live</title>
<link rel="stylesheet" href="weir.css" />
<style>
  #live-counters { font-size: 12px; padding: 8px; border: 1px solid var(--n-border); margin-bottom: 12px; }
  .wc-chip { font-size: 10px; padding: 1px 6px; border: 1px solid var(--n-border); margin-left: 4px; }
  .wc-ok { color: var(--n-green); } .wc-bad { color: var(--n-rose); }
  #cv { width: 100%; border: 1px solid var(--n-border); background: var(--n-bg); }
  .wc-sparks { margin-top: 10px; }
  .wc-spark { font-family: 'JetBrains Mono', monospace; font-size: 14px; margin-right: 16px; }
</style>
</head>
<body>
<div class="statusbar top">
  <span>weir-console · Live · <span data-weir-version></span></span>
  <span class="sb-spacer"></span>
  <span class="sb-dim">live pipeline</span>
</div>
<nav class="nav">
  <a href="index.html">Explorer</a>
  <a href="ops.html">Ops</a>
  <a href="live.html" class="active">Live</a>
</nav>
<div class="wrap">
  <div id="live-counters">connecting…</div>
  <canvas id="cv" width="1040" height="180"></canvas>
  <div class="wc-sparks">
    <span class="wc-spark">acc/s <span id="spark-acc"></span></span>
    <span class="wc-spark">queue <span id="spark-queue"></span></span>
  </div>
  <p class="sb-dim">Live rates are polled from the daemon's /metrics every ~1.5s (no per-record stream exists). The drain→sink stage reflects queue depth + sink health, not a per-second drain rate.</p>
</div>
<script src="live.js"></script>
</body>
</html>
```

- [ ] **Step 2: Create `live.js`**

`tools/weir-console/static/live.js`:

```js
const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach((e) => (e.textContent = "1.2.0"));

const POLL_MS = 1500;
const SPARK_CAP = 60;

async function getStatus() {
  const r = await fetch("/api/ops/status");
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

// ── pure helpers (unit-tested) ──
// Per-second rates from two counter samples. First sample (prev null) -> zeros.
// Counter reset (cur < prev, daemon restarted) -> 0 for that series, never negative.
function computeRates(prev, cur, dtSeconds) {
  const rate = (a, b) => {
    if (prev == null || dtSeconds <= 0) return 0;
    const d = (b ?? 0) - (a ?? 0);
    return d < 0 ? 0 : d / dtSeconds;
  };
  return {
    accepted: rate(prev && prev.accepted, cur.accepted),
    ack: rate(prev && prev.ack, cur.ack),
    nack: rate(prev && prev.nack, cur.nack),
  };
}

// Fixed-capacity ring buffer (oldest dropped past cap).
function makeRing(cap) {
  const buf = [];
  return {
    push(v) { buf.push(v); if (buf.length > cap) buf.shift(); return buf; },
    values() { return buf.slice(); },
  };
}

function fmtRate(r) { return (r >= 1000 ? (r / 1000).toFixed(1) + "k" : r.toFixed(0)) + "/s"; }
function fmtBytes(b) {
  b = b || 0;
  const K = 1024;
  if (b >= K * K) return (b / (K * K)).toFixed(1) + " MiB";
  if (b >= K) return (b / K).toFixed(1) + " KiB";
  return b + " B";
}
function chip(t, c) { return `<span class="wc-chip ${c || ""}">${t}</span>`; }

function countersHtml(summary, rates) {
  const m = summary || {};
  const health = m.sink_health === "healthy" ? chip("healthy", "wc-ok") : chip(m.sink_health || "?", "wc-bad");
  const alarms = (m.flusher_panics || 0) + (m.fsync_failures || 0);
  const alarmChip = alarms > 0 ? chip(`${alarms} alarm(s)`, "wc-bad") : chip("0 alarms", "wc-ok");
  return `acc ${fmtRate(rates.accepted)} · ack ${fmtRate(rates.ack)} · nack ${fmtRate(rates.nack)} · ` +
    `queue ${m.queue_depth || 0} · fsync ${m.fsync_avg_ms ?? "?"}ms · ` +
    `sink ${m.sink_type || "?"} ${health} · dl ${fmtBytes(m.dead_letter_bytes_on_disk)} · ${alarmChip}`;
}

function sparkline(vals) {
  if (!vals.length) return "";
  const blocks = "▁▂▃▄▅▆▇█";
  const max = Math.max(1, ...vals);
  return vals.map((v) => blocks[Math.min(blocks.length - 1, Math.floor((v / max) * (blocks.length - 1)))]).join("");
}

// ── live state ──
let prevSample = null;
let rates = { accepted: 0, ack: 0, nack: 0 };
let live = false;
let queueNow = 0;
const accRing = makeRing(SPARK_CAP);
const queueRing = makeRing(SPARK_CAP);

function renderCounters(status) {
  if (!status || status.daemon !== "up") {
    $("#live-counters").innerHTML = `<span class="wc-bad">waiting for daemon at ${(status && status.metrics_addr) || "?"}…</span>`;
    return;
  }
  $("#live-counters").innerHTML = countersHtml(status.summary, rates);
}

function renderSparks() {
  $("#spark-acc").textContent = sparkline(accRing.values());
  $("#spark-queue").textContent = sparkline(queueRing.values());
}

async function poll() {
  try {
    const status = await getStatus();
    if (status.daemon === "up") {
      const cur = status.summary || {};
      rates = computeRates(prevSample, cur, POLL_MS / 1000);
      prevSample = cur;
      live = true;
      queueNow = cur.queue_depth || 0;
      accRing.push(rates.accepted);
      queueRing.push(queueNow);
    } else {
      live = false;
      prevSample = null; // re-baseline when the daemon returns
    }
    renderCounters(status);
    renderSparks();
  } catch (e) {
    live = false;
    prevSample = null;
    $("#live-counters").innerHTML = `<span class="wc-bad">${e.message}</span>`;
  }
}

// ── canvas pipeline animation ──
const STAGES = ["Producer", "Socket", "WAB", "fsync", "Ack", "Drain", "Sink"];
let dots = [];
let spawnAccum = 0;
let lastTs = 0;

function drawPipeline(ctx, W, H) {
  ctx.clearRect(0, 0, W, H);
  const lane = H / 2;
  ctx.fillStyle = "#9aa4b2";
  ctx.font = "11px monospace";
  STAGES.forEach((s, i) => {
    const x = (i / (STAGES.length - 1)) * (W - 60) + 20;
    ctx.fillText(s, x, lane - 12);
    if (s === "Drain") ctx.fillText(`queue:${queueNow}`, x, lane + 22);
  });
  ctx.strokeStyle = "#2a2a2a";
  ctx.beginPath();
  ctx.moveTo(10, lane);
  ctx.lineTo(W - 10, lane);
  ctx.stroke();
  ctx.fillStyle = live ? "#38bdf8" : "#555";
  for (const d of dots) {
    ctx.beginPath();
    ctx.arc(d.x, lane, 3, 0, Math.PI * 2);
    ctx.fill();
  }
  if (!live) {
    ctx.fillStyle = "#f87171";
    ctx.fillText("waiting for daemon…", W / 2 - 50, lane - 30);
  }
}

function frame(ts) {
  const cv = $("#cv");
  const ctx = cv && cv.getContext ? cv.getContext("2d") : null;
  const dt = lastTs ? (ts - lastTs) / 1000 : 0;
  lastTs = ts;
  if (ctx) {
    if (live) {
      spawnAccum += Math.min(rates.accepted, 200) * dt; // cap spawn for sanity
      while (spawnAccum >= 1) { dots.push({ x: 0 }); spawnAccum -= 1; }
    }
    const W = cv.width, H = cv.height, speed = W / 2.5; // cross in ~2.5s
    for (const d of dots) d.x += speed * dt;
    dots = dots.filter((d) => d.x <= W);
    drawPipeline(ctx, W, H);
  }
  requestAnimationFrame(frame);
}

async function main() {
  await poll();
  if (typeof setInterval === "function") setInterval(poll, POLL_MS);
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(frame);
}
main();
```

- [ ] **Step 3: Activate the Live nav link in `index.html` and `ops.html`**

In `tools/weir-console/static/index.html`, change:
```html
  <a href="#" class="wc-disabled" title="coming soon">Live</a>
```
to:
```html
  <a href="live.html">Live</a>
```

In `tools/weir-console/static/ops.html`, change:
```html
  <a href="#" class="wc-disabled" title="coming soon">Live</a>
```
to:
```html
  <a href="live.html">Live</a>
```

- [ ] **Step 4: Validate the static files + curl smoke**

```bash
node --check tools/weir-console/static/live.js && echo "OK: live.js parses"
mkdir -p "$CLAUDE_JOB_DIR/tmp/live-smoke"
target_dir=$(cargo metadata --manifest-path tools/weir-console/Cargo.toml --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
cargo build --manifest-path tools/weir-console/Cargo.toml
"$target_dir/debug/weir-console" --wab-dir "$CLAUDE_JOB_DIR/tmp/live-smoke" --bind 127.0.0.1:18821 &
echo $! > /tmp/wc-live.pid
sleep 2
curl -s http://127.0.0.1:18821/live.html | grep -q 'src="live.js"' && echo "LIVE HTML OK"
curl -s http://127.0.0.1:18821/index.html | grep -q 'href="live.html"' && echo "INDEX NAV OK"
curl -s http://127.0.0.1:18821/ops.html | grep -q 'href="live.html"' && echo "OPS NAV OK"
kill "$(cat /tmp/wc-live.pid)"
```

Expected: `OK: live.js parses`, `LIVE HTML OK`, `INDEX NAV OK`, `OPS NAV OK`. Kill only the PID we started; no `pkill`.

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/static/live.html tools/weir-console/static/live.js tools/weir-console/static/index.html tools/weir-console/static/ops.html
git commit -m "feat(weir-console): Live view (real-data pipeline animation, counters, sparklines)"
```

---

### Task 2: Frontend smoke test (node, no backend)

**Files:**
- Create: `tools/weir-console/static/live.test.mjs`

- [ ] **Step 1: Write the smoke test**

`tools/weir-console/static/live.test.mjs` — mirrors `ops.test.mjs`: strips the trailing auto-`main()`, wraps `live.js` so its helpers are returnable, injects stubbed `document`/`fetch`/`setInterval`/`requestAnimationFrame`.

```js
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
```

- [ ] **Step 2: Run it**

Run: `node --test tools/weir-console/static/live.test.mjs`
Expected: `# pass 6`, `# fail 0`. Do not weaken assertions to force a pass — if `live.js` genuinely misbehaves, fix `live.js`; if a stub is mechanically incomplete, fix the harness but keep every assertion.

- [ ] **Step 3: Commit**

```bash
git add tools/weir-console/static/live.test.mjs
git commit -m "test(weir-console): Live frontend smoke (rates, reset guard, ring cap, render, down state)"
```

---

### Task 3: README + final gate

**Files:**
- Modify: `tools/weir-console/README.md`

- [ ] **Step 1: Extend the README**

Update the intro list and add a Live section. In `tools/weir-console/README.md`, change the intro list (the `- **WAB Explorer**` / `- **Ops Control Panel**` list, currently ending with "A **Live** view is planned (nav placeholder for now).") so the closing line becomes a third bullet instead:

Replace:
```markdown
A **Live** view is planned (nav placeholder for now).
```
with:
```markdown
- **Live** — a real-time pipeline animation of a running daemon: polled metric
  rates animate Producer→Socket→WAB→fsync→Ack→Drain→Sink, with live counters and
  sparklines.
```

Then add this section just before the `## Theme` section:

```markdown
## Live view

The **Live** tab animates the weir pipeline at a **running daemon's** measured
throughput. It polls the existing `/api/ops/status` (which shells out to
`weir-ctl metrics --json`) every ~1.5s, computes per-second rates from the counter
deltas, and renders:

- **Counter cards** — accepted/ack/nack per second, queue depth, avg fsync ms, sink
  health + type, dead-letter bytes, and a panic/fsync-failure alarm chip.
- **A canvas pipeline** — dots spawn at the real accepted/sec and flow
  Producer→…→Sink; the Drain stage shows the queue backlog and the Sink is tinted by
  sink health.
- **Sparklines** — a short rolling history of accepted/sec and queue depth.

Honest scope: the daemon exposes only pull `/metrics` (no event stream), so Live shows
**polled rates**, not per-record events; and the drain→sink stage reflects queue depth +
sink health, not a per-second drain rate. With no daemon running, Live shows a clean
"waiting for daemon…" state and resumes automatically. Live needs `--metrics-addr` to
point at the daemon's `/metrics` (default `127.0.0.1:9185`) and `weir-ctl` to be
resolvable (see the Ops flags).
```

- [ ] **Step 2: Confirm the tool is still clean (no Rust changed)**

Run: `cargo fmt --manifest-path tools/weir-console/Cargo.toml --check`
Expected: clean (no Rust touched).
Run: `cargo clippy --manifest-path tools/weir-console/Cargo.toml --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Full tool test + all node smokes + workspace-untouched check**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml`
Expected: all pass (5 wab + 13 ops).
Run: `node --test tools/weir-console/static/live.test.mjs && node --test tools/weir-console/static/ops.test.mjs && node --test tools/weir-console/static/explorer.test.mjs`
Expected: live 6/6, ops 5/5, explorer 3/3.
Run: `cargo build --workspace && git status --short Cargo.lock Cargo.toml`
Expected: workspace builds; the **root** `Cargo.lock`/`Cargo.toml` unchanged (empty output).

- [ ] **Step 4: Commit**

```bash
git add tools/weir-console/README.md
git commit -m "docs(weir-console): README for the Live view"
```

---

## Self-review

**Spec coverage:** pipeline animation (real data) — Task 1 (`frame`/`drawPipeline`, dots at `accepted/sec`); counters — Task 1 (`countersHtml`/`renderCounters`); sparklines — Task 1 (`sparkline`/`makeRing`); rates from deltas + reset guard — Task 1 (`computeRates`) + Task 2 tests; daemon-down "waiting" state — Task 1 (`renderCounters`/`drawPipeline`) + Task 2 test; reuses `/api/ops/status`, zero new Rust — Tasks 1–3 (no Cargo/src changes); nav activation — Task 1; testing — Task 2 (6 `node --test` cases over the pure layer + a frame smoke); README + honesty note — Tasks 1 & 3; out-of-scope (per-record stream, true drain rate, daemon control, /metrics parser) — not implemented. ✓ all covered.

**Placeholder scan:** no TBD/TODO; every step has complete code and exact commands with expected output.

**Type consistency:** `computeRates(prev, cur, dtSeconds)`, `makeRing(cap).{push,values}`, `fmtRate`/`fmtBytes`/`sparkline`/`countersHtml`/`renderCounters`/`poll`/`frame` are defined in Task 1 and returned by the Task 2 wrapper with identical names; the fields read (`status.daemon`, `status.metrics_addr`, `summary.{accepted,ack,nack,queue_depth,fsync_avg_ms,sink_type,sink_health,dead_letter_bytes_on_disk,flusher_panics,fsync_failures}`) match the verified `/api/ops/status` shape. No backend types involved.
