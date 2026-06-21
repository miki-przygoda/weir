# weir-console → demo site integration — design spec

**Date:** 2026-06-21 · **Status:** approved design, pre-plan

## Goal

Make all three `weir-console` views (Explorer / Ops / Live) **clickable on the static
demo site**, with **no backend** — a self-contained interactive mock that matches the
demo bundle's existing "run-the-simulation-in-your-browser" ethos. It exercises the
**real** view JS (verbatim copies) against a `fetch` shim that serves canned JSON in the
real `/api/*` shapes.

## Context & decisions (from the brainstorm)

- The `demo/` bundle is **static** (served by GitHub Pages / pulled into the personal
  site). `weir-console` needs a live Rust backend, so the live tool can't be embedded —
  instead a **mock `fetch` layer** stands in for the backend.
- **Reuse the real view JS verbatim** (copies of `tools/weir-console/static/*.js`) so the
  demo shows the actual UI behavior; a separate `mock.js`, loaded first, owns all the
  fake data. Zero changes to the real console code (`tools/weir-console/` is untouched).
- Reuse the demo's `../weir.css` (no duplicate theme) and `../version.js` (version stays
  single-sourced; the demo bundle's existing pattern).
- The mock store is **mutable** so Drop all / Requeue all visibly empty it, and the Live
  status counter **increments** so the canvas animates and sparklines fill.
- The examples page already has an **"Admin & inspection"** (`#admin`) row — the natural
  link target; also add a **Console** entry to the demo nav.
- Real-browser screenshots are out (no browser available here); the interactive mock IS
  the visual.

## Architecture

A new `demo/console/` directory:

- **`mock.js`** — loaded **before** each view script. Installs `globalThis.fetch` (a shim)
  that intercepts every `/api/wab/*` and `/api/ops/*` request and returns a
  `Response`-like `{ ok, status, json() }` with canned JSON in the **verified real
  shapes**. It owns:
  - a static **WAB inventory** (`/api/wab/segments`) — a clean sealed+confirmed segment,
    an **integrity-failed (CrcMismatch)** sealed segment, and an active segment — plus
    per-segment **records** (`/api/wab/segment`, incl. an **error row** for the corrupt
    one) and **verify** results (`/api/wab/verify`), and an Explorer **dead-letter**
    listing (`/api/wab/dead-letter`) with payloads.
  - a **mutable dead-letter store** for Ops (`/api/ops/dead-letter` = `dl list` shape);
    `POST /api/ops/drop` and `POST /api/ops/requeue` empty it; the `*/preview` routes
    report it as a dry-run (no mutation).
  - a **live status** source (`/api/ops/status`) whose `accepted`/`ack` counters
    increment on each call (so `live.js` computes non-zero rates) and whose
    `dead_letter_bytes_on_disk` reflects the mutable store.
- **`explorer.js` / `ops.js` / `live.js`** — **verbatim copies** of the
  `tools/weir-console/static/*.js` files.
- **`index.html` / `ops.html` / `live.html`** — adapted copies of the console pages:
  reference `../weir.css` + `../version.js`, add a "**sample data · no backend**" banner
  and a "← weir demo" back link to the statusbar, keep the console's own
  Explorer/Ops/Live nav, and load `mock.js` **before** the view script.

### Wiring into the demo bundle
- Add a **Console** link to the nav in `demo/index.html`, `demo/crates.html`,
  `demo/examples.html` → `console/index.html`.
- Point the examples page's **`#admin` "Admin & inspection"** row at `console/index.html`
  (the tool that does admin/inspection).
- A short paragraph in `demo/README.md` describing `console/` (interactive mock, sample
  data, links back to the real tool under `tools/weir-console/`).

## Data flow

```
browser ── (relative) fetch /api/* ──▶ mock.js shim ──▶ canned JSON (real shapes)
              the real explorer.js / ops.js / live.js render it, unchanged
```

No network, no backend. The mock is the only difference from the real tool.

## Error handling / fidelity

- The shim always returns well-formed JSON; the views' own error paths are not the focus
  (the mock can optionally surface one corrupt segment so the Explorer's
  corruption-display and an Ops/Live "healthy" status both show).
- Field names exactly match the real `weir-ctl`/`weir-wab` shapes verified end-to-end, so
  the mock can't silently drift the views into reading the wrong keys.

## Testing

- **`mock.test.mjs`** (`node --test`): load `mock.js`, then drive the installed `fetch`:
  - each route returns the right-shaped JSON (segments/segment/verify/dead-letter;
    ops status/dead-letter/requeue-preview/drop-preview);
  - `POST /api/ops/drop` and `POST /api/ops/requeue` empty the dead-letter store (a
    following `/api/ops/dead-letter` reports `count: 0`); the `*/preview` calls do NOT;
  - two `/api/ops/status` calls show a strictly increasing `accepted` (so Live animates).
- **`console.test.mjs`** (`node --test`): the **integration** smoke — load `mock.js`
  (installs fetch) + each view JS (with a DOM stub, mirroring the existing
  `explorer/ops/live.test.mjs` harness) and assert the view renders from the mock data
  (e.g. the Explorer tree shows a segment + the corrupt badge; the Ops dl panel lists a
  segment; the Live counters render after two polls). Proves the integrated demo works
  headlessly.
- `node --check` on `mock.js` and the three view-JS copies.
- The real `tools/weir-console/` is untouched → its tests are unaffected; the `cargo`
  workspace + `demo/version.js` drift-check are unaffected (no Rust, no version.js edit).

## Placement

- `demo/console/{mock.js, explorer.js, ops.js, live.js, index.html, ops.html, live.html,
  mock.test.mjs, console.test.mjs}`; edits to `demo/{index,crates,examples}.html` (nav +
  `#admin` row) and `demo/README.md`.

## Out of scope

- Embedding the live backend (impossible on a static site — that's the point of the mock).
- Real browser screenshots / recordings (separate, can't be produced here).
- Auto-syncing `demo/console/*.js` from `tools/weir-console/static/*.js` — they're
  committed copies (like `weir.css`); the README notes the provenance. A future drift
  check is a possible follow-up, not required here.

## Success criteria

- Opening `demo/console/index.html` (statically) shows the Explorer tree + record viewer
  from mock data incl. a corrupt segment; `ops.html` shows the status header + a
  dead-letter panel whose Drop/Requeue flows preview then empty the store; `live.html`
  animates the pipeline at the mock rate with filling sparklines — all with no backend.
- The demo nav + `#admin` row link to the console; a "← weir demo" link returns.
- `mock.test.mjs` + `console.test.mjs` pass; `node --check` clean; the real
  `tools/weir-console/` and the cargo workspace are untouched.
