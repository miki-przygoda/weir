# weir-console — Live view (view G) — design spec

**Date:** 2026-06-21 · **Status:** approved design, pre-plan

## Goal

Build the third and final view of **`weir-console`**: the **Live view**, which
animates the weir pipeline at a **real running daemon's measured throughput**. It
polls the metrics summary, computes client-side rates, and renders the
Producer→Socket→WAB→fsync→Ack→Drain→Sink flow "breathing" at real speed, with live
counter cards and short rolling sparklines. It is the real-data counterpart to the
standalone `demo/` simulation, and the visual proof that "this is your durable log,
right now."

## Context & decisions (from the brainstorm)

- `weir-console` is one unified tool with three views: **Explorer (D)** and **Ops (E)**
  are built; this spec is **Live (G)**, the last. The nav's currently-disabled **Live**
  tab activates here.
- **The daemon exposes only pull `/metrics`** — no SSE/websocket/event stream. So Live
  cannot stream individual records; it **polls** and computes **rates** from counter
  deltas. The animation is a faithful *rate* visualization (dots spawn at the measured
  accepted/sec), not a per-record event feed. This is stated honestly in the UI.
- **Data source:** Live **reuses the existing `/api/ops/status`** endpoint (which already
  shells out to `weir-ctl metrics --json`). **No new backend code, no new parser, no new
  CLI args.** Live is a new static page + activating the nav.
- **Scope:** a live pipeline animation + live counters + sparklines. NOT a Grafana clone
  (we chose Prometheus+Grafana for dashboards) and NOT the demo simulation (separate
  artifact). A fresh canvas — the demo's naive-comparison lane / crash / tier controls
  don't map onto a real running daemon.
- **No daemon running:** a clean "waiting for daemon at \<addr\>…" idle state; the
  animation resumes when status flips to up. No simulation fallback.

## Architecture

Extend `tools/weir-console/` (excluded from the root workspace, `publish = false`).
**Zero new Rust.** The feature is:

- `static/live.html` — the Live page (hr2-styled, reuses `weir.css` + the console shell).
- `static/live.js` — polls `/api/ops/status`, computes rates, renders the counters +
  canvas pipeline animation + sparklines.
- Activate the **Live** nav link in `static/index.html` and `static/ops.html` (each has a
  disabled `Live` placeholder today).
- `static/live.test.mjs` — a `node --test` smoke over the pure data layer.

The existing `ServeDir` fallback serves the new static files; the existing
`/api/ops/status` route (Ops view) is the sole data source. Live needs no backend route
of its own.

### `/api/ops/status` shape (the data source, already implemented)

```json
{ "daemon": "up" | "down", "metrics_addr": "127.0.0.1:9185",
  "summary": { "accepted", "ack", "nack", "fsync_avg_ms", "queue_depth",
               "wab_bytes_on_disk", "dead_letter_bytes_on_disk", "sink_type",
               "sink_health", "flusher_panics", "fsync_failures" } }
```

`accepted`/`ack`/`nack` are monotonic counters (→ rates via deltas); the rest are
point-in-time gauges (used directly).

## Frontend — `live.js`

### Data model (the testable core)
- Poll `/api/ops/status` every **~1.5s**.
- Keep the previous sample. **`computeRates(prev, cur, dtSeconds)`** → `{accepted, ack,
  nack}` per second = `(cur.total − prev.total) / dt`. Pure function — unit-tested.
  - **First sample:** no previous → rates are 0 / "—" (baseline only).
  - **Reset guard:** if any `cur.total < prev.total` (daemon restarted, counters reset),
    return 0 for that rate and re-baseline — never show a negative rate.
- A small **ring buffer** (cap ~60 samples) per sparkline series (accepted/sec, queue
  depth); `push` drops the oldest past the cap. Pure — unit-tested.
- `daemon: "down"` (or a fetch error) → enter the **paused** state; the animation idles
  and the header shows "waiting for daemon at \<metrics_addr\>…". Resumes on the next
  `up` sample.

### Render
- **Counter cards** (from the latest sample): accepted/sec, ack/sec, nack/sec, queue
  depth, fsync avg ms, sink health (colored chip) + sink type, dead-letter bytes, and an
  **alarms** chip (highlighted when `flusher_panics + fsync_failures > 0`).
- **Canvas pipeline** (`requestAnimationFrame`): a single weir lane
  Producer→Socket→WAB→fsync→Ack→Drain→Sink. Dots spawn at the producer at the real
  **accepted/sec**, flow left→right, and "land" at **Ack** at the **ack/sec**; the
  **Drain** stage shows `queue_depth` as a backlog badge and **Sink** is tinted by
  `sink_health`. An idle daemon (0/s) shows no dots — honestly still. The rate inputs
  update each poll; the animation interpolates between polls for smooth motion.
- **Sparklines:** short rolling history of accepted/sec and queue depth.
- A small honesty note in the UI: the drain→sink stage reflects **queue depth + sink
  health**, not a per-second drain rate (the summary doesn't expose one).

JS is plain `fetch` + DOM + canvas; no framework, no build step.

## Data flow

```
browser ──poll /api/ops/status (every ~1.5s)──▶ weir-console ──weir-ctl metrics --json──▶ daemon /metrics
   └── client-side: deltas → rates → counter cards + canvas animation + sparklines
```

## Error handling

- **Daemon down / unreachable / fetch error** → the paused "waiting" state, not a crash
  or an error banner spam. The poll keeps retrying; it recovers automatically when the
  daemon returns.
- **`weir-ctl` missing** (the `/api/ops/status` returns a `NotFound` error) → the header
  shows a one-line "weir-ctl not found — Ops/Live need it (see --weir-ctl)" note;
  no crash, no animation.
- **Counter reset** (daemon restart) → handled by the reset guard; the animation simply
  re-baselines.

## Testing

- **Frontend smoke** (`static/live.test.mjs`, `node --test`, mirroring
  `ops.test.mjs`/`explorer.test.mjs`): strip the trailing auto-`main()`, wrap `live.js`
  so its helpers are returnable, stub `document`/`fetch`/`requestAnimationFrame`/`canvas
  getContext`. Assert:
  - `computeRates`: two successive samples → correct per-second rates; first sample →
    zeros; a counter drop (reset) → 0, no negative.
  - the ring buffer caps at its limit and drops the oldest.
  - a `daemon:"down"` sample → the paused state / "waiting" header; an `up` sample →
    counters render (e.g. the sink-health chip, a non-zero accepted/sec after two polls).
  - counter formatting (rate + bytes) is correct.
  The canvas is exercised through a stubbed `getContext` (no-op 2d context) — we test the
  data/model layer, not pixels.
- **No backend tests** — Live adds no Rust; `/api/ops/status` is already covered by the
  Ops integration tests.

## Placement, workspace, deps

- Add to `tools/weir-console/static/`: `live.html`, `live.js`, `live.test.mjs`; activate
  the `Live` nav link in `index.html` and `ops.html`. Update the README with the Live
  view. **No `Cargo.toml`/dependency/Rust changes**; the published workspace, its
  lockfile, and CI are unaffected.

## Out of scope (this spec)

- Per-record streaming / an event feed (the daemon exposes no event stream).
- A true per-second drain/sink rate (the metrics summary doesn't expose one; the
  drain→sink stage uses queue depth + sink health).
- Any daemon control, or a `/metrics` raw-exposition parser in the console.
- The standalone `demo/` simulation (a separate artifact). A Live screenshot/recording
  for the demo site is the same noted, non-blocking showcase follow-up.

## Success criteria

- `cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <dir>
  --metrics-addr <daemon /metrics>` serves the **Live** tab; against a running daemon the
  pipeline animates at the real accepted/ack rate with live counters + sparklines, and
  against no daemon it shows a clean "waiting" state and recovers when the daemon returns.
- Rates are computed from deltas with a reset guard (no negative rates on restart).
- The frontend smoke test passes; `cargo fmt`/`clippy --all-targets -- -D warnings` stay
  clean (no Rust changed) and the existing tool tests (5 wab + 13 ops) + node smokes still
  pass; the main workspace + its lockfile/CI are unaffected.
