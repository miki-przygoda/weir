# weir demo bundle

A small, dependency-free static site for weir — a sell landing page built around
a live simulation, plus subpages for the crates and example projects. No build
step, no framework, no network beyond a web font: serve the `demo/` folder (or
open the files with `file://`) and it runs.

Styled to the shared **"hr2" palette** (dark neutral `#141414` background with
sky `#38bdf8`, violet `#a78bfa`, green `#4ade80`, and rose `#f87171` accents;
**Inter** body text with **JetBrains Mono** for labels/mono accents) to sit
natively inside the host personal site.

## Pages

| File | What it is |
|------|------------|
| `index.html` | **Landing + live simulation.** The pitch, the interactive Naive-vs-weir pipeline sim, "why weir", and a crate strip. |
| `crates.html` | **The crates.** Which-crate-do-I-need table, a dependency diagram, and a card per crate (when to reach for it, deps, platform). |
| `examples.html` | **Example projects by crate-ratio.** A crate-usage matrix + curated recipes spanning one crate → the full pipeline → a zero-Rust-dep wire client. |
| `clients/<lang>.html` | **Polyglot wire-client subpages** — one per language (`py`, `go`, `c`, `java`, `ts`). Each is a from-scratch, stdlib-only producer built from the wire spec + conformance vectors. Linked from `examples.html`. |
| `console/{index,ops,live}.html` | **Interactive `weir-console` demo** — the Explorer / Ops / Live views running on canned data via `mock.js` (no backend). Linked from the nav + the `examples.html` admin row. See below. |
| `weir.css` | Shared theme (palette, components) loaded by every page above. |

The subpages are a **pitch / onboarding layer** — they link out to the canonical,
versioned [docs site](https://miki-przygoda.github.io/weir) rather than
duplicating it.

## The simulation (index.html)

A live, animated model of weir's pipeline beside a naive "synchronous insert per
record" baseline:

- **Push records** (one, ten, or a stream) and watch tokens flow
  Producer → Socket → WAB → fsync → Ack, then drain in **batches** to the sink.
- **Toggle the durability tier** (Sync / Batched / Buffered) and see the effect
  on ack latency and on what survives a crash.
- **Crash the daemon** mid-flight and **Restart** to watch unconfirmed WAB
  segments replay — the visual proof of *"an ack is never a false ack."*
- **Live metric cards** compare producer-facing ack latency, downstream DB
  commits (the N→1 compression), and records lost on crash.

It's a *simulation*, not the daemon (no Unix-socket daemon in a browser), but the
model follows weir's real semantics. Latency figures are rounded from the
project's CI benchmarks (Sync/Batched ≈ 0.36 ms ack, Buffered ≈ 0.07 ms); the
naive baseline models a synchronous insert + commit round-trip (~8 ms), one
commit per record.

## Hosting / integration

The bundle is self-contained — relative links between the pages (including the
`clients/<lang>.html` subpages), one shared stylesheet, all JS inline. Drop the
whole `demo/` folder onto any static host.

For the **Next.js personal site**, follow the existing static-demo precedent
(`public/demo/<project>/`): copy this folder to `public/demo/weir/`, then link it
from the weir project page (or embed the simulation via an `<iframe>`):

```html
<iframe src="/demo/weir/index.html" width="100%" height="1400" style="border:0"></iframe>
```

There are no absolute asset paths to fix up; only the cross-page links
(`crates.html`, `examples.html`, the `clients/<lang>.html` subpages) and
`weir.css`, all relative.

## console/

`console/` is an **interactive, no-backend mock** of `weir-console` (the local tool under
`tools/weir-console/`): the three real views — **Explorer**, **Ops**, **Live** — running
on canned sample data via `mock.js` (a `fetch` shim). The `*.js` view files are committed
copies of `tools/weir-console/static/*.js`; `mock.js` serves JSON in the real `/api/*`
shapes — including a deliberately-corrupt segment (so the Explorer shows its integrity
badge + an error row), a mutable dead-letter store (so Ops Drop/Requeue visibly empty it),
and an incrementing counter (so the Live pipeline animates and the sparklines fill). For
the real tool, run `cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <dir>`.
