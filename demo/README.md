# weir demo bundle

A small, dependency-free static site for weir — a sell landing page built around
a live simulation, plus subpages for the crates and example projects. No build
step, no framework, no network beyond a web font: serve the `demo/` folder (or
open the files with `file://`) and it runs.

Styled to a **CGA / terminal-TUI aesthetic** (black + cyan/yellow/green,
JetBrains Mono) to sit natively inside the host personal site.

## Pages

| File | What it is |
|------|------------|
| `index.html` | **Landing + live simulation.** The pitch, the interactive Naive-vs-weir pipeline sim, "why weir", and a crate strip. |
| `crates.html` | **The crates.** Which-crate-do-I-need table, a dependency diagram, and a card per crate (when to reach for it, deps, platform). |
| `examples.html` | **Example projects by crate-ratio.** A crate-usage matrix + curated recipes spanning one crate → the full pipeline → a zero-Rust-dep wire client. |
| `weir.css` | Shared theme (palette, components) used by all three pages. |

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
project's CI benchmarks (Sync/Batched ≈ 0.39 ms ack, Buffered ≈ 0.07 ms); the
naive baseline models a synchronous insert + commit round-trip (~8 ms), one
commit per record.

## Hosting / integration

The bundle is self-contained — relative links between the three pages, one shared
stylesheet, all JS inline. Drop the whole `demo/` folder onto any static host.

For the **Next.js personal site**, follow the existing static-demo precedent
(`public/demo/<project>/`): copy this folder to `public/demo/weir/`, then link it
from the weir project page (or embed the simulation via an `<iframe>`):

```html
<iframe src="/demo/weir/index.html" width="100%" height="1400" style="border:0"></iframe>
```

There are no absolute asset paths to fix up; only the cross-page links
(`crates.html`, `examples.html`) and `weir.css`, all relative.
