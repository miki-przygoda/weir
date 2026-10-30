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
| `index.html` | **Landing + live simulation.** The pitch, the interactive Naive-vs-weir pipeline sim, "why weir", how the guarantee is tested, and a crate strip. |
| `crates.html` | **The crates.** Which-crate-do-I-need table, a dependency diagram, and a card per crate (when to reach for it, deps, platform). |
| `examples.html` | **Example projects by crate-ratio.** A crate-usage matrix + curated recipes spanning one crate → the full pipeline → a zero-Rust-dep wire client. |
| `clients/<lang>.html` | **Polyglot wire-client subpages** — one per language (`py`, `go`, `c`, `java`, `ts`). Each is a from-scratch, stdlib-only producer built from the wire spec + conformance vectors. Linked from `examples.html`. |
| `weir.css` | Shared theme (palette, components) loaded by every page above. |
| `version.js` | Sets the version shown in the page banners. Generated from `Cargo.toml` by `scripts/sync-demo-version.sh`; CI fails if it drifts, so do not edit it by hand. |

The subpages are a **pitch / onboarding layer** — they link out to the canonical,
versioned [docs site](https://miki-przygoda.github.io/weir) rather than
duplicating it.

## The simulation (index.html)

A live, animated model of weir's pipeline beside a naive "synchronous insert per
record" baseline:

- **Push records** (one, ten, or a stream) and watch tokens flow
  Producer → Socket → WAB → fsync → Ack, then drain in **batches** to the sink.
- **Toggle the durability tier** (Durable / Buffered) and see the effect
  on ack latency and on what survives a crash.
- **Kill the process** or **pull the plug** mid-flight, then **Restart** to
  watch unconfirmed WAB segments replay. The two crashes differ on purpose: a
  killed process loses no acked record at either tier, because the OS still
  holds every write; a power cut loses acked `Buffered` records that were not
  yet fsynced, and no acked `Durable` record. That difference is the contract.
- **Live metric cards** compare producer-facing ack latency, downstream DB
  commits (the N→1 compression), and records lost on crash.

It's a *simulation*, not the daemon (no Unix-socket daemon in a browser), but the
model follows weir's real semantics.

Latency figures come from the operator-run bare-metal capture
(`docs/benchmarks/bare-metal.md`: i9-9900K, SATA SSD, Linux `fdatasync`), **not**
from CI. `docs/benchmarks/environments.md` forbids citing CI runs for an
external claim, and this bundle is the most external artifact in the tree. A
`Durable` ack is one fsync, so it is the disk's number: p50 1.1 ms there.
`Buffered` is 19 µs on the same box. The Mac figures are deliberately not used:
macOS uses `F_BARRIERFSYNC`, a write barrier rather than a full flush, and is not
power-loss safe.

The naive baseline (~8 ms, one commit per record) is **modelled**, not measured:
a synchronous remote insert + commit round-trip. It is not a weir benchmark
figure — adjust it mentally for your own database.

## Hosting / integration

The bundle is self-contained — relative links between the pages (including the
`clients/<lang>.html` subpages), one shared stylesheet, and all JS inline except the small `version.js`. Drop the
whole `demo/` folder onto any static host.

For the **Next.js personal site**, follow the existing static-demo precedent
(`public/demo/<project>/`): copy this folder to `public/demo/weir/`, then link it
from the weir project page (or embed the simulation via an `<iframe>`):

```html
<iframe src="/demo/weir/index.html" width="100%" height="1400" style="border:0"></iframe>
```

There are no absolute asset paths to fix up; only the cross-page links
(`crates.html`, `examples.html`, the `clients/<lang>.html` subpages),
`weir.css` and `version.js`, all relative.
