# weir interactive demo

`index.html` is a single, self-contained, browser-only demo of weir. No build
step, no dependencies, no network — open it and it runs.

## What it shows

A live, animated simulation of weir's pipeline side by side with a naive
"synchronous insert per record" baseline:

- **Push records** (one, ten, or a continuous stream) and watch tokens flow
  Producer → Socket → WAB → fsync → Ack, then drain in **batches** to the sink.
- **Toggle the durability tier** (Sync / Batched / Buffered) and see the effect
  on ack latency and on what survives a crash.
- **Crash the daemon** mid-flight and **Restart** it to watch unconfirmed WAB
  segments replay — the visual proof of *"an ack is never a false ack."*
- **Live metric cards** compare producer-facing ack latency, downstream DB
  commits (the N→1 compression), and records lost on crash, naive vs weir.

## Faithfulness

It is a *simulation*, not the real daemon (you can't run a Unix-socket daemon
in a browser), but the model follows weir's real semantics:

- **Sync** — fsync before the ack; durable before the producer is told "ok".
- **Batched** — written before the ack, one group fsync per batch; durable
  before the ack, fewer fsyncs (the throughput sweet spot).
- **Buffered** — ack after the in-memory enqueue only; fastest, but the demo
  honestly shows the not-yet-fsynced window as lost on crash.
- **Drain** commits a whole sealed segment in one statement (the SQL sinks do
  N rows → 1 `INSERT`); segments are reclaimed only after the sink confirms.
- **Recovery** replays sealed-but-unconfirmed segments on restart.

Latency figures are rounded from the project's CI benchmarks (Sync/Batched
≈ 0.39 ms ack, Buffered ≈ 0.07 ms); the naive baseline models a synchronous
insert + commit + network round-trip (~8 ms), one commit per record.

## Hosting / embedding

The file is fully standalone — copy `index.html` anywhere (a personal site, a
static host, GitHub Pages) and serve it as-is, or open it directly with
`file://`. To embed it in an existing page, drop it in an `<iframe>`:

```html
<iframe src="index.html" width="100%" height="1200" style="border:0"></iframe>
```

All styles and scripts are inline, so there are no asset paths to fix up.
