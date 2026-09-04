# Drain throughput — the first delivery-side numbers

**Date:** 2026-09-03 (re-measured; supersedes the 2026-09-02 figures) · **Version:** 2.0.4 · **Suite:** `crates/weir-server/tests/load_drain.rs`

Until this release every published weir number described **ingest**: how fast the
daemon accepts records and gets them into the WAB. The load suite's own module
doc said so, under "Coverage caveats", and named this file's suite as the
follow-up.

That gap mattered because weir is a *buffer*. Ingest throughput says how fast the
buffer fills. Nothing said how fast it empties, which is the half that decides
whether it ever does.

These are the first delivery numbers the project has.

## Environment

| | |
|---|---|
| Machine | Apple M3 Max, 16 cores |
| OS | macOS 26.6.2 |
| Toolchain | rustc 1.97.1, `--release` |
| Sink | in-process HTTP/1.1 mock on loopback, answers in microseconds |
| Records | 256 bytes, `Durability::Buffered` |
| Segments | 64 KiB (`wab_segment_max_bytes`), 1 s idle seal |
| Daemon | `bench_preset`: 4 shards, 4 workers, ingest batch 64 |
| Sharding | **Inert in this suite.** Shard is assigned per *connection* (`crates/weir-server/src/socket/mod.rs:210`) and the fill opens one client (`tests/load_drain.rs:374-379`), so every record lands on one shard whatever `shard_count` says. |
| Sink batch size | `sink_max_batch_size` = 100, the default (`crates/weir-server/src/config/mod.rs:721`), never overridden here. The "batch 64" above is the *ingest* coalescing knob — a different setting. |

Numbers below are the **median of three consecutive runs**. The spread is wider
than a single figure suggests: per-record ranged 5,695–6,674 around its 5,961
median, i.e. **+12.0% / −4.5%** — an earlier version of this file claimed "under
±8%", which its own table contradicts.

> **Treat these as a first measurement, not a baseline.** A later investigation
> reproducing this suite saw a **2.6–4.1× spread across five consecutive runs**,
> and found the window is dominated by macOS `F_FULLFSYNC` on the confirm path
> plus a slice of the drain's own retry timer rather than by delivery work:
> moving the WAB to a RAM disk raised per-record from 6,548 to 20,233 rec/s, and
> shortening the retry base delay reached six figures. The methodology needs
> fixing before these numbers carry weight — see
> `.workdocs/explorations/2026-09-02/drain-architecture.md`.

## Results

**These supersede the figures first published on 2026-09-02, which were between
3.4x and 5.4x too low.** They were not measuring the drain. Two harness faults,
both now fixed:

1. **The clock started at the wrong moment.** Timing began when the mock sink was
   flipped healthy — but the drain was asleep in its retry backoff, and woke
   8-38 ms later. That sleep sat inside a window of roughly the same size, and it
   varied run to run. It is now excluded: the window opens at the *first
   delivered record*.
2. **`bulk()` asked for more than was sealed.** A 256-byte record costs ~270
   bytes framed, so a 64 KiB segment holds ~242 and only whole rotated segments
   seal. At 1,000 records that is ~726 sealed, but the wait asked for 750 — so it
   blocked on the 1 s idle-seal timer. `drain_slow_sink` was returning
   1,004/1,010/1,010 ms: three digits of timer, not of drain.

Median of five consecutive runs, with the full range, on the environment above:

| Scenario | min | **median** | max | spread |
|---|---:|---:|---:|---|
| `drain_http_ndjson` | 36,695 | **40,710 rec/s** | 42,412 | 1.16x |
| `drain_http_per_record` | 21,428 | **24,381 rec/s** | 27,782 | 1.30x |
| `drain_slow_sink_1ms_conc16` | 4,732 | **4,977 rec/s** | 5,436 | 1.15x |

The spread is the result that matters. It was **2.6-4.1x** across five runs
before; it is now **1.15-1.30x**. A number whose run-to-run range exceeds any
regression threshold you would set against it is not a measurement, and the
earlier figures were in that state.

## What changed in the conclusions

**"Delivery is the narrower half" is withdrawn.** It rested on ~6,000 rec/s
against an ingest figure measured differently. The drain sustains ~40,700 rec/s
NDJSON on this machine — comfortably above a single client's ~32,000 `Buffered`
and ~5,700 `Durable`. Whether delivery *ever* becomes the constraint is now an
open question rather than a settled one, and it needs measuring against a real
sink over a real network, not a loopback mock.

**NDJSON's advantage is ~1.67x here** (40,710 vs 24,381), larger than the 1.25x
first reported — but still not the order of magnitude the feature's framing
implies, and still measured against a mock with almost no per-request cost.

**Concurrency at a 1 ms sink gives ~5x**, not the ~16x its concurrency setting
might suggest. Serial delivery would cap at ~1,000 rec/s; 4,977 is real overlap,
sub-linear.

## Still not measured

- **Linux, and bare metal.** Everything here is macOS. The confirm path calls
  `sync_all`, which is `F_FULLFSYNC` on macOS — measured at 3,965 us against
  238 us for the `F_BARRIERFSYNC` used for record data. A Linux run is the single
  most valuable follow-up, and is why a 2-vCPU CI runner previously beat an
  M3 Max on every scenario. **Pending: a run on bare-metal Linux.**
- **A real sink over a real network.** The mock answers in microseconds on
  loopback. Every ratio above should be re-derived at a realistic RTT.
- **The suite's own storage sensitivity.** Set `WEIR_BENCH_WAB_DIR` to a
  tmpfs/RAM-disk path to separate drain cost from filesystem cost; every result
  line records which was used under `"wab"`.

## What these numbers are not

- **Loopback only.** No network latency, no TLS to the sink, no DNS. A real HTTP
  sink over a WAN pays an RTT per batch that dominates everything here.
- **One sink type.** HTTP only. The SQL sinks batch very differently.
- **Not steady state.** Each scenario fills, then drains. Behaviour where ingest
  and drain run concurrently at matched rates is still unmeasured.
- **Not a gate.** The suite asserts correctness — including that no record is
  lost between the WAB and the sink across an outage — but the rates are reported
  for tracking, not enforced. Failing a throughput threshold on shared CI
  hardware produces flakes, not signal.
- **Not yet trended.** `deploy/avg_benchmarks.py` renders only the
  deadline-suffixed *ingest* scenarios, so these numbers reach neither
  `latest.md` nor `history.md`. The `drain` CI job retains its JSONL as a
  30-day build artifact, which preserves the raw data but does not plot a trend
  — **a gradual delivery regression would currently go unnoticed.** Closing it
  means teaching the renderer about delivery scenarios (which have no deadline
  suffix and report `delivered_rps`, not `throughput_rps`); that touches the
  script writing main's committed baselines, so it is deliberately a separate
  change rather than a rider on this one.

## Reproducing

```sh
cargo test -p weir-server --test load_drain --release -- --nocapture
```

Each scenario prints one `BENCH: {json}` line. The `drain` CI job runs exactly
this.
