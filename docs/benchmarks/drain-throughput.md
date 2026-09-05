# Drain throughput — the first delivery-side numbers

**Date:** 2026-09-05 (Linux added) · **Version:** 2.0.5 · **Suite:** `crates/weir-server/tests/load_drain.rs`

Until this release every published weir number described **ingest**: how fast the
daemon accepts records and gets them into the WAB. The load suite's own module
doc said so, under "Coverage caveats", and named this file's suite as the
follow-up.

That gap mattered because weir is a *buffer*. Ingest throughput says how fast the
buffer fills. Nothing said how fast it empties, which is the half that decides
whether it ever does.

These are the first delivery numbers the project has.

## Environments

Three configurations, five consecutive runs each. The point of measuring more
than one is that the differences between them are larger than anything within
them — see [What the storage does](#what-the-storage-does).

| | **mac** | **linux-ssd** | **linux-tmpfs** |
|---|---|---|---|
| Machine | Apple M3 Max, 16 cores | 4-core x86_64 | same box |
| OS | macOS 26.6.2 | Ubuntu 24.04, kernel 7.0.0-30 | same |
| Toolchain | rustc 1.97.1 | cargo 1.94.0 | same |
| WAB storage | APFS, internal SSD | ext4 on Samsung 850 EVO (SATA) | tmpfs (`/dev/shm`) |
| `sync_all` becomes | `F_FULLFSYNC` | `fdatasync` to a SATA SSD | a memory write |
| Result line reports | `"wab":"default"` | `"wab":"default"` | `"wab":"external"` |

Common to all three:

| | |
|---|---|
| Sink | in-process HTTP/1.1 mock on loopback, answers in microseconds |
| Records | 256 bytes, `Durability::Buffered` |
| Segments | 64 KiB (`wab_segment_max_bytes`), 1 s idle seal |
| Daemon | `bench_preset`: 4 shards, 4 workers, ingest batch 64 |
| Sharding | **Inert in this suite.** Shard is assigned per *connection* (`crates/weir-server/src/socket/mod.rs:210`) and the fill opens one client (`tests/load_drain.rs:374-379`), so every record lands on one shard whatever `shard_count` says. |
| Sink batch size | `sink_max_batch_size` = 100, the default (`crates/weir-server/src/config/mod.rs:721`), never overridden here. The "batch 64" above is the *ingest* coalescing knob — a different setting. |

## Results

**These supersede the figures first published on 2026-09-02, which were between
3.4x and 5.4x too low.** They were not measuring the drain. Two harness faults,
both since fixed:

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

A third, found by running on Linux for the first time: **`WEIR_BENCH_WAB_DIR`
had never worked.** The knob this file told readers to use to separate drain
cost from filesystem cost panicked every scenario before a record was pushed —
`WeirServerBuilder::wab_dir` requires the caller to create the directory and
`load_drain.rs` did not. The `linux-tmpfs` column below is the first data it has
ever produced.

Median of five consecutive runs, with the full range:

| Scenario | | min | **median** | max | spread |
|---|---|---:|---:|---:|---|
| `drain_http_ndjson` | mac | 36,695 | **40,710** | 42,412 | 1.16x |
| | linux-ssd | 105,332 | **105,909** | 118,819 | 1.13x |
| | linux-tmpfs | 237,225 | **238,573** | 315,527 | 1.33x |
| `drain_http_per_record` | mac | 21,428 | **24,381** | 27,782 | 1.30x |
| | linux-ssd | 25,652 | **28,719** | 30,675 | 1.20x |
| | linux-tmpfs | 36,596 | **37,991** | 38,001 | 1.04x |
| `drain_slow_sink_1ms_conc16` | mac | 4,732 | **4,977** | 5,436 | 1.15x |
| | linux-ssd | 8,659 | **8,826** | 9,517 | 1.10x |
| | linux-tmpfs | 7,430 | **7,570** | 11,032 | 1.48x |

All figures rec/s. Ingest of the 2,000-record backlog, for scale: **176 ms** on
mac, **77 ms** linux-ssd, **41 ms** linux-tmpfs.

Within-configuration spread is 1.04-1.48x. **Between configurations it reaches
6.3x on the same scenario.** Any single headline number for this suite is a
number about a machine.

## What the storage does

The `sync_all` on the confirm path is the whole story, and it was worth two
separate measurements to see it.

| Scenario | linux-ssd ÷ mac | linux-tmpfs ÷ linux-ssd |
|---|---:|---:|
| `drain_http_ndjson` | **2.60x** | **2.25x** |
| `drain_http_per_record` | 1.18x | 1.32x |
| `drain_slow_sink_1ms_conc16` | 1.77x | 0.86x |

**A 4-core SATA-SSD Linux box beats a 16-core M3 Max on every scenario**, by
2.60x on NDJSON. This is the predicted result and it confirms the diagnosis:
macOS `sync_all` is `F_FULLFSYNC`, measured at 3,965 µs against 238 µs for the
`F_BARRIERFSYNC` used for record data. It is also why a 2-vCPU CI runner
previously beat an M3 Max here. **Nothing about the drain is slower on Apple
silicon; the confirm is.**

**Storage still dominates NDJSON on Linux.** tmpfs is a further 2.25x, so even
the ext4 figure is mostly a storage number. The drain's own ceiling, with the
filesystem taken out, is **~238,000 rec/s**.

**Per-record mode barely notices any of it** — 1.18x from macOS to Linux, 1.32x
from SATA to RAM. It is bounded by per-request HTTP cost, not by the WAB. That
is a genuinely useful separation: the two scenarios are measuring different
bottlenecks, and only one of them is the buffer.

**`drain_slow_sink` shows no storage sensitivity at all.** Its tmpfs range
(7,430-11,032) *contains* its ext4 range (8,659-9,517); the 0.86x median ratio
is inside that overlap and should not be read as tmpfs being slower. Bounded by
the 1 ms sink delay and concurrency, exactly as the scenario intends. It is the
only one of the three currently measuring what its name claims.

## What changed in the conclusions

**"Delivery is the narrower half" stays withdrawn, and is now clearly false.**
It rested on ~6,000 rec/s against an ingest figure measured differently. The
drain sustains 105,909 rec/s NDJSON on a modest Linux box — several times a
single client's ~32,000 `Buffered` and ~5,700 `Durable`. Delivery is the
*wider* half on every configuration measured. Any argument that prioritised
work on the grounds that delivery constrains the system needs remaking from
scratch.

**NDJSON's advantage is not a fixed ratio.** It is 1.67x on mac, **3.69x** on
linux-ssd, **6.28x** on linux-tmpfs. It grows as the confirm gets cheaper,
because batching amortises the per-batch confirm as much as the per-request
network cost. Publishing one number for it — as this file previously did, first
1.25x then 1.67x — was wrong in kind, not just in value.

**Concurrency at a 1 ms sink gives 8.8x on Linux**, against 5.0x on mac and a
~1,000 rec/s serial cap. Closer to the concurrency setting of 16 than the mac
figure suggested, still sub-linear.

**The NDJSON window is now very short.** 1,000 delivered records in ~4 ms on
tmpfs. `delivered_rps` is computed from `Duration::as_secs_f64()`, so this is
not timer quantisation — four of five runs agreed within 1.1% and one landed
32% high. But a window that short is one scheduling event away from an outlier,
and the record count was sized when `F_FULLFSYNC` made everything slow. **The
fill should scale with the platform before these numbers are trended.**

## Still not measured

- **NVMe, and a spinning disk.** The Linux box has both unmounted; only the
  SATA SSD and tmpfs were measured. NVMe should land between them and would say
  whether the ext4 figure generalises or is specific to SATA.
- **A record count sized for the platform.** 2,000 records was chosen when the
  fastest scenario took 25 ms; on tmpfs it takes 4. Trending these numbers
  before fixing that would trend scheduling noise.
- **A real sink over a real network.** The mock answers in microseconds on
  loopback. Every ratio above should be re-derived at a realistic RTT.
- **Concurrent ingest and drain.** Every scenario fills, then drains. A buffer
  under sustained matched load is the case operators actually run.

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
# What a real deployment sees: WAB on the default filesystem.
cargo test -p weir-server --test load_drain --release -- --nocapture

# The drain with the filesystem taken out. Any tmpfs/RAM-disk path works;
# the suite creates a per-scenario subdirectory under it.
WEIR_BENCH_WAB_DIR=/dev/shm/weirbench \
  cargo test -p weir-server --test load_drain --release -- --nocapture
```

Each scenario prints one `BENCH: {json}` line, carrying `"wab":"default"` or
`"wab":"external"` so the two are never averaged together. The `drain` CI job
runs the first form only.
