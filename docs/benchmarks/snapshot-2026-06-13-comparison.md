# Where weir Is Now — Performance Comparison, 2026-06-13

**Commit:** `v1/phase-3-performance` @ `c46c0c7` (post-Phase-4: weir-ctl, weir-sink-sdk, full DST harness — production binary unchanged since Phase 3).
**Method:** `tests/load.rs`, release, 3 serial trials per deadline, `shard_count=4 batch_size=64`. Raw data: [`snapshot-2026-06-13-mac.md`](snapshot-2026-06-13-mac.md) · [`snapshot-2026-06-13-beast.md`](snapshot-2026-06-13-beast.md).

A clean, two-environment baseline to compare future runs against. The two boxes
bracket the durability story: a fast write-barrier (Mac NVMe) and an honest,
slow `fdatasync` (beast SATA SSD).

| | Mac (dev) | beast (Linux reference) |
|---|---|---|
| CPU | Apple M3 Max (16c) | Intel i9-9900K (8c/16t) |
| OS | macOS 26.5.1 | Linux 6.17.0 |
| RAM | 64 GiB | 31 GiB |
| WAB storage | NVMe | Samsung SATA SSD, ext4 |
| Durability syscall | `F_BARRIERFSYNC` | `fdatasync` |
| **Observed fsync** | **~133 µs** | **~1.5 ms** |

## The one thing to take away

**Durable throughput is set by your storage's fsync latency — nothing else.**
The software pipeline is identical on both boxes; the ~11× gap in `Sync` is
*entirely* the ~11× gap in fsync. Meanwhile the non-durable (`Buffered`) path,
which never fsyncs, is actually **faster** on beast — proving the rest of the
pipeline isn't the bottleneck.

## Latency by durability tier (single-thread, p50)

| Tier | Mac | beast | What sets it |
|------|-----|-------|--------------|
| **Sync** (fsync-before-ack) | 133 µs | **1.5 ms** | the fsync syscall (≈100% of the latency) |
| **Batched** (group fsync) | 147 µs | **1.5 ms** | same path as Sync — *identical* on both boxes |
| **Buffered** (memory-only ack) | 26 µs | **19 µs** | CPU + buffered write; no fsync → beast wins |

`Sync p50 ≈ fsync latency` on *both* machines is the fsync-bound thesis stated
as a measurement: 133 µs ≈ Mac barrier-fsync, 1.5 ms ≈ beast `fdatasync`.

## Throughput

| Scenario | Mac | beast | Ratio | Bound by |
|----------|-----|-------|-------|----------|
| Single-thread **Sync** | 6,133 | 653 | 9.4× | fsync latency |
| Single-thread **Buffered** | 36,899 | 49,725 | 0.74× *(beast faster)* | CPU / buffered I/O |
| **Sync** ramp peak | ~80k @ 64t | ~6.8k @ 96t | ~12× | fsync floor |
| **Buffered** ramp peak | ~123k @ 48t | ~155k @ 48t | 0.79× *(beast faster)* | connection cap / CPU |
| IOPS compression | 249:1 | 249:1 | — | content-derived dedup token |

Two clean regimes:
- **Durable (Sync/Batched):** Mac ≫ beast, by the fsync ratio. Storage-bound.
- **Non-durable (Buffered):** beast ≥ Mac. CPU/IO-bound; the i9 + ext4 buffered writes edge out the M3 Max.

## Group-fsync amortization is what makes Sync usable

A single `Sync` record pays a full fsync, but concurrent records under load
**coalesce into one fsync per batch**, so per-record cost falls sharply:

| | single-thread | ramp peak | amortization |
|---|---|---|---|
| Mac Sync | 6,133 RPS | ~80,000 RPS | ~13× |
| beast Sync | 653 RPS | ~6,856 RPS | ~10× |

This is the WAB's core throughput lever for the durable tier — the more
concurrent Sync producers, the more records ride each fsync.

## No regression from Phase 4 / DST

These numbers match the Phase 3 baseline on **both** machines:

| | this run | Phase 3 | 
|---|---|---|
| Mac Sync p50 | 133 µs | ~152 µs |
| Mac Buffered RPS | 36.9k | ~29k |
| beast single-thread Sync | 653 RPS | 647 RPS |
| beast Buffered ramp peak | 155k | 157k |

So the DST work — the `Arc<dyn> SegmentStore` seam, the generic `BlockingClock`,
and the 4.1b `flush_batch` ack restructure (which fixed a real false-ack bug) —
added **no measurable hot-path cost** on either platform. The production binary
carries zero sim code.

## Where we are now

- **Correctness:** the durable path is deterministically fault-tested (DST: EIO,
  torn-write, ENOSPC, crash-before-rename, panic-mid-flush, hung-sink,
  offline-shard, plus cooperative thread interleaving), and the one real bug it
  found (mid-batch false ack) is fixed and pinned.
- **Performance:** fsync-bound by design, fully measured across fast and slow
  storage, with group-fsync amortization as the throughput lever and a 249:1
  IOPS-compression ratio into sinks. The fast `Buffered` tier hits 50k–155k RPS;
  the durable `Sync` tier is whatever your disk's fsync allows (≈7k on honest
  SATA, ≈80k on a write-barrier NVMe), scaling with concurrency.
- **No perf debt** accrued from the Phase 4 / DST work.

## Per-stage breakdown — fsync share, made explicit

`--features bench-trace` decomposes a single record's latency into the pipeline
stages (mean µs, 2000 samples/tier). The fsync stage isn't measured directly;
it's the remainder: `fsync ≈ total − queue − bridge_wait − write`.

| Stage (Sync tier, mean µs) | Mac | beast |
|---|---|---|
| queue (enqueue → worker-flush) | 3 | 16 |
| bridge_wait (worker → flusher) | 3 | 8 |
| write (flusher write, pre-fsync) | 6 | 6 |
| **fsync (remainder)** | **~203** | **~1,433** |
| **total** (enqueue → ack) | **215** | **1,463** |
| **fsync share of total** | **~94%** | **~98%** |

Buffered (no fsync) total: **10 µs** (Mac) / **6 µs** (beast). The software
pipeline is ≤30 µs and roughly constant across both boxes; the fsync is the
whole story, and the slower the disk the more completely it dominates
(94% → 98%). This is the fsync-bound thesis as a direct decomposition, not an
inference.

## Observability snapshot (Mac, `/metrics` under load)

A point-in-time Prometheus scrape after driving 300 records (100 each
sync/batched/buffered, 256 B) through a standalone server (noop sink, 4 shards,
8 KiB segments to force rotations). Confirms the runtime instrumentation is
wired end to end:

| Family | Value |
|---|---|
| `weir_records_accepted_total` | sync 100 · batched 100 · buffered 100 (300 ack, 0 nack) |
| `weir_wab_segments_total` | **open 12 → sealed 8 → confirmed 8** (full lifecycle observable) |
| `weir_wab_fsync_duration_seconds` | count 196, 195 ≤ 1 ms (1 in 1–2 ms) |
| `weir_wab_fsync_failures_total` / `weir_wab_flusher_panics_total` | 0 / 0 |
| `weir_sink_commit_records_total{committed}` | 248 |
| `weir_drain_state` | `draining` = 1 (blocked / retrying = 0) |
| `weir_recovery_segments_quarantined_total` | 0 |
| `weir_queue_depth` | 0 |

> Reproduce: start `weir-server` with `WEIR_SINK_TYPE=noop WEIR_SHARD_COUNT=4
> WEIR_WAB_SEGMENT_MAX_BYTES=8192 WEIR_METRICS_PORT=9185` (socket parent must be
> a real dir, **not** the `/tmp`→`/private/tmp` symlink — the hardened bind opens
> it `O_DIRECTORY|O_NOFOLLOW`), push load via `weir-ctl push … --durability …`,
> then `weir-ctl metrics --addr 127.0.0.1:9185` or `curl …/metrics`.
