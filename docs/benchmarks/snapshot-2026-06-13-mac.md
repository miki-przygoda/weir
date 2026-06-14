# Benchmark Snapshot — 2026-06-13 (Mac dev box)

**Machine:** Apple M3 Max (Mac15,8), 16 cores, 64 GB · macOS 26.5.1 · NVMe (`F_BARRIERFSYNC`)
**Branch / commit:** `v1/phase-3-performance` @ `f13748d` (post-Phase-4: weir-ctl, weir-sink-sdk, full DST harness)
**Server config:** `shard_count=4`, `batch_size=64` (the load suite's `bench_preset`)
**Method:** `tests/load.rs`, release build, **3 serial trials per deadline** (`--test-threads=1`, averaged), at `batch_deadline` 1 ms and 2 ms. Rendered with `deploy/avg_benchmarks.py`.

> A point-in-time, single-machine snapshot for tracking regressions over time.
> Compare **like-for-like**: this is the Mac dev box (fast `F_BARRIERFSYNC`).
> The CI baseline (`latest.md`) is a Linux GitHub runner; `phase3-results.md`
> has the slow-storage reference (beast, SATA SSD, `fdatasync` ~1.4 ms). New
> snapshots: `docs/benchmarks/snapshot-YYYY-MM-DD-<machine>.md`.

## TL;DR

- **The durable path is fsync-bound** (the Phase 3 finding still holds). A single `Sync` record is ~133 µs p50 on this NVMe — almost all of it `F_BARRIERFSYNC`. `Batched` is statistically identical (shared group-fsync code path). `Buffered` (memory-only ack) is the fast path at ~26 µs p50.
- **Group-fsync amortization scales the Sync tier under load:** single-thread Sync is ~6.1 k RPS, but the Sync saturation ramp reaches **~80 k RPS @ 64 threads** as concurrent records coalesce under one fsync.
- **Buffered peaks at ~123 k RPS @ 48 threads** (the connection cap), holding flat through saturation.
- **IOPS compression 249:1** — 4,980 records committed in 20 sink commits (the WAB's batch-to-sink amortization).
- **No perf regression from the Phase 4 / DST work.** These numbers match or slightly beat the Phase 3 Mac baseline, so the injectable seams (`Arc<dyn> SegmentStore`, generic `BlockingClock`) and the 4.1b `flush_batch` ack restructure cost nothing measurable on the hot path. (The production binary contains zero sim code.)

## Throughput — deadline comparison

| Scenario | RPS (1ms) | ±σ (1ms) | RPS (2ms) | ±σ (2ms) | d1ms/d2ms |
|----------|---------|------- | ---------|------- | ---------|
| single_thread_buffered | 36,899 | ±583 | 36,201 | ±249 | **1.02×** |
| single_thread_sync | 6,133 | ±110 | 6,281 | ±167 | **0.98×** |
| thundering_herd_8_threads | 9,285 | ±266 | 12,258 | ±1,924 | **0.76×** |
| thundering_herd_32_threads | 50,169 | ±1,663 | 51,119 | ±1,228 | **0.98×** |
| thundering_herd_64_threads | 87,178 | ±3,819 | 91,605 | ±4,151 | **0.95×** |
| connection_churn | 9,461 | ±830 | 6,980 | ±936 | **1.36×** |
| fire_and_forget_overload | 3,894 | ±104 | 3,838 | ±136 | **1.01×** |

## Latency — single thread, `batch_deadline_ms=1`

| Metric | Sync | Batched | Buffered |
|--------|------- | ------- | -------|
| Min | 110 µs | 116 µs | 18 µs |
| Mean | 180 µs | 199 µs | 26 µs |
| σ | 217 µs | 225 µs | 5 µs |
| p50 | 133 µs | 147 µs | 26 µs |
| p75 | 148 µs | 173 µs | 27 µs |
| p95 | 421 µs | 362 µs | 32 µs |
| p99 | 1.6 ms | 1.7 ms | 41 µs |
| p99.9 | 2.4 ms | 2.1 ms | 115 µs |
| Max | 3.1 ms | 3.5 ms | 167 µs |

## Latency — single thread, `batch_deadline_ms=2`

| Metric | Sync | Batched | Buffered |
|--------|------- | ------- | -------|
| Min | 111 µs | 114 µs | 18 µs |
| Mean | 188 µs | 207 µs | 26 µs |
| σ | 210 µs | 223 µs | 6 µs |
| p50 | 137 µs | 148 µs | 26 µs |
| p75 | 155 µs | 176 µs | 27 µs |
| p95 | 462 µs | 456 µs | 34 µs |
| p99 | 1.3 ms | 1.6 ms | 47 µs |
| p99.9 | 2.2 ms | 2.3 ms | 148 µs |
| Max | 3.0 ms | 2.9 ms | 194 µs |

## Saturation Ramp — Buffered tier

> Server started with `max_connections = 48`. Levels above 48 trigger
> connection-cap exhaustion; the server must survive every level.

| Threads | RPS (d1ms) | RPS (d2ms) | I/O drops | Status |
|---------|-------- | --------|-----------|--------|
| 8 | 27,330 | 27,031 | 0 | ok |
| 16 | 51,398 | 51,050 | 0 | ok |
| 32 | 93,371 | 93,285 | 0 | ok |
| 48 | 122,301 | 121,155 | 0 | ok |
| 64 | 122,233 | 121,258 | 16 | **SATURATED** ← |
| 96 | 122,631 | 121,135 | 48 | SATURATED |

## Saturation Ramp — Sync tier

> Uses Sync durability to stress the group-fsync path under escalating
> concurrency.

| Threads | RPS (d1ms) | RPS (d2ms) | I/O drops | Status |
|---------|-------- | --------|-----------|--------|
| 8 | 4,754 | 4,879 | 0 | ok |
| 16 | 24,248 | 25,687 | 0 | ok |
| 32 | 52,202 | 51,180 | 0 | ok |
| 48 | 77,229 | 74,431 | 0 | ok |
| 64 | 79,929 | 76,310 | 16 | **SATURATED** ← |
| 96 | 79,094 | 76,781 | 48 | SATURATED |

## IOPS compression

| Metric | Value |
|--------|-------|
| Records committed | 4,980 |
| Sink commits | 20 |
| **Records per commit** | **249:1** |
| Record size | 256 B · segment cap 64 KiB |

## How this compares

| | Mac M3 Max (here) | CI Linux runner (`latest.md`) | beast SATA SSD (`phase3-results.md`) |
|---|---|---|---|
| Durability syscall | `F_BARRIERFSYNC` | (GitHub runner) | `fdatasync` |
| Sync p50 | **133 µs** | 127 µs | **1,394 µs** |
| Buffered p50 | 26 µs | 33 µs | 18 µs |
| Single-thread Buffered RPS | 36,899 | 29,341 | 40,712 |
| Sync ramp peak | ~80 k @ 64t | 68.8 k @ 64t | 7.7 k @ 64t |
| Buffered ramp peak | ~123 k @ 48t | 101 k @ 48t | 157 k @ 48t |

The Mac's fast `F_BARRIERFSYNC` makes its Sync latency ~10× lower than beast's honest SATA `fdatasync` — beast remains the realistic "slow storage" durability reference. Within the Mac series, this is the number to beat for future runs.

## Reproduce

```sh
# 3 serial trials per deadline, averaged (matches this snapshot):
SNAP=/tmp/weir_snapshot.jsonl; : > "$SNAP"
for d in 1 2; do for i in 1 2 3; do
  WEIR_BENCH_DEADLINE=$d cargo test --release -p weir-server --test load -- \
    --nocapture --test-threads=1 2>/dev/null | grep -oE 'BENCH: \{[^}]*\}' >> "$SNAP"
done; done
python3 deploy/avg_benchmarks.py "$SNAP" /tmp/rendered.md && cat /tmp/rendered.md
```

> Note: use `grep -oE 'BENCH: \{[^}]*\}'` (not `grep '^BENCH:'`) with
> `--test-threads=1` — serial `--nocapture` inlines single-line BENCH output
> after the `test <name> ...` prefix, so an anchored grep silently drops every
> non-ramp scenario.
