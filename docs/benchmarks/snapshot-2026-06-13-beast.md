# Benchmark Snapshot — 2026-06-13 (beast, Linux SATA SSD)

**Machine:** beast — Intel Core i9-9900K (8c/16t) · 31 GiB · Linux 6.17.0 · **ext4 on a Samsung SATA SSD** (`/dev/sdc2`, `fdatasync`)
**Branch / commit:** `v1/phase-3-performance` @ `c46c0c7` (post-Phase-4: weir-ctl, weir-sink-sdk, full DST harness)
**Server config:** `shard_count=4`, `batch_size=64` (the load suite's `bench_preset`)
**Method:** `tests/load.rs`, release build, **3 serial trials per deadline** (`--test-threads=1`, averaged), at `batch_deadline` 1 ms and 2 ms. Rendered with `deploy/avg_benchmarks.py`.

> The honest **slow-storage** reference. beast's SATA `fdatasync` (~1.5 ms, no
> power-loss-protected write cache) is the realistic durability floor — ~10×
> the Mac dev box's `F_BARRIERFSYNC`. Compare like-for-like (beast-to-beast for
> future runs). Cross-environment analysis: `snapshot-2026-06-13-comparison.md`.

## TL;DR

- **The durable path is fsync-bound — and on honest storage it dominates everything.** A single `Sync` record is **1.5 ms p50**, of which ~99% is `fdatasync`. Single-thread Sync is just **653 RPS**.
- **`Batched` is identical to `Sync`** (1.5 ms p50) — same group-fsync code path.
- **`Buffered` (memory-only ack) is ~80× faster than Sync here** — 19 µs p50, **49,725 RPS** single-thread — because it never fsyncs. On this box the fast path actually beats the Mac.
- **Group-fsync amortization is the only thing that scales Sync:** single-thread 653 RPS → **~6.8k RPS @ 96 threads** as concurrent records coalesce under one fsync (~10× via batching), but the ~1.5 ms fsync floor caps it hard.
- **Buffered scales to ~155k RPS @ 48 threads** (the connection cap).
- **No regression from the Phase 4 / DST work** — these match the Phase 3 beast baseline (single-thread Sync 653 vs 647; Buffered ramp 155k vs 157k).

## Throughput — deadline comparison

| Scenario | RPS (1ms) | ±σ (1ms) | RPS (2ms) | ±σ (2ms) | d1ms/d2ms |
|----------|---------|------- | ---------|------- | ---------|
| single_thread_buffered | 49,725 | ±3,752 | 45,745 | ±2,727 | **1.09×** |
| single_thread_sync | 653 | ±19 | 617 | ±0 | **1.06×** |
| thundering_herd_8_threads | 1,258 | ±162 | 1,255 | ±235 | **1.00×** |
| thundering_herd_32_threads | 4,445 | ±507 | 4,367 | ±945 | **1.02×** |
| thundering_herd_64_threads | 9,296 | ±1,416 | 8,264 | ±1,586 | **1.12×** |
| connection_churn | 14,394 | ±967 | 14,916 | ±39 | **0.96×** |
| fire_and_forget_overload | 3,899 | ±8 | 3,942 | ±144 | **0.99×** |

## Latency — single thread, `batch_deadline_ms=1`

| Metric | Sync | Batched | Buffered |
|--------|------- | ------- | -------|
| Min | 1.1 ms | 1.1 ms | 16 µs |
| Mean | 1.6 ms | 1.6 ms | 19 µs |
| σ | 532 µs | 532 µs | 6 µs |
| p50 | 1.5 ms | 1.5 ms | 19 µs |
| p75 | 1.5 ms | 1.6 ms | 19 µs |
| p95 | 1.7 ms | 1.7 ms | 22 µs |
| p99 | 4.2 ms | 4.2 ms | 34 µs |
| p99.9 | 6.0 ms | 6.3 ms | 151 µs |
| Max | 7.2 ms | 7.0 ms | 188 µs |

## Latency — single thread, `batch_deadline_ms=2`

| Metric | Sync | Batched | Buffered |
|--------|------- | ------- | -------|
| Min | 1.2 ms | 1.2 ms | 16 µs |
| Mean | 1.6 ms | 1.6 ms | 18 µs |
| σ | 518 µs | 515 µs | 7 µs |
| p50 | 1.5 ms | 1.5 ms | 17 µs |
| p75 | 1.6 ms | 1.6 ms | 18 µs |
| p95 | 1.7 ms | 1.7 ms | 22 µs |
| p99 | 4.2 ms | 4.2 ms | 40 µs |
| p99.9 | 6.1 ms | 5.4 ms | 124 µs |
| Max | 6.8 ms | 5.5 ms | 279 µs |

## Saturation Ramp — Buffered tier

> Server started with `max_connections = 48`. Levels above 48 trigger
> connection-cap exhaustion; the server must survive every level.

| Threads | RPS (d1ms) | RPS (d2ms) | I/O drops | Status |
|---------|-------- | --------|-----------|--------|
| 8 | 30,864 | 32,579 | 0 | ok |
| 16 | 62,401 | 65,178 | 0 | ok |
| 32 | 116,550 | 118,661 | 0 | ok |
| 48 | 155,577 | 157,044 | 0 | ok |
| 64 | 149,343 | 149,293 | 16 | **SATURATED** ← |
| 96 | 151,028 | 154,255 | 48 | SATURATED |

## Saturation Ramp — Sync tier

> Uses Sync durability to stress the group-fsync path under escalating
> concurrency. The ~1.5 ms `fdatasync` floor caps throughput an order of
> magnitude below the Mac.

| Threads | RPS (d1ms) | RPS (d2ms) | I/O drops | Status |
|---------|-------- | --------|-----------|--------|
| 8 | 1,199 | 1,007 | 0 | ok |
| 16 | 2,282 | 1,954 | 0 | ok |
| 32 | 4,399 | 4,190 | 0 | ok |
| 48 | 6,178 | 5,381 | 0 | ok |
| 64 | 6,618 | 5,785 | 16 | **SATURATED** ← |
| 96 | 6,856 | 6,155 | 48 | SATURATED |

## IOPS compression

249:1 — 4,980 records committed in 20 sink commits (content-derived, deterministic; identical to the Mac run).

## Reproduce

Same as the Mac snapshot, run on beast (`git checkout c46c0c7` first; cargo is under `~/.cargo/bin` so `. ~/.cargo/env` in non-interactive shells):

```sh
. ~/.cargo/env; cd /root/weir
SNAP=/tmp/weir_beast.jsonl; : > "$SNAP"
for d in 1 2; do for i in 1 2 3; do
  WEIR_BENCH_DEADLINE=$d cargo test --release -p weir-server --test load -- \
    --nocapture --test-threads=1 2>/dev/null | grep -oE 'BENCH: \{[^}]*\}' >> "$SNAP"
done; done
python3 deploy/avg_benchmarks.py "$SNAP" /tmp/r.md && cat /tmp/r.md
```
