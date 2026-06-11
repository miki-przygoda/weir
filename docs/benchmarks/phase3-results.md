# Phase 3 — Performance — Results Snapshot

**Date:** 2026-06-11 · **Branch:** `v1/phase-3-performance` (→ 0.8.0)

A point-in-time record of what Phase 3 changed, what it measured, and what the numbers say. Captured across two machines (macOS dev + a real Linux box) so the conclusions aren't single-environment artifacts.

## TL;DR

**The durable write path is fsync-bound.** On a real Linux SATA SSD, a single Sync record takes ~1.4 ms, of which ~99% is `fdatasync`. Every part of weir's software pipeline that we optimized in Phase 3 (the bridge hop, write syscalls, payload allocations, the coalesce window) is *small* against that fsync floor. So the honest Phase 3 wins are: a **simpler topology**, **cheaper allocations**, **fewer write syscalls**, and — most importantly — **the ability to measure where the time actually goes**. The fsync floor itself is storage-bound and not something the software can move.

## Environments

| | Mac (dev) | beast (Linux test) |
|---|---|---|
| CPU | Apple M-series (arm64) | Intel i9-9900K, 8c/16t (x86_64) |
| OS / kernel | macOS, Darwin 25.5 | Linux 6.17.0 |
| RAM | — | 31 GiB |
| WAB storage | NVMe | Samsung 850 EVO **SATA SSD**, ext4 |
| Durability syscall | `F_BARRIERFSYNC` | `fdatasync` |
| **Observed fsync latency** | **~150 µs** | **~1.4 ms** |

> beast is dual-boot; its NVMe (970 EVO), 1 TB SATA SSD (860 EVO), and 3.6 TB spinning disk are all **Windows NTFS and left untouched**. The 250 GB SATA SSD is the only mounted Linux ext4 filesystem. Its honest, slow `fdatasync` (no power-loss protection) makes it a *more* realistic durability benchmark than the Mac's fast `F_BARRIERFSYNC`.

## Baselines (single-thread latency, `batch_deadline_ms=1`)

| metric | Mac (NVMe) | beast (SATA SSD) |
|---|---|---|
| Sync p50 / p99.9 | 152 µs / 3.1 ms | **1394 µs / 4.6 ms** |
| Batched p50 / p99.9 | 167 µs / 4.3 ms | 1392 µs / 4.8 ms |
| Buffered p50 | ~30 µs | 18 µs |
| Single-thread Buffered RPS | ~29 k | **40,712** |
| Single-thread Sync RPS | — | 647 |

beast throughput: herd Sync **764 / 2,797 / 7,726 RPS** at 8 / 32 / 64 producers (group-fsync amortization); Buffered ramp **157 k RPS @ 48 threads**; IOPS compression **249:1** records per sink commit.

**Sync ≡ Batched.** On both machines the two tiers are statistically identical (beast: p50 1394 vs 1392, p99.9 4616 vs 4801). This confirms they share one code path (group-fsync, ack-after-fsync) — there is **no "Batched anomaly"** (the original scout hypothesis was disproven by reading `flush_batch`; the old CI p99.9 gap was tail noise on a 2-vCPU runner).

## Per-stage breakdown (Mac, `bench-trace`, mean µs, Sync tier)

| stage | baseline | after Phase 3 |
|---|---|---|
| queue (enqueue → worker-flush) | 2 | 2 |
| bridge_wait (worker-flush → flusher) | 4 | **2** (Stream B) |
| write (flusher → write done) | 12 | **5** (Stream D writev) |
| **total** (enqueue → ack) | 166 | fsync-bound (~150–290) |

→ **fsync is ~89% of Sync latency** on the Mac, and ~99% on beast's slower SATA SSD.

## Stream-by-stream

| Stream | Status | Result |
|---|---|---|
| **A** Measurement | ✅ shipped | `bench-trace` per-stage histograms, Sync-tier ramp, wider samples + σ. The foundation that made everything below *measured, not guessed*. |
| **B** Bridge removal | ✅ shipped | Deleted one thread + one channel per shard (`WabRecord` type gone, worker feeds flusher directly). `bridge_wait` 4 → 2 µs. Per-shard FIFO, group-fsync coalescing, graceful shutdown all verified unchanged. |
| **C** Bytes payload | ✅ shipped | `Payload = Vec<u8>` → `bytes::Bytes`. Eliminated the drain's per-batch `.iter().cloned()` copy and the HTTP sink's `.to_vec()` (both now O(1) ref-bumps). Wire + on-disk formats **byte-identical** (codec/recovery/fuzz tests prove it). No throughput regression. |
| **D** write_vectored | ✅ shipped | 3 `write_all` syscalls → 1 `writev` per record (`write_all_vectored` is still unstable, so stable `write_vectored` + poison-on-short-write). Write stage **−60%** (Sync 12 → 5 µs). Format-identical. Total latency unchanged (fsync-bound). |
| **D** io_uring | ❌ not shipped | **Measured case against it.** `write_vectored` already collapsed the write syscalls; the dominant cost (fsync — 1.4 ms on beast) is storage-bound and io_uring cannot speed it up. io_uring is also Linux-only (absent on macOS) and adds unsafe complexity. A direct io_uring-vs-writev micro-benchmark on beast is the final confirmation (pending), but the baseline already makes the case. |
| **E** Adaptive coalesce | ⚠️ shipped (marginal) | EWMA of fsync latency sizes the worker's coalesce window. beast A/B (below) shows it helps low-concurrency-on-slow-storage but adds jitter at high concurrency — **within the noise overall**. Flagged for keep-or-revert. |
| **F** worker_count default | ✅ shipped | Default `worker_count` now follows `shard_count` (was a hardcoded 2), removing an idle worker thread in the default single-shard config. |

## Stream E A/B (beast, herd throughput RPS, 3 trials)

| workload | FIXED-200 µs | ADAPTIVE (≤2 ms) | ADAPTIVE (≤600 µs) |
|---|---|---|---|
| herd_8 (low conc.) | 925–1440 (noisy) | **1491–1504 (tight, +17%)** | 659–1466 (noisy) |
| herd_32 (moderate) | med 4690 | med 4515 | med 4488 |
| herd_64 (high conc.) | **9750–9836 (tight)** | 7336–9752 (noisy) | 9644–10006 (tight) |

No clamp wins everywhere: a window long enough to help low concurrency over-waits at high concurrency. The fixed window is simple and competitive. The adaptive window's one real win (low-conc on slow storage) is narrow and barely clears the noise — hence "marginal."

## Verdict — what we're working with

The durable-write path is now **lean and well-instrumented**, but it is **fsync-bound by physics**. The remaining big levers are *not* in the software pipeline (which is tight) — they are in **durability semantics** (e.g. a relaxed/group-commit tier that trades some durability for throughput) or **storage** (faster devices / batched-fsync hardware). Phase 3's lasting value: the `bench-trace` instrumentation makes any future change provable, and we now know exactly where the microseconds go.

---
*Snapshot generated during the Phase 3 performance pass. Raw per-run numbers are reproducible via `tests/load.rs` (+ `--features bench-trace` for the per-stage breakdown).*
