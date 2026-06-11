# Phase 3 — Performance — Design (umbrella spec)

**Version target:** 0.8.0
**Branch:** `v1/phase-3-performance` (off `v1/phase-2-clickhouse`; stacked, local-only, single push at the end of the v1 roadmap)
**Date:** 2026-06-11

## Goal

Make weir measurably faster on the write hot path **and prove every gain with before/after numbers**, without weakening any durability or correctness invariant. This phase is deliberately *measurement-first*: we instrument the pipeline before we optimise it, so each change is attributed to a stage and validated, not guessed.

## Why this is an umbrella spec

The agreed scope ("Everything") spans instrumentation, the threading topology, the core payload type, the write path, a kernel-specific spike, and adaptive tuning. That is too much surface for one plan. Phase 3 is therefore decomposed into **six work-streams**, each with its own implementation plan and each a reviewable group of commits on the Phase 3 branch. Streams run in order; **Stream A is a hard gate** because its numbers re-rank and gate B–F.

This spec defines the streams, the discipline shared across them, and the explicit kill-criteria for the speculative work. Per-stream design detail lives in each stream's plan under `docs/superpowers/plans/`.

## Findings basis

Phase 3 was scouted by five parallel agents (2026-06-10). Cross-validated findings (≥2 independent scouts) form the target list. The full scouting synthesis is captured in project memory (`phase3-perf-scouting`). Baseline numbers referenced below come from `docs/benchmarks/latest.md` (CI 2 vCPU): single-thread Sync ~3,216 RPS, Sync p50/p99/p99.9 = 278 µs / 687 µs / 3.8 ms, Batched p99.9 = 11.2 ms, saturation knee ~83,857 RPS at 48 connections.

## Shared discipline (applies to every stream)

1. **Prove it.** Every optimisation lands with a before/after measurement against Stream A's instrumentation. No "should be faster" merges.
2. **No durability regressions.** The fsync-before-ack contract (Sync tier), group-fsync semantics, crash-recovery invariants, and the flusher panic supervisor all hold unchanged. Any change touching the WAB write/fsync path re-runs the recovery + fuzz tests.
3. **Zero release-build cost for instrumentation.** All profiling hooks are behind a `bench-trace` cargo feature (off by default). A normal `cargo build --release` carries none of it.
4. **Cross-platform stays green.** macOS keeps its existing `F_BARRIERFSYNC` path; any Linux-only optimisation is `#[cfg(target_os = "linux")]`-gated with the portable path as the fallback. Windows/macOS/Linux CI must stay green (per CI discipline).
5. **Wire + on-disk formats are frozen.** The envelope wire format and the WAB segment on-disk format do not change in Phase 3. Payload bytes are identical; only in-memory representation may change.

## The six streams

### Stream A — Measurement foundation (THE GATE)

Without per-stage attribution, every later change is guesswork. Stream A makes latency attributable to a pipeline stage and tightens the benchmark's statistical confidence.

- **Per-stage latency**, behind a `bench-trace` feature: capture an enqueue timestamp at queue-push and a worker-flush timestamp, carry them forward to the WAB flusher, and record per-stage deltas (queue+coalesce wait, bridge+write, fsync) so we can say *where* the microseconds go for Sync vs Batched vs Buffered. The mechanism (shared aggregator vs metrics histograms vs round-trip) is decided in Stream A's plan; the constraint is zero cost when the feature is off.
- **Tighter bench stats:** widen latency-percentile sample counts (500 → 2000) and emit per-run σ in the `BENCH:` JSONL + `avg_benchmarks.py` tables.
- **Sync-tier saturation ramp:** the current `ramp_to_saturation` is Buffered-only and hides fsync saturation; add a Sync-tier variant that stresses the group-fsync path.
- **Baseline capture** committed under `docs/benchmarks/` before any hot-path change.

**Deliverable:** a per-stage latency breakdown + a captured baseline. **Gate:** B–F do not start until A's numbers exist; those numbers confirm or revise each later stream's hypothesis and the io_uring go/no-go.

### Stream B — Pipeline structure (latency)

- **Remove the bridge thread.** Today `main.rs` runs one extra thread per shard that unpacks a `Batch` of `WorkUnit`s into individual `WabRecord`s — a pure field-copy hop with a context switch. Collapse it: the worker hands batches to the WAB flusher directly (unify the `WorkUnit`/`WabRecord` types or have the worker own the flusher's sender). Per-shard FIFO and the ack back-channel are preserved.
- **Fix the Batched deadline anomaly.** Batched p99.9 (11.2 ms) is *worse* than Sync (3.8 ms), which is backwards. The hypothesis (to confirm with Stream A) is a second, independent `recv_timeout(batch_deadline)` in the WAB flusher that isn't restarted after a flush, so a record arriving just after a flush waits ~2× the deadline. Make the flusher stop independently re-waiting the full deadline. Target: Batched p99.9 ≈ Sync p99.9.

### Stream C — Allocation (throughput)

- **`pub type Payload = bytes::Bytes`.** The only avoidable steady-state heap work is the per-batch `.iter().cloned()` over every payload in the drain's commit path, plus the HTTP sink's `.to_vec()`. Migrating the payload type to ref-counted `Bytes` makes those clones O(1) ref-bumps. Touch points: wire decode (`Bytes::copy_from_slice` / `BytesMut::split_to`), the `SinkRecord` trait's `from_payload`/`into_payload`, the HTTP body, the segment read-back. The WAB writer already takes `&[u8]` (unchanged). **Wire + on-disk formats unchanged.**

### Stream D — Write path + io_uring spike

- **`write_vectored` first (portable).** The WAB writes a record as three separate `write_all` calls (len, crc, payload). Coalesce into one `writev` via `IoSlice`. Cross-platform, zero unsafe, no kernel-version constraint. (Honest expectation: modest on Sync p50 — fsync dominates — but real for Buffered/throughput.)
- **io_uring WAB write+fsync spike (Linux-only, behind an `io-uring` feature).** The scout's verdict was "not worth it" (group-fsync already captures io_uring's main batching win; fsync is storage-bound not syscall-bound). We **validate that with numbers** rather than assume it. Prototype the WAB write+fsync via io_uring (linked `WRITEV` → `FSYNC`/DATASYNC), benchmark against the `write_vectored` + group-fsync baseline on real hardware.
  - **KILL-CRITERION:** io_uring ships only if it beats the portable path by a meaningful margin (target: >10% on the flusher write+fsync path) on real hardware. Otherwise we document the negative result, keep `write_vectored`, and the `io-uring` feature is dropped or left off by default.
  - **Constraints:** kernel ≥ 5.6 runtime check; the panic supervisor tears down and recreates the ring on flusher respawn; the `poisoned`-segment and recovery invariants are unchanged; macOS keeps `F_BARRIERFSYNC`.

### Stream E — Adaptive coalesce window

- Replace the fixed 200 µs `COALESCE_WINDOW` in the worker with an EWMA of observed `wab_fsync_duration`, fed back to the worker, so the coalesce window tracks storage latency (≈100 µs on NVMe, longer on slow disks) instead of a hard-coded constant. Stream A first confirms the window is actually mistuned.

### Stream F — Micro-opts + tuning

- **Prometheus counter caching.** `records_accepted`/`records_ack` call `Family::get_or_create` (RwLock + HashMap) on every push/ack. Replace with a cached `[Counter; 3]` indexed by tier discriminant. **Conditional:** only if Stream A profiling shows this is material.
- **Tuning defaults.** Validate/adjust the default `agent_count`/`shard_count` using the existing `--ignored` sweep on the target hardware (the data shows `agent_count=1` beats `=4` by ~14% on ≤4-core boxes because more shards fragment the group-fsync batch). No production code beyond default constants.

## Sequencing and gates

```
A (measurement, gate) ──► numbers re-rank B–F
   ├─► B (bridge removal, Batched-deadline fix)
   ├─► C (Bytes migration)
   ├─► D (write_vectored ──► io_uring spike [kill-criterion])
   ├─► E (adaptive coalesce window)
   └─► F (counter cache [conditional], tuning defaults)
```

Each stream: plan → implement (TDD, frequent commits) → benchmark before/after → commit on `v1/phase-3-performance`. The version bump to 0.8.0 (workspace + the internal `weir-core` dep requirement in weir-server/weir-client — the known gotcha) lands with the final Phase 3 commit group.

## Out of scope (Phase 3)

- Socket-side io_uring / tokio-uring (would require rewriting the generic `handle_connection<S: AsyncRead + AsyncWrite>` API — disproportionate to the ~one-syscall-per-frame saving).
- `O_DIRECT` / page-cache-bypass write path (different architecture; defer).
- Any wire-protocol or on-disk-format change.

## Testing

- Existing suite stays green on every commit (`--test-threads=1` locally for the known WAB/socket `$TMPDIR` parallelism flake; CI runs green).
- WAB-touching streams (B, D) re-run recovery + cargo-fuzz trust-boundary targets.
- Each stream adds/extends load-suite coverage proving its specific gain (Stream A's per-stage breakdown + Sync ramp are the shared measuring stick).
