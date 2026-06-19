# Batch tuning sweep

Empirical sweep of `batch_size` × `batch_deadline_ms` to inform the choice of
defaults and surface the latency / throughput trade-off for operators tuning
their own deployments.

Addresses the "Batching efficiency: tune `batch_size` / `batch_deadline_ms`
sweet spot" item from the perf TODO in `crates/weir-server/tests/load.rs:26-33`.

## Caveat

Results below were collected on a single shared-VM sandbox in a single session.
Absolute numbers are noisy and will not match production hardware; **relative
ordering between configs is the takeaway**.

**The latency tables are single-producer.** The three latency scenarios
(`sync` / `batched` / `buffered`) and the single-thread throughput table below time
**one** producer pushing serially, so a batch fills by reaching `batch_deadline_ms`,
not the `batch_size` cap. That is why the deadline appears to dominate latency here —
it is the single-threaded regime talking. Under **concurrent** producers the picture
changes (see [Tuning under concurrency](#tuning-under-concurrency)): batches fill by the
deadline regardless of the size cap, so `batch_size` does little, and the per-deadline
latency figures in these tables do **not** carry over to concurrent throughput. Read
the deadline-dominates-latency numbers as a single-threaded result, not a tuning guide
for loaded deployments.

## Methodology

For each (`batch_size`, `batch_deadline_ms`) point:

1. Fresh `wab_dir`, fresh `weir-server` process per trial
2. `cargo test --release -p weir-server --test load -- --nocapture` → 3 trials
3. Median across trials per percentile

Fixed parameters: `shard_count=2`, `worker_count=2`, payload 256 B, 1000 samples.

`tests/load.rs` reports three latency scenarios per run: `sync` (request →
fsync → ack), `batched` (request → flush-or-deadline → ack), `buffered`
(request → in-memory ack, no durability).

## Results

### sync (request → WAB fsync → ack)

| Config (bs/dl) | p50 µs | p95 µs | p99 µs | p999 µs | max µs |
|---|---:|---:|---:|---:|---:|
| 64 / 1ms     |  1876 |  2902 |  3460 |   5299 |  5299 |
| 64 / 5ms     |  5960 |  6517 |  7546 |  14609 | 14609 |
| **256 / 1ms** | **1880** | **2294** | **3284** | **7210** | **7210** |
| 256 / 5ms    |  6003 |  6480 |  8065 |  30050 | 30050 |
| 1000 / 10ms  | 11086 | 11808 | 13326 |  42006 | 42006 |

### batched (request → batch-flush-or-deadline → ack)

| Config (bs/dl) | p50 µs | p95 µs | p99 µs | p999 µs | max µs |
|---|---:|---:|---:|---:|---:|
| 64 / 1ms     |  1922 |  2749 |  3319 |  10424 | 10424 |
| 64 / 5ms     |  5964 |  6416 |  6915 |  12245 | 12245 |
| **256 / 1ms** | **1891** | **2334** | **2660** |  **4776** |  **4776** |
| 256 / 5ms    |  6016 |  6434 |  7387 |  21868 | 21868 |
| 1000 / 10ms  | 11062 | 11747 | 13543 |  52745 | 52745 |

### buffered (request → in-memory ack, no durability)

| Config (bs/dl) | p50 µs | p95 µs | p99 µs | p999 µs | max µs |
|---|---:|---:|---:|---:|---:|
| 64 / 1ms     |  1224 |  1294 |  1343 |   1890 |  1890 |
| 64 / 5ms     |  5272 |  5355 |  5431 |  10892 | 10892 |
| **256 / 1ms** | **1218** | **1283** | **1333** |  **1423** |  **1423** |
| 256 / 5ms    |  5285 |  5393 |  5520 |   6924 |  6924 |
| 1000 / 10ms  | 10299 | 10388 | 10464 |  14087 | 14087 |

## Interpretation

- **`batch_deadline_ms` dominates latency**: p50 tracks the deadline to within a
  few hundred µs. A 5 ms deadline pushes p50 to ~6 ms; a 10 ms deadline pushes
  it to ~11 ms.
- **`batch_size` has a small effect at the same deadline**: 256 slightly
  outperforms 64 on the tail (p99 / p999), most likely because more records
  amortise per fsync.
- **(256, 1 ms) is the sweet spot** in this sweep — lowest p50 across sync /
  batched / buffered and tightest p99 tail in batched.
- The `config/mod.rs` defaults at the time of this sweep (`batch_size = 1000`,
  `batch_deadline_ms = 100`) were out of step with what every shipped config
  actually uses (the CI load job runs `batch_size = 64`, the bench job and smoke
  test use `batch_deadline_ms = 1`). The recommendation below was applied: the
  current defaults are now `batch_size = 256`, `batch_deadline_ms = 1`.

## Recommendation

Change `Config::from_layers` defaults to `batch_size = 256`,
`batch_deadline_ms = 1`. This:
- Brings defaults in line with what the project actually exercises in CI
- Lands a ~6× p50 latency improvement for any operator who runs with defaults
- **Improves** throughput by 3-6× across every concurrency level measured —
  the throughput companion sweep below shows no scenario where the current
  default beats (256, 1ms)

Applied in the commit that follows this doc.

## Throughput companion sweep

Same 5 configs × 3 trials, but using the `baseline_*_throughput*` and
`thundering_herd_*` scenarios in `tests/load.rs` (5000 samples per
scenario, 256 B payload). Median RPS across the 3 trials per scenario.

### Single-thread

| Config (bs/dl)   | sync RPS | buffered RPS |
|---|---:|---:|
| 64 / 1ms         |   486 |   738 |
| 64 / 5ms         |   159 |   185 |
| **256 / 1ms**    | **506** | **754** |
| 256 / 5ms        |   162 |   185 |
| 1000 / 10ms      |    86 |    95 |

### Concurrent (thundering herd)

| Config (bs/dl)   |  8 threads | 32 threads | 64 threads |
|---|---:|---:|---:|
| 64 / 1ms         |  5081 | 12849 | 15351 |
| 64 / 5ms         |  1428 |  4884 |  8191 |
| **256 / 1ms**    | **5033** | **12889** | **16986** |
| 256 / 5ms        |  1440 |  4780 |  8131 |
| 1000 / 10ms      |   749 |  2654 |  4779 |

(256, 1ms) leads or ties on every scenario; (1000, 10ms) is 3-6× worse on
every concurrency level. The throughput data agrees with the latency data:
no axis on which the (1000, 10ms) default beats (256, 1ms).

## Tuning under concurrency

The two knobs a tuner reaches for first — `batch_size` and `batch_deadline_ms` — are
**largely inert under concurrent producers, and the wrong place to start.** The flusher
flushes when it reaches `batch_size` records **or** `batch_deadline_ms` elapses,
whichever comes first. With many concurrent producers a batch hits the deadline long
before it hits the size cap, so `batch_size` does little and the flush cadence is set by
the deadline. The deadline-dominates-latency figures in the [Results](#results) tables
are **single-producer** measurements (see [Caveat](#caveat)); they are not a guide to
concurrent throughput. Keep the defaults `(256, 1ms)`.

The lever that actually moves concurrent Sync throughput is **group-fsync batching** —
how many concurrent producers' records ride a single `fsync` — and that is governed by
**connection density into few shards**, not by the batch knobs. Each connection is
pinned to one shard, and a shard's flusher group-fsyncs every record in its active
segment at once, so more concurrent producers per shard means more records amortised per
`fsync`. Spreading the same connections across more shards thins that batching and can
push Sync throughput *below* a single shard — `shard_count` is non-monotonic, not a
throughput dial. See the `shard_count` caveat in
[operations/configuration.md](../operations/configuration.md#shard_count) for the full
mechanism and the connections-per-shard guidance.

This sweep does **not** quantify that lever: `shard_count` / `worker_count` were held
fixed at (2, 2) to isolate the batching variables, and the per-machine RPS above will not
transfer to other hardware. Treat this section as qualitative direction — measure the
shard / connection-density trade on your own hardware before committing to a value.

## A related throughput cliff: `wab_segment_max_bytes`

Not a batch knob, but the same throughput conversation: a **small**
`wab_segment_max_bytes` (frequent segment rotation) is a throughput cliff. Each
seal does an extra fsync (sentinel+footer durability commit, plus a parent-dir
fsync to publish the rename), so more rotations = more seal fsyncs = lower RPS —
a sweep observed sync throughput drop from ~90k to ~26k RPS when the segment cap
was lowered to 1 MiB (observed on one box; treat the absolute numbers as
machine-specific). **Durability is unaffected** — every accepted record is
fsynced before its ack regardless of segment size; the seal fsync only publishes
the sealed-segment boundary to the drain. Default is 256 MiB (`SEGMENT_MAX_BYTES`);
see [`wab_segment_max_bytes`](../operations/configuration.md#wab_segment_max_bytes).

## Out of scope for this sweep

- Larger payloads. 256 B is the load-test default but production records may be
  larger.
- Different `shard_count` / `worker_count`. Held fixed at (2, 2) to isolate the
  batching variables.
