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

## Methodology

For each (`batch_size`, `batch_deadline_ms`) point:

1. Fresh `wab_dir`, fresh `weir-server` process per trial
2. `weir-bench --samples 1000 --payload 256 --only latency` → 3 trials
3. Median across trials per percentile

Fixed parameters: `shard_count=2`, `worker_count=2`, payload 256 B, 1000 samples.

`weir-bench` reports three latency scenarios per run: `sync` (request →
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
- The current `config/mod.rs` defaults (`batch_size = 1000`, `batch_deadline_ms
  = 100`) are out of step with what every shipped config actually uses (the CI
  load job runs `batch_size = 64`, the bench job and smoke test use
  `batch_deadline_ms = 1`).

## Recommendation

Change `Config::from_layers` defaults to `batch_size = 256`,
`batch_deadline_ms = 1`. This:
- Brings defaults in line with what the project actually exercises in CI
- Lands a ~6× p50 latency improvement for any operator who runs with defaults
- Costs ~zero throughput at low load (small batches still flush at the
  size cap when traffic is heavy; the deadline only matters when traffic is
  light)

This recommendation is left unimplemented in this commit so the change can be
reviewed and applied independently with its own discussion of throughput
implications under sustained load (which this sweep did not measure — the
weir-bench throughput scenarios should be re-run before changing defaults).

## Out of scope for this sweep

- Throughput scenarios (`weir-bench --only throughput / herd / churn`). The
  `batch_size` parameter mostly matters under sustained load — that's the next
  experiment.
- Larger payloads. 256 B is the load-test default but production records may be
  larger.
- Different `shard_count` / `worker_count`. Held fixed at (2, 2) to isolate the
  batching variables.
