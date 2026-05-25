# Benchmarks

weir is benchmarked on every push to `main`. The suite covers single-thread
throughput, multi-thread thundering-herd, connection churn, fire-and-forget
overload, per-tier latency percentiles, and a saturation ramp to find the
throughput ceiling.

---

## Sub-documents

| Document | Contents |
|----------|----------|
| [latest.md](benchmarks/latest.md) | Full results from the most recent CI run — throughput comparison, per-tier latency tables, saturation ramp |
| [history.md](benchmarks/history.md) | One row per CI run on `main` — headline Sync RPS, Sync p99, Buffered p50, and ramp peak over time |
| [environments.md](benchmarks/environments.md) | How CI and local numbers differ, what is safe to compare across environments, and how to run the suite locally |

---

## Headline numbers (latest CI run)

> See [latest.md](benchmarks/latest.md) for the full tables and
> [history.md](benchmarks/history.md) for the trend over time.

### Throughput at `batch_deadline_ms=1`

| Scenario | RPS |
|----------|-----|
| Single thread, Buffered | ~800 |
| Single thread, Sync | ~600 |
| Thundering herd, 64 threads | ~2,900 |
| Saturation ceiling (48 threads) | ~17,000 |

### Latency at `batch_deadline_ms=1` (single thread)

| Tier | p50 | p99 |
|------|-----|-----|
| Buffered | ~1.3 ms | ~1.6 ms |
| Batched | ~2.0 ms | ~2.3 ms |
| Sync | ~2.0 ms | ~2.3 ms |

*Numbers above are approximate CI baselines. Exact figures are in
[latest.md](benchmarks/latest.md) and updated automatically on every push.*

---

## Regression policy

Changes that move **single-thread throughput down by more than ~10%** or
**Sync p99 up by more than ~20%** should be investigated before merging.
Multi-thread and tail-latency (p99.9+) numbers are noisier in CI and should
be treated as directional signals, not hard thresholds.
