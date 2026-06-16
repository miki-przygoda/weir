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
| [bare-metal.md](benchmarks/bare-metal.md) | Operator-run results on named hardware — the canonical source for any external performance claim (CI runners are sandboxed and noisier) |
| [history.md](benchmarks/history.md) | One row per CI run on `main` — headline Sync RPS, Sync p99, Buffered p50, and ramp peak over time |
| [environments.md](benchmarks/environments.md) | How CI and local numbers differ, what is safe to compare across environments, and how to run the suite locally |
| [batch-tuning.md](benchmarks/batch-tuning.md) | `batch_size` × `batch_deadline_ms` sweep informing the current defaults |
| [agent-count-tuning.md](benchmarks/agent-count-tuning.md) | `shard_count` / `worker_count` sweep informing the startup advisory; cores-vs-agents heuristic |

---

## Headline numbers (latest CI run)

> See [latest.md](benchmarks/latest.md) for the full tables and
> [history.md](benchmarks/history.md) for the trend over time. The figures
> below are rounded from the most recent averaged CI run (5 passes per
> deadline, `shard_count=4`, `batch_size=64`) and are regenerated on every
> push to `main`.

### Throughput at `batch_deadline_ms=1`

| Scenario | RPS |
|----------|-----|
| Single thread, Buffered | ~15,200 |
| Single thread, Sync | ~2,550 |
| Thundering herd, 64 threads | ~36,600 |
| Saturation ceiling (Buffered, ~64 threads) | ~58,600 |

### Latency at `batch_deadline_ms=1` (single thread)

| Tier | p50 | p99 |
|------|-----|-----|
| Buffered | ~69 µs | ~106 µs |
| Batched | ~364 µs | ~702 µs |
| Sync | ~364 µs | ~751 µs |

*Numbers above are approximate CI baselines (sandboxed GitHub runners).
Exact figures are in [latest.md](benchmarks/latest.md); for claims on named
hardware see [bare-metal.md](benchmarks/bare-metal.md).*

---

## Regression policy

Changes that move **single-thread throughput down by more than ~10%** or
**Sync p99 up by more than ~20%** should be investigated before merging.
Multi-thread and tail-latency (p99.9+) numbers are noisier in CI and should
be treated as directional signals, not hard thresholds.
