# Benchmark Environments

## CI environment

All numbers in [`latest.md`](latest.md) and [`history.md`](history.md) are
produced on GitHub Actions `ubuntu-latest` runners (2 vCPUs, ~7 GB RAM).
The `load` CI job runs 5 passes at each of two batch deadlines (1 ms and 2 ms)
and averages the results.

Relevant constraints:
- **2 vCPUs** — multi-threaded scenarios (thundering herd, saturation ramp) are
  heavily oversubscribed above 2 threads. Throughput plateaus early and is not
  representative of production hardware.
- **Shared host** — noisy-neighbour effects inflate tail latency. p99.9 and Max
  values from CI are unreliable; p95 and below are generally stable.
- **No CPU pinning** — weir-server pins workers starting at core 2; on a 2-core
  runner this lands on the same cores as the OS scheduler.

## Local environment

When running the suite locally, numbers will differ from CI in predictable ways:

| Metric | Direction vs CI | Reason |
|--------|----------------|--------|
| Single-thread RPS | ±10% | Clock frequency, thermal throttling |
| Multi-thread RPS | **2–4× higher** | Real parallelism vs. 2-core oversubscription |
| p50 / p95 latency | Similar | Both reflect the batch deadline timer |
| p99.9 / Max latency | **Lower** | No noisy neighbours |
| Saturation threshold | Same thread count | `max_connections = 48` regardless of hardware |

### How to run locally

```sh
# Single pass at 1 ms deadline:
WEIR_BENCH_DEADLINE=1 cargo test -p weir-server --test load --release -- --nocapture

# Both deadlines, capture BENCH lines, generate latest.md:
for d in 1 2; do
  WEIR_BENCH_DEADLINE=$d \
    cargo test -p weir-server --test load --release -- --nocapture 2>/dev/null \
    | grep '^BENCH: ' >> load_results.jsonl
done
python3 deploy/avg_benchmarks.py load_results.jsonl docs/benchmarks/latest.md
```

## What to compare

- **Cross-commit regressions** — run the suite on the same machine before and
  after a change. A >10% drop in single-thread RPS or a >20% increase in Sync
  p99 warrants investigation.
- **Cross-environment comparisons** — only valid for single-thread scenarios and
  latency percentiles below p99. Multi-thread throughput is not meaningful
  across different core counts.
- **History table** — shows CI numbers only. Local runs are not appended.
