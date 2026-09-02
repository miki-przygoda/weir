# Benchmark Environments

weir publishes numbers from three distinct surfaces. They answer different
questions and have different regression gates.

| Surface | Source | Catches | Gate |
|--------|--------|---------|------|
| [`latest.md`](latest.md), [`history.md`](history.md) | CI (`ubuntu-latest`, 2 vCPU) | Order-of-magnitude regressions: missing `#[inline]`, accidental allocation on the hot path, an O(n²) loop in the WAB encode | >10× drop or any scenario going from non-zero to zero |
| [`bare-metal.md`](bare-metal.md) | Operator-run script, named hardware | Real performance regressions visible to a deployer | >10% drop in single-thread RPS, >20% increase in `Sync` p99, any saturation level regressing from `ok` to dropped I/O |
| [`drain-throughput.md`](drain-throughput.md) | Operator-run capture, `tests/load_drain.rs` — the same suite the CI `drain` job runs | Delivery-side (drain) regressions; every scenario above measures ingest only | No throughput gate. The CI `drain` job runs the suite on every push as a **correctness** gate (no record lost between the WAB and the sink across an outage) and retains its output as a build artifact, but does not compare rates to a threshold or write to this file. |

The CI gate is below the noise floor of the bare-metal numbers and
vice-versa. Performance claims in the README, release notes, or any
external comparison must cite [`bare-metal.md`](bare-metal.md), not
[`latest.md`](latest.md).

## CI environment

GitHub Actions `ubuntu-latest` runners: 2 vCPUs, ~7 GB RAM. The `load` CI
job runs 5 passes at each of two batch deadlines (1 ms and 2 ms) and
averages the results into [`latest.md`](latest.md).

Relevant constraints:
- **2 vCPUs** — multi-threaded scenarios (thundering herd, saturation ramp) are
  heavily oversubscribed above 2 threads. Throughput plateaus early and is not
  representative of production hardware.
- **Shared host** — noisy-neighbour effects inflate tail latency. p99.9 and Max
  values from CI are unreliable; p95 and below are generally stable.
- **No CPU pinning** — weir-server pins workers starting at core 2; on a 2-core
  runner this lands on the same cores as the OS scheduler.

## Bare-metal environment

Captured by running `deploy/run_bare_metal_bench.sh`
on the target machine and committing the output to
[`bare-metal.md`](bare-metal.md). The script records CPU model, kernel,
filesystem, mount options, block-device model, governor, SMT/turbo state,
and the relevant `vm.dirty_*` sysctls so two runs can be meaningfully
compared. See [`bare-metal.md`](bare-metal.md) for the full capture
procedure and when to re-run.

## Drain (delivery-side) environment

The CI `drain` job (`tests/load_drain.rs`) is the delivery-side counterpart to
`load`: every scenario in the other two surfaces measures ingest, and
`load_drain` is the first suite that measures how fast the buffer *empties*.
It runs once per push rather than the `load` job's ten passes, and unlike
`load` it is a correctness gate, not a throughput one — it asserts that no
record is lost between the WAB and the sink across a sink outage. Its
throughput numbers are printed and retained as a build artifact but are not
compared to a threshold, averaged, or written to `latest.md`/`history.md`.

[`drain-throughput.md`](drain-throughput.md) is a one-off, operator-captured
baseline from the same suite (see its own environment section for the
machine/sink details) rather than a CI-trended figure. Reproduce it locally
with:

```sh
cargo test -p weir-server --test load_drain --release -- --nocapture
```

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
