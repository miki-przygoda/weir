# Benchmark Environments

weir publishes numbers from three distinct surfaces. They answer different
questions and have different regression gates.

| Surface | Source | Catches | Gate |
|--------|--------|---------|------|
| [`latest.md`](latest.md), [`history.md`](history.md) | CI (`ubuntu-latest`, 2 vCPU) | Order-of-magnitude regressions: missing `#[inline]`, accidental allocation on the hot path, an O(n²) loop in the WAB encode | >10× drop or any scenario going from non-zero to zero |
| [`bare-metal.md`](bare-metal.md) | Operator-run script, named hardware | Real performance regressions visible to a deployer | >10% drop in single-thread RPS, >20% increase in `Sync` p99, any saturation level regressing from `ok` to dropped I/O — **provisional:** no capture exists yet, so this surface's run-to-run noise floor is unmeasured and the thresholds are unvalidated |
| [`drain-throughput.md`](drain-throughput.md) | Operator-run capture, `tests/load_drain.rs` — the same suite the CI `drain` job runs | Delivery-side (drain) regressions; every scenario above measures ingest only. Its published figures are a **first measurement whose methodology is under review** — see that file's caveat | No throughput gate. The CI `drain` job runs the suite on every push as a **correctness** gate (no record lost between the WAB and the sink across an outage) and retains its output as a build artifact, but does not compare rates to a threshold or write to this file. |

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
**first measurement** from the same suite (see its own environment section for
the machine/sink details) rather than a CI-trended figure — and explicitly not a
baseline: a later reproduction of the same command on the same machine class saw
a **2.6–4.1× spread across five consecutive runs**, and traced the timed window
to macOS `F_FULLFSYNC` on the drain's confirm path plus a slice of the drain's
own retry backoff, rather than to delivery work. Reproduce it locally with:

```sh
cargo test -p weir-server --test load_drain --release -- --nocapture
```

## Local environment

When running the suite locally, numbers will differ from CI in predictable ways:

| Metric | Direction vs CI | Reason |
|--------|----------------|--------|
| Single-thread Buffered RPS | **~2.3× higher** (31,943 vs 13,853) | Clock frequency and per-record CPU cost |
| Single-thread Durable RPS | **~1.5× higher** (5,658 vs 3,665) | One `fdatasync` per record — this ratio is the storage primitive's, not weir's |
| Multi-thread RPS | **2–4× higher** | Real parallelism vs. 2-core oversubscription |
| p50 / p95 latency | **~1.8–2.7× lower** (Buffered p50 26 µs vs 70 µs; Durable p50 145 µs vs 265 µs) | Faster cores and a faster fsync |
| p99.9 / Max latency | **Lower** | No noisy neighbours |
| Saturation threshold | Same thread count | `max_connections = 48` regardless of hardware |

The local column is one machine (Apple M3 Max, 16 cores, macOS 26.6.2, APFS,
`--release`) in one session, measured 2026-09-02 against CI's run of the same
day; it is an illustration of the size of the gap, not a second baseline. The
earlier rows here claimed "±10%" for single-thread RPS and "similar" p50/p95
latency, attributing both to the batch deadline timer. Neither holds: the gap on
single-thread Buffered is 2.3×, and p50 is nowhere near the deadline —
[`latest.md`](latest.md) reports Buffered p50 at 70 µs and Durable p50 at 265 µs
under a **1 ms** deadline, and barely moves (73 µs / 263 µs) when the deadline is
doubled to 2 ms.

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
  after a change, back to back. A >10% drop in single-thread RPS or a >20%
  increase in Sync p99 warrants investigation. This does **not** transfer to
  comparing two rows of [`history.md`](history.md): consecutive CI runs of the
  same released version differ there by up to 1.45× on Sync RPS and ~2× on Sync
  p99, so a 10% or 20% move between rows is noise.
- **Cross-environment comparisons** — do not compare absolute rates across
  machines, and in particular do not read a CI-vs-laptop difference as a
  statement about weir. Even single-thread scenarios fail to transfer: on one
  connection `Durable` is bounded by one `fdatasync` per record (the daemon acks
  frame N before reading frame N+1 —
  `crates/weir-server/src/socket/connection.rs:485-491`), so the rate is the
  platform's fsync primitive as much as it is weir's, and those primitives differ
  by two orders of magnitude. Measured on one M3 Max: 36 µs for plain `fsync(2)`
  (what `File::sync_all` is on Linux), 238 µs for `F_BARRIERFSYNC` (what the WAB
  record path uses on macOS — `crates/weir-server/src/wab/segment.rs:672-685`),
  and 3,965 µs for `F_FULLFSYNC` (what `File::sync_all` becomes on macOS, and
  what the drain's confirm path pays). Compare **ratios within one machine**
  instead — tier vs tier, config vs config, before vs after. Multi-thread
  throughput is not meaningful across different core counts either.
- **History table** — shows CI numbers only. Local runs are not appended.
