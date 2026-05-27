# weir-bench: load-test a running daemon

`weir-bench` is a standalone binary that connects to an already-running
`weir-server` over its Unix socket and drives configurable
throughput / latency / herd / churn scenarios against it. Output is
emitted in the same `BENCH: {json}` format as the in-tree
[`tests/load.rs`](../../crates/weir-server/tests/load.rs) so the same
[`deploy/avg_benchmarks.py`](../../deploy/avg_benchmarks.py) renderer
processes both sources.

## When to use weir-bench instead of `cargo test --test load`

| Goal | Use |
|------|-----|
| Catch order-of-magnitude regressions on every PR | `cargo test -p weir-server --test load` (CI runs this) |
| Capture publishable numbers on real hardware | `weir-bench` against a long-running daemon |
| Profile a hot path with `perf` / `samply` / `flamegraph` | `weir-bench` — the daemon is a separate process, so the profiler attaches to weir-server cleanly without test-harness noise |
| Test a non-default config (custom shard count, batch size, sink type) | `weir-bench` — point it at a daemon you started with the config you want |
| Measure HttpSink / MySqlSink end-to-end latency under load | `weir-bench` with `sink_type` set on the daemon |

`tests/load.rs` spins up a fresh server per test and runs in the
`cargo test` profile. That's right for CI: deterministic, hermetic,
sub-minute. It's wrong for capturing performance numbers, because the
process is short-lived (no time to reach steady state) and the
test-harness threads compete with the daemon for CPU.

`weir-bench` solves both: it drives an already-running daemon, so you
control the config completely and the daemon runs uncontested.

## Build

```sh
cargo build --release -p weir-bench
```

The binary lands at `target/release/weir-bench`.

## Run

Start the daemon in one terminal:

```sh
mkdir -p /tmp/weir/wab /tmp/weir/run && chmod 0700 /tmp/weir/run
./target/release/weir-server \
    --wab-dir /tmp/weir/wab \
    --socket-path /tmp/weir/run/weir.sock
```

In another terminal:

```sh
./target/release/weir-bench --socket /tmp/weir/run/weir.sock
```

## Options

```text
--socket PATH       Unix socket path (required)
--samples N         Records per latency/throughput scenario [default: 10000]
--payload N         Payload size in bytes [default: 256]
--deadline-ms N     Deadline tag in scenario names
                    [env: WEIR_BENCH_DEADLINE; default: 1]
--output PATH       Append JSONL result lines to PATH (creates if absent)
--only WHAT         Run only: latency | throughput | herd | churn
                    [default: all]
```

The `--deadline-ms` flag does **not** change the daemon's batch
deadline (the daemon owns that). It tags scenario names with the
deadline value so multiple captures against different daemon configs
can be compared side-by-side in `avg_benchmarks.py`'s output. Match it
to whatever you set on the running daemon.

## Scenarios

| Scenario | What it measures | BENCH line keys |
|----------|------------------|------------------|
| `latency_sync_d{N}ms`, `latency_batched_d{N}ms`, `latency_buffered_d{N}ms` | Per-push round-trip µs for each durability tier, single-threaded | `samples`, `min_us`, `mean_us`, `p50_us`, `p75_us`, `p95_us`, `p99_us`, `p999_us`, `max_us` |
| `single_thread_buffered_d{N}ms`, `single_thread_sync_d{N}ms` | Throughput at zero concurrency — useful for catching per-record overhead regressions | `threads=1`, `total_records`, `wall_ms`, `throughput_rps` |
| `thundering_herd_{8,32,64}_threads_d{N}ms` | Throughput when N threads push simultaneously through a shared barrier | `threads`, `total_records`, `wall_ms`, `throughput_rps` |
| `connection_churn_d{N}ms` | Throughput when each push gets a fresh connection — measures bind/accept/handshake cost relative to push cost | `threads=1`, `total_records=rounds`, `wall_ms`, `throughput_rps` |

## Capturing for `avg_benchmarks.py`

```sh
# Two passes, one per deadline, into the same JSONL file:
./target/release/weir-bench --socket /tmp/weir/run/weir.sock \
    --deadline-ms 1 --output /tmp/weir-bench.jsonl
./target/release/weir-bench --socket /tmp/weir/run/weir.sock \
    --deadline-ms 2 --output /tmp/weir-bench.jsonl

# Render to markdown:
python3 deploy/avg_benchmarks.py /tmp/weir-bench.jsonl /tmp/weir-bench-results.md
```

The renderer averages multiple passes per scenario, so for stable
numbers run each deadline 3–5 times before rendering.

## Pairing with `deploy/run_bare_metal_bench.sh`

[`deploy/run_bare_metal_bench.sh`](../../deploy/run_bare_metal_bench.sh)
captures the full machine context (CPU / kernel / NVMe / governor /
SMT / dirty-page sysctls) and runs `cargo test -p weir-server --test
load`. It's the right tool for committing reproducible numbers to
[`bare-metal.md`](bare-metal.md).

`weir-bench` is the right tool when you want to drive a daemon you've
already tuned, or when you're profiling. The capture script and
`weir-bench` are complementary, not interchangeable.
