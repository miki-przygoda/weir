# Agent count vs cores

`shard_count` and `worker_count` together define the size of the
**agent pool** — the set of (worker thread, flusher thread, WAB shard)
units that do the per-record work. Each agent costs ~2 OS threads
(one worker draining its queue partition, one flusher doing write +
fsync). Getting the count right matters: too many on too few cores
oversubscribes the CPU and *reduces* throughput; too few leaves
storage-side fsync parallelism unused.

The daemon emits a startup advisory (`advise_agent_count` in
`crates/weir-server/src/main.rs`) when the configured value falls
significantly outside the recommended band. The advisory is informational
only — operators who've measured their hardware should set the value
explicitly and ignore the log line.

## Empirical sweep (2026-05-27, 4-core sandbox)

Source: `tests/load.rs::sweep_agent_count_vs_throughput`. Workload:
64 producer threads, 100 Sync records each. Reproduce with
`cargo test --release --test load -- --ignored sweep_agent_count_vs_throughput`.

| agent_count | median RPS  | min     | max     | agents/cores |
|-------------|------------:|--------:|--------:|--------------|
| **1**       | **29,012**  | 28,462  | 33,194  | 0.25         |
| 2           | 26,233      | 25,684  | 29,303  | 0.50         |
| 3           | 23,113      | 23,080  | 25,405  | 0.75         |
| 4 (default) | 24,961      | 23,945  | 25,188  | 1.00         |
| 6           | 25,048      | 24,609  | 25,225  | 1.50         |
| 8           | 24,684      | 24,136  | 24,846  | 2.00         |

The peak is at `agent_count = 1` on this 4-core box — **14% above the
current bench-preset default of 4**. The trough is at `agent_count = 3`,
exactly where `agents × 2 threads + ~2 reserved (tokio + accept) = cores`
saturates the CPU.

## Why fewer is better on small hosts

Two compounding effects:

1. **Thread oversubscription.** With `agent_count = cores` the
   2 × agent_count thread budget plus the tokio runtime's worker
   threads plus the accept loop exceeds the core count, forcing
   context switches. Each context switch is ~5–10 μs of wasted CPU;
   on a hot path that processes records every ~30 μs, the overhead is
   significant.

2. **Group-fsync amortisation.** Fewer shards mean each shard's flusher
   sees more concurrent producers per batch. An investigation trace
   (the now-removed `investigate_herd_64_ceiling` test) showed
   `records_per_fsync` rising from **~7 at 4 agents** to **~60 at 1
   agent** under the same 64-producer Sync workload. Since fsync is the
   dominant cost on Sync, fatter batches win.

## The heuristic

```rust
fn recommended_agent_count(cores: usize) -> usize {
    // Reserve ~2 cores for tokio + accept + OS; give each remaining
    // core a 2-thread budget for one agent (worker + flusher).
    cores.saturating_sub(2).max(2) / 2
}
```

| cores | recommended | rationale                       |
|-------|------------:|---------------------------------|
| 1, 2  | 1           | Minimum; single-core hosts still need an agent. |
| 4     | 1           | Matches the empirical sandbox peak.             |
| 8     | 3           | 6 cores available / 2 threads per agent.        |
| 16    | 7           | 14 / 2 = 7.                                     |
| 32    | 15          | 30 / 2 = 15.                                    |
| 64    | 31          | 62 / 2 = 31.                                    |

The heuristic is **validated at 4 cores**. The linear extrapolation to
higher core counts is honest guesswork — the right value on a
production box depends on the storage device too (NVMe RAID can
handle many parallel fsyncs; a single SATA SSD cannot). Operators
running heterogeneous workloads should measure with
`sweep_agent_count_vs_throughput`.

## Advisory trigger bands

| Condition                                | Severity | What we say                                                 |
|------------------------------------------|----------|-------------------------------------------------------------|
| `actual > 2 × recommended`               | WARN     | Likely oversubscribed; CPU contention reduces throughput.   |
| `actual × 4 < recommended` and `recommended ≥ 4` | INFO     | May be leaving fsync parallelism unused; raise on parallel-fsync storage. |
| Otherwise                                | (silent) | Config looks reasonable for this host.                      |

## What's NOT measured here

- **Buffered-tier workloads.** The sweep used Sync because group-fsync
  amortisation is the dominant effect; Buffered would be CPU-bound
  differently and likely benefit from a *higher* agent_count.
- **Payload size.** All Sync records were 256 B. Larger payloads
  change the write/fsync ratio and may shift the peak.
- **Bare metal at 16+ cores.** Sandbox-only data. Production
  measurements welcome — re-run the sweep on a real box and PR the
  table.
