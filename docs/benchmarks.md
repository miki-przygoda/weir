# Benchmarks

weir is benchmarked on every push to `main`. The suite covers single-thread
throughput, multi-thread thundering-herd, connection churn, fire-and-forget
overload, per-tier latency percentiles, and a saturation ramp to find the
throughput ceiling.

**As of 2.0.0 there are two durability tiers, `Durable` and `Buffered`, not
three.** `Sync` and `Batched` are deprecated aliases for `Durable` — see the
[2.0.0 CHANGELOG entry](https://github.com/miki-przygoda/weir/blob/main/CHANGELOG.md). Some tables below still carry
`Sync` / `Batched` as separate rows; that is CI history collected before the
collapse (or, for the latency suite, a scenario kept under its old tag for
tag continuity — see `crates/weir-server/tests/load.rs`), not a claim that
they are two selectable tiers today.

---

## Sub-documents

| Document | Contents |
|----------|----------|
| [latest.md](benchmarks/latest.md) | Full results from the most recent CI run — throughput comparison, per-tier latency tables, saturation ramp |
| [bare-metal.md](benchmarks/bare-metal.md) | Operator-run results on named hardware — the canonical source for any external performance claim (CI runners are sandboxed and noisier) |
| [history.md](benchmarks/history.md) | One row per CI run on `main` — headline single-thread `Durable`-tier RPS and p99 (columns are still labelled `Sync`, their pre-2.0 name), `Buffered` p50, and ramp peak over time |
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
| Single thread, Durable | ~2,550 |
| Thundering herd, 64 threads | ~36,600 |
| Saturation ceiling (Buffered, ~64 threads) | ~58,600 |

### Latency at `batch_deadline_ms=1` (single thread)

Two tiers are selectable today. The load suite still runs a `Durable` push
under two scenario tags — `Sync` and `Batched` — kept only for continuity
with prior CI history; both push the same `Durability::Durable` and the two
rows below are the same tier measured twice, not two tiers.

| Tier (scenario tag) | p50 | p99 |
|------|-----|-----|
| Buffered | ~69 µs | ~106 µs |
| Durable (`Sync` tag) | ~364 µs | ~751 µs |
| Durable (`Batched` tag, historical) | ~364 µs | ~702 µs |

*Numbers above are approximate CI baselines (sandboxed GitHub runners).
Exact figures are in [latest.md](benchmarks/latest.md); for claims on named
hardware see [bare-metal.md](benchmarks/bare-metal.md).*

---

## Regression policy

Changes that move **single-thread throughput down by more than ~10%** or
**`Durable`-tier p99 up by more than ~20%** (the `Sync`-tagged scenario in
CI output) should be investigated before merging.
Multi-thread and tail-latency (p99.9+) numbers are noisier in CI and should
be treated as directional signals, not hard thresholds.
