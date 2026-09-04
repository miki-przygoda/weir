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
| [drain-throughput.md](benchmarks/drain-throughput.md) | **Delivery-side** rates — how fast the buffer *empties* into an HTTP sink, per-record vs NDJSON, and under a slow sink. Every other row on this table is ingest. **A first measurement, not a baseline** — its methodology is under review; read its caveat before quoting it. |
| [environments.md](benchmarks/environments.md) | How CI and local numbers differ, what is safe to compare across environments, and how to run the suite locally |
| [batch-tuning.md](benchmarks/batch-tuning.md) | `batch_size` × `batch_deadline_ms` sweep informing the current defaults |
| [agent-count-tuning.md](benchmarks/agent-count-tuning.md) | `shard_count` / `worker_count` sweep informing the startup advisory; cores-vs-agents heuristic |

---

## Headline numbers (latest CI run)

> See [latest.md](benchmarks/latest.md) for the full tables and
> [history.md](benchmarks/history.md) for the trend over time. The figures
> below are rounded from one averaged CI run (v2.0.3, 2026-09-02, 5 passes per
> deadline, `shard_count=4`, `batch_size=64`). **This index is hand-maintained
> and is not regenerated** — `deploy/avg_benchmarks.py` writes `latest.md` and
> appends to `history.md`, and says so at its own line 9. When the two disagree,
> `latest.md` is right and this table is stale.
>
> CI's run-to-run spread swamps the last digit of everything below: consecutive
> runs of the *same released version* in `history.md` differ by up to 1.45× on
> single-thread `Sync` RPS. The `±σ` column is the within-run-set deviation
> across the 5 passes only; it is not the spread between CI runs.

### Throughput at `batch_deadline_ms=1`

| Scenario | RPS | ±σ across the 5 passes |
|----------|-----|-----|
| Single thread, Buffered | ~13,850 | ±352 |
| Single thread, Durable | ~3,670 | ±82 |
| Thundering herd, 64 threads | ~52,250 | ±1,973 |
| Saturation ceiling (Buffered, 48 threads) | ~91,400 | not sampled |

### Latency at `batch_deadline_ms=1` (single thread)

Two tiers are selectable today. The load suite still runs a `Durable` push
under two scenario tags — `Sync` and `Batched` — kept only for continuity
with prior CI history; both push the same `Durability::Durable` and the two
rows below are the same tier measured twice, not two tiers.

| Tier (scenario tag) | p50 | p99 |
|------|-----|-----|
| Buffered | ~70 µs | ~96 µs |
| Durable (`Sync` tag) | ~265 µs | ~381 µs |
| Durable (`Batched` tag, historical) | ~265 µs | ~391 µs |

*Numbers above are approximate CI figures (sandboxed GitHub runners), not a baseline.
Exact figures are in [latest.md](benchmarks/latest.md); for claims on named
hardware see [bare-metal.md](benchmarks/bare-metal.md).*

---

## Regression policy

**These thresholds apply to same-machine, before-and-after comparisons — not to
CI rows.** On one machine, across a single change, a >10% drop in single-thread
throughput or a >20% rise in `Durable`-tier p99 (the `Sync`-tagged scenario in
CI output) is worth investigating before merging. The run-to-run noise floor of
the bare-metal surface these thresholds are meant for has never been measured —
[bare-metal.md](benchmarks/bare-metal.md) still has no capture — so treat them
as provisional until it is.

They are **not** usable against [history.md](benchmarks/history.md). Consecutive
CI runs of the *same released version* there span 1,926–2,783 single-thread
`Sync` RPS (1.45×) and 610 µs – 1.2 ms `Sync` p99 (~2×), so a 10% or 20% move
between CI rows is inside the run-to-run spread and carries no signal. The CI
surface's own gate is an order-of-magnitude one — see
[environments.md](benchmarks/environments.md).

Multi-thread and tail-latency (p99.9+) numbers are noisier still and should be
treated as directional signals, not thresholds.
