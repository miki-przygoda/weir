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
| [drain-throughput.md](benchmarks/drain-throughput.md) | **Delivery-side** rates — how fast the buffer *empties* into an HTTP sink, per-record vs NDJSON, and under a slow sink. Every other row on this table is ingest. Single basis as of 2026-09-18: mac re-measured 2026-09-15 and Linux 2026-09-18, both on the corrected window. |
| [wire-batching.md](benchmarks/wire-batching.md) | A6 `push_batch` throughput measured against an unbatched control, the fsync amortisation behind it, and how the advantage decays with record size |
| [sustained-load.md](benchmarks/sustained-load.md) | What `Durable` does over hours rather than seconds — a bimodal fsync oscillation that puts sustained throughput at ~40% of the short-run figure, with the controls that place it in the storage rather than in weir |
| [compression.md](benchmarks/compression.md) | What `wab_compression` costs and buys against record size. At ~4 KiB on slow storage zstd is a *net throughput win*; at ~150 B it is a bad trade |
| [buffering-and-recovery.md](benchmarks/buffering-and-recovery.md) | How much burst weir absorbs before backpressure reaches the producer, what sets that ceiling, and how long a restart takes against a held backlog |
| [environments.md](benchmarks/environments.md) | How CI and local numbers differ, what is safe to compare across environments, and how to run the suite locally |
| [batch-tuning.md](benchmarks/batch-tuning.md) | `batch_size` × `batch_deadline_ms` sweep informing the current defaults |
| [agent-count-tuning.md](benchmarks/agent-count-tuning.md) | `shard_count` / `worker_count` sweep informing the startup advisory; cores-vs-agents heuristic |

---

## Headline numbers (latest CI run)

> See [latest.md](benchmarks/latest.md) for the full tables and
> [history.md](benchmarks/history.md) for the trend over time. The figures
> below are rounded from one averaged CI run (v4.0.1, 2026-09-28, 5 passes per
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

| Scenario | Concurrency | RPS | ±σ across the 5 passes |
|----------|---|-----|-----|
| Single thread, Buffered | 1 | ~14,950 | ±350 |
| Single thread, Durable | 1 | ~3,670 | ±226 |
| Thundering herd, Buffered | 64 | ~46,300 | ±12,868 |
| Saturation ceiling, Buffered | 48 | ~97,000 | not sampled |
| Saturation ceiling, Durable | 48 | ~43,000 | not sampled |

> **The single-thread rows are latency reciprocals, not ceilings.** Read down
> the `Durable` rows: the same tier, on the same runner, in the same run set,
> does ~3,670 rec/s on one connection and ~43,000 across 48 — **12×**. Nothing
> about the daemon changed between those two rows.
>
> `Durable` is a group commit at the batch boundary
> (`crates/weir-core/src/durability.rs`), and the daemon acks frame N before
> reading frame N+1 on a connection
> (`crates/weir-server/src/socket/connection.rs`). One synchronous producer
> therefore pays one fsync per record and is bound by fsync *latency*: 1 ÷ 3,670
> = 273 µs, which sits between that run's `Durable` p50 of 259 µs and its mean
> of 283 µs. Concurrency is what fills a batch, and a filled batch is what
> amortises the fsync.
>
> **How much the runner distorts the tail varies between runs, and this run is
> a mild one.** Its `Durable` p99 of 567 µs against a p50 of 259 µs is a ratio of
> 2.2× — *tighter* than the bare-metal capture's 1.1 ms / 3.7 ms, which is 3.4×.
> The distortion has not gone away, it has moved outward: p99.9 is 7.6 ms and the
> max 9.5 ms, so the same runner is still injecting multi-millisecond stalls, just
> past the 99th percentile this time. Earlier run sets put that stall at the p99
> instead (one reported 22.4 ms against a 359 µs p50, a 62× ratio). Which
> percentile the noise lands on is not a property of weir, and it is the reason
> this index refuses to let a single CI percentile stand as a claim.
>
> So a single-thread figure answers "how fast can one caller push-and-wait on
> this hardware", and only a concurrent figure answers "how much can the daemon
> absorb". Quoting the first as the second understates weir by more than an
> order of magnitude — and, because the number is a property of the platform's
> fsync primitive as much as of weir, it does not transfer between machines
> either (see [environments.md](benchmarks/environments.md)).

### Latency at `batch_deadline_ms=1` (single thread)

Two tiers are selectable today. The load suite still runs a `Durable` push
under two scenario tags — `Sync` and `Batched` — kept only for continuity
with prior CI history; both push the same `Durability::Durable` and the two
rows below are the same tier measured twice, not two tiers.

| Tier (scenario tag) | p50 | p99 |
|------|-----|-----|
| Buffered | ~56 µs | ~96 µs |
| Durable (`Sync` tag) | ~359 µs | ~22.4 ms |
| Durable (`Batched` tag, historical) | ~366 µs | ~44.0 ms |

*Numbers above are approximate CI figures (sandboxed GitHub runners), not a baseline.
Exact figures are in [latest.md](benchmarks/latest.md); for claims on named
hardware see [bare-metal.md](benchmarks/bare-metal.md).*

---

## Regression policy

**These thresholds apply to same-machine, before-and-after comparisons — not to
CI rows.** On one machine, across a single change, a >10% drop in single-thread
throughput or a >20% rise in `Durable`-tier p99 (the `Sync`-tagged scenario in
CI output) is worth investigating before merging. The run-to-run noise floor of
the bare-metal surface these thresholds are meant for was measured on
2026-09-18: across four back-to-back captures on beast, on a byte-identical
measurement path,
single-thread `Sync` throughput moved 1.8% and `Sync` p99 did not move at the
precision published, so 10% and 20% both sit well above the noise. See
[bare-metal.md](benchmarks/bare-metal.md) for the full spread — including the
two scenarios whose run-to-run range exceeds 10% and so cannot be gated at
all.

They are **not** usable against [history.md](benchmarks/history.md). Consecutive
CI runs of the *same released version* there span 1,926–2,783 single-thread
`Sync` RPS (1.45×) and 610 µs – 1.2 ms `Sync` p99 (~2×), so a 10% or 20% move
between CI rows is inside the run-to-run spread and carries no signal. The CI
surface's own gate is an order-of-magnitude one — see
[environments.md](benchmarks/environments.md).

Multi-thread and tail-latency (p99.9+) numbers are noisier still and should be
treated as directional signals, not thresholds.
