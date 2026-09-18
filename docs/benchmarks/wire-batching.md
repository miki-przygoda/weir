# Wire batching (A6) — measured

`PushBatch`/`AckBatch` shipped in 4.0.0 with its throughput **modelled and never
measured**. The design document was explicit about it:

> **~70,000 rec/s, roughly 100× over 653, ±40%** — and refuse a second
> significant figure until measured.
> — `docs/superpowers/specs/2026-09-13-a6-wire-batching-design.md` §9

and, after the work landed:

> These are macOS figures and therefore a **lower bound**; no A6 throughput
> number has been published, because the beast run that would justify one has
> not happened. — §12

This is that run.

## Environment

| | |
|---|---|
| Host | beast — Intel i9-9900K, 16 logical cores, 32 GiB |
| Kernel | Linux 7.0.0-31-generic, `isolcpus=2-7,10-15`, `nohz_full`, `mitigations=off` |
| Storage | ext4 (`rw,relatime`) on a Samsung 850 EVO 250 GB SATA SSD — honest `fdatasync` |
| Governor | `schedutil` (`intel_pstate=disable`) |
| weir | 4.0.0, `--release`, `bench_preset` with `batch_size = 256` |
| Harness | `crates/weir-server/tests/load_batch.rs` |
| Method | 7 passes, median reported, **60 s idle between passes** (see *Why the gap* below) |

Every row is one connection except the two marked concurrent. Payload is 5
bytes, matching `tests/load.rs`'s `single_thread_sync`, which is what makes the
control here comparable to weir's published single-thread figure.

## Results

| Scenario | records/frame | median rec/s | spread | records per fsync | vs. control |
|---|---:|---:|---:|---:|---:|
| `batch_single_durable` *(control)* | 1 | **878** | 1.01x | **1.00** | 1x |
| `batch_durable_n16` | 16 | 9,016 | 1.06x | 14.31 | 10x |
| `batch_durable_n64` | 64 | 29,296 | 1.22x | 35.97 | 33x |
| `batch_durable_n256` | 256 | 108,868 | 1.42x | 153.85 | 124x |
| `batch_durable_n1024` | 1024 | **165,546** | 1.35x | 232.56 | **189x** |
| `batch_single_buffered` *(control)* | 1 | 49,360 | 1.02x | n/a | 1x |
| `batch_buffered_n256` | 256 | **975,492** | 1.07x | n/a | **19.8x** |
| `batch_concurrent_8p_1shard` | 1 | 3,716 | 1.03x | 5.22 | 4.2x |
| `batch_concurrent_32p_1shard` | 1 | 13,494 | 1.04x | 16.58 | 15x |

## The model understated, and by more than its error bar

§9 modelled `batch_size = 256`, one connection, and called it **~70,000 rec/s
±40%** — a band of 42,000–98,000. The matching row, `batch_durable_n256`,
measures **108,868**. The band does not contain it.

The model was built from a per-record write cost of 5–6 µs plus one 1.4–1.5 ms
`fdatasync`, giving 10.5–11.9 µs per record. The measured 108,868 rec/s is
9.2 µs per record, so the per-record write cost on this box is smaller than the
estimate it was built from. §9 was right to refuse a second significant figure;
it was also, in the end, conservative.

**The headline is 189x, not 100x.** Against the same box's own re-measured
control of 878 rec/s — not the 653 §9 quoted from elsewhere — a 1024-record
frame delivers 165,546 rec/s.

## Batched `Buffered`, which had never been measured at all

§9 flagged this as an open gap and corrected an earlier claim in passing: the
`Buffered` figure of 49,725 rec/s had been called a ceiling, wrongly, because it
is a latency reciprocal at one round trip per record rather than a throughput
bound.

Measured: **975,492 rec/s**, against an unbatched `Buffered` control of 49,360
on the same box in the same run — **19.8x**. The correction was right, and the
"ceiling" was low by an order of magnitude. Batching removes the round trip that
figure is the reciprocal of, and nothing else was ever holding it down.

## Concurrency is not a substitute, and the reason is structural

A6's premise is that real deployments do not already coalesce records per fsync.
The gate it had to survive was `0.6 × 256 ≈ 154` records per fsync reachable
without wire batching. Measured on this box:

| Producers on one shard | records per fsync | % of the producer count |
|---:|---:|---:|
| 1 | 1.00 | 100% |
| 8 | 5.22 | 65% |
| 32 | 16.58 | 52% |

The ceiling is not a tuning constant, it is the protocol: the daemon acks frame
N before reading frame N+1 on a connection, so **at most one record per
connection is ever in flight**, and a group fsync can therefore cover at most
`connections-per-shard` records. Beast reaches only half to two-thirds of even
that, and the fraction falls as producers are added.

So reaching 154 records per fsync by concurrency alone needs *at least* 154
concurrent producers pinned to one shard, and on this evidence closer to 300.
One producer using `push_batch` reaches 232 with a single connection. The
premise holds, and it holds for a reason that no amount of deployment tuning
changes.

## What this does not say

- **Not an end-to-end rate.** These are ingest figures against the noop sink.
  §7.5 of the design is explicit that on shipped defaults — `sink_http_batch`
  defaulting to per-record — the end-to-end win is **1.0x**. Batching the wire
  does not make a per-record HTTP sink faster.
- **Not a large-payload result.** 5-byte records measure per-record overhead,
  which is exactly what batching removes; the advantage shrinks as payloads
  grow. See the payload sweep in
  `crates/weir-server/tests/research.rs`.
- **Not a sizing rule.** §8 warns that A6 multiplies the rate at which the WAB
  outruns the single drain thread, against a `wab_max_bytes` that defaults to 0
  (disabled), and says A6 must not ship without a recommended value. It shipped
  without one. These numbers are the numerator of that calculation and not the
  rule itself.

## Why the gap between passes

The first attempt at this capture ran seven passes back to back and was thrown
away. Its `Durable` rows collapsed across the run — 877, 857, 874, 310, 178
rec/s on the control — while `batch_single_buffered` in the *same* passes held
49,585 / 49,155 / 49,138 / 49,015 / 49,183, a spread of 1.01x. A CPU, scheduler
or thermal effect would have moved both; package temperature was 58 °C against a
100 °C critical. What moved was the fsync path alone, from 1.15 ms to 5.6 ms per
call. Two minutes idle restored 887 rec/s.

With 60 s of idle between passes the control's spread is **1.01x** (873–882)
where it had been 4.93x. Every figure above is taken that way.

That transient is not an artefact to be engineered around — it is a property of
the `Durable` tier on this storage that no published weir number currently
accounts for, because every one of them is a short run against a rested disk.
It is being characterised separately, by the sustained-load experiments in
`crates/weir-server/tests/research.rs`.
