# The `Buffered` loss ceiling is bytes, not records

## Why this exists

The [first power-loss measurement](2026-08-28-first-power-loss-measurement.md)
put a number on what `Buffered` can lose: loss is uniform between zero and a
ceiling, and that run's ceiling was 126,782 records. The batched `Buffered` run
that followed raised throughput 1.45x and the record ceiling did not move
(123,664), so the ceiling is a quantity of data, not a duration.

Both runs used 256-byte records, so neither could say whether that quantity is
records or bytes. This sweep varies **only `record_size`** to separate the two.

## Run metadata

- **Venue:** beast, i9-9900K, dm-flakey power-loss injection, the same harness
  and protocol as the [first measurement](2026-08-28-first-power-loss-measurement.md#the-fault-is-not-what-the-name-suggests).
- **Date:** 2026-09-27, run back to back from one job queue.
- **Code:** the weir 4.0.x tree on beast that day. The queue did not record the
  commit.
- **Schedules:** [`powerloss-recsize-64.toml`](../../../chaos/schedules/powerloss-recsize-64.toml)
  and [`powerloss-recsize-1024.toml`](../../../chaos/schedules/powerloss-recsize-1024.toml).
  The 256 B row is the batched `Buffered` run,
  [`powerloss-batch-buffered.toml`](../../../chaos/schedules/powerloss-batch-buffered.toml).
  All three hold `shard_count = 4`, `wab_segment_max_bytes = 8 MiB`, 8 producer
  threads and a wire batch of 256.

## Result

| `record_size` | episodes | ceiling (records) | ceiling x `record_size` | canary `bit` | violations |
|---|---|---|---|---|---|
| 64 B | 250 | **461,956** | 29.6 MB | 250/250 | 0 |
| 256 B | 747 | 123,664 | 31.7 MB | 747/747 | 0 |
| 1024 B | 146 | **27,084** | 27.7 MB | 146/146 | 0 |

**The record ceilings span 17x. The byte ceilings agree within ±7% of ~30 MB.**
A flat record ceiling of ~124,000 is refuted at both ends, and refuting an upper
bound takes only one observation above it, so the episode counts don't weaken
that half of the result.

Per-episode loss is the difference between consecutive episodes' cumulative
`expected_loss` in each run's `episodes.jsonl`. The 1024 B leg was stopped
early, after 146 episodes, and has no generated `report.md`. Its distribution
moments fit a uniform distribution less well than the other legs' do, and 146
episodes is too few to read that as a change of shape.

## What this does not establish

- **What sets the ~30 MB.** The obvious candidate is the unsynced contents of
  the active segments, one per shard, each capped by `wab_segment_max_bytes`:
  4 x 8 MiB = 33.6 MB, and the three ceilings are 82.7–94.3% of it, always
  below. That fits, but every leg held both factors constant, so this sweep
  can't separate them from any other ~30 MB constant. The kernel's dirty-page
  thresholds are not it: on beast they are 3.13 GiB and 6.26 GiB, about 100x
  and 200x too large.
- **Larger records.** A 4096 B leg was scheduled and never ran.

The falsifying run is a leg with `wab_segment_max_bytes = 4 MiB` and everything
else held: if the hypothesis is right, the ceiling halves to ~15 MB. A second
leg varying `shard_count` would separate the two factors. If both hold, the
operator gets a sizing rule they control, `Buffered` exposure ≤ `shard_count` x
`wab_segment_max_bytes`. Until then, size `Buffered` exposure from a
measurement on your own configuration.
