# Power loss against batched `Durable` acks

## Why this exists

The [first power-loss measurement](2026-08-28-first-power-loss-measurement.md)
held `Durable` to zero loss across 5,311 simulated power cuts. Every one of those
episodes was driven by a load generator whose only producer call was a single
`push`. 4.0.0 added `PushBatch`, whose reply is a bitmap, and a set bit carries
the same promise as a single-record `Ack`: the record is on stable storage and
will replay after a crash. Nothing had cut power on that path.

This run does. It uses a wire batch of 256 against a 64-record group commit, so
one bitmap spans about four fsyncs and a set bit has to survive a commit
boundary rather than mapping onto a single one.

## Run metadata

- **Run id:** `1813189325188119`
- **Venue:** beast, i9-9900K, kernel 7.0.0-31-generic, dm-flakey power-loss
  injection, the same harness and protocol as the
  [first measurement](2026-08-28-first-power-loss-measurement.md#the-fault-is-not-what-the-name-suggests)
- **Date:** 2026-09-26 22:10 to 2026-09-27 03:58 (UTC+1)
- **Commit:** `da6ff74-dirty` (3 tracked files modified), which is
  `v4.0.0-21-gda6ff74`, between 4.0.0 and 4.0.1. The run did not record what the
  three modifications were.
- **Schedule:** [`chaos/schedules/powerloss-batch.toml`](../../../chaos/schedules/powerloss-batch.toml),
  seed `0xA6C47`: `tier = "D"`, 8 producer threads, 256-byte records,
  `wire_batch_size = 256`, `shard_count = 4`, `batch_size = 64`,
  `wab_segment_max_bytes = 8 MiB`, a 2 GiB test device

## Result

| | Batched (this run) | Unbatched ([2026-08-28](2026-08-28-first-power-loss-measurement.md)) |
|---|---|---|
| Power-loss episodes that completed | **1,189** | 5,311 |
| Acked `Durable` records | **239,069,696** | 42,814,591 |
| Acked records per episode (mean) | 201,068 | 8,061 |
| I1 violations (acked, never delivered) | **0** | 0 |
| I2 violations (delivered, never pushed) | 0 | 0 |
| Ledger conflicts | 0 | 0 |
| Nacked | 0 | 0 |
| Canary `bit` | 1,190/1,190 | 5,311/5,311 |
| Deliveries per distinct record | 1.062 | 1.000 |

**No acknowledged `Durable` record was lost in 1,189 completed power cuts on the
batched path.** The canary, a block overwritten while the fault is engaged and
read back after the remount, was destroyed in every episode, so each cut
discarded writes.

Mean steady state was 6.0 s in both runs, so the 25x in acked records per
episode is batching, not longer episodes.

### The 1,190th episode, and why it is excluded

The run's own report reads **FAIL, 1 violation**, and that is not what
happened. After 1,189 passing episodes the 2 GiB test device filled. The daemon
log for episode 1189 shows the sequence:

```
ERROR weir_server::wab::recovery: mid-file WAB corruption detected but the quarantine
copy FAILED (disk full / read-only / inode exhaustion?); refusing to truncate the valid
prefix because the corrupt tail may hold acked-durable records — segment left UNTOUCHED
for retry. Clear the failure and restart. path=/mnt/weir-wab/wab/shard_02/seg_00002996.wab
error=No space left on device
```

1. weir could not copy a torn segment into quarantine, so it **refused to
   truncate it** and left it in place rather than risk dropping acked records.
2. Recovery therefore never finished, the drain never settled, and the episode
   hit its 180 s quiescence timeout with `nonempty_active_segments=1`. It is the
   only one of 1,190 that did not quiesce.
3. The oracle ran I1 against that stalled daemon anyway and counted the 25,577
   records still on disk as lost. The orchestrator then died on `ENOSPC` writing
   the next canary, before the final pass.

The harness's disk floor measured the host filesystem (153 GiB free) and never
the test device. That was fixed in `2fae320` (CHANGELOG 4.0.1, "A full WAB device
manufactured a false durability violation"): a separate floor on the WAB mount
now stops a run **between** episodes, so the final pass and the post-mortem still
run.

**What this does not prove.** The WAB was deleted unread when the stack tore
down, so "preserved, not lost" rests on weir's log line and on the oracle having
run against an unquiesced daemon. It was not confirmed by reading the 25,577
records back. That is why the episode is excluded, not counted as a pass.

### Duplicates

Batching moved duplicate delivery from 1.000 to 1.062. This is at-least-once
behaving as documented: records the sink had committed before the cut, but
whose delivery weir had not yet confirmed on disk, replay after recovery, and
with ~25x more records in flight per episode there is more to replay. A sink
that cannot tolerate duplicates can deduplicate on the batch's `DedupToken`; see
[`architecture.md`](../../architecture.md).

## What this does not establish

- **The current code.** The run predates 4.0.1, the safety floor and the
  Foundations drain changes in 4.1.0. Every one of those touched recovery or the
  drain.
- **A final verification pass.** The run died before it. Nothing here covers
  weir's shutdown drain, and there is no WAB post-mortem.
- **Statistical strength.** Zero violations in 1,189 episodes bounds the
  per-episode violation rate at roughly 0.25% with 95% confidence (rule of
  three), against roughly 0.06% for the unbatched 5,311.
- **Other faults.** Power loss only. No `kill -9`, ENOSPC, slow disk or torn
  writes on this path.

## The run that would settle it

`powerloss-batch.toml` on current `main`, with the test device raised to 16 GiB
as [`powerloss-batch-buffered.toml`](../../../chaos/schedules/powerloss-batch-buffered.toml)
already is. At ~1.7 MiB of unconfirmed WAB per episode, 2 GiB fills around
episode 1,190; 16 GiB clears the 3,000-episode ceiling. A pass needs:

- 0 I1/I2 violations;
- the canary `bit` in every episode;
- a final pass that runs and drains the WAB to empty;
- a duplicate ratio near 1.062.

Disk is the constraint on beast: the ledgers took ~115 bytes per pushed record
here, about 28 GB for this run, and the run directory sits under the chaos
checkout.
