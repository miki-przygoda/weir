# Power loss against batched `Durable` acks

## Why this exists

The [first power-loss measurement](2026-08-28-first-power-loss-measurement.md)
held `Durable` to zero loss across 5,311 simulated power cuts. Every one of those
episodes was driven by a load generator whose only producer call was a single
`push`. 4.0.0 added `PushBatch`, whose reply is a bitmap, and a set bit carries
the same promise as a single-record `Ack`: the record is on stable storage and
will replay after a crash. Nothing had cut power on that path.

Two runs do. Both use a wire batch of 256 against a 64-record group commit, so
one bitmap spans about four fsyncs and a set bit has to survive a commit
boundary rather than mapping onto a single one. The first (2026-09-26) ran on
code from before 4.0.1 and ended when its test device filled. The second
(2026-10-30) reran the same schedule and seed on 4.1 `main` with a larger
device, and is the one that covers the code weir ships today.

## Run metadata

### Run 2: 4.1 `main` (2026-10-30)

- **Run id:** `1813189325188119` (the id is derived from the seed, so both runs
  share it; they ran in separate checkouts)
- **Venue:** beast, i9-9900K, kernel 7.0.0-38-generic, same harness and protocol
- **Date:** 2026-10-30 11:21 to 16:22 UTC
- **Commit:** `c3a1a76-dirty`, 4.1.0 `main`. The one modification is the
  schedule's `max_duration_secs`, lowered from 28800 to 18000 to fit a 5 h window
- **Schedule:** the same file and seed, with `size_mb = 16384` (a 16 GiB device)
- **Stopped by:** its 5 h deadline, between episodes, followed by the final pass

### Run 1: between 4.0.0 and 4.0.1 (2026-09-26)

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

| | Run 2, 4.1 `main` | Run 1, pre-4.0.1 | Unbatched ([2026-08-28](2026-08-28-first-power-loss-measurement.md)) |
|---|---|---|---|
| Power-loss episodes that completed | **779** | 1,189 | 5,311 |
| Acked `Durable` records | **64,712,499** | 239,069,696 | 42,814,591 |
| Acked records per episode (mean) | 83,071 | 201,068 | 8,061 |
| I1 violations (acked, never delivered) | **0** | **0** | 0 |
| I2 violations (delivered, never pushed) | 0 | 0 | 0 |
| Ledger conflicts | 0 | 0 | 0 |
| Nacked | 0 | 0 | 0 |
| Canary `bit` | 779/779 | 1,190/1,190 | 5,311/5,311 |
| Deliveries per distinct record | 1.053 | 1.062 | 1.000 |
| Final pass at zero slack | **passed** | did not run | passed |
| Anomalies | 1 (see below) | 1 (excluded episode) | 0 |

**No acknowledged `Durable` record was lost in 1,968 completed power cuts on the
batched path, 779 of them on 4.1 `main`.** The canary, a block overwritten while
the fault is engaged and read back after the remount, was destroyed in every
episode, so each cut discarded writes.

Mean steady state was 6.0 s in all three runs, so the extra acked records per
episode come from batching, not from longer episodes.

### Run 2's final pass

After the deadline the harness stopped the load generator (exit 0, ledger
complete), scraped `/metrics` with the daemon still alive, let weir's shutdown
drain finish (clean stop) and verified at `frontier_slack = 0`: every acked
record delivered, nothing delivered that was not pushed. The WAB post-mortem
found no unconfirmed sealed segment, no non-empty active segment and no
dead letters. It found 88 files in `quarantine/`, covered below.

### Run 2's anomaly: episode 517

Episode 517 timed out after 180 s waiting for the drain to settle, with
`stranded(0) != resumed(1)` as the only unmet condition. Its records were
accounted for (`ok: true`), and so were the next episode's. The daemon log shows
what happened:

```
14:28:52.588  weir serving ... recovery_quarantined=2 replay_segments=1
14:29:22.579  drain: sink recovered; re-queued stranded segment(s) for delivery resumed=1
14:29:24.920  drain: segment not found; treating as already-consumed path=.../shard_03/seg_00000928.wab.sealed
```

Recovery replayed one segment. Thirty seconds later the sink's health went from
down to up, and the recovery-edge rescan re-queued a segment that this process
had never stranded. That was the segment replay had already delivered, so the
drain found it gone and skipped it. `resumed_total` counted it anyway. The
counter's own documentation says it "may transiently OVER-COUNT at a
recovery-edge rescan", but both counters are per process and only go up, so in
that process the two could never be equal again. The harness's quiescence check
demands equality, so it waited out the 180 s.

This is a disagreement between the harness and a documented metric property,
not a durability failure. One of the two should change: the check could stop
treating the counters as exact, or the rescan could stop counting a segment that
was never stranded. That is tracked separately; nothing here depends on it.

### Run 2's quarantine

88 segments ended in `quarantine/`, 168 MB in all, at a rate of about one
per nine cuts. The reasons weir logged are torn tails ("shorter than segment
header", "zero length prefix ... trailing byte(s)"): writes a power cut destroyed
before any fsync covered them. weir sets such a segment aside rather than
deleting it, and truncates the live segment to its last valid record. With 0 I1
violations, no acked record was only in a quarantined segment. Run 1's WAB was
deleted unread, so there is no figure to compare against.

### Run 2's throughput

Acked records per second of steady state ranged from about 26,000 in the first
hour to between 1,400 and 11,500 later. They did not fall steadily: the rate
dropped to 9,200 around 12:39, came back to 20,000, then fluctuated through the
afternoon. The producer's push rate followed the ack rate closely, so the time
went into waiting for acks, which is fsync latency. Two things outside weir fit:
the [sustained-load study](../sustained-load.md) traced a similar oscillation to
this SATA SSD's fsync behaviour, and three other agent sessions plus another
project's test gates started on beast's shared SSD at about 13:00. This run did
not separate the two, and it is not a performance measurement. Correctness does
not depend on it, but it is why Run 2 completed 779 cuts in 5 h where Run 1
completed 1,189 in under 6.

The WAB did not accumulate as it did in Run 1. At episode 174, 31 MB of the
16 GiB device was in use, where Run 1's ~1.7 MiB of unconfirmed WAB per episode
would have reached about 300 MB. The 4.1 drain kept up. That is one reading, not
a series.

### Run 1's 1,190th episode, and why it is excluded

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

## Against the pass criteria set after Run 1

| Criterion | Run 2 |
|---|---|
| 0 I1/I2 violations | met |
| Canary `bit` in every episode | met, 779/779 |
| A final pass that runs and drains the WAB to empty | met: no unconfirmed or active segment left. The 88 quarantined files are torn tails, not undelivered data |
| A duplicate ratio near 1.062 | met, 1.053 |

## What this does not establish

- **Statistical strength on current code.** Zero violations in 779 episodes
  bounds the per-episode violation rate at roughly 0.39% with 95% confidence
  (rule of three). Across both runs, 1,968 episodes bound it at about 0.15%, but
  the runs tested different code. The unbatched 5,311 bound it at about 0.06%.
- **A clean exit.** Run 2 exited 1 because of the episode 517 anomaly. The
  anomaly is explained above, but the check behind it still needs fixing.
- **Throughput.** Run 2 shared its SSD with other work. Its ingest rates say
  nothing about weir's performance.
- **Other faults.** Power loss only. No `kill -9`, ENOSPC, slow disk or torn
  writes on this path.

Disk is the constraint on beast: the ledgers took about 65 bytes per pushed
record in Run 2 (4.3 GB) and about 115 in Run 1 (about 28 GB), and the run
directory sits under the chaos checkout.
