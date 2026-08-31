# WAB format v2 under chaos — first coverage, and what it changes

Until this run, weir 2.0's on-disk format v2 had **zero** chaos coverage. Not
the v2 write path, and more importantly not v2 *recovery* — truncating a torn
tail out of a compressed segment.

That gap was invisible rather than known. `wab_compression` defaults to
`"none"`, the harness never overrode it, and `"none"` produces segments
byte-identical to weir 1.x. So the 38.6M acked records and 665 crashes across
the two bare-metal soaks — every figure quoted in
[`2026-08-23-10h-vs-5h-phase1-soak.md`](2026-08-23-10h-vs-5h-phase1-soak.md) —
were all format v1. weir's own config documentation calls enabling compression
a **one-way door**, because a 1.x daemon refuses to read a v2 segment. Highest
risk change in 2.0, least tested.

## Result

| | beast Run A | beast Run B | this run |
|---|---|---|---|
| On-disk format | **v1** | **v1** | **v2 (zstd, level 3)** |
| Venue | bare metal, i9-9900K | bare metal, i9-9900K | container, Docker Desktop |
| Arch | x86_64 | x86_64 | **aarch64** |
| Duration | 10.01h | 5.02h | 3.42h (stopped early, by hand) |
| Episodes (`kill -9`) | 438 | 227 | 111 |
| Acked | 25,266,567 | 13,390,247 | 41,059,463 |
| `i1_missing` | 0 | 0 | **0** |
| `i2_leaked` | 0 | 0 | **0** |
| `i1_exempt` | 0 | 0 | **0** |
| `orphaned_delivered` | 0 | 0 | **0** |
| `ledger_conflicts` | 0 | 0 | **0** |
| ERROR lines | 0 | 0 | **0** |
| Duplicate rate | 1.000 | 1.000 | **1.0089** |

**41,059,463 acked records across 111 crashes at format v2, zero violations.**

`i1_exempt = 0` is the load-bearing zero, as before: I1's frontier exemption can
excuse records the loadgen may not have flushed yet, so a run can post
`i1_missing = 0` while quietly excusing thousands. Nothing was excused here.

The indeterminate-record identity holds exactly, as it did on bare metal:
`pushed − acked = 41,060,356 − 41,059,463 = 893 = unknown`.

## Recovery truncation is identical at v2

The sharpest result. Truncation warnings against kills:

| | Truncations | Kills | Per kill |
|---|---|---|---|
| beast Run A (v1) | 1,752 | 438 | **4.000** |
| beast Run B (v1) | 908 | 227 | **4.000** |
| this run (v2) | 444 | 111 | **4.000** |

`shard_count = 4` throughout. That is **666 kills across two on-disk formats,
two CPU architectures and two storage venues**, every one truncating exactly one
torn tail per shard, never a shard escaping with a clean tail and never one
needing a second pass.

Compression changes how bytes reach the disk. It did not perturb the recovery
discipline by a single event. This is the strongest evidence available that v2
is durability-neutral.

## The daemon log accounts for itself

| Level | Count |
|---|---|
| INFO | 2,740 |
| WARN | 668 |
| **ERROR** | **0** |

WARN decomposes exactly, with nothing left over:

- **444** `truncated mid-length-field` — 4.000 per kill, as above
- **112** `WEIR_SINK_BEARER_TOKEN is unset` — one per daemon start (111 + 1)
- **112** `shard_count/worker_count is significantly above the recommended` —
  the Docker VM exposes 4 CPUs, so `shard_count = 4` reads as over-provisioned.
  Harness/venue configuration, not a weir defect. (beast produced the same
  warning for the same reason, via `isolcpus`.)

`444 + 112 + 112 = 668`. No unexplained residue.

## What v2 DOES change: the shape of redelivery

This is the finding that did not exist before this run, and the one worth
carrying into the release notes.

The duplicate rate is **1.0089** here against a flat **1.000** on both v1 runs.
That is conformant — at-least-once permits redelivery and I2 is clean — but the
distribution is the interesting part. Per-episode redelivery is **bimodal**:

```
 5 spike episodes (>5k redelivered), 50 trickle episodes, 56 completely clean

  ep 14  redelivered=  57,611
  ep 38  redelivered=  30,942
  ep 40  redelivered=  85,909
  ep 57  redelivered=  97,998
  ep 63  redelivered=  90,606

spikes carry 363,066 of 365,433 redeliveries — 99.4%
trickle carries 2,367 — 0.6%
```

**Five episodes out of 111 carry 99.4% of all redelivery.** A crash usually
costs nothing in duplicates; occasionally it costs a large block, around 86,000
records at the median.

That shape is segment-granularity replay: when a kill lands after a segment's
records were delivered but before its `.confirmed` sidecar is durable, the whole
segment replays. Compression means each segment holds far more logical records,
so each such event costs proportionally more. The daemon log is consistent —
**46** `WAB segment rotated` events here against **zero** across the v1 runs.

So the claim for the release notes is narrow and defensible:

> Enabling compression does not affect durability, but it increases the
> redelivery burst size on crash recovery, because a segment holds more records.
> Sinks that dedupe are unaffected. Anyone sizing a dedup window from observed
> duplicate counts should know the distribution is spiky, not flat.

**This is an invariant-class observation, not a rate**, so the virtualised venue
does not undermine it. What the venue *does* prevent is quantifying the cost in
time or bytes.

## Scope limits — read before quoting anything

- **No number with a unit attached is quotable from this run.** Storage is
  virtualised through the Docker Desktop VM. Throughput, latency and
  bytes-per-second here mean nothing about weir. The invariant results are
  venue-robust because the injected fault is a process kill: `kill -9` does not
  lose the page cache, so the torn-tail recovery path reproduces faithfully.
  Power loss is what needs real hardware, and this is not that.
- **The payload is highly compressible** — `{"run":N,"seq":M,"pad":"aaaa…"}`.
  The v2 *format* path is genuinely exercised, but the compression *ratio* here
  is nothing like a realistic workload, and the records-per-segment figure that
  drives the spike size above is correspondingly inflated. The spike *mechanism*
  is real; its magnitude on real data is not measured.
- **Five spikes is a thin sample** for characterising a distribution. A longer
  v2 run is queued to extend it.
- **Stopped by hand at 111 episodes**, not by its deadline. `SIGINT` landed on
  the between-episode sleep, so episode 110 completed and was fully verified and
  the report was written — but the run skipped its **final pass**, the extra
  verification at `frontier_slack = 0` after a full drain. Every per-episode
  verdict stands; the end-of-run drained-state check is simply absent.
- Phase 1 only: the sole fault is a random `kill -9`, and the sole durability
  tier is `Sync`. Nothing here speaks to power loss, torn writes, slow disks,
  `ENOSPC`, or the `Buffered`-vs-`Sync` distinction.

## Reproduction

```bash
cd chaos
./run-docker-soak.sh schedules/soak8h-zstd.toml <label> 4h        # or @07:30
```

`soak8h-zstd.toml` is deliberately `soak5h.toml` with two lines changed —
`wab_compression` and `wab_compression_level` — so "v1 passed, v2 also passed"
is a comparison rather than two unrelated runs.

The launcher generates the schedule it runs into `schedules/generated/`, rather
than passing a command-line override, so the schedule recorded alongside the run
is the one that actually executed.

## Venue

Apple M3 Max, Docker Desktop (4 CPUs, 7.75 GiB) running `linux/arm64` natively,
ext4 on a 2 GiB loop device inside a privileged container. `shard_count = 4`,
`tier = "S"`, 8 MiB segments, 8 load threads, 256-byte records, zstd level 3.

Incidentally the project's first sustained run on `aarch64-unknown-linux-gnu`.

A footnote on why this run was possible at all: the verifier's oracle was
replaced the day before with a dense byte-array representation
([`2026-08-24-dense-oracle-design.md`](../../superpowers/specs/2026-08-24-dense-oracle-design.md)).
At 41M records the previous dict-based accumulator would have needed ~13.8 GB —
well past this VM's 7.75 GiB — and the run would have died around episode 60.
The dense one used 37 MB.
