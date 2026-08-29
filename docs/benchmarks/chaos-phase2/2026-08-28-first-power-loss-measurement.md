# Power loss, measured — the first Buffered-tier result

## Why this exists

weir's headline claim is *"an ack is never a false ack."* Until this run, every
chaos fault ever injected was a `kill -9`, which **does not lose the page
cache**. Five soaks, 100,606,180 acked records, 1,226 crashes, zero violations —
and not one of them touched power loss. The durability tier table in
`docs/wire_protocol.md` described Buffered's exposure in prose and had never
put a number on it.

This run puts a number on it.

## Run metadata

- **Run id:** `1729845351164241`
- **Venue:** beast, i9-9900K, kernel 7.0.0-30-generic, dm-flakey v1.5.0
- **Commit:** `3cc61f1`
- **Schedule:** [`chaos/schedules/powerloss-buffered.toml`](../../../chaos/schedules/powerloss-buffered.toml), seed `0x9F1A1`
- **Duration:** 6.01 h, 706 episodes, 30.6 s/episode
- **WAB format:** v1 (`Compression::None`, which is 2.0.0's default — v2/zstd is opt-in and is **not** covered here)

## The fault is not what the name suggests

`dm-flakey drop_writes` is a **lying disk**: it discards a write while
`fsync` returns 0 ([control experiment](2026-08-22-dm-flakey-control-experiment.md)).
Real power loss does not do that — under real power loss, a write whose fsync
returned *is* durable. So the naive protocol, engage the fault and let weir keep
acking inside the window, manufactures acked-but-missing records at the durable
tier and reports **the harness lying to weir** as a durability violation.

Two further facts about the layering, both found in review before any hardware
run and both fatal to the obvious design:

**`drop_writes` sits BELOW the page cache.** A dropped write completes
successfully, so the kernel marks the page clean — but the page is still
resident and still correct. `kill -9` does not evict it. The daemon would
restart, read the WAB, and get every byte back. **A naive injector cannot lose
a single byte.** The control experiment says as much without following the
implication: it measured *"at block level, no filesystem between the fault and
the observation."* That result is about a **device**; mounting a filesystem on
top inherits none of it.

**`dmsetup suspend` syncs the filesystem it is about to lie to.** Without
`--nolockfs` it flushes every dirty page to the still-honest disk before the
fault installs, so nothing is at risk when it goes live.

The protocol that works:

```
steady load
  -> kill -9                     # dead FIRST: cannot ack into the window
  -> engage drop_writes          # dmsetup suspend --nolockfs
  -> umount                      # its writeback is discarded by the lying disk
  -> disengage
  -> mount                       # ext4 journal replays to the pre-fault state
  -> restart daemon              # recovery runs against an honest disk
  -> drain -> verify
```

Killing first makes the lying window **exactly zero by construction** rather
than merely narrow. What is lost is the data that was dirty at the instant of
death and had never reached the platter — which is the definition of power
loss. The umount/mount cycle is what converts "writes were dropped" into "the
filesystem lost them"; it is what xfstests' `_flakey_drop_and_remount` does,
for the same reason.

## Result

| | |
|---|---|
| Episodes | 706 |
| Acked records | **604,485,602** |
| Canary (I6) | **`bit` in 706/706** |
| I1 violations | **0** |
| I2 leaks | **0** |
| Ledger conflicts | **0** |
| Quiescence failures | **0** |
| No-progress episodes | **0** |
| Final pass | `frontier_slack=0`, `advisory=False`, `clean_stop=True`, 0 anomalies |
| WAB residue | 2,704 quarantined; 0 unconfirmed-sealed, 0 non-empty active, 0 dead-letter |

`frontier_slack=0` on the final pass is the strongest line in the table. The
producer was stopped and both logs were complete, so no exemption was granted
at all — "nothing is missing" is unqualified rather than lenient.

**The canary is what makes the rest meaningful.** A known block is written
before the fault, overwritten while it is engaged, and read back after the
remount. `bit` means the overwrite was destroyed — a direct, per-episode
measurement that the injector fired, taken independently of anything weir did.
Without it, a silently-detached injector produces numbers identical to a clean
pass.

## Buffered exposure is uniform, and bounded

| | records | ≈ production |
|---|---|---|
| min | 0 | — |
| p05 | 3,277 | 0.05 s |
| p25 | 31,870 | 0.44 s |
| p50 | 63,790 | 0.89 s |
| p75 | 93,122 | 1.29 s |
| p95 | 116,534 | 1.62 s |
| p99 | 124,127 | 1.72 s |
| **max** | **126,782** | **1.76 s** |

Mean 62,335, stdev 36,178, over 706 episodes at ~72,000 records/s.

The quartiles are almost evenly spaced, and both moments match a uniform
distribution on `[0, max]`:

| statistic | observed | uniform predicts | error |
|---|---|---|---|
| mean | 62,335 | `max/2` = 63,391 | 1.7% |
| stdev | 36,178 | `max/√12` = 36,599 | 1.2% |

**Buffered loss is uniformly distributed between zero and one writeback
interval.** That is exactly what a kill landing at a uniformly random point in
a periodic writeback cycle produces, and it means the ceiling — not the mean —
is the number that characterises the tier.

> **Buffered's worst observed exposure is 1.76 seconds of acknowledged writes;
> the median is 0.89 seconds.**

Two episodes lost precisely zero while the canary still read `bit`: the fault
fired and there simply happened to be nothing dirty. Those two are why a
Buffered loss of exactly zero is reported as `inconclusive` rather than green —
across a whole run it would mean the injector never bit, but in a single
episode it is a legitimate outcome.

## Quarantine is the expected outcome, at 3.83 per kill

2,704 segments were quarantined across 706 kills. Power loss drops the active
segment's contents; on restart recovery finds a torn tail and parks it in
`quarantine/` rather than silently truncating it — the v1.1.0 Finding 2c
hardening, working. It sits beside Phase 1's **4.000 truncations per kill** as
the power-loss analogue of the same constant.

The harness originally flagged every one of these as an anomaly, and
`exit_code_for()` maps any anomaly to exit 1. A 200-episode run would have
reported "weir is broken" on essentially every episode for behaving exactly as
designed. The rule is now keyed on the **fault**: after `kill_random` a
quarantined segment is still an anomaly, because the page cache survives a
process crash and its presence means something genuinely went wrong.

## What this does NOT establish

Stated plainly, because the report's own prose once got this wrong and claimed
the Durable contract held on a run that contained no Durable records.

- **The Durable tier is untested under power loss.** A run is single-tier —
  `loadgen` takes one `--tier` for the whole run — so 706 episodes at `tier="U"`
  pushed not one Durable record. This run measures Buffered's exposure and the
  injector's reliability. It is not evidence for weir's headline claim.
- ~~WAB format v2 (zstd) is untested under power loss.~~ **Now covered** — see
  the controlled comparison below.
- `error_writes` — fail-closed nacking — is a different fault class, deferred.
- Slow disk, `ENOSPC`, read-only remount, dead-letter exhaustion: Phase 3.
- Single host, single filesystem, single kernel.

## Harness defects found along the way

Eight, and **not one weir defect** in 604,485,602 acked records across 706
power losses.

| | |
|---|---|
| `bed9143` | Two dm tests passed only because macOS has no `/dev/loop7`; on Linux they exercised the opposite branch from the one their names claimed |
| `148ba88` | Quarantined segments flagged as anomalies, turning correct recovery into exit 1 |
| `9fdc88c` | Both schedules said `episodes = 200` under a cap that stops at ~96 |
| `7e235f3` | The 45 s/episode overhead figure was a measurement error — it charged a once-per-run final pass to every episode |
| `3cc61f1` | No disk floor; a full disk truncates `delivered.log` and **manufactures false I1 violations** |
| `07e0b77` | Orphans blamed on a stale log, which the harness makes impossible; the real cause is ledger flush lag, and all 20 occurrences self-resolved |
| `9c17fae` | The report claimed the Durable tier held on a run with no Durable records, and put a cumulative column in a table of deltas |

The pattern in the first and the last is the same one Phase 1 recorded: a
statement that is true for a reason that makes it meaningless. A test that
passes because the dev machine lacks a device; a contract that "held" because
it was never exercised.


## Addendum, 2026-08-29 — WAB v2 (zstd) under power loss

A 40-episode controlled comparison against
[`powerloss-shape-check.toml`](../../../chaos/schedules/powerloss-shape-check.toml),
verified programmatically to differ in `weir.wab_compression` and nothing else.
Buffered on purpose: it produces ~3.8 quarantined segments per kill where
Durable produces zero, so it is the tier that actually stresses the recovery
path this was written to test.

**Correctness is unaffected.**

| | v1 (`none`) | v2 (`zstd`) |
|---|---|---|
| Canary | 40/40 `bit` | 40/40 `bit` |
| I1 / I2 / conflicts | 0 / 0 / 0 | 0 / 0 / 0 |
| Quarantined per kill | 3.90 | 4.00 |
| Final pass | `slack=0`, 0 anomalies | `slack=0`, 0 anomalies |

Recovery parses a torn **zstd frame** exactly as reliably as a torn plain
record, which was the specific risk: a decoder that rejects malformed input
rather than returning short data is where a recovery bug would have lived.

**But the exposure changes shape and size.**

| | v1 | v2 |
|---|---|---|
| p50 | 70,747 | 118,146 |
| p75 | 90,513 | 377,707 |
| max | 124,897 (~1.8 s) | **449,876 (~5.7 s)** |
| mean | 65,419 | 198,580 |
| distribution | uniform | **bimodal** |

The v1 loss is uniform: stdev 33,985 against `max/√12` = 36,055. The v2 loss is
not, and the split is unambiguous — **no episode of 40 landed between 127,711
and 307,263**, a gap of 179,552 records:

| mode | episodes | mean | range |
|---|---|---|---|
| low | 23 | 62,999 | 2,362 – 127,711 |
| high | 17 | 382,012 | 307,263 – 449,876 |

**The low mode reproduces the uncompressed distribution almost exactly** (v1
mean 65,419, max 124,897). Compression packs far more records behind each
unflushed byte, so when a cut catches the larger unit it takes proportionally
more records with it.

**Do not plan with the multiplier.** The load generator's payload is
`{"run":N,"seq":M,"pad":"aaa…"}`, which is far more compressible than real
data — the same caveat the 2026-08-25 v2 soak recorded when it found
compression shifts redelivery *variance*. The mechanism is demonstrated; its
magnitude on a realistic workload is unmeasured.
