# Phase 1 soak — 10h and 5h on bare metal, compared

Two long `kill -9` soaks against the same weir binary on the same host, run
back to back on 2026-08-22/23. Both clean. This is the first evidence weir's
durability claims hold over **hours** rather than the 30 minutes the Phase 1
gate measured, and the first run of that gate on real hardware rather than in a
container on virtualised storage.

**Read the scope limit first.** Both runs are Phase 1: the only fault injected
is a random `kill -9`, and the only durability tier exercised is `Sync`. Neither
run says anything about power loss, torn writes, slow disks, `ENOSPC`, or the
`Buffered`-vs-`Sync` distinction. See
[`../chaos-phase2/`](../chaos-phase2/2026-08-22-dm-flakey-control-experiment.md)
for where that work stands.

## Result

| | Run A | Run B |
|---|---|---|
| Duration | 10.01h | 5.02h |
| Episodes (`kill -9`) | 438 | 227 |
| Seed | `0xB0A75` | `0x5A0C5` |
| **Violations** | **0** | **0** |
| **Anomalies** | **0** | **0** |
| Acked | 25,266,567 | 13,390,247 |
| Distinct delivered | 25,268,317 | 13,391,155 |
| Nacked | 0 | 0 |
| Duplicate rate | 1.000 | 1.000 |

**38.6 million acked records across 665 crashes, zero durability violations.**

## The invariants, and the one that matters most

| Invariant | Meaning | Run A | Run B |
|---|---|---|---|
| `i1_missing` | acked record never delivered — the crown invariant | 0 | 0 |
| `i2_leaked` | nacked record delivered anyway | 0 | 0 |
| `orphaned_delivered` | delivered with no ledger provenance | 0 | 0 |
| `ledger_conflicts` | ledger disagreed with itself | 0 | 0 |
| **`i1_exempt`** | **would-be I1 hits excused by the frontier exemption** | **0** | **0** |

`i1_exempt = 0` is the load-bearing number and it is not in either report's
headline. I1 has an escape hatch: records the load generator may not have
flushed to its ledger yet are excused rather than counted as losses. A run can
post `i1_missing = 0` while quietly excusing thousands, which is a far weaker
claim than it appears. Across 665 episodes, **nothing was excused** — every
acked record was held to the invariant with no relief. That is what makes "0
violations" a result rather than a formality.

## Indeterminate records

`pushed − acked = unknown` holds exactly in both runs (4,303 and 2,291). These
are records in flight when the kill landed, whose ack never came back. Of them,
1,750 (A) and 908 (B) still reached the sink; the rest did not. Both fates are
conformant — that is what "indeterminate" licenses — and it is why distinct
delivered legitimately *exceeds* acked in both runs.

## No degradation over time

The question a soak exists to answer. Throughput per second of load time, by
quarter of each run:

| | Q1 | Q2 | Q3 | Q4 | drift |
|---|---|---|---|---|---|
| Run A (10h) | 843.0 | 855.0 | 855.0 | 851.0 | **+0.95%** |
| Run B (5h) | 889.0 | 885.2 | 891.5 | 891.1 | **+0.23%** |

Flat in both, marginally upward in each. No leak signature, no fragmentation
cost, no slow decay. Ten hours of continuous crash-and-recover leaves throughput
where it started.

Note this is measured *within* each run, which is a stronger test for
time-dependent decay than comparing the two runs to each other: it holds the
venue fixed and only varies elapsed time.

## Recovery behaves identically every single time

The sharpest structural signal in either run:

| | Truncation warnings | Episodes | Per kill |
|---|---|---|---|
| Run A | 1,752 | 438 | **4.000** |
| Run B | 908 | 227 | **4.000** |

`shard_count = 4`. Every kill caught all four shards mid-write, and recovery
truncated each back to its last valid record — never a shard escaping with a
clean tail, never one needing a second pass, across 665 independent crashes on
two different kill sequences. The *precision* is the finding: a ragged ratio
would point at uneven load distribution or nondeterministic recovery. This is
what a durability harness is supposed to be able to say.

## Zero errors

| | INFO | WARN | ERROR |
|---|---|---|---|
| Run A (10h) | 10,531 | 2,191 | **0** |
| Run B (5h) | 5,467 | 1,136 | **0** |

Every warning falls into the two benign classes above: the per-shard truncations
and one bearer-token advisory per daemon start (439 and 228 = episodes + 1).

Neither run recorded a real dead-letter event. A naive grep for "dead-letter"
returns one hit per daemon start — every one is the bearer-token advisory's own
wording, which happens to mention dead-lettering. Grep the level, not the noun.

## An unexplained 4.5% and why it is not called a speed-up

Run B sustained **889.2 acked/s** of load time against Run A's **851.1** — 4.5%
higher, with a byte-identical binary and a config differing only in seed,
episode ceiling and duration.

It is systematic, not drift: Run B sits near 889/s in *every* quartile while Run
A sits near 851/s in every quartile, so it is a property distinguishing the two
runs rather than something that developed inside either. Uncontrolled candidates,
none of them tested: thermal state (A ran overnight, B started ~40 minutes after
A finished), page-cache warmth, disk occupancy after A left 1.5 GB behind, and
kill timing relative to segment rotation under a different seed.

Recording it as an open question rather than a measurement. Two runs cannot
separate those hypotheses, and calling either run "faster" would be inventing a
cause for a difference this venue is not controlled enough to attribute.

## What these runs did NOT exercise

Worth stating plainly, because a clean 38-million-record result invites
over-quoting:

- **The 2.0 quarantine path was never entered.** Residue came back
  `quarantined = 0, dead_letter = 0` in both runs. That is not a pass: `kill -9`
  truncates *cleanly*, and quarantine exists for mid-file corruption. The
  quarantine tooling shipped in 2.0 has no coverage here.
- **The WAB cap and growth warning almost certainly never fired** — 8 MiB
  segments against a 2 GiB volume with the drain keeping up.
- **`Buffered` and `Unbuffered` are untested.** Both runs are `Sync` only, which
  is also what makes them comparable to the original Phase 1 gate.
- **Not a controlled comparison against that gate.** It ran on a different venue
  *and* different code, so the duplicate-rate difference (1.006 there, 1.000
  here) is an observation, not a measurement.

## Venue and reproduction

Both runs: Intel i9-9900K, kernel 7.0.0-28-generic, 31 GiB RAM, ext4 on a 2 GiB
loop device, `shard_count = 4`, `tier = "S"`, 8 MiB segments, 8 load threads,
256-byte records. The host is tuned for measurement (`isolcpus=2-7,10-15`,
`nohz_full`, `mitigations=off`, `intel_pstate=disable`), so runs are launched
under `taskset -c 0-15`; without it weir sees 4 cores and warns that
`shard_count = 4` is over-provisioned.

Each run's own `SETUP.md` — written into its directory while it started —
records the commit and dirty state, sha256 of all three binaries, the boot
cmdline, and the schedule verbatim. On a dirty tree those hashes are the only
exact identity a run has.

```bash
cd chaos
./run-soak.sh schedules/soak10h.toml soak10h 39600   # 10h, 11h backstop
./run-soak.sh schedules/soak5h.toml  soak5h  21600   # 5h,  6h backstop
```

`max_duration_secs` stops a run *between* episodes, so it always ends in a
verified state and still runs the final pass, writes its report, and tears down
the loop device and mount. Both runs stopped that way, at 10.01h and 5.02h, with
no stray devices left behind.
