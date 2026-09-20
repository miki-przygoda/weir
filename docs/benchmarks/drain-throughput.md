# Drain throughput — the first delivery-side numbers

**Date:** 2026-09-05 (Linux) · 2026-09-15 (mac re-measured on the corrected basis) · **Version:** 2.0.5 (Linux rows) · 3.0.0 (mac rows) · **Suite:** `crates/weir-server/tests/load_drain.rs`

Until this release every published weir number described **ingest**: how fast the
daemon accepts records and gets them into the WAB. The load suite's own module
doc said so, under "Coverage caveats", and named this file's suite as the
follow-up.

That gap mattered because weir is a *buffer*. Ingest throughput says how fast the
buffer fills. Nothing said how fast it empties, which is the half that decides
whether it ever does.

These are the first delivery numbers the project has.

## Environments

Three configurations, five consecutive runs each. The point of measuring more
than one is that the differences between them are larger than anything within
them — see [What the storage does](#what-the-storage-does).

| | **mac** | **linux-ssd** | **linux-tmpfs** |
|---|---|---|---|
| Machine | Apple M3 Max, 16 cores | Intel i9-9900K, 8c/16t (4 cores visible — `isolcpus`) | same box |
| OS | macOS 26.6.2 | Ubuntu 24.04, kernel 7.0.0-30 | same |
| Toolchain | rustc 1.97.1 | cargo 1.94.0 | same |
| WAB storage | APFS, internal SSD | ext4 on Samsung 850 EVO (SATA) | tmpfs (`/dev/shm`) |
| `sync_all` becomes | `F_FULLFSYNC` | `fdatasync` to a SATA SSD | a memory write |
| Result line reports | `"wab":"default"` | `"wab":"default"` | `"wab":"external"` |

Common to all three:

| | |
|---|---|
| Sink | in-process HTTP/1.1 mock on loopback, answers in microseconds |
| Records | 256 bytes, `Durability::Buffered` |
| Segments | 64 KiB (`wab_segment_max_bytes`), 1 s idle seal |
| Daemon | `bench_preset`: 4 shards, 4 workers, ingest batch 64 |
| Sharding | **Inert in this suite.** Shard is assigned per *connection* (`crates/weir-server/src/socket/mod.rs:210`) and the fill opens one client (`tests/load_drain.rs:374-379`), so every record lands on one shard whatever `shard_count` says. |
| Sink batch size | `sink_max_batch_size` = 100, the default (`crates/weir-server/src/config/mod.rs:721`), never overridden here. The "batch 64" above is the *ingest* coalescing knob — a different setting. |
| Timed window | **From the first delivered record to the target count, with the numerator counted over the same interval** — see [the 2026-09-12 correction](#2026-09-12--the-rows-above-were-computed-on-two-different-bases). The target is **the sealed half of the fill**, `bulk(n) = n/2` (`tests/load_drain.rs:350-360`): only whole rotated segments seal, so timing the tail would fold up to a second of idle-seal timer into a throughput number. Each scenario separately asserts the full count arrives, so the tail is checked for correctness without polluting the rate. |

## 2026-09-12 — the rows above were computed on two different bases

Every rate below this line is on a **superseded basis**, and the three scenarios
did not share it.

`drain_throughput_http_{per_record,ndjson}` started the clock at the first
delivered record but divided by the full `bulk(RECORDS)`, counting records
delivered *before* the window inside it. `drain_under_slow_sink` never adopted
that helper: it started the clock at the moment the sink was let through, so its
window included the drain's exponential retry backoff — time in which no record
is delivered at all. The first error overstates, the second understates, and the
cross-scenario ratios further down compare across both.

`tests/load_drain.rs` now computes the numerator where the window's endpoints are
observed, and every row emits `excluded_before_t0` and `wakeup_ms` so it states
its own basis.

**What the correction is worth, measured.** On beast — Intel Core i9-9900K
(8c/16t), 31 GiB, Linux 7.0.0, ext4 `rw,relatime` on a Samsung SSD 850 EVO
250 GB (SATA, non-rotational), honest `fdatasync` — three iterations of v3.0.0, and the pre-fix number recovered exactly from the
same runs' `wall_ms` + `wakeup_ms` + the known `bulk()` constant, which makes
the comparison paired rather than run-against-run:

| Scenario | pre-fix (3 runs) | post-fix (3 runs) | run-to-run spread |
|---|---|---|---|
| `drain_http_per_record` | 30,623 / 30,635 / 27,139 | 31,580 / 31,600 / 31,803 | 1.13x → **1.007x** |
| `drain_slow_sink_1ms_conc16` | 8,475 / 9,804 / 8,621 | 11,008 / 11,012 / 11,034 | 1.16x → **1.002x** |
| `drain_http_ndjson` | 67,842 / 106,358 / 118,710 | 80,385 / 86,056 / 100,518 | 1.75x → 1.25x |

What the Environments table above means by "4 cores visible — `isolcpus`", since
it decides how these numbers should be read. The kernel boots with
**`isolcpus=2-7,10-15`**, so twelve of the sixteen logical CPUs are held out of
the default scheduler and an unpinned process is confined to four (`0,1,8,9`) —
which is why `nproc` reports 4 while `nproc --all` reports 16. The part worth
spelling out is the interaction: weir's worker pool pins from core 2 upward
(`WORKER_CORE_START`), i.e. **into** the isolated range, so on this box the
daemon's workers run on cores the scheduler is holding empty for them while the
test harness and client share the four that are left. That is a deliberately
tuned rig, not a stock machine, and it is a reason to prefer beast for isolating
daemon cost — not a caveat against it.

One disclosure this file has not carried: the **drive's write-cache state
requires sudo and is undisclosed**. It does not affect the `Buffered` fill path
measured here, but `environments.md` asks for the storage and its fsync
primitive, and "undisclosed" is the honest answer rather than an omission.

The reconstruction checks out against this file's own history: the pre-fix
`drain_slow_sink_1ms_conc16` median of 8,621 sits beside the `linux-ssd` median
of **8,826** published in an earlier revision from a different release. So that
row understated by about 25% — post-fix the same scenario on the same box was
11,012.

**That prediction has since been measured.** The 2026-09-18 re-measure puts
`drain_slow_sink_1ms_conc16` on linux-ssd at a five-run median of **11,042**,
against the 11,012 predicted here — 0.3% apart, and +25.1% on the row it
replaces. The reconstruction was right about both the direction and the size.

**The gain is precision, not accuracy.** Post-fix `slow_sink` reports `wall_ms`
45, `delivered_records` 498 and `excluded_before_t0` 16 — identical on all three
runs; only `wakeup_ms` moved (14, 6, 13). The delivery is deterministic and the
old basis was folding 6–14 ms of unrelated backoff into a 45 ms window. Rows that
moved 13–16% between identical runs now move 0.2–0.7%, which is the difference
between a suite that can detect a regression and one that cannot.

**The numerator error was small, and a prior claim about it was wrong.** The
2026-09-05 sweep put it at "~33% overstatement ... worst at the fast end, so it
*flattens* curves", from `bulk / (bulk - excluded)` = 1000/751. Its 249-of-1,000
observation reproduces exactly, but the sink also *overshoots* the target
between 1 ms polls by a similar amount — head exclusion and tail overshoot are
one batch-granularity effect with opposite signs. Measured in the window across seven
runs: 847-1045, not 751 — an error from **-4.3% to +18.1%**, scattered either
side of zero rather than a systematic overstatement. See the table below.

**Across three hosts and seven runs, the numerator error is scattered, not
systematic.** Every post-fix observation collected on 2026-09-12 — macOS (2),
beast (3), CI (2):

| Scenario | `excluded_before_t0` | `delivered_records` | what the old numerator did |
|---|---|---|---|
| `drain_http_ndjson` | 200–249 | 847–1045 | **−4.3% to +18.1%** |
| `drain_http_per_record` | 1–19 | 1000–1074 | −6.9% to 0.0% |
| `drain_slow_sink_1ms_conc16` | 1–16 | 498–513 | −2.5% to +0.4% |

Both quantities take a small set of values rather than a stable one, because
both are governed by the 100-record sink batch: NDJSON delivers in 100-record
POSTs, so a large and variable slice lands between two 1 ms polls at each end of
the window, while per-record delivery puts almost nothing there (1–19 records).
That is why the exclusion is big only on NDJSON — and why it does not become a
33% overstatement: the overshoot past the target offsets it by a similar,
independently varying amount, so the net swings either side of zero.

So the sweep's observation was sound and its inference was not. Deriving the
error from the excluded head alone gives 1000/751 = 1.33x; measuring both ends of
the window gives a spread from −4.3% to +18.1% on the same scenario, centred near
zero. A correction that large and that one-directional was never there to find.

Two further notes on reading the table. `drain_slow_sink_1ms_conc16` reports
`wall_ms` 45 on beast and 45–47 on CI: a 1 ms-delay sink at concurrency 16 is
rate-limited largely independently of the host, which is why removing the retry
backoff from its window collapsed its spread so sharply. And CI's NDJSON window
is 4–5 ms, *narrower* than beast's 8–11 ms, with a correspondingly higher rate
(195,586–234,123). That is not a faster drain: the confirm path calls `sync_all`,
an honest `fdatasync` to a SATA SSD on beast and much cheaper on virtualised
runner storage — the same reason every row here records `wab_backing()`. It makes
the quantisation worse on CI, not better.

**NDJSON remains unfit for a Linux conclusion**, and this fix does not change
that. Its window is 8–11 ms against a 1 ms poll — about eight ticks — with
`excluded_before_t0` of 200–249 and overshoot quantised to the 100-record sink
batch. At `RECORDS = 2000` the scenario is measuring quantisation on a box where
the drain is this fast, and it should be resized before the NDJSON-concurrency
question is answered on it.

## Results

**These supersede the figures first published on 2026-09-02, which were between
3.4x and 5.4x too low.** They were not measuring the drain. Two harness faults,
both since fixed:

1. **The clock started at the wrong moment.** Timing began when the mock sink was
   flipped healthy — but the drain was asleep in its retry backoff, and woke
   8-38 ms later. That sleep sat inside a window of roughly the same size, and it
   varied run to run. It is now excluded: the window opens at the *first
   delivered record*.
2. **`bulk()` asked for more than was sealed.** A 256-byte record costs ~270
   bytes framed, so a 64 KiB segment holds ~242 and only whole rotated segments
   seal. At 1,000 records that is ~726 sealed, but the wait asked for 750 — so it
   blocked on the 1 s idle-seal timer. `drain_slow_sink` was returning
   1,004/1,010/1,010 ms: three digits of timer, not of drain.

A third, found by running on Linux for the first time: **`WEIR_BENCH_WAB_DIR`
had never worked.** The knob this file told readers to use to separate drain
cost from filesystem cost panicked every scenario before a record was pushed —
`WeirServerBuilder::wab_dir` requires the caller to create the directory and
`load_drain.rs` did not. The `linux-tmpfs` column below is the first data it has
ever produced.

Median of five consecutive runs, with the full range.

**Single basis, as of 2026-09-18.** The `mac` rows were re-measured on
2026-09-15 and the six Linux rows on 2026-09-18, both on the corrected basis
described in
[the 2026-09-12 correction](#2026-09-12--the-rows-above-were-computed-on-two-different-bases).
Nothing in this table is on the superseded basis any more, and every derived
figure below has been recomputed from these medians rather than carried
forward.

| Scenario | | min | **median** | max | spread | basis |
|---|---|---:|---:|---:|---|---|
| `drain_http_ndjson` | mac | 41,867 | **44,928** | 46,907 | 1.12x | current |
| | linux-ssd | 96,127 | **98,098** | 101,915 | 1.06x | current |
| | linux-tmpfs | 246,566 | **249,288** | 251,823 | 1.02x | current |
| `drain_http_per_record` | mac | 21,279 | **23,746** | 29,467 | 1.38x | current |
| | linux-ssd | 30,649 | **31,298** | 31,733 | 1.04x | current |
| | linux-tmpfs | 37,072 | **37,835** | 38,151 | 1.03x | current |
| `drain_slow_sink_1ms_conc16` | mac | 7,715 | **7,990** | 8,250 | 1.07x | current |
| | linux-ssd | 10,205 | **11,042** | 11,074 | 1.09x | current |
| | linux-tmpfs | 11,299 | **11,654** | 11,849 | 1.05x | current |

All figures rec/s. Ingest of the 2,000-record backlog, for scale: **176 ms** on
mac, **77 ms** linux-ssd, **41 ms** linux-tmpfs.

### What re-measuring the mac column changed

Only one of the three scenarios moved for the reason the correction predicts:

| Scenario | superseded median | current median | change |
|---|---:|---:|---|
| `drain_slow_sink_1ms_conc16` | 4,977 | **7,990** | **+60.5%** |
| `drain_http_per_record` | 24,381 | **23,746** | −2.6% |
| `drain_http_ndjson` | 40,710 | **44,928** | *not comparable* |

`drain_slow_sink` is the scenario that never adopted the shared helper: its clock
started when the sink was let through, so the drain's retry backoff sat inside
the window. On this machine that backoff is `wakeup_ms` ≈ 30 against a window of
≈ 62 ms, so roughly a third of the old window was time in which nothing was
delivered. **+60.5% is the size of that mistake on mac** — larger than the
+28% the same scenario showed on beast, because beast's wakeup is a smaller
fraction of its window. The old row understated; it did not overstate.

`drain_http_per_record` barely moved, which is the expected result: it already
started its clock at first delivery, so the only correction available to it was
the numerator, and §"The numerator error was small" measured that at −4.3% to
+18.1% scattered either side of zero.

**`drain_http_ndjson` is not comparable to its superseded row**, and the table
says "not comparable" rather than a percentage for that reason. Commit `24767fe`
resized the scenario from 2,000 to 20,000 records on 2026-09-13 — the old
version was measuring its own poll quantisation, since NDJSON delivers in POSTs
of `sink_max_batch_size` (100) while `await_delivery` polls on 1 ms. The two
numbers describe different experiments, not the same experiment on two bases.

### What re-measuring the Linux columns changed

The same correction, applied to the same suite on a different machine, lands the
same way — which is the useful part, because it was a prediction. The mac
re-measure said `drain_slow_sink` was the scenario the old basis understated,
by folding retry backoff into the window, and that the other two would barely
move:

| Scenario | | superseded | current | change |
|---|---|---:|---:|---|
| `drain_slow_sink_1ms_conc16` | linux-ssd | 8,826 | **11,042** | **+25.1%** |
| | linux-tmpfs | 7,570 | **11,654** | **+53.9%** |
| `drain_http_per_record` | linux-ssd | 28,719 | **31,298** | +9.0% |
| | linux-tmpfs | 37,991 | **37,835** | −0.4% |
| `drain_http_ndjson` | linux-ssd | 105,909 | **98,098** | *not comparable* |
| | linux-tmpfs | 238,573 | **249,288** | *not comparable* |

`drain_slow_sink` moved most on both machines and in the same direction — the
old rows understated it, they did not overstate it. `drain_http_ndjson` is
marked not comparable here for the same reason as the mac row: `24767fe` resized
it from 2,000 to 20,000 records on 2026-09-13, after these superseded rows were
taken, so the two numbers describe different experiments.

**The dispersion improved more than the medians moved.** Run-to-run spread on
these six rows went from 1.04–1.48x to 1.02–1.09x. Part of that is the corrected
window and part is the machine — these were taken on an otherwise idle beast
with `isolcpus`, where the superseded set was not — but the effect is that the
suite can now resolve differences it previously could not, which is what makes
the next paragraph possible.

**One conclusion flips.** This file previously said `drain_slow_sink` "shows no
storage sensitivity at all", on the grounds that its tmpfs range *contained* its
ext4 range. On the corrected basis the two ranges are **disjoint** — ext4
10,205–11,074 against tmpfs 11,299–11,849 — so there is a real, if small, storage
effect of about 1.06x. It remains by far the least storage-sensitive of the
three scenarios, which is the substance of the original claim; what is withdrawn
is the stronger word "at all".

### Conditions, disclosed

The first attempt at this re-measurement was thrown away. The machine had been
running the repo's own `deploy/monitoring` compose stack since 2026-09-13 —
including a loadgen pushing ≈ 59 rec/s into a containerised weir — for about two
days. Its effect is visible and worth recording, because it is the kind of thing
that quietly ruins a benchmark:

| Scenario | spread with loadgen | spread after stopping it |
|---|---|---|
| `drain_http_ndjson` | 1.85x | **1.12x** |
| `drain_http_per_record` | 1.90x | **1.38x** |
| `drain_slow_sink_1ms_conc16` | 1.11x | **1.07x** |

Medians moved little (ndjson 41,942 → 44,928); the *dispersion* is what it
destroyed, and dispersion is what decides whether the suite can detect a
regression. After stopping it, `drain_http_ndjson` delivered exactly 9,960
records on all five runs.

The published runs were taken with Docker empty (`docker ps` → 0 containers) and
a 1-minute load average of 3.5–5.6 on a 16-core machine, the remainder being a
browser and the desktop compositor. That is a working laptop, not an isolated
rig — which is the standing argument for preferring beast for anything where the
daemon's own cost is the question.

Within-configuration spread is 1.02-1.38x on the current basis, the wide end
being mac's `drain_http_per_record` and the tight end the re-measured Linux rows
on an isolated box. **Between configurations it reaches 5.55x on the same
scenario** (`drain_http_ndjson`, linux-tmpfs ÷ mac). Any single headline number
for this suite is a number about a machine.

The 6.3x this previously claimed was never a same-scenario figure — the
comparable quantity is NDJSON-vs-per-record *within* linux-tmpfs, now 6.59x and
quoted further down. No pair of same-scenario medians has ever produced it, on
either basis.

## What the storage does

The `sync_all` on the confirm path is the whole story, and it was worth two
separate measurements to see it.

> **Single basis (2026-09-18).** Both columns now divide current medians by
> current medians. The previous revision could not: `linux-ssd ÷ mac` mixed a
> superseded Linux median with a current mac one, and was published with a
> warning to read it as direction rather than magnitude. That warning was
> warranted — every figure in this table moved on re-measure, and
> `drain_slow_sink` moved by 24%.

| Scenario | linux-ssd ÷ mac | linux-tmpfs ÷ linux-ssd |
|---|---:|---:|
| `drain_http_ndjson` | **2.18x** | **2.54x** |
| `drain_http_per_record` | 1.32x | 1.21x |
| `drain_slow_sink_1ms_conc16` | 1.38x | 1.06x |

**A SATA-SSD Linux box beats an M3 Max on every scenario**, by
2.18x on NDJSON. (Not "a 4-core box beats a 16-core" — that phrasing survived
here for two hundred lines after the Environments note above corrected it.
`nproc` reports 4 on beast only because `isolcpus=2-7,10-15` holds twelve of its
sixteen logical CPUs out of the default scheduler.) This is the predicted result and it confirms the diagnosis:
macOS `sync_all` is `F_FULLFSYNC`, measured at 3,965 µs against 238 µs for the
`F_BARRIERFSYNC` used for record data. It is also why a 2-vCPU CI runner
previously beat an M3 Max here. **Nothing about the drain is slower on Apple
silicon; the confirm is.**

**Storage still dominates NDJSON on Linux.** tmpfs is a further 2.54x, so even
the ext4 figure is mostly a storage number. The drain's own ceiling, with the
filesystem taken out, is **~249,000 rec/s**.

**Per-record mode barely notices any of it** — 1.32x from macOS to Linux, 1.21x
from SATA to RAM. It is bounded by per-request HTTP cost, not by the WAB. That
is a genuinely useful separation: the two scenarios are measuring different
bottlenecks, and only one of them is the buffer.

**`drain_slow_sink` is the least storage-sensitive of the three, but not
insensitive.** At 1.06x tmpfs-over-ext4 it is an order of magnitude less
storage-bound than NDJSON's 2.54x — yet the two ranges no longer overlap
(10,205-11,074 against 11,299-11,849), so the effect is real and this file's
previous "no storage sensitivity at all" is withdrawn. That claim rested on
overlapping ranges produced by a 1.48x spread; on the corrected basis the spread
is 1.05x and the overlap is gone. Bounded by the 1 ms sink delay and
concurrency, exactly as the scenario intends. It is the only one of the three
currently measuring what its name claims.

## The CI runner's disk is not a disk

The `drain` CI job reports `"wab":"default"`, which reads as "what a real
deployment sees". On GitHub's 2-vCPU `ubuntu-latest` it is not — and the same
data shows the job's numbers cannot be compared with anything at all on two
of its three scenarios.

Five CI runs since the harness fix, from the job's retained artifacts:

| Scenario | min | median | max | spread | linux-ssd | linux-tmpfs |
|---|---:|---:|---:|---|---:|---:|
| `drain_http_ndjson` | 187,089 | **233,992** | 236,144 | 1.26x | 98,098 | 249,288 |
| `drain_http_per_record` | 24,411 | 25,858 | 37,713 | **1.54x** | 31,298 | 37,835 |
| `drain_slow_sink_1ms_conc16` | 1,864 | 6,460 | 7,284 | **3.91x** | 11,042 | 11,654 |

**What holds.** On `drain_http_ndjson` — the one storage-bound scenario — CI
sits at RAM-disk level: median 0.94x beast's tmpfs, **2.39x** beast's real
SATA SSD, and beast's SSD figure (98,098) falls far outside CI's entire
five-run range. That is the storage-sensitive scenario, the confirm is what
makes it storage-sensitive, and CI behaves as though the confirm is free.
Treat the CI drain figures as an upper bound with the filesystem removed, not
as a deployment number, and never compare them against hardware that honours
a flush.

**What does not hold, and was claimed here earlier today.** An earlier version
of this section argued that CI is *slower* than beast on the two
non-storage-bound scenarios — as a 2-vCPU runner should be — and faster only
on the storage-bound one, so the storage must be unreal. A fifth run
withdrew it. `drain_http_per_record`'s CI range now spans 24,411-37,713,
which *contains* beast's SSD figure and nearly reaches beast's RAM disk, so it
distinguishes nothing. That argument rested on four runs and did not survive
the fifth. The conclusion above stands on `drain_http_ndjson` alone.

This is not a diagnosis either. Nothing here identifies *why* the confirm is
cheap; host write-back caching on an ephemeral virtual disk is the obvious
candidate and is unverified.

**The spreads are the more useful finding.** 1.54x and 3.91x are not
measurements. In particular `drain_slow_sink_1ms_conc16` exists to catch a
regression that turns the HTTP sink serial — serial delivery caps at
~1,000 rec/s, and one of these five runs came in at **1,864**. A scenario
whose healthy range reaches down to twice its own failure threshold cannot
tell a serialisation regression from a busy runner. That is a gap in what
this suite can detect on CI, not a number to publish.

For scale, the five CI runs *before* the harness fix median 8,093 rec/s on
`drain_http_ndjson` against 233,992 after — the fault documented above, not a
change in the runner.

## What changed in the conclusions

> **Basis note (2026-09-18).** Recomputed. This section previously carried a
> warning that everything in it rested on a superseded `linux-ssd` NDJSON figure
> of 105,909 rec/s and should be redone when the box came back. The box came
> back; the figure is now 98,098 rec/s on the corrected basis, and the ratios
> below divide by it. The old warning's prediction held: the numbers moved by
> single-digit percentages and no ratio crossed 1.0.

**"Delivery is the narrower half" stays withdrawn — but it is not simply
false, and the previous wording here overstated the case.** The original claim
rested on ~6,000 rec/s against an ingest figure measured differently. The fixed
harness puts the drain at 98,098 rec/s NDJSON on linux-ssd. What that is
"several times" larger than depends entirely on which ingest number you set
beside it, and the first version of this paragraph picked the wrong one — it
compared this box's drain against an **M3 Max** client's ~32,000 `Buffered`
(`crates/weir-client/src/lib.rs`), which is the cross-machine comparison
[environments.md](environments.md) explicitly forbids.

Against the *same box's own* ingest ([snapshot-2026-06-13-beast.md](snapshot-2026-06-13-beast.md),
Samsung SATA SSD, same `bench_preset`):

| linux-ssd ingest | rec/s | Drain (98,098) is |
|---|---|---|
| Single thread, `Durable` | 653 | 150× wider |
| Single thread, `Buffered` | 49,725 | **2.0× wider** |
| 48 threads, `Durable` | 6,178 | 16× wider |
| 48 threads, `Buffered` | **155,577** | **0.63× — narrower** |

So delivery is the wider half against a *single* producer and against every
`Durable` configuration, and the **narrower** half against concurrent
`Buffered` ingest — by about 1.6×. That last row is not an embarrassment; it is
the case weir exists for. A buffer earns its keep precisely when a burst
arrives faster than it can be delivered, and the whole design — ack on the WAB,
drain behind it — assumes that gap.

Two limits on the table above. The ingest capture is from a different commit
three months earlier, so it is same-machine but **not back-to-back**, which is
the comparison environments.md actually sanctions; treat the ratios as
one significant figure. And the drain figure is measured against an in-process
HTTP mock on loopback answering in microseconds, so it bounds *weir's* side of
delivery and says nothing about a real sink's — the `drain_slow_sink_1ms_conc16`
scenario, where a 1 ms sink caps the serial path near 1,000 rec/s, is the
honest picture of what a network sink does to this number.

What does survive intact: any argument that prioritised work on the grounds
that **weir's own delivery code** constrains the system needs remaking from
scratch. The constraint is the sink, not the drain.

**NDJSON's advantage is not a fixed ratio.** It is 1.89x on mac, **3.13x** on
linux-ssd, **6.59x** on linux-tmpfs. It grows as the confirm gets cheaper,
because batching amortises the per-batch confirm as much as the per-request
network cost. Publishing one number for it — as this file previously did, first
1.25x then 1.67x — was wrong in kind, not just in value. (The mac figure is
1.89x rather than the 1.67x published before 2026-09-15, for the same reason
every mac-derived ratio moved: the re-measure.)

**Concurrency at a 1 ms sink gives 8.8x on Linux**, against 8.0x on mac and a
~1,000 rec/s serial cap. Both are closer to the concurrency setting of 16 than
the old mac figure of 5.0x suggested, and both are still sub-linear — the two
platforms agree far better on the corrected basis than they appeared to on the
old one.

**The NDJSON window is now very short.** 1,000 delivered records in ~4 ms on
tmpfs. `delivered_rps` is computed from `Duration::as_secs_f64()`, so this is
not timer quantisation — four of five runs agreed within 1.1% and one landed
32% high. But a window that short is one scheduling event away from an outlier,
and the record count was sized when `F_FULLFSYNC` made everything slow. **The
fill should scale with the platform before these numbers are trended.**

## Still not measured

- **NVMe, and a spinning disk.** The Linux box has both unmounted; only the
  SATA SSD and tmpfs were measured. NVMe should land between them and would say
  whether the ext4 figure generalises or is specific to SATA.
- **A record count sized for the platform.** 2,000 records was chosen when the
  fastest scenario took 25 ms; on tmpfs it takes 4. Trending these numbers
  before fixing that would trend scheduling noise — and the CI spreads above
  (1.54x and 3.91x) are what that looks like in practice.
- **Whether `drain_slow_sink_1ms_conc16` can still do its job on CI.** Its
  purpose is catching a drain that has gone serial (~1,000 rec/s); one CI run
  in five measured 1,864. Either the scenario needs a longer window or the
  gate needs to live somewhere less contended.
- **A real sink over a real network.** The mock answers in microseconds on
  loopback. Every ratio above should be re-derived at a realistic RTT.
- **Concurrent ingest and drain.** Every scenario fills, then drains. A buffer
  under sustained matched load is the case operators actually run.

## What these numbers are not

- **Loopback only.** No network latency, no TLS to the sink, no DNS. A real HTTP
  sink over a WAN pays an RTT per batch that dominates everything here.
- **One sink type.** HTTP only. The SQL sinks batch very differently.
- **Not steady state.** Each scenario fills, then drains. Behaviour where ingest
  and drain run concurrently at matched rates is still unmeasured.
- **Not a gate.** The suite asserts correctness — including that no record is
  lost between the WAB and the sink across an outage — but the rates are reported
  for tracking, not enforced. Failing a throughput threshold on shared CI
  hardware produces flakes, not signal.
- **Not yet trended.** `deploy/avg_benchmarks.py` renders only the
  deadline-suffixed *ingest* scenarios, so these numbers reach neither
  `latest.md` nor `history.md`. The `drain` CI job retains its JSONL as a
  30-day build artifact, which preserves the raw data but does not plot a trend
  — **a gradual delivery regression would currently go unnoticed.** Closing it
  means teaching the renderer about delivery scenarios (which have no deadline
  suffix and report `delivered_rps`, not `throughput_rps`); that touches the
  script writing main's committed baselines, so it is deliberately a separate
  change rather than a rider on this one.

## Reproducing

```sh
# What a real deployment sees: WAB on the default filesystem.
cargo test -p weir-server --test load_drain --release -- --nocapture

# The drain with the filesystem taken out. Any tmpfs/RAM-disk path works;
# the suite creates a per-scenario subdirectory under it.
WEIR_BENCH_WAB_DIR=/dev/shm/weirbench \
  cargo test -p weir-server --test load_drain --release -- --nocapture
```

Each scenario prints one `BENCH: {json}` line, carrying `"wab":"default"` or
`"wab":"external"` so the two are never averaged together. The `drain` CI job
runs the first form only.
