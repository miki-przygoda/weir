# chaos — real-kernel fault injection for weir

A standalone cargo project (not a weir workspace member) that runs weir against
a real device-mapper stack and verifies its durability claims from outside the
daemon, across crash and restart.

Design: [`docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md`](../docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md)

## Requirements

- **Linux.** Device-mapper and loopback devices. macOS is not supported and
  never will be: `F_BARRIERFSYNC` gives no power-loss guarantee, so a
  durability proof there would prove nothing.
- **root**, for the orchestrator (`run.py`): it owns loopback devices, mounts,
  and process lifecycle. The load generator and recorder are *intended* to
  run unprivileged, as observers that must not be able to corrupt what they
  measure — but as implemented today, `run.py` execs both as direct
  `subprocess.Popen` children with no privilege drop, so **they currently
  inherit root from the parent, same as the daemon under test.** A real drop
  needs `setuid`/capability plumbing that is its own piece of work; it is
  deferred, not landed, and "the observers are unprivileged" should be read
  as a design goal the code does not yet enforce.
- **Python 3** (stdlib only) and `losetup` / `dmsetup` / `mkfs.ext4`.
- **weir-server 4.1 or later** under test. The orchestrator always passes
  `--drain-shutdown-budget-secs` (3600 unless a schedule sets it, so the final
  pass still drains), and an older daemon refuses unknown flags at startup.
  Re-running a phase-1 or phase-2 schedule against a 4.0.x binary therefore
  fails at once; use the harness from that release instead.

## Running

```bash
cd chaos
cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
```

Everything runs from inside this directory. `cargo build` at the weir repo root
does not build this project.

## Status — Phase 1 gate PASSED (2026-08-01)

> **The Phase 1 exit gate passes: 20/20 episodes, 0 violations, 0 anomalies.**
> 7,383,717 records, 20 `kill -9`s, 86 unconfirmed segments replayed across
> restarts. Every acked record was delivered (`i1_missing = 0`), no refused
> record ever appeared downstream (`i2_leaked = 0`), and nothing was excused by
> the frontier exemption (`i1_exempt = 0`). Report and per-episode data:
> [`docs/benchmarks/chaos-phase1/`](../docs/benchmarks/chaos-phase1/).
>
> **Read the venue caveat before quoting any of it.** The run was in a
> privileged Linux container on virtualised storage, so **no number with a unit
> attached means anything** — this venue answers "does the harness work, and
> does it report false violations", which is Phase 1's whole criterion, and
> nothing about performance.
>
> **What it does NOT establish.** Only `kill -9` was injected. Power loss, disk
> full, slow disk, torn writes and read-only remount are Phases 2–3, and they
> are where the more interesting claims live — the `Buffered`-vs-`Sync` tier
> distinction is entirely untested here (the schedule runs `Sync` only). This is
> minutes of load, not the multi-day soak the design calls for.
>
> Getting here took eight defects in the harness against three in weir. Quiescence
> alone was wrong four times, in both directions. Treat a green run as a claim
> that needs its reasoning checked, not as a formality.

## Long soaks — 10h and 5h on bare metal, both clean (2026-08-23)

> **38.6 million acked records across 665 `kill -9`s, 0 violations, 0 anomalies.**
> [`docs/benchmarks/chaos-soak/2026-08-23-10h-vs-5h-phase1-soak.md`](../docs/benchmarks/chaos-soak/2026-08-23-10h-vs-5h-phase1-soak.md)
>
> Two things the 30-minute gate could not show. Throughput is **flat across ten
> hours** (+0.95% Q1→Q4), so there is no leak or decay signature. And recovery
> truncated **exactly 4.000 segments per kill** — one per shard — across 665
> crashes on two independent kill sequences.
>
> `i1_exempt = 0` in both runs: nothing was excused by the frontier exemption,
> which is what makes "0 violations" stronger than it looks.
>
> **Still Phase 1.** `kill -9` and `Sync` only. The 2.0 quarantine path was never
> entered (`kill -9` truncates cleanly; quarantine is for corruption), so it has
> no coverage here.

## Status — Phase 2 Buffered measured (2026-08-28)

> **706 power-loss episodes, 604,485,602 acked records, 0 violations,
> 0 anomalies. Canary `bit` in 706/706.**
>
> Full write-up:
> [`docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md`](../docs/benchmarks/chaos-phase2/2026-08-28-first-power-loss-measurement.md).
>
> **Buffered's exposure is now a number, not prose.** Loss is uniformly
> distributed between zero and the ceiling — mean and stdev both land within 2%
> of a uniform distribution's `max/2` and `max/√12` — with a ceiling of
> **126,782 records** (~32.5 MB at this run's 256-byte records) and a median of 63,790. The ceiling, not the mean, is what
> characterises the tier.
>
> **Read the ceiling in BYTES: not seconds, and not records.** This run rendered it as
> "~1.76 s of acknowledged writes" by dividing by its own ~72,000 rec/s, and
> called it "one writeback interval". The batched Buffered run
> ([`powerloss-batch-buffered.toml`](schedules/powerloss-batch-buffered.toml),
> 747 episodes, 2026-09-27) tested that reading by raising throughput 1.45x: the
> record ceiling did **not** move (123,664 against 126,782) and the duration fell
> to **1.18 s**. So the ceiling is a quantity of data and the seconds are
> throughput-specific. The record-size sweep
> ([`powerloss-recsize-64.toml`](schedules/powerloss-recsize-64.toml) and
> [`-1024`](schedules/powerloss-recsize-1024.toml), varying only `record_size`
> against the batched Buffered run) then showed the quantity is **bytes**:
> 461,956 records at 64 B (29.6 MB), 123,664 at 256 B (31.7 MB) and 27,084 at
> 1024 B (27.7 MB), a 17x spread in records and ±7% around ~30 MB in bytes, with
> 0 violations and the canary `bit` on every cut. All legs used 4 shards and
> 8 MiB segments, so `shard_count x wab_segment_max_bytes` (33.6 MB) is a
> hypothesis, not a result. The [`-4096`](schedules/powerloss-recsize-4096.toml)
> leg never ran. Write-up:
> [`2026-09-27-buffered-ceiling-is-bytes.md`](../docs/benchmarks/chaos-phase2/2026-09-27-buffered-ceiling-is-bytes.md).
>
> **What that run alone did NOT establish** — both since closed, and left here
> because the sequence is the point: a Buffered run measures Buffered, and
> saying so is what made the two follow-ups obviously necessary.
>
> - ~~The Durable tier is untested under power loss.~~ **Closed 2026-08-30:**
>   5,311 episodes over 24 h, 42,814,591 acked, **nothing lost**, and the WAB
>   completely empty at shutdown.
> - ~~WAB format v2 (zstd) is uncovered.~~ **Closed 2026-08-29:** a 40-episode
>   controlled comparison differing only in `wab_compression`. The contract
>   held; the exposure ceiling roughly tripled and went bimodal.
>
> **Phase 2 totals: 6,129 power-loss episodes, 687,712,504 acked records, zero
> durability violations, zero weir defects.** Both tiers, both on-disk formats.

## The batched ack path (A6) — `wire_batch_size`

Everything above was measured against a load generator whose only producer call
was a single `push`. 4.0.0 added `PushBatch`, whose reply is a **bitmap**, and a
set bit is a durability promise carrying the same crown invariant as a
single-record `Ack`. The whole 687.7M-record corpus therefore says nothing about
it: not one power cut had ever landed on a batched ack.

`[load].wire_batch_size` closes that. It is the number of records per
`push_batch` call; absent or `1` keeps the single-`push` call site byte for
byte, so **every pre-existing schedule still measures exactly what it measured
before** and stays comparable with the Phase 2 corpus.

It is deliberately *not* spelled `batch_size`, because `[weir].batch_size`
already means the daemon's group-commit size. The two interact and are worth
setting against each other rather than conflating: at `wire_batch_size = 256`
over `[weir].batch_size = 64`, one bitmap spans about four fsyncs, so a set bit
has to survive a commit boundary instead of mapping onto a single commit.

The oracle reasons **per record, not per call**: one batched call yields
`wire_batch_size` independent verdicts and the ledger gets one row each. The bit
mapping is the load-bearing part —

| bitmap bit | ledger outcome | why |
| --- | --- | --- |
| set | `Acked` | durable; held to I1 exactly as a single-record `Ack` is |
| clear | `Unknown` | "not durable as of this reply; retry, and it may nonetheless have been written" — identical to `Nack(InternalError)`. Recording it as `Nacked` would make I2 fire on a legitimate replay and manufacture a P0 out of correct behaviour. |

Two schedules: [`schedules/smoke-batch.toml`](schedules/smoke-batch.toml)
(`kill -9`, ten episodes — plumbing only: does `wire_batch_size` reach loadgen,
does each record get its own row, do the invariants hold across a kill) and
[`schedules/powerloss-batch.toml`](schedules/powerloss-batch.toml) (the
durability run).

**Result so far (2026-09-26, pre-4.0.1 code): 1,189 completed batched `Durable`
power cuts, 239,069,696 acked records, 0 violations, canary `bit` on every cut,
duplicate ratio 1.062.** A 1,190th episode filled the 2 GiB test device, and the
oracle counted the records weir had refused to truncate as lost. The episode is
excluded, the run has no final pass, and a rerun on current code is still owed.
Write-up:
[`2026-09-26-batched-durable-power-loss.md`](../docs/benchmarks/chaos-phase2/2026-09-26-batched-durable-power-loss.md).

**Check the daemon is really batching before trusting a batched run.** The
schedule key is easy to typo into silence, and a run that quietly used the
single-push path looks identical to a passing batched one. Scrape `/metrics`
mid-run: `weir_batch_records_sum / weir_batch_records_count` should equal
`wire_batch_size`, and `weir_batch_records_count` must be *rising*. A
`--batch-size 1` control that leaves that counter untouched is what proves the
default path never batches.

The injector itself was validated on real hardware first:
[`docs/benchmarks/chaos-phase2/2026-08-22-dm-flakey-control-experiment.md`](../docs/benchmarks/chaos-phase2/2026-08-22-dm-flakey-control-experiment.md).
`drop_writes` is a **lying disk** — it reports fsync success and discards the
write — which is not what real power loss does, and that difference dictates
the episode protocol rather than being a footnote. Read it before touching
`dm_stack.py`: it records three separate ways a flakey device can end up
running a configuration you did not ask for, and all three fail towards *false
violations against weir*.

**The protocol is kill-first**, and the reason is not obvious. `drop_writes`
sits BELOW the page cache, so a dropped write leaves the page resident and
correct, and `kill -9` does not evict it — an engage-then-kill injector cannot
lose a single byte. `dmsetup suspend` without `--nolockfs` additionally flushes
every dirty page to the still-honest disk before the fault installs. The
episode therefore kills first, then engages, unmounts, disengages and remounts,
which makes the lying window exactly zero by construction and models what power
loss actually takes: the data that was dirty at the instant of death.

**The canary (I6) is load-bearing, especially for a Durable run.** A known
block is written before the fault, overwritten while engaged, and read back
after the remount; `bit` means the overwrite was destroyed. A Buffered run can
argue the injector fired because something went missing. A Durable run cannot —
under a correct implementation nothing goes missing, so "zero loss" and "the
fault never fired" produce identical numbers.

## Foundations lifecycle schedules (pre-release gate)

Four schedules gate the Foundations release. Each one provokes a lifecycle
path weir only has since Foundations, then ends the episode in the same
dm-flakey power cut the Phase 2 corpus was measured with, at a seeded point
inside or after that path.

| Schedule | Trigger | Lands the cut on | Anomaly reasons |
|---|---|---|---|
| `shutdown-bounded.toml` | `sigterm_at` | mid-lame-duck to after-exit, half with a stalled sink | `shutdown_overran_bound`, `daemon_exit_code=N`, `daemon_dead_before_signal` |
| `shutdown-forced.toml` | `double_sigterm` | after the second signal | `forced_shutdown_overran_bound`, `shutdown_overran_bound`, `daemon_exit_code=N`, `daemon_dead_before_signal` |
| `startup-signal.toml` | `sigterm_in_recovery` | mid-recovery or after the pre-serving exit | `recovery_phase_not_observed`, `served_after_pre_serving_signal`, `producer_socket_left_after_pre_serving_exit`, `daemon_exit_code=N`, `daemon_dead_before_signal` |
| `pause-resume-cut.toml` | `admin_pause_resume` | while paused or 0-5 s after resume | `pause_not_observed`, `paused_gauge_not_set`, `delivered_while_paused`, `resume_did_not_see_pause`, `admin_error`, `admin_pause_refused`, `admin_resume_refused` |

Build weir-server with the test hooks, from the weir repo root:

```bash
cargo build --release -p weir-server --features test-hooks
```

`startup-signal.toml` needs them: the harness sets `WEIR_TEST_HOOKS=1` and
`WEIR_TEST_STARTUP_DELAY_MS` on the triggered start only, which holds the
daemon in `recovering` long enough to signal it there. The hooks are inert
without `WEIR_TEST_HOOKS=1`, and the daemon logs a loud WARN when they are
compiled in.

Since Foundations, weir bounds its SIGTERM drain at
`drain_shutdown_budget_secs`, so run.py passes
`--drain-shutdown-budget-secs 3600` to every daemon unless the schedule sets
a budget, and a schedule that sets a smaller one gets a drain-out restart
before the final pass's zero-slack verify (see "The final pass" below).

In these runs' records, `fault = power_loss` is the fault the oracle judges
(so the tier exemption, the power-loss verdict and the quarantine exemption
all apply unchanged), and `trigger = <kind>` names the lifecycle path.

run.py now unwinds on SIGTERM and SIGHUP as well as SIGINT. A run that ends on an
exception or a signal before its final pass keeps `wab.img` for forensics and
writes `crash-postmortem.json` beside `episodes.jsonl`, which `report.md`
renders as "Crash post-mortem".

When all four runs are done, the gate reads their run directories:

```bash
python3 orchestrator/gate.py runs/<id> runs/<id> runs/<id> runs/<id> [--allow-anomalies]
```

It passes only with ≥1,000 power cuts in total and ≥200 per schedule, 0
oracle violations, the canary bit on every cut, 0 anomalies unless
`--allow-anomalies` (each allowed one explained in the write-up), and a final
pass in every run.

## Phase 1 exit gate

Phase 1 is complete when a 30-minute smoke run produces **zero violations and
zero anomalies**:

```bash
# From the weir repo root, build both sides first.
cargo build --release -p weir-server
cd chaos && cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
```

`run.py` renders `runs/<run_id>/report.md` itself at the end of the run — no
separate `report.py` step. Exit code 0, and `0 violations, 0 anomalies` in
both the console summary and the report.

A **violation** is a durability failure (I1/I2): weir lost or leaked a
record. An **anomaly** is the harness failing to observe an episode cleanly —
a quiescence timeout, a dead observer, or an episode with no measurable
progress — and is *not*, by itself, evidence of a weir defect. The two are
counted and gated on separately so neither can hide inside the other.

### The final pass

After the last episode, and before anything is torn down, the run does one
more thing — recorded as `{"episode": "final"}` in `episodes.jsonl` and as the
last row of the report:

1. the load generator is stopped and **reaped** (it catches SIGTERM and
   flushes its ledger, so its exit status says whether the ledger is complete);
2. `/metrics` is scraped once more **while the daemon is still alive**;
3. the daemon is asked to shut down. Since Foundations, weir bounds its
   SIGTERM drain at `drain_shutdown_budget_secs` (default 10 s), so run.py
   passes `--drain-shutdown-budget-secs 3600` unless the schedule sets a
   budget, which restores the full drain this check needs. A schedule with
   a smaller budget gets a **drain-out restart** after this stop: the
   daemon is restarted with the budget at 3600, run.py waits for
   quiescence (including `weir_wab_undelivered_segments == 0`), then stops
   it again. A drain-out that does not quiesce makes step 4 advisory
   (`drain_out_not_quiesced`);
4. verification runs at **`frontier_slack=0`**. This is the only moment in a
   run where zero slack is a true statement rather than a stricter-than-reality
   one: the producer is stopped and both logs are complete. If any precondition
   failed — a dirty loadgen exit, a killed drain, a dead recorder — the check
   falls back to the normal slack and is marked **advisory**, and an advisory
   failure counts as an anomaly rather than a violation;
5. the WAB directory gets a **post-mortem** — surviving sealed segments,
   non-empty active segments, `quarantine/` and `dead_letter/` contents, with
   paths and sizes. A survivor is an anomaly *unless* it is a quarantined
   segment under power loss, which is the documented correct outcome and is
   exempted in `run.py` — `drop_writes` tears the active segment, recovery parks
   the torn tail rather than truncating it, and flagging that would put an
   anomaly on essentially every power-loss episode. A 747-episode Buffered run
   ended with 2,902 such survivors, ~3.9 per episode: one per shard per cut.
   Non-empty active segments and dead-letter files are never exempt. This
   evidence used to be
   deleted, unread, by `stack.teardown()`.

**The gate is zero FALSE violations, not "it ran".** Any violation at this
stage is far more likely to be a harness bug than a weir bug — Phase 1 injects
only random SIGKILL, which weir's existing system tests already cover. Treat a
Phase 1 violation as an oracle defect until proven otherwise: check that the
recorder fsynced before its 200, that quiescence really settled, and that no
record was NDJSON-dead-lettered for containing a newline. An anomaly deserves
the same scrutiny before it is dismissed as "just the harness": a recorder 4xx
is permanent to weir's drain, so a refused batch is dead-lettered and looks
identical to an acked-never-delivered weir defect — check `recorder.log`
first.

A harness that cries wolf is worse than no harness on a multi-day run.
