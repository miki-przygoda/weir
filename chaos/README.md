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
> Getting here took six defects in the harness against two in weir. Quiescence
> alone was wrong four times, in both directions. Treat a green run as a claim
> that needs its reasoning checked, not as a formality.

## Phase 2 — injector validated, injection NOT implemented

The `dm-flakey` injector Phase 2 depends on has been validated on real hardware:
[`docs/benchmarks/chaos-phase2/2026-08-22-dm-flakey-control-experiment.md`](../docs/benchmarks/chaos-phase2/2026-08-22-dm-flakey-control-experiment.md).
`drop_writes` is a **lying disk** — it reports fsync success and discards the
write — which is not what real power loss does, and the write-up explains why
that difference dictates the episode protocol rather than being a footnote.

**Phase 2 itself is still unwritten.** `dm_stack.py` builds loop → ext4 → mount
and nothing else; the episode loop injects `kill_random` unconditionally; the
`[faults]` tables are empty. Read the write-up before implementing it: it
records three separate ways a flakey device can end up running a configuration
you did not ask for, and all three fail towards *false violations against weir*.

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
3. the daemon is asked to shut down — weir's SIGTERM path is a *full drain*,
   not a seal-and-exit, so this is where the last tens of thousands of records
   are actually delivered;
4. verification runs at **`frontier_slack=0`**. This is the only moment in a
   run where zero slack is a true statement rather than a stricter-than-reality
   one: the producer is stopped and both logs are complete. If any precondition
   failed — a dirty loadgen exit, a killed drain, a dead recorder — the check
   falls back to the normal slack and is marked **advisory**, and an advisory
   failure counts as an anomaly rather than a violation;
5. the WAB directory gets a **post-mortem** — surviving sealed segments,
   non-empty active segments, `quarantine/` and `dead_letter/` contents, with
   paths and sizes. Any survivor is an anomaly. This evidence used to be
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
