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

## Status — built, never run

> **This harness has never executed against real hardware.** Every component is
> implemented and unit-tested (31 Rust tests, 73 Python), and the whole suite is
> green on macOS — but `run.py` requires Linux and root, and no Linux host has
> been available. `dm_stack.setup()`/`teardown()`, the episode loop, and the
> quiescence *success* path have therefore never executed outside a test double.
>
> **No durability claim about weir rests on this harness yet, and none should be
> made from it until the gate below has actually passed.** The whole point of the
> suite is evidence a skeptic would accept; "the code exists" is not that.
>
> Two rounds of review found the quiescence check broken in *opposite* directions
> (it always reported drained, then never did), so treat the first real run as
> genuinely unproven rather than a formality.

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
