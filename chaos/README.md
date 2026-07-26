# chaos — real-kernel fault injection for weir

A standalone cargo project (not a weir workspace member) that runs weir against
a real device-mapper stack and verifies its durability claims from outside the
daemon, across crash and restart.

Design: [`docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md`](../docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md)

## Requirements

- **Linux.** Device-mapper and loopback devices. macOS is not supported and
  never will be: `F_BARRIERFSYNC` gives no power-loss guarantee, so a
  durability proof there would prove nothing.
- **root**, for the orchestrator only. The load generator and recorder run
  unprivileged by design — they are the observers and must not be able to
  corrupt what they measure.
- **Python 3** (stdlib only) and `losetup` / `dmsetup` / `mkfs.ext4`.

## Running

```bash
cd chaos
cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
```

Everything runs from inside this directory. `cargo build` at the weir repo root
does not build this project.

## Phase 1 exit gate

Phase 1 is complete when a 30-minute smoke run produces **zero violations**:

```bash
# From the weir repo root, build both sides first.
cargo build --release -p weir-server
cd chaos && cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
python3 orchestrator/report.py runs/<run_id>
```

Exit code 0 and `0 violations` in the report.

**The gate is zero FALSE violations, not "it ran".** Any violation at this
stage is far more likely to be a harness bug than a weir bug — Phase 1 injects
only random SIGKILL, which weir's existing system tests already cover. Treat a
Phase 1 violation as an oracle defect until proven otherwise: check that the
recorder fsynced before its 200, that quiescence really settled, and that no
record was NDJSON-dead-lettered for containing a newline.

A harness that cries wolf is worse than no harness on a multi-day run.
