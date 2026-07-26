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
