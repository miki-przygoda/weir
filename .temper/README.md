# Gates for temper

[temper](https://github.com/miki-przygoda/temper) runs these inside each task's
worktree. They work on any Linux machine with cargo; nothing in them names a
machine.

| Script | Runs |
|---|---|
| `gate-quick.sh` | After every task and merge: fmt and workspace clippy, then all-features clippy and the tests of the crates the change touched (the drift guards when docs, deploy or demos changed). Stops at the first failure. |
| `gate-full.sh` | At the checkpoint and before a PR: CI's lint, test, msrv, advisories and dst jobs natively; its container jobs under [act](https://nektosact.com) in a clean clone of HEAD; the windows job and the release build matrix in docker. Takes an hour or more: run it detached. |
| `gate-run.sh` | One cargo command in a gate slot: `bash .temper/gate-run.sh test -p weir-wab`. |
| `gate-slot.sh` | Sourced by the three: takes a free slot, sets up the build env, logs each step. |

Each gate takes a free **slot**: a directory with its own lock, cargo target dir
and test scratch dir. A gate waits while every slot is busy. A slot remembers
the worktree that last built in it, and touches the next worktree's tracked
sources when that changes, because cargo judges freshness by file times.

Every run writes `<logs>/<worktree>/<gate>-<UTC>/summary.txt`, plus one log per
step, and points `<logs>/<worktree>/latest-<gate>` at it. The summary's last
line is always `GATE <gate> rc=<0|1>`.

Machine-specific values go in an env file, read when present
(`$WEIR_GATE_ENV`, default `~/.config/weir/gate.env`):

| Variable | Default |
|---|---|
| `WEIR_GATE_SLOTS` (space-separated) | `~/.cache/weir-gate/slot-a` |
| `WEIR_GATE_TMP` (tests' scratch, keep on fast storage) | `~/.cache/weir-gate/tmp` |
| `WEIR_GATE_LOGS` | `~/.cache/weir-gate/logs` |
| `WEIR_GATE_JOBS` (build jobs and test threads) | `4` |
| `WEIR_GATE_MIN_FREE_GB` | `20` |
| `WEIR_GATE_STEP_TIMEOUT` (seconds) | `3600` |
| `WEIR_ACT` | `act` |
| `SCCACHE_DIR`, `SCCACHE_CACHE_SIZE` (when sccache is installed) | `~/.cache/weir-gate/sccache`, `25G` |

sccache and mold are used when installed. No step uses sudo or installs
anything; the full gate needs docker, act, go, python3, make and a C compiler,
plus the 1.88 toolchain for the MSRV check.
