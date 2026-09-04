# Bare-metal benchmark results

CI numbers in [`latest.md`](latest.md) come from a 2 vCPU shared GitHub
Actions runner. They are useful for catching order-of-magnitude
regressions; they are **not** representative of the hardware weir is
designed to run on, and they are not the numbers any performance claim
should be made against.

This page holds the numbers captured on a real machine with named
hardware. They are intended to be the ship gate — but **there is no capture
yet, so the gate is not operable and its thresholds below are unvalidated.**

> **Status:** awaiting first capture. Run
> `deploy/run_bare_metal_bench.sh`
> on the target machine and replace this section with the script's
> output.

## Regression policy

| Surface | Gate | Action on violation |
|--------|------|---------------------|
| CI `latest.md` | >10× drop on any scenario, or any scenario going from non-zero to zero | Block the PR; investigate before merge. |
| Bare-metal `bare-metal.md` (this file) | >10% drop in single-thread RPS, >20% increase in `Sync` p99, or any saturation-ramp level regressing from `ok` to dropped I/O — **provisional, see below** | Block the release; investigate before tagging. |

**The bare-metal thresholds are not yet validated.** No capture exists, so this
surface's run-to-run noise floor is unmeasured, and a threshold below the noise
floor blocks releases on nothing. The only run-to-run evidence the project has is
[`history.md`](history.md), where consecutive CI runs of the *same released
version* span 1,926–2,783 single-thread `Sync` RPS (1.45×) and 610 µs – 1.2 ms
`Sync` p99 (~2×) — both far outside 10% / 20%. A dedicated bench box should be
much tighter than a shared CI runner, but that is an expectation, not a
measurement. Before letting these numbers block a release, take **at least three
back-to-back captures of identical code** and set the thresholds above the spread
they show.

The CI gate exists to catch the kind of mistake a code review wouldn't
(a missing `#[inline]`, an accidental `Mutex` on the hot path, a
serialisation step that quietly went from zero-copy to allocating).
Anything smaller than an order of magnitude is below CI's noise floor
and has to be re-tested against bare-metal numbers before it counts.

## Capture procedure

The script captures every piece of context needed to compare two runs:

- CPU model, core count, microcode revision, current governor
- Kernel version, libc version, glibc tunables
- Filesystem type and mount options for `wab_dir`
- Block device model (`lsblk -d -o NAME,MODEL,ROTA,TRAN`)
- `vm.dirty_background_bytes`, `vm.dirty_bytes`,
  `vm.dirty_expire_centisecs`
- Whether `mitigations=off`, SMT, and turbo are enabled

It then runs the load suite 5× at each of `batch_deadline_ms ∈ {1, 2}`
(same as CI), feeds the JSONL through `avg_benchmarks.py`, and writes
the combined env-header + result tables to stdout.

```sh
# On the target machine, after a clean build:
deploy/run_bare_metal_bench.sh > docs/benchmarks/bare-metal.md
git add docs/benchmarks/bare-metal.md
git commit -m "bench: refresh bare-metal numbers"
```

Re-capture after any of:

- CPU / kernel / libc upgrade on the bench machine
- A change to `weir-core`, `weir-server`, or the WAB on-disk format
- A change to the load suite's scenarios or sample counts

## Environment annotations

Every captured `bare-metal.md` must carry a header that names:

```text
Captured: <UTC timestamp>
Host: <hostname>
CPU: <vendor, model, MHz, cores/threads, microcode>
Memory: <total MiB, type if known>
Kernel: <uname -r>
Storage (wab_dir):
  Path: <absolute path>
  Filesystem: <type, mount options>
  Device: <model, rotational/SSD, NVMe/SATA>
Tunables:
  Governor: <performance | schedutil | …>
  SMT: <on | off>
  Turbo: <on | off>
  vm.dirty_background_bytes: <value>
  vm.dirty_bytes: <value>
```

A bare-metal run without this header is not a bare-metal result.

**Two captures are comparable only if the storage rows match.** `Durable`-tier
throughput is bounded by one `fdatasync` per record on a single connection — the
daemon acks frame N before reading frame N+1
(`crates/weir-server/src/socket/connection.rs:485-491`) — so the number reports
the platform's fsync primitive as much as it reports weir. Measured on one M3
Max, the three primitives this codebase can reach cost 36 µs (plain `fsync(2)`,
which is what `File::sync_all` is on Linux), 238 µs (`F_BARRIERFSYNC`, what the
WAB record path uses on macOS — `crates/weir-server/src/wab/segment.rs:672-685`)
and 3,965 µs (`F_FULLFSYNC`, what `File::sync_all` becomes on macOS, and what a
segment seal's parent-directory fsync and the drain's confirm path both pay).
`run_bare_metal_bench.sh` runs on macOS as well as Linux, so this is a real
hazard: a capture on a different OS or filesystem is a **different
measurement**, not a regression.
