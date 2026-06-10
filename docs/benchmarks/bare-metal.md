# Bare-metal benchmark results

CI numbers in [`latest.md`](latest.md) come from a 2 vCPU shared GitHub
Actions runner. They are useful for catching order-of-magnitude
regressions; they are **not** representative of the hardware weir is
designed to run on, and they are not the numbers any performance claim
should be made against.

This page holds the numbers captured on a real machine with named
hardware. They are the ship gate.

> **Status:** awaiting first capture. Run
> `deploy/run_bare_metal_bench.sh`
> on the target machine and replace this section with the script's
> output.

## Regression policy

| Surface | Gate | Action on violation |
|--------|------|---------------------|
| CI `latest.md` | >10× drop on any scenario, or any scenario going from non-zero to zero | Block the PR; investigate before merge. |
| Bare-metal `bare-metal.md` (this file) | >10% drop in single-thread RPS, >20% increase in `Sync` p99, or any saturation-ramp level regressing from `ok` to dropped I/O | Block the release; investigate before tagging. |

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
