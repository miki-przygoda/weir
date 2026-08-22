# dm-flakey control experiment — does `drop_writes` do what Phase 2 needs?

## Why this exists

The chaos design spec's risk table requires it:

> `dm-flakey drop_writes` semantics differ from expectation → **Validate against
> a deliberately-non-durable control program before trusting any weir result.**

Phase 2 injects `drop_writes` and then asks whether weir lost an acked record.
If the injector does not behave as assumed, every number Phase 2 produces is
measuring the harness rather than weir — and in the direction that manufactures
**false violations against weir**, which the spec calls worse than having no
harness at all. Phase 1 cost eight harness defects against three in weir, and
quiescence alone was wrong four times in both directions. This is not a
formality.

## Run metadata

- **Kernel:** 7.0.0-28-generic
- **Target:** dm-flakey v1.5.0 (`dmsetup targets`)
- **Venue:** bare metal, i9-9900K
- **Script:** [`2026-08-22-control-block-level.sh`](2026-08-22-control-block-level.sh)

Measured at **block level** — `dd` straight onto the mapped device, no
filesystem between the fault and the observation. A filesystem would add its own
journalling and writeback, and the question here is what the *device* does.

## Result: `RESULT=CONTROL_OK`

Both directions were run against the same stack. This is the whole point: a
one-directional result cannot tell a working fault from broken plumbing.

| Direction | Table | Observation |
|---|---|---|
| **B** — control | `0 N flakey <dev> 0 60 0 1 drop_writes` (up_interval=60, down_interval=0 → feature inert) | write reached the backing store; read back as `UP-INTERVAL-WRITE…` |
| **A** — fault | `0 N flakey <dev> 0 0 60 1 drop_writes` (up_interval=0, down_interval=60 → engaged) | backing store **still held Direction B's bytes**; the new write was discarded, and `dd conv=fsync` returned **exit 0** |

Reads in both directions were taken from the *backing loop device* after
removing the flakey mapping, so the observation never passes through the fault.

**`drop_writes` is a lying disk: it reports fsync success and discards the
data.** Direction B is what makes that meaningful — the identical read path did
surface a write when the fault was disengaged, so "the plumbing is broken" is
ruled out as the explanation for Direction A.

## Consequence for Phase 2's episode protocol

This is *not* what real power loss does. Under real power loss, a write whose
fsync **returned** is durable. Inside a `drop_writes` window, it is not.

So if the harness engages `drop_writes` and lets weir keep acking inside the
window, weir will show acked-but-missing records **at the Sync tier**, and a
naive oracle reports an I1 durability violation against weir that is really the
harness lying to weir. Phase 2 must therefore choose an episode protocol
explicitly:

- **(a) engage `drop_writes`, then immediately `kill -9`** — the window holds
  approximately zero acks. This is the faithful power-loss model, and it is the
  one that makes the Buffered result mean what Phase 2 wants it to mean:
  *Buffered loses because Buffered acks before fsync*, not because the window
  ate the write.
- **(b) keep a window, and exempt every record acked at or after the fault
  instant** — honest but weaker. The report must state how many records landed
  in the exempt zone, because a large exempt zone means the run proved little.
- **(c) use `error_writes` instead**, so fsync *fails* rather than lying. This
  tests "weir fails closed and nacks", which is real and worth measuring, but it
  is a different fault class and must not be relabelled as power loss.

A Buffered loss of exactly zero across every episode should be read as
**suspicious, not as success** — under a correct power-loss model it suggests
the injector was not actually active.

## Second finding: the feature flags default to erroring

Submit a flakey table with **no feature arguments** and the kernel fills them
in:

```
submitted:  0 32768 flakey /dev/loopN 0 60 0
reported:   0 32768 flakey 7:19 0 60 0 2 error_reads error_writes
```

With an explicit flag it is preserved verbatim:

```
submitted:  0 32768 flakey /dev/loopN 0 0 60 1 drop_writes
reported:   0 32768 flakey 7:19 0 0 60 1 drop_writes
```

So a table that *looks* like a harmless pass-through is actually an
**erroring** device when down. Phase 2 must always state feature flags
explicitly and should read the table back after creating it, because the
difference between `error_writes` and `drop_writes` is the difference between
testing "weir fails closed" and testing "weir survives a lying disk".

## Third finding: `dmsetup remove` is not synchronous

Found by breaking this script and watching it lie.

`dmsetup remove` can return **before** udev has released the device node. The
next `dmsetup create` on the same name then fails with `EBUSY` — and the
previous table is *still installed*. A script that does not check the create's
exit status will carry on and measure *the old mapping*, then report a verdict
about a device it never created.

That is precisely what happened here: the first portable version of this script
reported `RESULT=NO_DROP` — "drop_writes did not drop the write" — when what had
actually occurred was that Direction A's create failed and Direction B's
pass-through table was still in place. The original version had been slow enough
to hide the race, because it went through a `sudo` round-trip per command.

Two rules for the Phase 2 injector, which will cycle flakey tables constantly:

1. **A failed `dmsetup create` must abort the episode**, never fall through to
   measurement. A verdict from an unknown device configuration is worse than no
   verdict.
2. **After removing a mapping, poll `dmsetup info` until it is actually gone**
   (or use `dmsetup remove --retry`) before creating the next one.

Read the table back after creating it. Between this and the implicit
`error_reads error_writes` default above, there are two independent ways for a
flakey device to be running a different configuration than the one intended —
and both of them fail towards *false violations against weir*.

## A note on an earlier, retracted claim

An earlier automated run reported that `drop_writes` "did not behave as required
in BOTH directions". **That conclusion was never established.** The agent that
was to run the control experiment died before running it, and the surrounding
orchestration turned a null result into a finding. The measurement above is the
first time this was actually run to completion.

The orchestration now distinguishes `control-experiment-did-not-run` from
`control-experiment-failed` so that a missing result can never again be reported
as a negative one. The general lesson is worth keeping: **a null result and a
refuted hypothesis are not the same thing**, and any harness that conflates them
will eventually publish a conclusion nobody measured.

There is also a labelling disagreement worth recording: a source-level reading
of the Linux v6.17 dm-flakey implementation describes `drop_writes` as
"faithful power-loss emulation", while the measurement above calls it a lying
disk. Both are consistent — they agree entirely on the mechanism and differ only
on what to call it. The disagreement dissolves into the episode-protocol choice
above, which is why that choice is made explicit rather than assumed.

## Status: Phase 2 is not implemented

This experiment clears the injector for use. It does **not** mean Phase 2 can be
run, and the distinction has already cost time once:

- `chaos/orchestrator/dm_stack.py` builds loop → ext4 → mount and nothing else.
  The dm-flakey/dm-delay layer exists only as a comment marking where it would
  go.
- `chaos/orchestrator/run.py`'s episode loop injects `kill_random`
  unconditionally.
- Both shipped schedules carry an empty `[faults]` table.

Phase 2 is therefore *build the injection, then run it* — not *run an existing
suite*. Per the design spec it also wants eBPF targeted mid-fsync kills (F1
full) plus tier-aware I1, with the exit criterion being **a tier × fault matrix
populated with measured numbers**. The eBPF probe is its own significant piece
of work; the spec's risk table permits falling back to the random killer
provided the report states which mode produced each number.

## Reproducing

Needs root, a free loop device, and `dm-flakey` loaded — the module is **not**
loaded by default on a stock Ubuntu install, and `modprobe dm-flakey` does not
persist across reboots:

```bash
dmsetup targets | grep flakey || sudo modprobe dm-flakey
sudo bash docs/benchmarks/chaos-phase2/2026-08-22-control-block-level.sh
```

The script is self-contained: it creates its own sparse backing file and loop
device, tears down everything it made on exit, and prints `RESULT=` with one of
`CONTROL_OK`, `CONTROL_INVALID`, `NO_DROP`, or `DROPS_BUT_ERRORS`.
