# Phase 2: power loss, and what it costs each tier

## Problem

weir's headline claim is *"an ack is never a false ack."* Every chaos fault to
date is a `kill -9`, which **does not lose the page cache**. So that claim is
evidenced against process crashes only. Five soaks, 100,606,180 acked records,
1,226 crashes, zero violations — and none of it touches power loss.

Phase 2 was designed for this in
[`2026-07-25-chaos-fault-injection-design.md`](2026-07-25-chaos-fault-injection-design.md)
and never written. `chaos/orchestrator/dm_stack.py` builds loop → ext4 → mount
and stops; the dm-flakey layer is a comment. `run.py` injects `kill_random`
unconditionally. Every schedule ships an empty `[faults]` table.

This is the last gap blocking a 2.0.0 release that can honestly describe its own
durability guarantee.

## What the fault actually is

`dm-flakey drop_writes` was validated on real hardware
([control experiment](../../benchmarks/chaos-phase2/2026-08-22-dm-flakey-control-experiment.md)):
it discarded a write while `dd conv=fsync` returned **exit 0**. It is a **lying
disk**, and that is *not* what power loss does — under real power loss, a write
whose fsync returned is durable.

So a naive protocol that engages `drop_writes` and lets weir keep acking inside
the window produces acked-but-missing records at the durable tier, and an oracle
that calls that an I1 violation is reporting **the harness lying to weir**.

### Protocol (a), chosen

Engage `drop_writes`, then **immediately** `kill -9`. The window holds
approximately zero acks. This is the faithful power-loss model, and it is what
makes the Buffered result attributable: *Buffered loses because Buffered acks
before fsync*, not because the window ate the write.

The control doc's alternatives are deliberately not built:
(b) a wide window with post-fault exemption is "honest but weaker" and needs the
exempt-zone size reported; (c) `error_writes` tests fail-closed nacking, which is
real and worth measuring but is **a different fault class and must not be
relabelled power loss**.

### The episode

```
steady load  →  engage drop_writes  →  kill -9 (immediately)
             →  disengage           →  restart daemon  →  drain  →  verify
```

Disengage before restart: recovery must run against an honest disk, or the
restart is testing a second fault rather than recovery from the first.

## The contract being tested

| Tier | `kill -9` (process crash) | `drop_writes` (power loss) |
|---|---|---|
| **Durable** | zero loss required | **zero loss required** |
| **Buffered** | zero loss required | **loss permitted — and quantified** |

`Buffered` acks after the in-memory write, before any fsync. Losing records to
power loss is its documented contract, not a defect. Phase 2's job is to
**measure how much**, which no run has ever done.

### The negative control

> A Buffered loss of exactly zero across every episode should be read as
> **suspicious, not as success** — under a correct power-loss model it suggests
> the injector was not actually active.

This is the sharpest line in the control document and it becomes an explicit
harness check. A Phase 2 run where Buffered lost nothing must be reported as
**inconclusive**, not green. Any test that cannot fail proves nothing, and an
injector that silently detaches is exactly how a chaos harness starts lying.

## Tier-aware I1

`verify.py` treats all tiers alike today — deliberately. Its module docstring
says tier-aware I1 "arrives in Phase 2 with dm-flakey."

**A run is single-tier.** `loadgen` takes one `--tier` char for the whole run, so
this is a run-level rule, not a per-record one. The ledger already records the
tier per record (`seq tier t_micros rtt_micros outcome`), which the oracle
currently parses past and ignores — so no ledger format change is needed, and the
per-record field can be used to *verify* the run really was single-tier rather
than trusting the schedule.

The rule:

- **Durable + any fault** → an acked record not delivered is an **I1 violation**.
- **Buffered + power loss** → an acked record not delivered is **expected loss**,
  counted and reported, never a violation.
- **Buffered + process crash** → still a violation. `kill -9` does not lose the
  page cache, so a Buffered ack must survive it. This is the existing Phase 1
  contract and Phase 2 must not weaken it.

That last row is the trap: it would be easy to make "Buffered" globally exempt
and silently discard the Phase 1 guarantee. The exemption is keyed on **tier AND
fault**, never tier alone.

### Oracle discipline

This modifies the oracle that was rebuilt on 2026-08-24. The same rule applies:
the current tier-blind behaviour is kept as the reference, and the tier-aware
version must **prove it agrees** on Phase 1 inputs — a `kill_random` run must
produce byte-identical `VerifyResult`s under both. A legitimate new exemption
must not become a licence to weaken I1 generally.

## Tier characters

`'B'` (Batched) is **retired**, following the durability collapse that made
`Sync` and `Batched` one tier. New canonical char is `'D'`.

| Char | Meaning | Status |
|---|---|---|
| `D` | Durable | canonical |
| `S` | Durable | **accepted** — every existing schedule and all five historical `ledger.log` files use it |
| `B` | — | **rejected** |
| `U` | Buffered | canonical |

`'S'` must keep parsing for the same reason the retired `0x02` wire byte keeps
decoding: the oracle reads historical ledgers, and rejecting `'S'` would make it
unable to verify runs already banked. Decode permissive, emit canonical.

## Prerequisite: `chaos/` was missed by the durability sweep

`chaos/` is a separate cargo project (`weir-chaos`, `publish = false`), not a
workspace member, so `cargo clippy --workspace` never compiled it and the
durability collapse's call-site sweep did not reach it. It builds today with
deprecation warnings at `chaos/src/bin/loadgen.rs:176,177,547,548`.

Not broken — the aliases resolve correctly, which is what a deprecation path is
for — but weir's own harness should not use weir's own deprecated names, and the
gate structurally cannot catch it. Fixed as part of this work, since `loadgen.rs`
is a file this phase touches anyway.

## dm-flakey mechanics that will bite

Both from the control experiment, both of which silently give you a device
configured differently than you asked, and both failing towards **false**
violations against weir:

1. **A table with no feature arguments comes back as `2 error_reads
   error_writes`.** An innocuous-looking pass-through is an *erroring* device
   when down. Always pass explicit feature args and verify with `dmsetup table`.
2. **`dmsetup remove` is not synchronous.** It returns before udev releases the
   node; the next `create` fails `EBUSY` and the **previous table stays
   installed**, so the next episode measures the old mapping. Poll `dmsetup
   info` until a removed mapping is really gone, and abort on a failed create.

Table syntax:

```
engaged:    0 <sectors> flakey <dev> 0 0 <down> 1 drop_writes   # up=0, down=N
disengaged: 0 <sectors> flakey <dev> 0 <up> 0 1 drop_writes     # up=N, down=0
```

## Out of scope

- `error_writes` (fail-closed nacking) — a different fault class, deliberately
  deferred.
- Protocol (b)'s wide window with post-fault exemption.
- dm-delay (slow disk), `ENOSPC`, read-only remount — Phase 3.
- eBPF-targeted mid-fsync kills — the design doc's "F1 full".

## Venue

**beast is the only confirmed-capable machine** (dm-flakey v1.5.0, control
experiment passed). Docker Desktop **cannot** run this: its LinuxKit kernel
exposes only `crypt/striped/linear/error` and has no `/lib/modules` to load a
target into. The Pi is unverified — it was powered off before `dmsetup targets`
could be checked.

Consequence for this work: everything here is written and unit-tested on the
laptop, and the integration run waits for beast. Pure logic — table
construction, tier rules, the negative control — is testable without
device-mapper and must be tested that way, so the beast run exercises
integration rather than discovering syntax errors.
