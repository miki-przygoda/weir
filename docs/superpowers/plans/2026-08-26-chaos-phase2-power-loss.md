# Chaos Phase 2: Power Loss Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Inject real power loss with `dm-flakey drop_writes`, and measure what it costs each durability tier — closing the last gap before a 2.0.0 that can honestly describe its own guarantee.

**Architecture:** A dm-flakey target sits between the loop device and ext4. Protocol (a): engage `drop_writes`, then *immediately* `kill -9`, so the lying window holds ~zero acks. Tier-aware I1 permits Buffered loss under power loss only — never under `kill -9` — and a run where Buffered loses nothing is reported **inconclusive**, not green.

**Tech Stack:** Python 3.11+ orchestrator, `dmsetup`/`losetup`/`mkfs.ext4`, Rust load generator.

**Spec:** `docs/superpowers/specs/2026-08-26-chaos-phase2-power-loss.md`

## Global Constraints

- **Protocol (a) only.** Engage `drop_writes`, then immediately `kill -9`. Do not build protocol (b)'s wide window, and do not build `error_writes` — a different fault class that must not be relabelled power loss.
- **Disengage before restarting the daemon.** Recovery must run against an honest disk, or the restart tests a second fault rather than recovery from the first.
- **Always pass explicit dm-flakey feature args.** A table with none comes back as `2 error_reads error_writes` — an erroring device, not a pass-through. Verify with `dmsetup table` after every create and reload.
- **`dmsetup remove` is asynchronous.** It returns before udev releases the node; the next create fails `EBUSY` and the **previous table stays installed**, so the next episode silently measures the old mapping. Poll `dmsetup info` until gone, and abort on a failed create.
- **Tier-aware I1 is keyed on tier AND fault, never tier alone.** Buffered stays non-exempt under `kill -9` — a process crash does not lose the page cache, and the Phase 1 contract must survive Phase 2's new exemption.
- **Zero Buffered loss across every episode is INCONCLUSIVE, not green.**
- `'D'` is the canonical tier char, `'S'` is accepted (historical ledgers), `'B'` is rejected, `'U'` is Buffered.
- **beast is the only venue that can run this.** Everything here must be unit-testable without root or device-mapper, so the beast run exercises integration rather than discovering syntax errors.
- Python gate: `cd chaos/orchestrator && python3 -m pytest -q`. Rust gate for the chaos crate: `cd chaos && cargo build && cargo test`.

## File Structure

| File | Responsibility |
|---|---|
| `chaos/src/bin/loadgen.rs`, `chaos/src/lib.rs` (modify) | Tier chars: `'D'` canonical, `'B'` retired, deprecated aliases removed |
| `chaos/orchestrator/dm_flakey.py` (create) | **Pure** table construction + parsing. No subprocess calls — fully unit-testable |
| `chaos/orchestrator/dm_stack.py` (modify) | Insert/remove the dm layer; engage/disengage; the async-remove guard |
| `chaos/orchestrator/run.py` (modify) | `[faults]` dispatch; the protocol (a) episode |
| `chaos/orchestrator/verify.py` (modify) | Tier-aware I1, with the tier-blind version kept as reference |
| `chaos/orchestrator/report.py` (modify) | Buffered-loss quantification and the inconclusive verdict |
| `chaos/schedules/powerloss-*.toml` (create) | One schedule per tier |

Splitting table construction into its own module is the load-bearing decision: it makes the part that is easy to get silently wrong (feature args, up/down intervals, sector counts) testable on a laptop with no root.

---

### Task 1: Retire `'B'`, and stop using weir's deprecated names

**Files:**
- Modify: `chaos/src/bin/loadgen.rs:174-181, 547-548`, `chaos/src/lib.rs:83, 104`
- Test: `chaos/src/bin/loadgen.rs` (existing `mod tests`)

**Interfaces:**
- Produces: `tier_from_char('D'|'S') -> Durable`, `('U') -> Buffered`, `('B') -> None`.

- [ ] **Step 1: Write the failing test**

Replace the tier assertions in `loadgen.rs`'s test module:

```rust
#[test]
fn tier_chars_map_to_the_two_live_tiers() {
    assert_eq!(tier_from_char('D'), Some(Durability::Durable));
    // 'S' still parses: every schedule and all five historical ledger.log
    // files use it, and the oracle must stay able to verify banked runs.
    assert_eq!(tier_from_char('S'), Some(Durability::Durable));
    assert_eq!(tier_from_char('U'), Some(Durability::Buffered));
    // 'B' is retired with the Batched tier it named.
    assert_eq!(tier_from_char('B'), None);
    assert_eq!(tier_from_char('x'), None);
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cd chaos && cargo test tier_chars`
Expected: FAIL — `'B'` currently returns `Some(Durability::Batched)`.

- [ ] **Step 3: Implement**

`loadgen.rs`:

```rust
/// Maps a `--tier` char to a durability tier.
///
/// `'D'` is canonical. `'S'` is accepted because every existing schedule and
/// every historical `ledger.log` uses it — rejecting it would make the oracle
/// unable to verify runs already banked. `'B'` (Batched) is retired along with
/// the tier it named.
fn tier_from_char(c: char) -> Option<Durability> {
    match c {
        'D' | 'S' => Some(Durability::Durable),
        'U' => Some(Durability::Buffered),
        _ => None,
    }
}
```

`lib.rs:83` doc comment and the `debug_assert!` at `:104`: the valid set becomes `'D' | 'S' | 'U'`. Keep `'S'` in the assert — historical ledgers replay through this type.

- [ ] **Step 4: Verify the whole chaos crate is warning-free**

```bash
cd chaos && cargo build 2>&1 | grep -c 'deprecated'
```
Expected: `0`. This crate is not a workspace member, so `cargo clippy --workspace` never compiled it — that is why these four uses survived the durability collapse's sweep.

Then: `cargo test` — all green.

- [ ] **Step 5: Commit**

```bash
git add chaos/src
git commit -F - <<'MSG'
refactor(chaos): retire the 'B' tier char, stop using deprecated names

chaos/ is a separate cargo project, not a workspace member, so
`cargo clippy --workspace` never compiled it and the durability collapse's
call-site sweep did not reach it. Four uses of the deprecated Sync/Batched
aliases survived here.

'B' is retired with the tier it named. 'D' is canonical. 'S' still parses:
every existing schedule and all five historical ledger.log files use it, and
rejecting it would make the oracle unable to verify runs already banked — the
same decode-permissive logic that keeps the retired 0x02 wire byte alive.
MSG
```

---

### Task 2: Pure dm-flakey table construction

**Files:**
- Create: `chaos/orchestrator/dm_flakey.py`
- Create: `chaos/orchestrator/test_dm_flakey.py`

**Interfaces:**
- Produces: `flakey_table(device, sectors, engaged, down_secs=60)`, `parse_table(line)`, `table_is_engaged(line)`, `FEATURE = "drop_writes"`.

- [ ] **Step 1: Write the failing tests**

`chaos/orchestrator/test_dm_flakey.py`:

```python
"""Tests for dm-flakey table construction.

This module is pure on purpose. The two ways a flakey device silently ends up
configured differently than you asked — omitted feature args, and a stale table
surviving an async remove — are both invisible until a run reports a violation
that never happened. Table construction is the half that can be tested on a
laptop with no root, so it is tested exhaustively here.
"""
import unittest

import dm_flakey


class TestTableConstruction(unittest.TestCase):
    def test_engaged_table_is_down_for_the_whole_interval(self):
        # up_interval=0, down_interval=N -> the fault is always active.
        t = dm_flakey.flakey_table("/dev/loop7", 65536, engaged=True, down_secs=60)
        self.assertEqual(t, "0 65536 flakey /dev/loop7 0 0 60 1 drop_writes")

    def test_disengaged_table_is_up_for_the_whole_interval(self):
        # up_interval=N, down_interval=0 -> the feature is inert.
        t = dm_flakey.flakey_table("/dev/loop7", 65536, engaged=False, down_secs=60)
        self.assertEqual(t, "0 65536 flakey /dev/loop7 0 60 0 1 drop_writes")

    def test_feature_args_are_never_omitted(self):
        # A table with NO feature args comes back from the kernel as
        # `2 error_reads error_writes` — an ERRORING device, not a pass-through,
        # and it fails towards false violations against weir.
        for engaged in (True, False):
            t = dm_flakey.flakey_table("/dev/loop0", 1024, engaged=engaged)
            self.assertIn("1 drop_writes", t)
            self.assertNotIn("error_writes", t)

    def test_rejects_a_zero_or_negative_sector_count(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                dm_flakey.flakey_table("/dev/loop0", bad, engaged=True)


class TestTableParsing(unittest.TestCase):
    def test_reads_back_an_engaged_table(self):
        # `dmsetup table` reports the device as major:minor, not by path.
        line = "0 65536 flakey 7:19 0 0 60 1 drop_writes"
        self.assertTrue(dm_flakey.table_is_engaged(line))

    def test_reads_back_a_disengaged_table(self):
        line = "0 65536 flakey 7:19 0 60 0 1 drop_writes"
        self.assertFalse(dm_flakey.table_is_engaged(line))

    def test_detects_the_kernel_substituting_erroring_defaults(self):
        # This is what you get back if you submit no feature args.
        line = "0 32768 flakey 7:19 0 60 0 2 error_reads error_writes"
        with self.assertRaises(dm_flakey.UnexpectedTable):
            dm_flakey.table_is_engaged(line)

    def test_rejects_a_table_that_is_not_flakey_at_all(self):
        with self.assertRaises(dm_flakey.UnexpectedTable):
            dm_flakey.table_is_engaged("0 65536 linear 7:19 0")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_flakey.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'dm_flakey'`.

- [ ] **Step 3: Implement**

`chaos/orchestrator/dm_flakey.py`:

```python
"""dm-flakey table construction and read-back.

Pure by design: no subprocess, no root, no device. The two failure modes that
matter are both silent, and both fail towards FALSE violations against weir —
so they are pinned by unit tests rather than discovered on the target machine.

1. Submit a table with no feature arguments and the kernel fills them in as
   `2 error_reads error_writes`. An innocuous-looking pass-through is an
   ERRORING device when down.
2. `dmsetup table` reports the underlying device as `major:minor`, not the path
   you submitted — so a naive round-trip comparison never matches.
"""

#: The only feature this phase injects. `error_writes` (fail-closed nacking) is
#: a different fault class and must not be relabelled power loss.
FEATURE = "drop_writes"


class UnexpectedTable(Exception):
    """A device-mapper table is not the flakey table we asked for."""


def flakey_table(device, sectors, engaged, down_secs=60):
    """One dm-flakey table line.

    `engaged` selects which interval covers the whole cycle:
      engaged   -> up=0, down=down_secs  (the fault is always active)
      disengaged-> up=down_secs, down=0  (the feature is inert)

    Both forms carry explicit feature args. Never emit a table without them.
    """
    if sectors <= 0:
        raise ValueError(f"sectors must be positive, got {sectors}")
    up, down = (0, down_secs) if engaged else (down_secs, 0)
    return f"0 {sectors} flakey {device} 0 {up} {down} 1 {FEATURE}"


def parse_table(line):
    """Splits a `dmsetup table` line into its fields.

    Returns a dict with `sectors`, `target`, `device`, `up`, `down`, `features`.
    Raises `UnexpectedTable` for anything that is not a single-feature
    `drop_writes` flakey table — including the erroring default the kernel
    substitutes when feature args are omitted.
    """
    parts = line.strip().split()
    if len(parts) < 8 or parts[2] != "flakey":
        raise UnexpectedTable(f"not a flakey table: {line!r}")
    features = parts[8:]
    if features != [FEATURE]:
        raise UnexpectedTable(
            f"expected exactly [{FEATURE!r}], got {features!r} — a table "
            f"submitted without feature args comes back as "
            f"['error_reads', 'error_writes'], which is an ERRORING device: {line!r}"
        )
    return {
        "sectors": int(parts[1]),
        "target": parts[2],
        "device": parts[3],
        "up": int(parts[5]),
        "down": int(parts[6]),
        "features": features,
    }


def table_is_engaged(line):
    """True if this table has the fault active (up=0, down>0)."""
    t = parse_table(line)
    return t["up"] == 0 and t["down"] > 0
```

- [ ] **Step 4: Run and watch it pass**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_flakey.py`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/dm_flakey.py chaos/orchestrator/test_dm_flakey.py
git commit -F - <<'MSG'
feat(chaos): dm-flakey table construction, as a pure module

Split out from the device operations so the half that is easy to get silently
wrong is testable on a laptop with no root — beast is the only machine that can
run Phase 2, and discovering a table-syntax error there wastes a trip.

Both pinned failure modes fail towards FALSE violations against weir: a table
submitted without feature args comes back as `2 error_reads error_writes`, an
ERRORING device rather than a pass-through; and `dmsetup table` reports the
underlying device as major:minor, so a naive round-trip never matches.
MSG
```

---

### Task 3: Insert the dm layer into the stack

**Files:**
- Modify: `chaos/orchestrator/dm_stack.py`
- Modify: `chaos/orchestrator/test_dm_stack.py`

**Interfaces:**
- Consumes: Task 2's `flakey_table`, `table_is_engaged`, `UnexpectedTable`.
- Produces: `DmStack(..., with_flakey=False)`, `.engage_fault()`, `.disengage_fault()`, `.fault_device` (the path mkfs/mount target).

- [ ] **Step 1: Write the failing tests**

Add to `chaos/orchestrator/test_dm_stack.py` (mock `_run`; no root needed):

```python
class TestFlakeyLayer(unittest.TestCase):
    def test_without_flakey_the_stack_targets_the_loop_device(self):
        s = dm_stack.DmStack("/tmp/img", 512, "/mnt/x", with_flakey=False)
        s.loop_device = "/dev/loop7"
        self.assertEqual(s.fault_device, "/dev/loop7")

    def test_with_flakey_the_stack_targets_the_mapper_device(self):
        s = dm_stack.DmStack("/tmp/img", 512, "/mnt/x", with_flakey=True)
        s.loop_device = "/dev/loop7"
        s.dm_name = "weir-chaos-flakey"
        self.assertEqual(s.fault_device, "/dev/mapper/weir-chaos-flakey")

    def test_engage_is_refused_when_the_layer_was_never_built(self):
        # Silently doing nothing here would produce a "power loss" episode with
        # no power loss in it, and a green run proving nothing.
        s = dm_stack.DmStack("/tmp/img", 512, "/mnt/x", with_flakey=False)
        with self.assertRaises(RuntimeError):
            s.engage_fault()
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_stack.py -k Flakey`
Expected: FAIL — `DmStack() got an unexpected keyword argument 'with_flakey'`.

- [ ] **Step 3: Implement**

In `dm_stack.py`, add `with_flakey=False` to `__init__` (defaulting False so every existing Phase 1 schedule behaves exactly as before), plus `self.dm_name = None`.

Add a `fault_device` property returning `/dev/mapper/<dm_name>` when the layer exists, else `self.loop_device`.

In `setup()`, after `losetup` and **before** `mkfs.ext4`, when `with_flakey`:

```python
        if self.with_flakey:
            # Sector count of the loop device — the flakey table must map the
            # whole device or the filesystem sees a truncated disk.
            sectors = int(_run(["blockdev", "--getsz", self.loop_device]).stdout.strip())
            self.dm_name = f"weir-chaos-flakey-{os.getpid()}"
            # Created DISENGAGED. mkfs and the steady-state workload must run
            # against an honest disk; the fault is engaged per-episode.
            table = dm_flakey.flakey_table(self.fault_device_backing, sectors,
                                           engaged=False)
            self._dm_create(self.dm_name, table)
```

Then `mkfs.ext4` and `mount` target `self.fault_device`, not `self.loop_device`.

`_dm_create` must **verify the installed table** — the kernel silently substitutes erroring defaults for omitted feature args, and a create that failed `EBUSY` leaves the previous table in place:

```python
    def _dm_create(self, name, table):
        _run(["dmsetup", "create", name, "--table", table])
        installed = _run(["dmsetup", "table", name]).stdout.strip()
        # Raises UnexpectedTable if the kernel substituted error_reads/
        # error_writes, or if a stale table survived a failed create.
        if dm_flakey.table_is_engaged(installed):
            raise RuntimeError(
                f"{name} came up ENGAGED; it must be created disengaged so mkfs "
                f"and steady-state load run against an honest disk. Got: {installed}"
            )
```

`teardown()` gains a `dmsetup remove` step between `umount` and `losetup --detach`, using the polling guard from Task 4.

- [ ] **Step 4: Run and watch it pass**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_stack.py`
Expected: PASS, including the pre-existing tests — `with_flakey=False` must leave Phase 1 behaviour untouched.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/dm_stack.py chaos/orchestrator/test_dm_stack.py
git commit -F - <<'MSG'
feat(chaos): put a dm-flakey layer under the filesystem

losetup -> dmsetup -> mkfs -> mount, so ext4 sits on the fault device rather
than the loop device directly. Defaults to off, so every Phase 1 schedule
behaves exactly as before.

Created DISENGAGED and verified by reading the table back: mkfs and the
steady-state workload must run against an honest disk, and a device that comes
up engaged would corrupt the filesystem before the first episode. The read-back
also catches the kernel substituting `error_reads error_writes` for omitted
feature args.

engage_fault() raises rather than no-oping when the layer was never built — a
"power loss" episode with no power loss in it would go green while proving
nothing.
MSG
```

---

### Task 4: The async-remove guard

**Files:**
- Modify: `chaos/orchestrator/dm_stack.py`
- Modify: `chaos/orchestrator/test_dm_stack.py`

**Interfaces:**
- Produces: `_dm_remove(name, timeout_s=10)`.

- [ ] **Step 1: Write the failing test**

```python
class TestAsyncRemoveGuard(unittest.TestCase):
    def test_remove_polls_until_the_mapping_is_really_gone(self):
        # `dmsetup remove` returns before udev releases the node. If the next
        # create runs too early it fails EBUSY, the PREVIOUS table stays
        # installed, and the next episode measures the old mapping — a
        # confident result about a device that was never created.
        calls = []
        def fake_run(cmd, check=True):
            calls.append(cmd)
            if cmd[:2] == ["dmsetup", "info"]:
                # Present twice, then gone.
                n = sum(1 for c in calls if c[:2] == ["dmsetup", "info"])
                return _Result(0 if n <= 2 else 1, "", "")
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", fake_run):
            dm_stack._dm_remove("weir-chaos-flakey", timeout_s=5)
        infos = [c for c in calls if c[:2] == ["dmsetup", "info"]]
        self.assertGreaterEqual(len(infos), 3, "must poll, not fire once")

    def test_remove_raises_if_the_mapping_never_goes_away(self):
        def always_present(cmd, check=True):
            return _Result(0, "", "")
        with mock.patch.object(dm_stack, "_run", always_present):
            with self.assertRaises(RuntimeError):
                dm_stack._dm_remove("stuck", timeout_s=0.3)
```

(Define a small `_Result` namedtuple with `returncode`, `stdout`, `stderr` at the top of the test module if one does not already exist.)

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_stack.py -k AsyncRemove`
Expected: FAIL — `module 'dm_stack' has no attribute '_dm_remove'`.

- [ ] **Step 3: Implement**

```python
def _dm_remove(name, timeout_s=10):
    """Removes a mapping and WAITS for it to really be gone.

    `dmsetup remove` is not synchronous: it returns before udev releases the
    node. The next `create` then fails EBUSY and — this is the dangerous part —
    the PREVIOUS table stays installed, so the following episode measures the
    old mapping and reports a confident result about a device it never created.
    Found the hard way: a portable rewrite of the control script ran fast enough
    to lose this race, which the original's per-command sudo round-trip had hidden.
    """
    _run(["dmsetup", "remove", name], check=False)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _run(["dmsetup", "info", name], check=False).returncode != 0:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"dm mapping {name!r} still present {timeout_s}s after remove. "
        f"Refusing to continue: the next create would fail EBUSY and leave the "
        f"stale table installed, so the next episode would measure the wrong device."
    )
```

- [ ] **Step 4: Run and watch it pass**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_dm_stack.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/dm_stack.py chaos/orchestrator/test_dm_stack.py
git commit -F - <<'MSG'
fix(chaos): wait for a dm mapping to really be gone

dmsetup remove returns before udev releases the node. Create too soon and it
fails EBUSY, leaving the PREVIOUS table installed — so the next episode
measures a device it never created and reports a confident wrong answer.

This was found on beast by making the control script portable: it then ran fast
enough to lose a race the original's per-command sudo round-trip had hidden,
and reported RESULT=NO_DROP about a device that was never created.
MSG
```

---

### Task 5: Fault dispatch and the protocol (a) episode

**Files:**
- Modify: `chaos/orchestrator/run.py`
- Modify: `chaos/orchestrator/test_run.py`

**Interfaces:**
- Consumes: Task 3's `engage_fault`/`disengage_fault`.
- Produces: `fault_kind(sched)`, and a `power_loss` episode path.

- [ ] **Step 1: Write the failing test**

```python
class TestFaultDispatch(unittest.TestCase):
    def test_an_empty_faults_table_still_means_kill_random(self):
        # Every Phase 1 schedule ships `[faults]` empty. They must keep doing
        # exactly what they did before, or five banked soaks stop being
        # comparable to anything run after this.
        self.assertEqual(run.fault_kind({"faults": {}}), "kill_random")
        self.assertEqual(run.fault_kind({}), "kill_random")

    def test_power_loss_is_selected_explicitly(self):
        self.assertEqual(
            run.fault_kind({"faults": {"kind": "power_loss"}}), "power_loss")

    def test_an_unknown_fault_kind_is_refused_loudly(self):
        # Falling back to kill_random would silently run a Phase 1 episode
        # under a Phase 2 schedule and report it as power loss.
        with self.assertRaises(ValueError):
            run.fault_kind({"faults": {"kind": "typo"}})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_run.py -k FaultDispatch`
Expected: FAIL — `module 'run' has no attribute 'fault_kind'`.

- [ ] **Step 3: Implement**

```python
def fault_kind(sched):
    """Which fault this schedule injects. Defaults to Phase 1's `kill_random`.

    An unknown kind raises rather than falling back: silently running a Phase 1
    episode under a Phase 2 schedule and reporting it as power loss is exactly
    the class of harness lie this project exists to avoid.
    """
    kind = (sched.get("faults") or {}).get("kind", "kill_random")
    if kind not in ("kill_random", "power_loss"):
        raise ValueError(
            f"unknown fault kind {kind!r}; expected 'kill_random' or 'power_loss'")
    return kind
```

At the episode's fault site (currently `daemon.kill9(); daemon.start(...)`):

```python
                if kind == "power_loss":
                    # PROTOCOL (a). Engage, then kill IMMEDIATELY, so the lying
                    # window holds ~zero acks.
                    #
                    # drop_writes is a lying disk: it reports fsync success and
                    # discards the write. Real power loss does not do that — a
                    # write whose fsync returned is durable. So any ack weir
                    # emits inside this window is an ack reality would never
                    # have allowed, and counting it against weir is the harness
                    # lying to weir. Keeping the window at ~zero is what makes
                    # the Buffered result mean "Buffered acks before fsync"
                    # rather than "the window ate the write".
                    stack.engage_fault()
                    daemon.kill9()
                    # Disengage BEFORE restarting: recovery must run against an
                    # honest disk, or the restart tests a second fault rather
                    # than recovery from the first.
                    stack.disengage_fault()
                else:
                    daemon.kill9()
                daemon.start("http://127.0.0.1:9900/ingest")
```

Record the actual kind in the episode JSON — the existing record hardcodes `"fault": "kill_random"` in three places (the abort path and both episode writes). All must use `kind`.

- [ ] **Step 4: Run and watch it pass**

Run: `cd chaos/orchestrator && python3 -m pytest -q`
Expected: PASS, all files. The existing Phase 1 tests must be untouched.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/run.py chaos/orchestrator/test_run.py
git commit -F - <<'MSG'
feat(chaos): dispatch faults from the schedule, and inject power loss

Protocol (a): engage drop_writes, then kill -9 immediately, so the lying window
holds ~zero acks. drop_writes reports fsync success and discards the write,
which real power loss does not do — so an ack emitted inside the window is one
reality would never have allowed, and counting it against weir is the harness
lying to weir. A ~zero window is what makes the Buffered result mean "Buffered
acks before fsync" rather than "the window ate the write".

Disengaged before the restart: recovery must run against an honest disk.

An unknown fault kind raises instead of falling back to kill_random — silently
running a Phase 1 episode under a Phase 2 schedule and labelling it power loss
is the exact class of harness lie this project exists to avoid. An empty
[faults] table still means kill_random, so five banked soaks stay comparable.
MSG
```

---

### Task 6: Tier-aware I1, proven not to weaken Phase 1

**Files:**
- Modify: `chaos/orchestrator/verify.py`
- Modify: `chaos/orchestrator/test_verify.py`
- Modify: `chaos/orchestrator/test_dense_oracle.py`

**Interfaces:**
- Consumes: the ledger's existing per-record tier char at `parts[1]`.
- Produces: `check_counts(..., tier=None, fault=None)`, `VerifyResult.expected_loss`.

- [ ] **Step 1: Write the failing tests**

```python
class TestTierAwareI1(unittest.TestCase):
    def test_durable_loss_is_a_violation_under_power_loss(self):
        r = verify.check(ledger(**{"1": ("ACK", "")}), [],
                         tier="D", fault="power_loss")
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [1])

    def test_buffered_loss_is_expected_under_power_loss(self):
        # Buffered acks after the in-memory write, before any fsync. Losing
        # records to power loss is its documented contract.
        r = verify.check(ledger(**{"1": ("ACK", "")}), [],
                         tier="U", fault="power_loss")
        self.assertTrue(r.ok)
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.expected_loss, 1)

    def test_buffered_loss_is_STILL_a_violation_under_kill9(self):
        # THE TRAP. kill -9 does not lose the page cache, so a Buffered ack
        # must survive it. Exempting Buffered on tier alone would silently
        # discard the Phase 1 contract.
        r = verify.check(ledger(**{"1": ("ACK", "")}), [],
                         tier="U", fault="kill_random")
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [1])

    def test_omitting_tier_and_fault_is_exactly_phase_1_behaviour(self):
        r = verify.check(ledger(**{"1": ("ACK", "")}), [])
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [1])
        self.assertEqual(r.expected_loss, 0)
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_verify.py -k TierAware`
Expected: FAIL — `check() got an unexpected keyword argument 'tier'`.

- [ ] **Step 3: Implement**

Add `tier=None, fault=None` to `check_counts` and `check`, and `expected_loss: int = 0` to `VerifyResult`. The rule:

```python
    # Buffered acks after the in-memory write, before any fsync, so power loss
    # may legitimately eat an acked record. That exemption is keyed on tier AND
    # fault, never tier alone: `kill -9` does not lose the page cache, so a
    # Buffered ack must survive it, and that is the Phase 1 contract this phase
    # must not weaken.
    buffered_powerloss = (tier == "U" and fault == "power_loss")
    if buffered_powerloss:
        expected_loss = len(i1_absent - i1_exempt_seqs)
        i1_missing = []
    else:
        expected_loss = 0
        i1_missing = sorted(i1_absent - i1_exempt_seqs)
```

`DenseAccumulator.check()` takes and forwards the same two arguments.

- [ ] **Step 4: Prove Phase 1 is unchanged**

Extend `test_dense_oracle.py`'s differential test so every randomised comparison ALSO runs with `tier="D", fault="kill_random"` and asserts the result is identical to the no-argument call:

```python
    def test_tier_awareness_does_not_change_phase_1_results(self):
        # The new exemption must be reachable ONLY by Buffered + power_loss.
        # Any Phase 1 input must produce byte-identical results with or without
        # the new arguments, or a legitimate exemption has become a licence to
        # weaken I1 generally.
        rng = random.Random(99)
        ref = verify.ReferenceAccumulator(delivered_run_id=RUN_ID)
        dense = verify.DenseAccumulator(delivered_run_id=RUN_ID)
        for lg, dl in random_batches(rng):
            ref.ingest(lg, dl); dense.ingest(lg, dl)
            plain = dense.check()
            tiered = dense.check(tier="D", fault="kill_random")
            self.assert_same(plain, tiered, "tier-aware must not alter Phase 1")
            self.assert_same(ref.check(), tiered, "and must still match the reference")
```

Run: `cd chaos/orchestrator && python3 -m pytest -q`
Expected: PASS, all files.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py chaos/orchestrator/test_dense_oracle.py
git commit -F - <<'MSG'
feat(chaos): tier-aware I1, keyed on tier AND fault

Buffered acks after the in-memory write, before any fsync, so power loss may
legitimately eat an acked record — that is its documented contract, and Phase 2
exists to measure how much rather than to call it a violation.

The exemption is keyed on tier AND fault, never tier alone. kill -9 does not
lose the page cache, so a Buffered ack must still survive it. Exempting on tier
alone would have silently discarded the Phase 1 contract while looking like a
feature.

The differential test now asserts that any Phase 1 input produces byte-identical
results with and without the new arguments, so a legitimate new exemption cannot
become a licence to weaken I1 generally.
MSG
```

---

### Task 7: The negative control, schedules, and reporting

**Files:**
- Modify: `chaos/orchestrator/report.py`, `chaos/orchestrator/test_report.py`
- Create: `chaos/schedules/powerloss-durable.toml`, `chaos/schedules/powerloss-buffered.toml`

**Interfaces:**
- Consumes: Task 6's `expected_loss`.
- Produces: `powerloss_verdict(records)` returning `"pass" | "inconclusive" | "fail"`.

- [ ] **Step 1: Write the failing test**

```python
class TestPowerLossVerdict(unittest.TestCase):
    def test_buffered_losing_nothing_is_INCONCLUSIVE_not_green(self):
        # "A Buffered loss of exactly zero across every episode should be read
        # as suspicious, not as success — it suggests the injector was not
        # actually active." A test that cannot fail proves nothing.
        recs = [{"tier": "U", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": []} for _ in range(20)]
        self.assertEqual(report.powerloss_verdict(recs), "inconclusive")

    def test_buffered_losing_records_is_a_pass(self):
        recs = [{"tier": "U", "fault": "power_loss", "expected_loss": n,
                 "i1_missing": []} for n in (0, 14, 0, 31)]
        self.assertEqual(report.powerloss_verdict(recs), "pass")

    def test_any_durable_loss_is_a_fail_regardless(self):
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": [7]}]
        self.assertEqual(report.powerloss_verdict(recs), "fail")

    def test_durable_losing_nothing_is_a_pass_not_inconclusive(self):
        # The suspicion rule applies to Buffered only. Durable losing nothing
        # is the contract being upheld, not evidence of a dead injector.
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": []} for _ in range(20)]
        self.assertEqual(report.powerloss_verdict(recs), "pass")
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd chaos/orchestrator && python3 -m pytest -q test_report.py -k PowerLoss`
Expected: FAIL — `module 'report' has no attribute 'powerloss_verdict'`.

- [ ] **Step 3: Implement**

```python
def powerloss_verdict(records):
    """pass | inconclusive | fail for a power-loss run.

    Any durable-tier loss fails outright — that is the contract.

    A Buffered run that lost NOTHING across every episode is `inconclusive`,
    not `pass`. Under a correct power-loss model Buffered should lose
    something; losing nothing suggests the injector never bit. Reporting that
    as success is how a chaos harness starts lying: a test that cannot fail
    proves nothing.
    """
    pl = [r for r in records if r.get("fault") == "power_loss"]
    if not pl:
        return "pass"
    if any(r.get("i1_missing") for r in pl):
        return "fail"
    buffered = [r for r in pl if r.get("tier") == "U"]
    if buffered and sum(r.get("expected_loss", 0) for r in buffered) == 0:
        return "inconclusive"
    return "pass"
```

Surface the verdict, and total Buffered `expected_loss`, in `report.md`.

- [ ] **Step 4: Write the schedules**

`chaos/schedules/powerloss-buffered.toml` — the one that produces the headline number:

```toml
# Power loss against the Buffered tier. THE POINT of Phase 2.
#
# Buffered acks after the in-memory write, before any fsync, so power loss may
# legitimately eat an acked record. That is its documented contract and no run
# has ever measured how much. This one does.
#
# A result of zero loss across every episode is INCONCLUSIVE, not green — see
# powerloss_verdict(). It means the injector probably never bit.
seed = 0x9F1A1
episodes = 200
max_duration_secs = 7200
steady_lo_secs = 20.0
steady_hi_secs = 40.0
quiescence_timeout_secs = 180.0

[load]
threads = 8
record_size = 256
tier = "U"
min_acked_per_episode = 100
min_delivered_per_episode = 1

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
wab_segment_max_bytes = 8388608

[storage]
size_mb = 2048
with_flakey = true

[faults]
kind = "power_loss"
```

`powerloss-durable.toml` is identical except `seed = 0x9F1A2`, `tier = "D"`, and a header noting that **zero loss here is the pass condition**, not a suspicion.

Note `min_delivered_per_episode = 1` on the Buffered schedule: the Phase 1 floor of 100 assumes nothing is lost, which is the opposite of what this run expects.

- [ ] **Step 5: Full gate and commit**

```bash
cd chaos/orchestrator && python3 -m pytest -q
cd .. && cargo build && cargo test
python3 -c "import tomllib;[tomllib.load(open(f'schedules/{s}','rb')) for s in ('powerloss-durable.toml','powerloss-buffered.toml')];print('schedules parse')"
```

```bash
git add chaos/orchestrator/report.py chaos/orchestrator/test_report.py chaos/schedules/
git commit -F - <<'MSG'
feat(chaos): power-loss schedules, and a verdict that can say "inconclusive"

Zero Buffered loss across every episode reports INCONCLUSIVE, not pass. Under a
correct power-loss model Buffered should lose something — it acks before fsync —
so losing nothing suggests the injector never bit. A test that cannot fail
proves nothing, and a silently detached injector is how a chaos harness starts
lying.

The suspicion rule is Buffered-only: durable losing nothing is the contract
being upheld, not a dead injector, and any durable loss fails outright.

The Buffered schedule drops min_delivered_per_episode to 1 — the Phase 1 floor
of 100 assumes nothing is lost, which is precisely what this run does not.
MSG
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| Protocol (a): engage then immediate kill | 5 |
| Disengage before restart | 5 |
| Explicit feature args, verified by read-back | 2, 3 |
| Async-remove polling guard | 4 |
| Tier-aware I1 keyed on tier AND fault | 6 |
| Buffered non-exempt under kill -9 | 6 (explicit test) |
| Negative control: zero loss ⇒ inconclusive | 7 |
| `'D'` canonical, `'S'` accepted, `'B'` rejected | 1 |
| `chaos/` deprecated-alias prerequisite | 1 |
| Everything unit-testable without root | 2 (pure module), 3-7 (mocked `_run`) |
| `error_writes` / protocol (b) NOT built | absent by design |

**Placeholder scan:** none. Every code step carries runnable code; every command step names its expected output.

**Type consistency:** `flakey_table`/`table_is_engaged`/`UnexpectedTable` defined in Task 2, used in Task 3. `_dm_remove` defined in 4, used by 3's teardown — Task 3 references it forward, which is why 4 immediately follows. `fault_kind` in 5. `expected_loss` added to `VerifyResult` in 6, consumed by `powerloss_verdict` in 7.

**Known risk, accepted:** no task can be integration-tested before beast is powered on. Every task is therefore unit-tested against mocked `_run`, and Task 2 is pure specifically so the error-prone half needs no device at all. The first beast run will still be the first time the real kernel sees these tables — the guards in Tasks 3 and 4 exist because that first contact is exactly where the control experiment found both silent failure modes.
