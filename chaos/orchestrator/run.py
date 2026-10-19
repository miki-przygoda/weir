#!/usr/bin/env python3
"""Chaos harness episode driver. Root, Linux only.

Owns every privileged operation: the storage stack, process lifecycle, and
fault injection. The load generator and recorder are *intended* to run
unprivileged, as the observers that must not be able to corrupt what they
measure — but this module execs both as direct children with no privilege
drop, so today they inherit root from this process, same as the daemon under
test. A real drop needs `setuid`/capability plumbing that is its own piece of
work and is deferred; see README.md's Requirements section.

Phase 1 injects one fault class: random SIGKILL (`kill_random`), which is
also the default when a schedule's `[faults]` table is empty or absent —
every Phase 1 schedule stays byte-identical. Phase 2 adds `power_loss`
(dm-flakey `drop_writes`, protocol: kill first, then drop and remount — see
the episode loop below and the Phase 2 spec's "Protocol: kill first, then
drop and remount" section for why killing first is load-bearing, not
cosmetic). `fault_kind()` dispatches between them and raises on anything
else, rather than silently falling back. dm-delay, ENOSPC, remount and
dead-letter classes remain for Phase 3.

Usage: sudo python3 run.py schedules/smoke.toml
"""
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
import tomllib

import dm_stack
import faults
import quiescence
import report
import verify

HERE = os.path.dirname(os.path.abspath(__file__))
CHAOS_ROOT = os.path.dirname(HERE)
WEIR_ROOT = os.path.dirname(CHAOS_ROOT)

#: Loadgen's per-thread ledger flush threshold (`chaos/src/bin/loadgen.rs`'s
#: `LEDGER_FLUSH_THRESHOLD`), duplicated here as a bare Python int because
#: Python cannot import a Rust const. `test_run.py`'s
#: TestFrontierSlackContract pins the two together, in the same spirit as
#: TestDaemonCliContract pins the CLI flags, so a change on one side without
#: the other is caught immediately instead of silently reopening I3.
LEDGER_FLUSH_THRESHOLD = 256

#: How long to wait for the load generator to flush its ledger and exit after
#: SIGTERM. loadgen's `PUSH_TIMEOUT` is 30 s, so a thread parked in a push
#: against a wedged daemon can take that long to notice the stop flag; this is
#: two of those plus slack. Overrunning it means killing the generator, which
#: loses the ledger tail — recorded as an anomaly, never silently absorbed.
LOADGEN_STOP_TIMEOUT_SECS = 60


class HarnessInterrupted(BaseException):
    """A termination signal reached run.py.

    Raised FROM the signal handler so `main()`'s `finally` runs: the WAB
    post-mortem, the teardown of the loop device, dm mapping and mount, and
    the report. With Python's default SIGTERM disposition none of that ran.
    A BaseException, like KeyboardInterrupt, so that the `except Exception`
    guards inside the episode loop and `final_pass` cannot swallow it.
    """


def install_interrupt_handlers(signal_fn=signal.signal, getsignal_fn=signal.getsignal):
    """Maps SIGTERM and SIGHUP onto HarnessInterrupted. Returns the signals it took.

    One-shot: a second signal while `finally` is cleaning up is logged and
    ignored, because raising again mid-teardown would strand exactly the
    devices the first one is releasing. A SIGHUP that is already SIG_IGN
    (run under nohup) is left ignored, so a dropped SSH session does not stop a
    run the operator deliberately detached.
    """
    fired = []

    def _raise(signum, _frame):
        name = signal.Signals(signum).name
        if fired:
            print(f"run.py: {name} again during cleanup; ignored so teardown "
                  "can finish", file=sys.stderr)
            return None
        fired.append(signum)
        raise HarnessInterrupted(f"received {name}")

    taken = []
    for sig in (signal.SIGTERM, signal.SIGHUP):
        if getsignal_fn(sig) == signal.SIG_IGN:
            continue
        signal_fn(sig, _raise)
        taken.append(sig)
    return taken


def fault_kind(sched):
    """Which fault this schedule injects. Defaults to Phase 1's `kill_random`.

    Besides Phase 1's `kill_random` and Phase 2's `power_loss` this returns the
    four Foundations lifecycle triggers (`faults.TRIGGER_KINDS`). A trigger's
    episodes still END in a cut (`faults.cut_mode`), and the oracle judges
    them under `faults.oracle_fault`, not under this name.

    An unknown kind raises rather than falling back: silently running a Phase 1
    episode under a Phase 2 schedule and reporting it as power loss is exactly
    the class of harness lie this project exists to avoid.
    """
    kind = (sched.get("faults") or {}).get("kind", "kill_random")
    known = faults.LEGACY_KINDS + faults.TRIGGER_KINDS
    if kind not in known:
        raise ValueError(
            f"unknown fault kind {kind!r}; expected one of the legacy kinds "
            f"{list(faults.LEGACY_KINDS)} or the lifecycle triggers "
            f"{list(faults.TRIGGER_KINDS)}")
    return kind


#: Free bytes the run refuses to drop below. Generous on purpose: the cost of
#: stopping an hour early is one hour, and the cost of running out is a wrong
#: answer (see `disk_stop_reason`). Override per schedule with
#: `min_free_bytes`; 0 disables the guard entirely.
DEFAULT_MIN_FREE_BYTES = 5 * 2**30


#: Free bytes the WAB DEVICE refuses to drop below. A separate, much smaller
#: floor than `DEFAULT_MIN_FREE_BYTES` because it guards a different filesystem
#: for a different reason: the test device is deliberately tiny (`size_mb`,
#: typically 2 GiB) and is the thing under test, not the thing recording the
#: test. Override per schedule with `min_free_device_bytes`; 0 disables it.
DEFAULT_MIN_FREE_DEVICE_BYTES = 128 * 2**20


#: The drain budget the harness pins on every daemon unless the schedule sets
#: one. weir's own default since Foundations is 10 s (spec §13); every schedule
#: written before that was written against a SIGTERM that drained EVERYTHING,
#: and its final pass verifies at frontier_slack=0 on that premise. 3600 is the
#: top of weir's accepted range (0..=3600), and it puts the old behaviour back.
FINAL_DRAIN_BUDGET_SECS = 3600
#: weir's `shutdown_timeout_secs` default (config/mod.rs).
WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS = 30
#: spec §7: the documented grace is shutdown_timeout + budget + 5.
SHUTDOWN_GRACE_EXTRA_SECS = 5
#: Daemon.stop's wait before SIGKILL for a full-drain stop. Unchanged from 4.0.
LEGACY_STOP_TIMEOUT_SECS = 300
STOP_SLACK_SECS = 10
#: The recorder's ingest endpoint, the daemon's HTTP sink.
SINK_URL = "http://127.0.0.1:9900/ingest"
#: Socket-appearance wait. 10 s before Foundations; Foundations adds a settle
#: wait of up to 10 s before the producer bind (spec §6 step 10), so a healthy
#: restart can legitimately take longer. A crash on startup is still caught
#: on the next 50 ms poll, not at this timeout.
DAEMON_STARTUP_TIMEOUT_SECS = 30


def stop_timeout_for(budget_secs, shutdown_timeout_secs):
    """How long Daemon.stop waits before SIGKILL. A full-drain budget keeps the
    4.0-era 300 s, so every historical schedule's stop is unchanged. A bounded
    budget gets weir's documented grace plus slack, because overrunning THAT is
    itself a finding (spec §7's bound)."""
    if budget_secs >= FINAL_DRAIN_BUDGET_SECS:
        return LEGACY_STOP_TIMEOUT_SECS
    return shutdown_timeout_secs + budget_secs + SHUTDOWN_GRACE_EXTRA_SECS + STOP_SLACK_SECS


def _is_int(value):
    # TOML booleans are Python bools, which are ints; neither is a duration.
    return isinstance(value, int) and not isinstance(value, bool)


def validate_weir_table(weir):
    """Refuses `[weir]` values weir itself would refuse at startup, so the
    error comes at schedule load and not minutes into a run."""
    budget = weir.get("drain_shutdown_budget_secs")
    if budget is not None and not (
            _is_int(budget) and 0 <= budget <= FINAL_DRAIN_BUDGET_SECS):
        raise ValueError(
            f"[weir].drain_shutdown_budget_secs={budget!r} is outside weir's "
            f"accepted range 0..={FINAL_DRAIN_BUDGET_SECS}")
    timeout = weir.get("shutdown_timeout_secs")
    if timeout is not None and not (_is_int(timeout) and timeout >= 1):
        raise ValueError(
            f"[weir].shutdown_timeout_secs={timeout!r} must be an integer >= 1 "
            "(weir refuses 0)")


def _gib(n):
    return f"{n / 2**30:.1f} GiB"


def disk_stop_reason(free, floor):
    """Why the run must stop now, or None to continue. Pure, so it is testable
    without filling a disk.

    A FULL DISK MANUFACTURES FALSE DURABILITY VIOLATIONS, which is why this
    is a hard stop rather than a warning. `delivered.log` is append-only and
    is one half of the oracle's input: if a write to it fails, delivered
    records go unrecorded while their acks are already in `ledger.log`, and
    I1 is exactly "acked implies delivered". The oracle would then report
    weir losing acknowledged records when the only thing that failed was the
    harness's own disk — a confident wrong answer, which is strictly worse
    than no answer. The same logic is why the two ledgers can never be
    trimmed to reclaim space mid-run.

    Checked BETWEEN episodes, alongside the soak deadline, so the run still
    executes its final pass, writes its report, and tears down the loop and
    dm devices and the mount. An external disk-full failure would instead
    strand all three.

    A floor of 0 disables the guard, for a venue where the harness does not
    own the filesystem it is writing to.
    """
    if floor <= 0:
        return None
    if free >= floor:
        return None
    return (
        f"free space {_gib(free)} is below the floor of {_gib(floor)}. Stopping "
        f"cleanly: if delivered.log could not be appended to, its records would "
        f"go unrecorded while their acks stayed in ledger.log, and I1 would "
        f"report weir losing acknowledged records that were only ever lost by "
        f"this harness. Set `min_free_bytes` to change the floor, or 0 to "
        f"disable this check."
    )


#: Every key this orchestrator reads, by table. A key absent from here is a
#: TYPO, not a feature: nothing consumes it, so the run silently takes the
#: default and reports green having measured something other than what the
#: schedule says. That is the same class of harness lie `fault_kind` refuses an
#: unknown fault *value* for, applied to the key names.
#:
#: The `weir` list is exactly what `Daemon.argv` passes through to the daemon —
#: NOT weir's whole config surface. The harness never writes a weir config file,
#: so weir's own unknown-key advisory never sees a schedule typo and cannot
#: catch it. When a run needs a weir knob that is not here, plumb it through
#: `Daemon.argv` and add it here in the same change; the two lists are meant to
#: be read together.
SCHEDULE_KEYS = {
    None: {
        "seed", "episodes", "max_duration_secs", "steady_lo_secs",
        "steady_hi_secs", "quiescence_timeout_secs", "min_free_bytes",
        "min_free_device_bytes",
    },
    "load": {
        "threads", "record_size", "tier", "wire_batch_size",
        "min_acked_per_episode", "min_delivered_per_episode",
    },
    "weir": {
        "shard_count", "batch_size", "batch_deadline_ms",
        "wab_segment_max_bytes", "wab_compression", "wab_compression_level",
        "drain_shutdown_budget_secs", "shutdown_timeout_secs",
    },
    "storage": {"size_mb", "dm_target"},
    # A superset: every key SOME fault kind reads. faults.validate_faults
    # narrows it to the keys the schedule's own kind reads.
    "faults": faults.ALL_FAULT_KEYS,
}


def reject_unknown_schedule_keys(sched):
    """Refuses a schedule containing a key nothing reads.

    A misspelled key is the quietest failure this harness has. `wire_batch_size`
    typed as `wire_batchsize` runs the whole schedule down the single-`push`
    path, passes, and produces a report indistinguishable from a batched run
    that worked — so the run would claim coverage of A6's batched ack path
    having never sent one batch. Same for `dm_target`, where the silent default
    is "no dm layer at all", and for anything under `[weir]`.

    Raises on the first table with unknown keys, naming them and the table's
    accepted set, because the fix is always "spell it correctly or plumb it
    through".
    """
    tables = {k for k, v in sched.items() if isinstance(v, dict)}
    unknown_tables = tables - set(SCHEDULE_KEYS)
    if unknown_tables:
        raise ValueError(
            f"schedule has unknown table(s) {sorted(unknown_tables)}; "
            f"accepted tables are {sorted(k for k in SCHEDULE_KEYS if k)}"
        )
    for table, accepted in SCHEDULE_KEYS.items():
        if table is None:
            present = {k for k, v in sched.items() if not isinstance(v, dict)}
            where = "top level"
        else:
            present = set(sched.get(table) or {})
            where = f"[{table}]"
        unknown = present - accepted
        if unknown:
            raise ValueError(
                f"schedule has unknown {where} key(s) {sorted(unknown)}: "
                f"nothing reads them, so the run would silently take the "
                f"default and report having measured something it did not. "
                f"Accepted here: {sorted(accepted)}."
            )


def device_stop_reason(free, floor):
    """Why the run must stop because the WAB DEVICE is nearly full, or None.

    `disk_stop_reason` guards the filesystem holding the two ledgers -- the
    oracle's own input. This guards the mounted test device holding the WAB, and
    it is a genuinely separate check because the two are separate filesystems
    with wildly different sizes: a schedule's `size_mb = 2048` device can fill
    while the host still has 150 GiB free, so the host floor never fires.

    FOUND THE HARD WAY, run 1813189325188119 (2026-09-27). 1,189 batched Durable
    power-loss episodes passed, then the 2 GiB device filled. Three things
    followed, in order, and only the first was weir's:

    1. weir hit ENOSPC trying to copy a mid-file-corrupt segment into quarantine
       and REFUSED to truncate it, because the corrupt tail may hold
       acked-durable records -- "segment left UNTOUCHED for retry". That is
       correct, fail-closed behaviour: it chose to stop rather than risk loss.
    2. Recovery therefore never completed, the drain never settled, and the
       episode hit its quiescence timeout. The harness ran I1 anyway and
       reported 25,577 acked-but-undelivered records as a DURABILITY VIOLATION,
       when they were sitting intact on disk precisely because weir preserved
       them.
    3. The next episode's canary write raised an unhandled
       `OSError: [Errno 28]`, killing the run without a final pass.

    So a full test device does not merely end a run: it manufactures a false P0
    against weir for doing the right thing. Stopping between episodes instead
    keeps the final pass, the report and the WAB post-mortem.
    """
    if floor <= 0:
        return None
    if free >= floor:
        return None
    return (
        f"WAB device free space {_gib(free)} is below the device floor of "
        f"{_gib(floor)}. Stopping cleanly: a full WAB device makes weir refuse "
        f"to truncate a corrupt segment it cannot quarantine -- correct, "
        f"fail-closed behaviour -- which stalls recovery, times out quiescence, "
        f"and makes I1 report the records weir deliberately PRESERVED as records "
        f"it lost. Raise the schedule's `[storage].size_mb`, or set "
        f"`min_free_device_bytes` to change this floor (0 disables it)."
    )


def load_schedule(path):
    """Reads a schedule TOML.

    A relative path resolves against the CURRENT WORKING DIRECTORY, i.e. normal
    shell semantics. It previously resolved against this file's directory, which
    meant the invocation the README documents —

        cd chaos && python3 orchestrator/run.py schedules/smoke.toml

    — looked for `orchestrator/schedules/smoke.toml` and died before doing any
    work. The unit test missed it by passing `../schedules/smoke.toml` from
    inside `orchestrator/`, which happens to resolve correctly under both rules,
    so the test pinned the bug rather than catching it.

    Also refuses a schedule that pairs `fault.kind = "power_loss"` with any
    `storage.dm_target` other than `"flakey"` — `"linear"` (dm-flakey's
    pass-through stand-in, dm_stack.py) builds a real dm layer but injects
    nothing, and an ABSENT dm_target builds no dm layer at all. Either would
    run every episode injecting nothing, losing nothing, and reporting green
    (or, for the absent case, dying minutes in at the first `engage_fault()`
    call instead of failing here before steady-state load even starts). The
    absent-target case used to be considered "caught elsewhere" and let
    through — the same justification as the linear guard applies to it
    identically, so it is refused here too now. The same guard covers a
    lifecycle trigger whose episodes end in a power cut (`cut = "power_loss"`),
    after `faults.validate_faults` has refused any [faults] key the
    schedule's own kind does not read.
    """
    with open(path, "rb") as f:
        sched = tomllib.load(f)
    reject_unknown_schedule_keys(sched)
    validate_weir_table(sched.get("weir") or {})
    faults.validate_faults(sched.get("faults") or {})
    kind = fault_kind(sched)
    if kind == "power_loss" or faults.cut_mode(sched) == "power_loss":
        dm_target = sched.get("storage", {}).get("dm_target")
        if dm_target != "flakey":
            pairs = ("fault.kind='power_loss'" if kind == "power_loss"
                     else f"fault.kind={kind!r} (cut='power_loss')")
            raise ValueError(
                f"schedule pairs {pairs} with "
                f"storage.dm_target={dm_target!r}: power_loss needs a REAL "
                f"dm-flakey layer to inject into. 'linear' is dm-flakey's "
                f"pass-through stand-in and injects nothing; an absent "
                f"dm_target builds no dm layer at all. Either way this run "
                f"would report green having tested no power loss, or die "
                f"minutes in at the first engage_fault() call. Set "
                f"storage.dm_target = 'flakey' for a real power-loss run, or "
                f"fault.kind = 'kill_random' if this schedule is only "
                f"validating plumbing."
            )
    return sched


def run_id_from_seed(seed):
    """Derives a run id from the schedule seed.

    Deterministic, so a replayed seed produces the same run id and its records
    can never be confused with another run's.
    """
    return (seed * 2654435761) % (2**63)


def kill_delays(seed, count, lo, hi):
    """Seeded steady-state durations between kills. Reproducible by design."""
    rng = random.Random(seed)
    return [rng.uniform(lo, hi) for _ in range(count)]


def progress_floor_breached(delta_acked, delta_delivered, min_acked, min_delivered):
    """True if either per-episode delta falls below its schedule floor.

    C2: I1 (acked implies delivered) and I2 (nacked implies never delivered)
    are BOTH vacuously satisfied when `acked` is empty — a weir that refuses
    or delivers nothing would otherwise pass every episode. This is the
    guard against that, pulled out as a pure function so it is testable
    without root or a live daemon. Exactly at the floor does not breach it —
    only strictly below does.
    """
    return delta_acked < min_acked or delta_delivered < min_delivered


def frontier_slack_for(threads, wire_batch_size=1):
    """I3's bound on how far a still-buffering loadgen thread may lag delivery.

    `LEDGER_FLUSH_THRESHOLD` is the most records ONE thread can hold unflushed,
    so `threads * threshold` is the worst case across all of them at once.

    The `+ wire_batch_size - 1` is not slack for its own sake. loadgen checks
    the threshold AFTER appending, so a batched thread appends
    `wire_batch_size` rows in a single step and can come to rest at
    `LEDGER_FLUSH_THRESHOLD + wire_batch_size - 1` unflushed. At
    `wire_batch_size = 1` the term is zero and the bound is byte for byte what
    every pre-batching run used. Under batching, a bound that is too small
    reports I3 violations against a load generator that was merely still
    buffering — a false P0, which is the one failure mode this harness must
    never have.

    Pure, and therefore testable without root or a live daemon, for the same
    reason `progress_floor_breached` is.
    """
    return threads * (LEDGER_FLUSH_THRESHOLD + wire_batch_size - 1)


def canary_verdict(before, during, after):
    """I6: classifies a canary read-back, turning "did the injector bite"
    from an inference into a measurement.

    `before` is what was written (and fsynced) before the fault engaged;
    `during` is the overwrite made while it was engaged, still mounted —
    under the same conditions weir's own WAB writes are under. `after` is
    what the canary file actually contains once the remount completed.

    - "bit": `after == before` — the OVERWRITE was lost. This is what a
      correct power-loss injection does.
    - "did_not_bite": `after == during` — the overwrite SURVIVED the
      umount/remount cycle. The injector did not lose anything this episode,
      independent of what weir did.
    - "unexpected": neither — a missing file, corrupted content, or anything
      else. A harness fault distinct from either of the above.

    Pulled out as a pure function so it is testable without root, a live
    daemon, or a device.
    """
    if after == before:
        return "bit"
    if after == during:
        return "did_not_bite"
    return "unexpected"


def exit_code_for(violations, anomalies, verdict):
    """C5 + I5: the exit code must never contradict the console line or the
    report. `verdict` is `report.powerloss_verdict()`'s result.

    A durability violation, a harness anomaly, or a `fail` power-loss verdict
    is the worst outcome (1) — `fail` is checked independently of
    `violations` rather than trusted to already be reflected there, since the
    two are computed by different code paths. An `inconclusive` power-loss
    verdict on an otherwise-clean run means Buffered lost nothing across
    every power-loss episode — the injector may never have bitten at all.
    That is not success, but it is a DIFFERENTLY bad outcome from a
    violation, so it gets its own code (2) instead of either exit(0)'s false
    "clean" or exit(1)'s "weir is broken".
    """
    if violations or anomalies or verdict == "fail":
        return 1
    if verdict == "inconclusive":
        return 2
    return 0


#: Off the device under test for the admin trigger, so its control-plane
#: socket is not on the filesystem being cut. Other schedules pass no path,
#: and weir binds its default `<parent(wab_dir)>/wab.admin.sock` on the
#: device. That is harmless: every cut SIGKILLs first, and `bind_hardened`
#: removes the stale file on restart. /run/weir-chaos is 0700 (main), which
#: the admin bind requires (spec §11).
ADMIN_SOCKET_PATH = "/run/weir-chaos/weir.admin.sock"


def trigger_record_fields(kind, cut, trigger_result):
    """Record fields a lifecycle trigger adds. {} for the legacy kinds, so every
    Phase 1/2 record stays byte-identical."""
    if trigger_result is None:
        return {}
    return {
        "trigger": kind,
        "cut": cut,
        "trigger_result": {k: v for k, v in trigger_result.items()
                           if k not in ("canary", "anomaly_reasons")},
        "anomaly_reasons": list(trigger_result.get("anomaly_reasons", [])),
    }


def episode_counts(result_ok, advisory, quiesced, no_progress, trigger_anomaly_reasons):
    """(violations, anomalies) one episode adds. Same split as I5: a trigger
    that did not land, or a shutdown that broke its bound, is an anomaly, never
    a durability violation."""
    violation = 1 if (not result_ok and not advisory) else 0
    anomaly = 1 if (not quiesced or no_progress or advisory or trigger_anomaly_reasons) else 0
    return violation, anomaly


def cumulative_deltas(result, prev):
    """Per-episode deltas from the accumulator's CUMULATIVE totals.

    Returns `(deltas, totals)`; the caller passes `totals` back as `prev` next
    time. Every figure `verify.Accumulator` reports only ever grows, so a
    running total can never catch a weir that stopped making progress mid-run
    — C2's whole point — and rendering one under a "Δ" heading is the
    mislabelling that inflated the totals table by ~10x before I1 fixed it.
    """
    totals = {
        "acked": result.acked_count,
        "delivered": result.delivered_distinct,
        "nacked": result.nacked_count,
        "pushed": result.pushed,
    }
    return {k: v - prev.get(k, 0) for k, v in totals.items()}, totals


#: Where the crash-path post-mortem lands, beside episodes.jsonl. A separate
#: file and NOT an episodes.jsonl row: report.py and main()'s closing count
#: treat every non-"final" row as an episode, and this is not one.
CRASH_POSTMORTEM_FILE = "crash-postmortem.json"


def crash_postmortem(wab_dir, reason, residue_fn=None):
    """The WAB post-mortem for a run that ended on an exception.

    `final_pass` step 5 is the only other place the WAB is scanned before
    `stack.teardown()` unmounts the device and deletes `wab.img`. A run that
    raised out of the episode loop never reaches it, so the evidence of what
    weir left on disk was destroyed unread, on exactly the runs where it is
    most needed. Same scanner and the same 50-row cap as the final pass, so
    the two records read alike. Never raises: a failed scan is recorded.
    """
    residue_fn = residue_fn or quiescence.scan_wab_residue
    record = {"reason": reason, "wab_dir": wab_dir}
    if wab_dir is None:
        record["wab_scan_error"] = (
            "no WAB directory was created before the run failed"
        )
        return record
    try:
        residue = residue_fn(wab_dir)
    except Exception as exc:
        record["wab_scan_error"] = f"{type(exc).__name__}: {exc}"
        return record
    record["wab_residue"] = {
        "unconfirmed_sealed": residue.unconfirmed_sealed,
        "nonempty_active": residue.nonempty_active,
        "quarantined": residue.quarantined,
        "dead_letter": residue.dead_letter,
    }
    survivors = list(residue.survivors)
    record["wab_survivor_count"] = len(survivors)
    record["wab_survivors"] = survivors[:50]
    return record


def write_crash_postmortem(run_dir, record):
    """Writes `record` to `run_dir/crash-postmortem.json`; returns the path."""
    path = os.path.join(run_dir, CRASH_POSTMORTEM_FILE)
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    return path


#: How long the drain-out restart may take to deliver what a bounded shutdown
#: left behind. The quiescence timeout of a soak schedule is 180 s for ONE
#: episode's backlog; the final drain-out may hold several abandoned segments.
DRAIN_OUT_TIMEOUT_SECS = 600


def needs_drain_out(daemon):
    """True when the daemon's shutdown drain was bounded below a full drain,
    so its SIGTERM may legitimately leave sealed, unconfirmed segments."""
    return getattr(daemon, "drain_budget_secs", FINAL_DRAIN_BUDGET_SECS) < FINAL_DRAIN_BUDGET_SECS


def drain_out(daemon, sink_url=SINK_URL, timeout_secs=DRAIN_OUT_TIMEOUT_SECS,
              quiesce_fn=None):
    """Restart, deliver everything on disk, stop. Spec §16 chaos item 1:
    "restart and wait for weir_wab_undelivered_segments == 0 before the
    frontier_slack=0 verify". The restart replays every sealed, unconfirmed
    segment the bounded shutdown left. The budget is pinned so this stop is
    itself a full drain.

    Returns `{"ran", "quiesced", "reason", "clean_stop"}`. If the restart or
    the wait raises, the daemon is stopped before the exception propagates:
    `Daemon.start` can raise with the process still alive, and the caller
    goes on to scan, then unmount, the WAB it would be running on.
    """
    quiesce_fn = quiesce_fn or quiescence.wait_for_quiescence
    try:
        daemon.start(sink_url, budget_override=FINAL_DRAIN_BUDGET_SECS)
        ok, reason = quiesce_fn(daemon.metrics_url, timeout_secs, wab_dir=daemon.wab_dir)
    except BaseException:
        try:
            daemon.stop()
        except Exception as exc:
            print(f"drain-out: stop after a failed restart failed: {exc}",
                  file=sys.stderr)
        raise
    clean = daemon.stop()
    return {"ran": True, "quiesced": ok, "reason": reason, "clean_stop": clean}


def final_pass(
    loadgen, daemon, recorder, acc, ledger_tail, delivered_tail, wab_dir,
    frontier_slack, seed, prev_totals=None, scrape_fn=None, residue_fn=None,
    sleep_fn=None, stop_loadgen_fn=None, settle_secs=1.0, tier=None, fault=None,
    drain_out_fn=None,
):
    """The verification pass that runs AFTER the last episode. D1.

    Returns `(record, violations, anomalies)`. The record is appended to
    `episodes.jsonl` as `{"episode": "final", ...}`.

    Until this existed the episode loop verified after each fault and then the
    run simply stopped, which threw away the cheapest, highest-signal evidence
    in the whole harness:

    - the final stop runs a full drain (budget pinned at 3600 s, or a bounded
      stop plus the drain-out restart of step 3b), so it
      delivers the final tens of thousands of records — and nothing read them
      before `stack.teardown()` unmounted and deleted the backing image.
    - The last episode's frontier-exempt records were never re-judged.
    - This is the ONLY moment in a run when the producer is stopped and the
      drain has been given a real chance to finish.

    ORDER IS THE POINT, and each step depends on the one before it:

    1. Stop the load generator and `wait()` for it — its exit status decides
       whether step 4 may use zero slack (D2).
    2. Scrape `/metrics` while the daemon is STILL ALIVE. After step 3 there is
       nothing left to scrape, and stranded/resumed and queue depth at the
       moment of shutdown are exactly the numbers a stranded-segment finding
       would need.
    3. `daemon.stop()`: the SIGTERM stop, bounded by the daemon's
       `drain_shutdown_budget_secs`. Having to `kill9` is an anomaly.
    3b. When `needs_drain_out(daemon)` holds (a bounded budget), restart with
       the budget at 3600, wait for quiescence, stop again (`drain_out`), so
       step 4's zero slack is not judged against segments the bounded stop
       left on disk. A drain-out that does not quiesce makes step 4 advisory.
    4. Verify with `frontier_slack=0`. Zero slack is legitimate HERE AND ONLY
       HERE: the producer is stopped and both logs are complete, so "no
       exemption" is a true statement rather than a stricter-than-reality one.
       If any of the preconditions failed, fall back to the normal slack and
       mark the check ADVISORY — see `advisory_reasons` below. `tier`/`fault`
       are forwarded to `acc.check()` unchanged, gating the SAME tier-aware I1
       exemption the episode loop uses — a Buffered record can still be
       legitimately unrecovered here if the last fault ate it for good.
    5. WAB post-mortem, BEFORE teardown, while the mount still exists.

    Everything after that (recorder, `stack.teardown()`, report) stays with the
    caller's `finally` block.

    A non-advisory failure here is a VIOLATION; everything else this can find
    is an ANOMALY, and the record carries at most one of each so run.py's tally
    and report.py's row count cannot drift apart.
    """
    scrape_fn = scrape_fn or quiescence.scrape
    residue_fn = residue_fn or quiescence.scan_wab_residue
    sleep_fn = sleep_fn or time.sleep
    stop_loadgen_fn = stop_loadgen_fn or stop_loadgen
    drain_out_fn = drain_out_fn or drain_out
    prev_totals = prev_totals or {}

    # I1 (final review): the REAL fault, not a hardcoded "none" — a Buffered
    # final pass under power_loss legitimately gets expected_loss > 0, and a
    # record claiming "none" while carrying a nonzero exemption would be
    # self-contradictory. This is also what lets report.powerloss_verdict see
    # this record at all: it filters on fault == "power_loss", and the final
    # pass is the run's most authoritative measurement (producer stopped,
    # full drain, frontier_slack=0) — it must not be invisible to the verdict
    # it should help drive.
    record = {"episode": "final", "fault": fault, "seed": seed, "tier": tier}
    #: Conditions under which "acked but not delivered" is NOT weir's fault, so
    #: `frontier_slack=0` would manufacture a violation out of a harness or
    #: teardown artefact. Any one of them downgrades the check to advisory.
    advisory_reasons = []
    anomaly_reasons = []

    # 1. Stop the producer and reap it.
    exit_code, forced = stop_loadgen_fn(loadgen)
    record["loadgen_exit_code"] = exit_code
    record["loadgen_forced_kill"] = forced
    if loadgen is None:
        anomaly_reasons.append("loadgen_never_started")
        advisory_reasons.append("no load generator ran, so the ledger is empty")
    elif forced or exit_code != 0:
        anomaly_reasons.append(f"loadgen_dirty_exit(code={exit_code}, killed={forced})")
        # Exit code 1 is loadgen's own "ledger entries lost — verification
        # cannot be trusted"; a kill means the same thing by another route.
        # Either way the ledger's tail may be truncated, which is exactly what
        # a zero-slack pass must not run on top of.
        advisory_reasons.append(
            f"the load generator exited dirty (code={exit_code}, killed={forced}), "
            "so its ledger tail may be truncated"
        )

    # 2. Last scrape, while there is still something to scrape.
    daemon_alive = bool(daemon and daemon.proc and daemon.proc.poll() is None)
    record["daemon_alive_before_stop"] = daemon_alive
    if not daemon_alive:
        # `Daemon.stop()` reports True for an already-dead process, so without
        # this the run would read "clean shutdown" for a daemon that never got
        # one — and then hold weir to I1 for a drain it never had the chance to
        # perform.
        anomaly_reasons.append("daemon_not_running_at_final_pass")
        advisory_reasons.append(
            "the daemon was already dead, so it never ran a shutdown drain"
        )
        record["final_metrics"] = None
    else:
        try:
            m = scrape_fn(daemon.metrics_url)
        except Exception as exc:
            record["final_metrics"] = None
            record["final_metrics_error"] = f"{type(exc).__name__}: {exc}"
            anomaly_reasons.append("final_metrics_scrape_failed")
        else:
            record["final_metrics"] = {
                "stranded": m.get(quiescence.STRANDED),
                "resumed": m.get(quiescence.RESUMED),
                "queue_depth": m.get("weir_queue_depth"),
                "draining": m.get(quiescence.DRAINING),
                "sink_down": m.get(quiescence.SINK_DOWN),
            }

    # 2b. The sink weir is about to drain into must still be answering. The
    # episode loop only checks this at the TOP of an episode, so a recorder
    # that died during the LAST episode's quiescence wait was never noticed —
    # and the shutdown drain would then hit a dead sink, reintroducing exactly
    # the failure the teardown reorder removed.
    recorder_alive = recorder is not None and recorder.poll() is None
    record["recorder_alive"] = recorder_alive
    if not recorder_alive:
        anomaly_reasons.append("recorder_not_running")
        advisory_reasons.append(
            "the recorder was not running, so the shutdown drain had no sink"
        )

    # 3. The real graceful drain.
    clean_stop = daemon.stop() if daemon else True
    record["daemon_clean_stop"] = clean_stop
    if daemon_alive and not clean_stop:
        anomaly_reasons.append("daemon_kill_at_stop")
        advisory_reasons.append(
            "the shutdown drain overran its budget and the daemon was killed, "
            "so undrained segments are expected"
        )

    # 3b. Drain-out. A bounded shutdown (Foundations, spec §7) leaves sealed,
    # unconfirmed segments ON PURPOSE; verifying at zero slack on top of that
    # would report records sitting intact on disk as lost — a false P0.
    # Not into a dead recorder: the restart would strand everything it
    # replays, and step 4 is already advisory for that reason.
    record["drain_out"] = None
    if daemon is not None and needs_drain_out(daemon) and recorder_alive:
        try:
            out = drain_out_fn(daemon)
        except Exception as exc:
            out = {"ran": True, "quiesced": False,
                   "reason": f"{type(exc).__name__}: {exc}", "clean_stop": False}
        record["drain_out"] = out
        if not out["quiesced"]:
            anomaly_reasons.append("drain_out_not_quiesced")
            advisory_reasons.append(
                f"the drain-out restart did not quiesce ({out['reason']}), so "
                "acked records may still be on disk, undelivered")
        elif not out["clean_stop"]:
            # Quiesced, but its own full-budget stop overran and was SIGKILLed:
            # the same finding as a killed shutdown drain at step 3.
            anomaly_reasons.append("drain_out_kill_at_stop")

    # 4. Final verification.
    advisory = bool(advisory_reasons)
    slack = frontier_slack if advisory else 0
    record["frontier_slack"] = slack
    record["advisory"] = advisory
    if advisory:
        record["advisory_reasons"] = advisory_reasons
    # The recorder fsyncs before its 200 and the daemon has now exited, so
    # every delivery it acked is already durable; this only covers the last
    # response still being written as the daemon's socket closed.
    sleep_fn(settle_secs)
    result = None
    try:
        acc.ingest(ledger_tail.read_new(), delivered_tail.read_new())
        result = acc.check(frontier_slack=slack, tier=tier, fault=fault)
    except Exception as exc:
        # Do NOT let this skip step 5. A `LogTailer` refusal (an append-only
        # log that shrank) is a real finding, but the WAB post-mortem is the
        # one piece of evidence `stack.teardown()` is about to destroy — so
        # record the failure loudly and keep going.
        record["final_check_error"] = f"{type(exc).__name__}: {exc}"
        anomaly_reasons.append("final_check_failed_to_run")

    if result is not None:
        deltas, _ = cumulative_deltas(result, prev_totals)
        record.update({
            "ok": result.ok,
            "acked": result.acked_count,
            "delivered_distinct": result.delivered_distinct,
            "acked_delta": deltas["acked"],
            "delivered_delta": deltas["delivered"],
            "duplicate_rate": round(result.duplicate_rate, 4),
            "unknown": result.unknown_count,
            "nacked": result.nacked_count,
            "pushed": result.pushed,
            "nacked_delta": deltas["nacked"],
            "pushed_delta": deltas["pushed"],
            "i1_missing": result.i1_missing[:50],
            "i2_leaked": result.i2_leaked[:50],
            "orphaned_delivered": result.orphaned_delivered[:50],
            "ledger_conflicts": result.ledger_conflicts[:50],
            "i1_exempt": result.i1_exempt,
            "pending_provenance": result.pending_provenance,
            # Tier-aware I1 (Phase 2): non-zero only for a Buffered record
            # (tier="U") checked under fault="power_loss" — see
            # verify.check_counts. Always 0 for Phase 1 and for every other
            # tier/fault combination, so this never changes a Phase 1 report.
            "expected_loss": result.expected_loss,
        })
    else:
        # No check ran, so there is no durability claim to have passed. "ok":
        # True with an anomaly recorded, the same shape the loop's abort
        # record uses.
        record["ok"] = True

    # 5. WAB post-mortem, before teardown deletes the filesystem.
    try:
        residue = residue_fn(wab_dir)
    except Exception as exc:
        record["wab_scan_error"] = f"{type(exc).__name__}: {exc}"
        anomaly_reasons.append("wab_postmortem_scan_failed")
    else:
        record["wab_residue"] = {
            "unconfirmed_sealed": residue.unconfirmed_sealed,
            "nonempty_active": residue.nonempty_active,
            "quarantined": residue.quarantined,
            "dead_letter": residue.dead_letter,
        }
        survivors = list(residue.survivors)
        record["wab_survivor_count"] = len(survivors)
        record["wab_survivors"] = survivors[:50]
        # Quarantined segments are the DOCUMENTED correct outcome of power
        # loss, not evidence against weir. The fault drops the active
        # segment's contents; recovery finds a torn tail on restart and parks
        # it in quarantine/ rather than silently truncating it — the v1.1.0
        # Finding 2c hardening, working. Flagging that would put an anomaly on
        # essentially every power-loss episode, and exit_code_for() turns any
        # anomaly into exit 1, so a 200-episode run would report "weir is
        # broken" for behaving exactly as designed. Observed on the first real
        # calibration run, 2026-08-28: 4 zero-length quarantined segments, one
        # per shard.
        #
        # Keyed on the FAULT, never on the kind alone — the same discipline
        # tier-aware I1 uses. After `kill_random` the page cache survives, so
        # a quarantined segment there still means something genuinely went
        # wrong and stays an anomaly. And only `quarantine` is exempt: a
        # non-empty active segment or a dead-letter file is not explained by
        # power loss, and exempting the whole post-mortem would discard it.
        #
        # Exempt from the ALARM, never from the RECORD: the full list and
        # count stay in the episode record either way, exactly as Buffered's
        # `expected_loss` is counted rather than hidden.
        unexplained = [
            s for s in survivors
            if not (fault == "power_loss" and s.get("kind") == "quarantine")
        ]
        if unexplained:
            anomaly_reasons.append(f"wab_survivors={len(unexplained)}")

    if result is not None and not result.ok and advisory:
        anomaly_reasons.append("advisory_check_failed")

    record["anomaly_reasons"] = anomaly_reasons
    violations = 1 if (result is not None and not result.ok and not advisory) else 0
    anomalies = 1 if anomaly_reasons else 0
    return record, violations, anomalies


def stop_loadgen(proc, timeout_secs=LOADGEN_STOP_TIMEOUT_SECS):
    """Stops the load generator and REAPS it. Returns `(exit_code, forced)`.

    `forced` is True if it had to be SIGKILLed, which means its buffered ledger
    entries were discarded after all.

    SIGCONT FIRST, and it is now load-bearing. The episode loop SIGSTOPs the
    producer around each quiescence wait, and loadgen CATCHES SIGTERM
    (`chaos/src/bin/loadgen.rs`). A *caught* signal is not delivered to a
    stopped process — it stays pending until SIGCONT — so terminating a frozen
    loadgen without resuming it first would block until the wait below timed
    out, and the kill that followed would discard exactly the ledger tail the
    handler exists to preserve.

    This step carried the same comment before the handler existed, and the
    comment was WRONG: with the default disposition SIGTERM is fatal, and the
    kernel wakes and kills a stopped process outright rather than leaving the
    signal pending (`kernel/signal.c`'s `complete_signal()` sets
    `SIGNAL_GROUP_EXIT` and `signal_wake_up()`s every thread for a `sig_fatal`
    signal). The SIGCONT was a no-op that happened to describe a future.

    Then WAIT. Without it, the daemon's own SIGTERM — `Daemon.stop()`, which
    runs next — races producer connections that are still open, so weir begins
    its shutdown drain against live traffic.
    """
    if proc is None:
        return None, False
    if proc.poll() is not None:
        return proc.returncode, False
    proc.send_signal(signal.SIGCONT)
    proc.terminate()
    try:
        return proc.wait(timeout=timeout_secs), False
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        return proc.returncode, True


class Daemon:
    """A weir-server process under test."""

    def __init__(self, binary, wab_dir, socket_path, metrics_port, cfg, log_file,
                 log_path=None, admin_socket_path=None):
        self.binary = binary
        self.wab_dir = wab_dir
        self.socket_path = socket_path
        self.metrics_port = metrics_port
        self.cfg = cfg
        # A FILE, not a pipe. An undrained `stderr=PIPE` fills its OS buffer and
        # blocks the daemon's writer, which would surface as a false quiescence
        # timeout blamed on weir. A file also keeps the logs for diagnosis.
        self.log_file = log_file
        self.proc = None
        #: The daemon log's path, for readers that scan it (the file object
        #: above is only ever written to).
        self.log_path = log_path
        #: Passed as `--admin-socket-path` when set; absent, weir derives its
        #: default from `wab_dir`.
        self.admin_socket_path = admin_socket_path
        #: The schedule's shutdown drain budget, or the full-drain pin.
        self.drain_budget_secs = cfg.get(
            "drain_shutdown_budget_secs", FINAL_DRAIN_BUDGET_SECS)
        self.shutdown_timeout_secs = cfg.get(
            "shutdown_timeout_secs", WEIR_DEFAULT_SHUTDOWN_TIMEOUT_SECS)
        #: The budget the RUNNING process was started with: the drain-out
        #: restart overrides the schedule's for one start.
        self.active_budget_secs = self.drain_budget_secs
        #: How long stop() waits before SIGKILL. See stop_timeout_for().
        self.stop_timeout_secs = stop_timeout_for(
            self.drain_budget_secs, self.shutdown_timeout_secs)

    def argv(self, sink_url, budget_override=None):
        """The daemon's command line. Pure: TestDaemonCliContract pins every
        flag it emits against weir-server's CLI parser."""
        cmd = [
            self.binary,
            "--wab-dir", self.wab_dir,
            "--socket-path", self.socket_path,
            "--metrics-port", str(self.metrics_port),
            # Bind the metrics server to loopback explicitly; the harness never
            # needs it reachable off-box.
            "--metrics-bind", "127.0.0.1",
            "--sink-type", "http",
            "--sink-url", sink_url,
            "--sink-http-batch", "ndjson",
            "--shard-count", str(self.cfg["shard_count"]),
            "--batch-size", str(self.cfg["batch_size"]),
            "--batch-deadline-ms", str(self.cfg["batch_deadline_ms"]),
            "--wab-segment-max-bytes", str(self.cfg["wab_segment_max_bytes"]),
            # Idle-seal (weir default is 0 = disabled). Without it the OPEN
            # active segment never seals while the producer is paused, and its
            # contents are acked-but-undelivered records that no quiescence
            # signal covers: `weir_wab_bytes_on_disk` counts the open segment,
            # so it simply goes flat and reads as "drained". Under kill_random
            # that is masked, because crash recovery seals the pre-kill segments
            # before the socket reappears — but every Phase 2/3 fault that does
            # NOT restart the daemon (dm-delay, ENOSPC, remount) would leave a
            # full open segment and reproduce the ~85k phantom-I1 failure the
            # producer pause was written to cure.
            "--wab-segment-max-age-secs", "2",
        ]
        # Always passed: weir's default (10 s) would bound the final pass's
        # SIGTERM drain, and that pass verifies at zero slack. See
        # FINAL_DRAIN_BUDGET_SECS.
        budget = self.drain_budget_secs if budget_override is None else budget_override
        cmd += ["--drain-shutdown-budget-secs", str(budget)]
        if "shutdown_timeout_secs" in self.cfg:
            cmd += ["--shutdown-timeout-secs", str(self.cfg["shutdown_timeout_secs"])]
        if self.admin_socket_path:
            cmd += ["--admin-socket-path", self.admin_socket_path]
        # On-disk format. OMITTED ENTIRELY unless the schedule asks for it, so
        # every schedule written before this knob existed keeps writing format
        # v1 — byte-identical to weir 1.x — exactly as it did on the runs those
        # schedules already produced. Do not give this a default here: a default
        # would silently reinterpret every historical schedule.
        #
        # "zstd" writes format v2, which weir's own config docs call a one-way
        # door: a 1.x daemon refuses to read those segments. That is precisely
        # why it needs its own soak — the 10h and 5h runs on bare metal never
        # touched the v2 write OR recovery path, because the default is "none".
        compression = self.cfg.get("wab_compression")
        if compression:
            cmd += ["--wab-compression", compression]
            level = self.cfg.get("wab_compression_level")
            if level is not None:
                cmd += ["--wab-compression-level", str(level)]
        return cmd

    def spawn(self, sink_url, budget_override=None, extra_env=None):
        """Starts weir-server and returns at once, without waiting for its
        socket: a lifecycle trigger may need to signal a process that is
        still recovering. `start()` is spawn + wait_serving. `extra_env` is
        merged over the inherited environment (the test-hook variables)."""
        # Remove any stale socket BEFORE spawning. `kill -9` leaves the file on
        # disk, so the readiness poll in wait_serving would be satisfied
        # instantly by the dead process's socket — defeating the check on every
        # restart after episode 0, and misreporting a genuine crash-on-restart
        # as a quiescence timeout rather than the finding it is.
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        self.active_budget_secs = (
            self.drain_budget_secs if budget_override is None else budget_override)
        self.stop_timeout_secs = stop_timeout_for(
            self.active_budget_secs, self.shutdown_timeout_secs)
        # Where THIS process's output starts in the shared log, so the
        # triggers judge it by its own lines and never a predecessor's.
        self.log_offset = (os.path.getsize(self.log_path)
                           if self.log_path and os.path.exists(self.log_path) else 0)
        # NO_COLOR: tracing-subscriber emits ANSI escapes unconditionally, even
        # when its output is a plain FILE rather than a terminal. Measured at
        # 28% of the daemon log here, and the escapes break naive greps when
        # reconstructing what the daemon did around a violation.
        env = dict(os.environ, NO_COLOR="1")
        if extra_env:
            env.update(extra_env)
        self.proc = subprocess.Popen(
            self.argv(sink_url, budget_override),
            stdout=self.log_file, stderr=self.log_file, env=env,
        )

    def wait_serving(self, timeout_secs=DAEMON_STARTUP_TIMEOUT_SECS):
        """Waits for the producer socket to appear rather than sleeping a fixed
        interval. Raises RuntimeError if the process exits first or the socket
        does not appear within `timeout_secs`."""
        deadline = time.monotonic() + timeout_secs
        while True:
            if os.path.exists(self.socket_path):
                return
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"weir-server exited during startup (code {self.proc.returncode}); "
                    f"see the daemon log in the run directory"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"weir-server did not create its socket within {timeout_secs:g}s")
            time.sleep(0.05)

    def start(self, sink_url, budget_override=None):
        self.spawn(sink_url, budget_override)
        self.wait_serving()

    def signal(self, sig):
        """Sends `sig` to a live process. False, and nothing sent, if there is
        no process or it has already exited (a reaped pid may be reused)."""
        if self.proc is None or self.proc.poll() is not None:
            return False
        self.proc.send_signal(sig)
        return True

    def exit_code(self):
        """The exit status, or None while running (or before any spawn)."""
        return None if self.proc is None else self.proc.poll()

    def log_since_spawn(self):
        """The daemon log from this process's spawn onward (b"" without a
        log path)."""
        if not self.log_path:
            return b""
        try:
            with open(self.log_path, "rb") as f:
                f.seek(getattr(self, "log_offset", 0))
                return f.read()
        except FileNotFoundError:
            return b""

    def bound_secs(self):
        """spec §7's documented grace for THIS process's budget:
        shutdown_timeout_secs + active budget + 5."""
        return (self.shutdown_timeout_secs + self.active_budget_secs
                + SHUTDOWN_GRACE_EXTRA_SECS)

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    def stop(self):
        """Graceful shutdown. Returns True if it exited cleanly, False if killed.

        Since Foundations weir's SIGTERM drain is bounded by
        `drain_shutdown_budget_secs`; the harness pins 3600 unless a schedule
        sets a budget, and `final_pass` runs a drain-out restart when it does.
        `shutdown_timeout_secs` covers only the socket layer's connection
        drain, and the wait here is `stop_timeout_for` of both. The old 30 s
        budget was never binding because the recorder was killed first, so every
        sink call failed instantly and shutdown took 24.6 ms. Now that the
        recorder outlives the daemon, the drain is real and needs a real budget.
        A kill here means un-drained segments, which `stack.teardown()` then
        deletes — so the caller reports it rather than swallowing it.
        """
        if not (self.proc and self.proc.poll() is None):
            return True
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=self.stop_timeout_secs)
            return True
        except subprocess.TimeoutExpired:
            self.kill9()
            return False

    @property
    def metrics_url(self):
        return f"http://127.0.0.1:{self.metrics_port}/metrics"


def main():
    if os.geteuid() != 0:
        sys.exit("run.py must run as root (it owns loopback devices and mounts)")
    # Before anything that needs undoing: from here on a SIGTERM or SIGHUP
    # unwinds through the `finally` below (post-mortem, teardown, report)
    # instead of killing the process with the loop device still attached.
    install_interrupt_handlers()

    schedule_path = sys.argv[1] if len(sys.argv) > 1 else "../schedules/smoke.toml"
    sched = load_schedule(schedule_path)
    # Fixed for the whole run: one schedule injects exactly one fault class.
    # load_schedule() already refused the incoherent linear+power_loss
    # combination, and already raises on an unknown kind — this call cannot
    # fail here.
    kind = fault_kind(sched)
    seed = sched["seed"]
    # Optional wall-clock bound for soak schedules. A soak is bounded by TIME,
    # not episode count: episode duration varies with quiescence and restart,
    # so no fixed count lands on a target length. Absent from a schedule, the
    # run is episode-bounded exactly as before.
    max_duration_secs = sched.get("max_duration_secs")
    run_id = run_id_from_seed(seed)

    run_dir = os.path.join(CHAOS_ROOT, "runs", str(run_id))
    os.makedirs(run_dir, exist_ok=True)
    # Refuse to start below the floor rather than discover it hours in. The
    # between-episode check below is the one that matters for a long soak;
    # this one just makes an already-doomed run fail in the first second.
    min_free_bytes = sched.get("min_free_bytes", DEFAULT_MIN_FREE_BYTES)
    min_free_device_bytes = sched.get(
        "min_free_device_bytes", DEFAULT_MIN_FREE_DEVICE_BYTES
    )
    preflight = disk_stop_reason(shutil.disk_usage(run_dir).free, min_free_bytes)
    if preflight is not None:
        sys.exit(f"refusing to start: {preflight}")
    # Observers write HERE — the host filesystem, outside the fault zone.
    ledger_path = os.path.join(run_dir, "ledger.log")
    delivered_path = os.path.join(run_dir, "delivered.log")
    episodes_path = os.path.join(run_dir, "episodes.jsonl")
    # Written only when a run ends on an exception before its final pass.
    crash_path = os.path.join(run_dir, CRASH_POSTMORTEM_FILE)

    # Refuse to start on top of a previous run's leftovers rather than silently
    # stacking a mount or mixing two runs' logs. Re-running the same seed reuses
    # this directory, and mixed logs would produce spurious ledger conflicts on
    # exactly the reproduction run meant to confirm a finding.
    # The crash post-mortem too: a stale one would be rendered into the next
    # run's report as though that run had crashed.
    for path in (ledger_path, delivered_path, episodes_path, crash_path):
        if os.path.exists(path) and os.path.getsize(path) > 0:
            sys.exit(
                f"{path} already exists and is non-empty. Move or delete "
                f"{run_dir} before re-running this seed."
            )

    mount_point = "/mnt/weir-wab"
    os.makedirs(mount_point, exist_ok=True)
    if os.path.ismount(mount_point):
        sys.exit(
            f"{mount_point} is already a mount point — a previous run did not tear "
            "down cleanly. Unmount it (and check `losetup -a`) before starting."
        )
    socket_dir = "/run/weir-chaos"
    os.makedirs(socket_dir, mode=0o700, exist_ok=True)

    stack = dm_stack.StorageStack(
        backing_file=os.path.join(run_dir, "wab.img"),
        size_mb=sched["storage"]["size_mb"],
        mount_point=mount_point,
        # None unless the schedule names one explicitly (see StorageStack's
        # docstring for None | "flakey" | "linear"). Absent from every Phase 1
        # schedule, so those keep building exactly the Phase 1 stack.
        dm_target=sched["storage"].get("dm_target"),
    )

    loadgen_bin = os.path.join(CHAOS_ROOT, "target", "release", "loadgen")
    recorder_bin = os.path.join(CHAOS_ROOT, "target", "release", "recorder")
    weir_bin = os.path.join(WEIR_ROOT, "target", "release", "weir-server")
    for b in (loadgen_bin, recorder_bin, weir_bin):
        if not os.path.exists(b):
            sys.exit(f"missing binary: {b} — build it first")

    recorder = None
    loadgen = None
    daemon = None
    daemon_log = None
    recorder_log = None
    loadgen_log = None
    violations = 0
    anomalies = 0
    # Crash-path state for the post-mortem in `finally`. `wab_dir` stays None
    # if the run fails before it creates the WAB directory; `final_pass_ran`
    # says whether final_pass step 5 already scanned the WAB.
    wab_dir = None
    final_pass_ran = False
    crash_reason = None

    try:
        stack.setup()
        # `mount_point` itself contains `lost+found` after `mkfs.ext4` — benign
        # as root today, but the moment a privilege drop lands (see README's
        # deferred-privilege-drop note) a non-root weir-server would EACCES on
        # startup trying to use the mount root as its WAB dir. Give it its own
        # subdirectory instead.
        wab_dir = os.path.join(mount_point, "wab")
        os.makedirs(wab_dir, exist_ok=True)
        os.chmod(wab_dir, 0o700)

        recorder_log = open(os.path.join(run_dir, "recorder.log"), "ab")
        recorder = subprocess.Popen(
            [recorder_bin, "--bind", "127.0.0.1:9900", "--log", delivered_path],
            stdout=subprocess.DEVNULL, stderr=recorder_log,
        )
        # Poll for the recorder's port rather than sleeping a fixed interval — if
        # it is not accepting when the daemon's first batch drains, weir sees a
        # failed sink and the episode's numbers are about the harness, not weir.
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", 9900), timeout=0.2):
                    break
            except OSError:
                if recorder.poll() is not None:
                    sys.exit(
                        f"recorder exited during startup (code {recorder.returncode})"
                    )
                time.sleep(0.05)
        else:
            sys.exit("recorder did not accept connections within 5s")

        daemon_log = open(os.path.join(run_dir, "weir-server.log"), "ab")
        daemon = Daemon(
            weir_bin, wab_dir, os.path.join(socket_dir, "weir.sock"),
            # NOT 9185: that is weir-server's compiled-in default, so a real
            # instance on this host would collide with the harness.
            19185, sched["weir"], daemon_log,
            log_path=os.path.join(run_dir, "weir-server.log"),
            # Only the admin trigger drives the control plane; see
            # ADMIN_SOCKET_PATH for why the others keep weir's default.
            admin_socket_path=ADMIN_SOCKET_PATH if kind == "admin_pause_resume" else None,
        )
        daemon.start(SINK_URL)

        delays = kill_delays(
            seed, sched["episodes"], sched["steady_lo_secs"], sched["steady_hi_secs"]
        )
        # How each episode ends, and the fault the ORACLE judges it under. For
        # the legacy kinds `oracle == kind`, so their records are unchanged. A
        # lifecycle trigger is judged as the cut it ends in; the trigger itself
        # is recorded separately under `trigger`. The plan draws from its own
        # salted stream, so `delays` above is the same sequence as before.
        cut = faults.cut_mode(sched)
        oracle = faults.oracle_fault(kind, cut)
        plan = (faults.fault_plan(seed, sched["episodes"], kind, sched.get("faults") or {})
                if kind in faults.TRIGGER_KINDS else None)
        run_started = time.monotonic()
        deadline = run_started + max_duration_secs if max_duration_secs else None
        # The load must outlast the episodes. Steady-state sleeps are only part
        # of the wall clock: each episode also spends time on restart,
        # quiescence polling, and verification. Budget the worst case per
        # episode rather than a flat margin — if load stops early, the final
        # episodes verify an idle daemon and PASS vacuously, which is precisely
        # the false-confidence Phase 1 exists to rule out.
        per_episode_overhead = sched["quiescence_timeout_secs"] + 30
        total_secs = int(sum(delays) + per_episode_overhead * len(delays))

        loadgen_log = open(os.path.join(run_dir, "loadgen.log"), "ab")
        loadgen = subprocess.Popen([
            loadgen_bin,
            "--socket", daemon.socket_path,
            "--ledger", ledger_path,
            "--run-id", str(run_id),
            "--threads", str(sched["load"]["threads"]),
            "--record-size", str(sched["load"]["record_size"]),
            "--tier", sched["load"]["tier"],
            "--duration-secs", str(total_secs),
            # A6's batched path. Named `wire_batch_size` and NOT `batch_size`,
            # because [weir].batch_size already means the daemon group-commit
            # size and conflating the two would silently change the wrong knob.
            # Absent or 1 keeps the single-`push` call site,
            # so every pre-existing schedule measures exactly what it measured
            # before and stays comparable with the Phase 2 corpus.
            "--batch-size", str(sched["load"].get("wire_batch_size", 1)),
        ], stdout=subprocess.DEVNULL, stderr=loadgen_log)

        # Read each log byte exactly once across the whole run.
        ledger_tail = verify.LogTailer(ledger_path)
        delivered_tail = verify.LogTailer(delivered_path)
        acc = verify.Accumulator(delivered_run_id=run_id)
        # Frontier slack (I3): bounds how far a still-buffering loadgen thread
        # can lag the delivery log. The reasoning, including why batching widens
        # it, lives on frontier_slack_for.
        frontier_slack = frontier_slack_for(
            sched["load"]["threads"], sched["load"].get("wire_batch_size", 1)
        )
        # C2: the previous episode's CUMULATIVE totals, so the per-episode
        # DELTA (not the running total) is what gets judged against the
        # schedule's progress floors — and what the report renders.
        prev_totals = {}

        with open(episodes_path, "a") as ep_log:
            for i, delay in enumerate(delays):
                # Stop BETWEEN episodes, before the sleep and before the fault,
                # so the run always ends in a verified state and still executes
                # the final pass and writes its report. An external timeout
                # would instead kill it mid-episode and strand the loop/dm
                # devices and the mount.
                if deadline is not None and time.monotonic() >= deadline:
                    elapsed_h = (time.monotonic() - run_started) / 3600.0
                    print(
                        f"soak deadline reached after {i} episodes "
                        f"({elapsed_h:.2f}h) — stopping cleanly",
                        flush=True,
                    )
                    break
                # Same break-between-episodes discipline as the deadline, and
                # for a sharper reason: running the disk dry would not just end
                # the run, it would corrupt the oracle's own input and report
                # the harness's failure as weir's. See disk_stop_reason.
                disk_reason = disk_stop_reason(
                    shutil.disk_usage(run_dir).free, min_free_bytes
                )
                if disk_reason is not None:
                    print(
                        f"disk floor reached after {i} episodes: {disk_reason}",
                        flush=True,
                    )
                    break
                # The WAB device is a DIFFERENT filesystem from run_dir, orders
                # of magnitude smaller, and the check above cannot see it.
                device_reason = device_stop_reason(
                    shutil.disk_usage(mount_point).free, min_free_device_bytes
                )
                if device_reason is not None:
                    print(
                        f"WAB device floor reached after {i} episodes: "
                        f"{device_reason}",
                        flush=True,
                    )
                    break
                time.sleep(delay)

                # A dead load generator makes every subsequent verification
                # vacuous: an idle daemon trivially satisfies I1 and I2. Assert
                # liveness BEFORE the fault so a PASS always means something.
                # Both observers must be alive. A dead LOADGEN makes every
                # later verification vacuous — an idle daemon trivially
                # satisfies I1 and I2, so the run would go green while proving
                # nothing. A dead RECORDER is worse in the other direction:
                # deliveries stop arriving and every acked record looks lost,
                # manufacturing I1 violations that are pure harness failure.
                dead = None
                if loadgen.poll() is not None:
                    dead = ("loadgen_exited", loadgen.returncode,
                            "remaining episodes would pass vacuously")
                elif recorder.poll() is not None:
                    dead = ("recorder_exited", recorder.returncode,
                            "every acked record would look undelivered")
                if dead:
                    reason, code, consequence = dead
                    print(
                        f"episode {i:3d}  ABORT — {reason} (code {code}) before the "
                        f"fault; {consequence}",
                        flush=True,
                    )
                    # An observer dying is an anomaly (the harness failed to
                    # observe), not a durability violation — no verification
                    # ran, so there is no durability claim to have failed.
                    # "ok": True reflects that (no I1/I2 failure was found);
                    # `abort_reason` is what makes this count as an anomaly.
                    anomalies += 1
                    ep_log.write(json.dumps({
                        "episode": i, "fault": oracle,
                        **({"trigger": kind} if plan else {}), "ok": True,
                        "quiesced": False, "abort_reason": reason,
                        "exit_code": code, "seed": seed,
                    }) + "\n")
                    ep_log.flush()
                    break

                trigger_result = None
                if kind == "power_loss":
                    # The Phase 2 kill-first protocol and the I6 canary: see
                    # faults.power_cut, which the lifecycle triggers share.
                    canary_result = faults.power_cut(stack, daemon, i)
                elif kind in faults.TRIGGER_KINDS:
                    # Provoke the lifecycle path, then end in the schedule's
                    # cut (faults.cut). The trigger owns the canary when it
                    # ends in a power cut, and reports what it saw as
                    # anomaly_reasons (never violations: see episode_counts).
                    ops = faults.LiveOps(daemon, recorder, stack, cut, i, SINK_URL,
                                         admin_path=daemon.admin_socket_path,
                                         delivered_path=delivered_path)
                    trigger_result = faults.run_trigger(kind, plan[i], ops)
                    canary_result = trigger_result.get("canary")
                else:
                    daemon.kill9()
                    canary_result = None
                daemon.start(SINK_URL)

                # PAUSE THE PRODUCER before waiting for the drain.
                #
                # "The drain has caught up" is not a reachable state while the
                # producer runs flat out: new segments seal as fast as old ones
                # confirm, so `sealed == confirmed + quarantined` never holds.
                # The first real run proved it — 3/3 episodes timed out at 120 s
                # with a steady ~78k acked-but-undelivered gap, and verification
                # then ran early and reported ~85k phantom I1 misses against a
                # weir that had done nothing wrong.
                #
                # SIGSTOP is enough and needs no IPC. Records buffered in a
                # thread's ledger at freeze time simply stay unflushed, so they
                # are absent from the ledger and therefore not held to I1 —
                # under-checking, which is the safe direction. Their deliveries
                # land inside the frontier slack and count as pending
                # provenance rather than orphans.
                loadgen.send_signal(signal.SIGSTOP)
                try:
                    ok, reason = quiescence.wait_for_quiescence(
                        daemon.metrics_url,
                        sched["quiescence_timeout_secs"],
                        wab_dir=daemon.wab_dir,
                    )

                    # Give the recorder a moment to finish its final append.
                    time.sleep(1.0)
                    acc.ingest(ledger_tail.read_new(), delivered_tail.read_new())
                    result = acc.check(
                        frontier_slack=frontier_slack,
                        tier=sched["load"]["tier"], fault=oracle,
                    )
                finally:
                    # Always resume, even if the wait or the check raised —
                    # leaving the producer stopped would silently turn every
                    # later episode into a no-load episode.
                    loadgen.send_signal(signal.SIGCONT)

                # The recorder's liveness was asserted at the TOP of this
                # episode, but the quiescence wait is the longest unobserved
                # stretch in the loop and it can die inside one. If it did,
                # every delivery after its death is missing from the log, so
                # this episode's I1 result is about the harness rather than
                # about weir: the verdict becomes ADVISORY (an anomaly, never
                # a violation) and the run stops, because continuing would
                # drain into a dead sink at teardown.
                recorder_alive = recorder.poll() is None
                advisory = not recorder_alive

                # C2: acked/delivered are CUMULATIVE totals, so the floor is
                # judged against the DELTA since the previous episode — not
                # the running total, which only ever grows and would never
                # catch a weir that stopped making progress mid-run.
                deltas, prev_totals = cumulative_deltas(result, prev_totals)
                no_progress = progress_floor_breached(
                    deltas["acked"], deltas["delivered"],
                    sched["load"]["min_acked_per_episode"],
                    sched["load"]["min_delivered_per_episode"],
                )

                # Violations (durability, I1/I2) and anomalies (the harness
                # failing to observe cleanly) are counted separately — see I5.
                # A quiescence timeout or a no-progress episode is not
                # evidence weir lost or leaked anything; it means this
                # episode proved nothing, which is a different kind of bad.
                # A trigger's own findings (it did not land, a shutdown broke
                # its bound) are anomalies on the same footing.
                tf = trigger_record_fields(kind, cut, trigger_result)
                v, a = episode_counts(result.ok, advisory, ok, no_progress,
                                      tf.get("anomaly_reasons", []))
                violations += v
                anomalies += a

                record = {
                    "episode": i,
                    "fault": oracle,
                    "tier": sched["load"]["tier"],
                    "steady_secs": round(delay, 2),
                    "quiesced": ok,
                    "quiescence_note": reason,
                    "ok": result.ok,
                    "recorder_alive": recorder_alive,
                    "advisory": advisory,
                    "acked": result.acked_count,
                    "delivered_distinct": result.delivered_distinct,
                    # Per-episode deltas of the cumulative counts above — what
                    # `no_progress` is actually judged against, and what the
                    # report renders under its "Δ" headings.
                    "acked_delta": deltas["acked"],
                    "delivered_delta": deltas["delivered"],
                    "no_progress": no_progress,
                    "duplicate_rate": round(result.duplicate_rate, 4),
                    "unknown": result.unknown_count,
                    "nacked": result.nacked_count,
                    "pushed": result.pushed,
                    "nacked_delta": deltas["nacked"],
                    "pushed_delta": deltas["pushed"],
                    "i1_missing": result.i1_missing[:50],
                    "i2_leaked": result.i2_leaked[:50],
                    # Provenance anomalies, kept distinct from I1/I2 so neither
                    # is ever read as a durability violation by weir. An orphan
                    # is a delivered record whose ledger line the tailer has not
                    # seen yet: loadgen buffers up to LEDGER_FLUSH_THRESHOLD per
                    # thread, and when that lag briefly exceeds the frontier
                    # window (threads * threshold) the record falls outside the
                    # exemption and is counted. It resolves as soon as the line
                    # lands. A ledger conflict means the oracle's own input is
                    # corrupt, which is a different and worse thing.
                    #
                    # TRANSIENT IS BENIGN; PERSISTENT IS NOT. Measured over the
                    # 6h Buffered soak of 2026-08-28: 9 episodes of 397 showed
                    # orphans and every one was back to zero on the very next
                    # episode. An orphan that SURVIVES the next episode's
                    # ingest is a real signal — the ledger line is not merely
                    # late, it is never coming.
                    "orphaned_delivered": result.orphaned_delivered[:50],
                    "ledger_conflicts": result.ledger_conflicts[:50],
                    # Frontier exemption (I3): how many would-be I1/orphan hits
                    # were excused because they are within frontier_slack of
                    # the ledger's high-water seq and may simply not have
                    # caught up yet. Reported, not silently absorbed, so the
                    # exemption stays visible.
                    "i1_exempt": result.i1_exempt,
                    "pending_provenance": result.pending_provenance,
                    # Tier-aware I1 (Phase 2): see the matching comment in
                    # final_pass(). C4: report.powerloss_verdict and the
                    # headline both read this from the LAST verified
                    # power-loss record, not a sum — expected_loss is a
                    # currently-still-missing count, not a per-episode delta,
                    # so summing it double(or more)-counts any record that
                    # stays lost for the rest of the run.
                    "expected_loss": result.expected_loss,
                    # I6: None for kill_random (the canary only applies to
                    # power_loss episodes — see the loop above). "bit" /
                    # "did_not_bite" / "unexpected" for power_loss.
                    "canary": canary_result,
                    "seed": seed,
                }
                # {} for the legacy kinds. The trigger's `anomaly_reasons` stay
                # separate from the recorder's `advisory_reasons` below.
                record.update(tf)
                if advisory:
                    record["advisory_reasons"] = ["recorder_exited"]
                ep_log.write(json.dumps(record) + "\n")
                ep_log.flush()

                status = result.summary()
                print(
                    f"episode {i:3d}  quiesced={ok}  no_progress={no_progress}  "
                    f"{status}",
                    flush=True,
                )
                if not result.ok or no_progress:
                    print(
                        f"  REPRODUCER: sudo python3 run.py {schedule_path}  "
                        f"# seed={hex(seed)} episode={i}",
                        flush=True,
                    )
                if not recorder_alive:
                    print(
                        f"episode {i:3d}  ABORT — the recorder died during this "
                        "episode; its verdict is advisory and the run stops here "
                        "rather than draining into a dead sink at teardown",
                        flush=True,
                    )
                    break

            # D1: the final pass. Inside the `with`, so it appends to the same
            # episode log; after the loop, so it also runs on the abort paths
            # above — a run that stopped early still has a WAB directory worth
            # a post-mortem before teardown deletes it.
            final_record, v, a = final_pass(
                loadgen, daemon, recorder, acc, ledger_tail, delivered_tail,
                daemon.wab_dir, frontier_slack, seed, prev_totals=prev_totals,
                tier=sched["load"]["tier"], fault=oracle,
            )
            final_pass_ran = True
            violations += v
            anomalies += a
            ep_log.write(json.dumps(final_record) + "\n")
            ep_log.flush()
            print(
                f"episode fin  advisory={final_record['advisory']}  "
                f"slack={final_record['frontier_slack']}  "
                f"clean_stop={final_record['daemon_clean_stop']}  "
                f"wab_survivors={final_record.get('wab_survivor_count', '?')}  "
                f"anomalies={final_record['anomaly_reasons'] or 'none'}",
                flush=True,
            )
    except BaseException as exc:
        # Recorded for the crash post-mortem in `finally`, then re-raised
        # unchanged: this is evidence capture, not error handling.
        crash_reason = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # Each step isolated. A process can exit between `poll()` and
        # `terminate()`, and an unguarded ProcessLookupError there would skip
        # stack.teardown() — leaking a loop device and a mount — while also
        # masking whatever original exception brought us here.
        # ORDER MATTERS: loadgen, then the daemon, then the recorder.
        #
        # Killing the recorder first left weir draining into a dead sink during
        # its own shutdown — the first real run ended with 32 transport errors,
        # "sink health: down", and 4 stranded segments, all of it pure teardown
        # noise that reads exactly like a genuine sink outage. Stopping the
        # producer first means no new records; stopping the daemon next lets it
        # drain and seal against a recorder that is still answering; the
        # recorder goes last.
        #
        # On the normal path `final_pass()` has ALREADY stopped the first two,
        # having read the evidence they produce; every step below is a no-op
        # then. This block is the exception path — an error escaping the try
        # body before the final pass ran — and it still has to leave nothing
        # behind.
        try:
            stop_loadgen(loadgen)
        except Exception as exc:
            print(f"cleanup: loadgen terminate failed: {exc}", file=sys.stderr)
        try:
            if daemon:
                daemon.stop()
        except Exception as exc:
            print(f"cleanup: daemon stop failed: {exc}", file=sys.stderr)
        try:
            if recorder and recorder.poll() is None:
                recorder.terminate()
                # Reap it. An unwaited child is a zombie that outlives this
                # process's own accounting, and on the exception path the next
                # thing that happens is `stack.teardown()` unmounting the
                # filesystem — better to know the recorder is gone first.
                try:
                    recorder.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    recorder.kill()
                    recorder.wait(timeout=10)
        except Exception as exc:
            print(f"cleanup: recorder terminate failed: {exc}", file=sys.stderr)
        # The crash-path WAB post-mortem: what weir left on disk, read BEFORE
        # teardown unmounts the device. Only when final_pass did not already
        # scan it (step 5), i.e. the run raised before or inside the final pass.
        if not final_pass_ran:
            try:
                pm = crash_postmortem(
                    wab_dir, crash_reason or "the run ended before its final pass")
                # Only if it exists: an exception before or inside
                # stack.setup() never created it, and report.md must not point
                # the operator at an image that is not there.
                pm["image_kept"] = (
                    stack.backing_file if os.path.exists(stack.backing_file) else None)
                pm_path = write_crash_postmortem(run_dir, pm)
                print(f"crash path: WAB post-mortem written to {pm_path}",
                      file=sys.stderr)
            except Exception as exc:
                print(f"cleanup: crash post-mortem failed: {exc}", file=sys.stderr)
        try:
            # The crash path keeps wab.img: the post-mortem above is a summary,
            # the image is the WAB itself, readable after a read-only mount.
            stack.teardown(keep_image=not final_pass_ran)
        except Exception as exc:
            print(f"cleanup: storage teardown failed: {exc}", file=sys.stderr)
        if daemon_log:
            try:
                daemon_log.close()
            except Exception:
                pass
        if recorder_log:
            try:
                recorder_log.close()
            except Exception:
                pass
        if loadgen_log:
            try:
                loadgen_log.close()
            except Exception:
                pass

        # Render the report INSIDE this finally block, not after it: an
        # exception escaping the try body above (setup failure, an unguarded
        # bug in the episode loop) would otherwise propagate straight past a
        # report.write_report() call placed after the try/finally, skipping
        # it entirely. Putting it here means it runs on every exit path,
        # exceptional or not — episodes.jsonl is the source of truth on
        # disk, so this reads back exactly what a separate
        # `python3 report.py <run_dir>` invocation would, from whatever
        # episodes got through before the exception.
        report_path = report.write_report(run_dir)
        print(f"wrote {report_path}")

    v_word = "violation" if violations == 1 else "violations"
    a_word = "anomaly" if anomalies == 1 else "anomalies"
    # COUNT WHAT RAN, not what was scheduled. `sched["episodes"]` is a ceiling
    # the loop can exit before reaching — on the soak deadline, on a dead
    # observer, or on an exception — and reporting it as though every episode
    # executed overstates the run's coverage. episodes.jsonl is the same source
    # of truth report.py renders from, so the console line and report.md agree.
    try:
        with open(episodes_path) as f:
            episode_records = [json.loads(line) for line in f if line.strip()]
        ran = sum(1 for e in episode_records if e.get("episode") != "final")
    except (OSError, ValueError):
        episode_records = []
        ran = None
    scope = f"{ran} episodes" if ran is not None else "an unknown number of episodes"
    if ran is not None and ran < sched["episodes"]:
        scope += f" (of {sched['episodes']} scheduled)"
    print(
        f"\nrun {run_id} complete: {violations} {v_word}, {anomalies} {a_word} "
        f"across {scope} plus the final pass"
    )
    # C5: the power-loss verdict — same function report.md's own section
    # renders from — gates the exit code too. Console output used to be able
    # to say "0 violations, 0 anomalies" (exit 0) on a run report.md itself
    # marked INCONCLUSIVE: the verdict lived only in report prose, invisible
    # to anything scripting off the exit code.
    verdict = report.powerloss_verdict(episode_records)
    if verdict == "inconclusive":
        print(
            f"run {run_id}: POWER-LOSS VERDICT INCONCLUSIVE — Buffered lost "
            "nothing across every power-loss episode; the injector may not "
            "have bitten at all. See report.md's Power-loss verdict section.",
            flush=True,
        )
    elif verdict == "fail":
        print(
            f"run {run_id}: POWER-LOSS VERDICT FAIL — a Durable record was "
            "lost under power loss. See report.md's Power-loss verdict "
            "section.",
            flush=True,
        )
    # I5 + C5: gate on all three. A violation is a durability failure; an
    # anomaly is the harness failing to observe cleanly (quiescence timeout,
    # dead observer, no-progress episode, a shutdown drain that had to be
    # killed, WAB files surviving teardown); an inconclusive power-loss
    # verdict is neither, but is not success either — it gets its own,
    # distinct exit code rather than either exit(0)'s false "clean" or
    # exit(1)'s "weir is broken". None of the three is acceptable in a clean
    # run.
    sys.exit(exit_code_for(violations, anomalies, verdict))


if __name__ == "__main__":
    main()
