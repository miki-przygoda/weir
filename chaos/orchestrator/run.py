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


#: Free bytes the run refuses to drop below. Generous on purpose: the cost of
#: stopping an hour early is one hour, and the cost of running out is a wrong
#: answer (see `disk_stop_reason`). Override per schedule with
#: `min_free_bytes`; 0 disables the guard entirely.
DEFAULT_MIN_FREE_BYTES = 5 * 2**30


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
    identically, so it is refused here too now.
    """
    with open(path, "rb") as f:
        sched = tomllib.load(f)
    if fault_kind(sched) == "power_loss":
        dm_target = sched.get("storage", {}).get("dm_target")
        if dm_target != "flakey":
            raise ValueError(
                f"schedule pairs fault.kind='power_loss' with "
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


def final_pass(
    loadgen, daemon, recorder, acc, ledger_tail, delivered_tail, wab_dir,
    frontier_slack, seed, prev_totals=None, scrape_fn=None, residue_fn=None,
    sleep_fn=None, stop_loadgen_fn=None, settle_secs=1.0, tier=None, fault=None,
):
    """The verification pass that runs AFTER the last episode. D1.

    Returns `(record, violations, anomalies)`. The record is appended to
    `episodes.jsonl` as `{"episode": "final", ...}`.

    Until this existed the episode loop verified after each fault and then the
    run simply stopped, which threw away the cheapest, highest-signal evidence
    in the whole harness:

    - weir's SIGTERM path runs a FULL drain, not a seal-and-exit, so it
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
    3. `daemon.stop()` — the real graceful drain. Having to `kill9` is an
       anomaly, recorded.
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

    def __init__(self, binary, wab_dir, socket_path, metrics_port, cfg, log_file):
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
        #: Generous: the shutdown drain is real now. See stop().
        self.stop_timeout_secs = 300

    def start(self, sink_url):
        # Remove any stale socket BEFORE spawning. `kill -9` leaves the file on
        # disk, so the readiness poll below would be satisfied instantly by the
        # dead process's socket — defeating the check on every restart after
        # episode 0, and misreporting a genuine crash-on-restart as a
        # quiescence timeout rather than the finding it is.
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
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
        # NO_COLOR: tracing-subscriber emits ANSI escapes unconditionally, even
        # when its output is a plain FILE rather than a terminal. Measured at
        # 28% of the daemon log here, and the escapes break naive greps when
        # reconstructing what the daemon did around a violation.
        self.proc = subprocess.Popen(
            cmd, stdout=self.log_file, stderr=self.log_file,
            env=dict(os.environ, NO_COLOR="1"),
        )
        # Wait for the socket to appear rather than sleeping a fixed interval.
        for _ in range(200):
            if os.path.exists(self.socket_path):
                return
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"weir-server exited during startup (code {self.proc.returncode}); "
                    f"see the daemon log in the run directory"
                )
            time.sleep(0.05)
        raise RuntimeError("weir-server did not create its socket within 10s")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    def stop(self):
        """Graceful shutdown. Returns True if it exited cleanly, False if killed.

        weir's SIGTERM path runs a FULL drain of every sealed segment, not just
        a seal-and-exit, and `shutdown_timeout_secs` does not bound it — that
        setting covers only the socket layer's connection drain. The old 30 s
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
    preflight = disk_stop_reason(shutil.disk_usage(run_dir).free, min_free_bytes)
    if preflight is not None:
        sys.exit(f"refusing to start: {preflight}")
    # Observers write HERE — the host filesystem, outside the fault zone.
    ledger_path = os.path.join(run_dir, "ledger.log")
    delivered_path = os.path.join(run_dir, "delivered.log")
    episodes_path = os.path.join(run_dir, "episodes.jsonl")

    # Refuse to start on top of a previous run's leftovers rather than silently
    # stacking a mount or mixing two runs' logs. Re-running the same seed reuses
    # this directory, and mixed logs would produce spurious ledger conflicts on
    # exactly the reproduction run meant to confirm a finding.
    for path in (ledger_path, delivered_path, episodes_path):
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
        )
        daemon.start("http://127.0.0.1:9900/ingest")

        delays = kill_delays(
            seed, sched["episodes"], sched["steady_lo_secs"], sched["steady_hi_secs"]
        )
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
        ], stdout=subprocess.DEVNULL, stderr=loadgen_log)

        # Read each log byte exactly once across the whole run.
        ledger_tail = verify.LogTailer(ledger_path)
        delivered_tail = verify.LogTailer(delivered_path)
        acc = verify.Accumulator(delivered_run_id=run_id)
        # Frontier slack (I3): bounds how far a still-buffering loadgen thread
        # can lag the delivery log. LEDGER_FLUSH_THRESHOLD is loadgen's
        # per-thread flush threshold — the most records one thread can hold
        # unflushed before it is forced to write them — so
        # `threads * LEDGER_FLUSH_THRESHOLD` is the worst case across all of
        # them at once.
        frontier_slack = sched["load"]["threads"] * LEDGER_FLUSH_THRESHOLD
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
                        "episode": i, "fault": kind, "ok": True,
                        "quiesced": False, "abort_reason": reason,
                        "exit_code": code, "seed": seed,
                    }) + "\n")
                    ep_log.flush()
                    break

                if kind == "power_loss":
                    # PROTOCOL: kill first, then drop and remount. Superseded
                    # from the original "engage, then kill" — see the spec's
                    # "Protocol: kill first, then drop and remount" section
                    # for the full kernel-layering argument; summary:
                    #
                    # drop_writes sits BELOW the page cache. A dropped write
                    # still leaves a correct, resident page behind, and
                    # kill -9 does not evict it — so the only way to actually
                    # lose a byte is drop_and_remount's umount/mount cycle,
                    # which forces the dirty page out (discarded by the lying
                    # disk while engaged) and then rebuilds the filesystem
                    # from whatever actually reached the platter.
                    #
                    # KILLING FIRST makes the lying window EXACTLY ZERO BY
                    # CONSTRUCTION — weir is already dead and cannot ack into
                    # it — rather than merely narrow. The superseded ordering
                    # left two subprocess round trips (dmsetup resume plus a
                    # table read-back) between engage and kill: 5-10ms at
                    # full rate, hundreds of acks, each one a false
                    # Durable-tier I1 violation once C2 made the injector
                    # actually bite.
                    #
                    # I6: a canary block, written before the fault and
                    # overwritten while it's engaged, converts the negative
                    # control from an INFERENCE (zero Buffered loss is merely
                    # "suspicious") into a MEASUREMENT — if the overwrite
                    # survives the umount/remount cycle, the injector did not
                    # bite THIS episode, independent of anything weir did.
                    canary_before = f"weir-chaos-canary ep={i} phase=pre-fault\n".encode()
                    canary_during = f"weir-chaos-canary ep={i} phase=engaged\n".encode()
                    stack.write_canary(canary_before)

                    daemon.kill9()
                    stack.engage_fault()
                    try:
                        # Overwritten HERE: inside the engaged window and
                        # still mounted, the same conditions weir's own WAB
                        # writes are under.
                        stack.write_canary(canary_during)
                        stack.drop_and_remount(wab_dir=daemon.wab_dir)
                    finally:
                        # Disengage even if drop_and_remount raised partway
                        # through (e.g. its umount step failed while the
                        # fault was still live). A device left engaged means
                        # the NEXT episode's steady-state load runs against a
                        # lying disk, silently corrupting the run from that
                        # point on. Unlike the superseded protocol, weir is
                        # already dead here, so there is no timing pressure
                        # left on this cleanup — a redundant disengage on the
                        # success path (drop_and_remount already disengaged)
                        # costs one harmless extra round trip, not a false ack.
                        stack.disengage_fault()
                    canary_result = canary_verdict(
                        canary_before, canary_during, stack.read_canary()
                    )
                else:
                    daemon.kill9()
                    canary_result = None
                daemon.start("http://127.0.0.1:9900/ingest")

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
                        tier=sched["load"]["tier"], fault=kind,
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
                if not result.ok and not advisory:
                    violations += 1
                if not ok or no_progress or advisory:
                    anomalies += 1

                record = {
                    "episode": i,
                    "fault": kind,
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
                    # Provenance anomalies, kept distinct from I1/I2 so neither is
                    # ever read as a durability violation by weir: an orphan is
                    # most likely a stale log from an earlier run of this seed,
                    # and a ledger conflict means the oracle's own input is
                    # corrupt.
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
                tier=sched["load"]["tier"], fault=kind,
            )
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
        try:
            stack.teardown()
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
