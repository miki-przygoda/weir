#!/usr/bin/env python3
"""Chaos harness episode driver. Root, Linux only.

Owns every privileged operation: the storage stack, process lifecycle, and
fault injection. The load generator and recorder are *intended* to run
unprivileged, as the observers that must not be able to corrupt what they
measure — but this module execs both as direct children with no privilege
drop, so today they inherit root from this process, same as the daemon under
test. A real drop needs `setuid`/capability plumbing that is its own piece of
work and is deferred; see README.md's Requirements section.

Phase 1 injects one fault class (random SIGKILL, applied unconditionally in
the episode loop below). The dm-flakey, dm-delay, ENOSPC, remount and
dead-letter classes land in Phases 2-3 as additional entries in the
`[faults]` table.

Usage: sudo python3 run.py schedules/smoke.toml
"""
import json
import os
import random
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
    """
    with open(path, "rb") as f:
        return tomllib.load(f)


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
        self.proc = subprocess.Popen(cmd, stdout=self.log_file, stderr=self.log_file)
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
    seed = sched["seed"]
    run_id = run_id_from_seed(seed)

    run_dir = os.path.join(CHAOS_ROOT, "runs", str(run_id))
    os.makedirs(run_dir, exist_ok=True)
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
        # schedule's progress floors.
        prev_acked = 0
        prev_delivered = 0

        with open(episodes_path, "a") as ep_log:
            for i, delay in enumerate(delays):
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
                        "episode": i, "fault": "kill_random", "ok": True,
                        "quiesced": False, "abort_reason": reason,
                        "exit_code": code, "seed": seed,
                    }) + "\n")
                    ep_log.flush()
                    break

                daemon.kill9()
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
                    result = acc.check(frontier_slack=frontier_slack)
                finally:
                    # Always resume, even if the wait or the check raised —
                    # leaving the producer stopped would silently turn every
                    # later episode into a no-load episode.
                    loadgen.send_signal(signal.SIGCONT)

                # C2: acked/delivered are CUMULATIVE totals, so the floor is
                # judged against the DELTA since the previous episode — not
                # the running total, which only ever grows and would never
                # catch a weir that stopped making progress mid-run.
                delta_acked = result.acked_count - prev_acked
                delta_delivered = result.delivered_distinct - prev_delivered
                prev_acked, prev_delivered = result.acked_count, result.delivered_distinct
                no_progress = progress_floor_breached(
                    delta_acked, delta_delivered,
                    sched["load"]["min_acked_per_episode"],
                    sched["load"]["min_delivered_per_episode"],
                )

                # Violations (durability, I1/I2) and anomalies (the harness
                # failing to observe cleanly) are counted separately — see I5.
                # A quiescence timeout or a no-progress episode is not
                # evidence weir lost or leaked anything; it means this
                # episode proved nothing, which is a different kind of bad.
                if not result.ok:
                    violations += 1
                if not ok or no_progress:
                    anomalies += 1

                record = {
                    "episode": i,
                    "fault": "kill_random",
                    "steady_secs": round(delay, 2),
                    "quiesced": ok,
                    "quiescence_note": reason,
                    "ok": result.ok,
                    "acked": result.acked_count,
                    "delivered_distinct": result.delivered_distinct,
                    # Per-episode deltas of the cumulative counts above — what
                    # `no_progress` is actually judged against.
                    "acked_delta": delta_acked,
                    "delivered_delta": delta_delivered,
                    "no_progress": no_progress,
                    "duplicate_rate": round(result.duplicate_rate, 4),
                    "unknown": result.unknown_count,
                    "nacked": result.nacked_count,
                    "pushed": result.pushed,
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
                    "seed": seed,
                }
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
        for label, proc in (("loadgen", loadgen),):
            try:
                if proc and proc.poll() is None:
                    # SIGCONT first: a process stopped by SIGSTOP will not act
                    # on SIGTERM until it is resumed, so terminating a frozen
                    # loadgen would hang until the wait timed out.
                    proc.send_signal(signal.SIGCONT)
                    proc.terminate()
            except Exception as exc:
                print(f"cleanup: {label} terminate failed: {exc}", file=sys.stderr)
        try:
            if daemon:
                daemon.stop()
        except Exception as exc:
            print(f"cleanup: daemon stop failed: {exc}", file=sys.stderr)
        try:
            if recorder and recorder.poll() is None:
                recorder.terminate()
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
    print(
        f"\nrun {run_id} complete: {violations} {v_word}, {anomalies} {a_word} "
        f"across {sched['episodes']} episodes"
    )
    # I5: gate on both. A violation is a durability failure; an anomaly is
    # the harness failing to observe cleanly (quiescence timeout, dead
    # observer, no-progress episode) — neither is acceptable in a clean run.
    sys.exit(1 if (violations or anomalies) else 0)


if __name__ == "__main__":
    main()
