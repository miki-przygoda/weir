#!/usr/bin/env python3
"""Chaos harness episode driver. Root, Linux only.

Owns every privileged operation: the storage stack, process lifecycle, and
fault injection. The load generator and recorder run unprivileged by design —
they are the observers and must not be able to corrupt what they measure.

Phase 1 injects one fault class (random SIGKILL). The dm-flakey, dm-delay,
ENOSPC, remount and dead-letter classes land in Phases 2-3 as additional
entries in the `[faults]` table and additional branches in `inject`.

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
import verify

HERE = os.path.dirname(os.path.abspath(__file__))
CHAOS_ROOT = os.path.dirname(HERE)
WEIR_ROOT = os.path.dirname(CHAOS_ROOT)


def load_schedule(path):
    """Reads a schedule TOML relative to the orchestrator directory."""
    full = path if os.path.isabs(path) else os.path.join(HERE, path)
    with open(full, "rb") as f:
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
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.kill9()

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
    violations = 0

    try:
        stack.setup()

        recorder = subprocess.Popen(
            [recorder_bin, "--bind", "127.0.0.1:9900", "--log", delivered_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
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
            weir_bin, mount_point, os.path.join(socket_dir, "weir.sock"),
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

        loadgen = subprocess.Popen([
            loadgen_bin,
            "--socket", daemon.socket_path,
            "--ledger", ledger_path,
            "--run-id", str(run_id),
            "--threads", str(sched["load"]["threads"]),
            "--record-size", str(sched["load"]["record_size"]),
            "--tier", sched["load"]["tier"],
            "--duration-secs", str(total_secs),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Read each log byte exactly once across the whole run.
        ledger_tail = verify.LogTailer(ledger_path)
        delivered_tail = verify.LogTailer(delivered_path)
        acc = verify.Accumulator(delivered_run_id=run_id)

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
                    violations += 1
                    ep_log.write(json.dumps({
                        "episode": i, "fault": "kill_random", "ok": False,
                        "quiesced": False, "abort_reason": reason,
                        "exit_code": code, "seed": seed,
                    }) + "\n")
                    ep_log.flush()
                    break

                daemon.kill9()
                daemon.start("http://127.0.0.1:9900/ingest")

                ok, reason = quiescence.wait_for_quiescence(
                    daemon.metrics_url, sched["quiescence_timeout_secs"]
                )

                # Give the recorder a moment to finish its final append.
                time.sleep(1.0)
                acc.ingest(ledger_tail.read_new(), delivered_tail.read_new())
                result = acc.check()

                if not result.ok or not ok:
                    violations += 1

                record = {
                    "episode": i,
                    "fault": "kill_random",
                    "steady_secs": round(delay, 2),
                    "quiesced": ok,
                    "quiescence_note": reason,
                    "ok": result.ok,
                    "acked": result.acked_count,
                    "delivered_distinct": result.delivered_distinct,
                    "duplicate_rate": round(result.duplicate_rate, 4),
                    "unknown": result.unknown_count,
                    "i1_missing": result.i1_missing[:50],
                    "i2_leaked": result.i2_leaked[:50],
                    # Provenance anomalies, kept distinct from I1/I2 so neither is
                    # ever read as a durability violation by weir: an orphan is
                    # most likely a stale log from an earlier run of this seed,
                    # and a ledger conflict means the oracle's own input is
                    # corrupt.
                    "orphaned_delivered": result.orphaned_delivered[:50],
                    "ledger_conflicts": result.ledger_conflicts[:50],
                    "seed": seed,
                }
                ep_log.write(json.dumps(record) + "\n")
                ep_log.flush()

                status = result.summary()
                print(f"episode {i:3d}  quiesced={ok}  {status}", flush=True)
                if not result.ok:
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
        for label, proc in (("loadgen", loadgen), ("recorder", recorder)):
            try:
                if proc and proc.poll() is None:
                    proc.terminate()
            except Exception as exc:
                print(f"cleanup: {label} terminate failed: {exc}", file=sys.stderr)
        try:
            if daemon:
                daemon.stop()
        except Exception as exc:
            print(f"cleanup: daemon stop failed: {exc}", file=sys.stderr)
        try:
            stack.teardown()
        except Exception as exc:
            print(f"cleanup: storage teardown failed: {exc}", file=sys.stderr)
        if daemon_log:
            try:
                daemon_log.close()
            except Exception:
                pass

    print(f"\nrun {run_id} complete: {violations} violation(s) across {sched['episodes']} episodes")
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
