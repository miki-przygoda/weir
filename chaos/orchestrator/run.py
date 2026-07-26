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
import shutil
import signal
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

    def __init__(self, binary, wab_dir, socket_path, metrics_port, cfg):
        self.binary = binary
        self.wab_dir = wab_dir
        self.socket_path = socket_path
        self.metrics_port = metrics_port
        self.cfg = cfg
        self.proc = None

    def start(self, sink_url):
        cmd = [
            self.binary,
            "--wab-dir", self.wab_dir,
            "--socket-path", self.socket_path,
            "--metrics-port", str(self.metrics_port),
            "--sink-type", "http",
            "--sink-url", sink_url,
            "--sink-http-batch", "ndjson",
            "--shard-count", str(self.cfg["shard_count"]),
            "--batch-size", str(self.cfg["batch_size"]),
            "--batch-deadline-ms", str(self.cfg["batch_deadline_ms"]),
            "--wab-segment-max-bytes", str(self.cfg["wab_segment_max_bytes"]),
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        # Wait for the socket to appear rather than sleeping a fixed interval.
        for _ in range(200):
            if os.path.exists(self.socket_path):
                return
            if self.proc.poll() is not None:
                err = self.proc.stderr.read().decode(errors="replace")
                raise RuntimeError(f"weir-server exited during startup: {err}")
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

    mount_point = "/mnt/weir-wab"
    os.makedirs(mount_point, exist_ok=True)
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
    violations = 0

    try:
        stack.setup()

        recorder = subprocess.Popen(
            [recorder_bin, "--bind", "127.0.0.1:9900", "--log", delivered_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.5)

        daemon = Daemon(
            weir_bin, mount_point, os.path.join(socket_dir, "weir.sock"),
            9185, sched["weir"],
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
                if loadgen.poll() is not None:
                    print(
                        f"episode {i:3d}  ABORT — load generator exited "
                        f"(code {loadgen.returncode}) before the fault; "
                        "remaining episodes would pass vacuously",
                        flush=True,
                    )
                    violations += 1
                    ep_log.write(json.dumps({
                        "episode": i, "fault": "kill_random", "ok": False,
                        "quiesced": False, "abort_reason": "loadgen_exited",
                        "loadgen_exit_code": loadgen.returncode, "seed": seed,
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
        for p in (loadgen, recorder):
            if p and p.poll() is None:
                p.terminate()
        if daemon:
            daemon.stop()
        stack.teardown()

    print(f"\nrun {run_id} complete: {violations} violation(s) across {sched['episodes']} episodes")
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
