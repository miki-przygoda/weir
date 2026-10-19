#!/usr/bin/env python3
"""The Foundations pre-release chaos gate (spec §16).

PASS needs ALL of:
- at least 1,000 power cuts across the four lifecycle schedules, and at least
  200 in each;
- 0 oracle violations (a non-advisory I1/I2/ledger-conflict failure, in any
  episode or final pass);
- the I6 canary "bit" on every cut;
- 0 anomalies, unless --allow-anomalies (each allowed one must then be
  explained in the write-up);
- a final pass in every run (its zero-slack verify and WAB post-mortem), and
  no crash-postmortem.json in any run dir.
The duplicate rate is REPORTED against the phase-2 baseline, not gated.

Usage: python3 orchestrator/gate.py runs/<id> [runs/<id> ...] [--allow-anomalies]
"""
import argparse
import os
import sys

import report

#: The lifecycle trigger each gate schedule runs, and that schedule's name.
#: Only records whose `trigger` is one of these count as gate cuts, so a
#: legacy Phase 2 run passed in by mistake adds nothing to the totals.
GATE_TRIGGERS = {
    "sigterm_at": "shutdown-bounded",
    "double_sigterm": "shutdown-forced",
    "sigterm_in_recovery": "startup-signal",
    "admin_pause_resume": "pause-resume-cut",
}
MIN_CUTS_TOTAL = 1000
MIN_CUTS_PER_SCHEDULE = 200


def _is_cut(e):
    """A gate cut: a real episode (int `episode`, so never the final pass) of
    a gate trigger that ended in a dm-flakey power cut and was not aborted
    before its trigger ran. A `kill` cut is a SIGKILL, not power loss, and
    the gate's floor is measured in power cuts."""
    return (isinstance(e.get("episode"), int) and e.get("trigger") in GATE_TRIGGERS
            and e.get("cut") == "power_loss" and not e.get("abort_reason"))


def _is_anomaly(e):
    """The same split as run.episode_counts: an anomaly is the harness not
    observing cleanly (or a trigger not landing), never a durability failure."""
    return bool(e.get("anomaly_reasons") or e.get("abort_reason") or e.get("advisory")
                or not e.get("quiesced", True) or e.get("no_progress"))


def campaign_gate(runs, allow_anomalies=False):
    """Judges a campaign. `runs` maps a run dir to its episodes.jsonl records.
    Pure. Returns the verdict dict; `failures` names every reason it failed."""
    cuts = {k: 0 for k in GATE_TRIGGERS}
    canary_misses, violations, anomalies, dup = [], [], [], {}
    no_final = []
    for run_dir, records in sorted(runs.items()):
        # The final pass is the run's only zero-slack verify and its in-run WAB
        # post-mortem. A run that crashed, was SIGTERMed or hit run-soak.sh's
        # backstop has no `{"episode": "final"}` record and never had either.
        if not any(e.get("episode") == "final" for e in records):
            no_final.append(run_dir)
        for e in records:
            label = f"{run_dir}#{e.get('episode')}"
            if _is_cut(e):
                cuts[e["trigger"]] += 1
                if e.get("canary") != "bit":
                    canary_misses.append(label)
            if not e.get("ok", True) and not e.get("advisory"):
                violations.append(label)
            if _is_anomaly(e):
                anomalies.append(label)
        # The accumulator's rate is cumulative, so the last record carrying
        # one is the run's rate. Attributed only when the run is one trigger.
        kinds = {e.get("trigger") for e in records if e.get("trigger") in GATE_TRIGGERS}
        rated = [e for e in records if "duplicate_rate" in e]
        if len(kinds) == 1 and rated:
            dup[kinds.pop()] = rated[-1]["duplicate_rate"]
    total = sum(cuts.values())
    failures = []
    if total < MIN_CUTS_TOTAL:
        failures.append(f"{total} cuts in total < {MIN_CUTS_TOTAL}")
    for kind, n in cuts.items():
        if n < MIN_CUTS_PER_SCHEDULE:
            failures.append(f"{GATE_TRIGGERS[kind]}: {n} cuts < {MIN_CUTS_PER_SCHEDULE}")
    if violations:
        failures.append(f"{len(violations)} oracle violation(s): {violations[:10]}")
    if canary_misses:
        failures.append(f"{len(canary_misses)} cut(s) where the canary did not bite: "
                        f"{canary_misses[:10]}")
    if anomalies and not allow_anomalies:
        failures.append(f"{len(anomalies)} anomal(y/ies): {anomalies[:10]} "
                        "(explain each in the write-up, then --allow-anomalies)")
    if no_final:
        failures.append(f"{len(no_final)} run(s) without a final pass "
                        f"(crashed or interrupted): {no_final}")
    return {"ok": not failures, "cuts": cuts, "total_cuts": total,
            "violations": violations, "canary_misses": canary_misses,
            "anomalies": anomalies, "duplicate_rate": dup,
            "baseline_duplicate_rate": report.PHASE2_DUPLICATE_RATE,
            "runs_without_final_pass": no_final,
            "failures": failures}


def render_gate(result):
    """The console verdict: per-schedule cuts and duplicate rate against the
    phase-2 baseline, the three counts, then one FAIL line per failure."""
    lines = ["Foundations chaos gate: " + ("PASS" if result["ok"] else "FAIL"), ""]
    lines.append(f"{'schedule':<20} {'cuts':>6} {'dup rate':>9} {'vs 1.000':>9}")
    for kind, sched in GATE_TRIGGERS.items():
        rate = result["duplicate_rate"].get(kind)
        rate_s = "—" if rate is None else f"{rate:.4f}"
        delta = "—" if rate is None else f"{rate - result['baseline_duplicate_rate']:+.4f}"
        lines.append(f"{sched:<20} {result['cuts'][kind]:>6} {rate_s:>9} {delta:>9}")
    lines.append(f"{'total':<20} {result['total_cuts']:>6}")
    lines.append("")
    lines.append(f"violations: {len(result['violations'])}  canary misses: "
                 f"{len(result['canary_misses'])}  anomalies: {len(result['anomalies'])}")
    for f in result["failures"]:
        lines.append(f"FAIL: {f}")
    return "\n".join(lines)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("run_dirs", nargs="+")
    p.add_argument("--allow-anomalies", action="store_true")
    args = p.parse_args(argv)
    # run.py writes crash-postmortem.json only when the final pass did not
    # run; its presence is a crashed run whatever episodes.jsonl says.
    crashed = [d for d in args.run_dirs
               if os.path.exists(os.path.join(d, report.CRASH_POSTMORTEM_FILE))]
    if crashed:
        print(f"FAIL: crash post-mortem present in {crashed}")
        return 1
    runs = {d: report.load_episodes(d) for d in args.run_dirs}
    result = campaign_gate(runs, allow_anomalies=args.allow_anomalies)
    print(render_gate(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
