#!/usr/bin/env python3
"""Renders an episode log into a markdown report.

Phase 1 reports the essentials: what ran, what broke, and the duplicate rate.
Latency plots, resource curves and the tier x fault matrix arrive in Phases
2-4 as those measurements start existing.

A report that lists only successes is marketing. Limitations are a required
section, not an optional one.

I5: "violation" and "anomaly" are kept strictly separate here, mirroring
run.py's exit gate. A **violation** is a durability failure (I1/I2): weir
lost or leaked a record. An **anomaly** is the harness failing to observe
cleanly — a quiescence timeout, a dead observer, or a no-progress episode —
and must never be read as evidence of a weir defect by itself.
"""
import json
import os
import subprocess
import sys


def render(episodes, meta):
    """Renders episodes (list of dicts) into markdown."""
    total = len(episodes)
    violations = [e for e in episodes if not e.get("ok", True)]
    unquiesced = [e for e in episodes if not e.get("quiesced", True)]
    no_progress_eps = [e for e in episodes if e.get("no_progress")]
    # An anomaly is anything that makes an episode's verdict mean "the
    # harness didn't get a clean look", not "weir is broken": a quiescence
    # timeout, an observer dying (abort_reason), or a no-progress episode.
    # An episode counts once even if more than one applies.
    anomalies = [
        e for e in episodes
        if not e.get("quiesced", True) or e.get("abort_reason") or e.get("no_progress")
    ]

    lines = []
    lines.append("# weir chaos run — Phase 1 (spine)\n")

    lines.append("## Run metadata\n")
    for key in ("weir_commit", "kernel", "hardware", "filesystem", "seed", "duration"):
        if meta.get(key):
            lines.append(f"- **{key.replace('_', ' ').title()}:** {meta[key]}")
    lines.append("")

    v_word = "violation" if len(violations) == 1 else "violations"
    a_word = "anomaly" if len(anomalies) == 1 else "anomalies"
    lines.append("## Result\n")
    lines.append(
        f"**{total} episodes, {len(violations)} {v_word}, {len(anomalies)} "
        f"{a_word}.**\n"
    )
    lines.append(
        "A violation is a durability failure (I1/I2): weir lost or leaked a "
        "record. An anomaly is the harness failing to observe an episode "
        "cleanly — a quiescence timeout, a dead observer, or no measurable "
        "progress — and is **not**, by itself, evidence of a weir defect.\n"
    )
    if unquiesced:
        lines.append(
            f"{len(unquiesced)} episode(s) did not reach drain quiescence within "
            "the timeout. A drain that never quiesces is itself a finding.\n"
        )
    if no_progress_eps:
        lines.append(
            f"{len(no_progress_eps)} episode(s) made no progress: the acked "
            "and/or delivered delta fell below the schedule's floor "
            "(`min_acked_per_episode` / `min_delivered_per_episode`). I1/I2 "
            "are set-containment checks that are vacuously satisfied when "
            "nothing was acked, so this is what catches a weir that refuses "
            "or delivers nothing instead of letting it read as a clean pass. "
            "Check the episode's `nacked`/`pushed` figures below: a run that "
            "is pushing but never acking is a very different finding from "
            "one where load itself stalled.\n"
        )

    aborted = [e for e in episodes if e.get("abort_reason")]
    if aborted:
        lines.append(
            f"**Run aborted early at episode {aborted[0].get('episode')}: "
            f"`{aborted[0]['abort_reason']}`.** The run stopped because "
            "continuing would have produced passing episodes that verified an "
            "idle daemon. Every episode after this point is absent, not passing.\n"
        )

    # Totals as of the LAST verified episode, not a sum across episodes:
    # verify.Accumulator is cumulative, so summing would inflate every figure
    # by roughly episode-count/2 (20 episodes ending at 20,000 acked would
    # render as 210,000). Episodes without verification data (e.g. the abort
    # record written when an observer died before a check ran) are skipped
    # when picking the "last" one, so an abort never zeroes out real totals.
    verified = [e for e in episodes if "acked" in e]
    if verified:
        last = verified[-1]
        acked = last.get("acked", 0)
        distinct = last.get("delivered_distinct", 0)
        unknown = last.get("unknown", 0)
        dup_rate = last.get("duplicate_rate", 0.0)
        lines.append("## Totals\n")
        lines.append(
            "Run totals as of the last verified episode (cumulative — NOT a "
            "sum across episodes).\n"
        )
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines.append(f"| Acked records | {acked} |")
        lines.append(f"| Distinct delivered | {distinct} |")
        lines.append(f"| Unknown (indeterminate) | {unknown} |")
        lines.append(
            f"| Deliveries per distinct record (1.000 = no redelivery) | "
            f"{dup_rate:.3f} |"
        )
        lines.append("")
        lines.append(
            "This is a **multiplicity factor**, not a percentage: 1.000 means "
            "no redelivery, 2.000 means every record arrived twice on "
            "average. At-least-once delivery makes duplicates conformant — "
            "this is what a crash actually costs a sink that has to dedupe, "
            "which weir's own docs require but never quantify.\n"
        )

    orphaned = len(last.get("orphaned_delivered", [])) if verified else 0
    conflicts = len(last.get("ledger_conflicts", [])) if verified else 0
    if orphaned or conflicts:
        lines.append("## Provenance anomalies\n")
        if orphaned:
            lines.append(
                f"{orphaned} delivered record(s) had no ledger entry. These are NOT "
                "durability violations and are excluded from the duplicate rate. The "
                "likeliest cause is a stale delivery log from an earlier run of this "
                "seed, since the run id derives from it.\n"
            )
        if conflicts:
            lines.append(
                f"{conflicts} sequence number(s) appeared under two different ledger "
                "tags. That is corruption of the oracle's own input, so the affected "
                "episodes fail — but it is a harness finding, not a weir one.\n"
            )

    lines.append("## Episodes\n")
    lines.append(
        "| # | Fault | Quiesced | Verdict | Acked Δ | Delivered Δ | Dup rate | "
        "Unknown | Notes |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for e in episodes:
        notes = []
        if e.get("no_progress"):
            notes.append("no-progress")
        if not e.get("quiesced", True):
            notes.append("quiescence timeout")
        if e.get("abort_reason"):
            notes.append(f"aborted ({e['abort_reason']})")
        # Three verdicts, not two. An episode with no durability violation but
        # an anomaly is NOT a clean pass — reading "PASS" beside a no-progress
        # or unquiesced row is exactly the false reassurance this report exists
        # to avoid. FAIL means weir lost or leaked a record; ANOMALY means the
        # harness did not observe the episode cleanly.
        if not e.get("ok"):
            verdict = "**FAIL**"
        elif notes:
            verdict = "ANOMALY"
        else:
            verdict = "PASS"
        # No cumulative fallback for the deltas: showing a running total under
        # a "Δ" heading is the same silent mislabelling that inflated the
        # totals table by ~10x. If a delta is genuinely absent, say so.
        acked_delta = e.get("acked_delta")
        delivered_delta = e.get("delivered_delta")
        lines.append(
            f"| {e.get('episode')} | {e.get('fault', '?')} | "
            f"{'yes' if e.get('quiesced') else 'NO'} | "
            f"{verdict} | "
            f"{'—' if acked_delta is None else acked_delta} | "
            f"{'—' if delivered_delta is None else delivered_delta} | "
            f"{e.get('duplicate_rate', 0.0):.3f} | {e.get('unknown', 0)} | "
            f"{', '.join(notes) or '—'} |"
        )
    lines.append("")

    if violations:
        lines.append("## Violations\n")
        for e in violations:
            lines.append(f"### episode {e.get('episode')} — {e.get('fault', '?')}\n")
            if e.get("i1_missing"):
                lines.append(
                    f"**I1 — acked but never delivered** ({len(e['i1_missing'])} shown, "
                    f"truncated at 50): `{e['i1_missing']}`\n"
                )
            if e.get("i2_leaked"):
                lines.append(
                    f"**I2 — nacked but delivered** ({len(e['i2_leaked'])}): "
                    f"`{e['i2_leaked']}`\n"
                )
            lines.append(f"Reproducer: seed `{hex(e.get('seed', 0))}`, episode {e.get('episode')}\n")

    lines.append("## Limitations\n")
    lines.append(
        "- Phase 1 injects **random SIGKILL only**. Targeted mid-fsync kills, power "
        "loss, torn writes, disk-full, slow disk, read-only remount and dead-letter "
        "exhaustion are Phases 2-3 and are **not** covered by this run.\n"
        "- Invariant I1 is **not yet tier-aware**: all tiers are held to zero loss, "
        "which is correct for process-crash but will need relaxing for Buffered "
        "under power loss in Phase 2.\n"
        "- The seed reproduces the **schedule**, not the exact interleaving. Real "
        "kernel, real timing, real I/O — full determinism is not claimed.\n"
        "- Single host, single filesystem, single hardware configuration.\n"
    )
    return "\n".join(lines)


def load_episodes(run_dir):
    """Reads run_dir/episodes.jsonl into a list of dicts, `[]` if absent."""
    episodes = []
    ep_path = os.path.join(run_dir, "episodes.jsonl")
    if os.path.exists(ep_path):
        with open(ep_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    episodes.append(json.loads(line))
    return episodes


def gather_meta(episodes):
    """Best-effort run metadata: the weir commit, kernel, and the run's seed."""
    meta = {}
    try:
        meta["weir_commit"] = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        meta["kernel"] = subprocess.run(
            ["uname", "-r"], capture_output=True, text=True
        ).stdout.strip()
    except Exception:
        pass
    if episodes:
        meta["seed"] = hex(episodes[0].get("seed", 0))
    return meta


def write_report(run_dir):
    """Renders run_dir/episodes.jsonl into run_dir/report.md. Returns its path.

    Shared by this module's CLI entry point and run.py, which calls this at
    the end of every run so rendering the report is no longer a manual step
    in the exit gate.
    """
    episodes = load_episodes(run_dir)
    meta = gather_meta(episodes)
    out_path = os.path.join(run_dir, "report.md")
    with open(out_path, "w") as f:
        f.write(render(episodes, meta))
    return out_path


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: report.py <run_dir>")
    out_path = write_report(sys.argv[1])
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
