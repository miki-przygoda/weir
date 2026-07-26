#!/usr/bin/env python3
"""Renders an episode log into a markdown report.

Phase 1 reports the essentials: what ran, what broke, and the duplicate rate.
Latency plots, resource curves and the tier x fault matrix arrive in Phases
2-4 as those measurements start existing.

A report that lists only successes is marketing. Limitations are a required
section, not an optional one.
"""
import json
import os
import sys


def render(episodes, meta):
    """Renders episodes (list of dicts) into markdown."""
    total = len(episodes)
    violations = [e for e in episodes if not e.get("ok", True)]
    unquiesced = [e for e in episodes if not e.get("quiesced", True)]

    lines = []
    lines.append("# weir chaos run — Phase 1 (spine)\n")

    lines.append("## Run metadata\n")
    for key in ("weir_commit", "kernel", "hardware", "filesystem", "seed", "duration"):
        if meta.get(key):
            lines.append(f"- **{key.replace('_', ' ').title()}:** {meta[key]}")
    lines.append("")

    verdict = f"{len(violations)} violation" + ("" if len(violations) == 1 else "s")
    lines.append("## Result\n")
    lines.append(f"**{total} episodes, {verdict}.**\n")
    if unquiesced:
        lines.append(
            f"{len(unquiesced)} episode(s) did not reach drain quiescence within "
            "the timeout. A drain that never quiesces is itself a finding.\n"
        )

    aborted = [e for e in episodes if e.get("abort_reason")]
    if aborted:
        lines.append(
            f"**Run aborted early at episode {aborted[0].get('episode')}: "
            f"`{aborted[0]['abort_reason']}`.** The run stopped because "
            "continuing would have produced passing episodes that verified an "
            "idle daemon. Every episode after this point is absent, not passing.\n"
        )

    if episodes:
        acked = sum(e.get("acked", 0) for e in episodes)
        distinct = sum(e.get("delivered_distinct", 0) for e in episodes)
        unknown = sum(e.get("unknown", 0) for e in episodes)
        rates = [e.get("duplicate_rate", 0.0) for e in episodes if e.get("duplicate_rate")]
        avg_dup = sum(rates) / len(rates) if rates else 0.0
        lines.append("## Totals\n")
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines.append(f"| Acked records | {acked} |")
        lines.append(f"| Distinct delivered | {distinct} |")
        lines.append(f"| Unknown (indeterminate) | {unknown} |")
        lines.append(f"| Duplicate rate (mean) | {avg_dup:.3f} |")
        lines.append("")
        lines.append(
            "Duplicate rate is delivered-over-distinct. At-least-once delivery makes "
            "duplicates conformant; this is what a crash actually costs a sink that "
            "must dedupe.\n"
        )

    orphaned = sum(len(e.get("orphaned_delivered", [])) for e in episodes)
    conflicts = sum(len(e.get("ledger_conflicts", [])) for e in episodes)
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
    lines.append("| # | Fault | Quiesced | Verdict | Acked | Distinct | Dup rate | Unknown |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for e in episodes:
        lines.append(
            f"| {e.get('episode')} | {e.get('fault', '?')} | "
            f"{'yes' if e.get('quiesced') else 'NO'} | "
            f"{'PASS' if e.get('ok') else '**FAIL**'} | {e.get('acked', 0)} | "
            f"{e.get('delivered_distinct', 0)} | {e.get('duplicate_rate', 0.0):.3f} | "
            f"{e.get('unknown', 0)} |"
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


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: report.py <run_dir>")
    run_dir = sys.argv[1]
    episodes = []
    ep_path = os.path.join(run_dir, "episodes.jsonl")
    if os.path.exists(ep_path):
        with open(ep_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    episodes.append(json.loads(line))

    meta = {}
    try:
        import subprocess
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

    out_path = os.path.join(run_dir, "report.md")
    with open(out_path, "w") as f:
        f.write(render(episodes, meta))
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
