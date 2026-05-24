#!/usr/bin/env python3
"""
avg_benchmarks.py  <results_file>  <output_md>

Reads BENCH: {json} lines collected from multiple cargo-test runs,
averages the numeric fields across runs for each scenario, and rewrites
docs/benchmarks.md with updated tables.

Each input line must look like:
    BENCH: {"scenario":"...", "threads":N, ...}

Lines that don't match are silently skipped (test-harness output, etc.)
"""

import json
import math
import sys
import datetime
from collections import defaultdict

THROUGHPUT_SCENARIOS = [
    "single_thread_buffered",
    "single_thread_sync",
    "thundering_herd_8_threads",
    "thundering_herd_32_threads",
    "thundering_herd_64_threads",
    "connection_churn",
]

LATENCY_SCENARIO = "latency_sync"

# Ramp scenarios are detected dynamically (any scenario starting with "ramp_").
# They are sorted by thread count extracted from the name.
RAMP_PREFIX = "ramp_"


def parse_results(path: str) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("BENCH: "):
                continue
            try:
                obj = json.loads(line[len("BENCH: "):])
                groups[obj["scenario"]].append(obj)
            except (json.JSONDecodeError, KeyError):
                pass
    return dict(groups)


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = avg(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / len(values))


def fmt_rps(v: float) -> str:
    return f"{v:,.0f}"


def fmt_us(v: float) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f} s"
    if v >= 1_000:
        return f"{v/1_000:.1f} ms"
    return f"{v:.0f} µs"


def fmt_ms(v: float) -> str:
    return f"{v:.0f} ms"


def build_md(groups: dict[str, list[dict]], run_count: int) -> str:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "# Benchmark Results",
        "",
        f"Last updated: {now}  ",
        f"Averaged over: {run_count} CI run(s)  ",
        f"Server config: `shard_count=4`, `batch_size=64`, `batch_deadline_ms=2`",
        "",
        "> These numbers are the baseline for an ongoing performance improvement",
        "> effort. Changes that move throughput down or latency up by more than",
        "> ~10% should be investigated before merging.",
        "",
        "## Throughput",
        "",
        "| Scenario | Threads | Records | Avg RPS | ±StdDev | Wall time |",
        "|----------|---------|---------|---------|---------|-----------|",
    ]

    for scenario in THROUGHPUT_SCENARIOS:
        rows = groups.get(scenario, [])
        if not rows:
            lines.append(f"| {scenario} | — | — | — | — | — |")
            continue
        threads = rows[0].get("threads", 1)
        total_records = rows[0].get("total_records", 0)
        rps_vals = [float(r["throughput_rps"]) for r in rows if "throughput_rps" in r]
        ms_vals = [float(r["wall_ms"]) for r in rows if "wall_ms" in r]
        lines.append(
            f"| {scenario} | {threads} | {total_records:,} "
            f"| {fmt_rps(avg(rps_vals))} "
            f"| ±{fmt_rps(stddev(rps_vals))} "
            f"| {fmt_ms(avg(ms_vals))} |"
        )

    lines += [
        "",
        "## Latency (single thread, Sync durability)",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]

    lat_rows = groups.get(LATENCY_SCENARIO, [])
    if lat_rows:
        for field, label in [
            ("mean_us", "Mean"),
            ("p50_us", "p50"),
            ("p95_us", "p95"),
            ("p99_us", "p99"),
            ("p999_us", "p99.9"),
            ("max_us", "Max"),
        ]:
            vals = [float(r[field]) for r in lat_rows if field in r]
            if vals:
                lines.append(f"| {label} | {fmt_us(avg(vals))} |")
    else:
        lines.append("| (no data) | — |")

    # ── Saturation ramp ────────────────────────────────────────────────────
    ramp_scenarios = sorted(
        [s for s in groups if s.startswith(RAMP_PREFIX)],
        key=lambda s: int(s.split("_")[1]) if s.split("_")[1].isdigit() else 0,
    )
    if ramp_scenarios:
        lines += [
            "",
            "## Saturation Ramp",
            "",
            "> Server started with `max_connections = 48`. Levels above 48 threads",
            "> trigger connection-cap exhaustion; the server must survive every level.",
            "",
            "| Threads | Avg RPS | Acks | Nacks | I/O drops | Status |",
            "|---------|---------|------|-------|-----------|--------|",
        ]
        for scenario in ramp_scenarios:
            rows = groups[scenario]
            threads = rows[0].get("threads", "?")
            rps_vals = [float(r["throughput_rps"]) for r in rows if "throughput_rps" in r]
            ack_vals = [float(r["acks"]) for r in rows if "acks" in r]
            nack_vals = [float(r["nacks"]) for r in rows if "nacks" in r]
            io_vals = [float(r["io_errors"]) for r in rows if "io_errors" in r]
            saturated = avg(io_vals) > 0 if io_vals else False
            status = "SATURATED" if saturated else "ok"
            lines.append(
                f"| {threads} "
                f"| {fmt_rps(avg(rps_vals))} "
                f"| {fmt_rps(avg(ack_vals))} "
                f"| {fmt_rps(avg(nack_vals))} "
                f"| {fmt_rps(avg(io_vals))} "
                f"| {status} |"
            )

    lines += ["", "---", "*Generated by `deploy/avg_benchmarks.py`*", ""]
    return "\n".join(lines)


def main():
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <results_file> <output_md>", file=sys.stderr)
        sys.exit(1)

    results_path, output_path = sys.argv[1], sys.argv[2]
    groups = parse_results(results_path)

    if not groups:
        print("No BENCH: lines found — nothing to write.", file=sys.stderr)
        sys.exit(1)

    run_count = max(len(v) for v in groups.values())
    md = build_md(groups, run_count)

    with open(output_path, "w") as f:
        f.write(md)

    print(f"Wrote {output_path} ({run_count} run(s), {len(groups)} scenario(s))")


if __name__ == "__main__":
    main()
