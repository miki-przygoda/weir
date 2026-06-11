#!/usr/bin/env python3
"""
avg_benchmarks.py  <results_file>  <output_md>

Reads BENCH: {json} lines collected from multiple cargo-test runs,
averages the numeric fields across runs for each scenario, and rewrites
docs/benchmarks.md with updated tables.

Each input line must look like:
    BENCH: {"scenario":"...", "threads":N, ...}

Lines that don't match are silently skipped (test-harness output, etc.)

Scenario names include a deadline suffix (_d1ms, _d2ms, …) so the
comparison table can be rendered automatically.
"""

import json
import math
import sys
import datetime
from collections import defaultdict

# Base scenario names (without deadline suffix).
BASE_THROUGHPUT_SCENARIOS = [
    "single_thread_buffered",
    "single_thread_sync",
    "thundering_herd_8_threads",
    "thundering_herd_32_threads",
    "thundering_herd_64_threads",
    "connection_churn",
    "fire_and_forget_overload",
]

BASE_LATENCY_SCENARIOS = [
    ("latency_sync", "Sync"),
    ("latency_batched", "Batched"),
    ("latency_buffered", "Buffered"),
]
BASE_LATENCY_SCENARIO = "latency_sync"  # kept for single-deadline fallback

# Deadlines rendered in the comparison (left → right = fastest → slowest).
DEADLINES = ["1ms", "2ms"]

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


def rps_avg(rows: list[dict]) -> float:
    vals = [float(r["throughput_rps"]) for r in rows if "throughput_rps" in r]
    return avg(vals)


def rps_std(rows: list[dict]) -> float:
    vals = [float(r["throughput_rps"]) for r in rows if "throughput_rps" in r]
    return stddev(vals)


def build_md(groups: dict[str, list[dict]], run_count: int) -> str:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Detect which deadlines are actually present in results.
    present_deadlines = []
    for d in DEADLINES:
        key = f"{BASE_THROUGHPUT_SCENARIOS[0]}_d{d}"
        if key in groups:
            present_deadlines.append(d)

    lines = [
        "# Benchmark Results",
        "",
        f"Last updated: {now}  ",
        f"Averaged over: {run_count} CI run(s) per deadline  ",
        f"Server config: `shard_count=4`, `batch_size=64`",
        "",
        "> These numbers are the baseline for an ongoing performance improvement",
        "> effort. Changes that move throughput down or latency up by more than",
        "> ~10% should be investigated before merging.",
        "",
    ]

    # ── Throughput comparison ──────────────────────────────────────────────
    if len(present_deadlines) > 1:
        lines += [
            "## Throughput — deadline comparison",
            "",
        ]
        header_cols = " | ".join(
            f"RPS ({d}) | ±σ ({d})" for d in present_deadlines
        )
        sep_cols = " | ".join(
            "---------|-------" for _ in present_deadlines
        )
        speedup_header = " | d1ms/d2ms" if len(present_deadlines) == 2 else ""
        speedup_sep = " | ---------" if len(present_deadlines) == 2 else ""
        lines.append(f"| Scenario | {header_cols}{speedup_header} |")
        lines.append(f"|----------|{sep_cols}{speedup_sep}|")

        for base in BASE_THROUGHPUT_SCENARIOS:
            row_vals = []
            rps_per_deadline = []
            for d in present_deadlines:
                rows = groups.get(f"{base}_d{d}", [])
                if rows:
                    r = rps_avg(rows)
                    s = rps_std(rows)
                    row_vals.append(f"{fmt_rps(r)} | ±{fmt_rps(s)}")
                    rps_per_deadline.append(r)
                else:
                    row_vals.append("— | —")
                    rps_per_deadline.append(None)

            speedup_col = ""
            if len(present_deadlines) == 2 and all(v for v in rps_per_deadline):
                ratio = rps_per_deadline[0] / rps_per_deadline[1]
                speedup_col = f" | **{ratio:.2f}×**"

            lines.append(
                f"| {base} | {' | '.join(row_vals)}{speedup_col} |"
            )
    else:
        # Only one deadline present — fall back to a simple table.
        d = present_deadlines[0] if present_deadlines else DEADLINES[0]
        lines += [
            f"## Throughput (`batch_deadline_ms={d[:-2]}`)",
            "",
            "| Scenario | Threads | Records | Avg RPS | ±StdDev | Wall time |",
            "|----------|---------|---------|---------|---------|-----------|",
        ]
        for base in BASE_THROUGHPUT_SCENARIOS:
            rows = groups.get(f"{base}_d{d}", groups.get(base, []))
            if not rows:
                lines.append(f"| {base} | — | — | — | — | — |")
                continue
            threads = rows[0].get("threads", 1)
            total_records = rows[0].get("total_records", 0)
            rps_vals = [float(r["throughput_rps"]) for r in rows if "throughput_rps" in r]
            ms_vals = [float(r["wall_ms"]) for r in rows if "wall_ms" in r]
            lines.append(
                f"| {base} | {threads} | {total_records:,} "
                f"| {fmt_rps(avg(rps_vals))} "
                f"| ±{fmt_rps(stddev(rps_vals))} "
                f"| {fmt_ms(avg(ms_vals))} |"
            )

    # ── Latency comparison ─────────────────────────────────────────────────
    LATENCY_FIELDS = [
        ("min_us",      "Min"),
        ("mean_us",     "Mean"),
        ("stddev_us",   "σ"),
        ("p50_us",      "p50"),
        ("p75_us",      "p75"),
        ("p95_us",      "p95"),
        ("p99_us",      "p99"),
        ("p999_us",     "p99.9"),
        ("max_us",      "Max"),
    ]

    lines += [""]
    if len(present_deadlines) > 1:
        # Three-tier × two-deadline: one sub-table per deadline.
        for d in present_deadlines:
            lines += [
                f"## Latency — single thread, `batch_deadline_ms={d[:-2]}`",
                "",
            ]
            tier_header = " | ".join(label for _, label in BASE_LATENCY_SCENARIOS)
            tier_sep = " | ".join("-------" for _ in BASE_LATENCY_SCENARIOS)
            lines.append(f"| Metric | {tier_header} |")
            lines.append(f"|--------|{tier_sep}|")

            for field, label in LATENCY_FIELDS:
                row_vals = []
                for base, _ in BASE_LATENCY_SCENARIOS:
                    rows = groups.get(f"{base}_d{d}", [])
                    vals = [float(r[field]) for r in rows if field in r]
                    row_vals.append(fmt_us(avg(vals)) if vals else "—")
                lines.append(f"| {label} | {' | '.join(row_vals)} |")
            lines.append("")
    else:
        d = present_deadlines[0] if present_deadlines else DEADLINES[0]
        lines += [
            f"## Latency — single thread, `batch_deadline_ms={d[:-2]}`",
            "",
        ]
        tier_header = " | ".join(label for _, label in BASE_LATENCY_SCENARIOS)
        tier_sep = " | ".join("-------" for _ in BASE_LATENCY_SCENARIOS)
        lines.append(f"| Metric | {tier_header} |")
        lines.append(f"|--------|{tier_sep}|")

        for field, label in LATENCY_FIELDS:
            row_vals = []
            for base, _ in BASE_LATENCY_SCENARIOS:
                rows = groups.get(f"{base}_d{d}", groups.get(base, []))
                vals = [float(r[field]) for r in rows if field in r]
                row_vals.append(fmt_us(avg(vals)) if vals else "—")
            lines.append(f"| {label} | {' | '.join(row_vals)} |")

    # ── Saturation ramp ────────────────────────────────────────────────────
    ramp_scenarios = sorted(
        [s for s in groups if s.startswith(RAMP_PREFIX)],
        key=lambda s: (
            # Sort by thread count (second token), then deadline.
            int(s.split("_")[1]) if s.split("_")[1].isdigit() else 0,
            s,
        ),
    )
    if ramp_scenarios:
        lines += [
            "",
            "## Saturation Ramp",
            "",
            "> Server started with `max_connections = 48`. Levels above 48 threads",
            "> trigger connection-cap exhaustion; the server must survive every level.",
            "",
        ]

        # Group ramp scenarios by thread count.
        by_threads: dict[int, dict[str, list[dict]]] = defaultdict(dict)
        for s in ramp_scenarios:
            parts = s.split("_")
            n = int(parts[1]) if parts[1].isdigit() else 0
            # Detect deadline suffix (e.g. _d1ms).
            d_suffix = next((p for p in parts if p.startswith("d") and p.endswith("ms")), None)
            d_label = d_suffix if d_suffix else "?"
            by_threads[n][d_label] = groups[s]

        sorted_thread_counts = sorted(by_threads.keys())
        ramp_deadlines = sorted({d for td in by_threads.values() for d in td.keys()})

        # Determine the thread count at which saturation first appears.
        first_saturated = next(
            (
                n for n in sorted_thread_counts
                if avg([float(r.get("io_errors", 0))
                        for d in ramp_deadlines
                        for r in by_threads[n].get(d, [])]) > 0
            ),
            None,
        )

        if len(ramp_deadlines) > 1:
            d_header = " | ".join(f"RPS ({d})" for d in ramp_deadlines)
            d_sep = " | ".join("--------" for _ in ramp_deadlines)
            lines.append(f"| Threads | {d_header} | I/O drops | Status |")
            lines.append(f"|---------|{d_sep}|-----------|--------|")
            for n in sorted_thread_counts:
                rps_cols = []
                io_drops = None
                for d in ramp_deadlines:
                    rows = by_threads[n].get(d, [])
                    if rows:
                        rps_cols.append(fmt_rps(rps_avg(rows)))
                        if io_drops is None:
                            io_drops_vals = [float(r.get("io_errors", 0)) for r in rows]
                            io_drops = avg(io_drops_vals)
                    else:
                        rps_cols.append("—")
                saturated = (io_drops or 0) > 0
                status = "**SATURATED** ←" if n == first_saturated else ("SATURATED" if saturated else "ok")
                lines.append(
                    f"| {n} | {' | '.join(rps_cols)}"
                    f" | {fmt_rps(io_drops or 0)} | {status} |"
                )
        else:
            d = ramp_deadlines[0] if ramp_deadlines else "?"
            lines.append(f"| Threads | Avg RPS ({d}) | Acks | Nacks | I/O drops | Status |")
            lines.append(f"|---------|--------------|------|-------|-----------|--------|")
            for n in sorted_thread_counts:
                rows = by_threads[n].get(d, [])
                ack_vals = [float(r.get("acks", 0)) for r in rows]
                nack_vals = [float(r.get("nacks", 0)) for r in rows]
                io_vals = [float(r.get("io_errors", 0)) for r in rows]
                saturated = avg(io_vals) > 0
                status = "**SATURATED** ←" if n == first_saturated else ("SATURATED" if saturated else "ok")
                lines.append(
                    f"| {n} | {fmt_rps(rps_avg(rows))}"
                    f" | {fmt_rps(avg(ack_vals))}"
                    f" | {fmt_rps(avg(nack_vals))}"
                    f" | {fmt_rps(avg(io_vals))} | {status} |"
                )

    lines += ["", "---", "*Generated by `deploy/avg_benchmarks.py`*", ""]
    return "\n".join(lines)


HISTORY_HEADER = """\
# Benchmark History

One row appended per CI run on `main`. All numbers at `batch_deadline_ms=1`.
Sync p99 and Buffered p50 are single-thread latency measurements.
Ramp peak = highest throughput level before connection-cap saturation kicks in.

| Version | Date | Runs | Sync RPS | Sync p99 | Buf p50 | Ramp peak RPS |
|---------|------|------|----------|----------|---------|---------------|
"""


def append_history(groups: dict, run_count: int, history_path: str, version: str) -> None:
    import os

    def get_rps(scenario: str) -> float | None:
        rows = groups.get(scenario, [])
        return rps_avg(rows) if rows else None

    def get_lat(scenario: str, field: str) -> float | None:
        rows = groups.get(scenario, [])
        vals = [float(r[field]) for r in rows if field in r]
        return avg(vals) if vals else None

    sync_rps = get_rps("single_thread_sync_d1ms")
    sync_p99 = get_lat("latency_sync_d1ms", "p99_us")
    buf_p50 = get_lat("latency_buffered_d1ms", "p50_us")

    # Peak ramp = highest RPS at the last unsaturated level.
    peak_rps: float | None = None
    for n in [8, 16, 32, 48, 64, 96]:
        key = f"ramp_{n}_threads_d1ms"
        rows = groups.get(key, [])
        if rows:
            io = avg([float(r.get("io_errors", 0)) for r in rows])
            if io == 0:
                peak_rps = rps_avg(rows)

    if sync_rps is None:
        print("append_history: no d1ms Sync data — skipping", file=sys.stderr)
        return

    date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    row = (
        f"| {version} | {date_str} | {run_count}"
        f" | {fmt_rps(sync_rps)}"
        f" | {fmt_us(sync_p99) if sync_p99 else '—'}"
        f" | {fmt_us(buf_p50) if buf_p50 else '—'}"
        f" | {fmt_rps(peak_rps) if peak_rps else '—'} |"
    )

    if not os.path.exists(history_path):
        with open(history_path, "w") as f:
            f.write(HISTORY_HEADER)

    with open(history_path, "a") as f:
        f.write(row + "\n")

    print(f"Appended history row to {history_path}")


def main():
    if len(sys.argv) not in (3, 4):
        print(
            f"usage: {sys.argv[0]} <results_file> <output_md> [history_md]",
            file=sys.stderr,
        )
        sys.exit(1)

    import os

    results_path = sys.argv[1]
    output_path = sys.argv[2]
    history_path = sys.argv[3] if len(sys.argv) == 4 else None

    groups = parse_results(results_path)

    if not groups:
        print("No BENCH: lines found — nothing to write.", file=sys.stderr)
        sys.exit(1)

    run_count = max(len(v) for v in groups.values())
    md = build_md(groups, run_count)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write(md)
    print(f"Wrote {output_path} ({run_count} run(s), {len(groups)} scenario(s))")

    if history_path:
        version = os.environ.get("WEIR_VERSION", "dev")
        append_history(groups, run_count, history_path, version)


if __name__ == "__main__":
    main()
