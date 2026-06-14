#!/usr/bin/env python3
"""Generate one rich, single-instance Grafana dashboard per weir instance.

A weir "video wall" without the duplication: the panel set is defined ONCE here
and emitted per instance (min / med / high / max / chaos), each scoped to that
instance's scrape labels so every dashboard is a full, independent view of one
weir. Run after editing the panel set:

    python3 deploy/grafana/gen-dashboards.py

Outputs deploy/grafana/dashboards/weir-<name>.json. These are provisioned by the
monitoring stack (mounted into Grafana's dashboard provider path) and show up as
separate sidebar entries; open them in parallel tabs/windows. They import
standalone anywhere too — pick your Prometheus datasource on import.

The metric names/labels mirror crates/weir-server/src/metrics/mod.rs. The
per-stage histograms (weir_stage_*) are deliberately NOT used: they are gated
behind the `bench-trace` build feature and absent from the default image.
"""
import json
import os

DS = {"type": "prometheus", "uid": "${datasource}"}
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards")

# The instances to emit: (uid-suffix, human title, PromQL label selector).
INSTANCES = [
    ("min", "min", 'job="weir-levels", level="min"'),
    ("med", "med", 'job="weir-levels", level="med"'),
    ("high", "high", 'job="weir-levels", level="high"'),
    ("max", "max", 'job="weir-levels", level="max"'),
    ("chaos", "chaos", 'job="weir-chaos"'),
]


class Layout:
    """Flows panels left-to-right, wrapping at width 24, tracking y."""

    def __init__(self):
        self.panels, self.y, self.x, self.rowh = [], 0, 0, 0

    def row(self, title):
        if self.x > 0:
            self.y += self.rowh
        self.x, self.rowh = 0, 0
        self.panels.append({
            "type": "row", "title": title, "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y},
        })
        self.y += 1

    def add(self, panel, w, h):
        if self.x + w > 24:
            self.y += self.rowh
            self.x, self.rowh = 0, 0
        panel["gridPos"] = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.panels.append(panel)
        self.x += w
        self.rowh = max(self.rowh, h)


def target(expr, legend=None, fmt=None):
    t = {"refId": chr(65 + target._n), "expr": expr}
    target._n += 1
    if legend is not None:
        t["legendFormat"] = legend
    if fmt is not None:
        t["format"] = fmt
    return t


target._n = 0


def stat(title, expr, desc, unit=None, mappings=None, steps=None, color_mode="thresholds"):
    defaults = {"color": {"mode": color_mode}}
    if unit:
        defaults["unit"] = unit
    if mappings:
        defaults["mappings"] = mappings
    if steps:
        defaults["thresholds"] = {"mode": "absolute", "steps": steps}
    return {
        "type": "stat", "title": title, "description": desc, "datasource": DS,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"graphMode": "area", "colorMode": "background",
                    "reduceOptions": {"calcs": ["lastNotNull"]}},
        "targets": [target(expr)],
    }


def timeseries(title, targets, desc, unit="short", fill=10, overrides=None):
    return {
        "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "fieldConfig": {"defaults": {"unit": unit,
                        "custom": {"drawStyle": "line", "fillOpacity": fill}},
                        "overrides": overrides or []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                    "calcs": ["mean", "lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": targets,
    }


def heatmap(title, expr, desc, unit="s"):
    return {
        "type": "heatmap", "title": title, "description": desc, "datasource": DS,
        "targets": [target(expr, legend="{{le}}", fmt="heatmap")],
        "options": {"calculate": False,
                    "color": {"mode": "scheme", "scheme": "Spectral", "steps": 64,
                              "reverse": True},
                    "yAxis": {"unit": unit, "axisPlacement": "left"},
                    "cellGap": 1, "legend": {"show": True}, "tooltip": {"show": True}},
        "fieldConfig": {"defaults": {"custom": {"scaleDistribution": {"type": "linear"}}},
                        "overrides": []},
    }


def state_timeline(title, expr, desc):
    return {
        "type": "state-timeline", "title": title, "description": desc, "datasource": DS,
        "fieldConfig": {"defaults": {"custom": {"fillOpacity": 80},
                        "mappings": [{"type": "value", "options": {
                            "0": {"text": " "}, "1": {"text": "active"}}}],
                        "thresholds": {"mode": "absolute", "steps": [
                            {"color": "transparent", "value": None},
                            {"color": "blue", "value": 1}]}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "showValue": "never"},
        "targets": [target(expr, legend="{{state}}")],
    }


def piechart(title, expr, desc):
    return {
        "type": "piechart", "title": title, "description": desc, "datasource": DS,
        "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "right",
                    "values": ["value", "percent"]},
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                    "pieType": "donut"},
        "targets": [target(expr, legend="{{tier}}")],
    }


# one-hot ordinal encodings (0 healthy / 1 degraded / 2 down etc.)
HEALTH_MAP = [{"type": "value", "options": {
    "0": {"text": "Healthy", "color": "green", "index": 0},
    "1": {"text": "Degraded", "color": "yellow", "index": 1},
    "2": {"text": "DOWN", "color": "red", "index": 2}}}]
DRAIN_MAP = [{"type": "value", "options": {
    "0": {"text": "Draining", "color": "green", "index": 0},
    "1": {"text": "Retrying", "color": "yellow", "index": 1},
    "2": {"text": "BLOCKED", "color": "red", "index": 2}}}]
UP_MAP = [{"type": "value", "options": {
    "1": {"text": "UP", "color": "green", "index": 0},
    "0": {"text": "DOWN", "color": "red", "index": 1}}}]
TRI_STEPS = [{"color": "green", "value": None}, {"color": "yellow", "value": 1}, {"color": "red", "value": 2}]
RED_AT = lambda v: [{"color": "green", "value": None}, {"color": "red", "value": v}]


def build_panels(s):
    """Build the full single-instance panel set for label selector `s`."""
    target._n = 0
    L = Layout()

    # ── Summary strip ──────────────────────────────────────────────────────
    L.row("Summary")
    L.add(stat("Up", f"min(up{{{s}}})", "Is this instance being scraped.",
               mappings=UP_MAP, steps=[{"color": "red", "value": None}, {"color": "green", "value": 1}]), 3, 4)
    L.add(stat("Ingest rate", f"sum(rate(weir_records_accepted_total{{{s}}}[$__rate_interval]))",
               "Records accepted/sec (all tiers).", unit="reqps", color_mode="fixed"), 3, 4)
    L.add(stat("Ack rate", f"sum(rate(weir_records_ack_total{{{s}}}[$__rate_interval]))",
               "Records acked durably/sec.", unit="reqps", color_mode="fixed"), 3, 4)
    L.add(stat("Nack rate", f"sum(rate(weir_records_nack_total{{{s}}}[$__rate_interval]))",
               "Records Nacked/sec. Should be ~0.", unit="reqps", steps=RED_AT(0.001)), 3, 4)
    L.add(stat("fsync p99", f"histogram_quantile(0.99, sum by (le) (rate(weir_wab_fsync_duration_seconds_bucket{{{s}}}[$__rate_interval])))",
               "Durable-tier fsync p99 — the fsync-bound latency signal.", unit="s",
               steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 0.02}, {"color": "red", "value": 0.1}]), 3, 4)
    L.add(stat("Sink health", f'sum(weir_sink_health{{{s}, state="degraded"}}) + 2 * sum(weir_sink_health{{{s}, state="down"}})',
               "Active sink state (one-hot).", mappings=HEALTH_MAP, steps=TRI_STEPS), 3, 4)
    L.add(stat("Drain state", f'sum(weir_drain_state{{{s}, state="retrying_transient"}}) + 2 * sum(weir_drain_state{{{s}, state="blocked_dead_letter_full"}})',
               "Active drain state (one-hot).", mappings=DRAIN_MAP, steps=TRI_STEPS), 3, 4)
    L.add(stat("Queue depth", f"max(weir_queue_depth{{{s}}})",
               "Records waiting in the work queue.", unit="short", color_mode="fixed"), 3, 4)

    # ── Latency percentiles ────────────────────────────────────────────────
    L.row("Latency percentiles")
    L.add(timeseries("WAB fsync latency", [
        target(f"histogram_quantile(0.5,  sum by (le) (rate(weir_wab_fsync_duration_seconds_bucket{{{s}}}[$__rate_interval])))", "p50"),
        target(f"histogram_quantile(0.99, sum by (le) (rate(weir_wab_fsync_duration_seconds_bucket{{{s}}}[$__rate_interval])))", "p99"),
        target(f"histogram_quantile(0.999, sum by (le) (rate(weir_wab_fsync_duration_seconds_bucket{{{s}}}[$__rate_interval])))", "p99.9"),
    ], "fdatasync latency percentiles. weir is fsync-bound, so Sync-tier durability ≈ this.", unit="s"), 12, 8)
    L.add(heatmap("fsync latency distribution",
                  f"sum(rate(weir_wab_fsync_duration_seconds_bucket{{{s}}}[$__rate_interval])) by (le)",
                  "Full fdatasync latency histogram over time — see the modes, not just percentiles."), 12, 8)
    L.add(timeseries("Accept + sink-commit latency (p50/p99)", [
        target(f"histogram_quantile(0.5,  sum by (le) (rate(weir_accept_latency_seconds_bucket{{{s}}}[$__rate_interval])))", "accept p50"),
        target(f"histogram_quantile(0.99, sum by (le) (rate(weir_accept_latency_seconds_bucket{{{s}}}[$__rate_interval])))", "accept p99"),
        target(f"histogram_quantile(0.5,  sum by (le) (rate(weir_sink_commit_duration_seconds_bucket{{{s}}}[$__rate_interval])))", "commit p50"),
        target(f"histogram_quantile(0.99, sum by (le) (rate(weir_sink_commit_duration_seconds_bucket{{{s}}}[$__rate_interval])))", "commit p99"),
    ], "Socket-accept latency and Sink::commit latency — the non-fsync ends of the pipeline.", unit="s"), 12, 8)

    # ── Per-tier breakdown ─────────────────────────────────────────────────
    L.row("Per-tier breakdown")
    L.add(timeseries("Accepted/sec by tier", [
        target(f"sum by (tier) (rate(weir_records_accepted_total{{{s}}}[$__rate_interval]))", "{{tier}}"),
    ], "Ingest split by durability tier (Sync / Batched / Buffered).", unit="reqps"), 8, 7)
    L.add(timeseries("Acked/sec by tier", [
        target(f"sum by (tier) (rate(weir_records_ack_total{{{s}}}[$__rate_interval]))", "{{tier}}"),
    ], "Durable acks split by tier — compare against accepted to spot backpressure.", unit="reqps"), 8, 7)
    L.add(piechart("Tier share (cumulative)",
                   f"sum by (tier) (weir_records_accepted_total{{{s}}})",
                   "Share of total accepted records by tier."), 8, 7)

    # ── Segment lifecycle ──────────────────────────────────────────────────
    L.row("Segment lifecycle")
    L.add(timeseries("Segment transitions/sec by state", [
        target(f"sum by (state) (rate(weir_wab_segments_total{{{s}}}[$__rate_interval]))", "{{state}}"),
    ], "open → sealed → confirmed (→ quarantined) transitions/sec — segment churn at this load.", unit="short"), 12, 7)
    L.add(timeseries("WAB bytes on disk", [
        target(f"sum(weir_wab_bytes_on_disk{{{s}}})", "wab bytes"),
    ], "Current bytes held in WAB segment files (buffered, not yet drained).", unit="bytes"), 12, 7)

    # ── Sink & drain detail ────────────────────────────────────────────────
    L.row("Sink & drain detail")
    L.add(timeseries("Sink outcome/sec (committed / retried / dead-lettered)", [
        target(f"sum by (outcome) (rate(weir_sink_commit_records_total{{{s}}}[$__rate_interval]))", "{{outcome}}"),
    ], "Drain delivery outcomes/sec. dead_lettered = permanently rejected → on disk.", unit="reqps",
        overrides=[{"matcher": {"id": "byName", "options": "dead_lettered"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]}]), 12, 7)
    L.add(state_timeline("Drain state over time", f"weir_drain_state{{{s}}}",
                         "Exactly one active. blocked_dead_letter_full = all drain paused."), 12, 7)
    L.add(timeseries("Queue depth", [
        target(f"weir_queue_depth{{{s}}}", "queue depth"),
    ], "Work-queue depth — climbs when ingest outruns the WAB flushers.", unit="short"), 12, 7)
    L.add(timeseries("Dead-letter bytes on disk", [
        target(f"sum(weir_dead_letter_bytes_on_disk{{{s}}})", "dead-letter bytes"),
    ], "Permanently-rejected records awaiting manual handling. Reaching the cap BLOCKS the drain.",
        unit="bytes"), 12, 7)

    return L.panels


def build_dashboard(suffix, title, selector):
    return {
        "uid": f"weir-{suffix}",
        "title": f"weir — {title}",
        "tags": ["weir", "weir-level"],
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "graphTooltip": 1,
        "time": {"from": "now-15m", "to": "now"},
        "refresh": "10s",
        # Jump between instances (and the overview) without leaving the page.
        "links": [{
            "type": "dashboards", "title": "weir dashboards", "tags": ["weir"],
            "asDropdown": True, "includeVars": False, "keepTime": True,
            "targetBlank": False, "icon": "external link",
        }],
        "templating": {"list": [{
            "name": "datasource", "label": "Prometheus", "type": "datasource",
            "query": "prometheus", "current": {}, "hide": 0,
        }]},
        "panels": build_panels(selector),
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for suffix, title, selector in INSTANCES:
        dash = build_dashboard(suffix, title, selector)
        path = os.path.join(OUT_DIR, f"weir-{suffix}.json")
        with open(path, "w") as f:
            json.dump(dash, f, indent=2)
            f.write("\n")
        print(f"wrote {path}  ({len(dash['panels'])} panels, selector: {selector})")


if __name__ == "__main__":
    main()
