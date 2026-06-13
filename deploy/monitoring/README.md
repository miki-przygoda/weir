# weir monitoring

weir exposes Prometheus metrics at `/metrics` (default port 9185). This directory
is a turnkey example of the standard observability stack around it — and the
artifacts you'd drop into an existing one.

```
weir ── /metrics ──(scrape)──▶ Prometheus ──(query)──▶ Grafana
  (already built)               history + alerts        dashboard
```

weir's only job is exposing `/metrics`; Prometheus stores the history and
evaluates alerts, Grafana renders the dashboard. We don't reinvent either — the
deliverables are *config*, not software.

## Turnkey demo

```sh
cd deploy/monitoring
docker compose up --build
```

Brings up four containers — **weir** (noop sink, 4 shards, small segments),
a **loadgen** (pushes a steady trickle across all three durability tiers),
**Prometheus** (scraping weir, alerts loaded), and **Grafana** (datasource +
dashboard pre-provisioned).

- **Grafana:** http://localhost:3000 — anonymous (admin), the *weir — durable
  write buffer* dashboard is already loaded and showing live data.
- **Prometheus:** http://localhost:9090 — *Status → Rules* shows the alerts,
  *Status → Targets* shows weir being scraped.

Tear down with `docker compose down -v`.

It's an **example you own** — every Grafana panel is editable, and the alert
thresholds in `../prometheus/weir-alerts.yml` are yours to tune.

The dashboard opens with an at-a-glance **health strip** (up · fsync-failures ·
flusher-panics · ack-timeouts · sink-health · drain-state · ingest · nack),
then Ingest / Durability / Drain / System detail rows.

## Verify it (end-to-end smoke test)

```sh
./smoke-test.sh             # launch, generate load, assert, leave running
./smoke-test.sh --teardown  # ... and tear down at the end (CI)
```

Brings the stack up, simulates movement with the loadgen, then asserts the
metrics advance, the durability/health invariants hold, and the dashboard's own
panel queries — run through Grafana's datasource proxy, exactly as the UI does —
return the right data. Exits non-zero on any failure. Runs in CI (the
`monitoring` job).

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | The 4-service demo stack. |
| `weir.toml` | weir config for the demo (lively segment rotations). |
| `prometheus.yml` | Scrape config + loads the alert rules. |
| `loadgen.Dockerfile` / `loadgen-loop.sh` | Builds `weir-client`'s `push_simple` example and loops it. |
| `grafana/provisioning/` | Auto-loads the Prometheus datasource + the dashboard. |
| `smoke-test.sh` | End-to-end test: launch → load → assert metrics + dashboard data. |
| `../grafana/weir-dashboard.json` | The dashboard (importable anywhere). |
| `../prometheus/weir-alerts.yml` | The alert rules (`promtool check rules` validated). |

## Using these with your *existing* Prometheus + Grafana

1. **Scrape weir.** Add a job to your `prometheus.yml`:
   ```yaml
   scrape_configs:
     - job_name: weir
       static_configs:
         - targets: ["YOUR_WEIR_HOST:9185"]
   ```
   (The alert selectors assume `job="weir"` — match it or adjust them.)
2. **Load the alerts.** Reference `weir-alerts.yml` from your `rule_files:` and
   wire Alertmanager. Tune the `[TUNE]` thresholds to your storage/workload —
   anchor fsync thresholds on your measured p99.9 (see
   `docs/benchmarks/snapshot-2026-06-13-comparison.md`).
3. **Import the dashboard.** *Dashboards → Import* → upload
   `weir-dashboard.json`, then pick your Prometheus datasource.

Full operator guide + per-alert runbook: [`docs/monitoring.md`](../../docs/monitoring.md).
