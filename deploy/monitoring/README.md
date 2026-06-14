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

## Opt-in profiles: `levels` and `chaos`

Two heavier, opt-in scenarios layer on top of the default stack (services
without a profile always run; profiles add more):

```sh
docker compose --profile levels up --build   # min/med/high/max comparison
docker compose --profile chaos  up --build   # dead-letter / sink / peer-UID faults
docker compose --profile levels --profile chaos up --build   # both
```

**`levels` — a dashboard per usage level.** Four extra weir instances, each
driven by its own loadgen at `min` / `med` / `high` / `max` intensity (record
count + interval set in `loadgen-loop.sh`). Prometheus scrapes them as
`job=weir-levels` with a per-instance `level` label, and **each instance gets its
own full dashboard** — *weir — min*, *weir — med*, *weir — high*, *weir — max* —
that you open in parallel tabs/windows (a "weir dashboards" dropdown at the top of
every dashboard jumps between them). On a fresh run ingest separates cleanly
(`min ≪ med ≪ high ≪ max`). The per-instance dashboards are generated DRY from a
single panel definition by `../grafana/gen-dashboards.py` (run it after editing
the panel set); the `chaos` instance gets one too (*weir — chaos*).

**`chaos` — drive the should-be-zero panels.** The healthy demo correctly pins
dead-letter, sink-health, and peer-UID at zero (nothing's wrong), which makes
those panels look inert. This profile makes them *earn* their place:

- a **failmock** (`nginx`) returns **HTTP 400** to every sink POST. 400 is a
  *permanent* status in weir's classifier (4xx except 408/429), so records are
  **dead-lettered** rather than retried — `dead_letter_bytes_on_disk` climbs to
  its (small) cap and the drain transitions to **BLOCKED**;
- the sink's health probe HEADs the same URL, gets 400, and reports
  **Degraded** — the sink-health panel goes yellow;
- `peer_uid_check` is on and a root **chaos-probe** is refused by the
  `SO_PEERCRED` check while the uid-10001 loadgen is accepted —
  `connection_rejected_peer_uid_total` climbs.

The chaos instance is scraped as `job=weir-chaos`; because Prometheus regexes are
anchored, the shipped alerts (`job=~"weir"`) don't match it, so an idle/off chaos
profile never trips `WeirInstanceDown`.

> **TLS chaos is intentionally not here.** Handshake-failure metrics are gated
> behind the `tls` build feature (not in the default image), so demonstrating
> them needs a `--features tls` weir build + certs. Parked for Phase 5 (see
> `docs/explorations/parked-future-directions.md`); the TLS panel renders a
> clean `0` baseline in the meantime.

Tear down everything with `docker compose --profile levels --profile chaos down -v`.

It's an **example you own** — every Grafana panel is editable, and the alert
thresholds in `../prometheus/weir-alerts.yml` are yours to tune.

The overview dashboard opens with an at-a-glance **health strip** (up ·
fsync-failures · flusher-panics · ack-timeouts · sink-health · drain-state ·
ingest · nack), then Ingest / Durability / Drain / System detail rows. The
`levels`/`chaos` profiles add a **full per-instance dashboard** each (*weir —
min/med/high/max/chaos*) — reachable from the "weir dashboards" dropdown.

## Verify it (end-to-end smoke test)

```sh
./smoke-test.sh                       # launch, generate load, assert, leave running
./smoke-test.sh --teardown            # ... and tear down at the end (CI)
./smoke-test.sh --levels              # also assert the min/med/high/max comparison
./smoke-test.sh --chaos               # also assert the chaos panels light up
./smoke-test.sh --levels --chaos --teardown   # everything, then tear down
```

Brings the stack up, simulates movement with the loadgen, then asserts the
metrics advance, the durability/health invariants hold, and the dashboard's own
panel queries — run through Grafana's datasource proxy, exactly as the UI does —
return the right data. With `--levels` it additionally verifies the four level
instances diverge (`max` out-ingests `min`) and the Usage-levels panels return
per-level data; with `--chaos` it verifies dead-letter bytes climb, records are
dead-lettered, the sink reports Degraded, and peer-UID rejections accrue. Exits
non-zero on any failure. The core run (no profiles) runs in CI (the `monitoring`
job); the `--levels`/`--chaos` scenarios are for local verification.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | The demo stack: core services + `levels`/`chaos` profiles. |
| `weir.toml` | weir config for the demo (lively segment rotations). |
| `weir-chaos.toml` | Chaos-instance config: failing HTTP sink + small dead-letter cap + peer-UID on. |
| `prometheus.yml` | Scrape config (`weir`, `weir-levels`, `weir-chaos`) + loads the alert rules. |
| `loadgen.Dockerfile` | Builds `weir-client`'s `push_simple` example + bundles the loadgen/probe scripts. |
| `loadgen-loop.sh` | Push loop; `WEIR_LOAD_LEVEL` (min/med/high/max) sets the offered load. |
| `chaos-probe.sh` | Unauthorized (root) client for the chaos profile — refused by the peer-UID check. |
| `chaos-failmock.conf` | `nginx` config returning HTTP 400 to every request (permanent → dead-letter). |
| `grafana/provisioning/` | Auto-loads the Prometheus datasource + the dashboards. |
| `smoke-test.sh` | End-to-end test: launch → load → assert metrics + dashboard data (`--levels`/`--chaos`). |
| `../grafana/weir-dashboard.json` | The overview dashboard (importable anywhere). |
| `../grafana/gen-dashboards.py` | Generates the per-instance dashboards (DRY, one panel definition). |
| `../grafana/dashboards/` | The generated per-instance dashboards (`weir-min/med/high/max/chaos`). |
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
