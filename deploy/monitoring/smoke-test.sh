#!/usr/bin/env bash
# End-to-end smoke test for the weir monitoring stack.
#
#   ./smoke-test.sh                      # core stack: up, test, leave running
#   ./smoke-test.sh --teardown           # ... and `docker compose down -v` at the end (CI)
#   ./smoke-test.sh --levels             # also bring up the min/med/high/max profile + assert it
#   ./smoke-test.sh --chaos              # also bring up the chaos profile + assert the bad panels light up
#   ./smoke-test.sh --levels --chaos --teardown   # everything, then tear down
#
# It launches weir + Prometheus + Grafana + the loadgen(s), lets them generate
# "movement", then asserts (a) the metrics reflect the traffic, (b) the
# durability/health invariants hold, and (c) the dashboard's own panel queries —
# run through Grafana's datasource proxy, exactly as the UI does — return the
# right data. With --levels/--chaos it additionally verifies the "Usage levels"
# comparison row and the chaos (dead-letter / sink-health / peer-UID) panels.
# Exits non-zero on any failure.
set -uo pipefail
cd "$(dirname "$0")"

PROM="http://localhost:9090"
GRAF="http://localhost:3000"
GAUTH="admin:admin"
PASS=0
FAIL=0
TEARDOWN=0
LEVELS=0
CHAOS=0
for arg in "$@"; do
  case "$arg" in
    --teardown) TEARDOWN=1 ;;
    --levels)   LEVELS=1 ;;
    --chaos)    CHAOS=1 ;;
    *) echo "unknown flag: $arg (want --teardown|--levels|--chaos)" >&2; exit 2 ;;
  esac
done

PROFILE_ARGS=""
[ "$LEVELS" -eq 1 ] && PROFILE_ARGS="$PROFILE_ARGS --profile levels"
[ "$CHAOS" -eq 1 ] && PROFILE_ARGS="$PROFILE_ARGS --profile chaos"

check() { # <name> <0-if-pass>
  if [ "$2" -eq 0 ]; then printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS + 1));
  else printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL + 1)); fi
}

# Instant PromQL query → scalar value. Empty result (no samples yet / counter
# never incremented) reads as 0, which is the right default for the sum/counter
# queries this test uses.
promq() {
  curl -fsS --data-urlencode "query=$1" "$PROM/api/v1/query" 2>/dev/null \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else '0')" 2>/dev/null || echo "0"
}

# Run a PromQL expr through Grafana's datasource proxy (the UI's path) → number
# of data points returned across frames.
grafq() {
  local body
  body=$(python3 -c "import json,sys;print(json.dumps({'queries':[{'refId':'A','datasource':{'uid':'prometheus'},'expr':sys.argv[1],'instant':True}],'from':'now-5m','to':'now'}))" "$1")
  curl -fsS -u "$GAUTH" -X POST "$GRAF/api/ds/query" -H 'Content-Type: application/json' -d "$body" 2>/dev/null \
    | python3 -c "
import sys,json
d=json.load(sys.stdin); fr=d.get('results',{}).get('A',{}).get('frames',[]); n=0
for f in fr:
    vals=f.get('data',{}).get('values',[])
    if vals: n+=len(vals[-1])
print(n)" 2>/dev/null || echo 0
}

# float comparison helper: gt A B → exit 0 iff A > B
gt() { python3 -c "import sys;sys.exit(0 if float('$1')>float('$2') else 1)" 2>/dev/null; }

echo "── weir monitoring smoke test ─────────────────────────────────────────────"
echo "[setup] bringing the stack up (idempotent, builds if needed)...${PROFILE_ARGS:+ profiles:$PROFILE_ARGS}"
# shellcheck disable=SC2086
docker compose $PROFILE_ARGS up -d --build --wait >/dev/null 2>&1 || { echo "  docker compose up failed"; exit 1; }
# Prometheus loads its scrape config at startup. If it was already running from a
# previous `up`, the bind-mounted prometheus.yml may now be newer (e.g. the
# levels/chaos jobs were just added by passing --profile). Force a live reload so
# the active targets match the file on disk. (--web.enable-lifecycle in compose.)
curl -fsS -X POST "$PROM/-/reload" >/dev/null 2>&1 || true
echo "  stack healthy; waiting for Prometheus to scrape weir..."
for _ in $(seq 1 30); do
  [ "$(promq 'up{job="weir"}')" = "1" ] && break
  sleep 2
done

echo "[ingest] simulating movement (loadgen, ~14s window)..."
# Scope the baseline-movement check to the stable core `weir` instance. A global
# sum would be polluted by the high-volume, frequently-recreated level instances
# (whose counters reset on recreate), making the monotonic before<after assertion
# flaky. The core instance runs a steady trickle and is never recreated mid-run.
before=$(promq 'sum(weir_records_accepted_total{job="weir"})')
sleep 14
after=$(promq 'sum(weir_records_accepted_total{job="weir"})')
gt "$after" "$before"
check "ingest advanced ($before → $after accepted)" $?
gt "$(promq 'sum(weir_wab_fsync_duration_seconds_count{job="weir"})')" "0"
check "fsyncs are happening" $?

echo "[invariants] durability + health (healthy fleet; chaos excluded)..."
[ "$(promq 'sum(weir_wab_fsync_failures_total)')" = "0" ]; check "no fsync failures" $?
[ "$(promq 'sum(weir_wab_flusher_panics_total)')" = "0" ]; check "no flusher panics" $?
[ "$(promq 'sum(weir_ack_timeout_total)')" = "0" ]; check "no ack timeouts" $?
[ "$(promq 'sum(weir_recovery_segments_quarantined_total)')" = "0" ]; check "no quarantined segments" $?
# The chaos instance degrades its sink and blocks its drain on purpose, so the
# health invariants assert the NON-chaos fleet (job!="weir-chaos") is clean.
[ "$(promq 'sum(weir_sink_health{job!="weir-chaos", state="degraded"})+2*sum(weir_sink_health{job!="weir-chaos", state="down"})')" = "0" ]; check "sink health = Healthy (non-chaos)" $?
[ "$(promq 'sum(weir_drain_state{job!="weir-chaos", state="retrying_transient"})+2*sum(weir_drain_state{job!="weir-chaos", state="blocked_dead_letter_full"})')" = "0" ]; check "drain state = Draining (non-chaos)" $?

echo "[ui] UI data correct (panel queries via Grafana datasource proxy)..."
curl -fsS -u "$GAUTH" "$GRAF/api/dashboards/uid/weir-overview" >/dev/null 2>&1; check "dashboard provisioned (uid weir-overview)" $?
[ "$(curl -fsS -u "$GAUTH" "$GRAF/api/datasources/uid/prometheus/health" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("status"))' 2>/dev/null)" = "OK" ]; check "Grafana → Prometheus datasource healthy" $?
[ "$(grafq 'sum by (tier) (weir_records_accepted_total)')" -ge 1 ]; check "ingest-by-tier panel returns data" $?
[ "$(grafq 'sum by (state) (weir_wab_segments_total)')" -ge 1 ]; check "segment-lifecycle panel returns data" $?
[ "$(grafq 'sum(weir_sink_health{job="weir", state="degraded"}) + 2 * sum(weir_sink_health{job="weir", state="down"})')" -ge 1 ]; check "sink-health panel returns data (renders Healthy)" $?

# ── Usage levels (min/med/high/max) ─────────────────────────────────────────────
if [ "$LEVELS" -eq 1 ]; then
  echo "[levels] waiting for all four level instances to be scraped..."
  for _ in $(seq 1 30); do
    [ "$(promq 'count(group by (level) (up{job="weir-levels"} == 1))')" = "4" ] && break
    sleep 2
  done
  echo "[levels] letting the four levels diverge (~16s)..."
  sleep 16
  [ "$(promq 'count(group by (level) (up{job="weir-levels"} == 1))')" = "4" ]; check "all 4 level instances scraped (min/med/high/max)" $?
  mx=$(promq 'sum(weir_records_accepted_total{job="weir-levels", level="max"})')
  mn=$(promq 'sum(weir_records_accepted_total{job="weir-levels", level="min"})')
  gt "$mx" "$mn"; check "max out-ingests min ($mn → $mx accepted)" $?
  # Each level has its own provisioned single-instance dashboard.
  for lvl in min med high max; do
    curl -fsS -u "$GAUTH" "$GRAF/api/dashboards/uid/weir-$lvl" >/dev/null 2>&1
    check "dashboard 'weir — $lvl' provisioned (uid weir-$lvl)" $?
  done
  # Representative per-instance panel queries return data (the path the UI uses).
  [ "$(grafq 'histogram_quantile(0.99, sum by (le) (rate(weir_wab_fsync_duration_seconds_bucket{job="weir-levels", level="max"}[1m])))')" -ge 1 ]; check "weir — max fsync-percentiles panel returns data" $?
  [ "$(grafq 'sum by (tier) (rate(weir_records_accepted_total{job="weir-levels", level="high"}[1m]))')" -ge 1 ]; check "weir — high per-tier panel returns data" $?
fi

# ── Chaos (dead-letter / sink-health / peer-UID) ────────────────────────────────
if [ "$CHAOS" -eq 1 ]; then
  echo "[chaos] waiting for chaos instance + dead-lettering to begin (up to ~40s)..."
  for _ in $(seq 1 20); do
    [ "$(promq 'up{job="weir-chaos"}')" = "1" ] && gt "$(promq 'sum(weir_dead_letter_bytes_on_disk{job="weir-chaos"})')" "0" && break
    sleep 2
  done
  curl -fsS -u "$GAUTH" "$GRAF/api/dashboards/uid/weir-chaos" >/dev/null 2>&1; check "dashboard 'weir — chaos' provisioned (uid weir-chaos)" $?
  gt "$(promq 'sum(weir_dead_letter_bytes_on_disk{job="weir-chaos"})')" "0"; check "dead-letter bytes climbing (panel lights up)" $?
  gt "$(promq 'sum(weir_sink_commit_records_total{job="weir-chaos", outcome="dead_lettered"})')" "0"; check "records dead-lettered (sink-outcome panel)" $?
  [ "$(promq 'sum(weir_sink_health{job="weir-chaos", state="degraded"})')" != "0" ]; check "chaos sink reports Degraded (sink-health panel)" $?
  gt "$(promq 'sum(weir_connection_rejected_peer_uid_total{job="weir-chaos"})')" "0"; check "peer-UID rejections climbing (root probe refused)" $?
fi

echo "───────────────────────────────────────────────────────────────────────────"
echo "  ${PASS} passed, ${FAIL} failed"
[ "$TEARDOWN" -eq 1 ] && { echo "  tearing down..."; docker compose $PROFILE_ARGS down -v >/dev/null 2>&1; }
[ "$FAIL" -eq 0 ] || exit 1
echo "  ✓ monitoring stack verified end to end"
