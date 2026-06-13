#!/usr/bin/env bash
# End-to-end smoke test for the weir monitoring stack.
#
#   ./smoke-test.sh            # bring the stack up, test, leave it running
#   ./smoke-test.sh --teardown # ... and `docker compose down -v` at the end (CI)
#
# It launches weir + Prometheus + Grafana + the loadgen, lets the loadgen
# generate "movement", then asserts (a) the metrics reflect the traffic, (b) the
# durability/health invariants hold, and (c) the dashboard's own panel queries —
# run through Grafana's datasource proxy, exactly as the UI does — return the
# right data. Exits non-zero on any failure.
set -uo pipefail
cd "$(dirname "$0")"

PROM="http://localhost:9090"
GRAF="http://localhost:3000"
GAUTH="admin:admin"
PASS=0
FAIL=0
TEARDOWN=0
[ "${1:-}" = "--teardown" ] && TEARDOWN=1

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

echo "── weir monitoring smoke test ─────────────────────────────────────────────"
echo "[1/4] bringing the stack up (idempotent, builds if needed)..."
docker compose up -d --build --wait >/dev/null 2>&1 || { echo "  docker compose up failed"; exit 1; }
echo "  stack healthy; waiting for Prometheus to scrape weir..."
for _ in $(seq 1 30); do
  [ "$(promq 'up{job="weir"}')" = "1" ] && break
  sleep 2
done

echo "[2/4] simulating movement (loadgen, ~14s window)..."
before=$(promq 'sum(weir_records_accepted_total)')
sleep 14
after=$(promq 'sum(weir_records_accepted_total)')
python3 -c "import sys;sys.exit(0 if float('$after')>float('$before') else 1)" 2>/dev/null
check "ingest advanced ($before → $after accepted)" $?
python3 -c "import sys;sys.exit(0 if float('$(promq 'sum(weir_wab_fsync_duration_seconds_count)')')>0 else 1)" 2>/dev/null
check "fsyncs are happening" $?

echo "[3/4] durability + health invariants..."
[ "$(promq 'sum(weir_wab_fsync_failures_total)')" = "0" ]; check "no fsync failures" $?
[ "$(promq 'sum(weir_wab_flusher_panics_total)')" = "0" ]; check "no flusher panics" $?
[ "$(promq 'sum(weir_ack_timeout_total)')" = "0" ]; check "no ack timeouts" $?
[ "$(promq 'sum(weir_recovery_segments_quarantined_total)')" = "0" ]; check "no quarantined segments" $?
[ "$(promq 'sum(weir_sink_health{state="degraded"})+2*sum(weir_sink_health{state="down"})')" = "0" ]; check "sink health = Healthy" $?
[ "$(promq 'sum(weir_drain_state{state="retrying_transient"})+2*sum(weir_drain_state{state="blocked_dead_letter_full"})')" = "0" ]; check "drain state = Draining" $?

echo "[4/4] UI data correct (panel queries via Grafana datasource proxy)..."
curl -fsS -u "$GAUTH" "$GRAF/api/dashboards/uid/weir-overview" >/dev/null 2>&1; check "dashboard provisioned (uid weir-overview)" $?
[ "$(curl -fsS -u "$GAUTH" "$GRAF/api/datasources/uid/prometheus/health" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("status"))' 2>/dev/null)" = "OK" ]; check "Grafana → Prometheus datasource healthy" $?
[ "$(grafq 'sum by (tier) (weir_records_accepted_total)')" -ge 1 ]; check "ingest-by-tier panel returns data" $?
[ "$(grafq 'sum by (state) (weir_wab_segments_total)')" -ge 1 ]; check "segment-lifecycle panel returns data" $?
[ "$(grafq 'sum(weir_sink_health{state="degraded"}) + 2 * sum(weir_sink_health{state="down"})')" -ge 1 ]; check "sink-health panel returns data (renders Healthy)" $?

echo "───────────────────────────────────────────────────────────────────────────"
echo "  ${PASS} passed, ${FAIL} failed"
[ "$TEARDOWN" -eq 1 ] && { echo "  tearing down..."; docker compose down -v >/dev/null 2>&1; }
[ "$FAIL" -eq 0 ] || exit 1
echo "  ✓ monitoring stack verified end to end"
