#!/usr/bin/env bash
# weir-readiness.sh — can this weir daemon accept ingest right now?
#
# READY means exactly what the daemon's own GET /readyz means: recovery is
# done, a producer listener is bound, the daemon is not shutting down, the
# wab_max_bytes cap is not rejecting pushes, and at least one shard can take
# records. Sink health, drain state, dead-letter and quarantine, and fsync or
# flusher-panic history never gate it: weir keeps buffering durably while the
# sink is down, and a recovered panic is history, not a present failure. They
# are printed as WARN lines instead.
#
# Exit codes (the same table as `weir-ctl status`):
#   0  READY
#   1  NOT READY, if and only if /readyz answers 503 (the codes are printed)
#   2  DEAD: unreachable, not a weir daemon, or (via weir-ctl) an identity mismatch
#   3  READY, but a critical condition that does not gate readiness is active
#      (shard_offline, drain_stopped, ...). Only with --fail-on-critical;
#      without it the script exits 0 and prints WARN lines.
#
# How it decides:
#   1. GET /readyz. No answer is DEAD. A daemon from before the health routes
#      existed answers every path with its metrics; reachable then counts as
#      ready, and the script says so.
#   2. If a weir-ctl with a `status` subcommand is installed (PATH, or --ctl),
#      its exit code is the verdict; its output is relayed. It adds the admin
#      socket's identity checks and the admin view's reasons.
#   3. Otherwise a 503 is NOT READY, and a 200 is READY with the active
#      conditions read from weir_condition_active on /metrics (refreshed every
#      5 s by the daemon, from the same evaluator /readyz and /status use).
#
# Usage:
#   weir-readiness.sh [--addr HOST:PORT] [--ctl PATH] [--admin-socket PATH]
#                     [--wab-dir DIR] [--fail-on-critical] [--allow-noop]
#
#   --socket PATH is accepted and ignored: readiness no longer round-trips the
#   producer socket, because /readyz already reports whether it is bound.
#
#   On a host with more than one instance, pass --wab-dir (or --admin-socket)
#   with --addr: without either, weir-ctl asks the default admin socket
#   (/var/lib/weir/wab.admin.sock), which may be another instance's.
#
# For cron + alerting, a systemd timer, or a bare-metal deploy gate. Kubernetes
# should probe /startupz, /readyz and /livez directly (docs/monitoring.md).
# Requires bash, curl and awk, which the container image does not ship; its
# HEALTHCHECK probes /livez with bash alone.
set -euo pipefail

ADDR="127.0.0.1:9185"
CTL="$(command -v weir-ctl || true)"
ADMIN_SOCKET=""
WAB_DIR=""
FAIL_ON_CRITICAL=0
ALLOW_NOOP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --addr)             ADDR="$2"; shift 2 ;;
    --ctl)              CTL="$2"; shift 2 ;;
    --admin-socket)     ADMIN_SOCKET="$2"; shift 2 ;;
    --wab-dir)          WAB_DIR="$2"; shift 2 ;;
    --socket)           shift 2 ;;
    --fail-on-critical) FAIL_ON_CRITICAL=1; shift ;;
    --allow-noop)       ALLOW_NOOP=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

fail_dead() { echo "DEAD: $*" >&2; exit 2; }

body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT

# ── 1. /readyz: reachable? which daemon? ────────────────────────────────────
# No -f: a 503 is an answer, not a failure. %{content_type} is empty when the
# server sends none.
meta="$(curl -sS --max-time 3 -o "$body_file" -w '%{http_code} %{content_type}' \
  "http://${ADDR}/readyz" 2>/dev/null)" \
  || fail_dead "/readyz not reachable at http://${ADDR}/readyz"
code="${meta%% *}"
ctype="${meta#* }"
body="$(cat "$body_file")"

case "$ctype" in
  application/openmetrics-text*)
    # A daemon without the health routes answers every path with its metrics.
    grep -q 'weir_' "$body_file" \
      || fail_dead "http://${ADDR} serves metrics with no weir_* series - wrong target?"
    echo "READY: the daemon at ${ADDR} has no /readyz (an older weir), so answering is all it can report; upgrade it for a real verdict"
    exit 0 ;;
  text/plain*) ;;
  *) fail_dead "http://${ADDR}/readyz answered HTTP ${code} as '${ctype:-no content type}' - not a weir daemon?" ;;
esac

if [[ "$code" == 503 && "$body" == "not ready:"* ]]; then
  ready=0
elif [[ "$code" == 200 && "$body" == ok* ]]; then
  ready=1
else
  fail_dead "http://${ADDR}/readyz answered HTTP ${code} with '${body:0:80}' - not a weir daemon?"
fi

# ── 2. weir-ctl, when installed, decides ────────────────────────────────────
# `weir-ctl status` reads the same evaluator through the admin socket (with its
# identity checks) or /status, and exits with the table above, so its exit code
# is the verdict. /readyz has already answered, so a dead daemon never reaches
# here: weir-ctl's offline fallback would report it NOT READY instead of DEAD.
# A weir-ctl from before `status` existed exits 2 on `status --help` (clap);
# the /readyz verdict below is used for it.
if [[ -n "$CTL" && -x "$CTL" ]] && "$CTL" status --help >/dev/null 2>&1; then
  args=(status --addr "$ADDR")
  if [[ -n "$ADMIN_SOCKET" ]]; then args+=(--admin-socket "$ADMIN_SOCKET"); fi
  if [[ -n "$WAB_DIR" ]]; then args+=(--wab-dir "$WAB_DIR"); fi
  rc=0
  out="$("$CTL" "${args[@]}" 2>&1)" || rc=$?
  if [[ "$ALLOW_NOOP" -eq 1 ]]; then out="$(grep -v 'noop_sink' <<<"$out" || true)"; fi
  if [[ -n "$out" ]]; then printf '%s\n' "$out"; fi
  case "$rc" in
    0) exit 0 ;;
    1) exit 1 ;;
    3) if [[ "$FAIL_ON_CRITICAL" -eq 1 ]]; then exit 3; fi
       echo "WARN: a critical condition is active (pass --fail-on-critical to exit 3)"
       exit 0 ;;
    *) fail_dead "weir-ctl status exited ${rc} (unreachable, or the daemon failed weir-ctl's identity check)" ;;
  esac
fi

# ── 3. The verdict from /readyz (no weir-ctl) ───────────────────────────────
if [[ "$ready" -eq 0 ]]; then
  echo "NOT READY: ${body#not ready: }"
  exit 1
fi

# Ready. Report what does not gate readiness but an operator should see.
metrics="$(curl -fsS --max-time 3 "http://${ADDR}/metrics" 2>/dev/null)" \
  || fail_dead "/readyz answered but /metrics did not at http://${ADDR}/metrics"

# Lines look like: weir_condition_active{code="sink_down",severity="warning"} 1
# A gauge may print as 1 or 1.0, so any fraction is stripped before comparing,
# and the label order is not assumed.
active="$(awk '$1 ~ /^weir_condition_active\{/ { v = $2; sub(/\..*/, "", v); if (v + 0 >= 1) print $1 }' <<<"$metrics" \
  | sed -E -e 's/.*code="([a-z0-9_]+)".*severity="([a-z]+)".*/\1 \2/' \
           -e 's/.*severity="([a-z]+)".*code="([a-z0-9_]+)".*/\2 \1/')"

critical=0
while read -r cond sev; do
  if [[ -z "${cond:-}" ]]; then continue; fi
  if [[ "$cond" == noop_sink && "$ALLOW_NOOP" -eq 1 ]]; then continue; fi
  echo "WARN: ${cond} (${sev}) - see docs/monitoring.md#cond-${cond}"
  if [[ "$sev" == critical ]]; then critical=1; fi
done <<<"$active"

sink_type="$(grep -oE 'weir_sink_info\{sink_type="[a-z0-9_]+"\} 1' <<<"$metrics" \
  | sed -E 's/.*sink_type="([a-z0-9_]+)".*/\1/' || true)"

if [[ "$critical" -eq 1 && "$FAIL_ON_CRITICAL" -eq 1 ]]; then
  echo "READY, but a critical condition is active (exit 3: --fail-on-critical)"
  exit 3
fi
echo "READY: /readyz ok, sink=${sink_type:-?}"
exit 0
