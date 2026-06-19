#!/usr/bin/env bash
# weir-readiness.sh — liveness + readiness probe for a weir daemon.
#
# LIVENESS  (is the process answering at all?):
#   - weir-ctl health --socket <path>  (Unix-socket round-trip; proves the
#     accept loop is live)
#   - /metrics reachable               (proves the metrics server bound)
# READINESS (is it safe to send production traffic / is delivery healthy?):
#   - sink is not "down"
#   - drain is not blocked on a full dead-letter dir
#   - no fsync failures and no flusher panics (durability / shard-offline hazards)
#   - WARN (ready, but loud) if the sink is "noop" — records are DISCARDED.
#
# Metric names below are the EXPOSED forms: prometheus-client appends `_total`
# to counters in the exposition (the daemon registers them without the suffix).
# Verified against crates/weir-server/src/metrics/mod.rs + docs/monitoring.md.
# weir-ctl subcommands/flags verified against crates/weir-ctl/src/main.rs.
#
# Exit codes:  0 = ready   1 = degraded / not-ready   2 = dead (liveness failed)
#
# Usage:
#   weir-readiness.sh [--socket PATH] [--addr HOST:PORT] [--ctl PATH] [--allow-noop]
#
# Designed for: a systemd ExecStartPost, a k8s readinessProbe exec, or cron+alert.
# Requires bash, plus `curl` and `awk` (NOT installed in the container image —
# this script targets the bare-metal / systemd deployment, where they are
# standard). Inside the Docker image use the bash /dev/tcp probe from the
# Dockerfile's HEALTHCHECK instead.
set -euo pipefail

SOCKET="${WEIR_SOCKET_PATH:-/run/weir/weir.sock}"
ADDR="127.0.0.1:9185"
CTL="$(command -v weir-ctl || echo /usr/local/bin/weir-ctl)"
ALLOW_NOOP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --socket)     SOCKET="$2"; shift 2 ;;
    --addr)       ADDR="$2"; shift 2 ;;
    --ctl)        CTL="$2"; shift 2 ;;
    --allow-noop) ALLOW_NOOP=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

fail_dead() { echo "DEAD: $*" >&2; exit 2; }

# ── LIVENESS ──────────────────────────────────────────────────────────────────
# 1. Socket health via the admin CLI (exercises the real producer path).
if ! "$CTL" health --socket "$SOCKET" >/dev/null 2>&1; then
  fail_dead "weir-ctl health failed on socket $SOCKET (accept loop not answering)"
fi

# 2. /metrics endpoint must be scrapeable.
METRICS="$(curl -fsS --max-time 3 "http://${ADDR}/metrics" 2>/dev/null)" \
  || fail_dead "/metrics not reachable at http://${ADDR}/metrics"

# 3. The response must actually be weir's exposition. A wrong-but-live HTTP
#    target (another service, a reverse proxy, a stray `python3 -m http.server`)
#    answers 200 so curl -fsS succeeds, but exposes no `weir_*` metrics. Without
#    this guard every metric grep below finds nothing, so none of the not-ready
#    conditions trip and the script falls through to the summary line — which
#    then exits 1 with no output under `set -euo pipefail`. Treat "reachable but
#    not weir" as DEAD (wrong target), not as a silent not-ready. weir always
#    emits many `weir_*` HELP/TYPE/data lines regardless of runtime state, so a
#    single `weir_` token is a reliable fingerprint.
if ! grep -q 'weir_' <<<"$METRICS"; then
  fail_dead "/metrics reachable at http://${ADDR}/metrics but returned no weir_* metrics — wrong target?"
fi

# helper: read a single-series gauge/counter value by exact line prefix.
mval() { awk -v k="$1" '$1==k {print $2; exit}' <<<"$METRICS"; }

# helper: true when a metric value is numerically >= 1. Gauges are exposed in
# float form (`1.0`, `0.0`), so a literal `== "1"` test never matches a live
# daemon — strip any fractional part and compare as an integer. Empty / absent
# values (the series hasn't been emitted yet) are treated as "not set" → false.
is_set() { local v="${1:-}"; [[ -n "$v" && "${v%.*}" =~ ^[0-9]+$ && "${v%.*}" -ge 1 ]]; }

# ── READINESS ───────────────────────────────────────────────────────────────
problems=()

# Sink-health one-hot family: state="down" == 1 means delivery stalled
# (records still buffer durably in the WAB, but nothing reaches the sink).
if is_set "$(mval 'weir_sink_health{state="down"}')"; then
  problems+=("sink health=DOWN (delivery stalled; WAB still buffering)")
fi

# Drain blocked on a full dead-letter dir == ALL delivery paused.
if is_set "$(mval 'weir_drain_state{state="blocked_dead_letter_full"}')"; then
  problems+=("drain BLOCKED: dead-letter dir full (free space or raise dead_letter_max_bytes)")
fi

# fsync failures must be 0 (durability hazard otherwise).
fsync_fail="$(mval 'weir_wab_fsync_failures_total')"
if [[ -n "$fsync_fail" && "${fsync_fail%.*}" -gt 0 ]]; then
  problems+=("fsync failures=$fsync_fail (storage problem — durability compromised)")
fi

# Flusher panics must be 0 (a panicked shard Nacks everything routed to it).
panics="$(mval 'weir_wab_flusher_panics_total')"
if [[ -n "$panics" && "${panics%.*}" -gt 0 ]]; then
  problems+=("flusher panics=$panics (a shard may be offline; restart needed)")
fi

# noop sink = records acked then DISCARDED. WARN unless explicitly allowed.
if grep -q 'weir_sink_info{sink_type="noop"} 1' <<<"$METRICS"; then
  if [[ "$ALLOW_NOOP" -eq 1 ]]; then
    echo "WARN: sink=noop (records DISCARDED) — allowed via --allow-noop" >&2
  else
    problems+=("sink=NOOP — records are acked then DISCARDED, not delivered (pass --allow-noop for soak tests)")
  fi
fi

if [[ ${#problems[@]} -gt 0 ]]; then
  printf 'NOT-READY:\n'; printf '  - %s\n' "${problems[@]}"
  exit 1
fi

# Healthy summary line (handy in journald / probe logs).
# Both extractions can legitimately find nothing (the records_accepted series
# is absent until the first push; the sink_info line could be a sink_type this
# regex doesn't cover). Append `|| true` / default with `${var:-…}` so a missing
# match never trips `set -e` into a silent non-zero exit — we reached READY and
# must print it. (The DEAD/no-weir-metrics guard above already rules out the
# wrong-target case that this line previously masked.)
accepted="$(mval 'weir_records_accepted_total{tier="batched"}')"
sink_type="$(grep -oE 'weir_sink_info\{sink_type="[a-z]+"\} 1' <<<"$METRICS" \
  | sed -E 's/.*sink_type="([a-z]+)".*/\1/' || true)"
echo "READY: socket ok, /metrics ok, sink=${sink_type:-?} healthy, accepted=${accepted:-0}"
exit 0
