#!/bin/sh
# Wait for weir's socket, then push records across all durability tiers in a
# loop so the dashboard has live data.
#
# Intensity is set by WEIR_LOAD_LEVEL (min | med | high | max). Each level maps
# to a per-iteration record COUNT and an inter-iteration SLEEP — together they
# set the offered load. The `levels` compose profile runs one loadgen per level
# against its own weir so the dashboard can compare min/med/high/max side by
# side. When WEIR_LOAD_LEVEL is unset the loadgen keeps the original steady
# trickle (count=60, sleep=2) so the default single-instance demo is unchanged.
set -eu
SOCK="${WEIR_SOCKET:-/run/weir/weir.sock}"
LEVEL="${WEIR_LOAD_LEVEL:-}"

# Map the level to (COUNT, SLEEP). push_simple pushes COUNT records per tier
# across 3 tiers (sync/batched/buffered), each awaiting its ack, so the sync
# tier (fsync-bound) dominates wall-clock — higher COUNT + lower SLEEP = more
# offered load and a visibly higher ingest rate on the dashboard.
case "${LEVEL}" in
  min)  COUNT=5;   SLEEP=8 ;;
  med)  COUNT=40;  SLEEP=3 ;;
  high) COUNT=150; SLEEP=1 ;;
  max)  COUNT=500; SLEEP=0 ;;   # tight loop, no inter-iteration sleep
  "")   COUNT=60;  SLEEP=2 ;;   # default trickle (unchanged single-instance demo)
  *)    echo "loadgen: unknown WEIR_LOAD_LEVEL='${LEVEL}' (want min|med|high|max)" >&2; exit 1 ;;
esac

echo "loadgen: waiting for socket ${SOCK} ..."
i=0
while [ ! -S "${SOCK}" ]; do
  i=$((i + 1))
  if [ "${i}" -gt 60 ]; then
    echo "loadgen: socket never appeared at ${SOCK}" >&2
    exit 1
  fi
  sleep 1
done

echo "loadgen: socket up — level=${LEVEL:-default} count=${COUNT} sleep=${SLEEP}s"
while true; do
  push_simple --socket "${SOCK}" --count "${COUNT}" >/dev/null 2>&1 || \
    echo "loadgen: push batch failed (continuing)" >&2
  [ "${SLEEP}" -gt 0 ] && sleep "${SLEEP}" || true
done
