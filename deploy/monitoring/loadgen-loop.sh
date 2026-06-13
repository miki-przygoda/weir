#!/bin/sh
# Wait for weir's socket, then push a steady trickle across all durability tiers.
set -eu
SOCK="${WEIR_SOCKET:-/run/weir/weir.sock}"

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

echo "loadgen: socket up — pushing (sync/batched/buffered) every 2s"
while true; do
  push_simple --socket "${SOCK}" --count 60 >/dev/null 2>&1 || \
    echo "loadgen: push batch failed (continuing)" >&2
  sleep 2
done
