#!/bin/sh
# chaos-probe — an "unauthorized client" for the chaos profile.
#
# Runs as root (uid 0). The chaos weir runs as uid 10001 with peer_uid_check on,
# so weir's SO_PEERCRED check refuses every connection from this process and
# increments weir_connection_rejected_peer_uid_total. The connect succeeds at
# the socket layer (root can open the 0700 socket dir), but weir drops the
# stream right after accept, so the push fails — which is the point.
set -eu
SOCK="${WEIR_SOCKET:-/run/weir/weir.sock}"

echo "chaos-probe: waiting for socket ${SOCK} ..."
i=0
while [ ! -S "${SOCK}" ]; do
  i=$((i + 1))
  if [ "${i}" -gt 60 ]; then
    echo "chaos-probe: socket never appeared at ${SOCK}" >&2
    exit 1
  fi
  sleep 1
done

echo "chaos-probe: probing as uid $(id -u) every 3s (expect peer-UID rejection)"
while true; do
  # Expected to fail: weir rejects the mismatched-uid peer. The attempt is what
  # increments the rejection counter, so we ignore the error and keep probing.
  push_simple --socket "${SOCK}" --count 1 >/dev/null 2>&1 || true
  sleep 3
done
