#!/usr/bin/env bash
# Launch a weir-server with this project's isolated socket/wab/metrics.
# Writes the child PID to daemon.pid so you can kill ONLY your own daemon:
#     kill "$(cat daemon.pid)"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Repo root is two levels up from this demo (demos/py-wire-client). Default the
# server binary to the repo's debug build; override with WEIR_SERVER_BIN.
REPO="$(cd "$HERE/../.." && pwd)"
SERVER="${WEIR_SERVER_BIN:-$REPO/target/debug/weir-server}"

rm -f "$HERE/weir.sock"
"$SERVER" \
  --socket-path "$HERE/weir.sock" \
  --wab-dir "$HERE/wab" \
  --metrics-port 19008 \
  --log-level info \
  > "$HERE/daemon.log" 2>&1 &
echo $! > "$HERE/daemon.pid"
echo "weir-server pid=$(cat "$HERE/daemon.pid"), socket=$HERE/weir.sock"

for _ in $(seq 1 50); do
  [ -S "$HERE/weir.sock" ] && { echo "socket ready"; exit 0; }
  perl -e 'select(undef,undef,undef,0.1)'
done
echo "socket did not appear; see $HERE/daemon.log" >&2
exit 1
