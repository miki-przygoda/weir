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

# No `rm -f weir.sock`: the daemon removes a stale socket itself after a
# connect probe, and deleting a LIVE one would cut off its producers.
mkdir -p "$HERE/wab"
"$SERVER" \
  --socket-path "$HERE/weir.sock" \
  --wab-dir "$HERE/wab" \
  --metrics-port 19008 \
  --log-level info \
  > "$HERE/daemon.log" 2>&1 &
echo $! > "$HERE/daemon.pid"
echo "weir-server pid=$(cat "$HERE/daemon.pid"), socket=$HERE/weir.sock"

for _ in $(seq 1 50); do
  # Wait for this daemon to say it is listening (a stale socket file from an
  # earlier run would otherwise look ready).
  grep -q "socket listening" "$HERE/daemon.log" 2>/dev/null && { echo "socket ready"; exit 0; }
  kill -0 "$(cat "$HERE/daemon.pid")" 2>/dev/null || break
  perl -e 'select(undef,undef,undef,0.1)'
done
echo "daemon did not start listening; see $HERE/daemon.log" >&2
exit 1
