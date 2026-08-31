#!/bin/bash
# Launches a time-bounded soak under tmux, self-documenting.
#
# Usage: run-soak.sh <schedule.toml> <label> <backstop_secs>
#
# Runs under tmux so the soak is independent of any SSH session. Two agents
# died mid-run holding one, which is why this exists as a script rather than a
# command someone types into a connection they then walk away from.
set -u
SCHEDULE=$1
LABEL=$2
BACKSTOP=$3
CHAOS_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$CHAOS_DIR" || exit 1
LOG="$CHAOS_DIR/../$LABEL.console.log"

sudo -A modprobe dm-flakey dm-delay 2>/dev/null

DEADLINE=$(python3 -c "import tomllib; print(tomllib.load(open('$SCHEDULE','rb')).get('max_duration_secs','none'))")

{
  echo "==================================================================="
  echo "SOAK START      $(date -Is)"
  echo "label           $LABEL"
  echo "schedule        $SCHEDULE (Phase 1: kill -9 only)"
  echo "internal stop   ${DEADLINE}s, clean, between episodes"
  echo "backstop        ${BACKSTOP}s SIGINT, only if the run hangs"
  echo "==================================================================="
} | tee -a "$LOG"

# Writes SETUP.md into the run's own directory once run.py has created it.
# Backgrounded because it polls for that directory.
./capture-setup.sh "$SCHEDULE" "$LABEL" &

# -s INT, not the default TERM: run.py tears down the loop device, the mount
# and its children in a `finally` that a KeyboardInterrupt unwinds. SIGTERM
# would skip it and strand the devices.
# taskset 0-15: nproc reports 4 here because of isolcpus, so without this weir
# sees 4 cores and warns shard_count=4 is over-provisioned.
sudo -A timeout -s INT "$BACKSTOP" \
  taskset -c 0-15 python3 -u orchestrator/run.py "$SCHEDULE" 2>&1 | tee -a "$LOG"
RC=${PIPESTATUS[0]}

{
  echo "SOAK EXIT       rc=$RC at $(date -Is)"
  echo "  rc=0   completed and passed"
  echo "  rc=1   completed with violations or anomalies — read report.md"
  echo "  rc=124 backstop fired; the internal deadline did NOT stop it"
} | tee -a "$LOG"

# Keep the console log WITH the run it describes, so an archived run dir is
# self-contained rather than pointing at a file somewhere else on the box.
RUN_DIR=$(ls -td "$CHAOS_DIR"/runs/*/ 2>/dev/null | head -1)
if [ -n "$RUN_DIR" ]; then
  sudo -A cp "$LOG" "$RUN_DIR/console.log" 2>/dev/null && echo "console log copied into $RUN_DIR"
fi

exec bash
