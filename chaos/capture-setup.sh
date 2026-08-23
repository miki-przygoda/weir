#!/bin/bash
# Writes SETUP.md into a run's own directory, beside its logs.
#
# WHY THIS IS NOT OPTIONAL: a chaos result is only comparable to another chaos
# result if you can see what differed. Every earlier run recorded its schedule
# and nothing else — not the kernel, not the CPU pinning, not which binary
# actually ran — so comparing two of them meant reconstructing the venue from
# memory. It also captures the binary hashes, which is the only thing that
# cannot be recovered later: the tree moves on, but a sha256 pins exactly what
# was executed.
#
# Usage: capture-setup.sh <schedule.toml> <label> [duration_note]
set -u
SCHEDULE=$1
LABEL=$2
NOTE=${3:-}
CHAOS_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$CHAOS_DIR" || exit 1

# Reuse run.py's OWN derivation rather than re-implementing it here: if that
# formula ever changes, this must follow it, and a silent divergence would file
# SETUP.md into a directory no run ever writes to.
SEED=$(python3 -c "import tomllib,sys; print(tomllib.load(open('$SCHEDULE','rb'))['seed'])")
RUN_ID=$(python3 -c "
import sys; sys.path.insert(0, 'orchestrator')
import run
print(run.run_id_from_seed($SEED))
")
RUN_DIR="$CHAOS_DIR/runs/$RUN_ID"

# run.py creates the directory; wait for it rather than racing it.
for _ in $(seq 1 120); do
  [ -d "$RUN_DIR" ] && break
  sleep 1
done
if [ ! -d "$RUN_DIR" ]; then
  echo "capture-setup: run dir $RUN_DIR never appeared" >&2
  exit 1
fi

WEIR_BIN="$CHAOS_DIR/../target/release/weir-server"
COMMIT=$(git rev-parse --short HEAD 2>/dev/null)
DIRT=$(git status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')
[ "$DIRT" != "0" ] && COMMIT="$COMMIT-dirty ($DIRT tracked files modified)"

OUT=$(mktemp)
{
  echo "# Run setup — $LABEL"
  echo
  # RUN_STARTED lets a retrospective capture state the real start time
  # instead of the moment the file was written.
  echo "Run id \`$RUN_ID\` · started ${RUN_STARTED:-$(date -Is)}"
  [ -n "$NOTE" ] && { echo; echo "$NOTE"; }
  echo
  echo "## What this run is"
  echo
  echo "**Phase 1 — random \`kill -9\` only.** It is NOT Phase 2: dm-flakey"
  echo "injection is unimplemented (\`dm_stack.py\` builds loop -> ext4 -> mount"
  echo "and nothing else, and the episode loop injects \`kill_random\`"
  echo "unconditionally). A green result here says nothing about power loss,"
  echo "torn writes, or the durability tiers."
  echo
  echo "## Code under test"
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| weir commit | \`$COMMIT\` |"
  echo "| weir-server sha256 | \`$(sha256sum "$WEIR_BIN" 2>/dev/null | cut -c1-16)…\` |"
  echo "| weir-server built | $(stat -c %y "$WEIR_BIN" 2>/dev/null | cut -d. -f1) |"
  echo "| loadgen sha256 | \`$(sha256sum target/release/loadgen 2>/dev/null | cut -c1-16)…\` |"
  echo "| recorder sha256 | \`$(sha256sum target/release/recorder 2>/dev/null | cut -c1-16)…\` |"
  echo
  echo "A \`-dirty\` commit means the tree carried uncommitted changes: the"
  echo "binary hashes above are then the only exact identity this run has."
  echo
  echo "## Venue"
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| host | $(hostname) |"
  echo "| kernel | $(uname -r) |"
  echo "| cpu | $(lscpu | awk -F: '/Model name/{gsub(/^ +/,"",$2); print $2; exit}') |"
  echo "| cores visible / total | $(nproc) / $(lscpu | awk -F: '/^CPU\(s\)/{gsub(/ /,"",$2); print $2; exit}') |"
  echo "| memory | $(free -g | awk '/^Mem:/{print $2" GiB"}') |"
  echo "| filesystem under test | ext4 on loop device (see storage below) |"
  echo "| host free space | $(df -h "$CHAOS_DIR" | awk 'NR==2{print $4}') |"
  echo
  echo "**Boot cmdline** — this box is deliberately tuned for measurement, which"
  echo "is why \`nproc\` reports fewer cores than \`lscpu\`:"
  echo
  echo '```'
  cat /proc/cmdline
  echo '```'
  echo
  echo "\`isolcpus\` reserves those cores from the general scheduler. The run is"
  echo "launched under \`taskset -c 0-15\` to reach all of them; without it weir"
  echo "sees 4 cores and warns that \`shard_count=4\` is over-provisioned."
  echo
  echo "**Device-mapper modules loaded:** $(lsmod | grep -cE '^dm_(flakey|delay)') of 2"
  echo "(\`dm-flakey\`, \`dm-delay\` — unused by Phase 1, loaded so the box is ready"
  echo "for Phase 2; not reboot-persistent)."
  echo
  echo "## Stop mechanism"
  echo
  echo "\`max_duration_secs\` in the schedule stops the run BETWEEN episodes, so"
  echo "it always ends in a verified state and still runs the final pass, writes"
  echo "\`report.md\`, and tears down the loop device and mount. A \`timeout -s INT\`"
  echo "backstop sits an hour later and only matters if the run hangs."
  echo
  echo "## Schedule"
  echo
  echo "\`$SCHEDULE\`, reproduced verbatim:"
  echo
  echo '```toml'
  cat "$SCHEDULE"
  echo '```'
  echo
  echo "## Files in this directory"
  echo
  echo "| File | What it is |"
  echo "|---|---|"
  echo "| \`report.md\` | rendered result — episodes, totals, final pass |"
  echo "| \`episodes.jsonl\` | one JSON record per episode; the source of truth |"
  echo "| \`ledger.log\` | loadgen's record of what weir ACKED — oracle input (I1/I2) |"
  echo "| \`delivered.log\` | recorder's record of what arrived — oracle input (I1/I2) |"
  echo "| \`weir-server.log\` | the daemon under test |"
  echo "| \`loadgen.log\` / \`recorder.log\` | observer stderr |"
  echo
  echo "\`ledger.log\` and \`delivered.log\` ARE the oracle — I1 and I2 are"
  echo "set-containment checks over exactly those two files. They are large"
  echo "(~1.5 GB per 10h) and must never be trimmed to save space."
} > "$OUT"

sudo -A cp "$OUT" "$RUN_DIR/SETUP.md" 2>/dev/null || cp "$OUT" "$RUN_DIR/SETUP.md"
rm -f "$OUT"
echo "capture-setup: wrote $RUN_DIR/SETUP.md"
