#!/usr/bin/env bash
# One cargo run in a gate slot, for agents' single runs outside the gates:
#   bash .temper/gate-run.sh <cargo args>     e.g. test -p weir-wab segment
# Same slot, target dir, compiler cache and freshness touch as the gates; the
# output goes to the terminal and to the run's log dir.
set -uo pipefail
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
[ $# -gt 0 ] || { echo "gate-run: give cargo's arguments, e.g. test -p weir-wab" >&2; exit 2; }
top=$(git rev-parse --show-toplevel)
GATE_NAME=run
. "$top/.temper/gate-slot.sh"
printf 'cargo %s\n' "$*" >> "$OUT/summary.txt"
(cd "$top" && timeout -k 30 "$STEP_TIMEOUT" cargo "$@") 9>&- 2>&1 | tee "$OUT/run.log"
rc=${PIPESTATUS[0]}
echo "GATE run rc=$rc" | tee -a "$OUT/summary.txt"
exit "$rc"
