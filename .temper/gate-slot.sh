# Sourced by gate-quick.sh, gate-full.sh and gate-run.sh (bash, with $top set to
# the worktree root). Takes a free gate slot and sets:
#   T    the slot dir (its own lock, cargo target dir and test tmp dir)
#   OUT  this run's log dir (summary.txt plus one log per step)
# and defines step/finish for the gates.
#
# Nothing machine-specific lives here. A machine sets its paths in an env file,
# read when present: $WEIR_GATE_ENV, default ~/.config/weir/gate.env.
#   WEIR_GATE_SLOTS        space-separated slot dirs, tried in order
#   WEIR_GATE_TMP          root for tests' scratch dirs (keep it on fast storage)
#   WEIR_GATE_LOGS         where run logs go
#   WEIR_GATE_JOBS         cargo build jobs and test threads (default 4)
#   WEIR_GATE_MIN_FREE_GB  refuse to start below this much free disk in the slot (default 20)
#   WEIR_GATE_STEP_TIMEOUT seconds per step (default 3600)
#   WEIR_ACT               the act binary (default: act on PATH)
#   SCCACHE_DIR, SCCACHE_CACHE_SIZE  used when sccache is installed
WEIR_GATE_ENV=${WEIR_GATE_ENV:-$HOME/.config/weir/gate.env}
# shellcheck disable=SC1090
[ -f "$WEIR_GATE_ENV" ] && . "$WEIR_GATE_ENV"
gate_base=${XDG_CACHE_HOME:-$HOME/.cache}/weir-gate
SLOTS=${WEIR_GATE_SLOTS:-$gate_base/slot-a}
JOBS=${WEIR_GATE_JOBS:-4}
STEP_TIMEOUT=${WEIR_GATE_STEP_TIMEOUT:-3600}

T=
echo "gate: waiting for a gate slot" >&2
while [ -z "$T" ]; do
  for s in $SLOTS; do
    mkdir -p "$s" || continue
    exec 9>"$s/gate.lock"
    if flock -n 9; then T=$s; break; fi
    exec 9>&-
  done
  [ -n "$T" ] || sleep 10
done
echo "gate: slot $T" >&2

free_gb=$(df -Pk "$T" | awk 'NR==2 { print int($4 / 1048576) }')
if [ "$free_gb" -lt "${WEIR_GATE_MIN_FREE_GB:-20}" ]; then
  echo "gate: only ${free_gb} GB free under $T (floor ${WEIR_GATE_MIN_FREE_GB:-20} GB); clear your own leftovers first" >&2
  exit 1
fi

export CARGO_TARGET_DIR="$T/target" CARGO_BUILD_JOBS=$JOBS RUST_TEST_THREADS=$JOBS CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=line-tables-only CARGO_TERM_COLOR=never PYTHONDONTWRITEBYTECODE=1
if command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER=sccache SCCACHE_DIR=${SCCACHE_DIR:-$gate_base/sccache} SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE:-25G}
fi
# mold links when installed. Exported so the per-step `bash -c` sees it.
if command -v mold >/dev/null 2>&1; then cargo() { mold -run cargo "$@"; }; export -f cargo; fi

# Tests' scratch dir (CARGO_TARGET_TMPDIR is <target dir>/tmp) lives under
# WEIR_GATE_TMP, one per slot, so tests of two gates never share one.
tmp_root=${WEIR_GATE_TMP:-$gate_base/tmp}/$(basename "$T")
mkdir -p "$CARGO_TARGET_DIR" "$tmp_root/cargo"
[ -L "$CARGO_TARGET_DIR/tmp" ] || { rm -rf "$CARGO_TARGET_DIR/tmp"; ln -s "$tmp_root/cargo" "$CARGO_TARGET_DIR/tmp"; }

# Cargo calls a workspace crate fresh when its sources are older than its last
# build, from any worktree, so a slot last used by another worktree could test
# that worktree's code. The slot records the worktree that last built in it, and
# this worktree's tracked sources are touched only when that changed.
stamp="$T/.built-from"
if [ "$(cat "$stamp" 2>/dev/null)" != "$top" ]; then
  git -C "$top" ls-files -z -- crates Cargo.toml Cargo.lock | (cd "$top" && xargs -0 touch)
  printf '%s\n' "$top" > "$stamp"
fi

# Logs: <logs>/<worktree>/<gate>-<UTC>/, with <logs>/<worktree>/latest-<gate>
# pointing at the newest run.
gate_logs=${WEIR_GATE_LOGS:-$gate_base/logs}/$(basename "$top")
OUT="$gate_logs/$GATE_NAME-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
ln -sfn "$OUT" "$gate_logs/latest-$GATE_NAME"
echo "gate: $GATE_NAME at $(git -C "$top" rev-parse --short HEAD) ($(git -C "$top" rev-parse --abbrev-ref HEAD)), logs $OUT" | tee "$OUT/summary.txt"
[ -z "$(git -C "$top" status --porcelain)" ] || echo "gate: note: the worktree has uncommitted changes; steps that run in a clean clone test HEAD only" | tee -a "$OUT/summary.txt"

FAILED=0
# step <name> <command>: runs it under timeout with the slot lock closed (so
# nothing it leaves running holds the slot), logs it, and records rc and time.
# STOP_ON_FAIL=1 ends the gate at the first failure.
step() {
  local name=$1 cmd=$2 t0=$SECONDS rc=0
  (cd "$top" && timeout -k 30 "$STEP_TIMEOUT" bash -c "$cmd") >"$OUT/$name.log" 2>&1 9>&- || rc=$?
  printf '%-30s rc=%d  %ds\n' "$name" "$rc" $((SECONDS - t0)) | tee -a "$OUT/summary.txt"
  if [ "$rc" -ne 0 ]; then
    FAILED=$((FAILED + 1))
    tail -40 "$OUT/$name.log" | sed 's/^/    /'
    [ "${STOP_ON_FAIL:-0}" = 1 ] && finish
  fi
  return 0
}
# The last line of summary.txt is always `GATE <name> rc=<0|1>`: poll for it.
finish() {
  local rc=0
  [ "$FAILED" -eq 0 ] || rc=1
  echo "GATE $GATE_NAME rc=$rc ($FAILED failed)" | tee -a "$OUT/summary.txt"
  exit "$rc"
}
