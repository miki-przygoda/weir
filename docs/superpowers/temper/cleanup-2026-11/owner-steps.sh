#!/usr/bin/env bash
# Owner evidence steps for epic cleanup-2026-11 (branch epic/cleanup-2026-11).
#
# FOR THE OWNER ONLY. Agents never run any of this: the chaos campaign needs
# root, the macOS steps need the Mac, and the sink checks start real database
# containers and a daemon. Nothing here pushes, merges or deletes a branch.
# Read docs/superpowers/temper/cleanup-2026-11/owner-checklist.md first; it says
# what each step proves, how long it takes and what to paste back.
#
#   bash owner-steps.sh <step> [args]
#
# On beast (the build host):
#   chaos-setup                  detached worktree at the epic tip + release builds
#   chaos-campaign               the four Foundations lifecycle schedules, then gate.py
#                                (root; run it inside your own tmux session, ~12-24 h)
#   chaos-gate RUN_DIR...        re-run gate.py on finished run dirs
#   chaos-durable                optional: powerloss-durable capped at 6 h (root)
#   sinks-setup                  a second detached worktree + a release build with
#                                the clickhouse and s3 sinks (dedup-check, sink-check)
#   dedup-check [case...]        the new batch key and RecordId against real servers in
#                                weir's docker test rig and a local idempotent HTTP
#                                endpoint. Cases: ch-heartbeat http-ndjson-heartbeat
#                                s3-heartbeat ch-restart pg-restart mysql-restart
#                                http-record-restart (default: all)
#   baseline-setup               optional: the same build at the merge base with main,
#                                so dedup-check --baseline shows the checks catch the bug
#   dedup-check --baseline [case...]   the same cases against that baseline build
#   sink-check [case...]         optional: reclassified sink errors against real servers.
#                                Cases: pg-25006 mysql-1203 mysql-1040 s3-region
#                                ch-redact (default: all)
# On the Mac:
#   mac-tests LOCAL_WORKTREE     native macOS unit tests (mac-tests.sh)
#   darwin-smoke                 TLS smoke of the full gate's apple-darwin binaries
#
# Environment (all optional):
#   EPIC_REF      what to test (default epic/cleanup-2026-11, resolved in the clone)
#   EVIDENCE_DIR  where logs and results go (default ~/weir-evidence/cleanup-2026-11)
#   SLOT          darwin-smoke: the gate slot (a-d) whose build to use (default: the
#                 slot the newest full gate at this sha ran in)
#
# Bash 3.2 compatible on purpose: the Mac steps run under /bin/bash.
set -uo pipefail

EPIC=cleanup-2026-11
EPIC_REF=${EPIC_REF:-epic/$EPIC}
CLONE=/srv/bulk/Projects/weir/src
WORK=$HOME/work/weir/$EPIC
CHAOS_WT=$WORK/owner-chaos
SINKS_WT=$WORK/owner-sinks
BASE_WT=$WORK/owner-baseline
BIN=$SINKS_WT/target/release
EVIDENCE_DIR=${EVIDENCE_DIR:-$HOME/weir-evidence/$EPIC}
GATE_SCHEDULES="shutdown-bounded shutdown-forced startup-signal pause-resume-cut"
MAC_REPO=/Users/miki_przygoda/Projects/GitHub-Projects/weir
MAC_TESTS=$MAC_REPO/.workdocs/operability-2026-10-05/mac-tests.sh

say()  { printf '[owner] %s\n' "$*"; }
die()  { printf '[owner] ERROR: %s\n' "$*" >&2; exit 1; }
need_linux() { [ "$(uname -s)" = Linux ] || die "run '$1' on beast (Linux), not on $(uname -s)"; }
need_mac()   { [ "$(uname -s)" = Darwin ] || die "run '$1' on the Mac, not on $(uname -s)"; }
stamp() { date -u +%Y%m%dT%H%M%SZ; }

# The sha under test, resolved in beast's clone so every step names the same one.
resolve_sha() {
  git -C "$CLONE" rev-parse --verify --quiet "$EPIC_REF^{commit}" \
    || die "cannot resolve $EPIC_REF in $CLONE (did the head bring the branch here?)"
}

# make_worktree <dir> [sha]: a DETACHED worktree at the epic tip (or at sha).
# Detached, so the epic branch stays checked out in no worktree and the head can
# still move it. An existing worktree is reused only when it is clean; it is
# moved to the sha.
make_worktree() {
  local dir=$1 sha=${2:-}
  [ -n "$sha" ] || sha=$(resolve_sha)
  if [ -e "$dir/.git" ]; then
    [ -z "$(git -C "$dir" status --porcelain --untracked-files=no)" ] \
      || die "$dir has local changes; clean it up by hand first"
    git -C "$dir" switch --detach -q "$sha" || die "could not move $dir to $sha"
  else
    mkdir -p "$(dirname "$dir")"
    git -C "$CLONE" worktree add --detach "$dir" "$sha" || die "git worktree add $dir failed"
  fi
  say "$dir at $(git -C "$dir" rev-parse --short HEAD)"
}

check_quiet_host() {
  if [ -x "$HOME/.local/bin/jobq" ]; then
    say "jobq right now (a full gate or other heavy job running alongside skews a chaos run):"
    "$HOME/.local/bin/jobq" list 2>/dev/null | head -15 || true
  fi
  say "free space on / and under $WORK:"
  df -h / "$(cd "$HOME/work" && pwd -P)" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# chaos: the Foundations lifecycle power-cut campaign (needs root)
# ---------------------------------------------------------------------------
cmd_chaos_setup() {
  need_linux chaos-setup
  check_quiet_host
  make_worktree "$CHAOS_WT"
  mkdir -p "$EVIDENCE_DIR"
  # run.py runs <repo>/target/release/weir-server and chaos/target/release/*.
  # startup-signal.toml needs the test hooks (inert without WEIR_TEST_HOOKS=1).
  # -j 4: beast is shared; this keeps the build off other people's memory.
  ( cd "$CHAOS_WT" && cargo build --release -j 4 -p weir-server --features test-hooks ) \
    > "$EVIDENCE_DIR/chaos-build-weir.log" 2>&1 || die "weir-server build failed: $EVIDENCE_DIR/chaos-build-weir.log"
  ( cd "$CHAOS_WT/chaos" && cargo build --release -j 4 ) \
    > "$EVIDENCE_DIR/chaos-build-harness.log" 2>&1 || die "chaos build failed: $EVIDENCE_DIR/chaos-build-harness.log"
  ( cd "$CHAOS_WT/chaos/orchestrator" && python3 -m unittest discover -p 'test_*.py' ) \
    > "$EVIDENCE_DIR/chaos-oracle-unittests.log" 2>&1 || die "oracle unit tests failed: $EVIDENCE_DIR/chaos-oracle-unittests.log"
  sha256sum "$CHAOS_WT/target/release/weir-server" | tee "$EVIDENCE_DIR/chaos-weir-server.sha256"
  say "ready. Next, inside your own tmux session on beast:"
  say "  tmux new -s weir-chaos"
  say "  bash $CHAOS_WT/docs/superpowers/temper/$EPIC/owner-steps.sh chaos-campaign"
}

# run_id <schedule.toml>: run.py's own run id for a schedule (it derives it
# from the seed), so the run dir is known without guessing by mtime.
run_id() {
  ( cd "$CHAOS_WT/chaos" && python3 - "$1" <<'PY'
import sys, tomllib
sys.path.insert(0, "orchestrator")
import run
print(run.run_id_from_seed(tomllib.load(open(sys.argv[1], "rb"))["seed"]))
PY
  )
}

# Keeps the sudo timestamp warm for a long campaign, from this tty.
SUDO_KEEPER=
sudo_warm() {
  sudo -v || die "sudo is needed for the chaos harness (loop devices, dm-flakey, mounts)"
  ( while sleep 60; do sudo -n -v 2>/dev/null || exit 0; done ) &
  SUDO_KEEPER=$!
  trap '[ -n "$SUDO_KEEPER" ] && kill "$SUDO_KEEPER" 2>/dev/null' EXIT
}

# run_schedule <schedule.toml> <label>: one run.py run, as chaos/run-soak.sh
# does it (SETUP.md captured beside the run, console log kept with it), but
# with plain sudo from this tty instead of sudo -A, and no backstop SIGINT:
# every gate schedule stops itself at max_duration_secs between episodes and
# still does its final pass.
run_schedule() {
  local sched=$1 label=$2 id rc log ncpu
  id=$(run_id "$sched") || die "could not derive the run id for $sched"
  [ -e "$CHAOS_WT/chaos/runs/$id" ] && die "chaos/runs/$id already exists: a schedule's run id comes from its seed, so move the old run aside first"
  log="$EVIDENCE_DIR/chaos-$label.console.log"
  ncpu=$(nproc --all)
  say "$(date -Is) start $label ($sched), run dir chaos/runs/$id, console $log"
  # As root (sudo -n: the timestamp is warm), because run.py creates the run
  # dir as root and capture-setup.sh copies SETUP.md into it.
  ( cd "$CHAOS_WT/chaos" && sudo -n ./capture-setup.sh "$sched" "$label" ) > "$EVIDENCE_DIR/chaos-$label.setup.log" 2>&1 &
  # taskset: isolcpus makes nproc report fewer cores than the box has, and
  # weir then warns shard_count=4 is over-provisioned (run-soak.sh does the same).
  ( cd "$CHAOS_WT/chaos" && systemd-inhibit --what=sleep:idle --why="weir chaos $label" \
      sudo taskset -c "0-$((ncpu - 1))" python3 -u orchestrator/run.py "$sched" ) 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  sudo cp "$log" "$CHAOS_WT/chaos/runs/$id/console.log" 2>/dev/null || true
  say "$(date -Is) end $label rc=$rc (0 passed; 1 violations or anomalies: read chaos/runs/$id/report.md)"
  echo "$label $id rc=$rc" >> "$EVIDENCE_DIR/chaos-runs.txt"
  return "$rc"
}

cmd_chaos_campaign() {
  need_linux chaos-campaign
  [ -x "$CHAOS_WT/target/release/weir-server" ] || die "run chaos-setup first"
  [ -n "${TMUX:-}" ] || say "WARNING: not inside tmux; an ssh drop ends the campaign. Ctrl-C now and start 'tmux new -s weir-chaos'."
  [ "$(git -C "$CHAOS_WT" rev-parse HEAD)" = "$(resolve_sha)" ] \
    || say "WARNING: $CHAOS_WT is not at the current $EPIC_REF tip; re-run chaos-setup if the branch moved"
  mkdir -p "$EVIDENCE_DIR"
  check_quiet_host
  sudo_warm
  sudo modprobe dm-flakey dm-delay || die "modprobe dm-flakey dm-delay failed"
  local s dirs="" rcs=""
  for s in $GATE_SCHEDULES; do
    run_schedule "schedules/$s.toml" "$EPIC-$s"
    rcs="$rcs $s=$?"
    dirs="$dirs runs/$(run_id "schedules/$s.toml")"
  done
  say "schedules:$rcs"
  # shellcheck disable=SC2086
  cmd_chaos_gate $dirs
}

cmd_chaos_gate() {
  need_linux chaos-gate
  [ $# -gt 0 ] || die "usage: chaos-gate runs/<id> runs/<id> runs/<id> runs/<id> [--allow-anomalies]"
  mkdir -p "$EVIDENCE_DIR"
  local out rc
  out="$EVIDENCE_DIR/chaos-gate-$(stamp).txt"
  ( cd "$CHAOS_WT/chaos" && python3 orchestrator/gate.py "$@" ) 2>&1 | tee "$out"
  rc=${PIPESTATUS[0]}
  echo "GATE chaos rc=$rc" | tee -a "$out"
  say "gate verdict saved to $out (paste it into the PR's evidence section)"
  return "$rc"
}

cmd_chaos_durable() {
  need_linux chaos-durable
  [ -x "$CHAOS_WT/target/release/weir-server" ] || die "run chaos-setup first"
  # The committed schedule runs 24 h. A copy capped at 6 h goes into
  # chaos/schedules/generated/ (gitignored), so nothing tracked changes and
  # the run still ends with its own final pass.
  local gen="$CHAOS_WT/chaos/schedules/generated/$EPIC-powerloss-durable-6h.toml"
  mkdir -p "$(dirname "$gen")"
  sed 's/^max_duration_secs = .*/max_duration_secs = 21600  # capped at 6 h for the cleanup-2026-11 evidence run/' \
    "$CHAOS_WT/chaos/schedules/powerloss-durable.toml" > "$gen"
  grep -q '^max_duration_secs = 21600' "$gen" || die "could not cap max_duration_secs in $gen"
  mkdir -p "$EVIDENCE_DIR"
  sudo_warm
  sudo modprobe dm-flakey dm-delay || die "modprobe dm-flakey dm-delay failed"
  run_schedule "schedules/generated/$(basename "$gen")" "$EPIC-powerloss-durable-6h"
}

# ---------------------------------------------------------------------------
# sinks: shared by dedup-check and sink-check (weir's docker test rig)
# ---------------------------------------------------------------------------
COMPOSE_BASE=deploy/docker/test/docker-compose.yml
SC_DIR=
SC_PID=
SC_PORT=19386
SC_FAILS=0
SC_RESULTS=

cmd_sinks_setup() {
  need_linux sinks-setup
  check_quiet_host
  make_worktree "$SINKS_WT"
  mkdir -p "$EVIDENCE_DIR"
  ( cd "$SINKS_WT" && cargo build --release -j 4 -p weir-server -p weir-ctl \
      --features weir-server/clickhouse-sink,weir-server/s3-sink ) \
    > "$EVIDENCE_DIR/sinks-build.log" 2>&1 || die "build failed: $EVIDENCE_DIR/sinks-build.log"
  say "built $SINKS_WT/target/release/{weir-server,weir-ctl}. Next: bash $0 dedup-check"
}

# The rig's compose file, plus SC_DIR/override.yml when a step writes one.
compose() {
  if [ -f "$SC_DIR/override.yml" ]; then
    ( cd "$SINKS_WT" && docker compose -f "$COMPOSE_BASE" -f "$SC_DIR/override.yml" "$@" )
  else
    ( cd "$SINKS_WT" && docker compose -f "$COMPOSE_BASE" "$@" )
  fi
}

sc_wait_healthy() {
  local svc=$1 i st
  for i in $(seq 1 120); do
    st=$(docker inspect -f '{{.State.Health.Status}}' "weir-test-$svc" 2>/dev/null || echo starting)
    [ "$st" = healthy ] && return 0
    [ "$st" = unhealthy ] && die "weir-test-$svc is unhealthy (docker logs weir-test-$svc)"
    sleep 2
  done
  die "weir-test-$svc not healthy after 240 s"
}

mysql_root() { docker exec weir-test-mysql mysql -uroot -ptest -N -B -e "$1" 2>/dev/null; }
# Under max_connections = 1, root has a single admin slot that the rig's
# healthcheck (mysqladmin ping as root) also takes, so retry a statement that
# must land.
mysql_retry() {
  local i; for i in $(seq 1 20); do mysql_root "$1" >/dev/null && return 0; sleep 0.5; done
  say "WARNING: could not run on mysql: $1"; return 1
}
psql_root()  { docker exec weir-test-postgres psql -U postgres -d weir_test -tAq -c "$1"; }

# start_weir <case> <extra toml lines...>: a daemon on a fresh wab_dir. sink_url
# is passed as WEIR_SINK_URL by the caller when the case has one, never argv.
start_weir() {
  local case=$1; shift
  local d="$SC_DIR/$case"
  mkdir -p "$d"; chmod 700 "$d"
  {
    echo '[server]'
    echo "socket_path = \"$d/weir.sock\""
    echo "wab_dir = \"$d/wab\""
    echo "metrics_port = $SC_PORT"
    echo 'shard_count = 1'
    echo 'wab_segment_max_age_secs = 1'
    echo 'health_poll_interval_secs = 2'
    echo 'sink_max_retries = 3'
    echo 'sink_retry_base_delay_ms = 100'
    echo 'drain_shutdown_budget_secs = 5'
    echo 'log_level = "info"'
    local l; for l in "$@"; do echo "$l"; done
  } > "$d/weir.toml"
  "$BIN/weir-server" --config "$d/weir.toml" >> "$d/weir.log" 2>&1 &
  SC_PID=$!
  local i
  for i in $(seq 1 75); do
    [ -S "$d/weir.sock" ] && return 0
    kill -0 "$SC_PID" 2>/dev/null || { tail -20 "$d/weir.log"; return 1; }
    sleep 0.2
  done
  tail -20 "$d/weir.log"; return 1
}

stop_weir() {
  [ -n "$SC_PID" ] || return 0
  kill -TERM "$SC_PID" 2>/dev/null
  local i; for i in $(seq 1 60); do kill -0 "$SC_PID" 2>/dev/null || break; sleep 0.5; done
  kill -KILL "$SC_PID" 2>/dev/null
  wait "$SC_PID" 2>/dev/null
  SC_PID=
}

push_n() {  # push_n <case> <n>: distinct payloads owner-<case>-<i>, Durable
  local case=$1 n=$2 i
  for i in $(seq 1 "$n"); do
    "$BIN/weir-ctl" push "owner-$case-$i" --durability durable \
      --socket "$SC_DIR/$case/weir.sock" >/dev/null || return 1
  done
}

metric_sum() {  # metric_sum <name>: the sum of every sample of that metric
  curl -sf "http://127.0.0.1:$SC_PORT/metrics" \
    | awk -v m="$1" '$1 == m || index($1, m "{") == 1 { s += $NF } END { print s + 0 }'
}

# wait_for <secs> <shell condition>: polls once a second.
wait_for() {
  local secs=$1 i; shift
  for i in $(seq 1 "$secs"); do eval "$*" && return 0; sleep 1; done
  return 1
}

stranded_gt0() { [ "$(metric_sum weir_drain_segments_stranded_total)" != 0 ]; }
dl_bytes()     { metric_sum weir_dead_letter_bytes_on_disk; }

record() {  # record <case> <PASS|FAIL|INCONCLUSIVE> <why>
  SC_RESULTS="$SC_RESULTS
$1 $2 $3"
  [ "$2" = FAIL ] && SC_FAILS=$((SC_FAILS + 1))
  say "$1: $2 ($3)"
}

# evidence <case>: what a reader needs to check the verdict.
evidence() {
  local d="$SC_DIR/$1"
  {
    echo "== $1 metrics"
    curl -sf "http://127.0.0.1:$SC_PORT/metrics" | grep -E '^weir_(drain_segments_stranded_total|dead_letter_bytes_on_disk|sink_health)' || true
    echo "== $1 weir-ctl dl list"
    "$BIN/weir-ctl" dl list --wab-dir "$d/wab" 2>&1 || true
  } >> "$d/evidence.txt"
}

# The common shape: inject the fault, push, see a strand and no dead letter,
# clear the fault, see every record arrive.
#   strand_case <case> <n> <inject-fn> <clear-fn> <count-fn> <toml lines...>
strand_case() {
  local case=$1 n=$2 inject=$3 clear=$4 count=$5; shift 5
  say "--- $case"
  $inject || { record "$case" FAIL "could not inject the fault"; return; }
  if ! start_weir "$case" "$@"; then
    record "$case" FAIL "daemon did not start (see $SC_DIR/$case/weir.log)"; $clear; return
  fi
  push_n "$case" "$n" || { record "$case" FAIL "push failed"; stop_weir; $clear; return; }
  if ! wait_for 60 stranded_gt0; then
    evidence "$case"; stop_weir; $clear
    record "$case" FAIL "no strand within 60 s (dead-letter bytes $(dl_bytes))"; return
  fi
  local dl; dl=$(dl_bytes)
  evidence "$case"
  $clear
  if wait_for 90 "[ \"\$($count)\" -ge $n ]"; then
    if [ "$dl" = 0 ] && [ "$(dl_bytes)" = 0 ]; then
      record "$case" PASS "stranded, nothing dead-lettered, all $n delivered after the fault cleared"
    else
      record "$case" FAIL "delivered, but dead-letter bytes were $dl then $(dl_bytes)"
    fi
  else
    record "$case" FAIL "stranded, but only $($count) of $n arrived within 90 s of clearing the fault"
  fi
  evidence "$case"; stop_weir
}

# --- Postgres 25006 (sinks-r3-05, 05b1d00): a trigger raises the real SQLSTATE
# from the server, which is the error path the classification sees; the
# read-only standby variants are in the checklist.
pg_inject() {
  psql_root "CREATE OR REPLACE FUNCTION weir_owner_ro() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RAISE EXCEPTION 'owner check: read only' USING ERRCODE = '25006'; END \$\$;
             DROP TRIGGER IF EXISTS weir_owner_ro ON weir_records;
             CREATE TRIGGER weir_owner_ro BEFORE INSERT ON weir_records FOR EACH ROW EXECUTE FUNCTION weir_owner_ro();"
}
pg_clear() { psql_root "DROP TRIGGER IF EXISTS weir_owner_ro ON weir_records;"; }
pg_count() { psql_root "SELECT count(*) FROM weir_records WHERE convert_from(payload, 'UTF8') LIKE 'owner-pg-25006-%';"; }
case_pg_25006() {
  export WEIR_SINK_URL="postgres://postgres:test@127.0.0.1:55432/weir_test"
  strand_case pg-25006 20 pg_inject pg_clear pg_count 'sink_type = "postgres"'
  unset WEIR_SINK_URL
}

# --- MySQL 1203 (sinks-r3-06, ade74f3): a user capped at one connection,
# which another session already holds.
mysql_hold() {  # mysql_hold <user> <password>: one idle session, held 10 min
  docker exec -d weir-test-mysql mysql -u"$1" -p"$2" -h127.0.0.1 -e "SELECT SLEEP(600)"
  wait_for 15 "[ \"\$(mysql_root \"SELECT COUNT(*) FROM information_schema.processlist WHERE user='$1'\")\" -ge 1 ]"
}
mysql_release() {  # kills every session of that user (the holder only: weir's attempts are refused)
  local id
  for id in $(mysql_root "SELECT id FROM information_schema.processlist WHERE user='$1'"); do
    mysql_root "KILL $id" || true
  done
}
m1203_inject() {
  mysql_root "DROP USER IF EXISTS 'weir_lim'@'%';
              CREATE USER 'weir_lim'@'%' IDENTIFIED BY 'lim' WITH MAX_USER_CONNECTIONS 1;
              GRANT SELECT, INSERT ON weir_test.* TO 'weir_lim'@'%';" && mysql_hold weir_lim lim
}
m1203_clear() { mysql_retry "ALTER USER 'weir_lim'@'%' WITH MAX_USER_CONNECTIONS 0"; mysql_release weir_lim; }
m_count() { mysql_root "SELECT COUNT(*) FROM weir_test.weir_records WHERE payload LIKE 'owner-$1-%'"; }
m1203_count() { m_count mysql-1203; }
case_mysql_1203() {
  export WEIR_SINK_URL="mysql://weir_lim:lim@127.0.0.1:33306/weir_test"
  strand_case mysql-1203 20 m1203_inject m1203_clear m1203_count 'sink_type = "mysql"'
  unset WEIR_SINK_URL
  mysql_release weir_lim; mysql_root "DROP USER IF EXISTS 'weir_lim'@'%'" || true
}

# --- MySQL 1040: max_connections = 1, held by another non-admin user. root
# keeps its CONNECTION_ADMIN slot, so the checks below still get in.
m1040_inject() {
  mysql_root "DROP USER IF EXISTS 'weir_hold'@'%'; DROP USER IF EXISTS 'weir_c'@'%';
              CREATE USER 'weir_hold'@'%' IDENTIFIED BY 'hold';
              CREATE USER 'weir_c'@'%' IDENTIFIED BY 'c';
              GRANT SELECT, INSERT ON weir_test.* TO 'weir_c'@'%';
              SET GLOBAL max_connections = 1;" && mysql_hold weir_hold hold
}
m1040_clear() { mysql_retry "SET GLOBAL max_connections = 151"; mysql_release weir_hold; }
m1040_count() { m_count mysql-1040; }
case_mysql_1040() {
  export WEIR_SINK_URL="mysql://weir_c:c@127.0.0.1:33306/weir_test"
  strand_case mysql-1040 20 m1040_inject m1040_clear m1040_count 'sink_type = "mysql"'
  unset WEIR_SINK_URL
  mysql_retry "SET GLOBAL max_connections = 151"
  mysql_release weir_hold
  mysql_root "DROP USER IF EXISTS 'weir_hold'@'%'; DROP USER IF EXISTS 'weir_c'@'%'" || true
}

# --- S3 400 AuthorizationHeaderMalformed (sinks-r3-04, ed290f8). The rig's
# MinIO runs with MINIO_SITE_REGION=eu-west-1 (the override file), and the
# daemon signs for us-east-1. Config is not reloaded, so the fault clears by
# restarting on the SAME wab_dir with the right region: the stranded segments
# re-drain at startup.
s3_count() {
  docker exec weir-test-minio mc ls --recursive "local/weir-test/owner-check-$SC_RUN/" 2>/dev/null | wc -l
}
case_s3_region() {
  local case=s3-region
  say "--- $case"
  local common='sink_type = "s3"
sink_s3_bucket = "weir-test"
sink_s3_endpoint = "http://127.0.0.1:19000"
sink_s3_force_path_style = true
sink_s3_compression = "none"'
  common="$common
sink_s3_prefix = \"owner-check-$SC_RUN\""
  # The rig's own credentials (deploy/run-sink-integration-tests.sh); read from
  # the environment by the sink, as in a deployment.
  local init_rc
  init_rc=$(docker wait weir-test-minio-init 2>/dev/null || echo missing)
  if [ "$init_rc" != 0 ]; then
    record "$case" INCONCLUSIVE "minio-init exited $init_rc under the region override, so the bucket may not exist"; return
  fi
  export AWS_ACCESS_KEY_ID=weirtest AWS_SECRET_ACCESS_KEY=weirtestsecret
  if ! start_weir "$case" "$common" 'sink_s3_region = "us-east-1"'; then
    # Refusing to start on a wrong region dead-letters nothing either, but it
    # is not the drain path this fix changed.
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    record "$case" INCONCLUSIVE "the daemon refused to start with the wrong region (see $SC_DIR/$case/weir.log)"; return
  fi
  push_n "$case" 20 || { record "$case" FAIL "push failed"; stop_weir; return; }
  if ! wait_for 60 stranded_gt0; then
    evidence "$case"; stop_weir
    if [ "$(s3_count)" -gt 0 ]; then
      record "$case" INCONCLUSIVE "MinIO accepted the wrong region (MINIO_SITE_REGION not honoured by this image); use the real-AWS variant in the checklist"
    else
      record "$case" FAIL "no strand and nothing delivered within 60 s (dead-letter bytes $(dl_bytes))"
    fi
    return
  fi
  local dl; dl=$(dl_bytes)
  grep -h -o 'AuthorizationHeaderMalformed' "$SC_DIR/$case/weir.log" | head -1 > "$SC_DIR/$case/s3-code.txt"
  evidence "$case"; stop_weir
  # Restart on the same wab_dir with the right region.
  sed -i 's/^sink_s3_region = .*/sink_s3_region = "eu-west-1"/' "$SC_DIR/$case/weir.toml"
  "$BIN/weir-server" --config "$SC_DIR/$case/weir.toml" >> "$SC_DIR/$case/weir.log" 2>&1 &
  SC_PID=$!
  if wait_for 90 "[ \$(s3_count) -gt 0 ]" && [ "$dl" = 0 ] && [ "$(dl_bytes)" = 0 ]; then
    if [ -s "$SC_DIR/$case/s3-code.txt" ]; then
      record "$case" PASS "stranded on AuthorizationHeaderMalformed, nothing dead-lettered, delivered after a restart with the right region"
    else
      record "$case" INCONCLUSIVE "stranded and later delivered, but the log never names AuthorizationHeaderMalformed, so the strand may have another cause (read $SC_DIR/$case/weir.log)"
    fi
  else
    record "$case" FAIL "dead-letter bytes $dl at strand; objects after restart: $(s3_count)"
  fi
  evidence "$case"; stop_weir
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
}

# --- ClickHouse transport-error redaction (sinks-r3-03, 126e8a3): a sink URL
# with a ?password= canary pointed at a port nothing listens on, so every
# attempt is a transport error. The canary must not appear in the log,
# /status or weir-ctl status.
case_ch_redact() {
  local case=ch-redact canary
  say "--- $case"
  canary="ownercanary$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
  export WEIR_SINK_URL="http://127.0.0.1:18199/?user=default&password=$canary"
  if ! start_weir "$case" 'sink_type = "clickhouse"'; then
    unset WEIR_SINK_URL; record "$case" FAIL "daemon did not start"; return
  fi
  unset WEIR_SINK_URL
  push_n "$case" 5 || true
  wait_for 60 stranded_gt0 || true
  sleep 5   # at least two health polls, so the probe's error reaches /status
  curl -sf "http://127.0.0.1:$SC_PORT/status" > "$SC_DIR/$case/status.json" 2>&1 || true
  "$BIN/weir-ctl" status --wab-dir "$SC_DIR/$case/wab" \
    > "$SC_DIR/$case/weir-ctl-status.txt" 2>&1 || true
  evidence "$case"; stop_weir
  local hits retries
  hits=$(cat "$SC_DIR/$case/weir.log" "$SC_DIR/$case/status.json" "$SC_DIR/$case/weir-ctl-status.txt" \
           "$SC_DIR/$case/evidence.txt" | grep -c "$canary")
  retries=$(grep -c -i -E 'WARN|ERROR' "$SC_DIR/$case/weir.log")
  # Replace the canary on disk once counted: the files are evidence, and the
  # canary is a throwaway, but nothing that looks like a password stays around.
  sed -i "s/$canary/<canary>/g" "$SC_DIR/$case/"*.txt "$SC_DIR/$case/"*.json "$SC_DIR/$case/weir.log" 2>/dev/null
  if [ "$retries" -eq 0 ]; then
    record "$case" INCONCLUSIVE "no WARN/ERROR line: the transport-error path did not run"
  elif [ "$hits" -eq 0 ]; then
    record "$case" PASS "$retries WARN/ERROR lines, the password canary in none of log, /status, weir-ctl status"
  else
    record "$case" FAIL "the password canary appeared $hits time(s) (files under $SC_DIR/$case, canary replaced by <canary>)"
  fi
}

RIG_UP=
sc_cleanup() {
  stop_weir
  [ -n "$RIG_UP" ] && compose down -v --remove-orphans >/dev/null 2>&1
  RIG_UP=
  return 0
}

# rig_preflight <extra port...>: the rig's ports and ours must be free, and no
# weir-test-* container (running or stopped) or test_default network may be
# left from a gate: compose would reuse a stopped one against a deleted network.
rig_preflight() {
  command -v docker >/dev/null || die "docker not found"
  local p left
  for p in 33306 55432 18123 19000 "$@"; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      die "port $p is busy (a gate's sink-integration job, or the rig left up?); run this when it is free"
    fi
  done
  left=$(docker ps -a --filter name=weir-test- --format '{{.Names}} ({{.Status}})')
  [ -z "$left" ] || die "weir-test-* containers exist, likely a gate's sink-integration step is running or left them: $left"
  ! docker network inspect test_default >/dev/null 2>&1 \
    || die "a test_default docker network exists (a gate's sink-integration step?); run this when it is gone"
}

# rig_up: the rig, healthy, torn down (down -v) when the step exits.
rig_up() {
  trap sc_cleanup EXIT
  trap 'exit 130' INT TERM
  say "bringing up weir's test rig (weir-test-* containers) at $SINKS_WT/$COMPOSE_BASE"
  RIG_UP=1
  compose up -d >> "$SC_DIR/compose.log" 2>&1 || die "docker compose up failed: $SC_DIR/compose.log"
  local s
  for s in mysql postgres clickhouse minio; do sc_wait_healthy "$s"; done
}

cmd_sink_check() {
  need_linux sink-check
  [ -x "$BIN/weir-server" ] || die "run sinks-setup first"
  rig_preflight "$SC_PORT"
  SC_RUN=$(stamp)
  SC_DIR="$EVIDENCE_DIR/sink-check-$SC_RUN"
  mkdir -p "$SC_DIR"; chmod 700 "$SC_DIR"
  # Only the MinIO region differs from the committed rig, via an override file.
  printf 'services:\n  minio:\n    environment:\n      MINIO_SITE_REGION: eu-west-1\n' > "$SC_DIR/override.yml"
  rig_up
  local c cases
  cases=${*:-pg-25006 mysql-1203 mysql-1040 s3-region ch-redact}
  for c in $cases; do
    case $c in
      pg-25006) case_pg_25006 ;;
      mysql-1203) case_mysql_1203 ;;
      mysql-1040) case_mysql_1040 ;;
      s3-region) case_s3_region ;;
      ch-redact) case_ch_redact ;;
      *) record "$c" FAIL "unknown case" ;;
    esac
  done
  {
    echo "sink-check at $(git -C "$SINKS_WT" rev-parse --short HEAD), $(date -Is)"
    echo "$SC_RESULTS" | sed '/^$/d'
    echo "SINK CHECK failures=$SC_FAILS"
  } | tee "$SC_DIR/summary.txt"
  say "evidence under $SC_DIR (summary.txt, per-case weir.log and evidence.txt)"
  [ "$SC_FAILS" -eq 0 ]
}

# ---------------------------------------------------------------------------
# dedup: the new batch key (6679c8f) and RecordId (49c4188) against real servers
# ---------------------------------------------------------------------------
# Two shapes, each run against the real target:
#   heartbeat: one byte-identical record pushed three times, 3 s apart, so each
#     lands in its own timed-seal segment. Through 4.1.0 the ClickHouse token and
#     the NDJSON Idempotency-Key were a hash of the bytes, so the second and third
#     batch were dropped as duplicates while weir deleted them. PASS: 3 arrive.
#   restart: the record once, delivered, the daemon stopped with its WAB drained
#     empty, restarted on the same wab_dir (segment names restart at
#     seg_00000001), the same record again. Through 4.1.0 both got one RecordId,
#     so an id_column table or a per-record idempotent endpoint dropped the
#     second. PASS: 2 arrive under 2 distinct ids or keys.
# The HTTP target is a local idempotent endpoint (below): it stores a request's
# body once per Idempotency-Key and answers a repeated key with its cached 200,
# as a real one does.
DC_HTTP_PORT=19387
HTTPD_PID=
WATCH_PID=
DC_PAYLOAD=

dc_cleanup() {
  [ -n "$WATCH_PID" ] && kill "$WATCH_PID" 2>/dev/null
  [ -n "$HTTPD_PID" ] && kill "$HTTPD_PID" 2>/dev/null
  WATCH_PID=; HTTPD_PID=
  sc_cleanup
}

httpd_start() {
  cat > "$SC_DIR/idem_server.py" <<'PY'
import http.server, json, sys, threading
port, out = int(sys.argv[1]), sys.argv[2]
seen, lock = set(), threading.Lock()
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def ok(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_HEAD(self):
        self.ok()
    def do_GET(self):
        self.ok()
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        key = self.headers.get("Idempotency-Key") or ""
        with lock:
            replay = bool(key) and key in seen
            seen.add(key)
            with open(out, "a") as f:
                f.write(json.dumps({"path": self.path, "key": key, "replay": replay,
                                    "body": body.decode("utf-8", "replace")}) + "\n")
        self.ok()
http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
  python3 -I "$SC_DIR/idem_server.py" "$DC_HTTP_PORT" "$SC_DIR/http-requests.jsonl" \
    > "$SC_DIR/idem_server.log" 2>&1 &
  HTTPD_PID=$!
  wait_for 10 "curl -sf -I http://127.0.0.1:$DC_HTTP_PORT/ >/dev/null" \
    || die "the local idempotent endpoint did not start: $SC_DIR/idem_server.log"
}

# httpd_stat <case> <stored|keys|replays>: stored = records of DC_PAYLOAD the
# endpoint kept (replays excluded), keys = distinct Idempotency-Keys it saw,
# replays = requests it answered from its cache.
httpd_stat() {
  python3 -I - "$SC_DIR/http-requests.jsonl" "/$1" "$DC_PAYLOAD" "$2" <<'PY'
import json, sys
log, path, payload, what = sys.argv[1:5]
stored, keys, replays = 0, set(), 0
try:
    for line in open(log):
        r = json.loads(line)
        if r["path"] != path:
            continue
        keys.add(r["key"])
        if r["replay"]:
            replays += 1
        else:
            stored += sum(1 for l in r["body"].splitlines() if l == payload)
except FileNotFoundError:
    pass
print({"stored": stored, "keys": len(keys), "replays": replays}[what])
PY
}

ch_sql() { curl -sf "http://127.0.0.1:18123/" --data-binary "$1"; }

# Counters and id counts per target, for DC_PAYLOAD (only [a-z0-9-], so it is
# safe inside a quoted SQL literal).
cnt_ch()    { ch_sql "SELECT count() FROM default.weir_records WHERE payload = '$DC_PAYLOAD'"; }
cnt_pg()    { psql_root "SELECT count(*) FROM weir_records_keyed WHERE convert_from(payload, 'UTF8') = '$DC_PAYLOAD'"; }
ids_pg()    { psql_root "SELECT count(DISTINCT record_id) FROM weir_records_keyed WHERE convert_from(payload, 'UTF8') = '$DC_PAYLOAD'"; }
cnt_mysql() { mysql_root "SELECT COUNT(*) FROM weir_test.weir_records_keyed WHERE payload = '$DC_PAYLOAD'"; }
ids_mysql() { mysql_root "SELECT COUNT(DISTINCT record_id) FROM weir_test.weir_records_keyed WHERE payload = '$DC_PAYLOAD'"; }
cnt_s3()    { docker exec weir-test-minio mc ls --recursive "local/weir-test/$DC_S3_PREFIX/" 2>/dev/null | wc -l | tr -d ' '; }

# watch_segments <case> <phase>: notes, every 0.1 s, the names of the segment
# files in the case's WAB, so the evidence shows which names a phase used.
watch_segments() {
  local d="$SC_DIR/$1" ph=$2
  ( while :; do
      "$BIN/weir-ctl" --json segments --wab-dir "$d/wab" --files 2>/dev/null \
        | sed -n 's/.*"name": *"\(seg_[0-9]*\)\.wab.*/\1/p'
      sleep 0.1
    done ) >> "$d/segments-$ph.raw" 2>/dev/null &
  WATCH_PID=$!
}
unwatch_segments() {
  local d="$SC_DIR/$1" ph=$2
  [ -n "$WATCH_PID" ] && { kill "$WATCH_PID" 2>/dev/null; wait "$WATCH_PID" 2>/dev/null; }
  WATCH_PID=
  sort -u "$d/segments-$ph.raw" 2>/dev/null > "$d/segments-$ph.txt"
}

push_one() {  # push_one <case>: DC_PAYLOAD once, Durable
  "$BIN/weir-ctl" push "$DC_PAYLOAD" --durability durable --socket "$SC_DIR/$1/weir.sock" >/dev/null
}

# heartbeat_case <case> <count-fn> <toml lines...>
heartbeat_case() {
  local case=$1 count=$2; shift 2
  local d="$SC_DIR/$case" i n names
  say "--- $case"
  DC_PAYLOAD="owner-hb-$case-$SC_RUN"
  if ! start_weir "$case" "$@"; then
    record "$case" FAIL "daemon did not start (see $d/weir.log)"; return
  fi
  watch_segments "$case" 1
  for i in 1 2 3; do
    push_one "$case" || { unwatch_segments "$case" 1; stop_weir; record "$case" FAIL "push $i failed"; return; }
    sleep 3   # past wab_segment_max_age_secs = 1: the next push opens a new segment
  done
  wait_for 60 "[ \"\$($count)\" -ge 3 ]"
  sleep 5     # a fourth copy would be a bug too
  n=$($count); unwatch_segments "$case" 1
  names=$(wc -l < "$d/segments-1.txt" | tr -d ' ')
  evidence "$case"
  local dl; dl=$(dl_bytes)
  stop_weir
  echo "stored=$n segment-names-seen=$names dead-letter-bytes=$dl" >> "$d/evidence.txt"
  if [ "$n" = 3 ] && [ "$dl" = 0 ]; then
    if [ "$names" -ge 3 ]; then
      record "$case" PASS "3 of 3 identical records arrived from $names segments, nothing dead-lettered"
    else
      record "$case" INCONCLUSIVE "3 arrived, but only $names segment name(s) seen, so the three may have shared a batch"
    fi
  else
    record "$case" FAIL "$n of 3 identical records arrived (dead-letter bytes $dl): a batch was dropped as a duplicate"
  fi
}

# restart_case <case> <count-fn> <ids-fn> <toml lines...>; ids-fn prints the
# number of distinct ids or keys the target holds for DC_PAYLOAD.
restart_case() {
  local case=$1 count=$2 ids=$3; shift 3
  local d="$SC_DIR/$case" n k reused left
  say "--- $case"
  DC_PAYLOAD="owner-rs-$case-$SC_RUN"
  if ! start_weir "$case" "$@"; then
    record "$case" FAIL "daemon did not start (see $d/weir.log)"; return
  fi
  watch_segments "$case" 1
  push_one "$case" || { unwatch_segments "$case" 1; stop_weir; record "$case" FAIL "first push failed"; return; }
  if ! wait_for 60 "[ \"\$($count)\" -ge 1 ]"; then
    unwatch_segments "$case" 1; evidence "$case"; stop_weir
    record "$case" FAIL "the first record never arrived"; return
  fi
  sleep 4     # let the drain confirm and delete the segment, so the shard is empty
  unwatch_segments "$case" 1; evidence "$case"; stop_weir
  left=$(find "$d/wab" -name 'seg_*' 2>/dev/null | wc -l | tr -d ' ')
  { echo "== $case segment files left after phase 1: $left"; find "$d/wab" -name 'seg_*' 2>/dev/null; } >> "$d/evidence.txt"
  # Phase 2: the same wab_dir, the same config, the same record.
  "$BIN/weir-server" --config "$d/weir.toml" >> "$d/weir.log" 2>&1 &
  SC_PID=$!
  if ! wait_for 15 "[ -S '$d/weir.sock' ]"; then
    stop_weir; record "$case" FAIL "daemon did not restart (see $d/weir.log)"; return
  fi
  watch_segments "$case" 2
  push_one "$case" || { unwatch_segments "$case" 2; stop_weir; record "$case" FAIL "second push failed"; return; }
  wait_for 60 "[ \"\$($count)\" -ge 2 ]"
  sleep 5
  n=$($count); k=$($ids); unwatch_segments "$case" 2
  reused=$(comm -12 "$d/segments-1.txt" "$d/segments-2.txt" | tr '\n' ' ')
  evidence "$case"
  local dl; dl=$(dl_bytes)
  stop_weir
  echo "stored=$n distinct-ids=$k names-in-both-phases=${reused:-none} dead-letter-bytes=$dl" >> "$d/evidence.txt"
  if [ "$n" = 2 ] && [ "$k" = 2 ] && [ "$dl" = 0 ]; then
    if [ -n "$reused" ]; then
      record "$case" PASS "both arrived under 2 distinct ids, though the restart reused ${reused% }"
    else
      record "$case" INCONCLUSIVE "both arrived, but no segment name was seen in both phases, so the reuse this fix covers did not happen (files left after phase 1: $left)"
    fi
  else
    record "$case" FAIL "$n of 2 arrived under $k distinct id(s) (names in both phases: ${reused:-none}; dead-letter bytes $dl)"
  fi
}

# The cases. Every daemon gets the sink by sink_type plus WEIR_SINK_URL
# (never argv), as in a deployment.
case_ch_heartbeat() {
  export WEIR_SINK_URL="http://127.0.0.1:18123/"
  heartbeat_case ch-heartbeat cnt_ch 'sink_type = "clickhouse"'
  unset WEIR_SINK_URL
}
case_ch_restart() {
  export WEIR_SINK_URL="http://127.0.0.1:18123/"
  # One row per batch, so distinct rows here are distinct accepted inserts.
  restart_case ch-restart cnt_ch cnt_ch 'sink_type = "clickhouse"'
  unset WEIR_SINK_URL
}
cnt_http_ndjson() { httpd_stat http-ndjson-heartbeat stored; }
case_http_ndjson_heartbeat() {
  export WEIR_SINK_URL="http://127.0.0.1:$DC_HTTP_PORT/http-ndjson-heartbeat"
  heartbeat_case http-ndjson-heartbeat cnt_http_ndjson 'sink_type = "http"' 'sink_http_batch = "ndjson"'
  unset WEIR_SINK_URL
  echo "keys=$(httpd_stat http-ndjson-heartbeat keys) replays=$(httpd_stat http-ndjson-heartbeat replays)" \
    >> "$SC_DIR/http-ndjson-heartbeat/evidence.txt"
}
cnt_http_record() { httpd_stat http-record-restart stored; }
keys_http_record() { httpd_stat http-record-restart keys; }
case_http_record_restart() {
  export WEIR_SINK_URL="http://127.0.0.1:$DC_HTTP_PORT/http-record-restart"
  restart_case http-record-restart cnt_http_record keys_http_record 'sink_type = "http"' 'sink_http_batch = "none"'
  unset WEIR_SINK_URL
  echo "replays=$(httpd_stat http-record-restart replays)" >> "$SC_DIR/http-record-restart/evidence.txt"
}
case_pg_restart() {
  export WEIR_SINK_URL="postgres://postgres:test@127.0.0.1:55432/weir_test"
  restart_case pg-restart cnt_pg ids_pg 'sink_type = "postgres"' \
    'sink_postgres_table = "weir_records_keyed"' 'sink_postgres_id_column = "record_id"'
  unset WEIR_SINK_URL
}
case_mysql_restart() {
  export WEIR_SINK_URL="mysql://root:test@127.0.0.1:33306/weir_test"
  restart_case mysql-restart cnt_mysql ids_mysql 'sink_type = "mysql"' \
    'sink_mysql_table = "weir_records_keyed"' 'sink_mysql_id_column = "record_id"'
  unset WEIR_SINK_URL
}
case_s3_heartbeat() {
  DC_S3_PREFIX="owner-dedup-$SC_RUN"
  local init_rc
  init_rc=$(docker wait weir-test-minio-init 2>/dev/null || echo missing)
  if [ "$init_rc" != 0 ]; then
    record s3-heartbeat INCONCLUSIVE "minio-init exited $init_rc, so the bucket may not exist"; return
  fi
  export AWS_ACCESS_KEY_ID=weirtest AWS_SECRET_ACCESS_KEY=weirtestsecret
  heartbeat_case s3-heartbeat cnt_s3 'sink_type = "s3"' 'sink_s3_bucket = "weir-test"' \
    'sink_s3_endpoint = "http://127.0.0.1:19000"' 'sink_s3_force_path_style = true' \
    'sink_s3_region = "us-east-1"' 'sink_s3_compression = "none"' "sink_s3_prefix = \"$DC_S3_PREFIX\""
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  docker exec weir-test-minio mc ls --recursive "local/weir-test/$DC_S3_PREFIX/" \
    >> "$SC_DIR/s3-heartbeat/evidence.txt" 2>&1 || true
}

# What ClickHouse itself saw: the insert_deduplication_token of each insert into
# weir_records in this run (the 64-hex batch key; on the baseline, the DedupToken).
ch_tokens() {
  ch_sql "SYSTEM FLUSH LOGS" >/dev/null 2>&1
  ch_sql "SELECT event_time, Settings['insert_deduplication_token'] AS token, written_rows
          FROM system.query_log
          WHERE type = 'QueryFinish' AND query_kind = 'Insert'
            AND has(tables, 'default.weir_records') AND event_time >= now() - 3600
          ORDER BY event_time FORMAT TSV" > "$SC_DIR/clickhouse-insert-tokens.tsv" 2>&1 || true
}

cmd_baseline_setup() {
  need_linux baseline-setup
  check_quiet_host
  local base
  base=$(git -C "$CLONE" merge-base "$(resolve_sha)" main) || die "no merge base of $EPIC_REF and main in $CLONE"
  make_worktree "$BASE_WT" "$base"
  mkdir -p "$EVIDENCE_DIR"
  ( cd "$BASE_WT" && cargo build --release -j 4 -p weir-server -p weir-ctl \
      --features weir-server/clickhouse-sink,weir-server/s3-sink ) \
    > "$EVIDENCE_DIR/baseline-build.log" 2>&1 || die "build failed: $EVIDENCE_DIR/baseline-build.log"
  say "built the baseline ($(git -C "$BASE_WT" rev-parse --short HEAD), the merge base with main). Next: bash $0 dedup-check --baseline"
}

cmd_dedup_check() {
  need_linux dedup-check
  local label=dedup-check
  if [ "${1:-}" = --baseline ]; then
    shift; BIN=$BASE_WT/target/release; label=dedup-check-baseline
    [ -x "$BIN/weir-server" ] || die "run baseline-setup first"
  else
    [ -x "$BIN/weir-server" ] || die "run sinks-setup first"
  fi
  command -v python3 >/dev/null || die "python3 not found"
  rig_preflight "$SC_PORT" "$DC_HTTP_PORT"
  SC_RUN=$(stamp | tr '[:upper:]' '[:lower:]')
  SC_DIR="$EVIDENCE_DIR/$label-$SC_RUN"
  mkdir -p "$SC_DIR"; chmod 700 "$SC_DIR"
  rig_up
  trap dc_cleanup EXIT
  httpd_start
  local c cases
  cases=${*:-ch-heartbeat http-ndjson-heartbeat s3-heartbeat ch-restart pg-restart mysql-restart http-record-restart}
  for c in $cases; do
    case $c in
      ch-heartbeat) case_ch_heartbeat ;;
      http-ndjson-heartbeat) case_http_ndjson_heartbeat ;;
      s3-heartbeat) case_s3_heartbeat ;;
      ch-restart) case_ch_restart ;;
      pg-restart) case_pg_restart ;;
      mysql-restart) case_mysql_restart ;;
      http-record-restart) case_http_record_restart ;;
      *) record "$c" FAIL "unknown case" ;;
    esac
  done
  ch_tokens
  {
    echo "$label with $BIN at $(git -C "$BIN/../.." rev-parse --short HEAD), $(date -Is)"
    echo "$SC_RESULTS" | sed '/^$/d'
    echo "DEDUP CHECK failures=$SC_FAILS"
  } | tee "$SC_DIR/summary.txt"
  say "evidence under $SC_DIR (summary.txt, clickhouse-insert-tokens.tsv, http-requests.jsonl, per-case weir.log and evidence.txt)"
  [ "$SC_FAILS" -eq 0 ]
}

# ---------------------------------------------------------------------------
# Mac: native macOS tests and the darwin TLS smoke
# ---------------------------------------------------------------------------
beast_sha() { ssh -o BatchMode=yes beast "git -C $CLONE rev-parse --verify --quiet '$EPIC_REF^{commit}'"; }

cmd_mac_tests() {
  need_mac mac-tests
  local wt=${1:-}
  [ -n "$wt" ] && [ -d "$wt" ] || die "usage: mac-tests <local worktree of $EPIC_REF>"
  [ -x "$MAC_TESTS" ] || [ -f "$MAC_TESTS" ] || die "$MAC_TESTS not found"
  local here there out
  here=$(git -C "$wt" rev-parse HEAD) || die "$wt is not a git worktree"
  there=$(beast_sha) || die "cannot read $EPIC_REF on beast"
  [ "$here" = "$there" ] || die "$wt is at ${here:0:12}, beast's $EPIC_REF at ${there:0:12}: test the sha that will be pushed"
  out="$HOME/weir-evidence/$EPIC/mac-tests-${here:0:12}-$(stamp)"
  mkdir -p "$out"
  say "mac-tests.sh at ${here:0:12} -> $out (cross-builds on beast in docker, runs here)"
  bash "$MAC_TESTS" "$wt" "$out"
  grep -E 'rc=[1-9]|FAILED|DONE' "$out/mac-summary.txt"
  grep -q 'MAC TESTS DONE: 0 failing executables' "$out/mac-summary.txt"
}

GATE_LOGS=/srv/bulk/Projects/weir/logs/gate
cmd_darwin_smoke() {
  need_mac darwin-smoke
  local sha out gl gdir bslot t mt lt bin o rc=0
  sha=$(beast_sha) || die "cannot read $EPIC_REF on beast"
  out="$HOME/weir-evidence/$EPIC/darwin-smoke-${sha:0:12}-$(stamp)"
  mkdir -p "$out"
  # The newest full gate on beast at this sha (its summary's first line is
  # "gate: full at <short sha> ..."), whichever worktree it ran from.
  gl=$(ssh -o BatchMode=yes beast "grep -l '^gate: full at ${sha:0:7}' $GATE_LOGS/*/full-*/summary.txt 2>/dev/null | xargs -r ls -t | head -1")
  [ -n "$gl" ] || die "no full gate on beast at ${sha:0:7}: the head runs it first"
  gdir=$(dirname "$gl")
  ssh -o BatchMode=yes beast "cat '$gl'" > "$out/full-gate.txt"
  tail -1 "$out/full-gate.txt"
  tail -1 "$out/full-gate.txt" | grep -q '^GATE full rc=0' || die "that full gate did not pass ($gl)"
  # The full gate builds both apple-darwin targets with cargo-zigbuild into the
  # docker volume weir-xplat-zig-target-<slot>; jobq-output.log names its slot.
  bslot=${SLOT:-$(ssh -o BatchMode=yes beast "sed -n 's|^gate: slot .*/\([a-d]\)\$|\1|p' '$gdir/jobq-output.log' 2>/dev/null | head -1")}
  [ -n "$bslot" ] || die "cannot tell the slot of $gdir (no jobq-output.log); set SLOT=a..d"
  say "full gate $gdir, slot $bslot"
  for t in aarch64-apple-darwin x86_64-apple-darwin; do
    # The binary must be the one that gate's build step made: built no later than
    # that step's log was last written, and not an hour before it. A later gate
    # in the same slot rebuilds it, and then this smoke would test another sha.
    mt=$(ssh -o BatchMode=yes beast "docker run --rm -v weir-xplat-zig-target-$bslot:/target:ro alpine \
          stat -c %Y /target/$t/release/weir-server" 2>/dev/null)
    lt=$(ssh -o BatchMode=yes beast "stat -c %Y '$gdir/build-$t.log'" 2>/dev/null)
    echo "$t binary-mtime=$mt build-log-mtime=$lt" >> "$out/full-gate.txt"
    if [ -z "$mt" ] || [ -z "$lt" ] || [ "$mt" -gt $((lt + 60)) ] || [ "$mt" -lt $((lt - 3600)) ]; then
      die "$t in slot $bslot was not built by $gdir (binary ${mt:-missing}, step log ${lt:-missing}); rerun the full gate, then this"
    fi
  done
  for t in aarch64-apple-darwin x86_64-apple-darwin; do
    bin="$out/weir-server-$t"
    ssh -o BatchMode=yes beast "docker run --rm -v weir-xplat-zig-target-$bslot:/target:ro alpine \
        cat /target/$t/release/weir-server" > "$bin" || { say "$t: copy failed"; rc=1; continue; }
    chmod +x "$bin"; codesign -s - -f "$bin" 2>/dev/null
    shasum -a 256 "$bin" | tee -a "$out/sha256.txt"
    if [ "$t" = x86_64-apple-darwin ] && ! arch -x86_64 /usr/bin/true 2>/dev/null; then
      say "$t: no Rosetta here, copied but not run"; continue
    fi
    # CI's check (ci.yml, build job): a tls build fails on the missing cert,
    # a tls-less build says "built without the 'tls' feature".
    local wab; wab=$(mktemp -d)
    o=$("$bin" --tcp-bind 127.0.0.1:17100 --wab-dir "$wab" 2>&1); rm -rf "$wab"
    printf '%s\n' "$o" > "$out/smoke-$t.txt"
    if printf '%s' "$o" | grep -q "without the 'tls' feature"; then
      say "$t: TLS smoke FAILED (built without tls)"; rc=1
    elif printf '%s' "$o" | grep -q "tls_cert_path"; then
      say "$t: TLS smoke OK"
    else
      say "$t: TLS smoke UNCLEAR, read $out/smoke-$t.txt"; rc=1
    fi
  done
  echo "DARWIN SMOKE rc=$rc" | tee "$out/summary.txt"
  return "$rc"
}

usage() { sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d'; }

step=${1:-help}; [ $# -gt 0 ] && shift
case $step in
  chaos-setup)    cmd_chaos_setup "$@" ;;
  chaos-campaign) cmd_chaos_campaign "$@" ;;
  chaos-gate)     cmd_chaos_gate "$@" ;;
  chaos-durable)  cmd_chaos_durable "$@" ;;
  sinks-setup)    cmd_sinks_setup "$@" ;;
  dedup-check)    cmd_dedup_check "$@" ;;
  baseline-setup) cmd_baseline_setup "$@" ;;
  sink-check)     cmd_sink_check "$@" ;;
  mac-tests)      cmd_mac_tests "$@" ;;
  darwin-smoke)   cmd_darwin_smoke "$@" ;;
  help|-h|--help) usage ;;
  *) usage; exit 2 ;;
esac
