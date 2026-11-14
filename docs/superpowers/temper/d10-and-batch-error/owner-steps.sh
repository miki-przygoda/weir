#!/usr/bin/env bash
# Owner evidence steps for epic d10-and-batch-error (branch epic/d10-and-batch-error).
#
# FOR THE OWNER ONLY. Agents never run any of this: the Docker steps need a
# Docker daemon with registry access (the build boxes have neither), and the
# systemd step needs root on a host with systemd. Nothing here pushes, merges or
# deletes a branch. Read owner-checklist.md (next to this file) first; it says
# what each step proves, how long it takes and what to paste back.
#
#   bash owner-steps.sh <step> [args]
#
# On beast (the build host), as your own user (needs docker and cargo):
#   setup              detached worktree at the epic tip
#   docker-build       build weir-server:local from deploy/docker/Dockerfile
#   docker-tests       the acceptance tests CONFIG-STARTUP-05 and -08 against it
#                      (WEIR_ACCEPTANCE_DOCKER=1), the same run docker.yml does
#   docker-manual      the flows by hand: env-only start, explicit missing
#                      --config, a bind-mounted /etc/weir/weir.toml
#   systemd-bundle     the image's weir-server binary, the shipped unit and
#                      weir.toml, and this script, in one dir to copy to a
#                      disposable systemd host
#
# On a DISPOSABLE systemd host (a throwaway VM or system container, Debian 12+
# or Ubuntu 24.04+), as root, from inside the copied bundle dir:
#   systemd-check      install the shipped unit with no /etc/weir/weir.toml:
#                      `systemctl start weir` fails with the D10 error in the
#                      journal; then install weir.toml: the unit comes up
#   systemd-cleanup    remove what systemd-check installed (or just delete the
#                      disposable host)
#
# Environment (all optional):
#   EPIC_REF      what to test (default epic/d10-and-batch-error, resolved in the clone)
#   EVIDENCE_DIR  where logs and results go (default ~/weir-evidence/d10-and-batch-error;
#                 systemd-check writes to <bundle>/evidence)
set -uo pipefail

EPIC=d10-and-batch-error
EPIC_REF=${EPIC_REF:-epic/$EPIC}
CLONE=/srv/bulk/Projects/weir/src
WORK=$HOME/work/weir/$EPIC
WT=$WORK/owner-evidence
EVIDENCE_DIR=${EVIDENCE_DIR:-$HOME/weir-evidence/$EPIC}
IMAGE=weir-server:local
DEFAULT_PATH=/etc/weir/weir.toml
D10_MSG_DEFAULT="config file $DEFAULT_PATH (passed with --config) does not exist"
D10_MSG_MISSING="config file /etc/weir/missing.toml (passed with --config) does not exist"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)

say()  { printf '[owner] %s\n' "$*"; }
die()  { printf '[owner] ERROR: %s\n' "$*" >&2; exit 1; }
stamp() { date -u +%Y%m%dT%H%M%SZ; }
need_linux() { [ "$(uname -s)" = Linux ] || die "run '$1' on Linux, not on $(uname -s)"; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "'$1' is not installed here"; }

# Results: every check appends one PASS/FAIL line to $RESULTS; a step ends by
# printing its results and exits 1 if any check failed.
RESULTS=
FAILED=0
results_open() {
  mkdir -p "$EVIDENCE_DIR"
  RESULTS=$EVIDENCE_DIR/$1-$(stamp).results
  : > "$RESULTS"
  say "results: $RESULTS"
}
check() { # check <label> <command...>
  local label=$1; shift
  if "$@"; then
    printf 'PASS %s\n' "$label" | tee -a "$RESULTS"
  else
    printf 'FAIL %s\n' "$label" | tee -a "$RESULTS"
    FAILED=1
  fi
}
results_close() {
  say "---- $(basename "$RESULTS")"
  cat "$RESULTS"
  if [ "$FAILED" -ne 0 ]; then say "SOME CHECKS FAILED"; exit 1; fi
  say "all checks passed"
}
has() { grep -qF -- "$2" "$1"; }          # has <file> <fixed string>
lacks() { ! grep -qF -- "$2" "$1"; }
line_has() { # line_has <file> <line needle> <needle in that line>
  grep -F -- "$2" "$1" | grep -qF -- "$3"
}

resolve_sha() {
  git -C "$CLONE" rev-parse --verify --quiet "$EPIC_REF^{commit}" \
    || die "cannot resolve $EPIC_REF in $CLONE (did the head bring the branch here?)"
}

need_wt() {
  [ -e "$WT/.git" ] || die "no worktree at $WT: run 'bash owner-steps.sh setup' first"
  say "testing $(git -C "$WT" rev-parse HEAD) ($WT)"
}

# ---------------------------------------------------------------------------
# setup: a DETACHED worktree at the epic tip, so the epic branch stays checked
# out in no worktree and the head can still move it.
# ---------------------------------------------------------------------------
cmd_setup() {
  need_linux setup
  local sha; sha=$(resolve_sha)
  if [ -e "$WT/.git" ]; then
    [ -z "$(git -C "$WT" status --porcelain --untracked-files=no)" ] \
      || die "$WT has local changes; clean it up by hand first"
    git -C "$WT" switch --detach -q "$sha" || die "could not move $WT to $sha"
  else
    mkdir -p "$WORK"
    git -C "$CLONE" worktree add --detach "$WT" "$sha" || die "git worktree add $WT failed"
  fi
  mkdir -p "$EVIDENCE_DIR"
  git -C "$WT" rev-parse HEAD | tee "$EVIDENCE_DIR/sha.txt"
  say "$WT at $(git -C "$WT" rev-parse --short HEAD). Next: bash owner-steps.sh docker-build"
}

# ---------------------------------------------------------------------------
# docker-build: weir-server:local from the shipped Dockerfile (CMD ["weir-server"]).
# This retags weir-server:local if you already have one.
# ---------------------------------------------------------------------------
cmd_docker_build() {
  need_linux docker-build; need_cmd docker; need_wt
  mkdir -p "$EVIDENCE_DIR"
  local sha log; sha=$(git -C "$WT" rev-parse HEAD)
  log=$EVIDENCE_DIR/docker-build-$(stamp).log
  say "docker build (10-20 min cold); log: $log"
  ( cd "$WT" && docker build -t "$IMAGE" -t "weir-server:d10-${sha:0:12}" \
      -f deploy/docker/Dockerfile . ) > "$log" 2>&1 || die "docker build failed: $log"
  docker image inspect -f '{{.Id}}' "$IMAGE" | tee "$EVIDENCE_DIR/image-id.txt"
  docker image inspect -f '{{json .Config.Cmd}}' "$IMAGE" | tee "$EVIDENCE_DIR/image-cmd.txt"
  say "built $IMAGE for $sha"
}

# The image under test must be the one docker-build made for this sha.
need_image() {
  need_cmd docker
  docker image inspect "$IMAGE" >/dev/null 2>&1 || die "no $IMAGE: run docker-build first"
  [ -f "$EVIDENCE_DIR/image-id.txt" ] || die "no image-id.txt: run docker-build first"
  local want have; want=$(cat "$EVIDENCE_DIR/image-id.txt")
  have=$(docker image inspect -f '{{.Id}}' "$IMAGE")
  [ "$want" = "$have" ] || die "$IMAGE is $have, not the $want docker-build made: rerun docker-build"
}

# ---------------------------------------------------------------------------
# docker-tests: CONFIG-STARTUP-05 (bind-mounted default file is read) and -08
# (env-only start, INFO config source line, NOOP SINK WARN). Only these two:
# the rest of the file runs in the gate, and CONFIG-STARTUP-01 fails falsely
# on a host that has /etc/weir/weir.toml (an open item in the PR body).
# ---------------------------------------------------------------------------
cmd_docker_tests() {
  need_linux docker-tests; need_cmd cargo; need_wt; need_image
  results_open docker-tests
  local log=${RESULTS%.results}.log
  say "cargo test (builds weir-server's test profile, -j 4); log: $log"
  ( cd "$WT" && WEIR_ACCEPTANCE_DOCKER=1 cargo test -j 4 -p weir-server \
      --test acceptance_config_startup -- --exact \
      config_startup_05_default_file_present_is_read \
      config_startup_08_docker_image_starts_env_only ) > "$log" 2>&1
  local rc=$?
  check "cargo test exit status 0 (rc=$rc)" test "$rc" -eq 0
  check "both tests ran and passed (not skipped)" has "$log" "test result: ok. 2 passed"
  check "no skip note" lacks "$log" "skipping:"
  grep -E '^test |test result' "$log" | tee -a "$RESULTS"
  results_close
}

# ---------------------------------------------------------------------------
# docker-manual: the three container cases by hand, logs kept.
# ---------------------------------------------------------------------------
CONTAINERS=
container_cleanup() {
  local c
  for c in $CONTAINERS; do docker rm -f -v "$c" >/dev/null 2>&1 || true; done
  [ -n "${SCRATCH:-}" ] && rm -rf "$SCRATCH"
}

# wait_logs <container> <needle> <out file>: poll the logs for 30 s.
wait_logs() {
  for _ in $(seq 1 30); do
    docker logs "$1" > "$3" 2>&1
    has "$3" "$2" && return 0
    sleep 1
  done
  return 1
}
running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }

cmd_docker_manual() {
  need_linux docker-manual; need_wt; need_image
  results_open docker-manual
  local base=${RESULTS%.results} tag; tag=$$
  SCRATCH=$(mktemp -d)
  trap container_cleanup EXIT

  # 1. Env-only, no bind mount: starts on the image's CMD.
  local c1=weir-d10-envonly-$tag; CONTAINERS="$CONTAINERS $c1"
  say "1/3 env-only start ($c1)"
  docker run -d --name "$c1" -e WEIR_LOG_LEVEL=info -e WEIR_SHARD_COUNT=2 "$IMAGE" >/dev/null \
    || die "docker run $c1 failed"
  wait_logs "$c1" "NOOP SINK" "$base-envonly.log"
  sleep 3; docker logs "$c1" > "$base-envonly.log" 2>&1
  check "env-only: container still running" running "$c1"
  check "env-only: INFO config source names the default path" line_has "$base-envonly.log" "config source" "$DEFAULT_PATH"
  check "env-only: config source is INFO" line_has "$base-envonly.log" "config source" "INFO"
  check "env-only: config source loaded=false" line_has "$base-envonly.log" "config source" "loaded=false"
  check "env-only: WEIR_SHARD_COUNT applied (shards=2)" line_has "$base-envonly.log" "weir starting" "shards=2"
  check "env-only: NOOP SINK WARN present" line_has "$base-envonly.log" "NOOP SINK" "WARN"
  check "env-only: no D10 error" lacks "$base-envonly.log" "passed with --config"
  check "env-only: no old deprecation sentence" lacks "$base-envonly.log" "will become a startup error"
  docker rm -f -v "$c1" >/dev/null 2>&1

  # 2. An explicit --config that does not exist: exits 1 at once.
  local c2=weir-d10-missing-$tag; CONTAINERS="$CONTAINERS $c2"
  say "2/3 explicit missing --config ($c2)"
  timeout 60 docker run --name "$c2" "$IMAGE" weir-server --config /etc/weir/missing.toml \
    > "$base-missing.log" 2>&1
  local rc=$?
  check "missing --config: exit status 1 (got $rc)" test "$rc" -eq 1
  check "missing --config: the D10 message" has "$base-missing.log" "$D10_MSG_MISSING"
  check "missing --config: no config source line (failed before startup)" lacks "$base-missing.log" "config source"
  # Nothing created: list the WAB dir and /run/weir from the stopped container.
  docker cp "$c2:/var/lib/weir/wab" "$SCRATCH/wab" >/dev/null 2>&1
  docker cp "$c2:/run/weir" "$SCRATCH/run" >/dev/null 2>&1
  { echo "wab:"; ls -lA "$SCRATCH/wab" 2>&1; echo "run:"; ls -lA "$SCRATCH/run" 2>&1; } >> "$base-missing.log"
  check "missing --config: WAB dir copied out and empty (no lock, no shards)" \
    eval 'test -d "$SCRATCH/wab" && test -z "$(ls -A "$SCRATCH/wab")"'
  check "missing --config: /run/weir copied out, no socket in it" \
    eval 'test -d "$SCRATCH/run" && test ! -e "$SCRATCH/run/weir.sock"'
  docker rm -f -v "$c2" >/dev/null 2>&1

  # 3. A file bind-mounted at the default path, no --config: it is read.
  local c3=weir-d10-mounted-$tag; CONTAINERS="$CONTAINERS $c3"
  say "3/3 bind-mounted $DEFAULT_PATH ($c3)"
  sed 's/^shard_count *=.*/shard_count  = 3/' "$WT/deploy/systemd/weir.toml" > "$SCRATCH/weir.toml"
  chmod 0755 "$SCRATCH"; chmod 0644 "$SCRATCH/weir.toml"
  docker run -d --name "$c3" -v "$SCRATCH/weir.toml:$DEFAULT_PATH:ro" "$IMAGE" >/dev/null \
    || die "docker run $c3 failed"
  wait_logs "$c3" "config source" "$base-mounted.log"
  sleep 3; docker logs "$c3" > "$base-mounted.log" 2>&1
  check "mounted: container still running" running "$c3"
  check "mounted: config source loaded=true" line_has "$base-mounted.log" "config source" "loaded=true"
  check "mounted: origin is the default path" line_has "$base-mounted.log" "config source" "default path"
  check "mounted: shard_count from the file (shards=3)" line_has "$base-mounted.log" "weir starting" "shards=3"
  check "mounted: no missing-file NOOP SINK WARN" lacks "$base-mounted.log" "NOOP SINK: no config file"
  docker rm -f -v "$c3" >/dev/null 2>&1

  results_close
}

# ---------------------------------------------------------------------------
# systemd-bundle: what systemd-check needs, in one dir. The binary comes out of
# the image (built on Debian bookworm, glibc 2.36), so it runs on Debian 12+ or
# Ubuntu 24.04+; a binary built on beast may need a newer glibc than the
# disposable host has.
# ---------------------------------------------------------------------------
cmd_systemd_bundle() {
  need_linux systemd-bundle; need_wt; need_image
  local b=$EVIDENCE_DIR/systemd-bundle cid
  rm -rf "$b"; mkdir -p "$b"
  cid=$(docker create "$IMAGE") || die "docker create failed"
  docker cp "$cid:/usr/local/bin/weir-server" "$b/weir-server" >/dev/null || { docker rm -f "$cid" >/dev/null; die "docker cp failed"; }
  docker rm -f "$cid" >/dev/null
  cp "$WT/deploy/systemd/weir.service" "$WT/deploy/systemd/weir.toml" "$b/"
  cp "$WT/docs/superpowers/temper/$EPIC/owner-steps.sh" "$b/"
  git -C "$WT" rev-parse HEAD > "$b/sha.txt"
  ( cd "$b" && sha256sum weir-server weir.service weir.toml owner-steps.sh > SHA256SUMS )
  cat "$b/SHA256SUMS"
  say "bundle: $b"
  say "copy it to a disposable systemd host, then there, as root:"
  say "  cd <bundle dir> && bash owner-steps.sh systemd-check"
}

# ---------------------------------------------------------------------------
# systemd-check (root, disposable host): the shipped unit, unmodified.
# ---------------------------------------------------------------------------
UNIT=/etc/systemd/system/weir.service
MANIFEST=/var/tmp/weir-d10-evidence.manifest

need_bundle() {
  local f
  for f in weir-server weir.service weir.toml SHA256SUMS; do
    [ -f "$SCRIPT_DIR/$f" ] || die "no $f next to this script: run it from the systemd-bundle dir"
  done
  ( cd "$SCRIPT_DIR" && sha256sum -c --quiet SHA256SUMS ) || die "bundle checksums do not match"
}

# wait_journal <since> <needle> <out>: poll the unit's journal for 30 s.
wait_journal() {
  for _ in $(seq 1 30); do
    journalctl -u weir --since "$1" --no-pager -o cat > "$3" 2>&1
    has "$3" "$2" && return 0
    sleep 1
  done
  return 1
}
unit_prop() { systemctl show weir -p "$1" --value; }

cmd_systemd_check() {
  need_linux systemd-check
  [ "$(id -u)" -eq 0 ] || die "systemd-check installs a system unit: run it as root, on a disposable host"
  need_cmd systemctl; need_cmd journalctl
  [ -d /run/systemd/system ] || die "systemd is not PID 1 here"
  need_bundle
  # Never touch a real install.
  local p
  for p in "$UNIT" /etc/weir /usr/local/bin/weir-server /var/lib/weir /run/weir; do
    [ ! -e "$p" ] || die "$p exists: this host has (or had) weir; use a disposable host"
  done
  systemctl cat weir >/dev/null 2>&1 && die "a weir unit is already known to systemd; use a disposable host"

  EVIDENCE_DIR=$SCRIPT_DIR/evidence
  results_open systemd-check
  local base=${RESULTS%.results}
  say "sha under test: $(cat "$SCRIPT_DIR/sha.txt")"

  : > "$MANIFEST"
  if ! getent passwd weir >/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin weir || die "useradd weir failed"
    echo user >> "$MANIFEST"
  fi
  install -m0755 "$SCRIPT_DIR/weir-server" /usr/local/bin/weir-server && echo bin >> "$MANIFEST"
  install -m0644 "$SCRIPT_DIR/weir.service" "$UNIT" && echo unit >> "$MANIFEST"
  install -d -m0755 /etc/weir && echo etc >> "$MANIFEST"
  systemctl daemon-reload
  check "shipped ExecStart passes --config $DEFAULT_PATH" \
    has "$UNIT" "ExecStart=/usr/local/bin/weir-server --config $DEFAULT_PATH"

  # A. No /etc/weir/weir.toml: the unit fails with the D10 error.
  say "A: systemctl start weir with no $DEFAULT_PATH"
  local since; since=$(date '+%Y-%m-%d %H:%M:%S')
  sleep 1
  systemctl start weir; echo "systemctl start rc=$?" >> "$base-missing.log"
  wait_journal "$since" "$D10_MSG_DEFAULT" "$base-missing.journal"
  sleep 2
  { echo "state after the first exit:"
    systemctl show weir -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p NRestarts
    echo "/var/lib/weir/wab:"; ls -lA /var/lib/weir/wab 2>&1
    echo "/run/weir:"; ls -lA /run/weir 2>&1
  } >> "$base-missing.log"
  check "A: the D10 error is in the journal" has "$base-missing.journal" "$D10_MSG_DEFAULT"
  check "A: the unit's main process exited 1" test "$(unit_prop ExecMainStatus)" = 1
  check "A: systemd records a failure (Result=exit-code)" test "$(unit_prop Result)" = exit-code
  check "A: never active (no config source line)" lacks "$base-missing.journal" "config source"
  check "A: WAB dir empty (only ExecStartPre's mkdir; no lock, no shards)" \
    eval 'test -d /var/lib/weir/wab && test -z "$(ls -A /var/lib/weir/wab)"'
  check "A: no socket" test ! -e /run/weir/weir.sock
  # Restart=on-failure keeps retrying every 5 s; stop it before B.
  systemctl stop weir; systemctl reset-failed weir 2>/dev/null
  cat "$base-missing.log"

  # B. Install the shipped weir.toml (0640 root:weir, as deploy/systemd/README.md
  # says): the same unit comes up and reads it.
  say "B: install $DEFAULT_PATH and start again"
  install -m0640 -o root -g weir "$SCRIPT_DIR/weir.toml" "$DEFAULT_PATH" && echo toml >> "$MANIFEST"
  since=$(date '+%Y-%m-%d %H:%M:%S'); sleep 1
  systemctl start weir
  wait_journal "$since" "config source" "$base-present.journal"
  sleep 3
  journalctl -u weir --since "$since" --no-pager -o cat > "$base-present.journal" 2>&1
  systemctl status weir --no-pager > "$base-present.status" 2>&1
  check "B: unit active" test "$(systemctl is-active weir)" = active
  check "B: config source loaded=true" line_has "$base-present.journal" "config source" "loaded=true"
  check "B: origin --config" line_has "$base-present.journal" "config source" "--config"
  check "B: no D10 error" lacks "$base-present.journal" "does not exist"
  check "B: socket bound" test -S /run/weir/weir.sock
  systemctl stop weir
  check "B: clean stop (Result=success)" test "$(unit_prop Result)" = success

  say "journals and status: $EVIDENCE_DIR. Clean up with systemd-cleanup, or delete the host."
  results_close
}

cmd_systemd_cleanup() {
  [ "$(id -u)" -eq 0 ] || die "run as root"
  [ -f "$MANIFEST" ] || die "no $MANIFEST: systemd-check installed nothing here"
  systemctl stop weir 2>/dev/null; systemctl reset-failed weir 2>/dev/null
  grep -qx toml "$MANIFEST" && rm -f "$DEFAULT_PATH"
  grep -qx etc "$MANIFEST" && rmdir /etc/weir 2>/dev/null
  grep -qx unit "$MANIFEST" && rm -f "$UNIT"
  systemctl daemon-reload
  grep -qx bin "$MANIFEST" && rm -f /usr/local/bin/weir-server
  # StateDirectory= made /var/lib/weir for this unit; it held only the test's WAB.
  grep -qx unit "$MANIFEST" && rm -rf /var/lib/weir
  grep -qx user "$MANIFEST" && userdel weir
  rm -f "$MANIFEST"
  say "removed what systemd-check installed"
}

usage() { sed -n '2,34p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

step=${1:-help}; shift 2>/dev/null || true
case $step in
  setup)            cmd_setup "$@" ;;
  docker-build)     cmd_docker_build "$@" ;;
  docker-tests)     cmd_docker_tests "$@" ;;
  docker-manual)    cmd_docker_manual "$@" ;;
  systemd-bundle)   cmd_systemd_bundle "$@" ;;
  systemd-check)    cmd_systemd_check "$@" ;;
  systemd-cleanup)  cmd_systemd_cleanup "$@" ;;
  help|-h|--help)   usage ;;
  *)                usage; die "unknown step: $step" ;;
esac
