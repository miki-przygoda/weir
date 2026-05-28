#!/usr/bin/env bash
# deploy/run-sink-integration-tests.sh — exercise the SQL sinks against
# real MySQL and PostgreSQL backends.
#
# Brings up the docker-compose stack at deploy/docker/test/, waits for
# both services' healthchecks to pass, exports WEIR_TEST_MYSQL_URL and
# WEIR_TEST_POSTGRES_URL, runs the two `#[ignore]`-marked
# `*_sink_end_to_end` tests, then tears down the stack on exit.
#
# Exit code: 0 = both sink tests passed, non-zero = something failed.
#
# Usage:
#   bash deploy/run-sink-integration-tests.sh           # debug build (fast)
#   RELEASE=1 bash deploy/run-sink-integration-tests.sh # release build
#
# Requirements:
#   - Docker (or compatible runtime) with `docker compose` plugin.
#   - Ports 33306 (mysql) and 55432 (postgres) available on 127.0.0.1.

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[sink-int]${NC} $*"; }
warn()  { echo -e "${YELLOW}[sink-int]${NC} $*"; }
error() { echo -e "${RED}[sink-int] ERROR:${NC} $*" >&2; }
fail()  { error "$*"; exit 1; }

# ── Working directory ─────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE_FILE="deploy/docker/test/docker-compose.yml"
COMPOSE="docker compose -f $COMPOSE_FILE"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if ! command -v docker >/dev/null 2>&1; then
    fail "docker not found in PATH — install Docker or a compatible runtime"
fi

if ! docker compose version >/dev/null 2>&1; then
    fail "'docker compose' plugin not available — install docker-compose-plugin"
fi

# ── Bring up the stack ────────────────────────────────────────────────────────

info "starting MySQL + Postgres via $COMPOSE_FILE"
$COMPOSE up -d

# Tear down on any exit (including failures) so the runner is safe to
# re-invoke without leftover containers.
cleanup() {
    local exit_code=$?
    info "tearing down containers (exit code: $exit_code)"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

# ── Wait for healthchecks ─────────────────────────────────────────────────────

# `docker compose ps --format json` returns one line per service; we
# poll until each service's Health == "healthy". The compose file's
# healthchecks (`mysqladmin ping`, `pg_isready`) both gate on the
# init-script having completed, so a healthy status here means the
# schemas are present.

wait_for_healthy() {
    local service="$1"
    local max_seconds=120
    local elapsed=0
    info "waiting for $service to become healthy (timeout: ${max_seconds}s)"
    while [ $elapsed -lt $max_seconds ]; do
        local status
        status=$($COMPOSE ps --format json "$service" 2>/dev/null \
            | python3 -c 'import sys,json
try:
    line = sys.stdin.readline().strip()
    if not line: sys.exit(0)
    obj = json.loads(line)
    print(obj.get("Health", obj.get("State", "unknown")))
except Exception:
    sys.exit(0)' 2>/dev/null || echo "starting")
        case "$status" in
            healthy)
                info "$service: healthy"
                return 0
                ;;
            unhealthy)
                fail "$service became unhealthy — inspect logs with '$COMPOSE logs $service'"
                ;;
            *)
                # starting / unknown / empty — keep polling
                ;;
        esac
        sleep 2
        elapsed=$((elapsed + 2))
    done
    fail "$service did not become healthy within ${max_seconds}s — inspect logs with '$COMPOSE logs $service'"
}

wait_for_healthy mysql
wait_for_healthy postgres

# ── Run the integration tests ─────────────────────────────────────────────────

export WEIR_TEST_MYSQL_URL="mysql://root:test@127.0.0.1:33306/weir_test"
export WEIR_TEST_POSTGRES_URL="postgres://postgres:test@127.0.0.1:55432/weir_test"

info "WEIR_TEST_MYSQL_URL=$WEIR_TEST_MYSQL_URL"
info "WEIR_TEST_POSTGRES_URL=$WEIR_TEST_POSTGRES_URL"

CARGO_FLAGS=""
if [ "${RELEASE:-0}" = "1" ]; then
    CARGO_FLAGS="--release"
    info "release build"
else
    info "debug build (set RELEASE=1 for release)"
fi

info "running mysql_sink_end_to_end"
# shellcheck disable=SC2086
cargo test $CARGO_FLAGS -p weir-server --test system -- --ignored --exact \
    mysql_sink_end_to_end

info "running postgres_sink_end_to_end"
# shellcheck disable=SC2086
cargo test $CARGO_FLAGS -p weir-server --test system -- --ignored --exact \
    postgres_sink_end_to_end

info "both sink integration tests passed"
