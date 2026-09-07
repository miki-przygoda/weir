#!/usr/bin/env bash
# deploy/run-sink-integration-tests.sh — exercise every sink against a real
# backend: MySQL, PostgreSQL, ClickHouse, and MinIO for the S3 sink.
#
# Brings up the docker-compose stack at deploy/docker/test/, waits for
# every service's healthcheck to pass, exports the WEIR_TEST_* endpoints and
# the S3 credentials, runs the `#[ignore]`-marked sink tests, then tears down
# the stack on exit.
#
# Exit code: 0 = every sink test passed, non-zero = something failed.
#
# CI runs this via the `sink-integration` job. It did not until 2.1.0, and the
# cost of that was concrete: because these tests are `#[ignore]`-marked, the
# `test` job's `--test system` skipped them and nothing else invoked them, so
# all three SQL sink tests sat broken for an unknown period — asserting on a
# segment seal the default thresholds make impossible — with no signal at all.
# Keep the CI job and this script in step: the job runs exactly this file, so a
# test added here is a test CI runs.
#
# Usage:
#   bash deploy/run-sink-integration-tests.sh           # debug build (fast)
#   RELEASE=1 bash deploy/run-sink-integration-tests.sh # release build
#
# Requirements:
#   - Docker (or compatible runtime) with `docker compose` plugin.
#   - These ports free on 127.0.0.1: 33306 (mysql), 55432 (postgres),
#     18123 (clickhouse), 19000 (minio).

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
wait_for_healthy clickhouse
wait_for_healthy minio

# ── Run the integration tests ─────────────────────────────────────────────────

export WEIR_TEST_MYSQL_URL="mysql://root:test@127.0.0.1:33306/weir_test"
export WEIR_TEST_POSTGRES_URL="postgres://postgres:test@127.0.0.1:55432/weir_test"
export WEIR_TEST_CLICKHOUSE_URL="http://127.0.0.1:18123"
export WEIR_TEST_S3_ENDPOINT="http://127.0.0.1:19000"
# The S3 sink reads credentials from the environment, as a production
# deployment would; they never touch the generated config file.
export AWS_ACCESS_KEY_ID="weirtest"
export AWS_SECRET_ACCESS_KEY="weirtestsecret"

info "WEIR_TEST_MYSQL_URL=$WEIR_TEST_MYSQL_URL"
info "WEIR_TEST_POSTGRES_URL=$WEIR_TEST_POSTGRES_URL"
info "WEIR_TEST_CLICKHOUSE_URL=$WEIR_TEST_CLICKHOUSE_URL"
info "WEIR_TEST_S3_ENDPOINT=$WEIR_TEST_S3_ENDPOINT"

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

info "running clickhouse_sink_end_to_end"
# shellcheck disable=SC2086
cargo test $CARGO_FLAGS -p weir-server --features clickhouse-sink --test system -- --ignored --exact \
    clickhouse_sink_end_to_end

# The S3 suite is five tests, not one, and two of them carry the design:
# s3_sink_replay_is_an_idempotent_overwrite pins replay stability, and
# s3_sink_distinct_batches_of_identical_records_produce_distinct_objects pins
# collision freedom. Either alone passes a broken key scheme -- the first is
# equally true when the sink is overwriting its own data, the second when it is
# duplicating on every replay. Run them as a group so neither can be dropped.
info "running the s3 sink suite (5 tests) against MinIO"
# shellcheck disable=SC2086
cargo test $CARGO_FLAGS -p weir-server --features s3-sink --test system -- \
    --ignored --test-threads=1 s3_sink

info "all sink integration tests passed"
