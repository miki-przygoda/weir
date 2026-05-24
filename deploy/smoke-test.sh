#!/usr/bin/env bash
# deploy/smoke-test.sh — end-to-end smoke test for weir-server.
#
# Builds the binary, starts the daemon with a temp config, exercises the full
# pipeline (push across all three durability tiers, health check, metrics),
# then shuts down cleanly and verifies WAB segments were written to disk.
#
# Exit code: 0 = all checks passed, non-zero = something failed.
#
# Usage:
#   bash deploy/smoke-test.sh           # debug build (fast)
#   RELEASE=1 bash deploy/smoke-test.sh # release build

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[smoke]${NC} $*"; }
warn()  { echo -e "${YELLOW}[smoke]${NC} $*"; }
error() { echo -e "${RED}[smoke] ERROR:${NC} $*" >&2; }
fail()  { error "$*"; exit 1; }

# ── Working directory ─────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ── Temp dirs — cleaned up on any exit ───────────────────────────────────────
TMPDIR_ROOT="$(mktemp -d /tmp/weir-smoke-XXXXXX)"
WAB_DIR="$TMPDIR_ROOT/wab"
SOCKET_DIR="$TMPDIR_ROOT/run"
SOCKET_PATH="$SOCKET_DIR/weir.sock"
CONFIG_PATH="$TMPDIR_ROOT/weir.toml"
LOG_PATH="$TMPDIR_ROOT/weir-server.log"
METRICS_PORT=19185   # non-standard port to avoid colliding with a running daemon
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        info "Sending SIGTERM to weir-server (pid $SERVER_PID)..."
        kill -TERM "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
        info "weir-server exited."
    fi
    rm -rf "$TMPDIR_ROOT"
}
trap cleanup EXIT

mkdir -p "$WAB_DIR" "$SOCKET_DIR"
chmod 700 "$WAB_DIR"

# ── Build ─────────────────────────────────────────────────────────────────────
if [[ "${RELEASE:-0}" == "1" ]]; then
    info "Building weir-server (release)..."
    cargo build --release -p weir-server 2>&1 | tail -5
    BINARY="target/release/weir-server"
else
    info "Building weir-server (debug)..."
    cargo build -p weir-server 2>&1 | tail -5
    BINARY="target/debug/weir-server"
fi
[[ -x "$BINARY" ]] || fail "Binary not found: $BINARY"
info "Binary: $BINARY"

# ── Write config ──────────────────────────────────────────────────────────────
cat > "$CONFIG_PATH" <<EOF
[server]
socket_path = "$SOCKET_PATH"
wab_dir     = "$WAB_DIR"
metrics_port = $METRICS_PORT
shard_count  = 1
worker_count = 2
batch_size   = 100
batch_deadline_ms = 50
log_level    = "info"
EOF

# ── Start daemon ──────────────────────────────────────────────────────────────
info "Starting weir-server..."
"$BINARY" --config "$CONFIG_PATH" > "$LOG_PATH" 2>&1 &
SERVER_PID=$!
info "weir-server pid: $SERVER_PID"

# ── Wait for socket to appear (health-check polling) ─────────────────────────
info "Waiting for socket to be ready..."
DEADLINE=$((SECONDS + 15))
until [[ -S "$SOCKET_PATH" ]] || (( SECONDS >= DEADLINE )); do
    sleep 0.2
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        error "weir-server exited prematurely. Logs:"
        cat "$LOG_PATH"
        exit 1
    fi
done
if [[ ! -S "$SOCKET_PATH" ]]; then
    error "Socket did not appear within 15 seconds. Logs:"
    cat "$LOG_PATH"
    exit 1
fi
info "Socket ready: $SOCKET_PATH"

# ── Health check via push_simple (0 records = just connects) ─────────────────
info "Running health_check example..."
cargo run -q -p weir-client --example health_check -- \
    --socket "$SOCKET_PATH" \
    || fail "health_check example failed"
info "Health check: OK"

# ── Push records across all durability tiers ──────────────────────────────────
info "Pushing 10 records per durability tier (30 total)..."
cargo run -q -p weir-client --example push_simple -- \
    --socket "$SOCKET_PATH" \
    --count 10 \
    || fail "push_simple example failed"
info "All 30 records acked."

# ── Metrics endpoint ──────────────────────────────────────────────────────────
info "Checking metrics endpoint (http://127.0.0.1:$METRICS_PORT/metrics)..."
sleep 0.5  # allow metrics to flush
METRICS=$(curl -sf "http://127.0.0.1:$METRICS_PORT/metrics") \
    || fail "Metrics endpoint did not respond"

# Verify key metrics are present.
for metric in \
    weir_records_accepted_total \
    weir_records_ack_total \
    weir_queue_depth \
    weir_wab_bytes_on_disk \
    weir_drain_state; do
    if echo "$METRICS" | grep -q "$metric"; then
        info "  ✓ $metric"
    else
        fail "Metric missing from /metrics output: $metric"
    fi
done

# ── Verify WAB segments were written ─────────────────────────────────────────
info "Checking WAB directory for sealed segments..."
SEALED_COUNT=$(find "$WAB_DIR" -name "*.wab.sealed" | wc -l | tr -d ' ')
if [[ "$SEALED_COUNT" -gt 0 ]]; then
    info "  ✓ Found $SEALED_COUNT sealed WAB segment(s)."
else
    # Records may still be in the active segment if batch hasn't rotated yet.
    ACTIVE_COUNT=$(find "$WAB_DIR" -name "*.wab" | wc -l | tr -d ' ')
    if [[ "$ACTIVE_COUNT" -gt 0 ]]; then
        info "  ✓ Found $ACTIVE_COUNT active WAB segment(s) (not yet rotated — OK for small batches)."
    else
        fail "No WAB segments found in $WAB_DIR — records were not written to disk."
    fi
fi

# ── Graceful shutdown ─────────────────────────────────────────────────────────
info "Sending SIGTERM..."
kill -TERM "$SERVER_PID"
DEADLINE=$((SECONDS + 10))
while kill -0 "$SERVER_PID" 2>/dev/null && (( SECONDS < DEADLINE )); do
    sleep 0.2
done
if kill -0 "$SERVER_PID" 2>/dev/null; then
    warn "weir-server did not exit within 10 seconds — sending SIGKILL"
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    fail "Graceful shutdown timed out"
fi
SERVER_PID=""  # prevent double-kill in cleanup
info "weir-server exited cleanly."

echo ""
echo -e "${GREEN}✓ All smoke checks passed.${NC}"
