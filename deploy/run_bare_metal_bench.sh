#!/usr/bin/env bash
#
# run_bare_metal_bench.sh — capture a bare-metal benchmark run.
#
# Builds release, captures the environment metadata that makes the
# numbers reproducible, runs the load suite 5× at each of
# batch_deadline_ms ∈ {1, 2}, feeds the JSONL through
# avg_benchmarks.py, and writes a self-contained markdown doc to stdout
# (env header + result tables).
#
# Usage:
#   deploy/run_bare_metal_bench.sh > docs/benchmarks/bare-metal.md
#
# Exit codes:
#   0  success
#   1  build / test failure
#   2  prerequisite missing
#
# This script does NOT modify governor / SMT / turbo state — that needs
# root and varies by distro. It REPORTS them so the captured numbers
# can be interpreted. If you want maximum determinism, set them by hand
# before running:
#
#   sudo cpupower frequency-set -g performance
#   echo off | sudo tee /sys/devices/system/cpu/smt/control
#
# and revert after.

set -euo pipefail

# ── Prerequisites ─────────────────────────────────────────────────────────────

for cmd in cargo python3 awk uname; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: missing required command: $cmd" >&2
    exit 2
  fi
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f Cargo.toml ]]; then
  echo "error: not at repo root — Cargo.toml missing" >&2
  exit 2
fi

# ── Capture environment to stderr-tracked tempfiles ───────────────────────────

WORKDIR="$(mktemp -d -t weir-bench-XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT
JSONL="$WORKDIR/results.jsonl"
TABLES_MD="$WORKDIR/tables.md"
: >"$JSONL"

now_utc() { date -u '+%Y-%m-%d %H:%M:%S UTC'; }

read_sysctl() {
  # Linux only; quiet failure on macOS / missing files.
  local key="$1"
  [[ -r "/proc/sys/${key//.//}" ]] && cat "/proc/sys/${key//.//}" 2>/dev/null || echo "<unavailable>"
}

cpu_model() {
  if [[ -r /proc/cpuinfo ]]; then
    awk -F': ' '/^model name/ {print $2; exit}' /proc/cpuinfo
  elif command -v sysctl >/dev/null 2>&1; then
    sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "<unknown>"
  else
    echo "<unknown>"
  fi
}

cpu_microcode() {
  if [[ -r /proc/cpuinfo ]]; then
    awk -F': ' '/^microcode/ {print $2; exit}' /proc/cpuinfo
  else
    echo "<unavailable>"
  fi
}

core_count() { getconf _NPROCESSORS_ONLN 2>/dev/null || echo "<unknown>"; }

mem_total_mib() {
  if [[ -r /proc/meminfo ]]; then
    awk '/^MemTotal:/ {printf "%d", $2/1024}' /proc/meminfo
  else
    echo "<unknown>"
  fi
}

governor() {
  local g="/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
  [[ -r "$g" ]] && cat "$g" || echo "<unavailable>"
}

smt_state() {
  local s="/sys/devices/system/cpu/smt/active"
  [[ -r "$s" ]] && { [[ "$(cat "$s")" == "1" ]] && echo on || echo off; } || echo "<unavailable>"
}

turbo_state() {
  # Intel pstate path; AMD has a different one.
  local t="/sys/devices/system/cpu/intel_pstate/no_turbo"
  if [[ -r "$t" ]]; then
    [[ "$(cat "$t")" == "0" ]] && echo on || echo off
  else
    echo "<unavailable>"
  fi
}

mitigations_state() {
  if [[ -r /proc/cmdline ]] && grep -q 'mitigations=off' /proc/cmdline; then
    echo off
  else
    echo "on (default)"
  fi
}

# WAB directory inspection. The script doesn't know the production
# wab_dir, so it reports info on the temp dir used by the load suite
# (cargo test creates wab dirs under /tmp by default).
wab_test_root="${TMPDIR:-/tmp}"

filesystem_type() {
  df -T "$wab_test_root" 2>/dev/null | awk 'NR==2 {print $2}' || echo "<unknown>"
}

mount_options() {
  local dev
  dev=$(df "$wab_test_root" 2>/dev/null | awk 'NR==2 {print $1}') || return
  awk -v d="$dev" '$1 == d {print $4; exit}' /proc/mounts 2>/dev/null || echo "<unavailable>"
}

block_device_line() {
  if command -v lsblk >/dev/null 2>&1; then
    lsblk -d -o NAME,MODEL,ROTA,TRAN 2>/dev/null | awk 'NR==1 || NR<=6'
  else
    echo "lsblk not available"
  fi
}

# ── Build ─────────────────────────────────────────────────────────────────────

echo "building weir-server release (logged to $WORKDIR/build.log) ..." >&2
if ! cargo build --release -p weir-server >"$WORKDIR/build.log" 2>&1; then
  echo "error: release build failed — see $WORKDIR/build.log" >&2
  tail -40 "$WORKDIR/build.log" >&2
  exit 1
fi

# ── Run the load suite, 5× at each deadline ───────────────────────────────────

PASSES="${WEIR_BENCH_PASSES:-5}"
DEADLINES=(1 2)

for d in "${DEADLINES[@]}"; do
  for pass in $(seq 1 "$PASSES"); do
    echo "load suite: deadline=${d}ms pass ${pass}/${PASSES} ..." >&2
    if ! WEIR_BENCH_DEADLINE="$d" \
        cargo test --release -p weir-server --test load -- --nocapture \
        2>/dev/null \
        | grep '^BENCH: ' >>"$JSONL"; then
      echo "error: load suite failed (deadline=${d}, pass=${pass})" >&2
      exit 1
    fi
  done
done

# ── Render the tables via avg_benchmarks.py ───────────────────────────────────

python3 deploy/avg_benchmarks.py "$JSONL" "$TABLES_MD" >/dev/null

# ── Emit the combined doc on stdout ───────────────────────────────────────────

cat <<EOF
# Bare-metal benchmark results

Captured: $(now_utc)
Host: $(hostname)
CPU: $(cpu_model) — $(core_count) logical cores, microcode $(cpu_microcode)
Memory: $(mem_total_mib) MiB
Kernel: $(uname -srm)
libc: $(getconf GNU_LIBC_VERSION 2>/dev/null || echo "<unavailable>")

Storage (load-suite wab dirs land in ${wab_test_root}):
  Filesystem: $(filesystem_type)
  Mount opts: $(mount_options)
  Block devices:
\`\`\`
$(block_device_line)
\`\`\`

Tunables:
  Governor: $(governor)
  SMT: $(smt_state)
  Turbo: $(turbo_state)
  CPU mitigations: $(mitigations_state)
  vm.dirty_background_bytes: $(read_sysctl vm.dirty_background_bytes)
  vm.dirty_bytes: $(read_sysctl vm.dirty_bytes)
  vm.dirty_expire_centisecs: $(read_sysctl vm.dirty_expire_centisecs)

Run config: ${PASSES} passes × ${#DEADLINES[@]} deadlines (${DEADLINES[*]} ms)

---

EOF

# avg_benchmarks.py wrote a full doc with its own H1; strip that so the
# env header above remains the only top-level heading.
awk '
  /^# Benchmark Results/ { skip = 1; next }
  skip && /^$/           { skip = 0; next }
  skip                   { next }
  { print }
' "$TABLES_MD"
