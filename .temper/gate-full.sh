#!/usr/bin/env bash
# temper's full gate (the checkpoint's and publish's), run inside a worktree.
#   bash .temper/gate-full.sh
# It takes an hour or more, so run it detached and poll
# <logs>/<worktree>/latest-full/summary.txt for its last line, `GATE full rc=N`.
#
# 1. CI's lint, test, msrv, advisories and dst jobs, natively in the gate slot
#    (cargo, with the slot's target dir and the shared compiler cache).
# 2. CI's container jobs under act, in a clean clone of HEAD (act needs a real
#    .git, and the jobs write into the tree): conformance, harnesses,
#    sink-integration and monitoring from ci.yml, docker.yml's smoke and
#    docs.yml's build.
# 3. The jobs act can't run, in docker: the windows job (cargo-xwin) and the
#    release build matrix (cargo-zigbuild for both apple-darwin targets and
#    aarch64-linux, native x86_64-linux plus its TLS smoke).
# Left out: the load, bench and drain jobs (measurements, not checks; load
# commits baselines on main), and the darwin smoke and native macOS tests,
# which need a Mac: the head runs those before a push.
# Runs every step and reports all failures.
set -uo pipefail
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
top=$(git rev-parse --show-toplevel)
GATE_NAME=full
. "$top/.temper/gate-slot.sh"
ACT=${WEIR_ACT:-act}
SHA=$(git rev-parse HEAD)
SRV='--lib --test system --test docs_drift --test demo_drift --test load --test load_tls --test tls_client --test tls_listener --test deploy'

# --- 1. native -------------------------------------------------------------
step fmt               'cargo fmt --check'
step clippy-default    'cargo clippy --all-targets -- -D warnings'
step clippy-all        'cargo clippy --all-targets --all-features -- -D warnings'
step clippy-nodefault  'cargo clippy --all-targets --no-default-features -- -D warnings'
step clippy-s3         'cargo clippy --all-targets --features weir-server/s3-sink -- -D warnings'
step demo-version      './scripts/sync-demo-version.sh && git diff --exit-code demo/version.js'
step test-ws           'cargo test --workspace --exclude weir-server'
step test-s3           'cargo test -p weir-sink-s3'
step test-srv          "cargo test -p weir-server $SRV"
step test-srv-bins     'cargo test -p weir-server --bins -- --test-threads=1'
step test-ws-all       'cargo test --workspace --exclude weir-server --all-features'
step test-srv-all      "cargo test -p weir-server $SRV --all-features"
step test-srv-bins-all 'cargo test -p weir-server --bins --all-features -- --test-threads=1'
step load-norun        'cargo test -p weir-server --test load --no-run && cargo test -p weir-server --test load_drain --no-run'
step msrv              'rustup run 1.88 cargo check --workspace --all-features --locked'
step deny              'cargo deny fetch && cargo deny check advisories bans licenses sources'
step dst-300           'WEIR_DST_SWEEP=300 cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1'

# --- 2. act, in a clean clone of HEAD ---------------------------------------
clone="$tmp_root/act-src"
rm -rf "$clone"
git init -q "$clone" && git -C "$clone" fetch -q "$top" HEAD && git -C "$clone" checkout -q --detach FETCH_HEAD
if [ "$(git -C "$clone" rev-parse HEAD 2>/dev/null)" != "$SHA" ]; then
  echo "gate-full: could not make a clean clone of $SHA" | tee -a "$OUT/summary.txt"; FAILED=$((FAILED + 1))
else
  # docker.yml's smoke bind-mounts files it makes with mktemp. Under act those
  # paths exist only inside the job container and the host daemon mounts an
  # empty dir instead, so that job gets a TMPDIR that is the same path on the
  # host and in the container, emptied afterwards through docker (the files
  # belong to container uids).
  shared="$tmp_root/act-tmp"
  act_job() {  # act_job <workflow> <job>
    local extra=''
    if [ "$1" = docker.yml ]; then
      docker rm -f weir-smoke >/dev/null 2>&1; mkdir -p "$shared"
      extra="--env TMPDIR=$shared --container-options '-v $shared:$shared'"
    fi
    step "act-${1%.yml}-$2" "cd '$clone' && $ACT -W .github/workflows/$1 -j $2 --env CARGO_BUILD_JOBS=$JOBS $extra"
    if [ -n "$extra" ]; then
      docker run --rm -v "$shared:/s" alpine sh -c 'rm -rf /s/* /s/.[!.]*' >/dev/null 2>&1; rmdir "$shared" 2>/dev/null
    fi
  }
  for j in conformance harnesses sink-integration monitoring; do act_job ci.yml "$j"; done
  act_job docker.yml smoke
  act_job docs.yml build
fi

# --- 3. what act can't run, in docker ---------------------------------------
# One set of docker volumes per slot: toolchains and caches persist, and the
# slot's touch of the worktree's sources keeps cargo's freshness check honest.
slot=$(basename "$T")
run_in() {  # run_in <key> <image> <script>
  printf '%s' "docker run --rm --cpus $JOBS -e CARGO_BUILD_JOBS=$JOBS -e CARGO_TERM_COLOR=never \
    -e CARGO_TARGET_DIR=/target -v '$top:$top:ro' -w '$top' \
    -v weir-xplat-$1-cargo:/usr/local/cargo -v weir-xplat-$1-rustup:/usr/local/rustup \
    -v weir-xplat-$1-cache:/root/.cache -v weir-xplat-$1-target-$slot:/target \
    $2 bash -c $(printf '%q' "$3")"
}
PREP='set -e; rustup -q update stable --no-self-update >/dev/null; rustup -q default stable; rustc --version'
step windows "$(run_in xwin messense/cargo-xwin:latest "$PREP; rustup -q target add x86_64-pc-windows-msvc
set -x
cargo xwin build --release --target x86_64-pc-windows-msvc -p weir-client --features tls
cargo xwin check --target x86_64-pc-windows-msvc -p weir-core -p weir-wab -p weir-sink-sdk -p weir-status --all-features --locked")"
for t in x86_64-apple-darwin aarch64-apple-darwin aarch64-unknown-linux-gnu; do
  step "build-$t" "$(run_in zig ghcr.io/rust-cross/cargo-zigbuild:latest "$PREP; rustup -q target add $t
set -x
cargo zigbuild --release --target $t -p weir-server --features tls
cargo zigbuild --release --target $t -p weir-client --features tls")"
done
step build-x86_64-unknown-linux-gnu '
set -e
cargo build --release --target x86_64-unknown-linux-gnu -p weir-server --features tls
cargo build --release --target x86_64-unknown-linux-gnu -p weir-client --features tls
B=$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/weir-server; WAB=$(mktemp -d)
O=$("$B" --tcp-bind 127.0.0.1:17100 --wab-dir "$WAB" 2>&1) || true; echo "$O"; rm -rf "$WAB"
if printf %s "$O" | grep -q "without the '"'"'tls'"'"' feature"; then echo "TLS smoke FAILED"; exit 1; fi
echo "TLS smoke OK"'

rm -rf "$clone"
finish
