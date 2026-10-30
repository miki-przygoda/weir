#!/usr/bin/env bash
# temper's quick gate, run inside a worktree after every task and every merge.
#   bash .temper/gate-quick.sh
#
# On a task branch (temper/<epic>/<id>) it checks what the task changed against
# where the branch left epic/<epic>; after a merge commit, what the merge brought
# in; anywhere else, the whole quick set. Always: fmt and clippy over the whole
# workspace (so a break in a dependent crate fails here). Then, per changed
# crate, all-features clippy and its tests; weir-server's tests are its lib,
# bins, system test and the drift guards. A change outside crates/ that the
# drift guards read (docs, deploy, demos, READMEs) runs the drift guards. A
# change to the root Cargo.toml or Cargo.lock runs the whole set.
# Stops at the first failure. The full gate runs everything CI runs.
set -uo pipefail
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
top=$(git rev-parse --show-toplevel)
GATE_NAME=quick STOP_ON_FAIL=1
. "$top/.temper/gate-slot.sh"

SRV_TESTS='--lib --test system --test docs_drift --test demo_drift --test deploy'
DRIFT='cargo test -p weir-server --test docs_drift --test demo_drift --test deploy'

whole() {
  step fmt 'cargo fmt --check'
  step clippy 'cargo clippy --all-targets -- -D warnings'
  step clippy-all 'cargo clippy --all-targets --all-features -- -D warnings'
  step test-ws 'cargo test --workspace --exclude weir-server --all-features'
  step test-srv "cargo test -p weir-server $SRV_TESTS"
  step test-srv-bins 'cargo test -p weir-server --bins -- --test-threads=1'
  finish
}

branch=$(git rev-parse --abbrev-ref HEAD)
case "$branch" in
  temper/*/*)
    epic=${branch#temper/}; epic=${epic%%/*}
    if ! git rev-parse --verify --quiet "epic/$epic" >/dev/null; then
      echo "gate-quick: $branch: no branch epic/$epic to compare with" | tee -a "$OUT/summary.txt"
      FAILED=1; finish
    fi
    base=$(git merge-base HEAD "epic/$epic") ;;
  *)
    if git rev-parse --verify --quiet HEAD^2 >/dev/null; then
      base=HEAD^1
      echo "gate-quick: merge $(git log -1 --format=%s): the checks of what it brought in" | tee -a "$OUT/summary.txt"
    else
      echo "gate-quick: $branch: the whole quick set" | tee -a "$OUT/summary.txt"
      whole
    fi ;;
esac

files=$( { git diff --name-only "$base"; git ls-files --others --exclude-standard; } | sort -u)
if printf '%s\n' "$files" | grep -qxE 'Cargo\.toml|Cargo\.lock|rust-toolchain(\.toml)?|\.temper/.*'; then
  echo "gate-quick: the workspace manifest, lockfile or gates changed: the whole quick set" | tee -a "$OUT/summary.txt"
  whole
fi
crates=$(printf '%s\n' "$files" | awk -F/ '$1 == "crates" && NF > 2 { print $2 }' | sort -u)
drift=no
printf '%s\n' "$files" | grep -qE '^(docs/|deploy/|demo/|demos/|README\.md|CHANGELOG\.md|crates/[^/]+/README\.md)' && drift=yes

echo "gate-quick: $branch: crates: ${crates:-none}; drift guards: $drift" | tee -a "$OUT/summary.txt"
step fmt 'cargo fmt --check'
[ -z "$crates" ] && [ "$drift" = no ] && finish
step clippy 'cargo clippy --all-targets -- -D warnings'
for c in $crates; do
  step "clippy-all-$c" "cargo clippy -p $c --all-targets --all-features -- -D warnings"
  if [ "$c" = weir-server ]; then
    step test-srv "cargo test -p weir-server $SRV_TESTS"
    step test-srv-bins 'cargo test -p weir-server --bins -- --test-threads=1'
    drift=covered
  else
    step "test-$c" "cargo test -p $c --all-features"
  fi
done
[ "$drift" = yes ] && step drift "$DRIFT"
finish
