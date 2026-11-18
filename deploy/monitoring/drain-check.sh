# shellcheck shell=bash
# The smoke test's "drain state = Draining" check, sourced by smoke-test.sh and
# run as shipped by crates/weir-server/tests/deploy.rs against a stub Prometheus.
# Needs the caller's promq, gt and check (smoke-test.sh defines them).
#
# drain_check <expected>: every non-chaos instance Prometheus scrapes (up == 1)
# must report weir_drain_state{state="draining"} = 1, there must be at least
# <expected> such instances (1 for the core stack, 5 with --levels), and the
# core `weir` instance must be one of them.
#
# The instance count comes from `up`, not from the drain series under test: a
# count over the series that exist leaves an instance whose family is missing
# out of both sides, and 4 draining of 4 exporting passed with 5 instances up.
# A drain that has exited (state="stopped") or a missing family both leave the
# draining sum below the number of instances up.
drain_check() {
  local expected=$1 instances draining
  instances=$(promq 'count(up{job!="weir-chaos"} == 1)')
  draining=$(promq 'sum(weir_drain_state{job!="weir-chaos", state="draining"})')
  [ "$(promq 'sum(weir_drain_state{job="weir", state="draining"})')" = "1" ] \
    && ! gt "$expected" "$instances" && [ "$draining" = "$instances" ]
  check "drain state = Draining (non-chaos, $draining/$instances draining, $expected expected)" $?
}
