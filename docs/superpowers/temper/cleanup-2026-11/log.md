# cleanup-2026-11: log

## 2026-11-05: launch

- Base `main` 5e32265. Epic branch `epic/cleanup-2026-11`.
- No build stage: a stabilisation pass, so the run starts at stage 4 (`temper:stabilise`), then stage 5.
- Build host beast; every gate runs as a jobq job (`~/agents/CLAUDE.md`, 31 Oct): agents submit with `jobq submit` and wait with `jobq wait`.
- Overnight: `overnight_fix_default` critical+important; severe items and owner items go to the morning report (`severe_contact: report`).

## 2026-11-05 03:04: sweep stopped at the 5-hour limit, resumed

- Run wf_43ec56e9-e92 (part sweep): 8 of 20 agents finished; 12 reviewers/verifiers died on the usage limit. Their 5 orphaned jobq jobs (still queued) were cancelled by the head, then the run was resumed with the same args (`done` [] — the sweep commits nothing).
- Smoke: the quick gate on `main` 5e32265 passed inside a jobq job (`GATE quick rc=0`), so the base is green.

## 2026-11-05 07:00: morning report

**Stage:** 4a (sweep) still running; no fix has been made yet, so the branch has no code changes. Usage at 07:00: 5-hour 74%, weekly 72% (stop at 90%).

**Why it's slow:** the first sweep run lost 12 of 20 agents at the 5-hour limit (01:15–03:00). On resume, six lenses (durability and ack semantics, wab and recovery, drain and dead letters, wire socket and client, failure paths and lifecycle, weir-ctl and attest) returned blocked: their dead reviewers had left uncommitted probe edits in their throwaway worktrees (temper known issue, failure-paths-and-lifecycle-03), so the new reviewers refused to reuse them. Nothing was deleted; those six lenses are re-running under new labels (`<lens> r2`, run wf_2e021447-3b2). beast's jobq was also near its memory budget with Cairn and semblance work, so verifiers queue.

**Verified so far (reproduced on beast), before the remaining verifiers report:**
- important: `deploy/smoke-test.sh` metric check is flaky under `pipefail` (`echo | grep -q` SIGPIPE; 510/1000 false "missing").
- important: `deploy/smoke-test.sh` never reads the daemon's exit status on SIGTERM, so a failing shutdown passes.
- important / critical: several config/startup findings (config-reload-and-startup-01..07) and a status/admin URL-redaction finding are mid-verification; the critical ones are secret-leak class (sink URL secrets in logs, a TOML parse error echoing the offending line, `url_origin` passing a password through). One critical candidate was refuted.
- minor: docs.yml `workflow_dispatch` never deploys; chaos SETUP.md mislabels power-loss runs as kill -9 only.

**Severe items:** none verified.
**Owner items:** none yet.
**Flaky reruns:** none.

**Decision (head):** the run continues past 07:00 because the owner asked for the cleanup to be done in this session and weekly usage is under the 90% stop; fixes start when both sweeps have returned, critical+important only, one at a time with a regression check per fix.

## 2026-11-05 08:05: sweep closed, fixes launched (4b)

- Both sweep runs hit the 5-hour limit again (wf_43ec56e9-e92 finished 21/28 agents; the r2 rerun wf_2e021447-3b2 lost all 6). The head cancelled the runs' 5 orphaned queued jobq jobs by id. Much of the delay was jobq: the run's jobs sat for hours "reserved for" a 10G semblance job.
- **Decision (head):** weekly usage 76% against a 90% stop, and each sweep round has cost ~15% of the week, so the sweep is closed as it stands and the verified fixes go first. `sweeps: 1` for this run (no second sweep), to leave budget for stage 5.
- **Selected (verified, critical+important, kind fix):** config-reload-and-startup-02 (critical: non-UTF-8 WEIR_* env value printed verbatim, secrets included), config-reload-and-startup-04, deploy-ci-and-chaos-harness-01, -02, -03, and -06. -06 is the `deploy/smoke-test.sh` pipefail finding that the first deploy reviewer reported and its verifier reproduced (important); the resumed reviewer of that lens did not report it again, so the head carried it over under a new id.
- **Unreviewed lenses (open, for a later run):** failure paths and lifecycle, durability and ack semantics, wab and recovery, drain and dead letters, wire socket and client, sinks, weir-ctl and attest. Their throwaway worktrees (and the r2 ones) hold dead agents' probe edits; left in place for the owner/cleanup.
- **Unverified, not fixed overnight (to the owner):** critical: security-01 (HTTP sink URL path/query secrets logged in plain text at startup and on every transient retry), config-reload-and-startup-01 (TOML parse error echoes the offending line, credentials included), status-metrics-and-admin-01 (`url_origin` passes a password through when no `@host` follows); important: config-reload-and-startup-03 (no preflight check that the producer socket's parent can be bound), test-strength-and-docs-02 (`clean_disconnect_does_not_panic` cannot fail).
- **Minor (open items):** deploy-ci-and-chaos-harness-04 (bench header reports /run's mount options for tmpfs /tmp), -05 (smoke test says 30 records, pushes 20), the first sweep's docs.yml workflow_dispatch and chaos SETUP.md mislabelling; config-reload-and-startup-05/06/07, test-strength-and-docs-01 (unverified minors).

## 2026-11-05 ~12:00: paused by the owner; 2026-11-07 14:00: resumed

- Paused at the owner's request: the fix run wf_689c50be-017 was stopped and its running quick gate (cfg04-quick) cancelled. fix:config-reload-and-startup-02 had landed (4ac45c4); the config-reload-and-startup-04 fix was mid-way, its edits uncommitted in the epic worktree on beast (left in place).
- Resumed 7 Nov with beast's jobq idle and a fresh usage budget: done = [fix:config-reload-and-startup-02], attempt 1, same selection.

## 2026-11-07 14:30: review batch 1 (r3) returned

- Run wf_1b8a2c07-b7f, lenses durability and ack semantics r3, wab and recovery r3, drain and dead letters r3: all 8 agents finished; 5 findings verified, 0 refuted, none blocked. Drain and dead letters r3 reported nothing that survived.
  - critical wab-and-recovery-r3-01: recovery treats a short payload read (a corrupted length within the cap) as a torn tail and truncates without a quarantine copy, destroying acked records after it.
  - important durability-and-ack-semantics-r3-01: wab_dir is never fsynced after creating shard_NN, so the first Durable acks on a fresh wab_dir aren't crash-durable.
  - important durability-and-ack-semantics-r3-02: quarantine/ is created without a wab_dir fsync before the original is durably truncated.
  - important wab-and-recovery-r3-02: an unreadable foreign subdirectory of wab_dir (lost+found on a mount root) aborts startup.
  - important wab-and-recovery-r3-03: a failed timed seal drops the active segment instead of keeping it, stranding acked records until restart.
- Selected for a second fix run (critical+important), after the first fix run returns: the epic worktree takes one fix run at a time.
- Review batch 2 launched (wf_0cdc0916-764): wire socket and client r3, failure paths and lifecycle r3, sinks r3, weir-ctl and attest r3.

## 2026-11-07 ~15:40: fix round 1 stable

- wf_689c50be-017 returned `stable`: fixed config-reload-and-startup-04 (c1b8dd1), deploy-ci-and-chaos-harness-01 (212f3e7), -02 (7413c0e), -03 (afde04e), -06 (8c263ab), plus -02 config earlier (4ac45c4). Every first-wave fix had its regression check: no regressions, none reverted, none blocked. Every sha verified on the branch after the fetch-back.
- **Flaky rerun (flagged):** fix:deploy-ci-and-chaos-harness-02's quick gate failed once on `weir-server system::sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` (ECONNRESET instead of EOF on an idle connection, system.rs:6183; jq-01M6V6SJAE0K2G3Z0SZHZQQRJJ) and passed on a rerun (jq-01M6V7413Z2CFXZETJPPFKEHC7). That test is not on the project's flaky list, so the rerun broke temper's rule; the head records it here, and the full gate (never rerun) decides. The fix touched only deploy/smoke-test.sh, so the failure is not caused by it; a possible pre-existing flake in the SIGTERM path, for the owner.
- Fix round 2 launched with the five batch-1 findings (attempt 2).
