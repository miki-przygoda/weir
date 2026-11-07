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

## 2026-11-07 15:35: review batch 2 (r3) returned

- Run wf_0cdc0916-764 (wire socket and client r3, failure paths and lifecycle r3, sinks r3, weir-ctl and attest r3): all 21 agents finished; 17 findings verified, 0 refuted, none blocked.
- **Owner items (not fixed):** sinks-r3-01 (critical): ClickHouse `insert_deduplication_token` is the content-only DedupToken, pinned to the 1.x value by a test, so two distinct batches with identical bytes collide and ClickHouse silently drops the second; changing the token is a compatibility decision. wire-socket-and-client-r3-03 (minor): `PayloadTooLarge` reports a record count as bytes (public error type).
- **Selected for fix round 3 (critical+important, kind fix):** sinks-r3-02 (HTTP NDJSON Idempotency-Key collides for byte-identical batches), sinks-r3-03 (ClickHouse transport error logs a query-string password), failure-paths-and-lifecycle-r3-01 (preflight passes a socket whose parent can't be bound, so the drain delivers before the bind fails), sinks-r3-04 (S3 wrong-region 400 dead-letters the backlog), sinks-r3-05 (Postgres read-only 25006 dead-letters), sinks-r3-06 (MySQL 1040/1053 dead-letter), weir-ctl-and-attest-r3-01/-02/-03 (attest false BROKEN_LINK on out-of-order deletes, lost+found, a segment deleted mid-run), wire-socket-and-client-r3-01 (client never checks the whole PushBatch frame size), wire-socket-and-client-r3-02 (docs say batch and payload limits multiply).
- **Minor (open items):** weir-ctl-and-attest-r3-04 (.attest sidecars never removed), wire-socket-and-client-r3-04 (nack counters count frames, not records), -05 and -06 (TS/Python demo clients mis-handle a HealthCheckResponse and an empty Nack).
- A fix that would change a pinned contract returns `contract` and goes to the owner, never into a quiet fix.

## 2026-11-07 16:40: fix round 2 stable

- wf_02b45796-636 returned `stable`: fixed wab-and-recovery-r3-01 (1aa2c4f), durability-and-ack-semantics-r3-01 (04f0d4c), -r3-02 (1b004da), wab-and-recovery-r3-02 (f7efcc9), -r3-03 (5cb6066). Regression check per fix: none; none reverted, none blocked, no flaky reruns. Shas verified on the branch.
- Fix round 3 launched with the 11 batch-2 findings (attempt 3), and a `security r3` re-review for the first night's unverified secret leaks.

## 2026-11-07 ~17:00: security re-review (r3) returned

- Run wf_85b3fbd2-dc9 (security r3, status metrics and admin r3): all 10 agents finished; 8 findings verified, 0 refuted. The two lenses found the same four defects, so the head deduplicated them to four:
  - critical security-r3-01 (= status-metrics-and-admin-r3-01): HTTP sink URL secrets in the path or in query keys outside the 10-key denylist (`?code=`, `X-Amz-Signature`, `X-Amz-Security-Token`, `client_secret`) reach the startup INFO line, every transport-error retry and the admin /status sink-failure text.
  - critical security-r3-02 (= status-metrics-and-admin-r3-03): a TOML syntax error prints the offending line, so a credential on it reaches stderr.
  - critical status-metrics-and-admin-r3-02 (= security-r3-03, rated minor by its other verifier; the head keeps critical, per the severity notes: a secret in a log and /status): `url_origin` echoes `user:password` when no `@host` follows.
  - important security-r3-04 (= status-metrics-and-admin-r3-04): `drain_state_and_sink_health_pre_initialised` cannot fail, since the HELP text always contains "draining".
- These confirm the first night's unverified security-01, config-reload-and-startup-01, status-metrics-and-admin-01 and the metrics-test finding. Selected for fix round 4, after round 3 (same epic worktree).

## 2026-11-07 ~18:45: fix round 3 handover, owner decisions

- wf_2eaee17b-c89 returned `handover`. Fixed: sinks-r3-03 (126e8a3), failure-paths-and-lifecycle-r3-01 (e358b82), sinks-r3-04 (ed290f8), sinks-r3-05 (05b1d00), sinks-r3-06 (ade74f3), weir-ctl-and-attest-r3-01 (72bae97), -r3-02 (f1a1e0d), -r3-03 (fe31ac1), wire-socket-and-client-r3-01 (87a6332), -r3-02 (4228ac2). Contract: sinks-r3-02. No flaky reruns. Shas verified after the fetch-back.
- Regressions the per-fix checks proved:
  - fix:sinks-r3-05: docs regression (minor): `configuration.md`'s Postgres classification table and the `postgres.rs` module doc still say 25006 dead-letters. Goes into fix round 4 as `postgres-docs-25006`.
  - fix:weir-ctl-and-attest-r3-01 (72bae97): weakens tamper detection: any file named `seg_N.wab.confirmed` (even empty) or a junk-named `quarantine/` entry excuses a gap, so `rm` + `touch` hides a deleted segment (verify exit 0, was 1). A security decision.
  - fix:weir-ctl-and-attest-r3-03 (fe31ac1): under a drain race after the cached disposed set, an honest exit 2 becomes a false BROKEN_LINK (exit 1).
- **Owner decisions (7 Nov, asked in session):**
  - Attest: **revert both** 72bae97 and fe31ac1 (head: e8a93fb, 523afa5; the CHANGELOG conflict resolved by dropping only 72bae97's bullet). The original false-positive (out-of-order drain deletes read as BROKEN_LINK) and the deleted-mid-run NotFound go back to the open owner items for a proper design. f1a1e0d (lost+found) stays.
  - Dedup keys: **owner items for now.** sinks-r3-02 (HTTP NDJSON batch Idempotency-Key, content-only) and sinks-r3-01 (ClickHouse insert_deduplication_token, pinned to 1.x) stay unchanged in this PR and are written up with their evidence; a coordinate-based key (like S3's) is a separate change with a `Changed` note.
- Fix round 4 launched: security-r3-01, security-r3-02, status-metrics-and-admin-r3-02, security-r3-04, postgres-docs-25006 (attempt 4).

## 2026-11-07 ~20:00: fix round 4 stable, stage 4 done

- wf_ebee96d0-462 returned `stable`: security-r3-01 (9c7b33e), security-r3-02 (4179b12), status-metrics-and-admin-r3-02 (972b42f), postgres-docs-25006 (ef52f0b), security-r3-04 (e15903d). No regressions, none reverted or blocked, no flaky reruns. Shas verified.
- Stage 4 ends here (`sweeps: 1` per round; every area of the repo was reviewed once across the r1/r3 batches). Stage 5 (publish) launched.

## Open items (for the PR body)

**Owner decisions needed**
- sinks-r3-01 (critical): ClickHouse `insert_deduplication_token` is the content-only DedupToken, pinned to its 1.x value by `dedup_token_is_unchanged_from_weir_1_x`: two distinct batches with identical bytes (e.g. heartbeats) collide inside the dedup window and ClickHouse silently drops the second with HTTP 200. Owner: left for a separate change (7 Nov).
- sinks-r3-02 (critical, returned `contract`): HTTP NDJSON batch mode sends the content-only DedupToken as `Idempotency-Key`, so an idempotent endpoint drops a later byte-identical batch. Owner: left for a separate change with a coordinate-based key like the S3 sink's (7 Nov).
- weir-ctl-and-attest-r3-01 and -r3-03 (important): `attest verify` reports BROKEN_LINK when the drain reclaims a segment out of order, and errors (exit 2) on a segment the drain deletes mid-run. The fixes (72bae97, fe31ac1) were reverted at the owner's call (7 Nov) because they let `rm` + an empty `touch seg_N.wab.confirmed` hide a deleted segment; needs a design that can't be forged by a file name.
- D10: an explicit missing `--config` still only WARNs (deferred decision, unchanged).
- Possible pre-existing flake: `weir-server system::sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` failed once (ECONNRESET instead of EOF, system.rs:6183) in a quick gate on 7 Nov; the full gate decides.

**Minor (not fixed)**
- deploy-ci-and-chaos-harness-04: `run_bare_metal_bench.sh` reports /run's mount options for a tmpfs /tmp. -05: `deploy/smoke-test.sh` says it pushes 30 records across three tiers; it pushes 20 across two.
- docs.yml's `workflow_dispatch` builds the book but never deploys it; chaos `SETUP.md` / `run-soak.sh` label power-loss schedules as "kill -9 only".
- config-reload-and-startup-05/06/07 (unverified): admin socket under-wab_dir check is lexical; the invalid sink type error omits `s3`; `FEATURE_GATED_FLAGS` lacks the mysql/postgres `id_column` flags. test-strength-and-docs-01 (WEIR_DST_SEED printed in the repro line but never read) unverified.
- weir-ctl-and-attest-r3-04: `.attest` sidecars are never removed after their segment drains. wire-socket-and-client-r3-03 (owner, public error type): `PayloadTooLarge` reports a record count as bytes. -r3-04: nack counters count frames, not records. -r3-05/-06: the TS/Python demo clients mis-handle a HealthCheckResponse and an empty Nack.
- Leftover throwaway worktrees under beast's `~/work/weir/cleanup-2026-11/tmp/`, some holding dead agents' probe edits (usage-limit deaths, 5 Nov): for `/temper:cleanup` or by hand.
