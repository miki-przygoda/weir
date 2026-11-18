# release-4-2-0-sweep: log

A temper stabilise sweep (part 4a only: sweep and verification, no fixes) over everything weir 4.2.0
changes since 4.1.0, before the v4.2.0 tag. The owner picks what to fix from the list below.

- Base: `v4.1.0` (09762e7)
- Tip: `main` af7db97 (release PR #79, 1f93f8f, plus a bench-baseline commit)
- Scope: `git diff v4.1.0...sweep/4.2.0` (188 files): PR #77's cleanup and the d10-and-batch-error epic
- Date: 2026-11-18, cc4 box
- Already decided, not defects: D10 as a hard error; the coordinate dedup key and RecordId format
  change; the reverted attest gap fixes (attest-disposal-log); `PayloadTooLarge` count-as-bytes;
  a 2,048-record batch passing the client cap but Nacked by a default daemon.

## Runs

- 2026-11-18 launch stabilise part sweep attempt 0, run wf_30107a5c-7d1 (lenses: security; failure paths and lifecycle; test strength and docs; durability and ack semantics)

## 4a result (run wf_30107a5c-7d1, swept tip c408b22)

`status: "list"`. The four lens reviewers and two verifiers all returned. Nothing was unverified, refuted or blocked.

**Verdict: no verified `severe` or `critical` findings.**

### Findings, by severity

| id | lens | file:line | severity | kind | verified | verifier's reason |
|---|---|---|---|---|---|---|
| test-strength-and-docs-01 | test strength and docs | deploy/monitoring/smoke-test.sh:138 | important | fix | yes | Reproduced by running the check's own helpers and lines 137-142, unchanged, against a stub Prometheus on loopback. The stub had 4 non-chaos draining series and none for weir-med. The check printed `PASS drain state = Draining (non-chaos, 4/4 draining)`, though the fleet has 5 instances, because both counts are taken over the series under test. Only the core `job="weir"` instance is guarded. The levels section checks `up` only. So CHANGELOG ~262 ("fails when a non-chaos instance's series is missing") holds only for core. Fix: compare against `count(up{job!="weir-chaos"})` or another per-instance count. The full compose stack was not run. |
| test-strength-and-docs-02 | test strength and docs | README.md:191 | minor | fix | yes | Cargo.toml says 4.2.0, but README:191 says "stable at 4.1" and docs/getting-started/install.md:269 says "4.1.0 as of 2026-10-26". The verifier also found README:84 (`weir-client = "4.1"`) and README:185 (benchmarks "4.1.0 as of 2026-10-26"). The dated lines are true until 4.2.0 is published; "stable at 4.1" and the dependency line go stale when it ships. Docs only. |

- unverified: none
- refuted: 0
- blocked: none

The security, failure paths and lifecycle, and durability and ack semantics lenses returned `done` with no findings. Their summaries are in the run's journal. They covered:

- secret redaction in sink Debug, errors and redirects, and TOML errors;
- the admin socket symlink check;
- `BYTES_UNKNOWN` failing closed;
- the bounded admission decode;
- recovery fsyncs and the quarantine dir;
- telling a corrupt length from a torn tail;
- the `shard_<N>` filter;
- timed-seal failure;
- the RecordId `created_at`.

The reviewers built nothing. Verifier 01 ran a shell stub with no cargo build.

Throwaway worktrees remain under `.temper/worktrees/release-4-2-0-sweep/tmp/` (6, detached at c408b22). They are left for `temper:cleanup`, as the run skill says, and were not removed by hand.

## 4b (part fix)

- Selected by the owner (18 Nov): both verified findings, test-strength-and-docs-01 (important) and test-strength-and-docs-02 (minor). Nothing goes to open items from 4a.
- 2026-11-18 launch stabilise part fix attempt 0, run wf_de7474c6-a97 (gate_slots 1, done [])

## 4b result (run wf_de7474c6-a97, 14 agents, none errored or empty)

`status: "stable"`. fixed: test-strength-and-docs-01, test-strength-and-docs-02. reverted: none. regressions: none. contract_items: none. blocked: none. unverified: none. flaky_reruns: none. `second_sweep`: severe 0, others 6.

### Fixes

- **test-strength-and-docs-01**: 69ae941 `fix(monitoring): count expected drain instances from up, not the drain series`. The check moves to `deploy/monitoring/drain-check.sh` (sourced by smoke-test.sh, which `cd`s to its own dir, so CI's `deploy/monitoring/smoke-test.sh --teardown` still finds it). It counts instances from `count(up{job!="weir-chaos"} == 1)`, waits for the 1 (or 5 with `--levels`) it started, and requires the core `weir` draining. New stub-promq cases in `crates/weir-server/tests/deploy.rs`. The CHANGELOG 4.2.0 line is reworded to what the check now does. Regression check: none.
  - Proof (head, stub Prometheus on 127.0.0.1, 5 non-chaos instances up, weir-med with no `weir_drain_state`): old check (8b54cb7 lines 135-142, unchanged) `PASS  drain state = Draining (non-chaos, 4/4 draining)`; new check `FAIL  drain state = Draining (non-chaos, 4/5 draining, 5 expected)`; a healthy fleet `PASS ... 5/5 draining, 5 expected`.
- **test-strength-and-docs-02**: c12dcf8 `docs: name 4.2.0 as the current release, and guard it`. README (84, 185, 191), install.md, quickstart.md, crates/weir-rs README and crate docs now name 4.2; a new docs_drift guard ties them to the workspace version. Also docs/benchmarks.md's headline block and README's vintage line: `af7db97` (a `[skip ci]` bench-baseline commit) stamped latest.md 4.2.0, so the existing `benchmarks_index_vintage_matches_latest_md` guards were already red on main without CI running. Regression check: none.

### Second sweep (saved as `second_sweep_findings` in args.json; there is never a third)

No `severe` finding, so nothing was fixed in the second wave. All six are verified and go to the open items. temper's scale ranks severe (a security issue) above critical, but this project's severity notes call a secret in a log critical, so **second:security-01 goes to the owner before the tag**.

| id | lens | file:line | severity | kind | summary |
|---|---|---|---|---|---|
| second:security-01 | security | crates/weir-server/src/sink/clickhouse.rs:334 | critical | fix | The ClickHouse sink still copies a 3xx Location header into the permanent error unredacted, so a `?password=` it echoes is logged in full. This branch redacted the same query-string password on the transport, health and startup paths, and the CHANGELOG (line 228) and configuration.md say it is scrubbed wherever the SQL sinks log a URL. |
| second:durability-and-ack-semantics-01 | durability and ack semantics | crates/weir-server/src/wab/mod.rs:2593 | important | fix | recover_fsyncs_wab_dir_so_new_shard_dirs_are_durable says recover must fsync wab_dir after creating the shard dirs, but it checks only that some fsync happened, so it cannot catch an fsync done before the dirs exist. |
| second:durability-and-ack-semantics-02 | durability and ack semantics | crates/weir-server/src/wab/recovery.rs:1474 | important | fix | quarantine_fsyncs_wab_dir_after_creating_quarantine_dir and recovery_mid_file_corruption_fsyncs_wab_dir_before_truncating (line 1429) only count wab_dir fsyncs and inject a failure, so they cannot catch an fsync done before quarantine/ is created. |
| second:failure-paths-and-lifecycle-01 | failure paths and lifecycle | .temper/gate-slot.sh:76 | important | fix | latest-<gate> is repointed only after the slot wait and the free-disk check, so a run that is still waiting for a slot, or that exits on the disk floor, leaves latest-<gate>/summary.txt showing the previous run's final `GATE <gate> rc=N` line |
| second:test-strength-and-docs-01 | test strength and docs | crates/weir-server/tests/deploy.rs:699 | important | fix | the_drain_check_fails_when_the_core_instance_is_not_draining cannot catch removal of drain-check.sh's core-instance clause: both of its cases also fail on draining != instances, so a check with the core clause deleted still passes every deploy.rs test. |
| second:failure-paths-and-lifecycle-02 | failure paths and lifecycle | .temper/gate-full.sh:82 | minor | fix | act_job removes the fixed-name `weir-smoke` container before and after docker.yml's smoke job without checking whose it is, so two full gates in two slots break each other's smoke job |

## Open items

- second:security-01 (critical, crates/weir-server/src/sink/clickhouse.rs:334): The ClickHouse sink still copies a 3xx Location header into the permanent error unredacted, so a `?password=` it echoes is logged in full. This branch redacted the same query-string password on the transport, health and startup paths, and the CHANGELOG (line 228) and configuration.md say it is scrubbed wherever the SQL sinks log a URL. Input: sink_type = "clickhouse", sink_url = "http://ch.internal:8123/?user=default&password=S3CRET" (the query-parameter auth this branch's fix 126e8a3 covers), behind an http-to-https proxy that answers `return 301 https://$host$request_uri`, so the response carries `Location: https://ch.internal/?user=default&password=S3CRET&query=INSERT...&insert_deduplication_token=...`. Wrong result: commit() builds SqlSinkError::permanent("clickhouse", "http 301 ...: unexpected redirect to 'https://ch.internal/?user=default&password=S3CRET&query=...'"). The Location only goes through sanitize_log_excerpt and a 500-char cap, with no redact_password call. The drain then logs it at ERROR as `error!(error = %e, "drain: permanent sink error; dead-lettering batch")` (drain/mod.rs:4213), and the same string is stored as the dead-letter reason, so the ClickHouse password reaches the log and the dead-letter store in plaintext. The HTTP sink's matching path was changed on this branch to print only url_origin of the Location (security-r4-01); the ClickHouse path was not.
- second:durability-and-ack-semantics-01 (important, crates/weir-server/src/wab/mod.rs:2593): recover_fsyncs_wab_dir_so_new_shard_dirs_are_durable says recover must fsync wab_dir after creating the shard dirs, but it checks only that some fsync happened, so it cannot catch an fsync done before the dirs exist. Input: Move `segment::sync_path(wab_dir)?` in `recover` (mod.rs ~336) above the `for shard_id in 0..shard_count { create_dir_private(..) }` loop, then run the test. Wrong result: The test still passes. The first `recover` call counts one fsync of `dir` (calls >= 1), and the second call (fail=1) still returns EIO because the shard dirs already exist from the first call. But the new `shard_NN` entries are no longer made durable before the first Durable ack, which is the power-loss window the test says it guards. A test that checks the order (fsync count taken after the dirs exist on a fresh dir, or a hook that checks the shard dirs exist when wab_dir is synced) would catch it.
- second:durability-and-ack-semantics-02 (important, crates/weir-server/src/wab/recovery.rs:1474): quarantine_fsyncs_wab_dir_after_creating_quarantine_dir and recovery_mid_file_corruption_fsyncs_wab_dir_before_truncating (line 1429) only count wab_dir fsyncs and inject a failure, so they cannot catch an fsync done before quarantine/ is created. Input: In `create_quarantine_dir` (recovery.rs ~743), swap the two calls so that `super::segment::fsync_parent_dir(&quarantine_dir)?` (the wab_dir fsync) runs before `super::create_dir_private(quarantine_dir.clone())?`, then run both tests. Wrong result: Both tests still pass. The wab_dir fsync is still called (the call count matches) and an injected EIO still fails before any truncate or rename. But the `quarantine` entry in wab_dir is no longer durable when the caller truncates the original segment. A power loss can then drop quarantine/ and with it the only copy of the acked records after a mid-file corruption, which is the loss these tests say they prevent.
- second:failure-paths-and-lifecycle-01 (important, .temper/gate-slot.sh:76): latest-<gate> is repointed only after the slot wait and the free-disk check, so a run that is still waiting for a slot, or that exits on the disk floor, leaves latest-<gate>/summary.txt showing the previous run's final `GATE <gate> rc=N` line Input: Start `setsid nohup bash .temper/gate-full.sh` the documented way while every slot is busy, or while the slot has less than WEIR_GATE_MIN_FREE_GB free. A previous full run of this worktree ended `GATE full rc=0`. Then poll <logs>/<worktree>/latest-full/summary.txt until its last line is `GATE full rc=N`, as gate-full.sh's header, the README and the build rule say to. Wrong result: The poll stops at once on the old run's `GATE full rc=0`, so the gate reads green for code it never built or tested. In the slot-wait case the real run starts later and overwrites the verdict without anyone watching. In the disk-floor case (`exit 1` at line 41, before OUT exists) the run writes no GATE line and no log at all, and its stderr goes to /dev/null, so the stale green is the only result anyone sees. The README's 'every run writes ... the summary's last line is always GATE' does not hold for these two paths.
- second:test-strength-and-docs-01 (important, crates/weir-server/tests/deploy.rs:699): the_drain_check_fails_when_the_core_instance_is_not_draining cannot catch removal of drain-check.sh's core-instance clause: both of its cases also fail on draining != instances, so a check with the core clause deleted still passes every deploy.rs test. Input: Mutate deploy/monitoring/drain-check.sh line 20, replacing `[ "$(promq 'sum(weir_drain_state{job="weir", state="draining"})')" = "1" ] \` with `true \`, then run the deploy.rs drain-check tests. They feed (5,"5","4","0") and (1,"1","0","0"), and both still fail through the draining/instances comparison, so the tests stay green. A real case the clause exists for: a core-only run (expected=1) with the four level containers left up from an earlier --levels run (the stack is brought up idempotently) and the core `weir` instance down. Then up=4, draining=4, core=0. Wrong result: Reproduced with the shipped check()/gt() from smoke-test.sh and a stub promq. The original check gives FAIL for (expected 1, up 4, draining 4, core 0). The mutated check prints `PASS drain state = Draining (non-chaos, 4/4 draining, 1 expected)`, and the test meant to guard the core instance (and the four other deploy.rs drain tests) still pass. The core `weir` instance being down or not draining goes unreported, and no test catches the regression. Fix: add a case where only the core clause fails, e.g. drain_check(1, "4", "4", "0") must fail.
- second:failure-paths-and-lifecycle-02 (minor, .temper/gate-full.sh:82): act_job removes the fixed-name `weir-smoke` container before and after docker.yml's smoke job without checking whose it is, so two full gates in two slots break each other's smoke job Input: WEIR_GATE_SLOTS lists two slots, which gate-slot.sh supports and the README describes. Two worktrees run gate-full.sh at the same time, and one reaches `act_job docker.yml smoke` while the other's smoke job is running. Wrong result: The second gate's `docker rm -f weir-smoke` (before its job) or its cleanup (after its job) kills the first gate's running smoke container. That gate's `act-docker-smoke` step then fails with no defect in the code: a false red on a gate that is supposed to be judged on its steps.

Stage 4 is stable (2026-11-18). Throwaway worktrees from both runs remain under `.temper/worktrees/release-4-2-0-sweep/`, for `temper:cleanup`.

### Owner-directed fix (2026-11-18, outside stage 4)

- **second:security-01**: 32398f4 `fix(clickhouse): report only the origin of a 3xx Location`. The owner decided (18 Nov) to fix it before v4.2.0, by hand, with no new sweep. The 3xx dead-letter reason now carries `url_origin` of the `Location` (resolved against the request URL when relative), as the HTTP sink's does; the S29 sanitize and 500-char cap stay. `url_origin` over `redact_password`: it drops userinfo, path and query whole, where the query-key denylist misses a key it doesn't name. New test `commit_redirect_never_echoes_location_credentials` (`?password=`, `user:pass@`, relative `?quota_key=`): red before, green after. Other sinks: HTTP already origin-only; S3 never reads `Location`; Postgres/MySQL have no downstream URL. CHANGELOG 4.2.0 `### Fixed` line added.

### Released and cleaned up (2026-11-18)

Merged as PR #80 (5ad6591); the ten crates published at 4.2.0 and `v4.2.0` tagged on 5ad6591, the release workflow green (4 binaries). `temper:cleanup` removed the 18 throwaway worktrees and the local `sweep/4.2.0` branch (all its commits in main). Open items: the five second-sweep findings above, on Workbench as one P2 item.
