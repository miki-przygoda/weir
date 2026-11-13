# d10-and-batch-error: log

## 2026-11-10: spec and plan

- Owner decisions (10 Nov): ship with #77 as 4.2.0; Docker CMD becomes `["weir-server"]`; implicit missing default config → INFO (NOOP SINK WARN when the sink is noop); new `ClientError::TooManyRecords { count, limit }`; the systemd unit keeps its explicit `--config`.
- Plan: wave 0 `d10-hard-error` (C1) and `client-too-many-records` (C2) in parallel (gate_slots 2); wave 1 `changelog-upgrade-notes` (owns CHANGELOG.md). Behaviour groups `config-startup`, `client-batch`.
- Branch stacked on `epic/cleanup-2026-11`; rebase onto `main` after #77 merges, before the build runs, so the build starts from merged main.

## 2026-11-10: owner approved the spec; #77 merged; build launched

- Owner (10 Nov): spec approved; "continue and make decisions if needed"; merge #77 if fully green on remote.
- PR #77 merged by the head after its remote CI was fully green at f78181d (19 pass, 2 main-only jobs skipped): merge commit c1becf1. The epic branch is rebased onto main (no conflicts).
- Head decision: the build's checkpoint judges the full gate on its native steps only (miki_przygoda has no Docker since 9 Nov); the head runs the Docker half with `.workdocs/gate-docker-half.sh` before the PR.

## 2026-11-10 ~17:00: build paused (owner moving, then a beast shutdown)

- Build run wf_70eb87ee-39e stopped by the head (its orchestration runs on the Mac). State: wave 0 committed on its task branches — `temper/d10-and-batch-error/d10-hard-error` d6dbe91 (its checker's quick gate succeeded, jq-01M72ZSJT4JGDJH24CG838CN3K) and `temper/d10-and-batch-error/client-too-many-records` b98030c; both fetched back to the Mac; no uncommitted edits in their worktrees. Not yet merged into the epic branch; wave 1 (CHANGELOG) not started.
- Resume: refresh `done` (host_sync fetch + done_tasks.py), `attempt` 1, Workflow resume of wf_70eb87ee-39e with `target/temper/build-d10.js`.

## 2026-11-13: build relaunched in cc1

- The Mac's run wf_70eb87ee-39e can't be resumed here (a resume works only in the session that ran it), so the build launches fresh in cc1 with `host: local`, `done: []` (done_tasks.py at ad06f72) and attempt 0. The wave-0 task branches exist with their commits (d6dbe91, b98030c); the implementers continue from them.
- `act` is not in the box yet: installing it from nektos/act's release was refused by the box's auto-mode permission check. The build doesn't need it; the full gate (checkpoint, publish) does.
2026-11-13 relaunch build attempt 0

## 2026-11-13: build returned blocked at the checkpoint

- Run wf_d4e9b978-edc (attempt 0): done_now d10-hard-error, client-too-many-records, changelog-upgrade-notes (trailers on the epic branch at 63c15b2, done_tasks.py confirms); commits {}; no flaky reruns.
2026-11-13 blocked[none] checkpoint
- checkpoint (agent's reason, data): did not start the full gate: act is not at /usr/local/bin/act, no act runner images, and the workflow forbids sudo.
- **Owner answer (13 Nov ~20:35Z):** do NOT install act or any other binary. Judge the full gate on its native steps only (fmt, clippy x4, demo-version, every test step, load-norun, msrv, deny, dst-300, build-x86_64-unknown-linux-gnu with its TLS smoke): it passes when every native step has rc=0. The container steps (act-*, windows, the cross builds) are expected to fail without act or with docker limits: listed as "not run in the box (no act); covered by the PR GitHub CI", not counted. Publish's full gate uses the same rule.
- How it reaches the run: the build workflow's validator accepts `args.answers` keys only for task ids ("answer for unknown task checkpoint" would make the launch invalid), and the checkpoint prompt doesn't read answers; it does carry `build_rule`. So the answer is appended to `build_rule` in args.json (which stabilise and publish also pass), and `answers` stays `{}`.
2026-11-13 relaunch build attempt 1

## 2026-11-13: checkpoint blocked again (attempt 1): msrv

2026-11-13 blocked[none] checkpoint
- Full gate at 05f0d03, log /build/weir-gate/logs/epic/full-20261113T203955Z: `GATE full rc=1 (11 failed)`. Native steps rc=0: fmt, clippy-default, clippy-all, clippy-nodefault, clippy-s3, demo-version, test-ws, test-s3, test-srv, test-srv-bins, test-ws-all, test-srv-all, test-srv-bins-all, load-norun, deny, dst-300, build-x86_64-unknown-linux-gnu (with its TLS smoke).
- msrv rc=1, without checking the code: `error: toolchain '1.88-x86_64-unknown-linux-gnu' is not installed` (the box has only stable).
- Container steps, not counted (owner's rule): act-ci-conformance, act-ci-harnesses, act-ci-sink-integration, act-ci-monitoring, act-docker-smoke, act-docs-build (rc=127), windows, build-x86_64-apple-darwin, build-aarch64-apple-darwin, build-aarch64-unknown-linux-gnu (rc=1): not run in the box (no act); covered by the PR GitHub CI.
- Not decided here: the owner's rule says no other binary and counts msrv as native. Asked the owner: install the 1.88 toolchain with rustup, or treat msrv as covered by GitHub CI.
