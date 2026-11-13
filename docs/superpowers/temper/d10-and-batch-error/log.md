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
- **Owner answer (13 Nov):** count msrv as GitHub CI: no 1.88 toolchain in the box; msrv joins the steps "not run in the box; covered by the PR GitHub CI". Written into `build_rule` in args.json.
2026-11-13 relaunch build attempt 2

## 2026-11-13: MVP checkpoint (attempt 2)

- Full gate at ee210c1, log /build/weir-gate/logs/epic/full-20261113T213304Z: `GATE full rc=1 (11 failed)`; passes under the owner's rule (native steps only). Native steps rc=0: fmt, clippy-default, clippy-all, clippy-nodefault, clippy-s3, demo-version, test-ws, test-s3, test-srv, test-srv-bins, test-ws-all, test-srv-all, test-srv-bins-all, load-norun, deny, dst-300, build-x86_64-unknown-linux-gnu (log ends `TLS smoke OK`).
- Not run in the box (no act; no 1.88 toolchain); covered by the PR GitHub CI, not counted: msrv (rc=1, toolchain not installed), act-ci-conformance, act-ci-harnesses, act-ci-sink-integration, act-ci-monitoring, act-docker-smoke, act-docs-build (rc=127), windows, build-x86_64-apple-darwin, build-aarch64-apple-darwin, build-aarch64-unknown-linux-gnu (rc=1).
- Flows, by hand on the debug weir-server built in the slot at ee210c1:
  - 1 ok: `weir-server --config /etc/weir/missing.toml` in an empty dir prints `Error: config file /etc/weir/missing.toml (passed with --config) does not exist`, rc=1, the dir stays empty, no /var/lib/weir created.
  - 2 ok (binary, not the image): no config file, `WEIR_WAB_DIR`/`WEIR_SOCKET_PATH`/`WEIR_METRICS_PORT` only: starts; `INFO config source config_file=/etc/weir/weir.toml origin="default path" loaded=false`; with the default noop sink also `WARN ... NOOP SINK: no config file at the default path /etc/weir/weir.toml; ...`. With `WEIR_SINK_TYPE=http` only the INFO line. The Dockerfile's CMD is `["weir-server"]` (read); the image itself was not built (a docker build is outside the gates; act-docker-smoke covers it on GitHub CI).
  - 3 ok (user transient unit): the shipped unit's ExecStart is `weir-server --config /etc/weir/weir.toml`; `systemd-run --user --wait <bin> --config /etc/weir/weir.toml` fails `status=1/FAILURE` and the journal has `Error: config file /etc/weir/weir.toml (passed with --config) does not exist`. The system unit itself was not installed (needs root).
  - 4 ok: a throwaway example (removed, not committed) against a running daemon: `push_batch` of 2,049 one-byte records → `Err(TooManyRecords { count: 2049, limit: 2048 })` ("too many records: 2049 exceeds the 2048-record batch hard cap"); the next `push` → `Ok(())`.
- Gaps: none in the flows. Observation (not a flow): a 2,048-record batch passes the client's cap but the daemon's default `max_batch_records` is 1024, so it gets `Protocol("unexpected response to a PushBatch: Nack")` (daemon: `batch declared 2048 records, cap is 1024`) and poisons the client; whether that is intended was not checked here: noted for the stabilise sweep.

## 2026-11-13: build report (status mvp, attempt 2)

- Checkpoint f1b0e49: full gate /build/weir-gate/logs/epic/full-20261113T213304Z at ee210c1, script `GATE full rc=1 (11 failed)`, passes under the owner's rule: every native step rc=0 (fmt, clippy-default, clippy-all, clippy-nodefault, clippy-s3, demo-version, test-ws, test-s3, test-srv, test-srv-bins, test-ws-all, test-srv-all, test-srv-bins-all, load-norun, deny, dst-300, build-x86_64-unknown-linux-gnu with `TLS smoke OK`). Not run in the box; covered by the PR GitHub CI: msrv, six act-* steps, windows, three cross builds.
- Flows 1-4 ok (flow 2 on the binary, not the image; flow 3 as a user transient unit, not the system unit).
- Gap outside the flows, for stabilise: a 2,048-record batch passes the client's cap but the daemon's default max_batch_records is 1024; the client gets `Protocol("unexpected response to a PushBatch: Nack")` and is poisoned.
- Behaviour spec 481c27f; acceptance 577a0a6 (config-startup), 287da63 (client-batch): 17 passing, 0 failing, green rounds 0, no questions.
- Untestable here: CONFIG-STARTUP-05 and -08 need the weir-server:local image; `docker build` fails at apt-get update because the proxy refuses deb.debian.org (403). Both tests are committed and run only with WEIR_ACCEPTANCE_DOCKER=1; they must run where the image builds (GitHub CI, the head's ci box).
- All commit shas verified on the epic branch; done = acceptance:client-batch, acceptance:config-startup, behaviour-spec, changelog-upgrade-notes, checkpoint, client-too-many-records, d10-hard-error. No flaky reruns.

## 2026-11-13: stabilise 4a sweep (wf_f8b48cd5-696)

- Lenses: security, failure paths and lifecycle, test strength and docs, durability and ack semantics. 4 findings verified, 0 refuted, 0 unverified, no blocked lenses. Nothing severe or critical.
- important test-strength-and-docs-01: CONFIG-STARTUP-08 (env-only Docker start) skips unless WEIR_ACCEPTANCE_DOCKER=1, which nothing sets.
- minor failure-paths-and-lifecycle-01: acceptance 06/07/10 panic instead of skipping when /etc/weir/weir.toml exists; absence judged with Path::exists().
- minor failure-paths-and-lifecycle-02: docs/architecture.md:135 still describes the pre-D10 missing-file behaviour.
- minor test-strength-and-docs-02: docs/getting-started/integrating.md:120 leaves TooManyRecords out of the recoverable errors.
- Owner selected all four for the fix wave (13 Nov). No lens raised the checkpoint's 2,048-vs-1,024 batch-cap gap.

## 2026-11-13: stabilise 4b fix wave (wf_47926a06-77d): stable

- Fixed (each test-first, regression-checked, shas on the epic branch): test-strength-and-docs-01 3e65fa5 (docker.yml's smoke job tags weir-server:local and runs CONFIG-STARTUP-05/-08 with WEIR_ACCEPTANCE_DOCKER=1, plus a guard test for that wiring), failure-paths-and-lifecycle-01 6a34175, failure-paths-and-lifecycle-02 6d24577, test-strength-and-docs-02 721144d. Reverted none; regressions none; contract items none; no flaky reruns.
- Second sweep: severe 0, others 3 (all minor). Stage 4 is stable after two sweeps.

### Open items (minor, not fixed; for a follow-up)

- second:failure-paths-and-lifecycle-01, crates/weir-server/tests/acceptance_config_startup.rs:293: config_startup_01's reference run passes no `--config`, so it reads the host's /etc/weir/weir.toml and fails falsely where weir is installed (0640 root:weir) or the file is malformed; point it at a scratch config or skip like 06/07/10.
- second:test-strength-and-docs-01, docs/getting-started/integrating.md:124 (and the TooManyRecords doc, crates/weir-client/src/client.rs:132): "split the batch and retry" with `limit` 2048 fails against a daemon on the default max_batch_records = 1024: Nack, the client is poisoned. The docs should say to split to the daemon's max_batch_records. This is the checkpoint's 2,048-vs-1,024 gap; whether the client should learn the daemon's cap is a design question for the owner.
- second:test-strength-and-docs-02, docs/architecture.md:134: ConfigError's variant list leaves out ConfigFileMissing.
