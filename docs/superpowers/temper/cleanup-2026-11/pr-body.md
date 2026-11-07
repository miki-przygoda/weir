## What this is

A stabilisation pass over the whole repository at `main` 5e32265 (4.1.0 plus docs), run as temper stage 4 (sweep, verify, fix, regression check per fix) and stage 5 (docs, evidence, full gate, PR). Nothing new is built. Every fix below started from a verified finding: an input, the wrong result it gives, and a verifier that reproduced it on the branch. It is the base for a 4.1.1. There is no version bump, tag or publish.

The review covered twelve lenses across the whole tree: security; failure paths and lifecycle; test strength and docs; durability and ack semantics; wab and recovery; drain and dead letters; wire socket and client; config reload and startup; sinks; status metrics and admin; weir-ctl and attest; deploy, CI and chaos harness. Six lenses lost their first reviewers to the usage limit, so they ran a second time as `r3`. The first night's unverified security findings were reviewed again as `security r3`. Drain and dead letters r3 found nothing that held up under verification.

Spec: `docs/superpowers/temper/cleanup-2026-11/spec.md`. Run log: `docs/superpowers/temper/cleanup-2026-11/log.md`.

## What changed

24 fixes are kept. Two more were reverted (see below). Each fix has one entry under `CHANGELOG.md` `## [Unreleased]`. Two fixes are under `### Changed` because an operator can see the new behaviour.

### Secrets kept out of logs, stderr and `/status`

| Id | What it fixes | Commit |
|---|---|---|
| security-r3-01 (critical) | The HTTP sink printed the full `sink_url` in its `sink: http` startup line, in transport-error retries, in the health `Down` reason and in the admin status text. A webhook token in the path, or a presigned signature in any query key outside the 10-key denylist (`X-Amz-Signature`, `client_secret`, `?code=`), was printed in full. It now prints only `scheme://host[:port]` (**Changed**). | 9c7b33e |
| security-r3-02 (critical) | A TOML syntax error printed the offending source line to stderr, and a value with the wrong type was quoted, so a credential on that line was logged. The error now gives the line and column (or the key) and what was expected, and quotes no part of the file (**Changed**). | 4179b12 |
| status-metrics-and-admin-r3-02 (critical) | `url_origin` treated `user:password` as `host:port` when no `@host` followed (`mysql://weir:password`), so the `weir starting` line and the admin config view printed the password. A port must now be all digits, so such a URL shows as `<unparseable>`. | 972b42f |
| config-reload-and-startup-02 (critical) | A `WEIR_*` env var that is not valid UTF-8 had its value printed in the startup error (for example `WEIR_SINK_URL` credentials or `WEIR_SINK_S3_SECRET_ACCESS_KEY`). The error now names only the variable. | 4ac45c4 |
| sinks-r3-03 | A ClickHouse transport error included reqwest's full request URL, so a `?password=` query parameter reached the drain's WARN retry log, the sink failure status and the health probe error. These errors are now redacted. | 126e8a3 |

### WAB durability and recovery

| Id | What it fixes | Commit |
|---|---|---|
| wab-and-recovery-r3-01 (critical) | Recovery treated a corrupt record length (under the cap, but running past EOF) as a torn tail and truncated the segment with only a WARN. That destroyed the acked records after it. Now, if a shorter prefix of the bytes on disk matches the record's CRC, the segment is first copied to `quarantine/` and counted, as the other corruption branches already did. | 1aa2c4f |
| durability-and-ack-semantics-r3-01 | `wab_dir` was never fsynced after the `shard_NN` directories were created. A power loss could still remove the directory holding the first `Durable` acks on a fresh `wab_dir`. Startup now fsyncs `wab_dir`, and a failed fsync fails startup. | 04f0d4c |
| durability-and-ack-semantics-r3-02 | `quarantine/` was created without an fsync of `wab_dir` before the original segment was durably truncated. A power loss could therefore lose the only copy of the records after a mid-file corruption. `wab_dir` is now fsynced first, and if that fsync fails the segment is left untouched. | 1b004da |
| wab-and-recovery-r3-02 | A foreign subdirectory of `wab_dir` that the daemon could not read (a root-owned `lost+found` on a mount root) failed startup and marked every drain rescan incomplete. Recovery and replay now treat only `shard_<N>` directories as shards. | f7efcc9 |
| wab-and-recovery-r3-03 | After a failed timed seal (`segment_max_age` or `segment_max_lifetime`), the log said the segment "stays active". In fact it had already been retired, so its acked records were stranded in the unsealed `.wab` until a restart. The failure is now logged at ERROR with what actually happens, and every failed seal is counted in the new `weir_wab_seal_failures_total`. | 5cb6066 |

### Startup and config

| Id | What it fixes | Commit |
|---|---|---|
| failure-paths-and-lifecycle-r3-01 | Preflight accepted a `socket_path` whose parent directory was missing, was not a directory, or was not writable. The bind then failed only after the drain had started replaying, so under `Restart=on-failure` every restart delivered the head of the oldest unconfirmed segment again. Preflight now refuses such a path before recovery. | e358b82 |
| config-reload-and-startup-04 | A misspelt `log_level` (`warning`, `information`) was read as a target directive, which turned off every weir log line without saying so. A bare word must now be a level name. Anything else logs a WARN and the daemon runs at `info`. | c1b8dd1 |

### Sinks: transient errors strand instead of dead-lettering

| Id | What it fixes | Commit |
|---|---|---|
| sinks-r3-04 | S3/MinIO answers a request signed for the wrong region with `400 AuthorizationHeaderMalformed`. That answer dead-lettered the whole acked backlog. It now strands, the same way `SignatureDoesNotMatch` does. | ed290f8 |
| sinks-r3-05 | Postgres SQLSTATE `25006` (`read_only_sql_transaction`, from a hot standby after failover or from `default_transaction_read_only = on`) dead-lettered the backlog. It now strands. | 05b1d00 |
| postgres-docs-25006 | Docs regression from sinks-r3-05, found by its regression check: `configuration.md` and the `postgres.rs` module doc still said `25006` dead-letters. | ef52f0b |
| sinks-r3-06 | MySQL `1040` (too many connections), `1203` (too many user connections) and `1053` (server shutdown) dead-lettered acked records. They now strand. | ade74f3 |

### Wire, client, weir-ctl

| Id | What it fixes | Commit |
|---|---|---|
| wire-socket-and-client-r3-01 | `WeirClient::push_batch` checked each record against the hard cap but never checked the whole encoded body. A batch of records that each fit could therefore be sent, answered with `Nack(PayloadTooLarge)`, and leave the connection poisoned. The body is now checked locally and rejected with a recoverable `ClientError::PayloadTooLarge`. | 87a6332 |
| wire-socket-and-client-r3-02 | The configuration reference said `max_batch_records` and `max_payload_bytes` multiply. They do not: `max_payload_bytes` caps the whole `PushBatch` body. | 4228ac2 |
| weir-ctl-and-attest-r3-02 | `attest seal` / `verify` treated every directory under `wab_dir` as a shard. An unreadable `lost+found` made every `seal` exit 1 with no shard chained, and every `verify` exit 2. Only `shard_<N>` directories count now, which is the daemon's rule. | f1a1e0d |

### Tests, deploy, CI and chaos harness

| Id | What it fixes | Commit |
|---|---|---|
| security-r3-04 | `drain_state_and_sink_health_pre_initialised` could not fail, because it matched bare words such as `draining` that the HELP text contains. It now matches whole sample lines. | e15903d |
| deploy-ci-and-chaos-harness-01 | The monitoring smoke test's "drain state = Draining" check summed only two states, so a drain that had stopped, or a missing `weir_drain_state` series, still passed. | 212f3e7 |
| deploy-ci-and-chaos-harness-02 | `deploy/smoke-test.sh` only polled `kill -0` after SIGTERM and never read the exit status, so a failed shutdown passed as a clean exit. | 7413c0e |
| deploy-ci-and-chaos-harness-03 | The chaos Foundations gate (`gate.py`) counted `runs/A` and `runs/A/` (or a symlink) as two separate run dirs. Cuts could then count twice and pass the 1,000-cut and 200-per-schedule floors with half the cuts. A run dir named twice is now refused. | afde04e |
| deploy-ci-and-chaos-harness-06 | `deploy/smoke-test.sh` used `echo \| grep -q` under `pipefail`. When `grep -q` exited early, `echo` hit SIGPIPE and failed, so a metric that was present was reported missing on about half of runs (510/1000). The script now uses a here-string. | 8c263ab |

### Docs for this PR (this commit)

These docs are now aligned with the final code:

- `configuration.md` and `security/container.md`: what each surface prints for `sink_url`. The HTTP sink and the config view print only the origin. The SQL sinks keep the userinfo and query-key denylist.
- `wab_format.md`: only `shard_<N>` directories are shards, a corrupt record is copied to quarantine before truncation, and the `wab_dir` fsyncs.
- `sinks/s3.md`: `AuthorizationHeaderMalformed` is listed as stranded.
- `wire_protocol.md` and the `push_batch` rustdoc: the payload cap applies to the whole batch body.
- The CHANGELOG had two entries for Postgres `25006`, one for the sink and one for its docs. They are merged into one.

## Reverted at the owner's call: the two attest fixes

The per-fix regression checks found that both of these fixes weakened `attest verify`. On 7 Nov the owner decided to revert both. f1a1e0d (the `lost+found` fix) stays.

- **72bae97**, reverted by **523afa5**, was weir-ctl-and-attest-r3-01: "a gap the drain accounts for is a chain origin, not a broken link". It treated any file named `seg_N.wab.confirmed` as excusing a gap, even an empty one, and so did a junk-named entry in `quarantine/`. That let `rm` followed by `touch` hide a deleted segment: `verify` exited 0 where it had exited 1. Tamper detection is a security decision, so this needs a design that a file name cannot forge.
- **fe31ac1**, reverted by **e8a93fb**, was weir-ctl-and-attest-r3-03: "attest skips a segment the live drain deletes after listing it". If the drain raced it after the cached disposed set, an honest exit 2 became a false BROKEN_LINK (exit 1).

The CHANGELOG conflict from the reverts was resolved by dropping only 72bae97's entry. The original false positive and the NotFound on a segment deleted mid-run are back in the owner items below.

## How it was tested

- **A failing test first, for every fix that has a test harness.** Each Rust fix commit adds a test that shows the wrong result before the change and passes after it. The chaos gate fix adds a case to `chaos/orchestrator/test_gate.py`, and the `25006` docs fix adds a `docs_drift` guard. The three deploy shell-script fixes (212f3e7, 7413c0e, 8c263ab) have no in-tree harness. For those, the failing case is the verifier's reproduction on beast, for example 510 false "missing" results in 1000 runs for the `pipefail` bug. `deploy/monitoring/smoke-test.sh` runs in CI's monitoring job, and `deploy/smoke-test.sh` has no CI job.
- **Quick gate per fix, in jobq on beast** (`.temper/gate-quick.sh`: fmt, clippy with default and all features, then the tests of each changed crate, plus the drift guards). Every gate ran as a jobq job in the shared epic worktree.
- **A regression agent per fix.** For each fix, a separate agent checked out the fix in its own throwaway worktree and tried to break something next to it. That found the `25006` docs regression (fixed in ef52f0b) and the two attest weaknesses (reverted). It found no other regressions. No fix was patched on top of itself.
- **The full gate** (`.temper/gate-full.sh`, the same set CI runs) runs on the final branch in stage 5. Its result and every step with rc != 0 are in the evidence for this PR.
- **One flaky rerun, flagged, not hidden.** One quick gate for deploy-ci-and-chaos-harness-02 failed once on `weir-server system::sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` (ECONNRESET instead of EOF, system.rs:6183, jq-01M6V6SJAE0K2G3Z0SZHZQQRJJ). It passed on a rerun (jq-01M6V7413Z2CFXZETJPPFKEHC7). That fix touched only `deploy/smoke-test.sh`. The test is not on the project's flaky list, so this is listed under open items.

### Owner steps (evidence only the owner can produce)

- The chaos power-cut campaign on this branch, which needs root. The recovery and fsync changes (1aa2c4f, 04f0d4c, 1b004da, f7efcc9, 5cb6066) are what it should exercise.
- The native macOS tests and the darwin TLS smoke, run by the head on the Mac before a push.
- Optional: a check against real ClickHouse, Postgres, MySQL and S3 of the sink errors that were reclassified (`25006`; MySQL `1040`/`1203`/`1053`; S3 `400 AuthorizationHeaderMalformed`; ClickHouse transport-error redaction). The unit tests cover only the classification and redaction.

## Open items

**Owner decisions needed**
- sinks-r3-01 (critical): ClickHouse `insert_deduplication_token` is the content-only DedupToken, pinned to its 1.x value by `dedup_token_is_unchanged_from_weir_1_x`. Two distinct batches with identical bytes (heartbeats, for example) collide inside the dedup window, and ClickHouse silently drops the second one with HTTP 200. Owner: left for a separate change (7 Nov).
- sinks-r3-02 (critical, returned `contract`): HTTP NDJSON batch mode sends the content-only DedupToken as `Idempotency-Key`, so an idempotent endpoint drops a later batch with the same bytes. Owner: left for a separate change with a coordinate-based key like the S3 sink's (7 Nov).
- weir-ctl-and-attest-r3-01 and -r3-03 (important): `attest verify` reports BROKEN_LINK when the drain reclaims a segment out of order, and errors (exit 2) on a segment the drain deletes mid-run. The fixes (72bae97, fe31ac1) were reverted at the owner's call (7 Nov) because they let `rm` plus an empty `touch seg_N.wab.confirmed` hide a deleted segment. This needs a design that a file name cannot forge.
- D10: an explicit missing `--config` still only WARNs (deferred decision, unchanged).
- Possible pre-existing flake: `weir-server system::sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` failed once (ECONNRESET instead of EOF, system.rs:6183) in a quick gate on 7 Nov. The full gate decides.

**Minor (not fixed)**
- deploy-ci-and-chaos-harness-04: `run_bare_metal_bench.sh` reports /run's mount options for a tmpfs /tmp. -05: `deploy/smoke-test.sh` says it pushes 30 records across three tiers, but it pushes 20 across two.
- docs.yml's `workflow_dispatch` builds the book but never deploys it. The chaos `SETUP.md` and `run-soak.sh` label power-loss schedules as "kill -9 only".
- config-reload-and-startup-05/06/07 (unverified): the check that the admin socket is under `wab_dir` is lexical; the invalid sink type error omits `s3`; `FEATURE_GATED_FLAGS` lacks the mysql/postgres `id_column` flags. test-strength-and-docs-01 (unverified): `WEIR_DST_SEED` is printed in the repro line but never read.
- weir-ctl-and-attest-r3-04: `.attest` sidecars are never removed after their segment drains. wire-socket-and-client-r3-03 (owner, public error type): `PayloadTooLarge` reports a record count as bytes. -r3-04: nack counters count frames, not records. -r3-05/-06: the TS/Python demo clients mishandle a HealthCheckResponse and an empty Nack.
- Throwaway worktrees are left under beast's `~/work/weir/cleanup-2026-11/tmp/`. Some hold probe edits from agents that died at the usage limit on 5 Nov. Remove them with `/temper:cleanup` or by hand.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
