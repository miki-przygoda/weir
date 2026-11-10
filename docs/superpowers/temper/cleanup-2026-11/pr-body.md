## What this is

A stabilisation pass over the whole repository at `main` 5e32265 (4.1.0 plus docs), run as temper stage 4 (sweep, verify, fix, a regression check per fix) and stage 5 (docs, evidence, full gate, PR). Nothing new is built. Every fix started from a verified finding: an input, the wrong result it gives, and a verifier that reproduced it on the branch. It is the base for a 4.1.1. There is no version bump, tag or publish.

The first sweep covered twelve lenses across the whole tree: security; failure paths and lifecycle; test strength and docs; durability and ack semantics; wab and recovery; drain and dead letters; wire socket and client; config reload and startup; sinks; status metrics and admin; weir-ctl and attest; deploy, CI and chaos harness. Lenses that lost their reviewers to the usage limit ran again as `r3`. After the owner's review on 8 Nov, a second sweep (`r4`: security, durability and ack semantics, sinks, failure paths and lifecycle, test strength and docs, wire client and weir-ctl) went over the branch's own changes (`git diff main...epic/cleanup-2026-11`).

Spec: `docs/superpowers/temper/cleanup-2026-11/spec.md`. Run log: `docs/superpowers/temper/cleanup-2026-11/log.md`.

## What changed

51 fixes are kept; two more were reverted (see below). `CHANGELOG.md` `## [Unreleased]` has one entry per behaviour. Fixes that a later fix in this branch completed share an entry. Five entries are under `### Changed`, and each one says what happens once on upgrade:

- The HTTP sink prints only the origin of `sink_url` (and of a redirect target). Log searches that matched the full URL stop matching.
- A config parse error no longer quotes the offending line or value. A config that parses is unaffected.
- **The ClickHouse `insert_deduplication_token` and the HTTP NDJSON `Idempotency-Key` are a coordinate-based batch key** (`docs/sinks/batch-keys.md`), not the content-only `DedupToken`. A batch in flight at the upgrade is re-sent once under the new key.
- **Every `RecordId` names the segment instance** (`shard_NN/<file>@<created_at hex>`). Every per-record key, `id_column` value, S3 object name and batch key changes once at the upgrade, and a record in flight then may be stored twice, once.
- An `admin_socket_path` that reaches `wab_dir` through a symlink is refused at startup. Such a deployment fails to start until the path moves.

### Secrets kept out of logs, stderr and `/status`

| Id | What it fixes | Commit |
|---|---|---|
| security-r3-01 (critical) | The HTTP sink printed the full `sink_url` in its startup line, transport-error retries, health `Down` reason and admin status text. A webhook token in the path, or a presigned signature in a query key outside the 10-key denylist, was printed in full. It now prints only `scheme://host[:port]` (**Changed**). | 9c7b33e |
| security-r4-01 | Found by the second sweep in the code above: a 3xx dead-letter reason printed the full redirect `Location`, and a 4xx/5xx body excerpt could echo the configured path (`Cannot POST /path`). A redirect now shows only its origin, and the path and query are cut out of the excerpt. Same `Changed` entry. | 8260180 |
| security-r3-02 (critical) | A TOML syntax error printed the offending source line to stderr, and a wrongly typed value was quoted. The error now gives the line and column (or the key) and what was expected (**Changed**). | 4179b12 |
| status-metrics-and-admin-r3-02 (critical) | `url_origin` took `user:password` for `host:port` when no `@host` followed (`mysql://weir:password`), so the `weir starting` line and the admin config view printed the password. Such a URL now shows as `<unparseable>`. | 972b42f |
| config-reload-and-startup-02 (critical) | A non-UTF-8 `WEIR_*` env var had its value printed in the startup error. The error now names only the variable. | 4ac45c4 |
| sinks-r3-03 | A ClickHouse transport error carried reqwest's full URL, so a `?password=` reached the WARN retry log, the sink failure status and the health probe error. Now redacted. | 126e8a3 |
| security-r4-02 | The ClickHouse startup error for a non-http(s) URL (a `tcp://` DSN) still printed it unredacted. Same entry as sinks-r3-03. | 59ec753 |

### Sink keys: silent loss of byte-identical records

| Id | What it fixes | Commit |
|---|---|---|
| dedup-coordinate-key (sinks-r3-01 critical + sinks-r3-02 critical) | ClickHouse's `insert_deduplication_token` and HTTP NDJSON's `Idempotency-Key` were the content-only `DedupToken`, so two segments holding a byte-identical record (a heartbeat every 30 s) shared a key: ClickHouse dropped the second block with a 200, an idempotent endpoint replayed its cached 2xx, and weir deleted acked records. Both now send the S3-style coordinate key (segment `created_at`, first `RecordId`, count), falling back to `DedupToken` for a batch built without segment context (**Changed**). | 6679c8f |
| recordid-segment-instance (sinks-r4-02) | Segment names restart at `seg_00000001` when a shard drains empty and the daemon restarts, or a pod loses its volume, so a byte-identical record got the `RecordId` of one already delivered: an `id_column` table dropped it and an idempotent endpoint replayed its 2xx in per-record mode. The identity hashed into `RecordId::for_record` now carries `@<created_at hex>`; the SDK API is unchanged (**Changed**). | 49c4188 |
| attest-docs-recordid-identity | Regression the per-fix check proved on 49c4188: weir-attest still chains over the bare `<shard>/<segment>`, so five docs that said an attest head can be recomputed from the downstream's rows were wrong. The docs now say the chain input is the file-name identity, not the delivered id, and a `docs_drift` guard pins it. | 07e29ab |
| test-strength-and-docs-r4-01 | The sink-SDK docs and integration guide still offered `DedupToken` as a downstream key. They now point to `record_ids()` or the coordinate key. | ef15321 |
| test-strength-and-docs-r4-03 | The example config's `sink_send_idempotency_key` comment and the `clickhouse-sink` feature comment still named the content token. | d47fe27 |
| test-strength-and-docs-r4-04 | 6679c8f deleted weir-server's independent copy of the 1.x `DedupToken` digest, so re-blessing the SDK constant turned nothing red. The ClickHouse fallback key, which still sends the `DedupToken`, pins it again. | cc0f619 |

### WAB durability and recovery

| Id | What it fixes | Commit |
|---|---|---|
| wab-and-recovery-r3-01 (critical) | Recovery took a corrupt record length (under the cap, running past EOF) for a torn tail and truncated with only a WARN, destroying the acked records after it. When a shorter prefix matches the record's CRC, the segment is now copied to `quarantine/` first and counted. | 1aa2c4f |
| durability-and-ack-semantics-r3-01 | `wab_dir` was never fsynced after creating `shard_NN`, so the first `Durable` acks on a fresh `wab_dir` were not crash-durable. Startup fsyncs it; a failed fsync fails startup. | 04f0d4c |
| durability-and-ack-semantics-r3-02 | `quarantine/` was created without a `wab_dir` fsync before the original was durably truncated. Now fsynced first; on failure the segment is left untouched. | 1b004da |
| wab-and-recovery-r3-02 | An unreadable foreign subdirectory of `wab_dir` (a root-owned `lost+found` on a mount root) failed startup and every drain rescan. Recovery and replay now treat only `shard_<N>` directories as shards. | f7efcc9 |
| durability-and-ack-semantics-r4-01 | The WAB size scan still counted every directory, so the same `lost+found` failed every size scan and, with `wab_max_bytes` set, refused every push for good. Same entry as above. | 00a42a5 |
| wab-and-recovery-r3-03 | A failed timed seal logged that the segment "stays active"; it had been retired, stranding its acked records until a restart. Now logged at ERROR and counted in the new `weir_wab_seal_failures_total`. | 5cb6066 |
| minor-startup-scan-reject | After a failed startup size scan, the first successful scan went through the cap's hysteresis, so a WAB between low water and the cap kept pushes nacked and `/readyz` at 503. It now judges the WAB afresh. | 2ad211a |
| failure-paths-and-lifecycle-r4-01 | Completes the above: the failed startup scan left the measured size at 0, so the first push released the fail-closed flag and a WAB over the cap took writes without bound. Pushes now nack until a scan measures the WAB. Same entry. | 0081664 |

### Startup and config

| Id | What it fixes | Commit |
|---|---|---|
| failure-paths-and-lifecycle-r3-01 | Preflight accepted a `socket_path` whose parent was missing, not a directory or not writable, so the bind failed after the drain had begun replaying, and each `Restart=on-failure` re-delivered the oldest segment's head. Preflight now refuses it before recovery. | e358b82 |
| config-reload-and-startup-04 | A misspelt `log_level` (`warning`) was read as a target directive and silenced every weir log line. A bare word must now be a level name; anything else WARNs and runs at `info`. | c1b8dd1 |
| minor-admin-socket-lexical (config-reload-and-startup-05) | The "admin socket inside `wab_dir`" check was lexical, so a path through a symlink passed. It now compares canonical paths (**Changed**). | ade96c5 |
| minor-sink-type-s3 (config-reload-and-startup-06) | The unknown `sink_type` error did not list `s3`. | 4b72b22 |
| minor-feature-gated-flags (config-reload-and-startup-07) | `--sink-mysql-id-column` / `--sink-postgres-id-column` on a build without the feature got "unknown arguments" instead of naming the feature. | bbcbf61 |

### Sinks: transient errors strand instead of dead-lettering

| Id | What it fixes | Commit |
|---|---|---|
| sinks-r3-04 | S3/MinIO `400 AuthorizationHeaderMalformed` (wrong region) dead-lettered the acked backlog. It now strands. | ed290f8 |
| sinks-r3-05 | Postgres SQLSTATE `25006` (`read_only_sql_transaction`: a hot standby after failover) dead-lettered the backlog. It now strands. | 05b1d00 |
| postgres-docs-25006 | Regression the per-fix check found on 05b1d00: `configuration.md` and the `postgres.rs` module doc still said `25006` dead-letters. Same entry. | ef52f0b |
| sinks-r3-06 | MySQL `1040`, `1203` and `1053` (too many connections, server shutdown) dead-lettered acked records. They now strand. | ade74f3 |

### Wire, client, weir-ctl, demos

| Id | What it fixes | Commit |
|---|---|---|
| wire-socket-and-client-r3-01 | `WeirClient::push_batch` never checked the whole encoded body, so a batch of records that each fit could be sent, answered with `Nack(PayloadTooLarge)` and poison the connection. It is now rejected locally as a recoverable `ClientError::PayloadTooLarge`. | 87a6332 |
| wire-socket-and-client-r3-02 | The configuration reference said `max_batch_records` and `max_payload_bytes` multiply; `max_payload_bytes` caps the whole `PushBatch` body. | 4228ac2 |
| minor-nack-counters (wire-socket-and-client-r3-04) | A `PushBatch` refused whole at admission added 1 to the per-record nack counters instead of its record count. | 31c0489 |
| weir-ctl-and-attest-r3-02 | `attest seal` / `verify` treated every directory under `wab_dir` as a shard, so an unreadable `lost+found` failed every run. Only `shard_<N>` counts now. | f1a1e0d |
| failure-paths-and-lifecycle-r4-02 | `weir-ctl segments` and the offline `weir-ctl status` scan still showed such a directory as a phantom shard. Same entry. | 4d9833a |
| minor-demo-empty-nack (wire-socket-and-client-r3-06) | The TS and Python demo clients read an empty Nack as `InternalError` (transient) and kept a desynced connection. | cfa1d91 |
| test-strength-and-docs-r4-05 | Completes the above for the Python `health_check`, which also returned `False` on a reply of the wrong kind and kept the stream open. | d9881b0 |
| minor-ts-demo-health (wire-socket-and-client-r3-05) | The TS demo client resolved a Push answered with a `HealthCheckResponse` as acked (a false ack). Crossed replies are now a desync. | 4b99f49 |
| wire-client-and-weir-ctl-r4-01 | Completes the above: an untracked `AckTracked` or an unknown reply type left the request pending forever. | 062b888 |
| test-strength-and-docs-r4-02 | CI's conformance job did not run the demo clients' desync regression tests. It does now. | ade1ff0 |

### Tests, deploy, CI and chaos harness

| Id | What it fixes | Commit |
|---|---|---|
| security-r3-04 | `drain_state_and_sink_health_pre_initialised` could not fail (it matched words in the HELP text). It now matches whole sample lines. | e15903d |
| sigterm-test-accept-race | `sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` sent SIGTERM before the daemon had accepted its idle connection, so a loaded machine reset it from the backlog (ECONNRESET). One HealthCheck round-trip first makes it accepted; the test still asserts EOF. | 6648740 |
| minor-dst-seed (test-strength-and-docs-01) | The DST repro line `WEIR_DST_SEED=0x…` was never read, so it reran the default sweep and passed green. It now replays that seed. | 49c5619 |
| deploy-ci-and-chaos-harness-01 | The monitoring smoke test's drain-state check passed a stopped drain or a missing series. | 212f3e7 |
| deploy-ci-and-chaos-harness-02 | `deploy/smoke-test.sh` never read the exit status after SIGTERM, so a failed shutdown passed. | 7413c0e |
| deploy-ci-and-chaos-harness-06 | `deploy/smoke-test.sh` used `echo \| grep -q` under `pipefail`, so a present metric was reported missing on about half of runs (510/1000). Now a here-string. | 8c263ab |
| minor-smoke-record-count (deploy-ci-and-chaos-harness-05) | The smoke test said it pushes 30 records across three tiers; it pushes 20 across two. | ac61c29 |
| minor-bench-mount-opts (deploy-ci-and-chaos-harness-04) | `run_bare_metal_bench.sh` printed `/run`'s mount options for a tmpfs `/tmp`. | ea95ffd |
| deploy-ci-and-chaos-harness-03 | The chaos Foundations gate counted `runs/A` and `runs/A/` as two run dirs, so a campaign could pass the cut floors with half the cuts. A run dir named twice is refused. | afde04e |
| minor-chaos-setup-label | `run-soak.sh` and the per-run `SETUP.md` said "`kill -9` only" for every schedule, power-loss ones included. They now state the schedule's fault. | df9c0eb |
| minor-docs-dispatch | A manual run of the docs workflow built the book and deployed nothing. | 7c1dd2d |

### Docs for this PR (this commit)

- `CHANGELOG.md`: each `### Changed` entry now says what happens once on upgrade (the HTTP origin, the config parse error and the admin socket entries did not). The two demo-client entries are split by behaviour: an empty Nack, and a reply of the wrong kind (the Python `health_check` half moved to the second).
- `operations/configuration.md`: a `RecordId`'s segment includes its creation time; the first successful size scan after a failed startup scan judges the WAB against the cap, not the resume line.
- `operations/quarantine-recovery.md`: a requeue's duplicates get new keys because every key weir sends is derived from the record's place in the buffer; the page still credited the `DedupToken`, which no built-in sink sends now.

## Owner decisions

- **Attest fixes reverted (7 Nov).** The per-fix regression checks showed both weakened `attest verify`: **72bae97** (weir-ctl-and-attest-r3-01, reverted by **523afa5**) let `rm` plus an empty `touch seg_N.wab.confirmed` or a junk name in `quarantine/` hide a deleted segment (`verify` exit 0, was 1); **fe31ac1** (weir-ctl-and-attest-r3-03, reverted by **e8a93fb**) turned an honest exit 2 into a false BROKEN_LINK under a drain race. f1a1e0d (the `lost+found` fix) stays.
- **Dedup key and RecordId folded in (8 Nov).** First left as owner items on 7 Nov; on 8 Nov the owner folded sinks-r3-01 and sinks-r3-02 in as one change (`dedup-coordinate-key`, 6679c8f) and, after the second sweep, sinks-r4-02 as `recordid-segment-instance` (49c4188), both with a `Changed` note on the one-time re-send.
- **Attest chain ids kept, docs corrected (9 Nov).** weir-attest keeps chaining over the bare `<shard>/<segment>`, so existing `.attest` sidecars keep verifying; the docs that promised a head recomputable from the downstream's rows were corrected (07e29ab). Chaining over the delivered id, with a versioned sidecar, moves to the `attest-disposal-log` epic.
- **Separate epics.** `attest-disposal-log` (a hash-chained daemon record of disposed segments) takes the reverted attest false positives and the `.attest` sidecar cleanup (weir-ctl-and-attest-r3-04). `d10-missing-config-error` makes an explicit missing `--config` a hard error in 4.2.0.
- **`PayloadTooLarge` left as an owner item.** wire-socket-and-client-r3-03: it reports a record count as bytes, but it is a public error type.

## How it was tested

- **A failing test first.** Each Rust fix commit adds a test that shows the wrong result before the change and passes after it. The chaos fixes add cases to `chaos/orchestrator/test_gate.py` and `test_faults.py`, the demo-client fixes add `test_empty_nack.py`, `desync_empty_nack.ts` and `desync_crossed_reply.ts` (now run by CI's conformance job), and the docs fixes add `docs_drift` guards. The deploy shell-script fixes have no in-tree harness; their failing case is the verifier's reproduction on beast (for example 510 false "missing" in 1000 runs for the `pipefail` bug).
- **Quick gate per fix, in jobq on beast** (`.temper/gate-quick.sh`: fmt, clippy with default and all features, the changed crates' tests, the drift guards), in the shared epic worktree.
- **A regression agent per fix**, in its own throwaway worktree, trying to break what sits next to the fix. It found the `25006` docs regression (fixed, ef52f0b), the two attest weaknesses (reverted) and the attest-chain doc mismatch after the `RecordId` change (fixed, 07e29ab). No other regressions.
- **A second sweep over the branch's own changes** (8 Nov, six `r4` lenses, 67 files): 16 findings verified, 0 refuted, all fixed in round 6 (13 items after deduplication, plus the SIGTERM test race).
- **The full gate** (`.temper/gate-full.sh`, the set CI runs). It passed at 5afbe2e on 7 Nov (jq-01M6VZK1V1EHDBCQM1HA5E8GXN, `GATE full rc=0`) and PR #77's remote CI was all green; it is run again on this tip in stage 5, and the result is added below. Native macOS tests passed at 5afbe2e (20 test executables, 0 failing).
- **One flaky rerun, flagged and since fixed.** A quick gate on 7 Nov failed once on `sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit` (ECONNRESET instead of EOF) and passed on a rerun. It did not reproduce in 150 isolated runs; the cause was the race above, fixed in 6648740 without loosening the assertion.

### Evidence (owner steps)

See `docs/superpowers/temper/cleanup-2026-11/owner-checklist.md` and `owner-steps.sh`.

- The chaos power-cut campaign on this branch (root). It should exercise the recovery and fsync changes (1aa2c4f, 04f0d4c, 1b004da, f7efcc9, 00a42a5, 5cb6066).
- A check against real ClickHouse, Postgres, MySQL, S3 and an idempotent HTTP endpoint of the new batch key and `RecordId`: byte-identical records in distinct segments are all stored, a replay is still deduplicated, and the one-time re-send at upgrade is as described. Also the reclassified sink errors (`25006`; MySQL `1040`/`1203`/`1053`; S3 `AuthorizationHeaderMalformed`). The unit and sink-integration tests cover the keys and classification against mocks and the CI containers only.
- The darwin TLS smoke of the gate-built binaries, on the Mac.

## Open items

- weir-ctl-and-attest-r3-01 and -r3-03 (important): `attest verify` reports BROKEN_LINK when the drain reclaims a segment out of order, and errors (exit 2) on a segment deleted mid-run. Moved to the `attest-disposal-log` epic, with weir-ctl-and-attest-r3-04 (`.attest` sidecars never removed) and chaining over the delivered `RecordId`.
- D10: an explicit missing `--config` still only WARNs. Hard error in 4.2.0, epic `d10-missing-config-error`.
- wire-socket-and-client-r3-03 (owner, public error type): `PayloadTooLarge` reports a record count as bytes.
- The owner evidence steps above.
- Throwaway worktrees under beast's `~/work/weir/cleanup-2026-11/tmp/`, some holding probe edits from agents that died at the usage limit on 5 Nov: for `/temper:cleanup` or by hand.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
