# cleanup-2026-11: owner evidence checklist

These are the checks only the owner can run: they need root, the Mac, docker
on beast, or real database servers. No agent ran any of them. The commands are in
[`owner-steps.sh`](owner-steps.sh), next to this file. Run `bash owner-steps.sh help`
to list them. Each step saves its logs under `~/weir-evidence/cleanup-2026-11/`
on the machine it ran on.

Test the sha the head is about to push: the tip of `epic/cleanup-2026-11` in
beast's clone (`/srv/bulk/Projects/weir/src`). Every step resolves that ref and
prints the sha it used. The script makes detached worktrees under
`~/work/weir/cleanup-2026-11/owner-*`, so the epic branch stays checked out in no
worktree. It never pushes, merges or deletes anything.

The full gate (`.temper/gate-full.sh`) is not on this list. The head runs it in
jobq and reports it in the PR.

## 1. Chaos power-cut campaign on this branch (beast, root)

This tests the recovery and fsync changes under real power loss: 1aa2c4f
(quarantine before truncating a corrupt length), 04f0d4c (`wab_dir` fsync after
`shard_NN`), 1b004da (`wab_dir` fsync before truncation), f7efcc9 (only
`shard_<N>` directories are shards) and 5cb6066 (failed timed seal). Since the
first evidence commit the branch also changed the paths every episode crosses
on restart and drain: 49c4188 (every `RecordId` names the segment instance),
0081664 and 2ad211a (pushes stay nacked until a WAB scan succeeds) and 00a42a5
(the size scan counts only `shard_<N>`). Every episode ends in a dm-flakey
power cut, followed by a recovery on restart. Run it on the current tip, not
on 5afbe2e.

- [ ] **Setup:** run `bash owner-steps.sh chaos-setup` on beast, as your own
  user. It makes `owner-chaos` at the epic tip and builds `weir-server` with
  `--features test-hooks`, which `startup-signal.toml` needs. It also builds the
  chaos harness, runs the oracle's unit tests and records the daemon's sha256.
  Check `jobq list` first. A full gate running at the same time slows the build
  and skews the run.
- [ ] **Campaign:** open your own tmux session (`tmux new -s weir-chaos`). Then
  run `bash ~/work/weir/cleanup-2026-11/owner-chaos/docs/superpowers/temper/cleanup-2026-11/owner-steps.sh chaos-campaign`.
  It asks for your sudo password once and keeps the timestamp warm. It then runs
  `shutdown-bounded`, `shutdown-forced`, `startup-signal` and `pause-resume-cut`
  one after another, then `gate.py` on all four.
  - 300 episodes each, so 1,200 cuts in total. Each schedule stops itself at 6 h
    and still does its final pass. Expect 12-24 h.
  - Each run sits under `owner-chaos/chaos/runs/<id>/`, with `report.md`,
    `SETUP.md` and `console.log`. The console logs are also saved in the
    evidence dir.
  - To keep jobq from starting other work during the campaign, run
    `jobq drain` (owner only), and `jobq resume` afterwards.
- [ ] **Pass:** the last line is `GATE chaos rc=0`. That needs at least 1,000 cuts
  in total and at least 200 per schedule, 0 oracle violations, the canary `bit`
  on every cut, 0 anomalies, and a final pass in every run.
  - An anomaly is not a violation. If one needs to be allowed, rerun
    `owner-steps.sh chaos-gate runs/<a> runs/<b> runs/<c> runs/<d> --allow-anomalies`
    and explain each anomaly in the PR. The 4.1 rerun had one
    (`stranded_total != resumed_total` in quiescence, episode 517). See
    `chaos/README.md`.
  - A violation is a stop: the branch does not ship until it is understood.
- [ ] **Optional, the plain Durable tier:** `bash owner-steps.sh chaos-durable`,
  also inside tmux. It runs `powerloss-durable.toml` capped at 6 h through a
  copy in `chaos/schedules/generated/` (gitignored). Pass: `rc=0`, no loss, the
  canary `bit` on every cut.
- [ ] **Paste back:** the `chaos-gate-*.txt` verdict, the four run ids, the
  weir-server sha256, and the episode/acked totals from each `report.md`.

## 2. The new batch key and RecordId against real servers (beast, docker)

Two contract changes landed on 8 Nov, and the unit tests only pin their
formulas: 6679c8f (`dedup-coordinate-key`: the ClickHouse
`insert_deduplication_token` and the HTTP NDJSON `Idempotency-Key` are now a
hash of the batch's place in the WAB) and 49c4188 (`recordid-segment-instance`:
every `RecordId` names `shard_NN/<file>@<created_at hex>`). This step shows
both against real ClickHouse, Postgres, MySQL and MinIO in weir's test rig
(`deploy/docker/test/docker-compose.yml`, the `weir-test-*` containers,
unchanged), and against a local idempotent HTTP endpoint that the step starts
on 127.0.0.1:19387. That endpoint keeps a body once per `Idempotency-Key` and
answers a repeated key with its cached 200, as a real one does.

- [ ] Run `bash owner-steps.sh sinks-setup` (skip it if the build is already
  at the tip). It makes `owner-sinks` at the epic tip and builds `weir-server`
  with the clickhouse and s3 sinks, plus `weir-ctl`. Check `jobq list` first.
- [ ] Run `bash owner-steps.sh dedup-check`, or name cases from the table
  below. It refuses to start while ports 33306, 55432, 18123, 19000, 19386 or
  19387 are in use, while any `weir-test-*` container exists (running or
  stopped), or while a `test_default` network exists: those belong to a gate's
  `sink-integration` step, so wait for it. It tears the rig down (`down -v`)
  and stops the endpoint when it exits. About 5 minutes.

Each case runs its own daemon on a fresh `wab_dir`, with
`wab_segment_max_age_secs = 1`, and pushes one byte-identical record (a
heartbeat). A **heartbeat** case pushes it three times, 3 s apart, so each copy
is sealed into its own segment. A **restart** case pushes it once, waits until
it arrives and the shard is empty, stops the daemon, starts it again on the
same `wab_dir` (segment names restart at `seg_00000001`) and pushes it again.

| Case | Target | Before this branch | PASS means |
|---|---|---|---|
| `ch-heartbeat` | ClickHouse `default.weir_records` (dedup window 100) | rows 2 and 3 dropped with a 200: one shared token | 3 rows |
| `http-ndjson-heartbeat` | the endpoint, `sink_http_batch = "ndjson"` | batches 2 and 3 answered from the cache | 3 records stored, under 3 keys |
| `s3-heartbeat` | MinIO | already coordinate-keyed: the control | 3 objects |
| `ch-restart` | ClickHouse | the second insert dropped | 2 rows |
| `pg-restart` | Postgres `weir_records_keyed`, `sink_postgres_id_column = "record_id"` | one `record_id`, the second row dropped by `ON CONFLICT DO NOTHING` | 2 rows, 2 distinct `record_id`s |
| `mysql-restart` | MySQL `weir_records_keyed`, `sink_mysql_id_column = "record_id"` | the same, by `INSERT IGNORE` | 2 rows, 2 distinct `record_id`s |
| `http-record-restart` | the endpoint, `sink_http_batch = "none"` | the second record answered from the cache | 2 records stored, under 2 keys |

Every case also needs 0 dead-letter bytes.

- [ ] **Pass:** `DEDUP CHECK failures=0` in `dedup-check-<utc>/summary.txt`.
  - INCONCLUSIVE is not a failure, but it means the case did not exercise the
    fix: a heartbeat case whose three copies may have shared one segment, or a
    restart case where no segment name was seen in both phases (the evidence
    lists the files left in the WAB after phase 1). Rerun that case by name.
  - `clickhouse-insert-tokens.tsv` lists the `insert_deduplication_token` of
    every insert ClickHouse logged: on this branch each is a distinct 64-hex
    key. `http-requests.jsonl` has every request the endpoint took, its key, and
    whether it was a replay.
- [ ] **Optional, shows the checks catch the bug:** `bash owner-steps.sh
  baseline-setup`, then `bash owner-steps.sh dedup-check --baseline`. That
  runs the same cases against a build of the merge base with `main` (4.1.x).
  Expect FAIL on every case except `s3-heartbeat`, and the tokens file to show
  one token used three times. If a case passes on the baseline, it does not
  test what it claims: say so in the PR rather than counting its PASS above.
- [ ] **Optional, the one-time re-send on upgrade** (the `### Changed` notes):
  stop ClickHouse (`docker stop weir-test-clickhouse`) while a 4.1.x daemon has
  a sealed segment stranded, stop that daemon, start this branch's build on the
  same `wab_dir`, then start ClickHouse. Expect the batch delivered once under
  the new key: a second copy appears only if 4.1.x had already inserted it.
- [ ] **Paste back:** both `summary.txt` files (branch and, if run, baseline)
  and `clickhouse-insert-tokens.tsv`.

## 3. Native macOS tests (the Mac, before the push)

CI only builds on macOS and never runs a macOS test. This step is the only
place the `cfg(target_os = "macos")` paths execute.
The head ran it at 5afbe2e on 7 Nov (20 executables, 0 failing); the branch
has moved since, so it is needed again at the tip.

- [ ] Make a local worktree of `epic/cleanup-2026-11` at the same sha as
  beast's tip. The step refuses to run if the two differ.
- [ ] Run `bash owner-steps.sh mac-tests <local worktree>`. It runs
  `.workdocs/operability-2026-10-05/mac-tests.sh`, which cross-builds the test
  executables on beast in docker and runs them natively on the Mac.
- [ ] **Pass:** `MAC TESTS DONE: 0 failing executables`. Paste back
  `mac-summary.txt`. A failure is a blocked item, not a push.

## 4. darwin TLS smoke (the Mac, after the full gate)

- [ ] Run `bash owner-steps.sh darwin-smoke` after the head's full gate has
  passed on the sha that will be pushed. It finds the newest full gate on
  beast whose summary names that sha (it stops if there is none or it did not
  pass), reads its gate slot from `jobq-output.log` (or `SLOT=a`..`d`), and
  copies the `aarch64-apple-darwin` and `x86_64-apple-darwin` `weir-server`
  builds out of that slot's volume. It stops if a binary was not built by that
  gate's build step: a later gate in the same slot overwrites it. Then it
  ad-hoc signs them and runs CI's check. That check starts a daemon with `--tcp-bind` and no
  certificate: a TLS build fails on `tls_cert_path`, and a build without TLS
  says "built without the 'tls' feature". The x86_64 build runs only under
  Rosetta.
- [ ] `full-gate.txt` holds that gate's summary and each binary's build time
  against its build step's log time.
- [ ] **Pass:** `DARWIN SMOKE rc=0` and `TLS smoke OK` for aarch64. Paste back
  `summary.txt` and `sha256.txt`.

## 5. Optional: reclassified sink errors against real servers (beast, docker)

The unit tests cover only the classification tables and the redaction function.
This step checks that a real server produces each error, and that the daemon
strands the segment and does not dead-letter it. It uses weir's own test rig
(`deploy/docker/test/docker-compose.yml`, the `weir-test-*` containers).
MinIO gets one change from a temporary override file: `MINIO_SITE_REGION=eu-west-1`.

- [ ] Uses the `owner-sinks` build from step 2 (`sinks-setup`, a separate
  worktree because the chaos build uses different features).
- [ ] Run `bash owner-steps.sh sink-check`, or name cases from the table below.
  It makes the same checks as `dedup-check` before it starts (ports, leftover
  `weir-test-*` containers, `test_default`), and tears the rig down (`down -v`)
  when it exits.

| Case | Fix | What it injects | PASS means |
|---|---|---|---|
| `pg-25006` | sinks-r3-05, 05b1d00 | a `BEFORE INSERT` trigger that raises SQLSTATE `25006` | stranded, 0 dead-letter bytes, all 20 rows after the trigger is dropped |
| `mysql-1203` | sinks-r3-06, ade74f3 | a user with `MAX_USER_CONNECTIONS 1` whose one connection another session holds | the same, after the holder is killed |
| `mysql-1040` | sinks-r3-06, ade74f3 | `max_connections = 1`, held by another user | the same, after the limit is restored |
| `s3-region` | sinks-r3-04, ed290f8 | MinIO in `eu-west-1`, the daemon signing for `us-east-1` | stranded on `AuthorizationHeaderMalformed`, 0 dead-letter bytes, objects present after a restart with the right region |
| `ch-redact` | sinks-r3-03, 126e8a3 | a `?password=<random canary>` sink URL pointed at a closed port | the canary appears in none of the daemon log, `/status` or `weir-ctl status`, and the transport-error path ran |

- [ ] **Pass:** `SINK CHECK failures=0` in `sink-check-<UTC>/summary.txt`.
  INCONCLUSIVE means the rig did not reproduce the condition, for example if
  this MinIO image ignores `MINIO_SITE_REGION`. It is not a failure. Use the
  matching variant below. Paste back `summary.txt`.
- [ ] **Optional variants**, by hand, closer to production:
  - Postgres `25006` from a real read-only target. Point `WEIR_SINK_URL` at a
    hot standby, or run
    `ALTER DATABASE weir_test SET default_transaction_read_only = on` before
    starting the daemon. Write down whether delivery resumes without a daemon
    restart once the database is writable again, because pooled sessions keep
    the setting they connected with.
  - MySQL `1053`: `docker restart weir-test-mysql` during a large push. The
    timing is not deterministic. Pass: no dead-letter bytes, and every record
    arrives after the restart.
  - S3 wrong region on real AWS: a bucket in `eu-west-1`, with
    `sink_s3_region = "us-east-1"` and no `sink_s3_endpoint`. Give the
    credentials through the environment only. Pass: stranded, not
    dead-lettered, and delivered after a restart with the right region.

## What goes back to the head

Put the pasted results in the PR's evidence section, one line per step: the
step, the sha it ran on, pass or fail, and where the logs are. A failed step is
a blocked item for the PR, not something to rerun until it passes.
