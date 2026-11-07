# cleanup-2026-11: owner evidence checklist

These are the checks only the owner can run: they need root, the Mac, or real
database servers. No agent ran any of them. The commands are in
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
`shard_<N>` directories are shards) and 5cb6066 (failed timed seal). Every
episode ends in a dm-flakey power cut, followed by a recovery on restart.

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

## 2. Native macOS tests (the Mac, before the push)

CI only builds on macOS and never runs a macOS test. This step is the only
place the `cfg(target_os = "macos")` paths execute.

- [ ] Make a local worktree of `epic/cleanup-2026-11` at the same sha as
  beast's tip. The step refuses to run if the two differ.
- [ ] Run `bash owner-steps.sh mac-tests <local worktree>`. It runs
  `.workdocs/operability-2026-10-05/mac-tests.sh`, which cross-builds the test
  executables on beast in docker and runs them natively on the Mac.
- [ ] **Pass:** `MAC TESTS DONE: 0 failing executables`. Paste back
  `mac-summary.txt`. A failure is a blocked item, not a push.

## 3. darwin TLS smoke (the Mac, after the full gate)

- [ ] Run `bash owner-steps.sh darwin-smoke` after the head's full gate has
  finished on the sha that will be pushed. It copies the gate's
  `aarch64-apple-darwin` and `x86_64-apple-darwin` `weir-server` builds out of
  the newest gate-slot volume (set `SLOT=a`..`d` to pick one), ad-hoc signs
  them and runs CI's check. That check starts a daemon with `--tcp-bind` and no
  certificate: a TLS build fails on `tls_cert_path`, and a build without TLS
  says "built without the 'tls' feature". The x86_64 build runs only under
  Rosetta.
- [ ] Check that `full-gate.txt` shows the gate for this sha, and that it
  finished before the build time printed.
- [ ] **Pass:** `DARWIN SMOKE rc=0` and `TLS smoke OK` for aarch64. Paste back
  `summary.txt` and `sha256.txt`.

## 4. Optional: reclassified sink errors against real servers (beast, docker)

The unit tests cover only the classification tables and the redaction function.
This step checks that a real server produces each error, and that the daemon
strands the segment and does not dead-letter it. It uses weir's own test rig
(`deploy/docker/test/docker-compose.yml`, the `weir-test-*` containers).
MinIO gets one change from a temporary override file: `MINIO_SITE_REGION=eu-west-1`.

- [ ] Run `bash owner-steps.sh sinks-setup`. It makes `owner-sinks` and builds
  `weir-server` with the clickhouse and s3 sinks, plus `weir-ctl`. This is a
  separate worktree, because the chaos build uses different features.
- [ ] Run `bash owner-steps.sh sink-check`, or name cases from the table below.
  Don't run it while a gate's `sink-integration` job holds the rig's ports
  (33306, 55432, 18123, 19000). The step checks the ports first. It tears the
  rig down (`down -v`) when it exits.

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
