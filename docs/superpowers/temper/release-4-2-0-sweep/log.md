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
