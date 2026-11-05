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
