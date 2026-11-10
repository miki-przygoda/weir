# d10-and-batch-error: log

## 2026-11-10: spec and plan

- Owner decisions (10 Nov): ship with #77 as 4.2.0; Docker CMD becomes `["weir-server"]`; implicit missing default config → INFO (NOOP SINK WARN when the sink is noop); new `ClientError::TooManyRecords { count, limit }`; the systemd unit keeps its explicit `--config`.
- Plan: wave 0 `d10-hard-error` (C1) and `client-too-many-records` (C2) in parallel (gate_slots 2); wave 1 `changelog-upgrade-notes` (owns CHANGELOG.md). Behaviour groups `config-startup`, `client-batch`.
- Branch stacked on `epic/cleanup-2026-11`; rebase onto `main` after #77 merges, before the build runs, so the build starts from merged main.

## 2026-11-10: owner approved the spec; #77 merged; build launched

- Owner (10 Nov): spec approved; "continue and make decisions if needed"; merge #77 if fully green on remote.
- PR #77 merged by the head after its remote CI was fully green at f78181d (19 pass, 2 main-only jobs skipped): merge commit c1becf1. The epic branch is rebased onto main (no conflicts).
- Head decision: the build's checkpoint judges the full gate on its native steps only (miki_przygoda has no Docker since 9 Nov); the head runs the Docker half with `.workdocs/gate-docker-half.sh` before the PR.
