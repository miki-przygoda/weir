# d10-and-batch-error: log

## 2026-11-10: spec and plan

- Owner decisions (10 Nov): ship with #77 as 4.2.0; Docker CMD becomes `["weir-server"]`; implicit missing default config → INFO (NOOP SINK WARN when the sink is noop); new `ClientError::TooManyRecords { count, limit }`; the systemd unit keeps its explicit `--config`.
- Plan: wave 0 `d10-hard-error` (C1) and `client-too-many-records` (C2) in parallel (gate_slots 2); wave 1 `changelog-upgrade-notes` (owns CHANGELOG.md). Behaviour groups `config-startup`, `client-batch`.
- Branch stacked on `epic/cleanup-2026-11`; rebase onto `main` after #77 merges, before the build runs, so the build starts from merged main.
