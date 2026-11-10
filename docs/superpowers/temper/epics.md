# Epics

<!-- The source of truth for the epics, their status and the contracts between them. temper updates Status at every stage change. -->
<!-- Status is one of: spec, planned, building, stabilising, ready, merged. -->

| Epic | Status | Needs (contracts from other epics) | Spec |
|---|---|---|---|
| cleanup-2026-11 | ready | none | [spec](cleanup-2026-11/spec.md) |
| attest-disposal-log | spec | none | (to write) daemon appends a hash-chained record of each delivered or quarantined segment's attested head; `attest verify` excuses a gap only by a matching entry (owner, 8 Nov). Also covers the false BROKEN_LINK on out-of-order drain deletes, the NotFound on a segment deleted mid-run, and `.attest` sidecars never being removed. Also: chain over the RecordId the drain delivers (`<shard>/<segment>@<created_at>` since PR #77), so a head can be recomputed from the downstream's rows again; needs a versioned sidecar (owner, 9 Nov). |
| d10-and-batch-error | planned | cleanup-2026-11 (stacked; rebase onto main after #77) | [spec](d10-and-batch-error/spec.md): D10 hard error for an explicit missing `--config` with the Docker CMD change, and `ClientError::TooManyRecords`. Ships with #77 as 4.2.0 (owner, 10 Nov). |

## Contracts between epics

None.
