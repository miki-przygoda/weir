# Epics

<!-- The source of truth for the epics, their status and the contracts between them. temper updates Status at every stage change. -->
<!-- Status is one of: spec, planned, building, stabilising, ready, merged. -->

| Epic | Status | Needs (contracts from other epics) | Spec |
|---|---|---|---|
| cleanup-2026-11 | stabilising | none | [spec](cleanup-2026-11/spec.md) |
| attest-disposal-log | spec | none | (to write) daemon appends a hash-chained record of each delivered or quarantined segment's attested head; `attest verify` excuses a gap only by a matching entry (owner, 8 Nov). Also covers the false BROKEN_LINK on out-of-order drain deletes, the NotFound on a segment deleted mid-run, and `.attest` sidecars never being removed. |
| d10-missing-config-error | spec | none | (to write) an explicit missing `--config` becomes a startup error in 4.2.0, with the Dockerfile CMD change and a `Changed` note (owner, 8 Nov). |

## Contracts between epics

None.
