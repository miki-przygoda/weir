# weir chaos run — Phase 1 (spine)

## Run metadata

- **Kernel:** 6.12.54-linuxkit
- **Seed:** 0x5eed

## Result

**3 episodes plus a final verification pass, 0 violations, 0 anomalies.**

A violation is a durability failure (I1/I2): weir lost or leaked a record. An anomaly is the harness failing to observe an episode cleanly — a quiescence timeout, a dead observer, or no measurable progress — and is **not**, by itself, evidence of a weir defect.

## Totals

Run totals as of the last verified record (cumulative — NOT a sum across episodes). When a final pass ran, that is the record they come from: it is the most complete view of the run, taken after weir's shutdown drain delivered everything it still held.

| Metric | Value |
|---|---|
| Acked records | 164680 |
| Distinct delivered | 164691 |
| Unknown (indeterminate) | 25 |
| Deliveries per distinct record (1.000 = no redelivery) | 1.000 |

This is a **multiplicity factor**, not a percentage: 1.000 means no redelivery, 2.000 means every record arrived twice on average. At-least-once delivery makes duplicates conformant — this is what a crash actually costs a sink that has to dedupe, which weir's own docs require but never quantify.

## Final pass

One verification pass after the last episode, with the producer stopped and weir's SIGTERM drain — a **full** drain, not a seal-and-exit — given a real chance to finish.

Checked at **`frontier_slack=0`** — zero exemption. This is the only moment in a run where that is a *true* statement rather than a stricter-than-reality one: the producer is stopped and both logs are complete, so nothing is legitimately still in flight.

| Teardown step | Result |
|---|---|
| Load generator exit code | 0 |
| Daemon alive before shutdown | yes |
| Shutdown drain completed without a kill | yes |
| Recorder still answering | yes |
| `/metrics` at shutdown | stranded=0.0, resumed=0.0, queue_depth=0.0 |

### WAB post-mortem

The WAB directory was empty of backlog after shutdown: no sealed segment without a `.confirmed` sidecar, no non-empty active segment, nothing quarantined, nothing dead-lettered.

## Episodes

Every count column is a per-episode DELTA of a cumulative total — acked, delivered, nacked and pushed alike, so the row is internally consistent and no column silently means something different from the one beside it. (Nacked/Pushed were cumulative here until D3, sitting next to deltas.) I1 exempt / Pending prov. are the frontier-exemption counts from the same check (see Provenance anomalies above): how many would-be I1/orphan hits were excused as not-yet-caught-up rather than lost. The `final` row is the end-of-run pass, not an episode: no fault was injected and no quiescence wait ran, hence `n/a`.

| # | Fault | Quiesced | Verdict | Acked Δ | Delivered Δ | Dup rate | Unknown | Nacked Δ | Pushed Δ | I1 exempt | Pending prov. | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | kill_random | yes | PASS | 42044 | 42044 | 1.000 | 0 | 0 | 42044 | 0 | 787 | — |
| 1 | kill_random | yes | PASS | 92131 | 92136 | 1.000 | 10 | 0 | 92141 | 0 | 751 | — |
| 2 | kill_random | yes | PASS | 29833 | 29835 | 1.000 | 17 | 0 | 29840 | 0 | 676 | — |
| final | none | n/a | PASS | 672 | 676 | 1.000 | 25 | 0 | 680 | 0 | 0 | — |

## Limitations

- Phase 1 injects **random SIGKILL only**. Targeted mid-fsync kills, power loss, torn writes, disk-full, slow disk, read-only remount and dead-letter exhaustion are Phases 2-3 and are **not** covered by this run.
- Invariant I1 is **not yet tier-aware**: all tiers are held to zero loss, which is correct for process-crash but will need relaxing for Buffered under power loss in Phase 2.
- The seed reproduces the **schedule**, not the exact interleaving. Real kernel, real timing, real I/O — full determinism is not claimed.
- Single host, single filesystem, single hardware configuration.
