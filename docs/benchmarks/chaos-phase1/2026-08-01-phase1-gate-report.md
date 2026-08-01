# weir chaos run — Phase 1 (spine)

## Run metadata

- **Kernel:** 6.12.54-linuxkit
- **Seed:** 0x5eed

## Result

**20 episodes, 0 violations, 0 anomalies.**

A violation is a durability failure (I1/I2): weir lost or leaked a record. An anomaly is the harness failing to observe an episode cleanly — a quiescence timeout, a dead observer, or no measurable progress — and is **not**, by itself, evidence of a weir defect.

## Totals

Run totals as of the last verified episode (cumulative — NOT a sum across episodes).

| Metric | Value |
|---|---|
| Acked records | 7383717 |
| Distinct delivered | 7383795 |
| Unknown (indeterminate) | 152 |
| Deliveries per distinct record (1.000 = no redelivery) | 1.006 |

This is a **multiplicity factor**, not a percentage: 1.000 means no redelivery, 2.000 means every record arrived twice on average. At-least-once delivery makes duplicates conformant — this is what a crash actually costs a sink that has to dedupe, which weir's own docs require but never quantify.

## Episodes

Nacked/Pushed are cumulative counts (as stored in the episode record, same basis as the Totals table above), not per-episode deltas — labelled explicitly so they are never read the way the pre-I1-fix totals were. I1 exempt / Pending prov. are the frontier-exemption counts from the same check (see Provenance anomalies above): how many would-be I1/orphan hits were excused as not-yet-caught-up rather than lost.

| # | Fault | Quiesced | Verdict | Acked Δ | Delivered Δ | Dup rate | Unknown | Nacked (cum.) | Pushed (cum.) | I1 exempt | Pending prov. | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | kill_random | yes | PASS | 263290 | 263290 | 1.000 | 0 | 0 | 263290 | 0 | 823 | — |
| 1 | kill_random | yes | PASS | 390563 | 390567 | 1.000 | 8 | 0 | 653861 | 0 | 237 | — |
| 2 | kill_random | yes | PASS | 274657 | 274661 | 1.000 | 16 | 0 | 928526 | 0 | 222 | — |
| 3 | kill_random | yes | PASS | 450115 | 450119 | 1.000 | 25 | 0 | 1378650 | 0 | 350 | — |
| 4 | kill_random | yes | PASS | 351671 | 351679 | 1.000 | 40 | 0 | 1730336 | 0 | 0 | — |
| 5 | kill_random | yes | PASS | 380419 | 380419 | 1.012 | 40 | 0 | 2110755 | 0 | 395 | — |
| 6 | kill_random | yes | PASS | 387560 | 387564 | 1.010 | 48 | 0 | 2498323 | 0 | 886 | — |
| 7 | kill_random | yes | PASS | 423006 | 423010 | 1.009 | 56 | 0 | 2921337 | 0 | 118 | — |
| 8 | kill_random | yes | PASS | 331812 | 331817 | 1.008 | 64 | 0 | 3253157 | 0 | 220 | — |
| 9 | kill_random | yes | PASS | 458312 | 458316 | 1.007 | 72 | 0 | 3711477 | 0 | 428 | — |
| 10 | kill_random | yes | PASS | 336432 | 336436 | 1.006 | 80 | 0 | 4047917 | 0 | 418 | — |
| 11 | kill_random | yes | PASS | 402859 | 402863 | 1.006 | 88 | 0 | 4450784 | 0 | 571 | — |
| 12 | kill_random | yes | PASS | 381159 | 381163 | 1.010 | 96 | 0 | 4831951 | 0 | 575 | — |
| 13 | kill_random | yes | PASS | 317650 | 317654 | 1.009 | 104 | 0 | 5149609 | 0 | 331 | — |
| 14 | kill_random | yes | PASS | 352032 | 352036 | 1.008 | 112 | 0 | 5501649 | 0 | 914 | — |
| 15 | kill_random | yes | PASS | 366536 | 366541 | 1.008 | 121 | 0 | 5868194 | 0 | 319 | — |
| 16 | kill_random | yes | PASS | 327946 | 327950 | 1.008 | 128 | 0 | 6196147 | 0 | 620 | — |
| 17 | kill_random | yes | PASS | 443410 | 443414 | 1.007 | 136 | 0 | 6639565 | 0 | 773 | — |
| 18 | kill_random | yes | PASS | 306596 | 306600 | 1.007 | 144 | 0 | 6946169 | 0 | 362 | — |
| 19 | kill_random | yes | PASS | 437692 | 437696 | 1.006 | 152 | 0 | 7383869 | 0 | 778 | — |

## Limitations

- Phase 1 injects **random SIGKILL only**. Targeted mid-fsync kills, power loss, torn writes, disk-full, slow disk, read-only remount and dead-letter exhaustion are Phases 2-3 and are **not** covered by this run.
- Invariant I1 is **not yet tier-aware**: all tiers are held to zero loss, which is correct for process-crash but will need relaxing for Buffered under power loss in Phase 2.
- The seed reproduces the **schedule**, not the exact interleaving. Real kernel, real timing, real I/O — full determinism is not claimed.
- Single host, single filesystem, single hardware configuration.
