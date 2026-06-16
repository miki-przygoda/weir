# Test-coverage sweep — findings & dispositions (2026-06-16)

> Focused 8-agent test-coverage sweep on `v1/phase-5-sweep` (5 subsystem + 2
> invariant/property agents + completeness critic), with adversarial verification
> rejecting anything already-covered, tautological, or flaky. Goal: pin
> load-bearing logic no test proves. Directives: implement genuinely-untested
> **logic** gaps; if a gap reveals a problem needing a code change, **flag it**
> (don't write a test that locks in wrong behavior). Raw data: `findings.json`.

**21 raw → 18 confirmed** (17 test gaps + 1 flagged logic problem).

## Test gaps — ALL 17 IMPLEMENTED (gated, committed)

Each is a genuinely-uncovered branch/property, verified against the existing
suite. No production logic changed.

**WAB / recovery / replay**
- `recovery_truncates_mid_payload_keeping_valid_prefix` — the mid-PAYLOAD
  truncation branch (full len+crc, short payload).
- `recover_open_segments_isolates_a_corrupt_segment_from_healthy_siblings` —
  one un-recoverable segment is quarantined+skipped without aborting recovery of
  its healthy siblings.
- `replay_skips_already_confirmed_segment` — replay skips a `.confirmed` segment
  while still replaying its unconfirmed sibling (dedup half of at-least-once).
- `segment_reader_rejects_oversized_record_len_before_allocation` — a forged
  over-cap record length is rejected with InvalidData before any allocation.

**Drain**
- `oversized_dead_letter_batch_overshoots_cap_instead_of_blocking` — a single
  over-cap batch is written once (overshoot), not Blocked (F03).
- `resumed_segment_with_read_error_quarantines_not_panics` — a resumed (skip>0)
  segment hitting a read error quarantines via the early-return, never panics or
  confirm+deletes (G10).

**Socket / TLS**
- `tls_handshake_timeout_drops_stalled_tcp_client` — a stalled TLS handshake is
  dropped within ~handshake_timeout and the failure metric increments (F29).

**Sinks**
- `read_body_capped_stops_at_cap_on_oversized_body` — the response-body memory
  cap (S28).
- `http_permanent_status_truncates_large_body_excerpt` — a large 4xx body is
  truncated to a bounded dead-letter excerpt.
- `percent_decode_assembles_multibyte_utf8` / `_invalid_utf8_is_lossy_not_panic`
  — credential percent-decoding assembles multi-byte UTF-8 and is lossy, not a
  panic (F34).

**Config / env / queue**
- `empty_log_level_falls_back_to_info_not_silent` (F58).
- `metrics_bind_invalid_ip_rejected_and_valid_parsed`.
- `tcp_bind_rejects_relative_or_traversing_tls_paths` (S42).
- `env_bool_accepts_lenient_spellings` / `env_parse_wraps_parse_errors_with_field_name` (F57).
- `try_push_returns_unit_back_when_{full,disconnected}`.

**Durability invariant (integration)**
- `acked_records_delivered_to_sink_and_confirmed_after_crash_restart` — extends
  the crash-recovery tests to the delivery+confirm end state (the full
  crash→recovery→replay→drain→sink→confirm loop).

## Flagged — logic problem (NOT fixed; needs a code change)

- **L00 — short-header quarantine branch skips the quarantine metrics.** In
  `wab/recovery.rs::recover_segment`, the header-too-short branch quarantines the
  segment and returns Err but does NOT increment `recovery_segments_quarantined`
  or bump `wab_segments{state=quarantined}` — both of which the bad-magic and
  bad-version quarantine branches do. So a segment whose header was torn off
  (a plausible crash / at-rest truncation shape) is quarantined **invisibly** to
  operators alerting on those metrics, and the state-transition count is wrong.
  Fix (deferred): increment the two metrics in the short-header branch too, or
  funnel all three quarantine branches through one helper that always increments.
  A test is intentionally NOT added (it would lock in the current zero-metric
  behavior); add it alongside the fix.
