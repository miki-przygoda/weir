# Drain subsystem — adversarial verification (sweep 2026-06-14)

Verified 11 findings against the code on `v1/phase-4-cleanup`: 10 confirmed real (1 high, 4 medium, 5 low) and 1 confirmed real but with a partially-overstated cross-restart claim; none fully refuted. The most serious is an at-least-once accounting gap (#1) and a hard livelock/poison pair (#2, #3).

## Confirmed (real)

### Partial dead-letter write leaves an orphan active file that poisons all future dead-lettering (within-run)
- **File:** `crates/weir-server/src/drain/dead_letter.rs:64`
- **Severity:** high
- **Argument:** `write_records` creates `dl_NNNNNNNN.wab` via `WabSegment::create` (which uses `create_new(true)`, segment.rs:53-62), then `write_record` (73) / `seal` (75), and only bumps `next_counter` on full success (80). If write or seal fails partway (ENOSPC/EIO under dead-letter pressure) the partial `.wab` stays on disk and the counter does not advance. `commit_batch` treats the dead-letter write failure as transient (mod.rs:657/716) and retries the segment; on retry `write_records` reuses the **same** counter and calls `create_new` on the still-present path → `AlreadyExists`. Dead-lettering is then permanently broken for the rest of the run: permanent sink errors can never be dead-lettered, so the segment never confirms and retries forever, and every other segment's dead-letter attempt fails too.
- **Verdict:** real — `create_new(true)` at segment.rs:59 + `next_counter` only advanced at dead_letter.rs:80 guarantees a same-counter `AlreadyExists` collision on retry; no cleanup/Drop exists. NOTE: the cross-restart half of the original claim is wrong — `recover_open_segments` (recovery.rs:30-45) iterates every subdir of `wab_dir` and only skips `quarantine`, so `dead_letter/` IS visited and `recover_segment` re-seals the partial `dl_NNN.wab` to `dl_NNN.wab.sealed`; the orphan does not survive restart as an unreadable file. The within-run poison is the genuine defect.

### Drain confirms+deletes a segment without verifying the sink's CommitResult covers every input record
- **File:** `crates/weir-server/src/drain/mod.rs:612`
- **Severity:** high
- **Argument:** On the `Ok(commit_result)` arm, the drain increments metrics from `committed.len()` (622) and dead-letters `dead_lettered` (624-660), then returns `BatchResult::Ok` → `Confirmed` → `confirm_and_delete` DELETEs the segment. There is no check that `committed.len() + dead_lettered.len() == payloads.len()`. The SDK `CommitResult`/`commit` docs (weir-sink-sdk/src/lib.rs:104-141) never state that the two vectors must partition the input batch. A non-conforming external sink that drops a record from both vectors would have it silently confirmed-and-deleted — a false ack with zero detection.
- **Verdict:** real — no accounting guard exists between mod.rs:612 and the `Ok` return at 662; built-in sinks (http.rs:335-364 push every record to exactly one bucket; SQL/ClickHouse are all-or-nothing) happen to be safe, so this only bites a contract-violating third-party sink, but the unguarded false-ack path is genuine.

### Single permanently-rejected batch larger than dead_letter_max_bytes wedges the drain in a permanent block↔retry livelock
- **File:** `crates/weir-server/src/drain/mod.rs:381`
- **Severity:** medium
- **Argument:** The unblock condition is `dead_letter.total_bytes() < config.dead_letter_max_bytes` (381) — it checks only that *some* headroom exists, not that THIS batch fits. If a single batch's estimate exceeds the cap, then with an empty dir (total=0): wake → unblock check `0 < cap` true → push segment back to Draining (384) → process_segment → same permanent error → `would_exceed_cap(estimated, cap)` = `0 + estimated > cap` true (dead_letter.rs:57-58) → `BatchResult::Blocked` → re-enter blocked, forever. No other pending segment drains while parked (the blocked arm only buffers into `pending`, lines 359/384), and `sink.commit()` is re-called every interval, re-POSTing the committed prefix.
- **Verdict:** real — the unblock predicate (381) and the cap predicate (dead_letter.rs:57) use different conditions that can never both clear for an oversized single batch; reachable because config validation (config/mod.rs:589-601) only rejects a 0 cap and warns under 1 MiB, so a small cap is permitted.

### Sink::health() is called with no timeout backstop; a hanging health() stalls the entire drain
- **File:** `crates/weir-server/src/drain/mod.rs:280`
- **Severity:** medium
- **Argument:** `rt.block_on(sink.health())` is called with no timeout at mod.rs:280, 296, and 379, whereas `sink.commit` is wrapped in `tokio::time::timeout(config.commit_timeout, ...)` at 593 precisely because third-party sinks carry no built-in timeout. A hanging `health()` wedges the single-threaded current-thread runtime: in the idle loop (280) it never returns to `recv`, so channel-close shutdown is never seen and segments pile up; at 379 (blocked arm) it runs *before* the `channel_closed` check at 386, so shutdown-while-blocked is also missed.
- **Verdict:** real — none of the three `block_on(sink.health())` sites is wrapped in a timeout, and 379 precedes the channel_closed branch at 386, so the same hang-defense rationale applied to commit is absent for health.

### Retried multi-batch segment re-dead-letters earlier sub-batches, amplifying duplicate dead-letter files
- **File:** `crates/weir-server/src/drain/mod.rs:529`
- **Severity:** medium
- **Argument:** `process_segment` commits sub-batches sequentially (527-547). If an early sub-batch dead-letters successfully (grows `total_bytes`, writes a file) and a later sub-batch returns `Transient`/`Blocked` (531-534/542-545), the whole segment is preserved and retried from the first record (re-opened at 485). On retry the early sub-batch dead-letters AGAIN — `write_records` has no dedup and always creates a new file (dead_letter.rs:64-83) — producing duplicate dead-letter records and consuming extra cap each pass; in the Blocked case this is a feedback loop that pushes the dir further over cap.
- **Verdict:** real — retry restarts the segment from record 0 and `write_records` is unconditional, so any earlier dead-lettered sub-batch is re-written on every retry; duplicate SINK delivery is contracted, but duplicate dead-letter FILES + cap thrash are an unintended amplification with no test.

### Dead-letter total_bytes silently undercounts on metadata() failure, bypassing the cap
- **File:** `crates/weir-server/src/drain/dead_letter.rs:78`
- **Severity:** medium
- **Argument:** After sealing a new dead-letter segment, size is `std::fs::metadata(&sealed).map(|m| m.len()).unwrap_or(0)` (78). If the stat fails, `file_bytes` becomes 0 and `total_bytes` is not incremented even though a real file exists. `total_bytes` is the sole gate in `would_exceed_cap` (57-58), consulted before every dead-letter write (mod.rs:632/692), so an undercount lets the drain write past `dead_letter_max_bytes` — unbounded disk growth and a never-tripped safeguard, plus an under-reported gauge. `rescan()` would correct it but only runs from the blocked-full wake (mod.rs:369), which the undercount prevents entering. The error is swallowed (no log, no metric).
- **Verdict:** real — `unwrap_or(0)` at dead_letter.rs:78 silently drops the stat error and skips the `total_bytes` increment, and `would_exceed_cap` has no other size source.

### Dead-letter cap accounting omits fixed 60-byte per-file segment overhead
- **File:** `crates/weir-server/src/drain/mod.rs:729`
- **Severity:** low
- **Argument:** `would_exceed_cap` is checked against `estimated_write_bytes = sum(len + 8)` per record (729-731), but `write_records` seals a full `WabSegment` whose on-disk size is `estimated + SEGMENT_HEADER_LEN(24) + SENTINEL(4) + SEGMENT_FOOTER_LEN(32) = estimated + 60` (format.rs:69-75). `total_bytes` is corrected with the real file size (dead_letter.rs:78), so each write can creep ~60 bytes past the cap; over many tiny dead-letter segments the slop is unbounded relative to the documented hard limit.
- **Verdict:** real — constants verified in format.rs (24/4/32) and the estimate (mod.rs:729) excludes all three, so the cap is a soft bound; self-correcting per-write but never exactly enforced.

### Swallowed rescan() error can wedge the drain blocked despite an operator-freed directory
- **File:** `crates/weir-server/src/drain/mod.rs:369`
- **Severity:** low
- **Argument:** While blocked, `let _ = dead_letter.rescan();` (369) discards any error. This rescan is what reflects operator deletions so the drain can unblock. If `read_dir` fails inside `scan_dir` (transient EIO, permissions blip), `total_bytes` keeps its stale at-cap value, the unblock check at 381 stays false, and the drain stays blocked even after the operator cleared the dir — until a later rescan happens to succeed. No metric, no log.
- **Verdict:** real — `let _ =` at mod.rs:369 drops the `io::Result` from `rescan`/`scan_dir`, and the unblock path depends entirely on the refreshed `total_bytes`.

### dead_letter_full counter and blocked_since reset on every unblock→reblock cycle
- **File:** `crates/weir-server/src/drain/mod.rs:466`
- **Severity:** low
- **Argument:** When a blocked segment gets headroom it unblocks to Draining (384), but if the next commit again returns `Blocked` it flows through `transition_from_draining → next_state_after_process → enter_blocked` (466), which increments `dead_letter_full` (468) AND creates a fresh `blocked_since` Instant (467). A flapping cap inflates `weir_dead_letter_full` past the number of distinct block episodes and repeatedly resets `dead_letter_blocked_duration` to ~0. The existing per-wake test does not cover the unblock/reblock cycle.
- **Verdict:** real — re-entry to blocked via the Draining path goes through `enter_blocked` (468/467), so each flap re-increments the counter and resets the duration; only the within-stint per-wake case is tested (mod.rs:1708).

### .confirmed sidecar created without explicit 0o600 mode, tripping the daemon's own recovery audit under any non-0o077 umask
- **File:** `crates/weir-server/src/drain/confirmed.rs:70`
- **Severity:** low
- **Argument:** `write_confirmed_durably` uses `std::fs::File::create(confirmed)` with no mode (confirmed.rs:70), unlike segments (`.mode(0o600)` at segment.rs:61). The `.confirmed` file (`.wab.confirmed`, format.rs:90) only lands at 0o600 because main.rs:181-183 sets `libc::umask(0o077)`. `audit_segment_modes` (recovery.rs:69-84) explicitly checks `EXT_CONFIRMED` files and flags any mode != 0o600 as "possible tampering," logging a warning and bumping `weir_wab_unexpected_mode`. So under any other umask (library embedding, supervisor reset, or removal of that one line) the sidecar becomes group/world-readable and the daemon's own audit raises a false-positive security alert on the next restart. The main.rs:166-171 comment claiming "every file-creation path … specifies its mode bits explicitly today" is the counterexample.
- **Verdict:** real — confirmed.rs:70 has no `.mode()`, the file matches `EXT_CONFIRMED` audited at recovery.rs:71, and the 0o600 result depends solely on the umask set in main.rs.

## Refuted / dismissed

None fully refuted. Finding #3's cross-restart claim ("orphan survives restarts; recovery never touches the dead_letter dir") is inaccurate — recovery DOES descend into `dead_letter/` (recovery.rs:30-45 skips only `quarantine`) and re-seals the partial `.wab` — but the finding's core within-run poison defect is real, so it is filed under Confirmed with that correction noted in its verdict reason.

## Coverage gap (tracked separately, not a code defect)

`DeadLetterWriter` (dead_letter.rs) and `confirmed.rs` have no `#[cfg(test)]` module (verified: zero `#[test]`/`mod tests` in either file). The drain tests only construct `DeadLetterWriter::open()` and fault it by removing the dir (B1). `scan_dir` counter-recovery across restart, the `would_exceed_cap` boundary, and `rescan` after external deletion are untested; the system test (system.rs:358-362) only checks dead-letter metric NAMES exist. This is the original finding #4 — a legitimate test gap that leaves the defects above (especially #3 counter reuse and #7 boundary) uncaught.
