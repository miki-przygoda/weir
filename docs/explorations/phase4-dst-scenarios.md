# Phase 4 — Deterministic Simulation / Chaos Fault Catalog

> **Scope**: Research artifact for the DST harness sibling agent. This document
> enumerates (1) the invariants weir must never violate, (2) every concrete
> fault the simulation must inject, (3) which faults existing tests already
> cover vs gaps, (4) a prioritized first batch to build, and (5) open questions.
>
> **Branch**: `v1/phase-3-performance`  
> **Date**: 2026-06-11

---

## 1. Core Durability and Correctness Invariants

These are the properties that must hold under every fault scenario. Assertions
in the DST harness must be anchored to exactly these invariants — not to
implementation details that may change.

### I-1 Sync-acked records are durable

Any record for which the daemon returned `Ack` on a `Durability::Sync` or
`Durability::Batched` push has been `fdatasync`-ed (Linux) or
`F_BARRIERFSYNC`-ed (macOS) to the WAB segment file *before* the ack frame was
sent. After a crash, the record must appear in the WAB and must survive crash
recovery.

**Code anchor**: `fsync_observed()` in `wab/mod.rs`; the `flush_batch()` loop
that sends the group fsync before firing any `sync_acks` or `batched_acks`.

### I-2 Buffered acks carry no durability promise

A `Durability::Buffered` ack only guarantees that the payload reached the
daemon's in-process buffer. A crash before the next fsync may lose the record.
No test should assert Buffered records survive a SIGKILL.

**Code anchor**: the `Durability::Buffered` arm in `flush_batch()` which calls
`ack_tx.send(true)` immediately, without waiting for an fsync.

### I-3 No acked-but-lost record (at-least-once delivery to sink)

Every record that was acked at the `Sync` or `Batched` level must eventually
reach the configured sink (or be dead-lettered on a permanent sink error —
which is itself persisted). Recovery seals and replays unconfirmed segments;
the drain retries on transient errors; the confirmed sidecar prevents double
drain.

**Code anchor**: `replay_unconfirmed()` + `check_confirmed()` in `wab/mod.rs`;
`drain/mod.rs` retry loop; `drain/confirmed.rs`.

### I-4 No torn record treated as valid

A record whose payload bytes were only partially written to disk must never be
returned to the drain as a valid record. Partial writes trigger segment
poisoning (`WabSegment.poisoned`); crash recovery truncates at the last
validated CRC boundary.

**Code anchor**: `write_record()` short-writev detection in `segment.rs`;
`recover_segment()` CRC-per-record loop in `recovery.rs`.

### I-5 FIFO ordering within a shard

Records pushed to the same shard by the same producer must appear in the WAB
in the order they were submitted (and therefore in the order they reach the
sink). The partitioned queue, worker batching buffers, and flusher channel are
all FIFO structures; the sharding design preserves this property.

**Code anchor**: `QueueSender::push_timeout()` routes by `shard_id % partitions`;
`Worker::run()` maintains per-shard `buffers[shard]` in push order.

### I-6 No double-drain of a segment

Once a segment is confirmed (`.confirmed` sidecar written with a valid CRC), it
must not be replayed to the sink again even if the daemon restarts. The sidecar
has its own CRC to detect corruption; a corrupt sidecar causes both the segment
and sidecar to be quarantined rather than blindly re-replayed.

**Code anchor**: `check_confirmed()` in `recovery.rs`; `parse_confirmed()` in
`format.rs`.

### I-7 Flusher panic does not permanently lose the shard

A panicking WAB flusher is respawned up to `MAX_FLUSHER_RESPAWNS` (10) times
with linear back-off. Records buffered in the bounded channel survive a flusher
panic because they remain in the channel until the respawned flusher drains
them. Only after 10 consecutive panics does the shard go permanently offline.

**Code anchor**: `run_with_panic_supervision()` in `wab/mod.rs`.

### I-8 Queue saturation surfaces as Nack, not silent drop

When the per-partition work queue is full, the connection handler receives a
timeout from `push_timeout()` and sends `Nack(InternalError)` to the producer.
Records are never silently dropped; the producer always knows.

**Code anchor**: `QUEUE_PUSH_TIMEOUT` and `push_timeout()` in
`socket/connection.rs` / `queue.rs`.

### I-9 Dead-letter full blocks drain, not crashes

When the dead-letter directory would exceed `dead_letter_max_bytes`, the drain
enters `BlockedDeadLetterFull` and waits for headroom rather than panicking or
silently dropping records. The blocked segment is not confirmed until headroom
is available.

**Code anchor**: `DrainState::BlockedDeadLetterFull` in `drain/mod.rs`;
`DeadLetterWriter::would_exceed_cap()`.

### I-10 Crash during recovery does not corrupt surviving records

If the daemon crashes while `recover_segment()` is mid-execution (e.g., between
`file.set_len()` and `fs::rename()`), the original `.wab` file may be left in a
partially re-processed state. The next recovery pass must handle this case
without losing records that had been validated before the interrupted truncation.

**Code anchor**: `recover_segment()` in `recovery.rs` — all intermediate states
the function can leave on disk.

---

## 2. Fault Catalog

### 2.1 Disk Faults

| # | Fault | Injection Point in Code | Invariant | Severity |
|---|-------|------------------------|-----------|----------|
| D-1 | `ENOSPC` mid-record write (writev returns short) | `WabSegment::write_record()` → `file.write_vectored()` returns `Ok(n < total)` | I-4: partial write poisons segment; I-1: acked records not in this segment are safe; Nack must fire | Critical |
| D-2 | `ENOSPC` during segment creation (header write fails) | `WabSegment::create()` → `file.write_all(&header)` | I-8: producer must Nack; server must not panic | Critical |
| D-3 | `fdatasync` returns `EIO` ("fsyncgate") | `platform_fsync()` → `file.sync_data()` returns `Err` | I-1: fsync failure → ack must be `false` (Nack propagated); `wab_fsync_failures` must increment | Critical |
| D-4 | `fdatasync` hangs for > 30 s (slow disk) | `platform_fsync()` blocks indefinitely | I-8: ACK_TIMEOUT (30 s) fires; producer gets Nack; shard is not permanently offline | High |
| D-5 | Torn write: only N of M bytes reach stable storage after power loss | Simulated by writing a valid header + partial record bytes then closing the fd without fsync | I-3: recovery must truncate at last valid record boundary; I-4: torn record not returned to drain | Critical |
| D-6 | Read corruption on replay: single-bit flip in a sealed segment | Flip a byte in the payload region of a `.wab.sealed` file before recovery | I-4: CRC mismatch detected; partial records recovered up to the flip; no valid-looking corrupt data returned to sink | Critical |
| D-7 | Read corruption in segment header (magic or version field) | Overwrite `WEIR` magic or `FORMAT_VERSION` byte | Recovery quarantines rather than sealing; `recovery_segments_quarantined` increments | High |
| D-8 | `ENOSPC` during segment seal (sentinel + footer write) | `WabSegment::seal()` → `write_all(&build_sentinel())` fails | Seal fails; file left as `.wab` (unsealed); next recovery pass handles it | High |
| D-9 | `ENOSPC` during `fs::rename()` at seal time | `WabSegment::seal()` → `std::fs::rename()` fails | Footer is written; rename fails; `.wab` still on disk (not `.wab.sealed`); recovery seals it | Medium |
| D-10 | WAB directory becomes read-only after startup | `shard_dir_path()` write fails on next segment creation | New segments cannot be created; Nack fired; server does not crash | High |
| D-11 | Dead-letter write fails (ENOSPC on DL dir) | `DeadLetterWriter::write_records()` → `WabSegment::create()` fails | Error logged; batch count not incremented; segment confirm still proceeds (data accepted as lost) | Medium |
| D-12 | `.confirmed` write fails (e.g., disk full) | `confirmed::write_confirmed_file()` → `std::fs::write()` fails | Segment will be replayed on next restart (at-least-once); double-drain is safe if sink is idempotent | Medium |

### 2.2 Crash Points

| # | Crash Point | Crash Injection | Invariant | Severity |
|---|-------------|-----------------|-----------|----------|
| C-1 | Kill mid-batch (after some records written, before fsync) | SIGKILL after N `write_record` calls with `need_fsync = true`, before `fsync_observed()` | I-1: acked Sync records (if any were acked before kill) must be recoverable; un-acked records need not survive | Critical |
| C-2 | Kill mid-fsync (fsync in progress) | SIGKILL inside `platform_fsync()` | I-1: crash before fsync completes — no ack was sent, so records may be lost; I-4: partial flush must not produce valid-looking torn records on read-back | Critical |
| C-3 | Kill during seal — after footer write, before rename | SIGKILL between `file.sync_all()` and `fs::rename()` in `WabSegment::seal()` | Segment remains as `.wab` with valid footer; recovery sees a `.wab` that looks like an active segment; must re-seal cleanly | High |
| C-4 | Kill during seal — after sentinel, before footer | SIGKILL between `write_all(&build_sentinel())` and `write_all(&footer)` in `seal()` | Recovery reads partial sentinel (0x00000000 len field), stops there, truncates, writes fresh footer | High |
| C-5 | Kill during segment rotation — after old segment sealed, before new segment created | SIGKILL in `flush_batch()` after `drain_tx.send(sealed)`, before next `write_record()` call | Sealed segment queued to drain; recovery should find it via `replay_unconfirmed` | High |
| C-6 | Kill during recovery itself — between `file.set_len()` and `fs::rename()` | SIGKILL inside `recover_segment()` | `.wab` file is truncated but not renamed; next recovery sees a shorter `.wab`; must recover without losing valid records from before the truncation | High |
| C-7 | Kill during recovery header validation | SIGKILL inside `recover_segment()` before the CRC-replay loop starts | File untouched; next recovery re-attempts from scratch | Low |
| C-8 | Kill during drain processing — after some records committed, before `.confirmed` write | SIGKILL inside `process_segment()` between `commit_batch()` and `confirm_and_delete()` | At-least-once: segment replayed on restart; sink must handle duplicate delivery | Critical |
| C-9 | Kill after `.confirmed` written, before `remove_file()` | SIGKILL in `confirm_and_delete()` between the two calls | Orphan `.wab.sealed` on disk with valid `.confirmed` sidecar; recovery skips replay; operator can clean up | Medium |
| C-10 | Kill during dead-letter write | SIGKILL inside `DeadLetterWriter::write_records()` | Partial `dl_NNNN.wab` on disk; next startup scans dead-letter dir; partial file should not be confused with a valid dead-letter segment | Medium |
| C-11 | Daemon killed while `BlockedDeadLetterFull` | SIGKILL while drain is in `BlockedDeadLetterFull` state | Segment not confirmed; replay on restart; I-3 holds | Medium |
| C-12 | Kill during graceful shutdown under load | SIGKILL after SIGTERM received but before seal_current() completes | Active segment left as `.wab`; recovery seals it; replayed records must include everything acked | High |

### 2.3 Flusher Panic Scenarios

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| P-1 | Single transient panic on write_record | panic!() inside `write_record()` body | I-7: supervisor catches panic; channel records survive; shard restarts; `wab_flusher_panics` += 1 | High |
| P-2 | N transient panics (N < MAX_FLUSHER_RESPAWNS) then clean | N consecutive panics, then normal return | I-7: shard recovers; no records lost from channel; metric == N | High |
| P-3 | Permanent panic (every attempt panics) | Always panic | I-7: after MAX_FLUSHER_RESPAWNS attempts, shard goes offline; subsequent pushes Nack(InternalError); server continues serving other shards | High |
| P-4 | Panic during seal on shutdown | panic inside `writer.seal_current()` in the flusher's shutdown path | Active segment not sealed; records buffered in the channel may be lost (no ack was sent for them yet — this is correct) | Medium |
| P-5 | Worker thread panics (not flusher) | panic in `Worker::run()` | Worker is not supervised by `run_with_panic_supervision`; thread exits; subsequent queue sends to that partition Nack; investigate whether supervision should extend here | High (gap) |

### 2.4 Queue Saturation and Backpressure

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| Q-1 | Single partition at capacity | Fill partition to `QUEUE_CAPACITY / partitions`; new push arrives | I-8: `push_timeout()` waits up to `QUEUE_PUSH_TIMEOUT` (5 s); if still full, Nack(InternalError) | High |
| Q-2 | All partitions saturated simultaneously | Multiple producers flooding all shards | I-8: all pushes timeout; server stays alive; no deadlock with flusher threads | High |
| Q-3 | Queue saturated while flusher is hung (e.g., slow fsync) | Flusher blocked on `fsync_observed()` + producers flooding queue | I-8: `ACK_TIMEOUT` fires for in-flight acks; `QUEUE_PUSH_TIMEOUT` fires for new pushes; no cascading deadlock | Critical |
| Q-4 | Shard channel full (flusher → WAB bounded channel) | WAB flusher's per-shard bounded channel `crossbeam_channel::bounded(batch_size * 4)` full | Worker `send().ok()` discards silently — this is a **correctness gap** (ack already sent?) | Critical (gap — see §5) |

### 2.5 Sink Faults

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| S-1 | Transient sink error storm (N retries, then success) | `MockSink::commit()` returns `Err(Transient)` for N calls | I-3: segment stays on disk during retries; confirmed after success; `retried` metric increments | High |
| S-2 | Transient storm exhausts `max_retries` | Transient for `max_retries + 1` calls | Drain advances to next segment; failed segment stays on disk (not confirmed); log error | High |
| S-3 | Permanent error → dead-letter | `commit()` returns `Err(Permanent)` | I-3: records dead-lettered; segment confirmed; `dead_lettered` metric | High |
| S-4 | Permanent error + dead-letter full | `Err(Permanent)` when DL dir already at cap | I-9: `BlockedDeadLetterFull` state; drain waits; no confirm until headroom; `dead_letter_full` metric | High |
| S-5 | Dead-letter headroom restored while blocked | External deletion of DL files while drain is blocked | I-9: drain unblocks within `dead_letter_check_interval`; segment retried from start | Medium |
| S-6 | Partial dead-letter (sink returns `CommitResult` with mixed committed + dead_lettered) | `Ok(CommitResult { committed: [...], dead_lettered: [...] })` | I-3: committed records not double-dead-lettered; DL records written; segment confirmed | Medium |
| S-7 | Sink hangs indefinitely | `commit()` never returns | Drain thread blocks; new segments accumulate in drain channel (unbounded by default); operators should detect via `weir_sink_health` going stale | High (gap — no timeout on sink::commit) |
| S-8 | Sink `Retry-After` hint (extended backoff) | `SinkError::retry_after()` returns `Some(Duration)` | Drain sleeps exactly the hinted duration (capped at 5 min); subsequent retry uses doubled default if hint absent | Medium |
| S-9 | Sink health fluctuates (Healthy → Degraded → Down → Healthy) | `Sink::health()` rotates through states | `weir_sink_health` gauge tracks state; no correctness impact — health is advisory | Low |
| S-10 | Duplicate delivery on replay (at-least-once on crash mid-drain) | Crash at C-8; restart; same segment replayed | At-least-once contract; sink must be idempotent for records it already committed | Critical (contract) |

### 2.6 Clock / Timer Edge Cases

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| T-1 | `batch_deadline` expires with empty buffer | Flusher timer fires on idle shard | No-op (`batches.is_empty()` guard); no write or fsync; CPU not busy-looped | Low |
| T-2 | EWMA update with zero-latency sample | `fsync_observed()` measures zero elapsed time (test clock returns identical Instants) | `ewma_update_us(cur, 0)` shrinks window but does not underflow; clamp to `COALESCE_MIN_US` protects | Medium |
| T-3 | EWMA saturating-multiply overflow (`u64::MAX / 2` input) | `ewma_update_us(u64::MAX / 2, sample)` | `saturating_mul(3)` clamps to `u64::MAX`; no panic; coalesce hint stays bounded | Low |
| T-4 | `ACK_TIMEOUT` fires before flusher completes | Connection handler's `ack_timeout` expires while fsync is in progress | Producer gets `Nack(InternalError)`; record may still be written durably by the eventual fsync; at-least-once applies | High |
| T-5 | `QUEUE_PUSH_TIMEOUT` race with worker drain | Queue full exactly at the instant the worker drains it | Either the push succeeds (if worker drains first) or Nack fires (if timeout first); never deadlock | Medium |
| T-6 | Batch deadline timer starved by high-CPU producer flood | Flusher's `recv_timeout(batch_deadline)` starved; deadline fires late | Latency increases but no invariant is violated; `batch_deadline_timer_keeps_latency_bounded` documents acceptable bounds | Low |

### 2.7 Concurrent Producer Scenarios

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| R-1 | N producers racing on the same shard simultaneously | N threads pushing to shard_id=0 simultaneously | I-5: each producer's own records appear in submission order; cross-producer interleaving allowed | Critical |
| R-2 | Producer disconnects mid-stream (partial frame) | Connection drops between header and payload | Server reads partial payload, gets EOF, closes connection; next connection works normally | High |
| R-3 | Producer reconnects rapidly (100 rounds) | Same connection/push/disconnect cycle | Semaphore permit returned on each drop; no permit leak over 100 cycles | Medium |
| R-4 | Stalled producer holds semaphore permit | Client sends frame but never reads Ack | Server suspends that connection task; other connections proceed normally; `read_timeout` eventually closes the stalled connection | High |
| R-5 | FD limit exhausted (RLIMIT_NOFILE) | `RLIMIT_NOFILE` set to a low value; 200+ connections attempted | Server accepts what it can; new connections refused by kernel; existing connections work; no crash | Medium |
| R-6 | Connection at max_connections cap | All `max_connections` semaphore permits acquired | New connect requests queue in kernel backlog; server does not reject existing connections; no crash | Medium |

### 2.8 Graceful Shutdown Under Load Races

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| G-1 | SIGTERM during active Sync push | SIGTERM while flusher is mid-fsync | Acks for in-flight Sync records fire after fsync completes; connection sees Ok or Io(EOF); never Nack silently | Critical |
| G-2 | SIGTERM while drain is processing a segment | SIGTERM while `process_segment()` is running | Drain completes or is interrupted; unconfirmed segments replayed on next start | High |
| G-3 | SIGTERM while drain is `BlockedDeadLetterFull` | SIGTERM in `BlockedDeadLetterFull` state | Segment not confirmed; replayed on restart (I-3 holds via recovery) | Medium |
| G-4 | SIGTERM while flusher is in panic supervisor loop | SIGTERM during backoff sleep in `run_with_panic_supervision` | Respawn loop observes channel disconnect; exits cleanly (or panics capped) | Medium |
| G-5 | Shutdown timeout exceeded (seal_current slow) | `WabHandle::join_handles` not completing within `shutdown_timeout_secs` | Server does a hard exit after the budget; WAB left unsealed; recovery handles it | High |

### 2.9 Malformed Wire Frames

> Already covered by `cargo-fuzz` (`envelope_parse`, `wab_confirmed`) for
> parser panic safety. Extensions for the DST catalog focus on state-machine
> correctness beyond no-panic.

| # | Fault | Injection Point | Invariant | Severity |
|---|-------|-----------------|-----------|----------|
| M-1 | Bad header magic | 16-byte header with wrong magic bytes | `Header::decode()` → `DecodeError`; Nack(ProtocolError) sent; connection closed; next connection works | Medium |
| M-2 | Header CRC mismatch | Flip a bit in header CRC field | Nack(ProtocolError); connection closed | Medium |
| M-3 | Payload length claims over `MAX_PAYLOAD_HARD_CAP` | Header with `payload_len > 16 MiB` | Nack(PayloadTooLarge) before allocation; no OOM | High |
| M-4 | Payload CRC mismatch | Valid header + payload + wrong CRC trailer | Nack(CorruptPayload); connection closed | Medium |
| M-5 | Zero-length payload | `payload_len = 0` | Empty payload is accepted (smoke test passes); verify WAB record written correctly | Low |
| M-6 | Unknown `MessageType` byte | Header `message_type` byte not Push or HealthCheck | Nack or connection close; no panic | Medium |
| M-7 | Partial frame — exactly header bytes, no payload | TCP EOF after 16 bytes | Connection closes cleanly; next connection works | Medium |
| M-8 | Very fast frame flood without reading acks | Producer writes 1000 frames without reading responses | Read buffers fill; connection stalls; other connections unaffected; eventually read_timeout fires | Medium |

---

## 3. Existing Test Coverage vs Gaps

### 3.1 Already Covered

| Scenario(s) | Where covered |
|-------------|---------------|
| D-1 (EFBIG as proxy for ENOSPC) | `system::efbig_returns_nack_not_crash` |
| D-1 (ENOSPC real; opt-in) | `system::enospc_returns_nack_not_crash` (ignored, requires tmpfs) |
| D-6 (single-bit flip) | `wab/mod.rs::segment_reader_detects_crc_mismatch` |
| D-7 (bad magic) | `wab/recovery.rs::recovery_quarantines_bad_magic` |
| C-1 (kill + restart) | `system::wab_data_preserved_across_crash_restart` |
| C-1 byte-level | `system::wab_data_integrity_after_crash` |
| C-5/C-6 (recovery after truncation) | `wab/recovery.rs::recovery_crash_simulation_truncate_mid_record` |
| C-8 (double-drain guard) | `drain/mod.rs::confirmed_file_only_appears_after_successful_commit_not_during_retries` |
| C-9 (orphan .confirmed) | `drain/mod.rs::successful_drain_writes_confirmed_and_deletes_segment` + `confirmed_not_replayed_on_restart` |
| P-1 (single flusher panic) | `wab/mod.rs::supervisor_catches_str_panic_and_increments_metric` |
| P-2 (N transient panics) | `wab/mod.rs::flusher_panic_respawn_recovers_within_cap` |
| P-3 (cap-out) | `wab/mod.rs::flusher_panic_loop_caps_out_after_max_respawns` |
| P-4 (poisoned segment) | `wab/segment.rs::poisoned_segment_refuses_subsequent_writes`, `shardwriter_drops_segment_after_write_error` |
| Q-1/Q-8 (queue timeout / Nack) | `queue.rs::push_timeout_returns_unit_when_full_and_timeout_expires` |
| S-1/S-2 (transient retry) | `drain::transient_success_on_retry_writes_confirmed`, `transient_max_retries_exhausted_leaves_segment_on_disk` |
| S-3 (permanent → DL) | `drain::permanent_error_dead_letters_records_and_confirms_segment` |
| S-4/S-5 (DL full / unblock) | `drain::blocked_when_permanent_error_and_dead_letter_cap_exceeded`, `blocked_unblocks_and_retries_same_segment` |
| S-6 (partial DL from CommitResult) | `drain::commit_result_dead_lettered_records_written_to_dead_letter_dir` |
| S-8 (Retry-After) | `drain::drain_waits_retry_after_hint_before_retrying` |
| T-2/T-3/T-6 (EWMA edge cases) | `wab/mod.rs::ewma_*` unit tests |
| R-1 (concurrent producers FIFO) | `system::concurrent_producers_to_same_shard_preserve_per_producer_order` |
| R-2 (partial frame) | `system::partial_frame_does_not_corrupt_next_connection` |
| R-3 (reconnect semaphore) | `system::new_connection_accepted_after_previous_client_drops` |
| R-4 (stalled client isolation) | `system::stalled_client_does_not_block_other_connections` |
| R-5 (FD limit) | `system::fd_limit_exhaustion_does_not_crash_server` |
| G-1 (shutdown under load) | `system::graceful_shutdown_under_load` |
| M-1..M-5 (malformed frames) | `fuzz/envelope_parse`, `system::payload_size_boundary_enforced` |
| I-6 (.confirmed CRC quarantine) | `wab/recovery.rs::check_confirmed_bad_crc_quarantines` |
| Fuzz: envelope parser | `fuzz/envelope_parse.rs` |
| Fuzz: confirmed parser | `fuzz/wab_confirmed.rs` |

### 3.2 Gaps (Not Covered by Any Existing Test)

These are the high-value scenarios for the DST harness to address:

| Gap ID | Scenario | Why it is a gap |
|--------|----------|-----------------|
| **G-WAB-1** | D-3: `fdatasync` returns `EIO` at runtime | `efbig_returns_nack_not_crash` uses `RLIMIT_FSIZE=0` which returns `EFBIG` on `write`, not `EIO` on `fsync`. The fsync failure path (`fsync_observed()` returning `false` → ack `false`) has no integration test. |
| **G-WAB-2** | D-4: slow/hanging `fdatasync` (> ACK_TIMEOUT) | No test verifies that `ACK_TIMEOUT` (30 s) fires and the producer receives Nack(InternalError) while the flusher eventually completes the fsync. The interaction between `ack_timeout` and a slow fsync is untested. |
| **G-WAB-3** | C-3/C-4: crash during seal (footer partially written) | `wab_data_preserved_across_crash_restart` uses SIGKILL mid-push, not mid-seal. The specific case where `sync_all()` completed but `rename()` did not has no targeted test. |
| **G-WAB-4** | C-6: crash during recovery itself | No test validates that a crash during `recover_segment()` (mid-truncate or mid-rename) leaves the system in a state that the *next* recovery pass handles correctly. |
| **G-WAB-5** | D-8/D-9: ENOSPC during seal sentinel or rename | Seal-time write failures are untested. The `poisoned` flag only covers `write_record()`; `seal()` does not set `poisoned`. |
| **G-DRAIN-1** | S-7: sink hangs indefinitely (no timeout on `commit()`) | There is no `commit()` deadline. A hung sink blocks the drain thread indefinitely. No test captures this; no timeout is implemented. |
| **G-DRAIN-2** | C-8 verified at record-level granularity | The existing test verifies at segment granularity. There is no test that verifies at-least-once when the crash happens between two `commit_batch()` calls within the same segment (partially drained segment). |
| **G-FLUSHER-1** | P-5: worker thread panic (not flusher) | `Worker::run()` is not wrapped in `run_with_panic_supervision`. A worker panic kills that thread; the queue partition is drained but new pushes eventually fail. No supervision, no metric, no test. |
| **G-QUEUE-1** | Q-4: WAB flusher channel full (worker silent discard) | In `Worker::flush_shard()`, `shard_txs[shard].send(Batch {...}).ok()` silently discards if the bounded flusher channel is full. Records in the discarded Batch already had their ack senders dropped. This is a **potential correctness hole** that needs analysis: do the acks fire false before or after this point? |
| **G-RECOVERY-1** | I-3 + I-6: corrupt `.confirmed` with a valid CRC32 (adversarial) | The quarantine path for bad `.confirmed` relies on CRC32. A `.confirmed` file forged by an attacker who can write to the WAB directory (e.g., post-compromise) would bypass quarantine. The security model documents this limitation (`format.rs` docstring) but no test exercises the threat model boundary. |
| **G-MULTI-1** | Multi-shard crash + partial recovery (only some shards replay) | Crash during startup with one shard's recovery succeeding and another failing (e.g., one shard dir deleted). Recovery of the remaining shards must not be interrupted. |
| **G-SHUTDOWN-1** | G-5: shutdown timeout exceeded; daemon does hard exit with unsealed segment | `shutdown_timeout_secs` guards this but no test injects a slow seal to verify the timeout fires and the resulting `.wab` is recoverable on restart. |

---

## 4. Prioritized First Set of Scenarios to Build

The following 10 scenarios represent the highest-value additions from the gap
list above. They are ordered by: severity × likelihood × hardness-to-test-without-DST.

### Batch A — Critical correctness (build first)

**A-1: G-WAB-1 — EIO on fdatasync**

Inject `EIO` from `platform_fsync()` via a mock or interposable trait.
Assert: `fsync_observed()` returns `false`; ack_tx receives `false`; producer
observes Nack; `wab_fsync_failures` metric increments; no data corruption in the
WAB file for records written before the EIO.

Seed design: `seed = fault_at_fsync(shard=0, call_count=N)` → deterministically
injects `EIO` on the Nth call to `platform_fsync`.

---

**A-2: G-WAB-3 — Crash between seal fsync and rename**

Build a `WabSegment::seal_point` injection that panics (or returns error) between
`file.sync_all()` and `fs::rename()`. Assert: next recovery seals the file
(finds a `.wab` with valid footer bytes) and the records are replayed correctly.

Seed design: `seed = crash_at_seal_point(shard=0, segment=1)`.

---

**A-3: G-FLUSHER-1 — Worker thread panic**

Inject a panic into `Worker::run()` using a test-only hook. Assert: the
worker thread exits; the queue partition is orphaned; future pushes to that
partition eventually Nack; server stays alive and other shards work. Determine
whether worker supervision should be added (open question Q-1 below).

Seed design: `seed = worker_panic(partition=0, after_records=N)`.

---

**A-4: G-QUEUE-1 — WAB flusher channel full / silent discard**

Saturate the bounded `crossbeam_channel::bounded(batch_size * 4)` shard channel
while `flush_shard()` is running. Verify whether acks have already been sent
(they have not — acks fire after `fsync_observed()`, which runs in the flusher
thread after receiving the Batch). Confirm the silent `.ok()` discard is safe or
document the actual invariant violation and file a correctness issue.

Seed design: slow the flusher with a delay injection; fill worker buffer to
force a `flush_shard()` call; assert behavior.

---

**A-5: G-WAB-2 — ACK_TIMEOUT on slow fdatasync**

Inject a 40-second artificial delay inside `platform_fsync()`. Assert:
`ACK_TIMEOUT` fires (30 s); producer receives Nack(InternalError); the
flusher eventually completes; the segment is not corrupted; no panic occurs.

Seed design: `seed = slow_fsync(delay_ms=40_000, shard=0)`.

---

### Batch B — Drain and sink correctness (build second)

**B-1: G-DRAIN-1 — Hung sink**

Create a `HungSink` that blocks `commit()` forever. Assert: the drain thread
is blocked; new sealed segments accumulate in the drain channel; `weir_sink_health`
eventually shows stale; the server does not crash; after "unblocking" the sink
(by sending on a oneshot), drain catches up and confirms all segments.

Seed design: `seed = sink_hangs(unblock_after_segments=N)`.

---

**B-2: G-DRAIN-2 — At-least-once mid-segment crash**

Inject a crash (panic or tokio runtime abort) in `process_segment()` after the
first `commit_batch()` succeeds but before the second. Assert: on restart, the
segment is replayed from the beginning; the sink receives duplicate records for
the first batch; it handles them correctly (idempotent sink required).

Seed design: `seed = crash_after_batch(segment_path=X, batch_number=1)`.

---

**B-3: C-6 — Crash during recovery (recovery-of-recovery)**

Truncate a `.wab` file mid-way; start recovery; inject a crash between
`file.set_len()` and `fs::rename()`. Assert: the second recovery pass handles
the partially-re-processed `.wab` without losing the valid records before the
truncation point.

Seed design: two-phase test with two successive `recover_open_segments()` calls
with a crash injected between them.

---

**B-4: S-10 — At-least-once duplicate delivery contract**

Document and test the dedup contract with a `DeduplicatingSink` that tracks
committed payloads. Push N records; crash at C-8; restart; replay. Assert:
sink receives each record at least once; the `DeduplicatingSink` returns the
duplicate set for inspection; no record silently dropped.

Seed design: `seed = {records: [payload_0..N], crash_after_commit: K}`.

---

**B-5: G-SHUTDOWN-1 — Shutdown timeout with unsealed segment**

Configure `shutdown_timeout_secs = 1`; inject a 5-second delay in
`WabSegment::seal()`; send SIGTERM. Assert: shutdown completes within 2 s
(the extra second accounts for OS overhead); the active segment is left as
`.wab` (not sealed); recovery on restart seals and replays it.

Seed design: `seed = slow_seal(delay_ms=5_000)` + `shutdown_timeout_secs=1`.

---

## 5. Open Questions

**Q-1: Should worker threads be supervised like flusher threads?**

`Worker::run()` currently runs unsupervised. A panic kills the thread; all
records buffered in that thread's per-shard `buffers[shard]` are lost (their
ack channels are dropped without sending). Since the ack has not yet been sent
to the producer when the worker panics, this is not an I-1 violation — but it
is silent. Worker supervision would require storing the pending acks across
unwind boundaries which may require `AssertUnwindSafe` reasoning similar to the
flusher's. Recommendation: add supervision and increment a
`weir_worker_panics` metric; open a separate issue.

**Q-2: Does `flush_shard().ok()` create a window for data loss when the flusher channel is full?**

In `Worker::flush_shard()`, `shard_txs[shard].send(Batch {...}).ok()` silently
discards the Batch if the flusher channel is full or disconnected. The `ack_tx`
senders in each `WorkUnit` in that Batch are also dropped, causing the
connection handler's `ack_rx.await` to return `Err(RecvError)` which is
currently treated as... (needs code path investigation in `connection.rs`). If
it resolves to Nack(InternalError), data is safe at the cost of a false Nack.
If it hangs or panics, there is a bug. **Action**: trace the `ack_rx` drop
path through `handle_connection` and add a test.

**Q-3: Is there a timeout on `Sink::commit()`?**

The `commit()` call in `process_segment()` has no deadline. A sink that hangs
indefinitely blocks the drain thread (and by extension, all new segments queue
up in the drain channel unboundedly — the channel is `crossbeam_channel::Sender`
with no bound visible in the spawn call). Consider wrapping `commit()` in a
`tokio::time::timeout` with an operator-configurable deadline and transitioning
to `RetryingTransient` on timeout.

**Q-4: What is the exact seed format for the DST harness?**

This document proposes logical seeds (e.g., `fault_at_fsync(shard=0,
call_count=N)`) but the harness sibling agent defines the concrete
serialization. The two agents must agree on a seed schema before any scenario
can be made reproducible. Recommendation: use a Rust enum with `#[derive(Serialize, Deserialize)]` so seeds are both human-readable and binary-stable.

**Q-5: Should the dead-letter directory have a bounded channel as well?**

The drain channel from WAB flushers to the drain thread is the
`crossbeam_channel::Sender<PathBuf>` whose bound is not shown in `wab/mod.rs`'s
`spawn()` signature — it is created outside and passed in (likely unbounded
from `lib.rs`). Under a hung sink, the drain thread cannot consume from this
channel and segments pile up forever. Consider bounding the channel and
surfacing backpressure to producers.

**Q-6: How should the DST harness inject syscall failures portably?**

Options considered:
- `LD_PRELOAD` shim (Linux only, not macOS)
- `cfg(test)` trait seam replacing `platform_fsync` with a mock
- OS-level filesystem mounts (tmpfs for ENOSPC; `dm-flakey` for EIO)
- `libfiu` / `fail-rs` crate for named failure points

Recommendation: use a `cfg(test)` trait seam for unit-level scenarios
(fastest, portable) and OS-level mounts for system-level scenarios
(highest fidelity, CI-environment-gated with `#[ignore]` flags matching
the existing `enospc_returns_nack_not_crash` pattern).

---

*End of exploration — paired with the DST harness design agent.*
