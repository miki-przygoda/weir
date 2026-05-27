# System Test Suite Audit

File audited: `crates/weir-server/tests/system.rs` (1711 lines, 41 `#[test]` functions).

> **Status:** the DELETE / RENAME / REWRITE verdicts have been actioned
> (see commits `ba69803`, `968afd0`, `8e94cf2`). The STRENGTHEN verdicts
> are actioned in the follow-up commit on this branch — 9 of 10 land
> directly; the 10th (`concurrent_producers_all_acked_with_multiple_shards`,
> "assert work landed in ≥2 shard directories") is blocked on a separate
> fix to `src/socket/connection.rs` where `shard_id` is currently
> hardcoded to 0, contradicting the architecture doc. That fix and the
> reinstated assertion ship as their own feature commit.

## Executive summary

- **Total tests:** 41
- **Verdicts:** KEEP 25 / STRENGTHEN 10 / DELETE 3 / RENAME 2 / REWRITE 1
- **Top findings:**
  1. The suite over-tests "happy path acks succeed" (≈10 tests assert nothing more than `push().unwrap()`) and under-tests recovery: `wab_data_preserved_across_crash_restart` only checks that *bytes exist* post-restart, never that recovery actually replayed/sealed them — the `weir_recovery_records_replayed` metric is registered but never asserted by any test in this file.
  2. The four flagged tests are weaker than their names imply. `disk_full_returns_nack_not_crash` tests EFBIG, not ENOSPC; `metrics_consistent_across_crash_restart` does not assert any cross-restart invariant; `server_restarts_after_sigkill` and `wab_data_integrity_after_crash` overlap heavily (the former is a strict subset of the latter once you discount the trivial post-restart push). `stalled_client_does_not_block_other_connections` is the strongest test in the suite — confirmed.
  3. Several tests pass even if the feature they "test" is broken: `wab_segment_rotation_creates_multiple_segments` never observes rotation (writes 40 KiB into a 256 MiB segment); `all_durability_tiers_acked` cannot distinguish Sync from Buffered since it never inspects timing or fsync; `health_check_on_separate_connection_from_push` is indistinguishable from two unrelated health checks.

---

## Basic push / ack

### `smoke_single_push_ack` — system.rs:524
- **Asserts:** One Sync push round-trips.
- **Regression prevented:** Catches a totally broken binary (won't start, won't accept, won't ack at all). A canary; every other test would also fail if this did.
- **Verdict:** KEEP
- **Notes:** Cheapest possible smoke; useful as the first signal.

### `all_durability_tiers_acked` — system.rs:531
- **Asserts:** One push per tier (Sync/Batched/Buffered) returns Ok.
- **Regression prevented:** Catches a tier dispatch table that drops or panics for one variant.
- **Verdict:** STRENGTHEN
- **Notes:** The name promises that the tiers *behave* like their contract; the test only checks they don't error. A Buffered tier that secretly fsyncs, or a Sync tier that secretly skips fsync, would pass. Either rename to `all_durability_tiers_return_ok` or add a latency/wab-bytes differential check.

### `multiple_sequential_pushes_same_connection` — system.rs:546
- **Asserts:** 50 sequential Batched pushes on one connection all ack.
- **Regression prevented:** Catches a per-connection state machine that breaks after the first frame (e.g. forgets to advance read position).
- **Verdict:** KEEP

## Health check

### `health_check_returns_ok` — system.rs:559
- **Asserts:** HealthCheck request returns Ok.
- **Regression prevented:** Catches removal/break of the HealthCheck message type.
- **Verdict:** KEEP

### `health_check_on_separate_connection_from_push` — system.rs:566
- **Asserts:** A push on connection A and a health check on connection B both succeed.
- **Regression prevented:** None specific — neither connection observes the other; the test is equivalent to running two unrelated single-frame tests.
- **Verdict:** DELETE
- **Notes:** If you want to test that an in-flight push doesn't block health, you'd need to stall the push (use the stall trick from `stalled_client_does_not_block_other_connections`) and time the health check.

## Concurrent producers

### `concurrent_producers_all_acked` — system.rs:577
- **Asserts:** 8 threads × 100 Batched pushes all return Ok.
- **Regression prevented:** A race in the queue or batcher that drops/panics on contention; ack-channel mis-routing across workers.
- **Verdict:** KEEP

### `many_connections_open_simultaneously` — system.rs:610
- **Asserts:** 20 connections can be opened first, then each does one Buffered push.
- **Regression prevented:** A semaphore leak or accept-loop bug that fails when many idle connections coexist.
- **Verdict:** STRENGTHEN
- **Notes:** 20 is well below any sensible `max_connections` default. The name implies stress-testing the connection cap; either bump the count high enough to actually approach the cap, or rename to `twenty_idle_connections_each_accept_one_push`.

## WAB on-disk verification

### `records_written_to_wab_on_disk` — system.rs:633
- **Asserts:** After 20 Sync pushes, at least one `.wab` or `.wab.sealed` file exists.
- **Regression prevented:** A bug where the WAB writer silently no-ops and acks anyway (e.g. mocked out, wrong path, permissions).
- **Verdict:** KEEP

### `wab_segment_rotation_creates_multiple_segments` — system.rs:656
- **Asserts:** After 10 × 4 KiB pushes, at least one WAB file exists.
- **Regression prevented:** None — the assertion is identical to the previous test. 40 KiB cannot trigger rotation in a 256 MiB segment, and the test author admits this in the comment.
- **Verdict:** DELETE
- **Notes:** The name actively lies. Either implement rotation testing (override `SEGMENT_MAX_BYTES` via config, then assert ≥2 sealed segments) or remove it; in its current form it adds noise and false confidence.

### `wab_writes_nonzero_bytes_to_disk_after_sync_pushes` — system.rs:685
- **Asserts:** After 20 Sync pushes, total bytes under `wab_dir` > 0.
- **Regression prevented:** A WAB writer that creates empty files (slightly more than the previous test, which only counts files).
- **Verdict:** KEEP
- **Notes:** Subsumed if `records_written_to_wab_on_disk` is strengthened to assert `> 0` bytes; consider merging.

## Metrics accuracy (registration)

### `metrics_endpoint_responds_with_openmetrics_content` — system.rs:708
- **Asserts:** `/metrics` returns non-empty body containing `weir_` or `# EOF`.
- **Regression prevented:** Catches the metrics HTTP server failing to bind or hand back garbage.
- **Verdict:** KEEP

### `metrics_all_19_families_registered` — system.rs:720
- **Asserts:** 19 named metric families appear as `# HELP` lines.
- **Regression prevented:** A registry refactor that silently drops a family. The most valuable metrics test in the file — these names are public API.
- **Verdict:** KEEP

### `drain_state_shows_draining_and_not_blocked` — system.rs:756
- **Asserts:** On startup, `weir_drain_state{state="draining"}=1` and the other two states are 0.
- **Regression prevented:** A bug where the drain state gauge is initialised wrong, or the labels are renamed.
- **Verdict:** KEEP

### `sink_health_shows_healthy_via_noop_sink` — system.rs:777
- **Asserts:** With NoopSink, `weir_sink_health{state="healthy"}=1` and the other two are 0.
- **Regression prevented:** Sink health enum-to-label mapping breakage.
- **Verdict:** KEEP

## Graceful shutdown

### `server_shuts_down_cleanly_on_sigterm` — system.rs:799
- **Asserts:** `ServerHandle::shutdown` (SIGTERM + wait) returns without hanging.
- **Regression prevented:** A deadlock in shutdown signal handling.
- **Verdict:** STRENGTHEN
- **Notes:** No timeout is asserted — if shutdown takes 60 s the test still passes (slowly). Add a wall-clock bound (e.g. `< 5s`) like `graceful_shutdown_under_load` does. The "before-shutdown" push is unused.

### `server_exits_and_socket_disappears_after_sigterm` — system.rs:810
- **Asserts:** Socket file exists before shutdown, does not exist after.
- **Regression prevented:** Drop/cleanup logic that leaves stale socket files on graceful exit (would force operators into manual cleanup).
- **Verdict:** KEEP

## Reconnect / restart

### `new_connection_accepted_after_previous_client_drops` — system.rs:827
- **Asserts:** After client A disconnects, client B can connect and push.
- **Regression prevented:** A semaphore-permit leak where dropping a client doesn't release its connection slot.
- **Verdict:** STRENGTHEN
- **Notes:** Only verifies *one* reconnect. A leak that occurs after N drops would not be caught. Loop 100×.

## Payload edge cases

### `empty_payload_is_accepted` — system.rs:843
- **Asserts:** Zero-length payload Sync push acks.
- **Regression prevented:** Off-by-one in length validation that rejects len=0.
- **Verdict:** KEEP

### `binary_payload_round_trips` — system.rs:850
- **Asserts:** A 0..=255 byte payload Sync-pushes successfully.
- **Regression prevented:** Catches a hypothetical text-mode handler stripping NULs/high bytes.
- **Verdict:** RENAME
- **Notes:** "round_trips" implies read-back, which never happens — there's no Pop API. Rename to `arbitrary_binary_payload_accepted`. (If read-back is desired, decode the WAB on disk.)

### `large_payload_accepted` — system.rs:859
- **Asserts:** A 1 MiB Batched push acks.
- **Regression prevented:** Catches a buffer assumption (e.g. read in a single fixed-size chunk) that breaks above some threshold below 1 MiB.
- **Verdict:** STRENGTHEN
- **Notes:** Name implies testing the *limit*. Test at `MAX_PAYLOAD_HARD_CAP` (16 MiB) and at `MAX_PAYLOAD_HARD_CAP + 1` to confirm the boundary is enforced; 1 MiB is mid-range and proves little.

## Stress

### `sustained_load_1000_records_single_client` — system.rs:870
- **Asserts:** 1000 sequential Buffered pushes ack.
- **Regression prevented:** Slow leak (memory, fd, queue slot) that surfaces only after sustained load.
- **Verdict:** KEEP

### `mixed_durability_under_concurrent_load` — system.rs:881
- **Asserts:** 6 threads × 50 pushes (round-robin Sync/Batched/Buffered) all ack.
- **Regression prevented:** A per-tier code path that breaks when interleaved across workers (e.g. a Sync push poisoning a Batched batch).
- **Verdict:** KEEP

## Crash recovery

### `server_restarts_after_sigkill` — system.rs:913 *(flagged)*
- **Asserts:** Pre-crash push acks; socket file survives SIGKILL; restart succeeds; post-restart push acks.
- **Regression prevented:** A regression in restart-path that fails to clean up the stale socket, or a panic during cold start when WAB files exist.
- **Verdict:** STRENGTHEN
- **Notes:** Together with `wab_data_integrity_after_crash` this **is** redundant on the data-survival axis — the integrity test already crashes, restarts implicitly via WAB inspection (well, it inspects WAB *without* restarting actually), and goes further. Keep this one strictly for the "can restart at all" check, but consider folding into `stale_socket_removed_automatically_on_restart` — the two are near-duplicates differing only in whether they push first. My recommendation: keep this, DELETE `stale_socket_removed_automatically_on_restart` (it's a true subset).

### `stale_socket_removed_automatically_on_restart` — system.rs:935
- **Asserts:** After SIGKILL the socket file is still there; after restart, health_check works.
- **Regression prevented:** Same as above — `bind_cleanup` regression.
- **Verdict:** DELETE
- **Notes:** Strict subset of `server_restarts_after_sigkill` minus the pre-crash push. The "pushed nothing" angle adds no coverage.

### `wab_data_preserved_across_crash_restart` — system.rs:951
- **Asserts:** WAB bytes equal before/after SIGKILL, and `> 0` after restart.
- **Regression prevented:** A startup-time WAB scrub that wipes good segments.
- **Verdict:** STRENGTHEN
- **Notes:** The "preserved" claim is much weaker than verified: it checks total byte count, not that records replay or that the recovery pass classifies them as valid. With `weir_recovery_records_replayed` registered as a metric, this test should scrape it post-restart and assert `>= acked_count`. As written, a recovery pass that quarantines every segment but leaves the files on disk would pass this test.

## Fault injection

### `readonly_wab_dir_prevents_startup` — system.rs:985
- **Asserts:** With wab_dir at mode 0o000, server exits non-zero within 5 s.
- **Regression prevented:** A fail-open startup that runs without WAB writability (would silently lose data).
- **Verdict:** KEEP

## Multi-shard correctness

### `all_pushes_acked_with_multiple_shards` — system.rs:1033
- **Asserts:** 100 Batched pushes ack with shard_count=4.
- **Regression prevented:** A shard-routing crash or ack-channel cross-wiring across shards.
- **Verdict:** KEEP

### `shard_directories_created_on_disk` — system.rs:1044
- **Asserts:** With shard_count=3, ≥3 `shard_*` directories exist.
- **Regression prevented:** Lazy/missing shard directory creation.
- **Verdict:** KEEP

### `concurrent_producers_all_acked_with_multiple_shards` — system.rs:1074
- **Asserts:** 4 threads × 50 Sync pushes with shard_count=4 all ack.
- **Regression prevented:** A multi-shard concurrency race.
- **Verdict:** STRENGTHEN
- **Notes:** Heavy overlap with `concurrent_producers_all_acked`. To justify its existence, assert work landed in *different* shard directories (otherwise a buggy router that always picks shard 0 would still pass).

## Graceful shutdown under load

### `graceful_shutdown_under_load` — system.rs:1109
- **Asserts:** During 8-thread Sync flood + SIGTERM, no thread sees Nack/Protocol errors; shutdown completes in < 8 s; WAB has bytes.
- **Regression prevented:** Half-ack-then-die: server returns Nack mid-shutdown, or silently drops in-flight pushes, or hangs past `shutdown_timeout_secs`. One of the most valuable tests in the file.
- **Verdict:** KEEP

## Stalled client isolation

### `stalled_client_does_not_block_other_connections` — system.rs:1206 *(flagged — maintainer thinks good)*
- **Asserts:** With one client holding a connection open mid-frame (sent, not reading ack), 50 Sync pushes on a separate connection complete in < 5 s; server is still healthy afterward.
- **Regression prevented:** A regression where the per-connection async task blocks a shared worker (e.g. by accidentally holding a mutex across `.await`), or where the semaphore tracks "active work" rather than "live connection" and starves on a stuck reader. This is a genuine isolation property and the test exercises it the right way: real raw socket, real partial-frame state, real timing assertion.
- **Verdict:** KEEP — **confirmed good**
- **Notes:** The maintainer's judgment is right. Minor nit: the 5 s deadline is loose given 50 trivial Sync pushes usually finish in < 1 s; tightening to 2 s would surface degradation earlier. Also consider scaling the stall to N idle connections (say 16) to catch leaks that need multiple stalled clients to trigger.

## Partial frame injection

### `partial_frame_does_not_corrupt_next_connection` — system.rs:1276
- **Asserts:** After writing header + half a payload then closing, a fresh connection works.
- **Regression prevented:** Per-connection read state that leaks into the *accept* path (it shouldn't — each connection has its own buffer — but the test pins the invariant). Also catches a panic on mid-frame EOF that takes down the whole server.
- **Verdict:** KEEP

## Disk full

### `disk_full_returns_nack_not_crash` — system.rs:1322 *(flagged)*
- **Asserts:** With `RLIMIT_FSIZE=0`, the first Sync push returns `Nack(InternalError)`; server stays healthy.
- **Regression prevented:** A WAB write error path that panics or silently acks. That property is real and important.
- **Verdict:** RENAME (and consider adding a real ENOSPC variant)
- **Notes:** The name lies — this is EFBIG (write would exceed file-size limit), not ENOSPC (no space on device). The two error paths can diverge: EFBIG hits in the write syscall on the first byte over the limit, ENOSPC also hits in `fallocate`/`fsync`. **Recommendation:** rename this to `efbig_returns_nack_not_crash` (truthful) and add a separate `enospc_returns_nack_not_crash` that mounts a fixed-size tmpfs (or a loop device with a small backing file) and fills it. ENOSPC is the production scenario this test should be modeling; EFBIG just happens to be cheap to simulate without root. If only one can exist, the tmpfs variant is more valuable — EFBIG is essentially never hit in production deployments.

## WAB data integrity after crash

### `wab_data_integrity_after_crash` — system.rs:1349 *(flagged)*
- **Asserts:** Every payload that received an Ack for a Sync push appears verbatim in the raw WAB bytes after SIGKILL.
- **Regression prevented:** The Sync durability contract being violated: server acks before fsync, or fsync silently no-ops. This is the byte-level guarantee that defines the product. Highest-value test in the suite.
- **Verdict:** KEEP — **strongest test in the file**
- **Notes:** Together with `server_restarts_after_sigkill`: not redundant. `server_restarts_after_sigkill` proves the daemon can boot back up over its own crash debris; this one proves the data semantics. They test different invariants and both should stay. The substring search via `windows().any()` is O(N²) but at 50 records × small payloads it's fine. One subtle gap: this test never *restarts* the server after the crash, so it doesn't verify that recovery successfully reads those bytes back — that's the missing test the audit flags above.

## Socket takeover data safety

### `socket_takeover_does_not_corrupt_wab_data` — system.rs:1396
- **Asserts:** Spawning a second server B that takes A's socket leaves A's WAB byte-identical, with all 20 payloads still present.
- **Regression prevented:** A startup path that touches/locks the WAB of a previous-but-still-running instance via the socket path (would be catastrophic for data integrity). Models a misconfigured second-instance launch.
- **Verdict:** KEEP

## File descriptor limit exhaustion

### `fd_limit_exhaustion_does_not_crash_server` — system.rs:1485
- **Asserts:** With `RLIMIT_NOFILE=128`, flooding 200 raw connections then releasing them leaves the server healthy and able to ack a push.
- **Regression prevented:** A panic on `accept()` returning `EMFILE` — a real production failure mode.
- **Verdict:** STRENGTHEN
- **Notes:** Should additionally assert that at least *some* of the 200 connections were refused (otherwise the test passes if the fd limit is silently ignored). Counting `Err(_)` returns from `RawStream::connect` and asserting `>= 1` would do it.

## Metrics accuracy (counters)

### `records_accepted_counter_increments_after_sync_pushes` — system.rs:1524
- **Asserts:** After 10 Sync pushes, body contains `weir_records_accepted_total{tier="sync"} 10`.
- **Regression prevented:** Accepted counter miswired (wrong tier label, wrong increment site).
- **Verdict:** KEEP

### `records_ack_counter_increments_after_sync_pushes` — system.rs:1544
- **Asserts:** After 7 Sync pushes, body contains `weir_records_ack_total{tier="sync"} 7`.
- **Regression prevented:** Ack counter miswired.
- **Verdict:** KEEP
- **Notes:** Near-duplicate of the previous test; the two could be merged into one that checks both counters in one session, but the separation is cheap and the names are honest.

## Per-shard record ordering

### `per_shard_records_appear_in_submission_order` — system.rs:1573
- **Asserts:** With shard_count=1, payloads `order-00000..order-00029` appear in increasing byte offset in the WAB.
- **Regression prevented:** A queue or batcher that reorders records within a single shard — would silently break log-replay semantics for downstream consumers.
- **Verdict:** KEEP

## Batch deadline timer accuracy

### `batch_deadline_timer_keeps_latency_bounded` — system.rs:1621
- **Asserts:** Across 20 Sync pushes, each is < 100 ms (5×deadline) and the worst < 60 ms (3×deadline).
- **Regression prevented:** A batch-timer starvation regression (e.g. an accept loop that spins, or a worker that holds a lock past its deadline). Bounds latency in production-meaningful terms.
- **Verdict:** STRENGTHEN
- **Notes:** Mostly good. Two concerns: (a) labeling 20 samples' max as "p99" is sloppy — comment that out or run ≥100 samples. (b) On a loaded CI runner one occasional GC-pause-equivalent will flake; consider asserting on median + a separate looser tail.

## Metrics monotonicity under crash-restart

### `metrics_consistent_across_crash_restart` — system.rs:1662 *(flagged)*
- **Asserts:** In each of 3 rounds (with restarts between), `records_accepted ≤ pushes_made` and `records_ack ≤ records_accepted`.
- **Regression prevented:** *Within a single session*, only: counter overcount or ack-exceeds-accepted ordering. The cross-restart axis is entirely untested.
- **Verdict:** REWRITE
- **Notes:** The name is straight-up misleading. "Across crash-restart" implies a property that spans restarts; the only spanning behaviour the test exercises is "restart, then run the same per-round assertion again." The maintainer's read is correct: rename to `metrics_internally_consistent_per_session` if you keep the current logic, or rewrite to actually test cross-restart properties:
  - That after restart, `records_accepted_total` resets to 0 (it's a process-local atomic) — documents the deliberate non-persistence.
  - That `weir_recovery_records_replayed` advances by the number of records that were on disk pre-crash.
  - That `weir_wab_segments` reflects the on-disk file count after recovery.
  Without one of those, the test does not earn its name.
