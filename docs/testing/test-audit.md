# System test suite overview

File: `crates/weir-server/tests/system.rs` (~6724 lines, 110 `#[test]`
functions as of 4.1.0).

> **What this is.** A current snapshot of the system-integration suite —
> the reference to consult when adding, removing, or renaming a system
> test. It is organised by test **name** grouped under behavioural
> categories, deliberately *not* by line number: line numbers rot on the
> first edit, names do not. Treat the count above as a rough sanity check,
> not a contract — the source file is the source of truth.

Every test spawns the real `weir-server` binary via the `weir_server!`
macro from `weir-testkit`, waits for the socket, and tears everything
down on drop. Each test gets its own temp dir, socket path, WAB dir, and
metrics port, so the suite runs in parallel. The whole file is
`#![cfg(unix)]` (the daemon only binds Unix sockets). Because each test
is a real multi-threaded OS process, run with a bounded thread count:

```sh
cargo test -p weir-server --test system -- --test-threads=4
```

## Gating: which tests run by default

Of the 110 tests, **92 run on a plain `cargo test`**. Eleven are
`#[ignore]`d and seven more compile only with the `test-hooks` feature:

| Test | Gate | Why |
| --- | --- | --- |
| `enospc_returns_nack_not_crash` | `#[ignore]` | Needs a small pre-mounted tmpfs at `WEIR_TEST_ENOSPC_DIR` (creating one needs root). |
| `mysql_sink_end_to_end` | `#[ignore]` | Needs a live MySQL at `WEIR_TEST_MYSQL_URL`. |
| `postgres_sink_end_to_end` | `#[ignore]` | Needs a live Postgres at `WEIR_TEST_POSTGRES_URL`. |
| `clickhouse_sink_end_to_end` | `#[ignore]` + `#[cfg(feature = "clickhouse-sink")]` | Needs a live ClickHouse at `WEIR_TEST_CLICKHOUSE_URL` *and* the `clickhouse-sink` feature; otherwise the function isn't even compiled. |
| `sql_sink_content_keyed_schema_loses_a_distinct_duplicate_record` | `#[ignore]` | Needs the docker-compose stack; pins what a content-keyed schema costs. |
| `sql_sink_plain_mode_duplicate_keys_do_not_livelock_the_drain` | `#[ignore]` | Needs the docker-compose stack. |
| `s3_sink_end_to_end` | `#[ignore]` + `#[cfg(feature = "s3-sink")]` | Needs the docker-compose MinIO rig. |
| `s3_sink_replay_is_an_idempotent_overwrite` | same | Pins replay stability of the object key. |
| `s3_sink_distinct_batches_of_identical_records_produce_distinct_objects` | same | Pins collision freedom. Either of this pair alone passes a broken key scheme, so they are run as a group. |
| `s3_sink_keeps_delivering_when_head_bucket_is_denied` | same | Least-privilege IAM reads as Degraded, and a Degraded sink must still drain. |
| `s3_sink_an_unreachable_endpoint_strands_rather_than_dead_letters` | same | Pins strand-not-dead-letter on an unreachable endpoint. |
| `lock_stolen_during_startup_recovery_fails_closed_before_serving`, `metrics_answer_during_recovery_before_the_socket_binds`, `sigterm_during_recovery_exits_zero_without_ever_serving`, `routing_avoids_a_shard_whose_counter_scan_failed`, `no_live_shard_refuses_connections_and_counts_them`, `a_binary_with_test_hooks_announces_them`, `a_dead_only_flusher_does_not_stop_the_drain` | `#[cfg(feature = "test-hooks")]` | Need the fault hooks (a startup delay in the recovering phase, a forced counter-scan failure), which exist only in a `--features test-hooks` build and stay inert unless `WEIR_TEST_HOOKS=1`. CI's `--all-features` test line runs them. |

`wab_cap_nack_is_recoverable_for_a_real_client` is
`#[cfg(feature = "http-sink")]`, which is a default feature, so it runs on a
plain `cargo test`.

The sink end-to-end tests are also documented operationally in
[`sink-integration.md`](sink-integration.md), with the container-startup
recipes.

---

## Basic push / ack

### `smoke_single_push_ack`
The cheapest canary: a single `Durable` push round-trips with an `Ok`. If
this fails, the binary is fundamentally broken (won't start, accept, or
ack) and every other test would fail too.

### `all_durability_tiers_behave_per_contract`
Pushes 10 records per tier (`Buffered`, then `Durable`; those are the
only two tiers) and reads
`weir_wab_fsync_duration_seconds_count` (one increment per fsync syscall)
as a deterministic differential: **Buffered** moves the fsync counter by
≤1 (acks before any fsync), **Durable** moves it by ≥N (each sequential
push waits for its batch-boundary group fsync). This is the strengthened replacement for the old
`all_durability_tiers_acked`, which only checked each tier returned `Ok`
and so couldn't tell a tier that secretly fsynced from one that secretly
didn't.

### `multiple_sequential_pushes_same_connection`
*(Removed — see "Tests no longer present" below.)*

## Not yet described here

Not every test in the file has an entry below. 4.1.0 (the safety floor and
the operability Foundations) added about forty, and they are documented by
their doc comments in `system.rs` rather than here. By area:

- **`wab_dir` lock and refused duplicates:**
  `second_daemon_on_same_wab_dir_is_refused_and_first_keeps_acking`,
  `lock_is_released_when_the_holder_is_sigkilled`,
  `lock_file_stolen_by_second_daemon_fails_closed`,
  `lock_stolen_during_startup_recovery_fails_closed_before_serving`,
  `weir_ctl_beside_a_live_daemon_neither_sees_nor_disturbs_the_lock`,
  `refused_duplicate_never_delivers_shared_socket`,
  `refused_duplicate_never_delivers_shared_metrics_port`.
- **SIGHUP and reload:** `sighup_is_a_counted_noop_on_a_default_daemon`,
  `sighup_flood_does_not_kill_or_wedge`,
  `sighup_during_startup_recovery_is_served`,
  `admin_reload_and_sighup_share_one_reload_path`.
- **Startup phases and health routes:**
  `metrics_answer_during_recovery_before_the_socket_binds`,
  `a_slow_sink_backlog_does_not_delay_the_producer_socket`,
  `health_routes_answer_on_a_serving_daemon`,
  `a_serving_daemon_reports_ready_through_the_new_gauges`,
  `public_status_and_verbose_probes_carry_no_secrets`,
  `a_restart_over_the_cap_is_not_ready_at_once_and_recovers_with_zero_pushes`,
  `routing_avoids_a_shard_whose_counter_scan_failed`,
  `no_live_shard_refuses_connections_and_counts_them`,
  `startup_log_names_version_and_config_and_carries_no_ansi`,
  `missing_config_file_warns_and_logs_source_at_startup`,
  `a_binary_with_test_hooks_announces_them`.
- **Bounded shutdown:** `sigterm_exits_within_the_bound_with_an_idle_connection_open`,
  `sigterm_during_recovery_exits_zero_without_ever_serving`,
  `readyz_goes_503_within_200ms_of_sigterm_while_metrics_stay_up`,
  `sigterm_unlinks_the_socket_at_once_and_metrics_stay_up_until_exit`,
  `bounded_shutdown_with_a_full_drain_channel_loses_no_acked_record`,
  `a_second_sigterm_forces_a_prompt_exit_and_loses_nothing`,
  `sigterm_with_a_zero_budget_is_prompt_and_loses_nothing`,
  `a_dead_only_flusher_does_not_stop_the_drain`.
- **Admin socket and `weir-ctl`:**
  `admin_socket_serves_status_at_the_derived_path_and_is_removed_at_exit`,
  `an_explicit_admin_path_in_a_shared_directory_refuses_to_start`,
  `a_derived_admin_path_in_a_shared_parent_disables_admin_not_the_daemon`,
  `admin_pause_holds_delivery_while_ingest_acks_and_resume_delivers_all`,
  `admin_retry_stranded_redelivers_without_waiting_for_the_health_poll`,
  `weir_ctl_status_via_admin_http_and_offline_agrees_with_readyz`,
  `weir_ctl_drain_and_reload_round_trip_over_the_admin_socket`.

Tests from before 4.1.0 that also have no entry yet: the tracked-push and
batch tests (`tracked_push_returns_a_coordinate_that_matches_the_wab`,
`tracked_and_plain_pushes_interleave_on_one_connection`,
`buffered_push_also_gets_a_coordinate`,
`a_push_batch_is_answered_with_a_bitmap_and_counts_every_record`,
`a_batch_containing_an_empty_record_is_rejected_whole_and_closes`,
`a_batch_over_the_record_cap_is_refused`,
`the_client_can_push_a_batch_and_read_every_outcome`,
`the_client_refuses_a_locally_invalid_batch_without_poisoning`), the
segment-lifetime and compression tests
(`max_lifetime_seal_fires_for_a_producer_that_never_goes_idle`,
`max_lifetime_seal_is_off_by_default`,
`idle_seal_does_not_fire_for_a_producer_that_never_goes_idle`,
`wab_compression_knob_reaches_the_segment_header`,
`wab_compression_off_writes_v1_segments`), and
`retired_batched_wire_byte_is_still_accepted_by_a_live_daemon`,
`wab_cap_nack_is_recoverable_for_a_real_client`,
`one_connection_can_interleave_both_durability_tiers`,
`version_and_help_flags_short_circuit_and_exit_zero`,
`quarantined_records_after_a_corruption_can_be_recovered`,
`group_commit_histogram_counts_every_durable_record_exactly_once`.

## Health check

### `health_check_returns_ok`
A `HealthCheck` request returns `Ok`. Guards the existence of the
HealthCheck message type and the zero-length-payload path that distinguishes
a health check from an (rejected) empty push.

## Concurrent producers

### `concurrent_producers_all_acked`
8 threads × 100 Durable pushes. Beyond "no thread panicked", it scrapes
`weir_records_ack_total{tier="durable"}` and asserts it equals exactly
800 — a thread that silently dropped half its pushes would land here with
a short count rather than passing.

### `many_connections_open_simultaneously`
Opens 200 connections *first* (with a retry/back-off loop to ride out the
listen-backlog cap on macOS), then pushes one Buffered record from each.
Exercises the semaphore-based connection cap with real headroom under the
default `max_connections = 256`. (Bumped up from the old count of 20,
which was too far below the cap to test anything.)

## WAB on-disk verification

### `records_written_to_wab_on_disk`
After 20 Durable pushes, at least one `.wab` or `.wab.sealed` file exists.
Catches a WAB writer that silently no-ops while still acking (mocked
out, wrong path, permissions).

### `idle_seal_drains_low_volume_segment_without_shutdown`
With `wab_segment_max_age_secs = 1`, a single Durable record's segment is
sealed and drained on idle — without waiting to fill the 256 MiB segment
or for shutdown. Proven by polling
`weir_sink_commit_records_total{outcome="committed"}` past the idle
threshold and asserting it reaches ≥1.

## Durability tiers and ack contract under load

### `sustained_load_1000_records_single_client`
1000 sequential Buffered pushes; then asserts both
`weir_records_accepted_total{tier="buffered"}` and
`weir_records_ack_total{tier="buffered"}` equal 1000. Catches a slow
leak or silent drop that surfaces only after sustained load (counters
fall short while every `push().unwrap()` still passes).

### `mixed_durability_under_concurrent_load`
6 threads alternate between Durable and Buffered (50 records each). Asserts the
per-tier ack counter matches the records pushed for that tier, so a
dispatch table that mis-routes one tier (e.g. counts Buffered as Durable) or
drops a tier under contention is caught — which the old panic-on-`Err`
version could not see, since the client only observes Ack/Nack, not the
server's tier bookkeeping.

## Payload validation

### `empty_payload_rejected_locally_by_client` / `empty_payload_rejected_by_daemon_for_raw_frame`
An empty payload collides with the WAB end-of-records sentinel, so it is
refused at both ends. `weir-client` rejects it locally with
`ClientError::EmptyPayload` before sending anything, and the client stays
usable. A raw-socket producer that bypasses that guard gets
`Nack(NackReason::EmptyPayload)` from the daemon at ingest. (This
*inverts* the old, wrong `empty_payload_is_accepted` entry — the current
contract is rejection.)

### `arbitrary_binary_payload_accepted`
A full `0..=255` byte payload Durable-pushes successfully, confirming no
text-mode handling strips NULs or high bytes. Renamed from
`binary_payload_round_trips` — there is no Pop API, so nothing
"round-trips".

### `payload_size_boundary_enforced`
Tests the actual `MAX_PAYLOAD_HARD_CAP` (16 MiB) boundary, not a
mid-range size: a payload exactly at the cap is accepted; a payload one
byte over is rejected with `Nack(NackReason::PayloadTooLarge)`. The
over-cap case uses a raw socket that sends only the 16-byte header
declaring an over-cap length (no body), so the Nack reason can be read
directly rather than being masked by a BrokenPipe mid-write. Confirms the
server stays alive afterward. Replaces the weak `large_payload_accepted`
(a single 1 MiB push).

## Crash-restart durability and recovery

### `server_restarts_after_sigkill`
Push acks pre-crash; SIGKILL leaves the socket file behind; restart
succeeds within a 10 s budget; a post-restart push acks. Proves the
daemon can boot over its own crash debris (stale-socket cleanup, no panic
on cold start with WAB files present).

### `wab_data_preserved_across_crash_restart`
WAB bytes are identical before and after SIGKILL, non-zero after restart,
**and** `weir_recovery_records_replayed_total >= acked_count`. The replay
assertion is the load-bearing one: a recovery pass that quarantined every
segment would leave the bytes on disk while losing every record, and the
byte-count check alone could not catch that.

### `acked_records_delivered_to_sink_and_confirmed_after_crash_restart`
Extends crash recovery past the replay queue to the *delivery + confirm*
end state: after SIGKILL + restart, polls
`weir_sink_commit_records_total{outcome="committed"}` until every acked
record has been delivered to the (noop) sink and confirmed. Closes the
full crash → recovery → replay → drain → sink → confirm loop.

### `recovery_replays_records_after_crash`
Focused replay-metric test: push N Durable records, SIGKILL (active segment
left without a footer), restart, then assert
`weir_recovery_records_replayed_total >= N`. Pins that recovery seals and
replays the crashed active segment.

### `wab_data_integrity_after_crash`
The byte-level Durable durability contract: every payload that received an
`Ok` for a Durable push appears verbatim in the raw WAB bytes after SIGKILL.
If the client got an `Ok`, the fsync happened before the ack — this is
the guarantee that defines the product. (Note: this test does not restart
the server; the read-back-after-restart axis is covered by the recovery
tests above.)

## Metrics

### `metrics_endpoint_responds_with_openmetrics_content`
`/metrics` returns a non-empty body that looks like OpenMetrics
(`weir_` or `# EOF`). Catches the metrics HTTP server failing to bind or
returning garbage.

### `metrics_all_families_registered`
Asserts every expected metric family appears as a `# HELP weir_…` line
**and** that the count of distinct `# HELP weir_` lines equals the
expected-list length — a drift detector that fails loudly if a metric is
added to `metrics/mod.rs` without updating the list (or vice versa). The
expected list is **91 families on a default build**; under
`--all-features` (which turns on `bench-trace`) the four `weir_stage_*`
per-stage latency histograms register too, for **95 families**, and the
`#[cfg(feature = "bench-trace")]` branch extends the expected list to
match. This is the most valuable metrics test — these names are public
API. (The old doc's "19 families" count was stale.)

### `records_accepted_counter_increments_after_durable_pushes`
After 10 Durable pushes, the body contains
`weir_records_accepted_total{tier="durable"} 10`. Catches the accepted
counter being miswired (wrong tier label, wrong increment site).

### `records_ack_counter_increments_after_durable_pushes`
After 7 Durable pushes, the body contains
`weir_records_ack_total{tier="durable"} 7`. The ack-side counterpart.

### `drain_state_shows_draining_and_not_blocked`
On startup the pre-initialised `weir_drain_state` gauge vector reads
`draining=1`, `retrying_transient=0`, `blocked_dead_letter_full=0`.
Guards the gauge's initial value and its label names.

### `sink_health_shows_healthy_via_noop_sink`
With the NoopSink, `weir_sink_health` reads `healthy=1`, `degraded=0`,
`down=0`. Guards the sink-health enum-to-label mapping.

### `metrics_internally_consistent_per_session`
Across 3 rounds (restarting between rounds), within each session
`records_accepted <= pushes_made` and `records_ack <= records_accepted`.
A per-session invariant only — the cross-restart reset and the recovery
counter are tested separately. (This is the honestly-named rewrite of the
old, misleading `metrics_consistent_across_crash_restart`.)

### `metrics_reset_to_zero_after_restart`
`records_accepted_total` and `records_ack_total` are in-process atomics:
after driving both above zero and restarting, they read exactly 0 before
any new push. Documents the deliberate non-persistence of Prometheus
counters across a process restart.

## Graceful shutdown

### `server_shuts_down_cleanly_on_sigterm`
SIGTERM on an idle daemon completes within a 5 s budget. The wall-clock
bound is the point — without it, a shutdown that hung for any duration
short of cargo's timeout would still pass.

### `server_exits_and_socket_disappears_after_sigterm`
The socket file exists before shutdown and is gone after a clean exit.
Catches cleanup logic that leaves stale socket files behind on graceful
exit.

### `graceful_shutdown_under_load`
8-thread Durable flood + SIGTERM: no thread sees a `Nack` or protocol error
(only `Ok` or `Io` EOF), shutdown completes in < 8 s, and the WAB has
bytes. Catches half-ack-then-die, silent drops of in-flight pushes, or a
hang past `shutdown_timeout_secs`. One of the highest-value tests in the
file.

## Reconnect / restart

### `new_connection_accepted_after_previous_client_drops`
100 connect → push → drop rounds, then a final health check. Cycling well
past `max_connections` proves the connection-semaphore permit is reliably
returned on every drop; a leak that only triggers after N drops would be
invisible to a single-shot test.

## Connection isolation and fault tolerance

### `stalled_client_does_not_block_other_connections`
A raw-socket client sends one Push frame and then never reads the ack,
holding its connection (and its semaphore permit) open. Meanwhile a
separate connection completes 50 Durable pushes in < 5 s and the server
stays healthy. Catches a per-connection task that blocks a shared worker
(e.g. holding a mutex across `.await`) or a semaphore that tracks "active
work" rather than "live connection".

### `partial_frame_does_not_corrupt_next_connection`
A raw socket writes a valid header plus only half the declared payload,
then closes mid-frame. A fresh connection afterward must work normally —
the partial frame must not corrupt the per-connection read state or panic
the server on mid-frame EOF.

### `fd_limit_exhaustion_does_not_crash_server`
With `RLIMIT_NOFILE = 128`, flood 200 raw connections, release them, and
confirm the server still answers a health check and a Durable push. The
property under test is "doesn't crash or hang under fd pressure"; the
count of refused connects is logged as diagnostics but not asserted on,
because the kernel listen backlog absorbs connects on most Linux configs
even while `accept()` is returning EMFILE.

### `second_daemon_refuses_a_live_socket`
A second server B (its own `wab_dir` and metrics port) launched at server
A's socket path must refuse to start, because the connect-only liveness
probe finds A listening: B exits 1 with `in use by a running process`, A's
socket file is the same inode, A's WAB tree is byte-identical, and A still
acks a Durable push on a new connection. It replaced
`socket_takeover_does_not_corrupt_wab_data`, which asserted the old
behaviour (B unlinked A's live socket and took the path over).

### `shutdown_leaves_a_successor_socket_alone`
While A runs, the test unlinks A's socket and binds its own listener at
the same path. A's SIGTERM must leave that socket in place (same inode,
still accepting) and log that another instance replaced it: shutdown
removes only the inode it bound.

### `starts_cleanly_over_a_stale_socket_left_by_sigkill`
A daemon delivering to a recording HTTP sink is SIGKILLed, leaving its
socket file behind with nobody listening (ECONNREFUSED). Restarted on the
same paths, it must log `removed stale socket`, bind, recover, and deliver
every record acked before the kill and after the restart.

## Write-error handling (EFBIG / ENOSPC)

### `efbig_returns_nack_not_crash`
With `RLIMIT_FSIZE = 0` (and SIGXFSZ ignored), every WAB write fails with
EFBIG; the first Durable push must return `Nack(NackReason::InternalError)`
and the server must stay healthy. EFBIG is the cheapest write-failure
mode to simulate without root.

### `enospc_returns_nack_not_crash` *(`#[ignore]`)*
The production-shaped sibling of the EFBIG test: a real
filesystem-out-of-space rather than a per-process file-size rlimit.
Pushes 1 KiB records until one fails with `Nack(InternalError)`, then
confirms the server is still alive. Requires a small pre-mounted tmpfs at
`WEIR_TEST_ENOSPC_DIR` (the setup recipe is in the test's docstring), so
it's `#[ignore]`-marked.

### `readonly_wab_dir_prevents_startup`
With `wab_dir` at mode `0o000`, the server must exit non-zero within 5 s
rather than fail open and run without a writable WAB. When the test
harness runs as root it drops the child to uid `nobody` so the permission
bit actually bites.

## Multi-shard correctness

### `shard_directories_created_on_disk`
With `shard_count = 3`, at least three `shard_*` directories exist on
disk after a push. Catches lazy or missing shard-directory creation.

### `concurrent_producers_all_acked_with_multiple_shards`
4 threads × 50 Durable pushes with `shard_count = 4`. Beyond "all acked", it
walks the per-shard directories and asserts data landed in ≥2 of them —
without that check a buggy router that always picked shard 0 would be
indistinguishable from the single-shard concurrency test.

## Ordering

### `per_shard_records_appear_in_submission_order`
With `shard_count = 1`, payloads `order-00000..order-00029` appear at
strictly increasing byte offsets in the WAB. The fundamental append-log
ordering contract — any batching/queue change that reorders within a
shard is caught.

### `concurrent_producers_to_same_shard_preserve_per_producer_order`
Single shard, 4 concurrent producers × 50 records each. After all finish,
*each producer's own* records appear in ascending sequence order in the
WAB. Pins that the partition queue → worker buffer → flusher channel
chain stays FIFO per producer even with multiple workers and concurrent
writers.

## Latency bounds

### `batch_deadline_timer_keeps_latency_bounded`
With `batch_deadline_ms = 20`, across 100 Durable samples: every sample is
within 5× the deadline (100 ms tail ceiling) and the median is within 2×
(40 ms). The median bound catches batch-timer starvation on the common
path; the loose tail bound keeps the test from flaking on noisy CI
runners (the regressions it targets are order-of-magnitude, not 10%
drift).

## Sink delivery (end-to-end, gated)

These exercise a real downstream database and so are `#[ignore]`-marked
(and, for ClickHouse, feature-gated). See
[`sink-integration.md`](sink-integration.md) for the runner script and
schema setup. Each pushes 100 Durable records and asserts the sink committed
them all (`weir_sink_commit_records_total{outcome="committed"} >= 100`),
that at least one `commit()` happened, and that the records-per-commit
ratio is ≥ 10:1 (the IOPS-compression story — many records per
multi-row INSERT / RowBinary insert).

### `mysql_sink_end_to_end` *(`#[ignore]`)*
`sink_type = "mysql"`, URL from `WEIR_TEST_MYSQL_URL`. Verifies delivery
and IOPS compression against a real MySQL.

### `postgres_sink_end_to_end` *(`#[ignore]`)*
The Postgres counterpart: `sink_type = "postgres"`, URL from
`WEIR_TEST_POSTGRES_URL`.

### `clickhouse_sink_end_to_end` *(`#[ignore]`, `#[cfg(feature = "clickhouse-sink")]`)*
`sink_type = "clickhouse"`, URL from `WEIR_TEST_CLICKHOUSE_URL`. Verifies
the RowBinary HTTP insert path and the IOPS-compression ratio. Only
compiled when the `clickhouse-sink` feature is on.

---

## Maintaining this document

When you add, remove, or rename a system test, update the matching entry
here by **name** — do not add line numbers. Keep the category grouping;
add a new category only if a genuinely new behavioural area appears.

Two related invariants live outside this file and are worth knowing about
when you touch metrics tests:

- The metric-family count (91 default / 95 with `--all-features`) is
  enforced by `metrics_all_families_registered` against the expected list
  in the test and the `reg!(...)` registrations in
  `crates/weir-server/src/metrics/mod.rs`. Adding a metric means touching
  both, and the unit tests in `metrics/mod.rs` (`all_metric_names_present_in_output`)
  as well.
- The sink end-to-end tests are also documented in
  [`sink-integration.md`](sink-integration.md); keep the two in sync.

## Tests no longer present

The following names appeared in earlier revisions of this audit and are
**gone** from the current suite — do not look for them:

- `multiple_sequential_pushes_same_connection` — removed.
- `health_check_on_separate_connection_from_push` — removed (it observed
  no cross-connection interaction).
- `wab_segment_rotation_creates_multiple_segments` — removed (40 KiB
  could never rotate a 256 MiB segment; idle-seal behaviour is now
  covered by `idle_seal_drains_low_volume_segment_without_shutdown`).
- `wab_writes_nonzero_bytes_to_disk_after_sync_pushes` — removed (folded
  into the byte-level integrity / durability tests).
- `stale_socket_removed_automatically_on_restart` — removed (a strict
  subset of `server_restarts_after_sigkill`).
- `all_durability_tiers_acked` → renamed/strengthened to
  `all_durability_tiers_behave_per_contract`.
- `binary_payload_round_trips` → renamed to
  `arbitrary_binary_payload_accepted`.
- `large_payload_accepted` → replaced by `payload_size_boundary_enforced`.
- `empty_payload_is_accepted` → the behaviour **inverted**; the current
  tests are `empty_payload_rejected_locally_by_client` and
  `empty_payload_rejected_by_daemon_for_raw_frame` (an earlier
  `empty_payload_is_rejected` was split into these two).
- `metrics_all_19_families_registered` → renamed to
  `metrics_all_families_registered` and the count corrected (91 default /
  95 all-features as of 4.1.0, not 19).
- `disk_full_returns_nack_not_crash` → renamed to
  `efbig_returns_nack_not_crash`, with `enospc_returns_nack_not_crash`
  added alongside.
- `metrics_consistent_across_crash_restart` → rewritten and renamed to
  `metrics_internally_consistent_per_session`; the cross-restart axis is
  now covered by `metrics_reset_to_zero_after_restart` and
  `recovery_replays_records_after_crash`.
- `records_accepted_counter_increments_after_sync_pushes` → renamed to
  `records_accepted_counter_increments_after_durable_pushes` (the `Sync`
  tier is gone; behaviour unchanged).
- `records_ack_counter_increments_after_sync_pushes` → renamed to
  `records_ack_counter_increments_after_durable_pushes` (same reason).
- `socket_takeover_does_not_corrupt_wab_data` → replaced in 4.1.0 by
  `second_daemon_refuses_a_live_socket` (the old test asserted the takeover
  that 4.1.0 refuses).
