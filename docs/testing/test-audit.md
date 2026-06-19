# System test suite overview

File: `crates/weir-server/tests/system.rs` (~2257 lines, 44 `#[test]`
functions as of this writing).

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

Of the 44 tests, **40 run on a plain `cargo test`**. The remaining four
are gated:

| Test | Gate | Why |
| --- | --- | --- |
| `enospc_returns_nack_not_crash` | `#[ignore]` | Needs a small pre-mounted tmpfs at `WEIR_TEST_ENOSPC_DIR` (creating one needs root). |
| `mysql_sink_end_to_end` | `#[ignore]` | Needs a live MySQL at `WEIR_TEST_MYSQL_URL`. |
| `postgres_sink_end_to_end` | `#[ignore]` | Needs a live Postgres at `WEIR_TEST_POSTGRES_URL`. |
| `clickhouse_sink_end_to_end` | `#[ignore]` + `#[cfg(feature = "clickhouse-sink")]` | Needs a live ClickHouse at `WEIR_TEST_CLICKHOUSE_URL` *and* the `clickhouse-sink` feature; otherwise the function isn't even compiled. |

The three sink end-to-end tests are also documented operationally in
[`sink-integration.md`](sink-integration.md), with the container-startup
recipes.

---

## Basic push / ack

### `smoke_single_push_ack`
The cheapest canary: a single `Sync` push round-trips with an `Ok`. If
this fails, the binary is fundamentally broken (won't start, accept, or
ack) and every other test would fail too.

### `all_durability_tiers_behave_per_contract`
Pushes 10 records per tier and reads
`weir_wab_fsync_duration_seconds_count` (one increment per fsync syscall)
as a deterministic differential: **Buffered** moves the fsync counter by
≤1 (acks before any fsync), **Sync** moves it by ≥N (one fsync per
record), **Batched** moves it by ≥1 (records are durably flushed, not
silently skipped). This is the strengthened replacement for the old
`all_durability_tiers_acked`, which only checked each tier returned `Ok`
and so couldn't tell a tier that secretly fsynced from one that secretly
didn't.

### `multiple_sequential_pushes_same_connection`
*(Removed — see "Tests no longer present" below.)*

## Health check

### `health_check_returns_ok`
A `HealthCheck` request returns `Ok`. Guards the existence of the
HealthCheck message type and the zero-length-payload path that distinguishes
a health check from an (rejected) empty push.

## Concurrent producers

### `concurrent_producers_all_acked`
8 threads × 100 Batched pushes. Beyond "no thread panicked", it scrapes
`weir_records_ack_total{tier="batched"}` and asserts it equals exactly
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
After 20 Sync pushes, at least one `.wab` or `.wab.sealed` file exists.
Catches a WAB writer that silently no-ops while still acking (mocked
out, wrong path, permissions).

### `idle_seal_drains_low_volume_segment_without_shutdown`
With `wab_segment_max_age_secs = 1`, a single Batched record's segment is
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
6 threads round-robin across Sync/Batched/Buffered (50 each). Asserts the
per-tier ack counter matches the records pushed for that tier, so a
dispatch table that mis-routes one tier (e.g. counts Buffered as Sync) or
drops a tier under contention is caught — which the old panic-on-`Err`
version could not see, since the client only observes Ack/Nack, not the
server's tier bookkeeping.

## Payload validation

### `empty_payload_is_rejected`
A zero-length Sync push must be rejected with
`Nack(NackReason::EmptyPayload)`. An empty payload collides with the WAB
end-of-records sentinel, so the server refuses it at ingest rather than
risk truncating a segment. (This *inverts* the old, wrong
`empty_payload_is_accepted` entry — the current contract is rejection.)

### `arbitrary_binary_payload_accepted`
A full `0..=255` byte payload Sync-pushes successfully, confirming no
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
Focused replay-metric test: push N Sync records, SIGKILL (active segment
left without a footer), restart, then assert
`weir_recovery_records_replayed_total >= N`. Pins that recovery seals and
replays the crashed active segment.

### `wab_data_integrity_after_crash`
The byte-level Sync durability contract: every payload that received an
`Ok` for a Sync push appears verbatim in the raw WAB bytes after SIGKILL.
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
expected list is **31 families on a default build**; under
`--all-features` (which turns on `bench-trace`) the four `weir_stage_*`
per-stage latency histograms register too, for **35 families**, and the
`#[cfg(feature = "bench-trace")]` branch extends the expected list to
match. This is the most valuable metrics test — these names are public
API. (The old doc's "19 families" count was stale.)

### `records_accepted_counter_increments_after_sync_pushes`
After 10 Sync pushes, the body contains
`weir_records_accepted_total{tier="sync"} 10`. Catches the accepted
counter being miswired (wrong tier label, wrong increment site).

### `records_ack_counter_increments_after_sync_pushes`
After 7 Sync pushes, the body contains
`weir_records_ack_total{tier="sync"} 7`. The ack-side counterpart.

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
8-thread Sync flood + SIGTERM: no thread sees a `Nack` or protocol error
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
separate connection completes 50 Sync pushes in < 5 s and the server
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
confirm the server still answers a health check and a Sync push. The
property under test is "doesn't crash or hang under fd pressure"; the
count of refused connects is logged as diagnostics but not asserted on,
because the kernel listen backlog absorbs connects on most Linux configs
even while `accept()` is returning EMFILE.

### `socket_takeover_does_not_corrupt_wab_data`
A second server B launched at server A's socket path calls `bind_cleanup`
and takes the socket, but A's WAB files must remain byte-identical with
all 20 payloads still present. The socket file and the WAB are
independent resources; this models a misconfigured second-instance
launch.

## Write-error handling (EFBIG / ENOSPC)

### `efbig_returns_nack_not_crash`
With `RLIMIT_FSIZE = 0` (and SIGXFSZ ignored), every WAB write fails with
EFBIG; the first Sync push must return `Nack(NackReason::InternalError)`
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
4 threads × 50 Sync pushes with `shard_count = 4`. Beyond "all acked", it
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
With `batch_deadline_ms = 20`, across 100 Sync samples: every sample is
within 5× the deadline (100 ms tail ceiling) and the median is within 2×
(40 ms). The median bound catches batch-timer starvation on the common
path; the loose tail bound keeps the test from flaking on noisy CI
runners (the regressions it targets are order-of-magnitude, not 10%
drift).

## Sink delivery (end-to-end, gated)

These exercise a real downstream database and so are `#[ignore]`-marked
(and, for ClickHouse, feature-gated). See
[`sink-integration.md`](sink-integration.md) for the runner script and
schema setup. Each pushes 100 Sync records and asserts the sink committed
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

- The metric-family count (31 default / 35 with `--all-features`) is
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
  test is `empty_payload_is_rejected`.
- `metrics_all_19_families_registered` → renamed to
  `metrics_all_families_registered` and the count corrected (31 default /
  35 all-features, not 19).
- `disk_full_returns_nack_not_crash` → renamed to
  `efbig_returns_nack_not_crash`, with `enospc_returns_nack_not_crash`
  added alongside.
- `metrics_consistent_across_crash_restart` → rewritten and renamed to
  `metrics_internally_consistent_per_session`; the cross-restart axis is
  now covered by `metrics_reset_to_zero_after_restart` and
  `recovery_replays_records_after_crash`.
