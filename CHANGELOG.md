# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. Wire protocol version
changes are tracked separately under **Wire protocol** below.

---

## [Unreleased]

### Performance

This release ships a focused optimisation pass on the push hot path.
Sandbox numbers (4-core; not CI-bare-metal) across 3-trial medians,
baseline vs after the full series:

| Scenario                | Baseline RPS | After RPS  | Improvement |
|-------------------------|-------------:|-----------:|:-----------:|
| single_thread_buffered  |          732 |     ~5,900 | **8.0×**    |
| single_thread_sync      |          432 |     ~1,150 | **2.7×**    |
| thundering_herd_8       |        2,091 |     ~2,810 | 1.34×       |
| thundering_herd_32      |        1,774 |    ~10,790 | **6.1×**    |
| thundering_herd_64      |        2,643 |    ~19,900 | **7.5×**    |

Sandbox absolute values are lower than the CI bare-metal numbers in
`docs/benchmarks/latest.md` (which were captured before this work);
the *relative* multipliers above are the meaningful signal until CI
republishes on the next merge.

The five commits making up this pass:

- **`perf(connection): try_push fast path + cached Ack/HealthCheck frames`** —
  `handle_push` used to wrap every successful queue push in
  `task::spawn_blocking` so the tokio worker wouldn't stall on a
  potentially-blocking `crossbeam::Sender::send`. But crossbeam's send is
  wait-free when the partition has capacity (the steady-state case under
  normal load). Now: try the non-blocking `try_push` first; only fall
  back to `spawn_blocking + push_timeout` when the partition is genuinely
  full. Separately, the 20-byte `Ack` frame and `HealthCheckResponse`
  frame are entirely constant — memoised once via `OnceLock` so the
  steady-state ack path writes a borrowed `&'static [u8]` instead of
  allocating + CRC-ing + encoding identical bytes per response.
- **`perf(wab): group-fsync Sync records within a batch (was per-record)`** —
  the largest single lever. `flush_batch` used to fsync once per Sync
  record, which made Sync's per-record contract the bottleneck whenever
  multiple producers were hitting the same shard. Sync's contract is
  "fsync before ack" — NOT "fsync syscall per record" — and one fsync at
  the end of the batch covers every record written during it, so the
  contract is upheld with up to `batch_size×` fewer fsyncs under
  concurrent Sync load. Single-producer Sync is unchanged (a serial push
  pattern keeps batch size = 1 per fsync by construction).
- **`perf(worker): eager drain + adaptive coalesce window`** — two
  iterations landed: first a fixed-50 μs window after the wait-free drain
  (un-bounds single-thread RPS from the batch deadline), then an adaptive
  variant that predicts whether to wait by remembering whether the
  previous batch had ≥ 2 records. Bench-validated single_thread_buffered
  improves ~8×; multi-producer batching stays intact thanks to the
  prediction (self-correcting — one bad batch flips the predictor back).
- **`perf(connection): wrap socket reads in BufReader`** — every Push
  used three `read_exact` syscalls (header, payload, CRC) where the
  kernel typically buffers the whole frame from a single client
  `write_all` into one packet. `BufReader` collapses those to one
  syscall in the common case.
- **`chore(profile): release profile uses lto = "fat" + codegen-units = 1`** —
  standard production-release hygiene. Small gain on this codebase
  (the hot path is syscall-bound, which LTO can't optimise) but
  defensible for production-released binaries. `panic = "abort"`
  deliberately NOT enabled — would break the F15 flusher panic
  supervisor (`catch_unwind` requires unwinding panics).

### Changed

- **`Sync` durability now group-fsyncs at the batch boundary** instead of
  per-record. The wire contract (ack ⇒ durable) is unchanged: every Sync
  ack still fires only after a fsync covering that record completes. The
  observable difference is metric-shaped: under concurrent Sync load,
  `weir_wab_fsync_duration_seconds_count` increments slower (records per
  fsync rises from ~1 to up to `batch_size`). Single-producer serial Sync
  is unaffected — the batch only ever holds one record.

### Refactored

- **Shared SQL-sink infrastructure** (`crates/weir-server/src/sink/sql_common.rs`).
  Extracted from the now-near-twin `mysql.rs` and `postgres.rs` modules:
  - `validate_identifier(field, value, max_len)` — strict
    `[A-Za-z_][A-Za-z0-9_]{0,max_len-1}` validation, parametric on
    dialect (MySQL = 64, Postgres = 63). Single source of truth for the
    chokepoint that makes `format!("INSERT INTO {table} ...")` safe;
    drift between the two sinks here would be a SQL-injection vector.
    Per-sink build-error enums adopt a 3-line `From<InvalidIdentifier>`
    impl and supply their own `IDENTIFIER_MAX_LEN` constant.
  - `redact_password(url)` — URL password redaction used by both sinks'
    `Debug` impls. Identical implementation in both, now identical
    type. Drift here would leak credentials into log lines.
  - `SqlSinkError` — the runtime `commit()` error returned by both
    sinks. Same `Transient` / `Permanent` / `Timeout` shape as the old
    per-sink enums. The new enum's variants carry
    `driver: &'static str` so error messages still distinguish
    `"mysql sink transient: …"` from `"postgres sink transient: …"`.
    No external code names the old per-sink error types (the drain
    consumes sinks via `Sink::Error`), so collapsing them was a clean
    rename behind the abstraction.
  - Comprehensive shared test coverage moved here: identifier
    validation including a 16-case injection-attempt sweep, redaction
    of both `mysql://` and `postgres://` URLs, malformed-URL safety,
    URL-encoded password redaction, and driver-tag preservation in
    `SqlSinkError`'s `Display`.

  The driver-specific glue stays per-sink: the `classify` function
  (different `mysql_async::Error` vs `tokio_postgres::Error` shapes),
  the transient-codes table (MySQL error numbers vs Postgres
  SQLSTATEs), the `Sink` trait impl, the SQL-string builder, the
  config struct, the build-error enum's per-driver extras (Postgres'
  `PoolBuild` variant).

  Net production-code delta: −119 LOC across the two sinks, +147 LOC
  in `sql_common`, so +28 LOC; the future-payoff is that the next SQL
  sink (ClickHouse, SQLite, …) lands as ~150 LOC of driver glue
  instead of ~600 LOC of copy-pasted infrastructure.

  321 tests pass (up from 315: 6 new sql_common tests for coverage
  not present in either sink before).

### Documentation

- **`docs/testing/test-audit.md` refreshed.** Two items the audit
  flagged as STRENGTHEN / RENAME-with-ENOSPC-variant have since been
  implemented in code (the `weir_recovery_records_replayed_total`
  assertion in `wab_data_preserved_across_crash_restart`, and the
  `efbig_returns_nack_not_crash` rename + `enospc_returns_nack_not_crash`
  sibling test). The audit doc still carried the original
  recommendations as if pending; verdicts updated to KEEP and the top
  findings summary now strikes through the closed items. Audit
  remains useful as a record of what was found and what was done.

### Fixed

- **`weir_wab_segments_total{state="open"}` metric now actually
  increments**
  (`crates/weir-server/src/wab/segment.rs::ShardWriter::ensure_open`).
  The counter has been registered in `metrics/mod.rs` since the
  metric set was introduced but was never wired — the other three
  states (`sealed`, `confirmed`, `quarantined`) bump at their
  respective lifecycle points (`flush_batch`, drain confirmation,
  recovery quarantine) but `open` had no caller. `ShardWriter::new`
  now takes an `Arc<Metrics>`, and `ensure_open` bumps the counter
  every time a new segment file is created. Closes the
  open → sealed → confirmed/quarantined observability gap so
  operators get a complete state-transition count. New unit test
  `open_segment_counter_increments_when_ensure_open_creates_segment`
  pins the wiring.

- **Stale `validate_path` duplication note in
  `docs/architecture.md`.** The doc claimed `validate_path` lived in
  both `src/config/mod.rs` (canonical) and `src/wab/mod.rs` (local
  copy "with a TODO to consolidate"). A grep confirms only the
  canonical version actually exists; the WAB-side copy was removed
  long ago. The doc paragraph has been updated to reflect the
  current single-source-of-truth reality.

- **Stale `TODO(perf)` comment in `tests/load.rs`** removed. It
  listed planned improvement areas (end-to-end latency,
  thundering-herd queue contention, batching efficiency, connection
  setup cost) all of which were addressed by the post-v0.4
  performance pass. Future perf work belongs in the CHANGELOG, not
  inline in the load-test module-doc.

### Added

- **`PostgresSink` — Postgres counterpart to the MySQL sink**
  (`crates/weir-server/src/sink/postgres.rs`). Direct mirror of
  `MySqlSink`'s shape: multi-row INSERT, identifier validation,
  transient/permanent error classification, pooled connections, health
  probe. Replaces MySQL idioms with their Postgres equivalents —
  `ON CONFLICT DO NOTHING` in place of `INSERT IGNORE`, `$N`
  positional parameters in place of `?`, double-quoted identifiers in
  place of backticks, SQLSTATE-based transient classification (`40P01`
  `deadlock_detected`, `55P03` `lock_not_available`, `57014`
  `query_canceled`, `57P01`/`57P02`/`57P03` shutdown family, `23505`
  `unique_violation` under Plain mode only). Driver is `tokio-postgres`
  with a `deadpool-postgres` pool (max 4 connections). New config knobs
  `sink_postgres_table` / `sink_postgres_column` /
  `sink_postgres_insert_mode` mirror the existing MySQL ones; `sink_url`
  carries the `postgres://user:pass@host:5432/db` connection string and
  is required when `sink_type = "postgres"`. 15 unit tests cover
  identifier validation, password redaction, SQL generation in both
  insert modes, and SQLSTATE classification.
  - **TLS deliberately omitted in v1** — the initial sink ships with a
    `NoTls` connector to keep the dependency surface minimal. Cleartext
    is fine on private networks; deployments that need TLS should plumb
    a TLS-terminating proxy in front of the Postgres server. Native TLS
    via `tokio-postgres-rustls` is a planned follow-up.
- **Startup advisory for `shard_count` / `worker_count` vs core count**
  (`crates/weir-server/src/main.rs::advise_agent_count`). On boot the
  daemon compares the configured agent count against
  `recommended_agent_count(cores) = max(2, cores - 2) / 2` and emits a
  `WARN` (if oversubscribed by 2×+) or `INFO` (if under-utilising by 4×+
  on a host with ≥ 4 recommended agents). Advisory only — daemon starts
  normally regardless; operator config wins. Empirical basis is a sweep
  in `tests/load.rs::sweep_agent_count_vs_throughput` (ignored by
  default, run with `--ignored`): on a 4-core sandbox the
  Sync-herd-64 peak sits at `agent_count = 1`, **14% above** the current
  bench-preset default of 4. Two compounding effects: ~2 OS threads per
  agent compete with the tokio runtime once `agent_count ≥ cores`, and
  fewer shards mean each flusher sees more concurrent producers per
  group-fsync (records_per_fsync went from ~7 at 4 agents to ~60 at 1
  agent in the investigation trace). Heuristic validated at 4 cores;
  extrapolates linearly but unproven at higher core counts — labelled
  honestly in the advisory's source comment.
- **`weir-testkit` crate** (`crates/weir-testkit/`, `publish = false`). New
  workspace member that consolidates the test harness previously duplicated
  across `tests/system.rs` and `tests/load.rs`. Exposes a `WeirServer` handle
  + `WeirServerBuilder` covering all the spawn variants (`shard_count`,
  `worker_count`, `batch_size`, `batch_deadline_ms`, `max_connections`,
  `shutdown_timeout_secs`, `log_level`, `extra_config(line)`, `env(k,v)`,
  `wab_dir(path)`, `silence_logs()`, `unsafe pre_exec(closure)`,
  `bench_preset()`), plus `process_lock()` and `free_port()` helpers and
  a `weir_server!(tag)` macro that wires `env!("CARGO_BIN_EXE_weir-server")`
  at the call site. Refactor net: -766 LOC across `system.rs`/`load.rs`,
  +703 LOC in testkit, identical on-disk + on-wire behaviour.
- **`WeirClient::from_stream`** constructor in `crates/weir-client`. Wraps
  an already-connected `UnixStream` instead of opening one via `connect`.
  Used by the new client proptest harness (`UnixStream::pair`) and useful
  in production for callers managing their own connection setup (systemd
  socket activation, pre-authenticated file descriptors). One-liner; no
  behavioural change to `connect`.
- **Property-based tests for `WeirClient` response handling**
  (`crates/weir-client/tests/proptest_client.rs`). 5 tests (3 proptest
  properties × 256 cases + 2 deterministic regressions). Property: arbitrary
  byte sequences from the daemon must never make `push` / `health_check`
  panic. Complements weir-core's existing proptest_envelope.rs which proves
  the decoder is panic-free at the type level.
- **Four more reference frame vectors** in
  `crates/weir-core/tests/reference_frames.rs`: `Nack(VersionMismatch)` with
  the daemon-version byte, `Nack(BadHeaderCrc)`, `HealthCheck`,
  `HealthCheckResponse`. Total reference-frame coverage: 9 byte-exact
  assertions covering every shape the wire protocol defines. Non-Rust
  client implementers can copy any of the `REFERENCE_*` constants verbatim
  into their own test suite.
- **`MySqlSink` — first IOPS-compression sink**
  (`crates/weir-server/src/sink/mysql.rs`). Writes a whole batch with one
  multi-row `INSERT` statement: N records → 1 prepared statement → 1
  server-side commit. This is the headline claim weir was extracted to
  deliver; `HttpSink` (one POST per record) was a stepping stone, not the
  destination. Features:
  - `INSERT IGNORE` by default so at-least-once retries are idempotent
    against a `UNIQUE`-constrained target table; opt out via
    `sink_mysql_insert_mode = "plain"`.
  - Connection URL read from env (`WEIR_SINK_URL`), redacted from `Debug`,
    never sourced from the TOML file. rustls-only TLS (no system OpenSSL).
  - Table/column identifiers validated to
    `[A-Za-z_][A-Za-z0-9_]{0,63}` at build time — single source of truth,
    zero SQL-injection surface via configuration.
  - Conservative transient/permanent classification (1205/1213/1290/1317
    transient; 1062 transient in `plain` mode only; everything else
    permanent and dead-lettered with the server-supplied message).
  - Per-query timeout, surfaced as its own `MySqlSinkError::Timeout`
    variant so the drain can distinguish "endpoint slow" from "endpoint
    broken".
  - 17 unit tests + an `#[ignore]`'d `mysql_sink_end_to_end` system test
    gated on `WEIR_TEST_MYSQL_URL` (docker-compose recipe in the test
    docstring).
  - Dep cost: `mysql_async 0.34` with `default-features = false +
    ["minimal-rust", "rustls-tls"]`.
- **`compression_ratio_records_per_commit` load-suite scenario**
  (`crates/weir-server/tests/load.rs`). Configures the daemon so segments
  seal under bench load, pushes 5000 records, polls Prometheus until the
  committed counter plateaus, and emits a single BENCH line with the
  literal ratio. Asserts ≥ 10× compression (observed locally: 249×).
- **`wab_segment_max_bytes` config knob**. The WAB segment rotation
  threshold is now configurable end-to-end (CLI / env / TOML); default
  remains 256 MiB. Range 4 KiB – 4 GiB. Required for the compression
  scenario to demonstrate the ratio at bench scale; useful in production
  for storage-constrained deployments and high-volume deployments that
  want to amortise the seal cost over more records.
- **Configuration reference now documents three sinks** (`noop` / `http` /
  `mysql`), with a comparison table, MySQL-specific subsection including
  the recommended schema, error-classification table, and credential
  guidance.
- **System test suite audited** (`docs/testing/test-audit.md`). Per-test
  analysis of all 41 system tests against the "what regression does this
  prevent?" question. Verdicts: KEEP 25 / STRENGTHEN 10 / DELETE 3 /
  RENAME 2 / REWRITE 1.

### Fixed

- **Three HIGH-severity correctness fixes from the post-v0.4 audit.**
  An exploratory audit of the post-v0.4 codebase surfaced three issues
  that the existing test surface didn't catch. All three landed as
  separate commits with verification, fix, unit + integration tests,
  and honest disclosure of trade-offs:

  - **Partial-write corruption (audit F1, `wab/segment.rs`).**
    `WabSegment::write_record` issued three separate `write_all` calls
    (payload_len, crc32, payload); on partial failure (most likely
    ENOSPC mid-record) the OS file offset advanced past stray bytes
    that the in-memory `bytes_written` / `file_crc_hasher` /
    `record_count` accounting never observed. Subsequent records were
    written at the wrong offset over garbage; the segment's
    `file_crc32` footer didn't cover the stray bytes; the drain reader
    stopped at the first invalid record on replay, silently dropping
    every record that came after the partial write. Fix: added
    `poisoned: bool` to `WabSegment`; chained the three `write_all`s
    so any failure trips the flag; `ShardWriter::write_record` now
    drops the active segment on error so the next write opens a fresh
    file with a new counter. The orphaned poisoned file is left on
    disk and re-read at crash recovery up to the first invalid
    record — records written successfully before the failure remain
    drainable.
  - **Unsupervised flusher panics (audit F15, `wab/mod.rs`).**
    `flusher_thread` was spawned without `catch_unwind`. A panic
    (e.g. one of the five `expect("…overflow")` calls in `segment.rs`
    triggering, however unreachable in practice) silently killed the
    thread, dropped its `Receiver<WabRecord>`, and the corresponding
    `Sender` held inside `WabHandle.shard_txs` became a sender-to-
    dead-channel — every subsequent `worker.rs` batch routed to that
    shard was silently swallowed by `.ok()`. Producers saw
    `Nack(InternalError)` for that shard forever, with no metric, no
    tracing log of the underlying cause, no signal to operators that
    one shard was wedged. Fix: new `run_with_panic_supervision`
    helper wraps every flusher in `std::panic::catch_unwind`; on
    panic it logs the payload via `tracing::error!` with the
    `shard_id` and increments a new
    `weir_wab_flusher_panics` counter metric whose help text
    explicitly tells operators "any non-zero value requires attention."
    The shard remains offline until daemon restart (respawn would
    require keeping the receiver outside the panic scope; left as a
    follow-up), but the failure is now observable.
  - **Ordering across concurrent producers on the same shard
    (audit F3, `queue.rs` / `worker.rs` / `socket/connection.rs`).**
    The single MPMC work queue let multiple workers race for items
    destined for the same shard; the workers' independent per-shard
    batch buffers could flush to the shard's flusher channel in
    arbitrary order. Per-producer ordering was always preserved
    (the request/response protocol blocks a producer until the
    previous record is acked, and the ack post-dates the WAB write),
    but cross-producer ordering on a shared shard was undefined.
    `docs/architecture.md`'s unqualified "per-shard record ordering"
    overpromised; the existing
    `per_shard_records_appear_in_submission_order` system test only
    covered single-producer. Fix: replaced the MPMC queue with N
    partitioned sub-channels (one per worker), routed by
    `shard_id % worker_count`. Each shard now lands in exactly one
    worker's partition; partition receivers, intra-shard batch
    buffers, and per-shard flusher channels are all FIFO. New system
    test `concurrent_producers_to_same_shard_preserve_per_producer_order`
    pins the property. Trade-off documented in the
    `Config::worker_count` docstring: with `worker_count >
    shard_count` (e.g. the default `shard_count=1, worker_count=2`)
    excess workers idle; operators should set
    `worker_count == shard_count` for full parallelism.

- **fsync errors are now observable** (audit MED finding,
  `wab/mod.rs::flush_batch`). The two `writer.fsync_current().is_ok()`
  callsites discarded the underlying `io::Error`, leaving operators with
  no signal when the kernel buffered a write but couldn't push it to
  stable storage ("fsyncgate" hazard). Extracted a `fsync_observed`
  helper that emits a `tracing::error!` with `shard_id` + the
  `io::Error` string, and increments a new `weir_wab_fsync_failures`
  counter whose help text explicitly tells operators non-zero values
  are alertable. Behaviour on failure is unchanged (record nacked,
  segment continues); the change is purely observability.

- **`ack_rx.await` is now bounded by `ACK_TIMEOUT` (30s)** (audit LOW
  finding, correctness F5, `socket/connection.rs`). The handler's wait
  on the WAB ack channel was unbounded. A flusher that hadn't panicked
  but was wedged (stuck on a slow fsync, lock contention, kernel I/O
  stall) would never fire its oneshot, and the connection handler
  would sit in `.await` forever — holding a semaphore permit, blocking
  new connections, with no signal to operators. Now wraps the await in
  `tokio::time::timeout`; on elapse the producer receives
  `Nack(InternalError)` and the new `weir_ack_timeout` counter
  increments. `ACK_TIMEOUT` is plumbed through `ConnectionConfig` so
  tests pass tight values (100ms) without mocking the clock. The
  daemon's default 30s shutdown timeout has enough headroom to drain
  ack-timeouts naturally.

- **Graceful shutdown drain** (audit MED finding, correctness F8,
  `socket/mod.rs`). The previous shutdown sequence waited up to
  `shutdown_timeout_secs` for join_set to drain, then
  `join_set.abort_all()`'d whatever remained. Aborted tasks were torn
  down mid-await: a task awaiting `ack_rx` had its oneshot Receiver
  dropped, so even if the flusher subsequently wrote the record, the
  producer never received an ack/nack and could not tell whether the
  push had been durably accepted. Replaced with a `tokio::sync::watch`
  broadcast: every connection handler races its `read_exact` against
  the watch in a biased select, so idle connections exit immediately
  on shutdown signal and active connections complete their current
  request (capped by `ACK_TIMEOUT`) before exiting on the next loop
  iteration. `abort_all` becomes an emergency fallback with its own
  `weir_connections_aborted_at_shutdown` counter. New
  `idle_connection_exits_promptly_on_shutdown` test pins that an idle
  connection exits in <500ms even though `read_timeout` is 30s.

- **`compute_wab_bytes_on_disk` moved to `spawn_blocking`** (audit LOW
  finding, `main.rs`). The 5-second background scanner did `read_dir`
  + `metadata` per entry synchronously on a tokio worker. With many
  shards or many segments per shard, the walk blocked the runtime in
  proportion to segment count. Now wrapped in `tokio::task::spawn_blocking`.

### Security

- **Metrics endpoint defaults to `127.0.0.1`** (audit MED finding,
  `main.rs` + `metrics/server.rs`). Previously bound to `0.0.0.0` and
  was reachable from anything that could route to the daemon's metrics
  port. On multi-tenant hosts that leaked decode-error oracles
  (`weir_records_nack{reason=...}`) and internal sizing
  (`weir_connection_idle_timeout`, `weir_max_payload_bytes`) to the
  LAN. New config knobs: `metrics_bind` (default `127.0.0.1`; set to
  `0.0.0.0` to expose intentionally) and `metrics_max_connections`
  (default 8, range 1-1024). Server holds an `Arc<Semaphore>` and
  `try_acquire_owned`s per connection; cap-exhausted scrapes drop
  immediately rather than queueing.

- **`SO_PEERCRED` check on accept** (audit MED finding,
  `socket/peer.rs` (new) + `socket/mod.rs`). Defense-in-depth on top of
  the socket file's `0o600` mode: if those bits are loosened by
  operator error, or a peer reaches the socket some other way, the
  daemon now refuses the connection unless the peer's effective uid
  matches the daemon's. New `peer_uid_check: bool` config knob
  (default `true`); new `weir_connection_rejected_peer_uid` counter.
  Platform impls in `socket/peer.rs` use Linux's `SO_PEERCRED` →
  `libc::ucred` and macOS's `getpeereid`. Operators with multi-uid
  producer setups can opt out, but the default is the secure one.

- **CRC32 algorithm doc clarifies it's integrity, not authentication**
  (`docs/wire_protocol.md`). One paragraph in the CRC32 section
  explicitly states the algorithm is keyless, anyone with socket
  access can compute a valid CRC for any payload, and the trust
  boundary is the socket file's `0o600` mode — not the CRC.

### Changed

- **Multi-shard routing now actually fans out across connections.** The
  socket-layer accept loop previously hardcoded `shard_id: 0` in every
  `WorkUnit`, so `shard_count > 1` configurations spawned the extra
  flusher threads but funnelled every record into shard 0. The accept
  loop now assigns shard IDs round-robin (`accept_counter %
  shard_count`); each connection is pinned to a single shard for its
  lifetime — no per-record routing decision on the hot path. Single-shard
  deployments are unaffected (counter `% 1` is always 0).
- **System test suite refactored from audit findings**: 3 theatre tests
  deleted (`health_check_on_separate_connection_from_push`,
  `wab_segment_rotation_creates_multiple_segments`,
  `stale_socket_removed_automatically_on_restart`); 2 renamed
  (`binary_payload_round_trips` →
  `arbitrary_binary_payload_accepted`,
  `disk_full_returns_nack_not_crash` →
  `efbig_returns_nack_not_crash`); 1 split into two strict-assertion
  tests (`metrics_consistent_across_crash_restart` →
  `metrics_internally_consistent_per_session` +
  `metrics_reset_to_zero_after_restart`). All 10 STRENGTHEN items from
  `docs/testing/test-audit.md` actioned — each test now actually
  exercises the property its name claims (e.g.
  `all_durability_tiers_behave_per_contract` reads the
  `weir_wab_fsync_duration_seconds_count` histogram to verify the
  tiers differ in fsync behaviour, not just that they all return Ok).
  Top-finding #1 (happy-path over-testing) addressed: 2 redundant
  `push().unwrap()` loops deleted
  (`multiple_sequential_pushes_same_connection`,
  `all_pushes_acked_with_multiple_shards`); 3 stress tests
  (`concurrent_producers_all_acked`,
  `sustained_load_1000_records_single_client`,
  `mixed_durability_under_concurrent_load`) gained per-tier ack-count
  assertions so a silent-drop or tier-mis-routing regression now fails
  the test instead of slipping past unwrap()s.
- **`readonly_wab_dir_prevents_startup` is now harness-portable**: spawns
  the child with `setuid(65534)` when the test runner is root so the
  `chmod 0o000` it asserts on actually bites. Was silently broken in
  rootful containers.

- **Refactor pass on `weir-server`'s connection layer.** Four contained
  cleanups, each behind a separate commit and verified against the full
  test suite:
  - **`NackReason` mapping unified** (`socket/connection.rs`). Two
    parallel match tables on `DecodeError` collapsed into one function
    returning `(WireNack, MetricNack, &'static [u8])`. Adding a new
    `DecodeError` variant now touches one match instead of three files.
  - **Server-side framing collapsed onto `Envelope::encode`**
    (`socket/connection.rs`). `send_ack` / `send_nack` / the
    HealthCheck-response arm previously hand-assembled the frame
    (3 `write_all` calls each); now use the same single-write
    encode path the client and load tests already used. One syscall
    per response instead of 2-3.
  - **`MysqlInsertMode` deduped against `sink::mysql::InsertMode`**
    (`config/mod.rs` + `main.rs`). Two parallel enums with a one-shot
    manual `match` between them collapsed into one. Config now stores
    `sink::mysql::InsertMode` directly; the bridge in `main.rs` is gone.
  - **`.confirmed` sidecar I/O extracted to `drain/confirmed.rs`**
    (`drain/mod.rs` → 1481 → 1430 LOC). The four sidecar functions
    (`write_confirmed_file`, `confirmed_path`, `read_sealed_at_nanos`,
    `confirm_and_delete`) live in a new 83-line file with module-level
    docs explaining the crash semantics. State machine, drain loop, and
    retry helper stay in `mod.rs` adjacent to the doc-comment diagram
    they're documented in.

- **Sink error types migrated to `thiserror`.** `NoopError`,
  `HttpSinkError`, `HttpSinkBuildError`, `MySqlSinkBuildError`,
  `MySqlSinkError` previously hand-rolled `Display` + `Error`. Replaced
  with `#[derive(thiserror::Error)]` + per-variant `#[error("...")]`
  attributes. Net -40 LOC across the three sink files; public API
  byte-identical (same variant names, same Display output, same
  `SinkError::is_transient` / `retry_after` impls). Added `#[from]` on
  `HttpSinkBuildError::ClientBuild(reqwest::Error)` for ergonomic `?`
  propagation. `weir-core` deliberately keeps its hand-rolled impls
  (the comment in its Cargo.toml explains the security-sensitive
  trade-off); `weir-server` already pulls syn/quote/proc-macro2
  transitively via reqwest and mysql_async, so the build-time cost in
  weir-server is zero.

- **Dead-code cleanup.** Seven `#[allow(dead_code)]` markers triaged:
  4 methods deleted (`QueueSender::is_empty`, `QueueSender::partitions`,
  `WabSegment::bytes_written`, `WabSegment::path`); 2 moved under
  `#[cfg(test)]` (`QueueSender::push`, `WabSegment::record_count`);
  1 field (`Batch::shard_id`) kept with an explanatory docstring.

- **`tests/system.rs` audit pass.** Deleted
  `wab_writes_nonzero_bytes_to_disk_after_sync_pushes` (strict subset
  of `records_written_to_wab_on_disk`). Renamed
  `metrics_all_19_families_registered` →
  `metrics_all_families_registered` and refreshed its expected list
  (5 metrics from this branch's earlier work were missing). Added a
  drift-detector that counts `# HELP weir_` lines vs the expected list
  length so a future metric added without updating the test fails
  loudly with a "did a new metric land?" message.

- **Pedantic clippy triage.** Two real lints fixed:
  `clippy::zombie_processes` (one site in `tests/system.rs` where
  `child.wait()` ran on only one branch of the loop's exit) and 5
  `clippy::manual_let_else` sites modernized to `let … else`. The
  other 300+ pedantic warnings (cast family on intentional u32→usize,
  doc_markdown on wire-format tables, missing_errors_doc) are
  documented in the commit as intentionally not addressed.

### Removed

- **`weir-bench` crate deleted; bench coverage consolidated on
  `crates/weir-server/tests/load.rs`.** The standalone binary
  duplicated the load suite (same `BENCH:` JSONL format consumed by
  `deploy/avg_benchmarks.py`); the CI bench job and the bare-metal
  script had already diverged (bare-metal used `cargo test --test load`,
  CI used the binary). CI bench job rewritten to use
  `cargo test --release --test load -- --nocapture | grep '^BENCH: '`.
  Docker smoke test uses the existing `push_simple` example from
  `weir-client/examples/`. `Dockerfile` simplified (one less crate
  to copy + stub). Net: -359 LOC from the binary + ~80 across docs/CI.

### Added (tests)

- **`recovery_replays_records_after_crash`**: closes the audit gap that
  `weir_recovery_records_replayed_total` was registered but never
  asserted by any test.
- **`enospc_returns_nack_not_crash`** (`#[ignore]`'d, requires
  `WEIR_TEST_ENOSPC_DIR` pointing at a small pre-mounted tmpfs): the
  production-shaped variant of the EFBIG test; setup recipe in the
  docstring.

### Documentation

- **Wire protocol now has an implementer guide.** `docs/wire_protocol.md`
  gains four new sections targeting non-Rust client authors: the exact
  CRC-32 variant (IEEE/ISO 3309 — *not* CRC-32C; same algorithm as zlib,
  PNG, Ethernet, Java `CRC32`, Python `zlib.crc32`); the connection
  lifecycle table (which events keep the connection open vs close it);
  socket setup conventions; byte-by-byte worked examples of a Push, an
  Ack, and a Nack; and a minimum-producer checklist. Three frame
  test-vector constants live in
  `crates/weir-core/tests/reference_frames.rs` and are asserted against
  the encoder on every `cargo test` — the doc and code can't drift
  silently. Implementers can copy the constants verbatim into a non-Rust
  test suite to confirm byte-identical encoding.

- **CHANGELOG entry for this branch.** This `[Unreleased]` section now
  records every commit on the post-v0.4 branch (~30 commits): the three
  HIGH-severity audit fixes, the seven-fix security pass, four refactor
  cleanups, `weir-testkit` extraction, `weir-bench` removal, reference
  frame coverage expansion, client proptest, `thiserror` migration, and
  the clippy triage. Sequenced by section rather than chronologically.

### Infrastructure

- **`docs/benchmarks/bare-metal.md` scaffold** plus
  **`deploy/run_bare_metal_bench.sh`** — script captures CPU / kernel /
  filesystem / device / governor / SMT / turbo / dirty-page state and
  runs the load suite 5× at each of two batch deadlines, producing a
  self-contained markdown doc on stdout. Regression policy now split
  explicitly between CI (order-of-magnitude floor) and bare-metal
  (ship gate: 10% RPS / 20% p99 / any saturation level loss).
  Documented in `docs/benchmarks/environments.md`.

---

## [0.4.0] - 2026-05-26

### Added

- **Periodic `Sink::health()` polling**. The drain now polls health on a
  wall-clock interval (30 s) in addition to the existing per-segment
  polling. Covers two gaps that left `weir_sink_health{state}` stale:
  (a) idle deployment where no segments are flowing, so health was
  never re-checked; (b) `BlockedDeadLetterFull` state where no
  segments are processed until cap clears. The drain's blocking
  `drain_rx.recv()` is now a `recv_timeout` loop so the daemon
  wakes for health checks while idle. Degraded / Down health states
  now also log the sink-supplied reason at warn / error so operators
  can see *why* the sink is unhappy without `--log-level debug`.
- **`Retry-After` header honoring on the HTTP sink**. Transient responses
  (408 / 429 / 5xx) with a `Retry-After: <seconds>` header now propagate
  the hint through the new default-implemented `SinkError::retry_after()`
  method. The drain uses the hint as the next retry delay instead of its
  exponential-backoff default, capped at 5 minutes so a misbehaving
  endpoint can't stall the drain. HTTP-date form not supported in v0 (it's
  rare in practice and adding a date parser would inflate the dep tree).
- **`Idempotency-Key: sha256:<hex>` header on the HTTP sink (default on)**.
  Lets endpoints deduplicate the records that the drain re-POSTs on a
  retry. New config option `sink_send_idempotency_key` (default true) to
  disable for endpoints that can't tolerate the extra header. Adds the
  `sha2` crate (RustCrypto, no system dependency).
- **`HttpSink` — first real sink implementation**
  (`crates/weir-server/src/sink/http.rs`). Replaces the placeholder
  `NoopSink` as the recommended production sink. POSTs each record as an
  `application/octet-stream` body to a configurable URL, with strict
  transient/permanent classification:
  - 2xx → committed
  - 4xx (except 408 and 429) → dead-lettered with a body excerpt for
    operator debugging
  - 408, 429, 5xx, connect/timeout/transport failures → transient
    (drain retries the whole segment via the existing exponential
    backoff)
  Holds a reusable `reqwest::Client` (rustls TLS — no system OpenSSL
  dependency) with keep-alive pooling. Optional bearer token via
  `WEIR_SINK_BEARER_TOKEN` env var; never sourced from config files
  and redacted from `Debug` output. New config options
  `sink_type` (`"noop"` | `"http"`, default `"noop"`), `sink_url`,
  `sink_timeout_secs` (default 10, range 1-300),
  `sink_max_batch_size` (default 100, range 1-10_000), wired through
  CLI / env / file / defaults. 12 unit tests covering happy path,
  every classification bucket, connect-refused, mixed batch outcomes,
  and transient-mid-batch retry semantics. Documented in
  `docs/operations/configuration.md` (full reference with bearer-token
  security rationale).
- **`sink::noop::NoopSink`** extracted from `main.rs` into its own
  module for symmetry with the new `sink::http::HttpSink`.
- **Hardened socket bind sequence** (`bind_hardened` in
  `crates/weir-server/src/socket/mod.rs`). Replaces the original
  `lstat → check → remove → bind → set_permissions` pattern with a
  dirfd-pinned sequence: `O_PATH | O_DIRECTORY | O_NOFOLLOW` on the
  parent, `unlinkat` for stale-socket cleanup with
  `AT_SYMLINK_NOFOLLOW`, tightened umask (0o177) so `bind(2)` itself
  creates the socket inode at mode 0o600 directly (no post-bind chmod
  to redirect), and an inode-equality check between two `fstatat`
  calls to catch a late rename swap. 9 unit tests + 1 stress test in
  `socket::tests`. Full threat model in
  [`docs/security/socket-bind.md`](docs/security/socket-bind.md).
- **Slowloris guard**: new `connection_read_timeout_secs` config
  (default 30 s, range 1–600). Wraps every `read_exact` in the
  connection handler with `tokio::time::timeout`; idle connections
  past the timeout are dropped silently and increment
  `weir_connection_idle_timeout_total`. Wired through all four config
  layers (CLI flag `--connection-read-timeout`, env
  `WEIR_CONNECTION_READ_TIMEOUT_SECS`, TOML file, defaults). 2 unit
  tests in `socket::connection::tests`.
- **Startup `umask(0o077)`** in `weir-server::main`. Defense-in-depth:
  every file-creation path in weir today specifies mode bits
  explicitly, but a tighter daemon-wide umask means any future code
  that forgets gets daemon-private permissions by default.
- **WAB segment mode audit on recovery**: `audit_segment_modes` in
  `crates/weir-server/src/wab/recovery.rs` walks each shard directory
  during `recover_open_segments` and warns on any
  `.wab`/`.wab.sealed`/`.wab.confirmed` file whose permissions are not
  `0o600`. Increments `weir_wab_unexpected_mode_total`. Does not refuse
  startup — visibility-first signal. 2 unit tests.
- **`weir_accept_latency_seconds` histogram**: time from socket accept
  to handler spawn. Visibility into queueing delay introduced by the
  `Semaphore` cap, the `tokio::spawn` task creation cost, and the
  `task::spawn_blocking` initial dispatch.
- **Proptest suite for the wire protocol parser**
  (`crates/weir-core/tests/proptest_envelope.rs`, 12 properties):
  round-trip correctness for `Header` and `Envelope`, never-panic on
  arbitrary input, specific error-path correctness (`BadMagic`,
  `VersionMismatch`, `HeaderCrcMismatch`, `PayloadCrcMismatch`,
  `PayloadTooLarge`, `TruncatedFrame`), and a DoS pre-allocation
  guard (oversize `payload_len` rejected before any payload-sized
  allocation). `proptest` added as a dev-dependency on `weir-core`.
- **Security documentation tree** under `docs/security/`:
  [`threat-model.md`](docs/security/threat-model.md) (trust boundary,
  in-scope/out-of-scope threats, operator assumptions),
  [`socket-bind.md`](docs/security/socket-bind.md) (190-line TOCTOU
  analysis), [`container.md`](docs/security/container.md) (Dockerfile
  review + hardening guidance + recommended `docker run`).
- **`SECURITY.md`** at the repo root. GitHub Security tab destination
  with reporting policy and "what counts / what does not" lists.
- **`docs/operations/configuration.md`** — canonical configuration
  reference. Every option: type, default, range, CLI flag, env var,
  TOML key, what it controls, when to tune. Includes minimal-config
  and production-config examples.
- **`docs/getting-started/install.md`** and
  **`docs/getting-started/quickstart.md`** — install paths (source,
  container, planned `cargo install`) and a 5-minute first-run
  walkthrough.
- **`docs/README.md`** — docs sitemap and doc-conventions reference.

### Changed

- **`batch_size` default changed from 1000 to 256**, and
  **`batch_deadline_ms` default changed from 100 to 1**. Backed by the
  empirical batch-tuning sweep in
  [`docs/benchmarks/batch-tuning.md`](docs/benchmarks/batch-tuning.md):
  `(256, 1ms)` is the sweet spot for both latency (4–6× p50
  improvement over the old defaults) and throughput (3–6× across every
  concurrency level measured). Also updates `WabConfig::Default` for
  consistency.
- **Metrics HTTP server bind address: `127.0.0.1` → `0.0.0.0`** to
  make the endpoint reachable from container hosts and sidecars
  without `docker exec`. The bind is no longer the access control —
  use firewall rules or port mapping.
- **Dockerfile hardening** (`deploy/docker/Dockerfile`):
  `rust:slim-bookworm` → `rust:1-slim-bookworm` (bound to Rust 1.x);
  `cargo build` gains `--locked` flag (refuses lockfile updates
  during build, supply-chain hardening); daemon UID/GID pinned to
  10001 (was unspecified — drifted across rebuilds and broke
  bind-mounted volumes); `/run/weir` and `/var/lib/weir/wab` chmodded
  to 0o700 in the image; `STOPSIGNAL SIGTERM` made explicit;
  `HEALTHCHECK` added using bash `/dev/tcp` against the metrics port
  (no extra packages installed). Full review in
  [`docs/security/container.md`](docs/security/container.md).
- **README slimmed** from 94 lines to 56 (one-paragraph pitch + status
  callout + quickstart command + documentation index by role + crates
  + non-goals + license). Detail moved to dedicated docs under
  `docs/`. Optimised for both human evaluators landing on the GitHub
  page and AI agents loading it as initial context.
- **`docs/architecture.md` metrics count** updated from 16 → 19 to
  reflect three new metric families
  (`weir_accept_latency_seconds`, `weir_connection_idle_timeout_total`,
  `weir_wab_unexpected_mode_total`).
- **CI clippy** bumped from `cargo clippy -- -D warnings` to
  `cargo clippy --all-targets -- -D warnings` so integration-test
  clippy violations fail CI instead of accumulating (six had built up
  in `tests/load.rs` and `tests/system.rs` before this change).

### Fixed

- **Shutdown deadlock when a TCP scrape connection is mid-flight**:
  the tokio runtime is now dropped before joining pipeline threads.
  Background tokio tasks (queue depth poller, metrics server) hold
  `QueueSender` clones; without the explicit runtime drop, the
  pipeline join would wait for those tasks, which were waiting on
  runtime shutdown to be triggered. Surfaces during graceful shutdown
  with an active `/metrics` scrape connection.
- **`bind_cleanup` race window** that allowed an attacker with write
  access to the socket's parent directory to swap a symlink between
  the daemon's `bind(2)` and `set_permissions` calls, leaving the
  real socket inode at its bind-time mode (typically 0o755 under
  umask 022) while the path-based chmod silently operated on an
  attacker-controlled file. Fixed by `bind_hardened` (see Added).
- **Architecture.md bind-sequence description** that still documented
  the old, vulnerable `S_ISSOCK + chmod 0o600 after bind` pattern.
- **`docs/architecture.md`** metrics table out of sync with code (was
  describing 17 metrics; code had 18, then 19).

### Security

- TOCTOU window in socket bind closed (`bind_hardened`).
- Slowloris DoS vector closed (`connection_read_timeout_secs`).
- Defense-in-depth: startup umask, WAB segment mode audit.
- Container image hardened (pinned UID, supply-chain pin guidance,
  STOPSIGNAL, HEALTHCHECK, 0o700 on data directories).

---

## [0.3.0] - 2026-05-25

### Added

- **`weir-client`**: client library implementing the wire protocol over a Unix
  socket. `WeirClient::connect(path)` returns a connected client;
  `push(payload, durability)` sends a Push frame and awaits Ack/Nack;
  `health_check()` sends a HealthCheck frame. `ClientError` variants:
  `Io`, `Protocol`, `Nack(NackReason)`, `UnknownNack(u8)`.
- **CI pipeline** (`.github/workflows/ci.yml`): four jobs — `lint` (fmt +
  clippy), `test` (full test suite), `load` (5 × 1ms + 5 × 2ms deadline runs,
  benchmark result averaged and committed to `docs/benchmarks.md` on `main`),
  and `build` (cross-compiled release binaries for five targets: `x86_64-linux`,
  `aarch64-linux`, `x86_64-macos`, `aarch64-macos`, `x86_64-windows`).
- **Release pipeline** (`.github/workflows/release.yml`): triggered on version
  tags (`v*`); builds and attaches binaries to a GitHub release.
- **Docker infrastructure** (`deploy/Dockerfile`): multi-stage build —
  `rust:1-slim-bookworm` builder, `gcr.io/distroless/cc-debian12` runtime.
  `deploy/docker-compose.yml` mounts a local WAB directory and socket path.
  `deploy/smoke-test.sh` pushes five records via `weir-client` and checks the
  health endpoint; exits non-zero on any failure.
- **Load benchmark suite** (`crates/weir-server/tests/load.rs`): 9 scenarios
  across two batch deadlines (`WEIR_BENCH_DEADLINE` env var). Each scenario
  emits `BENCH: {json}` lines consumed by `deploy/avg_benchmarks.py`.
  Scenarios: `single_thread_buffered`, `single_thread_sync`,
  `thundering_herd_{8,32,64}_threads`, `connection_churn`,
  `fire_and_forget_overload`, `latency_sync`, and saturation ramp
  (`ramp_{8,16,32,48,64,96}_threads`).
- **`deploy/avg_benchmarks.py`**: averages `BENCH:` lines from multiple CI
  runs, renders deadline-comparison throughput table, latency percentile table,
  and saturation ramp table, and writes `docs/benchmarks.md`.
- **`docs/benchmarks.md`**: CI-populated throughput and latency baseline.
  Updated automatically on every push to `main`.
- **Systems hardening test suite** (10 new tests in
  `crates/weir-server/tests/system.rs`): graceful shutdown under load,
  stalled-client isolation, partial frame injection, disk-full nacks
  (`RLIMIT_FSIZE=0`), WAB byte-level integrity after SIGKILL, socket-takeover
  WAB data safety, fd-limit exhaustion (`RLIMIT_NOFILE=128`), per-shard record
  ordering, batch deadline timer accuracy, and metrics consistency across
  crash-restart cycles. Total system test count: 41.
- **`deploy/docker/weir.toml.example`**: annotated example config with all
  fields, defaults, valid ranges, and `WEIR_*` env-var equivalents.

---

## [0.2.0] - 2026-05-24

### Added
- Config (`weir-server::config`): three-layer configuration system (CLI >
  env > TOML file > defaults). `Config::load()` reads `pico-args` CLI flags,
  `WEIR_*` env vars, and an optional TOML file (default:
  `/etc/weir/weir.toml`), merges them in precedence order, and validates all
  values. `ConfigError` — manual `impl std::error::Error` with variants
  `InvalidValue`, `ParseError`, `IoError`, `PathInvalid`. Unknown TOML keys
  produce a `warn!` log; a missing config file is not an error. Paths
  validated at load time: absolute, no `..`, no null bytes, `canonicalize()`
  re-validated.
- Main loop (`weir-server::main`): full pipeline assembly — queue → workers →
  bridge threads (one per shard, converts `Batch<WorkUnit>` → `WabRecord`) →
  WAB flushers → drain (`NoopSink` placeholder). Graceful shutdown sequence:
  `SIGTERM`/`Ctrl-C` fires shutdown signal → socket layer drains connections →
  `queue_tx` dropped → workers flush → bridges exit → WAB seals segments →
  drain confirms remaining segments. Metrics HTTP server bound before socket
  accept loop.
- `deploy/docker/weir.toml.example`: annotated example config file with all
  fields documented with their defaults, valid ranges, and env-var equivalents.

### Changed
- `WorkUnit` gains `durability: Durability` field. The socket layer now
  passes the wire-frame durability tier through to the WAB instead of
  discarding it. Bridge threads propagate it from `WorkUnit` to `WabRecord`.
- `WabRecord.ack_tx` type changed from
  `crossbeam_channel::Sender<Result<(), io::Error>>` to
  `tokio::sync::oneshot::Sender<bool>`, matching `WorkUnit.ack_tx`. Bridge
  threads convert `WorkUnit → WabRecord` directly with no channel adaptation.
  WAB `flush_batch` sends `true`/`false` instead of `Ok(())`/`Err(e)`.

### Added
- `weir-core`: wire protocol types — `Envelope`, `Header`, `MessageType`,
  `Durability`, `NackReason`, `DecodeError`, `Payload`. See
  [docs/wire_protocol.md](docs/wire_protocol.md).
- WAB subsystem (`weir-server::wab`): write-ahead buffer with per-shard
  segment files, three durability tiers, and crash recovery. See
  [docs/wab_format.md](docs/wab_format.md).
- Work queue (`weir-server::queue`): bounded MPMC channel with blocking
  backpressure (`QUEUE_CAPACITY = 65 536`) and a `push_timeout` variant
  for the socket layer.
- Worker pool (`weir-server::worker`): per-shard batching layer between
  the queue and WAB; `ack_tx` travels intact to the drain.
- Socket manager (`weir-server::socket`): Unix socket accept loop with
  `Semaphore`-based connection cap and configurable shutdown timeout.
  Frame parser enforces the decode order specified in
  [docs/wire_protocol.md](docs/wire_protocol.md).
- Sink trait (`weir-server::sink`): `Sink`, `SinkRecord`, `SinkError`,
  `CommitResult`, and `SinkHealth` — the interface between the drain and
  any downstream store. Uses native async fn in trait (AFIT, no
  `async-trait` dep). Implementations classify errors as transient
  (retried with exponential backoff) or permanent (dead-lettered).
- Drain (`weir-server::drain`): three-state machine — `Draining`,
  `RetryingTransient` (exponential backoff, up to `MAX_RETRIES = 3`),
  and `BlockedDeadLetterFull` (waits for dead-letter cap headroom before
  retrying the preserved segment). Runs on a dedicated `std::thread` with
  a single-threaded Tokio runtime for async sink calls. Writes
  `.confirmed` sidecars after successful drain; deletes WAB segments only
  after confirmation. At-least-once delivery: sinks must be idempotent.
- Dead-letter writer (`weir-server::drain::dead_letter`): permanently
  rejected records are appended to sealed WAB segments under
  `<wab_dir>/dead_letter/` (shard ID `0xFFFF`), readable by
  `SegmentReader` without a separate parser. Cap enforced via a running
  byte total; rescanned from disk on each blocked-state wake to detect
  operator-deleted files.
- Metrics (`weir-server::metrics`): 16 Prometheus metrics covering all
  subsystems, registered with a `prometheus-client` registry. Includes
  counters (`weir_records_accepted_total`, `weir_sink_commit_records_total`,
  `weir_dead_letter_full_total`, etc.), gauges (`weir_drain_state`,
  `weir_dead_letter_blocked_duration_seconds`, etc.), and histograms
  (`weir_wab_fsync_duration_seconds`, `weir_sink_commit_duration_seconds`).
- Metrics HTTP server (`weir-server::metrics::server`): minimal tokio
  TCP server exposing `GET /metrics` in OpenMetrics text format
  (`application/openmetrics-text; version=1.0.0; charset=utf-8`).
  Binds to `127.0.0.1:{metrics_port}`; accepts a `TcpListener` for
  testability with OS-assigned ports.

### Security
- Segment files created with mode `0o600`; shard and quarantine
  directories with `0o700`.
- `O_NOFOLLOW` on segment creation and on the crash-recovery write pass
  to prevent symlink TOCTOU attacks.
- Socket bind refuses to remove a non-socket file at the configured path.
- Payload length cap (`MAX_PAYLOAD_HARD_CAP = 16 MiB`) enforced before
  any heap allocation in the frame parser and during WAB replay.
- WAB and socket paths validated: must be absolute, free of `..`
  components and null bytes.

### Notes
- Adapted from HTDIP (Hardware-Tuned Data Ingestion Pipeline). The WAB
  design and crash-recovery algorithm carry over; domain coupling,
  Rails integration, and MySQL-specific drain logic do not.

---

## Wire protocol

| Version | Status  | Notes                                              |
|---------|---------|----------------------------------------------------|
| v1      | current | See [docs/wire_protocol.md](docs/wire_protocol.md) |

---

[0.3.0]: https://github.com/miki-przygoda/weir/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/miki-przygoda/weir/compare/v0.1.0...v0.2.0
