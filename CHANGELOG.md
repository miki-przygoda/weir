# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. Wire protocol version
changes are tracked separately under **Wire protocol** below.

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
