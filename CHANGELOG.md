# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. Wire protocol version
changes are tracked separately under **Wire protocol** below.

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
