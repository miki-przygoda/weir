# weir — Architecture

## Overview

weir is a sink-agnostic write-ahead buffer daemon. Producers connect over a Unix socket, push records with an explicit durability tier, and receive an Ack after the record is durably stored. The daemon buffers records in a write-ahead buffer (WAB) on disk and forwards them to a pluggable sink.

weir is extracted from HTDIP (Hardware-Tuned Data Ingestion Pipeline), a production system built for a Rails + MySQL stack. The WAB design, crash-recovery proof, and benchmark methodology carry over; the domain coupling, Rails integration, and MySQL-specific drain logic do not.

---

## Data flow

```
Producer
  │  Unix socket (weir wire protocol v1)
  ▼
Socket layer          (async tokio, src/socket/)   [Unix only — #[cfg(unix)]]
  │  QueueSender::push_timeout  [crossing point: spawn_blocking]
  ▼
Work queue            (bounded MPMC, src/queue.rs)
  │  crossbeam_channel, QUEUE_CAPACITY = 65 536
  ▼
Worker pool           (std::thread, src/worker.rs)
  │  per-shard Batch channel (crossbeam Sender<Batch>) — direct, no bridge hop
  ▼
WAB flusher threads   (std::thread, src/wab/)
  │  per-shard segment files
  ▼
Drain channel         (crossbeam_channel<PathBuf>)  [sealed segment paths]
  ▼
Drain                 (std::thread, src/drain/)
  │  reads segments via SegmentReader
  ▼
Sink                  (async, single-threaded tokio runtime in drain thread)
  │  CommitResult: committed + dead_lettered
  ▼
Dead-letter writer    (src/drain/dead_letter.rs)   [on permanent rejection]
```

### Runtime boundary

The socket layer is async (tokio). Everything downstream runs on `std::thread` with blocking I/O. The async/sync crossing is the enqueue in `handle_connection`: the common path is a lock-free `queue_tx.try_push(...)` that returns immediately without leaving the async task. Only when the shard partition is full does it fall back to `task::spawn_blocking`, which moves the blocking `push_timeout` call (with the queue-push timeout) onto tokio's blocking thread pool so the async worker is never parked on a full queue.

Under sustained load, if every active connection is simultaneously blocked in that `spawn_blocking` fallback waiting for a queue slot, all blocking threads fill up and new connections stall at the socket layer. `max_connections` must therefore be ≤ the tokio blocking thread pool limit (default: 512).

---

## Component responsibilities

### Config (`src/config/`)

Three-layer configuration: `CLI > env > TOML file > defaults`.

- `Config::load()` reads CLI flags (pico-args), `WEIR_*` env vars, and an optional TOML file (default `/etc/weir/weir.toml`). Merges in precedence order, then validates all values.
- Each layer produces a `PartialConfig` (all fields `Option<T>`). `Config::from_layers()` merges and applies defaults; testable without touching real CLI args or env vars.
- `validate_path(field, path)` — full four-check sequence (absolute, no `..`, no null bytes, `canonicalize()` re-validated against same checks). Used for `wab_dir`. Returns `ConfigError::PathInvalid` with field name on failure.
- `validate_path_format(field, path)` — format-only check (no `canonicalize()`). Used for `socket_path`, which does not exist until bind time.
- `ConfigError` — manual `impl std::error::Error`, no `thiserror`. Variants: `InvalidValue`, `ParseError`, `IoError`, `PathInvalid`.
- Unknown TOML keys produce a `warn!` log; a missing config file is treated as an empty layer.
- See `deploy/docker/weir.toml.example` for all fields, defaults, valid ranges, and `WEIR_*` env-var equivalents.

### Socket layer (`src/socket/`) — Unix only

The entire socket module is gated `#[cfg(unix)]`. Unix domain sockets do not exist on Windows; `weir-core` remains cross-platform.

- Binds a Unix socket with TOCTOU-hardened bind sequence (S_ISSOCK check before removing stale socket, `chmod 0o600` after bind).
- Accepts connections up to `max_connections` (Semaphore-gated; over-cap streams are dropped immediately).
- Assigns each accepted connection a `shard_id` round-robin (`accept_counter % shard_count`). Every WorkUnit pushed on that connection inherits the same shard_id, so a connection is pinned to one WAB flusher for its lifetime — no per-record routing decision on the hot path.
- `handle_connection` parses one frame at a time in a loop. Validation order is fixed and security-critical — see [wire_protocol.md](wire_protocol.md).
- `QueueSender::push_timeout` is used with a 5-second deadline so a dead worker pool returns `InternalError` to the client instead of holding the semaphore slot open indefinitely.
- Each accepted connection lives in a `JoinSet`; graceful shutdown drains it within `shutdown_timeout_secs` before aborting.
- SIGTERM/Ctrl-C handling and `shutdown_timeout_secs` are wired in `src/main.rs`; the socket layer receives a `CancellationToken` and drains the `JoinSet` before returning.

### Work queue (`src/queue.rs`)

- Bounded MPMC `crossbeam_channel` with `QUEUE_CAPACITY = 65 536` slots.
- `QueueSender` implements `Clone` so multiple socket handlers can push concurrently without shared mutable state.
- `QueueSender::push` blocks the calling thread (intentional backpressure — stalls the socket handler rather than dropping records or allocating unboundedly).
- `QueueSender::push_timeout` is used by the socket layer with a 5-second deadline so a dead worker pool returns `InternalError` to the client instead of holding the semaphore slot open indefinitely.
- Generic over `T`; the socket layer instantiates `Queue<WorkUnit>` without the queue module depending on `WorkUnit`.

### Worker pool (`src/worker.rs`, `src/models.rs`)

- `WorkUnit { shard_id: u32, payload: Payload, ack_tx: oneshot::Sender<bool> }` — the ack channel travels intact through the worker and batch to the WAB drain, which resolves it after the durable write. Workers do not ack.
- `Batch { shard_id: u32, records: Vec<WorkUnit> }` — per-shard buffer flushed on batch-full or `batch_deadline`.
- Flush uses `std::mem::replace` to swap in a fresh pre-allocated buffer; zero allocation on the hot path.
- `on_disconnect` is `#[cold] #[inline(never)]` to keep it off the hot path and bias the branch predictor toward the `Ok(unit)` arm.
- Startup warmup: page pre-touch faults in batch buffer backing pages; 10 000 multiply-accumulate iterations prime the FP pipeline (`_mm_mul_ps` on x86_64, `vmulq_f32` on AArch64).
- Workers pinned starting at core 2 (`WORKER_CORE_START = 2`), leaving cores 0–1 free for the OS scheduler and network interrupt handlers.
- `QOS_CLASS_USER_INTERACTIVE` on macOS (declared via `unsafe extern "C"`; not in the libc crate); `SCHED_FIFO` on Linux — both fail-open.

### WAB (`src/wab/`)

Crash-safe write-ahead buffer. See [wab_format.md](wab_format.md) for the binary format and recovery algorithm.

- One flusher thread per shard; each holds an active `WabSegment`.
- Three durability tiers, all upholding "ack ⇒ durable": `Sync` and `Batched` both group-fdatasync at the batch boundary before acking every record in the batch; `Buffered` acks after the in-memory write with no fsync. (The historical distinction was "Sync = one fdatasync per record, Batched = one fdatasync per batch" — both now share the batch-boundary fsync, since one fsync at the end of the batch covers every record written during it. Single-producer serial workloads see no difference because the batch holds one record anyway; under concurrent producers, Sync amortises into the group fsync just like Batched.)
- Segments rotate when `bytes_written >= SEGMENT_MAX_BYTES` (256 MiB). Sealed segments are forwarded to the drain channel.
- Path validation (`validate_path`) is consolidated in `src/config/mod.rs` and used for `wab_dir` and `socket_path` at config-load time. The WAB module previously carried a local copy for belt-and-suspenders checking at flusher-spawn time; that duplicate has been removed in favour of the single source of truth.

### Sink (`src/sink/`)

- `Sink` trait uses native async fn in trait (AFIT, stable since Rust 1.75). The drain is generic `spawn<S: Sink>` to avoid `dyn Sink` object-safety issues with AFIT.
- `SinkError::is_transient()` classifies errors: transient errors trigger exponential backoff retry of the whole segment; permanent errors dead-letter the batch.
- `CommitResult<R>` splits a batch into `committed` and `dead_lettered` records — partial success is a first-class outcome, not an error.
- `SinkRecord` trait decouples the drain's `Payload` bytes from the sink's own record type; pass-through implementation (`impl SinkRecord for Payload`) is provided.
- `SinkHealth` (`Healthy / Degraded / Down`) is surfaced via the `weir_sink_health` gauge; queried per-segment by the drain and on a 30 s wall-clock interval so the gauge stays current during idle deployment and `BlockedDeadLetterFull`. Degraded / Down states log the sink-supplied reason at `warn` / `error`.

**Built-in sinks** (`sink_type` config value):

| `sink_type` | Module | Records per `commit()` | Use |
|------------|--------|--------------------|-----|
| `noop` | `sink::noop::NoopSink` | accepts all, forwards nothing | soak-testing the daemon pipeline |
| `http` | `sink::http::HttpSink` | one HTTP POST **per record**, up to `sink_http_concurrency` in flight | endpoints that already accept POST bodies |
| `mysql` | `sink::mysql::MySqlSink` | one multi-row `INSERT` **per batch** | the IOPS-compression downstream |

The `mysql` sink is the one that delivers on the headline claim: N
records push → 1 INSERT statement → 1 server-side commit. The
compression ratio is surfaced by the `compression_ratio_records_per_commit`
load-suite scenario and is observable in production via
`weir_sink_commit_records_total / weir_sink_commit_duration_seconds_count`.

### Drain (`src/drain/`)

Reads sealed segments via `SegmentReader`, forwards records to the sink in sub-batches respecting `Sink::max_batch_size()`, writes `.confirmed` sidecars, and deletes sealed segments after confirmation.

**State machine** — three explicit states, one active at a time:

```
Draining
  │  Err(Transient) → RetryingTransient
  │  Err(Permanent) AND dead-letter cap exceeded → BlockedDeadLetterFull
  │  Ok → write .confirmed, delete segment, stay Draining
  ▼
RetryingTransient
  │  retry succeeds → Draining
  │  MAX_RETRIES (3) exhausted → leave segment on disk, log, Draining
  │  (exponential backoff: base × 2ⁿ per attempt)
  ▼
BlockedDeadLetterFull
  │  wake every dead_letter_check_interval; rescan dead_letter/ dir
  │  bytes < cap → unblock; retry the SAME segment via RetryingTransient,
  │      RESUMING past the sub-batches already durably processed (F05)
  │  bytes ≥ cap → stay blocked
  │  bytes ≥ cap AND channel disconnect → exit without confirming (crash recovery replays)
```

- **At-least-once delivery**: sinks must be idempotent. If the daemon crashes after a partial commit but before writing `.confirmed`, the full segment is replayed on restart.
- **Payload clone before commit**: payloads are cloned before `Sink::commit()` so permanent errors can dead-letter records without recovering them from the error value.
- **Segment confirmed path**: `.wab.sealed` suffix stripped, `.wab.confirmed` appended. Confirmed sidecar format: see [wab_format.md](wab_format.md).
- **`dead_letter_full_total`** increments once per entry into `BlockedDeadLetterFull`, not per wake cycle — tracks distinct blocking events, not polling iterations.

### Dead-letter writer (`src/drain/dead_letter.rs`)

- Permanently rejected records are written to `<wab_dir>/dead_letter/` as sealed WAB segments (shard ID `0xFFFF`), so `SegmentReader` can read them without a separate parser.
- Files named `dl_NNNNNNNN.wab.sealed`; counter increments per file, persisted across restarts by scanning on open.
- Running `total_bytes` updated after every write; `rescan()` called on each blocked-state wake to detect files deleted by the operator outside the daemon.
- `would_exceed_cap(additional_bytes, cap)` is checked before every write; if it would exceed, the drain enters `BlockedDeadLetterFull` instead.

### Metrics (`src/metrics/`)

A family of Prometheus metrics registered with a `prometheus-client` registry (the full catalogue is in [monitoring.md](monitoring.md)). `Metrics::new()` returns `(Metrics, Registry)` — the metrics struct goes to subsystems; the registry goes to the HTTP server.

| Metric | Type | Description |
|--------|------|-------------|
| `weir_records_accepted_total{tier}` | counter | Records accepted from producers |
| `weir_records_ack_total{tier}` | counter | Records acknowledged to producers |
| `weir_records_nack_total{tier,reason}` | counter | Records rejected (Nack) |
| `weir_accept_latency_seconds` | histogram | Time from socket accept to handler spawn |
| `weir_connection_idle_timeout_total` | counter | Connections dropped after exceeding `connection_read_timeout_secs` mid-read (slowloris guard) |
| `weir_wab_segments_total{state}` | counter | WAB segment transitions |
| `weir_wab_bytes_on_disk` | gauge | WAB directory size |
| `weir_wab_fsync_duration_seconds` | histogram | fdatasync latency |
| `weir_sink_commit_duration_seconds` | histogram | `Sink::commit` latency |
| `weir_sink_commit_records_total{outcome}` | counter | Records by drain outcome |
| `weir_sink_health{state}` | gauge | Sink health (1 = active state) |
| `weir_queue_depth` | gauge | Work queue occupancy |
| `weir_recovery_records_replayed_total` | counter | Records replayed on startup |
| `weir_recovery_segments_quarantined_total` | counter | Segments quarantined on startup |
| `weir_wab_unexpected_mode_total` | counter | WAB segment files seen during recovery with permissions ≠ 0o600 (tampering / operator-error signal) |
| `weir_dead_letter_bytes_on_disk` | gauge | Dead-letter directory size |
| `weir_dead_letter_full_total` | counter | Distinct `BlockedDeadLetterFull` entries |
| `weir_drain_state{state}` | gauge | Drain state (exactly one label = 1) |
| `weir_dead_letter_blocked_duration_seconds` | gauge | Time in `BlockedDeadLetterFull`; alert target |

`weir_drain_state` and `weir_sink_health` are pre-initialised so all label values appear on the first scrape. The HTTP exposition server binds to `metrics_bind:metrics_port` (default `127.0.0.1:9185`, localhost only) and serves `GET /metrics` in OpenMetrics text format. Set `metrics_bind = "0.0.0.0"` to allow container-host scraping; restrict exposure via firewall / network namespace, and note that `/metrics` is unauthenticated.

### Testing infrastructure

**System tests** (`crates/weir-server/tests/system.rs`, Unix-only):

Each test spawns a real `weir-server` binary via `env!("CARGO_BIN_EXE_weir-server")` with its own temp directory, socket path, WAB directory, and metrics port. A global process mutex serialises server spawning so the suite runs safely with any `--test-threads` value. Tests are written against the client library (`weir-client`) and the raw wire protocol where needed.

Coverage includes: basic push/ack round-trips, all three durability tiers, multi-shard write distribution, crash recovery, graceful shutdown under load, stalled-client isolation, partial frame injection, disk-full nacks, WAB byte-level integrity after SIGKILL, socket takeover data safety, fd-limit exhaustion, per-shard record ordering, batch deadline timer accuracy, and metrics consistency across crash-restart cycles.

**Load tests** (`crates/weir-server/tests/load.rs`):

Throughput and latency measurements for single-threaded, thundering-herd, connection-churn, fire-and-forget overload, latency, and saturation-ramp scenarios. Results are emitted as `BENCH: {json}` lines; `deploy/avg_benchmarks.py` averages multiple runs and writes `docs/benchmarks/latest.md` and appends a row to `docs/benchmarks/history.md` (the `docs/benchmarks.md` index is hand-maintained). CI runs 5 passes at each of two batch deadlines (1ms and 2ms) and commits the averaged result on pushes to `main`.

---

## Security design

| Concern                                    | Mitigation                                                                                                                                                                                                                       |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pre-allocation DoS via large `payload_len` | Cap check (`min(config, MAX_PAYLOAD_HARD_CAP)`) before any allocation in both `handle_connection` and `SegmentReader`. `MAX_PAYLOAD_HARD_CAP` is defined once in `weir-core` and imported by every enforcement point.            |
| Symlink TOCTOU on segment creation         | `O_CREAT \| O_EXCL \| O_NOFOLLOW` on Unix; `create_new(true)` on Windows                                                                                                                                                         |
| Symlink TOCTOU on segment write-reopen     | `O_NOFOLLOW` on the recovery write pass                                                                                                                                                                                          |
| Stale socket file removal                  | `symlink_metadata` + S_ISSOCK check before `remove_file`; refuses to remove a non-socket                                                                                                                                         |
| Path traversal in WAB/socket paths         | Absolute-path + no-`..` + no-null-byte + `canonicalize` validation                                                                                                                                                               |
| World-readable WAB files                   | Segment files: `0o600`; shard dirs: `0o700`; quarantine dir: `0o700` — set atomically at creation time via `OpenOptionsExt::mode` and `DirBuilderExt::mode`                                                                      |
| Corrupt `.confirmed` sidecar               | Bad magic / unknown version / CRC mismatch quarantines both the sealed segment and sidecar                                                                                                                                       |
| Torn write in crash recovery               | Per-record CRC32 checked; replay truncates at the first corrupt entry                                                                                                                                                            |
| Blocked socket semaphore (dead workers)    | `push_timeout` (5 s) returns `InternalError` Nack rather than blocking indefinitely                                                                                                                                              |
| WAB integrity on shared/network storage    | **Out of scope.** CRC32 detects accidental corruption; it does not detect malicious modification. The WAB directory must be local storage accessible only to the daemon (`0o700`). Network filesystems break the security model. |
