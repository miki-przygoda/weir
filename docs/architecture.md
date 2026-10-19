# weir — Architecture

## Overview

weir is a sink-agnostic write-ahead buffer daemon. Producers connect over a Unix socket — or, behind the `tls` feature, over TCP with mutual TLS — push records with an explicit durability tier, and receive an Ack after the record is durably stored. The daemon buffers records in a write-ahead buffer (WAB) on disk and forwards them to a pluggable sink.

weir is extracted from HTDIP (Hardware-Tuned Data Ingestion Pipeline), a production system built for a Rails + MySQL stack. The WAB design, crash-recovery proof, and benchmark methodology carry over; the domain coupling, Rails integration, and MySQL-specific drain logic do not.

---

## Data flow

```
Producer
  │  Unix socket, or TCP + mutual TLS (`tls` feature) — weir wire protocol v1
  ▼
Socket layer          (async tokio, src/socket/)   [Unix only — #[cfg(unix)]]
  │    src/socket/mod.rs  Unix accept loop
  │    src/socket/tcp.rs  TCP accept loop   \  both feed the same
  │    src/socket/tls.rs  mTLS handshake    /  handle_connection
  │  QueueSender::push_timeout  [crossing point: spawn_blocking]
  ▼
Work queue            (partitioned bounded channels, src/queue.rs)
  │  one crossbeam sub-channel per worker; QUEUE_CAPACITY = 65 536 split across them
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

### Startup order

Ownership first, then every refuse-to-start check that can run early, then the
WAB (`src/main.rs`):

```
umask 0o077 + SIGHUP ignored
→ Config::load → tracing (colour only on a terminal; a bad log_level WARNs)
→ "config source", "weir starting"
                       carries version, wire_version and the effective config
                       as a ConfigView: no secrets, sink URLs reduced to
                       scheme://host:port                (src/status/config_view.rs)
→ wab_dir lock         flock(LOCK_EX|LOCK_NB) on <wab_dir>/.weir.lock   (src/wab/lock.rs)
→ Metrics::new → lock-integrity thread (1 s, from here to exit)
→ preflight            socket_path format + length, non-socket refusal, connect-only
                       liveness probe (never unlinks), metrics test-bind,
                       [tls + tcp_bind: TLS load + TCP test-bind]       (src/preflight.rs)
→ queue → drain channel
→ wab::spawn           crash recovery of active segments, flushers
→ workers → drain thread (+ its SegmentLedger)
→ replay of sealed-but-unconfirmed segments into the drain channel
→ startup cap gate     synchronous WAB size scan + wab_max_bytes evaluation;
                       a failed scan with a cap set starts rejecting (src/status/poll.rs)
→ tokio runtime:
    reload loop + SIGHUP handler → metrics bind
    → WAB / drain-liveness poll → [TLS load + TCP listener] → SIGTERM/Ctrl-C
    → socket::run      bind_hardened: probe, unlink only a stale socket, bind
→ joins (workers → flushers → drain) → exit; the kernel releases the lock
```

Any refusal up to and including preflight exits 1 before recovery has touched a
segment. The lock comes before the other checks so that a duplicate on the same
`wab_dir` reports `locked by` rather than a port or socket conflict. Preflight
test-binds and releases; the real binds and the real TLS load stay late and
authoritative, so they can still fail after delivery has begun, but only on a
race (something binding in between). Two late failures remain outside
preflight: a scan error in the startup replay, and a `bind_hardened` error
other than a live socket. The producer socket still binds only after recovery
has sealed the open segments, which keeps the segment counter safe.

---

## Component responsibilities

### Config (`src/config/`)

Three-layer configuration: `CLI > env > TOML file > defaults`.

- `Config::load()` reads CLI flags (pico-args), `WEIR_*` env vars, and an optional TOML file (default `/etc/weir/weir.toml`). Merges in precedence order, then validates all values.
- Each layer produces a `PartialConfig` (all fields `Option<T>`). `Config::from_layers()` merges and applies defaults; testable without touching real CLI args or env vars.
- `validate_path(field, path)` — full four-check sequence (absolute, no `..`, no null bytes, `canonicalize()` re-validated against same checks). Used for `wab_dir`. Returns `ConfigError::PathInvalid` with field name on failure.
- `validate_path_format(field, path)` — format-only check (no `canonicalize()`). Used for `socket_path`, which does not exist until bind time.
- `ConfigError` — manual `impl std::error::Error`, no `thiserror`. Variants: `InvalidValue`, `ParseError`, `IoError`, `PathInvalid`.
- Unknown TOML keys produce a `warn!` log; a missing config file is treated as an empty layer (not an error) but logs a WARN naming the path, escalated when the resolved sink is `noop`. The startup log names the config source.
- See `deploy/docker/weir.toml.example` for all fields, defaults, valid ranges, and `WEIR_*` env-var equivalents.

### Socket layer (`src/socket/`) — Unix only

The entire socket module is gated `#[cfg(unix)]`. Unix domain sockets do not exist on Windows; `weir-core` remains cross-platform.

- Binds a Unix socket with TOCTOU-hardened bind sequence (S_ISSOCK check, then a non-blocking connect-only liveness probe, before removing a stale socket — a socket someone is listening on, or one the probe cannot classify, refuses startup instead; then a tightened umask of `0o177` so `bind(2)` creates the socket inode at mode `0o600` directly — no post-bind `chmod`, which was the exact TOCTOU window the design removed). `bind_hardened` returns a `BoundSocket` carrying the bound inode's identity, and shutdown unlinks the path only if it still names that socket. See [security/socket-bind.md](security/socket-bind.md).
- Accepts connections up to `max_connections` (Semaphore-gated; over-cap streams are dropped immediately).
- Assigns each accepted connection a `shard_id` round-robin (`accept_counter % shard_count`). Every WorkUnit pushed on that connection inherits the same shard_id, so a connection is pinned to one WAB flusher for its lifetime — no per-record routing decision on the hot path.
- `handle_connection` parses one frame at a time in a loop. Validation order is fixed and security-critical — see [wire_protocol.md](wire_protocol.md).
- **A second transport lives in the same module**: `tcp.rs` + `tls.rs` (behind the `tls` feature) mirror the Unix accept loop — same global connection-limit semaphore, same round-robin shard assignment, and the same `handle_connection` — so everything below the accept loop is transport-agnostic. Operator setup is in [operations/tcp-mtls.md](operations/tcp-mtls.md). The `#[cfg(unix)]` gate above still applies: the module as a whole is Unix-only, TCP included.
- `QueueSender::push_timeout` is used with a 5-second deadline so a dead worker pool returns `InternalError` to the client instead of holding the semaphore slot open indefinitely.
- Each accepted connection lives in a `JoinSet`; graceful shutdown drains it within `shutdown_timeout_secs` before aborting.
- SIGTERM/Ctrl-C handling and `shutdown_timeout_secs` are wired in `src/main.rs`; the socket layer receives a `CancellationToken` and drains the `JoinSet` before returning.
- SIGHUP is ignored (`SIG_IGN`) from the top of `main` and handled once the runtime exists, through the one reload path in `src/reload.rs`: a TLS reload when TLS is loaded, otherwise a logged no-op. Both count in `weir_config_reloads_total{trigger,outcome}`. SIGHUP never terminates the daemon.
- Each push checks one `AtomicBool`, `wab_ownership_lost`, before the WAB-cap check; once the lock-integrity thread has proven another daemon owns the `wab_dir`, pushes are NACKed `InternalError` and the process exits 3.

### Work queue (`src/queue.rs`)

- **Partitioned, not a single shared channel.** `queue::new::<T>(partitions)` builds a fixed set of bounded `crossbeam_channel` sub-channels, one per partition, splitting `QUEUE_CAPACITY = 65 536` across them. Production calls `queue::new::<WorkUnit>(config.worker_count)` (`main.rs`), so there is one partition per worker — which is what lets each shard be owned by exactly one worker with no cross-worker contention.
- `QueueSender` implements `Clone` so multiple socket handlers can push concurrently without shared mutable state.
- **`try_push` and `push_timeout` are the production API**, both taking a `partition_key`. `push_timeout` is used by the socket layer with a 5-second deadline so a saturated or dead worker pool returns `Nack(InternalError)` to the client instead of holding the semaphore slot open indefinitely. A bare blocking `push` exists but is `#[cfg(test)]`-only, deliberately: an indefinite stall is not a behaviour the daemon should be able to reach.
- Generic over `T`; the queue module never depends on `WorkUnit`. (There is no `Queue<T>` type — the halves are `QueueSender<T>` / `QueueReceiver<T>`.)

### Worker pool (`src/worker.rs`, `src/models.rs`)

- `WorkUnit { shard_id: u32, payload: Payload, durability: Durability, ack_tx: oneshot::Sender<bool> }` — the ack channel travels intact through the worker and batch to the WAB drain, which resolves it after the durable write. Workers do not ack.
- `Batch { shard_id: u32, records: Vec<WorkUnit> }` — per-shard buffer flushed on batch-full or `batch_deadline`.
- Flush uses `std::mem::replace` to swap in a fresh pre-allocated buffer; zero allocation on the hot path.
- `on_disconnect` is `#[cold] #[inline(never)]` to keep it off the hot path and bias the branch predictor toward the `Ok(unit)` arm.
- Startup warmup: page pre-touch faults in batch buffer backing pages; 10 000 multiply-accumulate iterations prime the FP pipeline (`_mm_mul_ps` on x86_64, `vmulq_f32` on AArch64).
- Workers pinned starting at core 2 (`WORKER_CORE_START = 2`), leaving cores 0–1 free for the OS scheduler and network interrupt handlers.
- `QOS_CLASS_USER_INTERACTIVE` on macOS (declared via `unsafe extern "C"`; not in the libc crate); `SCHED_FIFO` on Linux — both fail-open.

### WAB (`src/wab/`)

Crash-safe write-ahead buffer. See [wab_format.md](wab_format.md) for the binary format and recovery algorithm.

- One flusher thread per shard; each holds an active `WabSegment`.
- Two durability tiers. `Durable` upholds "ack ⇒ durable": it group-fdatasyncs at the batch boundary before acking every record in the batch. `Buffered` trades durability for latency — it acks after the in-memory write, before any fsync, so it does **not** uphold "ack ⇒ durable" (a Buffered ack survives a process crash but not power loss). (Historically there was a third tier: `Sync` fsynced once per record while `Batched` fsynced once per batch, until both moved onto the same batch-boundary group fsync and the distinction between them stopped existing — one fsync at the end of the batch already covered every record written during it, so a single-producer serial workload saw no difference, and under concurrent producers `Sync` amortised into the group fsync just like `Batched`. weir 2.0 removed the now-redundant tier: wire byte `0x02` is retired and permanently reserved, still decodes to `Durable` for old producers, and is never emitted by the encoder. See [wire_protocol.md → Durability tiers](wire_protocol.md#durability-tiers).)
- Segments rotate when `bytes_written >= SEGMENT_MAX_BYTES` (256 MiB). Sealed segments are forwarded to the drain channel.
- Ownership: `src/wab/lock.rs` takes an exclusive `flock` on `<wab_dir>/.weir.lock` before anything reads the WAB, keeps the fd in a process-wide static until exit (never released explicitly, never unlinked, never re-opened), and is re-checked once a second by its own thread from right after acquisition (so through recovery and the startup replay too): a deleted lock file is re-created (`O_EXCL`) and re-locked, and a replacement nobody holds is re-locked unless its record names another daemon; one another instance holds, or one carrying another daemon's record, is proof of lost ownership, and the daemon exits 3 without sealing. `wab_dir_lock = "best-effort"` only covers a filesystem that cannot lock; then orphan deletion is off. File format: [wab_format.md](wab_format.md#the-wab_dir-lock-file).
- Path validation (`validate_path`) is consolidated in `src/config/mod.rs` and used for `wab_dir` and `socket_path` at config-load time. The WAB module previously carried a local copy for belt-and-suspenders checking at flusher-spawn time; that duplicate has been removed in favour of the single source of truth.

### Sink (`src/sink/`)

- `Sink` trait uses native async fn in trait (AFIT, stable since Rust 1.75). The drain is generic `spawn<S: Sink>` to avoid `dyn Sink` object-safety issues with AFIT.
- `SinkError::is_transient()` classifies errors: transient errors trigger exponential backoff retry of the whole segment; permanent errors dead-letter the batch.
- `CommitResult` splits a batch into `committed` and `dead_lettered` records — partial success is a first-class outcome, not an error.
- `SinkBatch` carries the batch's records alongside a `DedupToken`, a content-derived idempotency handle (`sha256` over length-delimited payloads) that any sink can forward to a downstream capable of deduplicating on it.
- `SinkHealth` (`Healthy / Degraded / Down`) is surfaced via the `weir_sink_health` gauge; queried per-segment by the drain and on a 30 s wall-clock interval so the gauge stays current during idle deployment and `BlockedDeadLetterFull`. Degraded / Down states log the sink-supplied reason at `warn` / `error`.

**Built-in sinks** (`sink_type` config value):

| `sink_type` | Module | Records per `commit()` | Use |
|------------|--------|--------------------|-----|
| `noop` | `sink::noop::NoopSink` | accepts all, forwards nothing | soak-testing the daemon pipeline |
| `http` | `sink::http::HttpSink` | one HTTP POST **per record** (up to `sink_http_concurrency` in flight), or one NDJSON POST **per batch** with `sink_http_batch = "ndjson"` | endpoints that accept POST bodies; NDJSON for Loki / ES `_bulk`-style ingesters |
| `mysql` | `sink::mysql::MySqlSink` | one multi-row `INSERT` **per batch** | the IOPS-compression downstream |
| `postgres` | `sink::postgres::PostgresSink` | one multi-row `INSERT … ON CONFLICT DO NOTHING` **per batch** | the IOPS-compression downstream on Postgres |
| `clickhouse` | `sink::clickhouse::ClickHouseSink` | one HTTP `INSERT … FORMAT RowBinary` **per batch** (opt-in `clickhouse-sink` feature) | bulk inserts into ClickHouse with replay-safe dedup |

The `mysql` sink is the one that delivers on the headline claim: N
records push → 1 INSERT statement → 1 server-side commit. The
compression ratio is surfaced by the `compression_ratio_records_per_commit`
load-suite scenario and is observable in production via
`weir_sink_commit_records_total / weir_sink_commit_duration_seconds_count`.

### Drain (`src/drain/`)

Reads sealed segments via `SegmentReader`, forwards records to the sink in sub-batches respecting `Sink::max_batch_size()`, writes `.confirmed` sidecars, and deletes sealed segments after confirmation. One OS thread, with a single-threaded tokio runtime for the sink calls.

**State machine** — three explicit states, one active at a time:

```
Draining
  │  every health_poll_interval: retry outstanding dispositions (never delivers)
  │      → probe sink + rescan for stranded segments → refresh DL/quarantine gauges
  │  next path (pending first, then the channel) → SegmentLedger::admit → process
  │  Ok → write .confirmed (atomic), delete segment, stay Draining
  │  corrupt bytes → quarantine (after delivering the readable prefix)
  │  newer format / persistent access error / 2nd panic on it → park, stay Draining
  │  Err(Transient) → RetryingTransient
  │  Err(Permanent) AND dead-letter write would exceed the cap → BlockedDeadLetterFull
  ▼
RetryingTransient
  │  retries_left == 0 → STRAND at once (no final wait): not_before = now + next delay,
  │      cursor kept; Sink cause counts as a sink strand, local cause does not → Draining
  │  else wait the backoff (base × 2ⁿ, or Retry-After, capped at 300 s), retry,
  │      resuming past the records already durably processed
  │  retry succeeds → Draining
  ▼
BlockedDeadLetterFull
  │  wake every dead_letter_check_interval; rescan dead_letter/ (re-created if deleted)
  │  the blocked write now fits (would_exceed_cap(needed) is false) → RetryingTransient
  │      on the SAME segment: write the carried rejects first, then resume past them
  │  still does not fit → stay blocked
  │  still does not fit AND channel disconnected → exit without confirming (replayed on restart)
```

**The ledger** (`src/drain/ledger.rs`). Drain-owned, in-memory bookkeeping keyed by segment path, owned by the panic supervisor and lent to each drain incarnation, so it survives a respawn but not a restart:

- **One admission filter.** A path reaches the drain from the crossbeam channel and from the in-memory `pending` queue, and the channel is not enumerable, so the same path can be in both. `admit` runs on every path from either source and drops it while it is `confirm_pending`, `quarantine_pending`, parked, or inside its `not_before` cool-down. The stranded-segment rescan applies the same predicate (`excludes`) *before* its `.confirmed` check, so a ledger-owned segment is never re-queued or quarantined by the scan.
- **Resume cursor.** A transient or blocked attempt records how many records were durably processed, pinned to the file's `(dev, ino)`. The cursor survives the strand: when the rescan re-admits the segment in the same run, it resumes past the committed prefix (F05 across strands). A cursor whose `(dev, ino)` no longer matches is discarded, both at admission and at the start of every attempt (the retry and the unblock carry `processed` in their state and skip admission), so a replaced file starts at record 0; confirm, quarantine and park drop it. Nothing is persisted, so a restart replays from record 0.
- **Dispositions, retried without re-delivery.** A failed sidecar write or delete puts a delivered segment in `confirm_pending`; a failed move puts it in `quarantine_pending`. Each health poll retries only that step (`ENOENT` counts as done).
- **Dead-letter carry.** When the sink committed a sub-batch but its rejected records could not be dead-lettered (the cap blocked, or the write failed with `LocalWrite`), the rejects are carried and written first on the next attempt, which then skips the sub-batch instead of re-committing it. All carries together are capped at 64 MiB (`DL_CARRY_MAX_BYTES`); past that the segment falls back to one re-commit, with a WARN.
- **Parking.** `unsupported_version` (a header from a newer weir), `local_read_failed` (an `Access`-class read error past the limit) and `drain_panic` (two panics while the segment was in flight, `DRAIN_PANIC_PARK_LIMIT`). A parked segment stays in place, is not retried this run, and replays on the next start.

**Read errors by class.** `InvalidData` / `UnexpectedEof` are corruption: a corrupt header quarantines on the first attempt, a corrupt record quarantines after the readable prefix is delivered, a short or mismatched footer quarantines after delivery. Anything else is a local read error, a transient whose class (`ledger::classify`) decides persistence: `Process` (`EMFILE`, `ENFILE`, `ENOMEM`, `ENOBUFS`, `EAGAIN`, `EINTR`) never escalates; `Access` (`EACCES`, `EPERM`, `EISDIR`, `ENOTDIR`, `ELOOP`) parks and `Media` (everything else) quarantines once the failures reach `(max_retries + 1) × LOCAL_READ_STRAND_LIMIT` (3 strand cycles) AND the segment has been failing for `LOCAL_READ_ESCALATION_MIN_AGE` (10 minutes) since its first local read failure in the run. Both, so a device blip that burns through the cycles in a minute does not set a good segment aside; until then it keeps cycling like a `Process` error. Local strands count in `weir_drain_local_io_strands_total{cause}`, not `weir_drain_segments_stranded_total`, and do not mark the sink down.

**Liveness.** `DrainHeartbeat` is bumped at the loop head, inside the long waits, after every sink commit and every `READ_HEARTBEAT_EVERY` (1024) records read. A `BusyGuard` records which blocking operation is in flight and since when (`sink_commit`, `sink_probe`, `segment_read`, `local_fsync`, `dead_letter_write`, `quarantine_move`, `dir_scan`; exported as `weir_drain_busy_seconds{kind}`). The poll task's verdict on a frozen heartbeat: `Stopped` if `drain_state` says so; a sink call within `commit_timeout + 15 s` is a slow sink (INFO); a local operation of any duration is slow I/O (WARN, no restart advice); otherwise `Wedged`.

- **At-least-once delivery**: sinks must be idempotent. If the daemon crashes after a partial commit but before writing `.confirmed`, the full segment is replayed on restart.
- **Payload clone before commit**: payloads are cloned before `Sink::commit()` so permanent errors can dead-letter records without recovering them from the error value.
- **Segment confirmed path**: `.wab.sealed` suffix stripped, `.wab.confirmed` appended, written through a fsynced `.wab.confirmed.tmp` and a rename, then a directory fsync. An orphan (a sealed segment beside a valid sidecar) is deleted by the startup scan and the rescan when the `wab_dir` lock is held. Format and procedure: see [wab_format.md](wab_format.md#post-drain-steady-state).
- **`dead_letter_full_total`** increments once per entry into `BlockedDeadLetterFull`, not per wake cycle — tracks distinct blocking events, not polling iterations.
- **Respawn budget**: a drain panic is caught and the drain respawned, up to `MAX_DRAIN_RESPAWNS` (10) panics per process; the 10th panic stops delivery instead of respawning. Parking a segment after its second panic means one poison segment costs two panics of the budget; five distinct ones still exhaust it and stop delivery.

### Dead-letter writer (`src/drain/dead_letter.rs`)

- Permanently rejected records are written to `<wab_dir>/dead_letter/` as sealed WAB segments (shard ID `0xFFFF`), so `SegmentReader` can read them without a separate parser.
- Files named `dl_NNNNNNNN.wab.sealed`; counter increments per file, persisted across restarts by scanning on open.
- Running `total_bytes` updated after every write; `rescan()` called on each blocked-state wake to detect files deleted by the operator outside the daemon. A `dead_letter/` directory deleted outright is re-created (total reset to 0, counter kept), by `rescan()` and by a write that meets `ENOENT`. `wab_dir` is fsynced before any record is written into a (re-)created `dead_letter/`, so a dead-lettered record, whose source segment is then confirmed and deleted, never sits in a directory whose own entry a power loss could undo.
- `would_exceed_cap(additional_bytes, cap)` is checked before every write; if it would exceed, the drain enters `BlockedDeadLetterFull` instead, and leaves it only once that same write fits.

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
| `weir_recovery_quarantine_copy_failed_total` | counter | Mid-file-corrupt segments whose quarantine copy failed; recovery left them un-truncated to preserve acked-durable tail records (fail-closed) and will retry |
| `weir_wab_unexpected_mode_total` | counter | WAB segment files seen during recovery with permissions ≠ 0o600 (tampering / operator-error signal) |
| `weir_dead_letter_bytes_on_disk` | gauge | Dead-letter directory size |
| `weir_dead_letter_full_total` | counter | Distinct `BlockedDeadLetterFull` entries |
| `weir_drain_state{state}` | gauge | Drain state (exactly one label = 1) |
| `weir_dead_letter_blocked_duration_seconds` | gauge | Time in `BlockedDeadLetterFull`; alert target |
| `weir_wab_dir_lock_held` | gauge | 1 while the `wab_dir` lock is held; 0 under the `best-effort` fallback or just before an ownership-lost exit |
| `weir_wab_dir_lock_integrity_failures_total` | counter | Integrity checks that found `.weir.lock` deleted or replaced (then re-locked), or could not verify it |
| `weir_config_reloads_total{trigger,outcome}` | counter | Reload requests (`sighup`; `admin` reserved) by `ok` / `failed` / `noop` / `dropped` |
| `weir_drain_local_io_strands_total{cause}` | counter | Strands caused by local I/O (`read`, `write`), kept out of the sink-oriented stranded counter |
| `weir_drain_segments_parked{reason}` | gauge | Parked segments (`local_read_failed`, `unsupported_version`, `drain_panic`) |
| `weir_drain_segments_disposition_pending{kind}` | gauge | Delivered or set-aside segments with a `confirm` or `quarantine` step outstanding |
| `weir_drain_confirm_failures_total{stage}` | counter | Failed confirm steps (`sidecar`, `delete`) |
| `weir_drain_quarantine_failures_total` | counter | Quarantine moves that failed (retried each poll) |
| `weir_drain_dead_letter_write_failures_total` | counter | Dead-letter writes that failed after the sink committed |
| `weir_drain_busy_seconds{kind}` | gauge | Seconds the drain has spent in its current blocking operation, by kind |

`weir_drain_state` and `weir_sink_health` are pre-initialised so all label values appear on the first scrape. The HTTP exposition server binds to `metrics_bind:metrics_port` (default `127.0.0.1:9185`, localhost only) and serves `GET /metrics` in OpenMetrics text format. Set `metrics_bind = "0.0.0.0"` to allow container-host scraping; restrict exposure via firewall / network namespace, and note that `/metrics` is unauthenticated.

### Testing infrastructure

**System tests** (`crates/weir-server/tests/system.rs`, Unix-only):

Each test spawns a real `weir-server` binary via `env!("CARGO_BIN_EXE_weir-server")` with its own temp directory, socket path, WAB directory, and metrics port. A global process mutex serialises server spawning so the suite runs safely with any `--test-threads` value. Tests are written against the client library (`weir-client`) and the raw wire protocol where needed.

Coverage includes: basic push/ack round-trips, both durability tiers, multi-shard write distribution, crash recovery, graceful shutdown under load, stalled-client isolation, partial frame injection, disk-full nacks, WAB byte-level integrity after SIGKILL, a second daemon refused on a live socket or a locked `wab_dir` (and the first one's ownership-loss exit when the lock file is stolen, at runtime or during a blocked startup replay), refused duplicates delivering nothing, SIGHUP surviving at every startup phase, fd-limit exhaustion, per-shard record ordering, batch deadline timer accuracy, and metrics consistency across crash-restart cycles.

**Load tests** (`crates/weir-server/tests/load.rs`):

Throughput and latency measurements for single-threaded, thundering-herd, connection-churn, fire-and-forget overload, latency, and saturation-ramp scenarios. Results are emitted as `BENCH: {json}` lines; `deploy/avg_benchmarks.py` averages multiple runs and writes `docs/benchmarks/latest.md` and appends a row to `docs/benchmarks/history.md` (the `docs/benchmarks.md` index is hand-maintained). CI runs 5 passes at each of two batch deadlines (1ms and 2ms) and commits the averaged result on pushes to `main`.

**Deterministic simulation testing — DST** (`crates/weir-server/src/wab/dst.rs`, behind the `dst` cargo feature):

The crown invariant — *an ack is never a false ack; no acked record is ever lost* — is checked directly against the durability path under injected fault schedules. The harness swaps the real segment backend for one that injects faults on a seeded schedule, then drives the flusher through scenarios and asserts the invariant in-process.

- **Fault/scenario model.** Injected faults: `fdatasync` returns `EIO`, a short/torn write at the *n*-th record, `rename` fails, and the shutdown seal fails with `ENOSPC`. These are exercised across five scenarios per seed — a synchronous flush under an fsync-`EIO`, a torn write mid-batch, a crash between `sync_all` and the seal-rename, an `ENOSPC` at the shutdown seal, and a cooperatively-scheduled producer/flusher interleaving. Every run asserts that no acked record is lost and that acks never precede stable storage.
- **Replayable seeds.** All randomness comes from a seeded `SplitMix64`, so every run is fully reproducible from its seed. A violated invariant panics with the invariant name and a one-line `WEIR_DST_SEED=0x… cargo test … --features dst` reproducer.
- **Pinned regression seeds.** Any discovered failure serialises into `crates/weir-server/tests/dst_seeds/*.json`; the `replay_pinned_seeds` test re-runs every pinned spec on each CI run, so a fixed bug stays fixed.
- **Random-seed sweep.** `sweep_random_seeds` runs `WEIR_DST_SWEEP` seeds (default 16 for fast PR runs; CI sets `WEIR_DST_SWEEP=300`), five scenarios each, on every push. Any seed that breaks an invariant fails the build with a pin-able repro.
- **Property-based fault-list exploration.** A proptest layer varies the *fault list* itself (durability-barrier faults at arbitrary record positions) and shrinks any failure to a minimal `(seed, fault-list, records)` counterexample — a standing auto-minimising guard for the day a regression reintroduces a false ack.

---

## Security design

| Concern                                    | Mitigation                                                                                                                                                                                                                       |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pre-allocation DoS via large `payload_len` | Cap check (`min(config, MAX_PAYLOAD_HARD_CAP)`) before any allocation in both `handle_connection` and `SegmentReader`. `MAX_PAYLOAD_HARD_CAP` is defined once in `weir-core` and imported by every enforcement point.            |
| Symlink TOCTOU on segment creation         | `O_CREAT \| O_EXCL \| O_NOFOLLOW` on Unix; `create_new(true)` on Windows                                                                                                                                                         |
| Symlink TOCTOU on segment write-reopen     | `O_NOFOLLOW` on the recovery write pass                                                                                                                                                                                          |
| Stale socket file removal                  | `symlink_metadata` + S_ISSOCK check, then a connect-only liveness probe, before `remove_file`; refuses to remove a non-socket or a live socket. Shutdown removes the path only if it still names the socket this daemon bound |
| Two daemons on one `wab_dir`               | Exclusive `flock` on `<wab_dir>/.weir.lock` before recovery; contention refuses startup; proven loss of the lock while running NACKs pushes and exits 3. Not a supported guard on network filesystems |
| Path traversal in WAB/socket paths         | Absolute-path + no-`..` + no-null-byte + `canonicalize` validation                                                                                                                                                               |
| World-readable WAB files                   | Segment files: `0o600`; shard dirs: `0o700`; quarantine dir: `0o700` — set atomically at creation time via `OpenOptionsExt::mode` and `DirBuilderExt::mode`                                                                      |
| Corrupt `.confirmed` sidecar               | Bad magic / unknown version / CRC mismatch quarantines both the sealed segment and sidecar                                                                                                                                       |
| Torn write in crash recovery               | Per-record CRC32 checked; replay truncates at the first corrupt entry                                                                                                                                                            |
| Blocked socket semaphore (dead workers)    | `push_timeout` (5 s) returns `InternalError` Nack rather than blocking indefinitely                                                                                                                                              |
| WAB integrity on shared/network storage    | **Out of scope.** CRC32 detects accidental corruption; it does not detect malicious modification. The WAB directory must be local storage accessible only to the daemon (`0o700`). Network filesystems break the security model. |

---

## Workspace & crate boundaries

weir ships as several crates rather than one, and the split is deliberate: each
piece is usable **without** pulling in the daemon, so you can compose the parts
you want instead of taking the whole thing. The boundaries are enforced by the
dependency graph, not just convention — verify with `cargo tree`.

| Crate | Depends on (normal) | Standalone use without the daemon |
|-------|---------------------|-----------------------------------|
| `weir-core` | — (just `bytes`, `crc32fast`) | The wire types + `Payload`. Build a producer or a non-Rust client against the frozen v1 wire protocol. |
| `weir-wab` | `weir-core`, `crc32fast` | Read/inspect/recover on-disk WAB segments. No async runtime, no daemon. `SegmentReader::open(path)` streams CRC-verified records straight off disk — useful for a recovery tool, an offline inspector, or a custom reader. |
| `weir-status` | — (just `serde`) | The `weir.status/v1` model, Conditions evaluator and probe predicates shared by the daemon and `weir-ctl`; pure Rust, no I/O. Evaluate a daemon from its metrics exposition or from disk facts without linking the daemon. |
| `weir-sink-sdk` | `weir-core` | Implement **and unit-test** a custom `Sink` against the stable trait — no `weir-server`, no tokio required (the crate's own tests drive `commit`/`health` with a tiny `block_on`). `BasicSinkError` / `Infallible` cover quick error types. |
| `weir-client` | `weir-core` | The synchronous producer client. Push records over the socket from your own app. |
| `weir-ctl` | `weir-core`, `weir-client`, `weir-wab` | The admin CLI. Notably does **not** depend on `weir-server` — it's a thin tool over the wire client + the segment reader, so it (and the WAB-reading capability it's built on) work independently of the daemon binary. |
| `weir-server` | the above + tokio/sink stacks | The daemon itself. |

**What this enables.** You can build your own system on the pieces — e.g. read
the WAB with `weir-wab`, run operations with `weir-ctl`, and forward to your own
destination by implementing a `Sink` with `weir-sink-sdk` — and only depend on
the daemon if you actually run it. (Today, *running* a custom sink inside the
shipped daemon still means compiling it into the sink-selection path; the SDK's
present, delivered value is authoring + testing a sink against a frozen contract.
A first-class plugin/entry-point is a candidate for a future minor release.)

The cost of the split is real but small — more `Cargo.toml`s and a published
surface to keep stable — and it buys genuine composability, not speculative
modularity: the boundaries have present consumers (`weir-ctl` uses `weir-wab`;
the built-in sinks implement `weir-sink-sdk`).

## Design notes

A couple of deliberate choices a contributor should know:

- **Config knobs are operability levers, not speculation.** The configuration
  surface is broad, but each knob is a tuning or safety dial with a
  production-ready default — you rarely set them, but they're there for the
  incident where the default is wrong for *your* deployment and a recompile isn't
  an option. Defaults are defined once as named constants (e.g.
  `drain::MAX_RETRIES`, `drain::HEALTH_POLL_INTERVAL`) and the knobs fall back to
  them, so the default and the tested behaviour can't drift. Adding a knob is
  SemVer-additive; removing one is breaking — so the bar to *add* is "a real
  deployment would plausibly need to change this," not "someone might." See the
  [Configuration reference](operations/configuration.md).
- **`Sink::Record` / `SinkRecord` was removed in 2.0.** The drain used to be
  generic over a record type, but the only implementation was ever the identity
  on `Payload` and every built-in sink used `type Record = Payload`, so the
  conversion was a no-op that allocated and cloned once per batch. Being part of
  the frozen 1.0 `Sink` trait, it could only go at a major. Records are now
  `Payload` throughout.
