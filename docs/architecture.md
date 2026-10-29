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
health surface, and only then the WAB (`src/main.rs`; Foundations §6). Each
step publishes a phase in `weir_startup_phase{phase}`:

```
umask 0o077 + SIGHUP ignored
→ Config::load → tracing (colour only on a terminal; a bad log_level WARNs)
→ "config source", "weir starting"
                       carries version, wire_version and the effective config
                       as a ConfigView: no secrets, sink URLs reduced to
                       scheme://host:port                (src/status/config_view.rs)
→ wab_dir lock         flock(LOCK_EX|LOCK_NB) on <wab_dir>/.weir.lock   (src/wab/lock.rs)
→ Metrics::new → lock-integrity thread (1 s, from here to exit)
→ initializing         StatusBoard                        (src/status/board.rs)
→ preflight            socket_path format + length, non-socket refusal, connect-only
                       liveness probe (never unlinks), metrics test-bind,
                       [tls + tcp_bind: TLS load + TCP test-bind]       (src/preflight.rs)
→ queue, drain channel, DrainControl (attached), Lifecycle   (src/lifecycle.rs)
→ tokio runtime        reload loop + SIGHUP handler, SIGTERM/SIGINT task,
                       metrics bind (answers from here), [TLS load], [admin socket]
→ recovering           wab::recover on the main thread (HTTP keeps answering)
→ handoff              wab::scan_replay (in memory, no sends), first WAB size +
                       wab_max_bytes scan; a failed scan with a cap set starts
                       rejecting                          (src/status/poll.rs)
→ pipeline             flushers, workers, drain (replay list = its backlog), poll tasks,
                       wait ≤ 10 s for every shard and worker to settle; routing
                       skips a shard that has not settled
→ binding              socket::bind_all: TCP, then the Unix socket (bind_hardened:
                       probe, unlink only a stale socket, bind); all listeners
                       before any accept loop
→ serving              accept loops start; "weir serving"
→ shutting_down        on SIGTERM/SIGINT; see "Shutdown order"
```

Any refusal up to and including preflight exits 1 before recovery has touched a
segment. The lock comes before the other checks so that a duplicate on the same
`wab_dir` reports `locked by` rather than a port or socket conflict. Preflight
test-binds and releases; the real binds stay late and authoritative, so they
can still fail, but only on a race (something binding in between). Two late
failures remain outside preflight: a scan error in `wab::scan_replay` and a
`bind_all` error other than a live socket; both exit before any producer is
served. A slow or unreachable sink no longer delays the producer socket: the
replay backlog is handed to the drain in memory and alternates with live
segments (they share no connection, so per-connection order is unchanged). The
producer socket still binds only after recovery has sealed the open segments,
which keeps the segment counter safe. A signal before `serving` lets the
current step finish (recovery is never interrupted) and skips the producer
listeners.

### Shutdown order

```
T0 first SIGTERM/SIGINT   phase shutting_down; drain absorb mode; flushers' flag;
                          lame duck: socket unlinked (inode-checked), TCP listener closed
   connection drain       ≤ shutdown_timeout_secs (aborted at once on a second signal)
T1 ingest closed          queue-depth task stopped; workers joined; flushers joined
                          (each seals, fsyncs, hands off; ≤ 1 s if the drain is stuck)
T2 drain budget           drain_shutdown_budget_secs, then the in-flight segment is
                          abandoned (no .confirmed); main waits ≤ budget + 2 s
T3 exit                   one summary line (no I/O), runtime stops, admin socket
                          unlinked (inode-checked), exit 0
second signal (any time)  forced: connection drains abort, the drain deadline is now;
                          the worker and flusher joins still run
```

`/metrics` answers until T3. If the drain is inside a sink call that never
yields, the daemon exits 0 at budget + 2 s with the segment unconfirmed; it
replays on the next start. The kernel releases the `wab_dir` lock at exit,
after everything above.

---

## Component responsibilities

### Config (`src/config/`)

Three-layer configuration: `CLI > env > TOML file > defaults`.

- `Config::load()` reads CLI flags (pico-args), `WEIR_*` env vars, and an optional TOML file (default `/etc/weir/weir.toml`). Merges in precedence order, then validates all values.
- Each layer produces a `PartialConfig` (all fields `Option<T>`). `Config::from_layers()` merges and applies defaults; testable without touching real CLI args or env vars.
- `validate_path(field, path)` — full four-check sequence (absolute, no `..`, no null bytes, `canonicalize()` re-validated against same checks). Used for `wab_dir`. Returns `ConfigError::PathInvalid` with field name on failure.
- `validate_path_format(field, path)` — format-only check (no `canonicalize()`). Used for `socket_path` and `admin_socket_path`, which do not exist until bind time.
- `ConfigError` — manual `impl std::error::Error`, no `thiserror`. Variants: `InvalidValue`, `ParseError`, `IoError`, `PathInvalid`.
- Unknown TOML keys produce a `warn!` log; a missing config file is treated as an empty layer (not an error) but logs a WARN naming the path, escalated when the resolved sink is `noop`. The startup log names the config source.
- See `deploy/docker/weir.toml.example` for all fields, defaults, valid ranges, and `WEIR_*` env-var equivalents.

### Socket layer (`src/socket/`) — Unix only

The entire socket module is gated `#[cfg(unix)]`. Unix domain sockets do not exist on Windows; `weir-core` remains cross-platform.

- Binds a Unix socket with TOCTOU-hardened bind sequence (S_ISSOCK check, then a non-blocking connect-only liveness probe, before removing a stale socket — a socket someone is listening on, or one the probe cannot classify, refuses startup instead; then a tightened umask of `0o177` so `bind(2)` creates the socket inode at mode `0o600` directly — no post-bind `chmod`, which was the exact TOCTOU window the design removed). `bind_hardened` returns a `BoundSocket` carrying the bound inode's identity, and shutdown unlinks the path only if it still names that socket. See [security/socket-bind.md](security/socket-bind.md).
- Accepts connections up to `max_connections` (Semaphore-gated; over-cap streams are dropped immediately).
- Assigns each accepted connection a `shard_id` round-robin over the shards that can take ingest (`src/socket/route.rs`: the `(accept_counter mod k)`-th of the `k` ingest-capable shards in the shard registry, so a shard whose flusher or worker is down is skipped; with none, the connection is refused). Every WorkUnit pushed on that connection inherits the same shard_id, so a connection is pinned to one WAB flusher for its lifetime — no per-record routing decision on the hot path.
- `handle_connection` parses one frame at a time in a loop. Validation order is fixed and security-critical — see [wire_protocol.md](wire_protocol.md).
- **A second transport lives in the same module**: `tcp.rs` + `tls.rs` (behind the `tls` feature) mirror the Unix accept loop — same global connection-limit semaphore, same round-robin shard assignment, and the same `handle_connection` — so everything below the accept loop is transport-agnostic. Operator setup is in [operations/tcp-mtls.md](operations/tcp-mtls.md). The `#[cfg(unix)]` gate above still applies: the module as a whole is Unix-only, TCP included.
- `QueueSender::push_timeout` is used with a 5-second deadline so a dead worker pool returns `InternalError` to the client instead of holding the semaphore slot open indefinitely.
- Each accepted connection lives in a `JoinSet`; graceful shutdown drains it within `shutdown_timeout_secs` before aborting.
- SIGTERM/Ctrl-C handling and `shutdown_timeout_secs` are wired in `src/main.rs`; the socket layer follows a `watch<ShutdownLevel>` from `src/lifecycle.rs`: on the first signal it unlinks its socket and closes the listener at once, then drains the `JoinSet` for at most `shutdown_timeout_secs`, and aborts it on a second signal.
- SIGHUP is ignored (`SIG_IGN`) from the top of `main` and handled once the runtime exists (right after the lock and preflight, before WAB recovery), through the one reload path in `src/reload.rs` (the admin socket's `reload` verb feeds the same bounded queue): a TLS reload when TLS is loaded, otherwise a logged no-op. Both count in `weir_config_reloads_total{trigger,outcome}`. SIGHUP never terminates the daemon.
- Each push checks one `AtomicBool`, `wab_ownership_lost`, before the WAB-cap check; once the lock-integrity thread has proven another daemon owns the `wab_dir`, pushes are NACKed `InternalError` and the process exits 3.

### Work queue (`src/queue.rs`)

- **Partitioned, not a single shared channel.** `queue::new::<T>(partitions)` builds a fixed set of bounded `crossbeam_channel` sub-channels, one per partition, splitting `QUEUE_CAPACITY = 65 536` across them. Production calls `queue::new::<WorkUnit>(config.worker_count)` (`main.rs`), so there is one partition per worker — which is what lets each shard be owned by exactly one worker with no cross-worker contention.
- `QueueSender` implements `Clone` so multiple socket handlers can push concurrently without shared mutable state.
- **`try_push` and `push_timeout` are the production API**, both taking a `partition_key`. `push_timeout` is used by the socket layer with a 5-second deadline so a saturated or dead worker pool returns `Nack(InternalError)` to the client instead of holding the semaphore slot open indefinitely. A bare blocking `push` exists but is `#[cfg(test)]`-only, deliberately: an indefinite stall is not a behaviour the daemon should be able to reach.
- Generic over `T`; the queue module never depends on `WorkUnit`. (There is no `Queue<T>` type — the halves are `QueueSender<T>` / `QueueReceiver<T>`.)

### Worker pool (`src/worker.rs`, `src/models.rs`)

- `WorkUnit { shard_id: u32, payload: Payload, durability: Durability, wants_coordinate: bool, ack_tx: oneshot::Sender<AckOutcome> }` — `wants_coordinate` is set for a `PushTracked` producer, which is told where its record landed. The ack channel travels intact through the worker and batch to the WAB drain, which resolves it after the durable write. Workers do not ack.
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
- Ownership: `src/wab/lock.rs` takes an exclusive `flock` on `<wab_dir>/.weir.lock` before anything reads the WAB, keeps the fd in a process-wide static until exit (never released explicitly, never unlinked, never re-opened), and is re-checked once a second by its own thread from right after acquisition (so through recovery too): a deleted lock file is re-created (`O_EXCL`) and re-locked, and a replacement nobody holds is re-locked unless its record names another daemon; one another instance holds, or one carrying another daemon's record, is proof of lost ownership, and the daemon exits 3 without sealing. `wab_dir_lock = "best-effort"` only covers a filesystem that cannot lock; then orphan deletion is off. File format: [wab_format.md](wab_format.md#the-wab_dir-lock-file).
- Path validation is consolidated in `src/config/mod.rs` and runs at config-load time (`validate_path` for `wab_dir`, `validate_path_format` for `socket_path` and `admin_socket_path`). The WAB module previously carried a local copy for belt-and-suspenders checking at flusher-spawn time; that duplicate has been removed in favour of the single source of truth.

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
| `s3` | the `weir-sink-s3` crate | one object **per batch** (opt-in `s3-sink` feature) | archiving to S3-API object storage; see [the S3 sink](sinks/s3.md) |

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

### Lifecycle and drain control (`src/lifecycle.rs`, `src/drain/control.rs`)

- `Lifecycle` holds the shutdown level in a `watch` channel. The level only rises: `Running` → `Graceful` (first SIGTERM/SIGINT) → `Forced` (second); a third signal only logs. `request_shutdown` does T0 inline on the signal task (readiness off, phase `shutting_down`, drain absorb mode, the flushers' shutdown flag, the lame duck); the listeners and the admin socket only read the level. SIGHUP is not handled here; it stays on the reload path.
- `DrainControl` is the drain's *intent*: what the lifecycle or an operator wants it to do. Every field is an atomic on one `Arc` (absorb, stop, a deadline that only ever moves earlier, pause, requeue tickets), so a handle is cheap to clone and never blocks the drain. It carries no channel handle; the drain owns the drain channel's only receiver. Production uses `DrainControl::attached()`: the drain's lifetime is this control, not the channel, so a closed channel (every flusher gone) no longer ends delivery; only `request_stop(budget)` or `force_stop` does. What the drain has actually done is published to the status board (`src/drain/publish.rs`), never read back from the control.

### Status board and shard registry (`src/status/`)

- `StatusBoard` (`src/status/board.rs`) is the daemon's live state: static info, the lifecycle phase, listener, drain, sink, WAB, admin and reload cells, condition latches, queue and drain-channel depth. Each cell has one writer; readers only load. Writers store atomics or swap `ArcSwap` cells, and no blocking lock type appears anywhere under `src/status/` (a test, `no_blocking_locks_under_status`, enforces it).
- `ShardRegistry` (`src/status/shards.rs`) records each shard's state (the flusher supervisor is the single writer of its terminal state) and each worker's running flag (written by the worker's drop guard). Routing reads it at accept: `pick_live` sends a new connection only to an ingest-capable shard, and startup waits up to 10 s for every shard and worker to settle before binding the producer listeners.
- The poll tasks (`src/status/poll.rs`): every 5 s, the WAB size scan (feeding the `wab_max_bytes` cap), the drain-heartbeat stall check, and the evaluation of the board into the `weir-status` Conditions and the status gauges; every 500 ms, queue depth and the drain channel's high-water stamp. `src/status/render.rs` turns the board into the `weir.status/v1` document that `/status`, admin `status` and `weir-ctl status` show.

### Admin socket (`src/admin/`) — Unix only

- A JSON-lines control socket, by default `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`, separate from the producer socket and never in the producer-shared directory. Bound before recovery with the same hardened bind as the producer socket; an explicit `admin_socket_path` that cannot be bound stops startup, a derived default that cannot be bound disables admin (`admin_unavailable`) and the daemon runs on. Keys: `admin_socket_enabled`, `admin_socket_path` ([configuration](operations/configuration.md#admin_socket_path)).
- Peers: the daemon's own euid or root only, always checked (`src/admin/identity.rs`; independent of `peer_uid_check`, which governs producers). Framing (`src/admin/framing.rs`): one JSON object per line each way, 64 KiB line limit enforced while reading, at most 64 requests per connection.
- Verbs (`src/admin/verbs.rs`): `status` reads the board; `reload` goes through `src/reload.rs`'s single loop; `drain.pause`, `drain.resume` and `drain.retry_stranded` store intent in `DrainControl` and wait (bounded) for the drain's observation on the board. Neither the drain nor the lifecycle ever waits on a client. A pause takes effect before the next commit batch, keeps ingest buffering, and is in memory only: a restart resumes delivery. `weir-ctl status`, `reload` and `drain pause|resume|retry-stranded` are the client side.

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
| `weir_config_reloads_total{trigger,outcome}` | counter | Reload requests (`sighup`, or `admin` for the admin socket's `reload` verb) by `ok` / `failed` / `noop` / `dropped` |
| `weir_drain_local_io_strands_total{cause}` | counter | Strands caused by local I/O (`read`, `write`), kept out of the sink-oriented stranded counter |
| `weir_drain_segments_parked{reason}` | gauge | Parked segments (`local_read_failed`, `unsupported_version`, `drain_panic`) |
| `weir_drain_segments_disposition_pending{kind}` | gauge | Delivered or set-aside segments with a `confirm` or `quarantine` step outstanding |
| `weir_drain_confirm_failures_total{stage}` | counter | Failed confirm steps (`sidecar`, `delete`) |
| `weir_drain_quarantine_failures_total` | counter | Quarantine moves that failed (retried each poll) |
| `weir_drain_dead_letter_write_failures_total` | counter | Dead-letter writes that failed after the sink committed |
| `weir_drain_busy_seconds{kind}` | gauge | Seconds the drain has spent in its current blocking operation, by kind |

The table lists the core series only; 4.1.0 added the status, lifecycle and admin families (`weir_ready`, `weir_live`, `weir_condition_active{code,severity}`, `weir_startup_phase{phase}`, `weir_admin_requests_total` and others), listed in [monitoring.md](monitoring.md).

`weir_drain_state` and `weir_sink_health` are pre-initialised so all label values appear on the first scrape. The HTTP listener binds to `metrics_bind:metrics_port` (default `127.0.0.1:9185`, localhost only) before WAB recovery, so it answers through recovery and until the end of shutdown. Set `metrics_bind = "0.0.0.0"` to allow container-host scraping; restrict exposure via firewall / network namespace, and note that every route on it is unauthenticated.

**Router** (`src/metrics/router.rs`, pure: request head → route, probe answer → response; `src/metrics/server.rs` does the socket work). Each connection first takes one of 64 head slots and must send its request head (≤ 4096 bytes) within 1 s, or it is closed. Then exact-path `GET`/`HEAD` of `/livez`, `/readyz`, `/startupz` and `/status` take one of 4 probe slots; any other method on those paths gets 405, and every other path takes one of `metrics_max_connections` and gets the metrics exposition byte for byte as before. `/livez` and `/readyz` answer `200 ok` or `503 not live: <codes>` / `503 not ready: <codes>`; `/startupz` is 200 once the daemon has ever served; `/status` is the `weir.status/v1` JSON document. The probes read the status board through `StatusSource` (`src/status/http.rs`) and evaluate it with the `weir-status` predicates on every request: loads only, no lock, no I/O. Connections closed without a response count in `weir_http_requests_dropped_total{reason}`. Probe use: [monitoring.md → Health and status endpoints](monitoring.md#health-and-status-endpoints).

### Testing infrastructure

**System tests** (`crates/weir-server/tests/system.rs`, Unix-only):

Each test spawns a real `weir-server` binary via `env!("CARGO_BIN_EXE_weir-server")` with its own temp directory, socket path, WAB directory, and metrics port. A global process mutex serialises server spawning so the suite runs safely with any `--test-threads` value. Tests are written against the client library (`weir-client`) and the raw wire protocol where needed.

Coverage includes: basic push/ack round-trips, both durability tiers, multi-shard write distribution, crash recovery, graceful shutdown under load, stalled-client isolation, partial frame injection, disk-full nacks, WAB byte-level integrity after SIGKILL, a second daemon refused on a live socket or a locked `wab_dir` (and the first one's ownership-loss exit when the lock file is stolen, at runtime or during recovery), refused duplicates delivering nothing, SIGHUP served as a reload from before recovery onward, bounded shutdown with a full drain channel, a forcing second SIGTERM, and a drain that outlives its flushers, fd-limit exhaustion, per-shard record ordering, batch deadline timer accuracy, and metrics consistency across crash-restart cycles.

**Load tests** (`crates/weir-server/tests/load.rs`):

Throughput and latency measurements for single-threaded, thundering-herd, connection-churn, fire-and-forget overload, latency, and saturation-ramp scenarios. Results are emitted as `BENCH: {json}` lines; `deploy/avg_benchmarks.py` averages multiple runs and writes `docs/benchmarks/latest.md` and appends a row to `docs/benchmarks/history.md` (the `docs/benchmarks.md` index is hand-maintained). CI runs 5 passes at each of two batch deadlines (1ms and 2ms) and commits the averaged result on pushes to `main`.

**Deterministic simulation testing — DST** (`crates/weir-server/src/wab/dst.rs`, behind the `dst` cargo feature):

The crown invariant — *an ack is never a false ack; no acked record is ever lost* — is checked directly against the durability path under injected fault schedules. The harness swaps the real segment backend for one that injects faults on a seeded schedule, then drives the flusher through scenarios and asserts the invariant in-process.

- **Fault/scenario model.** Injected faults: `fdatasync` returns `EIO`, a short/torn write at the *n*-th record, `rename` fails, the shutdown seal fails with `ENOSPC`, and (in a dedicated test) the flusher panics at the `fdatasync` barrier. The sweep exercises six scenarios per seed: a synchronous flush under an fsync-`EIO`, a torn write mid-batch, a crash between `sync_all` and the seal-rename, an `ENOSPC` at the shutdown seal, a cooperatively-scheduled producer/flusher interleaving, and the bounded hand-off to a slow or absent drain across a shutdown. Every run asserts that no acked record is lost and that acks never precede stable storage.
- **Replayable seeds.** All randomness comes from a seeded `SplitMix64`, so every run is fully reproducible from its seed. A violated invariant panics with the invariant name and a one-line `WEIR_DST_SEED=0x… cargo test … --features dst` reproducer.
- **Pinned regression seeds.** Any discovered failure serialises into `crates/weir-server/tests/dst_seeds/*.json`; the `replay_pinned_regression_seeds` test re-runs every pinned spec on each CI run, so a fixed bug stays fixed.
- **Random-seed sweep.** `sweep_random_seeds` runs `WEIR_DST_SWEEP` seeds (default 16 for fast PR runs; CI sets `WEIR_DST_SWEEP=300`), six scenarios each, on every push. Any seed that breaks an invariant fails the build with a pin-able repro.
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
| WAB integrity on shared/network storage    | **Out of scope.** CRC32 detects accidental corruption; it does not detect malicious modification. The WAB directory must be local storage accessible only to the daemon (`0o700`). Network filesystems break the security model. Sealed segments can carry tamper evidence after the fact with `weir-ctl attest` (the `weir-attest` hash chain). |

---

## Workspace & crate boundaries

weir ships as several crates rather than one, and the split is deliberate: each
piece is usable **without** pulling in the daemon, so you can compose the parts
you want instead of taking the whole thing. The boundaries are enforced by the
dependency graph, not just convention — verify with `cargo tree`.

| Crate | Depends on (normal) | Standalone use without the daemon |
|-------|---------------------|-----------------------------------|
| `weir-core` | — (just `bytes`, `crc32fast`) | The wire types + `Payload`. Build a producer or a non-Rust client against the frozen v1 wire protocol. |
| `weir-wab` | `weir-core`, `crc32fast`, `zstd` | Read/inspect/recover on-disk WAB segments. No async runtime, no daemon. `SegmentReader::open(path)` streams CRC-verified records straight off disk — useful for a recovery tool, an offline inspector, or a custom reader. |
| `weir-status` | — (just `serde`, `serde_json`) | The `weir.status/v1` model, Conditions evaluator and probe predicates shared by the daemon and `weir-ctl`; pure Rust, no I/O. Evaluate a daemon from its metrics exposition or from disk facts without linking the daemon. |
| `weir-sink-sdk` | `weir-core` | Implement **and unit-test** a custom `Sink` against the stable trait — no `weir-server`, no tokio required (the crate's own tests drive `commit`/`health` with a tiny `block_on`). `BasicSinkError` / `Infallible` cover quick error types. |
| `weir-client` | `weir-core` | The synchronous producer client. Push records over the socket from your own app. |
| `weir-attest` | `weir-wab`, `weir-sink-sdk` | A tamper-evident hash chain over sealed WAB segments, verifiable against the downstream's own rows (`weir-ctl attest`). |
| `weir-sink-s3` | `weir-core`, `weir-sink-sdk` | The S3-API object-storage sink (AWS S3, MinIO, R2, B2, Ceph); compiled into `weir-server` behind its optional `s3-sink` feature. |
| `weir-ctl` | `weir-core`, `weir-client`, `weir-wab`, `weir-attest`, `weir-status` | The admin CLI. Notably does **not** depend on `weir-server` — it's a thin tool over the wire client, the segment reader, the admin socket and the HTTP routes, so it (and the WAB-reading capability it's built on) work independently of the daemon binary; offline, `weir-ctl status` evaluates the same `weir-status` predicates from metrics and disk. |
| `weir-server` | the above + tokio/sink stacks | The daemon itself. |
| `weir-rs` | `weir-core`; optionally `weir-client`, `weir-sink-sdk`, `weir-wab` | A facade re-exporting the libraries above behind features, for one dependency line. |

Ten crates are published; `weir-testkit` (test helpers over `weir-client`) is workspace-only.

**What this enables.** You can build your own system on the pieces — e.g. read
the WAB with `weir-wab`, run operations with `weir-ctl`, and forward to your own
destination by implementing a `Sink` with `weir-sink-sdk` — and only depend on
the daemon if you actually run it. (Today, *running* a custom sink inside the
shipped daemon still means compiling it into the sink-selection path; the SDK's
present, delivered value is authoring + testing a sink against a frozen contract.
A first-class plugin/entry-point is a candidate for a future minor release.)

The cost of the split is real but small — more `Cargo.toml`s and a published
surface to keep stable — and it buys genuine composability, not speculative
modularity: the boundaries have present consumers (`weir-ctl` uses `weir-wab` and
`weir-status`; the built-in sinks implement `weir-sink-sdk`).

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
