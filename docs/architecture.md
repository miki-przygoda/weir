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
Socket layer          (async tokio, src/socket/)
  │  QueueSender::push_timeout  [crossing point: spawn_blocking]
  ▼
Work queue            (bounded MPMC, src/queue.rs)
  │  crossbeam_channel, QUEUE_CAPACITY = 65 536
  ▼
Worker pool           (std::thread, src/worker.rs)
  │  per-shard Batch channels
  ▼
WAB flusher threads   (std::thread, src/wab/)
  │  per-shard segment files
  ▼
Drain / Sink          (step 06+)
```

### Runtime boundary

The socket layer is async (tokio). Everything downstream runs on `std::thread` with blocking I/O. The only async/sync crossing is `task::spawn_blocking` in `handle_connection`, which moves the blocking `push_timeout` call onto tokio's blocking thread pool.

Under sustained load, if every active connection is simultaneously blocked waiting for a queue slot, all `spawn_blocking` threads fill up and new connections stall at the socket layer. `max_connections` must therefore be ≤ the tokio blocking thread pool limit (default: 512).

---

## Component responsibilities

### Socket layer (`src/socket/`)

- Binds a Unix socket with TOCTOU-hardened bind sequence (S_ISSOCK check before removing stale socket, chmod 0o600 after bind).
- Accepts connections up to `max_connections` (Semaphore-gated; over-cap streams are dropped immediately).
- `handle_connection` parses one frame at a time in a loop. Validation order is fixed and security-critical — see [wire_protocol.md](wire_protocol.md).
- Each accepted connection lives in a `JoinSet`; graceful shutdown drains it within `shutdown_timeout_secs` before aborting.

### Work queue (`src/queue.rs`)

- Bounded MPMC `crossbeam_channel` with `QUEUE_CAPACITY = 65 536` slots.
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
- Three durability tiers: `Sync` (fdatasync per record), `Batched` (group fdatasync per batch), `Buffered` (ack after memory write, no fsync).
- Segments rotate when `bytes_written >= SEGMENT_MAX_BYTES` (256 MiB). Sealed segments are forwarded to the drain channel.

### Drain (step 06, not yet implemented)

Reads sealed segments via `SegmentReader`, forwards records to the sink, writes `.confirmed` sidecars, and resolves `ack_tx` on each `WorkUnit`.

---

## Security design

| Concern | Mitigation |
|---|---|
| Pre-allocation DoS via large `payload_len` | Cap check (`min(config, MAX_PAYLOAD_HARD_CAP)`) before any allocation in both `handle_connection` and `SegmentReader` |
| Symlink TOCTOU on segment creation | `O_CREAT \| O_EXCL \| O_NOFOLLOW` on Unix; `create_new(true)` on Windows |
| Symlink TOCTOU on segment write-reopen | `O_NOFOLLOW` on the recovery write pass |
| Stale socket file removal | `symlink_metadata` + S_ISSOCK check before `remove_file`; refuses to remove a non-socket |
| Path traversal in WAB/socket paths | Absolute-path + no-`..` + no-null-byte + `canonicalize` validation |
| World-readable WAB files | Segment files: `0o600`; shard dirs: `0o700`; quarantine dir: `0o700` |
| Corrupt `.confirmed` sidecar | Bad magic / unknown version / CRC mismatch quarantines both the sealed segment and sidecar |
| Torn write in crash recovery | Per-record CRC32 checked; replay truncates at the first corrupt entry |
| Blocked socket semaphore (dead workers) | `push_timeout` (5 s) returns `InternalError` Nack rather than blocking indefinitely |
