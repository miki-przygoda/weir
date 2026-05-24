# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. The wire protocol
version (the `VERSION` byte in the envelope header) is tracked separately
under **Wire protocol** below and may evolve independently of crate versions.

---

## [Unreleased]

### Notes
- `weir` evolves from an earlier private project, HTDIP (Hardware-Tuned Data
  Ingestion Pipeline), which was built to a real production spec for a
  Rails + MySQL stack. `weir` removes the domain coupling entirely
  and reshapes the daemon as a sink-agnostic write buffer with explicit
  durability tiers, a user-implementable sink trait, and a public crate
  surface. The HTDIP write-ahead buffer design, crash-recovery proof, and
  benchmark methodology carry over; the command schema, Rails integration,
  and MySQL-specific drain logic do not.

### Added
- Conversion plan and architecture decisions (workspace-local, not committed).
- Initial repository scaffolding.
- **weir-core**: wire protocol types (`Envelope`, `Header`, `MessageType`,
  `Durability`, `NackReason`, `DecodeError`, `WeirError`, `Payload`).
  - `WIRE_VERSION = 1` and `MAX_PAYLOAD_HARD_CAP = 16 MiB` are the
    single-source-of-truth constants shared across all crates.
  - `NackReason::VersionMismatch` Nack payload encodes `[0x02, WIRE_VERSION]`
    so future clients can inspect the daemon's supported version.
  - `DecodeError::VersionMismatch { supported, received }` is a distinct
    variant (not collapsed into `BadMagic` or `HeaderCrcMismatch`), enabling
    producers to distinguish a version gate from a corrupt frame.
  - Envelope decode checks `PayloadTooLarge` before any heap allocation and
    before the full frame-length check, preventing pre-allocation DoS.
  - Header decode checks the version byte before the header CRC so a v2
    client gets `VersionMismatch` (actionable) rather than `HeaderCrcMismatch`
    (confusing) when talking to a v1 daemon.
- **WAB subsystem** (`weir-server::wab`): segment file format (FORMAT_VERSION=1),
  crash recovery, and the flusher thread pool.
  - Segment layout: 24-byte header + records (`payload_len u32 LE` + `crc32
    u32 LE` + payload bytes) + 4-byte zero sentinel + 32-byte footer.
  - `.confirmed` sidecar: 36-byte binary (`WCON` magic, version, reserved,
    `sealed_at i64 LE`, `record_count u64 LE`, `drained_at i64 LE`,
    `file_crc32 u32 LE`); CRC covers the first 32 bytes.
  - `WabSegment` accumulates a running `crc32fast::Hasher` over every byte
    written so the footer CRC requires no full-file re-read at seal time.
  - `ShardWriter::write_record` returns `Option<PathBuf>` on rotation so the
    flusher thread can forward the sealed path to the drain channel without an
    extra seal-current call.
  - Crash recovery replays every open segment, truncates trailing corrupt
    records, and rebuilds the footer. Segments with bad magic or unknown
    format versions are quarantined rather than deleted.
  - `SegmentReader` enforces `MAX_PAYLOAD_HARD_CAP` before allocation to
    prevent corrupt-length fields from triggering large heap allocations during
    replay.
  - `spawn` validates the WAB directory, advances shard counters past existing
    segments, replays unconfirmed sealed segments, and pins each flusher thread
    with `core_affinity`. SCHED_FIFO is attempted on Linux and fails open with
    a warning.
- **Work queue** (`weir-server::queue`): bounded MPMC channel between the
  socket layer and worker pool.
  - `QUEUE_CAPACITY = 65_536` slots. `QueueSender::push` blocks the calling
    thread when the channel is full — intentional backpressure that stalls the
    socket handler rather than dropping records or growing unboundedly.
  - Generic over `T` so the socket layer instantiates `Queue<WorkUnit>` without
    the queue module depending on `WorkUnit`.
  - `QueueSender` is `Clone`; dropping all clones closes the channel and
    propagates the shutdown signal to every worker `Receiver`.
- **Worker pool** (`weir-server::worker`, `weir-server::models`): batching
  layer between the queue and the WAB.
  - `WorkUnit { shard_id, payload, ack_tx }` — the `tokio::sync::oneshot`
    sender travels intact through the `Batch` to the WAB drain (step-06),
    which resolves it after the durable write. Workers do not ack directly.
  - `Batch { shard_id, records: Vec<WorkUnit> }` — flushed per shard via
    `std::mem::replace` (zero allocation on the hot path; fresh pre-allocated
    buffer swapped in on every flush).
  - Workers flush on batch-full or on `batch_deadline`; `on_disconnect` is
    `#[cold] #[inline(never)]` to keep it off the hot path and bias the branch
    predictor toward the `Ok(unit)` arm.
  - Startup warmup: page pre-touch faults in batch buffer backing pages;
    10 000 multiply-accumulate iterations prime the FP pipeline
    (`_mm_mul_ps` on x86_64, `vmulq_f32` on AArch64).
  - Workers pinned starting at core 2 (`WORKER_CORE_START = 2`), leaving
    cores 0–1 free for OS and network interrupt handlers.
  - `QOS_CLASS_USER_INTERACTIVE` on macOS (declared via `unsafe extern "C"`;
    not in the libc crate); `SCHED_FIFO` on Linux — both fail-open.

### Security
- `Envelope::decode` and `SegmentReader` both enforce `MAX_PAYLOAD_HARD_CAP`
  before any heap allocation, capping DoS surface from malformed length fields.
- `WabSegment::create` uses `O_CREAT | O_EXCL | O_NOFOLLOW` to prevent
  symlink-based TOCTOU attacks on segment file creation.
- WAB directory validation (`validate_path`) requires an absolute path free of
  `..` components and null bytes, then calls `canonicalize` to resolve symlinks;
  dangling symlinks return a `NotFound` error that includes a `mkdir -p && chmod
  700` hint (matching the PostgreSQL directory-creation UX).
- `.confirmed` sidecar files with bad magic, unknown version, or CRC mismatch
  are quarantined rather than trusted, preventing a corrupt sidecar from marking
  a segment as drained when it was not.
- Per-record CRC32 checked during crash recovery; only records up to the first
  corrupt entry are replayed, so a torn write at crash time never silently
  corrupts the replay stream.

---

## Wire protocol

The envelope format carries a `VERSION` byte in its fixed header. Changes to
the wire format are logged here so producers and consumers can negotiate.

### v1
- Initial implementation. 16-byte header (`WEIR` magic, version, type,
  durability, flags, payload length u32 LE, header CRC32 of first 12 bytes)
  followed by payload bytes and a trailing payload CRC32 u32 LE.
- Version byte is checked before the header CRC so version mismatches surface
  as `VersionMismatch` rather than `HeaderCrcMismatch`.
- `VersionMismatch` Nack payload: `[0x02, WIRE_VERSION]` (two bytes; second
  byte is the daemon's supported version).

---

[Unreleased]: https://github.com/miki-przygoda/weir