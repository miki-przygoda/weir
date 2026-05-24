# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. Wire protocol version
changes are tracked separately under **Wire protocol** below.

---

## [Unreleased]

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

| Version | Status  | Notes                        |
|---------|---------|------------------------------|
| v1      | current | See [docs/wire_protocol.md](docs/wire_protocol.md) |

---

[Unreleased]: https://github.com/miki-przygoda/weir
