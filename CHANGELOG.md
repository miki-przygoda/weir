# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

From v1.0 onward, `weir` follows Semantic Versioning: a breaking change to the
public Rust API of a published crate (`weir-core`, `weir-client`,
`weir-sink-sdk`, `weir-wab`) or to the wire protocol requires a major version
bump. Wire protocol version changes are tracked separately under **Wire
protocol** below.

---

## [1.3.0] - 2026-06-22

Hardening + test-completeness release from four pre-publication sweeps (three
adversarial bug-hunts plus a per-logic-layer action audit covering all 204
actions on two axes: behavioural test coverage and "the operator cannot
accidentally break their own system"). Two operator-foolproofing guardrails, a
few additive APIs, and ~30 new tests. No wire, on-disk-format, or existing-API
breaks — a clean minor.

### Added

- **weir-core — `From<MessageType> for u8`**, completing the symmetric
  `TryFrom<u8>` ⇄ `From<Self>` pair the other frozen wire enums (`Durability`,
  `NackReason`) already expose.
- **weir-wab — `SegmentVerifyError::FooterMismatch`** (the enum is
  `#[non_exhaustive]`): `verify_sealed_segment` now cross-checks the footer's
  `record_count` / `data_bytes` against the records actually walked — the one
  segment region the whole-file CRC does not cover.
- **`weir_recovery_quarantine_copy_failed_total`** metric — an alertable signal
  that recovery is stuck on a corrupt segment it cannot preserve (see Reliability).
- **weir-core — `NackReason` is now `#[non_exhaustive]`** so promoting a reserved
  reason byte (`0x0A`–`0xFF`) is an additive, not breaking, change.

### Reliability

- **Recovery fails closed on an un-preservable corrupt tail.** When a mid-file
  corrupt segment's quarantine *copy* fails (disk full / read-only mount / inode
  exhaustion), recovery no longer truncates the valid prefix — that discarded any
  acked-durable records sitting after the corruption. It now leaves the segment
  untouched, returns an error (left for manual inspection), and bumps
  `weir_recovery_quarantine_copy_failed_total`, so the crown invariant (an acked
  record is never lost) holds even when preservation is impossible.
- **`tls_handshake_timeout_secs` is range-validated `[1, 300]`.** A `0` (a
  plausible fat-finger) previously made every TLS handshake time out instantly,
  silently rejecting all TCP/mTLS clients while the daemon looked healthy.
- **WAB segment + shard-directory ordering is numeric, not lexicographic** — the
  documented "ascending counter order" held only while every name shared the same
  zero-pad width.
- **weir-ctl `dl requeue` skips a torn / unsealed `.wab.sealed` tail** instead of
  pushing its readable prefix and then deleting the file (which silently dropped
  the torn remainder).
- **Accept loop reaps finished connection handlers** each iteration (the `JoinSet`
  was only drained at shutdown, growing unbounded under many short connections).

### Wire protocol & conformance

- **Canonical decode order is length-before-magic** across the Rust reference and
  all five polyglot demo decoders; the Java decoder's field-parse order was fixed
  to match (`message_type` → `durability` → reserved flags). The conformance suite
  grew to **30 vectors** (`reject_partial_magic_short`,
  `reject_flags_and_unknown_durability`) to pin both precedences. No wire-format
  change — only which rejection tag a malformed frame carries.

### Tests

- **Per-layer action audit** added ~30 tests across all 11 layers, closing the
  coverage + secure-by-design gaps it surfaced: the client mTLS `connect_tls`
  trust boundary (previously zero-coverage), DST mid-batch rotation ack-fate +
  seal-error-during-rotation, client refuse-and-poison on hostile daemon
  responses, sink bearer / basic-auth delivery on the wire, the drain
  commit-timeout-resumes-past-cursor + supervisor give-up paths, recovery
  symlink / format-version trust boundaries, and the config range / zero guards.

### Docs

- Truth-ups: the architecture durability-tier claim (Buffered does **not** uphold
  "ack ⇒ durable"), the README throughput headline, the README→architecture DST
  cross-reference plus a "Deterministic Simulation Testing" section, version-
  agnostic crates.io-publish framing, and the conformance-vector counts across the
  demos.

---

## [1.2.0] - 2026-06-21

Additive operability + ecosystem release from the post-1.1 finalization sweeps: a
public `weir-wab` forensics read surface, machine-readable `weir-ctl --json`, a
Prometheus-label accessor on `NackReason`, a recoverable client-side empty-payload
guard, a Buffered-throughput fix, and a 5-language polyglot demo showcase. All
backward compatible — no wire, on-disk-format, or existing-API breaks (a clean
minor). The new `weir-wab` error enums are `#[non_exhaustive]` so future variants
stay additive.

### Added

- **weir-wab — public forensics read surface.** `verify_sealed_segment` (streamed
  whole-file CRC check) returning `SegmentVerification` / `SegmentVerifyError`;
  `list_segment_files` → `SegmentState` (Active/Sealed/Confirmed); `parse_segment_header`
  / `parse_segment_footer` / `crc32` with `SegmentHeaderMeta` / `SegmentFooterMeta`;
  and `SegmentReader::header()` / `into_inner()` / `get_ref()` / `terminated_cleanly()`.
- **weir-ctl — opt-in `--json`** global flag for machine-readable output on
  `health` / `metrics` / `segments` / `push` / `dl` (human tables stay the default).
- **weir-core — `NackReason::as_metric_label()` / `from_metric_label()`** mapping wire
  reasons to the Prometheus `reason=` labels (the wire byte `0x07` is unchanged).
- **weir-client — `ClientError::EmptyPayload`**, a recoverable local pre-send guard in
  `WeirClient::push` (an empty payload is rejected before any bytes are sent, leaving
  the connection usable, mirroring the oversized-payload guard).
- **demos/ — 5-language polyglot wire clients** (C, Java, TypeScript, Python, Go) built
  from the spec + conformance vectors with no weir-crate dependency, each passing all
  30 conformance vectors, plus per-client subpages in the demo bundle.

### Changed

- **Tier-aware coalesce.** A Buffered-only batch no longer pays the worker's Phase-2
  coalesce window (it has no group-fsync to amortise), restoring single-shard Buffered
  throughput (~8–12× when connections co-locate); per-shard FIFO and durability are
  unchanged.
- **Drain stranded-segment auto-resume** now runs from the `BlockedDeadLetterFull` and
  `RetryingTransient` states as well as `Draining`; a strand→resume reprocesses the
  whole segment (documented whole-segment at-least-once — sinks already dedupe).
- **Demo bundle restyled** to the personal-site "hr2" palette (dark neutral + sky/violet/
  green, Inter + JetBrains Mono).
- **Docs** — protocol/ops/monitoring truth-ups, decoder-tag→Nack-byte table, consumer
  rustdoc + integrating guide, conformance/sink-integration; **publish order corrected to
  `weir-core → weir-wab → weir-sink-sdk → weir-client / weir-server / weir-ctl`**
  (`weir-testkit` is `publish = false`).

### Fixed

- WAB mid-record truncation now surfaces a contextual error (preserving the
  `UnexpectedEof` kind); `verify_sealed_segment` rejects trailing bytes after the footer.
- weir-ctl: a missing wab dir now errors (instead of an empty-OK), `dl requeue`
  skip-semantics documented, `--socket-path` alias made visible, `--json` failures emit a
  JSON error object.
- weir-server: a one-shot warning when the HTTP sink runs unauthenticated, absolute
  socket-path help text, `weir_drain_state` / nack-counter metric truth-ups (nack series
  pre-initialised at 0).
- config: range guards asserted on every bounded scalar knob.

## [1.1.0] - 2026-06-19

Additive operability features, a recovery hardening, and extensive doc truth-ups
from the post-1.0 hands-on usage sweeps (Tier-4 features plus several rounds of
"build a real app against the crates" persona sweeps). All backward compatible —
no wire, on-disk, or API breaks.

### Added

- **Auto-resume for stranded segments** (drain). When the sink exhausts its
  transient-retry budget on a segment, the segment is left durably on disk
  ("stranded"). The drain now re-queues stranded segments automatically when the
  sink health recovers (a down→up edge at the idle health-poll), instead of
  waiting for a daemon restart. New `weir_drain_segments_resumed_total` metric.
- **Configurable sink retry budget** (config). `sink_max_retries` (default 3) and
  `sink_retry_base_delay_ms` (default 100) are now exposed via CLI/env/TOML; they
  were previously hardcoded.
- **HTTP sink NDJSON batch mode** (`sink_http_batch = none | ndjson`, default
  `none`). `ndjson` sends a whole commit batch as one newline-delimited POST
  (`application/x-ndjson`) with a single per-batch `Idempotency-Key` — the
  framing Loki / Elasticsearch `_bulk` ingesters expect — collapsing the
  per-record round-trip cost for sustained forwarding. The default per-record
  mode is unchanged.
- **`weir-ctl dl requeue`** re-submits dead-lettered records back through the
  daemon's socket and deletes each segment once all its records are re-accepted.
  Re-delivery is at-least-once (the HTTP sink's idempotency key dedupes identical
  payloads if a run is interrupted). Defaults to a dry run; `--yes` to apply.
- **New crate `weir-wab`** — the on-disk WAB segment format and `SegmentReader`,
  extracted so `weir-ctl` can read dead-letter segments with the *same* parser
  the daemon uses, without depending on the daemon's async/sink dependency tree.
  `FORMAT_VERSION = 1` is frozen under the SemVer promise above.
- **Client reconnect helpers** — `WeirClient::is_poisoned()` and
  `ClientError::is_recoverable()` let a long-lived or async-bridged producer
  decide when to drop and rebuild a connection without matching the
  `#[non_exhaustive]` `ClientError` enum.
- **`weir-server --version` / `-V`** (previously only `weir-ctl` had it).
- **`weir-wab` re-exports `Payload`** so a `weir-wab`-only consumer can name the
  `SegmentReader` item type without a direct `weir-core` dependency; the
  bad-segment-magic error is now human-readable (ASCII rendering + expected magic).
- **systemd deployment artifacts** (`deploy/systemd/`) — a hardened `weir.service`
  unit, an `EnvironmentFile` secret pattern, and a liveness+readiness probe
  script, for bare-metal / VM operators (the deploy story was previously
  Docker-only).

### Reliability

- **Active-segment crash recovery now quarantines a mid-file-corrupt tail**
  instead of silently truncating it. On a CRC mismatch in the *middle* of a
  recovered active segment (bit-rot, not a torn trailing write), recovery now
  copies the whole segment to `quarantine/`, bumps
  `weir_recovery_segments_quarantined_total`, and logs at ERROR — so acked
  records sitting after the corruption are preserved and surfaced rather than
  silently dropped, symmetric with the sealed-segment drain path. The normal
  torn-tail crash case is unchanged (clean truncate, no quarantine).

### Fixed

- **`docker build` was broken** — the Dockerfile's dependency-stub stage omitted
  the new `weir-wab` workspace member, failing the build; the `weir-wab` stub is
  now included.
- **docker-compose** used a `wget` healthcheck (absent from the
  `ca-certificates`-only runtime image) and did not set
  `WEIR_METRICS_BIND=0.0.0.0`, so the mapped metrics port was unscrapeable from
  the host while the container still reported healthy. Both fixed.
- **systemd readiness probe** compared gauge values as `"1"` while the daemon
  emits `1.0`, so the sink-down / drain-blocked not-ready paths never tripped;
  it also exited 1 silently on a wrong-but-live `/metrics` target. Both fixed.

### Documentation

- Extensive truth-ups from the persona sweeps: an async-producer guide (the
  blocking-`push()`-starves-the-runtime trap + the recommended dedicated-thread
  bridge); accurate `Durability` tier rustdoc (Sync and Batched both
  group-`fdatasync` at the batch boundary before ack); the Windows /
  cross-platform story (`weir-client`'s client type is Unix-only); a
  concurrency-aware tuning note (`shard_count` is non-monotonic, not a throughput
  dial); the transient-strands vs permanent-dead-letters distinction; the
  `weir_sink_health` HEAD-probe lag; and a quarantined-segment recovery recipe.
  The startup `shard_count` advisory no longer claims raising it "can unlock
  additional throughput."

---

## [1.0.0] - 2026-06-15

The 1.0 release. `weir` is now stable: the v1 wire protocol and the public Rust
API are frozen, backed by Semantic Versioning. This release is the culmination of
the post-0.9 hardening arc — four code-review passes, a deterministic-simulation
campaign, and two deliberate freeze passes (wire, then API) — that closed every
known data-loss path and locked the surfaces a 1.0 promises not to break.

The headline is stability, not features. If you built against 0.9, the breaking
changes below are small and mechanical; in exchange, everything you depend on at
1.0 carries a SemVer guarantee.

### Stability

- **Wire protocol v1 is frozen.** The 16-byte frame, message types, durability
  tiers, and Nack reason bytes are fixed. A new
  [language-neutral conformance suite](docs/conformance.md)
  (`docs/conformance/wire_v1_vectors.json`, 26 canonical vectors covering every
  message type, all nine Nack reasons, and every decode-rejection case) lets a
  non-Rust client or daemon prove byte-compatibility. `weir-core`'s own decoder
  is tested against it.
- **Public Rust API is frozen.** `weir-core`, `weir-client`, and `weir-sink-sdk`
  carry the SemVer promise above. The public error enums (`DecodeError`,
  `WeirError`, `ClientError`, `SinkHealth`) and `CommitResult` are
  `#[non_exhaustive]`, so the model can grow post-1.0 without a breaking change.
- **WAB on-disk format is stable** and crash-recovery replays unconfirmed
  segments on restart, as before.

### Breaking

These are the only source-level breaks versus 0.9, all in service of the freeze:

- **`Header::new` drops the `payload_len` argument** — its signature is now
  `Header::new(message_type, durability, flags)`. `Envelope` is the single source
  of truth for the on-wire length, so a header whose declared length disagrees
  with its payload is unrepresentable.
- **`Header` / `Envelope` fields are private**, accessed via methods
  (`header.message_type()`, `envelope.payload()`, …) rather than field access.
- **`Payload` is a newtype** (`struct Payload(Bytes)`) instead of a
  `type Payload = bytes::Bytes` alias. It derefs to `[u8]` and converts from
  `Vec<u8>` / `&[u8]` / `Bytes`, so most call sites are unaffected; its `Debug`
  prints only the length, never the bytes.
- **`CommitResult` is constructed with `CommitResult::new(committed, dead_lettered)`**
  instead of a struct literal (it is now `#[non_exhaustive]`).
- **The wire decoder is stricter:** a nonzero reserved `flags` byte and bytes
  trailing a complete frame are now rejected (`ReservedFlagsSet`,
  `TrailingBytes`) rather than ignored.

### Added

- **Conformance vectors** — `docs/conformance/wire_v1_vectors.json` plus a
  generator and a Rust suite (`cargo test -p weir-core --test conformance`); see
  [docs/conformance.md](docs/conformance.md).
- **Two new Nack reasons** — `UnknownMessage` (0x08) for a CRC-valid header with
  an unknown `message_type`/`durability` (a permanent, connection-closing error,
  distinct from the transient keep-open `InternalError`), and `ReservedFlagsSet`
  (0x09) for a nonzero reserved `flags` byte.
- **Concurrent HTTP sink delivery** — the HTTP sink issues up to
  `sink_http_concurrency` (default 8) per-record POSTs in flight, ordered, while
  keeping per-record idempotency keys and dead-lettering.
- **Client read/write timeouts** — opt-in `set_read_timeout` / `set_write_timeout`
  on the Unix and TLS clients turn a silent stalled-daemon hang into a clean
  error; the client also poisons itself on a desync so a stale frame can never be
  read as the next call's reply.
- **Symmetric `Payload` equality** (`slice == payload` as well as
  `payload == slice`) and crate-root re-exports of the wire `TryFrom` error
  structs (`UnknownMessageType`, `UnknownDurability`, `UnknownNackReason`).

### Reliability

The hardening passes found and fixed several latent **silent data-loss** paths
that could acknowledge or confirm a record that was never durable:

- **Recovery no longer resets the segment counter** after restart — the scan now
  parses the counter from the `seg_NNNNNNNN` prefix across all extensions, so a
  freshly recovered, undrained sealed segment can't be clobbered by the next seal.
- **The drain never confirms-and-deletes a segment whose records weren't
  delivered** — failed dead-letter writes, transient reader-open failures,
  mid-segment read errors (now quarantined), and partial commit results are all
  caught before a segment is reclaimed.
- **The HTTP/SQL/ClickHouse sinks no longer false-ack** — redirects are not
  followed (a redirected POST that returns 2xx is treated as undelivered),
  backpressure (408/429) and 5xx are retried, and 4xx are dead-lettered.
- **A torn write mid-batch can no longer ride another segment's fsync** — acks
  are split by segment fate, surfaced by the deterministic-simulation harness.
- **The drain thread is panic-supervised** (respawn capped at 10), parent
  directories are fsynced after seal/rename, and empty Push payloads (which alias
  the WAB end-of-records sentinel) are rejected at ingest.

### Fixed

- Numerous smaller correctness and robustness fixes across config parsing,
  credential handling (URLs split at the last `@`; passwords redacted from
  `Debug` and build errors), `accept(2)` resource-exhaustion backoff, quarantine
  namespacing across shards, metrics accounting, and `weir-ctl` dead-letter
  handling. Full detail in the git history for the `v1/phase-4-cleanup` branch.

### Changed

- **Drain retry resumes mid-segment** rather than reprocessing already-committed
  sub-batches, so a transient failure partway through a large segment no longer
  re-dead-letters earlier records on retry.

---

## [0.9.0] - 2026-06-14

The consolidation release — everything from the v1 development arc since 0.5.0,
gathered into one publishable cut and the last release before 1.0. Highlights: a
published **Sink SDK** and an **admin CLI** (`weir-ctl`), a **ClickHouse sink**, a
**performance pass** that proved the durable write path is fsync-bound, a
**deterministic simulation-testing (DST) harness** for the WAB that found and
fixed a real durability bug, and a full **observability package** (Grafana +
Prometheus + a turnkey monitoring stack). The public API is expected to be stable
but is not yet frozen — that promise lands at 1.0.

### Added

- **`weir-sink-sdk` crate** — the `Sink` trait and its `SinkError` / `CommitResult`
  contract, extracted into a standalone published crate so downstream authors can
  implement custom sinks without depending on the server internals.
- **`weir-ctl` admin CLI** — a separate binary for operating a running daemon:
  `health`, `push`, and `metrics`; `segments` for per-shard on-disk WAB
  inspection; and `dl` to list/drop entries in the dead-letter store.
- **ClickHouse sink** (feature `clickhouse-sink`, opt-in) — HTTP
  `INSERT … FORMAT RowBinary` batch inserts via reqwest, with a content-derived
  sha256 `insert_deduplication_token` per batch so a crash-replayed byte-identical
  batch is deduplicated by a `Replicated*MergeTree` engine. Reuses `sql_common`
  (identifier validation, password redaction, `SqlSinkError`). Config:
  `sink_type = "clickhouse"`, `sink_clickhouse_{database,table,column}`.
- **Observability package** (`deploy/`) — a Prometheus + Grafana stack you can
  drop in, deliverables-as-config:
  - a Grafana **overview dashboard** (`deploy/grafana/weir-dashboard.json`) — an
    at-a-glance health strip + Ingest / Durability / Drain / System rows;
  - **per-instance dashboards** generated DRY from one panel definition by
    `deploy/grafana/gen-dashboards.py` (latency percentiles + an fsync heatmap,
    per-tier throughput, segment lifecycle, sink/drain detail);
  - **Prometheus alert rules** (`deploy/prometheus/weir-alerts.yml`, 14 rules /
    4 groups, `promtool`-validated), each linking a per-alert runbook in the new
    operator guide `docs/monitoring.md`;
  - a **turnkey `docker compose` demo** (`deploy/monitoring/`) — weir + Prometheus
    + Grafana + load generators — with opt-in `levels` (min/med/high/max usage
    comparison, a dashboard per level) and `chaos` (dead-letter / degraded-sink /
    peer-UID faults) profiles, plus an end-to-end `smoke-test.sh` that runs in CI.
- **Per-stage latency instrumentation** (`bench-trace` feature, off by default,
  Stream A). Records enqueue → worker-flush → flusher → ack stage timings as
  Prometheus histograms (`weir_stage_*_seconds`) so the load suite can attribute
  latency to a pipeline stage. Also widens the latency-percentile samples
  (500 → 2000) with per-run σ, and adds a Sync-tier saturation ramp. Zero cost
  when the feature is off.

### Reliability

- **Deterministic Simulation Testing (DST) harness for the WAB.** The flusher's
  durability path is driven under a virtual clock against a fault-injecting
  segment store, with a causality ledger and an invariant oracle whose central
  assertion is that **an ack is never a false ack** — every record reported
  durable is recoverable. Injectable seams (`SegmentStore`, `BlockingClock`) let
  the sim model fsync errors, torn writes, a crash between sync and rename, ENOSPC
  at shutdown, and flusher panics; a cooperative scheduler (`SimExecutor`) explores
  thread interleavings deterministically. Pinned regression seeds replay in CI and
  any failure prints a `WEIR_DST_SEED=…` reproduction, alongside a random-seed
  sweep.
- **Fixed a false-ack durability bug** surfaced by the DST harness: after a write
  error dropped the active segment mid-batch, the group-fsync could cover the
  wrong segment and ack records that were not actually durable. Records in a
  segment dropped mid-batch are now Nacked, never falsely acked.
- **Drain backstop timeout** around `Sink::commit`, so a sink that hangs forever
  can no longer wedge the drain indefinitely — the commit times out and the
  segment is retried.

### Performance

A performance pass (Phase 3). The headline finding, proven with the new per-stage
instrumentation across a macOS dev box and a real Linux machine: the durable write
path is **fsync-bound** — `fdatasync` is ~89% of Sync latency on NVMe and ~99% on
a SATA SSD. The changes below make the software pipeline leaner and fully
measurable; the fsync floor itself is storage-bound.

- **Bridge-thread removal (Stream B).** The per-shard "bridge" thread that
  converted `Batch` → `WabRecord` is gone; the worker now feeds the WAB flusher
  directly over a single `Batch` channel per shard (the `WabRecord` type was
  removed). One fewer OS thread + channel hop per shard. Per-shard FIFO,
  group-fsync coalescing, panic supervision, and graceful shutdown are unchanged
  and verified.
- **`bytes::Bytes` payload (Stream C).** `Payload` changed from `Vec<u8>` to
  ref-counted `bytes::Bytes`, making payload clones O(1). Eliminates the drain's
  per-batch payload copy in `commit_batch` and the HTTP sink's `to_vec()`. Wire
  and on-disk formats are byte-identical (verified by the codec, crash-recovery,
  and fuzz suites).
- **Vectored WAB writes (Stream D).** `WabSegment::write_record` now issues one
  `writev` instead of three `write_all` syscalls per record — the per-record
  write stage drops ~60%. (`write_all_vectored` is still unstable, so a single
  `write_vectored` + poison-on-short-write is used.) Total latency is fsync-bound
  and unchanged.
- **io_uring evaluated and rejected.** A direct micro-benchmark on Linux showed
  io_uring (batched writes + IO_DRAIN datasync fsync) is never faster than
  `writev`+`fdatasync` for the WAB write+fsync pattern — the cost is the
  storage-bound fsync, which io_uring cannot accelerate (and its ring bookkeeping
  adds overhead). Not pursued; `write_vectored` already captured the portable
  write-syscall win.
- **Adaptive coalesce window (Stream E).** The worker's coalesce window is now
  sized from an exponential moving average (alpha=1/4) of observed fsync
  latency, fed back via a shared lock-free `Arc<AtomicU64>`, clamped to
  [50 µs, 2 ms]. On fast NVMe it converges near the old fixed 200 µs; the
  intended benefit is on slower storage where a fixed window fragments batches.
  (A/B on a SATA SSD shows it is marginal — helps low concurrency, neutral-to-
  slightly-worse at high concurrency.)

### Changed

- **Sinks are now feature-gated** — `http-sink`, `mysql-sink`, `postgres-sink`,
  `clickhouse-sink` (`noop` is always compiled). `default = ["http-sink",
  "mysql-sink", "postgres-sink"]`, so the default daemon build is unchanged;
  library consumers can trim the dependency tree with `default-features = false`.
  Requesting an unbuilt sink via `sink_type` now fails with a clear
  "requires the 'X-sink' feature" error.
- **`worker_count` defaults to `shard_count`** — no idle worker thread when the
  two are left unset.

### Fixed

- The Docker image build now includes the `weir-sink-sdk` and `weir-ctl`
  workspace members (stub manifests + sources) so `cargo build -p weir-server`
  resolves the full workspace.

### Publishing

- crates.io metadata (`keywords`, `categories`, docs.rs config) added to the
  published crates, under the **MIT** license. Publish order:
  `weir-core → weir-sink-sdk → weir-client / weir-server / weir-ctl`;
  `weir-testkit` is `publish = false` (dev-only).

## [0.5.0] - 2026-06-10

### Added

- **TCP listener with mandatory mutual TLS** (`weir-server --features tls`).
  Remote producers across an untrusted network can now connect over TCP using
  rustls (aws-lc-rs provider). Key properties:

  - **Mutual TLS — client cert required.** Every TCP client must present a
    certificate signed by the configured CA (`tls_client_ca_path`). Anonymous
    or cert-less clients are rejected at the TLS handshake. Trust model: CA
    issuance is the gate — issuing a client cert from the CA authorises that
    producer.
  - **Plaintext TCP is never exposed.** Setting `tcp_bind` without a complete
    TLS configuration is a fatal startup error; the daemon never opens a
    cleartext TCP socket.
  - **Concurrent with the Unix socket.** The TCP listener runs alongside the
    existing Unix socket listener and feeds the same pipeline. Both listeners
    share **one** global connection semaphore sized `max_connections`, so the
    total concurrent connections across both transports is bounded by
    `max_connections` (not 2×).
  - **Handshake-slowloris guard.** `tls_handshake_timeout_secs` (default 10s)
    bounds TLS-handshake duration; the connection permit is held across the
    handshake so a flood of stalled TCP connections is bounded by the existing
    connection cap.
  - **Wire protocol unchanged.** TLS wraps the existing weir frame protocol
    byte-for-byte. The on-wire format, CRC checks, payload caps, and frame
    semantics are identical to the Unix socket path.
  - **Default-off.** The feature is behind a `tls` Cargo feature on
    `weir-server` (and `weir-client`). A standard `cargo build` produces a
    Unix-only binary with no TLS code compiled in.

  **New config keys** (all follow the standard CLI > env > TOML > default
  merge order):

  | TOML key | CLI flag | Env var | Default |
  |----------|----------|---------|---------|
  | `tcp_bind` | `--tcp-bind` | `WEIR_TCP_BIND` | none (TCP disabled) |
  | `tls_cert_path` | `--tls-cert` | `WEIR_TLS_CERT` | none (required when `tcp_bind` set) |
  | `tls_key_path` | `--tls-key` | `WEIR_TLS_KEY` | none (required when `tcp_bind` set) |
  | `tls_client_ca_path` | `--tls-client-ca` | `WEIR_TLS_CLIENT_CA` | none (required when `tcp_bind` set) |
  | `tls_handshake_timeout_secs` | `--tls-handshake-timeout-secs` | `WEIR_TLS_HANDSHAKE_TIMEOUT_SECS` | `10` |

  **SIGHUP cert rotation.** `kill -HUP <pid>` reloads the TLS cert, key, and
  CA from the configured paths without dropping active connections. Reload is
  fail-safe: on any error (missing file, invalid PEM) the daemon keeps serving
  the previous TLS configuration, logs an error, and increments
  `weir_tls_config_reloads_total{outcome="failed"}`. A successful reload
  increments `{outcome="ok"}`. **SIGHUP reloads TLS material only** — all
  other configuration stays read-once-at-startup.

  **New metrics** (added by the `tls` feature):

  - `weir_tls_handshake_failures_total{reason}` — reason ∈ {`no_client_cert`,
    `bad_cert`, `timeout`, `other`}.
  - `weir_tls_config_reloads_total{outcome}` — outcome ∈ {`ok`, `failed`}.

- **`WeirClient::connect_tls`** (`weir-client --features tls`). Mutual-TLS
  client connector. `connect_tls(addr, ClientTlsConfig { client_cert,
  client_key, ca_cert, server_name, default_durability })` opens a TLS
  connection to a `weir-server` TCP listener, presenting the client cert and
  validating the server cert against the provided CA. The `server_name` must
  match a SAN in the server certificate.

- **Per-connection durability default** (`weir-client`). `connect_with_default`
  and `ClientTlsConfig.default_durability` set a per-connection fallback
  durability tier. `client.push_default(payload)` uses that tier without
  repeating the argument per-push; `client.set_default_durability(tier)`
  updates it at runtime. Plain `push(payload, tier)` is unchanged.

- **`crates/weir-client/examples/push_tls.rs`** — runnable example of a TLS
  client pushing records to a TCP-enabled daemon.

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

### Added (tests)

- **`postgres_sink_end_to_end` + docker-compose harness for the SQL
  sink integration suite.** The Postgres sink shipped without an
  end-to-end test against a real Postgres (the MySQL sink had one,
  `#[ignore]`-marked); the recent SQL-sink refactor commit
  (`bf5f047`) flagged the docker-compose harness as a "clean
  follow-up." This commit closes both:
  - **`postgres_sink_end_to_end`** (`crates/weir-server/tests/system.rs`)
    mirrors `mysql_sink_end_to_end`: 100 Sync pushes, asserts the
    sink committed all of them and did so with ≥10:1 records-per-commit
    IOPS compression. `#[ignore]`-marked; reads `WEIR_TEST_POSTGRES_URL`.
  - **`deploy/docker/test/docker-compose.yml`** (new) — a separate
    compose file (not entangled with the deployment example at
    `deploy/docker/docker-compose.yml`) that spins up `mysql:8.0` on
    `127.0.0.1:33306` and `postgres:16` on `127.0.0.1:55432` with
    the reference schemas pre-seeded from
    `init-mysql.sql` / `init-postgres.sql`. Healthchecks gate the
    runner script on init-completion.
  - **`deploy/run-sink-integration-tests.sh`** (new) — brings up the
    compose stack, polls each service's `docker compose ps`
    health status (timeout: 120 s per service), exports
    `WEIR_TEST_MYSQL_URL` and `WEIR_TEST_POSTGRES_URL`, runs both
    ignored tests via `cargo test -- --ignored --exact`, tears
    down on exit via `trap`. `RELEASE=1` env var switches to a
    release build (matches CI).
  - **`docs/testing/sink-integration.md`** (new) — operator-facing
    guide: quickstart, what the runner does, manual-setup
    fallback, what's deliberately out of scope (CI wiring,
    TLS-enabled containers).

  CI wiring (a `.github/workflows/sink-integration.yml`) is
  deliberately deferred — adds GitHub Actions service-container
  complexity that deserves its own review. The harness ships
  locally-runnable today.

- **Three drain-pipeline end-to-end tests + extended `MockSink`
  introspection** (`crates/weir-server/src/drain/mod.rs::tests`). The
  existing `MockSink` was canned (returned pre-baked
  `CommitResult`s) and gave tests no way to verify the drain
  actually passed the right payloads through to the sink, or that
  the drain's retry-after handling actually slept the wall-clock
  duration the hint asked for. The mock now captures every
  `commit()` call's timestamp and the committed / dead-lettered
  payloads it reported, with helper methods (`call_timestamps()`,
  `committed_records()`, `dead_lettered_records()`) for test
  introspection. The new tests:

  1. **`mock_captures_show_exact_payloads_pass_through_drain`** —
     three records into the segment, mock returns a 2-committed +
     1-dead-lettered split, asserts the mock's view, the metric
     counts, AND the dead-letter file on disk all agree. Catches
     a regression where the drain duplicates, drops, or reorders
     records between segment-read and sink-commit.
  2. **`drain_waits_retry_after_hint_before_retrying`** — first
     commit returns `MockError::TransientWithRetryAfter(75ms)`,
     second returns Ok; asserts the wall-clock gap between the two
     calls is ≥ 60 ms (well above the 1 ms `fast_config` default,
     allowing for sandbox jitter). The existing `next_retry_delay`
     tests verified the helper's math; this verifies the drain
     loop ACTUALLY USES that math.
  3. **`confirmed_file_only_appears_after_successful_commit_not_during_retries`** —
     three transient failures followed by Ok; asserts the
     `.confirmed` sidecar doesn't appear until the final success.
     Catches a regression where the drain optimistically writes
     `.confirmed` before the sink actually acks.

  Three `MockError` machinery additions: a
  `TransientWithRetryAfter(Duration)` variant, a `SinkError::retry_after`
  impl on `MockError` that returns the Duration, and three
  `Mutex<Vec<...>>` capture fields on `MockSink`. The existing
  tests' canned flow is unchanged.

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

### Added (tooling)

- **`fuzz/` directory + cargo-fuzz infrastructure for the trust-boundary
  parsers.** Two coverage-guided fuzz targets ship the on-disk and
  on-wire byte parsers — both reach attacker-controlled bytes (the
  WAB confirmed-file is on-disk-trustworthy only as long as the host
  isn't compromised, and the wire envelope decoder reads every byte
  off an unauthenticated socket):
  - **`wab_confirmed`** —
    `weir_server::wab::format::parse_confirmed`. Reached at startup
    by the drain's directory scan and at runtime by confirmation
    writes.
  - **`envelope_parse`** — `weir_core::Envelope::decode`. Every
    connected client gets to feed this function arbitrary bytes.

  Property under test (both targets): never panic on any input.
  Errors are expected for most random inputs; panics are not.

  The existing proptest harness in
  `crates/weir-core/tests/reference_frames.rs` covers the
  *round-trip* direction (encode → decode of valid envelopes
  yields the original); the fuzz targets cover the *inverse*
  (arbitrary input never panics, regardless of whether it
  round-trips).

  **`docs/testing/fuzzing.md`** ships as operator-facing
  documentation: how to install nightly + cargo-fuzz, how to
  invoke each target, how to seed a corpus from existing
  reference frames, and what's deliberately deferred (CI
  integration, the SegmentReader target — see the doc for
  rationale).

  Cargo-fuzz needs nightly Rust, so the fuzz crate lives outside
  the workspace (`fuzz/Cargo.toml` declares its own
  `[workspace]`) and stable users invoking `cargo build` from
  the repo root don't get pulled into nightly.

  A minimal `weir-server` library facade
  (`crates/weir-server/src/lib.rs`) was added to expose the
  format parsers to the fuzz crate without leaking
  internal types. The surface is deliberately narrow — only
  `wab::format` — to keep the production binary's
  `pub(crate)` boundary intact.

### Changed

- **WAB flusher: panic respawn replaces "offline until daemon restart."**
  Previously, a panic inside the flusher loop ran through
  `run_with_panic_supervision` (`crates/weir-server/src/wab/mod.rs:87`)
  which logged + incremented `weir_wab_flusher_panics` and then let
  the thread die. The shard stayed offline (every record routed to it
  Nack-ing) until daemon restart — the CHANGELOG explicitly marked
  respawn as a follow-up.

  Now: the supervisor wraps the body in `catch_unwind` AND a respawn
  loop bounded by `MAX_FLUSHER_RESPAWNS = 10` with linear backoff
  (10 ms × attempt). Channels, paths, and the metrics handle live in
  the outer scope and are cloned for each attempt — crossbeam
  channels share a queue under `Clone`, so in-flight `WabRecord`s
  buffered in the bounded queue survive the panic and the respawned
  flusher drains them in the original order. After 10 unsuccessful
  respawns the shard goes permanently offline (the previous behaviour,
  just delayed by ~0.5 s of attempts) and the final log promotes to
  `error` level.

  Failure-mode hierarchy:
  - Transient panic (rare): logged at `warn`, metric bumped, flusher
    respawns and steady state recovers within a few hundred ms.
  - Deterministic panic (logical bug in the flusher): every respawn
    panics on the same condition, the cap trips, the shard goes
    offline with a single loud `error` log. Identical end state to
    the pre-respawn implementation.

  Tests: `flusher_panic_respawn_recovers_within_cap` exercises the
  recoverable path (3 transient panics, then clean); the existing
  `supervisor_catches_*` tests are adapted to the new body-factory
  shape. New `flusher_panic_loop_caps_out_after_max_respawns` proves
  the loop terminates and that the metric records every attempt.

### Added

- **Postgres sink: TLS support via `?sslmode=require` in the URL.**
  The initial Postgres sink shipped with a `NoTls` connector and the
  CHANGELOG noted TLS as a "planned follow-up via
  `tokio-postgres-rustls`." This commit implements the opt-in:

  - **URL-based opt-in.** `?sslmode=require` triggers
    `tokio-postgres-rustls::MakeRustlsConnect` (webpki-roots bundle,
    aws-lc-rs crypto provider, explicitly selected via
    `ClientConfig::builder_with_provider`). The default
    (`SslMode::Prefer`) and explicit `?sslmode=disable` both keep
    the cleartext path — an upgrade does not silently enable TLS on
    a previously cleartext deployment.
  - **Dep surface.** Three new direct deps:
    `tokio-postgres-rustls = "0.14"` (default-features off,
    `["aws-lc-rs", "webpki-roots"]`), `rustls = "0.23"`,
    `webpki-roots = "1"`. All three are already in the lockfile
    transitively via reqwest / mysql_async; naming them as direct
    deps lets `postgres.rs` construct the `ClientConfig` /
    `RootCertStore` explicitly.
  - **Explicit provider selection.** The workspace pulls both `ring`
    (via reqwest's older default) and `aws-lc-rs` (via the new
    tokio-postgres-rustls feature), which causes rustls's
    auto-detect to panic with "could not determine CryptoProvider."
    `build_tls_connector` uses
    `ClientConfig::builder_with_provider(aws_lc_rs::default_provider())`
    to pick deterministically without touching the process-global
    default, so it's safe to call alongside other rustls users in
    the same binary.
  - **Three new unit tests** in `src/sink/postgres.rs::tests`:
    - `sslmode_require_builds_sink_with_tls_connector` — pins the
      TLS-required build path.
    - `default_sslmode_prefer_does_not_enable_tls_silently` —
      pins the non-breaking default behaviour with an assertion on
      `pg_config.get_ssl_mode() == SslMode::Prefer`.
    - `sslmode_disable_builds_sink_with_no_tls` — pins the
      explicit-disable path.
  - **`docs/operations/configuration.md`** updated: the "TLS is not
    yet supported" section is replaced with the opt-in recipe and
    a sample URL.

  Live TLS handshake testing is deferred to the sink integration
  suite (`deploy/run-sink-integration-tests.sh`) — a future
  follow-up will add a TLS-enabled Postgres variant to the
  compose stack.

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

| Version | Status            | Notes                                              |
|---------|-------------------|----------------------------------------------------|
| v1      | current (frozen at 1.0) | See [docs/wire_protocol.md](docs/wire_protocol.md) and the [conformance vectors](docs/conformance.md) |

---

[1.3.0]: https://github.com/miki-przygoda/weir/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/miki-przygoda/weir/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/miki-przygoda/weir/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/miki-przygoda/weir/compare/v0.9.0...v1.0.0
[0.9.0]: https://github.com/miki-przygoda/weir/compare/v0.5.0...v0.9.0
[0.5.0]: https://github.com/miki-przygoda/weir/compare/v0.4.0...v0.5.0
[0.3.0]: https://github.com/miki-przygoda/weir/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/miki-przygoda/weir/compare/v0.1.0...v0.2.0
