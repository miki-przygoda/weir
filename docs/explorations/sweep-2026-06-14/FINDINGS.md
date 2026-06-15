# Codebase sweep — findings & overnight fixes (2026-06-14 → 15)

> Generated from the Max-tier multi-agent sweep (orthogonal lens + independent subsystem sweeps, completeness critic, per-finding adversarial verification). Plan: [`PLAN.md`](PLAN.md). Per-subsystem detail: [`subsystems/`](subsystems/). Raw data: `findings.json`.

## TL;DR

- The sweep raised **76 raw** candidates → **60 confirmed-real** after adversarial verification (+ 1 uncertain, 1 refuted).
- Severity of the real findings: **1 critical, 9 high, 18 medium, 28 low, 4 info**.
- **Fixed tonight: 13** (one or grouped commits each, every change gated: fmt + clippy -D warnings + tests, DST sweep where durability-adjacent).
- **Needs your decision / redesign: 7** — all documented below with a recommendation. Nothing load-bearing is broken; these are 1.0-shape calls or post-1.0 refactors.
- **Queued safe fixes (not yet done tonight): 40** — localized, low-risk; listed so they're a clean follow-up batch.
- The headline: a **CRITICAL data-loss bug (F12)** was found AND fixed — exactly the kind of thing the prior sweeps' big-bug focus had masked.

## 🔴 Critical — fixed

**F12 — recovery counter-reset overwrites a recovered segment (data loss).** `crates/weir-server/src/wab/segment.rs:335` · fixed in `fb02a62`.

After Phase-1 recovery seals all .wab to .wab.sealed (recovery.rs:322), the flusher's scan_and_advance_counter (segment.rs:414) calls segment_counters (segment.rs:335) -> segment_counter_from_path which uses file_stem(); rustc-verified that seg_00000001.wab.sealed/.confirmed return None while seg_00000001.wab returns Some(1), so sealed/confirmed files are invisible and next_counter resets to 1. create_new/O_EXCL (segment.rs:59) only collides with an active .wab, not a .wab.sealed. The first new record creates seg_00000001.wab and on rotation/shutdown seal()'s std::fs::rename (segment.rs:258) atomically overwrites the recovered seg_00000001.wab.sealed (runtime-verified Unix rename overwrites). replay_unconfirmed only queues recovered segments (mod.rs:328-347); confirm_and_delete removes a sealed file only after a successful drain (confirmed.rs:42), so a slow/down sink leaves the recovered durably-acked-but-undelivered segment on disk to be destroyed — a retroactive false ack.

Fix: parse the segment counter from the `seg_NNNNNNNN` prefix regardless of extension, so the post-recovery counter scan advances past sealed/confirmed segments and never reuses a counter. Regression tests + the 300-seed DST sweep are green.

## ✅ Fixed tonight

| ID | Sev | Subsystem | Finding | Commit |
|----|-----|-----------|---------|--------|
| F12 | critical | wab | Segment counter resets to 1 after crash recovery; the new run's seal()-rename silently overwrites a still-undrained recovered .wab.sealed segment (data loss, crown-invariant violation) | `fb02a62` |
| F01 | high | drain | Partial dead-letter write leaves an orphan active file that poisons all future dead-lettering (within-run create_new collision) | `7acd6ee` |
| F02 | high | drain | Drain confirms+deletes a segment without verifying CommitResult covers every input record (committed + dead_lettered may not partition the batch) | `02382df` |
| F11 | high | drain | DeadLetterWriter has zero unit tests; scan_dir counter-recovery across restart is untested | `7acd6ee` |
| F31 | high | sink | MySQL sink dead-letters live data on a recoverable connection-time auth/access failure (asymmetric with Postgres) | `a40dc0b` |
| F40 | high | client-sdk-ctl | weir-ctl `dl list` / `dl drop` silently see zero dead-letter segments (wrong suffix filter) | `ee2841e` |
| F53 | high | config | shard_count in 65..=256 with defaulted worker_count rejects a documented, in-range config with a misleading error | `7a499f7` |
| F03 | medium | drain | Single permanently-rejected batch larger than dead_letter_max_bytes wedges the drain in a permanent block↔retry livelock | `02382df` |
| F04 | medium | drain | Sink::health() is called with no timeout backstop; a hanging health() stalls the entire drain | `02382df` |
| F06 | medium | drain | Dead-letter total_bytes silently undercounts on metadata() failure, bypassing the dead_letter_max_bytes cap | `7acd6ee` |
| F07 | low | drain | Dead-letter cap accounting omits fixed 60-byte per-file segment overhead, so dead_letter_max_bytes can be exceeded | `7acd6ee` |
| F08 | low | drain | Swallowed rescan() error can wedge the drain blocked despite an operator-freed dead-letter directory | `02382df` |
| F13 | low | wab | segment_counters doc comment justifies skipping sealed files as 'matches the historical scan' — stale rationale that masks the counter-reset data-loss bug | `fb02a62` |

## 🟠 Needs your decision / redesign

These were deliberately **not** changed tonight (they need a product/API call or a redesign). Each has my recommendation; most are 1.0-freeze decisions.

### F41 — CommitResult partition invariant (committed ∪ dead_lettered = batch) is unenforced; the drain confirms+deletes the segment unconditionally on Ok  
*(high · redesign · client-sdk-ctl · `crates/weir-sink-sdk/src/lib.rs:106`)*

CommitResult<R> { committed, dead_lettered } (lib.rs:104-111) is a plain public-field struct with no constructor and no enforced invariant. The drain's Ok(commit_result) path uses committed.len() only for a metric (mod.rs:622), writes dead_lettered (mod.rs:624-660), then returns BatchResult::Ok (mod.rs:662) with NO reconciliation against payloads.len(); BatchResult::Ok confirms and deletes the segment. A third-party Sink that omits a record from both vectors causes a false confirm / silent loss, violating the crown invariant. First-party sinks (noop/postgres/http) all partition correctly, masking it today. The drain already preserves the segment defensively on a dead-letter write failure (mod.rs:648-658) but has no analogous guard for an under-covered CommitResult.

➡️ **Mitigated tonight by F02** — the drain now refuses to confirm a CommitResult whose committed+dead_lettered don't cover the batch. The deeper fix (encode the partition invariant in the SDK type: a validating `CommitResult` constructor instead of public fields) is an irreversible 1.0 SDK-API choice. **Recommend:** fold into the freeze decisions; low urgency now that F02 guards the runtime.

### F42 — SinkRecord::into_payload is documented as the dead-letter recovery path but is bypassed for whole-batch permanent (and transient) errors  
*(high · redesign · client-sdk-ctl · `crates/weir-sink-sdk/src/lib.rs:71`)*

into_payload's doc (lib.rs:71) says it is used when a record must be dead-lettered, but the drain calls into_payload only for the per-record dead_lettered list of a successful CommitResult (mod.rs:625-629). On a whole-batch permanent error the drain writes the raw original payloads slice via dead_letter.write_records(payloads) (mod.rs:696), bypassing into_payload; the transient path never round-trips either. For a non-identity SinkRecord the dead-lettered contents on a permanent error are the original segment bytes, not into_payload output. The only impl in the workspace is the identity impl SinkRecord for Payload (lib.rs:77-85), so the divergence is latent today.

➡️ Related to F41. Whole-batch permanent/transient paths dead-letter raw payloads, bypassing `SinkRecord::into_payload`. For the built-in `Payload` record (identity transform) this is harmless; it only matters for a third-party custom `Record`. **Recommend:** decide alongside the `Sink::commit` signature freeze — either route all dead-letter paths through `into_payload`, or narrow its doc to 'per-record-result only'.

### F05 — Retried multi-batch segment re-dead-letters earlier sub-batches, amplifying duplicate dead-letter files and thrashing the cap  
*(medium · redesign · drain · `crates/weir-server/src/drain/mod.rs:529`)*

process_segment commits sub-batches sequentially (527-547). If an early sub-batch dead-letters successfully and a later one returns Transient/Blocked, the whole segment is preserved and retried from record 0 (re-opened at 485); the early sub-batch dead-letters again via write_records (no dedup, dead_letter.rs:64-83), duplicating records/files and, when Blocked, feeding a cap-overshoot loop.

➡️ Multi-batch segment retry re-dead-letters earlier sub-batches → duplicate dead-letter files. Duplicates in the dead-letter store are noise, not data loss (terminal inspection store). A real fix needs per-sub-batch progress tracking within a segment retry (a drain redesign). **Recommend:** defer post-1.0 unless dead-letter duplication is observed.

### F48 — Public error enums and CommitResult are not #[non_exhaustive]; adding any variant/field after 1.0 is a breaking change  
*(medium · decision · core · `crates/weir-core/src/error.rs:13`)*

DecodeError (error.rs:13) and WeirError (error.rs:101) are exhaustive public enums with no #[non_exhaustive]; the same holds for ClientError (weir-client/unix.rs:11), CommitResult (sink-sdk:106, all-public fields) and SinkHealth (sink-sdk:115). The module doc at error.rs:8 says 'one variant per frame-validation step', so variant growth is the documented expectation — making every future validation step a major bump. Mark these #[non_exhaustive] before 1.0 (free now, impossible later). Correctly excludes the wire enums MessageType/Durability/NackReason whose repr is the contract.

➡️ Public error enums (`DecodeError`, `WeirError`, `ClientError`) + `SinkHealth` + `CommitResult` aren't `#[non_exhaustive]`, so adding any variant/field post-1.0 is breaking. The error model explicitly expects variant growth (e.g. new NackReason/DecodeError). **Recommend (freeze):** mark the error enums + `SinkHealth` `#[non_exhaustive]` before 1.0; pair `CommitResult` with the F41 constructor decision.

### F50 — Header::new takes a payload_len argument that Envelope::new always overwrites — an API that invites desync  
*(low · decision · core · `crates/weir-core/src/envelope.rs:98`)*

Every production caller computes payload.len() as u32 and passes it to Header::new only for Envelope::new to overwrite it (unix.rs:90-92, connection.rs:431-437). For bare Header::encode without an Envelope (connection.rs:680-686), a caller can pass any value and produce a header whose declared length matches no payload — the desync the encapsulation work was meant to prevent. Dropping payload_len from Header::new (default 0, set by Envelope::new) would make the correct value the only reachable one.

➡️ `Header::new` takes a `payload_len` that `Envelope::new` always overwrites, and a bare `Header::encode` can still desync. **Recommend (freeze):** drop `payload_len` from `Header::new` so the only way to set it is via `Envelope` (which derives it). Small, pairs with R2.

### F24 — QueueSender exposes len() with no is_empty()  
*(info · decision · worker-queue-metrics · `crates/weir-server/src/queue.rs:55`)*

QueueSender::len() is public (queue.rs:55-57, used by weir_queue_depth poll per queue.rs:53-54) with no companion is_empty() — the classic clippy::len_without_is_empty wart, latent because the workspace has no clippy lint config (no [lints] table in any Cargo.toml, no #![deny(clippy::...)] in lib.rs). len() also sums across partitions (queue.rs:56), so it is a cross-partition in-flight total, not a slot count — mildly misleading. Worth a deliberate decision before the 1.0 API freeze.

➡️ `QueueSender::len()` has no `is_empty()` (clippy::len_without_is_empty), latent only because there's no clippy lint config. Trivial + safe; flagged 'decision' only because it adds a public method. **Recommend / will do:** add `is_empty()` (I'll fold this into the safe-fix pass).

### F52 — Reserved flags byte is preserved verbatim on decode but never validated to be zero, foreclosing in-version flag evolution  
*(info · decision · core · `crates/weir-core/src/envelope.rs:191`)*

Header::decode reads flags = buf[7] (envelope.rs:191) and stores it without a == 0 check, despite the field doc (envelope.rs:91), lib doc, and docs/wire_protocol.md calling flags 'reserved; zero on write'. proptest_envelope.rs:170/180 asserts arbitrary nonzero flags round-trip, confirming a deliberate preserve-don't-reject choice. With WIRE_VERSION fixed at 1, a v1 daemon silently accepts and ignores any future flag bit, so flag semantics can only be added via a WIRE_VERSION bump. Worth an explicit decision/comment; rejecting nonzero flags now would preserve in-version flag evolution.

➡️ Decode preserves arbitrary `flags` without checking zero, so a v1 daemon silently ignores future flag bits instead of rejecting. **Recommend (freeze):** decide flag-evolution policy — reject nonzero now (clean error on old daemons when a flag is added later) vs keep preserve-and-ignore. Tied to the wire-freeze cluster + the reserved-Nack-byte decision.

## 🟡 Queued safe fixes (not done tonight)

Localized, low-risk — a clean follow-up batch (I ran out of the night before reaching these; none need a decision).

| ID | Sev | Subsystem | Finding |
|----|-----|-----------|---------|
| F37 | high | sink | ClickHouse sink commit() request/response classification has no in-process test (only an #[ignore] live-docker integration test) |
| F14 | medium | wab | recover_segment renames .wab -> .wab.sealed without fsync_parent_dir, unlike WabSegment::seal — recovered seal not crash-durable |
| F15 | medium | wab | replay_unconfirmed silently skips sealed-but-unconfirmed segments on a per-entry DirEntry error (filter_map(\|e\| e.ok())) |
| F17 | medium | wab | Recovery: no test for the oversized-payload_len boundary (record at exactly MAX_PAYLOAD_HARD_CAP must survive; one over must truncate) |
| F20 | medium | worker-queue-metrics | Metrics HTTP server has no read/write timeout — slowloris permanently wedges the unauthenticated endpoint and blinds monitoring |
| F21 | medium | worker-queue-metrics | Phase-2 coalesce starves co-located shards on a shared worker (worker_count < shard_count) |
| F25 | medium | socket | UnknownMessageType/UnknownDurability decode errors are nacked as the (documented-as-transient, keep-connection-open) InternalError reason, yet the connection is closed |
| F32 | medium | sink | ClickHouse response body read is not bounded by the sink's configured timeout (error path can hang to the drain backstop) |
| F34 | medium | sink | ClickHouse does not percent-decode URL credentials before HTTP basic auth (silent divergence from SQL sinks) |
| F38 | medium | sink | HTTP concurrency: no test asserts a transient mid-batch error actually cancels still-in-flight POSTs |
| F39 | medium | sink | HTTP concurrency: no test pins record-to-outcome pairing (right payload in committed vs dead_lettered) under concurrent POSTs |
| F43 | medium | client-sdk-ctl | Blocking client sets no socket read/write/connect timeouts; a wedged daemon blocks producers indefinitely |
| F44 | medium | client-sdk-ctl | Client read_response allocates vec![0u8; payload_len] without the MAX_PAYLOAD_HARD_CAP guard the server applies before allocating |
| F54 | medium | config | Config-time warn! calls are silently dropped (tracing subscriber initialized after Config::load) |
| F09 | low | drain | dead_letter_full counter and blocked_since reset on every unblock→reblock cycle, inflating the metric and resetting blocked-duration |
| F10 | low | drain | .confirmed sidecar created without explicit 0o600 mode, tripping the daemon's own recovery mode audit under any non-0o077 umask |
| F16 | low | wab | recover_open_segments processes the dead_letter/ directory as if it were a shard directory |
| F18 | low | wab | Recovery: the partial-seal sentinel branch during crash recovery is untested |
| F22 | low | worker-queue-metrics | accept_latency histogram uses 1ms-floor buckets for a sub-millisecond measurement |
| F23 | low | worker-queue-metrics | flush_shard 'WAB is shutting down' comment misdescribes the permanent-shard-offline case |
| F26 | low | socket | Payload and CRC reads are not raced against handler_shutdown, so a mid-frame stall holds a semaphore permit until read_timeout during graceful shutdown |
| F27 | low | socket | Payload/CRC read timeout is a whole-transfer deadline but is documented and metered as a per-byte idle timeout |
| F28 | low | socket | Shutdown-time socket removal is path-based (std::fs::remove_file), inconsistent with the hardened unlinkat-based startup cleanup |
| F29 | low | socket | TLS handshake is not raced against handler_shutdown, so in-flight handshakes block graceful drain up to handshake_timeout |
| F30 | low | socket | umask save/restore in bind_hardened has no RAII guard; an unwind between set and restore would leak the tightened 0o177 umask process-wide |
| F33 | low | sink | ClickHouse split_credentials leaves username-only userinfo in base_url, contradicting the struct invariant |
| F35 | low | sink | ClickHouse content-derived dedup token silently breaks idempotency if sink_max_batch_size changes across a restart |
| F36 | low | sink | ClickHouse RowBinary encoding silently assumes a plain String column with no config validation and contract buried in a private fn comment |
| F45 | low | client-sdk-ctl | weir-sink-sdk re-export doc describes Payload as bytes::Bytes, contradicting the R1 newtype rationale |
| F46 | low | client-sdk-ctl | SDK doc overstates that health() is called after every commit attempt |
| F47 | low | client-sdk-ctl | Client discards the daemon-version byte the server sends on a VersionMismatch Nack |
| F49 | low | core | Envelope::new silently truncates payload length via payload.len() as u32, falsifying the documented 'cannot desync' invariant |
| F51 | low | core | Payload newtype: PartialEq impls and the Borrow<[u8]> hashmap-key contract are untested |
| F55 | low | config | Feature-gated [server] keys are silently dropped: in KNOWN_SERVER_KEYS but absent from RawConfig on builds without the feature |
| F56 | low | config | HELP advertises feature-gated CLI flags that fail as generic 'unknown arguments' when the feature is off, with no feature hint |
| F57 | low | config | Boolean env/CLI values accept only exactly 'true'/'false'; '1', '0', 'TRUE' abort startup |
| F58 | low | config | log_level is never validated; invalid or empty values silently degrade or disable logging |
| F59 | low | config | Config derives Debug, exposing sink_url credentials, inconsistent with the project's deliberate bearer_token redaction |
| F19 | info | wab | macOS data fsync uses F_BARRIERFSYNC (barrier) while directory durability uses plain fsync via sync_all — weaker than F_FULLFSYNC; an explicit, undertested tradeoff on the crown durability path |
| F60 | info | config | u64 durations range-checked via `as usize` truncate on 32-bit targets, letting out-of-range values pass |

## ⚪ Considered & dismissed

- **Refuted — Concurrent HTTP sink discards already-accumulated dead_lettered records when a later record in the same batch hits a transient error** (sink). http.rs:351-358 implements the documented 'transient aborts the segment so the drain retries' contract; at-least-once holds (nothing falsely confirmed) and a serial per-record loop returning Err on the first transient would behave identically, so it is intended behaviour, not a regression.
- **Uncertain — MySQL transient code 1317 (ER_QUERY_INTERRUPTED) is undocumented in module + classify docs and untested** (sink). 1317 is genuinely absent from the module docstring (mysql.rs:27-39) and the test (mysql.rs:476-490), but it IS documented at docs/operations/configuration.md:718, so 'undocumented' is only partly true and impact is info-level not low. — *safe to fix (doc+test); folded into the queued list.*

## Per-subsystem detail

- [`subsystems/client-sdk-ctl.md`](subsystems/client-sdk-ctl.md)
- [`subsystems/config.md`](subsystems/config.md)
- [`subsystems/core.md`](subsystems/core.md)
- [`subsystems/drain.md`](subsystems/drain.md)
- [`subsystems/sink.md`](subsystems/sink.md)
- [`subsystems/socket.md`](subsystems/socket.md)
- [`subsystems/wab.md`](subsystems/wab.md)
- [`subsystems/worker-queue-metrics.md`](subsystems/worker-queue-metrics.md)
