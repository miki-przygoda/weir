# Codebase sweep — findings & fixes (2026-06-14 → 15)

> Output of the Max-tier multi-agent sweep (orthogonal lens + independent subsystem sweeps, completeness critic, per-finding adversarial verification). Plan: [`PLAN.md`](PLAN.md). Per-subsystem detail: [`subsystems/`](subsystems/). Raw data: `findings.json`. Every fix is its own commit (grep the log for `[Fxx]`), gated (fmt + clippy -D warnings + tests; DST 300-seed where durability-adjacent).

## TL;DR

- **76 raw** → **60 confirmed-real** after adversarial verification (+ 1 uncertain, 1 refuted).
- **Fixed: 60** — incl. the one CRITICAL data-loss bug (F12), every high-severity bug, and the mediums.
- **Needs your decision: 0** → all in **[`ESCALATIONS.md`](ESCALATIONS.md)** (separate file so they're easy to find).
- **Queued safe fixes: 0** — being worked through; this list shrinks as they land.

## ⚠️ Your decisions live in [`ESCALATIONS.md`](ESCALATIONS.md)

_(all escalations resolved)_

## ✅ Fixed

| ID | Sev | Subsystem | Finding | Commit |
|----|-----|-----------|---------|--------|
| F12 | critical | wab | Segment counter resets to 1 after crash recovery; the new run's seal()-rename silently overwrites a still-undrained recovered .wab.sealed segment (data loss, crown-invariant violation) | `fb02a62` |
| F01 | high | drain | Partial dead-letter write leaves an orphan active file that poisons all future dead-lettering (within-run create_new collision) | `7acd6ee` |
| F02 | high | drain | Drain confirms+deletes a segment without verifying CommitResult covers every input record (committed + dead_lettered may not partition the batch) | `02382df` |
| F11 | high | drain | DeadLetterWriter has zero unit tests; scan_dir counter-recovery across restart is untested | `7acd6ee` |
| F31 | high | sink | MySQL sink dead-letters live data on a recoverable connection-time auth/access failure (asymmetric with Postgres) | `a40dc0b` |
| F37 | high | sink | ClickHouse sink commit() request/response classification has no in-process test (only an #[ignore] live-docker integration test) | `3c94aa3` |
| F40 | high | client-sdk-ctl | weir-ctl `dl list` / `dl drop` silently see zero dead-letter segments (wrong suffix filter) | `ee2841e` |
| F41 | high | client-sdk-ctl | CommitResult partition invariant (committed ∪ dead_lettered = batch) is unenforced; the drain confirms+deletes the segment unconditionally on Ok | `fef9199` |
| F42 | high | client-sdk-ctl | SinkRecord::into_payload is documented as the dead-letter recovery path but is bypassed for whole-batch permanent (and transient) errors | `afcbed4` |
| F53 | high | config | shard_count in 65..=256 with defaulted worker_count rejects a documented, in-range config with a misleading error | `7a499f7` |
| F03 | medium | drain | Single permanently-rejected batch larger than dead_letter_max_bytes wedges the drain in a permanent block↔retry livelock | `02382df` |
| F04 | medium | drain | Sink::health() is called with no timeout backstop; a hanging health() stalls the entire drain | `02382df` |
| F05 | medium | drain | Retried multi-batch segment re-dead-letters earlier sub-batches, amplifying duplicate dead-letter files and thrashing the cap | `bb7650e` |
| F06 | medium | drain | Dead-letter total_bytes silently undercounts on metadata() failure, bypassing the dead_letter_max_bytes cap | `7acd6ee` |
| F14 | medium | wab | recover_segment renames .wab -> .wab.sealed without fsync_parent_dir, unlike WabSegment::seal — recovered seal not crash-durable | `ed7ad63` |
| F15 | medium | wab | replay_unconfirmed silently skips sealed-but-unconfirmed segments on a per-entry DirEntry error (filter_map(\|e\| e.ok())) | `ed7ad63` |
| F17 | medium | wab | Recovery: no test for the oversized-payload_len boundary (record at exactly MAX_PAYLOAD_HARD_CAP must survive; one over must truncate) | `abe69c2` |
| F20 | medium | worker-queue-metrics | Metrics HTTP server has no read/write timeout — slowloris permanently wedges the unauthenticated endpoint and blinds monitoring | `0530fd2` |
| F21 | medium | worker-queue-metrics | Phase-2 coalesce starves co-located shards on a shared worker (worker_count < shard_count) | `9e5276a` |
| F25 | medium | socket | UnknownMessageType/UnknownDurability decode errors are nacked as the (documented-as-transient, keep-connection-open) InternalError reason, yet the connection is closed | `538129e` |
| F32 | medium | sink | ClickHouse response body read is not bounded by the sink's configured timeout (error path can hang to the drain backstop) | `7e15ac4` |
| F34 | medium | sink | ClickHouse does not percent-decode URL credentials before HTTP basic auth (silent divergence from SQL sinks) | `7e15ac4` |
| F38 | medium | sink | HTTP concurrency: no test asserts a transient mid-batch error actually cancels still-in-flight POSTs | `7daca2f` |
| F39 | medium | sink | HTTP concurrency: no test pins record-to-outcome pairing (right payload in committed vs dead_lettered) under concurrent POSTs | `7daca2f` |
| F43 | medium | client-sdk-ctl | Blocking client sets no socket read/write/connect timeouts; a wedged daemon blocks producers indefinitely | `0eaa4d5` |
| F44 | medium | client-sdk-ctl | Client read_response allocates vec![0u8; payload_len] without the MAX_PAYLOAD_HARD_CAP guard the server applies before allocating | `d04dd87` |
| F48 | medium | core | Public error enums and CommitResult are not #[non_exhaustive]; adding any variant/field after 1.0 is a breaking change | `54b4438` |
| F54 | medium | config | Config-time warn! calls are silently dropped (tracing subscriber initialized after Config::load) | `5a2fa9d` |
| F07 | low | drain | Dead-letter cap accounting omits fixed 60-byte per-file segment overhead, so dead_letter_max_bytes can be exceeded | `7acd6ee` |
| F08 | low | drain | Swallowed rescan() error can wedge the drain blocked despite an operator-freed dead-letter directory | `02382df` |
| F09 | low | drain | dead_letter_full counter and blocked_since reset on every unblock→reblock cycle, inflating the metric and resetting blocked-duration | `ba3b74b` |
| F10 | low | drain | .confirmed sidecar created without explicit 0o600 mode, tripping the daemon's own recovery mode audit under any non-0o077 umask | `d566e36` |
| F13 | low | wab | segment_counters doc comment justifies skipping sealed files as 'matches the historical scan' — stale rationale that masks the counter-reset data-loss bug | `fb02a62` |
| F16 | low | wab | recover_open_segments processes the dead_letter/ directory as if it were a shard directory | `84d55a9` |
| F18 | low | wab | Recovery: the partial-seal sentinel branch during crash recovery is untested | `abe69c2` |
| F22 | low | worker-queue-metrics | accept_latency histogram uses 1ms-floor buckets for a sub-millisecond measurement | `0530fd2` |
| F23 | low | worker-queue-metrics | flush_shard 'WAB is shutting down' comment misdescribes the permanent-shard-offline case | `238a364` |
| F26 | low | socket | Payload and CRC reads are not raced against handler_shutdown, so a mid-frame stall holds a semaphore permit until read_timeout during graceful shutdown | `57a8f7e` |
| F27 | low | socket | Payload/CRC read timeout is a whole-transfer deadline but is documented and metered as a per-byte idle timeout | `57a8f7e` |
| F28 | low | socket | Shutdown-time socket removal is path-based (std::fs::remove_file), inconsistent with the hardened unlinkat-based startup cleanup | `57a8f7e` |
| F29 | low | socket | TLS handshake is not raced against handler_shutdown, so in-flight handshakes block graceful drain up to handshake_timeout | `57a8f7e` |
| F30 | low | socket | umask save/restore in bind_hardened has no RAII guard; an unwind between set and restore would leak the tightened 0o177 umask process-wide | `57a8f7e` |
| F33 | low | sink | ClickHouse split_credentials leaves username-only userinfo in base_url, contradicting the struct invariant | `7e15ac4` |
| F35 | low | sink | ClickHouse content-derived dedup token silently breaks idempotency if sink_max_batch_size changes across a restart | `238a364` |
| F36 | low | sink | ClickHouse RowBinary encoding silently assumes a plain String column with no config validation and contract buried in a private fn comment | `238a364` |
| F45 | low | client-sdk-ctl | weir-sink-sdk re-export doc describes Payload as bytes::Bytes, contradicting the R1 newtype rationale | `238a364` |
| F46 | low | client-sdk-ctl | SDK doc overstates that health() is called after every commit attempt | `238a364` |
| F47 | low | client-sdk-ctl | Client discards the daemon-version byte the server sends on a VersionMismatch Nack | `65a39fb` |
| F49 | low | core | Envelope::new silently truncates payload length via payload.len() as u32, falsifying the documented 'cannot desync' invariant | `d04dd87` |
| F50 | low | core | Header::new takes a payload_len argument that Envelope::new always overwrites — an API that invites desync | `06b7102` |
| F51 | low | core | Payload newtype: PartialEq impls and the Borrow<[u8]> hashmap-key contract are untested | `b446533` |
| F55 | low | config | Feature-gated [server] keys are silently dropped: in KNOWN_SERVER_KEYS but absent from RawConfig on builds without the feature | `b0f2e9b` |
| F56 | low | config | HELP advertises feature-gated CLI flags that fail as generic 'unknown arguments' when the feature is off, with no feature hint | `b0f2e9b` |
| F57 | low | config | Boolean env/CLI values accept only exactly 'true'/'false'; '1', '0', 'TRUE' abort startup | `b0f2e9b` |
| F58 | low | config | log_level is never validated; invalid or empty values silently degrade or disable logging | `03cff76` |
| F59 | low | config | Config derives Debug, exposing sink_url credentials, inconsistent with the project's deliberate bearer_token redaction | `b0f2e9b` |
| F19 | info | wab | macOS data fsync uses F_BARRIERFSYNC (barrier) while directory durability uses plain fsync via sync_all — weaker than F_FULLFSYNC; an explicit, undertested tradeoff on the crown durability path | `238a364` |
| F24 | info | worker-queue-metrics | QueueSender exposes len() with no is_empty() | `e6aa1e7` |
| F52 | info | core | Reserved flags byte is preserved verbatim on decode but never validated to be zero, foreclosing in-version flag evolution | `538129e` |
| F60 | info | config | u64 durations range-checked via `as usize` truncate on 32-bit targets, letting out-of-range values pass | `03cff76` |

## 🟡 Queued safe fixes (in progress)

_(all queued-safe fixes landed)_

## ⚪ Considered & dismissed

- **Refuted — Concurrent HTTP sink discards already-accumulated dead_lettered records when a later record in the same batch hits a transient error** (sink). http.rs:351-358 implements the documented 'transient aborts the segment so the drain retries' contract; at-least-once holds (nothing falsely confirmed) and a serial per-record loop returning Err on the first transient would behave identically, so it is intended behaviour, not a regression.
- **Uncertain — MySQL transient code 1317 (ER_QUERY_INTERRUPTED) is undocumented in module + classify docs and untested** (sink). Safe to fix (doc+test); in the queued list.

## Per-subsystem detail

- [`subsystems/client-sdk-ctl.md`](subsystems/client-sdk-ctl.md)
- [`subsystems/config.md`](subsystems/config.md)
- [`subsystems/core.md`](subsystems/core.md)
- [`subsystems/drain.md`](subsystems/drain.md)
- [`subsystems/sink.md`](subsystems/sink.md)
- [`subsystems/socket.md`](subsystems/socket.md)
- [`subsystems/wab.md`](subsystems/wab.md)
- [`subsystems/worker-queue-metrics.md`](subsystems/worker-queue-metrics.md)
