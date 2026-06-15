# Escalations — your decisions needed (codebase sweep 2026-06-14)

> The findings from the sweep that I did **not** change autonomously: each needs a product/API call or a redesign. Nothing here is load-bearing-broken. Fixed items + queued-safe items live in [`FINDINGS.md`](FINDINGS.md).

**10 open decisions, grouped by what they touch.** Groups 1–2 are irreversible 1.0-freeze gates (decide before 1.0); Group 3 is reversible (land anytime). Jump to a group:

- **[Group 1 — Wire-protocol v1 freeze](#group-1--wire-protocol-v1-freeze)** — _irreversible · decide before 1.0_ (F25, F50, F52)
- **[Group 2 — Public Rust API freeze](#group-2--public-rust-api-freeze)** — _irreversible · decide before 1.0_ (F41, F42, F48)
- **[Group 3 — Reversible fixes (not freeze-gated)](#group-3--reversible-fixes-not-freeze-gated)** — _reversible · land anytime_ (F05, F43, F54, F24)

---

## Group 1 — Wire-protocol v1 freeze
_irreversible · decide before 1.0_

These lock the on-the-wire byte contract. Best decided together as one wire-freeze session, alongside the deferred wire-freeze hooks (reserved Nack-reason byte, language-neutral conformance vectors). Once 1.0 ships at WIRE_VERSION 1, changing any of these needs a version bump.

### F25 — UnknownMessageType/UnknownDurability decode errors are nacked as the (documented-as-transient, keep-connection-open) InternalError reason, yet the connection is closed  
*(medium · error-handling · socket · `crates/weir-server/src/socket/connection.rs:414`)*

UnknownMessageType/UnknownDurability (envelope.rs:188/190) arise on a CRC-valid header (the CRC check at envelope.rs:178-185 runs first), so they are permanent client protocol errors. nack_for_decode_error's `_ =>` arm (connection.rs:414) maps them to WireNack::InternalError + MetricNack::internal_error; the decode site then closes the connection (connection.rs:150). docs/wire_protocol.md:147 documents Nack(InternalError) as 'open — transient', so the wire contract is contradicted, the case is indistinguishable from a real pipeline InternalError (retry-loops forever), and the internal_error metric is polluted by malformed client input.

➡️ UnknownMessageType/UnknownDurability are nacked as `InternalError` (documented transient/keep-open) but the connection is then CLOSED — contradicting the wire contract. The clean fix needs either a dedicated Nack reason (a wire change) or deciding these stay open (the framing IS intact — valid header, unknown enum). **Recommend (freeze):** add a reserved `UnknownMessage` Nack reason in the wire-freeze cluster, or document+keep-open.

### F50 — Header::new takes a payload_len argument that Envelope::new always overwrites — an API that invites desync  
*(low · decision · core · `crates/weir-core/src/envelope.rs:98`)*

Every production caller computes payload.len() as u32 and passes it to Header::new only for Envelope::new to overwrite it (unix.rs:90-92, connection.rs:431-437). For bare Header::encode without an Envelope (connection.rs:680-686), a caller can pass any value and produce a header whose declared length matches no payload — the desync the encapsulation work was meant to prevent. Dropping payload_len from Header::new (default 0, set by Envelope::new) would make the correct value the only reachable one.

➡️ `Header::new` takes a `payload_len` that `Envelope::new` always overwrites, and a bare `Header::encode` can still desync. **Recommend (freeze):** drop `payload_len` from `Header::new` so it can only be set via `Envelope` (which derives it). Pairs with R2/F49.

### F52 — Reserved flags byte is preserved verbatim on decode but never validated to be zero, foreclosing in-version flag evolution  
*(info · decision · core · `crates/weir-core/src/envelope.rs:191`)*

Header::decode reads flags = buf[7] (envelope.rs:191) and stores it without a == 0 check, despite the field doc (envelope.rs:91), lib doc, and docs/wire_protocol.md calling flags 'reserved; zero on write'. proptest_envelope.rs:170/180 asserts arbitrary nonzero flags round-trip, confirming a deliberate preserve-don't-reject choice. With WIRE_VERSION fixed at 1, a v1 daemon silently accepts and ignores any future flag bit, so flag semantics can only be added via a WIRE_VERSION bump. Worth an explicit decision/comment; rejecting nonzero flags now would preserve in-version flag evolution.

➡️ Decode preserves arbitrary `flags` without checking zero, so a v1 daemon silently ignores future flag bits. **Recommend (freeze):** decide flag-evolution policy — reject nonzero now (clean error when a flag is added later) vs keep preserve-and-ignore. Tied to the wire-freeze cluster + reserved-Nack-byte decision.

## Group 2 — Public Rust API freeze
_irreversible · decide before 1.0_

These shape the public Rust types and the Sink/SDK contract before they're locked. CommitResult threads through F41 (its invariant) and F48 (its exhaustiveness); F42 is the SinkRecord::into_payload half of the same `Sink::commit` contract. `#[non_exhaustive]` + a validating constructor are free now and impossible after 1.0.

### F41 — CommitResult partition invariant (committed ∪ dead_lettered = batch) is unenforced; the drain confirms+deletes the segment unconditionally on Ok  
*(high · redesign · client-sdk-ctl · `crates/weir-sink-sdk/src/lib.rs:106`)*

CommitResult<R> { committed, dead_lettered } (lib.rs:104-111) is a plain public-field struct with no constructor and no enforced invariant. The drain's Ok(commit_result) path uses committed.len() only for a metric (mod.rs:622), writes dead_lettered (mod.rs:624-660), then returns BatchResult::Ok (mod.rs:662) with NO reconciliation against payloads.len(); BatchResult::Ok confirms and deletes the segment. A third-party Sink that omits a record from both vectors causes a false confirm / silent loss, violating the crown invariant. First-party sinks (noop/postgres/http) all partition correctly, masking it today. The drain already preserves the segment defensively on a dead-letter write failure (mod.rs:648-658) but has no analogous guard for an under-covered CommitResult.

➡️ **Mitigated tonight by F02** — the drain now refuses to confirm a CommitResult whose committed+dead_lettered don't cover the batch. The deeper fix (encode the partition invariant in the SDK type: a validating constructor instead of public fields) is an irreversible 1.0 SDK-API choice. **Recommend:** fold into the freeze decisions; low urgency now that F02 guards the runtime.

### F42 — SinkRecord::into_payload is documented as the dead-letter recovery path but is bypassed for whole-batch permanent (and transient) errors  
*(high · redesign · client-sdk-ctl · `crates/weir-sink-sdk/src/lib.rs:71`)*

into_payload's doc (lib.rs:71) says it is used when a record must be dead-lettered, but the drain calls into_payload only for the per-record dead_lettered list of a successful CommitResult (mod.rs:625-629). On a whole-batch permanent error the drain writes the raw original payloads slice via dead_letter.write_records(payloads) (mod.rs:696), bypassing into_payload; the transient path never round-trips either. For a non-identity SinkRecord the dead-lettered contents on a permanent error are the original segment bytes, not into_payload output. The only impl in the workspace is the identity impl SinkRecord for Payload (lib.rs:77-85), so the divergence is latent today.

➡️ Related to F41. Whole-batch permanent/transient paths dead-letter raw payloads, bypassing `SinkRecord::into_payload`. Harmless for the built-in `Payload` record (identity transform); only matters for a third-party custom `Record`. **Recommend:** decide with the `Sink::commit` signature freeze — route all dead-letter paths through `into_payload`, or narrow its doc to 'per-record-result only'.

### F48 — Public error enums and CommitResult are not #[non_exhaustive]; adding any variant/field after 1.0 is a breaking change  
*(medium · decision · core · `crates/weir-core/src/error.rs:13`)*

DecodeError (error.rs:13) and WeirError (error.rs:101) are exhaustive public enums with no #[non_exhaustive]; the same holds for ClientError (weir-client/unix.rs:11), CommitResult (sink-sdk:106, all-public fields) and SinkHealth (sink-sdk:115). The module doc at error.rs:8 says 'one variant per frame-validation step', so variant growth is the documented expectation — making every future validation step a major bump. Mark these #[non_exhaustive] before 1.0 (free now, impossible later). Correctly excludes the wire enums MessageType/Durability/NackReason whose repr is the contract.

➡️ Public error enums (`DecodeError`,`WeirError`,`ClientError`) + `SinkHealth` + `CommitResult` aren't `#[non_exhaustive]`, so any post-1.0 variant/field is breaking; the error model explicitly expects variant growth. **Recommend (freeze):** mark the error enums + `SinkHealth` `#[non_exhaustive]` before 1.0; pair `CommitResult` with F41.

## Group 3 — Reversible fixes (not freeze-gated)
_reversible · land anytime_

Independent fixes, each touching a different subsystem (client, config, drain, queue). None locks an API or the wire, so any of these can land before OR after 1.0 — pick them off when convenient. F05 is the only one needing real work (a drain segment-retry redesign); F24 is trivial.

### F05 — Retried multi-batch segment re-dead-letters earlier sub-batches, amplifying duplicate dead-letter files and thrashing the cap  
*(medium · redesign · drain · `crates/weir-server/src/drain/mod.rs:529`)*

process_segment commits sub-batches sequentially (527-547). If an early sub-batch dead-letters successfully and a later one returns Transient/Blocked, the whole segment is preserved and retried from record 0 (re-opened at 485); the early sub-batch dead-letters again via write_records (no dedup, dead_letter.rs:64-83), duplicating records/files and, when Blocked, feeding a cap-overshoot loop.

➡️ Multi-batch segment retry re-dead-letters earlier sub-batches → duplicate dead-letter files. Duplicates are noise in a terminal inspection store, not data loss. A real fix needs per-sub-batch progress tracking within a segment retry (drain redesign). **Recommend:** defer post-1.0 unless observed.

### F43 — Blocking client sets no socket read/write/connect timeouts; a wedged daemon blocks producers indefinitely  
*(medium · guard · client-sdk-ctl · `crates/weir-client/src/unix.rs:177`)*

connect/connect_with_default/from_stream (unix.rs:177-207) never set read or write timeouts and there is no connect timeout; connect_tls (tls.rs:37-74) likewise uses a plain blocking TcpStream::connect (tls.rs:70). read_response (unix.rs:136-160) blocks forever if the daemon accepts then never replies (flusher hang, SIGSTOP, half-open TCP). weir-ctl's scrape sets a 5s read timeout (main.rs:370), so the author knows the pattern; for a producer hot-path client the unbounded block is an availability hazard.

➡️ The blocking client sets no read/write/connect timeouts, so a wedged daemon blocks a producer forever. A fix needs a DEFAULT timeout value (a judgment call — too short breaks slow-but-legit Sync acks under load) and/or a configurable setter. **Recommend:** add `set_read_timeout`/`set_write_timeout` setters (opt-in, no default behaviour change) + document; optionally a generous default (~30s).

### F54 — Config-time warn! calls are silently dropped (tracing subscriber initialized after Config::load)  
*(medium · bug-fix · config · `crates/weir-server/src/main.rs:185`)*

Config::load() runs at main.rs:185; the only subscriber init is at main.rs:187-192, after. warn! emitted during config loading (file.rs:191/199 unknown-key net, mod.rs:596 dead_letter advisory) has no subscriber and is discarded, so TOML typos silently get defaults.

➡️ Config-load `warn!`s (unknown TOML keys, the dead_letter <1MiB advisory) are discarded because the tracing subscriber is initialised AFTER `Config::load()` — so a TOML typo silently takes defaults. Fix needs either collecting the warnings and emitting them post-init, or a reloadable filter (bootstrap level → reload to config.log_level). **Recommend:** collect-and-emit.

### F24 — QueueSender exposes len() with no is_empty()  
*(info · decision · worker-queue-metrics · `crates/weir-server/src/queue.rs:55`)*

QueueSender::len() is public (queue.rs:55-57, used by weir_queue_depth poll per queue.rs:53-54) with no companion is_empty() — the classic clippy::len_without_is_empty wart, latent because the workspace has no clippy lint config (no [lints] table in any Cargo.toml, no #![deny(clippy::...)] in lib.rs). len() also sums across partitions (queue.rs:56), so it is a cross-partition in-flight total, not a slot count — mildly misleading. Worth a deliberate decision before the 1.0 API freeze.

➡️ `QueueSender::len()` has no `is_empty()` (clippy::len_without_is_empty), latent only because there's no clippy lint config. Trivial + safe. **Recommend:** add `is_empty()` (could also just be a queued-safe fix).
