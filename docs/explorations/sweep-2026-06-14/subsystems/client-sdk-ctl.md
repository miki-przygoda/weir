# Subsystem audit: client / sink-sdk / ctl

All 8 findings verified against code: 7 confirmed real (1 with severity adjusted down), 1 doc finding confirmed. None refuted — every cited defect reproduces in the source.

## Confirmed (real)

### 1. `weir-ctl dl list` / `dl drop` always report an empty store (wrong suffix filter)
- **File:** `crates/weir-ctl/src/main.rs:303`
- **Severity:** high
- **Argument:** `dl_segments` filters with `n.starts_with("dl_") && n.ends_with(".wab")` (main.rs:303). But `DeadLetterWriter::write_records` creates `dl_NNNNNNNN.wab` then immediately calls `seg.seal()` (dead_letter.rs:69-75), which renames to `dl_NNNNNNNN.wab.sealed` (segment.rs:255-264 + `sealed_path_for` 269-279; dead_letter.rs:6 documents `.wab.sealed`). `.wab.sealed` does NOT match `.ends_with(".wab")`, so in steady state `dl_segments` returns empty: `dl list` always prints "dead-letter store is empty" (main.rs:316-318) and `dl drop --yes` always prints "nothing to drop" (main.rs:338-340). The same file proves the inconsistency: `cmd_segments` (main.rs:215-220) tests `.wab.confirmed` / `.wab.sealed` / `.wab` in order and counts dead-letter bytes, so `weir-ctl segments` shows a non-empty dead-letter store while `dl list` says empty. `dl_segments` has no test.
- **Verdict:** real — main.rs:303 requires `.wab` suffix; sealed files end `.wab.sealed` (dead_letter.rs:69 + segment.rs:258), so the filter never matches a steady-state dead-letter file.

### 2. `CommitResult` partition invariant is unenforced; drain confirms+deletes the segment unconditionally
- **File:** `crates/weir-sink-sdk/src/lib.rs:106`
- **Severity:** high (downgraded from critical)
- **Argument:** `CommitResult<R> { committed, dead_lettered }` (lib.rs:104-111) is a plain struct, public fields, no constructor, no enforced/asserted invariant that `committed ∪ dead_lettered` covers the batch. The drain's `commit_batch` `Ok(commit_result)` path uses `committed.len()` only for a metric (mod.rs:622), writes `dead_lettered` (mod.rs:624-660), then returns `BatchResult::Ok` (mod.rs:662) with NO reconciliation against `payloads.len()`. `BatchResult::Ok` leads to segment confirm + delete. A third-party `Sink` that silently omits a record from both vectors would have that record neither delivered nor dead-lettered, yet the segment is deleted — a false confirm and silent loss, violating the crown "an ack is never a false ack." All first-party sinks partition correctly (noop.rs:32-34 `committed: batch`; postgres.rs:384-401 either all committed or all dead-lettered; http.rs:335-364 pushes each record to exactly one vector), so this is masked today. Note the drain already added defensive transient-preservation for the dead-letter-write-failure case (mod.rs:648-658), demonstrating the team's own pattern — but there is no analogous guard for an under-covered `CommitResult`.
- **Severity rationale:** downgraded to high because no in-tree sink triggers it; exploitation requires a buggy/naive third-party `Sink`. The missing guard rail at the published SDK trust boundary is real, but present-day exploitability is gated on external misbehavior, so "critical" overstates it.
- **Verdict:** real — no code path computes `committed.len() + dead_lettered.len() == payloads.len()` before `BatchResult::Ok` (mod.rs:612-662), and `CommitResult` (lib.rs:106-111) offers no checked constructor.

### 3. `SinkRecord::into_payload` is documented as the dead-letter recovery path but bypassed for whole-batch errors
- **File:** `crates/weir-sink-sdk/src/lib.rs:71`
- **Severity:** high
- **Argument:** `into_payload`'s doc (lib.rs:71) says "Recover the raw payload bytes (used when a record must be dead-lettered)." The drain calls `into_payload` only for the per-record `dead_lettered` list inside a SUCCESSFUL `CommitResult` (mod.rs:625-629). On a whole-batch permanent error the drain writes the raw original `payloads` slice via `dead_letter.write_records(payloads)` (mod.rs:696), bypassing `into_payload`; the transient retry path (mod.rs:665-682) never round-trips either. So for any non-identity `SinkRecord` (one that transforms bytes in `from_payload`), dead-lettered contents on a permanent error are the original segment bytes, not what `into_payload` would produce — the documented contract is false on the most important dead-letter path. The generic is also premature: the only `SinkRecord` impl in the workspace is the identity `impl SinkRecord for Payload` (lib.rs:77-85), so the divergence is latent today.
- **Verdict:** real — mod.rs:696 dead-letters raw `payloads`, not `into_payload` output; contract at lib.rs:71 holds only for the identity record.

### 4. Blocking client sets no read/write/connect timeouts; a wedged daemon blocks producers indefinitely
- **File:** `crates/weir-client/src/unix.rs:177`
- **Severity:** medium
- **Argument:** `connect` / `connect_with_default` / `from_stream` (unix.rs:177-207) never call `set_read_timeout` / `set_write_timeout`, and there is no connect timeout. `connect_tls` (tls.rs:37-74) likewise uses a plain blocking `TcpStream::connect` (tls.rs:70) with no timeout. `read_response` (unix.rs:136-160) therefore blocks forever if the daemon accepts the connection and reads the push but never replies (flusher hang before ack-timeout fires, SIGSTOP'd daemon, half-open TCP on TLS). The author knows the pattern — `weir-ctl`'s scrape sets a 5s read timeout (main.rs:370). For a client in a producer hot path, the unbounded block is a real availability hazard.
- **Verdict:** real — no `set_read_timeout`/`set_write_timeout`/connect-timeout call exists in unix.rs or tls.rs; `read_response` (unix.rs:138/146/150) uses blocking `read_exact`.

### 5. Client `read_response` allocates `vec![0u8; payload_len]` without the `MAX_PAYLOAD_HARD_CAP` guard the server applies
- **File:** `crates/weir-client/src/unix.rs:144`
- **Severity:** medium
- **Argument:** `read_response` decodes the header then does `vec![0u8; payload_len]` (unix.rs:143-144) with `payload_len` up to `u32::MAX` (~4 GiB) before any read. The server's `Envelope::decode` rejects `payload_len > MAX_PAYLOAD_HARD_CAP` (16 MiB) BEFORE allocating (envelope.rs:270-276); `MAX_PAYLOAD_HARD_CAP` is a public const re-exported from `weir_core` (lib.rs:34, version.rs:13) and the client already depends on `weir-core`, so the guard is one comparison away. The header CRC covers only bytes [0..12] (envelope.rs:24), so a coincidentally-valid header after a framing slip, or a buggy/compromised daemon, can declare a multi-GiB payload and force a huge allocation even though the next `read_exact` would hit EOF. Real daemon responses (Ack/Nack/HealthCheckResponse) are tiny, so this is not a practical attack surface today. The proptest cannot catch it: random bytes (proptest_client.rs) never produce a valid header CRC, so the large-but-valid `payload_len` path is unreachable in the test.
- **Verdict:** real — unix.rs:144 allocates `payload_len` bytes with no cap, an asymmetry with envelope.rs:271-276; only mitigated by tiny real responses.

## Refuted / dismissed

(No findings refuted. The two doc findings below are confirmed-real documentation defects.)

### 6. SDK re-export doc describes `Payload` as `bytes::Bytes`, contradicting the R1 newtype rationale
- **File:** `crates/weir-sink-sdk/src/lib.rs:60`
- **Severity:** low
- **Argument:** The doc on `pub use weir_core::Payload` (lib.rs:60-62) reads "Opaque record payload bytes — a ref-counted `bytes::Bytes` (re-exported from `weir-core`)." After R1, `Payload` is `pub struct Payload(Bytes)` (payload.rs:17), explicitly a newtype, not an alias or a re-export of the `bytes` crate — payload.rs:7-15 states the reason (avoid leaking `bytes` semver into weir 1.0). A sink author trusting the SDK comment would expect to pass a raw `bytes::Bytes` (won't compile without `.into()`) or to have `Bytes` inherent methods (only the `Deref<Target=[u8]>` subset is available). This is the lone stale R1 survivor in public, soon-to-be-frozen API doc.
- **Verdict:** real (doc) — lib.rs:60-62 says `bytes::Bytes`; payload.rs:16-17 defines a newtype `Payload(Bytes)`.

### 7. SDK doc overstates that `health()` is called after every commit attempt
- **File:** `crates/weir-sink-sdk/src/lib.rs:149`
- **Severity:** low
- **Argument:** `Sink::health` doc (lib.rs:149-151) says "called after every commit attempt and on a wall-clock interval." In the drain, `sink.health()` runs after `process_segment` only in the `Draining` state (mod.rs:296-297). In `RetryingTransient`, `process_segment` runs (mod.rs:318-335) with no following `health()` call, so during a retry storm health is refreshed only by the periodic poll (mod.rs:277-282 idle poll, and the blocked-full poll at mod.rs:379), not after each attempt. The wording overstates the per-attempt guarantee.
- **Verdict:** real (doc) — mod.rs:303-336 `RetryingTransient` calls `process_segment` with no trailing `sink.health()`; only `Draining` (mod.rs:296) does.

### 8. Client discards the daemon-version byte the server sends on a `VersionMismatch` Nack
- **File:** `crates/weir-client/src/unix.rs:210`
- **Severity:** low
- **Argument:** The server appends the daemon's `WIRE_VERSION` as the second Nack-payload byte specifically so the client can render "daemon is on wire protocol vN; this client is on vM" (connection.rs:404-410 `nack_for_decode_error` returns `&[WIRE_VERSION]`; 418-421 documents `send_nack`'s `[reason] ++ extra`). The client's `nack_error` (unix.rs:210-218) reads only `payload.first()` and maps to `ClientError::Nack(NackReason::VersionMismatch)`; the `Display` impl (unix.rs:28) prints `server nack: VersionMismatch`. The cross-crate contract the server pays for is unfulfilled — an upgrading operator gets an opaque error.
- **Verdict:** real — connection.rs:409 sends `&[WIRE_VERSION]` as the second byte; unix.rs:211 reads only `payload.first()`, dropping it.
