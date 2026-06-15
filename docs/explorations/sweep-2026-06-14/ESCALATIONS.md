# Escalations — your decisions needed (codebase sweep 2026-06-14)

> The findings from the sweep that I did **not** change autonomously: each needs a product/API call or a redesign. Nothing here is load-bearing-broken. Fixed items + queued-safe items live in [`FINDINGS.md`](FINDINGS.md).

**3 open decisions, grouped by what they touch.** Each group's tag marks whether it's an irreversible 1.0-freeze gate (decide before 1.0) or a reversible fix (land anytime). Jump to a group:

- **[Group 2 — Public Rust API freeze](#group-2--public-rust-api-freeze)** — _irreversible · decide before 1.0_ (F41, F42, F48)

---

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
