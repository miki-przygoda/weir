# Phase 3 · Stream C — `Payload` → `bytes::Bytes` Migration — Implementation Plan

> **For agentic workers:** This is a compiler-driven type migration. Flip the alias, then fix every site the compiler flags. Keep all tests green; build + clippy clean across feature combos AND with `--features bench-trace`. Commit in logical chunks.

**Goal:** Change `pub type Payload = Vec<u8>` (weir-core) to `bytes::Bytes` so payload clones become O(1) ref-count bumps. This eliminates the per-batch `.iter().cloned()` copy in the drain's `commit_batch` and the HTTP sink's `payload.to_vec()`, and reduces allocator churn through the pipeline.

**Honest scope of the win:** This does NOT change push→ack latency (Stream A showed fsync dominates that, and the socket read still allocates once to receive bytes). The win is **drain/sink-side throughput + allocator pressure**. It is also **API-timed**: `Payload` is a `weir-core` public type that freezes at the 1.0 publish (Phase 5), so migrating now — before the freeze — avoids a breaking change later.

**Tech stack:** Rust, `bytes` crate (`Bytes` / `BytesMut`), `reqwest::Body` (which has `From<Bytes>`), existing wire codec + WAB segment format.

## Invariants (MUST hold)

1. **Wire format + WAB on-disk format are BYTE-IDENTICAL.** `Bytes` is only an in-memory container — the bytes written to the socket and to segment files are unchanged. All CRC32 computations run over the same bytes (`Bytes: Deref<Target=[u8]>`). The wire round-trip, crash-recovery, and cargo-fuzz trust-boundary tests are the safety net and must pass unchanged.
2. **`MAX_PAYLOAD_HARD_CAP` and the pre-allocation DoS-hardening order are unchanged** — still cap-check before allocating, same as today.
3. **`impl SinkRecord for Payload` stays** (it becomes `impl SinkRecord for Bytes`); the pass-through `from_payload`/`into_payload` are identity.

## Migration strategy (the two site classes)

- **BUILD sites** (mutable accumulation, then freeze to immutable `Bytes`): read the bytes into a `Vec<u8>`/`BytesMut` as today, then convert. `Bytes::from(vec)` is O(1) (takes ownership of the Vec's allocation). Prefer this minimal form: keep the existing `let mut buf = vec![0u8; len]; read_exact(&mut buf)?;` and end with `let payload: Payload = Bytes::from(buf);`. Known build sites:
  - Socket read in `socket/connection.rs` (`handle_connection`, the `vec![0u8; payload_len]` payload read).
  - WAB segment read-back: `SegmentReader::next()` in `wab/mod.rs` (`vec![0u8; payload_len]` → return `Bytes::from(buf)`).
  - Wire `Envelope::decode` in `weir-core/src/envelope.rs`: produce the payload as `Bytes::copy_from_slice(&buf[payload_range])` (or `BytesMut::split_to(...).freeze()` if decoding from an owned `BytesMut`). The CRC is computed over the same byte range as today.
  - `weir-client` payload construction (the `push(&[u8], ...)` path): `Bytes::copy_from_slice(data)` at the API boundary, or accept `impl Into<Bytes>`.
- **CLONE / read sites** (now cheap or unchanged):
  - Drain `commit_batch` `.iter().cloned()` (drain/mod.rs): unchanged source, now an O(1) ref-bump instead of a heap copy. The original slice stays available for dead-lettering as the comment requires — now for free.
  - HTTP sink: `req.body(payload.to_vec())` → `req.body(payload.clone())` (or move `payload`), using `reqwest::Body: From<Bytes>` — no copy.
  - Everything that reads a payload as `&[u8]` (CRC, `writer.write_record(&payload)`, clickhouse `encode_rowbinary`'s `extend_from_slice(&payload)`, SQL sinks' value binding): unchanged — `Bytes` derefs to `&[u8]`.

---

### Task 1: Add `bytes` to weir-core and flip the alias

**Files:** `crates/weir-core/Cargo.toml`, `crates/weir-core/src/payload.rs`

- [ ] **Step 1:** Add `bytes = "1"` to `[dependencies]` in `weir-core/Cargo.toml`. (Note: weir-core deliberately keeps deps minimal — `bytes` is a tiny, ubiquitous, zero-transitive-dep crate, acceptable for a published core. Add a one-line comment saying why, mirroring the existing `thiserror`-exclusion comment style.)
- [ ] **Step 2:** Change `payload.rs`:
```rust
/// Opaque payload bytes. Ref-counted `Bytes` so clones through the drain /
/// sink path are O(1) instead of heap copies. All weir-core and weir-server
/// APIs use `Payload`, never `Vec<u8>` directly for payload data.
pub type Payload = bytes::Bytes;
```
- [ ] **Step 3:** `cargo build -p weir-core` — fix any weir-core-internal sites (notably `Envelope::decode`/`encode` and `Envelope::new`). For `Envelope::new(header, payload: impl Into<Payload>)` consider accepting `impl Into<Bytes>` so existing `Vec<u8>` and `&[u8]` callers keep working ergonomically; otherwise update call sites. Build to green: `cargo build -p weir-core`.
- [ ] **Step 4:** `cargo test -p weir-core` — the wire-codec round-trip + proptest tests must pass (proves byte-identical encode/decode). Commit — `feat(core): Payload = bytes::Bytes (O(1) payload clones)`

---

### Task 2: Fix weir-server build + eliminate the copies

**Files:** `socket/connection.rs`, `wab/mod.rs` (`SegmentReader`), `drain/mod.rs`, `sink/http.rs`, `sink/{mysql,postgres,clickhouse,noop}.rs` as flagged, any test helpers constructing payloads.

- [ ] **Step 1:** `cargo build -p weir-server` and follow the compiler. Apply the build-site / clone-site strategy above. The two intentional wins:
  - **Drain `commit_batch`:** confirm the `.iter().cloned()` now compiles as a `Bytes` clone (cheap). Keep the dead-letter-retention behaviour.
  - **HTTP sink:** replace `payload.to_vec()` with the no-copy `reqwest::Body` path.
- [ ] **Step 2:** Update any test helpers / fixtures that build a `WorkUnit`/payload from `b"..."` or `vec![..]` — wrap with `Bytes::from(...)` / `Bytes::from_static(b"...")`.
- [ ] **Step 3:** Build + clippy clean across the matrix:
  - `cargo build -p weir-server` / `--features bench-trace` / `--all-features` — PASS.
  - `cargo clippy --all-targets -- -D warnings` and `... --all-features ...` and `... --features bench-trace ...` — PASS.
- [ ] **Step 4:** `cargo test -p weir-server --bin weir-server -- --test-threads=1` — PASS. Commit — `feat(server): adopt Bytes payload; drop drain clone + HTTP to_vec copies`

---

### Task 3: Fix weir-client + the integration/fuzz surfaces

**Files:** `crates/weir-client/src/*`, `crates/weir-server/tests/*`, `crates/weir-server/fuzz/*` (if present), `weir-testkit` if it constructs payloads.

- [ ] **Step 1:** `cargo build -p weir-client` and fix the payload construction in `push` (use `Bytes::copy_from_slice(data)` at the boundary; the public `push(&[u8], ...)` signature should stay unchanged so client callers are unaffected). `cargo test -p weir-client` — PASS (incl. its proptest).
- [ ] **Step 2:** `cargo build --workspace --all-targets` and `--all-targets --all-features` — PASS. Fix any remaining test/integration sites.
- [ ] **Step 3:** Verify the fuzz targets still build (they decode untrusted bytes → payloads): `cargo +nightly fuzz build` if nightly+cargo-fuzz is available, else `cargo build` the fuzz crate. If the toolchain isn't available, note it and rely on the in-tree fuzz-corpus regression tests.
- [ ] **Step 4:** Commit — `feat(client+tests): Bytes payload across client, tests, fuzz`

---

### Task 4: Full verification + prove no regression / copies gone

- [ ] **Step 1:** `cargo fmt --all -- --check` — PASS.
- [ ] **Step 2:** Full suites:
  - `cargo test -p weir-server --bin weir-server -- --test-threads=1` — PASS.
  - `cargo test -p weir-server --test system -- --test-threads=1` — PASS (esp. recovery/replay tests — they prove on-disk format identity).
  - `cargo test -p weir-server --test load --release -- --test-threads=1` — PASS, and eyeball throughput vs `docs/benchmarks/latest.md`: **no regression** (Bytes::from(vec) is O(1); the push path allocates the same as before). The compression-ratio scenario exercises the drain commit path that lost the clone.
  - `cargo test -p weir-core` and `cargo test -p weir-client` — PASS.
- [ ] **Step 3:** Document the eliminated copies in the commit message: the drain `commit_batch` per-record `Vec<u8>` clone and the HTTP `to_vec()` are now O(1) ref-bumps. (A `dhat` allocation count is a possible follow-up but not required here — code inspection + the regression-free benchmark suffices for this stream.)
- [ ] **Step 4:** Commit — `chore(bytes): fmt + full-suite verification; document eliminated copies [skip ci]`

---

## Done criteria

- `Payload = bytes::Bytes`; the drain `commit_batch` clone and the HTTP `to_vec()` are now O(1).
- Wire + WAB on-disk formats byte-identical — proven by the codec round-trip, recovery/replay, and fuzz/proptest suites passing unchanged.
- Default, `bench-trace`, and `all-features` builds clippy-clean; weir-core/weir-client/weir-server + system + load suites all green; no throughput regression vs the Stream A baseline.
- `weir-client::push(&[u8])` public signature unchanged (client callers unaffected).
