# Core subsystem audit (weir-core) — sweep 2026-06-14

All five findings hold against the code; four are genuine 1.0-freeze / API-shape
issues and one is a real test-coverage gap whose stated rationale (dedup depends
on `Borrow<[u8]>`) is wrong — no `HashMap`/`HashSet` is keyed by `Payload`.

## Confirmed (real)

### Public error enums and CommitResult are not `#[non_exhaustive]`
- **File:** crates/weir-core/src/error.rs:13 (and :101)
- **Severity:** medium
- **Argument:** `DecodeError` (error.rs:13) and `WeirError` (error.rs:101) are
  plain `pub enum`s with no `#[non_exhaustive]`. A workspace-wide grep for
  `non_exhaustive` returns zero hits, so the same is true of `ClientError`
  (weir-client/src/unix.rs:11), `CommitResult` (weir-sink-sdk/src/lib.rs:106,
  all fields `pub`), and `SinkHealth` (sink-sdk:115). The module doc at
  error.rs:8 literally says "one variant per frame-validation step" — variant
  growth is the documented expectation, so an exhaustive enum guarantees that the
  next validation step is a major bump. Adding a `DecodeError` variant or a
  `CommitResult` field after 1.0 breaks any downstream exhaustive `match` /
  struct-literal. Additive and free now, impossible later. The finding correctly
  excludes the wire enums (`MessageType`/`Durability`/`NackReason`), whose repr
  *is* the contract and whose break mechanism is a `WIRE_VERSION` bump.
- **Verdict:** real — grep confirms zero `#[non_exhaustive]` in the workspace;
  error.rs enums are exhaustive and `CommitResult` fields are all `pub`.

### `Envelope::new` truncates payload length via `as u32`, falsifying "cannot desync"
- **File:** crates/weir-core/src/envelope.rs:223
- **Severity:** low (downgraded from medium — unreachable in the daemon; the
  real defect is the overstated doc guarantee on a published library)
- **Argument:** envelope.rs:221-225 sets `payload_len: payload.len() as u32`
  with no bound check; `encode()` (240-247) then writes the *full* payload. For a
  payload >= 4 GiB the cast wraps, the header declares a wrong length, and a
  decoder desyncs — directly contradicting the doc at 217-219 ("the two cannot
  desync") and 206-209. `MAX_PAYLOAD_HARD_CAP` (16 MiB) is enforced only on the
  decode path (envelope.rs:271) and at socket ingest (connection.rs:159-160),
  never on construction. Not reachable in the daemon (all outbound frames are
  tiny; every inbound payload is capped at 16 MiB at ingest) and proptests only
  use `arb_small_payload` (0..=256 bytes, proptest_envelope.rs:57-61), so the
  path is untested. A real latent footgun + overstated doc claim being frozen
  into the 1.0 public API.
- **Verdict:** real — `payload.len() as u32` at envelope.rs:223 is unchecked and
  the doc at 217-219 promises an invariant the cast can violate; severity lowered
  because a single >= 4 GiB payload is impractical and the daemon never hits it.

### `Header::new` takes a `payload_len` that `Envelope::new` always overwrites
- **File:** crates/weir-core/src/envelope.rs:98
- **Severity:** low
- **Argument:** Every production caller computes `payload.len() as u32` and
  passes it to `Header::new` only to have `Envelope::new` overwrite it
  (weir-client/src/unix.rs:90-92; server connection.rs:431-437). For bare
  header-only encodes (`Header::encode` without an `Envelope`, e.g.
  connection.rs:680-686), the caller can pass any value, producing a header whose
  declared length matches no payload — the exact desync the encapsulation work
  was meant to foreclose. Dropping `payload_len` from `Header::new` (default 0,
  set authoritatively by `Envelope::new`) would make the correct value the only
  reachable one.
- **Verdict:** real — callers at unix.rs:90-92 and connection.rs:431-437 pass a
  value that envelope.rs:222-225 discards; the arg is a desync-inviting no-op on
  the dominant path.

### Payload `PartialEq` impls and the `Borrow<[u8]>` contract are untested
- **File:** crates/weir-core/src/payload.rs:61
- **Severity:** low (the coverage gap is real; the stated rationale is not)
- **Argument:** The three `PartialEq` impls (payload.rs:99/105/111) and the
  `Borrow<[u8]>` impl (payload.rs:61) have no direct test — the test module
  covers `from_static`/`from_vec`/Debug/clone/`Bytes` round-trip only. The
  Borrow/Hash agreement holds *today* (derive `Hash` at payload.rs:16 hashes the
  inner `Bytes`, which `Borrow` exposes as `&[u8]`), but it is unpinned, so a
  future change could silently break it. HOWEVER the finding's load-bearing
  rationale — "the drain's sha256 dedup and any byte-keyed lookup rely on this
  consistency" — does NOT hold: a grep for `HashMap<.*Payload` / `HashSet<.*
  Payload` returns nothing, and the dedup path computes content sha256
  (sink/clickhouse.rs:72 `dedup_token(&[Payload])` via `Sha256`; sink/http.rs:47)
  — it never uses `Payload` as a std hashmap key looked up by `&[u8]`. So a test
  is still worthwhile (pins a real public-API contract), but no current code
  depends on the `Borrow` consistency.
- **Verdict:** real (the impls/Borrow are genuinely untested) — but the dedup
  dependency is refuted: no `HashMap`/`HashSet` is keyed by `Payload`; dedup uses
  content sha256, not std `Borrow`/`Hash`.

### Reserved `flags` byte is preserved on decode, never validated to be zero
- **File:** crates/weir-core/src/envelope.rs:191
- **Severity:** info
- **Argument:** `Header::decode` reads `flags = buf[7]` (envelope.rs:191) and
  stores it with no `== 0` check, despite the field doc (envelope.rs:91), lib
  doc, and docs/wire_protocol.md all calling flags "reserved; zero on write". The
  proptest at proptest_envelope.rs:170/180 asserts arbitrary nonzero flags
  round-trip cleanly, confirming a deliberate "preserve, don't reject" choice.
  Consequence for 1.0: with `WIRE_VERSION` fixed at 1, a v1 daemon will silently
  accept-and-ignore any future flag bit rather than reject it, so flag semantics
  can only ever be added via a `WIRE_VERSION` bump, never within v1. Rejecting
  nonzero flags now (while the field is unused) would preserve in-version flag
  evolution. Worth an explicit decision/comment.
- **Verdict:** real — envelope.rs:191 stores `buf[7]` unchecked and
  proptest_envelope.rs:170/180 proves nonzero flags survive, foreclosing
  in-version flag evolution.

## Refuted / dismissed

None of the five findings was fully refuted. The only refuted *element* is the
rationale inside the Payload test-coverage finding: the claim that the drain
dedup path depends on `Payload`'s `Borrow<[u8]>` / `Hash` consistency is false —
no `HashMap`/`HashSet` keys on `Payload` (grep clean) and dedup is content
sha256 (sink/clickhouse.rs:72, sink/http.rs:47). The underlying coverage gap
itself remains real.
