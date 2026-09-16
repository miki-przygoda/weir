# Tamper-evident WAB — a chain over sealed segments

**Status:** Design. No code written.
**Date:** 2026-09-15.
**Target:** 4.0.0 (the major is already forced independently — see §1.3).
**Proposed crate:** `weir-attest`, published, `weir-ctl attest` as the operator surface.

---

## 1. What this is for, and what it is not for

### 1.1 The gap, in the project's own words

`docs/security/threat-model.md`, "Out of scope":

> **Signed audit log** — Records are written verbatim; no per-record signature or
> hash chain beyond the per-record CRC. **CRC catches accidental corruption, not
> deliberate tampering.**

That sentence is the whole motivation. weir's crown invariant — *an ack is never
a false ack* — is a promise about the daemon's own behaviour under crashes,
fsync failures and power loss. It says nothing about what happened to the bytes
afterwards. A CRC32 is a error-*detection* code, not a MAC: anyone who edits a
record can recompute the CRC in microseconds and the file reads back clean.

### 1.2 The honest threat model — read this before believing the feature

This is the section most likely to be overclaimed later, so it is stated first.

**In scope:**

- **Offline tampering.** A sealed segment copied to backup, archive, or another
  host, edited there, and presented later as authentic.
- **A different UID on the same host.** The daemon's directories are `0700`, so
  this requires a misconfiguration — but "requires a misconfiguration" is what
  most incidents are.
- **A shared or network filesystem.** `crates/weir-wab/src/lib.rs:32` already
  states that weir's guarantees "do not hold" there. A chain is precisely the
  thing that can still be checked when the filesystem cannot be trusted.
- **Silent corruption that happens to pass CRC32.** A 32-bit checksum admits one
  collision in 4.3 billion per corruption event. Rare, not impossible, and
  indistinguishable from tampering when it happens.
- **Answering "was this buffer edited?" at all.** Today there is no procedure
  that answers this. That is the deliverable.

**Explicitly NOT in scope:**

- **A compromised process running as the daemon UID.** Already out of scope in
  the threat model, and nothing here changes that. Such a process can rewrite
  the segment, rewrite the chain, and forge the log line. **This feature does
  not defend against root.**
- **Non-repudiation.** A hash chain is not a signature. It proves *internal
  consistency* and, via the anchor (§5), *consistency with what was already
  observed elsewhere*. It does not prove authorship. Signing is a possible
  follow-up and is out of scope here.
- **Detecting tampering in an active, unsealed segment.** Nothing chains a file
  that is still being appended to. The window is bounded by the seal cadence
  (`wab_segment_max_bytes`, `wab_segment_max_age_secs`).

The correct one-line description is **"prove a sealed buffer was not edited
after the fact"**, which is an audit and compliance property. It is not
"weir is now tamper-proof", and the documentation must not drift toward that.

### 1.3 This does not have to earn the major, because the major is already earned

`cargo-semver-checks` against the published 3.0.0:

```
--- failure enum_marked_non_exhaustive: enum marked #[non_exhaustive] ---
  enum MessageType in crates/weir-core/src/envelope.rs:49
Summary semver requires new major version: 1 major and 0 minor checks failed
```

That is the only major break across all six published library crates. 4.0.0 is
already required. **This feature is therefore free to be purely additive**, and
the design below deliberately spends that freedom: no `FORMAT_VERSION` bump, no
migration, no change to the frozen on-disk layout, and no change to any existing
published API.

---

## 2. Why a separate crate, and not inside the WAB

Three reasons, in order of weight.

### 2.1 The ack path is the product, and hashing on it is a tax

weir is fsync-bound; `weir_wab_group_commit_records` and
`weir_wab_fsync_duration_seconds` are the two numbers everything else is judged
against. Adding a SHA-256 of every record inside `ShardWriter::write_record`
puts a per-record cost on the path that produces the ack — the one path whose
latency the crown invariant is about.

Chaining **sealed** segments instead costs **exactly zero** on the ack path. The
work happens after the records are already durable and already acked.

### 2.2 No format change means no recovery interaction

A `FORMAT_VERSION` bump would have to be reasoned about against
`recovery::recover_open_segments`, the quarantine path, the mid-file-corruption
prefix rule, `weir-ctl dl requeue`, and the 30 frozen conformance vectors. The
sidecar design touches none of them: an `.attest` file is additive in exactly
the way `.wab.confirmed` already is.

### 2.3 The seam already exists and was built for this

`weir-wab` is published as the shared format crate, and already exposes
everything a verifier needs:

```rust
SegmentReader::open(path)      // -> io::Result<Self>
SegmentReader::header()        // -> &SegmentHeaderMeta  (shard id, format version, created_at)
impl Iterator for SegmentReader // -> Item = io::Result<Payload>
verify_sealed_segment(path)    // -> Result<SegmentVerification, SegmentVerifyError>
list_segment_files(dir)        // -> Vec<(PathBuf, SegmentState)>
```

`weir-attest` is a consumer of that crate and needs no privileged access to
daemon internals.

### The case against a separate crate, kept intact

- **The window.** Records are unprotected between being written and their
  segment being sealed. In-daemon hashing would have no window. This is the
  real cost of the decision and §6.1 states how it is bounded and reported.
- **A ninth published crate.** The workspace already publishes eight, in a
  documented order. This adds a ninth and a release step.
- **Two places that know the layout.** `weir-attest` reads segments through
  `weir-wab`, so the coupling is through a published API rather than duplicated
  parsing — but it is still a second consumer to keep in step.

---

## 3. The chain

### 3.1 What is chained: `RecordId`, not the raw payload

```
H₀    = SHA256( DOMAIN_SEP ‖ prev_segment_head ‖ segment_header_bytes )
Hᵢ    = SHA256( Hᵢ₋₁ ‖ RecordId(segment_name, i, payloadᵢ) )
head  = H_n     where n = footer.record_count
```

`RecordId` is `SHA256(len(segment) ‖ segment ‖ index ‖ len(payload) ‖ payload)`
(`crates/weir-sink-sdk/src/lib.rs:492`). Using it rather than the payload bytes
buys three properties:

1. **Position binding.** The id already commits to the segment name and the
   1-based index, so a record moved to a different segment or a different offset
   produces a different id even if its bytes are identical. A chain over raw
   payloads detects reordering only through the chain itself; this detects
   relocation too.
2. **It is the value the downstream already holds.** `RecordId` is what the
   drain hands a sink as its per-record idempotency key, and what an `AckTracked`
   returns to the producer. So the chain is verifiable *against systems outside
   weir* — an auditor can recompute the head from the sink's own rows. Nothing
   else in the tree gives that.
3. **It is already frozen and vector-pinned**, so the chain inherits a value
   whose construction cannot drift without an existing test failing.

`DOMAIN_SEP` is a fixed ASCII string (`b"weir-attest-v1"`) so a chain digest can
never be confused with a `RecordId` or a dedup token computed over similar bytes.

### 3.2 Per-segment chains, linked — not one chain per shard

A single continuous per-shard chain is the obvious design and it is wrong here,
for a reason specific to weir: **segments can be quarantined.** Mid-file
corruption produces a truncated valid prefix that is still delivered, plus a
quarantined forensic copy (`weir_wab_segments_total{state="quarantined"}`). A
continuous chain has no defined value across that discontinuity, and a gap would
invalidate every subsequent segment forever — turning one bad sector into a
permanently unverifiable shard.

So: **one chain per segment**, with `H₀` incorporating the previous sealed
segment's head. That yields a shard-level chain that *degrades locally*. A broken
link is reported at one segment boundary and every other segment still verifies
independently.

The first segment of a shard, and any segment whose predecessor is missing or
quarantined, uses `prev_segment_head = [0u8; 32]` and is reported as a **chain
origin** rather than as a failure. An origin is not evidence of tampering; an
*unexpected* origin is, and only the operator's own record of the shard's history
can distinguish them. Stated plainly rather than papered over.

### 3.3 The `.attest` sidecar

`<segment>.wab.sealed.attest`, mirroring the existing `.wab.confirmed` sidecar
convention so the directory listing stays legible and `list_segment_files` needs
only one new `SegmentState`-adjacent case.

Contents (exact layout in the implementation plan): magic, attest-format version,
shard id, segment name, `prev_segment_head`, `head`, record count, the seal
timestamp copied from the footer, and a CRC32 over the whole thing. The CRC is
for accidental corruption of the sidecar itself; it is explicitly **not** the
integrity mechanism, and the file must say so in a comment so nobody later reads
it as one.

---

## 4. The operator surface

```
weir-ctl attest seal   <wab-dir> [--shard N]   # chain any sealed segment lacking a sidecar
weir-ctl attest verify <wab-dir> [--shard N]   # recompute and compare
weir-ctl attest head   <wab-dir> --shard N     # print the current head, for anchoring
```

**This block is illustrative pseudo-syntax, not the shipped CLI.** As
implemented, `<wab-dir>` is the flag `--wab-dir` (not positional) on every
subcommand, and `head` takes no `--shard` — it prints the newest head for
every shard. See `docs/monitoring.md` for the real invocations.

`verify` is the command that matters. On mismatch it reports **the first
divergent record index**, not just a boolean — a verifier that says only "this
segment is bad" leaves the operator with a 256 MiB file and no next step.
Achievable because the chain is recomputed record by record, so the first index
whose recomputed `Hᵢ` diverges is exactly where the content stopped matching.

Exit codes: `0` verified, `1` mismatch, `2` unreadable/absent. Distinct because
a cron job needs to alert differently on "tampered" and "I could not check".

---

## 5. The anchor — and a correction to the obvious plan

A chain file sitting next to the segment it protects is **not evidence**. Anyone
able to edit the segment can recompute and rewrite the sidecar. The chain only
becomes evidence once its head has been observed somewhere the attacker does not
control.

The chosen anchor is **structured log + metrics**, with one refinement that must
not be silently dropped:

- **The structured log line is the actual anchor.** At each seal the daemon (or
  `attest seal`) emits `weir.attest.head` with the shard, segment name, record
  count and the 64-hex head. In any real deployment logs are shipped off-host
  within seconds; once that line is in the operator's log store, rewriting the
  on-disk chain no longer matches what was already observed elsewhere.

- **The metric cannot carry the head, and should not try.** A 256-bit value has
  no useful representation as a Prometheus sample, and putting it in a *label*
  creates unbounded cardinality — one new series per segment, forever. That is a
  well-known way to take down a Prometheus, and this design refuses it
  explicitly so it does not get re-proposed.

  The metrics are **counts and outcomes**, which is what alerting actually needs:

  | Metric | Type | Meaning |
  |---|---|---|
  | `weir_attest_segments` | gauge | segments examined by the last verify run |
  | `weir_attest_verify_failures` | gauge | **must be 0**; a firing alert here is an integrity incident |
  | `weir_attest_chain_origins` | gauge | segments observed with no predecessor in the last run (§3.2) |
  | `weir_attest_lag_seconds` | gauge | seal-to-attest delay; the §6.1 window, observable |

  All three (bar `lag_seconds`, which has no carrier yet — see §6.1) are
  `gauge`s written by a short-lived `weir-ctl attest verify --metrics-file`
  cron run that overwrites the textfile-collector file wholesale each time —
  never counters, and deliberately named without a `_total` suffix so they
  don't read as one. An alert rule built on `increase()` over such a gauge
  only stays true for one evaluation window after a 0→N transition and then
  goes quiet even while the incident persists; the rule matches the raw gauge
  value instead (`weir_attest_verify_failures > 0`).

  A new alert rule pairs with `verify_failures`, and — per the guard added
  this release — it must ship with a `promtool` test in **both** directions and a
  runbook anchor that resolves.

---

## 6. Known limits, stated so they cannot be discovered later as surprises

### 6.1 The seal-to-attest window

Records in an active segment are unchained. The window is bounded by the seal
cadence and is now *observable* via `weir_attest_lag_seconds`, but it is real.
An operator who needs it closed should reduce `wab_segment_max_age_secs`, and
the documentation should say so rather than implying the window does not exist.

### 6.2 Verification cost is a full re-read

`verify` re-reads every byte and recomputes a SHA-256 per record. On a 256 MiB
segment that is seconds, not milliseconds. It is an audit operation, not a
health check, and must not be wired into a readiness probe.

### 6.3 It does not stop tampering

It detects it, after the fact, if the anchor was observed. Nothing here prevents
a write. Any wording that suggests otherwise is wrong.

---

## 7. Out of scope for this cycle

- Signatures / non-repudiation (a chain is not a MAC).
- Chaining active segments.
- Verifying quarantined forensic copies (they are by definition already suspect).
- Any change to `FORMAT_VERSION`, `weir-wab`'s layout, or the wire protocol.
- Encrypted-at-rest WAB, which remains out of scope in the threat model.

---

## 8. Open questions for review

1. **Does `attest seal` belong in the daemon at all this cycle?** The chosen
   shape is crate + ctl, with the daemon untouched. The in-daemon hook closes the
   §6.1 window and is a small change (the drain already receives a `PathBuf` per
   sealed segment) — but it puts new work on a thread that must never block the
   flusher. Recommend: not this cycle; revisit once the chain format is proven.
2. **Should `attest verify` also check the segment's own CRCs**, or assume
   `verify_sealed_segment` was run first? Recommend: run both, and report them
   as distinct failure classes — "corrupt" and "tampered" have different runbooks.
3. **Publishing order.** `weir-attest` depends on `weir-wab` and `weir-sink-sdk`
   (for `RecordId`). That places it after both in the documented publish order:
   core → wab → sink-sdk → **attest** → sink-s3 → client → rs → server → ctl.
4. **Does `RecordId` belong in `weir-sink-sdk`** now that a second crate needs
   it? Moving it to `weir-core` would be a breaking change for `weir-sink-sdk`
   consumers unless re-exported. Recommend: re-export, do not move.
