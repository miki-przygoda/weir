# Tamper-Evident WAB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `weir-attest` — a published crate that chains sealed WAB segments so an operator can prove a buffer was not edited after the fact — plus a `weir-ctl attest` surface, without touching the ack path or the on-disk format.

**Architecture:** A new crate reads sealed segments through `weir-wab`'s existing `SegmentReader` seam and folds each record's `RecordId` into a SHA-256 chain. One chain per segment, linked to its predecessor's head so a quarantined segment breaks the link locally instead of poisoning the shard. The head is written to a `.attest` sidecar and anchored off-host by a structured log line. The daemon is not modified.

**Tech Stack:** Rust 2024, `sha2` (already a workspace dep via `weir-sink-sdk`), `crc32fast` (already via `weir-wab`), `clap` derive for the ctl surface. No new third-party dependencies.

**Spec:** `docs/superpowers/specs/2026-09-15-tamper-evident-wab-design.md`

## Global Constraints

- **MSRV 1.88** (`rust-version.workspace = true`). No feature newer than that.
- **No new third-party dependency.** `sha2`, `crc32fast` and `clap` are already in the workspace tree. Adding anything else needs a decision from the maintainer first.
- **`FORMAT_VERSION` must not change.** `weir-wab`'s on-disk layout, the 30 frozen conformance vectors, and the wire protocol are all untouched by this work. If a task appears to require changing them, stop and flag it.
- **The ack path must not be modified.** No edits to `crates/weir-server/src/wab/segment.rs`, `wab/mod.rs`, or `socket/`. This feature runs on sealed segments only.
- **Commits carry no trailers.** No `Co-Authored-By`, no `Claude-Session`. PR bodies do carry the Claude Code footer.
- **`git add` explicit paths only.** Never `git add <directory>` — it committed a Mach-O binary into this repo once already.
- **Every published claim needs evidence.** A doc sentence that states a number or a guarantee must be checkable against code or command output.
- **Run CI locally before opening a PR**: `act -W .github/workflows/ci.yml -j <job>` for every job touched. Note `act` on Apple silicon runs arm64 containers — for anything architecture-sensitive add `--container-architecture linux/amd64`.
- **Never run `pkill weir-server`** — it kills unrelated processes.

---

## Deviation from the spec, decided here and flagged for review

**Spec §5 says the anchor is "structured log + metrics". `weir-ctl` is a CLI that
exits; it cannot serve a Prometheus endpoint, and the spec's chosen shape leaves
the daemon untouched this cycle.** So the metrics half has no carrier as written.

**Resolution taken in this plan:** `attest verify` gains `--metrics-file <path>`,
which writes **node_exporter textfile-collector format** — the standard way a
cron or systemd-timer job exports metrics without a long-running process. The
daemon-side `weir_attest_*` gauges arrive with the in-daemon hook, which spec
open question 1 explicitly defers.

This is a real change to §5 and the maintainer should confirm it. It is recorded
in Task 8's notes and must appear in the PR body.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/weir-attest/Cargo.toml` | New published crate manifest |
| `crates/weir-attest/src/lib.rs` | Crate docs, re-exports, `AttestError` |
| `crates/weir-attest/src/chain.rs` | `ChainHead`, `chain_origin`, `chain_step` — the digest, and nothing else |
| `crates/weir-attest/src/sidecar.rs` | `.attest` file encode/decode |
| `crates/weir-attest/src/verify.rs` | Chain a segment, verify a segment, `Divergence` reporting |
| `crates/weir-attest/tests/vectors.rs` | Frozen known-answer vectors |
| `docs/conformance/attest_v1_vectors.json` | The vectors themselves |
| `docs/conformance/gen_attest_vectors.py` | Independent Python generator |
| `crates/weir-ctl/src/attest.rs` | `weir-ctl attest` subcommand implementation |
| `crates/weir-ctl/src/main.rs` | Wire `AttestCommand` into `Command` |
| `.github/workflows/ci.yml` | New `harnesses` job building `fuzz/` and `chaos/` |
| `docs/security/threat-model.md` | Move the "Signed audit log" row out of "Out of scope" |
| `docs/monitoring.md` | The new alert rule + runbook heading |
| `deploy/prometheus/weir-alerts{,_test}.yml` | `WeirAttestVerifyFailed` + both-direction tests |
| `CHANGELOG.md` | The `[Unreleased]` entry |

---

### Task 1: The chain digest

**Files:**
- Create: `crates/weir-attest/Cargo.toml`
- Create: `crates/weir-attest/src/lib.rs`
- Create: `crates/weir-attest/src/chain.rs`
- Modify: `Cargo.toml` (workspace `members` + `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `weir_sink_sdk::RecordId` (`as_bytes() -> &[u8; 32]`), `weir_wab::format::SegmentHeaderMeta` (`format_version: u8`, `shard_id: u16`, `created_at: i64`)
- Produces: `ChainHead`, `ChainHead::ORIGIN`, `ChainHead::to_hex`, `ChainHead::from_hex`, `ChainHead::as_bytes`, `chain_origin`, `chain_step`, `AttestError`

- [ ] **Step 1: Add the crate to the workspace**

In the root `Cargo.toml`, add `"crates/weir-attest",` to `members` after `"crates/weir-sink-sdk",`, and add to `[workspace.dependencies]`:

```toml
weir-attest = { path = "crates/weir-attest", version = "4.0.0" }
```

Note the version is `4.0.0` — the whole workspace bumps at release, and the five existing pins bump with it. Do NOT bump the others in this task.

- [ ] **Step 2: Write `crates/weir-attest/Cargo.toml`**

```toml
[package]
name = "weir-attest"
description = "Tamper-evident hash chain over sealed weir WAB segments"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
homepage.workspace = true
keywords = ["weir", "audit", "integrity", "hash-chain", "tamper-evident"]
categories = ["cryptography", "filesystem"]

[dependencies]
weir-core = { workspace = true }
weir-wab = { workspace = true }
# RecordId is the chain's input. It lives in the sink SDK because the drain
# hands it to sinks as an idempotency key; that is exactly why it is the right
# input here — the chain becomes verifiable against the downstream's own rows.
weir-sink-sdk = { workspace = true }
sha2 = "0.10"
crc32fast = "1.4"
```

- [ ] **Step 3: Write the failing test in `crates/weir-attest/src/chain.rs`**

```rust
//! The chain digest. This module owns the hash construction and nothing else —
//! no I/O, no file formats — so the one security-relevant computation in the
//! crate can be read in full on one screen.

use sha2::{Digest, Sha256};
use weir_sink_sdk::RecordId;
use weir_wab::format::SegmentHeaderMeta;

/// Domain separator. Fixed, and distinct from any other digest weir computes,
/// so a chain head can never be confused with a `RecordId` or a dedup token
/// computed over similar bytes.
pub const DOMAIN_SEP: &[u8] = b"weir-attest-v1";

/// A 256-bit chain head.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChainHead([u8; 32]);

impl ChainHead {
    /// The head a chain starts from when it has no predecessor.
    ///
    /// Not a failure: the first segment of a shard legitimately has none. An
    /// *unexpected* origin is evidence, and only the operator's record of the
    /// shard's history can tell the two apart.
    pub const ORIGIN: ChainHead = ChainHead([0u8; 32]);

    /// The raw digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Rebuild from a previously captured digest.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        ChainHead(bytes)
    }

    /// Lower-hex, 64 characters, no prefix — the form the log line carries.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in self.0 {
            let _ = write!(out, "{b:02x}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_is_all_zero_and_round_trips_through_hex() {
        assert_eq!(ChainHead::ORIGIN.as_bytes(), &[0u8; 32]);
        assert_eq!(ChainHead::ORIGIN.to_hex(), "0".repeat(64));
    }
}
```

- [ ] **Step 4: Run it and watch it fail**

Run: `cargo test -p weir-attest`
Expected: FAIL — the crate does not compile yet (`lib.rs` is missing).

- [ ] **Step 5: Write `crates/weir-attest/src/lib.rs`**

```rust
//! Tamper-evident hash chain over sealed weir WAB segments.
//!
//! # What this proves, and what it does not
//!
//! A CRC32 is an error-detection code, not a MAC: anyone who edits a record can
//! recompute the CRC in microseconds and the file reads back clean. This crate
//! chains sealed segments so that editing one is *detectable* — provided the
//! chain head was observed somewhere the editor does not control.
//!
//! It does **not** defend against a process running as the daemon UID (already
//! out of scope in the threat model — such a process can rewrite the segment,
//! the chain, and the log line), it is **not** non-repudiation (a chain is not a
//! signature), and it **detects rather than prevents**.
//!
//! The honest one-line description is: *prove a sealed buffer was not edited
//! after the fact.*
//!
//! Nothing here runs on the ack path. Chains are computed over segments that
//! are already sealed, already fsynced, and already acked.

#![deny(missing_docs)]

pub mod chain;

pub use chain::{ChainHead, DOMAIN_SEP};
```

- [ ] **Step 6: Run the test and watch it pass**

Run: `cargo test -p weir-attest`
Expected: PASS, 1 test.

- [ ] **Step 7: Write the failing test for the two digest functions**

Append to `crates/weir-attest/src/chain.rs`'s `mod tests`:

```rust
    fn meta() -> SegmentHeaderMeta {
        SegmentHeaderMeta {
            format_version: 1,
            compression: weir_wab::format::Compression::None,
            shard_id: 0,
            created_at: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn a_different_predecessor_gives_a_different_origin() {
        let a = chain_origin(ChainHead::ORIGIN, &meta(), "shard_00/seg_00000001.wab.sealed");
        let b = chain_origin(
            ChainHead::from_bytes([7u8; 32]),
            &meta(),
            "shard_00/seg_00000001.wab.sealed",
        );
        assert_ne!(
            a, b,
            "H0 must commit to the predecessor, or segments could be reordered \
             without breaking any chain"
        );
    }

    #[test]
    fn a_different_segment_name_gives_a_different_origin() {
        let a = chain_origin(ChainHead::ORIGIN, &meta(), "shard_00/seg_00000001.wab.sealed");
        let b = chain_origin(ChainHead::ORIGIN, &meta(), "shard_00/seg_00000002.wab.sealed");
        assert_ne!(a, b, "H0 must commit to the segment it covers");
    }

    #[test]
    fn chain_step_is_order_dependent() {
        let r1 = RecordId::from_bytes([1u8; 32]);
        let r2 = RecordId::from_bytes([2u8; 32]);
        let forward = chain_step(chain_step(ChainHead::ORIGIN, &r1), &r2);
        let reversed = chain_step(chain_step(ChainHead::ORIGIN, &r2), &r1);
        assert_ne!(
            forward, reversed,
            "a chain that is not order-dependent cannot detect reordering, which \
             is most of what it exists for"
        );
    }
```

- [ ] **Step 8: Run and watch it fail**

Run: `cargo test -p weir-attest`
Expected: FAIL — `chain_origin` and `chain_step` are not defined.

- [ ] **Step 9: Implement the two functions**

Insert into `crates/weir-attest/src/chain.rs` above `mod tests`:

```rust
/// `H₀` — the head a segment's chain starts from.
///
/// Commits to the predecessor's head, the segment header, and the segment's
/// name. The name matters: without it, two segments with identical headers and
/// identical records would produce identical chains, and one could be
/// substituted for the other.
pub fn chain_origin(
    prev: ChainHead,
    header: &SegmentHeaderMeta,
    segment_name: &str,
) -> ChainHead {
    let mut h = Sha256::new();
    h.update(DOMAIN_SEP);
    h.update(prev.as_bytes());
    h.update([header.format_version]);
    h.update(header.shard_id.to_le_bytes());
    h.update(header.created_at.to_le_bytes());
    // Length-prefixed, so ("a", "bc") and ("ab", "c") cannot collide.
    h.update((segment_name.len() as u64).to_le_bytes());
    h.update(segment_name.as_bytes());
    ChainHead(h.finalize().into())
}

/// `Hᵢ = SHA256(Hᵢ₋₁ ‖ RecordIdᵢ)`.
///
/// The input is the record's `RecordId`, not its payload. The id already
/// commits to the segment name and the 1-based index, so a record relocated to
/// a different segment or offset changes the chain even if its bytes are
/// identical — and, more usefully, the id is what the drain hands a sink as its
/// idempotency key, so the head can be recomputed from the downstream's rows.
pub fn chain_step(prev: ChainHead, record_id: &RecordId) -> ChainHead {
    let mut h = Sha256::new();
    h.update(prev.as_bytes());
    h.update(record_id.as_bytes());
    ChainHead(h.finalize().into())
}
```

Add to the `impl ChainHead` block:

```rust
    /// Parse 64 lower-hex characters.
    pub fn from_hex(s: &str) -> Result<Self, crate::AttestError> {
        if s.len() != 64 {
            return Err(crate::AttestError::BadHexLength { len: s.len() });
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| crate::AttestError::BadHexDigit)?;
        }
        Ok(ChainHead(out))
    }
```

And a `Debug` impl so a head never prints as a byte array in a failure message:

```rust
impl std::fmt::Debug for ChainHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChainHead({})", self.to_hex())
    }
}
```

- [ ] **Step 10: Add `AttestError` to `lib.rs`**

```rust
/// Why an attest operation failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AttestError {
    /// A hex string was not 64 characters.
    BadHexLength {
        /// The length seen.
        len: usize,
    },
    /// A hex string contained a non-hex character.
    BadHexDigit,
    /// Underlying I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for AttestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadHexLength { len } => write!(f, "chain head must be 64 hex characters, got {len}"),
            Self::BadHexDigit => write!(f, "chain head contains a non-hex character"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for AttestError {}

impl From<std::io::Error> for AttestError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
```

Add `pub use` for it: `pub use chain::{ChainHead, DOMAIN_SEP};` becomes that plus nothing — `AttestError` is defined in `lib.rs` so it is already public.

- [ ] **Step 11: Run the tests and watch them pass**

Run: `cargo test -p weir-attest`
Expected: PASS, 4 tests.

- [ ] **Step 12: Run the format and lint gate**

Run:
```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy -p weir-attest --all-targets -- -D warnings
```
Expected: clean.

- [ ] **Step 13: Commit**

```bash
git add Cargo.toml Cargo.lock crates/weir-attest/Cargo.toml crates/weir-attest/src/lib.rs crates/weir-attest/src/chain.rs
git commit -m "feat(attest): the chain digest, and the domain separator that keeps it distinct"
```

---

### Task 2: The `.attest` sidecar format

**Files:**
- Create: `crates/weir-attest/src/sidecar.rs`
- Modify: `crates/weir-attest/src/lib.rs` (add `pub mod sidecar;`)

**Interfaces:**
- Consumes: `ChainHead`, `AttestError` from Task 1
- Produces: `Sidecar { format_version: u8, shard_id: u16, segment_name: String, prev_head: ChainHead, head: ChainHead, record_count: u64, sealed_at: i64 }`, `Sidecar::encode() -> Vec<u8>`, `Sidecar::decode(&[u8]) -> Result<Sidecar, AttestError>`, `ATTEST_MAGIC`, `ATTEST_FORMAT_VERSION`

- [ ] **Step 1: Write the failing round-trip and rejection tests**

Create `crates/weir-attest/src/sidecar.rs`:

```rust
//! The `.attest` sidecar: what gets written next to a sealed segment.
//!
//! The trailing CRC32 here guards against accidental corruption OF THE SIDECAR
//! ITSELF. It is emphatically **not** the integrity mechanism — anyone who can
//! rewrite the segment can rewrite this file and its CRC. The integrity
//! mechanism is the chain head having been observed elsewhere (see the crate
//! docs). Nobody should ever read this CRC as security.

use crate::{AttestError, ChainHead};

/// File magic. Distinct from the WAB segment magic so a misrouted file is
/// rejected rather than half-parsed.
pub const ATTEST_MAGIC: &[u8; 4] = b"WATT";

/// Sidecar layout version. Leads the file after the magic so the layout can
/// grow: a reader meeting a version it does not know rejects rather than
/// parsing a prefix of a layout it has never seen.
pub const ATTEST_FORMAT_VERSION: u8 = 1;

/// A decoded `.attest` sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sidecar {
    /// Layout version.
    pub format_version: u8,
    /// Owning shard.
    pub shard_id: u16,
    /// Segment this chain covers, as `shard_NN/seg_........wab.sealed`.
    pub segment_name: String,
    /// The predecessor segment's head, or [`ChainHead::ORIGIN`].
    pub prev_head: ChainHead,
    /// The head after the last record.
    pub head: ChainHead,
    /// Records covered.
    pub record_count: u64,
    /// Seal time copied from the segment footer, unix nanoseconds.
    pub sealed_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Sidecar {
        Sidecar {
            format_version: ATTEST_FORMAT_VERSION,
            shard_id: 3,
            segment_name: "shard_03/seg_00000007.wab.sealed".into(),
            prev_head: ChainHead::from_bytes([9u8; 32]),
            head: ChainHead::from_bytes([4u8; 32]),
            record_count: 1234,
            sealed_at: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn encode_decode_round_trips() {
        let s = sample();
        assert_eq!(Sidecar::decode(&s.encode()).unwrap(), s);
    }

    #[test]
    fn a_flipped_byte_is_rejected_by_the_crc() {
        let mut bytes = sample().encode();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        assert!(
            Sidecar::decode(&bytes).is_err(),
            "the sidecar CRC must catch accidental corruption of the sidecar"
        );
    }

    #[test]
    fn an_unknown_version_is_rejected_rather_than_parsed() {
        let mut bytes = sample().encode();
        bytes[4] = 0x02;
        assert!(Sidecar::decode(&bytes).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bytes = sample().encode();
        bytes[0] = b'X';
        assert!(Sidecar::decode(&bytes).is_err());
    }

    #[test]
    fn a_truncated_sidecar_is_rejected() {
        let bytes = sample().encode();
        assert!(Sidecar::decode(&bytes[..bytes.len() - 3]).is_err());
    }
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `cargo test -p weir-attest sidecar`
Expected: FAIL — `encode`/`decode` are not defined.

- [ ] **Step 3: Add the error variants to `lib.rs`**

Extend `AttestError` with:

```rust
    /// Sidecar magic did not match.
    BadMagic,
    /// Sidecar layout version is one this build cannot parse.
    UnsupportedSidecarVersion {
        /// The version byte seen.
        version: u8,
    },
    /// Sidecar is shorter than its fixed prefix, or its declared name runs past
    /// the end.
    TruncatedSidecar,
    /// Sidecar CRC did not match its contents.
    SidecarCrcMismatch,
```

And the matching `Display` arms:

```rust
            Self::BadMagic => write!(f, "not a weir attest sidecar (bad magic)"),
            Self::UnsupportedSidecarVersion { version } => {
                write!(f, "unsupported attest sidecar version {version:#04x}")
            }
            Self::TruncatedSidecar => write!(f, "attest sidecar is truncated"),
            Self::SidecarCrcMismatch => write!(f, "attest sidecar CRC mismatch"),
```

- [ ] **Step 4: Implement `encode` and `decode`**

Append to `crates/weir-attest/src/sidecar.rs`, above `mod tests`:

```rust
/// Bytes before the variable-length segment name:
/// magic(4) + version(1) + shard_id(2) + prev_head(32) + head(32)
/// + record_count(8) + sealed_at(8) + name_len(2).
const FIXED_LEN: usize = 4 + 1 + 2 + 32 + 32 + 8 + 8 + 2;

impl Sidecar {
    /// Serialise. Little-endian throughout, matching the WAB format.
    pub fn encode(&self) -> Vec<u8> {
        let name = self.segment_name.as_bytes();
        let mut out = Vec::with_capacity(FIXED_LEN + name.len() + 4);
        out.extend_from_slice(ATTEST_MAGIC);
        out.push(self.format_version);
        out.extend_from_slice(&self.shard_id.to_le_bytes());
        out.extend_from_slice(self.prev_head.as_bytes());
        out.extend_from_slice(self.head.as_bytes());
        out.extend_from_slice(&self.record_count.to_le_bytes());
        out.extend_from_slice(&self.sealed_at.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse. Validation order is magic, version, length, CRC — the version
    /// before any field offset is interpreted, and the CRC before the name is
    /// turned into a `String`.
    pub fn decode(bytes: &[u8]) -> Result<Sidecar, AttestError> {
        if bytes.len() < FIXED_LEN + 4 {
            return Err(AttestError::TruncatedSidecar);
        }
        if &bytes[0..4] != ATTEST_MAGIC {
            return Err(AttestError::BadMagic);
        }
        let format_version = bytes[4];
        if format_version != ATTEST_FORMAT_VERSION {
            return Err(AttestError::UnsupportedSidecarVersion {
                version: format_version,
            });
        }
        let name_len = u16::from_le_bytes([bytes[FIXED_LEN - 2], bytes[FIXED_LEN - 1]]) as usize;
        let want = FIXED_LEN + name_len + 4;
        if bytes.len() != want {
            return Err(AttestError::TruncatedSidecar);
        }
        let body = &bytes[..want - 4];
        let want_crc = u32::from_le_bytes([
            bytes[want - 4],
            bytes[want - 3],
            bytes[want - 2],
            bytes[want - 1],
        ]);
        if crc32fast::hash(body) != want_crc {
            return Err(AttestError::SidecarCrcMismatch);
        }

        let mut prev = [0u8; 32];
        prev.copy_from_slice(&bytes[7..39]);
        let mut head = [0u8; 32];
        head.copy_from_slice(&bytes[39..71]);
        let segment_name = std::str::from_utf8(&bytes[FIXED_LEN..FIXED_LEN + name_len])
            .map_err(|_| AttestError::TruncatedSidecar)?
            .to_string();

        Ok(Sidecar {
            format_version,
            shard_id: u16::from_le_bytes([bytes[5], bytes[6]]),
            segment_name,
            prev_head: ChainHead::from_bytes(prev),
            head: ChainHead::from_bytes(head),
            record_count: u64::from_le_bytes(bytes[71..79].try_into().expect("8 bytes")),
            sealed_at: i64::from_le_bytes(bytes[79..87].try_into().expect("8 bytes")),
        })
    }
}
```

- [ ] **Step 5: Add the module to `lib.rs`**

```rust
pub mod sidecar;

pub use sidecar::{ATTEST_FORMAT_VERSION, ATTEST_MAGIC, Sidecar};
```

- [ ] **Step 6: Run and watch them pass**

Run: `cargo test -p weir-attest`
Expected: PASS, 9 tests.

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt --all
cargo clippy -p weir-attest --all-targets -- -D warnings
git add crates/weir-attest/src/sidecar.rs crates/weir-attest/src/lib.rs
git commit -m "feat(attest): the .attest sidecar, whose CRC is not the security mechanism"
```

---

### Task 3: Chain a real sealed segment

**Files:**
- Create: `crates/weir-attest/src/verify.rs`
- Modify: `crates/weir-attest/src/lib.rs`

**Interfaces:**
- Consumes: `chain_origin`, `chain_step`, `Sidecar`, `weir_wab::SegmentReader`, `weir_wab::verify_sealed_segment`
- Produces: `chain_segment(path: &Path, prev: ChainHead) -> Result<Sidecar, AttestError>`, `segment_name_for(path: &Path) -> String`

- [ ] **Step 1: Write the failing test**

Create `crates/weir-attest/src/verify.rs`:

```rust
//! Chaining and verifying a sealed segment on disk.

use crate::{AttestError, ChainHead, Sidecar, chain};
use std::path::Path;
use weir_sink_sdk::RecordId;

#[cfg(test)]
mod tests {
    use super::*;

    /// Chaining the same segment twice must give the same head, and a segment
    /// with one byte of payload changed must give a different one. Together
    /// these are the whole point of the crate.
    #[test]
    fn the_same_segment_chains_to_the_same_head_and_a_changed_one_does_not() {
        let dir = crate::verify::tests::scratch("chain_stability");
        let a = write_segment(&dir.join("seg_a.wab.sealed"), &[b"one", b"two", b"three"]);
        let b = write_segment(&dir.join("seg_b.wab.sealed"), &[b"one", b"two", b"three"]);
        let c = write_segment(&dir.join("seg_c.wab.sealed"), &[b"one", b"XXX", b"three"]);

        let ha = chain_segment(&a, ChainHead::ORIGIN).unwrap();
        let ha2 = chain_segment(&a, ChainHead::ORIGIN).unwrap();
        assert_eq!(ha.head, ha2.head, "chaining must be deterministic");

        let hb = chain_segment(&b, ChainHead::ORIGIN).unwrap();
        assert_ne!(
            ha.head, hb.head,
            "the segment NAME is part of H0, so identical records in differently \
             named segments must not collide"
        );

        let hc = chain_segment(&c, ChainHead::ORIGIN).unwrap();
        assert_ne!(ha.head, hc.head, "a changed payload must change the head");
        std::fs::remove_dir_all(&dir).ok();
    }
}
```

**Test helpers — build them with `weir_wab::format`'s PUBLIC builders, verified present:** `build_segment_header`, `build_segment_footer`, `build_sentinel`, `SENTINEL`, `SEGMENT_HEADER_LEN`. Do NOT hand-write byte offsets; that would duplicate the format this crate exists to avoid drifting from. `crates/weir-wab/tests/segment_reader.rs:170` (`write_sealed_segment`) is the shape to copy — it is test-private so you cannot call it, but it is the reference:

```rust
use weir_wab::format::{
    Compression, SENTINEL, build_segment_footer, build_segment_header, build_sentinel,
};

/// Build a real sealed segment through weir-wab's public builders.
fn write_segment(path: &std::path::Path, records: &[&[u8]]) -> std::path::PathBuf {
    use std::io::Write as _;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&build_segment_header(0, Compression::None));
    let mut data_bytes = 0u64;
    for r in records {
        bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
        bytes.extend_from_slice(r);
        data_bytes += r.len() as u64;
    }
    let file_crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&SENTINEL);
    bytes.extend_from_slice(&build_segment_footer(
        records.len() as u64,
        data_bytes,
        file_crc,
        1,
    ));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(&bytes).unwrap();
    f.sync_all().unwrap();
    path.to_path_buf()
}

/// A scratch directory whose mode does not depend on the process umask.
/// weir-server's testutil explains why at length; the short version is that a
/// directory created while another test holds umask 0o177 has no execute bit.
fn scratch(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("weir_attest_{label}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}
```

`crc32fast` is already a dependency of `weir-attest` (Task 1). The segment must live under a `shard_NN/` parent for `segment_name_for` to produce the shard-qualified name — create the segments inside `dir.join("shard_00")`.

- [ ] **Step 2: Run and watch it fail**

Run: `cargo test -p weir-attest chain_stability`
Expected: FAIL — `chain_segment` undefined.

- [ ] **Step 3: Implement `segment_name_for` and `chain_segment`**

```rust
/// The name a segment is chained under: `shard_NN/seg_....`.
///
/// The parent directory is included because `RecordId` commits to the segment
/// name the daemon uses, which is shard-qualified. Getting this wrong makes
/// every chain head disagree with what a sink could recompute.
pub fn segment_name_for(path: &Path) -> String {
    let file = path.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    match path.parent().and_then(|p| p.file_name()) {
        Some(shard) => format!("{}/{}", shard.to_string_lossy(), file),
        None => file,
    }
}

/// Compute the chain for one sealed segment.
///
/// Verifies the segment's own structure first via `weir-wab`, so "corrupt" and
/// "tampered" stay distinguishable — they have different runbooks.
pub fn chain_segment(path: &Path, prev: ChainHead) -> Result<Sidecar, AttestError> {
    let verification = weir_wab::verify_sealed_segment(path)
        .map_err(|e| AttestError::Io(std::io::Error::other(e.to_string())))?;

    let reader = weir_wab::SegmentReader::open(path)?;
    let header = *reader.header();
    let name = segment_name_for(path);

    let mut head = chain::chain_origin(prev, &header, &name);
    let mut count: u64 = 0;
    for (i, record) in reader.enumerate() {
        let payload = record?;
        // 1-based, matching RecordId's contract and the daemon's own indexing.
        let id = RecordId::for_record(&name, i as u64 + 1, &payload);
        head = chain::chain_step(head, &id);
        count += 1;
    }

    Ok(Sidecar {
        format_version: crate::ATTEST_FORMAT_VERSION,
        shard_id: header.shard_id,
        segment_name: name,
        prev_head: prev,
        head,
        record_count: count,
        sealed_at: verification.footer.sealed_at,
    })
}
```

**Verified:** `SegmentFooterMeta` has `record_count: u64`, `data_bytes: u64`, `file_crc32: u32` and `sealed_at: i64` (`crates/weir-wab/src/format.rs:438-455`). `verification.footer.sealed_at` is correct as written.

- [ ] **Step 4: Declare the module in `lib.rs`**

Without this the file is never compiled and every test in it silently does not exist.

```rust
pub mod verify;

pub use verify::{chain_segment, segment_name_for};
```

- [ ] **Step 5: Run and watch it pass**

Run: `cargo test -p weir-attest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/weir-attest/src/verify.rs crates/weir-attest/src/lib.rs
git commit -m "feat(attest): chain a sealed segment through the weir-wab reader"
```

---

### Task 4: Verification that says where it diverged

**Files:**
- Modify: `crates/weir-attest/src/verify.rs`

**Interfaces:**
- Produces: `Verdict` enum (`Verified`, `Diverged { at_record: u64, expected: ChainHead, actual: ChainHead }`, `MissingSidecar`), `verify_segment(path: &Path) -> Result<Verdict, AttestError>`

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn verification_names_the_first_divergent_record() {
        let dir = scratch("divergence");
        let seg = write_segment(&dir.join("seg_a.wab.sealed"), &[b"one", b"two", b"three"]);
        let side = chain_segment(&seg, ChainHead::ORIGIN).unwrap();
        std::fs::write(sidecar_path(&seg), side.encode()).unwrap();

        assert!(matches!(verify_segment(&seg).unwrap(), Verdict::Verified));

        // Rewrite the segment with the SECOND record changed.
        let tampered = write_segment(&dir.join("seg_a2.wab.sealed"), &[b"one", b"XXX", b"three"]);
        std::fs::copy(&tampered, &seg).unwrap();

        match verify_segment(&seg).unwrap() {
            Verdict::Diverged { at_record, .. } => assert_eq!(
                at_record, 2,
                "the operator needs the index, not a boolean — a 256 MiB file and \
                 'this is bad' is not a next step"
            ),
            other => panic!("expected divergence, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
```

**Note:** copying `seg_a2` over `seg_a` changes the file the sidecar's `segment_name` refers to while keeping the name — which is exactly the substitution attack the chain exists to catch. If `write_segment` embeds the name inside the segment such that this does not work, instead mutate the bytes of `seg_a` in place after chaining and adjust the expected index to match which record you corrupted.

- [ ] **Step 2: Run and watch it fail**

Run: `cargo test -p weir-attest divergence`
Expected: FAIL — `Verdict`, `verify_segment`, `sidecar_path` undefined.

- [ ] **Step 3: Implement**

```rust
/// Where the `.attest` sidecar for a segment lives.
pub fn sidecar_path(segment: &Path) -> std::path::PathBuf {
    let mut s = segment.as_os_str().to_os_string();
    s.push(".attest");
    std::path::PathBuf::from(s)
}

/// The outcome of verifying one segment.
#[derive(Debug)]
#[non_exhaustive]
pub enum Verdict {
    /// Recomputed head matches the sidecar.
    Verified,
    /// The chain diverged. `at_record` is 1-based.
    Diverged {
        /// First record whose recomputed head stopped matching.
        at_record: u64,
        /// What the sidecar claims.
        expected: ChainHead,
        /// What the segment now produces.
        actual: ChainHead,
    },
    /// No sidecar exists for this segment.
    MissingSidecar,
}

/// Recompute a segment's chain and compare it to its sidecar.
pub fn verify_segment(path: &Path) -> Result<Verdict, AttestError> {
    let sp = sidecar_path(path);
    if !sp.exists() {
        return Ok(Verdict::MissingSidecar);
    }
    let sidecar = Sidecar::decode(&std::fs::read(&sp)?)?;

    let reader = weir_wab::SegmentReader::open(path)?;
    let header = *reader.header();
    let name = segment_name_for(path);
    let mut head = chain::chain_origin(sidecar.prev_head, &header, &name);
    let mut count: u64 = 0;
    for (i, record) in reader.enumerate() {
        let payload = record?;
        let id = RecordId::for_record(&name, i as u64 + 1, &payload);
        head = chain::chain_step(head, &id);
        count += 1;
    }

    if head == sidecar.head && count == sidecar.record_count {
        return Ok(Verdict::Verified);
    }
    // Re-walk to find the FIRST divergence. A second pass costs one more read
    // and turns "this file is bad" into "record N is where it stopped matching",
    // which is the difference between a verdict and a next step.
    let at_record = first_divergence(path, &sidecar)?.unwrap_or(count.max(1));
    Ok(Verdict::Diverged {
        at_record,
        expected: sidecar.head,
        actual: head,
    })
}

/// The 1-based index of the first record whose chain step cannot be reconciled.
///
/// A chain gives no per-record checkpoint, so this locates the divergence by
/// finding the first record whose id differs from what a clean re-chain of the
/// sidecar's own prefix would produce. When the segment is shorter than the
/// sidecar claims, the answer is the first missing record.
fn first_divergence(path: &Path, sidecar: &Sidecar) -> Result<Option<u64>, AttestError> {
    let reader = weir_wab::SegmentReader::open(path)?;
    let mut seen: u64 = 0;
    for record in reader {
        record?;
        seen += 1;
    }
    if seen < sidecar.record_count {
        return Ok(Some(seen + 1));
    }
    Ok(None)
}
```

**Note for the implementer:** `first_divergence` as written only locates a *truncation*. Locating a mid-file content change to a specific index requires per-record checkpoints, which the sidecar does not carry. **Decide and flag:** either (a) store a per-record head vector in the sidecar (bigger file, exact index), or (b) report the index only for truncation and otherwise report `at_record: 0` meaning "somewhere in the segment". Do NOT silently ship a function whose name promises more than it delivers. Recommended: (b) for this cycle with the doc comment saying so plainly, and raise (a) with the maintainer.

- [ ] **Step 4: Run, adjust the test to the chosen semantics, watch it pass**

Run: `cargo test -p weir-attest`

- [ ] **Step 5: Commit**

```bash
git add crates/weir-attest/src/verify.rs
git commit -m "feat(attest): verification that reports where the chain diverged"
```

---

### Task 5: Frozen vectors, generated independently

**Files:**
- Create: `docs/conformance/gen_attest_vectors.py`
- Create: `docs/conformance/attest_v1_vectors.json`
- Create: `crates/weir-attest/tests/vectors.rs`

**Interfaces:**
- Consumes: `chain_origin`, `chain_step`, `ChainHead`
- Produces: a frozen vector file the chain cannot drift away from

- [ ] **Step 1: Write the Python generator**

`docs/conformance/gen_attest_vectors.py` — it must compute the chain **from the spec, using `hashlib`**, sharing no code with Rust. Mirror the structure of `gen_batch_vectors.py`. It emits, for a fixed set of inputs: `domain_sep`, each `record_id` (recomputed in Python from `sha256(len(seg) ‖ seg ‖ index ‖ len(payload) ‖ payload)`), `h0`, each `h_i`, and the final `head`, all as lower hex.

Include at minimum:
- a 1-record chain,
- a 3-record chain,
- the same 3 records under a different segment name (different head),
- the same 3 records reordered (different head),
- a chain whose `prev_head` is non-zero.

- [ ] **Step 2: Generate and eyeball**

```bash
python3 docs/conformance/gen_attest_vectors.py > docs/conformance/attest_v1_vectors.json
python3 -c "import json;d=json.load(open('docs/conformance/attest_v1_vectors.json'));print(len(d['vectors']),'vectors')"
```

- [ ] **Step 3: Write the Rust runner**

`crates/weir-attest/tests/vectors.rs` loads the JSON with `include_str!`, recomputes each chain with `chain_origin`/`chain_step`, and asserts every `h_i` and the final head match. Two independent SHA-256 implementations agreeing is the point.

- [ ] **Step 4: Run and watch it pass**

Run: `cargo test -p weir-attest --test vectors`

- [ ] **Step 5: Wire it into `run_vectors.py`**

Add an `attest` section to `docs/conformance/run_vectors.py` reporting on its own line, matching how the batch section does it.

- [ ] **Step 6: Commit**

```bash
git add docs/conformance/gen_attest_vectors.py docs/conformance/attest_v1_vectors.json crates/weir-attest/tests/vectors.rs docs/conformance/run_vectors.py
git commit -m "test(attest): frozen chain vectors, generated independently in Python"
```

---

### Task 6: The `weir-ctl attest` surface

**Files:**
- Create: `crates/weir-ctl/src/attest.rs`
- Modify: `crates/weir-ctl/src/main.rs`
- Modify: `crates/weir-ctl/Cargo.toml`

**Interfaces:**
- Consumes: everything from Tasks 1–4
- Produces: `weir-ctl attest seal|verify|head`

- [ ] **Step 1: Add the dependency**

In `crates/weir-ctl/Cargo.toml`, under `[dependencies]`:

```toml
# The chain lives in its own crate so the daemon's ack path never pays for it;
# weir-ctl is where an operator drives it from.
weir-attest = { workspace = true }
```

- [ ] **Step 2: Add the subcommand enum to `main.rs`**

Add to `Command`, after the `Quarantine` variant:

```rust
    /// Chain and verify sealed segments — tamper evidence for the buffer.
    ///
    /// A CRC catches accidental corruption; it does not catch an edit, because
    /// an editor recomputes it. This chains each sealed segment so an edit is
    /// detectable, PROVIDED the head was observed off-host (see `attest head`).
    #[command(subcommand)]
    Attest(AttestCommand),
```

And the enum:

```rust
/// Subcommands under `weir-ctl attest`.
#[derive(Subcommand)]
enum AttestCommand {
    /// Chain every sealed segment that has no sidecar yet.
    Seal {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Limit to one shard.
        #[arg(long)]
        shard: Option<u16>,
    },
    /// Recompute every chain and compare it to its sidecar.
    Verify {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Limit to one shard.
        #[arg(long)]
        shard: Option<u16>,
        /// Also write node_exporter textfile-collector metrics here.
        #[arg(long)]
        metrics_file: Option<PathBuf>,
    },
    /// Print the newest chain head per shard, for anchoring off-host.
    Head {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
    },
}
```

- [ ] **Step 3: Implement `crates/weir-ctl/src/attest.rs`**

It must:
- enumerate sealed segments with `weir_wab::list_segment_files(dir)` filtered to `SegmentState::Sealed`, sorted by name so chains link in segment order;
- for `seal`: chain each segment whose sidecar is absent, passing the previous segment's head, and write the sidecar;
- for `verify`: call `verify_segment` on each and aggregate;
- emit one structured line per segment on `seal`, in the anchor format:
  `weir.attest.head shard=3 segment=shard_03/seg_00000007.wab.sealed records=1234 head=<64 hex>`
- honour the **existing global `--json` flag**: `crates/weir-ctl/src/main.rs:38` defines `json: bool` on the top-level `Cli`, read at `:225` as `let json = cli.json;` and threaded into every `cmd_*` call as a trailing `json` argument (see `:227`, `:233`, `:236`). Your `cmd_attest_*` functions take the same trailing `json: bool` and follow the same output convention.

**Exit codes:** `0` all verified, `1` any divergence, `2` any segment unreadable or sidecar undecodable. Distinct because a cron job must alert differently on "tampered" and "I could not check".

- [ ] **Step 4: Write the CLI tests**

Add tests covering: seal-then-verify on a scratch directory returns `Verified` and exit 0; tampering a segment then verifying returns exit 1; a missing sidecar is reported without being treated as tampering.

- [ ] **Step 5: Run and commit**

```bash
cargo test -p weir-ctl
cargo clippy -p weir-ctl --all-targets -- -D warnings
git add crates/weir-ctl/src/attest.rs crates/weir-ctl/src/main.rs crates/weir-ctl/Cargo.toml
git commit -m "feat(ctl): weir-ctl attest seal|verify|head"
```

---

### Task 7: The alert rule, pinned in both directions

**Files:**
- Modify: `deploy/prometheus/weir-alerts.yml`
- Modify: `deploy/prometheus/weir-alerts_test.yml`
- Modify: `docs/monitoring.md`

- [ ] **Step 1: Add the rule**

**Correction made during implementation (Task 7 fix round):** the `expr` below
originally called `increase()` on the metric's counter-style name (with the
suffix this design later dropped — see §5 of the design spec and
`metrics_body` in `crates/weir-ctl/src/attest.rs`). That metric is actually a
**gauge**, rewritten whole on every cron run rather than accumulated — never
a counter, which is also why the suffix was wrong and got dropped.
`increase()` on a gauge that goes to 1 and then *holds* at 1 across every
subsequent cron run shows zero increase after one evaluation window, and the
alert goes quiet while the segment is still tampered. The corrected rule
matches the raw gauge value instead:

```yaml
      - alert: WeirAttestVerifyFailed
        expr: weir_attest_verify_failures > 0
        for: 0m
        labels: { severity: critical }
        annotations:
          summary: "weir attest verification FAILED on {{ $labels.instance }}"
          description: "A sealed WAB segment no longer matches its recorded hash chain. This is an integrity incident, not a durability one: the records were acked and fsynced correctly, and the bytes on disk have since stopped matching what was chained. Do not delete the segment. Compare the chain head against the value your log pipeline recorded at seal time, and run `weir-ctl attest verify --wab-dir <dir>` for the diverging segment."
          runbook: "docs/monitoring.md#weirattestverifyfailed"
```

- [ ] **Step 2: Add BOTH directions to the test suite**

A quiet case (flat counter, `exp_alerts: []`) and a firing case. **This is mandatory** — `every_alert_rule_has_a_unit_test` in `crates/weir-server/tests/docs_drift.rs` fails the build otherwise, by design.

**Do not assert any annotation containing a float-derived value.** A previous test pinned `49.96ms`, which renders `49.95ms` on amd64 and turned CI red.

- [ ] **Step 3: Add the runbook heading — and ONLY the heading**

`docs/monitoring.md` needs a `#### WeirAttestVerifyFailed` heading with its remediation text, or `every_alert_runbook_anchor_resolves_to_a_heading` fails in Task 10.

**Boundary with Task 9, so the two do not collide in the same file:** this task owns the `#### WeirAttestVerifyFailed` runbook heading and nothing else in `docs/monitoring.md`. Task 9 owns the separate prose section describing `weir-ctl attest` and the anchoring requirement, and must NOT re-add this heading.

- [ ] **Step 4: Verify on both architectures**

```bash
docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 test rules weir-alerts_test.yml
docker run --rm --platform linux/amd64 --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 test rules weir-alerts_test.yml
```
Both must print `SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add deploy/prometheus/weir-alerts.yml deploy/prometheus/weir-alerts_test.yml docs/monitoring.md
git commit -m "feat(alerts): WeirAttestVerifyFailed, pinned in both directions"
```

---

### Task 8: CI for the harnesses that never ran

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `CONTRIBUTING.md`

**Context the implementer needs:** `fuzz/` and `chaos/` are **standalone workspaces on purpose** — `fuzz/Cargo.toml` says so ("so the fuzz crate doesn't pull `weir-server` into a nightly toolchain"), and `chaos/Cargo.toml` says so ("Linux-only, needs root, device-mapper tooling"). **Do not add them to the root workspace `members`.** That would undo a deliberate decision. Build them *in place* instead.

- [ ] **Step 1: Add a `harnesses` job**

```yaml
  harnesses:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
      - name: Fuzz targets still compile
        run: cd fuzz && cargo +nightly build --all-targets
      - uses: dtolnay/rust-toolchain@stable
      - name: Chaos harness still compiles
        run: cd chaos && cargo check --all-targets
```

Rationale to put in a comment above the job: these two are standalone workspaces, so `cargo build` from the repo root never touches them — three fuzz targets and two chaos binaries could stop compiling and nothing would notice. This compiles them without running them; the fuzz *runs* and the chaos *runs* stay manual, because one needs a fuzzing budget and the other needs root and device-mapper.

- [ ] **Step 2: Fix `CONTRIBUTING.md`**

It currently claims the heavier suites "run in their own CI jobs" and lists `cargo +nightly fuzz run envelope_parse` under that heading. There is no fuzzing CI job that *runs* fuzz. Correct it to say the targets are **compiled** in CI and that running them is manual.

- [ ] **Step 3: Verify locally**

```bash
cd fuzz && cargo +nightly build --all-targets; cd ..
cd chaos && cargo check --all-targets; cd ..
```

If the nightly toolchain is unavailable on this machine, say so in the report rather than skipping silently — the job still needs to be right.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml CONTRIBUTING.md
git commit -m "ci: compile the fuzz and chaos harnesses, which nothing has been building"
```

---

### Task 9: Documentation and the changelog

**Files:**
- Modify: `docs/security/threat-model.md`
- Modify: `docs/monitoring.md`
- Modify: `CHANGELOG.md`
- Modify: `README.md` (the stale "Status — 2.0 (stable)" banner)

- [ ] **Step 1: Update the threat model**

Move the **Signed audit log** row out of "Out of scope" and replace it with an accurate entry. It must say what is now covered (offline tampering, a different UID, a shared filesystem, CRC-passing corruption) **and** what is still not (a process running as the daemon UID, non-repudiation, prevention). Do not overclaim — §1.2 of the spec is the wording to follow.

- [ ] **Step 2: Add the monitoring section**

A prose section describing `weir-ctl attest` and the anchoring requirement — a chain nobody observed off-host is not evidence.

**Do NOT add the `#### WeirAttestVerifyFailed` heading here.** Task 7 already added it; adding it again produces a duplicate heading and a confusing runbook link target.

- [ ] **Step 3: Write the CHANGELOG entry**

Under `### Added` in `[Unreleased]`. Lead with what it proves and what it does not. Include the publish-order change: `core → wab → sink-sdk → attest → sink-s3 → client → rs → server → ctl`.

- [ ] **Step 4: Fix the README status banner**

`README.md:50` says "Status — 2.0 (stable)" while the workspace is 3.0.0 heading to 4.0.0. Nothing guards it.

- [ ] **Step 5: Run the drift suites**

```bash
cargo test -p weir-server --test docs_drift --test demo_drift
```

- [ ] **Step 6: Commit**

```bash
git add docs/security/threat-model.md docs/monitoring.md CHANGELOG.md README.md
git commit -m "docs(attest): what the chain proves, and what it does not"
```

---

### Task 10: Full gate and PR

- [ ] **Step 1: The whole local gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo test --workspace
cargo test -p weir-server --bins
cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1
python3 docs/conformance/run_vectors.py
```

- [ ] **Step 2: `act` on every job touched**

```bash
act -W .github/workflows/ci.yml -j lint --pull=false
act -W .github/workflows/ci.yml -j test --pull=false
act -W .github/workflows/ci.yml -j conformance --pull=false
act -W .github/workflows/ci.yml -j harnesses --pull=false
```

Known false failure on this machine: the `docs` workflow fails under `act` because it runs an amd64 `mdbook` under Rosetta in an arm64 container. Verify with native `mdbook build` instead.

- [ ] **Step 3: Open the PR**

Body must state: what the feature proves and what it does not; the §5 metrics deviation (textfile collector, not daemon metrics) flagged for the maintainer; the publish-order change; that `fuzz/`/`chaos/` were deliberately NOT made workspace members and why.

---

## Self-review against the spec

| Spec section | Task |
|---|---|
| §1.2 honest threat model | Task 9 step 1, and the crate docs in Task 1 step 5 |
| §2 separate crate, no format change | Task 1 (global constraint forbids format edits) |
| §3.1 chain over `RecordId` | Task 1 step 9, Task 3 step 3 |
| §3.2 per-segment chains, linked | Task 3 (`prev`), Task 6 (link in segment order) |
| §3.3 `.attest` sidecar | Task 2 |
| §4 ctl surface + first divergent index | Task 6, Task 4 |
| §5 anchor | Task 6 step 3 (log line), Task 6/7 (metrics — **deviation flagged**) |
| §6.1 window observable | **Gap:** `weir_attest_lag_seconds` has no carrier without the daemon. Flag rather than fake it. |
| §6.2 verification cost | Task 9 step 2 |
| §7 out of scope | Global constraints |

**Known gaps flagged rather than papered over:** `weir_attest_lag_seconds` (§6.1) and the daemon-side metrics (§5) both need the in-daemon hook, which spec open question 1 defers. Task 4's `first_divergence` cannot locate a mid-file change without per-record checkpoints — decide with the maintainer.
