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

    /// Parse 64 lower- or upper-hex characters (mixed case is fine).
    pub fn from_hex(s: &str) -> Result<Self, crate::AttestError> {
        if s.len() != 64 {
            return Err(crate::AttestError::BadHexLength { len: s.len() });
        }
        // Byte length can be 64 with fewer than 64 chars if any of them are
        // multi-byte UTF-8 (e.g. one 3-byte char plus 61 ASCII bytes). Reject
        // that up front: every char below is indexed by byte offset, which is
        // only ever a char boundary when the whole string is ASCII.
        if !s.is_ascii() {
            return Err(crate::AttestError::BadHexDigit);
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| crate::AttestError::BadHexDigit)?;
        }
        Ok(ChainHead(out))
    }
}

impl std::fmt::Debug for ChainHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChainHead({})", self.to_hex())
    }
}

/// `H₀` — the head a segment's chain starts from.
///
/// Commits to the predecessor's head, the segment header, and the segment's
/// name. The name matters: without it, two segments with identical headers and
/// identical records would produce identical chains, and one could be
/// substituted for the other.
pub fn chain_origin(prev: ChainHead, header: &SegmentHeaderMeta, segment_name: &str) -> ChainHead {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_is_all_zero_and_round_trips_through_hex() {
        assert_eq!(ChainHead::ORIGIN.as_bytes(), &[0u8; 32]);
        assert_eq!(ChainHead::ORIGIN.to_hex(), "0".repeat(64));
    }

    // `SegmentHeaderMeta` is `#[non_exhaustive]`, so outside its defining crate
    // it cannot be built with a struct literal even with every field named —
    // that is what `#[non_exhaustive]` means. Round-trip it through weir-wab's
    // own byte-level constants and `parse_segment_header` instead, which yields
    // the exact same field values without needing any change to weir-wab.
    fn meta() -> SegmentHeaderMeta {
        meta_with(0, 1_700_000_000_000_000_000)
    }

    // Parameterised so a test can hold everything else constant and vary one
    // header field (see `a_different_shard_id_gives_a_different_origin`).
    fn meta_with(shard_id: u16, created_at: i64) -> SegmentHeaderMeta {
        use weir_wab::format::{SEGMENT_HEADER_LEN, SEGMENT_MAGIC, parse_segment_header};
        let mut buf = [0u8; SEGMENT_HEADER_LEN];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4] = 1; // format_version 1 (v1)
        buf[5] = 0; // flags — Compression::None under v1
        buf[6..8].copy_from_slice(&shard_id.to_le_bytes());
        buf[8..16].copy_from_slice(&created_at.to_le_bytes());
        parse_segment_header(&buf).expect("hand-built header is valid")
    }

    #[test]
    fn a_different_predecessor_gives_a_different_origin() {
        let a = chain_origin(
            ChainHead::ORIGIN,
            &meta(),
            "shard_00/seg_00000001.wab.sealed",
        );
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
        let a = chain_origin(
            ChainHead::ORIGIN,
            &meta(),
            "shard_00/seg_00000001.wab.sealed",
        );
        let b = chain_origin(
            ChainHead::ORIGIN,
            &meta(),
            "shard_00/seg_00000002.wab.sealed",
        );
        assert_ne!(a, b, "H0 must commit to the segment it covers");
    }

    #[test]
    fn from_hex_accepts_uppercase_the_same_as_lowercase() {
        let lower = "0f".repeat(32);
        let upper = "0F".repeat(32);
        let mixed = "0F0f".repeat(16);
        let want = ChainHead::from_bytes([0x0f; 32]);
        assert_eq!(ChainHead::from_hex(&lower).unwrap(), want);
        assert_eq!(ChainHead::from_hex(&upper).unwrap(), want);
        assert_eq!(ChainHead::from_hex(&mixed).unwrap(), want);
    }

    #[test]
    fn from_hex_rejects_a_multi_byte_char_instead_of_panicking() {
        // One 3-byte UTF-8 character ('中') plus 61 ASCII bytes is 64 bytes
        // long (so the length check alone would let it through) but only 62
        // chars, and the multi-byte character does not sit on a 2-byte
        // boundary — slicing the `&str` by byte index there must not panic.
        let s = format!("中{}", "a".repeat(61));
        assert_eq!(s.len(), 64, "test fixture must be exactly 64 bytes");
        let result = ChainHead::from_hex(&s);
        assert!(
            matches!(result, Err(crate::AttestError::BadHexDigit)),
            "a non-hex, non-ASCII byte must be a clean Err, not a panic; got {result:?}"
        );
    }

    #[test]
    fn a_different_shard_id_gives_a_different_origin() {
        let a = chain_origin(
            ChainHead::ORIGIN,
            &meta_with(0, 1_700_000_000_000_000_000),
            "shard_00/seg_00000001.wab.sealed",
        );
        let b = chain_origin(
            ChainHead::ORIGIN,
            &meta_with(1, 1_700_000_000_000_000_000),
            "shard_00/seg_00000001.wab.sealed",
        );
        assert_ne!(
            a, b,
            "H0 must commit to the segment header, or a forged header with the \
             same predecessor and segment name would produce the same chain"
        );
    }

    #[test]
    fn chain_step_commits_to_the_predecessor() {
        let r = RecordId::from_bytes([9u8; 32]);
        let head_a = ChainHead::from_bytes([1u8; 32]);
        let head_b = ChainHead::from_bytes([2u8; 32]);
        assert_ne!(
            chain_step(head_a, &r),
            chain_step(head_b, &r),
            "chain_step must commit to the predecessor, or the same record \
             appended to two different histories would produce the same head, \
             and history would not be preserved at all"
        );
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
}
