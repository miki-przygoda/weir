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
#[non_exhaustive]
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
