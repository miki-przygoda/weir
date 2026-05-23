//! On-disk format constants and byte-level helpers for WAB segment files.
//!
//! # File structure
//!
//! ```text
//! Segment header  — SEGMENT_HEADER_LEN (24) bytes
//! [0..4]   SEGMENT_MAGIC   b"WEIR"
//! [4]      FORMAT_VERSION  u8 = 1
//! [5]      reserved        u8 — zero on write
//! [6..8]   shard_id        u16 LE
//! [8..16]  created_at      i64 LE — unix nanoseconds
//! [16..24] reserved        [u8; 8] — zero on write
//!
//! Per-record (repeated):
//! [0..4]   payload_len     u32 LE
//! [4..8]   crc32           u32 LE — CRC32 of payload bytes only
//! [8..]    payload bytes
//!
//! End-of-records sentinel: [0u8; 4] — payload_len == 0
//!
//! Segment footer  — SEGMENT_FOOTER_LEN (32) bytes (immediately after sentinel)
//! [0..8]   record_count    u64 LE
//! [8..16]  data_bytes      u64 LE — total payload bytes
//! [16..20] file_crc32      u32 LE — CRC32 of all file bytes before the sentinel
//! [20..28] sealed_at       i64 LE — unix nanoseconds
//! [28..32] reserved        [u8; 4] — zero on write
//! ```
//!
//! Active segments: `seg_NNNNNNNN.wab`
//! Sealed segments: `seg_NNNNNNNN.wab.sealed`
//!
//! # Confirmation file (`.confirmed`)
//!
//! Path: `<shard_dir>/seg_NNNNNNNN.wab.confirmed`
//!
//! ```text
//! CONFIRMED_LEN (36) bytes total:
//! [0..4]   CONFIRMED_MAGIC b"WCON" — distinct from SEGMENT_MAGIC; a misplaced segment
//!                                     file cannot be parsed as a confirmation file
//! [4]      version         u8 = 1
//! [5..8]   reserved        [u8; 3] — zero on write; reserved for flags
//! [8..16]  sealed_at       i64 LE — unix nanos, copied from segment footer
//! [16..24] record_count    u64 LE — number of records drained from this segment
//! [24..32] drained_at      i64 LE — unix nanos when drain completed
//! [32..36] crc32           u32 LE — CRC32 of bytes [0..32]
//! ```
//!
//! # Security model
//!
//! CRC32 detects accidental corruption; it does not detect malicious modification.
//! A forged WAB segment or confirmation file with a valid CRC32 will be accepted.
//!
//! The WAB directory must be accessible only to the daemon process. Filesystem
//! permissions enforce this: `0700` on the directory, `0600` on segment files.
//! If the WAB is on a network filesystem or shared storage, the security model
//! does not hold. This is an explicit assumption, not a weakness to be fixed.

use std::time::{SystemTime, UNIX_EPOCH};

/// Identifies a WAB segment file. Distinct from `weir-core`'s wire magic `b"WEIR"` —
/// they share the same bytes intentionally, since this is the weir project's namespace,
/// but the FORMAT_VERSION byte distinguishes segment files from wire frames.
pub const SEGMENT_MAGIC: [u8; 4] = *b"WEIR";

pub const FORMAT_VERSION: u8 = 1;

/// Segment header size in bytes. Fixed for the lifetime of FORMAT_VERSION = 1.
pub const SEGMENT_HEADER_LEN: usize = 24;

/// Segment footer size in bytes. Fixed for the lifetime of FORMAT_VERSION = 1.
pub const SEGMENT_FOOTER_LEN: usize = 32;

/// End-of-records sentinel. A payload_len field of zero signals the footer follows.
pub const SENTINEL: [u8; 4] = [0u8; 4];

/// Rotate the active segment when it reaches this size (including header).
pub const SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;

pub const CONFIRMED_MAGIC: [u8; 4] = *b"WCON";
pub const CONFIRMED_VERSION: u8 = 1;
/// Total byte length of a `.confirmed` file.
pub const CONFIRMED_LEN: usize = 36;

/// Extension for active (unsealed) segment files.
pub const EXT_ACTIVE: &str = ".wab";
/// Extension for sealed segment files.
pub const EXT_SEALED: &str = ".wab.sealed";
/// Extension for confirmation files.
pub const EXT_CONFIRMED: &str = ".wab.confirmed";

/// Returns unix nanoseconds as `i64`. Overflows after year 2262; on overflow returns
/// `i64::MAX` rather than wrapping silently (the `as i64` cast would wrap to negative).
pub fn unix_nanos_now() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    nanos.try_into().unwrap_or(i64::MAX)
}

/// Builds the 24-byte segment file header.
pub fn build_segment_header(shard_id: u16) -> [u8; SEGMENT_HEADER_LEN] {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
    buf[4] = FORMAT_VERSION;
    // buf[5] = 0  (reserved)
    buf[6..8].copy_from_slice(&shard_id.to_le_bytes());
    buf[8..16].copy_from_slice(&unix_nanos_now().to_le_bytes());
    // buf[16..24] = 0  (reserved)
    buf
}

/// Builds the 4-byte end-of-records sentinel.
#[inline]
pub fn build_sentinel() -> [u8; 4] {
    SENTINEL
}

/// Builds the 32-byte segment footer.
pub fn build_segment_footer(
    record_count: u64,
    data_bytes: u64,
    file_crc32: u32,
    sealed_at: i64,
) -> [u8; SEGMENT_FOOTER_LEN] {
    let mut buf = [0u8; SEGMENT_FOOTER_LEN];
    buf[0..8].copy_from_slice(&record_count.to_le_bytes());
    buf[8..16].copy_from_slice(&data_bytes.to_le_bytes());
    buf[16..20].copy_from_slice(&file_crc32.to_le_bytes());
    buf[20..28].copy_from_slice(&sealed_at.to_le_bytes());
    // buf[28..32] = 0  (reserved)
    buf
}

/// Parsed confirmation file contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedMeta {
    pub sealed_at: i64,
    pub record_count: u64,
    pub drained_at: i64,
}

/// Builds the 36-byte `.confirmed` file for a drained segment.
/// `sealed_at` is copied from the segment footer's `sealed_at` field.
pub fn build_confirmed(sealed_at: i64, record_count: u64, drained_at: i64) -> [u8; CONFIRMED_LEN] {
    let mut buf = [0u8; CONFIRMED_LEN];
    buf[0..4].copy_from_slice(&CONFIRMED_MAGIC);
    buf[4] = CONFIRMED_VERSION;
    // buf[5..8] = 0  (reserved)
    buf[8..16].copy_from_slice(&sealed_at.to_le_bytes());
    buf[16..24].copy_from_slice(&record_count.to_le_bytes());
    buf[24..32].copy_from_slice(&drained_at.to_le_bytes());
    let crc = crc32fast::hash(&buf[..32]);
    buf[32..36].copy_from_slice(&crc.to_le_bytes());
    buf
}

#[derive(Debug)]
pub enum ConfirmedParseError {
    /// Not 36 bytes.
    WrongLength { got: usize },
    /// First four bytes are not `b"WCON"`.
    BadMagic,
    /// Version byte is not 1. Quarantine instead of treating as unconfirmed to
    /// avoid potential double-drain on unknown format.
    UnknownVersion(u8),
    /// CRC32 of bytes [0..32] does not match bytes [32..36].
    CrcMismatch { expected: u32, computed: u32 },
}

impl std::fmt::Display for ConfirmedParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongLength { got } => write!(f, "confirmation file is {got} bytes, expected {CONFIRMED_LEN}"),
            Self::BadMagic => write!(f, "confirmation file has bad magic (expected b\"WCON\")"),
            Self::UnknownVersion(v) => write!(
                f,
                "unknown confirmation format version {v}; cannot safely determine drain \
                 status — treating as unconfirmed would risk double-drain, quarantining instead"
            ),
            Self::CrcMismatch { expected, computed } => write!(
                f,
                "confirmation CRC mismatch: expected {expected:#010x}, computed {computed:#010x}"
            ),
        }
    }
}

/// Parses a `.confirmed` file's byte content.
/// Returns `Ok(ConfirmedMeta)` only when magic, version, and CRC all pass.
pub fn parse_confirmed(buf: &[u8]) -> Result<ConfirmedMeta, ConfirmedParseError> {
    if buf.len() != CONFIRMED_LEN {
        return Err(ConfirmedParseError::WrongLength { got: buf.len() });
    }
    if buf[0..4] != CONFIRMED_MAGIC {
        return Err(ConfirmedParseError::BadMagic);
    }
    if buf[4] != CONFIRMED_VERSION {
        return Err(ConfirmedParseError::UnknownVersion(buf[4]));
    }
    let expected_crc = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    let computed_crc = crc32fast::hash(&buf[..32]);
    if expected_crc != computed_crc {
        return Err(ConfirmedParseError::CrcMismatch {
            expected: expected_crc,
            computed: computed_crc,
        });
    }
    Ok(ConfirmedMeta {
        sealed_at: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
        record_count: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        drained_at: i64::from_le_bytes(buf[24..32].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_encode_decode_round_trip() {
        let sealed_at = 1_700_000_000_000_000_000i64;
        let record_count = 42u64;
        let drained_at = 1_700_000_001_000_000_000i64;

        let bytes = build_confirmed(sealed_at, record_count, drained_at);
        assert_eq!(bytes.len(), CONFIRMED_LEN);

        let meta = parse_confirmed(&bytes).unwrap();
        assert_eq!(meta.sealed_at, sealed_at);
        assert_eq!(meta.record_count, record_count);
        assert_eq!(meta.drained_at, drained_at);
    }

    #[test]
    fn confirmed_parse_rejects_bad_magic() {
        let mut bytes = build_confirmed(0, 0, 0);
        bytes[0] = 0xff;
        assert!(matches!(parse_confirmed(&bytes), Err(ConfirmedParseError::BadMagic)));
    }

    #[test]
    fn confirmed_parse_rejects_unknown_version() {
        let mut bytes = build_confirmed(0, 0, 0);
        bytes[4] = 42;
        // Recompute CRC so we test the version path, not the CRC path.
        let crc = crc32fast::hash(&bytes[..32]);
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());
        match parse_confirmed(&bytes) {
            Err(ConfirmedParseError::UnknownVersion(42)) => {}
            other => panic!("expected UnknownVersion(42), got {other:?}"),
        }
    }

    #[test]
    fn confirmed_parse_rejects_bad_crc() {
        let mut bytes = build_confirmed(0, 0, 0);
        bytes[32] ^= 0xff;
        assert!(matches!(
            parse_confirmed(&bytes),
            Err(ConfirmedParseError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn confirmed_parse_rejects_wrong_length() {
        assert!(matches!(
            parse_confirmed(&[0u8; 35]),
            Err(ConfirmedParseError::WrongLength { got: 35 })
        ));
    }

    #[test]
    fn segment_header_has_correct_magic_and_version() {
        let header = build_segment_header(3);
        assert_eq!(&header[0..4], b"WEIR");
        assert_eq!(header[4], FORMAT_VERSION);
        assert_eq!(header[5], 0); // reserved
        assert_eq!(&header[6..8], &3u16.to_le_bytes());
    }

    #[test]
    fn unix_nanos_now_is_positive() {
        assert!(unix_nanos_now() > 0);
    }
}
