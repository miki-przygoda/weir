use std::fmt;

/// All errors that can occur when decoding a weir wire frame.
///
/// Variants are ordered by the decode sequence: magic → version → header CRC → payload fields.
/// This ordering is meaningful: the server uses the specific variant to determine which Nack
/// reason byte to send. `VersionMismatch` is distinct from `BadMagic` and `HeaderCrcMismatch`
/// so the server can send the daemon's WIRE_VERSION in the Nack payload.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// First four bytes are not `b"WEIR"`. Not a weir frame or stream is desynced.
    BadMagic,
    /// Version byte does not equal `WIRE_VERSION`. Carries both sides for error messaging.
    /// Checked before header CRC so a version-mismatched frame from a newer client gets
    /// `VersionMismatch`, not a confusing `HeaderCrcMismatch`.
    VersionMismatch { supported: u8, received: u8 },
    /// Message type byte has no known variant.
    UnknownMessageType(u8),
    /// Durability byte has no known variant.
    UnknownDurability(u8),
    /// Header CRC32 (bytes [12..16]) does not match CRC of bytes [0..12].
    HeaderCrcMismatch { expected: u32, computed: u32 },
    /// Payload CRC32 (trailing 4 bytes) does not match CRC of the payload bytes.
    PayloadCrcMismatch { expected: u32, computed: u32 },
    /// Buffer is shorter than the declared frame length.
    TruncatedFrame,
    /// `payload_len` exceeds the hard cap. Rejection happens before any heap allocation.
    PayloadTooLarge { len: usize, cap: usize },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => {
                write!(f, "bad magic bytes: not a weir frame")
            }
            Self::VersionMismatch { supported, received } => {
                write!(
                    f,
                    "wire version mismatch: daemon supports v{supported}, frame carries v{received}"
                )
            }
            Self::UnknownMessageType(t) => {
                write!(f, "unknown message type: {t:#04x}")
            }
            Self::UnknownDurability(d) => {
                write!(f, "unknown durability byte: {d:#04x}")
            }
            Self::HeaderCrcMismatch { expected, computed } => {
                write!(
                    f,
                    "header CRC mismatch: expected {expected:#010x}, computed {computed:#010x}"
                )
            }
            Self::PayloadCrcMismatch { expected, computed } => {
                write!(
                    f,
                    "payload CRC mismatch: expected {expected:#010x}, computed {computed:#010x}"
                )
            }
            Self::TruncatedFrame => {
                write!(f, "truncated frame: buffer shorter than declared length")
            }
            Self::PayloadTooLarge { len, cap } => {
                write!(f, "payload length {len} exceeds cap {cap}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Top-level error type for the weir-core crate.
#[derive(Debug)]
pub enum WeirError {
    Decode(DecodeError),
}

impl fmt::Display for WeirError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(e) => write!(f, "decode error: {e}"),
        }
    }
}

impl std::error::Error for WeirError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => Some(e),
        }
    }
}

impl From<DecodeError> for WeirError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_error_display_is_human_readable() {
        let e = DecodeError::VersionMismatch { supported: 1, received: 2 };
        let s = e.to_string();
        assert!(s.contains("v1"), "display should contain supported version: {s}");
        assert!(s.contains("v2"), "display should contain received version: {s}");
    }

    #[test]
    fn weir_error_source_chains_to_decode_error() {
        use std::error::Error;
        let e = WeirError::from(DecodeError::BadMagic);
        assert!(e.source().is_some());
    }
}
