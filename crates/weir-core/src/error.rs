//! The decode failure taxonomy: [`DecodeError`] (one variant per frame-validation
//! step, which the daemon maps to a Nack reason) and the top-level [`WeirError`].

use std::fmt;

/// All errors that can occur when decoding a weir wire frame.
///
/// Each variant identifies a specific frame-validation failure, and the daemon
/// maps it to a distinct Nack reason byte: `VersionMismatch` is kept separate from
/// `BadMagic` and `HeaderCrcMismatch` so the daemon can return its `WIRE_VERSION`
/// in the Nack payload. The variants are NOT declared in strict decode order — the
/// decoder validates magic → version → header CRC → message_type / durability →
/// payload fields (so e.g. `UnknownMessageType` only arises on an
/// already-CRC-valid header). Match on the variant, never on its position.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// First four bytes are not `b"WEIR"`. Not a weir frame or stream is desynced.
    BadMagic,
    /// Version byte does not equal `WIRE_VERSION`. Carries both sides for error messaging.
    /// Checked before header CRC so a version-mismatched frame from a newer client gets
    /// `VersionMismatch`, not a confusing `HeaderCrcMismatch`.
    VersionMismatch {
        /// The `WIRE_VERSION` this build supports.
        supported: u8,
        /// The version byte carried by the rejected frame.
        received: u8,
    },
    /// Message type byte has no known variant.
    UnknownMessageType(u8),
    /// Durability byte has no known variant.
    UnknownDurability(u8),
    /// Header CRC32 (bytes [12..16]) does not match CRC of bytes [0..12].
    HeaderCrcMismatch {
        /// CRC carried in the header.
        expected: u32,
        /// CRC computed over bytes [0..12].
        computed: u32,
    },
    /// Payload CRC32 (trailing 4 bytes) does not match CRC of the payload bytes.
    PayloadCrcMismatch {
        /// CRC carried in the trailing 4 bytes.
        expected: u32,
        /// CRC computed over the payload bytes.
        computed: u32,
    },
    /// Buffer is shorter than the declared frame length.
    TruncatedFrame,
    /// `payload_len` exceeds the hard cap. Rejection happens before any heap allocation.
    PayloadTooLarge {
        /// The declared payload length.
        len: usize,
        /// The hard cap it exceeded.
        cap: usize,
    },
    /// The reserved `flags` byte was nonzero. In wire v1 `flags` must be zero; a
    /// daemon rejects a frame that sets any flag bit rather than silently
    /// ignoring it (F52). Carries the offending byte.
    ReservedFlagsSet {
        /// The nonzero `flags` byte the frame carried.
        flags: u8,
    },
    /// The buffer was longer than the single frame it declared. `Envelope::decode`
    /// requires its input to be exactly one frame (`HEADER_LEN + payload_len + 4`
    /// bytes): a longer buffer is rejected rather than decoding the first frame and
    /// silently discarding the remainder, so the caller — not the codec — owns
    /// framing and an over-long buffer surfaces as an error, not lost data (G18).
    TrailingBytes {
        /// The number of bytes past the declared frame length.
        extra: usize,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => {
                write!(f, "bad magic bytes: not a weir frame")
            }
            Self::VersionMismatch {
                supported,
                received,
            } => {
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
            Self::ReservedFlagsSet { flags } => {
                write!(f, "reserved flags byte must be zero, got {flags:#04x}")
            }
            Self::TrailingBytes { extra } => {
                write!(
                    f,
                    "buffer has {extra} byte(s) past the declared frame length"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Top-level error type for the weir-core crate.
#[derive(Debug)]
pub enum WeirError {
    /// A wire frame failed to decode.
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
        let e = DecodeError::VersionMismatch {
            supported: 1,
            received: 2,
        };
        let s = e.to_string();
        assert!(
            s.contains("v1"),
            "display should contain supported version: {s}"
        );
        assert!(
            s.contains("v2"),
            "display should contain received version: {s}"
        );
    }

    #[test]
    fn weir_error_source_chains_to_decode_error() {
        use std::error::Error;
        let e = WeirError::from(DecodeError::BadMagic);
        assert!(e.source().is_some());
    }
}
