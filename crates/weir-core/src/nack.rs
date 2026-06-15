//! The [`NackReason`] byte that prefixes every Nack message payload.

/// Reason byte carried as the first byte of every Nack message payload.
/// Wire values are fixed and must not change without a WIRE_VERSION bump.
///
/// VersionMismatch Nack payload format: `[NackReason::VersionMismatch (0x02), daemon_wire_version (u8)]`.
/// The second byte lets the client produce a specific error:
/// "daemon is on wire protocol v1; this client is built against v2 — upgrade the daemon or downgrade the client."
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackReason {
    /// Frame did not start with the `b"WEIR"` magic.
    BadMagic = 0x01,
    /// Frame's version byte did not equal the daemon's `WIRE_VERSION`. The Nack
    /// payload's second byte carries the daemon's version (see above).
    VersionMismatch = 0x02,
    /// Header CRC32 did not match the header bytes.
    BadHeaderCrc = 0x03,
    /// Declared `payload_len` exceeded the daemon's effective cap.
    PayloadTooLarge = 0x04,
    /// Payload CRC32 did not match the payload bytes.
    BadPayloadCrc = 0x05,
    /// The daemon hit an internal error (e.g. queue saturation); transient.
    InternalError = 0x06,
    /// The push carried a zero-length payload, which the WAB cannot represent:
    /// an empty record's length prefix is four zero bytes, identical to the
    /// end-of-records sentinel, so storing one would truncate the segment.
    /// Rejected at ingest.
    EmptyPayload = 0x07,
    /// The frame's header was structurally valid (magic, version, and header CRC
    /// all passed) but carried a `message_type` or `durability` byte this daemon
    /// does not recognise — a PERMANENT client protocol error (typically version
    /// skew). Distinct from `InternalError` (a transient daemon-side condition
    /// that keeps the connection open): the daemon closes the connection after
    /// this Nack. Retrying the identical frame will not succeed (F25).
    UnknownMessage = 0x08,
    /// The frame's header was structurally valid but set one or more bits in the
    /// reserved `flags` byte, which must be zero in wire v1. A daemon rejects
    /// such a frame rather than silently ignoring a flag it does not understand
    /// (which could mean a producer believed a semantic flag took effect when it
    /// did not). Permanent; the daemon closes the connection (F52).
    ReservedFlagsSet = 0x09,
}

/// Error returned when a `u8` does not map to a known `NackReason` variant.
/// Preserves the raw byte so the client can log or display it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownNackReason(pub u8);

impl std::fmt::Display for UnknownNackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown nack reason byte: {:#04x}", self.0)
    }
}

impl std::error::Error for UnknownNackReason {}

impl TryFrom<u8> for NackReason {
    type Error = UnknownNackReason;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(NackReason::BadMagic),
            0x02 => Ok(NackReason::VersionMismatch),
            0x03 => Ok(NackReason::BadHeaderCrc),
            0x04 => Ok(NackReason::PayloadTooLarge),
            0x05 => Ok(NackReason::BadPayloadCrc),
            0x06 => Ok(NackReason::InternalError),
            0x07 => Ok(NackReason::EmptyPayload),
            0x08 => Ok(NackReason::UnknownMessage),
            0x09 => Ok(NackReason::ReservedFlagsSet),
            v => Err(UnknownNackReason(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::WIRE_VERSION;

    #[test]
    fn try_from_accepts_all_known_reasons() {
        assert_eq!(NackReason::try_from(0x01).unwrap(), NackReason::BadMagic);
        assert_eq!(
            NackReason::try_from(0x02).unwrap(),
            NackReason::VersionMismatch
        );
        assert_eq!(
            NackReason::try_from(0x03).unwrap(),
            NackReason::BadHeaderCrc
        );
        assert_eq!(
            NackReason::try_from(0x04).unwrap(),
            NackReason::PayloadTooLarge
        );
        assert_eq!(
            NackReason::try_from(0x05).unwrap(),
            NackReason::BadPayloadCrc
        );
        assert_eq!(
            NackReason::try_from(0x06).unwrap(),
            NackReason::InternalError
        );
        assert_eq!(
            NackReason::try_from(0x07).unwrap(),
            NackReason::EmptyPayload
        );
        assert_eq!(
            NackReason::try_from(0x08).unwrap(),
            NackReason::UnknownMessage
        );
        assert_eq!(
            NackReason::try_from(0x09).unwrap(),
            NackReason::ReservedFlagsSet
        );
    }

    #[test]
    fn try_from_returns_unknown_for_unrecognised_byte() {
        // 0x0A is the first unassigned reason byte (0x01..=0x09 are known).
        let err = NackReason::try_from(0x0A).unwrap_err();
        assert_eq!(err.0, 0x0A);
        let err = NackReason::try_from(0x00).unwrap_err();
        assert_eq!(err.0, 0x00);
        let err = NackReason::try_from(0xff).unwrap_err();
        assert_eq!(err.0, 0xff);
    }

    #[test]
    fn repr_values_match_wire() {
        assert_eq!(NackReason::BadMagic as u8, 0x01);
        assert_eq!(NackReason::VersionMismatch as u8, 0x02);
        assert_eq!(NackReason::BadHeaderCrc as u8, 0x03);
        assert_eq!(NackReason::PayloadTooLarge as u8, 0x04);
        assert_eq!(NackReason::BadPayloadCrc as u8, 0x05);
        assert_eq!(NackReason::InternalError as u8, 0x06);
        assert_eq!(NackReason::EmptyPayload as u8, 0x07);
        assert_eq!(NackReason::UnknownMessage as u8, 0x08);
        assert_eq!(NackReason::ReservedFlagsSet as u8, 0x09);
    }

    /// Verifies the VersionMismatch Nack payload is [reason_byte, daemon_version_byte].
    /// The client parses this to produce: "daemon is on vN; this client is built against vM."
    #[test]
    fn version_mismatch_nack_payload_encodes_daemon_version() {
        let payload = [NackReason::VersionMismatch as u8, WIRE_VERSION];
        assert_eq!(payload, [0x02, 0x01]);
    }
}
