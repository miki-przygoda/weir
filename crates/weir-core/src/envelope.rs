use crate::{
    durability::Durability,
    error::DecodeError,
    payload::Payload,
    version::{MAX_PAYLOAD_HARD_CAP, WIRE_VERSION},
};

/// Magic bytes at offset 0. Presence at a fixed offset allows stream resync by scanning for b"WEIR".
const MAGIC: [u8; 4] = *b"WEIR";

/// Wire layout (16 bytes, big-endian fields are little-endian per spec):
/// [0..4]   magic         b"WEIR"
/// [4]      version       u8        — WIRE_VERSION
/// [5]      message_type  u8        — MessageType repr
/// [6]      durability    u8        — Durability repr
/// [7]      flags         u8        — reserved; zero on write
/// [8..12]  payload_len   u32 LE
/// [12..16] header_crc32  u32 LE    — CRC32 of bytes [0..12]
pub const HEADER_LEN: usize = 16;

/// Number of header bytes covered by the CRC. Everything except the CRC field itself.
const HEADER_CRC_COVERAGE: usize = 12;

/// Minimum valid frame: header + zero-length payload + payload CRC.
pub const MIN_FRAME_LEN: usize = HEADER_LEN + 4;

/// Wire message types. Values are fixed; changing them requires a WIRE_VERSION bump.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Push = 0x01,
    Ack = 0x02,
    Nack = 0x03,
    HealthCheck = 0x04,
    HealthCheckResponse = 0x05,
}

/// Error returned when a byte does not map to a known MessageType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownMessageType(pub u8);

impl std::fmt::Display for UnknownMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown message type byte: {:#04x}", self.0)
    }
}

impl std::error::Error for UnknownMessageType {}

impl TryFrom<u8> for MessageType {
    type Error = UnknownMessageType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(MessageType::Push),
            0x02 => Ok(MessageType::Ack),
            0x03 => Ok(MessageType::Nack),
            0x04 => Ok(MessageType::HealthCheck),
            0x05 => Ok(MessageType::HealthCheckResponse),
            v => Err(UnknownMessageType(v)),
        }
    }
}

/// Decoded wire header. `magic` and `header_crc32` are not stored: magic is always
/// `b"WEIR"` after a successful decode (storing it invites confusion about validity),
/// and CRC is computed on encode rather than held as state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub message_type: MessageType,
    pub durability: Durability,
    /// Currently reserved; must be zero on write. Preserved verbatim through encode/decode.
    pub flags: u8,
    pub payload_len: u32,
}

impl Header {
    /// Constructs a header for outbound frames. Version is always `WIRE_VERSION`.
    pub fn new(
        message_type: MessageType,
        durability: Durability,
        flags: u8,
        payload_len: u32,
    ) -> Self {
        Self {
            version: WIRE_VERSION,
            message_type,
            durability,
            flags,
            payload_len,
        }
    }

    /// Serialises the header to exactly `HEADER_LEN` bytes. CRC is computed here;
    /// the caller does not need to know the CRC coverage range.
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = self.version;
        buf[5] = self.message_type as u8;
        buf[6] = self.durability as u8;
        buf[7] = self.flags;
        buf[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decodes and validates a 16-byte header buffer.
    ///
    /// Validation order is deliberate:
    /// 1. Magic — cheapest check; eliminates non-weir traffic before any further work.
    /// 2. Version — before CRC, so a version-mismatched frame from a v2 client receives
    ///    `VersionMismatch` (with the daemon's version in the Nack payload) rather than
    ///    a confusing `HeaderCrcMismatch` if the frame layout shifted between versions.
    /// 3. Header CRC — validates the remaining bytes are uncorrupted before parsing them.
    /// 4. Typed field parsing — only after all integrity checks pass.
    pub fn decode(buf: &[u8; HEADER_LEN]) -> Result<Self, DecodeError> {
        if buf[0..4] != MAGIC {
            return Err(DecodeError::BadMagic);
        }

        let version = buf[4];
        if version != WIRE_VERSION {
            return Err(DecodeError::VersionMismatch {
                supported: WIRE_VERSION,
                received: version,
            });
        }

        let expected_crc = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let computed_crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        if expected_crc != computed_crc {
            return Err(DecodeError::HeaderCrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
            });
        }

        let message_type =
            MessageType::try_from(buf[5]).map_err(|e| DecodeError::UnknownMessageType(e.0))?;
        let durability =
            Durability::try_from(buf[6]).map_err(|e| DecodeError::UnknownDurability(e.0))?;
        let flags = buf[7];
        let payload_len = u32::from_le_bytes(buf[8..12].try_into().unwrap());

        Ok(Self {
            version,
            message_type,
            durability,
            flags,
            payload_len,
        })
    }
}

/// A complete weir wire frame: validated header + payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub header: Header,
    pub payload: Payload,
}

impl Envelope {
    pub fn new(header: Header, payload: Payload) -> Self {
        Self { header, payload }
    }

    /// Serialises the full frame: header bytes + payload bytes + payload CRC32 (LE).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + 4);
        out.extend_from_slice(&self.header.encode());
        out.extend_from_slice(&self.payload);
        let payload_crc = crc32fast::hash(&self.payload);
        out.extend_from_slice(&payload_crc.to_le_bytes());
        out
    }

    /// Decodes and validates a complete frame from a byte slice.
    ///
    /// Validation order defends against pre-allocation DoS:
    /// 1. Buffer length >= HEADER_LEN — check before any parsing.
    /// 2. Header decode (magic → version → CRC) — before touching payload.
    /// 3. `payload_len <= MAX_PAYLOAD_HARD_CAP` — rejection before any heap allocation.
    /// 4. Buffer length >= HEADER_LEN + payload_len + 4 — check frame completeness.
    /// 5. Payload CRC — validate after allocation, before returning data to caller.
    ///
    /// The socket layer applies an additional cap (`min(config.max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`)
    /// before reaching this function. This function applies only the hard cap as a safety floor.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < HEADER_LEN {
            return Err(DecodeError::TruncatedFrame);
        }

        let header_bytes: &[u8; HEADER_LEN] = buf[..HEADER_LEN]
            .try_into()
            .expect("slice is exactly HEADER_LEN bytes, checked above");
        let header = Header::decode(header_bytes)?;

        let payload_len = header.payload_len as usize;
        if payload_len > MAX_PAYLOAD_HARD_CAP {
            return Err(DecodeError::PayloadTooLarge {
                len: payload_len,
                cap: MAX_PAYLOAD_HARD_CAP,
            });
        }

        // payload_len <= MAX_PAYLOAD_HARD_CAP (16 MiB), so HEADER_LEN + payload_len + 4
        // cannot overflow usize on any supported platform (min usize on 32-bit: 4 GiB).
        let frame_len = HEADER_LEN + payload_len + 4;
        if buf.len() < frame_len {
            return Err(DecodeError::TruncatedFrame);
        }

        let payload = buf[HEADER_LEN..HEADER_LEN + payload_len].to_vec();
        let expected_crc =
            u32::from_le_bytes(buf[HEADER_LEN + payload_len..frame_len].try_into().unwrap());
        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            return Err(DecodeError::PayloadCrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
            });
        }

        Ok(Self { header, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::WIRE_VERSION;

    fn push_header(payload_len: u32) -> Header {
        Header::new(MessageType::Push, Durability::Sync, 0, payload_len)
    }

    // ── Header ──────────────────────────────────────────────────────────────

    #[test]
    fn header_encode_decode_round_trip() {
        let h = push_header(42);
        let encoded = h.encode();
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn header_encode_is_16_bytes() {
        let encoded = push_header(0).encode();
        assert_eq!(encoded.len(), HEADER_LEN);
    }

    #[test]
    fn header_decode_rejects_bad_magic() {
        let mut buf = push_header(0).encode();
        buf[0] = 0x00;
        assert_eq!(Header::decode(&buf), Err(DecodeError::BadMagic));
    }

    #[test]
    fn header_decode_rejects_future_version() {
        let mut buf = push_header(0).encode();
        buf[4] = WIRE_VERSION + 1;
        // Must recalculate CRC so we test the version path, not the CRC path.
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::VersionMismatch {
                supported: WIRE_VERSION,
                received: WIRE_VERSION + 1,
            })
        );
    }

    #[test]
    fn header_decode_rejects_older_version() {
        let mut buf = push_header(0).encode();
        buf[4] = WIRE_VERSION - 1;
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::VersionMismatch {
                supported: WIRE_VERSION,
                received: WIRE_VERSION - 1,
            })
        );
    }

    /// Version is checked before CRC. A frame with a wrong version but valid CRC must
    /// return VersionMismatch, not HeaderCrcMismatch.
    #[test]
    fn header_decode_version_checked_before_crc() {
        let mut buf = push_header(0).encode();
        // Set version to a future value and recompute a valid CRC for that version.
        buf[4] = WIRE_VERSION + 1;
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        // CRC is now valid for this (future-versioned) header.
        match Header::decode(&buf) {
            Err(DecodeError::VersionMismatch { .. }) => {}
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn header_decode_rejects_corrupted_crc() {
        let mut buf = push_header(0).encode();
        buf[12] ^= 0xff;
        assert!(matches!(
            Header::decode(&buf),
            Err(DecodeError::HeaderCrcMismatch { .. })
        ));
    }

    #[test]
    fn header_decode_rejects_unknown_message_type() {
        let mut buf = push_header(0).encode();
        buf[5] = 0xff;
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::UnknownMessageType(0xff))
        );
    }

    #[test]
    fn header_decode_rejects_unknown_durability() {
        let mut buf = push_header(0).encode();
        buf[6] = 0xff;
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::UnknownDurability(0xff))
        );
    }

    // ── MessageType ──────────────────────────────────────────────────────────

    #[test]
    fn message_type_try_from_accepts_all_known_values() {
        assert_eq!(MessageType::try_from(0x01).unwrap(), MessageType::Push);
        assert_eq!(MessageType::try_from(0x02).unwrap(), MessageType::Ack);
        assert_eq!(MessageType::try_from(0x03).unwrap(), MessageType::Nack);
        assert_eq!(
            MessageType::try_from(0x04).unwrap(),
            MessageType::HealthCheck
        );
        assert_eq!(
            MessageType::try_from(0x05).unwrap(),
            MessageType::HealthCheckResponse
        );
    }

    #[test]
    fn message_type_try_from_rejects_unknown() {
        assert!(MessageType::try_from(0x00).is_err());
        assert!(MessageType::try_from(0x06).is_err());
        assert!(MessageType::try_from(0xff).is_err());
    }

    #[test]
    fn message_type_repr_values_match_wire() {
        assert_eq!(MessageType::Push as u8, 0x01);
        assert_eq!(MessageType::Ack as u8, 0x02);
        assert_eq!(MessageType::Nack as u8, 0x03);
        assert_eq!(MessageType::HealthCheck as u8, 0x04);
        assert_eq!(MessageType::HealthCheckResponse as u8, 0x05);
    }

    // ── Envelope ─────────────────────────────────────────────────────────────

    fn round_trip(msg_type: MessageType) {
        let payload = b"hello weir".to_vec();
        let header = Header::new(msg_type, Durability::Sync, 0, payload.len() as u32);
        let env = Envelope::new(header, payload);
        let encoded = env.encode();
        let decoded = Envelope::decode(&encoded).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn envelope_round_trip_push() {
        round_trip(MessageType::Push);
    }

    #[test]
    fn envelope_round_trip_ack() {
        round_trip(MessageType::Ack);
    }

    #[test]
    fn envelope_round_trip_nack() {
        round_trip(MessageType::Nack);
    }

    #[test]
    fn envelope_round_trip_health_check() {
        round_trip(MessageType::HealthCheck);
    }

    #[test]
    fn envelope_round_trip_health_check_response() {
        round_trip(MessageType::HealthCheckResponse);
    }

    #[test]
    fn envelope_round_trip_empty_payload() {
        let header = push_header(0);
        let env = Envelope::new(header, vec![]);
        let encoded = env.encode();
        let decoded = Envelope::decode(&encoded).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn envelope_decode_rejects_truncated_header() {
        let env = Envelope::new(push_header(4), b"data".to_vec());
        let encoded = env.encode();
        assert_eq!(
            Envelope::decode(&encoded[..HEADER_LEN - 1]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_truncated_payload() {
        let payload = b"hello".to_vec();
        let env = Envelope::new(push_header(payload.len() as u32), payload);
        let encoded = env.encode();
        // Strip the last byte of the payload+CRC section.
        assert_eq!(
            Envelope::decode(&encoded[..encoded.len() - 1]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_header_only() {
        let env = Envelope::new(push_header(4), b"data".to_vec());
        let encoded = env.encode();
        assert_eq!(
            Envelope::decode(&encoded[..HEADER_LEN]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_corrupted_payload_crc() {
        let payload = b"hello".to_vec();
        let env = Envelope::new(push_header(payload.len() as u32), payload);
        let mut encoded = env.encode();
        // Flip a bit in the trailing CRC.
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(matches!(
            Envelope::decode(&encoded),
            Err(DecodeError::PayloadCrcMismatch { .. })
        ));
    }

    #[test]
    fn envelope_decode_rejects_oversized_payload_len() {
        // The cap check fires immediately after Header::decode, before the frame-length
        // check, so even a 16-byte buffer returns PayloadTooLarge (not TruncatedFrame).
        // This ordering is deliberate: it prevents any heap allocation attempt.
        let oversized_len = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
        let header = Header::new(MessageType::Push, Durability::Sync, 0, oversized_len);
        let header_bytes = header.encode();
        assert_eq!(
            Envelope::decode(&header_bytes),
            Err(DecodeError::PayloadTooLarge {
                len: MAX_PAYLOAD_HARD_CAP + 1,
                cap: MAX_PAYLOAD_HARD_CAP,
            })
        );
    }
}
