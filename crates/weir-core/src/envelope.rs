//! The weir wire frame: [`Header`] (the 16-byte fixed header) and [`Envelope`]
//! (header + payload), with the `encode`/`decode` codecs that own CRC
//! computation and the DoS-resistant validation order.

use crate::{
    durability::Durability,
    error::DecodeError,
    payload::Payload,
    version::{MAX_PAYLOAD_HARD_CAP, WIRE_VERSION},
};

/// Magic bytes at offset 0. Presence at a fixed offset allows stream resync by scanning for b"WEIR".
const MAGIC: [u8; 4] = *b"WEIR";

/// Wire layout (16 bytes; multi-byte fields are little-endian):
///
/// ```text
/// [0..4]   magic         b"WEIR"
/// [4]      version       u8        — WIRE_VERSION
/// [5]      message_type  u8        — MessageType repr
/// [6]      durability    u8        — Durability repr
/// [7]      flags         u8        — reserved; zero on write
/// [8..12]  payload_len   u32 LE
/// [12..16] header_crc32  u32 LE    — CRC32 of bytes [0..12]
/// ```
pub const HEADER_LEN: usize = 16;

/// Number of header bytes covered by the CRC. Everything except the CRC field itself.
const HEADER_CRC_COVERAGE: usize = 12;

/// Minimum valid frame: header + zero-length payload + payload CRC.
pub const MIN_FRAME_LEN: usize = HEADER_LEN + 4;

/// Wire message types. Values are fixed; changing them requires a WIRE_VERSION bump.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// Producer → daemon: a record to durably buffer.
    Push = 0x01,
    /// Daemon → producer: the pushed record was accepted at its durability tier.
    Ack = 0x02,
    /// Daemon → producer: the frame or record was rejected (see [`NackReason`](crate::NackReason)).
    Nack = 0x03,
    /// Producer → daemon: liveness probe (zero-length payload).
    HealthCheck = 0x04,
    /// Daemon → producer: reply to a [`HealthCheck`](MessageType::HealthCheck).
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
///
/// Fields are private and read-only (accessors below): a header is only ever
/// produced by [`Header::new`] (which pins `version` to [`WIRE_VERSION`]) or by
/// [`Header::decode`] (which validates every field). This makes it impossible
/// for a caller to desync `payload_len` from the real payload or set `version`
/// off `WIRE_VERSION` after construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    version: u8,
    message_type: MessageType,
    durability: Durability,
    /// Currently reserved; must be zero on write. Preserved verbatim through encode/decode.
    flags: u8,
    payload_len: u32,
}

impl Header {
    /// Constructs a header for outbound frames. Version is always `WIRE_VERSION`.
    ///
    /// `payload_len` is NOT a parameter: it is set to 0 here and becomes
    /// authoritative only when the header is wrapped in an [`Envelope`], which
    /// derives it from the actual payload (see [`Envelope::new`]). This makes a
    /// header whose declared length disagrees with its payload unrepresentable —
    /// a bare `Header::encode` always declares `payload_len = 0`, so it is only
    /// valid for the genuinely-empty-payload frames (Ack / HealthCheck) that use
    /// it (F50).
    pub fn new(message_type: MessageType, durability: Durability, flags: u8) -> Self {
        Self {
            version: WIRE_VERSION,
            message_type,
            durability,
            flags,
            payload_len: 0,
        }
    }

    /// Wire protocol version. Always [`WIRE_VERSION`] for a header built by
    /// [`Header::new`]; a decoded header only exists if its version matched.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// The frame's [`MessageType`].
    pub fn message_type(&self) -> MessageType {
        self.message_type
    }

    /// The frame's [`Durability`] tier.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Reserved flags byte (zero in v1; preserved verbatim through the codec).
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// Declared payload length in bytes. For an [`Envelope`] this always equals
    /// the actual payload length (see [`Envelope::new`]); on a bare decoded
    /// header it is the wire-declared length used to read the payload.
    pub fn payload_len(&self) -> u32 {
        self.payload_len
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
        // Reserved flags byte MUST be zero in wire v1. Reject a nonzero value
        // rather than preserving-and-ignoring it: silently dropping a flag that a
        // future version gives meaning to would let a producer believe a semantic
        // flag took effect when this daemon ignored it. Adding flags later is
        // therefore an explicit, version-gated change (F52).
        let flags = buf[7];
        if flags != 0 {
            return Err(DecodeError::ReservedFlagsSet { flags });
        }
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
///
/// Fields are private and read-only ([`header`](Envelope::header) /
/// [`payload`](Envelope::payload)). [`Envelope::new`] makes the payload
/// authoritative for `header.payload_len`, so the declared length and the
/// actual payload can never disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    header: Header,
    payload: Payload,
}

impl Envelope {
    /// Builds a frame from a header and payload. The header's `payload_len` is
    /// overwritten with the payload's actual length, so the two cannot desync
    /// — the payload is the single source of truth for the length on the wire.
    pub fn new(header: Header, payload: impl Into<Payload>) -> Self {
        let payload = payload.into();
        // Saturate rather than `as u32`-truncate: a >= 4 GiB payload would
        // otherwise wrap to a small length and desync the wire frame — the exact
        // failure this "cannot desync" constructor exists to prevent (F49). Such
        // a payload is far past MAX_PAYLOAD_HARD_CAP (16 MiB) and is rejected on
        // decode; saturating to u32::MAX keeps it rejected instead of wrapping.
        let payload_len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        let header = Header {
            payload_len,
            ..header
        };
        Self { header, payload }
    }

    /// The frame's validated [`Header`].
    pub fn header(&self) -> Header {
        self.header
    }

    /// The frame's payload bytes.
    pub fn payload(&self) -> &Payload {
        &self.payload
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
    /// 4. Buffer length == HEADER_LEN + payload_len + 4 — the buffer must be
    ///    *exactly* one frame: shorter is `TruncatedFrame`, longer is `TrailingBytes`
    ///    (the caller owns framing; the codec never silently discards a remainder — G18).
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
        // The buffer must be EXACTLY one frame. A longer buffer is rejected rather
        // than decoding the first frame and discarding the rest: this crate is the
        // executable wire reference, and silently dropping trailing bytes would let
        // a desynced or concatenated stream lose records without error. The caller
        // owns framing — weir's own socket/client paths read exactly frame_len and
        // never reach this arm (G18).
        if buf.len() > frame_len {
            return Err(DecodeError::TrailingBytes {
                extra: buf.len() - frame_len,
            });
        }

        let payload = Payload::copy_from_slice(&buf[HEADER_LEN..HEADER_LEN + payload_len]);
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

    fn push_header() -> Header {
        Header::new(MessageType::Push, Durability::Sync, 0)
    }

    // ── Header ──────────────────────────────────────────────────────────────

    #[test]
    fn header_encode_decode_round_trip() {
        let h = push_header();
        let encoded = h.encode();
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn header_encode_is_16_bytes() {
        let encoded = push_header().encode();
        assert_eq!(encoded.len(), HEADER_LEN);
    }

    #[test]
    fn header_decode_rejects_bad_magic() {
        let mut buf = push_header().encode();
        buf[0] = 0x00;
        assert_eq!(Header::decode(&buf), Err(DecodeError::BadMagic));
    }

    #[test]
    fn header_decode_rejects_future_version() {
        let mut buf = push_header().encode();
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
        let mut buf = push_header().encode();
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
        let mut buf = push_header().encode();
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
        let mut buf = push_header().encode();
        buf[12] ^= 0xff;
        assert!(matches!(
            Header::decode(&buf),
            Err(DecodeError::HeaderCrcMismatch { .. })
        ));
    }

    #[test]
    fn header_decode_rejects_unknown_message_type() {
        let mut buf = push_header().encode();
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
        let mut buf = push_header().encode();
        buf[6] = 0xff;
        let crc = crc32fast::hash(&buf[..HEADER_CRC_COVERAGE]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::UnknownDurability(0xff))
        );
    }

    /// In wire v1 the reserved flags byte must be zero. A header that sets any
    /// flag bit (with an otherwise-valid CRC over that byte) is rejected with
    /// `ReservedFlagsSet` rather than silently accepted (F52).
    #[test]
    fn header_decode_rejects_nonzero_flags() {
        let buf = Header::new(MessageType::Push, Durability::Sync, 0x01).encode();
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::ReservedFlagsSet { flags: 0x01 })
        );
        // The full byte is reported verbatim, not just "nonzero".
        let buf = Header::new(MessageType::Push, Durability::Sync, 0xff).encode();
        assert_eq!(
            Header::decode(&buf),
            Err(DecodeError::ReservedFlagsSet { flags: 0xff })
        );
    }

    /// The reserved-flags check fires only after magic/version/CRC/type/durability
    /// all pass: a frame that is *both* nonzero-flagged and unknown-durability
    /// surfaces the durability error first (the decode order is load-bearing for
    /// the daemon's Nack mapping).
    #[test]
    fn header_decode_flags_checked_after_durability() {
        let mut buf = Header::new(MessageType::Push, Durability::Sync, 0x01).encode();
        buf[6] = 0xff; // unknown durability
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
        let header = Header::new(msg_type, Durability::Sync, 0);
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
        let header = push_header();
        let env = Envelope::new(header, vec![]);
        let encoded = env.encode();
        let decoded = Envelope::decode(&encoded).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn envelope_decode_rejects_truncated_header() {
        let env = Envelope::new(push_header(), b"data".to_vec());
        let encoded = env.encode();
        assert_eq!(
            Envelope::decode(&encoded[..HEADER_LEN - 1]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_truncated_payload() {
        let payload = b"hello".to_vec();
        let env = Envelope::new(push_header(), payload);
        let encoded = env.encode();
        // Strip the last byte of the payload+CRC section.
        assert_eq!(
            Envelope::decode(&encoded[..encoded.len() - 1]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_header_only() {
        let env = Envelope::new(push_header(), b"data".to_vec());
        let encoded = env.encode();
        assert_eq!(
            Envelope::decode(&encoded[..HEADER_LEN]),
            Err(DecodeError::TruncatedFrame)
        );
    }

    #[test]
    fn envelope_decode_rejects_corrupted_payload_crc() {
        let payload = b"hello".to_vec();
        let env = Envelope::new(push_header(), payload);
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
    fn envelope_decode_rejects_trailing_bytes() {
        // A buffer longer than one frame is rejected (G18) rather than decoding
        // the first frame and silently discarding the rest — the codec never loses
        // a remainder; the caller owns framing.
        let env = Envelope::new(push_header(), b"hello".to_vec());
        let mut encoded = env.encode();
        encoded.extend_from_slice(b"garbage"); // 7 extra bytes
        assert_eq!(
            Envelope::decode(&encoded),
            Err(DecodeError::TrailingBytes { extra: 7 })
        );
    }

    #[test]
    fn envelope_decode_accepts_exact_frame() {
        // The boundary case: a buffer that is exactly one frame decodes cleanly.
        let env = Envelope::new(push_header(), b"hello".to_vec());
        let encoded = env.encode();
        assert_eq!(Envelope::decode(&encoded).unwrap(), env);
    }

    #[test]
    fn envelope_decode_trailing_bytes_checked_after_payload_too_large() {
        // A 16-byte buffer declaring an over-cap payload returns PayloadTooLarge,
        // not TrailingBytes: the cap check (step 3) precedes the frame-length check
        // (step 4), so an over-cap declared length is rejected before allocation
        // even though the buffer is "too short" for the declared frame.
        let oversized_len = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
        let mut header_bytes = push_header().encode();
        header_bytes[8..12].copy_from_slice(&oversized_len.to_le_bytes());
        let crc = crc32fast::hash(&header_bytes[..HEADER_CRC_COVERAGE]);
        header_bytes[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Envelope::decode(&header_bytes),
            Err(DecodeError::PayloadTooLarge {
                len: MAX_PAYLOAD_HARD_CAP + 1,
                cap: MAX_PAYLOAD_HARD_CAP,
            })
        );
    }

    #[test]
    fn envelope_decode_rejects_oversized_payload_len() {
        // The cap check fires immediately after Header::decode, before the frame-length
        // check, so even a 16-byte buffer returns PayloadTooLarge (not TruncatedFrame).
        // This ordering is deliberate: it prevents any heap allocation attempt.
        //
        // Header::new can no longer declare an oversized length (F50), so patch the
        // payload_len field + recompute the header CRC directly to put the over-cap
        // declared length on the wire.
        let oversized_len = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
        let mut header_bytes = Header::new(MessageType::Push, Durability::Sync, 0).encode();
        header_bytes[8..12].copy_from_slice(&oversized_len.to_le_bytes());
        let crc = crc32fast::hash(&header_bytes[..HEADER_CRC_COVERAGE]);
        header_bytes[12..16].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Envelope::decode(&header_bytes),
            Err(DecodeError::PayloadTooLarge {
                len: MAX_PAYLOAD_HARD_CAP + 1,
                cap: MAX_PAYLOAD_HARD_CAP,
            })
        );
    }
}
