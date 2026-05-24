//! Wire protocol tests from an external-crate perspective.
//!
//! These cover `weir-core`'s encode/decode contracts as a library consumer
//! would see them. No live server is needed.

use weir_core::{
    DecodeError, Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType,
    NackReason,
};

// ── Header round-trips ────────────────────────────────────────────────────────

#[test]
fn push_header_round_trip() {
    let h = Header::new(MessageType::Push, Durability::Sync, 0, 42);
    assert_eq!(Header::decode(&h.encode()).unwrap(), h);
}

#[test]
fn all_message_types_round_trip() {
    for mt in [
        MessageType::Push,
        MessageType::Ack,
        MessageType::Nack,
        MessageType::HealthCheck,
        MessageType::HealthCheckResponse,
    ] {
        let h = Header::new(mt, Durability::Sync, 0, 0);
        assert_eq!(Header::decode(&h.encode()).unwrap().message_type, mt);
    }
}

#[test]
fn all_durability_tiers_round_trip() {
    for d in [Durability::Sync, Durability::Batched, Durability::Buffered] {
        let h = Header::new(MessageType::Push, d, 0, 0);
        assert_eq!(Header::decode(&h.encode()).unwrap().durability, d);
    }
}

#[test]
fn header_is_exactly_16_bytes() {
    let encoded = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    assert_eq!(encoded.len(), HEADER_LEN);
}

#[test]
fn flags_field_preserved() {
    let h = Header::new(MessageType::Push, Durability::Sync, 0xAB, 0);
    assert_eq!(Header::decode(&h.encode()).unwrap().flags, 0xAB);
}

// ── Header decode rejections ──────────────────────────────────────────────────

#[test]
fn decode_rejects_bad_magic() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    buf[0] = 0x00;
    assert_eq!(Header::decode(&buf), Err(DecodeError::BadMagic));
}

#[test]
fn decode_rejects_future_version() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    buf[4] = 0xFF;
    let crc = crc32fast::hash(&buf[..12]).to_le_bytes();
    buf[12..16].copy_from_slice(&crc);
    assert!(matches!(
        Header::decode(&buf),
        Err(DecodeError::VersionMismatch { .. })
    ));
}

#[test]
fn decode_rejects_corrupted_header_crc() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    buf[12] ^= 0xFF;
    assert!(matches!(
        Header::decode(&buf),
        Err(DecodeError::HeaderCrcMismatch { .. })
    ));
}

#[test]
fn decode_rejects_unknown_message_type() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    buf[5] = 0xFF;
    let crc = crc32fast::hash(&buf[..12]).to_le_bytes();
    buf[12..16].copy_from_slice(&crc);
    assert_eq!(
        Header::decode(&buf),
        Err(DecodeError::UnknownMessageType(0xFF))
    );
}

#[test]
fn decode_rejects_unknown_durability() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0, 0).encode();
    buf[6] = 0xFF;
    let crc = crc32fast::hash(&buf[..12]).to_le_bytes();
    buf[12..16].copy_from_slice(&crc);
    assert_eq!(
        Header::decode(&buf),
        Err(DecodeError::UnknownDurability(0xFF))
    );
}

// ── Envelope round-trips ──────────────────────────────────────────────────────

#[test]
fn envelope_round_trip_nonempty_payload() {
    let payload = b"hello weir".to_vec();
    let header = Header::new(
        MessageType::Push,
        Durability::Batched,
        0,
        payload.len() as u32,
    );
    let env = Envelope::new(header, payload);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

#[test]
fn envelope_round_trip_empty_payload() {
    let header = Header::new(MessageType::Ack, Durability::Sync, 0, 0);
    let env = Envelope::new(header, vec![]);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

#[test]
fn envelope_round_trip_max_size_payload() {
    let payload = vec![0xABu8; 1024];
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let env = Envelope::new(header, payload);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

// ── Envelope decode rejections ────────────────────────────────────────────────

#[test]
fn envelope_decode_rejects_truncated_header() {
    let env = Envelope::new(
        Header::new(MessageType::Push, Durability::Sync, 0, 4),
        b"data".to_vec(),
    );
    let encoded = env.encode();
    assert_eq!(
        Envelope::decode(&encoded[..HEADER_LEN - 1]),
        Err(DecodeError::TruncatedFrame)
    );
}

#[test]
fn envelope_decode_rejects_truncated_payload() {
    let payload = b"hello".to_vec();
    let env = Envelope::new(
        Header::new(MessageType::Push, Durability::Sync, 0, payload.len() as u32),
        payload,
    );
    let encoded = env.encode();
    assert_eq!(
        Envelope::decode(&encoded[..encoded.len() - 1]),
        Err(DecodeError::TruncatedFrame)
    );
}

#[test]
fn envelope_decode_rejects_corrupted_payload_crc() {
    let payload = b"hello".to_vec();
    let env = Envelope::new(
        Header::new(MessageType::Push, Durability::Sync, 0, payload.len() as u32),
        payload,
    );
    let mut encoded = env.encode();
    let last = encoded.len() - 1;
    encoded[last] ^= 0xFF;
    assert!(matches!(
        Envelope::decode(&encoded),
        Err(DecodeError::PayloadCrcMismatch { .. })
    ));
}

#[test]
fn envelope_decode_rejects_oversized_payload_len() {
    let oversized = (MAX_PAYLOAD_HARD_CAP + 1) as u32;
    let header = Header::new(MessageType::Push, Durability::Sync, 0, oversized);
    let header_bytes = header.encode();
    assert_eq!(
        Envelope::decode(&header_bytes),
        Err(DecodeError::PayloadTooLarge {
            len: MAX_PAYLOAD_HARD_CAP + 1,
            cap: MAX_PAYLOAD_HARD_CAP,
        })
    );
}

// ── NackReason ────────────────────────────────────────────────────────────────

#[test]
fn nack_reason_repr_values_are_stable() {
    assert_eq!(NackReason::BadMagic as u8, 0x01);
    assert_eq!(NackReason::VersionMismatch as u8, 0x02);
    assert_eq!(NackReason::BadHeaderCrc as u8, 0x03);
    assert_eq!(NackReason::PayloadTooLarge as u8, 0x04);
    assert_eq!(NackReason::BadPayloadCrc as u8, 0x05);
    assert_eq!(NackReason::InternalError as u8, 0x06);
}

#[test]
fn nack_reason_round_trips_all_known_values() {
    for r in [
        NackReason::BadMagic,
        NackReason::VersionMismatch,
        NackReason::BadHeaderCrc,
        NackReason::PayloadTooLarge,
        NackReason::BadPayloadCrc,
        NackReason::InternalError,
    ] {
        assert_eq!(NackReason::try_from(r as u8).unwrap(), r);
    }
}

#[test]
fn nack_reason_rejects_unknown_byte() {
    assert!(NackReason::try_from(0x00).is_err());
    assert!(NackReason::try_from(0x07).is_err());
    assert!(NackReason::try_from(0xFF).is_err());
}
