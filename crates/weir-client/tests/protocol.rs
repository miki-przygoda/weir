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
    let h = Header::new(MessageType::Push, Durability::Sync, 0);
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
        let h = Header::new(mt, Durability::Sync, 0);
        assert_eq!(Header::decode(&h.encode()).unwrap().message_type(), mt);
    }
}

#[test]
fn all_durability_tiers_round_trip() {
    for d in [Durability::Sync, Durability::Batched, Durability::Buffered] {
        let h = Header::new(MessageType::Push, d, 0);
        assert_eq!(Header::decode(&h.encode()).unwrap().durability(), d);
    }
}

#[test]
fn header_is_exactly_16_bytes() {
    let encoded = Header::new(MessageType::Push, Durability::Sync, 0).encode();
    assert_eq!(encoded.len(), HEADER_LEN);
}

#[test]
fn nonzero_flags_rejected() {
    // Wire v1 reserves the flags byte: it must be zero. A frame that sets any
    // flag bit is rejected at decode (F52) rather than silently accepted — a
    // producer must never believe an unrecognised flag took effect.
    let h = Header::new(MessageType::Push, Durability::Sync, 0xAB);
    assert_eq!(
        Header::decode(&h.encode()),
        Err(DecodeError::ReservedFlagsSet { flags: 0xAB })
    );
}

#[test]
fn zero_flags_accepted() {
    let h = Header::new(MessageType::Push, Durability::Sync, 0);
    assert_eq!(Header::decode(&h.encode()).unwrap().flags(), 0);
}

// ── Header decode rejections ──────────────────────────────────────────────────

#[test]
fn decode_rejects_bad_magic() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0).encode();
    buf[0] = 0x00;
    assert_eq!(Header::decode(&buf), Err(DecodeError::BadMagic));
}

#[test]
fn decode_rejects_future_version() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0).encode();
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
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0).encode();
    buf[12] ^= 0xFF;
    assert!(matches!(
        Header::decode(&buf),
        Err(DecodeError::HeaderCrcMismatch { .. })
    ));
}

#[test]
fn decode_rejects_unknown_message_type() {
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0).encode();
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
    let mut buf = Header::new(MessageType::Push, Durability::Sync, 0).encode();
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
    let header = Header::new(MessageType::Push, Durability::Batched, 0);
    let env = Envelope::new(header, payload);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

#[test]
fn envelope_round_trip_empty_payload() {
    let header = Header::new(MessageType::Ack, Durability::Sync, 0);
    let env = Envelope::new(header, vec![]);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

#[test]
fn envelope_round_trip_max_size_payload() {
    let payload = vec![0xABu8; 1024];
    let header = Header::new(MessageType::Push, Durability::Buffered, 0);
    let env = Envelope::new(header, payload);
    assert_eq!(Envelope::decode(&env.encode()).unwrap(), env);
}

// ── Envelope decode rejections ────────────────────────────────────────────────

#[test]
fn envelope_decode_rejects_truncated_header() {
    let env = Envelope::new(
        Header::new(MessageType::Push, Durability::Sync, 0),
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
    let env = Envelope::new(Header::new(MessageType::Push, Durability::Sync, 0), payload);
    let encoded = env.encode();
    assert_eq!(
        Envelope::decode(&encoded[..encoded.len() - 1]),
        Err(DecodeError::TruncatedFrame)
    );
}

#[test]
fn envelope_decode_rejects_corrupted_payload_crc() {
    let payload = b"hello".to_vec();
    let env = Envelope::new(Header::new(MessageType::Push, Durability::Sync, 0), payload);
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
    // Header::new can no longer declare an oversized length (F50). Build a full
    // frame whose Envelope derives the over-cap length from a real (discarded)
    // payload, and decode only its 16-byte header: the cap check fires before the
    // frame-length check, so even those 16 bytes return PayloadTooLarge.
    let frame = Envelope::new(
        Header::new(MessageType::Push, Durability::Sync, 0),
        vec![0u8; MAX_PAYLOAD_HARD_CAP + 1],
    )
    .encode();
    assert_eq!(
        Envelope::decode(&frame[..HEADER_LEN]),
        Err(DecodeError::PayloadTooLarge {
            len: MAX_PAYLOAD_HARD_CAP + 1,
            cap: MAX_PAYLOAD_HARD_CAP,
        })
    );
}

// ── TryFrom error structs are reachable at the crate root (G19) ────────────────

#[test]
fn wire_tryfrom_error_structs_are_re_exported_at_crate_root() {
    // Each fixed-repr enum's TryFrom<u8> error must be nameable from the crate
    // root, not just its sub-module, so a caller can store/match the error type.
    use weir_core::{
        Durability, MessageType, UnknownDurability, UnknownMessageType, UnknownNackReason,
    };

    let e: UnknownMessageType = MessageType::try_from(0xFF).unwrap_err();
    assert_eq!(e.0, 0xFF);
    let e: UnknownDurability = Durability::try_from(0xFF).unwrap_err();
    assert_eq!(e.0, 0xFF);
    let e: UnknownNackReason = NackReason::try_from(0xFF).unwrap_err();
    assert_eq!(e.0, 0xFF);
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
    assert_eq!(NackReason::EmptyPayload as u8, 0x07);
    assert_eq!(NackReason::UnknownMessage as u8, 0x08);
    assert_eq!(NackReason::ReservedFlagsSet as u8, 0x09);
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
        NackReason::EmptyPayload,
        NackReason::UnknownMessage,
        NackReason::ReservedFlagsSet,
    ] {
        assert_eq!(NackReason::try_from(r as u8).unwrap(), r);
    }
}

#[test]
fn nack_reason_rejects_unknown_byte() {
    assert!(NackReason::try_from(0x00).is_err());
    // 0x01..=0x09 are known reasons (0x09 is ReservedFlagsSet, F52); 0x0A is the
    // first unused byte.
    assert!(NackReason::try_from(0x0A).is_err());
    assert!(NackReason::try_from(0xFF).is_err());
}

// ── Property-based tests ──────────────────────────────────────────────────────

mod proptest_wire {
    use proptest::prelude::*;
    use weir_core::{DecodeError, Durability, Envelope, HEADER_LEN, Header, MessageType};

    fn any_message_type() -> impl Strategy<Value = MessageType> {
        prop_oneof![
            Just(MessageType::Push),
            Just(MessageType::Ack),
            Just(MessageType::Nack),
            Just(MessageType::HealthCheck),
            Just(MessageType::HealthCheckResponse),
        ]
    }

    fn any_durability() -> impl Strategy<Value = Durability> {
        prop_oneof![
            Just(Durability::Sync),
            Just(Durability::Batched),
            Just(Durability::Buffered),
        ]
    }

    proptest! {
        /// Encode → decode is the identity for any valid header field combination.
        /// A zero flags byte round-trips; any nonzero flags byte is rejected with
        /// `ReservedFlagsSet` (F52), so the property is conditional on `flags`.
        #[test]
        fn header_encode_decode_roundtrip(
            mt in any_message_type(),
            d in any_durability(),
            flags in any::<u8>(),
        ) {
            let h = Header::new(mt, d, flags);
            let encoded = h.encode();
            if flags == 0 {
                prop_assert_eq!(Header::decode(&encoded), Ok(h));
            } else {
                prop_assert_eq!(
                    Header::decode(&encoded),
                    Err(DecodeError::ReservedFlagsSet { flags })
                );
            }
        }

        /// Encode → decode is the identity for any payload byte sequence.
        #[test]
        fn envelope_encode_decode_roundtrip(
            payload in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let header = Header::new(
                MessageType::Push,
                Durability::Sync,
                0);
            let env = Envelope::new(header, payload);
            prop_assert_eq!(Envelope::decode(&env.encode()), Ok(env));
        }

        /// `Header::decode` never panics on arbitrary 16-byte input.
        #[test]
        fn header_decode_never_panics(bytes: [u8; HEADER_LEN]) {
            let _ = Header::decode(&bytes);
        }

        /// `Envelope::decode` never panics on any byte sequence.
        #[test]
        fn envelope_decode_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            let _ = Envelope::decode(&bytes);
        }

        /// Corrupting one byte of a valid header always produces a result — never panics.
        #[test]
        fn header_single_byte_mutation_never_panics(
            mt in any_message_type(),
            d in any_durability(),
            pos in 0usize..HEADER_LEN,
            corrupt_byte in any::<u8>(),
        ) {
            let h = Header::new(mt, d, 0);
            let mut buf = h.encode();
            buf[pos] = corrupt_byte;
            let _ = Header::decode(&buf);
        }
    }
}
