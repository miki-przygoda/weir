//! Property-based tests for the wire protocol parser.
//!
//! The frame decoder in `envelope.rs` is the most attack-facing piece of code
//! in weir: every byte that arrives over the Unix socket flows through it
//! before any other validation. Hand-crafted tests cover the obvious paths;
//! these properties cover the long tail.
//!
//! Invariants checked:
//! - `Header::decode(&Header::new(...).encode())` round-trips for any valid
//!   header field combination.
//! - `Envelope::decode(&Envelope::new(...).encode())` round-trips for any
//!   valid (header, payload) combination.
//! - `Header::decode` and `Envelope::decode` never panic on arbitrary input.
//! - Specific error variants fire for the categories of malformed input we
//!   care about (bad magic, wrong version, bad CRC, oversize payload,
//!   truncation, unknown message type / durability).

use proptest::prelude::*;
use weir_core::{
    DecodeError, Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType,
    WIRE_VERSION,
};

// ── Strategies ────────────────────────────────────────────────────────────────

fn arb_message_type() -> impl Strategy<Value = MessageType> {
    prop_oneof![
        Just(MessageType::Push),
        Just(MessageType::Ack),
        Just(MessageType::Nack),
        Just(MessageType::HealthCheck),
        Just(MessageType::HealthCheckResponse),
    ]
}

fn arb_durability() -> impl Strategy<Value = Durability> {
    prop_oneof![
        Just(Durability::Sync),
        Just(Durability::Batched),
        Just(Durability::Buffered),
    ]
}

fn arb_header() -> impl Strategy<Value = Header> {
    // Header::new derives payload_len (0 for a bare header; set by Envelope::new
    // otherwise — F50), so the strategy only varies the fields the constructor
    // takes. Out-of-cap declared lengths are exercised by the dedicated
    // PayloadTooLarge property below.
    (arb_message_type(), arb_durability(), any::<u8>())
        .prop_map(|(mt, d, flags)| Header::new(mt, d, flags))
}

fn arb_small_payload() -> impl Strategy<Value = Vec<u8>> {
    // Keep payloads small so the suite stays fast; the size logic is exercised
    // by dedicated properties, not by every round-trip case.
    proptest::collection::vec(any::<u8>(), 0..=256)
}

// ── Properties: Header round-trip ─────────────────────────────────────────────

proptest! {
    /// Any header produced by `Header::new` survives a full encode/decode cycle
    /// with field equality preserved.
    #[test]
    fn header_round_trip(h in arb_header()) {
        let bytes = h.encode();
        let decoded = Header::decode(&bytes).expect("freshly encoded header must decode");
        prop_assert_eq!(decoded.version(), h.version());
        prop_assert_eq!(decoded.message_type(), h.message_type());
        prop_assert_eq!(decoded.durability(), h.durability());
        prop_assert_eq!(decoded.flags(), h.flags());
        prop_assert_eq!(decoded.payload_len(), h.payload_len());
    }

    /// `Header::decode` never panics on arbitrary 16-byte input. The decoder
    /// must always return a Result — no array-OOB, no integer overflow panic,
    /// no unwrap-on-None.
    #[test]
    fn header_decode_never_panics(bytes in any::<[u8; HEADER_LEN]>()) {
        let _ = Header::decode(&bytes); // discarded; we only assert no panic
    }
}

// ── Properties: Header error paths ────────────────────────────────────────────

proptest! {
    /// Any 16-byte input whose first four bytes are not `b"WEIR"` must decode
    /// to `BadMagic`. Tested before any other validation can fire.
    #[test]
    fn bad_magic_always_rejected(
        bad_magic in any::<[u8; 4]>().prop_filter("must differ from WEIR", |b| b != b"WEIR"),
        rest in any::<[u8; 12]>(),
    ) {
        let mut bytes = [0u8; HEADER_LEN];
        bytes[..4].copy_from_slice(&bad_magic);
        bytes[4..].copy_from_slice(&rest);
        match Header::decode(&bytes) {
            Err(DecodeError::BadMagic) => {}
            other => prop_assert!(
                false,
                "expected BadMagic, got {other:?}"
            ),
        }
    }

    /// A frame with correct magic but a version byte != WIRE_VERSION must
    /// decode to `VersionMismatch`, regardless of the rest of the bytes.
    /// Critical: this check fires *before* the header CRC, so a v2 client
    /// gets a clean VersionMismatch rather than a confusing HeaderCrcMismatch.
    #[test]
    fn wrong_version_rejected_before_crc(
        bad_version in any::<u8>().prop_filter("must differ from WIRE_VERSION", |&v| v != WIRE_VERSION),
        rest in any::<[u8; 11]>(),
    ) {
        let mut bytes = [0u8; HEADER_LEN];
        bytes[..4].copy_from_slice(b"WEIR");
        bytes[4] = bad_version;
        bytes[5..16].copy_from_slice(&rest);
        match Header::decode(&bytes) {
            Err(DecodeError::VersionMismatch { supported, received }) => {
                prop_assert_eq!(supported, WIRE_VERSION);
                prop_assert_eq!(received, bad_version);
            }
            other => prop_assert!(
                false,
                "expected VersionMismatch, got {other:?}"
            ),
        }
    }

    /// A frame with correct magic and version but a corrupted CRC byte must
    /// decode to `HeaderCrcMismatch`. We construct a valid header then flip
    /// a random bit in the CRC field to ensure a known-bad CRC.
    #[test]
    fn bad_header_crc_rejected(h in arb_header(), flip_bit in 0u8..32) {
        let mut bytes = h.encode();
        // Flip a single bit in the CRC field (bytes 12..16, so bits 96..128).
        let byte_idx = 12 + (flip_bit / 8) as usize;
        let bit_idx = flip_bit % 8;
        bytes[byte_idx] ^= 1 << bit_idx;
        match Header::decode(&bytes) {
            Err(DecodeError::HeaderCrcMismatch { .. }) => {}
            // It's possible (vanishingly so) that flipping a bit happens to
            // produce a valid CRC for *some* other interpretation; if so,
            // we accept any Err but not Ok.
            Err(_) => {} // any other decode error is also acceptable
            Ok(_) => prop_assert!(
                false,
                "header with flipped CRC bit at {flip_bit} must not decode"
            ),
        }
    }
}

// ── Properties: Envelope round-trip ───────────────────────────────────────────

proptest! {
    /// Any (message_type, durability, flags, payload) tuple survives encode +
    /// decode. payload_len in the header is set automatically from the
    /// payload length so the round-trip is on the full frame, not just the
    /// header.
    #[test]
    fn envelope_round_trip(
        mt in arb_message_type(),
        d in arb_durability(),
        flags in any::<u8>(),
        payload in arb_small_payload(),
    ) {
        let header = Header::new(mt, d, flags);
        let env = Envelope::new(header, payload.clone());
        let bytes = env.encode();
        let decoded = Envelope::decode(&bytes).expect("freshly encoded envelope must decode");
        prop_assert_eq!(decoded.header().version(), WIRE_VERSION);
        prop_assert_eq!(decoded.header().message_type(), mt);
        prop_assert_eq!(decoded.header().durability(), d);
        prop_assert_eq!(decoded.header().flags(), flags);
        prop_assert_eq!(decoded.header().payload_len(), payload.len() as u32);
        prop_assert_eq!(&decoded.payload()[..], &payload[..]);
    }

    /// Encoded frame length is exactly `HEADER_LEN + payload.len() + 4`. This
    /// is a load-bearing invariant: the server's frame-reader relies on it.
    #[test]
    fn encoded_frame_length_is_exact(payload in arb_small_payload()) {
        let header = Header::new(MessageType::Push, Durability::Sync, 0);
        let env = Envelope::new(header, payload.clone());
        let bytes = env.encode();
        prop_assert_eq!(bytes.len(), HEADER_LEN + payload.len() + 4);
    }

    /// `Envelope::decode` never panics on arbitrary input of any length. This
    /// is the strongest guarantee against malformed-input crashes from a
    /// connected client.
    #[test]
    fn envelope_decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
        let _ = Envelope::decode(&bytes); // discarded; we only assert no panic
    }
}

// ── Properties: Envelope error paths ──────────────────────────────────────────

proptest! {
    /// Any buffer shorter than `HEADER_LEN` must decode to `TruncatedFrame`.
    /// The decoder must check length before any byte-slice indexing.
    #[test]
    fn short_buffer_rejected_as_truncated(
        bytes in proptest::collection::vec(any::<u8>(), 0..HEADER_LEN),
    ) {
        match Envelope::decode(&bytes) {
            Err(DecodeError::TruncatedFrame) => {}
            other => prop_assert!(
                false,
                "buffer of length {} must decode to TruncatedFrame, got {other:?}",
                bytes.len()
            ),
        }
    }

    /// A header declaring `payload_len > MAX_PAYLOAD_HARD_CAP` must decode to
    /// `PayloadTooLarge` regardless of what follows. The cap check fires
    /// before any payload-sized allocation — the property here also
    /// exercises that the decoder doesn't crash on a tiny buffer with a
    /// giant declared length.
    #[test]
    fn oversize_payload_rejected_before_allocation(
        oversize_len in (MAX_PAYLOAD_HARD_CAP as u32 + 1)..=u32::MAX,
        mt in arb_message_type(),
        d in arb_durability(),
        flags in any::<u8>(),
    ) {
        // Header::new can no longer declare an arbitrary length (F50), so patch
        // the encoded header's payload_len field + recompute the header CRC to put
        // the (possibly un-allocatable) declared length on the wire. Only the
        // 16-byte header is emitted — no payload bytes — so if the decoder tried
        // to allocate before the cap check, it would fault here.
        let mut bytes = Header::new(mt, d, flags).encode();
        bytes[8..12].copy_from_slice(&oversize_len.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..12]);
        bytes[12..16].copy_from_slice(&crc.to_le_bytes());
        match Envelope::decode(&bytes) {
            Err(DecodeError::PayloadTooLarge { len, cap }) => {
                prop_assert_eq!(len as u32, oversize_len);
                prop_assert_eq!(cap, MAX_PAYLOAD_HARD_CAP);
            }
            other => prop_assert!(
                false,
                "oversize payload_len {oversize_len} must decode to PayloadTooLarge, got {other:?}"
            ),
        }
    }

    /// A frame with a valid header (small payload_len) but truncated payload
    /// must decode to `TruncatedFrame` rather than panicking on
    /// out-of-bounds slicing.
    #[test]
    fn truncated_payload_rejected(
        full_len in 1usize..=128,
        keep in 0usize..=128,
    ) {
        // Build a valid frame with payload of `full_len` zero bytes…
        // (Envelope::new derives payload_len from the payload.)
        let header = Header::new(MessageType::Push, Durability::Sync, 0);
        let env = Envelope::new(header, vec![0u8; full_len]);
        let full = env.encode();
        // …then keep only a prefix that is short of the full frame.
        let trunc_len = keep.min(full.len().saturating_sub(1));
        if trunc_len < HEADER_LEN + full_len + 4 {
            let bytes = &full[..trunc_len];
            match Envelope::decode(bytes) {
                Err(DecodeError::TruncatedFrame) => {}
                other => prop_assert!(
                    false,
                    "truncated frame ({trunc_len} bytes, expected {} + 4) must decode to TruncatedFrame, got {other:?}",
                    HEADER_LEN + full_len
                ),
            }
        }
    }

    /// A frame with a valid header (incl. CRC) but a corrupted payload byte
    /// must decode to `PayloadCrcMismatch`. Flips one bit in the payload
    /// after encoding so the header CRC stays valid.
    #[test]
    fn bad_payload_crc_rejected(payload in proptest::collection::vec(any::<u8>(), 1..=128)) {
        let header = Header::new(
            MessageType::Push,
            Durability::Sync,
            0);
        let env = Envelope::new(header, payload.clone());
        let mut bytes = env.encode();
        // Flip a bit in the first payload byte (at offset HEADER_LEN).
        bytes[HEADER_LEN] ^= 0x01;
        match Envelope::decode(&bytes) {
            Err(DecodeError::PayloadCrcMismatch { .. }) => {}
            other => prop_assert!(
                false,
                "envelope with corrupted payload byte must decode to PayloadCrcMismatch, got {other:?}"
            ),
        }
    }
}
