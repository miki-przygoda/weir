//! Reference frame byte sequences for non-Rust client implementers.
//!
//! Every test in this file builds a frame via the public encoder, then
//! asserts the bytes against a `const` published in
//! `docs/wire_protocol.md` (Worked examples section). If the encoder
//! ever drifts from the documented wire format, one of these tests
//! fails — keeping the doc and the code in lockstep without manual
//! review.
//!
//! Implementers writing a non-Rust client can copy the `REFERENCE_*`
//! constants below verbatim into their own test suite to confirm
//! their encoder is byte-identical to weir's.

use weir_core::{Durability, Envelope, Header, MessageType, WIRE_VERSION};

// ── Push of "hello", Sync durability ──────────────────────────────────────────

/// Push frame: magic + version + Push + Sync + flags=0 + payload_len=5 +
/// header_crc + "hello" + payload_crc. Total 25 bytes.
const REFERENCE_PUSH_HELLO_SYNC: &[u8; 25] = &[
    // Header (16 bytes)
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x01, // message_type = Push (0x01)
    0x01, // durability  = Sync (0x01)
    0x00, // flags
    0x05, 0x00, 0x00, 0x00, // payload_len = 5 (LE u32)
    0x66, 0xad, 0x7d, 0x3c, // header_crc32 (LE u32)
    // Payload (5 bytes)
    0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
    // Payload CRC (4 bytes)
    0x86, 0xa6, 0x10, 0x36, // payload_crc32 (LE u32; 0x3610a686 little-endian)
];

#[test]
fn push_hello_sync_encodes_to_reference_bytes() {
    let header = Header::new(MessageType::Push, Durability::Sync, 0, 5);
    assert_eq!(
        header.version(),
        WIRE_VERSION,
        "test assumes WIRE_VERSION = 1"
    );
    let envelope = Envelope::new(header, b"hello".to_vec());
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_PUSH_HELLO_SYNC.as_slice(),
        "encoder drifted from docs/wire_protocol.md Worked Examples → \
         Push of 5-byte payload. Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── Ack response ──────────────────────────────────────────────────────────────

/// Ack frame: same magic/version/CRC machinery as a Push, with
/// message_type=Ack (0x02), durability=Sync (filler — server fixes it
/// to Sync regardless of the request's tier), payload_len=0, and an
/// all-zero payload_crc (CRC of zero bytes is 0x00000000). Total 20 bytes.
const REFERENCE_ACK: &[u8; 20] = &[
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x02, // message_type = Ack (0x02)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x00, 0x00, 0x00, 0x00, // payload_len = 0
    0xc9, 0x47, 0x4b, 0x3a, // header_crc32
    0x00, 0x00, 0x00, 0x00, // payload_crc32 (empty payload)
];

#[test]
fn ack_encodes_to_reference_bytes() {
    let header = Header::new(MessageType::Ack, Durability::Sync, 0, 0);
    let envelope = Envelope::new(header, Vec::new());
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_ACK.as_slice(),
        "encoder drifted from docs/wire_protocol.md Worked Examples → \
         Ack response. Either restore byte-compatibility or update the \
         doc and this constant."
    );
}

// ── Nack(PayloadTooLarge) response ────────────────────────────────────────────

/// Nack frame with a single-byte payload carrying the NackReason. For
/// PayloadTooLarge that byte is 0x04. The server's `send_nack`
/// constructs the frame as Header(Nack, Sync, 0, 1) + [reason_byte] +
/// CRC. Total 21 bytes.
const REFERENCE_NACK_PAYLOAD_TOO_LARGE: &[u8; 21] = &[
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x03, // message_type = Nack (0x03)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x01, 0x00, 0x00, 0x00, // payload_len = 1
    0x18, 0x2b, 0x80, 0x24, // header_crc32
    0x04, // NackReason::PayloadTooLarge
    0x94, 0x2b, 0x6f, 0xd5, // payload_crc32 (of [0x04])
];

#[test]
fn nack_payload_too_large_encodes_to_reference_bytes() {
    use weir_core::NackReason;
    let header = Header::new(MessageType::Nack, Durability::Sync, 0, 1);
    let envelope = Envelope::new(header, vec![NackReason::PayloadTooLarge as u8]);
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_NACK_PAYLOAD_TOO_LARGE.as_slice(),
        "encoder drifted from docs/wire_protocol.md Worked Examples → \
         Nack(PayloadTooLarge). Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── Nack(VersionMismatch) response ────────────────────────────────────────────

/// Nack frame for VersionMismatch carries a 2-byte payload: the reason byte
/// (0x02) followed by the daemon's WIRE_VERSION. The second byte lets the
/// client render "daemon is on vN; you are on vM" without an extra round-trip.
/// Total 22 bytes.
const REFERENCE_NACK_VERSION_MISMATCH: &[u8; 22] = &[
    0x57,
    0x45,
    0x49,
    0x52, // magic = "WEIR"
    0x01, // version = 1
    0x03, // message_type = Nack (0x03)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x02,
    0x00,
    0x00,
    0x00, // payload_len = 2
    0xf6,
    0x84,
    0x35,
    0x36, // header_crc32
    0x02,
    WIRE_VERSION, // NackReason::VersionMismatch + daemon WIRE_VERSION
    0xeb,
    0x40,
    0xe8,
    0x04, // payload_crc32 (of [0x02, 0x01])
];

#[test]
fn nack_version_mismatch_encodes_to_reference_bytes() {
    use weir_core::NackReason;
    let header = Header::new(MessageType::Nack, Durability::Sync, 0, 2);
    let envelope = Envelope::new(
        header,
        vec![NackReason::VersionMismatch as u8, WIRE_VERSION],
    );
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_NACK_VERSION_MISMATCH.as_slice(),
        "encoder drifted from docs/wire_protocol.md → \
         Nack(VersionMismatch). Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── Nack(BadHeaderCrc) response ───────────────────────────────────────────────

/// Single-byte Nack payload: just the reason byte (0x03). No `extra` bytes
/// — the client already knows the frame's header was corrupt because it's
/// receiving this Nack. Total 21 bytes.
const REFERENCE_NACK_BAD_HEADER_CRC: &[u8; 21] = &[
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x03, // message_type = Nack (0x03)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x01, 0x00, 0x00, 0x00, // payload_len = 1
    0x18, 0x2b, 0x80, 0x24, // header_crc32 (same header as PayloadTooLarge)
    0x03, // NackReason::BadHeaderCrc
    0x37, 0xbe, 0x0b, 0x4b, // payload_crc32 (of [0x03])
];

#[test]
fn nack_bad_header_crc_encodes_to_reference_bytes() {
    use weir_core::NackReason;
    let header = Header::new(MessageType::Nack, Durability::Sync, 0, 1);
    let envelope = Envelope::new(header, vec![NackReason::BadHeaderCrc as u8]);
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_NACK_BAD_HEADER_CRC.as_slice(),
        "encoder drifted from docs/wire_protocol.md → \
         Nack(BadHeaderCrc). Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── HealthCheck request ───────────────────────────────────────────────────────

/// Client → daemon. Zero-length payload, all-zero payload CRC. The
/// durability byte is unused (clients send Sync by convention). Total 20 bytes.
const REFERENCE_HEALTHCHECK: &[u8; 20] = &[
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x04, // message_type = HealthCheck (0x04)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x00, 0x00, 0x00, 0x00, // payload_len = 0
    0xf3, 0x72, 0x9b, 0x59, // header_crc32
    0x00, 0x00, 0x00, 0x00, // payload_crc32 (empty payload → 0)
];

#[test]
fn healthcheck_encodes_to_reference_bytes() {
    let header = Header::new(MessageType::HealthCheck, Durability::Sync, 0, 0);
    let envelope = Envelope::new(header, Vec::new());
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_HEALTHCHECK.as_slice(),
        "encoder drifted from docs/wire_protocol.md → \
         HealthCheck request. Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── HealthCheckResponse ──────────────────────────────────────────────────────

/// Daemon → client. Same shape as HealthCheck but with the response
/// message_type. Total 20 bytes.
const REFERENCE_HEALTHCHECK_RESPONSE: &[u8; 20] = &[
    0x57, 0x45, 0x49, 0x52, // magic = "WEIR"
    0x01, // version = 1
    0x05, // message_type = HealthCheckResponse (0x05)
    0x01, // durability = Sync (filler)
    0x00, // flags
    0x00, 0x00, 0x00, 0x00, // payload_len = 0
    0x47, 0x79, 0xec, 0xff, // header_crc32
    0x00, 0x00, 0x00, 0x00, // payload_crc32 (empty payload → 0)
];

#[test]
fn healthcheck_response_encodes_to_reference_bytes() {
    let header = Header::new(MessageType::HealthCheckResponse, Durability::Sync, 0, 0);
    let envelope = Envelope::new(header, Vec::new());
    let encoded = envelope.encode();
    assert_eq!(
        encoded.as_slice(),
        REFERENCE_HEALTHCHECK_RESPONSE.as_slice(),
        "encoder drifted from docs/wire_protocol.md → \
         HealthCheckResponse. Either restore byte-compatibility or \
         update the doc and this constant."
    );
}

// ── CRC algorithm spot-check ──────────────────────────────────────────────────

/// Confirms the wire CRC is the IEEE/ISO-3309 variant (polynomial
/// 0x04C11DB7, reflected). Distinguishes it from CRC-32C, which would
/// produce wildly different values for the same input. The documented
/// CRC of `b"hello"` under the IEEE polynomial is `0x3610a686`.
#[test]
fn crc_algorithm_is_ieee_not_castagnoli() {
    let crc = crc32fast::hash(b"hello");
    assert_eq!(
        crc, 0x3610_a686,
        "crc32fast::hash(b\"hello\") should be 0x3610a686 under the IEEE \
         polynomial; if this fails the dep has switched algorithms and \
         every wire frame is now incompatible"
    );
}

// ── Empty payload CRC is 0 ────────────────────────────────────────────────────

#[test]
fn empty_payload_crc_is_zero() {
    // The Ack/HealthCheckResponse frame relies on this: an empty payload's
    // CRC is the all-zero post-XOR value. Wire-level confirmation so a
    // client author who wants to optimise away the CRC call for empty
    // payloads has a sanctioned shortcut.
    assert_eq!(crc32fast::hash(&[]), 0);
}
