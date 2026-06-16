//! Conformance suite: weir-core's decoder vs. the published wire vectors.
//!
//! `docs/conformance/wire_v1_vectors.json` is the language-neutral definition of
//! the weir v1 wire format: a list of hex-encoded byte buffers, each paired with
//! the result a conformant decoder MUST produce. A non-Rust implementer loads the
//! same file and runs their decoder against it; this test runs *weir's* decoder
//! against it, proving the reference implementation matches its own published
//! vectors (and, because the vectors were generated with a different CRC-32
//! implementation, that the two agree on every byte).
//!
//! If this test fails after an intentional wire change, regenerate the vectors
//! (see docs/conformance.md) — do not edit the JSON by hand to make it pass.

use serde_json::Value;
use weir_core::{
    DecodeError, Durability, Envelope, MAX_PAYLOAD_HARD_CAP, MessageType, WIRE_VERSION,
};

/// The canonical vectors, embedded at compile time so the test is hermetic.
const VECTORS_JSON: &str = include_str!("../../../docs/conformance/wire_v1_vectors.json");

/// Decodes a lowercase-hex string to bytes. Hand-rolled to keep the test free of
/// a hex-crate dependency; the input is machine-generated so it is always valid.
fn from_hex(s: &str) -> Vec<u8> {
    assert!(
        s.len().is_multiple_of(2),
        "hex string must have even length: {s:?}"
    );
    fn nibble(c: u8) -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => panic!("invalid hex digit: {:?}", c as char),
        }
    }
    s.as_bytes()
        .chunks(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Maps a `DecodeError` to the stable tag used in the vectors file. This is the
/// contract surface: every rejection vector names one of these.
fn error_tag(e: &DecodeError) -> &'static str {
    match e {
        DecodeError::BadMagic => "BadMagic",
        DecodeError::VersionMismatch { .. } => "VersionMismatch",
        DecodeError::UnknownMessageType(_) => "UnknownMessageType",
        DecodeError::UnknownDurability(_) => "UnknownDurability",
        DecodeError::HeaderCrcMismatch { .. } => "HeaderCrcMismatch",
        DecodeError::PayloadCrcMismatch { .. } => "PayloadCrcMismatch",
        DecodeError::TruncatedFrame => "TruncatedFrame",
        DecodeError::PayloadTooLarge { .. } => "PayloadTooLarge",
        DecodeError::ReservedFlagsSet { .. } => "ReservedFlagsSet",
        DecodeError::TrailingBytes { .. } => "TrailingBytes",
        // DecodeError is #[non_exhaustive] (F48). A new variant must be given a
        // tag here before a vector can reference it; until then, fail loudly
        // rather than silently mis-tagging it.
        other => panic!("conformance: unmapped DecodeError variant {other:?}"),
    }
}

fn message_type_name(mt: MessageType) -> &'static str {
    match mt {
        MessageType::Push => "Push",
        MessageType::Ack => "Ack",
        MessageType::Nack => "Nack",
        MessageType::HealthCheck => "HealthCheck",
        MessageType::HealthCheckResponse => "HealthCheckResponse",
    }
}

fn durability_name(d: Durability) -> &'static str {
    match d {
        Durability::Sync => "Sync",
        Durability::Batched => "Batched",
        Durability::Buffered => "Buffered",
    }
}

#[test]
fn vectors_file_pins_the_protocol_constants() {
    // If the vectors were generated against a different WIRE_VERSION or hard cap
    // than this build, every byte is suspect — fail loudly rather than run.
    let root: Value = serde_json::from_str(VECTORS_JSON).expect("vectors JSON parses");
    assert_eq!(
        root["wire_version"].as_u64().unwrap(),
        WIRE_VERSION as u64,
        "vectors file targets a different WIRE_VERSION than this build"
    );
    assert_eq!(
        root["max_payload_hard_cap"].as_u64().unwrap(),
        MAX_PAYLOAD_HARD_CAP as u64,
        "vectors file targets a different MAX_PAYLOAD_HARD_CAP than this build"
    );
}

#[test]
fn every_vector_decodes_as_specified() {
    let root: Value = serde_json::from_str(VECTORS_JSON).expect("vectors JSON parses");
    let vectors = root["vectors"].as_array().expect("vectors is an array");
    assert!(!vectors.is_empty(), "vectors file is empty");

    let mut checked = 0usize;
    for v in vectors {
        let name = v["name"].as_str().expect("vector has a name");
        let bytes = from_hex(v["hex"].as_str().expect("vector has hex"));
        let expected = v["decode"].as_str().expect("vector has a decode tag");
        let result = Envelope::decode(&bytes);

        if expected == "ok" {
            let env = result.unwrap_or_else(|e| {
                panic!("vector {name}: expected Ok, got Err({e:?})");
            });
            let header = env.header();
            assert_eq!(
                message_type_name(header.message_type()),
                v["message_type"].as_str().unwrap(),
                "vector {name}: message_type mismatch"
            );
            assert_eq!(
                durability_name(header.durability()),
                v["durability"].as_str().unwrap(),
                "vector {name}: durability mismatch"
            );
            assert_eq!(
                header.flags() as u64,
                v["flags"].as_u64().unwrap(),
                "vector {name}: flags mismatch"
            );
            assert_eq!(
                to_hex(&env.payload()[..]),
                v["payload_hex"].as_str().unwrap(),
                "vector {name}: payload mismatch"
            );
            // An "ok" vector is canonical: re-encoding the decoded frame must
            // reproduce the exact input bytes (encode/decode are inverses).
            assert_eq!(
                to_hex(&env.encode()),
                v["hex"].as_str().unwrap(),
                "vector {name}: re-encode is not byte-identical to the vector"
            );
        } else {
            let err =
                result.expect_err(&format!("vector {name}: expected Err({expected}), got Ok"));
            assert_eq!(
                error_tag(&err),
                expected,
                "vector {name}: wrong rejection error ({err:?})"
            );
        }
        checked += 1;
    }
    // Guard against a silently-empty or truncated run.
    assert_eq!(checked, vectors.len(), "did not check every vector");
}
