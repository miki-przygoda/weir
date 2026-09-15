//! Conformance suite for the batch extension: weir-core's codecs vs.
//! `docs/conformance/wire_v1_batch_vectors.json`.
//!
//! A third suite, for the reason there is a third JSON file: `wire_v1_vectors.json`
//! is frozen and is read by five polyglot demo clients whose decoders know
//! message types `0x01..=0x05`. A `PushBatch` (`0x08`) vector in there would
//! make every one of them fail on a frame they are *correct* to reject.
//!
//! The vectors come from `gen_batch_vectors.py`, whose CRC-32 is Python's
//! `zlib` and whose bitmap arithmetic is written out longhand rather than
//! ported. A vector can therefore only pass here if two independent
//! implementations agree on every byte — which is the only way to catch the one
//! bug this codec can have that nothing else detects: an inverted bitmap is a
//! well-formed frame, valid CRCs and correct length, that reports failures as
//! successes.

use serde_json::Value;
use weir_core::{
    ACK_BATCH_HEADER_LEN, ACK_BATCH_VERSION, BATCH_HEADER_LEN, BATCH_VERSION, BatchError,
    Durability, Envelope, MAX_ACK_BATCH_PAYLOAD_LEN, MAX_BATCH_RECORDS_HARD_CAP,
    MAX_PAYLOAD_HARD_CAP, MessageType, WIRE_VERSION, decode_ack_batch, decode_batch_body,
    encode_ack_batch, encode_batch_body,
};

const VECTORS_JSON: &str = include_str!("../../../docs/conformance/wire_v1_batch_vectors.json");

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

/// Maps a `BatchError` to the stable tag the vectors name — the contract
/// surface a non-Rust implementer maps a rejection onto.
fn batch_error_tag(e: &BatchError) -> &'static str {
    match e {
        BatchError::Truncated { .. } => "Truncated",
        BatchError::UnsupportedVersion { .. } => "UnsupportedVersion",
        BatchError::EmptyBatch => "EmptyBatch",
        BatchError::TooManyRecords { .. } => "TooManyRecords",
        BatchError::EmptyRecord { .. } => "EmptyRecord",
        BatchError::RecordTooLarge { .. } => "RecordTooLarge",
        BatchError::TruncatedRecord { .. } => "TruncatedRecord",
        BatchError::LengthMismatch { .. } => "LengthMismatch",
        BatchError::PaddingNotZero { .. } => "PaddingNotZero",
        // BatchError is #[non_exhaustive]: a new variant must be given a tag
        // here before a vector can name it, rather than being mis-tagged as an
        // existing one.
        other => panic!("conformance: unmapped BatchError variant {other:?}"),
    }
}

fn message_type_name(mt: MessageType) -> &'static str {
    match mt {
        MessageType::PushBatch => "PushBatch",
        MessageType::AckBatch => "AckBatch",
        other => panic!("conformance: batch vectors carry only 0x08/0x09, got {other:?}"),
    }
}

fn durability_name(d: Durability) -> &'static str {
    match d {
        Durability::Durable => "Durable",
        Durability::Buffered => "Buffered",
    }
}

fn doc() -> Value {
    serde_json::from_str(VECTORS_JSON).expect("batch vectors file is valid JSON")
}

#[test]
fn vectors_file_pins_the_protocol_constants() {
    // If the vectors were generated against different constants than this build
    // compiles with, every byte comparison below is meaningless.
    let doc = doc();
    assert_eq!(doc["wire_version"].as_u64().unwrap(), WIRE_VERSION as u64);
    assert_eq!(doc["batch_version"].as_u64().unwrap(), BATCH_VERSION as u64);
    assert_eq!(
        doc["ack_batch_version"].as_u64().unwrap(),
        ACK_BATCH_VERSION as u64
    );
    assert_eq!(
        doc["batch_header_len"].as_u64().unwrap(),
        BATCH_HEADER_LEN as u64
    );
    assert_eq!(
        doc["ack_batch_header_len"].as_u64().unwrap(),
        ACK_BATCH_HEADER_LEN as u64
    );
    assert_eq!(
        doc["max_batch_records_hard_cap"].as_u64().unwrap(),
        MAX_BATCH_RECORDS_HARD_CAP as u64
    );
    assert_eq!(
        doc["max_ack_batch_payload_len"].as_u64().unwrap(),
        MAX_ACK_BATCH_PAYLOAD_LEN as u64
    );
}

#[test]
fn every_frame_vector_decodes_and_round_trips() {
    let doc = doc();
    let vectors = doc["frame_vectors"]
        .as_array()
        .expect("frame_vectors array");
    assert!(!vectors.is_empty(), "the vectors file must not be empty");

    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let buf = from_hex(v["hex"].as_str().unwrap());
        assert_eq!(
            v["decode"].as_str().unwrap(),
            "ok",
            "{name}: only 'ok' frame vectors are defined"
        );

        let env = Envelope::decode(&buf).unwrap_or_else(|e| panic!("{name}: decode failed: {e:?}"));
        let h = env.header();
        assert_eq!(
            message_type_name(h.message_type()),
            v["message_type"].as_str().unwrap(),
            "{name}: message_type"
        );
        assert_eq!(
            durability_name(h.durability()),
            v["durability"].as_str().unwrap(),
            "{name}: durability"
        );
        assert_eq!(
            h.flags() as u64,
            v["flags"].as_u64().unwrap(),
            "{name}: flags"
        );
        assert_eq!(
            to_hex(env.payload()),
            v["payload_hex"].as_str().unwrap(),
            "{name}: payload"
        );
        assert_eq!(
            to_hex(&env.encode()),
            v["hex"].as_str().unwrap(),
            "{name}: re-encode is not byte-identical"
        );
    }
}

#[test]
fn every_body_vector_matches_the_decoder() {
    let doc = doc();
    let vectors = doc["body_vectors"].as_array().expect("body_vectors array");
    assert!(!vectors.is_empty(), "the vectors file must not be empty");

    let mut ok_seen = 0;
    let mut reject_seen = 0;
    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let buf = from_hex(v["hex"].as_str().unwrap());
        let expected = v["decode"].as_str().unwrap();

        match decode_batch_body(&buf, MAX_BATCH_RECORDS_HARD_CAP, MAX_PAYLOAD_HARD_CAP) {
            Ok(records) => {
                assert_eq!(expected, "ok", "{name}: expected rejection, but it decoded");
                let want: Vec<String> = v["records_hex"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r.as_str().unwrap().to_string())
                    .collect();
                let got: Vec<String> = records.iter().map(|r| to_hex(r)).collect();
                assert_eq!(got, want, "{name}: records");

                // Re-encoding must reproduce the vector byte for byte. Decode
                // alone would accept an encoder that emitted a different but
                // self-consistent layout.
                assert_eq!(
                    to_hex(&encode_batch_body(&records)),
                    v["hex"].as_str().unwrap(),
                    "{name}: re-encode is not byte-identical"
                );
                ok_seen += 1;
            }
            Err(e) => {
                assert_ne!(expected, "ok", "{name}: expected ok, got {e:?}");
                assert_eq!(batch_error_tag(&e), expected, "{name}: wrong rejection tag");
                reject_seen += 1;
            }
        }
    }
    assert!(
        ok_seen > 0 && reject_seen > 0,
        "the body vectors must cover both acceptance and rejection \
         ({ok_seen} ok, {reject_seen} rejected)"
    );
}

#[test]
fn every_ack_vector_matches_the_decoder() {
    let doc = doc();
    let vectors = doc["ack_vectors"].as_array().expect("ack_vectors array");
    assert!(!vectors.is_empty(), "the vectors file must not be empty");

    let mut ok_seen = 0;
    let mut reject_seen = 0;
    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let buf = from_hex(v["hex"].as_str().unwrap());
        let expected = v["decode"].as_str().unwrap();
        let count = v["expected"].as_u64().unwrap() as usize;

        match decode_ack_batch(&buf, count) {
            Ok(accepted) => {
                assert_eq!(expected, "ok", "{name}: expected rejection, but it decoded");
                assert_eq!(accepted.len(), count, "{name}: verdict count");

                // Exactly one of the two forms — the at-cap vector uses the
                // shorthand because 2048 booleans one per line is 20 KB of
                // noise in every diff of the vectors file.
                let want: Vec<bool> = match (v.get("accepted"), v.get("accepted_all")) {
                    (Some(a), None) => a
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|b| b.as_bool().unwrap())
                        .collect(),
                    (None, Some(all)) => vec![all.as_bool().unwrap(); count],
                    _ => panic!("{name}: give exactly one of 'accepted' / 'accepted_all'"),
                };
                assert_eq!(
                    accepted, want,
                    "{name}: verdicts disagree with the vector — if this is the \
                     N=9 asymmetric vector, the bitmap bit order is inverted"
                );
                assert_eq!(
                    to_hex(&encode_ack_batch(&accepted)),
                    v["hex"].as_str().unwrap(),
                    "{name}: re-encode is not byte-identical"
                );
                ok_seen += 1;
            }
            Err(e) => {
                assert_ne!(expected, "ok", "{name}: expected ok, got {e:?}");
                assert_eq!(batch_error_tag(&e), expected, "{name}: wrong rejection tag");
                reject_seen += 1;
            }
        }
    }
    assert!(
        ok_seen > 0 && reject_seen > 0,
        "the ack vectors must cover both acceptance and rejection \
         ({ok_seen} ok, {reject_seen} rejected)"
    );
}

/// The bitmap convention, pinned against a hand-written literal rather than
/// against `encode_ack_batch`'s own output.
///
/// Every other assertion in this file compares weir to Python. This one
/// compares it to bytes spelled out in the source, so that a reader can see the
/// convention without running anything: record 0 and record 2 accepted put bits
/// in the LOW end of byte 0.
#[test]
fn the_bitmap_is_lsb_first_and_a_literal_says_so() {
    let accepted = [true, false, true, false, false, false, false, false, true];
    let encoded = encode_ack_batch(&accepted);
    assert_eq!(
        encoded,
        vec![ACK_BATCH_VERSION, 9, 0, 0b0000_0101, 0b0000_0001],
        "LSB-first means bit i sits at mask 1 << (i % 8); MSB-first would give \
         a0 80 here, which is a well-formed frame with the opposite meaning"
    );
    assert_eq!(decode_ack_batch(&encoded, 9).unwrap(), accepted);
}
