//! The batch key the ClickHouse sink sends as `insert_deduplication_token` and
//! the HTTP sink sends as its NDJSON-mode `Idempotency-Key`.
//!
//! # Why not the `DedupToken`
//!
//! A [`DedupToken`](weir_sink_sdk::DedupToken) is a pure hash of the batch's
//! bytes. Two distinct batches that carry byte-identical records — a producer
//! sending the same heartbeat every 30 s, so each timed-seal segment drains as
//! a one-record batch — therefore share a token, and a downstream that
//! deduplicates on it keeps the first and silently discards the rest while the
//! drain confirms and deletes them (sinks-r3-01, sinks-r3-02).
//!
//! This key names where the batch sits in the buffer instead, the same
//! coordinate the S3 sink's object names use: the segment's `created_at`, the
//! batch's first [`RecordId`](weir_sink_sdk::RecordId) (which mixes in the
//! segment's name and the record's index in it), and the record count. A
//! replay of the same batch (same segment, same batch boundaries) presents the
//! same key; distinct segments present distinct keys even when the bytes
//! match; two sub-batches of one segment differ by their first record.
//!
//! # Format
//!
//! The key is `sha256(preimage)` as 64 lowercase hex characters, where the
//! preimage is the ASCII string
//!
//! ```text
//! {created_at:016x}-{first_record_id_hex}-{count}
//! ```
//!
//! - `created_at` is the segment header's creation time in unix nanoseconds,
//!   reinterpreted as a `u64` and written as 16 lowercase hex digits;
//! - `first_record_id_hex` is the batch's first `RecordId` as 64 lowercase hex
//!   digits;
//! - `count` is the number of records in the batch, in decimal with no
//!   padding.
//!
//! The preimage is byte-for-byte the S3 sink's object name for the same batch
//! (`weir-sink-s3`'s `batch_name`), without the extension. Pinned by
//! `batch_key_known_answer`.
//!
//! # Fallback
//!
//! A batch with no record ids or no segment `created_at` was not built by the
//! drain (it is a [`SinkBatch::new`] or `From<Vec<Payload>>` batch, e.g. a sink
//! author's test). It keeps the old key: the batch's `DedupToken` in hex.

use sha2::{Digest, Sha256};

use super::SinkBatch;

/// The batch's key as 64 lowercase hex characters. See the module docs for
/// the exact preimage and the fallback.
pub(crate) fn batch_key_hex(batch: &SinkBatch) -> String {
    let first = batch.record_ids().and_then(|ids| ids.first());
    match (batch.segment_created_at(), first) {
        (Some(created_at), Some(first)) => {
            coordinate_key_hex(created_at, &first.to_hex(), batch.len())
        }
        _ => batch.dedup_token().to_hex(),
    }
}

/// `sha256("{created_at:016x}-{first_record_id_hex}-{count}")` in lowercase
/// hex.
fn coordinate_key_hex(created_at: i64, first_record_id_hex: &str, count: usize) -> String {
    use std::fmt::Write as _;
    let preimage = format!("{:016x}-{first_record_id_hex}-{count}", created_at as u64);
    let digest = Sha256::digest(preimage.as_bytes());
    let mut out = String::with_capacity(64);
    for b in digest {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use weir_core::Payload;
    use weir_sink_sdk::{DedupToken, RecordId};

    const T: i64 = 1_788_704_730_000_000_000;

    fn heartbeats(n: usize) -> Vec<Payload> {
        (0..n)
            .map(|_| Payload::from_static(br#"{"status":"ok"}"#))
            .collect()
    }

    /// A batch as the drain builds it: records `start..start + n` of `segment`.
    fn drained(segment: &str, created_at: i64, start: u64, n: usize) -> SinkBatch {
        let records = heartbeats(n);
        let ids = records
            .iter()
            .enumerate()
            .map(|(i, r)| RecordId::for_record(segment, start + i as u64, r))
            .collect();
        let token = DedupToken::for_payloads(records.iter());
        SinkBatch::with_segment_context(records, token, ids, created_at)
    }

    /// Pins the derivation. Changing it changes the key every ClickHouse table
    /// and idempotent HTTP endpoint deduplicates on, so a batch in flight at the
    /// upgrade is delivered twice: that needs a CHANGELOG entry, not a quiet
    /// edit here.
    #[test]
    fn batch_key_known_answer() {
        let first = "11".repeat(32);
        assert_eq!(
            coordinate_key_hex(T, &first, 3),
            "f9f2be833231eb8c64a5eeafe1c97eceffa5b02da9f473bd617aa5de6639397d",
            "the batch key derivation changed"
        );
        // And the batch path feeds it exactly those three fields.
        let records = heartbeats(3);
        let ids = vec![RecordId::from_bytes([0x11; 32]); 3];
        let token = DedupToken::for_payloads(records.iter());
        let batch = SinkBatch::with_segment_context(records, token, ids, T);
        assert_eq!(batch_key_hex(&batch), coordinate_key_hex(T, &first, 3));
    }

    #[test]
    fn preimage_is_the_s3_object_name() {
        // A negative created_at is reinterpreted, not sign-extended into "-1".
        let a = coordinate_key_hex(-1, &"11".repeat(32), 3);
        let preimage = format!("ffffffffffffffff-{}-3", "11".repeat(32));
        let want: String = Sha256::digest(preimage.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(a, want);
    }

    #[test]
    fn identical_bytes_in_distinct_segments_get_distinct_keys() {
        let a = drained("seg-000001.wab", T, 0, 1);
        let b = drained("seg-000002.wab", T + 30_000_000_000, 0, 1);
        assert_eq!(a.dedup_token(), b.dedup_token(), "the bytes match");
        assert_ne!(batch_key_hex(&a), batch_key_hex(&b));
    }

    #[test]
    fn a_replay_of_the_same_batch_gets_the_same_key() {
        let a = drained("seg-000001.wab", T, 0, 4);
        let replay = drained("seg-000001.wab", T, 0, 4);
        assert_eq!(batch_key_hex(&a), batch_key_hex(&replay));
    }

    #[test]
    fn sub_batches_of_one_segment_get_distinct_keys() {
        // Same segment, same bytes, same size: only the first record differs.
        let a = drained("seg-000001.wab", T, 0, 2);
        let b = drained("seg-000001.wab", T, 2, 2);
        assert_ne!(batch_key_hex(&a), batch_key_hex(&b));
    }

    #[test]
    fn a_batch_without_segment_context_keeps_the_dedup_token() {
        let records = heartbeats(2);
        let token = DedupToken::for_payloads(records.iter());
        let plain = SinkBatch::new(records.clone(), token);
        assert_eq!(batch_key_hex(&plain), token.to_hex());
        assert_eq!(
            batch_key_hex(&SinkBatch::from(records.clone())),
            token.to_hex()
        );
        // Ids without a created_at (with_record_ids) also falls back.
        let ids = vec![RecordId::from_bytes([0x22; 32]); 2];
        let with_ids = SinkBatch::with_record_ids(records, token, ids);
        assert_eq!(batch_key_hex(&with_ids), token.to_hex());
    }
}
