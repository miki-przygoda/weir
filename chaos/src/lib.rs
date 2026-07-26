//! Shared types for the chaos harness: record identity and the outcome ledger.
//!
//! Linux-only, unpublished. See `docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md`.

/// A record's identity, carried in its payload and recovered by the recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    /// Derived from the schedule seed; distinguishes runs.
    pub run_id: u64,
    /// Monotonic per-run sequence number.
    pub seq: u64,
}

/// Encodes a record's identity as newline-free ASCII JSON, padded to `size`.
///
/// The encoding is load-bearing. The HTTP sink's NDJSON batch mode
/// dead-letters any record containing an embedded `0x0A`
/// (`crates/weir-server/src/sink/http.rs:513-525`) rather than failing the
/// batch — so a binary payload that happened to contain a newline would be
/// acked, silently diverted, and never delivered. The verifier would then
/// report a durability violation that is really a harness bug.
///
/// If `size` is too small to hold the identity, the identity wins and the
/// returned string is longer than `size`. Truncating would corrupt the oracle.
#[must_use]
pub fn encode_record(run_id: u64, seq: u64, size: usize) -> String {
    let head = format!("{{\"run\":{run_id},\"seq\":{seq},\"pad\":\"");
    let tail = "\"}";
    let overhead = head.len() + tail.len();
    let pad_len = size.saturating_sub(overhead);
    let mut s = String::with_capacity(overhead + pad_len);
    s.push_str(&head);
    s.extend(std::iter::repeat_n('a', pad_len));
    s.push_str(tail);
    s
}

/// Recovers a record's identity from an encoded payload line.
///
/// Hand-rolled rather than pulling in `serde_json`: the format is fixed and
/// produced by `encode_record` in this same crate, so a dependency would buy
/// nothing. Returns `None` for anything that is not a well-formed record.
#[must_use]
pub fn decode_record(line: &str) -> Option<Record> {
    let run_id = extract_u64(line, "\"run\":")?;
    let seq = extract_u64(line, "\"seq\":")?;
    Some(Record { run_id, seq })
}

fn extract_u64(haystack: &str, key: &str) -> Option<u64> {
    let start = haystack.find(key)? + key.len();
    let rest = &haystack[start..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_never_contains_a_newline() {
        let line = encode_record(7, 42, 128);
        assert_eq!(line.len(), 128, "encoded record must hit the requested size");
        assert!(
            !line.as_bytes().contains(&b'\n'),
            "payload must be newline-free: the NDJSON sink dead-letters records \
             containing 0x0A, which would look like a durability violation"
        );
        assert_eq!(decode_record(&line), Some(Record { run_id: 7, seq: 42 }));
    }

    #[test]
    fn rejects_a_size_too_small_to_hold_the_identity() {
        // 8 bytes cannot hold `{"run":7,"seq":42,"pad":""}`.
        let line = encode_record(7, 42, 8);
        assert!(line.len() > 8, "encoder must not truncate identity to hit size");
        assert_eq!(decode_record(&line), Some(Record { run_id: 7, seq: 42 }));
    }

    #[test]
    fn decode_rejects_junk() {
        assert_eq!(decode_record("not json"), None);
        assert_eq!(decode_record(""), None);
        assert_eq!(decode_record("{\"run\":1}"), None, "missing seq");
    }
}
