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
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// What the daemon told the producer about one record.
///
/// Exactly three outcomes, and the third is not a failure. `Unknown` records
/// are legitimately indeterminate — the connection died before a response — so
/// the verifier constrains neither their delivery nor their absence. Counting
/// them rather than reclassifying them is what keeps the oracle honest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The daemon returned Ack. This is a promise and the suite holds weir to it.
    Acked,
    /// The daemon returned Nack. It explicitly refused the record.
    Nacked(String),
    /// Pushed, but no response arrived. Either outcome conforms.
    Unknown,
}

/// One record's fate, as recorded by the load generator.
///
/// Serialised as a single space-delimited line. The Nack reason comes last so
/// it may contain spaces without needing quoting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// Per-run sequence number; pairs with the run id to identify the record.
    pub seq: u64,
    /// Durability tier: 'S' Sync, 'B' Batched, 'U' Buffered (unsynced).
    pub tier: char,
    /// What the daemon said.
    pub outcome: Outcome,
    /// Wall-clock microseconds since the Unix epoch at push time.
    pub t_micros: u64,
    /// Round-trip time of the push call, in microseconds.
    pub rtt_micros: u64,
}

impl LedgerEntry {
    /// Serialises to one line: `seq tier t_micros rtt_micros outcome [reason…]`.
    #[must_use]
    pub fn to_line(&self) -> String {
        let (tag, reason) = match &self.outcome {
            Outcome::Acked => ("ACK", String::new()),
            Outcome::Unknown => ("UNK", String::new()),
            Outcome::Nacked(r) => ("NACK", format!(" {}", r.replace('\n', " "))),
        };
        format!(
            "{} {} {} {} {}{}",
            self.seq, self.tier, self.t_micros, self.rtt_micros, tag, reason
        )
    }

    /// Parses a line produced by [`LedgerEntry::to_line`].
    #[must_use]
    pub fn from_line(line: &str) -> Option<Self> {
        let mut parts = line.splitn(6, ' ');
        let seq = parts.next()?.parse().ok()?;
        let tier = parts.next()?.chars().next()?;
        let t_micros = parts.next()?.parse().ok()?;
        let rtt_micros = parts.next()?.parse().ok()?;
        let outcome = match parts.next()? {
            "ACK" => Outcome::Acked,
            "UNK" => Outcome::Unknown,
            "NACK" => Outcome::Nacked(parts.next().unwrap_or("").to_string()),
            _ => return None,
        };
        Some(Self {
            seq,
            tier,
            outcome,
            t_micros,
            rtt_micros,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_never_contains_a_newline() {
        let line = encode_record(7, 42, 128);
        assert_eq!(
            line.len(),
            128,
            "encoded record must hit the requested size"
        );
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
        assert!(
            line.len() > 8,
            "encoder must not truncate identity to hit size"
        );
        assert_eq!(decode_record(&line), Some(Record { run_id: 7, seq: 42 }));
    }

    #[test]
    fn round_trips_at_numeric_and_size_boundaries() {
        for (run_id, seq) in [(0u64, 0u64), (u64::MAX, u64::MAX), (0, u64::MAX)] {
            for size in [0usize, 32, 4096] {
                let line = encode_record(run_id, seq, size);
                assert!(
                    !line.as_bytes().contains(&b'\n'),
                    "payload must stay newline-free at every boundary"
                );
                assert_eq!(
                    decode_record(&line),
                    Some(Record { run_id, seq }),
                    "round-trip must hold for run_id={run_id} seq={seq} size={size}"
                );
            }
        }
    }

    #[test]
    fn decode_rejects_junk() {
        assert_eq!(decode_record("not json"), None);
        assert_eq!(decode_record(""), None);
        assert_eq!(decode_record("{\"run\":1}"), None, "missing seq");
    }

    #[test]
    fn ledger_entry_round_trips_every_outcome() {
        let cases = vec![
            Outcome::Acked,
            Outcome::Nacked("PayloadTooLarge".to_string()),
            Outcome::Unknown,
        ];
        for outcome in cases {
            let entry = LedgerEntry {
                seq: 99,
                tier: 'S',
                outcome: outcome.clone(),
                t_micros: 1_700_000_000_000_000,
                rtt_micros: 364,
            };
            let line = entry.to_line();
            assert!(!line.contains('\n'), "ledger lines must be single-line");
            let parsed = LedgerEntry::from_line(&line).expect("must parse back");
            assert_eq!(parsed, entry);
        }
    }

    #[test]
    fn ledger_nack_reason_with_spaces_survives_round_trip() {
        let entry = LedgerEntry {
            seq: 1,
            tier: 'B',
            outcome: Outcome::Nacked("some reason with spaces".to_string()),
            t_micros: 5,
            rtt_micros: 6,
        };
        assert_eq!(LedgerEntry::from_line(&entry.to_line()), Some(entry));
    }

    #[test]
    fn ledger_rejects_malformed_lines() {
        assert_eq!(LedgerEntry::from_line(""), None);
        assert_eq!(LedgerEntry::from_line("1 S"), None);
    }
}
