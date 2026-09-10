//! [`RecordCoordinate`] — the durable address of one accepted record, and the
//! payload codec for the `AckTracked` frame that carries it back to a producer.
//!
//! A plain `Push` tells a producer its record was accepted and nothing more. A
//! [`PushTracked`](crate::MessageType::PushTracked) asks the daemon to say
//! *which* record it was, and the daemon answers with
//! [`AckTracked`](crate::MessageType::AckTracked) carrying this type.
//!
//! This crate only *carries* the coordinate — it never derives one. The
//! `record_id` digest is computed by `weir_sink_sdk::RecordId::for_record`,
//! which is also what the drain hands a sink as its per-record idempotency key;
//! keeping the derivation there is what stops the two from drifting, and keeps
//! `weir-core` free of a hash dependency.

/// Version byte at offset 0 of an `AckTracked` payload.
///
/// It leads the payload so the coordinate can grow *inside* wire v1: a client
/// that meets a version it does not know rejects the frame
/// ([`CoordinateError::UnsupportedVersion`]) instead of mis-parsing a layout it
/// has never seen. Bumping this is not a [`WIRE_VERSION`](crate::WIRE_VERSION)
/// change — the framing is unaffected — but it IS a breaking change for a
/// producer that reads the coordinate, so it moves only with a major release.
pub const COORDINATE_VERSION: u8 = 1;

/// Bytes of an `AckTracked` payload that precede the variable-length segment
/// name: `version(1) + index(8) + record_id(32) + segment_len(2)`.
pub const COORDINATE_FIXED_LEN: usize = 1 + 8 + 32 + 2;

/// Ceiling on the encoded segment identity.
///
/// A real identity is `shard_{id}/seg_{counter:08}.wab.sealed` — 48 bytes even
/// with a `u64::MAX` counter and a five-digit shard, so 255 is over five times
/// the worst case the current naming scheme can produce. The cap exists so a
/// *reader* has a small, fixed pre-allocation bound for a response frame (see
/// [`MAX_TRACKED_ACK_PAYLOAD_LEN`]) rather than trusting a peer-chosen `u16`.
pub const MAX_SEGMENT_NAME_LEN: usize = 255;

/// The largest `AckTracked` payload a conformant daemon sends, and therefore the
/// cap a client applies before allocating a response buffer.
///
/// This is the tracked-push counterpart of the 2-byte bound every *other* weir
/// response obeys. The bound is per message type on purpose: a client that never
/// sends `PushTracked` never sees an `AckTracked`, so its existing 2-byte cap
/// stays correct and unchanged.
pub const MAX_TRACKED_ACK_PAYLOAD_LEN: usize = COORDINATE_FIXED_LEN + MAX_SEGMENT_NAME_LEN;

/// Where one accepted record lives in the write-ahead buffer, plus the id the
/// downstream sees for it.
///
/// # What it is
///
/// - `segment` — the record's segment address, `<shard-dir>/<file-name>`, e.g.
///   `shard_00/seg_00000001.wab.sealed`. Treat it as an opaque, stable string,
///   not a filesystem path: it names the segment the drain will read the record
///   from, which is a **predicted** name at ack time (the segment is still open).
/// - `index` — the record's **1-based** ordinal within that segment.
/// - `record_id` — `sha256(segment ++ index ++ payload)` with length framing.
///   Byte-identical to the `Idempotency-Key` the HTTP sink sends for this
///   record, and to the id the S3 sink names objects with.
///
/// # What it is not
///
/// A coordinate is a **buffer address, not a per-producer sequence number.**
/// Records from other producers interleave, so one producer's indices have
/// holes; `(segment, index)` gives a total order per shard and lets a producer
/// recognise a record it has already sent, but it is not a gap-free sequence.
///
/// A coordinate is also **not a durability upgrade**. A `Buffered` push gets a
/// coordinate as readily as a `Durable` one; the tier still decides whether the
/// bytes survive power loss.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordCoordinate {
    segment: String,
    index: u64,
    record_id: [u8; 32],
}

impl RecordCoordinate {
    /// Builds a coordinate, rejecting a segment name that could not be encoded
    /// within [`MAX_SEGMENT_NAME_LEN`].
    ///
    /// Fallible rather than truncating: a shortened segment name still *looks*
    /// like an address and would send a producer to the wrong record — silently.
    /// The daemon treats an `Err` here as an internal error and nacks.
    pub fn new(segment: String, index: u64, record_id: [u8; 32]) -> Result<Self, CoordinateError> {
        if segment.len() > MAX_SEGMENT_NAME_LEN {
            return Err(CoordinateError::SegmentTooLong {
                len: segment.len(),
                cap: MAX_SEGMENT_NAME_LEN,
            });
        }
        Ok(Self {
            segment,
            index,
            record_id,
        })
    }

    /// The segment's address in the buffer, `<shard-dir>/<file-name>`.
    #[must_use]
    pub fn segment(&self) -> &str {
        &self.segment
    }

    /// The record's 1-based ordinal within [`segment`](Self::segment).
    #[must_use]
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The raw 32-byte record id digest.
    #[must_use]
    pub fn record_id(&self) -> &[u8; 32] {
        &self.record_id
    }

    /// The record id as 64 lowercase hex characters, no prefix — the form that
    /// matches `weir_sink_sdk::RecordId::to_hex` and the sink's
    /// `Idempotency-Key: sha256:<hex>` header.
    #[must_use]
    pub fn record_id_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for b in self.record_id {
            // Writing to a String is infallible.
            let _ = write!(out, "{b:02x}");
        }
        out
    }

    /// Serialises to the `AckTracked` payload layout.
    ///
    /// ```text
    /// [0]      coordinate_version
    /// [1..9]   index          u64 LE
    /// [9..41]  record_id      32 bytes
    /// [41..43] segment_len    u16 LE
    /// [43..]   segment        UTF-8
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COORDINATE_FIXED_LEN + self.segment.len());
        out.push(COORDINATE_VERSION);
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.record_id);
        // `new` caps the length at MAX_SEGMENT_NAME_LEN (255), so this cannot
        // truncate; the cast is bounded by the constructor, not by hope.
        out.extend_from_slice(&(self.segment.len() as u16).to_le_bytes());
        out.extend_from_slice(self.segment.as_bytes());
        out
    }

    /// Parses an `AckTracked` payload.
    ///
    /// The buffer must be *exactly* one coordinate — trailing bytes are rejected
    /// rather than ignored, matching [`Envelope::decode`](crate::Envelope::decode):
    /// a payload longer than it declares means the sender and receiver disagree
    /// about the layout, and quietly using the prefix is how a disagreement
    /// becomes a wrong address.
    pub fn decode(buf: &[u8]) -> Result<Self, CoordinateError> {
        if buf.len() < COORDINATE_FIXED_LEN {
            return Err(CoordinateError::Truncated {
                len: buf.len(),
                need: COORDINATE_FIXED_LEN,
            });
        }
        // Version first: every field offset below is only meaningful for a
        // layout this build knows.
        let version = buf[0];
        if version != COORDINATE_VERSION {
            return Err(CoordinateError::UnsupportedVersion { version });
        }
        let index = u64::from_le_bytes(buf[1..9].try_into().unwrap());
        let record_id: [u8; 32] = buf[9..41].try_into().unwrap();
        let segment_len = u16::from_le_bytes(buf[41..43].try_into().unwrap()) as usize;
        if segment_len > MAX_SEGMENT_NAME_LEN {
            return Err(CoordinateError::SegmentTooLong {
                len: segment_len,
                cap: MAX_SEGMENT_NAME_LEN,
            });
        }
        let expected = COORDINATE_FIXED_LEN + segment_len;
        if buf.len() != expected {
            return Err(CoordinateError::LengthMismatch {
                declared: expected,
                actual: buf.len(),
            });
        }
        let segment = std::str::from_utf8(&buf[COORDINATE_FIXED_LEN..])
            .map_err(|_| CoordinateError::SegmentNotUtf8)?
            .to_owned();
        Ok(Self {
            segment,
            index,
            record_id,
        })
    }
}

/// Why a [`RecordCoordinate`] could not be built or parsed.
///
/// `#[non_exhaustive]` for the same reason [`NackReason`](crate::NackReason) is:
/// the coordinate payload may grow within wire v1, and a downstream `match`
/// must already carry a wildcard arm so that growth stays non-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CoordinateError {
    /// The payload is shorter than the fixed prefix every coordinate has.
    Truncated {
        /// Bytes actually present.
        len: usize,
        /// Bytes the fixed prefix needs.
        need: usize,
    },
    /// The leading version byte is one this build does not know how to parse.
    UnsupportedVersion {
        /// The version byte received.
        version: u8,
    },
    /// The segment name exceeds [`MAX_SEGMENT_NAME_LEN`].
    SegmentTooLong {
        /// The offending length.
        len: usize,
        /// The cap.
        cap: usize,
    },
    /// The declared total length disagrees with the buffer's actual length.
    LengthMismatch {
        /// What `segment_len` implies the payload should be.
        declared: usize,
        /// What it actually is.
        actual: usize,
    },
    /// The segment name bytes are not valid UTF-8.
    SegmentNotUtf8,
}

impl std::fmt::Display for CoordinateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinateError::Truncated { len, need } => write!(
                f,
                "record coordinate truncated: {len} bytes, need at least {need}"
            ),
            CoordinateError::UnsupportedVersion { version } => write!(
                f,
                "unsupported record-coordinate version {version:#04x}; this build understands {COORDINATE_VERSION:#04x}"
            ),
            CoordinateError::SegmentTooLong { len, cap } => write!(
                f,
                "record coordinate segment name is {len} bytes, over the {cap}-byte cap"
            ),
            CoordinateError::LengthMismatch { declared, actual } => write!(
                f,
                "record coordinate declares {declared} bytes but the payload is {actual}"
            ),
            CoordinateError::SegmentNotUtf8 => {
                write!(f, "record coordinate segment name is not valid UTF-8")
            }
        }
    }
}

impl std::error::Error for CoordinateError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RecordCoordinate {
        RecordCoordinate::new("shard_00/seg_00000001.wab.sealed".into(), 7, [0xAB; 32]).unwrap()
    }

    #[test]
    fn round_trips() {
        let c = sample();
        assert_eq!(RecordCoordinate::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn round_trips_at_the_boundaries() {
        for (segment, index) in [
            (String::new(), 0u64),
            ("a".to_string(), 1),
            ("x".repeat(MAX_SEGMENT_NAME_LEN), u64::MAX),
        ] {
            let c = RecordCoordinate::new(segment, index, [0x5A; 32]).unwrap();
            assert_eq!(
                RecordCoordinate::decode(&c.encode()).unwrap(),
                c,
                "coordinate did not survive a round trip"
            );
        }
    }

    /// The wire layout is a published contract; pin the offsets so a field
    /// reorder cannot slip through as "still round-trips".
    #[test]
    fn encoded_layout_is_pinned() {
        let c = RecordCoordinate::new("ab".into(), 0x0102_0304_0506_0708, [0x11; 32]).unwrap();
        let e = c.encode();
        assert_eq!(e.len(), COORDINATE_FIXED_LEN + 2);
        assert_eq!(e[0], COORDINATE_VERSION);
        assert_eq!(&e[1..9], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&e[9..41], &[0x11; 32]);
        assert_eq!(&e[41..43], &[0x02, 0x00]);
        assert_eq!(&e[43..], b"ab");
    }

    #[test]
    fn encoded_length_never_exceeds_the_response_cap() {
        let c = RecordCoordinate::new("z".repeat(MAX_SEGMENT_NAME_LEN), 1, [0; 32]).unwrap();
        assert_eq!(c.encode().len(), MAX_TRACKED_ACK_PAYLOAD_LEN);
    }

    #[test]
    fn new_rejects_an_oversized_segment_name() {
        let err =
            RecordCoordinate::new("z".repeat(MAX_SEGMENT_NAME_LEN + 1), 1, [0; 32]).unwrap_err();
        assert_eq!(
            err,
            CoordinateError::SegmentTooLong {
                len: MAX_SEGMENT_NAME_LEN + 1,
                cap: MAX_SEGMENT_NAME_LEN,
            }
        );
    }

    #[test]
    fn decode_rejects_a_short_payload() {
        let e = sample().encode();
        for take in [0usize, 1, COORDINATE_FIXED_LEN - 1] {
            assert_eq!(
                RecordCoordinate::decode(&e[..take]),
                Err(CoordinateError::Truncated {
                    len: take,
                    need: COORDINATE_FIXED_LEN,
                })
            );
        }
    }

    #[test]
    fn decode_rejects_an_unknown_version() {
        let mut e = sample().encode();
        e[0] = COORDINATE_VERSION + 1;
        assert_eq!(
            RecordCoordinate::decode(&e),
            Err(CoordinateError::UnsupportedVersion {
                version: COORDINATE_VERSION + 1,
            })
        );
    }

    /// Version is checked before any field is read, so a payload that is BOTH
    /// a future version and a wrong length reports the version — the actionable
    /// error ("upgrade this client"), not the derived one.
    #[test]
    fn decode_checks_version_before_length() {
        let mut e = sample().encode();
        e[0] = 0xFF;
        e.push(0x00);
        assert_eq!(
            RecordCoordinate::decode(&e),
            Err(CoordinateError::UnsupportedVersion { version: 0xFF })
        );
    }

    #[test]
    fn decode_rejects_a_declared_length_over_the_cap() {
        let mut e = sample().encode();
        e[41..43].copy_from_slice(&((MAX_SEGMENT_NAME_LEN + 1) as u16).to_le_bytes());
        assert_eq!(
            RecordCoordinate::decode(&e),
            Err(CoordinateError::SegmentTooLong {
                len: MAX_SEGMENT_NAME_LEN + 1,
                cap: MAX_SEGMENT_NAME_LEN,
            })
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes_and_a_short_tail() {
        let base = sample().encode();
        let segment_len = base.len() - COORDINATE_FIXED_LEN;

        let mut long = base.clone();
        long.push(0x00);
        assert_eq!(
            RecordCoordinate::decode(&long),
            Err(CoordinateError::LengthMismatch {
                declared: base.len(),
                actual: base.len() + 1,
            })
        );

        let short = &base[..base.len() - 1];
        assert_eq!(
            RecordCoordinate::decode(short),
            Err(CoordinateError::LengthMismatch {
                declared: COORDINATE_FIXED_LEN + segment_len,
                actual: base.len() - 1,
            })
        );
    }

    #[test]
    fn decode_rejects_a_non_utf8_segment_name() {
        let mut e = sample().encode();
        let last = e.len() - 1;
        e[last] = 0xFF;
        assert_eq!(
            RecordCoordinate::decode(&e),
            Err(CoordinateError::SegmentNotUtf8)
        );
    }

    #[test]
    fn record_id_hex_is_64_lowercase_chars() {
        let mut id = [0u8; 32];
        id[0] = 0x0A;
        id[31] = 0xFF;
        let c = RecordCoordinate::new("s".into(), 1, id).unwrap();
        let hex = c.record_id_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.starts_with("0a"));
        assert!(hex.ends_with("ff"));
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn errors_are_displayable_std_errors() {
        for e in [
            CoordinateError::Truncated { len: 0, need: 43 },
            CoordinateError::UnsupportedVersion { version: 2 },
            CoordinateError::SegmentTooLong { len: 300, cap: 255 },
            CoordinateError::LengthMismatch {
                declared: 43,
                actual: 44,
            },
            CoordinateError::SegmentNotUtf8,
        ] {
            assert!(!e.to_string().is_empty());
            let _: &dyn std::error::Error = &e;
        }
    }
}
