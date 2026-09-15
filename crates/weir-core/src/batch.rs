//! Payload codecs for the `PushBatch` / `AckBatch` frame pair — N records in
//! one round trip, answered once.
//!
//! Both are **additive within wire v1**: message-type bytes `0x08`–`0xFF` are
//! reserved for exactly this, so [`WIRE_VERSION`](crate::WIRE_VERSION) is
//! unchanged and the thirty frozen conformance vectors stay byte-identical. A
//! daemon that predates these types answers `Nack(UnknownMessage)`, which is an
//! actionable, permanent error rather than a misparse.
//!
//! # Why the reply is a bitmap and what a clear bit means
//!
//! Every sub-record is validated at **ingest**, before any record of the batch
//! is enqueued. A validation failure — an empty record, one over
//! `max_payload_bytes`, a framing disagreement — rejects the *whole frame* with
//! a plain `Nack` carrying its reason, and closes the connection. That is the
//! existing "permanent protocol error ⇒ close" contract, unchanged.
//!
//! The consequence is what makes a bare bitmap sufficient: by the time an
//! `AckBatch` is sent at all, every possible per-record failure is a *runtime*
//! one — WAB cap, queue refusal, a write or fsync that did not succeed, an ack
//! deadline — and every one of those is `InternalError`, which the wire
//! protocol already defines as the sole transient reason. So a clear bit needs
//! no reason byte to be actionable:
//!
//! > **bit 1 ⇒ the record is durable at the requested tier.**
//! > **bit 0 ⇒ not durable as of this reply: retry it, and expect that it may
//! > nonetheless have been written** (at-least-once, exactly as a single-record
//! > `Nack(InternalError)` already means).
//!
//! The asymmetry is deliberate and must be documented wherever this is
//! described: a set bit is a strong statement inheriting weir's crown
//! invariant, while a clear bit is a weak one. A reader who treats clear bits as
//! "definitely not written" will build exactly-once retry on a foundation that
//! does not support it.

use crate::version::MAX_PAYLOAD_HARD_CAP;

/// Version byte at offset 0 of a `PushBatch` body.
///
/// It leads for the same reason [`COORDINATE_VERSION`](crate::COORDINATE_VERSION)
/// does: the body can then grow *inside* wire v1, because a reader meeting a
/// version it does not know rejects the frame instead of parsing a prefix of a
/// layout it has never seen.
pub const BATCH_VERSION: u8 = 1;

/// Version byte at offset 0 of an `AckBatch` payload.
pub const ACK_BATCH_VERSION: u8 = 1;

/// `version(1) + record_count(2)` — the fixed prefix of a `PushBatch` body.
pub const BATCH_HEADER_LEN: usize = 1 + 2;

/// `version(1) + record_count(2)` — the fixed prefix of an `AckBatch` payload,
/// followed by `ceil(N / 8)` bitmap bytes.
pub const ACK_BATCH_HEADER_LEN: usize = 1 + 2;

/// Absolute ceiling on records in one `PushBatch`, across all code paths.
///
/// Chosen so the `AckBatch` reply stays inside the response bound weir already
/// has: `3 + ceil(2048 / 8)` = 259 bytes, under the 298-byte
/// [`MAX_TRACKED_ACK_PAYLOAD_LEN`](crate::MAX_TRACKED_ACK_PAYLOAD_LEN). That
/// keeps 298 the largest response payload weir ever sends, so no client gains a
/// new maximum to allocate for and no published size claim changes.
///
/// It is a *safety* bound, not a tuning knob. `record_count` is a `u16`, so a
/// ~320 KiB frame could otherwise declare 65,535 records — 99.998% of the
/// daemon's entire global queue capacity, all of it targeting the single
/// partition its connection is pinned to.
pub const MAX_BATCH_RECORDS_HARD_CAP: usize = 2048;

/// Largest `AckBatch` payload: the fixed prefix plus a bitmap for the hard cap.
pub const MAX_ACK_BATCH_PAYLOAD_LEN: usize =
    ACK_BATCH_HEADER_LEN + MAX_BATCH_RECORDS_HARD_CAP.div_ceil(8);

/// Why a `PushBatch` body or `AckBatch` payload could not be decoded.
///
/// Variants name the conformance-vector tags, matching
/// [`CoordinateError`](crate::CoordinateError)'s convention so a non-Rust
/// implementer can map a rejection to the vector that pins it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchError {
    /// Shorter than the fixed prefix every body has.
    Truncated {
        /// Bytes present.
        len: usize,
        /// Bytes needed.
        need: usize,
    },
    /// The leading version byte is one this build cannot parse.
    UnsupportedVersion {
        /// The version byte received.
        version: u8,
    },
    /// `record_count` was zero. An empty batch is not a no-op to be tolerated:
    /// it is a framing disagreement, because nothing legitimate produces one.
    EmptyBatch,
    /// `record_count` exceeds the cap in force.
    TooManyRecords {
        /// The declared count.
        count: usize,
        /// The cap.
        cap: usize,
    },
    /// A record inside the batch declared a zero length. The WAB cannot
    /// represent an empty record — a zero `payload_len` is its end-of-records
    /// sentinel — so this is rejected here rather than deeper.
    EmptyRecord {
        /// Index of the offending record.
        index: usize,
    },
    /// A record inside the batch exceeds the per-record cap. Checked per record
    /// and not only per frame, because the frame cap bounds the *batch* and an
    /// operator who sets `max_payload_bytes` is bounding a *record*.
    RecordTooLarge {
        /// Index of the offending record.
        index: usize,
        /// Its declared length.
        len: usize,
        /// The per-record cap.
        cap: usize,
    },
    /// An entry's declared length runs past the end of the body.
    TruncatedRecord {
        /// Index of the offending record.
        index: usize,
    },
    /// The walk consumed a different number of bytes than the body holds, or
    /// found a different number of records than `record_count` declared.
    ///
    /// Both are the same class of fault: the two ends disagree about the layout.
    /// Using whichever half seems more plausible is how that disagreement turns
    /// into records silently dropped or invented.
    LengthMismatch {
        /// What the body declared or implied.
        declared: usize,
        /// What was actually found.
        actual: usize,
    },
    /// A padding bit in the final bitmap byte was set.
    ///
    /// Rejected rather than ignored, following the `ReservedFlagsSet`
    /// precedent: weir does not silently tolerate a set bit it has no meaning
    /// for, because the next version might give it one.
    PaddingNotZero {
        /// The offending final byte.
        byte: u8,
    },
}

impl core::fmt::Display for BatchError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Truncated { len, need } => {
                write!(f, "batch body truncated: {len} bytes, need at least {need}")
            }
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported batch version {version:#04x}")
            }
            Self::EmptyBatch => write!(f, "batch declared zero records"),
            Self::TooManyRecords { count, cap } => {
                write!(f, "batch declared {count} records, cap is {cap}")
            }
            Self::EmptyRecord { index } => {
                write!(f, "record {index} is zero-length")
            }
            Self::RecordTooLarge { index, len, cap } => {
                write!(f, "record {index} is {len} bytes, cap is {cap}")
            }
            Self::TruncatedRecord { index } => {
                write!(f, "record {index} runs past the end of the body")
            }
            Self::LengthMismatch { declared, actual } => {
                write!(f, "batch declared {declared}, found {actual}")
            }
            Self::PaddingNotZero { byte } => {
                write!(f, "padding bits set in final bitmap byte {byte:#04x}")
            }
        }
    }
}

impl core::error::Error for BatchError {}

/// Encodes a `PushBatch` body: version, count, then length-prefixed records.
///
/// Encoding does not enforce the caps — `decode_batch_body` does, and the
/// conformance suite needs to build deliberately-oversized bodies to check that
/// the rejection happens.
pub fn encode_batch_body(records: &[&[u8]]) -> Vec<u8> {
    let total: usize = BATCH_HEADER_LEN + records.iter().map(|r| 4 + r.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.push(BATCH_VERSION);
    out.extend_from_slice(&(records.len() as u16).to_le_bytes());
    for r in records {
        out.extend_from_slice(&(r.len() as u32).to_le_bytes());
        out.extend_from_slice(r);
    }
    out
}

/// Decodes a `PushBatch` body into borrowed record slices.
///
/// # Validation order
///
/// Deliberate, and every step happens before any allocation sized by a
/// peer-chosen number. The frame's payload CRC has already been verified by the
/// caller, but that proves only the absence of *accidental* corruption: a
/// hostile peer computes a perfectly valid CRC over a body declaring 65,535
/// records in three bytes. The CRC contributes nothing to these checks and they
/// are all mandatory.
///
/// 1. the fixed prefix is present
/// 2. the version is one we know
/// 3. the count is non-zero
/// 4. the count is within `max_records` — **before** reserving anything
/// 5. each entry is non-empty, within `max_record_len`, and inside the body
/// 6. the walk ends exactly at the end of the body, having found exactly
///    `record_count` records
pub fn decode_batch_body(
    body: &[u8],
    max_records: usize,
    max_record_len: usize,
) -> Result<Vec<&[u8]>, BatchError> {
    if body.len() < BATCH_HEADER_LEN {
        return Err(BatchError::Truncated {
            len: body.len(),
            need: BATCH_HEADER_LEN,
        });
    }
    let version = body[0];
    if version != BATCH_VERSION {
        return Err(BatchError::UnsupportedVersion { version });
    }
    let declared = u16::from_le_bytes([body[1], body[2]]) as usize;
    if declared == 0 {
        return Err(BatchError::EmptyBatch);
    }
    let cap = max_records.min(MAX_BATCH_RECORDS_HARD_CAP);
    if declared > cap {
        return Err(BatchError::TooManyRecords {
            count: declared,
            cap,
        });
    }

    // `declared` is now bounded by a value this daemon chose, so reserving
    // against it is safe. This is the line the 2.0.3 payload_len fix exists to
    // prevent the unbounded version of.
    let mut out: Vec<&[u8]> = Vec::with_capacity(declared);
    let mut cursor = BATCH_HEADER_LEN;
    let record_cap = max_record_len.min(MAX_PAYLOAD_HARD_CAP);

    while cursor < body.len() {
        let index = out.len();
        if cursor + 4 > body.len() {
            return Err(BatchError::TruncatedRecord { index });
        }
        let len = u32::from_le_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]) as usize;
        cursor += 4;

        if len == 0 {
            return Err(BatchError::EmptyRecord { index });
        }
        if len > record_cap {
            return Err(BatchError::RecordTooLarge {
                index,
                len,
                cap: record_cap,
            });
        }
        if cursor + len > body.len() {
            return Err(BatchError::TruncatedRecord { index });
        }
        // Stop before overrunning the declared count, so a body carrying more
        // records than it declares is a LengthMismatch rather than a silent
        // truncation of the extras.
        if index == declared {
            return Err(BatchError::LengthMismatch {
                declared,
                actual: declared + 1,
            });
        }
        out.push(&body[cursor..cursor + len]);
        cursor += len;
    }

    if out.len() != declared {
        return Err(BatchError::LengthMismatch {
            declared,
            actual: out.len(),
        });
    }
    if cursor != body.len() {
        return Err(BatchError::LengthMismatch {
            declared: cursor,
            actual: body.len(),
        });
    }
    Ok(out)
}

/// Bit `i` of the bitmap, **LSB-first within each byte**.
///
/// The order is stated once, here, and every other site refers to it. It is the
/// single most dangerous under-specification in this codec: a reader using the
/// opposite convention produces a *well-formed* frame whose meaning is inverted
/// — header CRC passes, payload CRC passes, `payload_len` is correct — and the
/// result is a producer told a record is durable when the daemon said it failed.
/// That is a false ack, the one outcome weir exists to prevent, and no checksum
/// or cap detects it.
///
/// LSB-first is chosen because it is the idiom of every language weir has a
/// client in: Rust and C bit arithmetic, Go's `math/bits`, Java's `BitSet`.
#[inline]
#[must_use]
pub const fn bitmap_bit(index: usize) -> (usize, u8) {
    (index / 8, 1u8 << (index % 8))
}

/// Encodes an `AckBatch` payload from per-record outcomes.
///
/// `accepted[i] == true` means record `i` is durable at the requested tier.
/// Padding bits in the final byte are left zero, which the decoder requires.
pub fn encode_ack_batch(accepted: &[bool]) -> Vec<u8> {
    let n = accepted.len();
    let mut out = Vec::with_capacity(ACK_BATCH_HEADER_LEN + n.div_ceil(8));
    out.push(ACK_BATCH_VERSION);
    out.extend_from_slice(&(n as u16).to_le_bytes());
    out.resize(ACK_BATCH_HEADER_LEN + n.div_ceil(8), 0);
    for (i, &ok) in accepted.iter().enumerate() {
        if ok {
            let (byte, mask) = bitmap_bit(i);
            out[ACK_BATCH_HEADER_LEN + byte] |= mask;
        }
    }
    out
}

/// Decodes an `AckBatch` payload into per-record outcomes.
///
/// `expected` is the record count the caller sent in its `PushBatch`. It is
/// checked against the count echoed in the payload, because a bitmap is the
/// first weir response whose meaning depends on client-held state: `Ack` means
/// "your last record was accepted" and `AckTracked` carries its own coordinate,
/// but a bitmap is meaningless without knowing which batch it answers. Since
/// `ceil(N/8)` is not injective — N of 1017 through 1024 all yield 131 bytes —
/// the echoed count is the only thing that can catch a desync.
pub fn decode_ack_batch(payload: &[u8], expected: usize) -> Result<Vec<bool>, BatchError> {
    if payload.len() < ACK_BATCH_HEADER_LEN {
        return Err(BatchError::Truncated {
            len: payload.len(),
            need: ACK_BATCH_HEADER_LEN,
        });
    }
    let version = payload[0];
    if version != ACK_BATCH_VERSION {
        return Err(BatchError::UnsupportedVersion { version });
    }
    let declared = u16::from_le_bytes([payload[1], payload[2]]) as usize;
    if declared == 0 {
        return Err(BatchError::EmptyBatch);
    }
    if declared != expected {
        return Err(BatchError::LengthMismatch {
            declared,
            actual: expected,
        });
    }

    let want = ACK_BATCH_HEADER_LEN + declared.div_ceil(8);
    if payload.len() != want {
        return Err(BatchError::LengthMismatch {
            declared: want,
            actual: payload.len(),
        });
    }

    // Padding bits must be zero. Ignoring them would make `popcount == N` — the
    // obvious way to ask "did the whole batch succeed" — silently wrong.
    let used_in_last = declared % 8;
    if used_in_last != 0 {
        let last = payload[payload.len() - 1];
        let padding_mask = !((1u8 << used_in_last) - 1);
        if last & padding_mask != 0 {
            return Err(BatchError::PaddingNotZero { byte: last });
        }
    }

    let bits = &payload[ACK_BATCH_HEADER_LEN..];
    Ok((0..declared)
        .map(|i| {
            let (byte, mask) = bitmap_bit(i);
            bits[byte] & mask != 0
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: usize = 1024;
    const REC: usize = 16 * 1024;

    #[test]
    fn a_batch_body_round_trips() {
        let records: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        let body = encode_batch_body(&records);
        assert_eq!(decode_batch_body(&body, CAP, REC).unwrap(), records);
    }

    #[test]
    fn a_single_record_batch_is_legal() {
        let body = encode_batch_body(&[b"only"]);
        assert_eq!(
            decode_batch_body(&body, CAP, REC).unwrap(),
            vec![&b"only"[..]]
        );
    }

    #[test]
    fn an_unknown_body_version_is_rejected_not_prefix_parsed() {
        let mut body = encode_batch_body(&[b"x"]);
        body[0] = 0x02;
        assert_eq!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::UnsupportedVersion { version: 0x02 })
        );
    }

    #[test]
    fn a_zero_record_batch_is_rejected() {
        let body = encode_batch_body(&[]);
        assert_eq!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::EmptyBatch)
        );
    }

    #[test]
    fn a_count_over_the_cap_is_rejected_before_any_reservation() {
        // Three bytes of input must not be able to reserve for 65,535 records.
        let mut body = encode_batch_body(&[b"x"]);
        body[1..3].copy_from_slice(&u16::MAX.to_le_bytes());
        assert_eq!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::TooManyRecords {
                count: 65_535,
                cap: CAP
            })
        );
    }

    #[test]
    fn the_hard_cap_binds_even_when_the_caller_asks_for_more() {
        let mut body = encode_batch_body(&[b"x"]);
        body[1..3].copy_from_slice(&u16::MAX.to_le_bytes());
        match decode_batch_body(&body, usize::MAX, REC) {
            Err(BatchError::TooManyRecords { cap, .. }) => {
                assert_eq!(cap, MAX_BATCH_RECORDS_HARD_CAP);
            }
            other => panic!("expected the hard cap to bind, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_record_inside_a_batch_is_rejected() {
        // The WAB cannot represent one: a zero payload_len is its
        // end-of-records sentinel. Catching it here is what keeps it from
        // reaching flush_batch, where it would nack its neighbours.
        let body = encode_batch_body(&[b"ok", b"", b"also ok"]);
        assert_eq!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::EmptyRecord { index: 1 })
        );
    }

    #[test]
    fn a_record_over_the_per_record_cap_is_rejected() {
        // The frame cap bounds the BATCH; an operator setting max_payload_bytes
        // is bounding a RECORD. Without this the setting silently stops meaning
        // what three documents say it means.
        let big = vec![b'x'; 64];
        let body = encode_batch_body(&[b"small", &big]);
        assert_eq!(
            decode_batch_body(&body, CAP, 32),
            Err(BatchError::RecordTooLarge {
                index: 1,
                len: 64,
                cap: 32
            })
        );
    }

    #[test]
    fn a_truncated_record_is_rejected() {
        let mut body = encode_batch_body(&[b"alpha", b"beta"]);
        body.truncate(body.len() - 2);
        assert!(matches!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::TruncatedRecord { .. })
        ));
    }

    #[test]
    fn trailing_bytes_after_the_last_record_are_rejected() {
        let mut body = encode_batch_body(&[b"alpha"]);
        body.push(0xAA);
        assert!(matches!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::TruncatedRecord { .. } | BatchError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn a_count_that_disagrees_with_the_body_is_rejected_either_way() {
        // Declared fewer than present.
        let mut body = encode_batch_body(&[b"a", b"b", b"c"]);
        body[1..3].copy_from_slice(&2u16.to_le_bytes());
        assert!(matches!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::LengthMismatch { .. })
        ));

        // Declared more than present.
        let mut body = encode_batch_body(&[b"a", b"b"]);
        body[1..3].copy_from_slice(&3u16.to_le_bytes());
        assert_eq!(
            decode_batch_body(&body, CAP, REC),
            Err(BatchError::LengthMismatch {
                declared: 3,
                actual: 2
            })
        );
    }

    // ---- AckBatch bitmap ----

    #[test]
    fn an_ack_bitmap_round_trips() {
        for n in [1usize, 7, 8, 9, 63, 64, 65, 255] {
            let accepted: Vec<bool> = (0..n).map(|i| i % 3 != 0).collect();
            let payload = encode_ack_batch(&accepted);
            assert_eq!(
                decode_ack_batch(&payload, n).unwrap(),
                accepted,
                "round trip failed at N={n}"
            );
        }
    }

    /// The test that catches a reversed bitmap.
    ///
    /// N=9 with only records 0 and 8 set is asymmetric across the byte
    /// boundary, so LSB-first and MSB-first produce *different bytes*. Any
    /// vector with N<=8, or a symmetric pattern, encodes identically under both
    /// conventions and would pass while the implementation was inverted — and an
    /// inverted bitmap is a well-formed frame that reports failures as
    /// successes. That is a false ack, which no CRC or cap can detect.
    #[test]
    fn the_bitmap_is_lsb_first_and_the_bytes_say_so() {
        let mut accepted = vec![false; 9];
        accepted[0] = true;
        accepted[8] = true;
        let payload = encode_ack_batch(&accepted);

        assert_eq!(payload[0], ACK_BATCH_VERSION);
        assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 9);
        assert_eq!(payload.len(), ACK_BATCH_HEADER_LEN + 2);

        // LSB-first: record 0 is bit 0 of byte 0 -> 0b0000_0001.
        // MSB-first would put it at 0b1000_0000 = 0x80.
        assert_eq!(
            payload[ACK_BATCH_HEADER_LEN], 0b0000_0001,
            "record 0 must be the LEAST significant bit of the first byte"
        );
        assert_eq!(
            payload[ACK_BATCH_HEADER_LEN + 1],
            0b0000_0001,
            "record 8 must be the least significant bit of the SECOND byte"
        );
        assert_eq!(decode_ack_batch(&payload, 9).unwrap(), accepted);
    }

    #[test]
    fn a_set_padding_bit_is_rejected_rather_than_ignored() {
        // Following ReservedFlagsSet: weir does not tolerate a set bit it has no
        // meaning for. Ignoring padding would also make `popcount == N` — the
        // obvious "did the whole batch succeed" check — silently wrong.
        let accepted = vec![true; 9];
        let mut payload = encode_ack_batch(&accepted);
        let last = payload.len() - 1;
        payload[last] |= 0b1000_0000;
        assert!(matches!(
            decode_ack_batch(&payload, 9),
            Err(BatchError::PaddingNotZero { .. })
        ));
    }

    #[test]
    fn an_ack_for_a_different_batch_is_rejected() {
        // ceil(N/8) is not injective: 1017..=1024 all give 128 bitmap bytes. The
        // echoed count is the only thing that can catch a pipelined desync.
        let accepted = vec![true; 1024];
        let payload = encode_ack_batch(&accepted);
        assert!(decode_ack_batch(&payload, 1024).is_ok());
        assert_eq!(
            decode_ack_batch(&payload, 1017),
            Err(BatchError::LengthMismatch {
                declared: 1024,
                actual: 1017
            })
        );
    }

    #[test]
    fn the_largest_ack_batch_fits_the_existing_response_bound() {
        // The reason MAX_BATCH_RECORDS_HARD_CAP is 2048: weir's largest response
        // payload must stay 298, so no client gains a new maximum to allocate
        // for and no published size claim changes.
        let accepted = vec![true; MAX_BATCH_RECORDS_HARD_CAP];
        let payload = encode_ack_batch(&accepted);
        assert_eq!(payload.len(), MAX_ACK_BATCH_PAYLOAD_LEN);
        assert!(
            payload.len() <= crate::MAX_TRACKED_ACK_PAYLOAD_LEN,
            "the largest AckBatch ({}) exceeds the existing response cap ({}), \
             which would force every client to grow its bound",
            payload.len(),
            crate::MAX_TRACKED_ACK_PAYLOAD_LEN
        );
    }
}
