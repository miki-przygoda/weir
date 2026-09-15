//! The [`NackReason`] byte that prefixes every Nack message payload.

/// Reason byte carried as the first byte of every Nack message payload.
/// Wire values are fixed and must not change without a WIRE_VERSION bump.
///
/// VersionMismatch Nack payload format: `[NackReason::VersionMismatch (0x02), daemon_wire_version (u8)]`.
/// The second byte lets the client produce a specific error:
/// "daemon is on wire protocol v1; this client is built against v2 — upgrade the daemon or downgrade the client."
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Reason bytes 0x0A-0xFF are reserved for future use within wire v1 (see
// wire_protocol.md): a new reason can be added in a minor release without a
// WIRE_VERSION bump. #[non_exhaustive] makes that growth non-breaking for
// downstream matchers (they must already carry a wildcard arm), matching the
// rationale on its decode-side twin DecodeError. Adding it later would itself be
// breaking, so it lands before 1.x ships. The wire stays forward-compatible
// regardless: an unknown byte surfaces as ClientError::UnknownNack(u8), not a
// new variant.
//
// `MessageType` USED to be left exhaustive here on the grounds that "changing
// them requires a WIRE_VERSION bump (a major event)". Both halves of that were
// wrong and are now disproved by the tree: 3.0 added PushTracked/AckTracked and
// 4.0 added PushBatch/AckBatch, all with WIRE_VERSION still 1, because a new
// message type is additive on the wire exactly as a new reason byte is. What it
// was NOT is additive in the Rust API, which is what forced 3.0.0 — so
// `MessageType` carries `#[non_exhaustive]` too as of 4.0.0. `Durability` stays
// exhaustive on its own merits: 0x02 is retired, no tier is planned, and
// matching a tier exhaustively against a config string is a legitimate use.
#[non_exhaustive]
pub enum NackReason {
    /// Frame did not start with the `b"WEIR"` magic.
    BadMagic = 0x01,
    /// Frame's version byte did not equal the daemon's `WIRE_VERSION`. The Nack
    /// payload's second byte carries the daemon's version (see above).
    VersionMismatch = 0x02,
    /// Header CRC32 did not match the header bytes.
    BadHeaderCrc = 0x03,
    /// Declared `payload_len` exceeded the daemon's effective cap.
    PayloadTooLarge = 0x04,
    /// Payload CRC32 did not match the payload bytes.
    BadPayloadCrc = 0x05,
    /// The daemon hit an internal error (e.g. queue saturation); transient.
    InternalError = 0x06,
    /// The push carried a zero-length payload, which the WAB cannot represent:
    /// an empty record's length prefix is four zero bytes, identical to the
    /// end-of-records sentinel, so storing one would truncate the segment.
    /// Rejected at ingest.
    EmptyPayload = 0x07,
    /// The frame's header was structurally valid (magic, version, and header CRC
    /// all passed) but carried a `message_type` or `durability` byte this daemon
    /// does not recognise — a PERMANENT client protocol error (typically version
    /// skew). Distinct from `InternalError` (a transient daemon-side condition
    /// that keeps the connection open): the daemon closes the connection after
    /// this Nack. Retrying the identical frame will not succeed (F25).
    UnknownMessage = 0x08,
    /// The frame's header was structurally valid but set one or more bits in the
    /// reserved `flags` byte, which must be zero in wire v1. A daemon rejects
    /// such a frame rather than silently ignoring a flag it does not understand
    /// (which could mean a producer believed a semantic flag took effect when it
    /// did not). Permanent; the daemon closes the connection (F52).
    ReservedFlagsSet = 0x09,
    /// A `PushBatch` body could not be parsed: bad batch version, zero records,
    /// a record length that disagrees with the body, or trailing bytes.
    ///
    /// `0x0B`, not `0x0A`. `0x0A` is this tree's worked example of an
    /// *unassigned* reason — a `try_from` test asserts it errors, a client test
    /// asserts it surfaces as `UnknownNack(0x0A)`, four polyglot clients branch
    /// on `>= 0x0A`, and the frozen vector `nack_reserved_reason` IS `0x0A`.
    /// Assigning it would break a frozen vector, which the freeze forbids.
    BadBatchFraming = 0x0B,
}

impl NackReason {
    /// The canonical snake_case metric-label string for this reason.
    ///
    /// These strings match the daemon's Prometheus `reason` label values exactly
    /// (the `nack_total{reason=...}` series), so an ops consumer — an autoscaler,
    /// an alerting rule, a dashboard — can map a `NackReason` it receives over the
    /// wire to the metric label it must query, without hard-coding the strings.
    /// Inverse of [`NackReason::from_metric_label`].
    ///
    /// ```
    /// use weir_core::NackReason;
    /// assert_eq!(NackReason::PayloadTooLarge.as_metric_label(), "payload_too_large");
    /// ```
    #[must_use]
    pub fn as_metric_label(&self) -> &'static str {
        match self {
            NackReason::BadMagic => "bad_magic",
            NackReason::VersionMismatch => "version_mismatch",
            NackReason::BadHeaderCrc => "bad_header_crc",
            NackReason::PayloadTooLarge => "payload_too_large",
            NackReason::BadPayloadCrc => "bad_payload_crc",
            NackReason::InternalError => "internal_error",
            NackReason::EmptyPayload => "empty_payload",
            NackReason::UnknownMessage => "unknown_message",
            NackReason::ReservedFlagsSet => "reserved_flags_set",
            NackReason::BadBatchFraming => "bad_batch_framing",
        }
    }

    /// Parses a snake_case metric label back into its [`NackReason`], or `None` if
    /// the string is not a known label. Inverse of
    /// [`NackReason::as_metric_label`].
    ///
    /// ```
    /// use weir_core::NackReason;
    /// assert_eq!(
    ///     NackReason::from_metric_label("payload_too_large"),
    ///     Some(NackReason::PayloadTooLarge),
    /// );
    /// assert_eq!(NackReason::from_metric_label("nope"), None);
    /// ```
    #[must_use]
    pub fn from_metric_label(s: &str) -> Option<NackReason> {
        match s {
            "bad_magic" => Some(NackReason::BadMagic),
            "version_mismatch" => Some(NackReason::VersionMismatch),
            "bad_header_crc" => Some(NackReason::BadHeaderCrc),
            "payload_too_large" => Some(NackReason::PayloadTooLarge),
            "bad_payload_crc" => Some(NackReason::BadPayloadCrc),
            "internal_error" => Some(NackReason::InternalError),
            "empty_payload" => Some(NackReason::EmptyPayload),
            "unknown_message" => Some(NackReason::UnknownMessage),
            "reserved_flags_set" => Some(NackReason::ReservedFlagsSet),
            "bad_batch_framing" => Some(NackReason::BadBatchFraming),
            _ => None,
        }
    }
}

impl From<NackReason> for u8 {
    /// The wire byte for this reason. Inverse of [`NackReason::try_from`].
    fn from(reason: NackReason) -> u8 {
        reason as u8
    }
}

impl std::fmt::Display for NackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            NackReason::BadMagic => "bad frame magic",
            NackReason::VersionMismatch => "wire protocol version mismatch",
            NackReason::BadHeaderCrc => "header CRC mismatch",
            NackReason::PayloadTooLarge => "payload exceeds the daemon's cap",
            NackReason::BadPayloadCrc => "payload CRC mismatch",
            NackReason::InternalError => "daemon internal error (transient)",
            NackReason::EmptyPayload => "empty payload (not representable)",
            NackReason::UnknownMessage => "unknown message type or durability byte",
            NackReason::ReservedFlagsSet => "reserved flags byte was non-zero",
            NackReason::BadBatchFraming => "batch body could not be parsed",
        };
        write!(f, "{msg}")
    }
}

// A Nack reason is a legitimate error a producer can propagate, so it implements
// the std error trait (Display + Debug are both present). Lets callers do
// `Box<dyn Error>`/`?` with a `NackReason` directly.
impl std::error::Error for NackReason {}

/// Error returned when a `u8` does not map to a known `NackReason` variant.
/// Preserves the raw byte so the client can log or display it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownNackReason(pub u8);

impl std::fmt::Display for UnknownNackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown nack reason byte: {:#04x}", self.0)
    }
}

impl std::error::Error for UnknownNackReason {}

impl TryFrom<u8> for NackReason {
    type Error = UnknownNackReason;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(NackReason::BadMagic),
            0x02 => Ok(NackReason::VersionMismatch),
            0x03 => Ok(NackReason::BadHeaderCrc),
            0x04 => Ok(NackReason::PayloadTooLarge),
            0x05 => Ok(NackReason::BadPayloadCrc),
            0x06 => Ok(NackReason::InternalError),
            0x07 => Ok(NackReason::EmptyPayload),
            0x08 => Ok(NackReason::UnknownMessage),
            0x09 => Ok(NackReason::ReservedFlagsSet),
            0x0B => Ok(NackReason::BadBatchFraming),
            v => Err(UnknownNackReason(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::WIRE_VERSION;

    #[test]
    fn try_from_accepts_all_known_reasons() {
        assert_eq!(NackReason::try_from(0x01).unwrap(), NackReason::BadMagic);
        assert_eq!(
            NackReason::try_from(0x02).unwrap(),
            NackReason::VersionMismatch
        );
        assert_eq!(
            NackReason::try_from(0x03).unwrap(),
            NackReason::BadHeaderCrc
        );
        assert_eq!(
            NackReason::try_from(0x04).unwrap(),
            NackReason::PayloadTooLarge
        );
        assert_eq!(
            NackReason::try_from(0x05).unwrap(),
            NackReason::BadPayloadCrc
        );
        assert_eq!(
            NackReason::try_from(0x06).unwrap(),
            NackReason::InternalError
        );
        assert_eq!(
            NackReason::try_from(0x07).unwrap(),
            NackReason::EmptyPayload
        );
        assert_eq!(
            NackReason::try_from(0x08).unwrap(),
            NackReason::UnknownMessage
        );
        assert_eq!(
            NackReason::try_from(0x09).unwrap(),
            NackReason::ReservedFlagsSet
        );
    }

    #[test]
    fn from_for_u8_is_inverse_of_try_from_and_display_is_nonempty() {
        // Swept, not ranged: `0x01..=0x09` stopped covering the type when
        // BadBatchFraming was assigned 0x0B, and the assigned set is not
        // contiguous (0x0A is permanently reserved as the frozen vectors'
        // worked "unknown reason" example).
        let mut seen = 0;
        for byte in 0x00u8..=0xFF {
            let Ok(r) = NackReason::try_from(byte) else {
                continue;
            };
            seen += 1;
            assert_eq!(u8::from(r), byte);
            assert!(!r.to_string().is_empty(), "{r:?} has an empty Display");
        }
        assert!(seen >= 10, "the sweep decoded only {seen} reasons");
        // Usable as a std error.
        let _: &dyn std::error::Error = &NackReason::BadMagic;
    }

    #[test]
    fn try_from_returns_unknown_for_unrecognised_byte() {
        // 0x0A is the first unassigned reason byte (0x01..=0x09 are known).
        let err = NackReason::try_from(0x0A).unwrap_err();
        assert_eq!(err.0, 0x0A);
        let err = NackReason::try_from(0x00).unwrap_err();
        assert_eq!(err.0, 0x00);
        let err = NackReason::try_from(0xff).unwrap_err();
        assert_eq!(err.0, 0xff);
    }

    #[test]
    fn repr_values_match_wire() {
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
    fn as_metric_label_pins_every_variant() {
        // Drift guard: these strings MUST match the daemon's private metrics
        // `NackReason` label enum (weir-server/src/metrics/mod.rs) exactly, so an
        // ops consumer can map a wire NackReason to the metric label it queries.
        //
        // This list is a spelling pin and nothing more — it asserts the exact
        // strings an operator types into a dashboard query. It is NOT the guard
        // that catches a missing variant: a hand-written list cannot, which is
        // how `BadBatchFraming` was added to the enum while this test, the
        // server's label enum and its pre-registration sweep all stayed at nine
        // entries. `metric_label_round_trips_for_every_variant` below sweeps the
        // byte space and is what actually holds the set closed.
        assert_eq!(NackReason::BadMagic.as_metric_label(), "bad_magic");
        assert_eq!(
            NackReason::VersionMismatch.as_metric_label(),
            "version_mismatch"
        );
        assert_eq!(NackReason::BadHeaderCrc.as_metric_label(), "bad_header_crc");
        assert_eq!(
            NackReason::PayloadTooLarge.as_metric_label(),
            "payload_too_large"
        );
        assert_eq!(
            NackReason::BadPayloadCrc.as_metric_label(),
            "bad_payload_crc"
        );
        assert_eq!(
            NackReason::InternalError.as_metric_label(),
            "internal_error"
        );
        assert_eq!(NackReason::EmptyPayload.as_metric_label(), "empty_payload");
        assert_eq!(
            NackReason::UnknownMessage.as_metric_label(),
            "unknown_message"
        );
        assert_eq!(
            NackReason::ReservedFlagsSet.as_metric_label(),
            "reserved_flags_set"
        );
        assert_eq!(
            NackReason::BadBatchFraming.as_metric_label(),
            "bad_batch_framing"
        );
    }

    #[test]
    fn metric_label_round_trips_for_every_variant() {
        // Swept over the whole byte space, not over `0x01..=0x09`.
        //
        // That range was written when 0x09 was the last assigned reason, and it
        // silently stopped covering the type the moment `BadBatchFraming`
        // (0x0B) was assigned — so the new variant's label, Display and
        // round-trip all shipped untested by a test whose name promises "every
        // variant". Sweeping every byte and testing whichever ones decode means
        // the next reason is covered the day its byte is assigned rather than
        // the day someone remembers to widen a range.
        //
        // Note the gap this also documents: 0x0A is permanently unassigned,
        // pinned as the worked "unknown reason" example by the frozen vector
        // `nack_reserved_reason`, so the assigned set is NOT contiguous.
        let mut seen = 0;
        for byte in 0x00u8..=0xFF {
            let Ok(r) = NackReason::try_from(byte) else {
                continue;
            };
            seen += 1;
            assert_eq!(
                NackReason::from_metric_label(r.as_metric_label()),
                Some(r),
                "metric label for {r:?} ({byte:#04x}) did not round-trip"
            );
            assert!(
                !r.as_metric_label().is_empty(),
                "{r:?} ({byte:#04x}) has no metric label"
            );
        }
        assert!(
            seen >= 10,
            "the sweep decoded only {seen} reasons; it is no longer reaching the \
             assigned set"
        );
        assert!(
            NackReason::try_from(0x0A).is_err(),
            "0x0A must stay unassigned — the frozen vector nack_reserved_reason \
             pins it as the worked example of an unknown reason byte"
        );

        // An unknown label maps to None.
        assert_eq!(NackReason::from_metric_label("not_a_reason"), None);
        assert_eq!(NackReason::from_metric_label(""), None);
    }

    /// Verifies the VersionMismatch Nack payload is [reason_byte, daemon_version_byte].
    /// The client parses this to produce: "daemon is on vN; this client is built against vM."
    #[test]
    fn version_mismatch_nack_payload_encodes_daemon_version() {
        let payload = [NackReason::VersionMismatch as u8, WIRE_VERSION];
        assert_eq!(payload, [0x02, 0x01]);
    }
}
