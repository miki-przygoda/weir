//! The [`Durability`] tier — the per-record durability guarantee a producer
//! requests in the frame header.

/// Durability tier requested by the producer for a given record.
///
/// There are two tiers because there are two behaviours. `Durable` writes and
/// `fdatasync`s the record — as part of one batch-boundary group fsync —
/// *before* the ACK, so an ack means the bytes are on stable storage and will
/// replay after a crash. `Buffered` trades that for latency: it acks after the
/// in-memory write, before any fsync, so a `Buffered` ack survives a process
/// crash but not power loss.
///
/// # Wire values
///
/// `0x01` and `0x03` are fixed and must not change without a `WIRE_VERSION`
/// bump. **`0x02` is retired and permanently reserved**:
/// it was `Batched`, which fsynced once per batch while `Sync` fsynced once per
/// record. Both moved to the batch-boundary group fsync, at which point the two
/// tiers became the same thing. `try_from` still accepts `0x02` so producers
/// built against weir 1.x keep working; the encoder only ever emits `0x01`.
/// Reusing `0x02` for a future tier would turn every 1.x client into a silent
/// mis-tier, so it stays reserved.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Durable before ACK: written and `fdatasync`ed via the batch-boundary
    /// group fsync before the ACK is sent.
    Durable = 0x01,
    /// Memory write only. ACK is sent after the record enters the in-memory
    /// queue — survives a process crash, but not power loss.
    Buffered = 0x03,
}

// These deprecated consts are named `Sync` and `Batched` on purpose: they are
// aliases for the old variant names, and being spelled exactly like those
// variants is their entire reason to exist — a 1.x caller's `Durability::Sync`
// / `Durability::Batched` must keep compiling unchanged through the
// deprecation path. Renaming them to satisfy `non_upper_case_globals` would
// silently break that path, so the lint is suppressed here instead.
#[allow(non_upper_case_globals)]
impl Durability {
    /// Former name of [`Durability::Durable`].
    #[deprecated(since = "2.0.0", note = "renamed to `Durable`")]
    pub const Sync: Self = Self::Durable;
    /// Former tier that was identical to `Sync` in behaviour. See the type docs.
    #[deprecated(
        since = "2.0.0",
        note = "`Batched` was always identical to `Sync`; use `Durable`"
    )]
    pub const Batched: Self = Self::Durable;
}

impl From<Durability> for u8 {
    /// The canonical wire byte for this tier. Never returns the retired `0x02`.
    fn from(d: Durability) -> u8 {
        d as u8
    }
}

impl std::fmt::Display for Durability {
    /// Also the value of the `tier` label on
    /// `weir_records_{accepted,ack,nack}_total`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Durability::Durable => "durable",
            Durability::Buffered => "buffered",
        };
        write!(f, "{s}")
    }
}

/// Error returned when a `u8` does not map to a known `Durability` variant.
/// Preserves the raw byte for logging by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownDurability(pub u8);

impl std::fmt::Display for UnknownDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown durability byte: {:#04x}", self.0)
    }
}

impl std::error::Error for UnknownDurability {}

impl TryFrom<u8> for Durability {
    type Error = UnknownDurability;

    /// Permissive by design: `0x02` (retired `Batched`) decodes to
    /// [`Durability::Durable`] so weir 1.x producers keep working.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 | 0x02 => Ok(Durability::Durable),
            0x03 => Ok(Durability::Buffered),
            v => Err(UnknownDurability(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_values_are_frozen() {
        assert_eq!(Durability::Durable as u8, 0x01);
        assert_eq!(Durability::Buffered as u8, 0x03);
    }

    #[test]
    fn legacy_batched_byte_still_decodes_to_durable() {
        // weir 1.3.1 is live and 0x02 is a published contract. A 1.x client
        // pushing Batched must keep working against a 2.0 daemon.
        assert_eq!(Durability::try_from(0x02).unwrap(), Durability::Durable);
    }

    #[test]
    fn encoder_never_emits_the_retired_byte() {
        // Decode is permissive, encode is canonical.
        assert_eq!(u8::from(Durability::Durable), 0x01);
        assert_ne!(u8::from(Durability::Durable), 0x02);
    }

    #[test]
    fn unknown_bytes_are_still_rejected() {
        assert!(Durability::try_from(0x00).is_err());
        assert!(Durability::try_from(0x04).is_err());
        assert_eq!(
            Durability::try_from(0x7f).unwrap_err(),
            UnknownDurability(0x7f)
        );
    }

    #[test]
    fn deprecated_aliases_resolve_to_durable() {
        #![allow(deprecated)]
        assert_eq!(Durability::Sync, Durability::Durable);
        assert_eq!(Durability::Batched, Durability::Durable);
    }

    #[test]
    fn display_matches_the_metric_label() {
        assert_eq!(Durability::Durable.to_string(), "durable");
        assert_eq!(Durability::Buffered.to_string(), "buffered");
    }
}
