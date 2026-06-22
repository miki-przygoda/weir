//! The [`Durability`] tier — the per-record durability guarantee a producer
//! requests in the frame header.

/// Durability tier requested by the producer for a given record.
/// Wire values are fixed and must not change without a WIRE_VERSION bump.
///
/// `Sync` and `Batched` carry the **same durability guarantee today**: the
/// record is written and `fdatasync`ed — as part of one batch-boundary group
/// fsync — *before* the ACK is sent, so an ack means the bytes are on stable
/// storage and replay after a crash. They remain distinct wire values for
/// historical and forward-compatibility reasons (originally `Sync` fsynced once
/// per record and `Batched` once per batch; both now share the batch-boundary
/// group fsync — see `docs/architecture.md`). `Buffered` trades durability for
/// latency: it acks before any fsync.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Durable before ACK: written and `fdatasync`ed (via the batch-boundary
    /// group fsync) before the ACK — on stable storage, replays after a crash.
    Sync = 0x01,
    /// Durable before ACK — the same guarantee as [`Durability::Sync`] today:
    /// written and `fdatasync`ed at the batch boundary before the ACK.
    /// (Historically `Batched` deferred fsync to the batch boundary while `Sync`
    /// fsynced per record; both now share the batch-boundary group fsync.)
    Batched = 0x02,
    /// Memory write only. ACK is sent after the record enters the in-memory queue.
    Buffered = 0x03,
}

impl From<Durability> for u8 {
    /// The wire byte for this tier. Inverse of [`Durability::try_from`].
    fn from(d: Durability) -> u8 {
        d as u8
    }
}

impl std::fmt::Display for Durability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Durability::Sync => "sync",
            Durability::Batched => "batched",
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

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Durability::Sync),
            0x02 => Ok(Durability::Batched),
            0x03 => Ok(Durability::Buffered),
            v => Err(UnknownDurability(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_accepts_known_values() {
        assert_eq!(Durability::try_from(0x01).unwrap(), Durability::Sync);
        assert_eq!(Durability::try_from(0x02).unwrap(), Durability::Batched);
        assert_eq!(Durability::try_from(0x03).unwrap(), Durability::Buffered);
    }

    #[test]
    fn from_for_u8_round_trips_and_display() {
        for d in [Durability::Sync, Durability::Batched, Durability::Buffered] {
            assert_eq!(Durability::try_from(u8::from(d)).unwrap(), d);
        }
        assert_eq!(Durability::Sync.to_string(), "sync");
        assert_eq!(Durability::Buffered.to_string(), "buffered");
    }

    #[test]
    fn try_from_rejects_unknown_values() {
        assert!(Durability::try_from(0x00).is_err());
        assert!(Durability::try_from(0x04).is_err());
        assert!(Durability::try_from(0xff).is_err());
    }

    #[test]
    fn unknown_durability_preserves_byte() {
        let err = Durability::try_from(0xab).unwrap_err();
        assert_eq!(err.0, 0xab);
    }

    #[test]
    fn repr_values_match_wire() {
        assert_eq!(Durability::Sync as u8, 0x01);
        assert_eq!(Durability::Batched as u8, 0x02);
        assert_eq!(Durability::Buffered as u8, 0x03);
    }
}
