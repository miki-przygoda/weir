//! The `wab_max_bytes` hysteresis (spec §5.5). The push path
//! (`socket::connection::over_wab_cap`) and the 5 s poll task call the same
//! function, so the flag clears without traffic, and the two writers compute
//! the same idempotent answer from the same inputs.
//!
//! Once rejecting, keep rejecting until bytes fall below `cap * 9 / 10`, so
//! ingest does not flap at the boundary. `cap == 0` disables the check. The
//! flag is shared daemon-wide: one observer of recovery releases every
//! connection, and one observer of the cap holds them all. A failed WAB scan
//! must NOT call this (the last value stands: fail-closed).
use std::sync::atomic::{AtomicBool, Ordering};

/// The level below which a rejecting cap releases. Divides before
/// multiplying: `cap * 9` overflows above `u64::MAX / 9`, and the rounding
/// loss is at most 9 bytes.
pub(crate) fn low_water(cap: u64) -> u64 {
    cap / 10 * 9
}

/// Applies the hysteresis to `bytes`, stores the result in `rejecting` and
/// returns it.
pub(crate) fn evaluate(cap: u64, bytes: u64, rejecting: &AtomicBool) -> bool {
    if cap == 0 {
        return false;
    }
    let was = rejecting.load(Ordering::Relaxed);
    let now = if was {
        bytes >= low_water(cap)
    } else {
        bytes >= cap
    };
    if now != was {
        rejecting.store(now, Ordering::Relaxed);
    }
    now
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// (cap, flag before, bytes) -> (returned, flag after). The push path and
    /// the poll task both call this, so one table pins both.
    #[test]
    fn hysteresis_table() {
        let cases: &[(u64, bool, u64, bool)] = &[
            (0, false, u64::MAX, false), // cap 0 disables the check
            (1_000, false, 999, false),
            (1_000, false, 1_000, true), // reaching the cap starts rejecting
            (1_000, true, 950, true),    // between low water and cap: still rejecting
            (1_000, true, 900, true),    // low water itself still rejects
            (1_000, true, 899, false),   // below low water: accepting again
            (1_000, false, 950, false),  // below the cap from accepting: stays accepting
            (u64::MAX, false, u64::MAX, true), // no overflow in the low-water arithmetic
        ];
        for &(cap, before, bytes, want) in cases {
            let flag = AtomicBool::new(before);
            assert_eq!(
                evaluate(cap, bytes, &flag),
                want,
                "cap={cap} before={before} bytes={bytes}"
            );
            assert_eq!(
                flag.load(Ordering::Relaxed),
                want,
                "flag after, cap={cap} bytes={bytes}"
            );
        }
    }

    #[test]
    fn low_water_divides_before_multiplying() {
        assert_eq!(low_water(1_000), 900);
        assert_eq!(low_water(u64::MAX), u64::MAX / 10 * 9);
        assert_eq!(low_water(9), 0);
    }
}
