//! Unix-nanosecond → UTC calendar-field conversion.
//!
//! Hand-rolled because the workspace has no date library in its production
//! dependency tree — `time` is a dev-dependency, reachable only through `rcgen`
//! in `weir-testkit` — and this crate's dependency budget is spent on `hmac`.
//!
//! The algorithm is Howard Hinnant's `civil_from_days`, which is public domain
//! and exact for the proleptic Gregorian calendar across the whole span an
//! `i64` nanosecond count can express (1677-09-21 .. 2262-04-11).

/// UTC calendar fields for an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Utc {
    pub(crate) year: i64,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
}

/// Days since 1970-01-01 → `(year, month, day)`.
///
/// Howard Hinnant, "chrono-Compatible Low-Level Date Algorithms". The
/// `if z >= 0 { z } else { z - 146_096 }` emulates floor division under Rust's
/// truncating `/`, which is what makes negative (pre-epoch) inputs correct.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Converts unix nanoseconds to UTC calendar fields.
///
/// Uses Euclidean division so pre-epoch (negative) instants floor rather than
/// truncate toward zero — truncation would place 1969-12-31T23:59:59Z on
/// 1970-01-01.
pub(crate) fn utc_from_unix_nanos(nanos: i64) -> Utc {
    let secs = nanos.div_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Utc {
        year,
        month,
        day,
        hour: (sod / 3_600) as u32,
        minute: ((sod % 3_600) / 60) as u32,
        second: (sod % 60) as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_1970_01_01() {
        let t = utc_from_unix_nanos(0);
        assert_eq!((t.year, t.month, t.day), (1970, 1, 1));
        assert_eq!((t.hour, t.minute, t.second), (0, 0, 0));
    }

    #[test]
    fn a_known_instant_decodes_correctly() {
        // 2015-08-30T12:36:00Z — the instant the AWS SigV4 test suite signs at,
        // so this doubles as a cross-check for the x-amz-date header.
        let t = utc_from_unix_nanos(1_440_938_160 * 1_000_000_000);
        assert_eq!((t.year, t.month, t.day), (2015, 8, 30));
        assert_eq!((t.hour, t.minute, t.second), (12, 36, 0));
    }

    #[test]
    fn leap_day_decodes_correctly() {
        // 2024-02-29T23:59:59Z. A civil-date bug almost always shows up here.
        let t = utc_from_unix_nanos(1_709_251_199 * 1_000_000_000);
        assert_eq!((t.year, t.month, t.day), (2024, 2, 29));
        assert_eq!((t.hour, t.minute, t.second), (23, 59, 59));
    }

    #[test]
    fn the_gregorian_century_rules_are_applied() {
        // 2000 WAS a leap year (divisible by 400); 1900 was not (divisible by
        // 100 but not 400). Both rules live in the /100 and /400 terms.
        let t = utc_from_unix_nanos(951_782_400 * 1_000_000_000);
        assert_eq!((t.year, t.month, t.day), (2000, 2, 29));
        // 1900-03-01T00:00:00Z — the day after the leap day 1900 did not have.
        let t = utc_from_unix_nanos(-2_203_891_200 * 1_000_000_000);
        assert_eq!((t.year, t.month, t.day), (1900, 3, 1));
    }

    #[test]
    fn pre_epoch_instants_floor_rather_than_truncate() {
        // Euclidean division is load-bearing: truncating division would give
        // days=0, sod=-1 and place this on 1970-01-01.
        let t = utc_from_unix_nanos(-1_000_000_000);
        assert_eq!((t.year, t.month, t.day), (1969, 12, 31));
        assert_eq!((t.hour, t.minute, t.second), (23, 59, 59));
    }

    #[test]
    fn the_extremes_of_i64_nanoseconds_do_not_panic_or_wrap() {
        // i64 nanos bound the reachable span to 1677..=2262, which is why
        // amz_date's [..8] slice is always safe.
        let lo = utc_from_unix_nanos(i64::MIN);
        let hi = utc_from_unix_nanos(i64::MAX);
        assert_eq!(lo.year, 1677);
        assert_eq!(hi.year, 2262);
    }
}
