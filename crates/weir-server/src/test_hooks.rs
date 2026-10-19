//! Fault hooks for the system tests (Foundations §16).
//!
//! Two layers of opt-in: the `test-hooks` Cargo feature (absent from every
//! release build) and the `WEIR_TEST_HOOKS=1` environment variable. With the
//! feature off, every function here is an inlined no-op, so production code
//! calls them unconditionally and pays nothing.
//!
//! The system tests need faults a real daemon cannot be talked into from
//! outside: a shard whose segment-counter scan fails, or a recovery phase
//! long enough to observe. A build with the feature logs a WARN at startup
//! whether or not the hooks are armed.

use std::time::Duration;

/// `WEIR_TEST_HOOKS` value that arms the hooks.
const ARMED: &str = "1";

/// Pure parser behind [`startup_delay`]; separate so the tests need no
/// process-global environment mutation.
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn parse_delay(armed: Option<&str>, ms: Option<&str>) -> Option<Duration> {
    if armed != Some(ARMED) {
        return None;
    }
    ms?.trim().parse::<u64>().ok().map(Duration::from_millis)
}

/// Pure parser behind [`fail_counter_scan`].
#[cfg_attr(not(any(test, feature = "test-hooks")), allow(dead_code))]
fn parse_shard_list(armed: Option<&str>, list: Option<&str>, shard_id: u16) -> bool {
    armed == Some(ARMED)
        && list.is_some_and(|l| {
            l.split(',')
                .filter_map(|s| s.trim().parse::<u16>().ok())
                .any(|s| s == shard_id)
        })
}

#[cfg(feature = "test-hooks")]
fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// True when the hooks are compiled in AND armed with `WEIR_TEST_HOOKS=1`.
#[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
#[inline]
pub(crate) fn enabled() -> bool {
    #[cfg(feature = "test-hooks")]
    {
        env("WEIR_TEST_HOOKS").as_deref() == Some(ARMED)
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        false
    }
}

/// Logs, once at startup, that this binary carries the hooks.
#[inline]
pub(crate) fn warn_if_compiled_in() {
    #[cfg(feature = "test-hooks")]
    tracing::warn!(
        armed = enabled(),
        "THIS BINARY CONTAINS TEST HOOKS (built with --features test-hooks); never run it \
         in production"
    );
}

/// `WEIR_TEST_STARTUP_DELAY_MS`: how long main sleeps at the start of the
/// `recovering` phase, so a test can observe it.
#[inline]
pub(crate) fn startup_delay() -> Option<Duration> {
    #[cfg(feature = "test-hooks")]
    {
        parse_delay(
            env("WEIR_TEST_HOOKS").as_deref(),
            env("WEIR_TEST_STARTUP_DELAY_MS").as_deref(),
        )
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        None
    }
}

/// `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN=1,3`: those shards' flushers fail their
/// segment-counter scan every attempt, so they end `Offline(CounterScanFailed)`
/// exactly as a persistent `read_dir` failure would.
#[inline]
pub(crate) fn fail_counter_scan(shard_id: u16) -> bool {
    #[cfg(feature = "test-hooks")]
    {
        parse_shard_list(
            env("WEIR_TEST_HOOKS").as_deref(),
            env("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN").as_deref(),
            shard_id,
        )
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = shard_id;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn delay_needs_the_master_switch_and_a_number() {
        assert_eq!(
            parse_delay(Some("1"), Some("250")),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_delay(None, Some("250")), None, "not armed");
        assert_eq!(
            parse_delay(Some("0"), Some("250")),
            None,
            "armed means exactly 1"
        );
        assert_eq!(
            parse_delay(Some("1"), Some("soon")),
            None,
            "garbage is ignored"
        );
        assert_eq!(parse_delay(Some("1"), None), None);
    }

    #[test]
    fn counter_scan_list_matches_exact_shard_ids() {
        assert!(parse_shard_list(Some("1"), Some("0"), 0));
        assert!(parse_shard_list(Some("1"), Some("3, 7"), 7));
        assert!(!parse_shard_list(Some("1"), Some("3,7"), 1));
        assert!(!parse_shard_list(None, Some("0"), 0), "not armed");
        assert!(!parse_shard_list(Some("1"), Some("x"), 0));
    }
}
