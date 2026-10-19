//! Test-only hooks (feature `test-hooks`). Each is inert unless the process
//! also has `WEIR_TEST_HOOKS=1`.
//!
//! The system tests (spec §16) need faults a real daemon cannot be talked
//! into from outside, such as a shard whose segment-counter scan fails. The
//! module exists only in a `--features test-hooks` build, and that build logs
//! a WARN at startup whether or not the hooks are switched on.

use tracing::warn;

/// Whether the hooks are switched on for this process (`WEIR_TEST_HOOKS=1`).
pub(crate) fn enabled() -> bool {
    std::env::var("WEIR_TEST_HOOKS").as_deref() == Ok("1")
}

/// `WEIR_TEST_FAIL_SHARD_COUNTER_SCAN=1,3`: those shards' flushers fail their
/// segment-counter scan, so they end `Offline(CounterScanFailed)`.
pub(crate) fn fail_counter_scan(shard_id: u16) -> bool {
    enabled()
        && std::env::var("WEIR_TEST_FAIL_SHARD_COUNTER_SCAN")
            .map(|v| {
                v.split(',')
                    .any(|s| s.trim().parse::<u16>() == Ok(shard_id))
            })
            .unwrap_or(false)
}

/// Logs, once at startup, that this binary carries the test hooks.
pub(crate) fn warn_if_compiled_in() {
    warn!(
        active = enabled(),
        "this weir-server was built with the test-hooks feature; it is for system tests only"
    );
}
