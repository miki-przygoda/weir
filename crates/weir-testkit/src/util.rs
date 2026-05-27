//! Test-isolation primitives: cross-test mutex + free TCP port allocator.

use std::net::TcpListener;
use std::sync::OnceLock;

/// A process-wide mutex held by every spawned [`WeirServer`](crate::WeirServer)
/// for its lifetime. Prevents two tests from racing on [`free_port`]
/// allocation (the ask-the-OS-for-port-0 pattern has a brief TOCTOU window
/// that's only safe if one test holds it at a time).
///
/// Held across the lifetime of the handle, not just during `start_impl`,
/// because the WAB / segment / drain threads can still race on temp files
/// if two daemons share the temp-dir parent.
pub fn process_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
}

/// Asks the OS for a free TCP port. The listener is dropped immediately
/// (brief TOCTOU window, but acceptable in tests — far safer than a fixed
/// range that clashes with stale processes from previous runs).
///
/// Callers must hold [`process_lock`] across the bind-to-use window so two
/// concurrent tests can't be handed the same port by the kernel.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .unwrap()
        .port()
}
