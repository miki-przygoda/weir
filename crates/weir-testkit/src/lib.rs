//! Test harness for `weir-server` integration tests.
//!
//! Spawns the daemon as a real child process, manages temp directories,
//! and exposes lifecycle helpers (graceful + ungraceful shutdown, restart,
//! metrics scraping). Used by `weir-server`'s `tests/system.rs` and
//! `tests/load.rs`.
//!
//! # Quick start
//!
//! ```ignore
//! use weir_testkit::WeirServer;
//!
//! let srv = WeirServer::builder("my_test").start();
//! let mut client = srv.client();
//! client.push(b"hello", weir_core::Durability::Sync).unwrap();
//! // Drop cleans up the child process and temp directory.
//! ```
//!
//! # Test isolation
//!
//! Every spawned server holds a process-wide [`process_lock`] mutex for its
//! lifetime so two tests can't race on [`free_port`] allocation. Tests
//! within one test binary therefore serialise — `cargo test` runs them on
//! one thread effectively.

mod harness;
mod util;

pub use harness::{WeirServer, WeirServerBuilder};
pub use util::{free_port, process_lock};

/// Convenience: starts a [`WeirServerBuilder`] with the `weir-server` binary
/// path already wired up via `env!("CARGO_BIN_EXE_weir-server")`. Only usable
/// from inside an integration test of the `weir-server` crate, where Cargo
/// sets that env var.
///
/// ```ignore
/// let srv = weir_server!("smoke").shard_count(2).start();
/// ```
#[macro_export]
macro_rules! weir_server {
    ($tag:expr) => {
        $crate::WeirServer::builder($tag).binary_path(env!("CARGO_BIN_EXE_weir-server"))
    };
}
