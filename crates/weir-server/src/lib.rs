//! Library facade for `weir-server`.
//!
//! The daemon's runtime lives in `main.rs`. This file exposes a
//! minimal surface so external crates inside the same workspace
//! (the `fuzz/` crate, primarily) can call into the WAB on-disk
//! format parsers without going through a binary boundary.
//!
//! Production code does not depend on this file: `main.rs` declares
//! the same modules as private siblings (`mod config; mod wab; …`),
//! and the binary build compiles independently of the library
//! build.
//!
//! The surface is deliberately narrow — only the WAB format module,
//! re-exported from the `weir-wab` crate (the single source of truth
//! for the on-disk format). Exposing wider modules (sink, drain,
//! socket) would surface visibility-leak warnings because their
//! `pub fn` signatures reference `pub(crate)` types like `Metrics`;
//! the fuzz harness has no need for that surface anyway.

/// WAB on-disk format parsers. Re-exports `weir_wab::format` — the same
/// module the daemon reaches internally via `crate::wab::format` — so the
/// fuzz harness can call `weir_server::wab::format::parse_confirmed`.
pub mod wab {
    pub use weir_wab::format;
}
