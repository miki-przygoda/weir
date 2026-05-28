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
//! The surface is deliberately narrow — only the WAB format module
//! and its closures of pure-byte parsers. Exposing wider modules
//! (sink, drain, socket) would surface visibility-leak warnings
//! because their `pub fn` signatures reference `pub(crate)` types
//! like `Metrics`; the fuzz harness has no need for that surface
//! anyway.

/// WAB on-disk format parsers. Surface mirrors the underlying module
/// (`src/wab/format.rs`). Only the parsing entry points are reachable
/// here; segment / recovery state machines stay private.
pub mod wab {
    pub mod format {
        pub use crate::wab_format::*;
    }
}

// Internal re-export — the file is shared with main.rs's `mod wab; mod
// format;` chain, so the actual source is `src/wab/format.rs` declared
// via this `#[path]` attribute.
#[path = "wab/format.rs"]
mod wab_format;
