//! Fuzz target: `weir_server::wab::format::parse_confirmed`.
//!
//! Feeds arbitrary bytes into the parser for the `.confirmed` sidecar
//! file. The parser is reached at daemon startup when the drain
//! scans the WAB directory and at runtime when the drain writes
//! confirmation records; the bytes on disk are attacker-controlled
//! after a host compromise, so a panic or unbounded-allocation in
//! the parser would translate into a denial-of-service vector.
//!
//! Property under test: the parser never panics on any input. Errors
//! are fine (in fact, expected for most random inputs); panics are
//! not.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = weir_server::wab::format::parse_confirmed(data);
});
