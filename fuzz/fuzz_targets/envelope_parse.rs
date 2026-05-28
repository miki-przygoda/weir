//! Fuzz target: `weir_core::Envelope::decode`.
//!
//! The wire envelope parser sits on the trust boundary — every byte
//! comes from an unauthenticated socket client (modulo SO_PEERCRED).
//! `weir_core` already ships a proptest harness for round-tripping
//! valid envelopes; this target covers the inverse: arbitrary
//! attacker-controlled bytes hitting the decoder.
//!
//! Property under test: `Envelope::decode` never panics on any input.
//! Errors are expected for most random inputs; panics are not.

#![no_main]

use libfuzzer_sys::fuzz_target;
use weir_core::Envelope;

fuzz_target!(|data: &[u8]| {
    let _ = Envelope::decode(data);
});
