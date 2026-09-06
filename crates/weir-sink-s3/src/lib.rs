//! S3-API object-storage sink for the [weir] daemon.
//!
//! [weir]: https://github.com/miki-przygoda/weir
#![deny(missing_docs)]
// TEMPORARY — removed in the commit that lands `S3Sink` (Task 8).
// The crate is built bottom-up: `time`, `key`, `framing` and `sigv4` are pure
// modules with no consumer until the sink itself exists, and `dead_code` fires
// on every one of them in the meantime. Landing them un-linted would mean
// either skipping the clippy gate or squashing six tasks into one commit.
// If you are reading this after `S3Sink` exists, delete it — the final gate in
// Task 11 greps for this attribute and fails if it survives.
#![allow(dead_code)]

pub(crate) mod client;
pub(crate) mod creds;
pub(crate) mod framing;
pub(crate) mod key;
pub(crate) mod redact;
pub(crate) mod sigv4;
pub(crate) mod time;

#[doc(hidden)]
/// Internal seams exposed for the vendored SigV4 vector suite
/// (`tests/sigv4_vectors.rs`), which is a separate crate and cannot reach
/// `pub(crate)` items. Not a public API: no stability guarantee, and it may
/// change in a patch release.
pub mod sigv4_test_hooks {
    pub use crate::sigv4::{
        Signed, SigningParams, canonical_request, sign, signature, string_to_sign, uri_encode_path,
    };
}
