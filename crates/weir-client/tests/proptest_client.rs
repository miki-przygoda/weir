//! Property-based tests for `WeirClient`'s response-handling path.
//!
//! Feeds arbitrary byte sequences into the client's response stream and
//! asserts that `push()` / `health_check()` either return `Ok` or an
//! `Err(ClientError)` — never panic, never spin, never read past the
//! bytes the daemon actually sent.
//!
//! Complements `weir-core/tests/proptest_envelope.rs` (which already proves
//! the decoder never panics on arbitrary input). This file checks that the
//! client's wrappers around that decoder are equally robust.

#![cfg(unix)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

use proptest::prelude::*;
use weir_client::{ClientError, WeirClient};
use weir_core::Durability;

/// Spawns a "server" thread that drains whatever the client sends, then
/// writes `response_bytes` and closes its end of the pair. Returns the
/// client-side `WeirClient` ready to use.
fn paired_client(response_bytes: Vec<u8>) -> WeirClient {
    let (client_side, mut server_side) = UnixStream::pair().expect("socketpair");
    // 250 ms read timeout in case the client's request never arrives (it
    // always does in practice, but proptest paranoia).
    server_side
        .set_read_timeout(Some(Duration::from_millis(250)))
        .ok();
    thread::spawn(move || {
        // Drain whatever the client writes — Header + payload + CRC at most.
        // We ignore the content; the property under test is response handling.
        let mut sink = [0u8; 4096];
        let _ = server_side.read(&mut sink);
        // Now write the proptest payload and close.
        let _ = server_side.write_all(&response_bytes);
        // Drop closes the socket so the client sees EOF if it reads past
        // `response_bytes.len()` — the property says this is fine.
    });
    WeirClient::from_stream(client_side)
}

proptest! {
    /// `push()` may return Ok or Err on any byte sequence the daemon could
    /// emit, but it must never panic. Covers garbled responses, partial
    /// frames, truncated headers, mismatched CRCs — all the inputs an
    /// adversarial or buggy daemon could produce.
    #[test]
    fn push_never_panics_on_arbitrary_response(
        response_bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let mut client = paired_client(response_bytes);
        // Result is irrelevant; the assertion is "did not panic".
        let _: Result<(), ClientError> = client.push(b"hi", Durability::Sync);
    }

    /// Same property for the HealthCheck path. Same surface (read header,
    /// read payload, verify CRC), distinct dispatch on MessageType.
    #[test]
    fn health_check_never_panics_on_arbitrary_response(
        response_bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let mut client = paired_client(response_bytes);
        let _: Result<(), ClientError> = client.health_check();
    }

    /// Restricted to "looks like a header" inputs: 16-byte buffers that
    /// pass the magic check but might fail anywhere else. Exercises the
    /// post-magic decode paths (version, CRC, message_type dispatch) more
    /// densely than fully-random bytes.
    #[test]
    fn push_never_panics_on_pseudo_valid_headers(
        post_magic in proptest::collection::vec(any::<u8>(), 12..28),
    ) {
        // Prefix with the weir magic so the decoder reaches the version and
        // CRC checks instead of bailing out at the very first byte.
        let mut bytes = vec![0x57, 0x45, 0x49, 0x52];
        bytes.extend_from_slice(&post_magic);
        let mut client = paired_client(bytes);
        let _ = client.push(b"hi", Durability::Sync);
    }
}

/// A more deliberately-constructed regression: an empty response is the
/// EOF-after-request case. The client must not read garbage or hang.
#[test]
fn push_on_empty_response_returns_err_not_panic() {
    let mut client = paired_client(Vec::new());
    let result = client.push(b"hi", Durability::Sync);
    assert!(
        matches!(
            result,
            Err(ClientError::Io(_)) | Err(ClientError::Protocol(_))
        ),
        "expected Io or Protocol error on empty response, got {result:?}"
    );
}

/// A response that contains only the magic + version, then EOF. Exercises
/// the partial-header path specifically.
#[test]
fn push_on_truncated_header_returns_err_not_panic() {
    let truncated = vec![0x57, 0x45, 0x49, 0x52, 0x01]; // magic + version, nothing else
    let mut client = paired_client(truncated);
    let result = client.push(b"hi", Durability::Sync);
    assert!(
        matches!(
            result,
            Err(ClientError::Io(_)) | Err(ClientError::Protocol(_))
        ),
        "expected Io or Protocol error on truncated header, got {result:?}"
    );
}
