//! Unix-domain-socket transport for [`WeirClient`].
//!
//! Only the constructors and the socket-timeout setters live here; everything
//! that does not need a Unix socket is in [`crate::client`].

use std::{io, os::unix::net::UnixStream, path::Path, time::Duration};

use weir_core::Durability;

use crate::{ClientError, WeirClient};

#[cfg(test)]
use crate::client::nack_error;
#[cfg(test)]
use weir_core::{Envelope, Header, MAX_PAYLOAD_HARD_CAP, MessageType, NackReason};

// ── Unix-specific constructors ─────────────────────────────────────────────────

impl WeirClient<UnixStream> {
    /// Opens a connection to the weir daemon's Unix socket at `path`.
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path.as_ref())?;
        Ok(Self {
            stream,
            default_durability: None,
            poisoned: false,
            closed_after_nack: false,
        })
    }

    /// Opens a connection and sets a default durability tier in one step.
    ///
    /// Equivalent to calling [`connect`][Self::connect] then
    /// [`set_default_durability`][Self::set_default_durability].
    pub fn connect_with_default(
        path: impl AsRef<Path>,
        durability: Durability,
    ) -> Result<Self, ClientError> {
        let mut c = Self::connect(path)?;
        c.default_durability = Some(durability);
        Ok(c)
    }

    /// Wraps an already-connected [`UnixStream`]. Useful for callers that
    /// manage their own connection setup (systemd socket activation,
    /// pre-authenticated file descriptors passed from a parent process,
    /// `UnixStream::pair`-based test harnesses).
    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream,
            default_durability: None,
            poisoned: false,
            closed_after_nack: false,
        }
    }

    /// Sets the read timeout on the underlying socket. `None` (the default)
    /// blocks indefinitely.
    ///
    /// **Opt-in availability guard.** By default every method blocks in the
    /// response-read path (inside [`push`][Self::push] /
    /// [`health_check`][Self::health_check]) waiting for the daemon's Ack/Nack, so a
    /// wedged daemon (hung flusher, `SIGSTOP`, half-open connection) would block
    /// a producer's hot path forever. With a read timeout set, a stalled reply
    /// surfaces as a [`ClientError::Io`] timeout instead; the producer can retry
    /// — the record may still have been durably written, which the at-least-once
    /// contract covers. Pick a value comfortably above the daemon's Durable ack
    /// latency under load: the daemon's own `ACK_TIMEOUT` is 30 s, so e.g.
    /// 45–60 s lets the daemon's Nack win rather than racing it.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Sets the write timeout on the underlying socket. `None` (the default)
    /// blocks indefinitely. See [`set_read_timeout`][Self::set_read_timeout] for
    /// the rationale — this bounds a stalled `write_all` of the request frame.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `is_poisoned()` and `!is_recoverable()` are documented as equivalent.
    /// `health_check` broke that: it propagated a write failure with a bare `?`
    /// while `push` poisoned first, so a caller saw a non-recoverable error on a
    /// client that still claimed to be usable — and the stream may be desynced
    /// by a partial frame write.
    #[test]
    fn health_check_write_failure_poisons_like_push_does() {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        // Drop the peer so the very next write is a broken pipe.
        drop(b);
        let mut c = WeirClient::from_stream(a);
        let err = c
            .health_check()
            .expect_err("writing to a closed peer must fail");
        assert!(
            !err.is_recoverable(),
            "an I/O failure is non-recoverable; got {err:?}"
        );
        assert!(
            c.is_poisoned(),
            "the documented invariant is that a non-recoverable error leaves the \
             client poisoned — health_check used to return one without poisoning"
        );
    }

    #[test]
    fn push_default_without_default_errors() {
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(a);
        assert!(matches!(
            c.push_default(b"x").unwrap_err(),
            ClientError::NoDefaultDurability
        ));
    }

    #[test]
    fn set_default_durability_used_by_push_default() {
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        // Buffered, not Durable: Durable (0x01) is the wire byte several
        // other code paths default to, so asserting Durable here would pass
        // even if push_default ignored the stored default and hardcoded
        // Durable. Buffered (0x03) is the only tier that can't leak through
        // by accident, so it's the one that actually proves the stored
        // default is threaded onto the wire.
        c.set_default_durability(Durability::Buffered);

        let reader = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let mut hdr = [0u8; weir_core::HEADER_LEN];
            server_end.read_exact(&mut hdr).unwrap();
            let h = weir_core::Header::decode(&hdr).unwrap();
            let mut rest = vec![0u8; h.payload_len() as usize + 4];
            server_end.read_exact(&mut rest).unwrap();
            // Send back an Ack so push_default can complete.
            let ack = weir_core::Envelope::new(
                weir_core::Header::new(
                    weir_core::MessageType::Ack,
                    weir_core::Durability::Durable,
                    0,
                ),
                vec![],
            )
            .encode();
            server_end.write_all(&ack).unwrap();
            // Byte 6 of the header is the durability byte on the wire.
            (h.durability(), hdr[6])
        });

        c.push_default(b"hello").unwrap();
        let (durability, wire_byte) = reader.join().unwrap();
        assert_eq!(durability, Durability::Buffered);
        assert_eq!(wire_byte, 0x03);
    }

    #[test]
    fn nack_error_surfaces_daemon_version_on_version_mismatch() {
        // Daemon sends `[VersionMismatch (0x02), daemon_wire_version]`.
        let payload = [NackReason::VersionMismatch as u8, 7];
        match nack_error(&payload) {
            ClientError::VersionMismatch { daemon_version } => assert_eq!(daemon_version, 7),
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn nack_error_version_mismatch_without_version_byte_falls_back() {
        // A malformed VersionMismatch Nack with no second byte must not panic;
        // it degrades to the bare reason.
        let payload = [NackReason::VersionMismatch as u8];
        assert!(matches!(
            nack_error(&payload),
            ClientError::Nack(NackReason::VersionMismatch)
        ));
    }

    #[test]
    fn nack_error_other_reasons_unaffected() {
        let payload = [NackReason::PayloadTooLarge as u8];
        assert!(matches!(
            nack_error(&payload),
            ClientError::Nack(NackReason::PayloadTooLarge)
        ));
    }

    // ── Bug #1: oversized payloads must surface as PayloadTooLarge, not broken-pipe ──

    #[test]
    fn push_rejects_over_hard_cap_locally() {
        // A payload above the protocol hard cap is rejected locally, before any
        // bytes hit the wire — no round-trip, and the connection stays usable.
        let (client_end, _server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let oversized = vec![0u8; MAX_PAYLOAD_HARD_CAP + 1];
        let err = c.push(&oversized, Durability::Durable).unwrap_err();
        assert!(
            matches!(err, ClientError::PayloadTooLarge { len, limit }
                if len == MAX_PAYLOAD_HARD_CAP + 1 && limit == MAX_PAYLOAD_HARD_CAP),
            "expected a local PayloadTooLarge, got {err:?}"
        );
        assert!(
            !c.poisoned,
            "a local rejection must not poison the connection"
        );
    }

    #[test]
    fn push_surfaces_payload_too_large_nack_when_server_closes_mid_write() {
        // Mirrors the daemon's over-configured-cap path: read only the header, send
        // Nack(PayloadTooLarge), then close without draining the payload. Before the
        // fix the client's large write hit the closed socket and returned a bare
        // broken-pipe, hiding the Nack. Now the Nack is read from the receive buffer
        // and surfaced — whether the write fails partway or just-barely succeeds.
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);

        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let mut hdr = [0u8; weir_core::HEADER_LEN];
            server_end.read_exact(&mut hdr).unwrap();
            let nack = weir_core::Envelope::new(
                weir_core::Header::new(
                    weir_core::MessageType::Nack,
                    weir_core::Durability::Durable,
                    0,
                ),
                vec![NackReason::PayloadTooLarge as u8],
            )
            .encode();
            server_end.write_all(&nack).unwrap();
            // drop server_end here → connection closes without reading the payload.
        });

        // 2 MiB: above any default socket buffer (so the write blocks then fails
        // once the server stops reading and closes), but under the hard cap (so it
        // is not caught by the local pre-check).
        let payload = vec![0u8; 2 * 1024 * 1024];
        let err = c.push(&payload, Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(
            matches!(err, ClientError::Nack(NackReason::PayloadTooLarge)),
            "expected Nack(PayloadTooLarge) surfaced from the buffered reply, got {err:?}"
        );
    }

    // ── Bug #2: a connection-closing Nack must give a clear next-call error ──────

    // Reads one full frame (header + payload + CRC) off a server-side stream.
    fn drain_one_frame(s: &mut std::os::unix::net::UnixStream) {
        use std::io::Read;
        let mut hdr = [0u8; weir_core::HEADER_LEN];
        s.read_exact(&mut hdr).unwrap();
        let n = weir_core::Header::decode(&hdr).unwrap().payload_len() as usize + 4;
        let mut rest = vec![0u8; n];
        s.read_exact(&mut rest).unwrap();
    }

    fn nack_frame(reason: NackReason) -> Vec<u8> {
        weir_core::Envelope::new(
            weir_core::Header::new(
                weir_core::MessageType::Nack,
                weir_core::Durability::Durable,
                0,
            ),
            vec![reason as u8],
        )
        .encode()
    }

    #[test]
    fn closing_nack_makes_next_call_fail_with_a_clear_reconnect_error() {
        // The daemon closes the connection after a validation Nack. Before the fix,
        // the next push hit the dead socket and returned a bare broken-pipe; now it
        // fails fast with a clear "reconnect" error.
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            server_end
                .write_all(&nack_frame(NackReason::EmptyPayload))
                .unwrap();
            // drop server_end → connection closes, as the daemon does after a Nack.
        });
        let first = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(
            matches!(first, ClientError::Nack(NackReason::EmptyPayload)),
            "first push should surface the real reason, got {first:?}"
        );
        // Second call must NOT be a broken-pipe — it must clearly say reconnect.
        match c.push(b"y", Durability::Durable).unwrap_err() {
            ClientError::Protocol(msg) => assert!(
                msg.contains("closed by the daemon after a Nack") && msg.contains("reconnect"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected a clear reconnect Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn internal_error_nack_keeps_connection_usable() {
        // InternalError is transient — the daemon keeps the connection open — so the
        // client must NOT mark it closed, and a retry on the same connection works.
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            server_end
                .write_all(&nack_frame(NackReason::InternalError))
                .unwrap();
            drain_one_frame(&mut server_end); // the retry
            let ack = weir_core::Envelope::new(
                weir_core::Header::new(
                    weir_core::MessageType::Ack,
                    weir_core::Durability::Durable,
                    0,
                ),
                vec![],
            )
            .encode();
            server_end.write_all(&ack).unwrap();
        });
        let first = c.push(b"a", Durability::Durable).unwrap_err();
        assert!(
            matches!(first, ClientError::Nack(NackReason::InternalError)),
            "got {first:?}"
        );
        assert!(
            !c.closed_after_nack,
            "InternalError is transient and must not close the connection"
        );
        c.push(b"b", Durability::Durable).unwrap(); // retry on the same connection succeeds
        server.join().unwrap();
    }

    // ── Hostile / malformed daemon responses: refuse + poison, never false-ack ──

    /// F44: a daemon-declared response payload_len above the hard cap must be
    /// refused BEFORE allocating ~4 GiB, and poison the connection.
    #[test]
    fn response_oversized_payload_len_refuses_to_allocate_and_poisons() {
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            // A valid 16-byte header whose payload_len is CAP+1, with a recomputed
            // header CRC; then EOF (no payload) — the client must refuse before it
            // would try to read/allocate the declared payload.
            let mut hdr = Header::new(MessageType::Ack, Durability::Durable, 0).encode();
            let big = (MAX_PAYLOAD_HARD_CAP as u32).wrapping_add(1);
            hdr[8..12].copy_from_slice(&big.to_le_bytes());
            let crc = crc32fast::hash(&hdr[0..12]);
            hdr[12..16].copy_from_slice(&crc.to_le_bytes());
            server_end.write_all(&hdr).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        match err {
            ClientError::Protocol(msg) => assert!(
                msg.contains("refusing to allocate"),
                "expected the cap refusal, got: {msg}"
            ),
            other => panic!("expected a Protocol cap refusal, got {other:?}"),
        }
        assert!(
            c.is_poisoned(),
            "an oversized response must poison the client"
        );
    }

    /// An empty Nack payload is a stream desync: `nack_error` reports `Protocol`,
    /// and the connection must be poisoned so a stale frame can't be mis-read as a
    /// later ack (a false ack).
    #[test]
    fn empty_nack_payload_is_protocol_desync_and_poisons() {
        // (1) the helper maps an empty payload to a Protocol error.
        match nack_error(&[]) {
            ClientError::Protocol(msg) => assert!(msg.contains("empty payload"), "{msg}"),
            other => panic!("expected Protocol for empty Nack, got {other:?}"),
        }
        // (2) end-to-end: an empty-payload Nack poisons + is not recoverable.
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            let frame = Envelope::new(
                Header::new(MessageType::Nack, Durability::Durable, 0),
                vec![],
            )
            .encode();
            server_end.write_all(&frame).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(matches!(err, ClientError::Protocol(_)), "got {err:?}");
        assert!(
            !err.is_recoverable(),
            "a desync Protocol error is not recoverable"
        );
        assert!(
            c.is_poisoned(),
            "an empty-Nack desync must poison the client"
        );
    }

    /// A corrupted response payload CRC is a protocol violation: surface it and
    /// poison (the stream tail is now untrustworthy).
    #[test]
    fn response_payload_crc_mismatch_poisons() {
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            // A Nack frame with a flipped trailing CRC byte.
            let mut frame = nack_frame(NackReason::InternalError);
            *frame.last_mut().unwrap() ^= 0xff;
            server_end.write_all(&frame).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        match err {
            ClientError::Protocol(msg) => assert!(msg.contains("CRC mismatch"), "{msg}"),
            other => panic!("expected a CRC-mismatch Protocol error, got {other:?}"),
        }
        assert!(
            c.is_poisoned(),
            "a CRC-mismatched response must poison the client"
        );
    }

    /// An unrecognised Nack reason byte (reserved range) surfaces as
    /// `UnknownNack(byte)` and closes the connection.
    #[test]
    fn unknown_nack_reason_byte_surfaces_unknown_nack() {
        assert!(matches!(
            nack_error(&[0x0A]),
            ClientError::UnknownNack(0x0A)
        ));

        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            let frame = Envelope::new(
                Header::new(MessageType::Nack, Durability::Durable, 0),
                vec![0x0A],
            )
            .encode();
            server_end.write_all(&frame).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(matches!(err, ClientError::UnknownNack(0x0A)), "got {err:?}");
        assert!(
            c.is_poisoned(),
            "an unknown closing Nack must mark the client unusable"
        );
    }

    /// A VersionMismatch Nack surfaces the daemon's version through push() and is
    /// non-recoverable + connection-closing.
    #[test]
    fn version_mismatch_surfaces_through_push() {
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            let frame = Envelope::new(
                Header::new(MessageType::Nack, Durability::Durable, 0),
                vec![NackReason::VersionMismatch as u8, 9],
            )
            .encode();
            server_end.write_all(&frame).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(
            matches!(err, ClientError::VersionMismatch { daemon_version: 9 }),
            "got {err:?}"
        );
        assert!(!err.is_recoverable());
        assert!(c.is_poisoned());
    }

    /// health_check() against a daemon that replies with the wrong frame type is a
    /// desync: Protocol error + poison + non-recoverable.
    #[test]
    fn health_check_unexpected_frame_type_poisons() {
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            // Reply to a HealthCheck with an Ack — never expected here.
            let frame = Envelope::new(
                Header::new(MessageType::Ack, Durability::Durable, 0),
                vec![],
            )
            .encode();
            server_end.write_all(&frame).unwrap();
        });
        let err = c.health_check().unwrap_err();
        server.join().unwrap();
        match &err {
            ClientError::Protocol(msg) => {
                assert!(msg.contains("expected HealthCheckResponse"), "{msg}")
            }
            other => panic!("expected a Protocol desync, got {other:?}"),
        }
        assert!(!err.is_recoverable());
        assert!(c.is_poisoned());
    }

    // ── F43: opt-in socket timeouts ───────────────────────────────────────────

    #[test]
    fn set_read_timeout_applies_to_socket() {
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let c = WeirClient::from_stream(a);
        c.set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        assert_eq!(
            c.stream.read_timeout().unwrap(),
            Some(Duration::from_millis(250))
        );
        // None clears it back to blocking.
        c.set_read_timeout(None).unwrap();
        assert_eq!(c.stream.read_timeout().unwrap(), None);
    }

    #[test]
    fn read_timeout_bounds_a_silent_daemon_instead_of_blocking_forever() {
        // `_b` is the daemon end: it's held open (so the connection stays up)
        // but never replies. Without a read timeout, push would block forever.
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(a);
        c.set_read_timeout(Some(Duration::from_millis(150)))
            .unwrap();
        // push writes the (tiny) frame, then blocks reading the reply that never
        // comes → the timeout fires → an Io error rather than an indefinite hang.
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        assert!(
            matches!(err, ClientError::Io(_)),
            "expected Io timeout, got {err:?}"
        );
    }

    #[test]
    fn client_is_poisoned_after_a_read_failure() {
        // G04: after a response read fails (here a timeout), the stream may hold
        // leftover bytes that a subsequent read would mis-attribute to the next
        // request — a false ack. The client must poison itself and reject further
        // use fast, instead of reading again.
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(a);
        c.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();

        // First push: the peer never replies → read times out → Io error.
        let first = c.push(b"x", Durability::Durable).unwrap_err();
        assert!(matches!(first, ClientError::Io(_)), "{first:?}");

        // Second push must fail FAST as poisoned, not block on another read.
        let started = std::time::Instant::now();
        let second = c.push(b"y", Durability::Durable).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "poisoned client must reject immediately, not read again"
        );
        match second {
            ClientError::Protocol(msg) => assert!(msg.contains("poisoned"), "{msg}"),
            other => panic!("expected a poisoned Protocol error, got {other:?}"),
        }
    }

    // ── is_poisoned / is_recoverable helpers ─────────────────────────────────

    #[test]
    fn is_poisoned_flips_after_a_read_failure() {
        // A fresh client is not poisoned; after a response-read failure (here a
        // timeout) the flag flips, mirroring `client_is_poisoned_after_a_read_failure`.
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(a);
        assert!(!c.is_poisoned(), "a fresh client must not be poisoned");

        c.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        // Peer never replies → the response read times out → the client poisons.
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        assert!(matches!(err, ClientError::Io(_)), "{err:?}");
        assert!(
            c.is_poisoned(),
            "a read failure must flip is_poisoned() to true"
        );
        // And the poisoning error itself is non-recoverable.
        assert!(!err.is_recoverable(), "an Io failure is not recoverable");
    }

    #[test]
    fn is_poisoned_after_a_connection_closing_nack() {
        // A connection-closing Nack (every reason except InternalError) leaves the
        // connection dead. `is_poisoned()` must report that — it reflects
        // `closed_after_nack`, not just `poisoned` — and the connection must be
        // refused by `ensure_usable` on the next call. (Models the closing-nack
        // test `closing_nack_makes_next_call_fail_with_a_clear_reconnect_error`.)
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        assert!(!c.is_poisoned(), "a fresh client must not be poisoned");
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            server_end
                .write_all(&nack_frame(NackReason::EmptyPayload))
                .unwrap();
            // drop server_end → connection closes, as the daemon does after a Nack.
        });
        let first = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        assert!(
            matches!(first, ClientError::Nack(NackReason::EmptyPayload)),
            "expected the real Nack reason, got {first:?}"
        );
        // The closing Nack is non-recoverable, and the client reports poisoned.
        assert!(
            !first.is_recoverable(),
            "a connection-closing Nack is non-recoverable"
        );
        assert!(
            c.is_poisoned(),
            "a connection-closing Nack must flip is_poisoned() to true"
        );
        // And the connection is refused fast on the next attempt.
        assert!(
            matches!(c.ensure_usable(), Err(ClientError::Protocol(_))),
            "ensure_usable must reject a closed-after-nack connection"
        );
    }

    #[test]
    fn is_poisoned_and_is_recoverable_agree_after_a_desync() {
        // A stream desync — the daemon sends an unexpected message type where an
        // Ack/Nack was expected — poisons the connection: leftover/unexpected bytes
        // could be mis-read as a later reply (a false ack). `is_poisoned()` must be
        // true AND the returned error must be non-recoverable, i.e. the two agree.
        let (client_end, mut server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        assert!(!c.is_poisoned(), "a fresh client must not be poisoned");
        let server = std::thread::spawn(move || {
            use std::io::Write;
            drain_one_frame(&mut server_end);
            // A HealthCheck frame is a valid frame, but it is NOT a reply push expects
            // (push expects Ack or Nack) — a genuine stream desync.
            let bogus = weir_core::Envelope::new(
                weir_core::Header::new(
                    weir_core::MessageType::HealthCheck,
                    weir_core::Durability::Durable,
                    0,
                ),
                vec![],
            )
            .encode();
            server_end.write_all(&bogus).unwrap();
        });
        let err = c.push(b"x", Durability::Durable).unwrap_err();
        server.join().unwrap();
        // The error is a Protocol desync, which is non-recoverable...
        assert!(
            matches!(err, ClientError::Protocol(_)),
            "expected a Protocol desync error, got {err:?}"
        );
        assert!(!err.is_recoverable(), "a stream desync is non-recoverable");
        // ...and is_poisoned() agrees (the two must never disagree).
        assert!(
            c.is_poisoned(),
            "a stream desync must flip is_poisoned() to true"
        );
        assert_eq!(
            c.is_poisoned(),
            !err.is_recoverable(),
            "is_poisoned() and !is_recoverable() must agree"
        );
    }

    #[test]
    fn is_recoverable_true_for_local_over_cap_rejection() {
        // The local pre-send PayloadTooLarge rejection does not touch the socket, so
        // it is recoverable and must not poison the client.
        let (client_end, _server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let oversized = vec![0u8; MAX_PAYLOAD_HARD_CAP + 1];
        let err = c.push(&oversized, Durability::Durable).unwrap_err();
        assert!(
            matches!(err, ClientError::PayloadTooLarge { .. }),
            "expected a local PayloadTooLarge, got {err:?}"
        );
        assert!(
            err.is_recoverable(),
            "a local over-cap rejection keeps the connection usable"
        );
        assert!(!c.is_poisoned(), "a recoverable error must not poison");
    }

    #[test]
    fn push_rejects_empty_payload_locally_without_poisoning() {
        // An empty payload is the WAB end-of-records sentinel; the daemon would
        // Nack+close it. The local guard rejects it before any bytes are sent, so
        // the connection stays usable (recoverable, not poisoned) — mirroring the
        // over-cap guard.
        let (client_end, _server_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut c = WeirClient::from_stream(client_end);
        let err = c.push(b"", Durability::Durable).unwrap_err();
        assert!(
            matches!(err, ClientError::EmptyPayload),
            "expected a local EmptyPayload, got {err:?}"
        );
        assert!(
            err.is_recoverable(),
            "a local empty-payload rejection keeps the connection usable"
        );
        assert!(!c.is_poisoned(), "a recoverable error must not poison");
    }

    #[test]
    fn is_recoverable_true_for_no_default_durability() {
        // A local misconfiguration returned before any I/O is recoverable.
        let err = ClientError::NoDefaultDurability;
        assert!(err.is_recoverable());
    }

    #[test]
    fn is_recoverable_true_for_internal_error_nack() {
        // InternalError is the one Nack the daemon keeps the connection open for.
        let err = ClientError::Nack(NackReason::InternalError);
        assert!(
            err.is_recoverable(),
            "a transient InternalError Nack keeps the connection usable"
        );
    }

    #[test]
    fn is_recoverable_false_for_connection_closing_nacks() {
        // Every Nack reason except InternalError closes the connection, so it is
        // non-recoverable. VersionMismatch and UnknownNack are Nacks too.
        for err in [
            ClientError::Nack(NackReason::EmptyPayload),
            ClientError::Nack(NackReason::PayloadTooLarge),
            ClientError::Nack(NackReason::UnknownMessage),
            ClientError::VersionMismatch { daemon_version: 7 },
            ClientError::UnknownNack(0xff),
        ] {
            assert!(
                !err.is_recoverable(),
                "{err:?} closes the connection and must be non-recoverable"
            );
        }
    }

    #[test]
    fn is_recoverable_false_for_io_and_protocol() {
        // A socket failure poisons; a protocol violation poisons or IS the
        // reconnect guard — both non-recoverable.
        let io = ClientError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "pipe"));
        assert!(!io.is_recoverable());
        let proto = ClientError::Protocol("connection poisoned".into());
        assert!(!proto.is_recoverable());
    }
}
