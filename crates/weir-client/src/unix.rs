use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::Duration,
};

use weir_core::{
    Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType, NackReason,
};

/// All errors that can be returned by [`WeirClient`] methods.
///
/// `#[non_exhaustive]`: the client error set may grow post-1.0 (it already gained
/// `VersionMismatch` during the 1.0 hardening), so downstream matches must carry
/// a wildcard arm and adding a variant later is not a breaking change.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// An I/O error on the underlying socket.
    Io(io::Error),
    /// The daemon sent a Nack with a recognised reason.
    Nack(NackReason),
    /// The daemon rejected the frame because its wire version differs from the
    /// client's. Carries the daemon's `WIRE_VERSION` (the second Nack-payload
    /// byte) so the caller can report both sides and decide whether to upgrade
    /// the daemon or downgrade the client.
    VersionMismatch {
        /// The wire-protocol version the daemon speaks.
        daemon_version: u8,
    },
    /// The daemon sent a Nack with an unrecognised reason byte.
    UnknownNack(u8),
    /// The payload exceeds the protocol hard cap and was rejected locally,
    /// before any bytes were sent. The daemon would always reject a payload this
    /// large (and a large frame can race the daemon's Nack-and-close, surfacing as
    /// a bare broken-pipe), so the client rejects it up front with no round-trip.
    /// A deployment's configured `max_payload_bytes` may be lower than this cap; a
    /// payload over that lower limit is reported by the daemon as
    /// [`ClientError::Nack`]`(`[`NackReason::PayloadTooLarge`]`)`.
    PayloadTooLarge {
        /// The rejected payload's length in bytes.
        len: usize,
        /// The protocol hard cap ([`weir_core::MAX_PAYLOAD_HARD_CAP`]).
        limit: usize,
    },
    /// The daemon's response violated the wire protocol.
    Protocol(String),
    /// `push_default` was called but no default durability was set.
    NoDefaultDurability,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "socket I/O error: {e}"),
            Self::Nack(r) => write!(f, "server nack: {r:?}"),
            Self::VersionMismatch { daemon_version } => write!(
                f,
                "wire version mismatch: daemon speaks v{daemon_version}, this client speaks v{}",
                weir_core::WIRE_VERSION
            ),
            Self::UnknownNack(b) => write!(f, "server nack with unknown reason {b:#04x}"),
            Self::PayloadTooLarge { len, limit } => write!(
                f,
                "payload too large: {len} bytes exceeds the {limit}-byte protocol hard cap"
            ),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::NoDefaultDurability => {
                write!(f, "push_default called but no default durability was set")
            }
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// A synchronous blocking client for one connection to the weir daemon.
///
/// The type parameter `S` is the underlying transport. The default is
/// [`UnixStream`] (Unix domain socket). When the `tls` feature is enabled,
/// `S` can also be `TlsStream` (TCP + mutual TLS).
///
/// The underlying stream runs in blocking mode. Each method sends one request
/// frame and reads one response frame before returning. Requests are not
/// pipelined; for concurrent producers, create one `WeirClient` per thread.
///
/// Drop the client to close the connection. The daemon treats EOF as a clean
/// disconnect.
pub struct WeirClient<S = UnixStream> {
    // pub(crate) so the TLS connector (a sibling module) can reach the
    // underlying TcpStream to apply socket timeouts (F43).
    pub(crate) stream: S,
    default_durability: Option<Durability>,
    /// Set once a response read fails (a timeout mid-frame, a partial/aborted
    /// read, or a protocol violation). The stream is then in an indeterminate
    /// state — leftover bytes from the aborted response could be mis-read as the
    /// NEXT request's reply, acking a record the daemon never confirmed (a false
    /// ack). Once poisoned, every method fails fast; the caller must reconnect
    /// (G04).
    poisoned: bool,
    /// Set once the daemon Nacked a frame for a reason that closes the connection
    /// (every Nack except the transient `InternalError`). The connection is then
    /// cleanly dead — the next call would otherwise just hit a broken pipe — so we
    /// fail fast with a clear "reconnect" error instead (QA#2).
    closed_after_nack: bool,
}

// Manual Debug (not derived): a `#[derive(Debug)]` would require `S: Debug`, which
// excludes `WeirClient<TlsStream>` (rustls's `StreamOwned` is not `Debug`). Print
// the non-secret fields and omit the transport so every client type — Unix and
// TLS — implements `Debug` (C-DEBUG, S40).
impl<S> std::fmt::Debug for WeirClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeirClient")
            .field("default_durability", &self.default_durability)
            .field("poisoned", &self.poisoned)
            .field("closed_after_nack", &self.closed_after_nack)
            .finish_non_exhaustive()
    }
}

// ── Shared methods over any blocking Read+Write transport ──────────────────────

impl<S: Read + Write> WeirClient<S> {
    /// Sets the default durability tier used by [`push_default`][Self::push_default].
    pub fn set_default_durability(&mut self, durability: Durability) {
        self.default_durability = Some(durability);
    }

    /// Pushes `payload` to the daemon with the given `durability` tier.
    ///
    /// Blocks until the daemon replies. On `Ack` the record is durably stored
    /// according to the requested tier. On `Nack` returns the reason as
    /// [`ClientError::Nack`].
    pub fn push(
        &mut self,
        payload: impl AsRef<[u8]>,
        durability: Durability,
    ) -> Result<(), ClientError> {
        self.ensure_usable()?;
        let bytes = payload.as_ref();
        // Local guard against the protocol hard cap. The daemon always rejects a
        // payload this large, and for a big frame it Nacks then closes the
        // connection before we finish streaming — so the Nack races our write and
        // would surface as a bare broken-pipe. Reject up front, no round-trip, with
        // a clear typed error. (A deployment's configured cap may be lower than the
        // hard cap; a payload over that lower limit still goes to the daemon and is
        // handled by the write-error recovery below.)
        if bytes.len() > MAX_PAYLOAD_HARD_CAP {
            return Err(ClientError::PayloadTooLarge {
                len: bytes.len(),
                limit: MAX_PAYLOAD_HARD_CAP,
            });
        }
        // copy_from_slice at the API boundary so downstream handling is zero-copy.
        let payload = weir_core::Payload::copy_from_slice(bytes);
        let header = Header::new(MessageType::Push, durability, 0);
        let frame = Envelope::new(header, payload).encode();

        if let Err(write_err) = self.stream.write_all(&frame) {
            // The write failed partway. The daemon may have Nacked (e.g. a payload
            // over its configured cap) and closed the connection before we finished
            // streaming — its Nack can already be sitting in our receive buffer.
            // Read it so the caller sees the real reason instead of a broken-pipe.
            // Either way the connection is now dead.
            if let Ok(resp) = self.read_response()
                && resp.header().message_type() == MessageType::Nack
            {
                self.closed_after_nack = true;
                return Err(nack_error(resp.payload()));
            }
            self.poisoned = true;
            return Err(write_err.into());
        }

        let resp = self.read_response()?;
        match resp.header().message_type() {
            MessageType::Ack => Ok(()),
            MessageType::Nack => Err(self.note_nack(nack_error(resp.payload()))),
            other => Err(ClientError::Protocol(format!(
                "expected Ack or Nack, got {other:?}"
            ))),
        }
    }

    /// Records that a Nack closed the connection (every reason except the
    /// transient `InternalError`, which the daemon keeps open) so the next call
    /// fails fast with a clear reconnect error rather than a broken pipe (QA#2).
    /// Returns the error unchanged for convenient `Err(self.note_nack(e))` use.
    fn note_nack(&mut self, err: ClientError) -> ClientError {
        if !matches!(err, ClientError::Nack(NackReason::InternalError)) {
            self.closed_after_nack = true;
        }
        err
    }

    /// Pushes at the connection's default durability tier.
    ///
    /// Returns [`ClientError::NoDefaultDurability`] if no default was set via
    /// [`set_default_durability`][Self::set_default_durability] (or via a
    /// `connect_with_default` / `connect_tls` constructor that accepted a
    /// `default_durability`).
    pub fn push_default(&mut self, payload: impl AsRef<[u8]>) -> Result<(), ClientError> {
        let d = self
            .default_durability
            .ok_or(ClientError::NoDefaultDurability)?;
        self.push(payload, d)
    }

    /// Sends a `HealthCheck` frame and returns `Ok(())` on a valid
    /// `HealthCheckResponse`. Returns an error if the daemon is unreachable
    /// or responds with a Nack.
    pub fn health_check(&mut self) -> Result<(), ClientError> {
        self.ensure_usable()?;
        let header = Header::new(MessageType::HealthCheck, Durability::Sync, 0);
        let frame = Envelope::new(header, vec![]).encode();
        self.stream.write_all(&frame)?;

        let resp = self.read_response()?;
        match resp.header().message_type() {
            MessageType::HealthCheckResponse => Ok(()),
            MessageType::Nack => Err(self.note_nack(nack_error(resp.payload()))),
            other => Err(ClientError::Protocol(format!(
                "expected HealthCheckResponse, got {other:?}"
            ))),
        }
    }

    /// Returns the poisoned-connection error if a prior response read failed.
    /// Once poisoned the stream may hold leftover bytes from an aborted response
    /// that would be mis-read as this call's reply — a false ack — so we refuse
    /// to use it (G04). The caller must drop the client and reconnect.
    fn ensure_usable(&self) -> Result<(), ClientError> {
        if self.closed_after_nack {
            return Err(ClientError::Protocol(
                "connection closed by the daemon after a Nack; \
                 drop this client and reconnect before sending again"
                    .into(),
            ));
        }
        if self.poisoned {
            return Err(ClientError::Protocol(
                "client connection poisoned by a prior response-read error/timeout; \
                 drop this client and reconnect before sending again"
                    .into(),
            ));
        }
        Ok(())
    }

    fn read_response(&mut self) -> Result<Envelope, ClientError> {
        // Any failure here leaves the stream in an indeterminate state (a timeout
        // mid-frame, a partial/aborted read, or a protocol violation): the unread
        // tail of this response could be mis-read as the NEXT request's reply.
        // Poison the client so every subsequent call fails fast (G04).
        let result = self.read_response_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn read_response_inner(&mut self) -> Result<Envelope, ClientError> {
        let mut header_buf = [0u8; HEADER_LEN];
        self.stream.read_exact(&mut header_buf)?;

        let header =
            Header::decode(&header_buf).map_err(|e| ClientError::Protocol(e.to_string()))?;

        let payload_len = header.payload_len() as usize;
        // Cap before allocating, mirroring the server's pre-allocation guard
        // (Envelope::decode). A malformed or hostile daemon could otherwise
        // declare payload_len up to u32::MAX and make the client allocate ~4 GiB
        // for a response (F44). Daemon responses are tiny, so this never trips
        // legitimately.
        if payload_len > MAX_PAYLOAD_HARD_CAP {
            return Err(ClientError::Protocol(format!(
                "response payload_len {payload_len} exceeds MAX_PAYLOAD_HARD_CAP \
                 ({MAX_PAYLOAD_HARD_CAP}); refusing to allocate"
            )));
        }
        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload_buf)?;
        }

        let mut crc_buf = [0u8; 4];
        self.stream.read_exact(&mut crc_buf)?;
        let expected = u32::from_le_bytes(crc_buf);
        let computed = crc32fast::hash(&payload_buf);
        if expected != computed {
            return Err(ClientError::Protocol(format!(
                "response payload CRC mismatch: expected {expected:#010x}, computed {computed:#010x}"
            )));
        }

        Ok(Envelope::new(header, payload_buf))
    }

    /// Crate-internal constructor so `tls.rs` can build the struct without
    /// going through a Unix-socket constructor.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn from_parts(stream: S, default_durability: Option<Durability>) -> Self {
        Self {
            stream,
            default_durability,
            poisoned: false,
            closed_after_nack: false,
        }
    }
}

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
    /// contract covers. Pick a value comfortably above the daemon's Sync ack
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

fn nack_error(payload: &[u8]) -> ClientError {
    match payload.first().copied() {
        Some(b) => match NackReason::try_from(b) {
            // A VersionMismatch Nack carries the daemon's WIRE_VERSION as a
            // second payload byte (`[0x02, daemon_version]`). Surface it so the
            // caller can report both sides; fall back to the bare reason if the
            // daemon (somehow) omitted it.
            Ok(NackReason::VersionMismatch) => match payload.get(1).copied() {
                Some(daemon_version) => ClientError::VersionMismatch { daemon_version },
                None => ClientError::Nack(NackReason::VersionMismatch),
            },
            Ok(r) => ClientError::Nack(r),
            Err(_) => ClientError::UnknownNack(b),
        },
        None => ClientError::Protocol("Nack frame had empty payload".into()),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        c.set_default_durability(Durability::Batched);

        let reader = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let mut hdr = [0u8; weir_core::HEADER_LEN];
            server_end.read_exact(&mut hdr).unwrap();
            let h = weir_core::Header::decode(&hdr).unwrap();
            let mut rest = vec![0u8; h.payload_len() as usize + 4];
            server_end.read_exact(&mut rest).unwrap();
            // Send back an Ack so push_default can complete.
            let ack = weir_core::Envelope::new(
                weir_core::Header::new(weir_core::MessageType::Ack, weir_core::Durability::Sync, 0),
                vec![],
            )
            .encode();
            server_end.write_all(&ack).unwrap();
            h.durability()
        });

        c.push_default(b"hello").unwrap();
        assert_eq!(reader.join().unwrap(), Durability::Batched);
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
        let err = c.push(&oversized, Durability::Sync).unwrap_err();
        assert!(
            matches!(err, ClientError::PayloadTooLarge { len, limit }
                if len == MAX_PAYLOAD_HARD_CAP + 1 && limit == MAX_PAYLOAD_HARD_CAP),
            "expected a local PayloadTooLarge, got {err:?}"
        );
        assert!(!c.poisoned, "a local rejection must not poison the connection");
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
                    weir_core::Durability::Sync,
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
        let err = c.push(&payload, Durability::Sync).unwrap_err();
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
            weir_core::Header::new(weir_core::MessageType::Nack, weir_core::Durability::Sync, 0),
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
            server_end.write_all(&nack_frame(NackReason::EmptyPayload)).unwrap();
            // drop server_end → connection closes, as the daemon does after a Nack.
        });
        let first = c.push(b"x", Durability::Sync).unwrap_err();
        server.join().unwrap();
        assert!(
            matches!(first, ClientError::Nack(NackReason::EmptyPayload)),
            "first push should surface the real reason, got {first:?}"
        );
        // Second call must NOT be a broken-pipe — it must clearly say reconnect.
        match c.push(b"y", Durability::Sync).unwrap_err() {
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
            server_end.write_all(&nack_frame(NackReason::InternalError)).unwrap();
            drain_one_frame(&mut server_end); // the retry
            let ack = weir_core::Envelope::new(
                weir_core::Header::new(weir_core::MessageType::Ack, weir_core::Durability::Sync, 0),
                vec![],
            )
            .encode();
            server_end.write_all(&ack).unwrap();
        });
        let first = c.push(b"a", Durability::Sync).unwrap_err();
        assert!(
            matches!(first, ClientError::Nack(NackReason::InternalError)),
            "got {first:?}"
        );
        assert!(
            !c.closed_after_nack,
            "InternalError is transient and must not close the connection"
        );
        c.push(b"b", Durability::Sync).unwrap(); // retry on the same connection succeeds
        server.join().unwrap();
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
        let err = c.push(b"x", Durability::Sync).unwrap_err();
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
        let first = c.push(b"x", Durability::Sync).unwrap_err();
        assert!(matches!(first, ClientError::Io(_)), "{first:?}");

        // Second push must fail FAST as poisoned, not block on another read.
        let started = std::time::Instant::now();
        let second = c.push(b"y", Durability::Sync).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "poisoned client must reject immediately, not read again"
        );
        match second {
            ClientError::Protocol(msg) => assert!(msg.contains("poisoned"), "{msg}"),
            other => panic!("expected a poisoned Protocol error, got {other:?}"),
        }
    }
}
