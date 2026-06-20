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
    /// `push` was called with an empty payload, rejected locally before any bytes
    /// are sent. An empty payload IS the WAB end-of-records sentinel, so the daemon
    /// Nacks it ([`NackReason::EmptyPayload`]) and closes the connection; rejecting
    /// up front keeps the connection usable, mirroring the local
    /// [`PayloadTooLarge`][Self::PayloadTooLarge] reject.
    EmptyPayload,
}

impl ClientError {
    /// Whether the connection is still usable after this error.
    ///
    /// - **Recoverable (`true`)** — the connection is still usable: the error is a
    ///   *local* or *per-record* rejection that does **not** poison the client.
    ///   Retry a different payload, or continue, on the **same** client.
    /// - **Non-recoverable (`false`)** — the connection is dead or in an
    ///   indeterminate state: the error poisoned the client (set its internal
    ///   `poisoned`/`closed_after_nack` flag). **Drop this client and reconnect**;
    ///   every subsequent call on it will fail fast (see [`WeirClient::is_poisoned`]).
    ///
    /// This is the stable way to branch on whether to reconnect: [`ClientError`] is
    /// `#[non_exhaustive]`, so an exhaustive `match` on its variants will not compile
    /// downstream, and the variant set may grow post-1.0. Calling `is_recoverable`
    /// (or checking [`WeirClient::is_poisoned`] after a failed call) keeps working
    /// across additions.
    ///
    /// The classification is kept in lock-step with the client's poison logic: a
    /// variant is recoverable **iff** producing it does not set the client's
    /// `poisoned` or `closed_after_nack` flag.
    ///
    /// | Variant | Recoverable? | Why |
    /// |---|---|---|
    /// | [`PayloadTooLarge`][Self::PayloadTooLarge] | yes | local pre-send rejection; no bytes sent, connection untouched |
    /// | [`EmptyPayload`][Self::EmptyPayload] | yes | local pre-send rejection; no bytes sent, connection untouched |
    /// | [`NoDefaultDurability`][Self::NoDefaultDurability] | yes | local misconfiguration; returned before any I/O |
    /// | [`Nack`][Self::Nack]`(`[`InternalError`][NackReason::InternalError]`)` | yes | transient daemon condition; the daemon keeps the connection open |
    /// | [`Nack`][Self::Nack]`(`any other reason`)` | no | the daemon closes the connection after the Nack |
    /// | [`VersionMismatch`][Self::VersionMismatch] | no | a Nack reason; the daemon closes the connection |
    /// | [`UnknownNack`][Self::UnknownNack] | no | a Nack reason; the daemon closes the connection |
    /// | [`Io`][Self::Io] | no | a socket read/write failure poisons the client |
    /// | [`Protocol`][Self::Protocol] | no | a malformed/aborted response poisons the client (also the "reconnect" guard error) |
    ///
    /// Note: a *server* `Nack(PayloadTooLarge)` (a payload over the daemon's
    /// configured cap) is **not** recoverable — the daemon Nacks then closes the
    /// connection — whereas the *local* [`PayloadTooLarge`][Self::PayloadTooLarge]
    /// error (a payload over the protocol hard cap, rejected before any bytes are
    /// sent) **is**. They are distinct variants despite the shared name.
    ///
    /// New `#[non_exhaustive]` variants default to **non-recoverable** (`false`):
    /// "drop and reconnect" is the safe assumption for an error this build does not
    /// yet understand.
    #[must_use]
    pub fn is_recoverable(&self) -> bool {
        match self {
            // Local, pre-I/O rejections: the connection was never touched.
            Self::PayloadTooLarge { .. } | Self::NoDefaultDurability | Self::EmptyPayload => true,
            // The only Nack the daemon keeps the connection open for (transient);
            // every other Nack reason closes it (see `WeirClient::note_nack`).
            Self::Nack(NackReason::InternalError) => true,
            // Every other Nack reason closes the connection.
            Self::Nack(_) => false,
            // VersionMismatch / UnknownNack are Nack reasons too — the daemon
            // closes the connection after them (they route through `note_nack`).
            Self::VersionMismatch { .. } | Self::UnknownNack(_) => false,
            // A socket failure poisons the client; a protocol violation either
            // poisons it (bad response) or IS the "drop and reconnect" guard error.
            Self::Io(_) | Self::Protocol(_) => false,
            // `#[non_exhaustive]`: a future variant we don't recognise — assume the
            // worst (reconnect) rather than risk reusing a poisoned connection.
            // Unreachable within this crate (all current variants are matched
            // above), but required so the match stays total if a variant is added.
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }
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
            Self::EmptyPayload => write!(
                f,
                "payload is empty; rejected locally (an empty payload is the WAB end-of-records sentinel)"
            ),
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
    ///
    /// An `Ack` means the record is durably **buffered** at the requested tier —
    /// **not** that it has reached the downstream sink yet (the daemon drains in
    /// batches once a segment seals). See the crate-level "Ack vs. delivery" note.
    ///
    /// The ack is **per record, not a liveness probe for the next one.** If the
    /// daemon dies (or the socket is severed) between two pushes, the *first*
    /// push after the failure can still return `Ok` — its frame was written and
    /// acked, or the breakage isn't observed until the next read/write — and only
    /// the *following* push surfaces the broken pipe. So a single `Ok` does not
    /// guarantee the connection is healthy for subsequent pushes. After any push
    /// error, branch on [`ClientError::is_recoverable`] /
    /// [`is_poisoned`][Self::is_poisoned] to decide whether to keep using this
    /// client or drop and reconnect, rather than assuming a prior `Ok` proved the
    /// connection good.
    #[must_use = "a dropped push result hides whether the record was acked; an \
                  unhandled Nack also closes the connection"]
    pub fn push(
        &mut self,
        payload: impl AsRef<[u8]>,
        durability: Durability,
    ) -> Result<(), ClientError> {
        self.ensure_usable()?;
        let bytes = payload.as_ref();
        // Local guard against an empty payload. An empty payload IS the WAB
        // end-of-records sentinel, so the daemon Nacks it (NackReason::EmptyPayload)
        // and closes the connection — which would poison this client. Reject it up
        // front, before any bytes are sent, so the connection stays usable
        // (recoverable), mirroring the hard-cap guard below.
        if bytes.is_empty() {
            return Err(ClientError::EmptyPayload);
        }
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
            // A Nack may carry an empty payload, which `nack_error` reports as a
            // `Protocol` desync; poison in that case (see `surface_nack`).
            MessageType::Nack => Err(self.surface_nack(resp.payload())),
            // Any other frame type is a stream desync: the daemon sent something we
            // never expect here, so leftover/unexpected bytes could be mis-read as a
            // later reply (a false ack). Poison the connection (Protocol → not
            // recoverable, matching `ensure_usable`).
            other => {
                self.poisoned = true;
                Err(ClientError::Protocol(format!(
                    "expected Ack or Nack, got {other:?}"
                )))
            }
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

    /// Decodes a Nack frame's payload and records its connection effect.
    ///
    /// `nack_error` maps a well-formed Nack to a `Nack`/`VersionMismatch`/`UnknownNack`
    /// error (routed through [`note_nack`][Self::note_nack] to mark a closing Nack),
    /// but an **empty** Nack payload is a stream desync that it reports as
    /// [`ClientError::Protocol`]. A desync means leftover/unexpected bytes could be
    /// mis-read as a later reply (a false ack), so we poison the connection in that
    /// case — keeping `Protocol` non-recoverable and `is_poisoned()` in agreement.
    fn surface_nack(&mut self, payload: &[u8]) -> ClientError {
        match nack_error(payload) {
            ClientError::Protocol(msg) => {
                self.poisoned = true;
                ClientError::Protocol(msg)
            }
            other => self.note_nack(other),
        }
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
            // An empty Nack payload is a desync `nack_error` reports as `Protocol`;
            // poison in that case (see `surface_nack`).
            MessageType::Nack => Err(self.surface_nack(resp.payload())),
            // Any other frame type is a stream desync: poison the connection so a
            // later call can't mis-read a stale frame as its reply (Protocol → not
            // recoverable, matching `ensure_usable`).
            other => {
                self.poisoned = true;
                Err(ClientError::Protocol(format!(
                    "expected HealthCheckResponse, got {other:?}"
                )))
            }
        }
    }

    /// Whether this client's connection is dead and must be rebuilt.
    ///
    /// Reports the union of the two ways a connection becomes unusable, so it is the
    /// exact complement of [`is_recoverable`][ClientError::is_recoverable] for the
    /// error that caused it:
    ///
    /// - an **indeterminate stream** (the internal `poisoned` flag) — a
    ///   response-read failure/timeout, a protocol violation in the reply, or any
    ///   stream desync (an unexpected daemon message type, an empty-payload Nack);
    ///   leftover/unexpected bytes could be mis-read as a later reply (a false ack);
    /// - a **connection-closing Nack** (the internal `closed_after_nack` flag) —
    ///   every Nack reason except the transient
    ///   [`InternalError`][NackReason::InternalError], after which the daemon closes
    ///   the connection.
    ///
    /// In either state **every** method fails fast (it will not touch the socket
    /// again); the caller must drop this client and open a fresh connection.
    ///
    /// A long-lived producer thread (e.g. one driven from an async runtime) should
    /// check this after a failed call — or, equivalently, branch on
    /// [`ClientError::is_recoverable`] — and rebuild the connection when it reports
    /// poisoned / non-recoverable, rather than retrying on the dead client:
    ///
    /// ```no_run
    /// # use weir_client::{WeirClient, ClientError};
    /// # use weir_core::Durability;
    /// # fn reconnect() -> WeirClient { unreachable!() }
    /// # let mut client = reconnect();
    /// if let Err(e) = client.push(b"record", Durability::Sync) {
    ///     if !e.is_recoverable() || client.is_poisoned() {
    ///         client = reconnect(); // drop the dead client, rebuild
    ///     }
    /// }
    /// ```
    ///
    /// See [`ClientError::is_recoverable`] for the per-error view of the same
    /// distinction (recoverable = connection still usable; non-recoverable = drop
    /// and reconnect). The two are kept equivalent: `is_poisoned()` is true after a
    /// failed call exactly when that call's error is non-recoverable.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned || self.closed_after_nack
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
            server_end
                .write_all(&nack_frame(NackReason::EmptyPayload))
                .unwrap();
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
            server_end
                .write_all(&nack_frame(NackReason::InternalError))
                .unwrap();
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
        let err = c.push(b"x", Durability::Sync).unwrap_err();
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
        let first = c.push(b"x", Durability::Sync).unwrap_err();
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
                    weir_core::Durability::Sync,
                    0,
                ),
                vec![],
            )
            .encode();
            server_end.write_all(&bogus).unwrap();
        });
        let err = c.push(b"x", Durability::Sync).unwrap_err();
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
        let err = c.push(&oversized, Durability::Sync).unwrap_err();
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
        let err = c.push(b"", Durability::Sync).unwrap_err();
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
