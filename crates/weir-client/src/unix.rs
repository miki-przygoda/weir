use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
};

use weir_core::{
    Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType, NackReason,
};

/// All errors that can be returned by [`WeirClient`] methods.
#[derive(Debug)]
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
    stream: S,
    default_durability: Option<Durability>,
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
        // copy_from_slice at the API boundary so downstream handling is zero-copy.
        let payload = weir_core::Payload::copy_from_slice(payload.as_ref());
        let len = payload.len() as u32;
        let header = Header::new(MessageType::Push, durability, 0, len);
        let frame = Envelope::new(header, payload).encode();
        self.stream.write_all(&frame)?;

        let resp = self.read_response()?;
        match resp.header().message_type() {
            MessageType::Ack => Ok(()),
            MessageType::Nack => Err(nack_error(resp.payload())),
            other => Err(ClientError::Protocol(format!(
                "expected Ack or Nack, got {other:?}"
            ))),
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
        let header = Header::new(MessageType::HealthCheck, Durability::Sync, 0, 0);
        let frame = Envelope::new(header, vec![]).encode();
        self.stream.write_all(&frame)?;

        let resp = self.read_response()?;
        match resp.header().message_type() {
            MessageType::HealthCheckResponse => Ok(()),
            MessageType::Nack => Err(nack_error(resp.payload())),
            other => Err(ClientError::Protocol(format!(
                "expected HealthCheckResponse, got {other:?}"
            ))),
        }
    }

    fn read_response(&mut self) -> Result<Envelope, ClientError> {
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
        }
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
                weir_core::Header::new(
                    weir_core::MessageType::Ack,
                    weir_core::Durability::Sync,
                    0,
                    0,
                ),
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
}
