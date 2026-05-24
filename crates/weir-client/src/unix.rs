use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
};

use weir_core::{Durability, Envelope, HEADER_LEN, Header, MessageType, NackReason};

/// All errors that can be returned by [`WeirClient`] methods.
#[derive(Debug)]
pub enum ClientError {
    /// An I/O error on the underlying socket.
    Io(io::Error),
    /// The daemon sent a Nack with a recognised reason.
    Nack(NackReason),
    /// The daemon sent a Nack with an unrecognised reason byte.
    UnknownNack(u8),
    /// The daemon's response violated the wire protocol.
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "socket I/O error: {e}"),
            Self::Nack(r) => write!(f, "server nack: {r:?}"),
            Self::UnknownNack(b) => write!(f, "server nack with unknown reason {b:#04x}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
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
/// The underlying [`UnixStream`] runs in blocking mode. Each method sends one
/// request frame and reads one response frame before returning. Requests are
/// not pipelined; for concurrent producers, create one `WeirClient` per thread.
///
/// Drop the client to close the connection. The daemon treats EOF as a clean
/// disconnect.
pub struct WeirClient {
    stream: UnixStream,
}

impl WeirClient {
    /// Opens a connection to the weir daemon's Unix socket at `path`.
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path.as_ref())?;
        Ok(Self { stream })
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
        let payload = payload.as_ref().to_vec();
        let len = payload.len() as u32;
        let header = Header::new(MessageType::Push, durability, 0, len);
        let frame = Envelope::new(header, payload).encode();
        self.stream.write_all(&frame)?;

        let resp = self.read_response()?;
        match resp.header.message_type {
            MessageType::Ack => Ok(()),
            MessageType::Nack => Err(nack_error(&resp.payload)),
            other => Err(ClientError::Protocol(format!(
                "expected Ack or Nack, got {other:?}"
            ))),
        }
    }

    /// Sends a `HealthCheck` frame and returns `Ok(())` on a valid
    /// `HealthCheckResponse`. Returns an error if the daemon is unreachable
    /// or responds with a Nack.
    pub fn health_check(&mut self) -> Result<(), ClientError> {
        let header = Header::new(MessageType::HealthCheck, Durability::Sync, 0, 0);
        let frame = Envelope::new(header, vec![]).encode();
        self.stream.write_all(&frame)?;

        let resp = self.read_response()?;
        match resp.header.message_type {
            MessageType::HealthCheckResponse => Ok(()),
            MessageType::Nack => Err(nack_error(&resp.payload)),
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

        let payload_len = header.payload_len as usize;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload)?;
        }

        let mut crc_buf = [0u8; 4];
        self.stream.read_exact(&mut crc_buf)?;
        let expected = u32::from_le_bytes(crc_buf);
        let computed = crc32fast::hash(&payload);
        if expected != computed {
            return Err(ClientError::Protocol(format!(
                "response payload CRC mismatch: expected {expected:#010x}, computed {computed:#010x}"
            )));
        }

        Ok(Envelope::new(header, payload))
    }
}

fn nack_error(payload: &[u8]) -> ClientError {
    match payload.first().copied() {
        Some(b) => match NackReason::try_from(b) {
            Ok(r) => ClientError::Nack(r),
            Err(_) => ClientError::UnknownNack(b),
        },
        None => ClientError::Protocol("Nack frame had empty payload".into()),
    }
}
