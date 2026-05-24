use std::{io, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    task,
};
use tracing::debug;

use weir_core::{
    DecodeError, Durability, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType, NackReason,
    WIRE_VERSION,
};

use crate::{models::WorkUnit, queue::QueueSender};

/// How long `handle_connection` waits for the queue to accept a work unit before
/// giving up and nacking. Prevents a saturated or dead worker pool from holding
/// socket connections open indefinitely.
pub const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-connection configuration derived from the server config.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Cap applied before allocation: `min(config.max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`.
    /// The field already holds the effective minimum so no further clamping is needed here.
    pub max_payload_bytes: usize,
}

/// Handles one client connection: parses frames in a loop, queues work units,
/// and sends Ack/Nack responses.
///
/// Frame parsing order (DoS hardening — never allocate before validating):
/// 1. Read exactly 16 bytes (header).
/// 2. `Header::decode()` — magic → version → header CRC.
/// 3. `payload_len` cap check before any heap allocation.
/// 4. Allocate and read exactly `payload_len` bytes.
/// 5. Read and validate 4-byte payload CRC32.
/// 6. Construct `WorkUnit`, push to queue via `spawn_blocking`.
/// 7. Await ack, send `Ack` or `Nack(InternalError)` back to client.
///
/// Any validation failure sends the appropriate Nack and closes the connection.
pub async fn handle_connection(
    mut stream: UnixStream,
    queue_tx: QueueSender<WorkUnit>,
    config: ConnectionConfig,
) -> io::Result<()> {
    loop {
        // ── 1. Read header ───────────────────────────────────────────────────
        let mut header_buf = [0u8; HEADER_LEN];
        match stream.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e)
                if e.kind() == io::ErrorKind::UnexpectedEof
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        // ── 2. Decode and validate header ────────────────────────────────────
        let header = match Header::decode(&header_buf) {
            Ok(h) => h,
            Err(e) => {
                send_decode_nack(&mut stream, &e).await?;
                return Ok(());
            }
        };

        // ── 3. Cap check before allocation ───────────────────────────────────
        // Effective cap is min(config, hard cap). The hard cap is applied here
        // regardless of ConnectionConfig contents so the check holds even when
        // handle_connection is called directly (e.g. in tests) without run().
        let payload_len = header.payload_len as usize;
        let cap = config.max_payload_bytes.min(MAX_PAYLOAD_HARD_CAP);
        if payload_len > cap {
            send_nack(&mut stream, NackReason::PayloadTooLarge, &[]).await?;
            return Ok(());
        }

        // ── 4. Read payload ──────────────────────────────────────────────────
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;

        // ── 5. Read and validate payload CRC ────────────────────────────────
        let mut crc_buf = [0u8; 4];
        stream.read_exact(&mut crc_buf).await?;
        let expected_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            send_nack(&mut stream, NackReason::BadPayloadCrc, &[]).await?;
            return Ok(());
        }

        // ── 6 & 7. Dispatch by message type ─────────────────────────────────
        match header.message_type {
            MessageType::Push => {
                handle_push(&mut stream, queue_tx.clone(), header.durability, payload).await?;
            }
            MessageType::HealthCheck => {
                let resp =
                    Header::new(MessageType::HealthCheckResponse, Durability::Sync, 0, 0).encode();
                let payload_crc = crc32fast::hash(&[]).to_le_bytes();
                stream.write_all(&resp).await?;
                stream.write_all(&payload_crc).await?;
            }
            _ => {
                debug!(msg_type = ?header.message_type, "unexpected message type from client");
                send_nack(&mut stream, NackReason::InternalError, &[]).await?;
                return Ok(());
            }
        }
    }
}

async fn handle_push(
    stream: &mut UnixStream,
    queue_tx: QueueSender<WorkUnit>,
    durability: Durability,
    payload: Vec<u8>,
) -> io::Result<()> {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let unit = WorkUnit {
        shard_id: 0,
        payload,
        durability,
        ack_tx,
    };

    let push_result = task::spawn_blocking(move || queue_tx.push_timeout(unit, QUEUE_PUSH_TIMEOUT))
        .await
        .map_err(io::Error::other)?;

    if push_result.is_err() {
        send_nack(stream, NackReason::InternalError, &[]).await?;
        return Ok(());
    }

    match ack_rx.await {
        Ok(true) => send_ack(stream).await,
        _ => send_nack(stream, NackReason::InternalError, &[]).await,
    }
}

/// Sends a Nack whose payload is `[reason_byte] ++ extra`.
///
/// For `VersionMismatch`, pass `extra = &[WIRE_VERSION]` so the client can
/// produce: "daemon is on wire protocol v{WIRE_VERSION}; this client is on vN."
async fn send_nack(stream: &mut UnixStream, reason: NackReason, extra: &[u8]) -> io::Result<()> {
    let mut nack_payload = Vec::with_capacity(1 + extra.len());
    nack_payload.push(reason as u8);
    nack_payload.extend_from_slice(extra);

    let payload_len = nack_payload.len() as u32;
    let header = Header::new(MessageType::Nack, Durability::Sync, 0, payload_len).encode();
    let payload_crc = crc32fast::hash(&nack_payload).to_le_bytes();

    stream.write_all(&header).await?;
    stream.write_all(&nack_payload).await?;
    stream.write_all(&payload_crc).await?;
    Ok(())
}

async fn send_ack(stream: &mut UnixStream) -> io::Result<()> {
    let header = Header::new(MessageType::Ack, Durability::Sync, 0, 0).encode();
    let payload_crc = crc32fast::hash(&[]).to_le_bytes();
    stream.write_all(&header).await?;
    stream.write_all(&payload_crc).await?;
    Ok(())
}

/// Maps a `DecodeError` to the appropriate Nack.
async fn send_decode_nack(stream: &mut UnixStream, err: &DecodeError) -> io::Result<()> {
    match err {
        DecodeError::BadMagic => send_nack(stream, NackReason::BadMagic, &[]).await,
        DecodeError::VersionMismatch { .. } => {
            // Second byte is the daemon's WIRE_VERSION so the client can produce
            // a human-readable "upgrade the daemon / downgrade the client" message.
            send_nack(stream, NackReason::VersionMismatch, &[WIRE_VERSION]).await
        }
        DecodeError::HeaderCrcMismatch { .. } => {
            send_nack(stream, NackReason::BadHeaderCrc, &[]).await
        }
        _ => send_nack(stream, NackReason::InternalError, &[]).await,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{models::WorkUnit, queue};
    use tokio::net::UnixStream;
    use weir_core::{
        Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType, NackReason,
        WIRE_VERSION,
    };

    /// Default test config: cap = MAX_PAYLOAD_HARD_CAP (no config-level restriction).
    fn test_cfg() -> ConnectionConfig {
        ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
        }
    }

    /// Spawns a connection handler with a queue that immediately acks every WorkUnit.
    /// Returns the client-side stream.
    async fn spawn_handler(cfg: ConnectionConfig) -> UnixStream {
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>();

        // Auto-acker: blocking crossbeam recv must run on an OS thread, not in a
        // tokio task — blocking a tokio worker thread stalls the entire runtime.
        std::thread::spawn(move || {
            let rx = queue_rx.get();
            while let Ok(unit) = rx.recv() {
                let _ = unit.ack_tx.send(true);
            }
        });

        tokio::spawn(handle_connection(server, queue_tx, cfg));
        client
    }

    /// Encodes a complete Push frame (header + payload + payload CRC).
    fn push_frame(payload: &[u8]) -> Vec<u8> {
        let header = Header::new(MessageType::Push, Durability::Sync, 0, payload.len() as u32);
        let env = Envelope::new(header, payload.to_vec());
        env.encode()
    }

    /// Reads one response frame from the stream, returning its MessageType and payload.
    async fn read_response(stream: &mut UnixStream) -> (MessageType, Vec<u8>) {
        let mut header_buf = [0u8; HEADER_LEN];
        stream.read_exact(&mut header_buf).await.unwrap();
        let header = Header::decode(&header_buf).unwrap();
        let mut payload = vec![0u8; header.payload_len as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }
        let mut crc_buf = [0u8; 4];
        stream.read_exact(&mut crc_buf).await.unwrap();
        (header.message_type, payload)
    }

    // ── Frame-level tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn valid_push_returns_ack() {
        let mut client = spawn_handler(test_cfg()).await;
        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, _) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Ack);
    }

    #[tokio::test]
    async fn bad_magic_returns_nack_bad_magic() {
        let mut client = spawn_handler(test_cfg()).await;
        // Corrupt the magic bytes.
        let mut frame = push_frame(b"data");
        frame[0..4].copy_from_slice(b"XXXX");
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::BadMagic as u8);
    }

    #[tokio::test]
    async fn future_version_returns_nack_version_mismatch_with_daemon_version() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        frame[4] = WIRE_VERSION + 1; // future version byte
        // Recompute header CRC so it's not rejected as BadHeaderCrc first.
        let crc = crc32fast::hash(&frame[..12]).to_le_bytes();
        frame[12..16].copy_from_slice(&crc);
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::VersionMismatch as u8);
        // Second byte MUST be the daemon's WIRE_VERSION so the client can say
        // "daemon is on vN; you are on vM — upgrade the daemon or downgrade the client."
        assert_eq!(
            payload[1], WIRE_VERSION,
            "second byte must be daemon WIRE_VERSION"
        );
    }

    #[tokio::test]
    async fn past_version_returns_nack_version_mismatch() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        frame[4] = 0; // version 0 (past)
        let crc = crc32fast::hash(&frame[..12]).to_le_bytes();
        frame[12..16].copy_from_slice(&crc);
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(
            payload[0],
            NackReason::VersionMismatch as u8,
            "version 0 must produce VersionMismatch, not a generic bad-header nack"
        );
        assert_eq!(payload[1], WIRE_VERSION);
    }

    #[tokio::test]
    async fn bad_header_crc_returns_nack_before_payload_read() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        // Corrupt the header CRC field directly.
        frame[12] ^= 0xff;
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::BadHeaderCrc as u8);
    }

    #[tokio::test]
    async fn payload_len_exceeding_config_cap_returns_nack_before_allocation() {
        // Set a tight cap of 16 bytes.
        let cfg = ConnectionConfig {
            max_payload_bytes: 16,
        };
        let mut client = spawn_handler(cfg).await;

        // Build a frame claiming 17-byte payload (1 over cap).
        let oversized_payload = vec![0u8; 17];
        client
            .write_all(&push_frame(&oversized_payload))
            .await
            .unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::PayloadTooLarge as u8);
    }

    #[tokio::test]
    async fn bad_payload_crc_returns_nack_after_payload_read() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        // Corrupt the trailing payload CRC (last 4 bytes).
        let last = frame.len();
        frame[last - 1] ^= 0xff;
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::BadPayloadCrc as u8);
    }

    #[tokio::test]
    async fn max_payload_hard_cap_enforced_regardless_of_config() {
        // Config cap higher than MAX_PAYLOAD_HARD_CAP must still be rejected.
        // The effective cap is min(config, MAX_PAYLOAD_HARD_CAP).
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP + 1,
        };
        let mut client = spawn_handler(cfg).await;

        // Build a header claiming MAX_PAYLOAD_HARD_CAP + 1 bytes.
        let header = Header::new(
            MessageType::Push,
            Durability::Sync,
            0,
            (MAX_PAYLOAD_HARD_CAP + 1) as u32,
        );
        let frame_header = header.encode();
        // Write just the header — server must reject before reading the payload.
        client.write_all(&frame_header).await.unwrap();
        // Dummy payload CRC placeholder (won't be read if cap check works).
        client.write_all(&[0u8; 4]).await.unwrap();

        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::PayloadTooLarge as u8);
    }

    #[tokio::test]
    async fn multiple_requests_on_single_connection() {
        let mut client = spawn_handler(test_cfg()).await;
        for _ in 0..5 {
            client.write_all(&push_frame(b"record")).await.unwrap();
            let (msg_type, _) = read_response(&mut client).await;
            assert_eq!(msg_type, MessageType::Ack);
        }
    }

    #[tokio::test]
    async fn clean_disconnect_does_not_panic() {
        let client = spawn_handler(test_cfg()).await;
        // Close without sending anything — handler should exit cleanly.
        drop(client);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn queue_saturated_returns_internal_error_nack() {
        // Drop the receiver immediately so push_timeout returns Disconnected at once.
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>();
        drop(queue_rx); // no receivers → Disconnected on first push
        let cfg = test_cfg();
        tokio::spawn(handle_connection(server, queue_tx, cfg));

        let mut client = client;
        client.write_all(&push_frame(b"data")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::InternalError as u8);
    }
}
