use std::{io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    task,
};
use tracing::debug;

use weir_core::{
    DecodeError, Durability, Envelope, HEADER_LEN, Header, MAX_PAYLOAD_HARD_CAP, MessageType,
    NackReason as WireNack, WIRE_VERSION,
};

use crate::{
    metrics::{Metrics, NackLabel, NackReason as MetricNack, TierLabel, TierValue},
    models::WorkUnit,
    queue::QueueSender,
};

/// How long `handle_connection` waits for the queue to accept a work unit before
/// giving up and nacking. Prevents a saturated or dead worker pool from holding
/// socket connections open indefinitely.
pub const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `handle_connection` waits on the WAB ack oneshot before giving up
/// and nacking. Bounds the blast radius of a wedged flusher (one that hasn't
/// panicked but is stuck on a slow fsync, lock contention, etc.) so it can't
/// hold a connection's semaphore permit forever.
///
/// 30s is well above any healthy fsync (microseconds on SSD, ~100ms on
/// contended rotational disk) but short enough that an operator who hits this
/// timeout investigates rather than ignores. A producer whose record fires
/// the timeout receives Nack(InternalError); the record may still be durably
/// written by the eventual flusher completion — at-least-once semantics.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-connection configuration derived from the server config.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Cap applied before allocation: `min(config.max_payload_bytes, MAX_PAYLOAD_HARD_CAP)`.
    /// The field already holds the effective minimum so no further clamping is needed here.
    pub max_payload_bytes: usize,
    /// Maximum time the handler will sit in `read_exact` waiting for the next
    /// byte. Caps slowloris-style attacks where a connected client never sends
    /// (or sends only a partial frame) and would otherwise hold a semaphore
    /// permit indefinitely.
    pub read_timeout: Duration,
    /// Maximum time the handler will wait on the WAB ack oneshot. See
    /// [`ACK_TIMEOUT`] for the production default and rationale; tests
    /// override this to a short value so they don't need to mock the clock.
    pub ack_timeout: Duration,
    /// Target shard for every WorkUnit pushed on this connection. The accept
    /// loop in `socket::run` assigns this round-robin (counter % shard_count)
    /// at connection time so a multi-shard config (shard_count > 1) actually
    /// fans out work across the per-shard WAB flusher threads. With
    /// shard_count = 1 every connection gets shard_id = 0, which matches the
    /// pre-multi-shard behaviour.
    pub shard_id: u32,
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
    stream: UnixStream,
    queue_tx: QueueSender<WorkUnit>,
    config: ConnectionConfig,
    metrics: Arc<Metrics>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    // Wrap the stream in a BufReader so the typical Push frame (header +
    // payload + CRC) — which the client writes in a single write_all and
    // the kernel delivers in one packet — comes off the socket in one
    // read syscall instead of three. Writes still go to the underlying
    // stream via `stream.get_mut()` so ack/nack responses don't sit in
    // a write buffer.
    //
    // 8 KiB default capacity fits the common case (16 B header + payload
    // up to ~8 KiB + 4 B CRC) without bumping per-connection memory much.
    // Larger payloads will issue a follow-up read, same as before.
    let mut stream = tokio::io::BufReader::new(stream);
    loop {
        // ── Shutdown check ──────────────────────────────────────────────────
        // If the daemon is shutting down, exit cleanly between frames so the
        // connection task drops its semaphore permit and the JoinSet drain
        // can complete. Doing the check HERE (not mid-await) means the
        // current frame, if any, has already been acked or nacked.
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        // ── 1. Read header ───────────────────────────────────────────────────
        let mut header_buf = [0u8; HEADER_LEN];
        let read_result = tokio::select! {
            biased;
            res = tokio::time::timeout(config.read_timeout, stream.read_exact(&mut header_buf)) => res,
            // Shutdown fires → exit immediately so an idle connection does
            // not wait out read_timeout. The header has not been consumed
            // yet so there's no in-flight request to ack/nack.
            _ = shutdown_rx.changed() => return Ok(()),
        };
        match read_result {
            Ok(Ok(_)) => {}
            Ok(Err(e))
                if e.kind() == io::ErrorKind::UnexpectedEof
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                // Idle past read_timeout — slowloris guard. Bump the counter,
                // drop the connection (which releases its semaphore permit),
                // do not send a Nack (the client isn't reading anyway).
                metrics.connection_idle_timeout.inc();
                debug!(
                    timeout_secs = config.read_timeout.as_secs(),
                    "connection idle past read_timeout during header read; dropping"
                );
                return Ok(());
            }
        }

        // ── 2. Decode and validate header ────────────────────────────────────
        let header = match Header::decode(&header_buf) {
            Ok(h) => h,
            Err(e) => {
                let (wire, metric, extra) = nack_for_decode_error(&e);
                send_nack(stream.get_mut(), wire, extra).await?;
                metrics
                    .records_nack
                    .get_or_create(&NackLabel {
                        tier: TierValue::sync,
                        reason: metric,
                    })
                    .inc();
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
            let tv = durability_to_tier(header.durability);
            send_nack(stream.get_mut(), WireNack::PayloadTooLarge, &[]).await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::payload_too_large,
                })
                .inc();
            return Ok(());
        }

        // ── 4. Read payload ──────────────────────────────────────────────────
        let mut payload = vec![0u8; payload_len];
        match tokio::time::timeout(config.read_timeout, stream.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                metrics.connection_idle_timeout.inc();
                debug!(
                    payload_len,
                    timeout_secs = config.read_timeout.as_secs(),
                    "connection idle past read_timeout during payload read; dropping"
                );
                return Ok(());
            }
        }

        // ── 5. Read and validate payload CRC ────────────────────────────────
        let mut crc_buf = [0u8; 4];
        match tokio::time::timeout(config.read_timeout, stream.read_exact(&mut crc_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                metrics.connection_idle_timeout.inc();
                debug!(
                    timeout_secs = config.read_timeout.as_secs(),
                    "connection idle past read_timeout during CRC read; dropping"
                );
                return Ok(());
            }
        }
        let expected_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            let tv = durability_to_tier(header.durability);
            send_nack(stream.get_mut(), WireNack::BadPayloadCrc, &[]).await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::bad_payload_crc,
                })
                .inc();
            return Ok(());
        }

        // ── 6 & 7. Dispatch by message type ─────────────────────────────────
        match header.message_type {
            MessageType::Push => {
                let tv = durability_to_tier(header.durability);
                metrics
                    .records_accepted
                    .get_or_create(&TierLabel { tier: tv.clone() })
                    .inc();
                handle_push(
                    stream.get_mut(),
                    queue_tx.clone(),
                    header.durability,
                    payload,
                    tv,
                    config.shard_id,
                    config.ack_timeout,
                    &metrics,
                )
                .await?;
            }
            MessageType::HealthCheck => {
                stream
                    .get_mut()
                    .write_all(healthcheck_response_frame_bytes())
                    .await?;
            }
            _ => {
                debug!(msg_type = ?header.message_type, "unexpected message type from client");
                send_nack(stream.get_mut(), WireNack::InternalError, &[]).await?;
                metrics
                    .records_nack
                    .get_or_create(&NackLabel {
                        tier: TierValue::sync,
                        reason: MetricNack::internal_error,
                    })
                    .inc();
                return Ok(());
            }
        }
    }
}

// 8 args — clippy's threshold is 7. Grouping these into a `PushCtx`
// struct is mechanically possible but doesn't improve call-site
// readability: each field is a distinct concept the caller already
// has separately (`queue_tx`, `metrics`, etc. are top-level handles
// in the connection loop; bundling them just adds a constructor
// roundtrip). The fn body uses each argument once.
#[allow(clippy::too_many_arguments)]
async fn handle_push(
    stream: &mut UnixStream,
    queue_tx: QueueSender<WorkUnit>,
    durability: Durability,
    payload: Vec<u8>,
    tv: TierValue,
    shard_id: u32,
    ack_timeout: Duration,
    metrics: &Arc<Metrics>,
) -> io::Result<()> {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let unit = WorkUnit {
        shard_id,
        payload,
        durability,
        ack_tx,
    };

    // Partition by shard_id so every record destined for a given shard lands
    // in the same worker's queue, preserving per-shard FIFO across multiple
    // concurrent producers. See worker::spawn_workers for the full chain.
    let partition_key = unit.shard_id as usize;
    // Fast path: try a non-blocking push. crossbeam's MPMC send is lock-free
    // when the partition has capacity (the common case under normal load),
    // so we avoid the spawn_blocking cross-thread hop entirely. Only fall
    // back to the blocking timeout path when the partition is actually full
    // — that preserves the existing backpressure behaviour (short waits
    // tolerated, sustained saturation nacks).
    let push_result: Result<(), ()> = match queue_tx.try_push(partition_key, unit) {
        Ok(()) => Ok(()),
        Err(unit) => task::spawn_blocking(move || {
            queue_tx.push_timeout(partition_key, unit, QUEUE_PUSH_TIMEOUT)
        })
        .await
        .map_err(io::Error::other)?
        .map_err(|_| ()),
    };

    if push_result.is_err() {
        send_nack(stream, WireNack::InternalError, &[]).await?;
        metrics
            .records_nack
            .get_or_create(&NackLabel {
                tier: tv,
                reason: MetricNack::internal_error,
            })
            .inc();
        return Ok(());
    }

    match tokio::time::timeout(ack_timeout, ack_rx).await {
        Ok(Ok(true)) => {
            metrics
                .records_ack
                .get_or_create(&TierLabel { tier: tv })
                .inc();
            send_ack(stream).await
        }
        Ok(_) => {
            // Flusher fired ack with `false` (write/fsync error) or dropped
            // the sender (panic). Either way: InternalError nack.
            send_nack(stream, WireNack::InternalError, &[]).await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::internal_error,
                })
                .inc();
            Ok(())
        }
        Err(_elapsed) => {
            // ACK_TIMEOUT exceeded. The flusher is wedged (not panicked —
            // a panic would have dropped the sender, which is the Ok(_)
            // branch above). The record may still get written; producer
            // sees Nack(InternalError) and retries on a fresh connection.
            metrics.ack_timeout.inc();
            send_nack(stream, WireNack::InternalError, &[]).await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::internal_error,
                })
                .inc();
            Ok(())
        }
    }
}

fn durability_to_tier(d: Durability) -> TierValue {
    match d {
        Durability::Sync => TierValue::sync,
        Durability::Batched => TierValue::batched,
        Durability::Buffered => TierValue::buffered,
    }
}

/// Single source of truth mapping a [`DecodeError`] to the three pieces of
/// information every nack site needs: the wire-level [`WireNack`] variant
/// (sent on the socket), the Prometheus-label [`MetricNack`] variant
/// (incremented on `records_nack`), and the extra bytes that accompany the
/// reason byte in the Nack payload.
///
/// Adding a new `DecodeError` variant now requires touching only this one
/// match — the previous design had two parallel tables (`decode_err_to_metric_nack`
/// and `send_decode_nack`) that could drift independently.
fn nack_for_decode_error(e: &DecodeError) -> (WireNack, MetricNack, &'static [u8]) {
    match e {
        DecodeError::BadMagic => (WireNack::BadMagic, MetricNack::bad_magic, &[]),
        // Second byte is the daemon's WIRE_VERSION so the client can render
        // "daemon is on wire protocol v{WIRE_VERSION}; this client is on vN."
        DecodeError::VersionMismatch { .. } => (
            WireNack::VersionMismatch,
            MetricNack::version_mismatch,
            &[WIRE_VERSION],
        ),
        DecodeError::HeaderCrcMismatch { .. } => {
            (WireNack::BadHeaderCrc, MetricNack::bad_header_crc, &[])
        }
        _ => (WireNack::InternalError, MetricNack::internal_error, &[]),
    }
}

/// Sends a Nack whose payload is `[reason_byte] ++ extra`.
///
/// For `VersionMismatch`, pass `extra = &[WIRE_VERSION]` so the client can
/// produce: "daemon is on wire protocol v{WIRE_VERSION}; this client is on vN."
async fn send_nack(stream: &mut UnixStream, reason: WireNack, extra: &[u8]) -> io::Result<()> {
    let mut nack_payload = Vec::with_capacity(1 + extra.len());
    nack_payload.push(reason as u8);
    nack_payload.extend_from_slice(extra);

    let header = Header::new(
        MessageType::Nack,
        Durability::Sync,
        0,
        nack_payload.len() as u32,
    );
    let frame = Envelope::new(header, nack_payload).encode();
    stream.write_all(&frame).await
}

/// The Ack frame's 20 bytes are entirely constant — fixed magic, fixed
/// version, fixed message_type, zero-length payload, header CRC over the
/// same fixed bytes, and empty-payload CRC = 0. Memoised on first call so
/// the steady state writes a borrowed `&'static [u8]` instead of
/// allocating + encoding + CRC-ing identical 20-byte buffers per response.
fn ack_frame_bytes() -> &'static [u8] {
    static ACK_FRAME: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    ACK_FRAME.get_or_init(|| {
        let header = Header::new(MessageType::Ack, Durability::Sync, 0, 0);
        Envelope::new(header, Vec::new()).encode()
    })
}

/// Same memoisation story as [`ack_frame_bytes`] but for the
/// `HealthCheckResponse` frame (different message_type → different header
/// CRC, otherwise identical shape).
fn healthcheck_response_frame_bytes() -> &'static [u8] {
    static HEALTHCHECK_RESPONSE_FRAME: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    HEALTHCHECK_RESPONSE_FRAME.get_or_init(|| {
        let header = Header::new(MessageType::HealthCheckResponse, Durability::Sync, 0, 0);
        Envelope::new(header, Vec::new()).encode()
    })
}

async fn send_ack(stream: &mut UnixStream) -> io::Result<()> {
    stream.write_all(ack_frame_bytes()).await
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

    /// Default test config: cap = MAX_PAYLOAD_HARD_CAP, generous read timeout.
    fn test_cfg() -> ConnectionConfig {
        ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
        }
    }

    /// Watch receiver wired to a sender that's leaked into a static so the
    /// receiver never sees a `changed()` event. Tests that don't exercise
    /// shutdown pass this; the value is permanently `false`.
    fn never_shutdown_rx() -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        // Leak the sender so the channel can't be closed (a closed sender
        // would cause shutdown_rx.changed() to return Err — also a valid
        // exit signal — which we don't want in tests).
        Box::leak(Box::new(tx));
        rx
    }

    /// Spawns a connection handler with a queue that immediately acks every WorkUnit.
    /// Returns the client-side stream.
    async fn spawn_handler(cfg: ConnectionConfig) -> UnixStream {
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);

        // Auto-acker: blocking crossbeam recv must run on an OS thread, not in a
        // tokio task — blocking a tokio worker thread stalls the entire runtime.
        std::thread::spawn(move || {
            let rx = queue_rx.get(0);
            while let Ok(unit) = rx.recv() {
                let _ = unit.ack_tx.send(true);
            }
        });

        tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));
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
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
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
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
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
    async fn ack_timeout_nacks_when_flusher_does_not_respond() {
        // Simulates a wedged flusher: the receiver gets the WorkUnit but
        // never fires ack_tx. The handler must give up after ack_timeout
        // and Nack(InternalError), and the ack_timeout counter must
        // increment exactly once.
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_millis(100), // tight bound so the test is fast
            shard_id: 0,
        };
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let metrics_for_check = std::sync::Arc::clone(&metrics);

        // Never-acker: receives the unit and parks the ack_tx forever (a
        // wedged flusher that hasn't panicked). The Vec keeps the ack_tx
        // alive so the handler's await sees a pending oneshot, not a
        // dropped sender.
        std::thread::spawn(move || {
            let rx = queue_rx.get(0);
            let mut parked: Vec<tokio::sync::oneshot::Sender<bool>> = Vec::new();
            while let Ok(unit) = rx.recv() {
                parked.push(unit.ack_tx);
            }
        });

        tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));

        let mut client = client;
        let t0 = std::time::Instant::now();
        client.write_all(&push_frame(b"data")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        let elapsed = t0.elapsed();

        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::InternalError as u8);
        assert!(
            elapsed >= Duration::from_millis(100),
            "nack returned before ack_timeout elapsed: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "nack returned too long after ack_timeout: {elapsed:?}"
        );
        assert_eq!(
            metrics_for_check.ack_timeout.get(),
            1,
            "ack_timeout counter must increment exactly once on timeout"
        );
    }

    #[tokio::test]
    async fn queue_saturated_returns_internal_error_nack() {
        // Drop the receiver immediately so push_timeout returns Disconnected at once.
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        drop(queue_rx); // no receivers → Disconnected on first push
        let cfg = test_cfg();
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));

        let mut client = client;
        client.write_all(&push_frame(b"data")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::InternalError as u8);
    }

    // ── Read timeout (slowloris guard) ────────────────────────────────────────

    /// A client that opens a connection and never sends a byte must be
    /// dropped within `read_timeout`. Without this, a slow attacker can hold
    /// a connection semaphore permit forever.
    #[tokio::test]
    async fn read_timeout_drops_idle_connection_before_header() {
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_millis(150),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
        };
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let metrics_for_check = std::sync::Arc::clone(&metrics);

        let start = std::time::Instant::now();
        let handle = tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));

        // The handler should return Ok within ~150ms+buffer once the
        // read times out. We give it 1s of margin.
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        let elapsed = start.elapsed();
        let _ = client; // keep client end alive; the server times out, not the client

        let handler_result = result.expect("handler did not exit within 1s");
        assert!(
            handler_result.is_ok(),
            "handler join error: {handler_result:?}"
        );
        let connection_result = handler_result.unwrap();
        assert!(
            connection_result.is_ok(),
            "handler returned Err: {connection_result:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(150),
            "handler returned before timeout elapsed: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(800),
            "handler took too long after timeout: {elapsed:?}"
        );
        assert_eq!(
            metrics_for_check.connection_idle_timeout.get(),
            1,
            "connection_idle_timeout counter must increment exactly once"
        );
    }

    /// A client that sends a valid header then stalls during the payload
    /// read must also be dropped within `read_timeout`. Same threat model as
    /// the header-stall case; tests the second of the three read sites.
    #[tokio::test]
    async fn read_timeout_drops_idle_connection_during_payload() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_millis(150),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
        };
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let metrics_for_check = std::sync::Arc::clone(&metrics);

        // Send a header advertising a 1024-byte payload, then send NOTHING.
        let header = Header::new(MessageType::Push, Durability::Sync, 0, 1024).encode();
        let handle = tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));
        client.write_all(&header).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("handler did not exit within 1s")
            .expect("handler task panicked");
        // The handler may return Ok (clean drop after timeout) or Err
        // (UnexpectedEof if the client end is dropped). The timeout path is
        // what we care about, observable via the counter.
        let _ = result;
        assert_eq!(
            metrics_for_check.connection_idle_timeout.get(),
            1,
            "connection_idle_timeout counter must increment on payload-read timeout"
        );
    }
}
