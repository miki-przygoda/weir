use std::{io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
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
    /// Deadline applied to each `read_exact` phase of a frame — the 16-byte
    /// header, the payload, and the 4-byte CRC are each bounded by this value.
    /// It is a whole-phase deadline, not a strict per-byte idle timer: a client
    /// that never sends (slowloris) and a client streaming a large payload
    /// pathologically slowly (e.g. <~`payload_len`/`read_timeout` B/s) are both
    /// cut off, so a connection can't hold a semaphore permit indefinitely. The
    /// `weir_connection_idle_timeout` counter is bumped in either case.
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
pub async fn handle_connection<S>(
    stream: S,
    queue_tx: QueueSender<WorkUnit>,
    config: ConnectionConfig,
    metrics: Arc<Metrics>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
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
                send_nack(stream.get_mut(), wire, extra, config.read_timeout).await?;
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
        let payload_len = header.payload_len() as usize;
        let cap = config.max_payload_bytes.min(MAX_PAYLOAD_HARD_CAP);
        if payload_len > cap {
            let tv = durability_to_tier(header.durability());
            send_nack(
                stream.get_mut(),
                WireNack::PayloadTooLarge,
                &[],
                config.read_timeout,
            )
            .await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::payload_too_large,
                })
                .inc();
            return Ok(());
        }
        if payload_len == 0 && header.message_type() == MessageType::Push {
            // An empty Push payload can't be represented in the WAB: a zero
            // length prefix is the end-of-records sentinel, so storing one would
            // truncate the segment (silently dropping records written after it).
            // Reject at ingest rather than let it reach the WAB. This applies
            // ONLY to Push — a HealthCheck frame legitimately carries a
            // zero-length payload (see docs/wire_protocol.md) and must pass
            // through to the dispatch below.
            let tv = durability_to_tier(header.durability());
            send_nack(
                stream.get_mut(),
                WireNack::EmptyPayload,
                &[],
                config.read_timeout,
            )
            .await?;
            metrics
                .records_nack
                .get_or_create(&NackLabel {
                    tier: tv,
                    reason: MetricNack::empty_payload,
                })
                .inc();
            return Ok(());
        }

        // ── 4. Read payload ──────────────────────────────────────────────────
        // Accumulate into Vec<u8> then freeze to Bytes (O(1) ownership transfer).
        // Raced against shutdown like the header read: a client stalled mid-frame
        // at shutdown must not hold its semaphore permit until read_timeout and
        // eat the drain window. No ack/nack has been sent for this frame, so the
        // client sees the dropped connection as a failed push and retries — no
        // false ack, no data loss (F26).
        let mut payload_buf = vec![0u8; payload_len];
        let payload_read = tokio::select! {
            biased;
            res = tokio::time::timeout(config.read_timeout, stream.read_exact(&mut payload_buf)) => res,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        match payload_read {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                metrics.connection_idle_timeout.inc();
                debug!(
                    payload_len,
                    timeout_secs = config.read_timeout.as_secs(),
                    "connection exceeded read_timeout during payload read; dropping"
                );
                return Ok(());
            }
        }
        // Freeze: O(1) ownership transfer from Vec allocation to Bytes.
        let payload = weir_core::Payload::from(payload_buf);

        // ── 5. Read and validate payload CRC ────────────────────────────────
        let mut crc_buf = [0u8; 4];
        let crc_read = tokio::select! {
            biased;
            res = tokio::time::timeout(config.read_timeout, stream.read_exact(&mut crc_buf)) => res,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        match crc_read {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                metrics.connection_idle_timeout.inc();
                debug!(
                    timeout_secs = config.read_timeout.as_secs(),
                    "connection exceeded read_timeout during CRC read; dropping"
                );
                return Ok(());
            }
        }
        let expected_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            let tv = durability_to_tier(header.durability());
            send_nack(
                stream.get_mut(),
                WireNack::BadPayloadCrc,
                &[],
                config.read_timeout,
            )
            .await?;
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
        match header.message_type() {
            MessageType::Push => {
                let tv = durability_to_tier(header.durability());
                metrics
                    .records_accepted
                    .get_or_create(&TierLabel { tier: tv.clone() })
                    .inc();
                handle_push(
                    stream.get_mut(),
                    queue_tx.clone(),
                    header.durability(),
                    payload,
                    tv,
                    config.shard_id,
                    config.ack_timeout,
                    config.read_timeout,
                    &metrics,
                )
                .await?;
            }
            MessageType::HealthCheck => {
                write_all_timeout(
                    stream.get_mut(),
                    healthcheck_response_frame_bytes(),
                    config.read_timeout,
                )
                .await?;
            }
            _ => {
                debug!(msg_type = ?header.message_type(), "unexpected message type from client");
                send_nack(
                    stream.get_mut(),
                    WireNack::InternalError,
                    &[],
                    config.read_timeout,
                )
                .await?;
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
async fn handle_push<S>(
    stream: &mut S,
    queue_tx: QueueSender<WorkUnit>,
    durability: Durability,
    payload: weir_core::Payload,
    tv: TierValue,
    shard_id: u32,
    ack_timeout: Duration,
    write_timeout: Duration,
    metrics: &Arc<Metrics>,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let unit = WorkUnit {
        shard_id,
        payload,
        durability,
        ack_tx,
        #[cfg(feature = "bench-trace")]
        enqueued_at: std::time::Instant::now(),
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
        send_nack(stream, WireNack::InternalError, &[], write_timeout).await?;
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
            send_ack(stream, write_timeout).await
        }
        Ok(_) => {
            // Flusher fired ack with `false` (write/fsync error) or dropped
            // the sender (panic). Either way: InternalError nack.
            send_nack(stream, WireNack::InternalError, &[], write_timeout).await?;
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
            send_nack(stream, WireNack::InternalError, &[], write_timeout).await?;
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
        // CRC-valid header but an unrecognised message_type/durability byte: a
        // PERMANENT client protocol error (version skew). Distinct from the
        // transient, keep-open InternalError so the client can tell the two apart
        // (F25). The connection is closed after the Nack (see the decode site).
        DecodeError::UnknownMessageType(_) | DecodeError::UnknownDurability(_) => {
            (WireNack::UnknownMessage, MetricNack::unknown_message, &[])
        }
        // Nonzero reserved flags: a permanent, connection-closing protocol error
        // (the frame set a bit that must be zero in wire v1) — F52.
        DecodeError::ReservedFlagsSet { .. } => (
            WireNack::ReservedFlagsSet,
            MetricNack::reserved_flags_set,
            &[],
        ),
        _ => (WireNack::InternalError, MetricNack::internal_error, &[]),
    }
}

/// Sends a Nack whose payload is `[reason_byte] ++ extra`.
///
/// For `VersionMismatch`, pass `extra = &[WIRE_VERSION]` so the client can
/// produce: "daemon is on wire protocol v{WIRE_VERSION}; this client is on vN."
async fn send_nack<S: AsyncWrite + Unpin>(
    stream: &mut S,
    reason: WireNack,
    extra: &[u8],
    write_timeout: Duration,
) -> io::Result<()> {
    let mut nack_payload = Vec::with_capacity(1 + extra.len());
    nack_payload.push(reason as u8);
    nack_payload.extend_from_slice(extra);

    let header = Header::new(MessageType::Nack, Durability::Sync, 0);
    let frame = Envelope::new(header, nack_payload).encode();
    write_all_timeout(stream, &frame, write_timeout).await
}

/// Writes a complete response frame, bounded by `write_timeout`. A client that
/// stops reading (or reads pathologically slowly) must not pin the connection —
/// and the semaphore permit it holds — indefinitely on a blocked write: with the
/// global connection cap, enough such clients would exhaust every permit. On
/// timeout the write surfaces as a `TimedOut` error, which propagates up and drops
/// the connection (releasing the permit), mirroring the read-side slowloris guard
/// that already bounds reads with the same `read_timeout` (S02).
async fn write_all_timeout<S: AsyncWrite + Unpin>(
    stream: &mut S,
    bytes: &[u8],
    write_timeout: Duration,
) -> io::Result<()> {
    match tokio::time::timeout(write_timeout, stream.write_all(bytes)).await {
        Ok(res) => res,
        Err(_elapsed) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "response write exceeded write timeout; dropping connection",
        )),
    }
}

/// The Ack frame's 20 bytes are entirely constant — fixed magic, fixed
/// version, fixed message_type, zero-length payload, header CRC over the
/// same fixed bytes, and empty-payload CRC = 0. Memoised on first call so
/// the steady state writes a borrowed `&'static [u8]` instead of
/// allocating + encoding + CRC-ing identical 20-byte buffers per response.
fn ack_frame_bytes() -> &'static [u8] {
    static ACK_FRAME: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    ACK_FRAME.get_or_init(|| {
        let header = Header::new(MessageType::Ack, Durability::Sync, 0);
        Envelope::new(header, Vec::new()).encode()
    })
}

/// Same memoisation story as [`ack_frame_bytes`] but for the
/// `HealthCheckResponse` frame (different message_type → different header
/// CRC, otherwise identical shape).
fn healthcheck_response_frame_bytes() -> &'static [u8] {
    static HEALTHCHECK_RESPONSE_FRAME: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    HEALTHCHECK_RESPONSE_FRAME.get_or_init(|| {
        let header = Header::new(MessageType::HealthCheckResponse, Durability::Sync, 0);
        Envelope::new(header, Vec::new()).encode()
    })
}

async fn send_ack<S: AsyncWrite + Unpin>(
    stream: &mut S,
    write_timeout: Duration,
) -> io::Result<()> {
    write_all_timeout(stream, ack_frame_bytes(), write_timeout).await
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

    /// Spawns a handler whose flusher resolves each work unit's ack oneshot with
    /// `ack` — `Some(true)` = durable, `Some(false)` = write/fsync failed, `None`
    /// = drop the unit (and its sender) without responding, modelling a flusher
    /// panic. Used to exercise the non-durable → Nack(InternalError) paths (S14).
    async fn spawn_handler_acking(cfg: ConnectionConfig, ack: Option<bool>) -> UnixStream {
        let (client, server) = UnixStream::pair().unwrap();
        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);

        std::thread::spawn(move || {
            let rx = queue_rx.get(0);
            while let Ok(unit) = rx.recv() {
                match ack {
                    Some(b) => {
                        let _ = unit.ack_tx.send(b);
                    }
                    None => drop(unit), // drop the sender without responding
                }
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

    #[tokio::test]
    async fn false_ack_from_flusher_returns_internal_error_nack() {
        let mut client = spawn_handler_acking(test_cfg(), Some(false)).await;
        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(
            payload[0],
            NackReason::InternalError as u8,
            "a non-durable (ack=false) flusher outcome must Nack(InternalError), never Ack"
        );
    }

    #[tokio::test]
    async fn dropped_ack_sender_returns_internal_error_nack() {
        let mut client = spawn_handler_acking(test_cfg(), None).await;
        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(
            payload[0],
            NackReason::InternalError as u8,
            "a dropped ack sender (flusher panic) must Nack(InternalError), never Ack"
        );
    }

    /// Encodes a complete Push frame (header + payload + payload CRC).
    fn push_frame(payload: &[u8]) -> Vec<u8> {
        let header = Header::new(MessageType::Push, Durability::Sync, 0);
        let env = Envelope::new(header, payload.to_vec());
        env.encode()
    }

    /// Builds an encoded Push header that DECLARES `payload_len` bytes without a
    /// matching payload — used to test the read/timeout/shutdown paths where the
    /// daemon waits for an advertised payload that never arrives. Header::new can
    /// no longer desync the declared length (F50), so patch the field + CRC.
    fn header_declaring(payload_len: u32) -> [u8; HEADER_LEN] {
        let mut h = Header::new(MessageType::Push, Durability::Sync, 0).encode();
        h[8..12].copy_from_slice(&payload_len.to_le_bytes());
        let crc = crc32fast::hash(&h[..12]);
        h[12..16].copy_from_slice(&crc.to_le_bytes());
        h
    }

    /// Reads one complete response frame (header + payload + 4-byte payload
    /// CRC) from any async-read stream, returning its MessageType and payload.
    ///
    /// Generic over the transport so the same helper drains the full 20-byte
    /// Ack frame whether the test runs over a `UnixStream` or an in-memory
    /// `tokio::io::duplex` pipe (see
    /// `handle_connection_works_over_non_unix_stream`).
    async fn read_response<R>(stream: &mut R) -> (MessageType, Vec<u8>)
    where
        R: AsyncRead + Unpin,
    {
        let mut header_buf = [0u8; HEADER_LEN];
        stream.read_exact(&mut header_buf).await.unwrap();
        let header = Header::decode(&header_buf).unwrap();
        let mut payload = vec![0u8; header.payload_len() as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }
        let mut crc_buf = [0u8; 4];
        stream.read_exact(&mut crc_buf).await.unwrap();
        (header.message_type(), payload)
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
    async fn unknown_message_type_returns_nack_unknown_message_and_closes() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        // Patch the message_type byte to an unrecognised value and recompute the
        // header CRC so the frame passes magic/version/CRC and reaches typed-field
        // parsing — exercising the UnknownMessageType → UnknownMessage path (F25).
        frame[5] = 0xff;
        let crc = crc32fast::hash(&frame[..12]).to_le_bytes();
        frame[12..16].copy_from_slice(&crc);
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::UnknownMessage as u8);
        // Permanent protocol error: the daemon closes the connection after the
        // Nack so a client cannot retry the identical (never-valid) frame.
        let mut buf = [0u8; 1];
        assert_eq!(
            client.read(&mut buf).await.unwrap(),
            0,
            "connection must close after an UnknownMessage Nack"
        );
    }

    #[tokio::test]
    async fn nonzero_flags_returns_nack_reserved_flags_and_closes() {
        let mut client = spawn_handler(test_cfg()).await;
        let mut frame = push_frame(b"data");
        // Set a bit in the reserved flags byte and recompute the header CRC so the
        // frame is structurally valid up to the flags check — exercising the
        // ReservedFlagsSet path (F52).
        frame[7] = 0x01;
        let crc = crc32fast::hash(&frame[..12]).to_le_bytes();
        frame[12..16].copy_from_slice(&crc);
        client.write_all(&frame).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::ReservedFlagsSet as u8);
        // Permanent protocol error: the daemon closes the connection after the Nack.
        let mut buf = [0u8; 1];
        assert_eq!(
            client.read(&mut buf).await.unwrap(),
            0,
            "connection must close after a ReservedFlagsSet Nack"
        );
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

        // Build a header claiming MAX_PAYLOAD_HARD_CAP + 1 bytes. Header::new can
        // no longer declare a length that disagrees with its payload (F50), so we
        // patch the encoded header's payload_len field + recompute the header CRC
        // to put the oversized declared length on the wire directly.
        let mut frame_header = Header::new(MessageType::Push, Durability::Sync, 0).encode();
        frame_header[8..12].copy_from_slice(&((MAX_PAYLOAD_HARD_CAP + 1) as u32).to_le_bytes());
        let crc = crc32fast::hash(&frame_header[..12]);
        frame_header[12..16].copy_from_slice(&crc.to_le_bytes());
        // Write just the header — server must reject before reading the payload.
        client.write_all(&frame_header).await.unwrap();
        // Dummy payload CRC placeholder (won't be read if cap check works).
        client.write_all(&[0u8; 4]).await.unwrap();

        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::PayloadTooLarge as u8);
    }

    #[tokio::test]
    async fn empty_payload_rejected_at_ingest() {
        // A Push claiming a zero-length payload: the WAB cannot represent it (the
        // length prefix is the end-of-records sentinel), so the server must Nack
        // it at ingest before it reaches the WAB rather than let it truncate a
        // segment.
        let mut client = spawn_handler(test_cfg()).await;
        let header = Header::new(MessageType::Push, Durability::Sync, 0);
        client.write_all(&header.encode()).await.unwrap();
        // Dummy payload CRC — not read; the empty-payload check fires first.
        client.write_all(&[0u8; 4]).await.unwrap();

        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(payload[0], NackReason::EmptyPayload as u8);
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

    /// Proves that `handle_connection` works over any `AsyncRead + AsyncWrite +
    /// Unpin + Send` transport, not just `UnixStream`. Uses a
    /// `tokio::io::duplex` in-memory pipe as the transport. A passing test
    /// here confirms the generic refactor doesn't break the happy-path Push→Ack
    /// flow.
    #[tokio::test]
    async fn handle_connection_works_over_non_unix_stream() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);

        let (queue_tx, queue_rx) = queue::new::<WorkUnit>(1);
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_secs(5),
            ack_timeout: Duration::from_millis(500),
            shard_id: 0,
        };

        // Auto-acker: blocking recv must run on an OS thread so it doesn't
        // stall the tokio runtime (same pattern as spawn_handler above).
        std::thread::spawn(move || {
            let rx = queue_rx.get(0);
            while let Ok(unit) = rx.recv() {
                let _ = unit.ack_tx.send(true);
            }
        });

        let handle = tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            never_shutdown_rx(),
        ));

        let frame = push_frame(b"hello");
        client.write_all(&frame).await.unwrap();

        // Drain the FULL 20-byte Ack frame (header + empty payload + CRC) via
        // the generic read_response helper — confirms the generic transport
        // refactor handles the complete response, not just the header.
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Ack);
        assert!(payload.is_empty(), "Ack frame carries no payload");

        drop(client);
        handle.await.unwrap().unwrap();
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
        let header = header_declaring(1024);
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

    /// F26: a client stalled mid-payload at shutdown must be released promptly
    /// via the shutdown race, not held until read_timeout. read_timeout is set
    /// to 30 s so the test would hang for that long if the payload read ignored
    /// the shutdown signal.
    #[tokio::test]
    async fn shutdown_releases_connection_stalled_mid_payload() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
        };
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Header advertising a 1 KiB payload, then send nothing → stall mid-frame.
        let header = header_declaring(1024);
        let handle = tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            shutdown_rx,
        ));
        client.write_all(&header).await.unwrap();
        // Let the handler consume the header and block on the payload read.
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("handler not released promptly on shutdown mid-payload")
            .expect("handler task panicked")
            .expect("handler returned Err");
    }

    /// F26: same as above but stalled mid-CRC (header + full payload received,
    /// CRC withheld). Guards the second of the two reads the fix touched.
    #[tokio::test]
    async fn shutdown_releases_connection_stalled_mid_crc() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let (queue_tx, _queue_rx) = queue::new::<WorkUnit>(1);
        let cfg = ConnectionConfig {
            max_payload_bytes: MAX_PAYLOAD_HARD_CAP,
            read_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
            shard_id: 0,
        };
        let (m, _reg) = crate::metrics::Metrics::new();
        let metrics = std::sync::Arc::new(m);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Header + the 5-byte payload, but withhold the 4-byte CRC → stall on CRC.
        let header = header_declaring(5);
        let handle = tokio::spawn(handle_connection(
            server,
            queue_tx,
            cfg,
            metrics,
            shutdown_rx,
        ));
        client.write_all(&header).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("handler not released promptly on shutdown mid-CRC")
            .expect("handler task panicked")
            .expect("handler returned Err");
    }

    // ── S02: response writes are bounded by a write timeout ─────────────────────

    /// An `AsyncWrite` that never accepts a byte — models a client that has
    /// stopped reading, so the socket send buffer is full and `write_all` would
    /// block forever.
    struct StalledWriter;
    impl AsyncWrite for StalledWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Pending
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Pending
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    #[tokio::test]
    async fn send_ack_is_bounded_by_write_timeout() {
        // A non-reading client must not pin the connection (and its permit) forever
        // on a blocked Ack write; the write is bounded by the write timeout (S02).
        let mut w = StalledWriter;
        let err = send_ack(&mut w, Duration::from_millis(50))
            .await
            .expect_err("a stalled writer must time out, not hang");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn send_nack_is_bounded_by_write_timeout() {
        let mut w = StalledWriter;
        let err = send_nack(
            &mut w,
            WireNack::InternalError,
            &[],
            Duration::from_millis(50),
        )
        .await
        .expect_err("a stalled writer must time out, not hang");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }
}
