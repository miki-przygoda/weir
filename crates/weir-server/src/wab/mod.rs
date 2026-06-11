pub mod format;
pub mod recovery;
pub mod segment;

use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use crate::models::Batch;
use format::{EXT_SEALED, FORMAT_VERSION, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN};
use recovery::{check_confirmed, recover_open_segments};
use segment::ShardWriter;
use weir_core::{Durability, MAX_PAYLOAD_HARD_CAP, Payload};

/// Configuration for the WAB subsystem.
pub struct WabConfig {
    /// Number of shards (one flusher thread per shard).
    pub shard_count: usize,
    /// Maximum number of records per flush batch.
    pub batch_size: usize,
    /// Maximum time to accumulate a batch before flushing.
    pub batch_deadline: Duration,
    /// Rotation threshold in bytes. The active segment is sealed and a new one
    /// opened once `bytes_written` reaches this value. Default 256 MiB matches
    /// the historical hard-coded behaviour; tests and storage-constrained
    /// deployments may set it lower.
    pub segment_max_bytes: u64,
}

impl Default for WabConfig {
    fn default() -> Self {
        WabConfig {
            shard_count: 1,
            batch_size: 256,
            batch_deadline: Duration::from_millis(1),
            segment_max_bytes: crate::wab::format::SEGMENT_MAX_BYTES,
        }
    }
}

/// Returned by `spawn`. Drop `shard_txs` to initiate shutdown (flusher threads
/// exit when their receiver disconnects), then join the handles to wait for all
/// segments to be sealed.
pub struct WabHandle {
    /// One sender per shard. Drop all of them to signal shutdown.
    pub shard_txs: Vec<Sender<Batch>>,
    pub join_handles: Vec<thread::JoinHandle<()>>,
}

fn shard_dir_path(wab_dir: &Path, shard_id: usize) -> PathBuf {
    wab_dir.join(format!("shard_{shard_id:02}"))
}

/// Best-effort string extraction from a `catch_unwind` payload. `panic!` with
/// a string literal lands in `&'static str`; `panic!("{}", ...)` lands in
/// `String`. Anything else gets a placeholder so the log line still says
/// *something*.
fn panic_message_str(payload: &Box<dyn std::any::Any + Send>) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return s;
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.as_str();
    }
    "<non-string panic payload>"
}

/// Maximum number of times a single shard's flusher will be respawned
/// after panicking before we give up and leave the shard offline. Each
/// respawn bumps `weir_wab_flusher_panics` and emits a warn-level log
/// with the panic payload and the attempt number; reaching the cap
/// promotes the final log to error-level so it's visible in any
/// reasonable log retention window.
///
/// The cap exists to bound damage from a runaway loop — a flusher
/// that panics deterministically on every record would otherwise spin
/// forever, burning CPU and producing an unbounded log stream. With
/// the cap, the shard goes permanently offline (Nack-everything) after
/// `MAX_FLUSHER_RESPAWNS` attempts; the previous behaviour (offline
/// after the first panic) is the upper bound of how-bad-can-it-get.
pub(crate) const MAX_FLUSHER_RESPAWNS: u32 = 10;

/// Wraps a flusher body in `catch_unwind` and, on panic, respawns the
/// flusher up to `MAX_FLUSHER_RESPAWNS` times. Each respawn:
///   - bumps `weir_wab_flusher_panics`,
///   - logs the panic payload + attempt at warn level,
///   - sleeps `attempt * 100 ms` before the next attempt (linear
///     backoff, capped by the respawn cap itself).
///
/// On reaching the cap, the shard is left offline — records routed to
/// it Nack(InternalError) until daemon restart, the same end state as
/// the original non-respawning implementation, just delayed by N
/// attempts.
///
/// `body_factory` is invoked once per attempt to produce a fresh closure
/// so the caller can clone any per-attempt state (channels are
/// `Clone`able shared handles, so the SAME channels feed every
/// attempt — a panicking flusher does not lose in-flight records,
/// they sit in the bounded queue until the respawned flusher drains
/// them).
///
/// `AssertUnwindSafe`: the flusher's only shared mutable state across
/// the call boundary is via `Arc<Metrics>` (atomic counters) and the
/// crossbeam channels (lock-free, panic-safe). We accept the
/// unwind-safety claim — matching the previous implementation's
/// rationale.
fn run_with_panic_supervision<F, B>(shard_id: usize, metrics: Arc<Metrics>, mut body_factory: F)
where
    F: FnMut() -> B,
    B: FnOnce(),
{
    let mut attempt: u32 = 0;
    loop {
        let body = body_factory();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
        match result {
            // Clean exit (channel disconnected at daemon shutdown) —
            // the supervisor is done, NOT a panic.
            Ok(()) => return,
            Err(panic_payload) => {
                let msg = panic_message_str(&panic_payload);
                metrics.wab_flusher_panics.inc();
                attempt += 1;
                if attempt > MAX_FLUSHER_RESPAWNS {
                    tracing::error!(
                        shard = shard_id,
                        attempts = attempt - 1,
                        panic = %msg,
                        "WAB flusher panicked {MAX_FLUSHER_RESPAWNS} times — \
                         shard now PERMANENTLY offline. Records routed to \
                         this shard will Nack(InternalError) until the \
                         daemon is restarted."
                    );
                    return;
                }
                tracing::warn!(
                    shard = shard_id,
                    attempt,
                    panic = %msg,
                    "WAB flusher panicked — respawning (attempt {attempt} of {MAX_FLUSHER_RESPAWNS})"
                );
                // Linear backoff: 10 ms × attempt. Worst-case total
                // sleep across the full respawn loop is
                // sum(10..=100 ms) ≈ 550 ms — fast enough to keep
                // the cap-out test snappy, slow enough that a
                // deterministically-panicking flusher doesn't melt
                // a core. Production impact is minimal: real flusher
                // panics are from logical bugs that respawning won't
                // fix, so the loop's job is mostly to surface a
                // clean "now permanently offline" log line within
                // about a second.
                thread::sleep(Duration::from_millis(10 * u64::from(attempt)));
            }
        }
    }
}

/// Creates a directory (and all parents) with mode `0o700` on Unix.
/// On non-Unix platforms falls back to `create_dir_all` with the process umask.
pub(crate) fn create_dir_private(path: PathBuf) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(&path)
    }
}

/// Runs crash recovery, replays sealed-but-unconfirmed segments to `drain_tx`,
/// then spawns one flusher thread per shard.
pub fn spawn(
    wab_dir: PathBuf,
    config: WabConfig,
    drain_tx: Sender<PathBuf>,
    metrics: Arc<Metrics>,
) -> io::Result<WabHandle> {
    // Caller (`Config::load`) has already validated and canonicalised `wab_dir`.

    for shard_id in 0..config.shard_count {
        create_dir_private(shard_dir_path(&wab_dir, shard_id))?;
    }

    // Phase 1 (calling thread): crash recovery — unsealed .wab → .wab.sealed
    recover_open_segments(&wab_dir, &metrics)?;

    // Phase 2 (calling thread): replay sealed-but-unconfirmed segments
    replay_unconfirmed(&wab_dir, config.shard_count, &drain_tx, &metrics)?;

    // Phase 3: one flusher thread per shard
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut shard_txs = Vec::with_capacity(config.shard_count);
    let mut join_handles = Vec::with_capacity(config.shard_count);

    for shard_id in 0..config.shard_count {
        let (tx, rx) = crossbeam_channel::bounded::<Batch>(config.batch_size * 4);
        shard_txs.push(tx);

        let sdir = shard_dir_path(&wab_dir, shard_id);
        let drain_clone = drain_tx.clone();
        let metrics_for_flusher = Arc::clone(&metrics);
        let metrics_for_panic = Arc::clone(&metrics);
        let batch_size = config.batch_size;
        let batch_deadline = config.batch_deadline;
        let segment_max_bytes = config.segment_max_bytes;
        let core_id = core_ids.get(shard_id % core_ids.len().max(1)).copied();

        let handle = thread::Builder::new()
            .name(format!("wab-flusher-{shard_id}"))
            .spawn(move || {
                // All per-attempt inputs are kept in the OUTER scope so a
                // panic inside `catch_unwind` doesn't drop them. The body
                // factory clones the channel / Arc / PathBuf handles for
                // each attempt — channel clones share the same queue, so
                // Batches buffered in the bounded queue survive a flusher
                // panic and are drained by the respawned flusher in the
                // same order.
                run_with_panic_supervision(shard_id, metrics_for_panic, || {
                    let sdir = sdir.clone();
                    let rx = rx.clone();
                    let drain_clone = drain_clone.clone();
                    let metrics_for_flusher = Arc::clone(&metrics_for_flusher);
                    move || {
                        flusher_thread(
                            shard_id as u16,
                            sdir,
                            rx,
                            drain_clone,
                            batch_size,
                            batch_deadline,
                            segment_max_bytes,
                            core_id,
                            metrics_for_flusher,
                        );
                    }
                });
            })
            .map_err(io::Error::other)?;

        join_handles.push(handle);
    }

    Ok(WabHandle {
        shard_txs,
        join_handles,
    })
}

fn replay_unconfirmed(
    wab_dir: &Path,
    shard_count: usize,
    drain_tx: &Sender<PathBuf>,
    metrics: &Arc<Metrics>,
) -> io::Result<()> {
    for shard_id in 0..shard_count {
        let sdir = shard_dir_path(wab_dir, shard_id);
        if !sdir.exists() {
            continue;
        }
        let mut sealed_segments: Vec<PathBuf> = fs::read_dir(&sdir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with(EXT_SEALED))
            .collect();
        sealed_segments.sort(); // ascending counter order

        for sealed in sealed_segments {
            match check_confirmed(&sealed, wab_dir) {
                Ok(true) => {
                    info!(sealed = %sealed.display(), "skipping replay — segment already confirmed");
                }
                Ok(false) => {
                    let record_count = read_segment_record_count(&sealed).unwrap_or(0);
                    info!(sealed = %sealed.display(), records = record_count, "queuing segment for drain replay");
                    metrics.recovery_records_replayed.inc_by(record_count);
                    drain_tx.send(sealed).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "drain channel closed during startup replay",
                        )
                    })?;
                }
                Err(e) => {
                    // check_confirmed quarantined the segment; skip it.
                    warn!(error = %e, "skipping quarantined segment during replay");
                }
            }
        }
    }
    Ok(())
}

/// Reads `record_count` from the footer of a sealed segment file without reading
/// all records. The footer occupies the last `SEGMENT_FOOTER_LEN` bytes; its first
/// 8 bytes are `record_count` as a u64 LE (see wab/format.rs for the layout).
fn read_segment_record_count(path: &Path) -> io::Result<u64> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::End(-(SEGMENT_FOOTER_LEN as i64)))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)?;
    Ok(u64::from_le_bytes(footer))
}

#[allow(clippy::too_many_arguments)]
fn flusher_thread(
    shard_id: u16,
    shard_dir: PathBuf,
    work_rx: Receiver<Batch>,
    drain_tx: Sender<PathBuf>,
    batch_size: usize,
    batch_deadline: Duration,
    segment_max_bytes: u64,
    core_id: Option<core_affinity::CoreId>,
    metrics: Arc<Metrics>,
) {
    // Core affinity (fail-open: log and continue if denied)
    if let Some(id) = core_id
        && !core_affinity::set_for_current(id)
    {
        warn!(
            shard = shard_id,
            "failed to set CPU affinity; continuing without affinity"
        );
    }

    // SCHED_FIFO (Linux only, requires CAP_SYS_NICE; fail-open)
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param { sched_priority: 1 };
        let ret = unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) };
        if ret == -1 {
            warn!(
                shard = shard_id,
                "failed to set SCHED_FIFO; continuing with default scheduler"
            );
        }
    }

    // Startup warmup
    let mut writer = ShardWriter::new(shard_id, shard_dir, segment_max_bytes, Arc::clone(&metrics));
    if let Err(e) = writer.scan_and_advance_counter() {
        warn!(shard = shard_id, error = %e, "failed to scan segment counters; starting at 1");
    }

    // Scratch buffer pre-touch: fault in the backing page before the first record arrives.
    // push + clear is preferred over write_volatile — safe Rust, identical effect.
    let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
    scratch.push(0u8);
    scratch.clear();

    // CRC/SIMD warmup: prime the instruction cache.
    let _ = crc32fast::hash(&scratch);

    info!(shard = shard_id, "WAB flusher started");

    // Accumulate multiple Batches per fsync to preserve cross-batch coalescing.
    // record_count tracks the total records accumulated so we stop draining once
    // we reach batch_size (bounding memory and latency under very high load).
    let mut batches: Vec<Batch> = Vec::new();
    let mut record_count = 0usize;

    loop {
        // Block on the first Batch (or detect channel close / deadline).
        match work_rx.recv_timeout(batch_deadline) {
            Ok(batch) => {
                record_count += batch.records.len();
                batches.push(batch);
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if !batches.is_empty() {
                    flush_batch(&mut writer, &mut batches, &drain_tx, shard_id, &metrics);
                    record_count = 0;
                }
                continue;
            }
        }

        // Drain any additional available Batches up to batch_size records total.
        // Using try_recv (non-blocking) preserves today's coalescing without
        // introducing a second timed wait.
        while record_count < batch_size {
            match work_rx.try_recv() {
                Ok(batch) => {
                    record_count += batch.records.len();
                    batches.push(batch);
                }
                Err(_) => break,
            }
        }

        flush_batch(&mut writer, &mut batches, &drain_tx, shard_id, &metrics);
        record_count = 0;
    }

    // Graceful shutdown: seal the active segment and send to drain.
    match writer.seal_current() {
        Ok(Some(sealed)) => {
            info!(shard = shard_id, sealed = %sealed.display(), "WAB flusher sealed segment on shutdown");
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::sealed,
                })
                .inc();
            drain_tx.send(sealed).ok();
        }
        Ok(None) => {
            info!(
                shard = shard_id,
                "WAB flusher shut down with no active segment"
            );
        }
        Err(e) => {
            tracing::error!(shard = shard_id, error = %e, "WAB flusher failed to seal segment on shutdown");
        }
    }
}

fn flush_batch(
    writer: &mut ShardWriter,
    batches: &mut Vec<Batch>,
    drain_tx: &Sender<PathBuf>,
    shard_id: u16,
    metrics: &Arc<Metrics>,
) {
    // Sync records used to fsync per-record; now they ride the same
    // group-fsync as Batched. Sync's contract is "fsync before ack" and
    // a single fsync at the end of the batch covers every record written
    // during it, so the contract still holds while the fsync rate drops
    // by up to batch_size× under concurrent Sync load (the previous
    // per-record fsync was the bottleneck in the herd benchmarks).
    let mut sync_acks: Vec<oneshot::Sender<bool>> = Vec::new();
    let mut batched_acks: Vec<oneshot::Sender<bool>> = Vec::new();
    let mut need_fsync = false;

    // bench-trace: per-record enqueued_at for stage_total observation after
    // the fsync. Cleared alongside sync/batched_acks.
    #[cfg(feature = "bench-trace")]
    let mut sync_ts: Vec<Instant> = Vec::new();
    #[cfg(feature = "bench-trace")]
    let mut batched_ts: Vec<Instant> = Vec::new();

    for batch in batches.drain(..) {
        // worker_flushed_at is the instant the worker stamped the Batch.
        // All records in this Batch share the same stamp.
        #[cfg(feature = "bench-trace")]
        let worker_flushed_at = batch.flushed_at;

        for unit in batch.records {
            // Capture flusher-received-at and observe queue + bridge_wait stages.
            #[cfg(feature = "bench-trace")]
            let flusher_recv_at = {
                let now = Instant::now();
                metrics
                    .stage_queue
                    .observe((worker_flushed_at - unit.enqueued_at).as_secs_f64());
                metrics
                    .stage_bridge_wait
                    .observe((now - worker_flushed_at).as_secs_f64());
                now
            };

            // write_record returns Some(sealed_path) when the segment rotated.
            let Ok(rotation) = writer.write_record(&unit.payload) else {
                let _ = unit.ack_tx.send(false);
                continue;
            };

            // Observe the write stage (pre-fsync).
            #[cfg(feature = "bench-trace")]
            metrics
                .stage_write
                .observe(flusher_recv_at.elapsed().as_secs_f64());

            if let Some(sealed) = rotation {
                // Segment was sealed (seal includes fsync). Notify drain.
                info!(shard = shard_id, sealed = %sealed.display(), "WAB segment rotated");
                metrics
                    .wab_segments
                    .get_or_create(&SegmentStateLabel {
                        state: SegmentState::sealed,
                    })
                    .inc();
                drain_tx.send(sealed).ok();
            }

            match unit.durability {
                Durability::Sync => {
                    need_fsync = true;
                    sync_acks.push(unit.ack_tx);
                    #[cfg(feature = "bench-trace")]
                    sync_ts.push(unit.enqueued_at);
                }
                Durability::Batched => {
                    need_fsync = true;
                    batched_acks.push(unit.ack_tx);
                    #[cfg(feature = "bench-trace")]
                    batched_ts.push(unit.enqueued_at);
                }
                Durability::Buffered => {
                    // Observe stage_total for buffered records (ack fires immediately).
                    #[cfg(feature = "bench-trace")]
                    metrics
                        .stage_total
                        .observe(unit.enqueued_at.elapsed().as_secs_f64());
                    let _ = unit.ack_tx.send(true);
                }
            }
        }
    }

    // Group fsync covering every Sync and Batched record written during
    // this flush. One fsync per batch instead of one per Sync record;
    // both tiers' acks fire after it completes.
    if need_fsync {
        let ok = fsync_observed(writer, shard_id, metrics);
        // Observe stage_total for Sync + Batched records (ack fires after fsync).
        #[cfg(feature = "bench-trace")]
        for enqueued_at in sync_ts.into_iter().chain(batched_ts) {
            metrics
                .stage_total
                .observe(enqueued_at.elapsed().as_secs_f64());
        }
        for ack_tx in sync_acks.into_iter().chain(batched_acks) {
            let _ = ack_tx.send(ok);
        }
    }
}

/// Fsyncs the active segment, observing the duration and recording any error
/// through both a tracing log line (so operators see the underlying
/// io::Error string) and a Prometheus counter (so the failure rate is
/// alertable). Returns the bool the caller propagates to ack_tx.
fn fsync_observed(writer: &mut ShardWriter, shard_id: u16, metrics: &Arc<Metrics>) -> bool {
    let t = Instant::now();
    let result = writer.fsync_current();
    metrics
        .wab_fsync_duration
        .observe(t.elapsed().as_secs_f64());
    match result {
        Ok(()) => true,
        Err(e) => {
            tracing::error!(
                shard = shard_id,
                error = %e,
                "WAB fsync failed — durability hazard; the record cannot be \
                 guaranteed durable on stable storage. Producer receives \
                 Nack(InternalError); operator should investigate the \
                 underlying disk/filesystem."
            );
            metrics.wab_fsync_failures.inc();
            false
        }
    }
}

/// An iterator over records in a sealed WAB segment file.
///
/// Streams records without materialising the whole segment. Applies
/// `MAX_PAYLOAD_HARD_CAP` before every heap allocation to bound memory usage
/// during recovery. Stops at the end-of-records sentinel or on the first error.
pub struct SegmentReader {
    reader: BufReader<File>,
    done: bool,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header = [0u8; SEGMENT_HEADER_LEN];
        reader.read_exact(&mut header)?;

        if &header[0..4] != b"WEIR" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad segment magic: {:?}", &header[0..4]),
            ));
        }
        if header[4] != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown segment format version: {}", header[4]),
            ));
        }

        Ok(SegmentReader {
            reader,
            done: false,
        })
    }
}

impl Iterator for SegmentReader {
    type Item = io::Result<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            self.done = true;
            return None; // sentinel
        }

        // Cap check before allocation — MAX_PAYLOAD_HARD_CAP from weir-core.
        if payload_len > MAX_PAYLOAD_HARD_CAP {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record payload_len {payload_len} exceeds MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"
                ),
            )));
        }

        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            self.done = true;
            return Some(Err(e));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut payload_buf = vec![0u8; payload_len];
        if let Err(e) = self.reader.read_exact(&mut payload_buf) {
            self.done = true;
            return Some(Err(e));
        }

        let computed_crc = crc32fast::hash(&payload_buf);
        if expected_crc != computed_crc {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record CRC mismatch: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
                ),
            )));
        }

        // Freeze: O(1) ownership transfer from Vec allocation to Bytes.
        Some(Ok(Payload::from(payload_buf)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wab::segment::{WabSegment, segment_path};
    use std::fs;

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weir_wab_{label}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ── SegmentReader ─────────────────────────────────────────────────────────

    #[test]
    fn segment_reader_round_trip() {
        let dir = tmp_dir("rdroundtrip");
        let path = segment_path(&dir, 1);
        let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma delta"];
        let mut seg = WabSegment::create(&path, 0).unwrap();
        for p in &payloads {
            seg.write_record(p).unwrap();
        }
        let sealed = seg.seal().unwrap();

        let got: Vec<Payload> = SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(got, payloads);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn segment_reader_detects_crc_mismatch() {
        let dir = tmp_dir("rdcrc");
        let path = segment_path(&dir, 1);
        let mut seg = WabSegment::create(&path, 0).unwrap();
        seg.write_record(b"data").unwrap();
        let sealed = seg.seal().unwrap();

        // Flip a bit in the payload bytes.
        // Layout: 24 header + 4 payload_len + 4 crc = offset 32 is start of payload.
        let mut bytes = fs::read(&sealed).unwrap();
        bytes[32] ^= 0xff;
        fs::write(&sealed, &bytes).unwrap();

        let mut reader = SegmentReader::open(&sealed).unwrap();
        assert!(reader.next().unwrap().is_err());
        fs::remove_dir_all(dir).ok();
    }

    // ── Panic supervision ─────────────────────────────────────────────────────

    /// Helper: build a body factory that panics on the first N attempts
    /// (with `panic_payload`) and returns cleanly on attempt N+1. Lets
    /// the existing panic-catching tests stay one-panic shaped while
    /// the new respawn tests dial N up.
    fn panic_then_recover_factory(
        n_panics: u32,
        panic_payload: &'static str,
    ) -> impl FnMut() -> Box<dyn FnOnce() + Send> {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        move || {
            let counter = std::sync::Arc::clone(&counter);
            let payload = panic_payload;
            Box::new(move || {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n < n_panics {
                    panic!("{payload}");
                }
                // clean return on the (n_panics + 1)th attempt
            })
        }
    }

    #[test]
    fn supervisor_catches_str_panic_and_increments_metric() {
        let (m, _reg) = crate::metrics::Metrics::new();
        let m = Arc::new(m);
        // One panic then clean recovery — verify the supervisor caught
        // it and bumped the metric exactly once.
        run_with_panic_supervision(0, Arc::clone(&m), panic_then_recover_factory(1, "boom"));
        assert_eq!(m.wab_flusher_panics.get(), 1);
    }

    #[test]
    fn supervisor_catches_formatted_panic_and_increments_metric() {
        // panic!("{}", ...) lands in String, not &'static str — verify the
        // downcast covers both shapes. The dynamic-string case is now
        // tested via `panic_then_recover_factory` (the payload is a
        // &'static str but the panic macro materialises it through the
        // formatter path, producing a String payload).
        let (m, _reg) = crate::metrics::Metrics::new();
        let m = Arc::new(m);
        let shard = 7;
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        run_with_panic_supervision(shard, Arc::clone(&m), move || {
            let counter = std::sync::Arc::clone(&counter);
            move || {
                if counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    panic!("panic from shard {shard}");
                }
            }
        });
        assert_eq!(m.wab_flusher_panics.get(), 1);
    }

    #[test]
    fn supervisor_lets_clean_exit_pass_through() {
        let (m, _reg) = crate::metrics::Metrics::new();
        let m = Arc::new(m);
        run_with_panic_supervision(0, Arc::clone(&m), || {
            || { /* normal return on first attempt */ }
        });
        assert_eq!(m.wab_flusher_panics.get(), 0);
    }

    /// Three transient panics then a clean recovery. Verifies the
    /// supervisor respawns the flusher rather than leaving the shard
    /// offline after the first panic (the pre-respawn behaviour). The
    /// metric records every panic — three of them — but the loop
    /// continues past each and ultimately terminates cleanly.
    #[test]
    fn flusher_panic_respawn_recovers_within_cap() {
        let (m, _reg) = crate::metrics::Metrics::new();
        let m = Arc::new(m);
        run_with_panic_supervision(
            0,
            Arc::clone(&m),
            panic_then_recover_factory(3, "transient boom"),
        );
        assert_eq!(
            m.wab_flusher_panics.get(),
            3,
            "metric should record every panic, not just the first"
        );
    }

    /// A flusher that panics deterministically on every attempt — the
    /// supervisor must give up after `MAX_FLUSHER_RESPAWNS` retries
    /// rather than spinning forever. Verifies the cap-out terminates
    /// the loop and that the panic metric reflects every attempt.
    /// Note the wall-clock budget: linear backoff totals
    /// sum(10..=100 ms) ≈ 550 ms.
    #[test]
    fn flusher_panic_loop_caps_out_after_max_respawns() {
        let (m, _reg) = crate::metrics::Metrics::new();
        let m = Arc::new(m);
        let start = std::time::Instant::now();
        run_with_panic_supervision(0, Arc::clone(&m), || {
            // Every attempt panics — never recovers.
            || panic!("persistent panic")
        });
        let elapsed = start.elapsed();
        assert_eq!(
            m.wab_flusher_panics.get(),
            u64::from(MAX_FLUSHER_RESPAWNS + 1),
            "metric records initial attempt plus each respawn before cap-out"
        );
        // Sanity: the loop did exit within a reasonable time. If the
        // cap stopped working the test would hang.
        assert!(
            elapsed < Duration::from_secs(5),
            "respawn loop should terminate within ~1 s, took {elapsed:?}"
        );
    }

    #[test]
    fn panic_message_str_handles_known_payload_shapes() {
        // Construct payloads the same way `panic!` does, then box them as
        // `dyn Any + Send` to match the catch_unwind signature.
        let str_payload: Box<dyn std::any::Any + Send> = Box::new("static str panic");
        assert_eq!(panic_message_str(&str_payload), "static str panic");

        let string_payload: Box<dyn std::any::Any + Send> =
            Box::new(String::from("owned string panic"));
        assert_eq!(panic_message_str(&string_payload), "owned string panic");

        // Non-string payload — must not panic the panic-message extractor itself.
        let int_payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(
            panic_message_str(&int_payload),
            "<non-string panic payload>"
        );
    }
}
