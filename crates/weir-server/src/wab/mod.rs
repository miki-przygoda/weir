pub(crate) mod clock;
#[cfg(any(test, feature = "dst"))]
pub(crate) mod dst;
pub(crate) mod recovery;
pub(crate) mod segment;

// The on-disk segment format + reader live in the `weir-wab` crate (shared with
// weir-ctl, so there is exactly one parser). Re-exported here so the daemon's
// existing `crate::wab::format::…` and `crate::wab::SegmentReader` paths are
// unchanged.
pub(crate) use weir_wab::{SegmentReader, format};

use std::{
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use crate::models::Batch;
use clock::{BlockingClock, RealClock};
use recovery::{check_confirmed, recover_open_segments};
use segment::{FsSegmentStore, SegmentStore, ShardWriter};
use weir_core::Durability;
use weir_wab::format::{EXT_SEALED, SEGMENT_FOOTER_LEN};

/// Exponential moving average update, fixed-point microseconds.
/// alpha = 1/4 (new sample weighted 25%). Pure fn so it's unit-testable.
///
/// On fast NVMe (fsync ~150 µs) this converges to ~150–200 µs — close to
/// the old fixed 200 µs constant — so there is no throughput change on
/// local NVMe. The win shows on slower storage (cloud volumes, spinning
/// disks) where fsync latency is higher and a fixed 200 µs window is too
/// short, causing extra fsyncs. Throughput gain on slow storage deferred
/// to Linux/cloud validation.
pub(crate) fn ewma_update_us(current_us: u64, sample_us: u64) -> u64 {
    // current*3/4 + sample/4, integer math, no overflow for realistic µs.
    (current_us.saturating_mul(3) / 4).saturating_add(sample_us / 4)
}

/// Configuration for the WAB subsystem.
pub(crate) struct WabConfig {
    /// Number of shards (one flusher thread per shard).
    pub(crate) shard_count: usize,
    /// Maximum number of records per flush batch.
    pub(crate) batch_size: usize,
    /// Maximum time to accumulate a batch before flushing.
    pub(crate) batch_deadline: Duration,
    /// Rotation threshold in bytes. The active segment is sealed and a new one
    /// opened once `bytes_written` reaches this value. Default 256 MiB matches
    /// the historical hard-coded behaviour; tests and storage-constrained
    /// deployments may set it lower.
    pub(crate) segment_max_bytes: u64,
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
pub(crate) struct WabHandle {
    /// One sender per shard. Drop all of them to signal shutdown.
    pub(crate) shard_txs: Vec<Sender<Batch>>,
    pub(crate) join_handles: Vec<thread::JoinHandle<()>>,
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
fn run_with_panic_supervision<F, B, C>(
    shard_id: usize,
    metrics: Arc<Metrics>,
    clock: &C,
    mut body_factory: F,
) where
    F: FnMut() -> B,
    B: FnOnce(),
    C: BlockingClock,
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
                clock.sleep(Duration::from_millis(10 * u64::from(attempt)));
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
///
/// `coalesce_hint` is an `Arc<AtomicU64>` holding the EWMA of fsync latency in
/// microseconds (init 200). Each flusher thread updates it after every fsync;
/// the worker threads read it to size their coalesce window.
pub(crate) fn spawn(
    wab_dir: PathBuf,
    config: WabConfig,
    drain_tx: Sender<PathBuf>,
    metrics: Arc<Metrics>,
    coalesce_hint: Arc<AtomicU64>,
) -> io::Result<WabHandle> {
    // Caller (`Config::load`) has already validated and canonicalised `wab_dir`.

    for shard_id in 0..config.shard_count {
        create_dir_private(shard_dir_path(&wab_dir, shard_id))?;
    }

    // Phase 1 (calling thread): crash recovery — unsealed .wab → .wab.sealed
    recover_open_segments(&wab_dir, &metrics)?;

    // NOTE: replay of sealed-but-unconfirmed segments is intentionally NOT done
    // here. The caller invokes `replay_unconfirmed` AFTER the drain consumer is
    // spawned — otherwise the blocking sends into the bounded drain channel would
    // dead-lock the startup thread once the recovery backlog exceeds the channel
    // capacity and no consumer exists yet (B3).

    // Phase 2: one flusher thread per shard
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
        let coalesce_hint_for_flusher = Arc::clone(&coalesce_hint);

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
                run_with_panic_supervision(shard_id, metrics_for_panic, &RealClock, || {
                    let sdir = sdir.clone();
                    let rx = rx.clone();
                    let drain_clone = drain_clone.clone();
                    let metrics_for_flusher = Arc::clone(&metrics_for_flusher);
                    let coalesce_hint = Arc::clone(&coalesce_hint_for_flusher);
                    move || {
                        // Production filesystem backend; the DST harness drives
                        // `flusher_thread` directly with a fault-injecting store.
                        let store: Arc<dyn SegmentStore> = Arc::new(FsSegmentStore);
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
                            coalesce_hint,
                            &RealClock,
                            store,
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

/// Scans every shard directory under `wab_dir` for sealed-but-unconfirmed
/// segments (`.wab.sealed` with no `.confirmed` sidecar), returning their paths
/// in ascending per-shard counter order. Already-confirmed segments are skipped;
/// a segment that fails the confirmed check is quarantined by `check_confirmed`
/// and skipped. Shared by startup [`replay_unconfirmed`] and the drain's
/// sink-recovery rescan of stranded segments.
///
/// Enumerates EVERY shard directory on disk, not just `0..shard_count` —
/// scanning only the configured range would strand sealed-but-unconfirmed
/// segments in dirs whose index is >= shard_count after an operator REDUCED
/// shard_count across a restart (acked-durable data never replayed, S05). The
/// drain is shard-agnostic, so draining an orphaned dir's backlog is correct.
/// Dirent errors propagate (no `.ok()` filtering): a silently-skipped sealed
/// segment is silently-dropped acked data.
///
/// `shard_count` only drives the "backlog beyond the configured count" advisory;
/// pass `None` (e.g. from the recovery rescan) to skip it.
pub(crate) fn scan_unconfirmed_sealed(
    wab_dir: &Path,
    shard_count: Option<usize>,
) -> io::Result<Vec<PathBuf>> {
    let mut shard_dirs: Vec<PathBuf> = fs::read_dir(wab_dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    shard_dirs.sort();

    let mut unconfirmed = Vec::new();
    for sdir in shard_dirs {
        if !sdir.is_dir() {
            continue;
        }
        let name = sdir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        // Skip the daemon's reserved subdirs (mirrors recover_open_segments):
        // quarantine/ holds parked segments; dead_letter/ is owned by the
        // DeadLetterWriter and must not be replayed as a shard.
        if name == "quarantine" || name == "dead_letter" {
            continue;
        }
        if let Some(shard_count) = shard_count
            && let Some(idx) = name
                .strip_prefix("shard_")
                .and_then(|n| n.parse::<usize>().ok())
            && idx >= shard_count
        {
            warn!(
                shard_dir = %sdir.display(),
                idx,
                shard_count,
                "draining backlog from a shard directory beyond the configured shard_count (shard_count reduced across a restart?); records are recovered, not stranded"
            );
        }
        let mut sealed_segments: Vec<PathBuf> = fs::read_dir(&sdir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|p| p.to_string_lossy().ends_with(EXT_SEALED))
            .collect();
        sealed_segments.sort(); // ascending counter order

        for sealed in sealed_segments {
            match check_confirmed(&sealed, wab_dir) {
                Ok(true) => {
                    info!(sealed = %sealed.display(), "skipping — segment already confirmed");
                }
                Ok(false) => unconfirmed.push(sealed),
                Err(e) => {
                    // check_confirmed quarantined the segment; skip it.
                    warn!(error = %e, "skipping quarantined segment");
                }
            }
        }
    }
    Ok(unconfirmed)
}

/// Replays sealed-but-unconfirmed segments from a previous run by sending their
/// paths to the drain. MUST be called only after the drain consumer is running:
/// the sends block on the bounded drain channel, so with no consumer a backlog
/// larger than the channel capacity would dead-lock the caller (B3).
pub(crate) fn replay_unconfirmed(
    wab_dir: &Path,
    shard_count: usize,
    drain_tx: &Sender<PathBuf>,
    metrics: &Arc<Metrics>,
) -> io::Result<()> {
    for sealed in scan_unconfirmed_sealed(wab_dir, Some(shard_count))? {
        // A footer-read failure here only undercounts the
        // recovery_records_replayed metric (the segment is still queued +
        // delivered); surface it rather than silently reporting 0 so the
        // undercount is explainable.
        let record_count = match read_segment_record_count(&sealed) {
            Ok(n) => n,
            Err(e) => {
                warn!(sealed = %sealed.display(), error = %e, "could not read record count for replay metric; reporting 0");
                0
            }
        };
        info!(sealed = %sealed.display(), records = record_count, "queuing segment for drain replay");
        metrics.recovery_records_replayed.inc_by(record_count);
        drain_tx.send(sealed).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "drain channel closed during startup replay",
            )
        })?;
    }
    Ok(())
}

/// Reads `record_count` from the footer of a sealed segment file without reading
/// all records. The footer occupies the last `SEGMENT_FOOTER_LEN` bytes; its first
/// 8 bytes are `record_count` as a u64 LE (see wab/format.rs for the layout).
///
/// `pub(crate)` so the drain can cross-check the footer's authoritative count
/// against the number of records it actually read, catching a post-seal tail
/// truncation that the sequential reader would otherwise see as a clean end of
/// stream (S01).
pub(crate) fn read_segment_record_count(path: &Path) -> io::Result<u64> {
    let mut file = fs::File::open(path)?;
    // Guard the seek-from-end: a file shorter than the footer would seek to a
    // negative offset, which surfaces as a cryptic platform error ("Invalid
    // argument") rather than a clear cause. Reject it explicitly first.
    let len = file.metadata()?.len();
    if len < SEGMENT_FOOTER_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "segment '{}' is {len} bytes, shorter than the {SEGMENT_FOOTER_LEN}-byte footer",
                path.display()
            ),
        ));
    }
    file.seek(SeekFrom::End(-(SEGMENT_FOOTER_LEN as i64)))?;
    let mut footer = [0u8; 8];
    file.read_exact(&mut footer)?;
    Ok(u64::from_le_bytes(footer))
}

#[allow(clippy::too_many_arguments)]
fn flusher_thread<C: BlockingClock>(
    shard_id: u16,
    shard_dir: PathBuf,
    work_rx: Receiver<Batch>,
    drain_tx: Sender<PathBuf>,
    batch_size: usize,
    batch_deadline: Duration,
    segment_max_bytes: u64,
    core_id: Option<core_affinity::CoreId>,
    metrics: Arc<Metrics>,
    coalesce_hint: Arc<AtomicU64>,
    clock: &C,
    store: Arc<dyn SegmentStore>,
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
    let mut writer = ShardWriter::new_with_store(
        shard_id,
        shard_dir,
        segment_max_bytes,
        Arc::clone(&metrics),
        store,
    );
    // Establish the segment counter before sealing anything. A failure here is
    // NOT benign: defaulting to 1 while crash recovery has already sealed
    // undrained seg_NNNNNNNN.wab.sealed files would let the first seal's rename
    // overwrite one of them — silent loss of acked-but-undrained records (the F12
    // hole, re-reachable via a transient read_dir failure under fd/mem pressure —
    // G02). Retry briefly to ride out a transient blip; if it still fails, refuse
    // to seal at an unestablished counter and take the shard offline (return →
    // work_rx drops → workers Nack; no respawn). The crown invariant — never a
    // false ack — outranks this shard's availability; a restart re-runs recovery.
    {
        let mut attempt = 0u32;
        loop {
            match writer.scan_and_advance_counter() {
                Ok(()) => break,
                Err(e) if attempt < 3 => {
                    attempt += 1;
                    warn!(shard = shard_id, error = %e, attempt, "segment-counter scan failed; retrying before giving up");
                    clock.sleep(Duration::from_millis(50u64.saturating_mul(attempt as u64)));
                }
                Err(e) => {
                    error!(
                        shard = shard_id,
                        error = %e,
                        "segment-counter scan failed repeatedly; refusing to seal at an \
                         unestablished counter (would risk overwriting a recovered sealed \
                         segment) — taking shard offline; records will Nack until restart"
                    );
                    return;
                }
            }
        }
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
        match clock.recv_timeout(&work_rx, batch_deadline) {
            Ok(batch) => {
                record_count += batch.records.len();
                batches.push(batch);
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if !batches.is_empty() {
                    flush_batch(
                        &mut writer,
                        &mut batches,
                        &drain_tx,
                        shard_id,
                        &metrics,
                        &coalesce_hint,
                    );
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

        flush_batch(
            &mut writer,
            &mut batches,
            &drain_tx,
            shard_id,
            &metrics,
            &coalesce_hint,
        );
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
            if let Err(crossbeam_channel::SendError(unsent)) = drain_tx.send(sealed) {
                warn!(shard = shard_id, sealed = %unsent.display(), "drain channel closed on shutdown; sealed segment is durable and will be delivered on restart");
            }
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
    coalesce_hint: &Arc<AtomicU64>,
) {
    // Sync + Batched records both ride a single group fsync at the end of the
    // flush (the per-record fsync was the herd-benchmark bottleneck). Their acks
    // are split by the FATE of the segment each record landed in:
    //
    //   - `durable_acks` — records in a segment that ROTATED mid-flush. Rotation
    //     seals the segment, which fsyncs it, so those records are already on
    //     stable storage → ack `true` unconditionally (even if the active
    //     segment's later fsync fails).
    //   - `pending_acks` — records in the CURRENT active segment, not yet
    //     synced. They ack with the end-of-batch group fsync's result. Crucially,
    //     if a later mid-batch `write_record` error DROPS the active segment
    //     (`ShardWriter` sets `active = None`), every record collected for it is
    //     Nacked here — otherwise they would ride a group fsync of a *different*
    //     (or absent) segment and be falsely acked durable while their bytes sit
    //     in an abandoned, never-fsynced file (silent data loss on crash).
    let mut durable_acks: Vec<oneshot::Sender<bool>> = Vec::new();
    let mut pending_acks: Vec<oneshot::Sender<bool>> = Vec::new();

    // bench-trace: per-record enqueued_at for stage_total observation, split the
    // same way as the acks.
    #[cfg(feature = "bench-trace")]
    let mut durable_ts: Vec<Instant> = Vec::new();
    #[cfg(feature = "bench-trace")]
    let mut pending_ts: Vec<Instant> = Vec::new();

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
            let rotation = match writer.write_record(&unit.payload) {
                Ok(rotation) => rotation,
                Err(_) => {
                    // The active segment was dropped. Records already collected
                    // for it (pending_acks) are now in an abandoned, un-fsynced
                    // file — Nack them rather than let the group fsync below
                    // falsely ack them. Records in already-rotated (sealed +
                    // fsynced) segments are durable and untouched.
                    for ack_tx in pending_acks.drain(..) {
                        let _ = ack_tx.send(false);
                    }
                    #[cfg(feature = "bench-trace")]
                    pending_ts.clear();
                    let _ = unit.ack_tx.send(false);
                    continue;
                }
            };

            // Observe the write stage (pre-fsync).
            #[cfg(feature = "bench-trace")]
            metrics
                .stage_write
                .observe(flusher_recv_at.elapsed().as_secs_f64());

            let rotated = rotation.is_some();
            if let Some(sealed) = rotation {
                // Segment was sealed (seal includes fsync) — every record in it
                // is now durable: this triggering record plus all prior records
                // collected for that segment. Promote them and notify drain.
                info!(shard = shard_id, sealed = %sealed.display(), "WAB segment rotated");
                metrics
                    .wab_segments
                    .get_or_create(&SegmentStateLabel {
                        state: SegmentState::sealed,
                    })
                    .inc();
                if let Err(crossbeam_channel::SendError(unsent)) = drain_tx.send(sealed) {
                    warn!(shard = shard_id, sealed = %unsent.display(), "drain channel closed; sealed segment is durable and will be delivered on restart");
                }
                durable_acks.append(&mut pending_acks);
                #[cfg(feature = "bench-trace")]
                durable_ts.append(&mut pending_ts);
            }

            match unit.durability {
                Durability::Sync | Durability::Batched => {
                    // A record that triggered rotation is in the just-sealed
                    // (durable) segment; otherwise it's in the current active
                    // segment, awaiting the group fsync.
                    if rotated {
                        durable_acks.push(unit.ack_tx);
                        #[cfg(feature = "bench-trace")]
                        durable_ts.push(unit.enqueued_at);
                    } else {
                        pending_acks.push(unit.ack_tx);
                        #[cfg(feature = "bench-trace")]
                        pending_ts.push(unit.enqueued_at);
                    }
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

    // One group fsync covers every record still in the active segment; they ack
    // with its result. (If every fsync-tier record rotated out, pending_acks is
    // empty and we skip the now-redundant fsync.)
    if !pending_acks.is_empty() {
        let ok = fsync_observed(writer, shard_id, metrics, coalesce_hint);
        #[cfg(feature = "bench-trace")]
        for enqueued_at in pending_ts {
            metrics
                .stage_total
                .observe(enqueued_at.elapsed().as_secs_f64());
        }
        for ack_tx in pending_acks {
            let _ = ack_tx.send(ok);
        }
    }

    // Records in rotated (sealed + fsynced) segments are durable regardless of
    // the active segment's fsync outcome — ack them true.
    #[cfg(feature = "bench-trace")]
    for enqueued_at in durable_ts {
        metrics
            .stage_total
            .observe(enqueued_at.elapsed().as_secs_f64());
    }
    for ack_tx in durable_acks {
        let _ = ack_tx.send(true);
    }
}

/// Fsyncs the active segment, observing the duration and recording any error
/// through both a tracing log line (so operators see the underlying
/// io::Error string) and a Prometheus counter (so the failure rate is
/// alertable). Updates `coalesce_hint` with an EWMA of the observed fsync
/// duration so the worker can size its coalesce window dynamically. Returns
/// the bool the caller propagates to ack_tx.
fn fsync_observed(
    writer: &mut ShardWriter,
    shard_id: u16,
    metrics: &Arc<Metrics>,
    coalesce_hint: &Arc<AtomicU64>,
) -> bool {
    let t = Instant::now();
    let result = writer.fsync_current();
    let elapsed = t.elapsed();
    let sample_us = elapsed.as_micros() as u64;
    metrics.wab_fsync_duration.observe(elapsed.as_secs_f64());
    // Update the shared EWMA hint (Relaxed: heuristic, not a correctness signal).
    let cur = coalesce_hint.load(Relaxed);
    coalesce_hint.store(ewma_update_us(cur, sample_us), Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wab::segment::{WabSegment, segment_path};
    use std::fs;
    // SegmentReader (and the round-trip tests that pair it with WabSegment) live
    // here even though the reader itself moved to weir-wab — these exercise the
    // reader against the real writer. Payload + the cap are only used by them.
    use weir_core::{MAX_PAYLOAD_HARD_CAP, Payload};

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
    fn read_segment_record_count_rejects_short_file_clearly() {
        let dir = tmp_dir("shortfooter");
        let path = dir.join("truncated.wab.sealed");
        // Shorter than the footer — the seek-from-end would otherwise go
        // negative and yield a cryptic platform error.
        fs::write(&path, b"xy").unwrap();
        let err = read_segment_record_count(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");
        assert!(err.to_string().contains("shorter than"), "{err}");
        fs::remove_dir_all(dir).ok();
    }

    // ── B3: recovery replay must not dead-lock on the bounded drain channel ──
    #[test]
    fn replay_needs_a_live_consumer_for_backlog_over_channel_capacity() {
        // replay_unconfirmed blocking-sends each sealed-but-unconfirmed segment
        // into the bounded drain channel. If the backlog exceeds the channel
        // capacity and no consumer is draining, the send blocks forever — the
        // startup deadlock (B3). The fix is in the startup wiring: the drain
        // consumer is spawned BEFORE replay runs. This test characterises the
        // hazard — replay blocks without a consumer, and streams the whole backlog
        // once one exists.
        let dir = tmp_dir("replay_consumer");
        let shard_dir = shard_dir_path(&dir, 0);
        fs::create_dir_all(&shard_dir).unwrap();

        const N: u64 = 300; // > the bounded(256) drain channel capacity
        for i in 1..=N {
            let path = segment_path(&shard_dir, i);
            let mut seg = WabSegment::create(&path, 0).unwrap();
            seg.write_record(b"replayed").unwrap();
            seg.seal().unwrap();
        }

        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);
        let dir_for_replay = dir.clone();
        let replay = std::thread::spawn(move || {
            let metrics = Arc::new(crate::metrics::Metrics::new().0);
            replay_unconfirmed(&dir_for_replay, 1, &tx, &metrics).unwrap();
            let _ = done_tx.send(());
            // tx dropped here → the consumer below sees the channel disconnect.
        });

        // No consumer yet: replay fills the channel and blocks (the deadlock).
        assert!(
            done_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "replay must block on a full channel with no consumer (the B3 deadlock)"
        );

        // Live consumer → replay streams the remaining backlog and completes.
        let consumer = std::thread::spawn(move || {
            let mut count = 0u64;
            while rx.recv().is_ok() {
                count += 1;
            }
            count
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("replay must complete once a consumer drains the channel");
        let received = consumer.join().unwrap();
        replay.join().unwrap();
        assert_eq!(received, N, "every backlog segment must reach the drain");
        fs::remove_dir_all(dir).ok();
    }

    // ── S05: replay must drain shard dirs beyond the configured shard_count ──────
    #[test]
    fn replay_drains_shard_dirs_beyond_configured_count() {
        // An operator who REDUCES shard_count across a restart must not strand
        // sealed-but-unconfirmed segments in the now-orphaned shard dirs. Recovery
        // already seals active segments in every dir; replay must likewise scan
        // every dir, not just 0..shard_count (S05).
        let dir = tmp_dir("replay_orphan_shards");
        for s in 0..4u16 {
            let shard_dir = shard_dir_path(&dir, s as usize);
            fs::create_dir_all(&shard_dir).unwrap();
            let path = segment_path(&shard_dir, 1);
            let mut seg = WabSegment::create(&path, s).unwrap();
            seg.write_record(b"orphaned").unwrap();
            seg.seal().unwrap();
        }

        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
        let metrics = Arc::new(crate::metrics::Metrics::new().0);
        // Configured shard_count = 2, but 4 shard dirs hold backlog on disk.
        replay_unconfirmed(&dir, 2, &tx, &metrics).unwrap();
        drop(tx);

        let received: Vec<String> = rx.iter().map(|p| p.display().to_string()).collect();
        assert_eq!(
            received.len(),
            4,
            "all 4 shard dirs' segments must replay, including the 2 beyond shard_count"
        );
        assert!(
            received.iter().any(|n| n.contains("shard_02")),
            "orphaned shard_02 backlog must be drained, not stranded"
        );
        assert!(
            received.iter().any(|n| n.contains("shard_03")),
            "orphaned shard_03 backlog must be drained, not stranded"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T01): replay must SKIP a segment that already has a valid
    /// `.confirmed` sidecar (delivered + confirmed on a prior run, not yet GC'd)
    /// while still replaying its unconfirmed sibling — the at-least-once + dedup
    /// invariant's "don't re-deliver an already-confirmed segment" half.
    #[test]
    fn replay_skips_already_confirmed_segment() {
        use crate::wab::format::{build_confirmed, confirmed_path_for};
        let dir = tmp_dir("replay_skip_confirmed");
        let shard_dir = shard_dir_path(&dir, 0);
        fs::create_dir_all(&shard_dir).unwrap();

        // seg 1: sealed AND confirmed (delivered last run; segment not yet deleted).
        let p1 = segment_path(&shard_dir, 1);
        let mut s1 = WabSegment::create(&p1, 0).unwrap();
        s1.write_record(b"already-delivered").unwrap();
        let sealed1 = s1.seal().unwrap();
        fs::write(confirmed_path_for(&sealed1), build_confirmed(0, 1, 1)).unwrap();

        // seg 2: sealed, NOT confirmed (genuinely undrained).
        let p2 = segment_path(&shard_dir, 2);
        let mut s2 = WabSegment::create(&p2, 0).unwrap();
        s2.write_record(b"needs-delivery").unwrap();
        let sealed2 = s2.seal().unwrap();

        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
        let metrics = Arc::new(crate::metrics::Metrics::new().0);
        replay_unconfirmed(&dir, 1, &tx, &metrics).unwrap();
        drop(tx);

        let queued: Vec<PathBuf> = rx.iter().collect();
        assert_eq!(
            queued.len(),
            1,
            "only the unconfirmed segment may be replayed; got {queued:?}"
        );
        assert_eq!(
            queued[0], sealed2,
            "the undrained segment must be the one queued, not the confirmed one"
        );
        let _ = sealed1;
        fs::remove_dir_all(dir).ok();
    }

    /// Directly exercises `scan_unconfirmed_sealed` on the `shard_count = None`
    /// path — the entry point the 4a sink-recovery rescan
    /// (`drain::probe_and_resume_stranded`) uses. That path is otherwise only
    /// covered indirectly (the drain resume test queues a *single* segment, and
    /// the replay tests always pass `Some(count)`), so the cross-shard ordering
    /// and confirmed-skip behaviour of the recovery scan itself is untested.
    ///
    /// Asserts the contract the rescan relies on: every sealed-but-unconfirmed
    /// segment across ALL shard dirs is returned in deterministic ascending
    /// order, already-confirmed segments are skipped, and `None` neither filters
    /// dirs by index nor warns (it must still scan dirs a `Some` count would flag
    /// as "beyond configured count" — recovery must not strand them).
    #[test]
    fn scan_unconfirmed_sealed_none_count_returns_all_shards_ordered_skipping_confirmed() {
        use crate::wab::format::{build_confirmed, confirmed_path_for};
        let dir = tmp_dir("scan_none_recovery");

        // shard_00: two unconfirmed sealed segments (counters 1 and 2).
        let sd0 = shard_dir_path(&dir, 0);
        fs::create_dir_all(&sd0).unwrap();
        let s0_1 = {
            let mut s = WabSegment::create(&segment_path(&sd0, 1), 0).unwrap();
            s.write_record(b"a").unwrap();
            s.seal().unwrap()
        };
        let s0_2 = {
            let mut s = WabSegment::create(&segment_path(&sd0, 2), 0).unwrap();
            s.write_record(b"b").unwrap();
            s.seal().unwrap()
        };

        // shard_01: one CONFIRMED (must be skipped) + one unconfirmed sealed.
        let sd1 = shard_dir_path(&dir, 1);
        fs::create_dir_all(&sd1).unwrap();
        let s1_confirmed = {
            let mut s = WabSegment::create(&segment_path(&sd1, 1), 1).unwrap();
            s.write_record(b"done").unwrap();
            s.seal().unwrap()
        };
        fs::write(confirmed_path_for(&s1_confirmed), build_confirmed(1, 1, 1)).unwrap();
        let s1_2 = {
            let mut s = WabSegment::create(&segment_path(&sd1, 2), 1).unwrap();
            s.write_record(b"c").unwrap();
            s.seal().unwrap()
        };

        // shard_05: an "orphaned" dir whose index would be >= a small configured count.
        let sd5 = shard_dir_path(&dir, 5);
        fs::create_dir_all(&sd5).unwrap();
        let s5_1 = {
            let mut s = WabSegment::create(&segment_path(&sd5, 1), 5).unwrap();
            s.write_record(b"orphan").unwrap();
            s.seal().unwrap()
        };

        let got = scan_unconfirmed_sealed(&dir, None).unwrap();
        assert!(
            !got.contains(&s1_confirmed),
            "the confirmed segment must be skipped by the recovery scan: {got:?}"
        );
        assert_eq!(
            got,
            vec![s0_1, s0_2, s1_2, s5_1],
            "all unconfirmed sealed segments across every shard dir, ascending"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T08): a corrupt/forged per-record length field exceeding
    /// MAX_PAYLOAD_HARD_CAP must be rejected with InvalidData BEFORE any
    /// allocation — the disk-read DoS bound. Existing reader tests cover CRC
    /// mismatch and round-trip but not the cap branch.
    #[test]
    fn segment_reader_rejects_oversized_record_len_before_allocation() {
        let dir = tmp_dir("rd_oversized_len");
        let path = segment_path(&dir, 1);
        {
            let mut seg = WabSegment::create(&path, 0).unwrap();
            seg.write_record(b"keep-me").unwrap();
        } // drop flushes

        // Splice an over-cap record-length field where the next record would begin.
        let oversized = (MAX_PAYLOAD_HARD_CAP as u32 + 1).to_le_bytes();
        {
            use std::io::Write;
            let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&oversized).unwrap();
        }

        let mut reader = SegmentReader::open(&path).unwrap();
        let first = reader.next().expect("a record").expect("valid record");
        assert_eq!(first, b"keep-me" as &[u8]);
        let err = reader
            .next()
            .expect("an item")
            .expect_err("an over-cap length must be an Err, never a giant allocation");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
        assert!(
            err.to_string().contains("exceeds MAX_PAYLOAD_HARD_CAP"),
            "error must name the cap: {err}"
        );
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
        run_with_panic_supervision(
            0,
            Arc::clone(&m),
            &RealClock,
            panic_then_recover_factory(1, "boom"),
        );
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
        run_with_panic_supervision(shard, Arc::clone(&m), &RealClock, move || {
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
        run_with_panic_supervision(0, Arc::clone(&m), &RealClock, || {
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
            &RealClock,
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
        run_with_panic_supervision(0, Arc::clone(&m), &RealClock, || {
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

    // ── EWMA helper ──────────────────────────────────────────────────────────

    /// `ewma_update_us` converges toward a constant input. After enough
    /// samples the EWMA should be within 10% of the constant.
    #[test]
    fn ewma_converges_toward_constant_input() {
        let target = 500u64;
        let mut cur = 200u64; // start below target
        for _ in 0..64 {
            cur = ewma_update_us(cur, target);
        }
        // After 64 updates alpha=1/4 EWMA is well within 10% of target.
        assert!(
            cur > target * 9 / 10 && cur < target * 11 / 10,
            "EWMA should converge to ~{target}, got {cur}"
        );
    }

    /// A single spike should move the EWMA by at most 25% of the spike
    /// magnitude (alpha=1/4 → new = old*3/4 + spike*1/4).
    #[test]
    fn ewma_single_spike_is_dampened() {
        let start = 200u64;
        let spike = 2_000u64;
        let after = ewma_update_us(start, spike);
        // Expected: 200*3/4 + 2000/4 = 150 + 500 = 650
        assert_eq!(after, 650, "single spike should land at 650 µs");
        // The move is 450 out of 1800 possible = 25%; verify it's ≤ 25%.
        let delta = after.saturating_sub(start);
        let max_delta = (spike - start) / 4 + 1; // +1 for integer rounding
        assert!(
            delta <= max_delta,
            "spike should shift EWMA by at most 25% of spike delta, but delta={delta} max={max_delta}"
        );
    }

    /// Zero sample (degenerate: no fsync measured) should reduce the EWMA
    /// towards zero without panic.
    #[test]
    fn ewma_zero_sample_does_not_panic() {
        let result = ewma_update_us(200, 0);
        assert_eq!(result, 150, "zero sample: 200*3/4 + 0/4 = 150");
    }

    /// Very large sample (saturating multiply guard) — must not overflow.
    #[test]
    fn ewma_large_values_do_not_overflow() {
        // u64::MAX / 2 as current — saturating_mul(3) would overflow without
        // saturation; verify the function handles it without panic.
        let large = u64::MAX / 2;
        let result = ewma_update_us(large, 1_000_000);
        // We can't assert an exact value, just that it didn't panic.
        let _ = result;
    }

    /// The worker's clamping range [50, 2000] does not clip the converged
    /// EWMA when fsync is in the realistic NVMe range (~150–250 µs). This
    /// documents that on fast local NVMe the window stays near the old
    /// fixed 200 µs constant — no local throughput change expected.
    #[test]
    fn ewma_nvme_range_is_not_clipped() {
        const MIN_US: u64 = 50;
        const MAX_US: u64 = 2_000;
        // Simulate NVMe fsync latency of 150 µs; start from the default 200.
        let mut cur = 200u64;
        for _ in 0..64 {
            cur = ewma_update_us(cur, 150);
        }
        let clamped = cur.clamp(MIN_US, MAX_US);
        // The converged EWMA (~150) should be within the [50, 2000] range
        // — no clamping needed on NVMe latencies.
        assert_eq!(
            cur, clamped,
            "NVMe-range EWMA ({cur} µs) should not be clipped by [50, 2000] bounds"
        );
    }
}
