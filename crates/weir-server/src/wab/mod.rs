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
use format::{EXT_SEALED, FORMAT_VERSION, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN};
use recovery::{check_confirmed, recover_open_segments};
use segment::ShardWriter;
use weir_core::{Durability, MAX_PAYLOAD_HARD_CAP, Payload};

/// A record queued by a connection handler for writing to the WAB.
pub struct WabRecord {
    pub payload: Payload,
    pub durability: Durability,
    /// Per-request ack channel. The flusher sends `true` after the record is
    /// durably written according to the requested tier, or `false` on an
    /// unrecoverable write failure.
    pub ack_tx: oneshot::Sender<bool>,
}

/// Configuration for the WAB subsystem.
pub struct WabConfig {
    /// Number of shards (one flusher thread per shard).
    pub shard_count: usize,
    /// Maximum number of records per flush batch.
    pub batch_size: usize,
    /// Maximum time to accumulate a batch before flushing.
    pub batch_deadline: Duration,
}

impl Default for WabConfig {
    fn default() -> Self {
        WabConfig {
            shard_count: 1,
            batch_size: 1000,
            batch_deadline: Duration::from_millis(100),
        }
    }
}

/// Returned by `spawn`. Drop `shard_txs` to initiate shutdown (flusher threads
/// exit when their receiver disconnects), then join the handles to wait for all
/// segments to be sealed.
pub struct WabHandle {
    /// One sender per shard. Drop all of them to signal shutdown.
    pub shard_txs: Vec<Sender<WabRecord>>,
    pub join_handles: Vec<thread::JoinHandle<()>>,
}

fn shard_dir_path(wab_dir: &Path, shard_id: usize) -> PathBuf {
    wab_dir.join(format!("shard_{shard_id:02}"))
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
        let (tx, rx) = crossbeam_channel::bounded::<WabRecord>(config.batch_size * 4);
        shard_txs.push(tx);

        let sdir = shard_dir_path(&wab_dir, shard_id);
        let drain_clone = drain_tx.clone();
        let metrics_clone = Arc::clone(&metrics);
        let batch_size = config.batch_size;
        let batch_deadline = config.batch_deadline;
        let core_id = core_ids.get(shard_id % core_ids.len().max(1)).copied();

        let handle = thread::Builder::new()
            .name(format!("wab-flusher-{shard_id}"))
            .spawn(move || {
                flusher_thread(
                    shard_id as u16,
                    sdir,
                    rx,
                    drain_clone,
                    batch_size,
                    batch_deadline,
                    core_id,
                    metrics_clone,
                );
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
    work_rx: Receiver<WabRecord>,
    drain_tx: Sender<PathBuf>,
    batch_size: usize,
    batch_deadline: Duration,
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
    let mut writer = ShardWriter::new(shard_id, shard_dir);
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

    let mut batch: Vec<WabRecord> = Vec::with_capacity(batch_size);

    loop {
        // Block on the first record of the batch (or detect channel close).
        match work_rx.recv_timeout(batch_deadline) {
            Ok(record) => batch.push(record),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if !batch.is_empty() {
                    flush_batch(&mut writer, &mut batch, &drain_tx, shard_id, &metrics);
                }
                continue;
            }
        }

        // Drain any additional available records up to batch_size.
        while batch.len() < batch_size {
            match work_rx.try_recv() {
                Ok(record) => batch.push(record),
                Err(_) => break,
            }
        }

        flush_batch(&mut writer, &mut batch, &drain_tx, shard_id, &metrics);
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
    batch: &mut Vec<WabRecord>,
    drain_tx: &Sender<PathBuf>,
    shard_id: u16,
    metrics: &Arc<Metrics>,
) {
    let mut batched_acks: Vec<oneshot::Sender<bool>> = Vec::new();
    let mut need_fsync = false;

    for record in batch.drain(..) {
        // write_record returns Some(sealed_path) when the segment rotated.
        let rotation = match writer.write_record(&record.payload) {
            Err(_) => {
                let _ = record.ack_tx.send(false);
                continue;
            }
            Ok(maybe_sealed) => maybe_sealed,
        };

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

        match record.durability {
            Durability::Sync => {
                let t = Instant::now();
                let ok = writer.fsync_current().is_ok();
                metrics
                    .wab_fsync_duration
                    .observe(t.elapsed().as_secs_f64());
                let _ = record.ack_tx.send(ok);
            }
            Durability::Batched => {
                need_fsync = true;
                batched_acks.push(record.ack_tx);
            }
            Durability::Buffered => {
                let _ = record.ack_tx.send(true);
            }
        }
    }

    // Group fsync for all Batched records in this flush.
    if need_fsync {
        let t = Instant::now();
        let ok = writer.fsync_current().is_ok();
        metrics
            .wab_fsync_duration
            .observe(t.elapsed().as_secs_f64());
        for ack_tx in batched_acks {
            let _ = ack_tx.send(ok);
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

        let mut payload = vec![0u8; payload_len];
        if let Err(e) = self.reader.read_exact(&mut payload) {
            self.done = true;
            return Some(Err(e));
        }

        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record CRC mismatch: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
                ),
            )));
        }

        Some(Ok(payload))
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

        let got: Vec<Vec<u8>> = SegmentReader::open(&sealed)
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
}
