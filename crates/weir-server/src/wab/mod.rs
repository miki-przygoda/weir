pub mod format;
pub mod recovery;
pub mod segment;

use std::{
    fs::{self, File},
    io::{self, BufReader, Read},
    path::{Component, Path, PathBuf},
    thread,
    time::Duration,
};

use crossbeam_channel::{Receiver, Sender};
use tracing::{info, warn};

use format::{EXT_SEALED, FORMAT_VERSION, SEGMENT_HEADER_LEN};
use recovery::{check_confirmed, recover_open_segments};
use segment::ShardWriter;
use weir_core::{Durability, MAX_PAYLOAD_HARD_CAP, Payload};

/// A record queued by a connection handler for writing to the WAB.
pub struct WabRecord {
    pub payload: Payload,
    pub durability: Durability,
    /// Per-request ack channel. The flusher sends `Ok(())` after the record is
    /// durably written according to the requested tier, or `Err` on an
    /// unrecoverable write failure.
    pub ack_tx: Sender<Result<(), io::Error>>,
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

/// Runs crash recovery, replays sealed-but-unconfirmed segments to `drain_tx`,
/// then spawns one flusher thread per shard.
pub fn spawn(
    wab_dir: PathBuf,
    config: WabConfig,
    drain_tx: Sender<PathBuf>,
) -> io::Result<WabHandle> {
    // TODO step-08: replace with `config::validate_path(&wab_dir)?` once config/ exists.
    let wab_dir = validate_path(&wab_dir)?;

    for shard_id in 0..config.shard_count {
        fs::create_dir_all(shard_dir_path(&wab_dir, shard_id))?;
    }

    // Phase 1 (calling thread): crash recovery — unsealed .wab → .wab.sealed
    recover_open_segments(&wab_dir)?;

    // Phase 2 (calling thread): replay sealed-but-unconfirmed segments
    replay_unconfirmed(&wab_dir, config.shard_count, &drain_tx)?;

    // Phase 3: one flusher thread per shard
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut shard_txs = Vec::with_capacity(config.shard_count);
    let mut join_handles = Vec::with_capacity(config.shard_count);

    for shard_id in 0..config.shard_count {
        let (tx, rx) = crossbeam_channel::bounded::<WabRecord>(config.batch_size * 4);
        shard_txs.push(tx);

        let sdir = shard_dir_path(&wab_dir, shard_id);
        let drain_clone = drain_tx.clone();
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
                    info!(sealed = %sealed.display(), "queuing segment for drain replay");
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

fn flusher_thread(
    shard_id: u16,
    shard_dir: PathBuf,
    work_rx: Receiver<WabRecord>,
    drain_tx: Sender<PathBuf>,
    batch_size: usize,
    batch_deadline: Duration,
    core_id: Option<core_affinity::CoreId>,
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
                    flush_batch(&mut writer, &mut batch, &drain_tx, shard_id);
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

        flush_batch(&mut writer, &mut batch, &drain_tx, shard_id);
    }

    // Graceful shutdown: seal the active segment and send to drain.
    match writer.seal_current() {
        Ok(Some(sealed)) => {
            info!(shard = shard_id, sealed = %sealed.display(), "WAB flusher sealed segment on shutdown");
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
) {
    let mut batched_acks: Vec<Sender<Result<(), io::Error>>> = Vec::new();
    let mut need_fsync = false;

    for record in batch.drain(..) {
        // write_record returns Some(sealed_path) when the segment rotated.
        let rotation = match writer.write_record(&record.payload) {
            Err(e) => {
                let _ = record
                    .ack_tx
                    .send(Err(io::Error::new(e.kind(), e.to_string())));
                continue;
            }
            Ok(maybe_sealed) => maybe_sealed,
        };

        if let Some(sealed) = rotation {
            // Segment was sealed (seal includes fsync). Notify drain.
            info!(shard = shard_id, sealed = %sealed.display(), "WAB segment rotated");
            drain_tx.send(sealed).ok();
        }

        match record.durability {
            Durability::Sync => match writer.fsync_current() {
                Ok(()) => {
                    let _ = record.ack_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = record.ack_tx.send(Err(e));
                }
            },
            Durability::Batched => {
                need_fsync = true;
                batched_acks.push(record.ack_tx);
            }
            Durability::Buffered => {
                let _ = record.ack_tx.send(Ok(()));
            }
        }
    }

    // Group fsync for all Batched records in this flush.
    if need_fsync {
        let fsync_result = writer.fsync_current();
        for ack_tx in batched_acks {
            match &fsync_result {
                Ok(()) => {
                    let _ = ack_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = ack_tx.send(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
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

/// Validates a WAB directory path. Four-check sequence:
/// 1. Must be absolute.
/// 2. Must not contain `..` components.
/// 3. Must not contain null bytes (Unix).
/// 4. `canonicalize()` — requires the directory to already exist; re-validates the
///    canonical path against checks 1–2 to catch symlink escapes.
///
/// The daemon does not create the WAB directory (PostgreSQL model). The operator
/// must create it before starting the daemon.
///
/// TODO step-08: move to `config/mod.rs` and import from there; shared with socket
/// bind (step-05) and config load (step-08).
pub fn validate_path(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "WAB directory path '{}' is not absolute — provide an absolute path",
                path.display()
            ),
        ));
    }

    if path.components().any(|c| c == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "WAB directory path '{}' contains '..' components — remove all '..' from the path",
                path.display()
            ),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().contains(&0u8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAB directory path contains a null byte — null bytes are not permitted in paths",
            ));
        }
    }

    let canonical = std::fs::canonicalize(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "WAB directory '{}' does not exist or is not a directory. \
                     Create it before starting the daemon: \
                     mkdir -p {p} && chmod 700 {p}",
                    path.display(),
                    p = path.display()
                ),
            )
        } else {
            e
        }
    })?;

    // Re-validate the resolved path to catch symlink escapes.
    if !canonical.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "canonicalized WAB path '{}' is not absolute — possible symlink escape",
                canonical.display()
            ),
        ));
    }
    if canonical.components().any(|c| c == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "canonicalized WAB path '{}' contains '..' components",
                canonical.display()
            ),
        ));
    }

    Ok(canonical)
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

    // ── validate_path ─────────────────────────────────────────────────────────

    #[test]
    fn validate_path_rejects_relative() {
        let err = validate_path(Path::new("relative/path")).unwrap_err();
        assert!(err.to_string().contains("not absolute"), "{err}");
    }

    #[test]
    fn validate_path_rejects_dotdot() {
        let err = validate_path(Path::new("/valid/../escape")).unwrap_err();
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    #[test]
    fn validate_path_rejects_nonexistent_with_mkdir_hint() {
        let err = validate_path(Path::new("/weir_no_such_dir_xyzzy_12345")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mkdir"), "{msg}");
        assert!(msg.contains("chmod 700"), "{msg}");
    }

    #[test]
    fn validate_path_accepts_existing_absolute_dir() {
        let dir = tmp_dir("validpath");
        assert!(validate_path(&dir).is_ok());
        fs::remove_dir_all(dir).ok();
    }

    /// A symlink whose target does not exist is rejected — `canonicalize()` returns
    /// `NotFound`, and the error message must contain the mkdir hint.
    ///
    /// This is the "symlink escape caught by canonicalize" case: the symlink bypasses
    /// the pre-canonicalize path-string checks (the symlink path itself is absolute, has
    /// no `..`, has no null bytes), but `canonicalize()` follows it to a non-existent
    /// target and returns `NotFound`.
    #[test]
    #[cfg(unix)]
    fn validate_path_rejects_dangling_symlink() {
        let dir = tmp_dir("dangling_symlink");
        let link = dir.join("dangling_link");
        // Point to a target that does not exist.
        std::os::unix::fs::symlink("/weir_nonexistent_target_xyzzy_98765", &link).unwrap();

        let err = validate_path(&link).unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::NotFound,
            "expected NotFound: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("mkdir"),
            "error should contain mkdir hint: {msg}"
        );
        assert!(
            msg.contains("chmod 700"),
            "error should contain chmod 700 hint: {msg}"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// A symlink to a real directory is accepted. `canonicalize()` resolves it to
    /// the target's absolute path, which becomes the canonical wab_dir used for all
    /// subsequent operations. The symlink path itself is not stored.
    #[test]
    #[cfg(unix)]
    fn validate_path_symlink_to_real_directory_resolves_to_canonical_target() {
        let dir_a = tmp_dir("symlink_src");
        let dir_b = tmp_dir("symlink_tgt");
        let link = dir_a.join("link_to_b");
        std::os::unix::fs::symlink(&dir_b, &link).unwrap();

        let resolved = validate_path(&link).unwrap();
        // The returned path is the canonical target, not the symlink path.
        assert_eq!(resolved, dir_b.canonicalize().unwrap());
        assert_ne!(resolved, link);

        fs::remove_dir_all(dir_a).ok();
        fs::remove_dir_all(dir_b).ok();
    }
}
