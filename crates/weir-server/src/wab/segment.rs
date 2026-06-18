use std::{
    fs::{File, OpenOptions},
    io::{self, IoSlice, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use std::sync::Arc;

use crc32fast::Hasher as CrcHasher;

use super::format::{
    EXT_ACTIVE, EXT_SEALED, SEGMENT_HEADER_LEN, build_segment_footer, build_segment_header,
    build_sentinel, unix_nanos_now,
};
use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use weir_core::MAX_PAYLOAD_HARD_CAP;

/// An active WAB segment file. Owns the file handle and tracks write accounting.
/// The running `file_crc_hasher` accumulates CRC32 over every byte written to the
/// file (including the header), so the footer's `file_crc32` field requires no
/// full-file re-read at seal time.
pub(crate) struct WabSegment {
    file: File,
    path: PathBuf,
    /// Total bytes written to the file (header + records). Used for rotation.
    bytes_written: u64,
    record_count: u64,
    /// Total payload bytes written (excludes per-record header overhead).
    data_bytes: u64,
    /// CRC32 accumulator over all file bytes written before the sentinel.
    file_crc_hasher: CrcHasher,
    /// Set when a `write_record` call returned an error after partially writing
    /// to the underlying file. The OS file offset has advanced past stray bytes
    /// that the in-memory `bytes_written` / `file_crc_hasher` accounting did not
    /// see, so any further writes to this segment would interleave good records
    /// with the unaccounted garbage and produce a CRC mismatch at drain time.
    /// Once set, `write_record` refuses further writes; the caller (`ShardWriter`)
    /// drops the segment and opens a fresh one.
    poisoned: bool,
}

impl WabSegment {
    /// Creates a new segment file at `path`.
    ///
    /// Uses `O_CREAT | O_EXCL | O_NOFOLLOW` to prevent symlink attacks and
    /// ensure the file is newly created. `O_EXCL` *enforces* non-existence: if
    /// `path` already exists the call returns [`io::ErrorKind::AlreadyExists`]
    /// rather than truncating or appending, so the caller never has to check
    /// first (and a racing creator can't be silently clobbered).
    pub(crate) fn create(path: &Path, shard_id: u16) -> io::Result<Self> {
        // O_NOFOLLOW: reject symlinks at the target path (prevents TOCTOU redirect).
        // mode 0o600: segment files are private to the daemon; no group/other read.
        #[cfg(unix)]
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600)
            .open(path)?;

        #[cfg(not(unix))]
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;

        let header = build_segment_header(shard_id);

        let mut hasher = CrcHasher::new();
        hasher.update(&header);

        let mut seg = WabSegment {
            file,
            path: path.to_owned(),
            bytes_written: SEGMENT_HEADER_LEN as u64,
            record_count: 0,
            data_bytes: 0,
            file_crc_hasher: hasher,
            poisoned: false,
        };

        seg.file.write_all(&header)?;
        // Make the new file's directory entry durable. Records written here are
        // group-fsynced for their *data*, but the dirent that links this file into
        // the shard dir is only crash-durable after a parent-dir fsync — without
        // it, a crash could orphan a file whose Sync records we acked as durable.
        fsync_parent_dir(path)?;
        Ok(seg)
    }

    /// Writes one record to the segment.
    ///
    /// Format: `payload_len (u32 LE)` + `crc32 (u32 LE)` + `payload bytes`.
    ///
    /// Uses checked arithmetic throughout: a panic here means the segment has grown
    /// beyond addressable bounds, which is unrecoverable.
    pub(crate) fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "segment is poisoned by a previous partial write",
            ));
        }
        // Defense-in-depth: an empty payload serialises to a zero length prefix,
        // which is the end-of-records sentinel — writing one would truncate the
        // segment on the next read. Empty payloads are rejected at ingest
        // (NackReason::EmptyPayload), so this should be unreachable, but the WAB
        // is the hard durability boundary and must never store a record it can't
        // read back.
        if payload.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty payload cannot be represented in a WAB segment",
            ));
        }
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "payload length {} exceeds u32::MAX — record rejected",
                    payload.len()
                ),
            )
        })?;

        // Belt-and-suspenders: MAX_PAYLOAD_HARD_CAP is already enforced at the socket
        // layer, but we re-check here so the WAB can never be fed oversized records
        // regardless of the call path.
        if payload.len() > MAX_PAYLOAD_HARD_CAP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "payload length {} exceeds MAX_PAYLOAD_HARD_CAP {}",
                    payload.len(),
                    MAX_PAYLOAD_HARD_CAP
                ),
            ));
        }

        let crc32 = crc32fast::hash(payload);
        let len_bytes = payload_len.to_le_bytes();
        let crc_bytes = crc32.to_le_bytes();

        // One vectored write (writev) instead of three separate write_all
        // syscalls per record. `write_all_vectored` is still unstable, so we
        // issue a single `write_vectored` and treat anything short of a full
        // write the same way the old three-write sequence treated a mid-record
        // failure: the OS file offset has advanced past stray bytes the
        // in-memory accounting below won't see, so poison the segment. For a
        // regular file a short writev essentially only occurs on ENOSPC, which
        // is poison-worthy anyway.
        let bufs = [
            IoSlice::new(&len_bytes),
            IoSlice::new(&crc_bytes),
            IoSlice::new(payload),
        ];
        let total = len_bytes.len() + crc_bytes.len() + payload.len();
        match self.file.write_vectored(&bufs) {
            Ok(n) if n == total => {}
            Ok(_) => {
                self.poisoned = true;
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short vectored write of WAB record",
                ));
            }
            Err(e) => {
                self.poisoned = true;
                return Err(e);
            }
        }

        self.file_crc_hasher.update(&len_bytes);
        self.file_crc_hasher.update(&crc_bytes);
        self.file_crc_hasher.update(payload);

        const RECORD_OVERHEAD: u64 = 8; // payload_len (4) + crc32 (4)
        self.bytes_written = self
            .bytes_written
            .checked_add(RECORD_OVERHEAD)
            .and_then(|b| b.checked_add(payload.len() as u64))
            .expect("bytes_written overflow: segment exceeded u64 bounds");
        self.record_count = self
            .record_count
            .checked_add(1)
            .expect("record_count overflow");
        self.data_bytes = self
            .data_bytes
            .checked_add(payload.len() as u64)
            .expect("data_bytes overflow");

        Ok(())
    }

    /// Calls the platform-appropriate sync operation on the segment file.
    ///
    /// macOS: `F_BARRIERFSYNC` — orders this write ahead of later ones with a
    /// barrier without waiting for the drive to flush its volatile cache. Faster
    /// than `F_FULLFSYNC`; it guarantees ordering and survival of a process/OS
    /// crash, but NOT a guaranteed media flush on sudden power loss (a drive with
    /// a volatile write cache can still lose the most recent writes). Run
    /// production data paths on Linux; see the durability note in the README.
    ///
    /// Linux: `fdatasync` — flushes data and critical metadata without waiting for
    /// directory entry updates.
    pub(crate) fn fsync(&self) -> io::Result<()> {
        platform_fsync(&self.file)
    }

    /// Returns true if the segment has reached or exceeded the configured
    /// rotation threshold. The threshold is owned by `ShardWriter` so a single
    /// `WabSegment` instance can be reused under different policies (e.g. tests).
    pub(crate) fn should_rotate(&self, max_bytes: u64) -> bool {
        self.bytes_written >= max_bytes
    }

    /// Number of records written so far. Test-only accessor — production
    /// code reads the in-memory counter directly during `seal()` to write
    /// the segment footer.
    #[cfg(test)]
    pub(crate) fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Writes the sentinel + footer and fsyncs, making every prior record
    /// durable at the segment's `.wab` path. Returns that (still-`.wab`) path.
    ///
    /// This is the **durability commit point**: once it returns `Ok`, the data
    /// survives a crash. [`seal`](Self::seal) then renames the path to
    /// `.wab.sealed` to publish it. A crash *between* this fsync and that rename
    /// leaves a fully-formed segment at the `.wab` path, which crash recovery
    /// re-seals via the sentinel branch in `recover_segment` — the DST harness
    /// exercises exactly that window by failing the rename after this returns.
    ///
    /// Consumes `self` so the segment cannot be written to afterwards.
    pub(crate) fn finalize_to_disk(mut self) -> io::Result<PathBuf> {
        let sealed_at = unix_nanos_now();

        // Finalise CRC before writing sentinel (sentinel is not covered by file_crc32).
        let file_crc32 = self.file_crc_hasher.finalize();

        self.file.write_all(&build_sentinel())?;

        let footer =
            build_segment_footer(self.record_count, self.data_bytes, file_crc32, sealed_at);
        self.file.write_all(&footer)?;

        platform_fsync(&self.file)?;

        Ok(self.path)
    }

    /// Seals the segment: writes sentinel + footer, fsyncs (see
    /// [`finalize_to_disk`](Self::finalize_to_disk)), and atomically renames the
    /// file from `.wab` to `.wab.sealed`.
    ///
    /// Consumes `self` to ensure the segment cannot be written to after sealing.
    /// Returns the path of the newly sealed file.
    pub(crate) fn seal(self) -> io::Result<PathBuf> {
        let active_path = self.finalize_to_disk()?;
        let sealed_path = sealed_path_for(&active_path);
        // Refuse to clobber an existing sealed segment. rename(2) silently
        // REPLACES its destination; if next_counter ever regressed (e.g. a failed
        // startup counter-scan defaulting to 1), sealing seg_NNNNNNNN.wab over a
        // recovered-but-undrained seg_NNNNNNNN.wab.sealed would lose acked records
        // — the F12/G02 data-loss hole. The flusher already refuses to start at an
        // unestablished counter (wab/mod.rs), so this is belt-and-suspenders that
        // turns any residual overwrite into a loud error rather than silent loss.
        // Single-writer-per-shard, so the check→rename window is not a TOCTOU risk.
        if sealed_path.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "refusing to seal '{}' over existing '{}' — would overwrite a sealed segment",
                    active_path.display(),
                    sealed_path.display()
                ),
            ));
        }
        std::fs::rename(&active_path, &sealed_path)?;
        // Publish the rename durably: without a parent-dir fsync a crash can lose
        // the .wab.sealed dirent. (Recovery would re-seal the orphaned .wab, so
        // this is lower-stakes than the create-time fsync above — but it keeps the
        // documented "seal is the durability commit point" honest.)
        fsync_parent_dir(&sealed_path)?;
        Ok(sealed_path)
    }
}

/// Derives the `.wab.sealed` path from an active `.wab` path.
pub(crate) fn sealed_path_for(active_path: &Path) -> PathBuf {
    let mut p = active_path.to_owned();
    let name = p
        .file_name()
        .expect("segment path has no file name")
        .to_owned();
    let mut sealed_name = name.to_os_string();
    sealed_name.push(EXT_SEALED.strip_prefix(EXT_ACTIVE).unwrap_or(""));
    p.set_file_name(sealed_name);
    p
}

/// Derives the segment counter from a segment filename regardless of which
/// lifecycle extension it carries — `seg_00000001.wab`, `seg_00000001.wab.sealed`,
/// and `seg_00000001.wab.confirmed` all yield `1`. The counter is the digit run
/// immediately after the `seg_` prefix; everything after it is ignored.
///
/// Counting EVERY on-disk segment (not just active `.wab` files) is load-bearing:
/// after crash recovery seals the active segment, a shard directory can hold only
/// `.wab.sealed` (and `.wab.confirmed`) files awaiting drain. If the counter scan
/// ignored those, a fresh writer would reset `next_counter` to 1 and its first
/// segment's seal-rename would silently overwrite a recovered-but-undrained
/// sealed segment — data loss. `file_stem()` strips only the *last* extension, so
/// it left `seg_00000001.wab` on a sealed file and failed to parse; we parse the
/// leading digit run directly to avoid that.
pub(crate) fn segment_counter_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("seg_")?;
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

/// Formats a segment file name for a given shard directory and counter.
pub(crate) fn segment_path(shard_dir: &Path, counter: u64) -> PathBuf {
    shard_dir.join(format!("seg_{counter:08}{EXT_ACTIVE}"))
}

/// An open, writable WAB segment — the seam between [`ShardWriter`] and the
/// bytes on disk. Production uses [`WabSegment`] directly (see the impl below);
/// the deterministic-simulation harness swaps in a backend that injects
/// `fsync`/`seal` faults on a seeded schedule. Held as a `Box<dyn SegmentHandle>`
/// so `ShardWriter`'s rotate/seal lifecycle is backend-agnostic. The single
/// vtable hop per write/fsync is negligible against the real syscall it guards.
pub(crate) trait SegmentHandle: Send {
    /// Append one record. Mirrors [`WabSegment::write_record`].
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()>;
    /// Sync the segment to stable storage. Mirrors [`WabSegment::fsync`].
    fn fsync(&self) -> io::Result<()>;
    /// True once the segment has reached the rotation threshold. Mirrors
    /// [`WabSegment::should_rotate`].
    fn should_rotate(&self, max_bytes: u64) -> bool;
    /// Seal the segment (sentinel + footer + fsync + atomic rename) and return
    /// the `.wab.sealed` path. Consumes the handle. Mirrors [`WabSegment::seal`].
    fn seal(self: Box<Self>) -> io::Result<PathBuf>;
}

/// Creates segments and enumerates existing ones for a shard — the complete
/// filesystem boundary for [`ShardWriter`]. Production is [`FsSegmentStore`]
/// (real files); the DST harness injects a fault-aware store. Held as
/// `Arc<dyn SegmentStore>` so the generic stays out of the public
/// [`super::spawn`] signature.
pub(crate) trait SegmentStore: Send + Sync {
    /// Create a fresh active segment at `path` for `shard_id`.
    fn create(&self, path: &Path, shard_id: u16) -> io::Result<Box<dyn SegmentHandle>>;
    /// Counter values of every segment file in `dir` — active (`.wab`), sealed
    /// (`.wab.sealed`), AND confirmed (`.wab.confirmed`). All lifecycle states
    /// must be counted so [`ShardWriter::scan_and_advance_counter`] advances past
    /// recovered-but-undrained sealed segments; counting only active files lets a
    /// post-recovery writer reuse a counter and overwrite a sealed segment (data
    /// loss). Used by [`ShardWriter::scan_and_advance_counter`].
    fn segment_counters(&self, dir: &Path) -> io::Result<Vec<u64>>;
}

/// Production [`SegmentStore`]: real files on the local filesystem via
/// [`WabSegment`].
pub(crate) struct FsSegmentStore;

impl SegmentStore for FsSegmentStore {
    fn create(&self, path: &Path, shard_id: u16) -> io::Result<Box<dyn SegmentHandle>> {
        Ok(Box::new(WabSegment::create(path, shard_id)?))
    }

    fn segment_counters(&self, dir: &Path) -> io::Result<Vec<u64>> {
        let mut counters = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(n) = segment_counter_from_path(&entry.path()) {
                counters.push(n);
            }
        }
        Ok(counters)
    }
}

impl SegmentHandle for WabSegment {
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        WabSegment::write_record(self, payload)
    }
    fn fsync(&self) -> io::Result<()> {
        WabSegment::fsync(self)
    }
    fn should_rotate(&self, max_bytes: u64) -> bool {
        WabSegment::should_rotate(self, max_bytes)
    }
    fn seal(self: Box<Self>) -> io::Result<PathBuf> {
        WabSegment::seal(*self)
    }
}

/// Manages the active segment for one shard. Opens the segment lazily on first write.
pub(crate) struct ShardWriter {
    shard_id: u16,
    shard_dir: PathBuf,
    /// Counter used to name the next segment file (incremented on each rotation).
    next_counter: u64,
    /// Bytes threshold at which the active segment is sealed and rotated.
    /// Set via `WabConfig::segment_max_bytes` at flusher-thread spawn time.
    segment_max_bytes: u64,
    active: Option<Box<dyn SegmentHandle>>,
    /// The filesystem backend. `FsSegmentStore` in production; a fault-injecting
    /// store in the DST harness. Boxed as `Arc<dyn>` so `ShardWriter` and the
    /// public `spawn` API stay free of a backend generic.
    store: Arc<dyn SegmentStore>,
    /// Metrics handle so `ensure_open` can bump the
    /// `weir_wab_segments_total{state="open"}` counter every time a new
    /// segment file is created. The other state transitions (sealed,
    /// confirmed, quarantined) are already incremented at their
    /// respective lifecycle points (`flush_batch`, `confirmed::*`,
    /// `recovery::*`); the open state was registered but never wired —
    /// this field closes that gap so operators get a complete
    /// open → sealed → confirmed / quarantined transition count.
    metrics: Arc<Metrics>,
}

impl ShardWriter {
    /// Construct over an injected [`SegmentStore`] — the sole filesystem
    /// boundary, so every segment creation, rotation, fsync, seal, and
    /// counter-scan flows through it. Production injects [`FsSegmentStore`] at
    /// the flusher's single construction point (see [`super::spawn`]); the DST
    /// harness injects a fault-injecting store.
    pub(crate) fn new_with_store(
        shard_id: u16,
        shard_dir: PathBuf,
        segment_max_bytes: u64,
        metrics: Arc<Metrics>,
        store: Arc<dyn SegmentStore>,
    ) -> Self {
        ShardWriter {
            shard_id,
            shard_dir,
            next_counter: 1,
            segment_max_bytes,
            active: None,
            store,
            metrics,
        }
    }

    /// Sets `next_counter` to one past the highest existing segment counter in the
    /// shard directory. Called during startup so new segments don't collide with
    /// existing (sealed) ones.
    pub(crate) fn scan_and_advance_counter(&mut self) -> io::Result<()> {
        let max = self
            .store
            .segment_counters(&self.shard_dir)?
            .into_iter()
            .max()
            .unwrap_or(0);
        if max >= self.next_counter {
            self.next_counter = max.checked_add(1).expect("segment counter overflow");
        }
        Ok(())
    }

    /// Writes one record, opening the segment lazily.
    ///
    /// Returns `Some(sealed_path)` if the write caused the segment to hit the rotation
    /// threshold and be sealed. The caller is responsible for sending the sealed path
    /// to the drain channel.
    ///
    /// On write error, drops the active segment so the next call opens a fresh one.
    /// The orphaned file is left on disk; crash recovery seals it and the drain
    /// reader stops at the first invalid record — records written successfully
    /// before the failure remain drainable.
    pub(crate) fn write_record(&mut self, payload: &[u8]) -> io::Result<Option<PathBuf>> {
        self.ensure_open()?;
        if let Err(e) = self
            .active
            .as_mut()
            .expect("ensure_open guarantees Some")
            .write_record(payload)
        {
            // The segment is poisoned (or its create() left it half-headered).
            // Drop it so the next write opens a fresh segment.
            self.active = None;
            return Err(e);
        }
        let should_rotate = self
            .active
            .as_ref()
            .is_some_and(|s| s.should_rotate(self.segment_max_bytes));
        if should_rotate {
            let sealed = self.active.take().unwrap().seal()?;
            return Ok(Some(sealed));
        }
        Ok(None)
    }

    /// Fsyncs the current active segment. No-op if no segment is open.
    pub(crate) fn fsync_current(&self) -> io::Result<()> {
        if let Some(seg) = &self.active {
            seg.fsync()?;
        }
        Ok(())
    }

    /// Seals the current active segment and returns its sealed path.
    /// Returns `None` if no segment is currently open.
    pub(crate) fn seal_current(&mut self) -> io::Result<Option<PathBuf>> {
        match self.active.take() {
            Some(seg) => Ok(Some(seg.seal()?)),
            None => Ok(None),
        }
    }

    fn ensure_open(&mut self) -> io::Result<()> {
        if self.active.is_none() {
            let path = segment_path(&self.shard_dir, self.next_counter);
            self.next_counter = self
                .next_counter
                .checked_add(1)
                .expect("segment counter overflow");
            self.active = Some(self.store.create(&path, self.shard_id)?);
            // Newly opened segment — bump the `open` state counter so the
            // open → sealed → confirmed/quarantined transition story is
            // observable end to end via `weir_wab_segments_total{state="..."}`.
            self.metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::open,
                })
                .inc();
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn platform_fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // F_BARRIERFSYNC (85): issues a write barrier — data reaches the drive and is
    // ordered before later writes — without forcing the drive's volatile cache to
    // the medium. This is a DELIBERATE durability/throughput tradeoff (F19): unlike
    // F_FULLFSYNC, a barrier does NOT guarantee survival of a sudden power loss on a
    // drive with a non-volatile-safe write cache (most consumer SSDs). It matches
    // what fdatasync provides on Linux for typical configurations and is the
    // platform-recommended fast path; operators who need power-loss durability on
    // such hardware should ensure the drive honours cache-flush/barriers (or front
    // the WAB with a battery/capacitor-backed cache). The crown invariant
    // (acked-true ⇒ on stable storage) holds under an OS crash; the residual is the
    // hardware-cache-on-power-loss case.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn platform_fsync(file: &File) -> io::Result<()> {
    file.sync_data() // fdatasync on Linux
}

/// Fsyncs the parent directory of `path` so a preceding create or rename of that
/// entry is durable across a crash.
///
/// `platform_fsync` makes a file's *data* durable, but POSIX only guarantees the
/// *directory entry* (the create/rename that links the file into its dir) is
/// durable after an fsync on the parent directory. Without this, a crash right
/// after [`WabSegment::create`] could orphan a file whose records we later ack as
/// durable, and a crash right after [`WabSegment::seal`]'s rename could lose the
/// `.wab.sealed` publication. Opening a directory read-only and fsync-ing its fd
/// flushes its entries on Linux and macOS (`F_BARRIERFSYNC` is file-specific; the
/// plain `fsync` behind `sync_all` is the directory-durability primitive).
///
/// No-op on Windows: the WAB's `fdatasync`-based durability model is Unix-first,
/// and opening a directory as a `File` is not portable there.
#[cfg(not(windows))]
pub(crate) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn fsync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    pub(crate) fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weir_seg_{label}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn wab_segment_tracks_record_count() {
        let dir = tmp_dir("count");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        assert_eq!(seg.record_count(), 0);
        seg.write_record(b"a").unwrap();
        seg.write_record(b"bb").unwrap();
        assert_eq!(seg.record_count(), 2);
        let _ = seg.seal();
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_segment_seals_and_renames() {
        let dir = tmp_dir("seal");
        let path = dir.join("seg_00000001.wab");
        let seg = WabSegment::create(&path, 0).unwrap();
        let sealed = seg.seal().unwrap();
        assert!(sealed.exists());
        assert!(sealed.to_str().unwrap().ends_with(".wab.sealed"));
        assert!(!path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn fsync_parent_dir_smoke() {
        // The dir-fsync helper used after create/seal/.confirmed to make directory
        // entries crash-durable (B4). Must succeed for a file in a real directory
        // and be a harmless no-op when there is no parent component.
        let dir = tmp_dir("fsyncdir");
        let path = dir.join("seg_00000001.wab");
        fs::write(&path, b"x").unwrap();
        fsync_parent_dir(&path).unwrap();
        fsync_parent_dir(std::path::Path::new("no_parent_component")).unwrap();
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn write_record_rejects_empty_payload() {
        // An empty payload would serialise to the end-of-records sentinel and
        // truncate the segment — the WAB must reject it (defense-in-depth behind
        // the ingest-layer NackReason::EmptyPayload check).
        let dir = tmp_dir("emptyrec");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        let err = seg.write_record(b"").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_segment_should_rotate_at_threshold() {
        const TEST_THRESHOLD: u64 = 1024;
        let dir = tmp_dir("rotate");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        assert!(!seg.should_rotate(TEST_THRESHOLD));
        seg.bytes_written = TEST_THRESHOLD;
        assert!(seg.should_rotate(TEST_THRESHOLD));
        assert!(seg.should_rotate(TEST_THRESHOLD - 1));
        let _ = seg.seal();
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn sealed_path_for_derives_correctly() {
        let active = PathBuf::from("/wab/shard_00/seg_00000001.wab");
        let sealed = sealed_path_for(&active);
        assert_eq!(
            sealed,
            PathBuf::from("/wab/shard_00/seg_00000001.wab.sealed")
        );
    }

    #[test]
    fn poisoned_segment_refuses_subsequent_writes() {
        // Simulates the post-partial-write state by setting the flag directly
        // (the real trigger — write_all returning Err mid-record — is hard to
        // induce deterministically without a tmpfs; see
        // `tests/system.rs::enospc_returns_nack_not_crash` for the integration
        // variant).
        let dir = tmp_dir("poison_refuse");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        seg.write_record(b"valid").unwrap();
        seg.poisoned = true;
        let err = seg
            .write_record(b"after-poison")
            .expect_err("write after poison must fail");
        assert!(
            err.to_string().contains("poisoned"),
            "error message should mention poisoning; got: {err}"
        );
        let _ = seg.seal();
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn shardwriter_drops_segment_after_write_error() {
        // After an inner segment returns Err from write_record, ShardWriter must
        // drop its `active` segment so the next call opens a fresh file with a
        // new counter — otherwise subsequent writes would interleave with stray
        // bytes from the failed write. We induce a deterministic write error with
        // an oversized payload (rejected before any bytes reach the file) rather
        // than poking the inner segment's private `poisoned` flag, which is no
        // longer reachable through the `SegmentHandle` trait object. The
        // drop-on-error behaviour under test is identical regardless of which
        // error the inner write returned; the poison path itself is covered by
        // `poisoned_segment_refuses_subsequent_writes`.
        let dir = tmp_dir("shardwriter_drop");
        let metrics = Arc::new(Metrics::new().0);
        let mut writer = ShardWriter::new_with_store(
            0,
            dir.clone(),
            1024 * 1024,
            metrics,
            Arc::new(FsSegmentStore),
        );

        writer.write_record(b"first").unwrap();
        assert!(writer.active.is_some(), "segment open after first write");

        // Oversized payload → inner write_record returns Err → active dropped.
        let too_large = vec![0u8; MAX_PAYLOAD_HARD_CAP + 1];
        writer
            .write_record(&too_large)
            .expect_err("oversized write must fail");
        assert!(
            writer.active.is_none(),
            "active segment must be dropped after write error"
        );

        // The next write opens a fresh segment file. The orphaned first segment
        // is left on disk, so the shard dir now holds two active `.wab` files —
        // proving recovery opened a new file rather than reusing the orphan.
        writer.write_record(b"recovered").unwrap();
        assert!(writer.active.is_some(), "segment open after recovery");

        let active_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "wab"))
            .count();
        assert_eq!(
            active_count, 2,
            "recovery must open a new segment file, not reuse the orphaned one"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn segment_counter_from_path_parses_all_lifecycle_extensions() {
        assert_eq!(
            segment_counter_from_path(Path::new("/s/seg_00000001.wab")),
            Some(1)
        );
        assert_eq!(
            segment_counter_from_path(Path::new("/s/seg_00000007.wab.sealed")),
            Some(7)
        );
        assert_eq!(
            segment_counter_from_path(Path::new("/s/seg_00000042.wab.confirmed")),
            Some(42)
        );
        assert_eq!(segment_counter_from_path(Path::new("/s/README")), None);
        assert_eq!(segment_counter_from_path(Path::new("/s/seg_abc.wab")), None);
    }

    #[test]
    fn scan_and_advance_counter_advances_past_sealed_segments() {
        // Regression for the recovery counter-reset data-loss bug: after crash
        // recovery seals the active segment, the shard dir can hold only
        // seg_00000001.wab.sealed (recovered, awaiting drain). A fresh writer MUST
        // advance past it — otherwise its first seal renames seg_00000001.wab over
        // the recovered, undrained sealed segment and loses those records.
        let dir = tmp_dir("scan_past_sealed");
        let active = segment_path(&dir, 1);
        let mut seg = WabSegment::create(&active, 0).unwrap();
        seg.write_record(b"recovered-record").unwrap();
        let sealed = seg.seal().unwrap();
        assert!(
            sealed
                .to_string_lossy()
                .ends_with("seg_00000001.wab.sealed")
        );
        let original = fs::read(&sealed).unwrap();

        // Fresh writer over the same shard dir, as at startup post-recovery.
        let metrics = Arc::new(Metrics::new().0);
        let mut writer = ShardWriter::new_with_store(
            0,
            dir.clone(),
            1024 * 1024,
            metrics,
            Arc::new(FsSegmentStore),
        );
        writer.scan_and_advance_counter().unwrap();

        // New ingest -> new segment -> seal. It must NOT reuse counter 1.
        writer.write_record(b"new-record").unwrap();
        let new_sealed = writer.seal_current().unwrap().expect("a segment was open");
        assert!(
            new_sealed
                .to_string_lossy()
                .ends_with("seg_00000002.wab.sealed"),
            "new segment reused a counter: {}",
            new_sealed.display()
        );
        // The recovered segment survived untouched.
        assert!(sealed.exists(), "recovered sealed segment was deleted");
        assert_eq!(
            fs::read(&sealed).unwrap(),
            original,
            "recovered sealed segment was overwritten"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// G02 (defense-in-depth): seal() must refuse to rename an active segment
    /// over an EXISTING .wab.sealed rather than silently clobber it. This is the
    /// last-line guard behind the flusher's refusal to start at an unestablished
    /// counter — even if the counter ever regressed, no sealed segment is lost.
    #[test]
    fn seal_refuses_to_overwrite_an_existing_sealed_segment() {
        let dir = tmp_dir("seal_no_clobber");
        let active = segment_path(&dir, 1);
        let mut seg = WabSegment::create(&active, 0).unwrap();
        seg.write_record(b"would-clobber").unwrap();

        // Pre-place the sealed target (as a recovered, undrained sealed segment).
        let sealed_target = sealed_path_for(&active);
        fs::write(&sealed_target, b"PRE-EXISTING-RECOVERED-SEGMENT").unwrap();

        let err = seg.seal().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        // The pre-existing sealed segment is untouched.
        assert_eq!(
            fs::read(&sealed_target).unwrap(),
            b"PRE-EXISTING-RECOVERED-SEGMENT",
            "seal clobbered an existing sealed segment"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// G02 (precondition): a failing segment-counter scan (here forced via an
    /// unreadable dir, modelling a transient EMFILE/ENOMEM read_dir failure) must
    /// ERROR and leave next_counter at its default 1 — NOT silently advance. The
    /// flusher reacts to that error by going offline rather than sealing at an
    /// unestablished counter that could overwrite a recovered sealed segment.
    #[test]
    fn scan_and_advance_counter_errors_on_unreadable_dir_leaving_counter_unadvanced() {
        let metrics = Arc::new(Metrics::new().0);
        let missing =
            std::env::temp_dir().join(format!("weir_g02_no_such_shard_dir_{}", std::process::id()));
        let _ = fs::remove_dir_all(&missing);
        let mut writer =
            ShardWriter::new_with_store(0, missing, 1024 * 1024, metrics, Arc::new(FsSegmentStore));
        let err = writer.scan_and_advance_counter().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert_eq!(
            writer.next_counter, 1,
            "a failed scan must not advance next_counter past its default"
        );
    }

    #[test]
    fn wab_segment_rejects_oversized_payload() {
        let dir = tmp_dir("oversize");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        let too_large = vec![0u8; MAX_PAYLOAD_HARD_CAP + 1];
        assert!(seg.write_record(&too_large).is_err());
        let _ = seg.seal();
        fs::remove_dir_all(dir).ok();
    }

    /// Regression: `weir_wab_segments_total{state="open"}` was registered in
    /// `metrics/mod.rs` but never incremented (the other three states —
    /// sealed, confirmed, quarantined — were wired at their respective
    /// lifecycle points). This test pins the new wiring: every time
    /// `ensure_open` creates a fresh segment, the `open` counter moves.
    #[test]
    fn open_segment_counter_increments_when_ensure_open_creates_segment() {
        let dir = tmp_dir("open_counter");
        let metrics = Arc::new(Metrics::new().0);
        // Tiny segment_max_bytes so a single write rotates and creates a
        // second segment — this exercises ensure_open twice in one
        // writer's lifetime (initial + post-rotation).
        let mut writer = ShardWriter::new_with_store(
            0,
            dir.clone(),
            32,
            Arc::clone(&metrics),
            Arc::new(FsSegmentStore),
        );

        let open_counter = || -> u64 {
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::open,
                })
                .get()
        };

        let before = open_counter();
        writer.write_record(b"first").unwrap();
        let after_first = open_counter();
        assert_eq!(
            after_first - before,
            1,
            "first write should have opened one segment"
        );

        // Second write triggers rotation (segment_max_bytes=32 exceeded
        // after the first record's overhead). Opening the replacement
        // segment must also bump the counter.
        writer.write_record(b"second").unwrap();
        let after_second = open_counter();
        assert_eq!(
            after_second - after_first,
            1,
            "post-rotation write should have opened a second segment"
        );

        fs::remove_dir_all(dir).ok();
    }
}
