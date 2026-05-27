use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crc32fast::Hasher as CrcHasher;

use super::format::{
    EXT_ACTIVE, EXT_SEALED, SEGMENT_HEADER_LEN, build_segment_footer, build_segment_header,
    build_sentinel, unix_nanos_now,
};
use weir_core::MAX_PAYLOAD_HARD_CAP;

/// An active WAB segment file. Owns the file handle and tracks write accounting.
/// The running `file_crc_hasher` accumulates CRC32 over every byte written to the
/// file (including the header), so the footer's `file_crc32` field requires no
/// full-file re-read at seal time.
pub struct WabSegment {
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
    /// ensure the file is newly created. The caller must guarantee `path` does
    /// not already exist.
    pub fn create(path: &Path, shard_id: u16) -> io::Result<Self> {
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
        Ok(seg)
    }

    /// Writes one record to the segment.
    ///
    /// Format: `payload_len (u32 LE)` + `crc32 (u32 LE)` + `payload bytes`.
    ///
    /// Uses checked arithmetic throughout: a panic here means the segment has grown
    /// beyond addressable bounds, which is unrecoverable.
    pub fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "segment is poisoned by a previous partial write",
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

        // The three write_alls form one atomic logical record. If any one
        // fails after another has written bytes, the OS file offset has
        // advanced past stray bytes that the in-memory accounting below
        // won't see — poison the segment so subsequent writes are refused.
        if let Err(e) = self
            .file
            .write_all(&len_bytes)
            .and_then(|_| self.file.write_all(&crc_bytes))
            .and_then(|_| self.file.write_all(payload))
        {
            self.poisoned = true;
            return Err(e);
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
    /// macOS: `F_BARRIERFSYNC` — writes data to persistent storage with a barrier,
    /// guaranteeing ordering without waiting for all pending media writes. Faster than
    /// `F_FULLFSYNC` and sufficient for WAB durability.
    ///
    /// Linux: `fdatasync` — flushes data and critical metadata without waiting for
    /// directory entry updates.
    pub fn fsync(&self) -> io::Result<()> {
        platform_fsync(&self.file)
    }

    /// Returns true if the segment has reached or exceeded the configured
    /// rotation threshold. The threshold is owned by `ShardWriter` so a single
    /// `WabSegment` instance can be reused under different policies (e.g. tests).
    pub fn should_rotate(&self, max_bytes: u64) -> bool {
        self.bytes_written >= max_bytes
    }

    /// Number of records written so far. Test-only accessor — production
    /// code reads the in-memory counter directly during `seal()` to write
    /// the segment footer.
    #[cfg(test)]
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Seals the segment: writes sentinel + footer, fsyncs, and atomically renames
    /// the file from `.wab` to `.wab.sealed`.
    ///
    /// Consumes `self` to ensure the segment cannot be written to after sealing.
    /// Returns the path of the newly sealed file.
    pub fn seal(mut self) -> io::Result<PathBuf> {
        let sealed_at = unix_nanos_now();

        // Finalise CRC before writing sentinel (sentinel is not covered by file_crc32).
        let file_crc32 = self.file_crc_hasher.finalize();

        self.file.write_all(&build_sentinel())?;

        let footer =
            build_segment_footer(self.record_count, self.data_bytes, file_crc32, sealed_at);
        self.file.write_all(&footer)?;

        platform_fsync(&self.file)?;

        let sealed_path = sealed_path_for(&self.path);
        std::fs::rename(&self.path, &sealed_path)?;

        Ok(sealed_path)
    }
}

/// Derives the `.wab.sealed` path from an active `.wab` path.
pub fn sealed_path_for(active_path: &Path) -> PathBuf {
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

/// Derives the segment counter from a file stem like `seg_00000001`.
pub fn segment_counter_from_path(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    stem.strip_prefix("seg_")?.parse().ok()
}

/// Formats a segment file name for a given shard directory and counter.
pub fn segment_path(shard_dir: &Path, counter: u64) -> PathBuf {
    shard_dir.join(format!("seg_{counter:08}{EXT_ACTIVE}"))
}

/// Manages the active segment for one shard. Opens the segment lazily on first write.
pub struct ShardWriter {
    shard_id: u16,
    shard_dir: PathBuf,
    /// Counter used to name the next segment file (incremented on each rotation).
    next_counter: u64,
    /// Bytes threshold at which the active segment is sealed and rotated.
    /// Set via `WabConfig::segment_max_bytes` at flusher-thread spawn time.
    segment_max_bytes: u64,
    active: Option<WabSegment>,
}

impl ShardWriter {
    pub fn new(shard_id: u16, shard_dir: PathBuf, segment_max_bytes: u64) -> Self {
        ShardWriter {
            shard_id,
            shard_dir,
            next_counter: 1,
            segment_max_bytes,
            active: None,
        }
    }

    /// Sets `next_counter` to one past the highest existing segment counter in the
    /// shard directory. Called during startup so new segments don't collide with
    /// existing (sealed) ones.
    pub fn scan_and_advance_counter(&mut self) -> io::Result<()> {
        let mut max: u64 = 0;
        for entry in std::fs::read_dir(&self.shard_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(n) = segment_counter_from_path(&path)
                && n > max
            {
                max = n;
            }
        }
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
    pub fn write_record(&mut self, payload: &[u8]) -> io::Result<Option<PathBuf>> {
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
    pub fn fsync_current(&self) -> io::Result<()> {
        if let Some(seg) = &self.active {
            seg.fsync()?;
        }
        Ok(())
    }

    /// Seals the current active segment and returns its sealed path.
    /// Returns `None` if no segment is currently open.
    pub fn seal_current(&mut self) -> io::Result<Option<PathBuf>> {
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
            self.active = Some(WabSegment::create(&path, self.shard_id)?);
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn platform_fsync(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // F_BARRIERFSYNC (85): flushes data to persistent storage with a write barrier,
    // ensuring ordering without waiting for all pending media writes. Faster than
    // F_FULLFSYNC while still providing the ordering guarantee the WAB requires.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    pub fn tmp_dir(label: &str) -> PathBuf {
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
        // After an inner WabSegment returns Err from write_record, ShardWriter
        // must drop its `active` segment so the next call opens a fresh file
        // with a new counter — otherwise subsequent writes would interleave
        // with the stray bytes from the failed write.
        let dir = tmp_dir("shardwriter_drop");
        let mut writer = ShardWriter::new(0, dir.clone(), 1024 * 1024);
        writer.write_record(b"first").unwrap();
        let active_path_before = writer
            .active
            .as_ref()
            .expect("segment open after first write")
            .path
            .clone();

        // Poison the inner segment so the next write_record fails.
        writer.active.as_mut().unwrap().poisoned = true;
        writer
            .write_record(b"poisoned")
            .expect_err("write to poisoned segment must fail");
        assert!(
            writer.active.is_none(),
            "active segment must be dropped after write error"
        );

        // The next write opens a fresh segment with a different file path.
        writer.write_record(b"recovered").unwrap();
        let active_path_after = writer
            .active
            .as_ref()
            .expect("segment open after recovery")
            .path
            .clone();
        assert_ne!(
            active_path_before, active_path_after,
            "recovery must open a new segment file, not reuse the poisoned one"
        );

        fs::remove_dir_all(dir).ok();
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
}
