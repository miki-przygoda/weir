use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crc32fast::Hasher as CrcHasher;

use super::format::{
    EXT_ACTIVE, EXT_SEALED, SEGMENT_HEADER_LEN, SEGMENT_MAX_BYTES, build_segment_footer,
    build_segment_header, build_sentinel, unix_nanos_now,
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

        self.file.write_all(&len_bytes)?;
        self.file.write_all(&crc_bytes)?;
        self.file.write_all(payload)?;

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

    /// Returns true if the segment has reached or exceeded `SEGMENT_MAX_BYTES`.
    pub fn should_rotate(&self) -> bool {
        self.bytes_written >= SEGMENT_MAX_BYTES
    }

    #[allow(dead_code)]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
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
    active: Option<WabSegment>,
}

impl ShardWriter {
    pub fn new(shard_id: u16, shard_dir: PathBuf) -> Self {
        ShardWriter {
            shard_id,
            shard_dir,
            next_counter: 1,
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
    pub fn write_record(&mut self, payload: &[u8]) -> io::Result<Option<PathBuf>> {
        self.ensure_open()?;
        let seg = self.active.as_mut().expect("ensure_open guarantees Some");
        seg.write_record(payload)?;
        if seg.should_rotate() {
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
        let dir = tmp_dir("rotate");
        let path = dir.join("seg_00000001.wab");
        let mut seg = WabSegment::create(&path, 0).unwrap();
        assert!(!seg.should_rotate());
        // Artificially set bytes_written to the threshold.
        seg.bytes_written = SEGMENT_MAX_BYTES;
        assert!(seg.should_rotate());
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
