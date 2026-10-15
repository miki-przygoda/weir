//! Dead-letter segment writer.
//!
//! Records permanently rejected by the sink are appended to sealed WAB segments
//! under `<wab_dir>/dead_letter/`. The directory uses the same segment format as
//! the main WAB so `SegmentReader` can read dead-letter files without a separate
//! parser. Dead-letter files are named `dl_NNNNNNNN.wab.sealed`.
//!
//! The total size of the dead-letter directory is tracked as a running counter
//! (scanned from disk on startup) and used to enforce `dead_letter_max_bytes`.

use std::{
    io,
    path::{Path, PathBuf},
};

use tracing::warn;
use weir_core::Payload;

use crate::wab::{
    create_dir_private,
    format::{Compression, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, SENTINEL},
    segment::{WabSegment, fsync_parent_dir},
};

/// Bytes a sealed dead-letter segment occupies for `records`: the header +
/// 8 bytes/record (payload_len + crc32) + the payload bytes + the sentinel +
/// the footer. Single source of truth for dead-letter cap accounting — used both
/// to pre-check `would_exceed_cap` and as a fallback when stat-after-seal fails.
pub(crate) fn estimated_segment_bytes(records: &[Payload]) -> u64 {
    const RECORD_OVERHEAD: u64 = 8; // payload_len (4) + crc32 (4)
    let body: u64 = records
        .iter()
        .map(|p| RECORD_OVERHEAD + p.len() as u64)
        .sum();
    SEGMENT_HEADER_LEN as u64 + body + SENTINEL.len() as u64 + SEGMENT_FOOTER_LEN as u64
}

/// Writes permanently-rejected records to the dead-letter directory and tracks
/// the running total byte size.
pub(crate) struct DeadLetterWriter {
    dir: PathBuf,
    next_counter: u64,
    /// Running total of all file bytes in the dead-letter directory. Accurate
    /// once `open` has scanned the directory; updated after every successful write.
    total_bytes: u64,
    /// True once `wab_dir` has been fsynced since the directory was last
    /// (re)created, so its entry there is durable. No record is written while
    /// it is false: a dead-lettered record counts as handled and its source
    /// segment is then confirmed and deleted, so the record must not live in a
    /// directory a power loss could unlink (conf-durability-4).
    dir_entry_durable: bool,
}

impl DeadLetterWriter {
    /// Opens the dead-letter directory (creating it if needed) and scans existing
    /// files to initialise `total_bytes` and `next_counter`.
    pub(crate) fn open(wab_dir: &Path) -> io::Result<Self> {
        let dir = wab_dir.join("dead_letter");
        create_dir_private(dir.clone())?;
        let (total_bytes, max_counter) = scan_dir(&dir)?;
        // The fsync of `wab_dir` is left to the first write: an existing
        // directory from an earlier run needs none, and a failure here would
        // stop the drain from starting over a sync no record depends on yet.
        Ok(Self {
            dir,
            next_counter: max_counter + 1,
            total_bytes,
            dir_entry_durable: false,
        })
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Rescans the dead-letter directory and refreshes `total_bytes`. Called by the
    /// drain when it wakes from a blocked-full wait to detect externally-deleted files.
    ///
    /// A directory that no longer exists is the operator's most direct way to
    /// free headroom (`rm -rf dead_letter/`), so it is not an error: it is
    /// recreated (private, like [`Self::open`]) and counts as empty (spec §4.5).
    /// Before, the `read_dir` error left `total_bytes` stuck at its at-cap value
    /// and a drain blocked on headroom never unblocked. `next_counter` is kept,
    /// so no file name is reused within the run.
    pub(crate) fn rescan(&mut self) -> io::Result<()> {
        match scan_dir(&self.dir) {
            Ok((total_bytes, _)) => {
                self.total_bytes = total_bytes;
                Ok(())
            }
            // NotFound can also be a file that vanished between `read_dir` and
            // its stat; only a missing directory means "empty".
            Err(e) if e.kind() == io::ErrorKind::NotFound && self.dir_is_missing() => {
                self.recreate_dir()
            }
            Err(e) => Err(e),
        }
    }

    /// True when the dead-letter directory itself is gone.
    fn dir_is_missing(&self) -> bool {
        matches!(
            std::fs::symlink_metadata(&self.dir),
            Err(e) if e.kind() == io::ErrorKind::NotFound
        )
    }

    /// Recreates a deleted dead-letter directory. Everything that was in it is
    /// gone, so nothing counts toward the cap any more.
    ///
    /// The new directory's entry in `wab_dir` is then made durable. A failed
    /// fsync is an error, but the directory is kept and counted empty; the
    /// next write retries the fsync before it writes anything.
    fn recreate_dir(&mut self) -> io::Result<()> {
        warn!(
            dir = %self.dir.display(),
            "dead-letter: directory is missing (deleted while the daemon runs?); recreating it"
        );
        self.create_dir()?;
        self.total_bytes = 0;
        self.make_dir_entry_durable()
    }

    /// Creates the directory (a no-op when it exists) and marks its entry as
    /// not yet durable.
    fn create_dir(&mut self) -> io::Result<()> {
        self.dir_entry_durable = false;
        create_dir_private(self.dir.clone())
    }

    /// Fsyncs `wab_dir` unless that already happened since the directory was
    /// last created, so a record is never written into a directory whose own
    /// entry a power loss could still undo.
    fn make_dir_entry_durable(&mut self) -> io::Result<()> {
        if !self.dir_entry_durable {
            fsync_parent_dir(&self.dir)?;
            self.dir_entry_durable = true;
        }
        Ok(())
    }

    /// Returns true if adding `additional_bytes` would push the total over `cap`.
    pub(crate) fn would_exceed_cap(&self, additional_bytes: u64, cap: u64) -> bool {
        self.total_bytes.saturating_add(additional_bytes) > cap
    }

    /// Writes `records` to a new sealed dead-letter segment. The segment is sealed
    /// (fsynced and renamed) before this function returns, so a crash after a
    /// successful call leaves a complete file.
    ///
    /// A deleted `dead_letter/` directory (ENOENT) is recreated once and the
    /// write retried (spec §4.9): only `open()` created it before, so a
    /// directory removed while the daemon ran failed every dead-letter write
    /// for the rest of the run.
    pub(crate) fn write_records(&mut self, records: &[Payload]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        if let Some(e) = write_hook::on_write(&self.dir) {
            return Err(e);
        }
        self.make_dir_entry_durable()?;
        match self.write_sealed(records) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if self.dir_is_missing() {
                    self.recreate_dir()?;
                } else {
                    self.create_dir()?;
                    self.make_dir_entry_durable()?;
                }
                self.write_sealed(records)
            }
            other => other,
        }
    }

    /// One attempt at [`Self::write_records`]: a new sealed segment under a
    /// freshly reserved counter.
    fn write_sealed(&mut self, records: &[Payload]) -> io::Result<()> {
        let counter = self.next_counter;
        // Reserve the counter BEFORE the fallible create/write/seal. If any of
        // those fail mid-way they leave a partial dl_<counter>.wab behind;
        // without reserving, the next call would reuse `counter`, hit
        // create_new's AlreadyExists on that partial, and then fail EVERY
        // subsequent dead-letter for the rest of the run (commit_batch retries
        // dead-letter failures transiently, so the poison is permanent). The
        // cost of reserving is at most a skipped counter on failure. Crash
        // recovery does NOT re-seal the orphaned partial: it skips the
        // dead_letter/ dir entirely (F16, scan_unconfirmed_sealed). The orphan
        // is instead accounted for by the next DeadLetterWriter::open — scan_dir
        // advances next_counter past the partial's counter, so the orphan is
        // never reused and never poisons a future write (it just lingers on disk
        // until an operator clears it via weir-ctl dl).
        self.next_counter += 1;

        let active_path = self.dir.join(format!("dl_{counter:08}.wab"));
        // Shard ID 0xFFFF is reserved as a dead-letter marker.
        //
        // Dead-letter segments are ALWAYS written uncompressed, regardless of
        // the daemon's wab_compression setting. They are a small forensic
        // artifact whose whole purpose is being readable later, possibly by
        // tooling that is not this daemon; compressing them trades nothing for
        // a readability risk.
        let mut seg = WabSegment::create(&active_path, 0xFFFF, Compression::None)?;
        for payload in records {
            seg.write_record(payload)?;
        }
        let sealed = seg.seal()?;

        // Update the running total with the sealed file's actual size. The
        // records are already durably sealed here; if the stat fails, fall back
        // to the content-size estimate rather than 0 — a 0 would silently
        // undercount total_bytes and let the dead-letter dir grow past
        // dead_letter_max_bytes (the cap's only gate).
        let file_bytes = match std::fs::metadata(&sealed) {
            Ok(m) => m.len(),
            Err(e) => {
                warn!(
                    path = %sealed.display(),
                    error = %e,
                    "dead-letter: stat after seal failed; using estimated size for cap accounting"
                );
                estimated_segment_bytes(records)
            }
        };
        self.total_bytes = self.total_bytes.saturating_add(file_bytes);

        Ok(())
    }
}

/// Test-only fault injection for [`DeadLetterWriter::write_records`], keyed by
/// the dead-letter directory so concurrently running tests (each in its own
/// scratch dir) never see each other's faults.
#[cfg(test)]
pub(crate) mod write_hook {
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    struct Entry {
        errno: i32,
        fail_left: u32,
        fired: u64,
    }

    static HOOKS: Mutex<Option<HashMap<PathBuf, Entry>>> = Mutex::new(None);

    fn with<R>(f: impl FnOnce(&mut HashMap<PathBuf, Entry>) -> R) -> R {
        let mut guard = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        f(guard.get_or_insert_with(HashMap::new))
    }

    /// Fails the next `times` dead-letter writes into `dir` with `errno`.
    pub(crate) fn fail(dir: &Path, errno: i32, times: u32) {
        with(|m| {
            m.insert(
                dir.to_path_buf(),
                Entry {
                    errno,
                    fail_left: times,
                    fired: 0,
                },
            )
        });
    }

    /// How many writes into `dir` the fault has failed.
    pub(crate) fn fired(dir: &Path) -> u64 {
        with(|m| m.get(dir).map_or(0, |e| e.fired))
    }

    /// Stops faulting `dir`.
    pub(crate) fn forget(dir: &Path) {
        with(|m| m.remove(dir));
    }

    pub(super) fn on_write(dir: &Path) -> Option<io::Error> {
        with(|m| {
            let e = m.get_mut(dir)?;
            if e.fail_left == 0 {
                return None;
            }
            e.fail_left -= 1;
            e.fired += 1;
            Some(io::Error::from_raw_os_error(e.errno))
        })
    }
}

/// Returns `(total_bytes, max_counter)` for the dead-letter directory.
fn scan_dir(dir: &Path) -> io::Result<(u64, u64)> {
    let mut total_bytes = 0u64;
    let mut max_counter = 0u64;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();

        if name.starts_with("dl_") {
            total_bytes = total_bytes.saturating_add(entry.metadata()?.len());

            // Extract counter from "dl_NNNNNNNN.wab" or "dl_NNNNNNNN.wab.sealed"
            if let Some(after_prefix) = name.strip_prefix("dl_")
                && let Some(counter_str) = after_prefix.split('.').next()
                && let Ok(counter) = counter_str.parse::<u64>()
            {
                max_counter = max_counter.max(counter);
            }
        }
    }

    Ok((total_bytes, max_counter))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = crate::testutil::scratch_dir(&format!("dl_{label}"));
        crate::testutil::mkdir_p(&dir);
        dir
    }

    fn p(s: &'static [u8]) -> Payload {
        Payload::from_static(s)
    }

    #[test]
    fn write_records_seals_advances_counter_and_tracks_bytes() {
        let dir = tmp_dir("write");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        assert_eq!(dl.total_bytes(), 0);

        dl.write_records(&[p(b"alpha"), p(b"beta")]).unwrap();
        assert!(dl.dir.join("dl_00000001.wab.sealed").exists());
        assert!(dl.total_bytes() > 0);

        dl.write_records(&[p(b"gamma")]).unwrap();
        assert!(dl.dir.join("dl_00000002.wab.sealed").exists());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn open_recovers_counter_and_bytes_across_restart() {
        let dir = tmp_dir("restart");
        {
            let mut dl = DeadLetterWriter::open(&dir).unwrap();
            dl.write_records(&[p(b"one")]).unwrap();
            dl.write_records(&[p(b"two")]).unwrap();
        }
        // A fresh writer (process restart) must scan past the existing files.
        let dl2 = DeadLetterWriter::open(&dir).unwrap();
        assert_eq!(
            dl2.next_counter, 3,
            "counter must resume past existing files"
        );
        assert!(
            dl2.total_bytes() > 0,
            "total_bytes must be recovered from disk"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn write_records_does_not_poison_after_a_failed_attempt() {
        // F01 regression: a partial dl_00000001.wab left by a failed attempt must
        // not poison all future dead-lettering. The first call collides with the
        // partial (create_new -> AlreadyExists) and errors, but because the
        // counter is reserved up front the retry uses a fresh counter and succeeds.
        let dir = tmp_dir("poison");
        let dldir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dldir);
        std::fs::write(dldir.join("dl_00000001.wab"), b"partial").unwrap();

        // Model the in-run state right after the failure (counter not yet past
        // the partial); open() would scan past it, so construct directly.
        let mut dl = DeadLetterWriter {
            dir: dldir.clone(),
            next_counter: 1,
            total_bytes: 0,
            dir_entry_durable: false,
        };
        assert!(
            dl.write_records(&[p(b"x")]).is_err(),
            "first attempt collides with the leftover partial"
        );
        dl.write_records(&[p(b"y")])
            .expect("retry must use a fresh counter, not re-collide");
        assert!(dldir.join("dl_00000002.wab.sealed").exists());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn estimated_segment_bytes_equals_real_sealed_size() {
        // Locks F06/F07: the cap estimate must match the real sealed file size,
        // so the stat-failure fallback and the would_exceed_cap pre-check are
        // accurate (no silent over/under-count of dead_letter_max_bytes).
        let dir = tmp_dir("estimate");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let recs = vec![p(b"alpha"), p(b"beta-record"), p(b"")];
        // empty payloads can't be dead-lettered (sentinel), drop it
        let recs: Vec<Payload> = recs.into_iter().filter(|r| !r.is_empty()).collect();
        dl.write_records(&recs).unwrap();
        let sealed = dl.dir.join("dl_00000001.wab.sealed");
        let real = std::fs::metadata(&sealed).unwrap().len();
        assert_eq!(
            estimated_segment_bytes(&recs),
            real,
            "cap estimate must equal the real sealed size"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn would_exceed_cap_boundary() {
        let dir = tmp_dir("cap");
        let dl = DeadLetterWriter::open(&dir).unwrap();
        assert!(!dl.would_exceed_cap(100, 100));
        assert!(dl.would_exceed_cap(101, 100));
        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.5: an operator who `rm -rf`s `dead_letter/` to free headroom
    /// must not leave the writer stuck at its at-cap total. `rescan` recreates
    /// the directory, counts it as empty, and keeps the counter (so a name is
    /// never reused within the run).
    #[test]
    fn rescan_recreates_a_deleted_directory_and_counts_it_empty() {
        let dir = tmp_dir("rescan_gone");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        dl.write_records(&[p(b"one")]).unwrap();
        assert!(dl.total_bytes() > 0);
        let counter = dl.next_counter;

        std::fs::remove_dir_all(&dl.dir).unwrap();
        dl.rescan()
            .expect("a missing dead_letter/ is recreated, not an error");
        assert!(dl.dir.is_dir(), "the directory is recreated");
        assert_eq!(dl.total_bytes(), 0, "nothing is left in it");
        assert_eq!(dl.next_counter, counter, "the counter is kept");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dl.dir).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "recreated private, like open()");
        }

        // A rescan of a directory that exists but cannot be listed still fails:
        // only a missing directory means "empty".
        std::fs::remove_dir_all(&dl.dir).unwrap();
        std::fs::write(&dl.dir, b"not a directory").unwrap();
        assert!(dl.rescan().is_err());

        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.9 rider: only `open()` created the directory, so a deleted
    /// `dead_letter/` failed every write for the rest of the run. One ENOENT
    /// recreates it and retries the write once.
    #[test]
    fn write_records_recreates_a_deleted_directory_once() {
        let dir = tmp_dir("write_gone");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        dl.write_records(&[p(b"before")]).unwrap();
        std::fs::remove_dir_all(&dl.dir).unwrap();

        dl.write_records(&[p(b"after")])
            .expect("a missing dead_letter/ is recreated and the write retried");
        let sealed: Vec<_> = std::fs::read_dir(&dl.dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
            .collect();
        assert_eq!(
            sealed.len(),
            1,
            "exactly one dead-letter segment: {sealed:?}"
        );
        let records: Vec<Payload> = crate::wab::SegmentReader::open(&sealed[0])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(records, vec![p(b"after")]);
        assert_eq!(
            dl.total_bytes(),
            std::fs::metadata(&sealed[0]).unwrap().len(),
            "the deleted files no longer count toward the cap"
        );

        // Something other than ENOENT is not retried: a file squatting on the
        // directory's name fails the write.
        std::fs::remove_dir_all(&dl.dir).unwrap();
        std::fs::write(&dl.dir, b"not a directory").unwrap();
        assert!(dl.write_records(&[p(b"lost?")]).is_err());

        std::fs::remove_dir_all(dir).ok();
    }

    /// conf-durability-4: a `dead_letter/` recreated at runtime is a new
    /// directory entry in `wab_dir`, and only an fsync of `wab_dir` makes it
    /// durable. A dead-lettered record counts as handled, so its source segment
    /// is then confirmed and deleted; if a power loss lost the recreated
    /// directory's entry, the record would be gone with nothing pointing at it.
    /// So no record is written into a recreated directory until `wab_dir` has
    /// been fsynced: a failed fsync fails the write, and the next write that
    /// finds the directory in place still syncs `wab_dir` before writing.
    ///
    /// Covers both re-creation paths: the blocked drain's `rescan` and the
    /// write's own ENOENT retry.
    #[cfg(unix)]
    #[test]
    fn a_recreated_directory_is_fsynced_into_wab_dir_before_any_write() {
        use crate::wab::segment::sync_hook;
        for via_rescan in [true, false] {
            let dir = tmp_dir(&format!("recreate_sync_{via_rescan}"));
            let mut dl = DeadLetterWriter::open(&dir).unwrap();
            dl.write_records(&[p(b"before")]).unwrap();
            std::fs::remove_dir_all(&dl.dir).unwrap();

            sync_hook::fail(&dir, libc::EIO, u32::MAX);
            if via_rescan {
                assert!(
                    dl.rescan().is_err(),
                    "a recreated directory whose entry cannot be made durable is an error"
                );
                assert!(dl.dir.is_dir(), "it is recreated all the same");
                assert_eq!(dl.total_bytes(), 0, "and it is empty");
            }
            assert!(
                dl.write_records(&[p(b"lost?")]).is_err(),
                "via_rescan={via_rescan}: no record goes into a directory wab_dir has not synced"
            );
            let written: Vec<_> = std::fs::read_dir(&dl.dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();
            assert!(written.is_empty(), "nothing written: {written:?}");
            let failed_syncs = sync_hook::calls(&dir);
            assert!(failed_syncs >= 1, "wab_dir's fsync was attempted");

            sync_hook::clear(&dir);
            dl.write_records(&[p(b"after")]).unwrap();
            assert_eq!(
                sync_hook::calls(&dir),
                failed_syncs + 1,
                "the write that finds the directory in place syncs wab_dir first"
            );
            dl.write_records(&[p(b"again")]).unwrap();
            assert_eq!(
                sync_hook::calls(&dir),
                failed_syncs + 1,
                "once durable, later writes do not sync wab_dir again"
            );
            sync_hook::forget(&dir);
            std::fs::remove_dir_all(dir).ok();
        }
    }

    #[test]
    fn the_write_hook_fails_a_bounded_number_of_writes() {
        let dir = tmp_dir("write_hook");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        write_hook::fail(&dl.dir, libc::ENOSPC, 2);
        for _ in 0..2 {
            let e = dl.write_records(&[p(b"x")]).unwrap_err();
            assert_eq!(e.raw_os_error(), Some(libc::ENOSPC));
        }
        dl.write_records(&[p(b"x")]).unwrap();
        assert_eq!(write_hook::fired(&dl.dir), 2);
        write_hook::forget(&dl.dir);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dead_letter_segments_are_always_uncompressed() {
        // Deliberate, and independent of the daemon's wab_compression setting:
        // a dead-letter segment exists to be read by a human or by tooling that
        // may not be this daemon. It is small, and readability beats ratio.
        let dir = tmp_dir("dl_uncompressed");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        dl.write_records(&[p(b"rejected")]).unwrap();

        let path = dl.dir.join("dl_00000001.wab.sealed");
        let mut header = [0u8; crate::wab::format::SEGMENT_HEADER_LEN];
        {
            use std::io::Read;
            std::fs::File::open(&path)
                .unwrap()
                .read_exact(&mut header)
                .unwrap();
        }
        assert_eq!(
            header[4],
            crate::wab::format::FORMAT_VERSION_V1,
            "dead-letter segments must stay format v1"
        );
        assert_eq!(header[5], 0, "and carry no compression flag");

        std::fs::remove_dir_all(dir).ok();
    }
}
