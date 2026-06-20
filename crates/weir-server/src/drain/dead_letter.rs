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
    format::{SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, SENTINEL},
    segment::WabSegment,
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
}

impl DeadLetterWriter {
    /// Opens the dead-letter directory (creating it if needed) and scans existing
    /// files to initialise `total_bytes` and `next_counter`.
    pub(crate) fn open(wab_dir: &Path) -> io::Result<Self> {
        let dir = wab_dir.join("dead_letter");
        create_dir_private(dir.clone())?;
        let (total_bytes, max_counter) = scan_dir(&dir)?;
        Ok(Self {
            dir,
            next_counter: max_counter + 1,
            total_bytes,
        })
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Rescans the dead-letter directory and refreshes `total_bytes`. Called by the
    /// drain when it wakes from a blocked-full wait to detect externally-deleted files.
    pub(crate) fn rescan(&mut self) -> io::Result<()> {
        let (total_bytes, _) = scan_dir(&self.dir)?;
        self.total_bytes = total_bytes;
        Ok(())
    }

    /// Returns true if adding `additional_bytes` would push the total over `cap`.
    pub(crate) fn would_exceed_cap(&self, additional_bytes: u64, cap: u64) -> bool {
        self.total_bytes.saturating_add(additional_bytes) > cap
    }

    /// Writes `records` to a new sealed dead-letter segment. The segment is sealed
    /// (fsynced and renamed) before this function returns, so a crash after a
    /// successful call leaves a complete file.
    pub(crate) fn write_records(&mut self, records: &[Payload]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }

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
        let mut seg = WabSegment::create(&active_path, 0xFFFF)?;
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
        let dir = std::env::temp_dir().join(format!("weir_dl_{label}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
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
        std::fs::create_dir_all(&dldir).unwrap();
        std::fs::write(dldir.join("dl_00000001.wab"), b"partial").unwrap();

        // Model the in-run state right after the failure (counter not yet past
        // the partial); open() would scan past it, so construct directly.
        let mut dl = DeadLetterWriter {
            dir: dldir.clone(),
            next_counter: 1,
            total_bytes: 0,
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
}
