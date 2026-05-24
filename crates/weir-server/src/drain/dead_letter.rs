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

use weir_core::Payload;

use crate::wab::{create_dir_private, segment::WabSegment};

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

        let active_path = self.dir.join(format!("dl_{:08}.wab", self.next_counter));
        // Shard ID 0xFFFF is reserved as a dead-letter marker.
        let mut seg = WabSegment::create(&active_path, 0xFFFF)?;
        for payload in records {
            seg.write_record(payload)?;
        }
        let sealed = seg.seal()?;

        // Update running total with the sealed file's actual size.
        let file_bytes = std::fs::metadata(&sealed).map(|m| m.len()).unwrap_or(0);
        self.total_bytes = self.total_bytes.saturating_add(file_bytes);
        self.next_counter += 1;

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
