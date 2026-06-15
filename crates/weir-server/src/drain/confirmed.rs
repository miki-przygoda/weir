//! `.confirmed` sidecar file handling for drained segments.
//!
//! After the drain thread successfully forwards every record of a sealed
//! segment to its sink, it writes a small `.confirmed` sidecar file next to
//! the segment and then deletes the segment itself. The sidecar carries the
//! `sealed_at` timestamp (copied from the segment footer), the record count,
//! and the `drained_at` timestamp so an operator inspecting the WAB
//! directory can reconstruct the drain timeline without re-reading the
//! original segment.
//!
//! Crash semantics: crash recovery (in `wab::recovery::check_confirmed`)
//! skips segments that have a valid `.confirmed` file, so a crash between
//! `write_confirmed_file` and `remove_file` leaves the segment in an
//! "already drained, orphan file on disk" state — safe; an operator can
//! clean it up at any time.

use std::io;
use std::path::{Path, PathBuf};

use tracing::{error, warn};

use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use crate::wab::format::{SEGMENT_FOOTER_LEN, build_confirmed, unix_nanos_now};

/// Writes the `.confirmed` sidecar for `sealed` and removes the sealed
/// segment. Bumps the `wab_segments{state=confirmed}` counter so the
/// confirmed transition is observable in Prometheus.
pub(super) fn confirm_and_delete(sealed: &Path, record_count: u64, metrics: &Metrics) {
    if let Err(e) = write_confirmed_file(sealed, record_count) {
        // Could not durably record the confirmation. Do NOT delete the segment:
        // leaving it (with no .confirmed sidecar) keeps recovery consistent — the
        // segment is re-drained on the next restart (a duplicate, absorbed by the
        // at-least-once + dedup contract) rather than being deleted with no
        // record that it was ever drained.
        error!(
            path = %sealed.display(),
            error = %e,
            "drain: .confirmed write failed; preserving segment for re-drain on restart"
        );
        return;
    }
    if let Err(e) = std::fs::remove_file(sealed) {
        warn!(path = %sealed.display(), error = %e, "drain: failed to delete confirmed segment");
    }
    metrics
        .wab_segments
        .get_or_create(&SegmentStateLabel {
            state: SegmentState::confirmed,
        })
        .inc();
}

/// Writes the `.confirmed` sidecar next to `sealed`, making both its contents and
/// its directory entry durable. Returns `Err` if it can't be durably written; the
/// caller (`confirm_and_delete`) then preserves the segment so it is re-drained on
/// the next restart (recovery treats a missing `.confirmed` as "needs draining").
pub(super) fn write_confirmed_file(sealed: &Path, record_count: u64) -> io::Result<()> {
    let confirmed = confirmed_path(sealed);
    let sealed_at = read_sealed_at_nanos(sealed).unwrap_or(0);
    let bytes = build_confirmed(sealed_at, record_count, unix_nanos_now());
    write_confirmed_durably(&confirmed, &bytes)
}

/// Writes the sidecar and makes both its contents and its directory entry
/// durable. Without the fsyncs a crash can lose (or tear) the `.confirmed` file,
/// causing the already-delivered segment to be re-drained on restart — duplicate
/// delivery (tolerated by the at-least-once + dedup contract, but avoidable).
fn write_confirmed_durably(confirmed: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;

    // Explicit 0o600 so the sidecar is daemon-private regardless of the process
    // umask. Plain File::create relies on the umask alone (default mode 0o666);
    // under any umask other than 0o077 the .confirmed file becomes
    // group/world-readable, and the daemon's own recovery audit
    // (audit_segment_modes) flags a confirmed file with mode != 0o600 as possible
    // tampering — a false-positive on the next restart. Matches the hardened mode
    // in WabSegment::create (F10). create+truncate (not create_new) so a torn
    // sidecar from a crashed earlier attempt is overwritten on re-drain.
    #[cfg(unix)]
    let mut f = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(confirmed)?
    };
    #[cfg(not(unix))]
    let mut f = std::fs::File::create(confirmed)?;

    f.write_all(bytes)?;
    f.sync_all()?; // sidecar contents durable
    crate::wab::segment::fsync_parent_dir(confirmed)?; // sidecar dirent durable
    Ok(())
}

/// Derives the `.wab.confirmed` path from a `.wab.sealed` path. Thin wrapper
/// over the shared [`crate::wab::format::confirmed_path_for`] (the single
/// source of truth, shared with recovery's read side). Kept `pub(super)` so the
/// drain tests can verify side-effect presence
/// (`assert!(confirmed_path(&sealed).exists())`).
pub(super) fn confirmed_path(sealed: &Path) -> PathBuf {
    crate::wab::format::confirmed_path_for(sealed)
}

/// Reads the `sealed_at` timestamp from the segment footer (last 32 bytes
/// of the file). Returns 0 on any read failure — the field is informational
/// only.
fn read_sealed_at_nanos(path: &Path) -> io::Result<i64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len < (SEGMENT_FOOTER_LEN as u64 + 4) {
        return Ok(0);
    }
    file.seek(SeekFrom::End(-(SEGMENT_FOOTER_LEN as i64)))?;
    let mut footer = [0u8; SEGMENT_FOOTER_LEN];
    file.read_exact(&mut footer)?;
    // sealed_at is at footer bytes [20..28] — see wab_format.md.
    Ok(i64::from_le_bytes(footer[20..28].try_into().unwrap()))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// F10: the `.confirmed` sidecar must be created 0o600 explicitly, not left
    /// to the umask. With an explicit 0o600 the result is umask-independent
    /// (0o600 has no group/other bits for any standard umask to clear), so the
    /// daemon's own recovery audit never false-positives on it.
    #[test]
    fn confirmed_sidecar_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("weir_f10_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg_00000000.wab.confirmed");

        write_confirmed_durably(&path, b"sidecar-bytes").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "sidecar mode {mode:#o} != 0o600");

        std::fs::remove_dir_all(&dir).ok();
    }
}
