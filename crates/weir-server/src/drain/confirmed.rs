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
//! skips segments that have a valid `.confirmed` file. The sidecar is written
//! atomically (a `.confirmed.tmp` that is fsynced, then renamed into place), so
//! the final name only ever holds complete, durable contents: a crash or a
//! full disk mid-write leaves at worst a stale `.tmp`, which every walker
//! ignores and the next scan deletes, never a torn sidecar that recovery would
//! read as corruption. A crash between the sidecar and the delete leaves an
//! orphan sealed file beside a valid sidecar; the scan deletes it once the
//! daemon owns the `wab_dir` (`DrainConfig::orphan_cleanup`).
//!
//! Neither step is ever retried by re-delivering the segment. A failure is
//! reported as a [`ConfirmOutcome`] and the drain's ledger retries only the
//! step that failed, on every health poll (spec §4.3).

use std::io;
use std::path::{Path, PathBuf};

use tracing::{error, warn};

use crate::metrics::{
    ConfirmFailureLabel, ConfirmFailureStage, Metrics, SegmentState, SegmentStateLabel,
};
use crate::wab::format::{SEGMENT_FOOTER_LEN, build_confirmed, unix_nanos_now};

/// How far a confirm got. The drain's ledger keeps a segment that is not
/// `Done` in `confirm_pending` and retries only the outstanding step; the
/// segment is never delivered again in this run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfirmOutcome {
    /// The sidecar is durable and the segment is gone (or was already gone).
    Done,
    /// The sidecar could not be made durable under its final name. The segment
    /// is kept and nothing was deleted.
    SidecarFailed,
    /// The sidecar is durable, but the segment could not be deleted.
    DeleteFailed,
}

/// Writes the `.confirmed` sidecar for `sealed` and removes the sealed
/// segment. Bumps the `wab_segments{state=confirmed}` counter once the sidecar
/// is durable, so the confirmed transition is observable in Prometheus, and
/// `weir_drain_confirm_failures_total{stage}` on a failed step.
pub(super) fn confirm_and_delete(
    sealed: &Path,
    record_count: u64,
    metrics: &Metrics,
) -> ConfirmOutcome {
    if !sealed.exists() {
        // The segment is already gone — e.g. process_segment reported a NotFound
        // open as Confirmed{0} (a genuine no-op), or an operator removed a
        // segment whose confirm was outstanding. Writing a .confirmed sidecar for
        // a segment that no longer exists would leave an orphan sidecar with no
        // segment behind it; there is nothing to confirm or delete, so skip (S23).
        return ConfirmOutcome::Done;
    }
    if let Err(e) = write_confirmed_file(sealed, record_count) {
        // Could not durably record the confirmation. Do NOT delete the segment:
        // leaving it (with no .confirmed sidecar under the final name) keeps
        // recovery consistent. The ledger retries the sidecar on every health
        // poll without re-delivering; a restart before that succeeds re-drains
        // it once (a duplicate absorbed by the at-least-once + dedup contract)
        // rather than deleting it with no record that it was ever drained.
        error!(
            path = %sealed.display(),
            error = %e,
            "drain: .confirmed write failed; segment kept, the sidecar is retried on \
             every health poll (the segment is not re-delivered)"
        );
        count_failure(metrics, ConfirmFailureStage::sidecar);
        return ConfirmOutcome::SidecarFailed;
    }
    metrics
        .wab_segments
        .get_or_create(&SegmentStateLabel {
            state: SegmentState::confirmed,
        })
        .inc();
    delete_confirmed_segment(sealed, metrics)
}

/// Removes a sealed segment whose sidecar is already durable. `NotFound` is
/// `Done`: the startup replay scan and the drain can both reach an orphan, and
/// whichever deletes it second finds nothing to do.
pub(super) fn delete_confirmed_segment(sealed: &Path, metrics: &Metrics) -> ConfirmOutcome {
    match remove_segment(sealed) {
        Ok(()) => ConfirmOutcome::Done,
        Err(e) if e.kind() == io::ErrorKind::NotFound => ConfirmOutcome::Done,
        Err(e) => {
            warn!(
                path = %sealed.display(),
                error = %e,
                "drain: failed to delete a confirmed segment; its sidecar is durable, so it \
                 is never re-delivered — the delete is retried on every health poll"
            );
            count_failure(metrics, ConfirmFailureStage::delete);
            ConfirmOutcome::DeleteFailed
        }
    }
}

fn count_failure(metrics: &Metrics, stage: ConfirmFailureStage) {
    metrics
        .drain_confirm_failures
        .get_or_create(&ConfirmFailureLabel { stage })
        .inc();
}

fn remove_segment(sealed: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(e) = confirm_hook::on_delete(sealed) {
        return Err(e);
    }
    std::fs::remove_file(sealed)
}

/// Writes the `.confirmed` sidecar next to `sealed`, making both its contents and
/// its directory entry durable. Returns `Err` if it can't be durably written; the
/// caller (`confirm_and_delete`) then keeps the segment and reports
/// `SidecarFailed` (recovery treats a missing `.confirmed` as "needs draining").
pub(super) fn write_confirmed_file(sealed: &Path, record_count: u64) -> io::Result<()> {
    let confirmed = confirmed_path(sealed);
    let sealed_at = read_sealed_at_nanos(sealed).unwrap_or(0);
    let bytes = build_confirmed(sealed_at, record_count, unix_nanos_now());
    write_confirmed_durably(&confirmed, &bytes)
}

/// Writes the sidecar atomically and makes both its contents and its directory
/// entry durable: `<confirmed>.tmp` is created, written, fsynced, renamed to
/// `confirmed`, then the parent directory is fsynced.
///
/// The final name therefore exists only for complete, fsynced contents. The
/// in-place write this replaces left a 0-byte or partial sidecar under the
/// final name on ENOSPC (the create succeeds, the write fails), which the next
/// scan parsed as a corrupt sidecar and answered by QUARANTINING the delivered
/// segment; and after a failed `sync_all` it left a page-cache-only sidecar
/// that parsed as valid without being durable. A failure after the rename (the
/// directory fsync) is still an `Err`: the rename itself is not yet durable, so
/// the caller must not delete the segment on the strength of it.
fn write_confirmed_durably(confirmed: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = crate::wab::confirmed_tmp_path(confirmed);
    let result = write_tmp_then_rename(confirmed, &tmp, bytes);
    if result.is_err() {
        // Best effort: a stale tmp is harmless (every walker ignores it and the
        // scan deletes it), but there is no reason to leave one behind. A
        // directory squatting on the tmp name is not ours to remove;
        // remove_file refuses it.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_tmp_then_rename(confirmed: &Path, tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;

    // Explicit 0o600 so the sidecar is daemon-private regardless of the process
    // umask. Plain File::create relies on the umask alone (default mode 0o666);
    // under any umask other than 0o077 the .confirmed file becomes
    // group/world-readable, and the daemon's own recovery audit
    // (audit_segment_modes) flags a confirmed file with mode != 0o600 as possible
    // tampering — a false-positive on the next restart. Matches the hardened mode
    // in WabSegment::create (F10). The mode survives the rename. create+truncate
    // (not create_new) so a stale tmp from a crashed earlier attempt is
    // overwritten.
    #[cfg(unix)]
    let mut f = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(tmp)?
    };
    #[cfg(not(unix))]
    let mut f = std::fs::File::create(tmp)?;

    #[cfg(test)]
    if let Some(e) = confirm_hook::on_write(confirmed, &mut f, bytes) {
        return Err(e);
    }
    f.write_all(bytes)?;
    #[cfg(test)]
    if let Some(e) = confirm_hook::on_sync(confirmed) {
        return Err(e);
    }
    f.sync_all()?; // sidecar contents durable before the name points at them
    drop(f);
    std::fs::rename(tmp, confirmed)?;
    crate::wab::segment::fsync_parent_dir(confirmed)?; // the rename is durable
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

/// Test-only fault injection for the confirm steps, keyed by the SEALED segment
/// path like the drain's other hooks, so concurrent tests never see each
/// other's faults. Root bypasses permission tricks in CI, so the write, fsync
/// and delete failures are injected here rather than provoked.
#[cfg(test)]
pub(crate) mod confirm_hook {
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    /// Which confirm step fails.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Fault {
        /// The sidecar write: half the bytes land in the tmp file, then the
        /// write fails (ENOSPC mid-write).
        ShortWrite,
        /// The sidecar's `sync_all`, after a complete write.
        Sync,
        /// The segment's `remove_file`, after a durable sidecar.
        Delete,
    }

    #[derive(Default)]
    struct Entry {
        faults: HashMap<FaultKey, (i32, u32)>,
        sidecar_writes: u64,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct FaultKey(u8);

    impl From<Fault> for FaultKey {
        fn from(f: Fault) -> Self {
            FaultKey(f as u8)
        }
    }

    static HOOKS: Mutex<Option<HashMap<PathBuf, Entry>>> = Mutex::new(None);

    fn with<R>(f: impl FnOnce(&mut HashMap<PathBuf, Entry>) -> R) -> R {
        let mut guard = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        f(guard.get_or_insert_with(HashMap::new))
    }

    /// Fails the next `times` runs of `fault` for `sealed` with `errno`
    /// (`u32::MAX` = until cleared). Also starts counting sidecar writes.
    pub(crate) fn fail(sealed: &Path, fault: Fault, errno: i32, times: u32) {
        with(|m| {
            m.entry(sealed.to_path_buf())
                .or_default()
                .faults
                .insert(fault.into(), (errno, times))
        });
    }

    /// Stops every fault for `sealed`, keeping the counters.
    pub(crate) fn clear(sealed: &Path) {
        with(|m| {
            if let Some(e) = m.get_mut(sealed) {
                e.faults.clear();
            }
        });
    }

    /// Stops tracking `sealed`.
    pub(crate) fn forget(sealed: &Path) {
        with(|m| m.remove(sealed));
    }

    /// How many sidecar writes for `sealed` reached the write step since it was
    /// registered.
    pub(crate) fn sidecar_writes(sealed: &Path) -> u64 {
        with(|m| m.get(sealed).map_or(0, |e| e.sidecar_writes))
    }

    fn fire(m: &mut HashMap<PathBuf, Entry>, sealed: &Path, fault: Fault) -> Option<io::Error> {
        let e = m.get_mut(sealed)?;
        let (errno, left) = e.faults.get_mut(&fault.into())?;
        if *left == 0 {
            return None;
        }
        if *left != u32::MAX {
            *left -= 1;
        }
        Some(io::Error::from_raw_os_error(*errno))
    }

    fn sealed_for(m: &HashMap<PathBuf, Entry>, confirmed: &Path) -> Option<PathBuf> {
        m.keys()
            .find(|sealed| super::confirmed_path(sealed) == confirmed)
            .cloned()
    }

    /// Called with the open tmp file, before the real write.
    pub(super) fn on_write(
        confirmed: &Path,
        f: &mut std::fs::File,
        bytes: &[u8],
    ) -> Option<io::Error> {
        use std::io::Write;
        with(|m| {
            let sealed = sealed_for(m, confirmed)?;
            m.get_mut(&sealed)?.sidecar_writes += 1;
            let e = fire(m, &sealed, Fault::ShortWrite)?;
            // A real short write: the first half of the bytes land, then the
            // device is full.
            f.write_all(&bytes[..bytes.len() / 2]).ok();
            Some(e)
        })
    }

    /// Called after a complete write, before `sync_all`.
    pub(super) fn on_sync(confirmed: &Path) -> Option<io::Error> {
        with(|m| {
            let sealed = sealed_for(m, confirmed)?;
            fire(m, &sealed, Fault::Sync)
        })
    }

    /// Called before the segment's `remove_file`.
    pub(super) fn on_delete(sealed: &Path) -> Option<io::Error> {
        with(|m| fire(m, sealed, Fault::Delete))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::wab::format::parse_confirmed;

    fn sealed_in(label: &str) -> (PathBuf, PathBuf) {
        let dir = crate::testutil::scratch_dir(label);
        crate::testutil::mkdir_p(&dir);
        let sealed = dir.join("seg_00000001.wab.sealed");
        std::fs::write(&sealed, b"sealed segment bytes").unwrap();
        (dir, sealed)
    }

    fn metrics() -> Metrics {
        Metrics::new().0
    }

    fn failures(m: &Metrics, stage: ConfirmFailureStage) -> u64 {
        m.drain_confirm_failures
            .get_or_create(&ConfirmFailureLabel { stage })
            .get()
    }

    /// The happy path through the atomic write: the final sidecar is complete
    /// and valid, no tmp is left behind, and the segment is gone.
    #[test]
    fn confirm_writes_the_sidecar_atomically_and_deletes() {
        let (dir, sealed) = sealed_in("confirm_atomic_ok");
        let m = metrics();
        assert_eq!(confirm_and_delete(&sealed, 3, &m), ConfirmOutcome::Done);
        let confirmed = confirmed_path(&sealed);
        let parsed = parse_confirmed(&std::fs::read(&confirmed).unwrap()).unwrap();
        assert_eq!(parsed.record_count, 3);
        assert!(!crate::wab::confirmed_tmp_path(&confirmed).exists());
        assert!(!sealed.exists());
        std::fs::remove_dir_all(dir).ok();
    }

    /// ENOSPC mid-write. The in-place write left a 0-byte or partial sidecar
    /// under the FINAL name, which the next scan read as corruption and answered
    /// by quarantining the delivered segment. Now the final name never appears.
    #[test]
    fn a_short_write_leaves_no_final_name_sidecar() {
        let (dir, sealed) = sealed_in("confirm_short_write");
        confirm_hook::fail(&sealed, confirm_hook::Fault::ShortWrite, libc::ENOSPC, 1);
        let m = metrics();
        assert_eq!(
            confirm_and_delete(&sealed, 1, &m),
            ConfirmOutcome::SidecarFailed
        );
        let confirmed = confirmed_path(&sealed);
        assert!(!confirmed.exists(), "no torn sidecar under the final name");
        assert!(
            !crate::wab::confirmed_tmp_path(&confirmed).exists(),
            "the partial tmp is cleaned up"
        );
        assert!(sealed.exists(), "the segment is kept");
        assert_eq!(failures(&m, ConfirmFailureStage::sidecar), 1);
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A failed `sync_all` after a complete write: the page-cache sidecar must
    /// not be renamed into place (it would parse as valid without being
    /// durable), and the segment must not be deleted.
    #[test]
    fn a_failed_sidecar_fsync_neither_renames_nor_deletes() {
        let (dir, sealed) = sealed_in("confirm_sync_fail");
        confirm_hook::fail(&sealed, confirm_hook::Fault::Sync, libc::EIO, 1);
        let m = metrics();
        assert_eq!(
            confirm_and_delete(&sealed, 1, &m),
            ConfirmOutcome::SidecarFailed
        );
        assert!(!confirmed_path(&sealed).exists(), "no rename");
        assert!(sealed.exists(), "no delete");
        assert_eq!(
            m.wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::confirmed,
                })
                .get(),
            0,
            "an undurable sidecar is not a confirmed transition"
        );
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The directory fsync AFTER the rename fails (conf-tests-13). The sidecar
    /// already sits under its final name, but the rename is not durable: a
    /// power loss could undo it. Deleting the segment on the strength of it
    /// could then leave neither the segment nor its tombstone, so the shard
    /// counter would rewind and reuse a delivered segment's name. So the step
    /// is a `SidecarFailed` like any other sidecar failure: nothing is deleted,
    /// the ledger owns the segment at the `Sidecar` stage, and the next
    /// disposition retry finishes the confirm without re-delivering it.
    #[test]
    fn a_failed_dir_fsync_after_the_rename_keeps_the_segment_until_a_retry() {
        use super::super::ledger::{ConfirmStage, LedgerCounts, SegmentLedger};
        let (dir, sealed) = sealed_in("confirm_dir_sync_fail");
        crate::wab::segment::sync_hook::fail(&dir, libc::EIO, 1);
        let m = metrics();
        assert_eq!(
            confirm_and_delete(&sealed, 2, &m),
            ConfirmOutcome::SidecarFailed,
            "a rename that is not yet durable is not a confirm"
        );
        assert_eq!(crate::wab::segment::sync_hook::calls(&dir), 1);
        assert!(sealed.exists(), "the segment is NOT deleted");
        assert_eq!(failures(&m, ConfirmFailureStage::sidecar), 1);
        assert_eq!(failures(&m, ConfirmFailureStage::delete), 0);
        let confirmed_counter = || {
            m.wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::confirmed,
                })
                .get()
        };
        assert_eq!(confirmed_counter(), 0, "not a confirmed transition yet");

        // The drain hands the outcome to the ledger, which retries only the
        // confirm on the next health poll.
        let mut ledger = SegmentLedger::new(std::sync::Arc::new(LedgerCounts::default()));
        ledger.note_confirm_outcome(&sealed, ConfirmOutcome::SidecarFailed, 2);
        assert_eq!(ledger.confirm_stage(&sealed), Some(ConfirmStage::Sidecar));
        ledger.retry_dispositions(&dir, &m, &super::super::DrainHeartbeat::new());
        assert_eq!(ledger.confirm_stage(&sealed), None, "the retry finished it");
        assert!(!sealed.exists(), "deleted once the rename is durable");
        let parsed = parse_confirmed(&std::fs::read(confirmed_path(&sealed)).unwrap()).unwrap();
        assert_eq!(parsed.record_count, 2);
        assert_eq!(confirmed_counter(), 1);
        assert_eq!(crate::wab::segment::sync_hook::calls(&dir), 2);

        crate::wab::segment::sync_hook::forget(&dir);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A delete failure after a durable sidecar is reported as `DeleteFailed`
    /// and counted; a retry of the delete alone finishes the confirm, and a
    /// segment that has meanwhile vanished (ENOENT) counts as done.
    #[test]
    fn a_failed_delete_is_reported_and_enoent_counts_as_done() {
        let (dir, sealed) = sealed_in("confirm_delete_fail");
        confirm_hook::fail(&sealed, confirm_hook::Fault::Delete, libc::EIO, 1);
        let m = metrics();
        assert_eq!(
            confirm_and_delete(&sealed, 1, &m),
            ConfirmOutcome::DeleteFailed
        );
        assert!(confirmed_path(&sealed).exists(), "the sidecar is durable");
        assert!(sealed.exists());
        assert_eq!(failures(&m, ConfirmFailureStage::delete), 1);
        assert_eq!(failures(&m, ConfirmFailureStage::sidecar), 0);

        assert_eq!(delete_confirmed_segment(&sealed, &m), ConfirmOutcome::Done);
        assert!(!sealed.exists());
        assert_eq!(
            delete_confirmed_segment(&sealed, &m),
            ConfirmOutcome::Done,
            "ENOENT on a delete is done: another scan got there first"
        );
        assert_eq!(confirm_hook::sidecar_writes(&sealed), 1);
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// An operator deleted the segment while its confirm was outstanding:
    /// there is nothing left to confirm, and no orphan sidecar is written.
    #[test]
    fn confirming_a_vanished_segment_is_done_without_a_sidecar() {
        let (dir, sealed) = sealed_in("confirm_vanished");
        std::fs::remove_file(&sealed).unwrap();
        assert_eq!(
            confirm_and_delete(&sealed, 1, &metrics()),
            ConfirmOutcome::Done
        );
        assert!(!confirmed_path(&sealed).exists());
        std::fs::remove_dir_all(dir).ok();
    }

    /// F10: the `.confirmed` sidecar must be created 0o600 explicitly, not left
    /// to the umask. With an explicit 0o600 the result is umask-independent
    /// (0o600 has no group/other bits for any standard umask to clear), so the
    /// daemon's own recovery audit never false-positives on it.
    #[test]
    fn confirmed_sidecar_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = crate::testutil::scratch_dir("f10");
        crate::testutil::mkdir_p(&dir);
        let path = dir.join("seg_00000000.wab.confirmed");

        write_confirmed_durably(&path, b"sidecar-bytes").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "sidecar mode {mode:#o} != 0o600");

        std::fs::remove_dir_all(&dir).ok();
    }
}
