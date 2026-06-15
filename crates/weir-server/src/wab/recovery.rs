use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::{error, info, warn};

use super::format::{
    ConfirmedParseError, EXT_ACTIVE, EXT_CONFIRMED, EXT_SEALED, FORMAT_VERSION, SEGMENT_HEADER_LEN,
    SEGMENT_MAGIC, build_segment_footer, build_sentinel, parse_confirmed, unix_nanos_now,
};
use super::segment::sealed_path_for;
use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use weir_core::MAX_PAYLOAD_HARD_CAP;

/// Scans all shard directories under `wab_dir` and runs crash recovery on any
/// unsealed `.wab` files found. Sealed files are left untouched by this function;
/// the replay pass (in `spawn`) handles those.
///
/// Shard directories are processed in sorted (name) order rather than
/// `read_dir`'s OS-arbitrary order. The recovery outcome is order-independent —
/// each shard is recovered in isolation — but a deterministic order keeps the
/// recovery logs reproducible and removes a latent `read_dir`-order dependence
/// from the durability path.
pub(crate) fn recover_open_segments(wab_dir: &Path, metrics: &Arc<Metrics>) -> io::Result<()> {
    // Collect first (propagating any dirent error, as the streaming `?` did)
    // so we can sort before processing.
    let mut shard_dirs: Vec<PathBuf> = fs::read_dir(wab_dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    shard_dirs.sort();

    for shard_dir in shard_dirs {
        if !shard_dir.is_dir() {
            continue;
        }
        let name = shard_dir.file_name().unwrap_or_default().to_string_lossy();
        if name == "quarantine" {
            continue;
        }
        audit_segment_modes(&shard_dir, metrics);
        recover_shard_dir(&shard_dir, wab_dir, metrics)?;
    }
    Ok(())
}

/// Walks a shard directory and warns about any `.wab`, `.wab.sealed`, or
/// `.wab.confirmed` file whose permissions are not exactly 0o600.
///
/// Defense-in-depth: segments are created with mode 0o600 via
/// `OpenOptions::mode(0o600)` and the shard directory is 0o700, so a wider
/// mode here means either an operator copied a segment in with wrong perms
/// or something tampered with the WAB. We log a warning and bump
/// `weir_wab_unexpected_mode_total` so it's alertable, but do not refuse
/// to start — recovery on a slightly-wide segment is safer than refusing to
/// run and dropping all in-flight durability.
fn audit_segment_modes(shard_dir: &Path, metrics: &Arc<Metrics>) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(entries) = fs::read_dir(shard_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let is_segment = name.ends_with(EXT_ACTIVE)
                || name.ends_with(EXT_SEALED)
                || name.ends_with(EXT_CONFIRMED);
            if !is_segment {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            let mode = meta.permissions().mode() & 0o777;
            if mode != 0o600 {
                warn!(
                    path = %path.display(),
                    actual_mode = format!("{mode:#o}"),
                    expected_mode = "0o600",
                    "WAB segment file has unexpected permissions; possible tampering or operator error"
                );
                metrics.wab_unexpected_mode.inc();
            }
        }
    }
    let _ = shard_dir;
    let _ = metrics;
}

fn recover_shard_dir(shard_dir: &Path, wab_dir: &Path, metrics: &Arc<Metrics>) -> io::Result<()> {
    // Collect the unsealed `.wab` segments (propagating any dirent error, as the
    // streaming `?` did) and sort so recovery seals them in a deterministic
    // counter order rather than read_dir's OS-arbitrary order. Each segment is
    // sealed in isolation, so the outcome is order-independent; the sort is for
    // reproducible logs and to keep the durability path free of read_dir-order
    // dependence.
    let mut active: Vec<PathBuf> = fs::read_dir(shard_dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| {
            path.extension().and_then(|e| e.to_str()) == Some("wab")
                && path.to_string_lossy().ends_with(EXT_ACTIVE)
        })
        .collect();
    active.sort();

    for path in active {
        info!(path = %path.display(), "recovering unsealed WAB segment");
        match recover_segment(&path, wab_dir, metrics) {
            Ok(sealed) => {
                info!(sealed = %sealed.display(), "recovery complete");
            }
            Err(e) => {
                error!(path = %path.display(), error = %e, "recovery failed; segment left for manual inspection");
            }
        }
    }
    Ok(())
}

/// Recovers a single unsealed `.wab` file.
///
/// Validates the segment header, replays records verifying per-record CRC,
/// truncates at the first corrupt or incomplete record, writes the sentinel and
/// footer, and renames to `.wab.sealed`. Returns the path of the sealed file.
///
/// If the header has bad magic or an unknown version, the segment is quarantined
/// rather than silently skipped or left in place.
pub(crate) fn recover_segment(
    path: &Path,
    wab_dir: &Path,
    metrics: &Arc<Metrics>,
) -> io::Result<PathBuf> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // ── Validate header ──────────────────────────────────────────────────────
    let mut header_buf = [0u8; SEGMENT_HEADER_LEN];
    match reader.read_exact(&mut header_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            quarantine(path, wab_dir, "file is shorter than the segment header")?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}: quarantined — shorter than segment header",
                    path.display()
                ),
            ));
        }
        Err(e) => return Err(e),
    }

    if header_buf[0..4] != SEGMENT_MAGIC {
        let reason = format!(
            "bad magic bytes: expected {:?}, got {:?}",
            SEGMENT_MAGIC,
            &header_buf[0..4]
        );
        quarantine(path, wab_dir, &reason)?;
        metrics.recovery_segments_quarantined.inc();
        metrics
            .wab_segments
            .get_or_create(&SegmentStateLabel {
                state: SegmentState::quarantined,
            })
            .inc();
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: quarantined — {reason}", path.display()),
        ));
    }

    if header_buf[4] != FORMAT_VERSION {
        let reason = format!(
            "unknown format version: expected {FORMAT_VERSION}, got {}",
            header_buf[4]
        );
        quarantine(path, wab_dir, &reason)?;
        metrics.recovery_segments_quarantined.inc();
        metrics
            .wab_segments
            .get_or_create(&SegmentStateLabel {
                state: SegmentState::quarantined,
            })
            .inc();
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: quarantined — {reason}", path.display()),
        ));
    }

    // ── Replay records ───────────────────────────────────────────────────────
    // running_crc covers the header bytes plus every complete record that passes.
    let mut running_crc = crc32fast::Hasher::new();
    running_crc.update(&header_buf);

    let mut record_count: u64 = 0;
    let mut data_bytes: u64 = 0;

    // Byte offset of the last successfully validated record boundary. We truncate
    // here if the next read is corrupt or incomplete.
    let mut valid_end_offset = SEGMENT_HEADER_LEN as u64;

    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // File ended before a complete length field — truncate here.
                warn!(
                    path = %path.display(),
                    records = record_count,
                    "WAB segment truncated mid-length-field; truncating at last valid record"
                );
                break;
            }
            Err(e) => return Err(e),
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Sentinel: 4 zero bytes signals end-of-records written by a prior seal.
        // This shouldn't appear in a crash recovery (the file was never sealed),
        // but handle it gracefully in case of partial seals.
        if payload_len == 0 {
            info!(path = %path.display(), records = record_count, "found sentinel during recovery — file was partially sealed");
            break;
        }

        if payload_len > MAX_PAYLOAD_HARD_CAP {
            warn!(
                path = %path.display(),
                payload_len,
                cap = MAX_PAYLOAD_HARD_CAP,
                records = record_count,
                "oversized payload_len field — likely corruption; truncating at last valid record"
            );
            break;
        }

        let mut crc_buf = [0u8; 4];
        match reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                warn!(path = %path.display(), records = record_count, "WAB segment truncated mid-CRC-field; truncating at last valid record");
                break;
            }
            Err(e) => return Err(e),
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut payload = vec![0u8; payload_len];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                warn!(path = %path.display(), records = record_count, "WAB segment truncated mid-payload; truncating at last valid record");
                break;
            }
            Err(e) => return Err(e),
        }

        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            warn!(
                path = %path.display(),
                records = record_count,
                expected = format!("{expected_crc:#010x}"),
                computed = format!("{computed_crc:#010x}"),
                "CRC mismatch on record — truncating at last valid record"
            );
            break;
        }

        // Record is valid. Accumulate CRC and advance accounting.
        running_crc.update(&len_buf);
        running_crc.update(&crc_buf);
        running_crc.update(&payload);

        record_count = record_count.checked_add(1).expect("record_count overflow");
        data_bytes = data_bytes
            .checked_add(payload_len as u64)
            .expect("data_bytes overflow");
        valid_end_offset = valid_end_offset
            .checked_add(8 + payload_len as u64)
            .expect("valid_end_offset overflow");
    }

    // ── Rebuild: truncate + write sentinel + footer + rename ─────────────────
    let file_crc32 = running_crc.finalize();
    let sealed_at = unix_nanos_now();

    // O_NOFOLLOW: guards against a symlink being swapped in between the read
    // pass above and this write pass. If the path is now a symlink the open
    // fails rather than following it to an attacker-controlled target.
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?
    };
    #[cfg(not(unix))]
    let mut file = fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_end_offset)?;
    file.seek(SeekFrom::Start(valid_end_offset))?;
    file.write_all(&build_sentinel())?;
    file.write_all(&build_segment_footer(
        record_count,
        data_bytes,
        file_crc32,
        sealed_at,
    ))?;
    file.sync_all()?;
    drop(file);

    let sealed = sealed_path_for(path);
    fs::rename(path, &sealed)?;
    // Make the rename's dirent crash-durable, exactly as WabSegment::seal does
    // (B4). Without this a crash right after recovery could lose the .wab.sealed
    // dirent and re-present the segment as an unsealed .wab on the next start —
    // recovery is idempotent so no data is lost, but the parent fsync keeps the
    // recovered seal as durable as a live one (F14).
    super::segment::fsync_parent_dir(&sealed)?;

    info!(
        sealed = %sealed.display(),
        records = record_count,
        "recovery sealed segment"
    );
    Ok(sealed)
}

/// Moves a corrupt file to `<wab_dir>/quarantine/` and logs the reason.
/// Failure to quarantine is returned as an error so the caller can decide
/// whether to abort or continue.
///
/// Segment counters are shard-local — every `ShardWriter` starts at 1 — so the
/// same basename (e.g. `seg_00000001.wab`) exists in every shard directory.
/// Quarantining by basename alone into one flat directory would let one
/// shard's corrupt segment silently clobber another's via `rename(2)`,
/// destroying a forensic artifact. We therefore prefix the destination with
/// the source's parent (shard) directory name, and never overwrite an existing
/// quarantined file (the same shard+counter can recur across a restart once the
/// original is moved out of the shard dir and the counter resets) — a free
/// suffix is found first.
pub(crate) fn quarantine(path: &Path, wab_dir: &Path, reason: &str) -> io::Result<()> {
    let quarantine_dir = wab_dir.join("quarantine");
    super::create_dir_private(quarantine_dir.clone())?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let shard_name = path
        .parent()
        .and_then(Path::file_name)
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown_shard".to_string());
    let base = format!("{shard_name}__{}", file_name.to_string_lossy());
    let dest = non_clobbering_dest(&quarantine_dir, &base)?;
    error!(
        path = %path.display(),
        dest = %dest.display(),
        reason,
        "quarantining WAB segment"
    );
    fs::rename(path, &dest)
}

/// Returns a path inside `dir` based on `base` that does not yet exist, so the
/// caller's `rename` cannot silently overwrite an earlier quarantined file.
/// Tries `base`, then `base.1`, `base.2`, … There is a small TOCTOU between
/// the existence check and the caller's rename; quarantine is a best-effort
/// forensic path and the shard-prefixed `base` already makes a same-name
/// collision rare, so probing for a free name is sufficient.
fn non_clobbering_dest(dir: &Path, base: &str) -> io::Result<PathBuf> {
    let first = dir.join(base);
    if !first.try_exists()? {
        return Ok(first);
    }
    for n in 1..=10_000u32 {
        let candidate = dir.join(format!("{base}.{n}"));
        if !candidate.try_exists()? {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "quarantine: exhausted unique names for '{base}' in {}",
            dir.display()
        ),
    ))
}

/// Checks the `.confirmed` file for a sealed segment and returns whether it is
/// valid (safe to skip replay).
///
/// - Missing `.confirmed`: returns `Ok(false)` — segment was not confirmed before crash.
/// - Bad CRC or unknown version: quarantines the segment and its `.confirmed` file,
///   returns `Err` so the caller knows to skip this segment entirely.
/// - Valid: returns `Ok(true)`.
pub(crate) fn check_confirmed(sealed_path: &Path, wab_dir: &Path) -> io::Result<bool> {
    // Shared sealed→confirmed name mapping (the drain's write side uses the
    // same helper) — see crate::wab::format::confirmed_path_for.
    let confirmed_path = super::format::confirmed_path_for(sealed_path);

    if !confirmed_path.exists() {
        return Ok(false);
    }

    let buf = fs::read(&confirmed_path)?;
    match parse_confirmed(&buf) {
        Ok(_) => Ok(true),
        Err(e @ ConfirmedParseError::BadMagic)
        | Err(e @ ConfirmedParseError::CrcMismatch { .. })
        | Err(e @ ConfirmedParseError::WrongLength { .. }) => {
            let reason = format!("invalid .confirmed file: {e}");
            quarantine(sealed_path, wab_dir, &reason)?;
            quarantine(&confirmed_path, wab_dir, &reason)?;
            Err(io::Error::new(io::ErrorKind::InvalidData, reason))
        }
        Err(e @ ConfirmedParseError::UnknownVersion(_)) => {
            let reason = format!("unknown .confirmed version: {e}");
            quarantine(sealed_path, wab_dir, &reason)?;
            quarantine(&confirmed_path, wab_dir, &reason)?;
            Err(io::Error::new(io::ErrorKind::InvalidData, reason))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::wab::format::build_confirmed;
    use std::fs;

    fn tmp_dir(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("weir_recovery_{label}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn noop_metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    fn make_segment(dir: &Path, shard_id: u16, payloads: &[&[u8]]) -> PathBuf {
        use crate::wab::segment::{WabSegment, segment_path};
        let path = segment_path(dir, 1);
        let mut seg = WabSegment::create(&path, shard_id).unwrap();
        for p in payloads {
            seg.write_record(p).unwrap();
        }
        path
    }

    #[test]
    fn recovery_seals_a_clean_segment() {
        let dir = tmp_dir("clean");
        let path = make_segment(&dir, 0, &[b"alpha", b"beta", b"gamma"]);
        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();
        assert!(sealed.exists());
        assert!(sealed.to_str().unwrap().ends_with(".wab.sealed"));
        assert!(!path.exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn recovery_handles_empty_segment() {
        let dir = tmp_dir("empty");
        let path = make_segment(&dir, 0, &[]);
        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();
        assert!(sealed.exists());
        // Read back — should yield zero records.
        let mut reader = crate::wab::SegmentReader::open(&sealed).unwrap();
        assert!(reader.next().is_none());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn recovery_crash_simulation_truncate_mid_record() {
        let dir = tmp_dir("crash");
        let path = make_segment(&dir, 0, &[b"record1", b"record2", b"record3"]);

        // Simulate crash: truncate the file mid-way through the third record.
        let file_len = fs::metadata(&path).unwrap().len();
        // Keep header (24) + record1 (8+7=15) + record2 (8+7=15) + partial third record header
        let truncate_at = 24 + 15 + 15 + 4; // stops mid-CRC of record3
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncate_at)
            .unwrap();
        assert!(fs::metadata(&path).unwrap().len() < file_len);

        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();

        // Recovery should have recovered exactly 2 records.
        let records: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], b"record1" as &[u8]);
        assert_eq!(records[1], b"record2" as &[u8]);

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn recover_open_segments_seals_every_segment_per_shard_deterministically() {
        use crate::wab::segment::{WabSegment, segment_path};
        let wab_dir = tmp_dir("recover_multi");
        let shard_dir = wab_dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        // Two unsealed segments in one shard, distinct counters. Recovery
        // iterates read_dir, so this exercises the sort that makes the seal
        // order deterministic (counter order) instead of OS-arbitrary.
        for (counter, payload) in [(1u64, b"seg-one" as &[u8]), (2, b"seg-two")] {
            let path = segment_path(&shard_dir, counter);
            let mut seg = WabSegment::create(&path, 0).unwrap();
            seg.write_record(payload).unwrap();
            // Left active (unsealed) on purpose — recovery must seal it.
        }

        recover_open_segments(&wab_dir, &noop_metrics()).unwrap();

        // Both segments are sealed and every record survives, in counter order.
        let mut sealed: Vec<PathBuf> = fs::read_dir(&shard_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
            .collect();
        sealed.sort();
        assert_eq!(sealed.len(), 2, "both segments must be sealed");

        let mut recovered: Vec<Vec<u8>> = Vec::new();
        for path in &sealed {
            for rec in crate::wab::SegmentReader::open(path).unwrap() {
                recovered.push(rec.unwrap().to_vec());
            }
        }
        assert_eq!(recovered, vec![b"seg-one".to_vec(), b"seg-two".to_vec()]);

        fs::remove_dir_all(wab_dir).ok();
    }

    #[test]
    fn recovery_quarantines_bad_magic() {
        let dir = tmp_dir("badmagic");
        let path = make_segment(&dir, 0, &[b"x"]);
        // Corrupt the magic.
        let mut f = fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all(b"XXXX").unwrap();
        drop(f);

        let metrics = noop_metrics();
        let result = recover_segment(&path, &dir, &metrics);
        assert!(result.is_err());
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        // Original path should be gone (quarantined).
        assert!(!path.exists());
        // Quarantine dir should contain it.
        assert!(dir.join("quarantine").exists());

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_preserves_same_named_segments_from_different_shards() {
        // Segment counters are shard-local, so seg_00000001.wab exists in every
        // shard dir. Quarantining the same basename from two shards must not let
        // one clobber the other.
        let wab_dir = tmp_dir("quarantine_crossshard");
        let shard0 = wab_dir.join("shard_00");
        let shard1 = wab_dir.join("shard_01");
        fs::create_dir_all(&shard0).unwrap();
        fs::create_dir_all(&shard1).unwrap();

        let seg0 = shard0.join("seg_00000001.wab");
        let seg1 = shard1.join("seg_00000001.wab");
        fs::write(&seg0, b"corrupt-from-shard-0").unwrap();
        fs::write(&seg1, b"corrupt-from-shard-1").unwrap();

        quarantine(&seg0, &wab_dir, "test shard 0").unwrap();
        quarantine(&seg1, &wab_dir, "test shard 1").unwrap();

        // Both originals moved out.
        assert!(!seg0.exists());
        assert!(!seg1.exists());

        // Both forensic artifacts survive, distinctly named, with their
        // original contents intact (no clobber).
        let q = wab_dir.join("quarantine");
        let d0 = fs::read(q.join("shard_00__seg_00000001.wab")).unwrap();
        let d1 = fs::read(q.join("shard_01__seg_00000001.wab")).unwrap();
        assert_eq!(d0, b"corrupt-from-shard-0");
        assert_eq!(d1, b"corrupt-from-shard-1");

        fs::remove_dir_all(wab_dir).ok();
    }

    #[test]
    fn quarantine_does_not_clobber_same_shard_recurrence() {
        // A restart can reset a shard's counter and recreate seg_00000001.wab
        // after the original was quarantined. The second quarantine of the same
        // shard+counter must get a distinct name, not overwrite the first.
        let wab_dir = tmp_dir("quarantine_recurrence");
        let shard0 = wab_dir.join("shard_00");
        fs::create_dir_all(&shard0).unwrap();

        let seg = shard0.join("seg_00000001.wab");
        fs::write(&seg, b"first-corrupt").unwrap();
        quarantine(&seg, &wab_dir, "first").unwrap();

        fs::write(&seg, b"second-corrupt").unwrap();
        quarantine(&seg, &wab_dir, "second").unwrap();

        let q = wab_dir.join("quarantine");
        let first = fs::read(q.join("shard_00__seg_00000001.wab")).unwrap();
        let second = fs::read(q.join("shard_00__seg_00000001.wab.1")).unwrap();
        assert_eq!(first, b"first-corrupt");
        assert_eq!(second, b"second-corrupt");

        fs::remove_dir_all(wab_dir).ok();
    }

    #[test]
    fn check_confirmed_missing_file_returns_false() {
        let dir = tmp_dir("noconf");
        let sealed = dir.join("seg_00000001.wab.sealed");
        fs::write(&sealed, b"placeholder").unwrap();
        assert!(!check_confirmed(&sealed, &dir).unwrap());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn check_confirmed_valid_returns_true() {
        let dir = tmp_dir("validconf");
        let sealed = dir.join("seg_00000001.wab.sealed");
        let confirmed = dir.join("seg_00000001.wab.confirmed");
        fs::write(&sealed, b"placeholder").unwrap();
        fs::write(&confirmed, build_confirmed(0, 5, 1)).unwrap();
        assert!(check_confirmed(&sealed, &dir).unwrap());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn check_confirmed_bad_crc_quarantines() {
        let dir = tmp_dir("badcrc");
        let sealed = dir.join("seg_00000001.wab.sealed");
        let confirmed = dir.join("seg_00000001.wab.confirmed");
        fs::write(&sealed, b"placeholder").unwrap();
        let mut bytes = build_confirmed(0, 5, 1);
        bytes[32] ^= 0xff; // corrupt CRC
        fs::write(&confirmed, bytes).unwrap();

        assert!(check_confirmed(&sealed, &dir).is_err());
        assert!(!sealed.exists());
        assert!(!confirmed.exists());
        assert!(dir.join("quarantine").exists());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn check_confirmed_unknown_version_quarantines_with_specific_message() {
        let dir = tmp_dir("badver");
        let sealed = dir.join("seg_00000001.wab.sealed");
        let confirmed = dir.join("seg_00000001.wab.confirmed");
        fs::write(&sealed, b"placeholder").unwrap();
        let mut bytes = build_confirmed(0, 5, 1);
        bytes[4] = 99; // unknown version
        // Recompute CRC so we hit the version path, not the CRC path.
        let crc = crc32fast::hash(&bytes[..32]);
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());
        fs::write(&confirmed, bytes).unwrap();

        let err = check_confirmed(&sealed, &dir).unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention the version byte"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    #[cfg(unix)]
    fn audit_segment_modes_flags_wide_perms() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir("mode_audit");
        // Create three segment-shaped files: one correct (0o600), two wide.
        let good = dir.join("seg_00000001.wab.sealed");
        let wide1 = dir.join("seg_00000002.wab.sealed");
        let wide2 = dir.join("seg_00000003.wab.confirmed");
        let unrelated = dir.join("README"); // not a segment, must not trigger
        fs::write(&good, b"x").unwrap();
        fs::write(&wide1, b"x").unwrap();
        fs::write(&wide2, b"x").unwrap();
        fs::write(&unrelated, b"x").unwrap();
        fs::set_permissions(&good, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&wide1, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&wide2, fs::Permissions::from_mode(0o660)).unwrap();
        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o644)).unwrap();

        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        audit_segment_modes(&dir, &metrics);

        assert_eq!(
            metrics.wab_unexpected_mode.get(),
            2,
            "audit must flag the two wide-mode segments and ignore README + the correct file"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    #[cfg(unix)]
    fn audit_segment_modes_clean_dir_increments_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir("mode_audit_clean");
        let path = dir.join("seg_00000001.wab.sealed");
        fs::write(&path, b"x").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        audit_segment_modes(&dir, &metrics);

        assert_eq!(metrics.wab_unexpected_mode.get(), 0);
        fs::remove_dir_all(dir).ok();
    }
}
