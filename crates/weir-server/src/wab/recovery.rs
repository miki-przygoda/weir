use std::{
    fs,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::{error, info, warn};

use super::format::{
    Compression, ConfirmedParseError, EXT_ACTIVE, EXT_CONFIRMED, EXT_SEALED,
    FORMAT_VERSION_MAX_SUPPORTED, SEGMENT_FOOTER_LEN, SEGMENT_HEADER_LEN, SEGMENT_MAGIC,
    build_segment_footer, build_sentinel, max_stored_record_bytes, parse_confirmed,
    parse_segment_footer, parse_segment_header, unix_nanos_now,
};
use super::segment::{sealed_path_for, segment_counter_from_path, shard_id_from_path};
use crate::metrics::{Metrics, SegmentState, SegmentStateLabel};
use weir_core::MAX_PAYLOAD_HARD_CAP;

/// Scans all shard directories under `wab_dir` and runs crash recovery on any
/// unsealed `.wab` files found. Sealed files are left untouched by this function;
/// the replay pass (in `spawn`) handles those.
///
/// Shard directories are processed in ascending numeric shard order rather than
/// `read_dir`'s OS-arbitrary order. The recovery outcome is order-independent —
/// each shard is recovered in isolation — but a deterministic order keeps the
/// recovery logs reproducible and removes a latent `read_dir`-order dependence
/// from the durability path.
pub(crate) fn recover_open_segments(wab_dir: &Path, metrics: &Arc<Metrics>) -> io::Result<()> {
    // Collect first (propagating any dirent error, as the streaming `?` did)
    // so we can sort before processing. Numeric shard order, not lexicographic:
    // `shard_{id:02}` is a minimum width, so a plain sort would mis-order
    // shard_100 before shard_20 once shard_count exceeds 100 (P2-F3).
    let mut shard_dirs: Vec<PathBuf> = fs::read_dir(wab_dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    shard_dirs.sort_by_key(|p| shard_id_from_path(p));

    for shard_dir in shard_dirs {
        if !shard_dir.is_dir() {
            continue;
        }
        let name = shard_dir.file_name().unwrap_or_default().to_string_lossy();
        // Skip the daemon's own reserved subdirectories. `quarantine/` holds
        // unrecoverable segments parked for an operator. `dead_letter/` holds
        // segment-format files (dl_NNNNNNNN.wab[.sealed]) owned by the
        // DeadLetterWriter, which manages their dl_ counter and size accounting;
        // a crash mid-write can leave an active dl_*.wab there, and recovery must
        // NOT treat dead_letter/ as a shard dir and re-seal that file — doing so
        // bypasses dead-letter accounting and counter ownership (F16).
        if name == "quarantine" || name == "dead_letter" {
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
    // ascending counter order rather than read_dir's OS-arbitrary order. Each
    // segment is sealed in isolation, so the outcome is order-independent; the
    // sort is for reproducible logs and to keep the durability path free of
    // read_dir-order dependence. Sort by the parsed numeric counter, not a
    // lexicographic PathBuf sort, which would mis-order past the :08 pad's 8th
    // digit (P2-F3).
    let mut active: Vec<PathBuf> = fs::read_dir(shard_dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| {
            path.extension().and_then(|e| e.to_str()) == Some("wab")
                && path.to_string_lossy().ends_with(EXT_ACTIVE)
        })
        .collect();
    active.sort_by_key(|p| segment_counter_from_path(p));

    for path in active {
        info!(path = %path.display(), "recovering unsealed WAB segment");
        match recover_segment(&path, wab_dir, metrics) {
            Ok(sealed) => {
                info!(sealed = %sealed.display(), "recovery complete");
            }
            Err(e) => {
                // An Err here does NOT imply recovery failed. `quarantine_and_count`
                // returns Err as CONTROL FLOW *after* a successful quarantine — it
                // has already moved the file and bumped
                // `recovery_segments_quarantined` — so counting every Err as a
                // failure fires this counter on a routine, fully-handled path and
                // trains operators to ignore it.
                //
                // The honest discriminator is the file itself, not the error kind
                // (several sites produce InvalidData, so the kind does not
                // discriminate). `quarantine()` RENAMES, so a segment that is gone
                // from `path` was handled and is already counted, and is reachable
                // with `weir-ctl quarantine`. Only a segment still sitting at `path`
                // was genuinely left behind, which is what this counter documents.
                //
                // `copy_to_quarantine()` copies rather than renames, but its callers
                // return Ok (the mid-file path falls through to Ok(sealed)), so it
                // does not reach this arm.
                // symlink_metadata (lstat), not exists() (stat): exists() follows
                // symlinks, so a DANGLING symlink left at `path` would read as
                // "gone" and go uncounted — and a symlink is exactly what the
                // O_NOFOLLOW open above rejects, so that is a reachable shape here.
                if path.symlink_metadata().is_ok() {
                    metrics.recovery_segments_failed.inc();
                    error!(path = %path.display(), error = %e, "recovery failed; segment left in place for manual inspection");
                } else {
                    error!(path = %path.display(), error = %e, "segment quarantined during recovery; recover it with `weir-ctl quarantine`");
                }
            }
        }
    }
    Ok(())
}

/// Quarantines a corrupt SEGMENT and records it in the quarantine metrics, then
/// returns the `InvalidData` error describing why.
///
/// **Every segment-quarantine site funnels through here** so the
/// `recovery_segments_quarantined` counter and the `wab_segments{state=quarantined}`
/// gauge are ALWAYS bumped. Two separate sites have quarantined invisibly in the
/// past: the short-header branch (L00, unlike bad-magic and bad-version), and
/// `check_confirmed`, which moved BOTH a segment and its `.confirmed` sidecar
/// while bumping neither counter. If you add a third, route it here.
///
/// The one deliberate exception is the `.confirmed` **sidecar** itself, which
/// `check_confirmed` moves with the bare [`quarantine`]: it is a companion
/// artifact travelling with its segment, not a second quarantined segment, and
/// counting it would double-count one event.
///
/// The returned error is CONTROL FLOW, not a report of failure — the quarantine
/// SUCCEEDED. `recover_shard_dir` relies on that distinction and must not treat
/// this Err as a recovery failure. If the quarantine move itself fails, that
/// error is returned as-is and the metrics are not bumped (nothing was
/// quarantined).
fn quarantine_and_count(path: &Path, wab_dir: &Path, metrics: &Metrics, reason: &str) -> io::Error {
    if let Err(qe) = quarantine(path, wab_dir, reason) {
        return qe;
    }
    metrics.recovery_segments_quarantined.inc();
    metrics
        .wab_segments
        .get_or_create(&SegmentStateLabel {
            state: SegmentState::quarantined,
        })
        .inc();
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: quarantined — {reason}", path.display()),
    )
}

/// Recovers a single unsealed `.wab` file.
///
/// Validates the segment header, replays records verifying per-record CRC,
/// truncates at the first corrupt or incomplete record, writes the sentinel and
/// footer, and renames to `.wab.sealed`. Returns the path of the sealed file.
///
/// If the header is too short, has bad magic, or an unknown version, the segment
/// is quarantined (and counted) rather than silently skipped or left in place.
pub(crate) fn recover_segment(
    path: &Path,
    wab_dir: &Path,
    metrics: &Arc<Metrics>,
) -> io::Result<PathBuf> {
    // O_NOFOLLOW on the read pass too, matching the write pass below: a segment
    // path that is a symlink (swapped in by an attacker with write access to the
    // WAB dir) must fail the open rather than be followed to an attacker-chosen
    // target. The threat model claims segment opens don't follow symlinks; this
    // makes the read side honour that (S26).
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?
    };
    #[cfg(not(unix))]
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    // ── Validate header ──────────────────────────────────────────────────────
    let mut header_buf = [0u8; SEGMENT_HEADER_LEN];
    match reader.read_exact(&mut header_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(quarantine_and_count(
                path,
                wab_dir,
                metrics,
                "shorter than segment header",
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
        return Err(quarantine_and_count(path, wab_dir, metrics, &reason));
    }

    if header_buf[4] == 0 || header_buf[4] > FORMAT_VERSION_MAX_SUPPORTED {
        let reason = format!(
            "unknown format version: this build reads 1..={FORMAT_VERSION_MAX_SUPPORTED}, got {}",
            header_buf[4]
        );
        return Err(quarantine_and_count(path, wab_dir, metrics, &reason));
    }

    // Recovery must accept exactly what the writer was allowed to write. Under
    // Zstd a stored record may be up to compress_bound(MAX_PAYLOAD_HARD_CAP)
    // (max_stored_record_bytes()), which exceeds the flat MAX_PAYLOAD_HARD_CAP
    // by ~65 KiB — incompressible payloads land in that window (zstd -1 over
    // 16 MiB of random data produces 16,777,614 bytes). Comparing against the
    // flat cap regardless of compression turns an acked record into
    // "corruption" and truncates every acked record behind it. Mirrors the
    // writer's cap selection (segment.rs:139-142).
    let header_meta = parse_segment_header(&header_buf)
        .map_err(|e| quarantine_and_count(path, wab_dir, metrics, &e.to_string()))?;
    let max_stored = match header_meta.compression {
        Compression::None => MAX_PAYLOAD_HARD_CAP,
        Compression::Zstd => max_stored_record_bytes(),
    };

    // ── Replay records ───────────────────────────────────────────────────────
    // running_crc covers the header bytes plus every complete record that passes.
    let mut running_crc = crc32fast::Hasher::new();
    running_crc.update(&header_buf);

    let mut record_count: u64 = 0;
    let mut data_bytes: u64 = 0;

    // Byte offset of the last successfully validated record boundary. We truncate
    // here if the next read is corrupt or incomplete.
    let mut valid_end_offset = SEGMENT_HEADER_LEN as u64;

    // EOF-truncate vs corruption-quarantine distinction (durability-critical).
    //
    // The record loop stops for one of two fundamentally different reasons, and
    // they must be handled differently:
    //
    //   * TORN TAIL (clean EOF): an `UnexpectedEof` while reading a length, CRC,
    //     or payload field means the file simply ended part-way through a record.
    //     That trailing partial write was never made durable (the writer fsyncs
    //     whole records), so nothing meaningful exists after `valid_end_offset`.
    //     We truncate at the last valid boundary and seal — the canonical, lossless
    //     crash-recovery path that DST and the kill-9 tests exercise. Leave
    //     `quarantine_reason` as None for these.
    //
    //   * MID-FILE CORRUPTION (bytes remain after the truncation point): a CRC
    //     mismatch on a record whose length+CRC+payload were ALL fully read, or an
    //     oversized `payload_len` field with more data behind it. Here the bytes
    //     past `valid_end_offset` were fully written and may include
    //     individually-valid, acked-durable records that happen to sit after the
    //     corrupt one. Silently truncating them away is invisible data loss. So we
    //     still recover+seal the valid prefix (unchanged), but FIRST preserve the
    //     whole original segment in quarantine for manual recovery, bump the
    //     quarantine metrics, and log at ERROR — mirroring the sealed-segment drain
    //     path (drain::process_segment), which quarantines on the analogous
    //     mid-segment read error rather than confirming+deleting the tail. Set
    //     `quarantine_reason` to Some(reason) at those break points.
    let mut quarantine_reason: Option<String> = None;

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
            // Is this zero a REAL seal, or corruption sitting in front of acked
            // records? Truncating here destroys everything behind it AND rewrites
            // the footer to match, so the drain's record-count cross-check cannot
            // see the loss — the tail has to be judged before we do that. Both
            // neighbouring branches (oversized length, CRC mismatch) quarantine
            // on the same reasoning.
            //
            // SIZE ALONE CANNOT DECIDE IT. 2.0.1's first attempt quarantined only
            // a tail LONGER than `4 + SEGMENT_FOOTER_LEN`, on the theory that a
            // real partial seal leaves at most that. True, but not sufficient: a
            // corrupt tail can be short too. The spec's own reproduction — three
            // 7-byte records, middle length-prefix zeroed — leaves 26 trailing
            // bytes, under the threshold, and was still silently truncated (F1).
            //
            // CONTENT decides it. `finalize_to_disk` (segment.rs) writes exactly
            // `sentinel || 32-byte footer` and nothing else, and the footer's
            // record_count / data_bytes / file_crc32 describe precisely the
            // records this replay loop just accumulated. So a genuine seal agrees
            // with the replay, byte for byte; corruption does not agree with
            // itself. We accept exactly two shapes:
            //
            //   * NO trailing bytes — a crash between the sentinel write and the
            //     footer write. Unambiguous: there is no tail, so nothing can be
            //     lost by truncating at the sentinel.
            //   * EXACTLY a footer that cross-checks — a crash between
            //     `finalize_to_disk`'s fsync and `seal`'s rename, which leaves a
            //     fully-formed segment at the `.wab` path. Re-seal it cleanly.
            //
            // Everything else — a short/torn tail we cannot parse as a footer, a
            // footer-sized tail that disagrees, or a tail too long to be a footer
            // at all — is quarantined. That is deliberately asymmetric: a
            // quarantine is a COPY (the valid prefix is still sealed and
            // delivered) costing an operator an ERROR line and some disk, while a
            // wrong "clean seal" verdict destroys acked records with no trace.
            let field_start = valid_end_offset;
            let sentinel_end = field_start + 4;
            // Stat the ALREADY-OPEN handle, not the path. `fs::metadata(path)`
            // re-resolves the name and follows symlinks, which defeats the
            // O_NOFOLLOW guard this function opened the file with (S26) and
            // reintroduces a TOCTOU window between the open and this call. The
            // sibling oversized-length branch below already does it this way.
            match reader.get_ref().metadata() {
                Ok(m) => {
                    let trailing = m.len().saturating_sub(sentinel_end);
                    if trailing == 0 {
                        info!(path = %path.display(), records = record_count,
                              "found sentinel during recovery — file was partially sealed \
                               (crash between the sentinel and the footer)");
                    } else if trailing == SEGMENT_FOOTER_LEN as u64 {
                        // Read the trailing bytes and cross-check them against
                        // the replay. `running_crc` is cloned, not consumed: the
                        // rebuild below finalises the same hasher.
                        let mut footer_buf = [0u8; SEGMENT_FOOTER_LEN];
                        match reader
                            .read_exact(&mut footer_buf)
                            .map_err(|e| e.to_string())
                            .and_then(|()| {
                                parse_segment_footer(&footer_buf).map_err(|e| e.to_string())
                            }) {
                            Ok(footer) => {
                                let replay_crc = running_crc.clone().finalize();
                                if footer.record_count == record_count
                                    && footer.data_bytes == data_bytes
                                    && footer.file_crc32 == replay_crc
                                {
                                    info!(path = %path.display(), records = record_count,
                                          "found sentinel during recovery — file was fully \
                                           sealed but never renamed; footer cross-checks \
                                           against the replayed records");
                                } else {
                                    quarantine_reason = Some(format!(
                                        "zero length prefix at offset {field_start} followed by a \
                                         footer-sized tail that CONTRADICTS the replayed records \
                                         (footer says record_count={} data_bytes={} \
                                         file_crc32={:#010x}; replay found record_count={record_count} \
                                         data_bytes={data_bytes} file_crc32={replay_crc:#010x}) — \
                                         not a partial seal",
                                        footer.record_count, footer.data_bytes, footer.file_crc32
                                    ));
                                }
                            }
                            Err(e) => {
                                quarantine_reason = Some(format!(
                                    "zero length prefix at offset {field_start} with a \
                                     footer-sized tail that could not be read or parsed as a \
                                     footer ({e}) — cannot confirm a partial seal"
                                ));
                            }
                        }
                    } else {
                        quarantine_reason = Some(format!(
                            "zero length prefix at offset {field_start} with {trailing} trailing \
                             byte(s) — a genuine seal leaves either none (crash before the \
                             footer) or exactly {SEGMENT_FOOTER_LEN} (the footer), so these \
                             bytes are not a partial seal"
                        ));
                    }
                }
                Err(e) => {
                    // Same fail-closed reasoning as the oversized-length branch:
                    // if we cannot stat we cannot rule out a live tail, so
                    // preserve the segment and surface the ACTUAL error rather
                    // than inventing a byte count from a sentinel file length.
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "could not stat segment at a mid-file sentinel; conservatively preserving \
                         the segment in quarantine rather than risk silently dropping a trailing tail"
                    );
                    quarantine_reason = Some(format!(
                        "zero length prefix at offset {field_start} and metadata() failed ({e}); \
                         cannot confirm a partial seal or rule out a trailing tail — preserving \
                         conservatively"
                    ));
                }
            }
            break;
        }

        if payload_len > max_stored {
            // Oversized length field — corruption. If anything follows the
            // truncation point (i.e. more bytes than just this 4-byte field were
            // already on disk), those bytes were fully written and might hold
            // individually-valid records after the corrupt one; preserve the
            // segment for manual recovery rather than dropping them silently.
            // (When the oversized field is the very last 4 bytes of the file —
            // e.g. a torn final length write — nothing follows, so this is a
            // clean truncate, no quarantine.)
            warn!(
                path = %path.display(),
                payload_len,
                cap = MAX_PAYLOAD_HARD_CAP,
                records = record_count,
                "oversized payload_len field — likely corruption; truncating at last valid record"
            );
            let field_start = valid_end_offset; // this length field began here
            // Determining whether bytes follow this oversized field requires the
            // file length. If `metadata()` fails we must NOT coerce file_len to 0:
            // that would make the `file_len > field_start + 4` guard false and
            // silently downgrade a possible mid-file corruption (tail still on
            // disk) into a clean truncate, dropping that tail with no quarantine.
            // On a stat error take the CONSERVATIVE branch — assume data may follow
            // and preserve the segment — surfacing the stat error rather than
            // swallowing it.
            match reader.get_ref().metadata() {
                Ok(m) => {
                    let file_len = m.len();
                    if file_len > field_start + 4 {
                        quarantine_reason = Some(format!(
                            "oversized payload_len {payload_len} (cap {MAX_PAYLOAD_HARD_CAP}) mid-file with {} trailing byte(s)",
                            file_len - (field_start + 4)
                        ));
                    }
                }
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "could not stat segment after oversized payload_len; conservatively preserving the segment in quarantine rather than risk silently dropping a trailing tail"
                    );
                    quarantine_reason = Some(format!(
                        "oversized payload_len {payload_len} (cap {MAX_PAYLOAD_HARD_CAP}) and metadata() failed ({e}); cannot rule out a trailing tail — preserving conservatively"
                    ));
                }
            }
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
            // Mid-file bit-rot: this record's length, CRC, and payload were ALL
            // fully read (no EOF), so it was a complete, durable on-disk record
            // whose content was later corrupted. Everything from here to the end
            // of the file was fully written too and may include valid,
            // acked-durable records sitting after the corrupt one. Truncating them
            // away silently (the old behaviour) is invisible data loss; instead we
            // recover+seal the valid prefix AND preserve the original segment in
            // quarantine. NOTE: warn! is kept here for the original wording, but
            // the authoritative operator-visible message is the ERROR logged at
            // the quarantine step below.
            warn!(
                path = %path.display(),
                records = record_count,
                expected = format!("{expected_crc:#010x}"),
                computed = format!("{computed_crc:#010x}"),
                "CRC mismatch on record — truncating at last valid record"
            );
            quarantine_reason = Some(format!(
                "CRC mismatch on record {record_count}: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
            ));
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

    // ── Preserve a mid-file-corrupt tail before we truncate it away ──────────
    // The loop set `quarantine_reason` only when the bytes past `valid_end_offset`
    // were fully written (mid-file CRC bit-rot, or an oversized length with data
    // behind it) — NOT for the clean torn-tail/EOF cases. Copy (not move: we still
    // need to truncate+seal the valid prefix in place) the WHOLE original segment
    // into quarantine, giving manual recovery full context, then bump the same
    // quarantine metrics the header-validation sites use and log at ERROR so the
    // discarded tail is surfaced, not silently dropped.
    if let Some(reason) = &quarantine_reason {
        // Preserve the corrupt tail before we truncate it away. The bytes past
        // `valid_end_offset` were fully written and MAY hold individually-valid,
        // acked-durable records sitting after the corrupt one — truncating them
        // would lose acked records, violating the crown invariant. Copy (not
        // move: the valid prefix must still be sealed in place) the whole segment
        // into quarantine first.
        match copy_to_quarantine(path, wab_dir, reason) {
            Ok(dest) => {
                metrics.recovery_segments_quarantined.inc();
                metrics
                    .wab_segments
                    .get_or_create(&SegmentStateLabel {
                        state: SegmentState::quarantined,
                    })
                    .inc();
                error!(
                    path = %path.display(),
                    quarantine = %dest.display(),
                    recovered_records = record_count,
                    reason = %reason,
                    "mid-file WAB corruption: recovered + sealed the valid prefix of {record_count} record(s) for delivery; \
                     the corrupt record and all bytes after it were PRESERVED in quarantine for manual recovery"
                );
            }
            Err(qe) => {
                // FAIL CLOSED. If we cannot preserve the tail (disk full /
                // read-only mount / inode exhaustion), we must NOT truncate it —
                // doing so would silently discard any acked-durable records after
                // the corruption. Leave the segment untouched on disk and return
                // Err: the caller logs "left for manual inspection", the daemon
                // still starts, and the next recovery pass retries once the
                // operator clears the failure. No acked record is ever destroyed.
                metrics.recovery_quarantine_copy_failed.inc();
                error!(
                    path = %path.display(),
                    error = %qe,
                    reason = %reason,
                    "mid-file WAB corruption detected but the quarantine copy FAILED \
                     (disk full / read-only / inode exhaustion?); refusing to truncate \
                     the valid prefix because the corrupt tail may hold acked-durable \
                     records — segment left UNTOUCHED for retry. Clear the failure and restart."
                );
                return Err(io::Error::other(format!(
                    "quarantine copy of mid-file-corrupt segment {} failed ({qe}); \
                     refusing to truncate to avoid losing acked-durable tail records",
                    path.display()
                )));
            }
        }
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

    // Count the open → sealed transition, exactly as the three live seal sites do
    // (rotation, idle-seal, shutdown-seal in wab/mod.rs). A recovery seal IS a
    // real transition — this process wrote the sentinel + footer and published the
    // .wab.sealed dirent — and the drain will later count the matching
    // `confirmed`. Omitting it left `weir_wab_segments` non-conserving after every
    // restart: `confirmed` ran permanently ahead of `sealed` by one segment per
    // shard, so "sealed − confirmed is growing ⇒ the drain is behind" read wrong
    // for the whole life of the process (W1, found by the chaos harness over 20
    // kill -9 episodes).
    //
    // Placement: AFTER the rename+fsync, so only a segment that actually became a
    // .wab.sealed is counted. The earlier returns (header quarantine, failed
    // quarantine copy) produce no sealed file and so are correctly uncounted; the
    // mid-file-corruption path falls through to here and is counted BOTH
    // `quarantined` (the preserved forensic copy) and `sealed` (the truncated
    // prefix, which the drain will confirm) — both transitions really happened.
    //
    // Not double-counted anywhere: `replay_unconfirmed` only *queues* pre-existing
    // .wab.sealed files, which were counted by whichever process sealed them; a
    // replay is not a transition, so it must not bump this counter. Idempotency:
    // recovery runs once per process (wab::spawn) and only ever sees unsealed
    // .wab files, so within a process each seal is counted exactly once. If a
    // crash between the rename and the parent fsync lost the dirent, the next
    // process re-seals and re-counts — but that is a different process with a
    // freshly-zeroed counter, so the per-process totals stay consistent.
    metrics
        .wab_segments
        .get_or_create(&SegmentStateLabel {
            state: SegmentState::sealed,
        })
        .inc();

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

/// Like [`quarantine`], but COPIES the segment into `<wab_dir>/quarantine/`
/// instead of moving it, so the caller can still truncate+seal the valid prefix
/// in place. Used by active-segment recovery when mid-file corruption (a CRC
/// mismatch on a fully-read record, or an oversized length with data behind it)
/// would otherwise silently discard fully-written bytes that may include
/// acked-durable records sitting after the corrupt one. The whole original
/// segment is copied (not just the tail) so manual recovery has full context.
///
/// Reuses the exact quarantine dir + shard-prefixed, non-clobbering naming scheme
/// as [`quarantine`]; only the final filesystem op differs (copy vs rename), so
/// the preserved artifact lands next to (and is named like) move-quarantined ones.
/// Returns the destination path on success.
fn copy_to_quarantine(path: &Path, wab_dir: &Path, reason: &str) -> io::Result<PathBuf> {
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
        "preserving mid-file-corrupt WAB segment in quarantine (copy)"
    );
    fs::copy(path, &dest)?;
    // Make the quarantine artifact crash-durable BEFORE the caller truncates the
    // original in place. `fs::copy` only writes into the page cache; without these
    // fsyncs a crash after the (durable) truncate+seal but before the copy's data
    // and dirent reach disk would lose the ONLY copy of the corrupt tail — the
    // exact opposite of the "PRESERVED in quarantine" guarantee the caller logs.
    // Order: copy → fsync the copied file's data → fsync the quarantine dir so the
    // new dirent survives a crash. `fsync_parent_dir(&dest)` fsyncs dest's parent,
    // i.e. the quarantine dir, and is a no-op on non-Unix. The caller only truncates
    // after we return Ok, so durability is established first.
    fs::File::open(&dest)?.sync_all()?;
    super::segment::fsync_parent_dir(&dest)?;
    Ok(dest)
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
pub(crate) fn check_confirmed(
    sealed_path: &Path,
    wab_dir: &Path,
    metrics: &Metrics,
) -> io::Result<bool> {
    // Shared sealed→confirmed name mapping (the drain's write side uses the
    // same helper) — see crate::wab::format::confirmed_path_for.
    let confirmed_path = super::format::confirmed_path_for(sealed_path);

    if !confirmed_path.exists() {
        return Ok(false);
    }

    let buf = fs::read(&confirmed_path)?;
    match parse_confirmed(&buf) {
        Ok(_) => Ok(true),
        Err(
            e @ (ConfirmedParseError::BadMagic
            | ConfirmedParseError::CrcMismatch { .. }
            | ConfirmedParseError::WrongLength { .. }),
        ) => {
            let reason = format!("invalid .confirmed file: {e}");
            // Count the SEGMENT once (the .confirmed sidecar is a companion
            // artifact, not a second quarantined segment), then park the sidecar
            // beside it. Before this, both files moved and NEITHER counter did,
            // so an operator alerting on the quarantine counters missed this case
            // entirely — the same invisibility bug quarantine_and_count was
            // introduced to fix, surviving in a second site.
            let e = quarantine_and_count(sealed_path, wab_dir, metrics, &reason);
            quarantine(&confirmed_path, wab_dir, &reason)?;
            Err(e)
        }
        Err(e @ ConfirmedParseError::UnknownVersion(_)) => {
            let reason = format!("unknown .confirmed version: {e}");
            // Count the SEGMENT once (the .confirmed sidecar is a companion
            // artifact, not a second quarantined segment), then park the sidecar
            // beside it. Before this, both files moved and NEITHER counter did,
            // so an operator alerting on the quarantine counters missed this case
            // entirely — the same invisibility bug quarantine_and_count was
            // introduced to fix, surviving in a second site.
            let e = quarantine_and_count(sealed_path, wab_dir, metrics, &reason);
            quarantine(&confirmed_path, wab_dir, &reason)?;
            Err(e)
        }
        // `ConfirmedParseError` is `#[non_exhaustive]`; any future parse failure
        // is also "cannot trust this .confirmed file", so quarantine and skip —
        // the same conservative path as a bad CRC, never a silent accept.
        Err(e) => {
            let reason = format!("unparseable .confirmed file: {e}");
            // Count the SEGMENT once (the .confirmed sidecar is a companion
            // artifact, not a second quarantined segment), then park the sidecar
            // beside it. Before this, both files moved and NEITHER counter did,
            // so an operator alerting on the quarantine counters missed this case
            // entirely — the same invisibility bug quarantine_and_count was
            // introduced to fix, surviving in a second site.
            let e = quarantine_and_count(sealed_path, wab_dir, metrics, &reason);
            quarantine(&confirmed_path, wab_dir, &reason)?;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::Compression;
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
        make_segment_with(dir, shard_id, Compression::None, payloads)
    }

    /// As `make_segment`, at a chosen compression. `WabSegment::write_record`
    /// takes the STORED bytes, so a compressed segment is built by compressing
    /// each payload first — the same thing `ShardWriter` does in production.
    fn make_segment_with(
        dir: &Path,
        shard_id: u16,
        compression: Compression,
        payloads: &[&[u8]],
    ) -> PathBuf {
        use crate::wab::segment::{WabSegment, segment_path};
        let path = segment_path(dir, 1);
        let mut seg = WabSegment::create(&path, shard_id, compression).unwrap();
        for p in payloads {
            match compression {
                Compression::None => seg.write_record(p).unwrap(),
                Compression::Zstd => seg
                    .write_record(&zstd::bulk::compress(p, 1).unwrap())
                    .unwrap(),
            }
        }
        path
    }

    /// Current `weir_wab_segments{state="sealed"}` value.
    fn sealed_count(metrics: &Metrics) -> u64 {
        metrics
            .wab_segments
            .get_or_create(&SegmentStateLabel {
                state: SegmentState::sealed,
            })
            .get()
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

    // ── W1: the recovery seal is a counted open → sealed transition ───────────

    /// W1. Recovery renames an unsealed `.wab` to `.wab.sealed` and hands it to
    /// the drain, which later bumps `weir_wab_segments{state="confirmed"}`. If the
    /// seal itself isn't counted, `confirmed` runs permanently ahead of `sealed`
    /// after every restart (one segment per shard) and the counter family stops
    /// conserving — the chaos harness measured exactly that over 20 `kill -9`
    /// episodes.
    #[test]
    fn recovery_seal_counts_the_sealed_transition() {
        let dir = tmp_dir("seal_counter");
        let path = make_segment(&dir, 0, &[b"alpha", b"beta"]);
        let metrics = noop_metrics();
        assert_eq!(sealed_count(&metrics), 0, "counter starts at zero");

        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert!(sealed.exists());
        assert_eq!(
            sealed_count(&metrics),
            1,
            "recovery sealing a segment must bump weir_wab_segments{{state=\"sealed\"}} — \
             the drain will bump the matching confirmed"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// W1. A segment that never becomes a `.wab.sealed` must not be counted as
    /// sealed. The header-quarantine branches return before the rename, so the
    /// increment's placement after the rename+fsync is what makes this hold.
    #[test]
    fn quarantined_segment_counts_no_sealed_transition() {
        let dir = tmp_dir("seal_counter_quarantine");
        let path = crate::wab::segment::segment_path(&dir, 1);
        // Fewer than SEGMENT_HEADER_LEN bytes — quarantined, never sealed.
        fs::write(&path, b"WEI").unwrap();

        let metrics = noop_metrics();
        assert!(recover_segment(&path, &dir, &metrics).is_err());

        assert_eq!(
            sealed_count(&metrics),
            0,
            "a quarantined segment produced no .wab.sealed and must not be counted as sealed"
        );
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        fs::remove_dir_all(dir).ok();
    }

    /// W1. Mid-file corruption preserves a forensic COPY in quarantine and still
    /// seals the valid prefix for delivery. Both transitions really happen, so
    /// both are counted — otherwise the drain's `confirmed` for the sealed prefix
    /// would again have no matching `sealed`.
    #[test]
    fn mid_file_corruption_counts_both_sealed_and_quarantined() {
        let dir = tmp_dir("seal_counter_midfile");
        let path = make_segment(&dir, 0, &[b"recordA", b"recordB"]);
        // Flip a byte inside record B's payload (offset 24 + 15 + 8 = 47) so its
        // CRC fails with its length + CRC fields intact — the mid-file branch.
        let mut bytes = fs::read(&path).unwrap();
        bytes[47] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert!(sealed.exists());
        assert_eq!(
            sealed_count(&metrics),
            1,
            "the truncated-but-sealed prefix is a real open → sealed transition"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1,
            "the preserved forensic copy is still counted as quarantined"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// W1. A `.wab.sealed` left by a previous process was already counted as
    /// sealed by whichever process sealed it. Startup must NOT re-count it: the
    /// replay path (`wab::replay_unconfirmed`) only *queues* it for the drain, and
    /// `recover_open_segments` only touches unsealed `.wab` files. Exactly one
    /// sealed transition is counted here — for the active segment recovery
    /// actually seals.
    #[test]
    fn already_sealed_segments_are_not_recounted_on_startup() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("seal_counter_replay");
        let shard_dir = dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        // seg 1: sealed by the "previous process" — already counted there.
        let mut seg =
            WabSegment::create(&segment_path(&shard_dir, 1), 0, Compression::None).unwrap();
        seg.write_record(b"old").unwrap();
        let pre_sealed = seg.seal().unwrap();
        // seg 2: still active at crash time — recovery seals this one.
        let active = segment_path(&shard_dir, 2);
        let mut seg = WabSegment::create(&active, 0, Compression::None).unwrap();
        seg.write_record(b"new").unwrap();
        drop(seg);

        let metrics = noop_metrics();
        recover_open_segments(&dir, &metrics).unwrap();

        assert!(
            pre_sealed.exists(),
            "the pre-sealed segment is left untouched"
        );
        assert!(
            sealed_path_for(&active).exists(),
            "the active one was sealed"
        );
        assert_eq!(
            sealed_count(&metrics),
            1,
            "only the segment recovery actually sealed is counted; a segment sealed by an \
             earlier process must not be re-counted when it is replayed"
        );
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
    fn recovery_truncates_at_crc_mismatch_keeping_valid_prefix() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        let dir = tmp_dir("crc_mismatch");
        let path = make_segment(&dir, 0, &[b"recordA", b"recordB"]);
        // Flip a byte inside record B's PAYLOAD, leaving its length + CRC fields
        // intact: the reader reads the 7-byte payload fully but the computed CRC no
        // longer matches the stored one, so recovery must (a) truncate at the last
        // valid record (A) for delivery AND (b) PRESERVE the original segment in
        // quarantine — the mid-file bit-rot branch (S33), now non-silent. Layout:
        // header 24 + record [4 len + 4 crc + 7 payload = 15]; record B's payload
        // begins at 24 + 15 + 8 = 47.
        let original_bytes = fs::read(&path).unwrap();
        let mut bytes = original_bytes.clone();
        bytes[47] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();
        let records: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            records.len(),
            1,
            "a CRC mismatch in record B must truncate to the valid prefix (A)"
        );
        assert_eq!(records[0], b"recordA" as &[u8]);

        // The corrupt tail is preserved (not silently dropped): a quarantine copy
        // exists, the counter + gauge were bumped, and the copy holds the FULL
        // original (corrupted) bytes including record B.
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "mid-file CRC corruption must bump recovery_segments_quarantined"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1,
            "mid-file CRC corruption must bump wab_segments{{quarantined}}"
        );
        let q_dir = dir.join("quarantine");
        let q_entry = fs::read_dir(&q_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .expect("a quarantine copy must exist");
        let preserved = fs::read(&q_entry).unwrap();
        assert_eq!(
            preserved, bytes,
            "the quarantined copy must hold the full corrupt segment (incl. record B), not just the prefix"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// Headline durability regression (mid-file bit-rot in the MIDDLE of a run):
    /// K valid records, flip one byte in record i's payload so its CRC fails, then
    /// records i+1..K follow it on disk. Before the fix, recovery silently dropped
    /// record i AND every following (individually-valid, acked-durable) record with
    /// only a WARN. Now it must: (a) recover+seal records 0..i for delivery, (b)
    /// quarantine a COPY whose bytes still contain records i..K, (c) bump
    /// recovery_segments_quarantined, (d) lose nothing silently.
    #[test]
    fn recovery_mid_file_corruption_preserves_corrupt_and_following_records() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        use crate::wab::segment::segment_path;
        let dir = tmp_dir("mid_file_corruption");
        let path = segment_path(&dir, 1);
        // Five equal-length records so the byte offsets are easy to reason about.
        // Each record on disk is [4 len + 4 crc + 7 payload = 15] bytes after the
        // 24-byte header. Records: 0..5 at payload offsets 24 + i*15 + 8.
        let payloads: &[&[u8]] = &[b"rec--00", b"rec--01", b"rec--02", b"rec--03", b"rec--04"];
        {
            use crate::wab::segment::WabSegment;
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            for p in payloads {
                seg.write_record(p).unwrap();
            }
        }
        let original = fs::read(&path).unwrap();
        let original_len = original.len();

        // Corrupt record i=2's payload (offset 24 + 2*15 + 8 = 62), leaving its
        // length + CRC fields intact so the payload reads fully and the CRC check
        // fires — with records 3 and 4 fully written AFTER it.
        let corrupt_index = 2usize;
        let payload_off = 24 + corrupt_index * 15 + 8;
        let mut corrupt = original.clone();
        corrupt[payload_off] ^= 0xff;
        fs::write(&path, &corrupt).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        // (a) records 0..i recovered + sealed (delivered).
        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            corrupt_index,
            "only the valid prefix (records 0..i) is delivered"
        );
        for (k, rec) in recovered.iter().enumerate() {
            assert_eq!(rec.as_ref(), payloads[k], "prefix record {k} must survive");
        }

        // (b) + (d) a quarantine copy exists and holds the FULL corrupt segment —
        // including the corrupt record AND every record after it (records i..K) —
        // so nothing was silently lost.
        let q_dir = dir.join("quarantine");
        let q_entry = fs::read_dir(&q_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .expect("a quarantine copy must exist after mid-file corruption");
        let preserved = fs::read(&q_entry).unwrap();
        assert_eq!(
            preserved.len(),
            original_len,
            "quarantine must preserve the whole segment, not just the readable prefix"
        );
        assert_eq!(
            preserved, corrupt,
            "quarantined bytes must equal the on-disk corrupt segment (records i..K preserved)"
        );
        // Sanity: the post-corruption records (3, 4) are byte-present in the copy.
        for k in (corrupt_index + 1)..payloads.len() {
            let off = 24 + k * 15 + 8;
            assert_eq!(
                &preserved[off..off + payloads[k].len()],
                payloads[k],
                "record {k} (after the corrupt one) must be preserved verbatim in quarantine"
            );
        }

        // (c) metrics bumped.
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1
        );

        // The active segment was sealed (renamed away) — the valid prefix is live.
        assert!(!path.exists());
        assert!(sealed.to_str().unwrap().ends_with(".wab.sealed"));

        fs::remove_dir_all(dir).ok();
    }

    /// FAIL-CLOSED: when a mid-file-corrupt segment cannot be preserved (the
    /// quarantine copy fails — disk full / read-only mount / inode exhaustion),
    /// recovery must REFUSE to truncate the valid prefix, because the corrupt
    /// tail may hold acked-durable records whose loss would violate the crown
    /// invariant. The segment is left byte-for-byte UNTOUCHED for a retry, the
    /// caller leaves it for manual inspection, and the failure is alertable via
    /// `recovery_quarantine_copy_failed`. (Regression guard for the prior
    /// fail-OPEN behaviour that truncated the tail away on copy failure.)
    #[test]
    fn recovery_mid_file_corruption_fails_closed_when_quarantine_copy_fails() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("mid_file_quarantine_copy_fails");
        let path = segment_path(&dir, 1);
        let payloads: &[&[u8]] = &[b"rec--00", b"rec--01", b"rec--02", b"rec--03", b"rec--04"];
        {
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            for p in payloads {
                seg.write_record(p).unwrap();
            }
        }
        // Corrupt record 2's payload (records 3,4 fully written after it) so the
        // CRC check fires mid-file and recovery tries to quarantine the tail.
        let mut bytes = fs::read(&path).unwrap();
        let payload_off = 24 + 2 * 15 + 8;
        bytes[payload_off] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        // Force copy_to_quarantine to fail: occupy `wab_dir/quarantine` with a
        // regular FILE so the dir cannot be created. (Stands in for ENOSPC /
        // read-only mount — any copy-stage failure takes the same Err arm.)
        fs::write(dir.join("quarantine"), b"not a dir").unwrap();

        let metrics = noop_metrics();
        let err = recover_segment(&path, &dir, &metrics)
            .expect_err("recovery must FAIL when the corrupt tail can't be quarantined");
        assert!(
            err.to_string().contains("refusing to truncate"),
            "error must explain the fail-closed refusal, got: {err}"
        );

        // The segment is left UNTOUCHED (not truncated, not sealed): the
        // acked-durable tail is preserved for a retry.
        assert!(path.exists(), "the corrupt segment must be left in place");
        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "the segment must be byte-for-byte unchanged — no truncation"
        );
        assert!(
            !path.with_extension("wab.sealed").exists()
                && fs::read_dir(&dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .all(|e| !e.path().to_string_lossy().ends_with(".wab.sealed")),
            "no sealed segment must be produced"
        );

        // The event is alertable, and the normal quarantine metric did NOT fire
        // (nothing was successfully quarantined).
        assert_eq!(metrics.recovery_quarantine_copy_failed.get(), 1);
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);

        fs::remove_dir_all(dir).ok();
    }

    /// Counterpart to the headline test: the TORN-TAIL (clean EOF) crash-recovery
    /// path must stay byte-for-byte unchanged — truncate cleanly, NO quarantine,
    /// NO metric bump. A partial trailing write was never durable, so there is
    /// nothing to preserve. This guards against the corruption-quarantine logic
    /// leaking into the normal kill-9 / DST path.
    #[test]
    fn recovery_torn_tail_truncates_without_quarantine() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        let dir = tmp_dir("torn_tail_no_quarantine");
        let path = make_segment(&dir, 0, &[b"record1", b"record2", b"record3"]);

        // Truncate mid-CRC of record3: header(24)+rec1(15)+rec2(15)+4 = 58.
        let truncate_at = 24 + 15 + 15 + 4;
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncate_at)
            .unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            2,
            "torn tail recovers the whole-record prefix"
        );

        // The crux: a clean EOF truncation must NOT quarantine.
        assert!(
            !dir.join("quarantine").exists(),
            "a torn-tail (EOF) truncation must NOT create a quarantine entry"
        );
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            0,
            "a torn-tail truncation must NOT bump recovery_segments_quarantined"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            0,
            "a torn-tail truncation must NOT bump wab_segments{{quarantined}}"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T02): the mid-PAYLOAD truncation branch. Existing tests hit
    /// mid-CRC-field, CRC-mismatch, oversized-len, and sentinel; none reads a full
    /// len+crc prefix then a short payload. A regression mishandling a short
    /// payload read (treating it as a zero-length record, or not breaking) would
    /// otherwise go uncaught.
    #[test]
    fn recovery_truncates_mid_payload_keeping_valid_prefix() {
        let dir = tmp_dir("crash_mid_payload");
        let path = make_segment(&dir, 0, &[b"record1", b"record2", b"record3"]);

        // header(24) + rec1(15) + rec2(15) + rec3 len(4) + rec3 crc(4) + 3 of 7
        // payload bytes = 65: the len + crc fields read fully, the payload read
        // hits UnexpectedEof — the distinct mid-payload branch.
        let truncate_at = 24 + 15 + 15 + 4 + 4 + 3;
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncate_at)
            .unwrap();

        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();
        let records: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            records.len(),
            2,
            "a payload torn mid-bytes must truncate to the last whole record (rec2)"
        );
        assert_eq!(records[0], b"record1" as &[u8]);
        assert_eq!(records[1], b"record2" as &[u8]);
        assert!(!path.exists());
        assert!(sealed.to_str().unwrap().ends_with(".wab.sealed"));

        fs::remove_dir_all(dir).ok();
    }

    /// L00: a segment whose header was torn off (file shorter than the 24-byte
    /// header) is quarantined AND counted — like the bad-magic and bad-version
    /// branches — so operators alerting on the quarantine metrics see it. Before
    /// the fix this branch quarantined invisibly (no metric bump).
    #[test]
    fn recovery_short_header_quarantines_and_counts() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        use crate::wab::segment::segment_path;
        let dir = tmp_dir("short_header");
        let path = segment_path(&dir, 1);
        fs::write(&path, b"WEI").unwrap(); // 3 bytes — shorter than the 24-byte header

        let metrics = noop_metrics();
        let err = recover_segment(&path, &dir, &metrics).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
        assert!(
            err.to_string().contains("shorter than segment header"),
            "{err}"
        );
        assert!(
            !path.exists(),
            "the short segment must be quarantined (moved)"
        );
        assert!(dir.join("quarantine").is_dir());
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "a short-header quarantine must increment recovery_segments_quarantined (L00)"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1,
            "a short-header quarantine must bump wab_segments{{quarantined}} (L00)"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T03): fault isolation across siblings. A segment whose
    /// recovery FAILS (bad magic → quarantine → Err) must be skipped without
    /// aborting recovery of the other healthy segments in the same shard dir, and
    /// recover_open_segments must still return Ok. The corrupt counter (1) sorts
    /// first, so this also proves the failure doesn't short-circuit the loop.
    #[test]
    fn recover_open_segments_isolates_a_corrupt_segment_from_healthy_siblings() {
        use crate::wab::segment::{WabSegment, sealed_path_for, segment_path};
        use std::io::Write;
        let wab_dir = tmp_dir("recover_isolate_corrupt");
        let shard_dir = wab_dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        // Segment 1: valid then magic-corrupted → recover_segment quarantines + Errs.
        let corrupt = segment_path(&shard_dir, 1);
        let mut s1 = WabSegment::create(&corrupt, 0, Compression::None).unwrap();
        s1.write_record(b"healthy-1").unwrap();
        drop(s1);
        {
            let mut f = fs::OpenOptions::new().write(true).open(&corrupt).unwrap();
            f.write_all(b"XXXX").unwrap();
        }

        // Segment 2: healthy, left active — must still be sealed.
        let healthy = segment_path(&shard_dir, 2);
        let mut s2 = WabSegment::create(&healthy, 0, Compression::None).unwrap();
        s2.write_record(b"healthy-2").unwrap();
        drop(s2);

        let metrics = noop_metrics();
        recover_open_segments(&wab_dir, &metrics).unwrap();

        let healthy_sealed = sealed_path_for(&healthy);
        assert!(
            healthy_sealed.exists(),
            "healthy sibling must be sealed despite the corrupt neighbour"
        );
        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&healthy_sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0], b"healthy-2" as &[u8]);

        assert!(
            !corrupt.exists(),
            "the corrupt segment must be moved out of the shard dir (quarantined)"
        );
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "the corrupt segment must be counted as quarantined"
        );

        fs::remove_dir_all(wab_dir).ok();
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
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
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

    /// F16: recovery must NOT descend into `dead_letter/` and re-seal a torn
    /// `dl_*.wab` there. Those files (and their dl_ counter) are owned by the
    /// DeadLetterWriter; re-sealing one bypasses dead-letter accounting.
    #[test]
    fn recover_open_segments_skips_dead_letter_dir() {
        use crate::wab::segment::{WabSegment, segment_path};
        let wab_dir = tmp_dir("recover_skips_dl");

        // A real shard with an active segment — recovery MUST seal this one.
        let shard_dir = wab_dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();
        let shard_seg = segment_path(&shard_dir, 1);
        WabSegment::create(&shard_seg, 0, Compression::None)
            .unwrap()
            .write_record(b"live")
            .unwrap();

        // A torn (still-active) dead-letter segment, segment-format so that
        // WITHOUT the skip recovery would re-seal it.
        let dl_dir = wab_dir.join("dead_letter");
        fs::create_dir_all(&dl_dir).unwrap();
        let dl_seg = dl_dir.join("dl_00000001.wab");
        WabSegment::create(&dl_seg, 0, Compression::None)
            .unwrap()
            .write_record(b"dead")
            .unwrap();

        recover_open_segments(&wab_dir, &noop_metrics()).unwrap();

        // The shard segment was sealed (active gone, a .wab.sealed appeared)…
        let shard_sealed = fs::read_dir(&shard_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.path().to_string_lossy().ends_with(".wab.sealed"));
        assert!(!shard_seg.exists(), "active shard segment should be gone");
        assert!(shard_sealed, "shard segment should have been sealed");
        // …but the dead-letter file is untouched: still active, never sealed.
        assert!(
            dl_seg.exists(),
            "dead_letter dl_*.wab must be left in place"
        );
        assert!(
            !dl_dir.join("dl_00000001.wab.sealed").exists(),
            "recovery must not re-seal a dead_letter segment"
        );

        fs::remove_dir_all(wab_dir).ok();
    }

    /// Appends raw bytes to an active `.wab` segment after the WabSegment that
    /// created it has been dropped (and so flushed). Used to splice in a crafted
    /// trailing field (oversized length, sentinel) that the writer would never
    /// emit, to exercise recovery's defensive decode branches.
    fn append_raw(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let mut f = fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
    }

    /// Zeroes the 4-byte length prefix of record `index` (0-based) of a
    /// segment written by `make_segment`, turning it into a mid-file sentinel
    /// in place while leaving every other byte — including any records
    /// behind it — untouched. Walks the same header + (len, crc, payload)*
    /// layout `recover_segment` parses, so it finds the field regardless of
    /// each record's size.
    fn zero_length_prefix_of_record(path: &Path, index: usize) {
        let bytes = fs::read(path).unwrap();
        let mut offset = SEGMENT_HEADER_LEN;
        for _ in 0..index {
            let len_buf: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
            let payload_len = u32::from_le_bytes(len_buf) as usize;
            offset += 8 + payload_len;
        }
        let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(offset as u64)).unwrap();
        f.write_all(&[0u8; 4]).unwrap();
    }

    /// F17 boundary: a record whose length field is exactly MAX_PAYLOAD_HARD_CAP
    /// is a LEGAL record and must be recovered, not truncated. Guards the `>` in
    /// the oversized-payload_len check against a `>=` off-by-one that would
    /// silently drop the largest legal records on recovery.
    #[test]
    fn recovery_recovers_record_at_exactly_max_payload_cap() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("recover_max_payload");
        let path = segment_path(&dir, 1);
        {
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            seg.write_record(&vec![0xABu8; MAX_PAYLOAD_HARD_CAP])
                .unwrap();
        }

        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "the max-size record must survive recovery"
        );
        assert_eq!(
            recovered[0].len(),
            MAX_PAYLOAD_HARD_CAP,
            "recovered record must be exactly MAX_PAYLOAD_HARD_CAP bytes"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// F17 just-over: a record whose length field is MAX_PAYLOAD_HARD_CAP + 1 is
    /// treated as corruption; recovery truncates at the last valid record. We
    /// splice only the 4-byte oversized length field (recovery rejects it before
    /// reading any payload), so no giant buffer is needed.
    #[test]
    fn recovery_truncates_at_oversized_payload_len() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("recover_oversized");
        let path = segment_path(&dir, 1);
        {
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            seg.write_record(b"keep-me").unwrap();
        }
        // Splice an oversized length field where the next record would start.
        let oversized = (MAX_PAYLOAD_HARD_CAP as u32 + 1).to_le_bytes();
        append_raw(&path, &oversized);

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "recovery must truncate at the last valid record, keeping only it"
        );
        assert_eq!(recovered[0], b"keep-me" as &[u8]);
        // The oversized field is the file's LAST 4 bytes (nothing follows it), so
        // it is treated as a clean torn-tail truncation: NO quarantine.
        assert!(
            !dir.join("quarantine").exists(),
            "an oversized len as the trailing 4 bytes must NOT quarantine (nothing follows)"
        );
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);

        fs::remove_dir_all(dir).ok();
    }

    /// Companion to the trailing-oversized case: an oversized `payload_len` field
    /// with MORE bytes behind it is mid-file corruption — those trailing bytes
    /// were fully written and may hold valid records, so recovery must preserve
    /// the whole segment in quarantine (and count it), not silently drop the tail.
    #[test]
    fn recovery_oversized_payload_len_midfile_quarantines() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("recover_oversized_midfile");
        let path = segment_path(&dir, 1);
        {
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            seg.write_record(b"keep-me").unwrap();
        }
        // Splice an oversized length field, THEN trailing bytes after it so the
        // corruption is mid-file (file_len > field_start + 4).
        let oversized = (MAX_PAYLOAD_HARD_CAP as u32 + 1).to_le_bytes();
        append_raw(&path, &oversized);
        append_raw(&path, b"trailing-data-after-the-bad-length");
        let on_disk = fs::read(&path).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "valid prefix still recovered + delivered"
        );
        assert_eq!(recovered[0], b"keep-me" as &[u8]);

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "an oversized len with trailing bytes must bump recovery_segments_quarantined"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1
        );
        let q_dir = dir.join("quarantine");
        let q_entry = fs::read_dir(&q_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .expect("a quarantine copy must exist");
        assert_eq!(
            fs::read(&q_entry).unwrap(),
            on_disk,
            "the quarantined copy must hold the full segment incl. the trailing bytes"
        );

        fs::remove_dir_all(dir).ok();
    }

    /// F18: a partial seal (sentinel written, footer/rename not) must be handled
    /// gracefully — recovery stops at the sentinel and recovers exactly the
    /// pre-sentinel records.
    #[test]
    fn recovery_stops_at_partial_seal_sentinel() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("recover_sentinel");
        let path = segment_path(&dir, 1);
        {
            let mut seg = WabSegment::create(&path, 0, Compression::None).unwrap();
            seg.write_record(b"before-sentinel").unwrap();
        }
        // A sentinel is a zero-length record-length field (4 zero bytes), written
        // by a seal before the footer/rename. Splice one in with no footer.
        append_raw(&path, &[0u8; 4]);

        let sealed = recover_segment(&path, &dir, &noop_metrics()).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "recovery must stop at the sentinel, recovering only pre-sentinel records"
        );
        assert_eq!(recovered[0], b"before-sentinel" as &[u8]);

        fs::remove_dir_all(dir).ok();
    }

    /// Task 1 (2.0.1): a zero length-prefix sentinel is ALWAYS mid-file
    /// corruption when live records sit behind it, never a partial seal —
    /// `seal()` consumes the `WabSegment` handle (segment.rs:383) and
    /// `write_record` rejects empty payloads at two layers (segment.rs:117,
    /// :546), so nothing can write a real record after a sentinel that this
    /// process itself wrote. A sentinel with a full record behind it means
    /// lost writeback or bit-rot, and must quarantine like its CRC-mismatch
    /// and oversized-length siblings — not truncate the tail away with no
    /// trace, which is what silently destroyed acked records here.
    #[test]
    fn mid_file_sentinel_with_a_live_tail_is_quarantined_not_truncated() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        let dir = tmp_dir("sentinel_live_tail");
        // B and C are sized so the corrupted tail (B's leftover crc+payload,
        // plus C's whole record) is far longer than a footer — the LONG-tail
        // shape. F1's sibling test covers the short-tail shape the original
        // size-only rule let through; size no longer decides either of them,
        // but keeping both shapes covered guards the branch end to end.
        let path = make_segment(
            &dir,
            0,
            &[
                b"A-acked",
                b"B-acked-with-enough-bytes-to-clear-the-footer-threshold",
                b"C-acked-with-enough-bytes-to-clear-the-footer-threshold",
            ],
        );

        // Zero B's 4-byte length prefix in place, leaving C intact behind it.
        zero_length_prefix_of_record(&path, 1);
        let on_disk = fs::read(&path).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "a mid-file sentinel with a live tail must quarantine, not truncate"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1
        );

        let q_dir = dir.join("quarantine");
        let q_entry = fs::read_dir(&q_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .expect("a quarantine copy must exist — C-acked must not be lost with no trace");
        assert_eq!(
            fs::read(&q_entry).unwrap(),
            on_disk,
            "the quarantine copy must hold the full original segment bytes, including C-acked, \
             for forensics — the original bytes must be preserved, not truncated away"
        );

        // The valid prefix in front of the sentinel is still recovered and
        // sealed for delivery.
        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "only the pre-sentinel record A is delivered"
        );
        assert_eq!(recovered[0], b"A-acked" as &[u8]);

        fs::remove_dir_all(dir).ok();
    }

    /// F1 (final 2.0.1 review): the AMBIGUOUS BAND, which Task 1 left open.
    ///
    /// Task 1 quarantined a mid-file zero length-prefix only when the trailing
    /// bytes exceeded `4 + SEGMENT_FOOTER_LEN` (36) — "the most a real partial
    /// seal could leave". At or below 36 the original silent truncate stood, so
    /// the spec's OWN reproduction still reproduced: three 7-byte records with
    /// the middle length-prefix zeroed leaves a 26-byte tail (B's orphaned
    /// crc+payload, 11, plus C's whole 15-byte record), which is under the
    /// threshold. Recovery truncated C away and rewrote the footer to match, so
    /// the drain's record-count cross-check could not see the loss: a
    /// CRC-correct, acked-durable record destroyed with no trace.
    ///
    /// Size is not what distinguishes a partial seal from corruption — CONTENT
    /// is. A genuine seal writes `sentinel + a 32-byte footer whose
    /// record_count / data_bytes / file_crc32 describe exactly the records the
    /// replay loop just accumulated`. Corruption does not agree with itself.
    /// This fixture is the spec's original one, byte for byte.
    #[test]
    fn mid_file_sentinel_below_the_footer_threshold_is_quarantined_not_truncated() {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        let dir = tmp_dir("sentinel_small_tail");
        let path = make_segment(&dir, 0, &[b"A-acked", b"B-acked", b"C-acked"]);

        // Zero B's 4-byte length prefix in place, leaving C intact behind it.
        // Trailing bytes after the zeroed field: 26 — under the old 36-byte
        // threshold, so pre-fix this took the silent-truncate branch.
        zero_length_prefix_of_record(&path, 1);
        let on_disk = fs::read(&path).unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "a mid-file sentinel whose tail is too SMALL to hold a footer is still corruption — \
             C-acked must not be destroyed silently"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1
        );

        let q_dir = dir.join("quarantine");
        let q_entry = fs::read_dir(&q_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .next()
            .expect("a quarantine copy must exist — C-acked must not be lost with no trace");
        assert_eq!(
            fs::read(&q_entry).unwrap(),
            on_disk,
            "the quarantine copy must hold the full original segment bytes, including C-acked"
        );

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "only the pre-sentinel record A is delivered"
        );
        assert_eq!(recovered[0], b"A-acked" as &[u8]);

        fs::remove_dir_all(dir).ok();
    }

    /// F1: the complementary half — a segment that crashed between
    /// `finalize_to_disk`'s fsync and `seal`'s rename is a GENUINE complete
    /// seal sitting at the `.wab` path. Its sentinel is followed by exactly a
    /// 32-byte footer that agrees with the replayed records, so recovery must
    /// re-seal it cleanly with NO quarantine. This is the case the content
    /// cross-check has to get right, or every crash in that window turns into
    /// a spurious quarantine + ERROR during recovery.
    #[test]
    fn a_complete_footer_that_matches_the_replay_is_a_clean_partial_seal() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("sentinel_full_footer");
        let path = segment_path(&dir, 1);
        {
            let seg = {
                let mut s = WabSegment::create(&path, 0, Compression::None).unwrap();
                s.write_record(b"A-acked").unwrap();
                s.write_record(b"B-acked").unwrap();
                s
            };
            // Sentinel + footer + fsync, but NOT the rename: exactly the crash
            // window `finalize_to_disk` documents.
            seg.finalize_to_disk().unwrap();
        }

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            0,
            "a footer that agrees with the replayed records is a real seal, not corruption"
        );
        assert!(
            !dir.join("quarantine").exists(),
            "no quarantine directory should be created for a genuine partial seal"
        );

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0], b"A-acked" as &[u8]);
        assert_eq!(recovered[1], b"B-acked" as &[u8]);

        fs::remove_dir_all(dir).ok();
    }

    /// F1: a 32-byte tail is not a free pass either. If the trailing bytes are
    /// footer-SIZED but disagree with what the replay loop accumulated, they
    /// are not a footer — they are a corrupt record's remains that happen to
    /// be 32 bytes long. Quarantine.
    #[test]
    fn a_footer_sized_tail_that_disagrees_with_the_replay_is_quarantined() {
        let dir = tmp_dir("sentinel_bad_footer");
        let path = make_segment(&dir, 0, &[b"A-acked"]);
        // Sentinel + a footer claiming 99 records / 9999 bytes / a bogus CRC.
        append_raw(&path, &[0u8; 4]);
        append_raw(
            &path,
            &crate::wab::format::build_segment_footer(99, 9999, 0xdead_beef, 1),
        );

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "a footer that contradicts the replayed records must quarantine"
        );

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recovered.len(), 1);

        fs::remove_dir_all(dir).ok();
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

    /// An unknown segment-header FORMAT_VERSION is quarantined like a bad magic
    /// (mirrors `recovery_quarantines_bad_magic` for the version byte at [4]).
    #[test]
    fn recovery_quarantines_unknown_format_version() {
        let dir = tmp_dir("badversion");
        let path = make_segment(&dir, 0, &[b"keep"]);
        // Bump the header's format-version byte (offset 4) to an unknown value.
        let mut bytes = fs::read(&path).unwrap();
        bytes[4] = bytes[4].wrapping_add(7);
        fs::write(&path, &bytes).unwrap();

        let metrics = noop_metrics();
        let err = recover_segment(&path, &dir, &metrics).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("unknown format version"),
            "got: {err}"
        );
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        assert!(
            !path.exists(),
            "the bad-version segment must be quarantined"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// O_NOFOLLOW on the recovery read pass: a segment path that is a symlink
    /// (swapped in by an attacker with write access to the WAB dir) must fail the
    /// open with ELOOP rather than be followed to an attacker-chosen target. Pins
    /// the threat-model claim (S26) that segment opens don't follow symlinks.
    #[cfg(unix)]
    #[test]
    fn recovery_refuses_to_follow_a_symlinked_segment_path() {
        use crate::wab::segment::segment_path;
        let dir = tmp_dir("recover_symlink");
        // A real target file the symlink points at (must NOT be opened/followed).
        let target = dir.join("target.bin");
        fs::write(&target, b"attacker-controlled").unwrap();
        // seg_00000001.wab is a symlink to the target.
        let link = segment_path(&dir, 1);
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let metrics = noop_metrics();
        let err = recover_segment(&link, &dir, &metrics).unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ELOOP),
            "O_NOFOLLOW must refuse the symlink (ELOOP), got: {err}"
        );
        // The symlink target was not touched, and nothing was quarantined.
        assert_eq!(fs::read(&target).unwrap(), b"attacker-controlled");
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
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
        assert!(!check_confirmed(&sealed, &dir, &noop_metrics()).unwrap());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_quarantined_confirmed_sidecar_bumps_the_quarantine_metrics() {
        // Before this, check_confirmed called the bare quarantine() twice and
        // bumped NEITHER counter, so an operator alerting on them missed the case
        // entirely — the identical invisibility bug quarantine_and_count exists to
        // prevent, surviving in a second site while its doc comment claimed
        // "every quarantine site funnels through here".
        //
        // The SEGMENT is counted once; the .confirmed sidecar is a companion
        // artifact that moves with it, not a second quarantined segment.
        let dir = tmp_dir("conf_sidecar_counted");
        let sealed = dir.join("seg_00000001.wab.sealed");
        let confirmed = dir.join("seg_00000001.wab.confirmed");
        fs::write(&sealed, b"placeholder").unwrap();
        let mut bad = build_confirmed(0, 5, 1);
        let n = bad.len();
        bad[n - 1] ^= 0xff; // wreck the CRC
        fs::write(&confirmed, bad).unwrap();

        let metrics = noop_metrics();
        assert!(check_confirmed(&sealed, &dir, &metrics).is_err());
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "the quarantined segment must be counted exactly once"
        );
        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1,
            "wab_segments{{quarantined}} must move too"
        );
        assert!(!sealed.exists(), "the segment was moved to quarantine");
        assert!(!confirmed.exists(), "the sidecar moved with it");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn check_confirmed_valid_returns_true() {
        let dir = tmp_dir("validconf");
        let sealed = dir.join("seg_00000001.wab.sealed");
        let confirmed = dir.join("seg_00000001.wab.confirmed");
        fs::write(&sealed, b"placeholder").unwrap();
        fs::write(&confirmed, build_confirmed(0, 5, 1)).unwrap();
        assert!(check_confirmed(&sealed, &dir, &noop_metrics()).unwrap());
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

        assert!(check_confirmed(&sealed, &dir, &noop_metrics()).is_err());
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

        let err = check_confirmed(&sealed, &dir, &noop_metrics()).unwrap_err();
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

    // ── Compression (format v2) ───────────────────────────────────────────────
    //
    // These exist to PROVE the design's central claim rather than assume it:
    // the per-record CRC covers the STORED bytes, so recovery validates framing
    // and checksums without ever decompressing, and needs no compression
    // awareness at all. If any of these required a production change, the claim
    // would be wrong.

    #[test]
    fn torn_tail_in_a_compressed_segment_truncates_at_the_last_valid_record() {
        let dir = tmp_dir("v2_torn_tail");
        let path = make_segment_with(
            &dir,
            0,
            Compression::Zstd,
            &[b"record1", b"record2", b"record3"],
        );

        // Lop bytes off the end so the final record is torn mid-payload. Frame
        // sizes vary, so measure rather than hardcode an offset.
        let len = fs::metadata(&path).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 5)
            .unwrap();

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recovered.len(),
            2,
            "torn tail keeps the whole-record prefix"
        );
        assert_eq!(recovered[0].as_ref(), b"record1");
        assert_eq!(recovered[1].as_ref(), b"record2");

        assert!(
            !dir.join("quarantine").exists(),
            "a torn-tail (EOF) truncation must NOT quarantine, compressed or not"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn recovery_preserves_the_compression_flag() {
        // Recovery finalises an EXISTING file, so it never rewrites the header.
        // If this ever failed, a recovered segment would become unreadable — its
        // records are frames but its header would claim they are not.
        let dir = tmp_dir("v2_flag_preserved");
        let path = make_segment_with(&dir, 0, Compression::Zstd, &[b"one", b"two"]);

        let metrics = noop_metrics();
        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        let mut header = [0u8; crate::wab::format::SEGMENT_HEADER_LEN];
        {
            use std::io::Read;
            fs::File::open(&sealed)
                .unwrap()
                .read_exact(&mut header)
                .unwrap();
        }
        let meta = crate::wab::format::parse_segment_header(&header).unwrap();
        assert_eq!(meta.compression, Compression::Zstd);
        assert_eq!(meta.format_version, crate::wab::format::FORMAT_VERSION_V2);

        // And the records still read back as plaintext.
        let recovered: Vec<weir_core::Payload> = crate::wab::SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].as_ref(), b"one");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn mid_file_corruption_in_a_compressed_segment_quarantines() {
        // The 1.1.0 behaviour must hold under compression: a CRC mismatch in the
        // MIDDLE quarantines rather than truncating, so acked records sitting
        // after the corruption are preserved rather than silently dropped.
        let dir = tmp_dir("v2_midfile");
        let path = make_segment_with(&dir, 0, Compression::Zstd, &[b"recordA", b"recordB"]);

        // Corrupt a byte inside the FIRST record's stored frame, leaving its
        // length and CRC fields intact — the mid-file branch, since a valid
        // record follows it.
        let mut bytes = fs::read(&path).unwrap();
        let first_payload_start = crate::wab::format::SEGMENT_HEADER_LEN + 8;
        bytes[first_payload_start] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let metrics = noop_metrics();
        let _ = recover_segment(&path, &dir, &metrics);

        assert_eq!(
            metrics
                .wab_segments
                .get_or_create(&SegmentStateLabel {
                    state: SegmentState::quarantined,
                })
                .get(),
            1,
            "a mid-file-corrupt compressed segment must be quarantined"
        );
        fs::remove_dir_all(dir).ok();
    }

    /// Task 2 (2.0.1): recovery's payload cap must match the writer's under
    /// Zstd. The writer bounds a STORED record by `max_stored_record_bytes()`
    /// (`compress_bound(MAX_PAYLOAD_HARD_CAP)` ~= 16,843,025 — segment.rs:139),
    /// but recovery used to compare against the flat `MAX_PAYLOAD_HARD_CAP`
    /// (16,777,216) regardless of compression. `zstd -1` over 16 MiB of random
    /// data produces 16,777,614 bytes — 398 over the flat cap but comfortably
    /// under the writer's — so an incompressible (e.g. pre-compressed or
    /// encrypted) payload can be written, fsynced, and acked `true`, then
    /// quarantined as "corruption" by the very next crash recovery, along with
    /// every acked record behind it.
    ///
    /// No real zstd frame is needed to exercise this: recovery's replay loop
    /// never decompresses (see the "Compression (format v2)" note above) — it
    /// only compares `payload_len` against the cap and checks the STORED
    /// bytes' CRC. A hand-built stored payload of the right LENGTH drives the
    /// real cap comparison without the cost of a real 16 MiB zstd compress.
    #[test]
    fn recovery_accepts_a_record_the_writer_was_allowed_to_write_under_zstd() {
        use crate::wab::segment::{WabSegment, segment_path};
        let dir = tmp_dir("zstd_cap_window");
        let path = segment_path(&dir, 1);
        // Above the flat MAX_PAYLOAD_HARD_CAP, but within the writer's Zstd
        // bound (max_stored_record_bytes() ~= 16,843,025).
        let stored_len = MAX_PAYLOAD_HARD_CAP + 400;
        {
            let mut seg = WabSegment::create(&path, 0, Compression::Zstd).unwrap();
            seg.write_record(&vec![0xABu8; stored_len]).unwrap();
        }

        let metrics = noop_metrics();
        let before = metrics.recovery_segments_quarantined.get();

        let sealed = recover_segment(&path, &dir, &metrics).unwrap();

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            before,
            "a record the writer legitimately accepted must not be read as corruption"
        );

        // The hand-built payload isn't a real zstd frame, so we don't round-trip
        // it through `SegmentReader` (which decompresses) — recovery's replay
        // loop never decompresses either (see the "Compression (format v2)"
        // note above). Instead confirm directly that the WHOLE record made it
        // into the sealed file untruncated: header + (len+crc+payload) +
        // sentinel + footer, with no bytes dropped from the stored payload.
        assert!(sealed.exists());
        let expected_len = SEGMENT_HEADER_LEN as u64
            + 8
            + stored_len as u64
            + 4 // sentinel
            + SEGMENT_FOOTER_LEN as u64;
        assert_eq!(
            fs::metadata(&sealed).unwrap().len(),
            expected_len,
            "the full stored record must survive recovery untruncated"
        );

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_quarantined_segment_is_not_counted_as_a_recovery_failure() {
        // This fixture (a header too short to parse) was originally written for
        // `recovery_segments_failed` — WRONGLY. It never reached a genuine
        // failure: a short header goes to `quarantine_and_count`, which MOVES the
        // file and bumps `recovery_segments_quarantined`, then returns Err purely
        // as control flow. Counting that Err as a failure fires the counter on a
        // routine, fully-handled path — the "always fires, so it's noise" trap.
        //
        // The segment here is quarantined and IS reachable, via
        // `weir-ctl quarantine`. So: quarantined counter up, failed counter flat.
        let dir = tmp_dir("recovery_quarantined_not_failed");
        let shard_dir = dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        let path = crate::wab::segment::segment_path(&shard_dir, 1);
        fs::write(&path, b"nope").unwrap();

        let metrics = noop_metrics();
        recover_open_segments(&dir, &metrics).unwrap();
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            1,
            "a short header is quarantined, and that is what must be counted"
        );
        assert_eq!(
            metrics.recovery_segments_failed.get(),
            0,
            "a successfully quarantined segment is NOT a recovery failure — it was \
             moved to quarantine/, not left on disk, and it is recoverable"
        );
        assert!(
            !path.exists(),
            "quarantine renames, so the segment must be gone from its original path"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_segment_left_in_place_by_a_failed_recovery_is_counted() {
        // The case `recovery_segments_failed` actually documents, which had NO
        // test until now: recovery errors WITHOUT quarantining, so the segment is
        // still sitting at its original path and nothing will ever reach it.
        //
        // A directory at the segment path reaches that branch: the open succeeds,
        // the header read fails with something other than UnexpectedEof, so
        // recover_segment propagates it rather than routing to quarantine.
        let dir = tmp_dir("recovery_failed_left_in_place");
        let shard_dir = dir.join("shard_00");
        fs::create_dir_all(&shard_dir).unwrap();

        let path = crate::wab::segment::segment_path(&shard_dir, 1);
        fs::create_dir(&path).unwrap();

        let metrics = noop_metrics();
        recover_open_segments(&dir, &metrics).unwrap();
        assert_eq!(
            metrics.recovery_segments_failed.get(),
            1,
            "a segment left at its original path by a failed recovery must be counted"
        );
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            0,
            "nothing was quarantined — that is precisely why this one is a failure"
        );
        assert!(path.exists(), "the fixture must still be in place");
        fs::remove_dir_all(dir).ok();
    }
}
