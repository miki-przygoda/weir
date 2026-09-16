//! `weir-ctl attest` — the operator surface over `weir-attest`'s hash chain.
//!
//! `weir-attest` computes and verifies chains; this module is where an
//! operator drives it: enumerate sealed segments, chain the ones that need
//! it, and verify the rest against their `.attest` sidecars.
//!
//! Two things here are load-bearing rather than incidental:
//!
//! - **Chain order.** Segments within a shard are always processed sorted by
//!   file name, so each segment's `prev_head` is genuinely its predecessor's
//!   head. Chaining out of order would still produce a chain that verifies
//!   against itself — `verify_segment` only checks a segment against its own
//!   recorded `prev_head`, never against a sibling — but it would not mean
//!   anything: a reordered or substituted segment would go undetected.
//! - **The `weir.attest.head` line.** The `.attest` sidecar can be rewritten
//!   by anyone who can rewrite the segment next to it (see `weir-attest`'s
//!   crate docs). The line `seal` prints to stdout is the only thing that
//!   makes a chain *evidence* rather than just bookkeeping — it only works if
//!   something outside this process's control (a log shipper, an operator
//!   pasting it somewhere) captures it.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use weir_attest::{
    ChainHead, Sidecar, Verdict, chain_segment, segment_name_for, sidecar_path, verify_segment,
};
use weir_wab::SegmentState;

/// Parses the numeric shard id out of a `shard_NN` directory name, or `None`
/// for anything else (including the daemon's reserved `quarantine/` and
/// `dead_letter/` subdirs).
///
/// Mirrors `weir-server`'s `wab::segment::shard_id_from_path` (`pub(crate)`
/// there, so re-derived rather than imported — different crate). Needed for
/// ordering, not just filtering: `shard_{id:02}` is a MINIMUM width, so past
/// `shard_99` a plain lexicographic sort would put `shard_100` before
/// `shard_20` (P2-F3, already fixed twice in `weir-server`: `wab/mod.rs` and
/// `wab/recovery.rs`).
fn shard_id_from_dir_name(path: &Path) -> Option<usize> {
    path.file_name()?
        .to_str()?
        .strip_prefix("shard_")?
        .parse()
        .ok()
}

/// Shard directories under `wab_dir`, optionally narrowed to one shard id,
/// in ascending numeric shard order (see [`shard_id_from_dir_name`]).
///
/// Mirrors the walk `scan_segments` (in `main.rs`) uses for `weir-ctl
/// segments`, plus the daemon's own reserved-subdir exclusion
/// (`weir-server`'s `wab/mod.rs` and `wab/recovery.rs` both skip
/// `quarantine` and `dead_letter` for the same reason): every OTHER
/// subdirectory is a shard directory, named `shard_NN` by the daemon
/// (`weir-server`'s `shard_dir_path`), which is what a `--shard` filter
/// reconstructs directly rather than scanning for it.
///
/// Excluding `quarantine` here is load-bearing, not cosmetic: it is one flat
/// directory shared by every shard's parked forensic copies, so chaining it
/// as if it were a shard would link segments from DIFFERENT shards into one
/// meaningless cross-shard chain, and would write a `.attest` sidecar inside
/// a directory whose entire purpose is pristine, untouched copies.
fn shard_dirs(wab_dir: &Path, shard: Option<u16>) -> Result<Vec<PathBuf>, String> {
    if let Some(id) = shard {
        return Ok(vec![wab_dir.join(format!("shard_{id:02}"))]);
    }
    let entries =
        std::fs::read_dir(wab_dir).map_err(|e| format!("read {}: {e}", wab_dir.display()))?;
    let mut dirs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter(|p| {
            !matches!(
                p.file_name().and_then(|n| n.to_str()),
                Some("dead_letter") | Some("quarantine")
            )
        })
        .collect();
    dirs.sort_by_key(|p| shard_id_from_dir_name(p));
    Ok(dirs)
}

/// Sealed segments in one shard directory, in the order a chain must link
/// them: sorted by file name. `list_segment_files` already returns its
/// results path-sorted; the filter below preserves that order.
fn sealed_segments_in(shard_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let files = weir_wab::list_segment_files(shard_dir)
        .map_err(|e| format!("read {}: {e}", shard_dir.display()))?;
    Ok(files
        .into_iter()
        .filter(|(_, state)| *state == SegmentState::Sealed)
        .map(|(p, _)| p)
        .collect())
}

/// Best-effort read of an existing sidecar's `prev_head`, used only to feed
/// the `weir_attest_chain_origins_total` metric. `None` on any failure to
/// read or decode — a decode failure surfaces separately, as an `errors`
/// entry from `verify_segment` itself; this helper never duplicates it.
fn sidecar_prev_head(seg: &Path) -> Option<ChainHead> {
    let bytes = std::fs::read(sidecar_path(seg)).ok()?;
    Sidecar::decode(&bytes).ok().map(|s| s.prev_head)
}

/// The anchor line's fields as a JSON object, for `--json`.
fn attest_head_json(sidecar: &Sidecar) -> serde_json::Value {
    serde_json::json!({
        "event": "weir.attest.head",
        "shard": sidecar.shard_id,
        "segment": sidecar.segment_name,
        "records": sidecar.record_count,
        "head": sidecar.head.to_hex(),
    })
}

/// Emits the anchor line for one segment's chain head: the load-bearing
/// output of `seal` (freshly computed) and `head` (re-printing the newest
/// existing one), in exactly the same shape both times so an operator or log
/// shipper can treat them identically.
///
/// Deliberately a single, compact object under `--json` too, PRINTED
/// IMMEDIATELY per segment — a deliberate divergence from this crate's usual
/// `--json` convention (one pretty end-of-run blob via `print_json`, as
/// `attest verify` itself still uses). Batching these into one blob at the
/// end would mean a process killed partway through a `seal` run loses the
/// anchor for every segment it had already chained but not yet reported —
/// exactly the evidence this line exists to preserve. So: compact, one
/// object per line, flushed as each segment is chained, with an added
/// `"event"` field (beyond the four named fields) so a line is self
/// describing if merged into a broader JSON log stream.
fn emit_attest_head_line(sidecar: &Sidecar, json: bool) {
    if json {
        let value = attest_head_json(sidecar);
        match serde_json::to_string(&value) {
            Ok(s) => println!("{s}"),
            Err(_) => println!("{value}"),
        }
    } else {
        println!(
            "weir.attest.head shard={} segment={} records={} head={}",
            sidecar.shard_id,
            sidecar.segment_name,
            sidecar.record_count,
            sidecar.head.to_hex()
        );
    }
}

/// `weir-ctl attest seal`: chain every sealed segment that has no `.attest`
/// sidecar yet.
///
/// Per shard, segments are walked oldest-to-newest; a segment that already
/// has a sidecar contributes its recorded head as the next segment's
/// `prev_head` without being re-chained (chaining is idempotent per segment,
/// but re-doing it would just re-derive the same head at the cost of a full
/// re-read — the sidecar already has the answer). A shard's first sealed
/// segment chains from [`ChainHead::ORIGIN`].
pub(crate) fn cmd_attest_seal(
    wab_dir: &Path,
    shard: Option<u16>,
    json: bool,
) -> Result<(), String> {
    let dirs = shard_dirs(wab_dir, shard)?;
    let mut chained = 0usize;
    for dir in &dirs {
        let segments = sealed_segments_in(dir)?;
        let mut prev = ChainHead::ORIGIN;
        for seg in segments {
            let sp = sidecar_path(&seg);
            if sp.exists() {
                let bytes =
                    std::fs::read(&sp).map_err(|e| format!("read {}: {e}", sp.display()))?;
                let sidecar =
                    Sidecar::decode(&bytes).map_err(|e| format!("decode {}: {e}", sp.display()))?;
                prev = sidecar.head;
                continue;
            }
            let sidecar =
                chain_segment(&seg, prev).map_err(|e| format!("chain {}: {e}", seg.display()))?;
            std::fs::write(&sp, sidecar.encode())
                .map_err(|e| format!("write {}: {e}", sp.display()))?;
            emit_attest_head_line(&sidecar, json);
            prev = sidecar.head;
            chained += 1;
        }
    }
    if chained == 0 && !json {
        println!(
            "no unattested sealed segments found under {}",
            wab_dir.display()
        );
    }
    Ok(())
}

/// `weir-ctl attest head`: the newest chained head per shard, re-printed in
/// the same anchor shape `seal` emits when it freshly chains a segment — so
/// an operator can re-observe (and re-anchor off-host) the current state at
/// any time, not only right after a `seal` run.
///
/// A sealed segment newer than the last one with a sidecar does not move the
/// reported head: the chain only extends as far as `seal` has actually run.
pub(crate) fn cmd_attest_head(wab_dir: &Path, json: bool) -> Result<(), String> {
    let dirs = shard_dirs(wab_dir, None)?;
    let mut printed = 0usize;
    for dir in &dirs {
        let segments = sealed_segments_in(dir)?;
        let mut newest: Option<Sidecar> = None;
        for seg in segments {
            let sp = sidecar_path(&seg);
            if !sp.exists() {
                continue;
            }
            let bytes = std::fs::read(&sp).map_err(|e| format!("read {}: {e}", sp.display()))?;
            let sidecar =
                Sidecar::decode(&bytes).map_err(|e| format!("decode {}: {e}", sp.display()))?;
            newest = Some(sidecar);
        }
        if let Some(sidecar) = newest {
            emit_attest_head_line(&sidecar, json);
            printed += 1;
        }
    }
    if printed == 0 && !json {
        println!("no attested segments found under {}", wab_dir.display());
    }
    Ok(())
}

/// One `Verdict::Diverged` in `verify`'s aggregated report.
struct DivergedEntry {
    segment: String,
    at_record: Option<u64>,
    expected: String,
    actual: String,
}

/// The machine-readable form of `attest verify`'s report. Always the same
/// shape regardless of outcome — a consumer parses one schema whether the run
/// was clean, found tampering, or hit an unreadable segment; the exit code,
/// not the JSON shape, is what changes.
fn verify_report_json(
    wab_dir: &Path,
    segments_total: u64,
    verified: u64,
    diverged: &[DivergedEntry],
    missing_sidecar: &[String],
    errors: &[String],
) -> serde_json::Value {
    let diverged_json: Vec<serde_json::Value> = diverged
        .iter()
        .map(|d| {
            serde_json::json!({
                "segment": d.segment,
                "at_record": d.at_record,
                "expected": d.expected,
                "actual": d.actual,
            })
        })
        .collect();
    serde_json::json!({
        "wab_dir": wab_dir.display().to_string(),
        "segments_total": segments_total,
        "verified": verified,
        "diverged": diverged_json,
        "missing_sidecar": missing_sidecar,
        "errors": errors,
    })
}

/// The human-readable form of `attest verify`'s report: one line per problem
/// (a diverged segment, a missing sidecar, an unreadable segment), then a
/// summary line with the totals a cron job's log would want.
///
/// When `segments_total` is `0` and nothing went wrong enumerating the WAB
/// directory, the summary line is replaced with an explicit "nothing to
/// verify" line instead: `segments=0 verified=0 ... exit 0` on its own reads
/// as "everything verified", when the truth is "there was nothing to check"
/// — the same distinction `cmd_segments` and `cmd_attest_seal` already make
/// for their own empty cases. A non-empty `errors` (e.g. the WAB directory
/// itself could not be read) is deliberately NOT covered by that early
/// message: that case already explains itself via its `ERROR` line(s) and
/// exits non-zero.
///
/// Built as a `String` (rather than printing directly) so the exact wording
/// is unit-testable without capturing stdout.
fn verify_report_text(
    wab_dir: &Path,
    segments_total: u64,
    verified: u64,
    diverged: &[DivergedEntry],
    missing_sidecar: &[String],
    errors: &[String],
) -> String {
    if segments_total == 0 && errors.is_empty() {
        return format!(
            "no sealed segments found under {} — nothing to verify\n",
            wab_dir.display()
        );
    }
    let mut out = String::new();
    for d in diverged {
        let at = d
            .at_record
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        out.push_str(&format!(
            "DIVERGED {} at_record={at} expected={} actual={}\n",
            d.segment, d.expected, d.actual
        ));
    }
    for m in missing_sidecar {
        out.push_str(&format!("MISSING_SIDECAR {m}\n"));
    }
    for e in errors {
        out.push_str(&format!("ERROR {e}\n"));
    }
    out.push_str(&format!(
        "{}: segments={segments_total} verified={verified} diverged={} missing_sidecar={} errors={}\n",
        wab_dir.display(),
        diverged.len(),
        missing_sidecar.len(),
        errors.len()
    ));
    out
}

/// The node_exporter textfile-collector body for `--metrics-file`.
///
/// All three are `gauge`s, not `counter`s, despite the `_total` naming: each
/// run overwrites the file with this run's snapshot rather than accumulating,
/// so the value can legitimately go down between scrapes (e.g. segments
/// pruned, or a shard added). `_total` here means "count observed this run",
/// matching how batch/cron jobs commonly name textfile-collector gauges.
fn metrics_body(segments_total: u64, verify_failures: u64, chain_origins: u64) -> String {
    format!(
        "# HELP weir_attest_segments_total Sealed segments examined by the last weir-ctl attest verify run.\n\
         # TYPE weir_attest_segments_total gauge\n\
         weir_attest_segments_total {segments_total}\n\
         # HELP weir_attest_verify_failures_total Segments that diverged or could not be verified in the last run.\n\
         # TYPE weir_attest_verify_failures_total gauge\n\
         weir_attest_verify_failures_total {verify_failures}\n\
         # HELP weir_attest_chain_origins_total Segments observed with no chain predecessor in the last run.\n\
         # TYPE weir_attest_chain_origins_total gauge\n\
         weir_attest_chain_origins_total {chain_origins}\n"
    )
}

/// Writes node_exporter textfile-collector metrics to `path`: rendered to a
/// temp file in the same directory, fsynced, then renamed into place. The
/// rename (same filesystem) is atomic, so a collector scraping concurrently
/// either sees the old file or the complete new one — never a half-written
/// one.
fn write_metrics_file(
    path: &Path,
    segments_total: u64,
    verify_failures: u64,
    chain_origins: u64,
) -> Result<(), String> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("weir_attest.prom");
    let tmp_path = dir.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let body = metrics_body(segments_total, verify_failures, chain_origins);
    {
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| format!("create {}: {e}", tmp_path.display()))?;
        f.write_all(body.as_bytes())
            .map_err(|e| format!("write {}: {e}", tmp_path.display()))?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {e}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, path)
        .map_err(|e| format!("rename {} -> {}: {e}", tmp_path.display(), path.display()))?;
    Ok(())
}

/// `weir-ctl attest verify`: recompute every sealed segment's chain and
/// compare it to its `.attest` sidecar.
///
/// Returns its exit code directly (rather than the crate's usual
/// `Result<(), String>`) because a cron job must alert differently on three
/// distinct outcomes:
///
/// - `0` — every segment verified.
/// - `1` — at least one [`Verdict::Diverged`] — tampering.
/// - `2` — at least one segment could not even be checked: `verify_segment`
///   returned `Err` (unreadable segment, undecodable sidecar), or the WAB
///   directory itself could not be enumerated. A structurally corrupt segment
///   falls in here too, via the same `Err` path — "corrupt" and "tampered"
///   have different runbooks, and this is what keeps them distinguishable at
///   the exit code.
///
/// [`Verdict::MissingSidecar`] is neither: a segment nobody has run `seal` on
/// yet is not evidence of tampering, so it does not move the exit code off
/// `0` by itself (unless something else in the run does) — it is reported in
/// the output and counted, not treated as a failure. Likewise, a WAB
/// directory with sealed segments to check that verify all clean is
/// reported distinctly from one with NOTHING to check at all — the latter
/// says so explicitly (see [`verify_report_text`]) rather than presenting an
/// empty run as "everything verified".
///
/// `--json` here prints ONE pretty JSON object summarising the whole run
/// (`print_json`, same convention as `segments`/`dl list`) — unlike
/// `seal`/`head`, which stream one compact object per segment (see
/// [`emit_attest_head_line`]). `verify` has nothing that must survive a
/// mid-run crash to remain evidence, so there is no reason to stream it.
pub(crate) fn cmd_attest_verify(
    wab_dir: &Path,
    shard: Option<u16>,
    metrics_file: Option<&Path>,
    json: bool,
) -> ExitCode {
    let mut errors: Vec<String> = Vec::new();
    let dirs = match shard_dirs(wab_dir, shard) {
        Ok(d) => d,
        Err(e) => {
            errors.push(e);
            Vec::new()
        }
    };

    let mut segments_total: u64 = 0;
    let mut verified: u64 = 0;
    let mut chain_origins: u64 = 0;
    let mut diverged: Vec<DivergedEntry> = Vec::new();
    let mut missing_sidecar: Vec<String> = Vec::new();

    for dir in &dirs {
        let segments = match sealed_segments_in(dir) {
            Ok(s) => s,
            Err(e) => {
                errors.push(e);
                continue;
            }
        };
        for seg in segments {
            segments_total += 1;
            let name = segment_name_for(&seg);
            match verify_segment(&seg) {
                Ok(Verdict::Verified) => {
                    verified += 1;
                    if sidecar_prev_head(&seg) == Some(ChainHead::ORIGIN) {
                        chain_origins += 1;
                    }
                }
                Ok(Verdict::Diverged {
                    at_record,
                    expected,
                    actual,
                }) => {
                    if sidecar_prev_head(&seg) == Some(ChainHead::ORIGIN) {
                        chain_origins += 1;
                    }
                    diverged.push(DivergedEntry {
                        segment: name,
                        at_record,
                        expected: expected.to_hex(),
                        actual: actual.to_hex(),
                    });
                }
                Ok(Verdict::MissingSidecar) => missing_sidecar.push(name),
                Err(e) => errors.push(format!("{name}: {e}")),
                // `Verdict` is `#[non_exhaustive]`: a future weir-attest release
                // could add a variant this build does not know how to classify.
                // Treat that as "could not check" (exit 2), not silent success —
                // the whole point of this split is to never mis-report tampering
                // as clean.
                Ok(_) => errors.push(format!(
                    "{name}: unrecognized Verdict variant (weir-attest version skew?)"
                )),
            }
        }
    }

    // Mirrors the exit-code split: a "failure" here is exactly what pushes the
    // exit code off 0 — diverged (tampered) and errors (could not check).
    // `MissingSidecar` is deliberately excluded (see the function docs).
    let verify_failures = diverged.len() as u64 + errors.len() as u64;

    if let Some(path) = metrics_file
        && let Err(e) = write_metrics_file(path, segments_total, verify_failures, chain_origins)
    {
        errors.push(format!("metrics file: {e}"));
    }

    if json {
        crate::print_json(&verify_report_json(
            wab_dir,
            segments_total,
            verified,
            &diverged,
            &missing_sidecar,
            &errors,
        ));
    } else {
        print!(
            "{}",
            verify_report_text(
                wab_dir,
                segments_total,
                verified,
                &diverged,
                &missing_sidecar,
                &errors,
            )
        );
    }

    if !errors.is_empty() {
        ExitCode::from(2)
    } else if !diverged.is_empty() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weir_wab::format::{
        Compression, build_segment_footer, build_segment_header, build_sentinel,
    };

    /// A scratch directory unique to one test, cleaned up on drop via an RAII
    /// guard so a panicking assertion still removes it.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("weir_ctl_attest_{label}_{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    /// Writes a real sealed segment (header + records + sentinel + footer),
    /// the same shape `weir-attest`'s own tests and `weir-ctl`'s dead-letter
    /// fixtures use, so `chain_segment`/`verify_segment` operate on the exact
    /// on-disk format `weir-wab` produces.
    fn write_segment(path: &Path, shard_id: u16, records: &[&[u8]]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&build_segment_header(shard_id, Compression::None));
        let mut data_bytes = 0u64;
        for r in records {
            body.extend_from_slice(&(r.len() as u32).to_le_bytes());
            body.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
            body.extend_from_slice(r);
            data_bytes += r.len() as u64;
        }
        let file_crc = crc32fast::hash(&body);
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&body).unwrap();
        f.write_all(&build_sentinel()).unwrap();
        f.write_all(&build_segment_footer(
            records.len() as u64,
            data_bytes,
            file_crc,
            1,
        ))
        .unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn seal_then_verify_reports_verified_and_exits_zero() {
        let scratch = Scratch::new("seal_verify_ok");
        let shard = scratch.path().join("shard_00");
        write_segment(&shard.join("seg_00000000.wab.sealed"), 0, &[b"a", b"b"]);
        write_segment(&shard.join("seg_00000001.wab.sealed"), 0, &[b"c"]);

        cmd_attest_seal(scratch.path(), None, false).expect("seal must succeed");
        assert!(
            sidecar_path(&shard.join("seg_00000000.wab.sealed")).exists(),
            "seal must write a sidecar for every sealed segment"
        );
        assert!(sidecar_path(&shard.join("seg_00000001.wab.sealed")).exists());

        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::SUCCESS,
            "an untampered, freshly sealed chain must verify and exit 0"
        );
    }

    #[test]
    fn seal_chains_the_second_segment_to_the_firsts_head_not_origin() {
        // The property Task 6 adds beyond weir-attest itself: segments are
        // chained in file-name order, so the second segment's `prev_head` is
        // the first segment's REAL head, not ChainHead::ORIGIN.
        let scratch = Scratch::new("seal_order");
        let shard = scratch.path().join("shard_00");
        let seg0 = shard.join("seg_00000000.wab.sealed");
        let seg1 = shard.join("seg_00000001.wab.sealed");
        write_segment(&seg0, 0, &[b"a"]);
        write_segment(&seg1, 0, &[b"b"]);

        cmd_attest_seal(scratch.path(), None, false).unwrap();

        let head0 = Sidecar::decode(&std::fs::read(sidecar_path(&seg0)).unwrap())
            .unwrap()
            .head;
        let sidecar1 = Sidecar::decode(&std::fs::read(sidecar_path(&seg1)).unwrap()).unwrap();
        assert_eq!(
            sidecar1.prev_head, head0,
            "the second segment must chain from the first segment's real head"
        );
        assert_ne!(
            sidecar1.prev_head,
            ChainHead::ORIGIN,
            "only a shard's FIRST segment chains from ORIGIN"
        );
    }

    #[test]
    fn tampering_a_sealed_segment_after_seal_is_reported_as_diverged_and_exits_one() {
        let scratch = Scratch::new("tamper");
        let shard = scratch.path().join("shard_00");
        let seg = shard.join("seg_00000000.wab.sealed");
        write_segment(&seg, 0, &[b"one", b"two", b"three"]);

        cmd_attest_seal(scratch.path(), None, false).unwrap();
        assert_eq!(
            cmd_attest_verify(scratch.path(), None, None, false),
            ExitCode::SUCCESS
        );

        // Tamper: overwrite the sealed segment's bytes with different content
        // at the same path, WITHOUT touching its `.attest` sidecar — exactly
        // the substitution the chain exists to catch.
        write_segment(&seg, 0, &[b"one", b"XXX", b"three"]);

        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::from(1),
            "a tampered segment must be reported as diverged and exit 1"
        );
    }

    #[test]
    fn a_missing_sidecar_is_reported_without_being_treated_as_tampering() {
        let scratch = Scratch::new("missing_sidecar");
        let shard = scratch.path().join("shard_00");
        // Sealed on disk, but `seal` was never run — no `.attest` sidecar.
        write_segment(&shard.join("seg_00000000.wab.sealed"), 0, &[b"a"]);

        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::SUCCESS,
            "a segment with no sidecar yet is not tampering and must not move the exit code"
        );
    }

    #[test]
    fn an_unreadable_segment_exits_two_not_one() {
        // A structurally corrupt segment fails inside `SegmentReader`, so
        // `verify_segment` returns `Err`, which must map to exit 2
        // ("could not check"), never 1 ("tampered") — corrupt and tampered
        // have different runbooks. `verify_segment` only reaches the reader
        // once a sidecar exists (a missing sidecar short-circuits earlier), so
        // this seals first, THEN corrupts the segment bytes in place.
        let scratch = Scratch::new("unreadable");
        let shard = scratch.path().join("shard_00");
        let seg = shard.join("seg_00000000.wab.sealed");
        write_segment(&seg, 0, &[b"a", b"b"]);
        cmd_attest_seal(scratch.path(), None, false).unwrap();
        assert!(
            sidecar_path(&seg).exists(),
            "sealing must have written a sidecar"
        );

        std::fs::write(&seg, b"not a real segment").unwrap();

        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::from(2),
            "an unreadable/corrupt segment must exit 2, distinct from tampering's exit 1"
        );
    }

    #[test]
    fn verify_writes_node_exporter_metrics() {
        let scratch = Scratch::new("metrics");
        let shard = scratch.path().join("shard_00");
        write_segment(&shard.join("seg_00000000.wab.sealed"), 0, &[b"a"]);
        cmd_attest_seal(scratch.path(), None, false).unwrap();

        let metrics_path = scratch.path().join("weir_attest.prom");
        let code = cmd_attest_verify(scratch.path(), None, Some(&metrics_path), false);
        assert_eq!(code, ExitCode::SUCCESS);

        let body = std::fs::read_to_string(&metrics_path).unwrap();
        assert!(body.contains("# TYPE weir_attest_segments_total gauge"));
        assert!(body.contains("weir_attest_segments_total 1"));
        assert!(body.contains("weir_attest_verify_failures_total 0"));
        assert!(body.contains("weir_attest_chain_origins_total 1"));
        // No leftover temp file: the write-then-rename must not leak its
        // staging file next to the final one.
        let leftover = std::fs::read_dir(scratch.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".tmp"));
        assert!(
            !leftover,
            "the temp file must be renamed away, not left behind"
        );
    }

    #[test]
    fn head_reprints_the_newest_chained_segment_per_shard() {
        let scratch = Scratch::new("head");
        let shard = scratch.path().join("shard_00");
        write_segment(&shard.join("seg_00000000.wab.sealed"), 0, &[b"a"]);
        write_segment(&shard.join("seg_00000001.wab.sealed"), 0, &[b"b"]);
        cmd_attest_seal(scratch.path(), None, false).unwrap();

        // `head` must succeed and not error even though it has nothing to
        // chain — it only reads existing sidecars.
        cmd_attest_head(scratch.path(), false).expect("head must succeed against a sealed chain");
    }

    #[test]
    fn shard_filter_limits_seal_and_verify_to_one_shard() {
        let scratch = Scratch::new("shard_filter");
        write_segment(
            &scratch
                .path()
                .join("shard_00")
                .join("seg_00000000.wab.sealed"),
            0,
            &[b"a"],
        );
        write_segment(
            &scratch
                .path()
                .join("shard_01")
                .join("seg_00000000.wab.sealed"),
            1,
            &[b"b"],
        );

        cmd_attest_seal(scratch.path(), Some(0), false).unwrap();
        assert!(
            sidecar_path(
                &scratch
                    .path()
                    .join("shard_00")
                    .join("seg_00000000.wab.sealed")
            )
            .exists(),
            "the selected shard must be chained"
        );
        assert!(
            !sidecar_path(
                &scratch
                    .path()
                    .join("shard_01")
                    .join("seg_00000000.wab.sealed")
            )
            .exists(),
            "a shard excluded by --shard must be left untouched"
        );
    }

    #[test]
    fn shard_dirs_excludes_quarantine_and_dead_letter() {
        let scratch = Scratch::new("shard_dirs_exclude");
        std::fs::create_dir_all(scratch.path().join("shard_00")).unwrap();
        std::fs::create_dir_all(scratch.path().join("shard_01")).unwrap();
        std::fs::create_dir_all(scratch.path().join("quarantine")).unwrap();
        std::fs::create_dir_all(scratch.path().join("dead_letter")).unwrap();

        let dirs = shard_dirs(scratch.path(), None).unwrap();
        let names: Vec<String> = dirs
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["shard_00", "shard_01"],
            "quarantine/ and dead_letter/ must never be treated as shard directories"
        );
    }

    #[test]
    fn shard_dirs_sorts_numerically_not_lexicographically() {
        // P2-F3: `shard_{id:02}` is a MINIMUM width, so past shard_99 a plain
        // lexicographic sort would put "shard_100" before "shard_20". This
        // codebase has already fixed the identical bug twice in weir-server
        // (wab/mod.rs, wab/recovery.rs); `weir-ctl` must not reintroduce it.
        let scratch = Scratch::new("shard_dirs_numeric");
        for id in [20, 100, 3] {
            std::fs::create_dir_all(scratch.path().join(format!("shard_{id:02}"))).unwrap();
        }
        let dirs = shard_dirs(scratch.path(), None).unwrap();
        let ids: Vec<Option<usize>> = dirs.iter().map(|p| shard_id_from_dir_name(p)).collect();
        assert_eq!(
            ids,
            vec![Some(3), Some(20), Some(100)],
            "shard directories must sort in ascending NUMERIC shard order"
        );
    }

    #[test]
    fn seal_does_not_chain_a_segment_parked_in_quarantine() {
        // Reproduces the review finding: quarantine/ is one flat directory
        // shared by every shard's forensic copies. Treating it as a shard
        // would chain segments from DIFFERENT shards into one meaningless
        // chain, and would write a `.attest` sidecar into a directory whose
        // entire purpose is pristine, untouched copies.
        let scratch = Scratch::new("seal_quarantine");
        let real = scratch
            .path()
            .join("shard_00")
            .join("seg_00000000.wab.sealed");
        write_segment(&real, 0, &[b"a"]);
        // Named the way weir-server's quarantine actually does:
        // `shard_NN__seg_....wab.sealed`, flattened into one directory.
        let quarantined = scratch
            .path()
            .join("quarantine")
            .join("shard_00__seg_00000001.wab.sealed");
        write_segment(&quarantined, 0, &[b"b"]);

        cmd_attest_seal(scratch.path(), None, false).unwrap();

        assert!(
            sidecar_path(&real).exists(),
            "the real shard segment must still be chained"
        );
        assert!(
            !sidecar_path(&quarantined).exists(),
            "a segment parked in quarantine/ must never get a `.attest` sidecar"
        );
    }

    #[test]
    fn verify_on_an_empty_wab_dir_says_so_and_exits_zero() {
        // segments=0 verified=0 ... exit 0 reads as "everything verified" when
        // the truth is "nothing was there to check" — these must be
        // distinguishable in the report, not just in the (identical) exit code.
        let scratch = Scratch::new("verify_empty");
        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::SUCCESS,
            "an empty WAB directory is not a failure"
        );

        let text = verify_report_text(scratch.path(), 0, 0, &[], &[], &[]);
        assert!(
            text.contains("nothing to verify"),
            "an empty run must say so explicitly rather than print zeroed counters \
             that read as success: {text:?}"
        );
    }

    #[test]
    fn verify_report_text_does_not_claim_nothing_to_verify_when_it_actually_errored() {
        // segments_total can be 0 AND errors non-empty at once (e.g. the WAB
        // directory itself could not be enumerated) — that case must keep its
        // ERROR line(s), not get swallowed by the "nothing to verify" message.
        let scratch = Scratch::new("verify_empty_but_errored");
        let text = verify_report_text(
            scratch.path(),
            0,
            0,
            &[],
            &[],
            &["shard_00: read failed".to_string()],
        );
        assert!(!text.contains("nothing to verify"));
        assert!(text.contains("ERROR shard_00: read failed"));
    }

    #[test]
    fn diverged_and_unreadable_together_exit_two_not_one() {
        // The brief's priority check: when a run has BOTH a tampered segment
        // AND an unreadable one, "could not check everything" must win over
        // "found tampering" — downgrading to exit 1 would tell a cron job
        // the run was merely tampered when part of it could not be verified
        // at all.
        let scratch = Scratch::new("mixed_outcomes");
        let shard = scratch.path().join("shard_00");
        let tampered = shard.join("seg_00000000.wab.sealed");
        let corrupt = shard.join("seg_00000001.wab.sealed");
        write_segment(&tampered, 0, &[b"one", b"two"]);
        write_segment(&corrupt, 0, &[b"three"]);

        cmd_attest_seal(scratch.path(), None, false).unwrap();
        assert_eq!(
            cmd_attest_verify(scratch.path(), None, None, false),
            ExitCode::SUCCESS
        );

        // Tamper the first segment's content, sidecar untouched.
        write_segment(&tampered, 0, &[b"one", b"XXX"]);
        // Corrupt the second segment's bytes outright, sidecar untouched.
        std::fs::write(&corrupt, b"not a real segment").unwrap();

        let code = cmd_attest_verify(scratch.path(), None, None, false);
        assert_eq!(
            code,
            ExitCode::from(2),
            "errors (could-not-check) must outrank a divergence in the exit code"
        );
    }
}
