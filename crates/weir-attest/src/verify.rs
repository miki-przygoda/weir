//! Chaining and verifying a sealed segment on disk.

use crate::{AttestError, ChainHead, Sidecar, chain};
use std::path::Path;
use weir_sink_sdk::RecordId;

/// The name a segment is chained under: `shard_NN/seg_....`.
///
/// The parent directory is included because `RecordId` commits to the segment
/// name the daemon uses, which is shard-qualified. Getting this wrong makes
/// every chain head disagree with what a sink could recompute.
pub fn segment_name_for(path: &Path) -> String {
    let file = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    match path.parent().and_then(|p| p.file_name()) {
        Some(shard) => format!("{}/{}", shard.to_string_lossy(), file),
        None => file,
    }
}

/// Compute the chain for one sealed segment.
///
/// Verifies the segment's own structure first via `weir-wab`, so "corrupt" and
/// "tampered" stay distinguishable — they have different runbooks.
pub fn chain_segment(path: &Path, prev: ChainHead) -> Result<Sidecar, AttestError> {
    let verification = weir_wab::verify_sealed_segment(path)
        .map_err(|e| AttestError::Io(std::io::Error::other(e.to_string())))?;

    let reader = weir_wab::SegmentReader::open(path)?;
    let header = reader.header().clone();
    let name = segment_name_for(path);

    let mut head = chain::chain_origin(prev, &header, &name);
    let mut count: u64 = 0;
    for (i, record) in reader.enumerate() {
        let payload = record?;
        // 1-based, matching RecordId's contract and the daemon's own indexing.
        let id = RecordId::for_record(&name, i as u64 + 1, &payload);
        head = chain::chain_step(head, &id);
        count += 1;
    }

    Ok(Sidecar {
        format_version: crate::ATTEST_FORMAT_VERSION,
        shard_id: header.shard_id,
        segment_name: name,
        prev_head: prev,
        head,
        record_count: count,
        sealed_at: verification.footer.sealed_at,
    })
}

/// Where the `.attest` sidecar for a segment lives.
pub fn sidecar_path(segment: &Path) -> std::path::PathBuf {
    let mut s = segment.as_os_str().to_os_string();
    s.push(".attest");
    std::path::PathBuf::from(s)
}

/// The outcome of verifying one segment.
#[derive(Debug)]
#[non_exhaustive]
pub enum Verdict {
    /// Recomputed head matches the sidecar.
    Verified,
    /// The chain diverged.
    Diverged {
        /// Where the chain stopped matching, 1-based — `Some` only when that is
        /// actually knowable.
        ///
        /// A truncated segment has an exact answer: the first record the
        /// sidecar claims and the file does not have. A mid-file CONTENT
        /// change does not: the chain carries no per-record checkpoints, so
        /// every head after the edit differs and none of them identifies
        /// which record moved. Reporting a number there would be inventing
        /// precision the data does not support.
        at_record: Option<u64>,
        /// What the sidecar claims.
        expected: ChainHead,
        /// What the segment now produces.
        actual: ChainHead,
    },
    /// No sidecar exists for this segment.
    MissingSidecar,
}

/// Recompute a segment's chain and compare it to its sidecar.
pub fn verify_segment(path: &Path) -> Result<Verdict, AttestError> {
    let sp = sidecar_path(path);
    if !sp.exists() {
        return Ok(Verdict::MissingSidecar);
    }
    let sidecar = Sidecar::decode(&std::fs::read(&sp)?)?;

    let reader = weir_wab::SegmentReader::open(path)?;
    let header = reader.header().clone();
    let name = segment_name_for(path);
    let mut head = chain::chain_origin(sidecar.prev_head, &header, &name);
    let mut count: u64 = 0;
    for (i, record) in reader.enumerate() {
        let payload = record?;
        let id = RecordId::for_record(&name, i as u64 + 1, &payload);
        head = chain::chain_step(head, &id);
        count += 1;
    }

    if head == sidecar.head && count == sidecar.record_count {
        return Ok(Verdict::Verified);
    }
    // Re-walk to find the first divergence. A second pass costs one more read
    // and turns "this file is bad" into "record N is where it stopped
    // matching" for the cases where that is knowable — see `first_divergence`.
    let at_record = first_divergence(path, &sidecar)?;
    Ok(Verdict::Diverged {
        at_record,
        expected: sidecar.head,
        actual: head,
    })
}

/// The 1-based index of the first record the sidecar claims that the segment
/// does not have.
///
/// This locates a **truncation only**. A chain gives no per-record
/// checkpoint, so a mid-file CONTENT change (a record replaced in place,
/// record count unchanged) changes every head from that point on but leaves
/// nothing here that identifies which record moved — the honest answer in
/// that case is `Ok(None)`, not a guessed index.
fn first_divergence(path: &Path, sidecar: &Sidecar) -> Result<Option<u64>, AttestError> {
    let reader = weir_wab::SegmentReader::open(path)?;
    let mut seen: u64 = 0;
    for record in reader {
        record?;
        seen += 1;
    }
    if seen < sidecar.record_count {
        return Ok(Some(seen + 1));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weir_wab::format::{Compression, SENTINEL, build_segment_footer, build_segment_header};

    /// Chaining the same segment twice must give the same head, and a segment
    /// with one byte of payload changed must give a different one. Together
    /// these are the whole point of the crate.
    #[test]
    fn the_same_segment_chains_to_the_same_head_and_a_changed_one_does_not() {
        let dir = crate::verify::tests::scratch("chain_stability");
        let a = write_segment(
            &dir.join("shard_00").join("seg_a.wab.sealed"),
            &[b"one" as &[u8], b"two", b"three"],
        );
        let b = write_segment(
            &dir.join("shard_00").join("seg_b.wab.sealed"),
            &[b"one" as &[u8], b"two", b"three"],
        );
        let c = write_segment(
            &dir.join("shard_00").join("seg_c.wab.sealed"),
            &[b"one" as &[u8], b"XXX", b"three"],
        );

        let ha = chain_segment(&a, ChainHead::ORIGIN).unwrap();
        let ha2 = chain_segment(&a, ChainHead::ORIGIN).unwrap();
        assert_eq!(ha.head, ha2.head, "chaining must be deterministic");

        // A fourth property, cheap to add and not implied by the three above:
        // the *fields* of the returned `Sidecar` are correct, not just its
        // head. In particular `segment_name` must carry the shard-qualified
        // form `shard_00/seg_a.wab.sealed`, not just the bare file name — a
        // `segment_name_for` that dropped the shard prefix would still pass
        // every head-comparison assertion above (two files in the same shard
        // directory would still get different, deterministic, payload-
        // sensitive names), so nothing else in this test would catch it.
        assert_eq!(ha.segment_name, "shard_00/seg_a.wab.sealed");
        assert_eq!(ha.record_count, 3, "three records were written");
        assert_eq!(ha.shard_id, 0, "shard_id must come from the segment header");
        assert_eq!(ha.prev_head, ChainHead::ORIGIN);

        let hb = chain_segment(&b, ChainHead::ORIGIN).unwrap();
        assert_ne!(
            ha.head, hb.head,
            "the segment NAME is part of H0, so identical records in differently \
             named segments must not collide"
        );

        let hc = chain_segment(&c, ChainHead::ORIGIN).unwrap();
        assert_ne!(ha.head, hc.head, "a changed payload must change the head");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn verification_confirms_an_untampered_segment() {
        let dir = scratch("verified");
        let seg = write_segment(
            &dir.join("seg_a.wab.sealed"),
            &[b"one" as &[u8], b"two", b"three"],
        );
        let side = chain_segment(&seg, ChainHead::ORIGIN).unwrap();
        std::fs::write(sidecar_path(&seg), side.encode()).unwrap();

        assert!(matches!(verify_segment(&seg).unwrap(), Verdict::Verified));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_segment_with_no_sidecar_is_reported_as_such() {
        let dir = scratch("missing_sidecar");
        let seg = write_segment(&dir.join("seg_a.wab.sealed"), &[b"one" as &[u8], b"two"]);
        assert!(matches!(
            verify_segment(&seg).unwrap(),
            Verdict::MissingSidecar
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A mid-file CONTENT change: the second record is replaced but the record
    /// count is unchanged. The chain has no per-record checkpoints, so this can
    /// be detected but not localized to a specific record — see the ruling on
    /// `Verdict::Diverged::at_record` in `verify.rs`.
    #[test]
    fn a_content_change_diverges_without_naming_a_record() {
        let dir = scratch("divergence_content");
        let seg = write_segment(
            &dir.join("seg_a.wab.sealed"),
            &[b"one" as &[u8], b"two", b"three"],
        );
        let side = chain_segment(&seg, ChainHead::ORIGIN).unwrap();
        std::fs::write(sidecar_path(&seg), side.encode()).unwrap();

        assert!(matches!(verify_segment(&seg).unwrap(), Verdict::Verified));

        // Rewrite the segment with the SECOND record changed, then copy it over
        // the original — same name (so the same sidecar applies), same record
        // count, different content. This is the substitution attack the chain
        // exists to catch.
        let tampered = write_segment(
            &dir.join("seg_a2.wab.sealed"),
            &[b"one" as &[u8], b"XXX", b"three"],
        );
        std::fs::copy(&tampered, &seg).unwrap();

        match verify_segment(&seg).unwrap() {
            Verdict::Diverged {
                at_record,
                expected,
                actual,
            } => {
                assert_eq!(expected, side.head, "expected head is the sidecar's");
                assert_ne!(
                    actual, side.head,
                    "the recomputed head must reflect the tampered content"
                );
                assert_eq!(
                    at_record, None,
                    "a mid-file content change with an unchanged record count \
                     leaves no per-record checkpoint to localize — reporting a \
                     number here would be inventing precision the data does not \
                     support"
                );
            }
            other => panic!("expected divergence, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A truncation: the segment is overwritten in place (same name) with fewer
    /// records than the sidecar claims. Unlike a content change, this has an
    /// exact answer — the first record the sidecar claims and the file does not
    /// have — because the record that's missing is *identified by its absence*,
    /// not by a chain comparison.
    #[test]
    fn a_truncated_segment_names_the_first_missing_record() {
        let dir = scratch("divergence_truncation");
        let seg = write_segment(
            &dir.join("seg_a.wab.sealed"),
            &[b"one" as &[u8], b"two", b"three"],
        );
        let side = chain_segment(&seg, ChainHead::ORIGIN).unwrap();
        std::fs::write(sidecar_path(&seg), side.encode()).unwrap();

        // Overwrite the same path with only the first two records.
        write_segment(&seg, &[b"one" as &[u8], b"two"]);

        match verify_segment(&seg).unwrap() {
            Verdict::Diverged { at_record, .. } => assert_eq!(
                at_record,
                Some(3),
                "the operator needs the index, not a boolean — a 256 MiB file \
                 and 'this is bad' is not a next step"
            ),
            other => panic!("expected divergence, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Build a real sealed segment through weir-wab's public builders.
    fn write_segment(path: &std::path::Path, records: &[&[u8]]) -> std::path::PathBuf {
        use std::io::Write as _;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(0, Compression::None));
        let mut data_bytes = 0u64;
        for r in records {
            bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
            bytes.extend_from_slice(r);
            data_bytes += r.len() as u64;
        }
        let file_crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&SENTINEL);
        bytes.extend_from_slice(&build_segment_footer(
            records.len() as u64,
            data_bytes,
            file_crc,
            1,
        ));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&bytes).unwrap();
        f.sync_all().unwrap();
        path.to_path_buf()
    }

    /// A scratch directory whose mode does not depend on the process umask.
    /// weir-server's testutil explains why at length; the short version is that a
    /// directory created while another test holds umask 0o177 has no execute bit.
    pub(super) fn scratch(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weir_attest_{label}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        dir
    }
}
