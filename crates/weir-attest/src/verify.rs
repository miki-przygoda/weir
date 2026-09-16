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
