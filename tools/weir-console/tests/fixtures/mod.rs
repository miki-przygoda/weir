//! Builds a deterministic fixtures wab dir using weir-wab's public byte builders,
//! so the wab-core integration tests have known good/corrupt/edge inputs.
use std::fs;
use std::path::{Path, PathBuf};
use weir_wab::format::{build_confirmed, build_segment_footer, build_segment_header, build_sentinel, crc32};

/// One on-disk record: payload_len(LE u32) + crc32(payload)(LE u32) + payload.
fn record_bytes(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + payload.len());
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    v.extend_from_slice(&crc32(payload).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

/// Header + records + (if `sealed`) sentinel + footer. Returns the raw bytes.
pub fn segment_bytes(shard_id: u16, records: &[&[u8]], sealed: bool) -> Vec<u8> {
    let mut buf = build_segment_header(shard_id).to_vec();
    let mut data_bytes: u64 = 0;
    for r in records { buf.extend_from_slice(&record_bytes(r)); data_bytes += r.len() as u64; }
    if sealed {
        let file_crc = crc32(&buf); // CRC over all bytes before the sentinel
        buf.extend_from_slice(&build_sentinel());
        buf.extend_from_slice(&build_segment_footer(records.len() as u64, data_bytes, file_crc, 1_700_000_000_000_000_000));
    }
    buf
}

/// Writes a complete fixtures wab dir under `root` and returns `root`.
/// Layout: shard_00/{sealed, active, confirmed sidecar, crc-corrupt sealed, truncated sealed}
/// and dead_letter/ with one sealed dead-letter segment.
pub fn build_fixtures(root: &Path) -> PathBuf {
    let shard = root.join("shard_00");
    fs::create_dir_all(&shard).unwrap();

    // A clean sealed segment: seg_00000001.wab.sealed (2 records)
    fs::write(shard.join("seg_00000001.wab.sealed"),
        segment_bytes(0, &[b"alpha", b"beta"], true)).unwrap();

    // An active (unsealed) segment: seg_00000002.wab (1 record, no sentinel/footer)
    fs::write(shard.join("seg_00000002.wab"),
        segment_bytes(0, &[b"gamma"], false)).unwrap();

    // A confirmed sidecar for segment 1: seg_00000001.wab.confirmed
    fs::write(shard.join("seg_00000001.wab.confirmed"),
        build_confirmed(1_700_000_000_000_000_000, 2, 1_700_000_001_000_000_000)).unwrap();

    // A CRC-corrupt sealed segment: flip a byte of the stored footer `file_crc32`
    // (footer bytes [16..20]) so the recomputed file CRC no longer matches.
    // NB: corrupting a *payload* byte instead would trip the per-record CRC check,
    // which `verify_sealed_segment` performs *before* the file-level CRC, surfacing
    // as `BadRecord` rather than `CrcMismatch`. Corrupting the stored file_crc32
    // leaves every record CRC valid and isolates the file-level `CrcMismatch` path.
    let mut corrupt = segment_bytes(0, &[b"corruptme"], true);
    let footer_crc_off = corrupt.len() - weir_wab::format::SEGMENT_FOOTER_LEN + 16;
    corrupt[footer_crc_off] ^= 0xff;
    fs::write(shard.join("seg_00000003.wab.sealed"), corrupt).unwrap();

    // A truncated sealed segment: drop the trailing footer bytes -> MissingFooter/NoSentinel
    let mut truncated = segment_bytes(0, &[b"shortlived"], true);
    truncated.truncate(truncated.len() - 10);
    fs::write(shard.join("seg_00000004.wab.sealed"), truncated).unwrap();

    // A record-CRC-corrupt sealed segment: flip a PAYLOAD byte after building, so the
    // stored per-record CRC no longer matches. verify_sealed_segment checks the
    // per-record CRC *before* the file CRC, so this surfaces as `BadRecord`, and
    // reading the records yields an error row at that record — distinct from seg_3's
    // file-level `CrcMismatch`.
    let mut rec_corrupt = segment_bytes(0, &[b"badrecord"], true);
    rec_corrupt[weir_wab::format::SEGMENT_HEADER_LEN + 8] ^= 0xff; // first payload byte
    fs::write(shard.join("seg_00000005.wab.sealed"), rec_corrupt).unwrap();

    // Dead-letter store: dead_letter/dl_00000001.wab.sealed (2 rejected payloads)
    let dl = root.join("dead_letter");
    fs::create_dir_all(&dl).unwrap();
    fs::write(dl.join("dl_00000001.wab.sealed"),
        segment_bytes(0, &[b"rejected-1", b"rejected-2"], true)).unwrap();

    root.to_path_buf()
}
