//! Cross-crate integration tests for `weir_wab::SegmentReader`.
//!
//! The in-module unit tests cover the happy path + header rejection + CRC
//! mismatch via internal helpers; these exercise the *public* API across the
//! crate boundary and pin the behaviours an external consumer (weir-ctl's
//! `dl requeue`) depends on: clean N-record round-trip, stop-at-sentinel
//! (trailing bytes after the sentinel are ignored), a CRC mismatch surfacing at
//! the right record index, and a segment truncated mid-record yielding an error
//! rather than a silent short read.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use weir_wab::SegmentReader;
use weir_wab::format::{SENTINEL, build_segment_header, build_sentinel};

fn tmp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "weir_wab_it_{label}_{}_{}.wab",
        std::process::id(),
        label.len()
    ))
}

/// Writes header + `[len][crc][payload]`* + sentinel + `trailer` bytes.
fn write_segment(path: &Path, records: &[&[u8]], trailer: &[u8]) {
    let mut f = File::create(path).unwrap();
    f.write_all(&build_segment_header(0xFFFF)).unwrap();
    for r in records {
        f.write_all(&(r.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&crc32fast::hash(r).to_le_bytes()).unwrap();
        f.write_all(r).unwrap();
    }
    f.write_all(&build_sentinel()).unwrap();
    f.write_all(trailer).unwrap();
    f.sync_all().unwrap();
}

fn read_ok(path: &Path) -> Vec<Vec<u8>> {
    SegmentReader::open(path)
        .unwrap()
        .map(|r| r.unwrap().as_ref().to_vec())
        .collect()
}

#[test]
fn round_trip_n_records_across_crate_boundary() {
    let path = tmp_path("rt");
    write_segment(&path, &[b"one", b"two", b"three"], b"");
    assert_eq!(
        read_ok(&path),
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn stops_at_sentinel_ignoring_trailing_bytes() {
    // A footer (or any trailing junk) after the sentinel must be ignored — the
    // reader stops at the sentinel and never decodes the trailer as records.
    let path = tmp_path("sentinel");
    write_segment(&path, &[b"a", b"b"], b"GARBAGE-AFTER-SENTINEL-\x00\x01\x02");
    assert_eq!(read_ok(&path), vec![b"a".to_vec(), b"b".to_vec()]);
    std::fs::remove_file(&path).ok();
}

#[test]
fn crc_mismatch_surfaces_at_the_offending_record() {
    // Records 0 and 2 are valid; record 1 has a corrupted CRC. The reader must
    // yield Ok, Err(at index 1), then stop.
    let path = tmp_path("crc_idx");
    let mut f = File::create(&path).unwrap();
    f.write_all(&build_segment_header(0)).unwrap();
    // record 0 — valid
    let r0 = b"good0";
    f.write_all(&(r0.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&crc32fast::hash(r0).to_le_bytes()).unwrap();
    f.write_all(r0).unwrap();
    // record 1 — wrong CRC
    let r1 = b"bad1";
    f.write_all(&(r1.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&0u32.to_le_bytes()).unwrap();
    f.write_all(r1).unwrap();
    f.write_all(&SENTINEL).unwrap();
    f.sync_all().unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.next().unwrap().unwrap().as_ref(), b"good0");
    let err = reader.next().unwrap().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(reader.next().is_none(), "iteration ends after the error");
    std::fs::remove_file(&path).ok();
}

#[test]
fn truncated_mid_record_is_an_error_not_a_silent_short_read() {
    // Header + a record length/crc claiming 100 payload bytes, but only 3 written
    // and no sentinel: the reader must error (UnexpectedEof), never silently
    // return a short/empty record.
    let path = tmp_path("trunc");
    let mut f = File::create(&path).unwrap();
    f.write_all(&build_segment_header(0)).unwrap();
    f.write_all(&100u32.to_le_bytes()).unwrap();
    f.write_all(&crc32fast::hash(b"xyz").to_le_bytes()).unwrap();
    f.write_all(b"xyz").unwrap(); // only 3 of the claimed 100 bytes
    f.sync_all().unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let err = reader.next().unwrap().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    std::fs::remove_file(&path).ok();
}

#[test]
fn truncated_header_is_rejected_at_open() {
    // A file shorter than the fixed header can't be opened as a segment. The
    // bare read_exact EOF is wrapped into an InvalidData error with context.
    let path = tmp_path("shorthdr");
    File::create(&path).unwrap().write_all(b"WEI").unwrap();
    let err = SegmentReader::open(&path).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(msg.contains("too short"), "expected 'too short' in: {msg}");
    assert!(msg.contains("header"), "expected 'header' in: {msg}");
    std::fs::remove_file(&path).ok();
}

#[test]
fn empty_file_is_rejected_at_open_with_context() {
    // A 0-byte file likewise can't carry a header.
    let path = tmp_path("emptyhdr");
    File::create(&path).unwrap();
    let err = SegmentReader::open(&path).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(msg.contains("too short"), "expected 'too short' in: {msg}");
    assert!(msg.contains("header"), "expected 'header' in: {msg}");
    std::fs::remove_file(&path).ok();
}

#[test]
fn segment_reader_is_a_fused_iterator() {
    // Generic bound: this only compiles if SegmentReader: FusedIterator.
    fn assert_fused<I: std::iter::FusedIterator>(_it: &I) {}

    let path = tmp_path("fused");
    write_segment(&path, &[b"only"], b"");
    let mut reader = SegmentReader::open(&path).unwrap();
    assert_fused(&reader);

    // Drain, then confirm it stays None across repeated calls past the end.
    assert_eq!(reader.next().unwrap().unwrap().as_ref(), b"only");
    assert!(reader.next().is_none(), "sentinel ends iteration");
    assert!(reader.next().is_none(), "stays None (1)");
    assert!(reader.next().is_none(), "stays None (2)");
    std::fs::remove_file(&path).ok();
}

// ---- Forensics read surface (sweep #6), exercised across the crate boundary ----

use weir_wab::format::build_segment_footer;
use weir_wab::{SegmentState, SegmentVerifyError, list_segment_files, verify_sealed_segment};

/// Writes a fully sealed segment (header + records + sentinel + footer with a
/// correct whole-file CRC). Returns `(path, sentinel_offset, first_payload_off)`.
fn write_sealed_segment(label: &str, shard: u16, records: &[&[u8]]) -> (PathBuf, usize, usize) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&build_segment_header(shard));
    let mut data_bytes = 0u64;
    let mut first_payload_off = 0usize;
    for (i, r) in records.iter().enumerate() {
        bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
        if i == 0 {
            first_payload_off = bytes.len();
        }
        bytes.extend_from_slice(r);
        data_bytes += r.len() as u64;
    }
    let sentinel_off = bytes.len();
    let file_crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&SENTINEL);
    bytes.extend_from_slice(&build_segment_footer(
        records.len() as u64,
        data_bytes,
        file_crc,
        1,
    ));

    let path = tmp_path(label);
    let mut f = File::create(&path).unwrap();
    f.write_all(&bytes).unwrap();
    f.sync_all().unwrap();
    (path, sentinel_off, first_payload_off)
}

#[test]
fn verify_sealed_segment_round_trips_metas() {
    let (path, _, _) = write_sealed_segment("verify_ok", 9, &[b"one", b"two"]);
    let v = verify_sealed_segment(&path).unwrap();
    assert_eq!(v.header.shard_id, 9);
    assert_eq!(v.footer.record_count, 2);
    assert_eq!(v.footer.data_bytes, (3 + 3) as u64);
    std::fs::remove_file(&path).ok();
}

#[test]
fn verify_sealed_segment_flipped_payload_is_rejected() {
    let (path, _, first_payload_off) = write_sealed_segment("verify_flip", 1, &[b"payload"]);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[first_payload_off] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    match verify_sealed_segment(&path) {
        Err(SegmentVerifyError::BadRecord(_)) => {}
        other => panic!("expected BadRecord, got {other:?}"),
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn verify_sealed_segment_truncated_before_footer_errors() {
    let (path, sentinel_off, _) = write_sealed_segment("verify_trunc", 0, &[b"data"]);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..sentinel_off + SENTINEL.len()]).unwrap();
    match verify_sealed_segment(&path) {
        Err(SegmentVerifyError::MissingFooter) => {}
        other => panic!("expected MissingFooter, got {other:?}"),
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn list_segment_files_classifies_by_extension() {
    let dir = std::env::temp_dir().join(format!("weir_wab_it_list_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for name in [
        "seg_00000005.wab",
        "seg_00000003.wab.sealed",
        "seg_00000004.wab.confirmed",
        "README.md",
    ] {
        File::create(dir.join(name)).unwrap();
    }
    let listed = list_segment_files(&dir).unwrap();
    assert_eq!(listed.len(), 3, "README.md must be ignored");
    // Sorted by path: ...003.sealed < ...004.confirmed < ...005.wab.
    assert_eq!(listed[0].1, SegmentState::Sealed);
    assert_eq!(listed[1].1, SegmentState::Confirmed);
    assert_eq!(listed[2].1, SegmentState::Active);
    std::fs::remove_dir_all(&dir).ok();
}
