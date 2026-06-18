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
    // A file shorter than the fixed header can't be opened as a segment.
    let path = tmp_path("shorthdr");
    File::create(&path).unwrap().write_all(b"WEI").unwrap();
    let err = SegmentReader::open(&path).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    std::fs::remove_file(&path).ok();
}
