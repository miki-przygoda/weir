mod fixtures;
use weir_console::wab; // see Step 3 note on exposing the lib
use tempfile::tempdir;

#[test]
fn inventory_reports_state_meta_and_integrity() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let inv = wab::inventory(&root).unwrap();

    // 1 clean-sealed + 1 active + 1 footer-CRC-corrupt + 1 truncated + 1 record-CRC-corrupt
    // = 5 segments; the confirmed sidecar is metadata, not counted as a segment.
    assert_eq!(inv.totals.segments, 5);
    assert_eq!(inv.totals.active, 1);
    assert_eq!(inv.totals.dead_letter, 1);

    let sealed_ok = inv.segments.iter().find(|s| s.file.ends_with("seg_00000001.wab.sealed")).unwrap();
    assert_eq!(sealed_ok.state, "sealed");
    assert_eq!(sealed_ok.footer.as_ref().unwrap().record_count, 2);
    assert!(sealed_ok.integrity.as_ref().unwrap().ok);
    assert!(sealed_ok.confirmed.is_some()); // sidecar present -> confirmed meta attached

    let corrupt = inv.segments.iter().find(|s| s.file.ends_with("seg_00000003.wab.sealed")).unwrap();
    let integ = corrupt.integrity.as_ref().unwrap();
    assert!(!integ.ok);
    assert_eq!(integ.kind.as_deref(), Some("CrcMismatch"));

    // The record-CRC-corrupt segment fails the per-record check first -> BadRecord.
    let rec_corrupt = inv.segments.iter().find(|s| s.file.ends_with("seg_00000005.wab.sealed")).unwrap();
    assert_eq!(rec_corrupt.integrity.as_ref().unwrap().kind.as_deref(), Some("BadRecord"));

    let active = inv.segments.iter().find(|s| s.file.ends_with("seg_00000002.wab")).unwrap();
    assert_eq!(active.state, "active");
    assert!(active.footer.is_none()); // unsealed: no footer/integrity
    assert!(active.header.is_some());
}

#[test]
fn records_reads_payloads_with_crc_and_termination() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());

    let ok = wab::records(&root, "shard_00/seg_00000001.wab.sealed", 0, 100).unwrap();
    assert_eq!(ok.records.len(), 2);
    assert!(ok.records[0].crc_ok && ok.records[1].crc_ok);
    assert_eq!(ok.records[0].utf8_preview.as_deref(), Some("alpha"));
    assert_eq!(ok.terminated_cleanly, Some(true)); // sentinel present

    // The CRC-corrupt segment yields an error row for the bad record.
    let bad = wab::records(&root, "shard_00/seg_00000005.wab.sealed", 0, 100).unwrap();
    assert!(bad.records.iter().any(|r| r.error.is_some()));

    // Path escape is rejected.
    assert!(matches!(wab::records(&root, "../etc/passwd", 0, 10), Err(wab::WabError::BadPath(_))));
}

#[test]
fn dead_letter_lists_payload_records() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let dl = wab::dead_letter(&root).unwrap();
    assert_eq!(dl.segments.len(), 1);
    assert_eq!(dl.segments[0].records.len(), 2);
    assert_eq!(dl.segments[0].records[0].utf8_preview.as_deref(), Some("rejected-1"));
}

#[test]
fn verify_returns_ok_and_structured_error() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    assert!(wab::verify(&root, "shard_00/seg_00000001.wab.sealed").unwrap().ok);
    let bad = wab::verify(&root, "shard_00/seg_00000003.wab.sealed").unwrap();
    assert!(!bad.ok && bad.kind.as_deref() == Some("CrcMismatch"));
}

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn http_segments_ok_and_bad_path_400() {
    let dir = tempdir().unwrap();
    let root = fixtures::build_fixtures(dir.path());
    let app = weir_console::server::router(root.clone());

    let resp = app.clone().oneshot(Request::get("/api/wab/segments").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.oneshot(Request::get("/api/wab/segment?path=../x&offset=0&limit=10").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
