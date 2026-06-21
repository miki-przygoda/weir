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
