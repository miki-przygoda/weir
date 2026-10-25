//! The admin rendering of `weir.status/v1` (spec §9.1). The golden file is the
//! contract weir-ctl and PR 5's public projection read; re-bless only on an
//! intended, additive change: `WEIR_BLESS=1 cargo test -p weir-status --test golden`.
use serde_json::{Value, json};
use std::path::Path;
use weir_status::testing::{FixedLatches, full_evidence};
use weir_status::*;

const NOW: u64 = 1_800_000_000_000;

fn doc() -> AdminStatusDoc {
    let e = full_evidence(NOW);
    let eval = evaluate(&e, &Thresholds::DEFAULT, &FixedLatches(NOW - 60_000));
    AdminStatusDoc::build(
        e.identity.as_ref().unwrap(),
        &e,
        &eval,
        &Thresholds::DEFAULT,
        &[LivezCheck::NoLiveShard],
    )
}

fn golden_check(rel: &str, actual: Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
    if std::env::var_os("WEIR_BLESS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        return;
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("run once with WEIR_BLESS=1 to create {}", path.display()));
    let expected: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        actual, expected,
        "{rel} drifted; if intended, re-bless with WEIR_BLESS=1 and review the diff"
    );
}

#[test]
fn admin_golden_matches() {
    golden_check(
        "tests/golden/status_v1_admin.json",
        serde_json::to_value(doc()).unwrap(),
    );
}

#[test]
fn admin_top_level_keys_are_the_spec_9_1_set() {
    let v = serde_json::to_value(doc()).unwrap();
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "admin",
            "backlog",
            "conditions",
            "config",
            "drain",
            "generated_at_ms",
            "ingest",
            "instance",
            "lifecycle",
            "pid",
            "reload",
            "schema",
            "shards",
            "sink",
            "started_at_ms",
            "startup",
            "unevaluated",
            "uptime_secs",
            "verdict",
            "version",
            "view",
            "wire_version",
        ]
    );
    assert_eq!(v["schema"], "weir.status/v1");
    assert_eq!(v["view"], "admin");
    assert_eq!(v["drain"]["channel_capacity"], 256);
}

#[test]
fn the_golden_populates_every_admin_only_field_pr5_plants_sentinels_into() {
    let v = serde_json::to_value(doc()).unwrap();
    for pointer in [
        "/instance/wab_dir",
        "/instance/socket_path",
        "/drain/paused_by_uid",
        "/drain/last_panic",
        "/drain/in_flight/segment",
        "/drain/last_requeue/scan_error",
        "/sink/last_failure/message",
        "/sink/probe_reason",
        "/reload/last/items/0/detail",
        "/admin/path",
        "/config/view/wab_dir",
        "/conditions/0/detail",
        "/shards/0/id",
    ] {
        let x = v
            .pointer(pointer)
            .unwrap_or_else(|| panic!("{pointer} missing"));
        assert!(
            !x.is_null(),
            "{pointer} is null; the fixture must populate it"
        );
    }
}

#[test]
fn verdict_and_conditions_come_from_the_shared_evaluator() {
    let d = doc();
    assert!(d.verdict.ready);
    assert!(d.verdict.startup_complete);
    assert_eq!(d.verdict.livez_checks, vec![LivezCheck::NoLiveShard]);
    let codes: Vec<ConditionCode> = d.conditions.iter().map(|c| c.code).collect();
    assert_eq!(
        codes,
        vec![
            ConditionCode::DrainPaused,
            ConditionCode::SinkFailing,
            ConditionCode::TlsReloadFailed
        ]
    );
    assert_eq!(d.conditions[0].since_unix_ms, Some(NOW - 60_000));
    assert!(d.unevaluated.is_empty());
}

#[test]
fn a_newer_daemons_document_still_parses() {
    let mut v = serde_json::to_value(doc()).unwrap();
    v["conditions"][0]["code"] = json!("brand_new_code");
    v["brand_new_section"] = json!({ "x": 1 });
    v["drain"]["brand_new_field"] = json!(true);
    let d: AdminStatusDoc = serde_json::from_value(v).unwrap();
    assert_eq!(d.conditions[0].code, ConditionCode::Unknown);
    let again = serde_json::to_value(&d).unwrap();
    assert_eq!(again["conditions"][0]["code"], "brand_new_code");
}

#[test]
fn the_document_is_bounded() {
    let mut e = full_evidence(NOW);
    let template = e.shards.as_ref().unwrap()[0].clone();
    e.shards = Some(
        (0..300u16)
            .map(|id| ShardEvidence {
                id,
                ..template.clone()
            })
            .collect(),
    );
    let eval = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
    let d = AdminStatusDoc::build(
        e.identity.as_ref().unwrap(),
        &e,
        &eval,
        &Thresholds::DEFAULT,
        &[],
    );
    assert_eq!(d.shards.len(), MAX_DOC_SHARDS);
    assert!(d.conditions.len() <= MAX_DOC_CONDITIONS);
}
