//! The public `weir.status/v1` rendering carries no free text (spec §9.1
//! redaction rules, §16 "Sentinel redaction test").
//!
//! The admin fixture is PR 1's golden admin document with a unique sentinel
//! planted in every admin-only field. The test first proves each sentinel
//! survived deserialization into `AdminStatusDoc` (serde ignores unknown keys,
//! so a mistyped pointer would otherwise make the test vacuous), then proves
//! none survives `public()`.

use serde_json::{Value, json};
use weir_status::{
    AdminStatusDoc, EvidenceValue, MAX_PUBLIC_CONDITIONS, MAX_PUBLIC_SHARDS, public,
};

const GOLDEN_ADMIN: &str = include_str!("golden/status_v1_admin.json");
const PID_SENTINEL: u64 = 3_141_592_653;
const UID_SENTINEL: u64 = 2_718_281_828;
/// Planted in memory, not through JSON: `EvidenceValue` is untagged with
/// `Code(String)` before `Text(String)`, so a plain JSON string always reads
/// back as `Code`, which the public document keeps.
const TEXT_EVIDENCE_SENTINEL: &str = "SENTINEL-evidence-text";

fn plant(doc: &mut Value, pointer: &str, value: Value) {
    let (parent, key) = pointer.rsplit_once('/').expect("pointer has a parent");
    let slot = doc.pointer_mut(parent).unwrap_or_else(|| {
        panic!("golden admin doc has no {parent}; PR 1's golden must populate every Option")
    });
    match slot {
        Value::Object(m) => {
            m.insert(key.to_string(), value);
        }
        Value::Array(a) => {
            let i: usize = key.parse().expect("array index");
            a[i] = value;
        }
        other => panic!("{parent} is {other}; PR 1's golden must populate it with an object"),
    }
}

/// `(pointer, planted value, the text that must not appear in public output)`.
fn sentinels() -> Vec<(&'static str, Value, String)> {
    let text = |s: &str| (Value::String(s.to_string()), s.to_string());
    let mut out = Vec::new();
    for (ptr, s) in [
        ("/instance/wab_dir", "/SENTINEL-wab-dir"),
        ("/instance/socket_path", "/SENTINEL-socket-path"),
        ("/conditions/0/detail", "SENTINEL-condition-detail"),
        ("/drain/last_panic", "SENTINEL-panic-payload"),
        ("/drain/last_requeue/scan_error", "SENTINEL-scan-error"),
        ("/sink/last_failure/message", "SENTINEL-sink-last-error"),
        ("/sink/probe_reason", "SENTINEL-probe-reason"),
        ("/reload/last/items/0/detail", "SENTINEL-reload-detail"),
        ("/admin/path", "/SENTINEL-admin-socket"),
    ] {
        let (v, needle) = text(s);
        out.push((ptr, v, needle));
    }
    out.push(("/pid", json!(PID_SENTINEL), PID_SENTINEL.to_string()));
    out.push((
        "/drain/paused_by_uid",
        json!(UID_SENTINEL),
        UID_SENTINEL.to_string(),
    ));
    out
}

fn sentinel_admin_doc() -> AdminStatusDoc {
    let mut v: Value = serde_json::from_str(GOLDEN_ADMIN).expect("golden admin doc parses");
    for (ptr, value, _) in sentinels() {
        plant(&mut v, ptr, value);
    }
    let mut doc: AdminStatusDoc =
        serde_json::from_value(v).expect("planted admin doc still deserializes");
    // Text evidence cannot be planted through JSON: an untagged string reads back as Code.
    doc.conditions[0].evidence.insert(
        "sentinel_text".into(),
        EvidenceValue::Text(TEXT_EVIDENCE_SENTINEL.into()),
    );
    doc
}

#[test]
fn every_sentinel_reaches_the_admin_doc() {
    let admin = serde_json::to_string(&sentinel_admin_doc()).unwrap();
    for (ptr, _, needle) in sentinels() {
        assert!(
            admin.contains(&needle),
            "sentinel planted at {ptr} did not survive deserialization into AdminStatusDoc; \n\
             the pointer no longer names a real field, so the redaction test below is vacuous for it"
        );
    }
    assert!(
        admin.contains(TEXT_EVIDENCE_SENTINEL),
        "the Text evidence sentinel is missing from the admin doc"
    );
}

#[test]
fn no_sentinel_reaches_the_public_doc() {
    let public_json = serde_json::to_string(&public(&sentinel_admin_doc())).unwrap();
    for (ptr, _, needle) in sentinels() {
        assert!(
            !public_json.contains(&needle),
            "admin-only value planted at {ptr} leaked into the public /status document"
        );
    }
    assert!(
        !public_json.contains(TEXT_EVIDENCE_SENTINEL),
        "Text evidence planted at /conditions/0/evidence/sentinel_text leaked into the public /status document"
    );
    assert!(!public_json.contains("SENTINEL"), "{public_json}");
}

/// Every string in the public document comes from a closed set: an enum
/// rendering, a code, a segment identity, the schema or version, or a
/// template-rendered `summary` (covered by the sentinel test). No absolute
/// path, no capital letters, no spaces.
fn assert_closed_strings(v: &Value, path: &str) {
    match v {
        Value::String(s) => {
            let key = path.rsplit('/').next().unwrap_or("");
            let ok = match key {
                "schema" => s == "weir.status/v1",
                "version" => s
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || ".-+".contains(c)),
                "summary" => true,
                "runbook" => s.starts_with("docs/monitoring.md#cond-"),
                _ => {
                    !s.is_empty()
                        && s.len() <= 96
                        && !s.starts_with('/')
                        && s.chars().all(|c| {
                            c.is_ascii_lowercase() || c.is_ascii_digit() || "_-./:".contains(c)
                        })
                }
            };
            assert!(
                ok,
                "public doc string at {path} is not from a closed set: {s:?}"
            );
        }
        Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                assert_closed_strings(x, &format!("{path}/{i}"));
            }
        }
        Value::Object(o) => {
            for (k, x) in o {
                assert!(
                    k.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                    "public doc key {path}/{k} is not snake_case"
                );
                assert_closed_strings(x, &format!("{path}/{k}"));
            }
        }
        _ => {}
    }
}

#[test]
fn public_doc_strings_are_closed() {
    for doc in [
        serde_json::from_str::<AdminStatusDoc>(GOLDEN_ADMIN).unwrap(),
        sentinel_admin_doc(),
    ] {
        let v = serde_json::to_value(public(&doc)).unwrap();
        assert_eq!(v["view"], "public");
        assert_closed_strings(&v, "");
    }
}

#[test]
fn public_doc_is_bounded() {
    let mut v: Value = serde_json::from_str(GOLDEN_ADMIN).unwrap();
    let shard = v["shards"][0].clone();
    v["shards"] = Value::Array(vec![shard; 300]);
    let cond = v["conditions"][0].clone();
    v["conditions"] = Value::Array(vec![cond; 50]);
    let doc: AdminStatusDoc = serde_json::from_value(v).unwrap();
    let p = public(&doc);
    assert_eq!(p.shards.len(), MAX_PUBLIC_SHARDS);
    assert_eq!(p.conditions.len(), MAX_PUBLIC_CONDITIONS);
}

#[test]
fn public_doc_round_trips() {
    let p = public(&sentinel_admin_doc());
    let back: weir_status::PublicStatusDoc =
        serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    assert_eq!(back, p);
}
