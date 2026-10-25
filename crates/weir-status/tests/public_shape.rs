//! The public document's keys are a contract (spec §9.1, additive-only):
//! weir-ctl parses them, docs_drift guards them, dashboards scrape them.

use serde_json::Value;
use std::collections::BTreeSet;
use weir_status::{AdminStatusDoc, public};

const GOLDEN_ADMIN: &str = include_str!("golden/status_v1_admin.json");
const GOLDEN_PUBLIC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/status_v1_public.json"
);

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().expect("an object").keys().cloned().collect()
}
fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}
fn public_value() -> Value {
    let admin: AdminStatusDoc = serde_json::from_str(GOLDEN_ADMIN).unwrap();
    serde_json::to_value(public(&admin)).unwrap()
}

#[test]
fn public_top_level_keys_are_exactly_the_spec_list() {
    assert_eq!(
        keys(&public_value()),
        set(&[
            "schema",
            "view",
            "version",
            "wire_version",
            "generated_at_ms",
            "started_at_ms",
            "uptime_secs",
            "verdict",
            "conditions",
            "unevaluated",
            "startup",
            "lifecycle",
            "ingest",
            "shards",
            "drain",
            "sink",
            "backlog",
            "reload",
            "admin",
            "config",
        ])
    );
}

#[test]
fn admin_only_keys_are_absent_from_every_public_section() {
    let v = public_value();
    assert_eq!(
        keys(&v["verdict"]),
        set(&[
            "ready",
            "live",
            "startup_complete",
            "livez_checks",
            "readiness_failing",
            "liveness_failing"
        ])
    );
    assert_eq!(
        keys(&v["conditions"][0]),
        set(&[
            "code", "severity", "affects", "since_ms", "summary", "evidence", "runbook", "gates"
        ])
    );
    assert_eq!(
        keys(&v["drain"]),
        set(&[
            "state",
            "state_since_ms",
            "paused",
            "pause_requested",
            "held",
            "paused_since_ms",
            "generation",
            "observed_requeue",
            "in_flight",
            "pending_segments",
            "backlog_segments",
            "channel_depth",
            "channel_capacity",
            "heartbeat_age_secs",
            "blocked_since_ms",
            "last_requeue",
            "exit",
        ])
    );
    assert_eq!(
        keys(&v["drain"]["last_requeue"]),
        set(&[
            "ticket",
            "requeued",
            "already_queued",
            "backoff_cut",
            "coalesced",
            "scan_failed",
            "at_ms"
        ])
    );
    assert_eq!(
        keys(&v["sink"]),
        set(&[
            "type",
            "health",
            "health_since_ms",
            "last_success_ms",
            "last_failure",
            "consecutive_failures"
        ])
    );
    assert_eq!(keys(&v["sink"]["last_failure"]), set(&["at_ms", "kind"]));
    assert_eq!(
        keys(&v["reload"]["last"]["items"][0]),
        set(&["item", "outcome"])
    );
    assert_eq!(
        keys(&v["admin"]),
        set(&["protocol", "verbs", "state", "reason_code"])
    );
}

#[test]
fn public_golden_matches() {
    let rendered = serde_json::to_string_pretty(&public_value()).unwrap() + "\n";
    if std::env::var_os("WEIR_BLESS").is_some() {
        std::fs::write(GOLDEN_PUBLIC_PATH, &rendered).unwrap();
    }
    let golden = std::fs::read_to_string(GOLDEN_PUBLIC_PATH).expect(
        "run once with WEIR_BLESS=1 to write tests/golden/status_v1_public.json, then review it",
    );
    assert_eq!(
        rendered, golden,
        "public /status rendering changed; additive-only (spec §9.1). Re-bless only for an additive change"
    );
}
