//! `weir-ctl status` (spec §14): the daemon's own status document when a
//! daemon answers, otherwise the same evaluator run locally over /metrics and
//! disk. One exit-code table (weir_status::exit) for every source.
//!
//! Every `weir-status` call weir-ctl's status path makes sits in this file, so
//! a rename in that crate is a one-file fix here.

use serde_json::{Value, json};
use weir_status::exit::StatusExit;
use weir_status::offline::{NoLatches, daemon_unreachable, recovery_pending};
use weir_status::{BacklogEvidence, Evidence, Source, Thresholds, evaluate, readiness};

use crate::offline::DiskFacts;

/// `weir_condition_active{code,severity}` at 1, plus `weir_ready`. `None`
/// when the exposition has no condition series (a 4.0.x daemon) or no
/// `weir_ready`. The series are read by weir-status's
/// `active_conditions_from_metrics`, so weir-ctl has no second parser to
/// drift, and newer codes read as `unknown` (spec §17).
pub(crate) fn conditions_from_metrics(body: &str) -> Option<(bool, Vec<Value>)> {
    let active = weir_status::active_conditions_from_metrics(body)?;
    let ready = crate::get_metric(body, "weir_ready")? >= 1.0;
    let conds = active
        .into_iter()
        .map(|(code, severity)| {
            json!({
                "code": code, "severity": severity,
                "summary": "reported active by weir_condition_active (no detail over /metrics)",
                "runbook": format!("docs/monitoring.md#cond-{}", code.as_str()),
            })
        })
        .collect();
    Some((ready, conds))
}

/// Spec §14 source 3: `/metrics`, plus disk when it could be read. A 4.1
/// daemon's own verdicts (`weir_condition_active`, `weir_ready`) are used
/// as-is; an older exposition is turned into Evidence and evaluated locally
/// by the same `evaluate` / `readiness` the daemon runs.
pub(crate) fn metrics_doc(
    body: &str,
    disk: Option<&DiskFacts>,
    now_ms: u64,
) -> (Value, StatusExit) {
    let mut e = weir_status::exposition::evidence_from_metrics(body, now_ms);
    // Disk carries keys the scrape cannot (unreadable segments, last delivery,
    // dead-letter file count), so it renders the block when it answered; the
    // scrape's own backlog series are the fallback.
    let backlog = match disk {
        Some(f) => backlog_json(f, now_ms),
        None => e
            .backlog
            .as_ref()
            .map_or(Value::Null, evidence_backlog_json),
    };
    let shards = disk.map_or(Value::Null, |f| shards_json(f, now_ms));
    if let Some((ready, conditions)) = conditions_from_metrics(body) {
        let any_critical = conditions.iter().any(|c| c["severity"] == "critical");
        let doc = json!({
            "schema": weir_status::SCHEMA, "view": "metrics", "generated_at_ms": now_ms,
            "verdict": {"ready": ready, "live": null, "startup_complete": null, "readiness_failing": null},
            "conditions": conditions, "unevaluated": [],
            "backlog": backlog, "shards": shards,
        });
        return (doc, StatusExit::from_parts(ready, any_critical));
    }
    if let Some(f) = disk {
        e.source = Source::MetricsAndDisk;
        if e.backlog.is_none() {
            e.backlog = Some(disk_backlog_evidence(f, now_ms));
        }
    }
    let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
    let r = readiness(&e);
    let exit = StatusExit::from_verdict(r.ok, &ev.conditions);
    let doc = json!({
        "schema": weir_status::SCHEMA, "view": "metrics", "generated_at_ms": now_ms,
        "verdict": {"ready": r.ok, "live": null, "startup_complete": null, "readiness_failing": r.failing},
        "conditions": ev.conditions, "unevaluated": ev.unevaluated,
        "backlog": backlog, "shards": shards,
    });
    (doc, exit)
}

/// No daemon answered but the disk did. The exit comes from evaluating the
/// offline facts (spec §14, README Q9): `verdict.ready` is false
/// (`daemon_unreachable`), so it is `StatusExit::NotReady` (1), never 2.
pub(crate) fn offline_doc(f: &DiskFacts, now_ms: u64) -> (Value, StatusExit) {
    let mut e = Evidence::empty(now_ms, Source::Disk);
    e.backlog = Some(disk_backlog_evidence(f, now_ms));
    let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
    let mut conditions = vec![daemon_unreachable(now_ms)];
    conditions.extend(recovery_pending(
        f.undelivered_segments(),
        f.active_segments(),
    ));
    conditions.extend(ev.conditions);
    let exit = StatusExit::from_verdict(false, &conditions);
    let doc = json!({
        "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
        "wab_dir": f.wab_dir.display().to_string(),
        "verdict": {"ready": false, "live": null, "startup_complete": null,
                    "readiness_failing": ["daemon_unreachable"]},
        "conditions": conditions, "unevaluated": ev.unevaluated,
        "backlog": backlog_json(f, now_ms), "shards": shards_json(f, now_ms),
    });
    (doc, exit)
}

/// No daemon answered and there are no offline facts: the only exit-2
/// document (README Q9).
pub(crate) fn unreachable_doc(now_ms: u64) -> Value {
    json!({
        "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
        "verdict": {"ready": false, "live": null, "startup_complete": null,
                    "readiness_failing": ["daemon_unreachable"]},
        "conditions": [daemon_unreachable(now_ms)], "unevaluated": [],
    })
}

fn disk_backlog_evidence(f: &DiskFacts, now_ms: u64) -> BacklogEvidence {
    BacklogEvidence {
        undelivered_segments: f.undelivered_segments(),
        undelivered_records: f.undelivered_records(),
        oldest_undelivered_age_secs: f.oldest_undelivered_age_secs(now_ms),
        dead_letter_bytes: f.dead_letter_bytes,
        dead_letter_max_bytes: None, // offline does not know the config
        quarantine_bytes: f.quarantine_bytes,
        quarantine_files: f.quarantine_files,
        scanned_at_ms: f.scanned_at_ms,
    }
}

/// The spec §9.1 `backlog` keys, plus the additive `unreadable_segments`,
/// `last_delivery_ms` and `dead_letter_files` that only disk can supply.
fn backlog_json(f: &DiskFacts, now_ms: u64) -> Value {
    json!({
        "undelivered_segments": f.undelivered_segments(),
        "undelivered_records": f.undelivered_records(),
        "unreadable_segments": f.unreadable_segments(),
        "oldest_undelivered_age_secs": f.oldest_undelivered_age_secs(now_ms),
        "last_delivery_ms": f.last_delivery_ns().map(|n| (n.max(0) / 1_000_000) as u64),
        "dead_letter_bytes": f.dead_letter_bytes,
        "dead_letter_files": f.dead_letter_files,
        "dead_letter_max_bytes": null,
        "quarantine_bytes": f.quarantine_bytes,
        "quarantine_files": f.quarantine_files,
        "scanned_at_ms": f.scanned_at_ms,
    })
}

/// The spec §9.1 `backlog` keys as a 4.1 scrape reports them
/// (`weir_wab_undelivered_*`), when no disk facts are at hand.
fn evidence_backlog_json(b: &BacklogEvidence) -> Value {
    json!({
        "undelivered_segments": b.undelivered_segments,
        "undelivered_records": b.undelivered_records,
        "oldest_undelivered_age_secs": b.oldest_undelivered_age_secs,
        "dead_letter_bytes": b.dead_letter_bytes,
        "dead_letter_max_bytes": b.dead_letter_max_bytes,
        "quarantine_bytes": b.quarantine_bytes,
        "quarantine_files": b.quarantine_files,
        "scanned_at_ms": b.scanned_at_ms,
    })
}

fn shards_json(f: &DiskFacts, now_ms: u64) -> Value {
    let age = |ns: Option<i64>| {
        ns.map(|c| (((now_ms as i128) * 1_000_000 - c as i128).max(0) as f64) / 1e9)
    };
    Value::Array(
        f.shards
            .iter()
            .map(|s| {
                json!({
                    "shard": s.name, "id": s.id,
                    "undelivered_segments": s.undelivered_segments,
                    "undelivered_records": s.undelivered_records,
                    "unreadable_segments": s.unreadable_segments,
                    "active_segments": s.active_segments,
                    "active_segment_age_secs": age(s.oldest_active_created_ns),
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offline::{DiskFacts, ShardDisk};
    use serde_json::json;

    const NOW: u64 = 1_700_000_100_000;

    fn facts(undelivered: u64, active: u64) -> DiskFacts {
        DiskFacts {
            wab_dir: "/var/lib/weir/wab".into(),
            shards: vec![ShardDisk {
                name: "shard_00".into(),
                id: Some(0),
                undelivered_segments: undelivered,
                undelivered_records: undelivered * 10,
                oldest_undelivered_created_ns: (undelivered > 0)
                    .then_some(1_700_000_000_000_000_000),
                active_segments: active,
                ..ShardDisk::default()
            }],
            scanned_at_ms: NOW,
            ..DiskFacts::default()
        }
    }

    fn keys(v: &serde_json::Value) -> Vec<String> {
        let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    #[test]
    fn offline_doc_shape_and_lead_condition() {
        let (v, exit) = offline_doc(&facts(3, 1), NOW);
        // README Q9: disk facts printed, so the exit comes from evaluating
        // them: not ready (daemon_unreachable), exit 1, never 2.
        assert_eq!(exit, StatusExit::NotReady);
        assert_eq!(
            keys(&v),
            [
                "backlog",
                "conditions",
                "generated_at_ms",
                "schema",
                "shards",
                "unevaluated",
                "verdict",
                "view",
                "wab_dir"
            ]
        );
        assert_eq!(v["schema"], weir_status::SCHEMA);
        assert_eq!(v["view"], "offline");
        assert_eq!(v["verdict"]["ready"], false);
        assert_eq!(
            v["verdict"]["readiness_failing"],
            json!(["daemon_unreachable"])
        );
        assert_eq!(v["conditions"][0]["code"], "daemon_unreachable");
        assert_eq!(v["conditions"][1]["code"], "recovery_pending");
        assert_eq!(v["backlog"]["undelivered_segments"], 3);
        assert_eq!(v["backlog"]["undelivered_records"], 30);
        assert!(
            v["backlog"]["dead_letter_max_bytes"].is_null(),
            "offline does not know the cap"
        );
        assert_eq!(v["shards"][0]["shard"], "shard_00");
        // Nothing pending: no recovery_pending.
        let (empty, exit) = offline_doc(&facts(0, 0), NOW);
        assert_eq!(exit, StatusExit::NotReady);
        assert!(
            !empty["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["code"] == "recovery_pending")
        );
    }

    #[test]
    fn condition_series_on_metrics_are_read_with_weir_ready() {
        let body = "\
weir_ready 1
weir_condition_active{code=\"shard_offline\",severity=\"critical\"} 1
weir_condition_active{code=\"drain_paused\",severity=\"warning\"} 0
";
        let (ready, conds) = conditions_from_metrics(body).unwrap();
        assert!(ready);
        assert_eq!(conds.len(), 1, "only series at 1 are active");
        assert_eq!(conds[0]["code"], "shard_offline");
        assert_eq!(conds[0]["severity"], "critical");
        let (doc, exit) = metrics_doc(body, None, NOW);
        assert_eq!(exit, StatusExit::ReadyWithCritical);
        assert_eq!(doc["view"], "metrics");
        assert_eq!(
            keys(&doc),
            [
                "backlog",
                "conditions",
                "generated_at_ms",
                "schema",
                "shards",
                "unevaluated",
                "verdict",
                "view"
            ]
        );
        let (_, exit) = metrics_doc(&body.replace("weir_ready 1", "weir_ready 0"), None, NOW);
        assert_eq!(exit, StatusExit::NotReady);
        assert!(
            conditions_from_metrics("weir_queue_depth 1\n").is_none(),
            "4.0.x: no condition series"
        );
    }

    #[test]
    fn a_pre_4_1_exposition_is_evaluated_locally_with_parity() {
        // spec §14 parity, on the weir-ctl side: the exit agrees with
        // readiness() over the same Evidence.
        let bodies = [
            "weir_records_accepted_total{tier=\"durable\"} 5\nweir_sink_health{state=\"healthy\"} 1\nweir_sink_info{sink_type=\"http\"} 1\n",
            "weir_records_accepted_total{tier=\"durable\"} 5\nweir_sink_info{sink_type=\"noop\"} 1\nweir_wab_cap_rejections_total 3\n",
        ];
        for body in bodies {
            let e = weir_status::exposition::evidence_from_metrics(body, NOW);
            let ok = weir_status::readiness(&e).ok;
            let (doc, exit) = metrics_doc(body, None, NOW);
            assert_eq!(doc["verdict"]["ready"], ok, "{body}");
            assert_eq!(
                matches!(exit, StatusExit::Ready | StatusExit::ReadyWithCritical),
                ok,
                "{body}"
            );
        }
    }

    #[test]
    fn disk_fills_the_backlog_an_old_daemon_cannot_report() {
        let body = "weir_records_accepted_total{tier=\"durable\"} 5\n";
        let (doc, _) = metrics_doc(body, Some(&facts(2, 0)), NOW);
        assert_eq!(doc["backlog"]["undelivered_segments"], 2);
        let (doc, _) = metrics_doc(body, None, NOW);
        assert!(doc["backlog"].is_null());
    }

    #[test]
    fn unreachable_doc_without_any_facts() {
        let v = unreachable_doc(NOW);
        assert_eq!(v["view"], "offline");
        assert_eq!(v["conditions"][0]["code"], "daemon_unreachable");
        assert_eq!(v["verdict"]["ready"], false);
    }

    #[test]
    fn without_disk_a_4_1_scrape_s_own_backlog_series_fill_the_block() {
        let body = "weir_records_accepted_total{tier=\"durable\"} 5\n\
weir_wab_undelivered_segments 4\nweir_wab_undelivered_records 40\n";
        let (doc, _) = metrics_doc(body, None, NOW);
        assert_eq!(doc["backlog"]["undelivered_segments"], 4);
        assert_eq!(doc["backlog"]["undelivered_records"], 40);
        // Disk, when it answered, renders the block (it has keys a scrape lacks).
        let (doc, _) = metrics_doc(body, Some(&facts(2, 0)), NOW);
        assert_eq!(doc["backlog"]["undelivered_segments"], 2);
        assert!(doc["backlog"].get("unreadable_segments").is_some());
    }
}
