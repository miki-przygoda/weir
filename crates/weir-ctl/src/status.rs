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

/// `--source`: `auto` walks the spec §14 chain; any other value consults
/// that one source only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum SourceMode {
    Auto,
    Admin,
    Http,
    Metrics,
    Disk,
}

/// Which source the printed document came from (`--json` key `source`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Admin,
    Http,
    Metrics,
    MetricsAndDisk,
    Disk,
    None,
}

impl SourceKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            SourceKind::Admin => "admin",
            SourceKind::Http => "http",
            SourceKind::Metrics => "metrics",
            SourceKind::MetricsAndDisk => "metrics+disk",
            SourceKind::Disk => "disk",
            SourceKind::None => "none",
        }
    }
}

/// One entry of `sources_tried`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceAttempt {
    pub source: &'static str,
    pub ok: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceFail {
    /// Try the next source.
    Skip(String),
    /// Stop: the admin socket failed a client-side identity check. Falling
    /// back to `/status` would hide the impersonation the check exists to
    /// catch (README Q9).
    Identity(String),
}

/// The four spec §14 sources, behind a trait so every branch of the chain is
/// testable without a network.
pub(crate) trait StatusSources {
    /// The admin `status` result (`AdminStatusDoc`), identity already checked.
    fn admin(&self) -> Result<Value, SourceFail>;
    /// `GET /status` (`PublicStatusDoc`), JSON content type already checked.
    fn http_status(&self) -> Result<Value, SourceFail>;
    /// `GET /metrics` body containing weir series.
    fn metrics(&self) -> Result<String, SourceFail>;
    fn disk(&self) -> Result<DiskFacts, SourceFail>;
}

#[derive(Debug)]
pub(crate) struct StatusOutcome {
    /// The document as rendered, plus the `source` and `sources_tried` keys.
    pub doc: Value,
    pub source: SourceKind,
    pub tried: Vec<SourceAttempt>,
    pub exit: StatusExit,
}

/// The exit for a daemon-rendered document: its own `verdict.ready` (the same
/// `readiness` /readyz renders) plus any critical condition. A document that
/// is not `weir.status/v1` with a boolean `verdict.ready` is refused, so the
/// chain moves on instead of trusting it.
pub(crate) fn daemon_doc_exit(doc: &Value) -> Result<StatusExit, String> {
    if doc.get("schema").and_then(Value::as_str) != Some(weir_status::SCHEMA) {
        return Err(format!(
            "the answer is not a {} document",
            weir_status::SCHEMA
        ));
    }
    let ready = doc
        .pointer("/verdict/ready")
        .and_then(Value::as_bool)
        .ok_or_else(|| "the document has no verdict.ready".to_string())?;
    let any_critical = doc
        .get("conditions")
        .and_then(Value::as_array)
        .is_some_and(|cs| cs.iter().any(|c| c["severity"] == "critical"));
    Ok(StatusExit::from_parts(ready, any_critical))
}

fn ok_attempt(source: &'static str) -> SourceAttempt {
    SourceAttempt {
        source,
        ok: true,
        reason: None,
    }
}

fn failed(source: &'static str, why: String) -> SourceAttempt {
    SourceAttempt {
        source,
        ok: false,
        reason: Some(why),
    }
}

fn finish(
    mut doc: Value,
    source: SourceKind,
    tried: Vec<SourceAttempt>,
    exit: StatusExit,
) -> StatusOutcome {
    if let Some(o) = doc.as_object_mut() {
        o.insert("source".into(), json!(source.label()));
        o.insert(
            "sources_tried".into(),
            Value::Array(
                tried
                    .iter()
                    .map(|a| json!({"source": a.source, "ok": a.ok, "reason": a.reason}))
                    .collect(),
            ),
        );
    }
    StatusOutcome {
        doc,
        source,
        tried,
        exit,
    }
}

/// Spec §14: in auto mode the first success wins, in the order admin socket,
/// `GET /status`, `/metrics` plus disk, disk alone. A forced `mode` consults
/// only that source (`metrics` still reads the disk to fill the backlog). An
/// identity mismatch on the admin socket ends the chain with exit 2.
pub(crate) fn collect_status(
    src: &dyn StatusSources,
    mode: SourceMode,
    now_ms: u64,
) -> StatusOutcome {
    let want = |m: SourceMode| mode == SourceMode::Auto || mode == m;
    let mut tried = Vec::new();

    // A daemon-rendered document (admin, then http) is used as-is.
    for kind in [SourceKind::Admin, SourceKind::Http] {
        let (m, label) = match kind {
            SourceKind::Admin => (SourceMode::Admin, "admin"),
            _ => (SourceMode::Http, "http"),
        };
        if !want(m) {
            continue;
        }
        let fetched = match kind {
            SourceKind::Admin => src.admin(),
            _ => src.http_status(),
        };
        match fetched {
            Ok(doc) => match daemon_doc_exit(&doc) {
                Ok(exit) => {
                    tried.push(ok_attempt(label));
                    return finish(doc, kind, tried, exit);
                }
                Err(why) => tried.push(failed(label, why)),
            },
            Err(SourceFail::Identity(why)) => {
                tried.push(failed(label, format!("identity mismatch: {why}")));
                // Nothing from the refused socket: no instance, no conditions.
                let doc = json!({
                    "schema": weir_status::SCHEMA, "view": "offline", "generated_at_ms": now_ms,
                    "verdict": {"ready": false, "live": null, "startup_complete": null,
                                "readiness_failing": null},
                    "conditions": [], "unevaluated": [],
                    "identity_error": why,
                });
                return finish(doc, SourceKind::None, tried, StatusExit::Unreachable);
            }
            Err(SourceFail::Skip(why)) => tried.push(failed(label, why)),
        }
    }

    if want(SourceMode::Metrics) {
        match src.metrics() {
            Ok(body) => {
                tried.push(ok_attempt("metrics"));
                let disk = match src.disk() {
                    Ok(f) => {
                        tried.push(ok_attempt("disk"));
                        Some(f)
                    }
                    Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => {
                        tried.push(failed("disk", w));
                        None
                    }
                };
                let kind = if disk.is_some() {
                    SourceKind::MetricsAndDisk
                } else {
                    SourceKind::Metrics
                };
                let (doc, exit) = metrics_doc(&body, disk.as_ref(), now_ms);
                return finish(doc, kind, tried, exit);
            }
            Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => tried.push(failed("metrics", w)),
        }
    }

    if want(SourceMode::Disk) {
        match src.disk() {
            Ok(f) => {
                tried.push(ok_attempt("disk"));
                // README Q9: disk facts are printed, so the exit is evaluated
                // from them (not ready, 1), not "unreachable" (2).
                let (doc, exit) = offline_doc(&f, now_ms);
                return finish(doc, SourceKind::Disk, tried, exit);
            }
            Err(SourceFail::Skip(w) | SourceFail::Identity(w)) => tried.push(failed("disk", w)),
        }
    }

    // No daemon answered and there are no offline facts: the exit-2 case.
    finish(
        unreachable_doc(now_ms),
        SourceKind::None,
        tried,
        StatusExit::Unreachable,
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

    use std::cell::RefCell;

    struct Fake {
        admin: Result<serde_json::Value, SourceFail>,
        http: Result<serde_json::Value, SourceFail>,
        metrics: Result<String, SourceFail>,
        disk: Result<DiskFacts, SourceFail>,
        calls: RefCell<Vec<&'static str>>,
    }

    impl Fake {
        fn down() -> Fake {
            let skip = || SourceFail::Skip("down".into());
            Fake {
                admin: Err(skip()),
                http: Err(skip()),
                metrics: Err(skip()),
                disk: Err(skip()),
                calls: RefCell::new(Vec::new()),
            }
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls.borrow().clone()
        }
    }

    impl StatusSources for Fake {
        fn admin(&self) -> Result<serde_json::Value, SourceFail> {
            self.calls.borrow_mut().push("admin");
            self.admin.clone()
        }
        fn http_status(&self) -> Result<serde_json::Value, SourceFail> {
            self.calls.borrow_mut().push("http");
            self.http.clone()
        }
        fn metrics(&self) -> Result<String, SourceFail> {
            self.calls.borrow_mut().push("metrics");
            self.metrics.clone()
        }
        fn disk(&self) -> Result<DiskFacts, SourceFail> {
            self.calls.borrow_mut().push("disk");
            self.disk.clone()
        }
    }

    fn daemon_doc(view: &str, ready: bool, severity: &str) -> serde_json::Value {
        json!({"schema": weir_status::SCHEMA, "view": view,
               "verdict": {"ready": ready, "readiness_failing": if ready { json!([]) } else { json!(["starting"]) }},
               "conditions": [{"code": "noop_sink", "severity": severity, "summary": "s"}],
               "unevaluated": []})
    }

    fn tried(o: &StatusOutcome) -> Vec<&'static str> {
        o.tried.iter().map(|a| a.source).collect()
    }

    #[test]
    fn auto_prefers_the_admin_socket_and_adds_source_keys_only() {
        let f = Fake {
            admin: Ok(daemon_doc("admin", true, "warning")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Admin);
        assert_eq!(o.exit, StatusExit::Ready);
        assert_eq!(f.calls(), ["admin"]);
        let mut expected = keys(&daemon_doc("admin", true, "warning"));
        expected.extend(["source".to_string(), "sources_tried".to_string()]);
        expected.sort();
        assert_eq!(
            keys(&o.doc),
            expected,
            "--json is the document plus two keys"
        );
        assert_eq!(o.doc["source"], "admin");
        assert_eq!(
            o.doc["sources_tried"],
            json!([{"source": "admin", "ok": true, "reason": null}])
        );
    }

    #[test]
    fn admin_down_falls_through_to_http_with_the_reason_recorded() {
        let f = Fake {
            admin: Err(SourceFail::Skip(
                "no admin socket at /var/lib/weir/wab.admin.sock".into(),
            )),
            http: Ok(daemon_doc("public", false, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Http);
        assert_eq!(o.exit, StatusExit::NotReady);
        assert_eq!(tried(&o), ["admin", "http"]);
        assert!(
            o.tried[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("no admin socket")
        );
    }

    #[test]
    fn an_identity_mismatch_is_terminal_and_exits_2() {
        let f = Fake {
            admin: Err(SourceFail::Identity(
                "the daemon serves wab_dir (dev 1, ino 2)".into(),
            )),
            http: Ok(daemon_doc("public", true, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(
            f.calls(),
            ["admin"],
            "no fallback after a failed identity check"
        );
        assert_eq!(o.exit, StatusExit::Unreachable);
        assert_eq!(o.source, SourceKind::None);
        assert!(o.doc["identity_error"].as_str().unwrap().contains("ino 2"));
        assert!(
            o.tried[0]
                .reason
                .as_deref()
                .unwrap()
                .starts_with("identity mismatch")
        );
    }

    #[test]
    fn an_old_daemon_goes_to_metrics_plus_disk() {
        let f = Fake {
            http: Err(SourceFail::Skip(
                "/status is not JSON (application/openmetrics-text); a pre-4.1 daemon".into(),
            )),
            metrics: Ok("weir_records_accepted_total{tier=\"durable\"} 5\n".into()),
            disk: Ok(facts(2, 0)),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::MetricsAndDisk);
        assert_eq!(o.doc["source"], "metrics+disk");
        assert_eq!(tried(&o), ["admin", "http", "metrics", "disk"]);
        assert_eq!(o.doc["backlog"]["undelivered_segments"], 2);
    }

    #[test]
    fn disk_only_is_not_ready_with_facts() {
        // README Q9: disk facts printed, so the exit is evaluated from them (1).
        let f = Fake {
            disk: Ok(facts(1, 1)),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Disk);
        assert_eq!(o.exit, StatusExit::NotReady);
        assert_eq!(o.doc["view"], "offline");
        assert_eq!(tried(&o), ["admin", "http", "metrics", "disk"]);
    }

    #[test]
    fn nothing_at_all_is_unreachable() {
        let o = collect_status(&Fake::down(), SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::None);
        assert_eq!(o.exit, StatusExit::Unreachable);
        assert_eq!(o.doc["source"], "none");
        assert_eq!(o.doc["conditions"][0]["code"], "daemon_unreachable");
    }

    #[test]
    fn a_forced_source_consults_only_that_source() {
        let f = Fake {
            admin: Ok(daemon_doc("admin", true, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Http, NOW);
        assert_eq!(f.calls(), ["http"]);
        assert_eq!(o.exit, StatusExit::Unreachable);
        let f = Fake {
            disk: Ok(facts(0, 0)),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Disk, NOW);
        assert_eq!(f.calls(), ["disk"]);
        assert_eq!(o.source, SourceKind::Disk);
    }

    #[test]
    fn a_daemon_document_must_be_weir_status_v1_with_a_verdict() {
        assert_eq!(
            daemon_doc_exit(&daemon_doc("admin", true, "critical")),
            Ok(StatusExit::ReadyWithCritical)
        );
        assert_eq!(
            daemon_doc_exit(&daemon_doc("admin", false, "critical")),
            Ok(StatusExit::NotReady)
        );
        assert!(
            daemon_doc_exit(&json!({"schema": "weir.status/v2", "verdict": {"ready": true}}))
                .is_err()
        );
        assert!(daemon_doc_exit(&json!({"schema": weir_status::SCHEMA})).is_err());
        // A bad admin document falls through instead of being trusted.
        let f = Fake {
            admin: Ok(json!({"hello": 1})),
            http: Ok(daemon_doc("public", true, "info")),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Auto, NOW);
        assert_eq!(o.source, SourceKind::Http);
        assert!(
            o.tried[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("weir.status/v1")
        );
    }

    #[test]
    fn an_identity_mismatch_under_a_forced_admin_source_is_also_exit_2() {
        let f = Fake {
            admin: Err(SourceFail::Identity(
                "socket owned by uid 1001, not 1000".into(),
            )),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Admin, NOW);
        assert_eq!(f.calls(), ["admin"]);
        assert_eq!(o.exit, StatusExit::Unreachable);
        assert_eq!(o.doc["source"], "none");
        assert!(
            o.doc["identity_error"]
                .as_str()
                .unwrap()
                .contains("uid 1001")
        );
        // Nothing from the refused socket reaches the document.
        assert!(o.doc.get("instance").is_none());
        assert!(o.doc["conditions"].as_array().unwrap().is_empty());
    }

    #[test]
    fn source_mode_cli_values_and_kind_labels() {
        use clap::ValueEnum;
        let names: Vec<String> = SourceMode::value_variants()
            .iter()
            .map(|m| m.to_possible_value().unwrap().get_name().to_string())
            .collect();
        assert_eq!(names, ["auto", "admin", "http", "metrics", "disk"]);
        let labels: Vec<&str> = [
            SourceKind::Admin,
            SourceKind::Http,
            SourceKind::Metrics,
            SourceKind::MetricsAndDisk,
            SourceKind::Disk,
            SourceKind::None,
        ]
        .into_iter()
        .map(SourceKind::label)
        .collect();
        assert_eq!(
            labels,
            ["admin", "http", "metrics", "metrics+disk", "disk", "none"]
        );
    }

    #[test]
    fn metrics_without_disk_is_the_metrics_source() {
        let f = Fake {
            metrics: Ok("weir_records_accepted_total{tier=\"durable\"} 5\n".into()),
            ..Fake::down()
        };
        let o = collect_status(&f, SourceMode::Metrics, NOW);
        assert_eq!(f.calls(), ["metrics", "disk"]);
        assert_eq!(o.source, SourceKind::Metrics);
        assert_eq!(o.doc["source"], "metrics");
        assert_eq!(
            o.tried[1],
            SourceAttempt {
                source: "disk",
                ok: false,
                reason: Some("down".into())
            }
        );
    }
}
