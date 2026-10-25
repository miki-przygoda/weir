//! Conditions and their evaluation result (spec §5.1).
use crate::codes::{Affects, ConditionCode, Gate, Severity};
use crate::segment::SegmentId;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// One evidence value attached to a condition. `Text` is admin-only free
/// text; the public rendering (PR 5) has no variant to put it in.
/// Untagged; on deserialise a plain string reads as `Segment` when it is a
/// segment id, else `Code` (a deserialised document is for display only).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EvidenceValue {
    U64(u64),
    F64(f64),
    Bool(bool),
    Segment(SegmentId),
    Shards(Vec<u16>),
    Code(String),
    Text(String),
}

/// One active condition (spec §5.1). Serialised through `RawCondition`, so
/// `since_unix_ms` is `since_ms` on the wire and an unknown code survives a
/// parse/serialise round trip verbatim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(from = "RawCondition", into = "RawCondition")]
pub struct Condition {
    pub code: ConditionCode,
    pub severity: Severity,
    pub affects: Affects,
    pub since_unix_ms: Option<u64>,
    /// PUBLIC: rendered from a typed template (summary.rs).
    pub summary: String,
    /// ADMIN ONLY: may carry free text (panic payload, uid, sink error).
    pub detail: Option<String>,
    pub evidence: BTreeMap<String, EvidenceValue>,
    pub runbook: Cow<'static, str>,
    pub gates: Vec<Gate>,
}

/// The wire shape: the code as a raw string, so an unknown code survives a
/// parse/serialise round trip unchanged (spec §5.1 rules).
#[derive(Serialize, Deserialize)]
struct RawCondition {
    code: String,
    severity: Severity,
    affects: Affects,
    since_ms: Option<u64>,
    summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(default)]
    evidence: BTreeMap<String, EvidenceValue>,
    runbook: String,
    #[serde(default)]
    gates: Vec<Gate>,
}

impl From<RawCondition> for Condition {
    fn from(r: RawCondition) -> Self {
        let code = ConditionCode::parse_lossy(&r.code);
        let mut evidence = r.evidence;
        if code == ConditionCode::Unknown {
            evidence.insert("code_raw".to_string(), EvidenceValue::Code(r.code));
        }
        Condition {
            code,
            severity: r.severity,
            affects: r.affects,
            since_unix_ms: r.since_ms,
            summary: r.summary,
            detail: r.detail,
            evidence,
            runbook: Cow::Owned(r.runbook),
            gates: r.gates,
        }
    }
}

impl From<Condition> for RawCondition {
    fn from(c: Condition) -> Self {
        let code = match (c.code, c.evidence.get("code_raw")) {
            (ConditionCode::Unknown, Some(EvidenceValue::Code(raw))) => raw.clone(),
            (code, _) => code.as_str().to_string(),
        };
        RawCondition {
            code,
            severity: c.severity,
            affects: c.affects,
            since_ms: c.since_unix_ms,
            summary: c.summary,
            detail: c.detail,
            evidence: c.evidence,
            runbook: c.runbook.into_owned(),
            gates: c.gates,
        }
    }
}

/// What the evaluator returns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Evaluation {
    /// Active conditions, in catalogue order.
    pub conditions: Vec<Condition>,
    /// Codes whose evidence was missing. Never shown as healthy.
    pub unevaluated: Vec<ConditionCode>,
}

impl Evaluation {
    /// Whether `code` is among the active conditions.
    pub fn is_active(&self, code: ConditionCode) -> bool {
        self.conditions.iter().any(|c| c.code == code)
    }

    /// The active condition for `code`, if any.
    pub fn get(&self, code: ConditionCode) -> Option<&Condition> {
        self.conditions.iter().find(|c| c.code == code)
    }
}

/// First-seen time of an active condition (the daemon's latches).
pub trait SinceLookup {
    fn since(&self, code: ConditionCode) -> Option<u64>;
}

/// No latches (offline evaluation): `since` is unknown.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLatches;

impl SinceLookup for NoLatches {
    fn since(&self, _code: ConditionCode) -> Option<u64> {
        None
    }
}

/// Condition thresholds. Constants, not knobs (owner decision, spec §9.3).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Thresholds {
    /// `drain_backpressure`: a shard blocked handing off for this long.
    pub handoff_blocked_ms: u64,
    /// `drain_backpressure`: drain channel depth at or above this ...
    pub channel_high_depth: u64,
    /// ... for this long.
    pub channel_high_ms: u64,
    /// `wab_scan_stale`: no successful scan for this long.
    pub wab_scan_stale_ms: u64,
    /// `drain_wedged`: grace over `commit_timeout` / `health_poll_interval`.
    pub wedge_grace_ms: u64,
    /// `delivery_stalled`: no successful commit for this long.
    pub delivery_stalled_ms: u64,
    /// `delivery_lagging`: oldest undelivered record older than this.
    pub delivery_lagging_secs: f64,
    /// `sink_failing`: this many consecutive commit failures.
    pub sink_failing_consecutive: u64,
    /// `dead_letter_near_cap`: this percentage of the cap.
    pub dead_letter_near_cap_percent: u64,
    /// `active_segment_aging`: an active segment older than this.
    pub active_segment_aging_secs: f64,
    /// `ack_timeouts_recent`'s inferred cause: coalesce EWMA above this.
    pub coalesce_slow_us: u64,
}

impl Thresholds {
    pub const DEFAULT: Thresholds = Thresholds {
        handoff_blocked_ms: 1_000,
        channel_high_depth: 230,
        channel_high_ms: 10_000,
        wab_scan_stale_ms: 30_000,
        wedge_grace_ms: 15_000,
        delivery_stalled_ms: 300_000,
        delivery_lagging_secs: 900.0,
        sink_failing_consecutive: 3,
        dead_letter_near_cap_percent: 80,
        active_segment_aging_secs: 3_600.0,
        coalesce_slow_us: 1_000_000,
    };
}

impl Default for Thresholds {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Windows the daemon applies when it computes `RecentCounters`.
pub const ACK_TIMEOUT_WINDOW_SECS: u64 = 300;
pub const FSYNC_WINDOW_SECS: u64 = 900;
pub const PANIC_WINDOW_SECS: u64 = 900;
/// `drain_stopped`'s admin detail carries at most this much panic text.
pub const DETAIL_PANIC_MAX_BYTES: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(code: ConditionCode) -> Condition {
        let mut evidence = BTreeMap::new();
        evidence.insert("shards".to_string(), EvidenceValue::Shards(vec![1, 2]));
        evidence.insert("bytes".to_string(), EvidenceValue::U64(5));
        Condition {
            code,
            severity: Severity::Critical,
            affects: Affects::Ingest,
            since_unix_ms: Some(10),
            summary: "s".into(),
            detail: Some("d".into()),
            evidence,
            runbook: Cow::Borrowed("docs/monitoring.md#cond-shard_offline"),
            gates: vec![],
        }
    }

    #[test]
    fn a_condition_serialises_since_as_since_ms() {
        let json = serde_json::to_value(sample(ConditionCode::ShardOffline)).unwrap();
        assert_eq!(json["since_ms"], 10);
        assert!(json.get("since_unix_ms").is_none());
        assert_eq!(json["code"], "shard_offline");
        assert_eq!(json["evidence"]["shards"], serde_json::json!([1, 2]));
    }

    #[test]
    fn a_known_condition_round_trips() {
        let c = sample(ConditionCode::ShardOffline);
        let back: Condition = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn an_unknown_code_is_kept_verbatim_with_its_wire_severity() {
        let json = r#"{"code":"brand_new_code","severity":"warning","affects":"delivery",
            "since_ms":null,"summary":"x","evidence":{},"runbook":"r","gates":[]}"#;
        let c: Condition = serde_json::from_str(json).unwrap();
        assert_eq!(c.code, ConditionCode::Unknown);
        assert_eq!(c.severity, Severity::Warning);
        assert_eq!(
            c.evidence.get("code_raw"),
            Some(&EvidenceValue::Code("brand_new_code".into()))
        );
        let again = serde_json::to_value(&c).unwrap();
        assert_eq!(
            again["code"], "brand_new_code",
            "re-emitted verbatim, never as \"unknown\""
        );
    }

    #[test]
    fn defaults_are_the_spec_constants() {
        let t = Thresholds::default();
        assert_eq!(t, Thresholds::DEFAULT);
        assert_eq!(t.channel_high_depth, 230);
        assert_eq!(t.wedge_grace_ms, 15_000);
        assert_eq!(t.delivery_lagging_secs, 900.0);
        assert_eq!(NoLatches.since(ConditionCode::Starting), None);
    }
}
