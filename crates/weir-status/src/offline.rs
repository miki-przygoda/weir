//! Conditions only `weir-ctl status` emits, when no daemon answered and the
//! facts come from disk (spec §9.3, "Emitted only by weir-ctl offline").
//! Summaries take numbers only: no `&str` parameter, so no path or error text
//! can reach them (spec §9.1 redaction rules).

use std::borrow::Cow;
use std::collections::BTreeMap;

#[cfg(test)]
use crate::SinceLookup;
use crate::{Affects, Condition, ConditionCode, EvidenceValue, Severity};

/// PR 1's unit `SinceLookup`: offline evaluation has no condition latches.
pub use crate::NoLatches;

pub const RUNBOOK_DAEMON_UNREACHABLE: &str = "docs/monitoring.md#cond-daemon_unreachable";
pub const RUNBOOK_RECOVERY_PENDING: &str = "docs/monitoring.md#cond-recovery_pending";

/// No daemon answered on the admin socket, `/status` or `/metrics`.
pub fn daemon_unreachable(now_unix_ms: u64) -> Condition {
    Condition {
        code: ConditionCode::DaemonUnreachable,
        severity: Severity::Critical,
        affects: Affects::Ingest,
        since_unix_ms: None,
        summary: "no daemon answered on the admin socket, /status or /metrics; the facts shown \
                  are read from the wab_dir on disk"
            .to_string(),
        detail: None,
        evidence: BTreeMap::from([("checked_at_ms".to_string(), EvidenceValue::U64(now_unix_ms))]),
        runbook: Cow::Borrowed(RUNBOOK_DAEMON_UNREACHABLE),
        gates: Vec::new(),
    }
}

/// The wab_dir holds sealed-but-unconfirmed or active segments that the next
/// start's recovery will seal and replay. `None` when there is nothing.
pub fn recovery_pending(undelivered_segments: u64, active_segments: u64) -> Option<Condition> {
    if undelivered_segments == 0 && active_segments == 0 {
        return None;
    }
    Some(Condition {
        code: ConditionCode::RecoveryPending,
        severity: Severity::Info,
        affects: Affects::Durability, // PR 1's OFFLINE_ONLY catalogue entry
        since_unix_ms: None,
        summary: format!(
            "{undelivered_segments} sealed segment(s) await delivery and {active_segments} \
             active segment(s) will be sealed by recovery at the next start"
        ),
        detail: None,
        evidence: BTreeMap::from([
            (
                "undelivered_segments".to_string(),
                EvidenceValue::U64(undelivered_segments),
            ),
            (
                "active_segments".to_string(),
                EvidenceValue::U64(active_segments),
            ),
        ]),
        runbook: Cow::Borrowed(RUNBOOK_RECOVERY_PENDING),
        gates: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_unreachable_is_critical_ingest_with_its_anchor() {
        let c = daemon_unreachable(1_700_000_000_000);
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["code"], "daemon_unreachable");
        assert_eq!(v["severity"], "critical");
        assert_eq!(v["affects"], "ingest");
        assert_eq!(v["runbook"], RUNBOOK_DAEMON_UNREACHABLE);
        assert!(c.detail.is_none(), "offline conditions carry no free text");
        assert!(
            c.gates.is_empty(),
            "no /readyz exists to gate when the daemon is down"
        );
    }

    #[test]
    fn recovery_pending_only_when_disk_holds_work_for_recovery() {
        assert!(recovery_pending(0, 0).is_none());
        let c = recovery_pending(3, 1).expect("3 sealed + 1 active segment is pending recovery");
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["code"], "recovery_pending");
        assert_eq!(v["severity"], "info");
        assert_eq!(v["runbook"], RUNBOOK_RECOVERY_PENDING);
        assert!(
            c.summary.contains('3') && c.summary.contains('1'),
            "{}",
            c.summary
        );
    }

    #[test]
    fn runbook_anchors_follow_the_cond_code_convention() {
        for (code, anchor) in [
            ("daemon_unreachable", RUNBOOK_DAEMON_UNREACHABLE),
            ("recovery_pending", RUNBOOK_RECOVERY_PENDING),
        ] {
            assert_eq!(anchor, format!("docs/monitoring.md#cond-{code}"));
        }
    }

    #[test]
    fn no_latches_reports_no_first_seen_time() {
        assert_eq!(NoLatches.since(ConditionCode::DaemonUnreachable), None);
    }

    /// Severity, `affects`, gates and runbook come from PR 1's `OFFLINE_ONLY`
    /// catalogue entries, which the monitoring.md table (PR 8) is checked against.
    #[test]
    fn offline_conditions_match_the_catalogue() {
        let built = [daemon_unreachable(1), recovery_pending(1, 1).unwrap()];
        for c in built {
            let e = crate::entry(c.code).expect("catalogued");
            assert_eq!(
                (c.severity, c.affects),
                (e.severity, e.affects),
                "{:?}",
                c.code
            );
            assert_eq!(c.gates, e.gates.to_vec(), "{:?}", c.code);
            assert_eq!(c.runbook, e.runbook, "{:?}", c.code);
        }
    }
}
