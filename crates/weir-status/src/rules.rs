//! One rule per catalogue code (spec §9.3), plus `evaluate`. A rule returns
//! `Unevaluated` when the evidence it needs is missing, never `Inactive`.
use crate::codes::{CATALOGUE, ConditionCode};
#[allow(unused_imports)] // DETAIL_PANIC_MAX_BYTES is first used by Task 1.5's drain_stopped
use crate::condition::{
    Condition, DETAIL_PANIC_MAX_BYTES, Evaluation, EvidenceValue, SinceLookup, Thresholds,
};
use crate::evidence::*;
use crate::summary;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Evaluates every catalogue code against `e` (spec §5.1). Active
/// conditions come back in catalogue order with their latch time.
pub fn evaluate(e: &Evidence, t: &Thresholds, latches: &dyn SinceLookup) -> Evaluation {
    let mut out = Evaluation::default();
    for entry in CATALOGUE {
        match rule(entry.code, e, t) {
            Outcome::Unevaluated => out.unevaluated.push(entry.code),
            Outcome::Inactive => {}
            Outcome::Active(f) => out.conditions.push(Condition {
                code: entry.code,
                severity: entry.severity,
                affects: entry.affects,
                since_unix_ms: latches.since(entry.code),
                summary: f.summary,
                detail: f.detail,
                evidence: f.evidence,
                runbook: Cow::Borrowed(entry.runbook),
                gates: entry.gates.to_vec(),
            }),
        }
    }
    out
}

pub(crate) struct Fired {
    pub(crate) summary: String,
    pub(crate) detail: Option<String>,
    pub(crate) evidence: BTreeMap<String, EvidenceValue>,
}

impl Fired {
    fn new(summary: String) -> Self {
        Fired {
            summary,
            detail: None,
            evidence: BTreeMap::new(),
        }
    }
    fn with(mut self, key: &'static str, v: EvidenceValue) -> Self {
        self.evidence.insert(key.to_string(), v);
        self
    }
    #[allow(dead_code)] // first used by Task 1.5's drain_stopped; Task 1.5 drops this allow
    fn detail(mut self, d: Option<String>) -> Self {
        self.detail = d;
        self
    }
}

pub(crate) enum Outcome {
    Unevaluated,
    Inactive,
    Active(Fired),
}

impl From<Fired> for Outcome {
    fn from(f: Fired) -> Self {
        Outcome::Active(f)
    }
}

/// Why producers saw ACK_TIMEOUT nacks (spec §9.3 code 9), as a code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AckTimeoutCause {
    DeliveryStallingIngest,
    StorageSlowOrFailing,
    FlusherNotCompleting,
}

impl AckTimeoutCause {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::DeliveryStallingIngest => "delivery_stalling_ingest",
            Self::StorageSlowOrFailing => "storage_slow_or_failing",
            Self::FlusherNotCompleting => "flusher_not_completing",
        }
    }
}

fn u(v: u64) -> EvidenceValue {
    EvidenceValue::U64(v)
}
fn code(s: &'static str) -> EvidenceValue {
    EvidenceValue::Code(s.to_string())
}
fn age_ms(now: u64, then: u64) -> u64 {
    now.saturating_sub(then)
}
fn tri_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

pub(crate) fn rule(c: ConditionCode, e: &Evidence, t: &Thresholds) -> Outcome {
    use ConditionCode as C;
    match c {
        C::Starting => starting(e),
        C::ShuttingDown => shutting_down(e),
        C::ListenerDown => listener_down(e),
        C::NoLiveShard => no_live_shard(e),
        C::ShardOffline => shard_offline(e),
        C::WorkerExited => worker_exited(e),
        C::WabCapRejecting => wab_cap_rejecting(e),
        C::DrainBackpressure => drain_backpressure(e, t),
        C::AckTimeoutsRecent => ack_timeouts_recent(e, t),
        C::WabScanStale => wab_scan_stale(e, t),
        C::WabGrowingUnbounded => wab_growing_unbounded(e),
        // Tasks 1.5 and 1.6 replace this arm with the remaining rules.
        _ => Outcome::Unevaluated,
    }
}

fn starting(e: &Evidence) -> Outcome {
    let Some(phase) = e.phase else {
        return Outcome::Unevaluated;
    };
    if !phase.before_serving() {
        return Outcome::Inactive;
    }
    let secs = e
        .phase_since_unix_ms
        .map_or(0, |s| age_ms(e.now_unix_ms, s) / 1000);
    Fired::new(summary::starting(phase, secs))
        .with("phase", code(phase.as_str()))
        .with("phase_secs", u(secs))
        .into()
}

fn shutting_down(e: &Evidence) -> Outcome {
    match e.shutting_down {
        None => Outcome::Unevaluated,
        Some(false) => Outcome::Inactive,
        Some(true) => {
            let left = e
                .drain_deadline_unix_ms
                .map(|d| d.saturating_sub(e.now_unix_ms) / 1000);
            let mut f = Fired::new(summary::shutting_down(left, e.drain_budget_secs))
                .with("forced", EvidenceValue::Bool(e.forced.unwrap_or(false)));
            if let Some(l) = left {
                f = f.with("budget_left_secs", u(l));
            }
            if let Some(b) = e.drain_budget_secs {
                f = f.with("budget_secs", u(b));
            }
            f.into()
        }
    }
}

fn listener_down(e: &Evidence) -> Outcome {
    let Some(listeners) = &e.listeners else {
        return Outcome::Unevaluated;
    };
    let mut unknown = false;
    for l in listeners {
        let down = match l.state {
            ListenerState::Failed => Some(true),
            ListenerState::Closed => e.shutting_down.map(|sd| !sd),
            ListenerState::NotConfigured | ListenerState::Pending | ListenerState::Bound => {
                Some(false)
            }
        };
        match down {
            Some(true) => {
                return Fired::new(summary::listener_down(l.kind))
                    .with("listener", code(l.kind.as_str()))
                    .with("state", code(l.state.as_str()))
                    .into();
            }
            None => unknown = true,
            Some(false) => {}
        }
    }
    if unknown {
        Outcome::Unevaluated
    } else {
        Outcome::Inactive
    }
}

fn no_live_shard(e: &Evidence) -> Outcome {
    let (Some(phase), Some(shards)) = (e.phase, &e.shards) else {
        return Outcome::Unevaluated;
    };
    if phase != Phase::Serving || shards.iter().any(ShardEvidence::ingest_capable) {
        return Outcome::Inactive;
    }
    Fired::new(summary::no_live_shard())
        .with("shard_count", u(shards.len() as u64))
        .into()
}

fn shard_offline(e: &Evidence) -> Outcome {
    let (Some(shards), Some(sd)) = (&e.shards, e.shutting_down) else {
        return Outcome::Unevaluated;
    };
    if sd {
        return Outcome::Inactive;
    }
    let offline: Vec<&ShardEvidence> = shards
        .iter()
        .filter(|s| s.state == ShardState::Offline)
        .collect();
    if offline.is_empty() || offline.len() == shards.len() {
        return Outcome::Inactive;
    }
    let ids: Vec<u16> = offline.iter().map(|s| s.id).collect();
    let reason = offline[0].reason.unwrap_or(OfflineReason::FlusherExited);
    Fired::new(summary::shard_offline(&ids, reason))
        .with("reason", code(reason.as_str()))
        .with("shards", EvidenceValue::Shards(ids))
        .into()
}

fn worker_exited(e: &Evidence) -> Outcome {
    let (Some(workers), Some(sd)) = (&e.workers, e.shutting_down) else {
        return Outcome::Unevaluated;
    };
    if sd {
        return Outcome::Inactive;
    }
    // `exited_at` distinguishes a worker that ran and exited from one not
    // started yet (startup): only the former is a condition.
    let Some(w) = workers
        .iter()
        .find(|w| !w.running && w.exited_at_unix_ms.is_some())
    else {
        return Outcome::Inactive;
    };
    let n = workers.len() as u16;
    let shards: Vec<u16> = e
        .shards
        .iter()
        .flatten()
        .map(|s| s.id)
        .filter(|id| n > 0 && id % n == w.id)
        .collect();
    Fired::new(summary::worker_exited(w.id, &shards))
        .with("worker", u(u64::from(w.id)))
        .with("shards", EvidenceValue::Shards(shards))
        .into()
}

fn wab_cap_rejecting(e: &Evidence) -> Outcome {
    let Some(c) = &e.wab_cap else {
        return Outcome::Unevaluated;
    };
    if !c.rejecting {
        return Outcome::Inactive;
    }
    Fired::new(summary::wab_cap_rejecting(
        c.bytes_now,
        c.max_bytes,
        c.low_water_bytes,
    ))
    .with("bytes", u(c.bytes_now))
    .with("cap", u(c.max_bytes))
    .with("low_water", u(c.low_water_bytes))
    .into()
}

fn drain_backpressure(e: &Evidence, t: &Thresholds) -> Outcome {
    let now = e.now_unix_ms;
    let blocked = e.shards.as_ref().map(|shards| {
        let ids: Vec<u16> = shards
            .iter()
            .filter(|s| {
                s.handoff_blocked_since_unix_ms
                    .is_some_and(|b| age_ms(now, b) >= t.handoff_blocked_ms)
            })
            .map(|s| s.id)
            .collect();
        let longest_ms = shards
            .iter()
            .filter_map(|s| s.handoff_blocked_since_unix_ms)
            .map(|b| age_ms(now, b))
            .max()
            .unwrap_or(0);
        (ids, longest_ms / 1000)
    });
    let channel_high = e.channel.map(|c| {
        c.high_since_unix_ms
            .is_some_and(|h| age_ms(now, h) >= t.channel_high_ms)
    });
    match tri_or(
        blocked.as_ref().map(|(ids, _)| !ids.is_empty()),
        channel_high,
    ) {
        None => Outcome::Unevaluated,
        Some(false) => Outcome::Inactive,
        Some(true) => {
            let (ids, secs) = blocked.unwrap_or_default();
            let depth = e.channel.map(|c| c.depth);
            let mut f = Fired::new(summary::drain_backpressure(depth, &ids, secs))
                .with("blocked_secs", u(secs))
                .with("shards", EvidenceValue::Shards(ids));
            if let Some(d) = depth {
                f = f.with("channel_depth", u(d));
            }
            f.into()
        }
    }
}

fn ack_timeouts_recent(e: &Evidence, t: &Thresholds) -> Outcome {
    let Some(n) = e.recent.as_ref().and_then(|r| r.ack_timeouts_5m) else {
        return Outcome::Unevaluated;
    };
    if n == 0 {
        return Outcome::Inactive;
    }
    let storage = e.recent.as_ref().is_some_and(|r| {
        r.fsync_failures_15m.unwrap_or(0) > 0
            || r.coalesce_ewma_us.unwrap_or(0) > t.coalesce_slow_us
    });
    let cause = if matches!(drain_backpressure(e, t), Outcome::Active(_)) {
        AckTimeoutCause::DeliveryStallingIngest
    } else if storage {
        AckTimeoutCause::StorageSlowOrFailing
    } else {
        AckTimeoutCause::FlusherNotCompleting
    };
    Fired::new(summary::ack_timeouts_recent(n, cause))
        .with("ack_timeouts", u(n))
        .with("cause", code(cause.as_str()))
        .into()
}

fn wab_scan_stale(e: &Evidence, t: &Thresholds) -> Outcome {
    // Scan times exist only inside the daemon; a scrape cannot say.
    if e.source != Source::Daemon {
        return Outcome::Unevaluated;
    }
    let Some(c) = &e.wab_cap else {
        return Outcome::Unevaluated;
    };
    match c.last_scan_ok_unix_ms {
        None => Fired::new(summary::wab_scan_stale(true, 0))
            .with("never", EvidenceValue::Bool(true))
            .into(),
        Some(at) if age_ms(e.now_unix_ms, at) > t.wab_scan_stale_ms => {
            let secs = age_ms(e.now_unix_ms, at) / 1000;
            Fired::new(summary::wab_scan_stale(false, secs))
                .with("scan_age_secs", u(secs))
                .into()
        }
        Some(_) => Outcome::Inactive,
    }
}

fn wab_growing_unbounded(e: &Evidence) -> Outcome {
    match e.wab_cap.as_ref().and_then(|c| c.growing_unbounded) {
        None => Outcome::Unevaluated,
        Some(false) => Outcome::Inactive,
        Some(true) => Fired::new(summary::wab_growing_unbounded()).into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes::{Gate, Severity};
    use crate::testing::serving_evidence;

    pub(crate) const NOW: u64 = 1_800_000_000_000;
    use ConditionCode as C;

    pub(crate) fn outcome(code: ConditionCode, e: &Evidence) -> &'static str {
        match rule(code, e, &Thresholds::DEFAULT) {
            Outcome::Unevaluated => "unevaluated",
            Outcome::Inactive => "inactive",
            Outcome::Active(_) => "active",
        }
    }
    pub(crate) fn with(f: impl FnOnce(&mut Evidence)) -> Evidence {
        let mut e = serving_evidence(NOW);
        f(&mut e);
        e
    }
    pub(crate) fn fired(code: ConditionCode, e: &Evidence) -> Fired {
        match rule(code, e, &Thresholds::DEFAULT) {
            Outcome::Active(f) => f,
            _ => panic!("{code:?} did not fire"),
        }
    }

    const INGEST: [ConditionCode; 11] = [
        C::Starting,
        C::ShuttingDown,
        C::ListenerDown,
        C::NoLiveShard,
        C::ShardOffline,
        C::WorkerExited,
        C::WabCapRejecting,
        C::DrainBackpressure,
        C::AckTimeoutsRecent,
        C::WabScanStale,
        C::WabGrowingUnbounded,
    ];

    #[test]
    fn a_healthy_serving_daemon_raises_no_ingest_condition() {
        let e = serving_evidence(NOW);
        for c in INGEST {
            assert_eq!(outcome(c, &e), "inactive", "{c:?}");
        }
    }

    #[test]
    fn missing_evidence_makes_every_ingest_rule_unevaluated() {
        let e = Evidence::empty(NOW, Source::Daemon);
        for c in INGEST {
            assert_eq!(outcome(c, &e), "unevaluated", "{c:?}");
        }
    }

    #[test]
    fn starting_fires_in_every_phase_before_serving_and_names_it() {
        for p in [
            Phase::Initializing,
            Phase::Recovering,
            Phase::Handoff,
            Phase::Binding,
        ] {
            let e = with(|e| {
                e.phase = Some(p);
                e.phase_since_unix_ms = Some(NOW - 7_000);
            });
            let f = fired(C::Starting, &e);
            assert_eq!(f.evidence["phase"], EvidenceValue::Code(p.as_str().into()));
            assert!(
                f.summary
                    .contains(&format!("startup phase {} for 7s", p.as_str())),
                "{}",
                f.summary
            );
        }
        assert_eq!(
            outcome(C::Starting, &with(|e| e.phase = Some(Phase::ShuttingDown))),
            "inactive"
        );
    }

    #[test]
    fn shutting_down_fires_and_names_the_budget_only_when_known() {
        let e = with(|e| e.shutting_down = Some(true));
        assert_eq!(
            fired(C::ShuttingDown, &e).summary,
            "graceful shutdown; new connections refused"
        );
        let e = with(|e| {
            e.shutting_down = Some(true);
            e.drain_deadline_unix_ms = Some(NOW + 4_000);
            e.drain_budget_secs = Some(10);
        });
        assert!(
            fired(C::ShuttingDown, &e)
                .summary
                .ends_with("drain has 4s of its 10s budget")
        );
    }

    #[test]
    fn listener_down_needs_a_failure_or_a_close_outside_shutdown() {
        let set = |state, sd| {
            with(|e| {
                e.listeners = Some(vec![ListenerEvidence {
                    kind: ListenerKind::Tcp,
                    state,
                }]);
                e.shutting_down = sd;
            })
        };
        assert_eq!(
            outcome(C::ListenerDown, &set(ListenerState::Failed, Some(true))),
            "active"
        );
        assert_eq!(
            outcome(C::ListenerDown, &set(ListenerState::Closed, Some(false))),
            "active"
        );
        assert_eq!(
            outcome(C::ListenerDown, &set(ListenerState::Closed, Some(true))),
            "inactive"
        );
        assert_eq!(
            outcome(C::ListenerDown, &set(ListenerState::Closed, None)),
            "unevaluated"
        );
        assert_eq!(
            outcome(C::ListenerDown, &set(ListenerState::Pending, Some(false))),
            "inactive"
        );
        assert!(
            fired(C::ListenerDown, &set(ListenerState::Failed, Some(false)))
                .summary
                .starts_with("tcp listener")
        );
    }

    #[test]
    fn no_live_shard_counts_ingest_capable_shards_only_while_serving() {
        let down = |phase| {
            with(|e| {
                e.phase = Some(phase);
                for s in e.shards.as_mut().unwrap() {
                    s.state = ShardState::Offline;
                }
            })
        };
        assert_eq!(outcome(C::NoLiveShard, &down(Phase::Serving)), "active");
        assert_eq!(
            outcome(C::NoLiveShard, &down(Phase::Recovering)),
            "inactive"
        );
        let worker_gone = with(|e| {
            for s in e.shards.as_mut().unwrap() {
                s.worker_running = false
            }
        });
        assert_eq!(outcome(C::NoLiveShard, &worker_gone), "active");
        let respawning = with(|e| e.shards.as_mut().unwrap()[0].state = ShardState::Respawning);
        assert_eq!(outcome(C::NoLiveShard, &respawning), "inactive");
    }

    #[test]
    fn shard_offline_is_a_partial_outage_outside_shutdown() {
        let one = with(|e| {
            let s = &mut e.shards.as_mut().unwrap()[1];
            s.state = ShardState::Offline;
            s.reason = Some(OfflineReason::CounterScanFailed);
        });
        let f = fired(C::ShardOffline, &one);
        assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![1]));
        assert!(
            f.summary
                .starts_with("shard(s) 1 offline (counter_scan_failed)"),
            "{}",
            f.summary
        );
        let all = with(|e| {
            for s in e.shards.as_mut().unwrap() {
                s.state = ShardState::Offline
            }
        });
        assert_eq!(
            outcome(C::ShardOffline, &all),
            "inactive",
            "all offline is no_live_shard's job"
        );
        let mut stopping = one.clone();
        stopping.shutting_down = Some(true);
        assert_eq!(outcome(C::ShardOffline, &stopping), "inactive");
    }

    #[test]
    fn worker_exited_names_the_shards_it_owned_and_ignores_unstarted_workers() {
        let e = with(|e| {
            let w = &mut e.workers.as_mut().unwrap()[1];
            w.running = false;
            w.exited_at_unix_ms = Some(NOW - 1);
        });
        let f = fired(C::WorkerExited, &e);
        assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![1]));
        assert_eq!(f.summary, "worker 1 exited; shards 1 cannot accept records");
        let unstarted = with(|e| e.workers.as_mut().unwrap()[1].running = false);
        assert_eq!(outcome(C::WorkerExited, &unstarted), "inactive");
    }

    #[test]
    fn cap_rejecting_carries_bytes_cap_and_low_water() {
        let e = with(|e| {
            let c = e.wab_cap.as_mut().unwrap();
            c.rejecting = true;
            c.bytes_now = 950_000_000;
        });
        let f = fired(C::WabCapRejecting, &e);
        assert_eq!(f.evidence["low_water"], EvidenceValue::U64(900_000_000));
        assert!(
            f.summary
                .starts_with("WAB holds 950000000 of the 1000000000 cap")
        );
    }

    #[test]
    fn backpressure_is_three_valued() {
        let blocked = with(|e| {
            e.shards.as_mut().unwrap()[0].handoff_blocked_since_unix_ms = Some(NOW - 2_500)
        });
        let f = fired(C::DrainBackpressure, &blocked);
        assert_eq!(f.evidence["shards"], EvidenceValue::Shards(vec![0]));
        assert_eq!(f.evidence["blocked_secs"], EvidenceValue::U64(2));
        let brief =
            with(|e| e.shards.as_mut().unwrap()[0].handoff_blocked_since_unix_ms = Some(NOW - 500));
        assert_eq!(outcome(C::DrainBackpressure, &brief), "inactive");
        let high = with(|e| {
            e.channel = Some(ChannelEvidence {
                depth: 240,
                capacity: 256,
                high_since_unix_ms: Some(NOW - 11_000),
            })
        });
        assert_eq!(outcome(C::DrainBackpressure, &high), "active");
        // PR 1: no channel depth yet and no shard blocked -> cannot say "no backpressure".
        let unknown_channel = with(|e| e.channel = None);
        assert_eq!(
            outcome(C::DrainBackpressure, &unknown_channel),
            "unevaluated"
        );
        let mut blocked_unknown_channel = blocked.clone();
        blocked_unknown_channel.channel = None;
        assert_eq!(
            outcome(C::DrainBackpressure, &blocked_unknown_channel),
            "active"
        );
    }

    #[test]
    fn ack_timeouts_infer_their_cause_in_spec_order() {
        let timeouts = |f: &dyn Fn(&mut Evidence)| {
            with(|e| {
                e.recent.as_mut().unwrap().ack_timeouts_5m = Some(3);
                f(e);
            })
        };
        let cause = |e: &Evidence| fired(C::AckTimeoutsRecent, e).evidence["cause"].clone();
        let code = |s: &str| EvidenceValue::Code(s.into());
        assert_eq!(
            cause(&timeouts(&|e| e.shards.as_mut().unwrap()[0]
                .handoff_blocked_since_unix_ms =
                Some(NOW - 5_000))),
            code("delivery_stalling_ingest")
        );
        assert_eq!(
            cause(&timeouts(&|e| e
                .recent
                .as_mut()
                .unwrap()
                .fsync_failures_15m = Some(1))),
            code("storage_slow_or_failing")
        );
        assert_eq!(
            cause(&timeouts(&|e| e
                .recent
                .as_mut()
                .unwrap()
                .coalesce_ewma_us = Some(2_000_000))),
            code("storage_slow_or_failing")
        );
        assert_eq!(cause(&timeouts(&|_| {})), code("flusher_not_completing"));
    }

    #[test]
    fn scan_staleness_and_unbounded_growth() {
        let never = with(|e| e.wab_cap.as_mut().unwrap().last_scan_ok_unix_ms = None);
        assert!(
            fired(C::WabScanStale, &never)
                .summary
                .starts_with("no successful WAB scan yet")
        );
        let old = with(|e| e.wab_cap.as_mut().unwrap().last_scan_ok_unix_ms = Some(NOW - 31_000));
        assert_eq!(
            fired(C::WabScanStale, &old).evidence["scan_age_secs"],
            EvidenceValue::U64(31)
        );
        let offline = with(|e| e.source = Source::Metrics);
        assert_eq!(
            outcome(C::WabScanStale, &offline),
            "unevaluated",
            "only the daemon knows scan times"
        );
        assert_eq!(
            outcome(
                C::WabGrowingUnbounded,
                &with(|e| e.wab_cap.as_mut().unwrap().growing_unbounded = Some(true))
            ),
            "active"
        );
        assert_eq!(
            outcome(
                C::WabGrowingUnbounded,
                &with(|e| e.wab_cap.as_mut().unwrap().growing_unbounded = None)
            ),
            "unevaluated"
        );
    }

    #[test]
    fn evaluate_attaches_catalogue_metadata_and_latch_times() {
        struct Fixed;
        impl SinceLookup for Fixed {
            fn since(&self, _: ConditionCode) -> Option<u64> {
                Some(77)
            }
        }
        let e = with(|e| e.wab_cap.as_mut().unwrap().rejecting = true);
        let ev = evaluate(&e, &Thresholds::DEFAULT, &Fixed);
        let c = ev.get(C::WabCapRejecting).expect("active");
        assert_eq!(c.since_unix_ms, Some(77));
        assert_eq!(c.severity, Severity::Critical);
        assert_eq!(c.runbook, "docs/monitoring.md#cond-wab_cap_rejecting");
        assert_eq!(c.gates, vec![Gate::Readiness]);
        assert!(!ev.unevaluated.contains(&C::WabCapRejecting));
    }
}
