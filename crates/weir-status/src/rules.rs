//! One rule per catalogue code (spec §9.3), plus `evaluate`. A rule returns
//! `Unevaluated` when the evidence it needs is missing, never `Inactive`.
use crate::codes::{CATALOGUE, ConditionCode};
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
        C::DrainStopped => drain_stopped(e),
        C::DrainWedged => drain_wedged(e, t),
        C::DrainPaused => drain_paused(e),
        C::DrainBlockedDeadLetterFull => drain_blocked_dead_letter_full(e),
        C::DeliveryStalled => delivery_stalled(e, t),
        C::DeliveryLagging => delivery_lagging(e, t),
        C::SegmentsStranded => segments_stranded(e),
        C::SinkDown => sink_down(e),
        C::SinkDegraded => sink_degraded(e),
        C::SinkFailing => sink_failing(e, t),
        C::DeadLetterNearCap => dead_letter_near_cap(e, t),
        C::NoopSink => noop_sink(e),
        C::ActiveSegmentAging => active_segment_aging(e, t),
        // Task 1.6 replaces this arm with the remaining rules.
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

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

fn drain_stopped(e: &Evidence) -> Outcome {
    let (Some(d), Some(sd)) = (&e.drain, e.shutting_down) else {
        return Outcome::Unevaluated;
    };
    if d.exit != DrainExitCode::GaveUp || sd {
        return Outcome::Inactive;
    }
    let panics = e
        .recent
        .as_ref()
        .and_then(|r| r.drain_panics_total)
        .unwrap_or(0);
    Fired::new(summary::drain_stopped(panics))
        .with("panics", u(panics))
        .detail(
            d.last_panic
                .as_deref()
                .map(|p| truncate(p, DETAIL_PANIC_MAX_BYTES)),
        )
        .into()
}

fn drain_wedged(e: &Evidence, t: &Thresholds) -> Outcome {
    let (Some(d), Some(c)) = (&e.drain, &e.config) else {
        return Outcome::Unevaluated;
    };
    if d.exit != DrainExitCode::Running {
        return Outcome::Inactive;
    }
    let now = e.now_unix_ms;
    if let Some(since) = d.commit_in_flight_since_unix_ms {
        let Some(ct) = c.commit_timeout_secs else {
            return Outcome::Unevaluated;
        };
        let age = age_ms(now, since);
        return if age > ct.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
            Fired::new(summary::drain_wedged(true, age / 1000))
                .with("commit_age_secs", u(age / 1000))
                .into()
        } else {
            Outcome::Inactive
        };
    }
    // The safety floor's verdict table (main.rs `classify_drain_stall`, spec
    // safety floor §4.8): busy in local I/O is `SlowLocalIo`, never a wedge;
    // busy in a sink call (a non-yielding probe) is `SlowSinkCall` until
    // commit_timeout + 15 s. One verdict for the log line and the condition (G2).
    if let Some(b) = d.busy {
        if !b.sink_call {
            return Outcome::Inactive;
        }
        let Some(ct) = c.commit_timeout_secs else {
            return Outcome::Unevaluated;
        };
        let age = age_ms(now, b.since_unix_ms);
        return if age > ct.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
            Fired::new(summary::drain_wedged(true, age / 1000))
                .with("busy_secs", u(age / 1000))
                .into()
        } else {
            Outcome::Inactive
        };
    }
    let (Some(p), Some(hp)) = (d.last_progress_unix_ms, c.public.health_poll_interval_secs) else {
        return Outcome::Unevaluated;
    };
    let age = age_ms(now, p);
    if age >= hp.saturating_mul(1000).saturating_add(t.wedge_grace_ms) {
        Fired::new(summary::drain_wedged(false, age / 1000))
            .with("progress_age_secs", u(age / 1000))
            .into()
    } else {
        Outcome::Inactive
    }
}

fn drain_paused(e: &Evidence) -> Outcome {
    let Some(d) = &e.drain else {
        return Outcome::Unevaluated;
    };
    if !(d.pause_requested || d.paused) {
        return Outcome::Inactive;
    }
    let unbounded = e
        .config
        .as_ref()
        .is_some_and(|c| c.public.wab_max_bytes == Some(0));
    let held = d.in_flight.filter(|_| d.held);
    let since = d.paused_since_unix_ms.unwrap_or(e.now_unix_ms);
    let mut f = Fired::new(summary::drain_paused(
        since,
        held.map(|i| i.processed_records),
        unbounded,
    ))
    .with("held", EvidenceValue::Bool(d.held))
    .with("in_effect", EvidenceValue::Bool(d.paused))
    .detail(d.paused_by_uid.map(|uid| format!("paused by uid {uid}")));
    if let Some(i) = held {
        f = f
            .with("held_at_record", u(i.processed_records))
            .with("segment", EvidenceValue::Segment(i.segment));
    }
    f.into()
}

fn drain_blocked_dead_letter_full(e: &Evidence) -> Outcome {
    let Some(d) = &e.drain else {
        return Outcome::Unevaluated;
    };
    if d.state != DrainStateCode::BlockedDeadLetterFull {
        return Outcome::Inactive;
    }
    let dl = e.backlog.as_ref().map(|b| b.dead_letter_bytes);
    let cap = e
        .config
        .as_ref()
        .and_then(|c| c.public.dead_letter_max_bytes);
    let mut f = Fired::new(summary::drain_blocked(dl, cap));
    if let Some(b) = d.blocked_since_unix_ms {
        f = f.with("blocked_since_ms", u(b));
    }
    f.into()
}

fn delivery_stalled(e: &Evidence, t: &Thresholds) -> Outcome {
    let (Some(b), Some(s), Some(d)) = (&e.backlog, &e.sink, &e.drain) else {
        return Outcome::Unevaluated;
    };
    if b.undelivered_segments == 0 || d.paused || d.pause_requested {
        return Outcome::Inactive;
    }
    let Some(reference) = s.last_success_unix_ms.or(e.started_at_unix_ms) else {
        return Outcome::Unevaluated;
    };
    let idle = age_ms(e.now_unix_ms, reference);
    if idle < t.delivery_stalled_ms {
        return Outcome::Inactive;
    }
    Fired::new(summary::delivery_stalled(
        b.undelivered_segments,
        idle / 1000,
    ))
    .with("undelivered_segments", u(b.undelivered_segments))
    .with("idle_secs", u(idle / 1000))
    .into()
}

fn delivery_lagging(e: &Evidence, t: &Thresholds) -> Outcome {
    let Some(b) = &e.backlog else {
        return Outcome::Unevaluated;
    };
    match b.oldest_undelivered_age_secs {
        Some(a) if a > t.delivery_lagging_secs => Fired::new(summary::delivery_lagging(a as u64))
            .with("oldest_age_secs", EvidenceValue::F64(a))
            .into(),
        _ => Outcome::Inactive,
    }
}

fn segments_stranded(e: &Evidence) -> Outcome {
    let Some(n) = e
        .recent
        .as_ref()
        .and_then(|r| r.stranded_since_last_success)
    else {
        return Outcome::Unevaluated;
    };
    if n == 0 {
        return Outcome::Inactive;
    }
    Fired::new(summary::segments_stranded(n))
        .with("stranded", u(n))
        .into()
}

fn sink_down(e: &Evidence) -> Outcome {
    let Some(s) = &e.sink else {
        return Outcome::Unevaluated;
    };
    match s.health {
        None => Outcome::Unevaluated,
        Some(SinkHealthCode::Down) => Fired::new(summary::sink_down(s.health_since_unix_ms))
            .detail(s.probe_reason.clone())
            .into(),
        Some(_) => Outcome::Inactive,
    }
}

fn sink_degraded(e: &Evidence) -> Outcome {
    let Some(s) = &e.sink else {
        return Outcome::Unevaluated;
    };
    match s.health {
        None => Outcome::Unevaluated,
        Some(SinkHealthCode::Degraded) => Fired::new(summary::sink_degraded())
            .detail(s.probe_reason.clone())
            .into(),
        Some(_) => Outcome::Inactive,
    }
}

fn sink_failing(e: &Evidence, t: &Thresholds) -> Outcome {
    let Some(s) = &e.sink else {
        return Outcome::Unevaluated;
    };
    if s.consecutive_failures < t.sink_failing_consecutive || s.health == Some(SinkHealthCode::Down)
    {
        return Outcome::Inactive;
    }
    let mut f = Fired::new(summary::sink_failing(
        s.consecutive_failures,
        s.last_failure_kind,
    ))
    .with("consecutive_failures", u(s.consecutive_failures))
    .detail(s.last_error.clone());
    if let Some(k) = s.last_failure_kind {
        f = f.with("kind", code(k.as_str()));
    }
    f.into()
}

fn dead_letter_near_cap(e: &Evidence, t: &Thresholds) -> Outcome {
    let (Some(b), Some(c)) = (&e.backlog, &e.config) else {
        return Outcome::Unevaluated;
    };
    let Some(cap) = c.public.dead_letter_max_bytes else {
        return Outcome::Unevaluated;
    };
    if cap == 0
        || b.dead_letter_bytes.saturating_mul(100)
            < cap.saturating_mul(t.dead_letter_near_cap_percent)
    {
        return Outcome::Inactive;
    }
    Fired::new(summary::dead_letter_near_cap(b.dead_letter_bytes, cap))
        .with("bytes", u(b.dead_letter_bytes))
        .with("cap", u(cap))
        .into()
}

fn noop_sink(e: &Evidence) -> Outcome {
    let Some(c) = &e.config else {
        return Outcome::Unevaluated;
    };
    if c.sink_type == "noop" {
        Fired::new(summary::noop_sink()).into()
    } else {
        Outcome::Inactive
    }
}

fn active_segment_aging(e: &Evidence, t: &Thresholds) -> Outcome {
    // Ages come from the lag scan; without a backlog record it has not run.
    let (Some(shards), Some(c), Some(_)) = (&e.shards, &e.config, &e.backlog) else {
        return Outcome::Unevaluated;
    };
    let (Some(age), Some(life)) = (
        c.public.wab_segment_max_age_secs,
        c.public.wab_segment_max_lifetime_secs,
    ) else {
        return Outcome::Unevaluated;
    };
    if age != 0 || life != 0 {
        return Outcome::Inactive;
    }
    let oldest = shards
        .iter()
        .filter_map(|s| s.active_segment_age_secs.map(|a| (s.id, a)))
        .filter(|(_, a)| *a > t.active_segment_aging_secs)
        .max_by(|a, b| a.1.total_cmp(&b.1));
    let Some((shard, secs)) = oldest else {
        return Outcome::Inactive;
    };
    Fired::new(summary::active_segment_aging(shard, secs as u64))
        .with("shard", u(u64::from(shard)))
        .with("age_secs", EvidenceValue::F64(secs))
        .into()
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

    use crate::segment::SegmentId;

    const DELIVERY: [ConditionCode; 13] = [
        C::DrainStopped,
        C::DrainWedged,
        C::DrainPaused,
        C::DrainBlockedDeadLetterFull,
        C::DeliveryStalled,
        C::DeliveryLagging,
        C::SegmentsStranded,
        C::SinkDown,
        C::SinkDegraded,
        C::SinkFailing,
        C::DeadLetterNearCap,
        C::NoopSink,
        C::ActiveSegmentAging,
    ];

    #[test]
    fn a_healthy_serving_daemon_raises_no_delivery_condition() {
        let e = serving_evidence(NOW);
        for c in DELIVERY {
            assert_eq!(outcome(c, &e), "inactive", "{c:?}");
        }
    }

    #[test]
    fn missing_evidence_makes_every_delivery_rule_unevaluated() {
        let e = Evidence::empty(NOW, Source::Daemon);
        for c in DELIVERY {
            assert_eq!(outcome(c, &e), "unevaluated", "{c:?}");
        }
    }

    #[test]
    fn drain_stopped_is_gave_up_outside_shutdown_with_a_capped_panic_detail() {
        let gave_up = with(|e| {
            let d = e.drain.as_mut().unwrap();
            d.exit = DrainExitCode::GaveUp;
            d.last_panic = Some("x".repeat(1_000));
            e.recent.as_mut().unwrap().drain_panics_total = Some(10);
        });
        let f = fired(C::DrainStopped, &gave_up);
        assert!(
            f.summary
                .starts_with("drain supervisor gave up after 10 panics")
        );
        assert_eq!(
            f.detail.as_deref().map(str::len),
            Some(DETAIL_PANIC_MAX_BYTES)
        );
        let mut stopping = gave_up.clone();
        stopping.shutting_down = Some(true);
        assert_eq!(outcome(C::DrainStopped, &stopping), "inactive");
        assert_eq!(
            outcome(
                C::DrainStopped,
                &with(|e| e.drain.as_mut().unwrap().exit = DrainExitCode::Stopped)
            ),
            "inactive"
        );
    }

    #[test]
    fn drain_wedged_uses_the_commit_timer_or_the_progress_timer() {
        // commit_timeout 60 s + 15 s grace
        let commit = |age| {
            with(|e| e.drain.as_mut().unwrap().commit_in_flight_since_unix_ms = Some(NOW - age))
        };
        assert_eq!(outcome(C::DrainWedged, &commit(75_000)), "inactive");
        assert_eq!(outcome(C::DrainWedged, &commit(75_001)), "active");
        // health_poll_interval 30 s + 15 s grace
        let progress =
            |age| with(|e| e.drain.as_mut().unwrap().last_progress_unix_ms = Some(NOW - age));
        assert_eq!(outcome(C::DrainWedged, &progress(44_999)), "inactive");
        assert_eq!(outcome(C::DrainWedged, &progress(45_000)), "active");
        let none = with(|e| e.drain.as_mut().unwrap().last_progress_unix_ms = None);
        assert_eq!(outcome(C::DrainWedged, &none), "unevaluated");
        let gone = with(|e| {
            let d = e.drain.as_mut().unwrap();
            d.exit = DrainExitCode::GaveUp;
            d.last_progress_unix_ms = Some(NOW - 600_000);
        });
        assert_eq!(
            outcome(C::DrainWedged, &gone),
            "inactive",
            "a stopped drain is drain_stopped, not wedged"
        );
        // The safety floor's busy signal (main.rs `classify_drain_stall`): local
        // I/O is a slow disk, never a wedge, however long; a sink call is a wedge
        // only past commit_timeout + 15 s.
        let busy = |sink_call, age| {
            with(|e| {
                let d = e.drain.as_mut().unwrap();
                d.busy = Some(BusyEvidence {
                    sink_call,
                    since_unix_ms: NOW - age,
                });
                d.last_progress_unix_ms = Some(NOW - age);
            })
        };
        assert_eq!(
            outcome(C::DrainWedged, &busy(false, 600_000)),
            "inactive",
            "SlowLocalIo is never a wedge"
        );
        assert_eq!(
            outcome(C::DrainWedged, &busy(true, 50_000)),
            "inactive",
            "SlowSinkCall inside the limit"
        );
        assert_eq!(outcome(C::DrainWedged, &busy(true, 75_000)), "inactive");
        assert_eq!(outcome(C::DrainWedged, &busy(true, 75_001)), "active");
        assert_eq!(
            fired(C::DrainWedged, &busy(true, 80_000)).evidence["busy_secs"],
            EvidenceValue::U64(80)
        );
    }

    #[test]
    fn drain_paused_reports_held_records_unbounded_growth_and_the_uid_as_admin_detail() {
        let e = with(|e| {
            let d = e.drain.as_mut().unwrap();
            d.pause_requested = true;
            d.paused = true;
            d.held = true;
            d.paused_since_unix_ms = Some(NOW - 30_000);
            d.paused_by_uid = Some(1000);
            d.in_flight = Some(InFlightEvidence {
                segment: SegmentId {
                    shard: 0,
                    counter: 9,
                },
                attempt: 1,
                retries_left: 3,
                processed_records: 512,
                next_retry_at_unix_ms: None,
            });
            e.config.as_mut().unwrap().public.wab_max_bytes = Some(0);
        });
        let f = fired(C::DrainPaused, &e);
        assert!(
            f.summary.contains("(held: mid-segment at record 512)"),
            "{}",
            f.summary
        );
        assert!(
            f.summary.contains("without bound: wab_max_bytes=0"),
            "{}",
            f.summary
        );
        assert!(
            !f.summary.contains("1000"),
            "the uid is admin-only: {}",
            f.summary
        );
        assert_eq!(f.detail.as_deref(), Some("paused by uid 1000"));
        assert_eq!(
            f.evidence["segment"],
            EvidenceValue::Segment(SegmentId {
                shard: 0,
                counter: 9
            })
        );
        let requested = with(|e| e.drain.as_mut().unwrap().pause_requested = true);
        assert_eq!(
            outcome(C::DrainPaused, &requested),
            "active",
            "requested counts before it takes effect"
        );
    }

    #[test]
    fn blocked_stalled_lagging_and_stranded() {
        let blocked =
            with(|e| e.drain.as_mut().unwrap().state = DrainStateCode::BlockedDeadLetterFull);
        assert!(
            fired(C::DrainBlockedDeadLetterFull, &blocked)
                .summary
                .starts_with("dead_letter/ at 0/100000000")
        );
        let stalled = with(|e| {
            e.backlog.as_mut().unwrap().undelivered_segments = 4;
            e.sink.as_mut().unwrap().last_success_unix_ms = Some(NOW - 300_000);
        });
        assert_eq!(outcome(C::DeliveryStalled, &stalled), "active");
        let mut paused = stalled.clone();
        paused.drain.as_mut().unwrap().paused = true;
        assert_eq!(
            outcome(C::DeliveryStalled, &paused),
            "inactive",
            "a pause is not a stall"
        );
        let never = with(|e| {
            e.backlog.as_mut().unwrap().undelivered_segments = 1;
            e.sink.as_mut().unwrap().last_success_unix_ms = None;
        });
        assert_eq!(
            outcome(C::DeliveryStalled, &never),
            "active",
            "measured from process start (10 min ago)"
        );
        let lag = with(|e| e.backlog.as_mut().unwrap().oldest_undelivered_age_secs = Some(901.0));
        assert_eq!(
            fired(C::DeliveryLagging, &lag).evidence["oldest_age_secs"],
            EvidenceValue::F64(901.0)
        );
        let stranded = with(|e| e.recent.as_mut().unwrap().stranded_since_last_success = Some(2));
        assert_eq!(outcome(C::SegmentsStranded, &stranded), "active");
        let unknown = with(|e| e.recent.as_mut().unwrap().stranded_since_last_success = None);
        assert_eq!(outcome(C::SegmentsStranded, &unknown), "unevaluated");
    }

    #[test]
    fn sink_health_failing_and_noop() {
        let down = with(|e| {
            let s = e.sink.as_mut().unwrap();
            s.health = Some(SinkHealthCode::Down);
            s.probe_reason = Some("connect refused".into());
        });
        let f = fired(C::SinkDown, &down);
        assert_eq!(f.detail.as_deref(), Some("connect refused"));
        assert!(!f.summary.contains("refused"), "probe reason is admin-only");
        assert_eq!(
            outcome(
                C::SinkDegraded,
                &with(|e| e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Degraded))
            ),
            "active"
        );
        let failing = |health| {
            with(|e| {
                let s = e.sink.as_mut().unwrap();
                s.consecutive_failures = 3;
                s.health = Some(health);
                s.last_failure_kind = Some(SinkFailureKind::Timeout);
                s.last_error = Some("POST failed".into());
            })
        };
        let f = fired(C::SinkFailing, &failing(SinkHealthCode::Healthy));
        assert!(f.summary.ends_with("last kind timeout"));
        assert_eq!(f.detail.as_deref(), Some("POST failed"));
        assert_eq!(
            outcome(C::SinkFailing, &failing(SinkHealthCode::Down)),
            "inactive",
            "sink_down covers it"
        );
        assert_eq!(
            outcome(
                C::NoopSink,
                &with(|e| e.config.as_mut().unwrap().sink_type = "noop".into())
            ),
            "active"
        );
    }

    #[test]
    fn dead_letter_near_cap_and_active_segment_aging() {
        let near = |bytes| with(|e| e.backlog.as_mut().unwrap().dead_letter_bytes = bytes);
        assert_eq!(outcome(C::DeadLetterNearCap, &near(79_999_999)), "inactive");
        assert_eq!(outcome(C::DeadLetterNearCap, &near(80_000_000)), "active");
        let aging = with(|e| e.shards.as_mut().unwrap()[1].active_segment_age_secs = Some(3_601.0));
        let f = fired(C::ActiveSegmentAging, &aging);
        assert_eq!(f.evidence["shard"], EvidenceValue::U64(1));
        let mut timer_set = aging.clone();
        timer_set
            .config
            .as_mut()
            .unwrap()
            .public
            .wab_segment_max_age_secs = Some(60);
        assert_eq!(outcome(C::ActiveSegmentAging, &timer_set), "inactive");
        let mut no_lag = aging.clone();
        no_lag.backlog = None;
        assert_eq!(
            outcome(C::ActiveSegmentAging, &no_lag),
            "unevaluated",
            "ages come from the lag scan"
        );
    }
}
