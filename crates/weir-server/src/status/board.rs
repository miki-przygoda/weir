//! The status board's cells (spec §5.2). Every cell has one writer; readers
//! only load.
use super::config_view::ConfigView;
use super::shards::ShardRegistry;
use super::unix_ms_now;
use crate::config::Config;
use crate::metrics::DrainStateValue;
use crate::reload::{ReloadOutcome, ReloadReport, ReloadTrigger};
use arc_swap::{ArcSwap, ArcSwapOption};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use weir_status::{
    ALL, AdminEvidence, AdminReason, AdminState, BusyEvidence, ConditionCode, DrainEvidence,
    DrainExitCode, DrainStateCode, Evaluation, InFlightEvidence, ListenerEvidence, ListenerKind,
    ListenerState, NotReloadedEvidence, NotReloadedReason, Phase, RecentCounters, ReloadEvidence,
    ReloadItem, ReloadItemEvidence, ReloadItemOutcome, ReloadOutcomeCode, ReloadTriggerCode,
    RequeueEvidence, SegmentId, SinceLookup, SinkEvidence, SinkFailureKind, SinkHealthCode,
};

const NONE_MS: u64 = 0;
const NO_BUDGET: u64 = u64::MAX;

fn opt_ms(v: u64) -> Option<u64> {
    (v != NONE_MS).then_some(v)
}

/// Startup phase and shutdown state. Phase stores use Release, so a reader
/// that sees `Serving` also sees the listener stores made before it (spec §6
/// step 12).
pub(crate) struct LifecycleCell {
    phase: AtomicU8,
    phase_since_ms: AtomicU64,
    shutting_down: Arc<AtomicBool>,
    shutting_down_since_ms: AtomicU64,
    forced: AtomicBool,
    ever_served: AtomicBool,
    drain_deadline_ms: AtomicU64,
    drain_budget_secs: AtomicU64,
}

impl LifecycleCell {
    pub(crate) fn new(now_ms: u64) -> Self {
        LifecycleCell {
            phase: AtomicU8::new(Phase::Initializing.as_u8()),
            phase_since_ms: AtomicU64::new(now_ms),
            shutting_down: Arc::new(AtomicBool::new(false)),
            shutting_down_since_ms: AtomicU64::new(NONE_MS),
            forced: AtomicBool::new(false),
            ever_served: AtomicBool::new(false),
            drain_deadline_ms: AtomicU64::new(NONE_MS),
            drain_budget_secs: AtomicU64::new(NO_BUDGET),
        }
    }

    /// Moves to `phase`. Refused (false) once `ShuttingDown`: a startup step
    /// that finishes after SIGTERM must not report the daemon as serving.
    pub(crate) fn enter(&self, phase: Phase, now_ms: u64) -> bool {
        let target = phase.as_u8();
        let shutting = Phase::ShuttingDown.as_u8();
        let moved = self
            .phase
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur != shutting || target == shutting).then_some(target)
            })
            .is_ok();
        if moved {
            self.phase_since_ms.store(now_ms, Ordering::Relaxed);
            if phase == Phase::Serving {
                self.ever_served.store(true, Ordering::Release);
            }
        }
        moved
    }

    pub(crate) fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Acquire)).unwrap_or(Phase::Initializing)
    }

    pub(crate) fn phase_since_ms(&self) -> u64 {
        self.phase_since_ms.load(Ordering::Relaxed)
    }

    /// T0 (spec §7): sets the shared flag and the phase. True only for the
    /// first caller.
    pub(crate) fn begin_shutdown(&self, now_ms: u64) -> bool {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.shutting_down_since_ms.store(now_ms, Ordering::Relaxed);
        self.enter(Phase::ShuttingDown, now_ms);
        true
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 4 (second signal) writes this
    pub(crate) fn mark_forced(&self) {
        self.forced.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    pub(crate) fn shutting_down_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutting_down)
    }

    pub(crate) fn shutting_down_since_ms(&self) -> Option<u64> {
        opt_ms(self.shutting_down_since_ms.load(Ordering::Relaxed))
    }

    pub(crate) fn forced(&self) -> bool {
        self.forced.load(Ordering::Relaxed)
    }

    pub(crate) fn ever_served(&self) -> bool {
        self.ever_served.load(Ordering::Acquire)
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 4 (T2 request_stop) writes this
    pub(crate) fn set_drain_deadline(&self, deadline_unix_ms: u64, budget_secs: u64) {
        self.drain_budget_secs.store(budget_secs, Ordering::Relaxed);
        self.drain_deadline_ms
            .store(deadline_unix_ms, Ordering::Relaxed);
    }

    pub(crate) fn drain_deadline_ms(&self) -> Option<u64> {
        opt_ms(self.drain_deadline_ms.load(Ordering::Relaxed))
    }

    pub(crate) fn drain_budget_secs(&self) -> Option<u64> {
        let v = self.drain_budget_secs.load(Ordering::Relaxed);
        (v != NO_BUDGET).then_some(v)
    }
}

/// One listener's state.
pub(crate) struct ListenerCell(AtomicU8);

impl ListenerCell {
    pub(crate) fn new(state: ListenerState) -> Self {
        ListenerCell(AtomicU8::new(state.as_u8()))
    }
    pub(crate) fn set(&self, state: ListenerState) {
        self.0.store(state.as_u8(), Ordering::Release);
    }
    pub(crate) fn get(&self) -> ListenerState {
        ListenerState::from_u8(self.0.load(Ordering::Acquire)).unwrap_or(ListenerState::Failed)
    }
}

/// The producer listeners. A listener the config does not enable starts (and
/// stays) `NotConfigured`, and is left out of the evidence.
pub(crate) struct ListenerCells {
    pub(crate) unix: ListenerCell,
    pub(crate) tcp: ListenerCell,
}

impl ListenerCells {
    pub(crate) fn new(unix_configured: bool, tcp_configured: bool) -> Self {
        let initial = |configured| {
            if configured {
                ListenerState::Pending
            } else {
                ListenerState::NotConfigured
            }
        };
        ListenerCells {
            unix: ListenerCell::new(initial(unix_configured)),
            tcp: ListenerCell::new(initial(tcp_configured)),
        }
    }

    pub(crate) fn evidence(&self) -> Vec<ListenerEvidence> {
        [
            (ListenerKind::Unix, &self.unix),
            (ListenerKind::Tcp, &self.tcp),
        ]
        .into_iter()
        .map(|(kind, cell)| ListenerEvidence {
            kind,
            state: cell.get(),
        })
        .filter(|l| l.state != ListenerState::NotConfigured)
        .collect()
    }
}

pub(crate) const DEPTH_UNPUBLISHED: usize = usize::MAX;
pub(crate) const QUEUE_UNPUBLISHED: u64 = u64::MAX;
const NO_UID: u64 = u64::MAX;

// ── Drain ────────────────────────────────────────────────────────────────

#[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InFlight {
    /// `wab::segment::segment_identity` of the segment.
    pub segment: String,
    pub attempt: u32,
    pub retries_left: u32,
    pub processed: u64,
    pub next_retry_at_ms: Option<u64>,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RequeueResult {
    pub ticket: u64,
    pub requeued: u64,
    pub already_queued: u64,
    pub scan_error: Option<String>,
    pub backoff_cut: bool,
    pub coalesced: bool,
    pub at_ms: u64,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DrainExitReason {
    Stopped,
    DeadlineAbandoned,
    GaveUp,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 2 constructs these
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrainExitInfo {
    pub reason: DrainExitReason,
    pub abandoned: Option<(String, u64)>,
    pub pending_left: usize,
    pub backlog_left: usize,
    pub last_panic: Option<String>,
    pub at_ms: u64,
}

fn drain_code(v: &DrainStateValue) -> DrainStateCode {
    match v {
        DrainStateValue::draining => DrainStateCode::Draining,
        DrainStateValue::retrying_transient => DrainStateCode::RetryingTransient,
        DrainStateValue::blocked_dead_letter_full => DrainStateCode::BlockedDeadLetterFull,
        DrainStateValue::stopped => DrainStateCode::Stopped,
    }
}

fn drain_value(c: DrainStateCode) -> DrainStateValue {
    match c {
        DrainStateCode::Draining => DrainStateValue::draining,
        DrainStateCode::RetryingTransient => DrainStateValue::retrying_transient,
        DrainStateCode::BlockedDeadLetterFull => DrainStateValue::blocked_dead_letter_full,
        DrainStateCode::Stopped => DrainStateValue::stopped,
    }
}

/// Written only by the drain thread and its supervisor (PR 2), except
/// `pause_requested` / `paused_by_uid`, which mirror DrainControl's intent
/// (PR 2, PR 6). Unpublished until the first `set_state`: evidence is then
/// `None` (unknown), never a healthy default.
pub(crate) struct DrainCell {
    state: AtomicU8, // 0 = unpublished, else DrainStateCode::as_u8() + 1
    pub(crate) state_since_ms: AtomicU64,
    pub(crate) pause_requested: AtomicBool,
    pub(crate) paused_by_uid: AtomicU64,
    pub(crate) paused_since_ms: AtomicU64,
    pub(crate) held: AtomicBool,
    pub(crate) observed_requeue: AtomicU64,
    pub(crate) generation: AtomicU64,
    pub(crate) in_flight: ArcSwapOption<InFlight>,
    pub(crate) commit_in_flight_since_ms: AtomicU64,
    pub(crate) last_progress_ms: AtomicU64,
    pub(crate) pending_len: AtomicUsize,
    pub(crate) backlog_len: AtomicUsize,
    pub(crate) replay_total: AtomicU64,
    pub(crate) replay_remaining: AtomicU64,
    pub(crate) blocked_since_ms: AtomicU64,
    pub(crate) stopping: AtomicBool,
    pub(crate) deadline_ms: AtomicU64,
    pub(crate) last_requeue: ArcSwapOption<RequeueResult>,
    pub(crate) exit: ArcSwapOption<DrainExitInfo>,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 2 (the drain) and PR 6 (pause uid) write these
impl DrainCell {
    pub(crate) fn new() -> Self {
        DrainCell {
            state: AtomicU8::new(0),
            state_since_ms: AtomicU64::new(NONE_MS),
            pause_requested: AtomicBool::new(false),
            paused_by_uid: AtomicU64::new(NO_UID),
            paused_since_ms: AtomicU64::new(NONE_MS),
            held: AtomicBool::new(false),
            observed_requeue: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            in_flight: ArcSwapOption::empty(),
            commit_in_flight_since_ms: AtomicU64::new(NONE_MS),
            last_progress_ms: AtomicU64::new(NONE_MS),
            pending_len: AtomicUsize::new(0),
            backlog_len: AtomicUsize::new(0),
            replay_total: AtomicU64::new(0),
            replay_remaining: AtomicU64::new(0),
            blocked_since_ms: AtomicU64::new(NONE_MS),
            stopping: AtomicBool::new(false),
            deadline_ms: AtomicU64::new(NONE_MS),
            last_requeue: ArcSwapOption::empty(),
            exit: ArcSwapOption::empty(),
        }
    }
    /// Stamps `state_since_ms` only on a change.
    pub(crate) fn set_state(&self, state: DrainStateValue) {
        let enc = drain_code(&state).as_u8() + 1;
        if self.state.swap(enc, Ordering::AcqRel) != enc {
            self.state_since_ms.store(unix_ms_now(), Ordering::Relaxed);
        }
    }
    pub(crate) fn state(&self) -> Option<DrainStateValue> {
        let enc = self.state.load(Ordering::Acquire);
        if enc == 0 {
            None
        } else {
            DrainStateCode::from_u8(enc - 1).map(drain_value)
        }
    }
    pub(crate) fn set_pause_requested(&self, requested: bool, by_uid: Option<u32>) {
        self.paused_by_uid
            .store(by_uid.map_or(NO_UID, u64::from), Ordering::Relaxed);
        self.pause_requested.store(requested, Ordering::Release);
    }
    /// `paused_since_ms == 0` means "not paused".
    pub(crate) fn set_paused(&self, paused_since_ms: u64, held: bool) {
        self.held.store(held, Ordering::Relaxed);
        self.paused_since_ms
            .store(paused_since_ms, Ordering::Release);
    }
    pub(crate) fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Relaxed);
    }
    pub(crate) fn set_in_flight(&self, in_flight: Option<InFlight>) {
        self.in_flight.store(in_flight.map(Arc::new));
    }
    /// CommitGuard construction (PR 2): marks a commit in flight and stamps progress.
    pub(crate) fn commit_started(&self) {
        let now = unix_ms_now();
        self.last_progress_ms.store(now, Ordering::Relaxed);
        self.commit_in_flight_since_ms.store(now, Ordering::Release);
    }
    /// CommitGuard drop (PR 2): clears the marker on every path, unwinding included.
    pub(crate) fn commit_finished(&self) {
        self.commit_in_flight_since_ms
            .store(NONE_MS, Ordering::Release);
        self.last_progress_ms
            .store(unix_ms_now(), Ordering::Relaxed);
    }
    pub(crate) fn stamp_progress(&self) {
        self.last_progress_ms
            .store(unix_ms_now(), Ordering::Relaxed);
    }
    pub(crate) fn set_queue_lens(&self, pending: usize, backlog: usize) {
        self.pending_len.store(pending, Ordering::Relaxed);
        self.backlog_len.store(backlog, Ordering::Relaxed);
    }
    pub(crate) fn set_replay(&self, total: u64, remaining: u64) {
        self.replay_total.store(total, Ordering::Relaxed);
        self.replay_remaining.store(remaining, Ordering::Relaxed);
    }
    /// 0 clears.
    pub(crate) fn set_blocked_since(&self, unix_ms: u64) {
        self.blocked_since_ms.store(unix_ms, Ordering::Relaxed);
    }
    pub(crate) fn set_stopping(&self, deadline_unix_ms: Option<u64>) {
        self.deadline_ms
            .store(deadline_unix_ms.unwrap_or(NONE_MS), Ordering::Relaxed);
        self.stopping.store(true, Ordering::Release);
    }
    pub(crate) fn set_last_requeue(&self, result: RequeueResult) {
        self.last_requeue.store(Some(Arc::new(result)));
    }
    pub(crate) fn set_observed_requeue(&self, ticket: u64) {
        self.observed_requeue.store(ticket, Ordering::Release);
    }
    pub(crate) fn set_exit(&self, info: DrainExitInfo) {
        self.exit.store(Some(Arc::new(info)));
    }

    /// `last_progress` = max(heartbeat change, CommitGuard stamps) (spec §9.3 code 13).
    /// `busy` is the 5 s poll task's reading of `DrainHeartbeat::busy()`, as
    /// published on the board (`StatusBoard::drain_busy_*`).
    pub(crate) fn evidence(
        &self,
        heartbeat_age_secs: Option<f64>,
        heartbeat_changed_ms: Option<u64>,
        busy: Option<BusyEvidence>,
    ) -> Option<DrainEvidence> {
        let enc = self.state.load(Ordering::Acquire);
        if enc == 0 {
            return None;
        }
        let state = DrainStateCode::from_u8(enc - 1)?;
        let paused_since = self.paused_since_ms.load(Ordering::Acquire);
        let uid = self.paused_by_uid.load(Ordering::Relaxed);
        let exit = self.exit.load_full();
        let last_progress =
            opt_ms(self.last_progress_ms.load(Ordering::Relaxed)).max(heartbeat_changed_ms);
        Some(DrainEvidence {
            state,
            state_since_unix_ms: self.state_since_ms.load(Ordering::Relaxed),
            pause_requested: self.pause_requested.load(Ordering::Acquire),
            paused: paused_since != NONE_MS,
            held: self.held.load(Ordering::Relaxed),
            paused_since_unix_ms: opt_ms(paused_since),
            paused_by_uid: (uid != NO_UID).then(|| u32::try_from(uid).ok()).flatten(),
            generation: self.generation.load(Ordering::Relaxed),
            observed_requeue: self.observed_requeue.load(Ordering::Acquire),
            in_flight: self.in_flight.load_full().and_then(|i| {
                Some(InFlightEvidence {
                    segment: SegmentId::parse(&i.segment)?,
                    attempt: i.attempt,
                    retries_left: i.retries_left,
                    processed_records: i.processed,
                    next_retry_at_unix_ms: i.next_retry_at_ms,
                })
            }),
            commit_in_flight_since_unix_ms: opt_ms(
                self.commit_in_flight_since_ms.load(Ordering::Acquire),
            ),
            busy,
            last_progress_unix_ms: last_progress,
            heartbeat_age_secs,
            pending_segments: self.pending_len.load(Ordering::Relaxed) as u64,
            backlog_segments: self.backlog_len.load(Ordering::Relaxed) as u64,
            replay_total: self.replay_total.load(Ordering::Relaxed),
            replay_remaining: self.replay_remaining.load(Ordering::Relaxed),
            blocked_since_unix_ms: opt_ms(self.blocked_since_ms.load(Ordering::Relaxed)),
            last_requeue: self.last_requeue.load_full().map(|r| RequeueEvidence {
                ticket: r.ticket,
                requeued: r.requeued,
                already_queued: r.already_queued,
                backoff_cut: r.backoff_cut,
                coalesced: r.coalesced,
                scan_error: r.scan_error.clone(),
                at_unix_ms: r.at_ms,
            }),
            exit: match exit.as_deref().map(|i| i.reason) {
                None => DrainExitCode::Running,
                Some(DrainExitReason::GaveUp) => DrainExitCode::GaveUp,
                Some(DrainExitReason::Stopped | DrainExitReason::DeadlineAbandoned) => {
                    DrainExitCode::Stopped
                }
            },
            last_panic: exit.as_deref().and_then(|i| i.last_panic.clone()),
        })
    }
}

// ── Sink ─────────────────────────────────────────────────────────────────

/// Written only by the drain thread (PR 2). Strings arrive already
/// redacted, stripped of control characters and capped (PR 2's
/// `drain::publish::sanitize_admin_text`). Unpublished until the first write.
pub(crate) struct SinkCell {
    published: AtomicBool,
    health: AtomicU8, // 0 = unknown, else SinkHealthCode::as_u8() + 1
    pub(crate) health_since_ms: AtomicU64,
    pub(crate) last_success_ms: AtomicU64,
    pub(crate) last_failure_ms: AtomicU64,
    last_failure_kind: AtomicU8, // 0 = none, else SinkFailureKind::as_u8() + 1
    pub(crate) consecutive_failures: AtomicU64,
    pub(crate) last_error: ArcSwapOption<String>,
    pub(crate) probe_reason: ArcSwapOption<String>,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 2 (the drain) writes these
impl SinkCell {
    pub(crate) fn new() -> Self {
        SinkCell {
            published: AtomicBool::new(false),
            health: AtomicU8::new(0),
            health_since_ms: AtomicU64::new(NONE_MS),
            last_success_ms: AtomicU64::new(NONE_MS),
            last_failure_ms: AtomicU64::new(NONE_MS),
            last_failure_kind: AtomicU8::new(0),
            consecutive_failures: AtomicU64::new(0),
            last_error: ArcSwapOption::empty(),
            probe_reason: ArcSwapOption::empty(),
        }
    }
    pub(crate) fn set_health(&self, health: &crate::sink::SinkHealth, reason: Option<String>) {
        let code = match health {
            crate::sink::SinkHealth::Healthy => SinkHealthCode::Healthy,
            crate::sink::SinkHealth::Down(_) => SinkHealthCode::Down,
            // `SinkHealth` is #[non_exhaustive]: anything newer is partial health.
            _ => SinkHealthCode::Degraded,
        };
        let enc = code.as_u8() + 1;
        if self.health.swap(enc, Ordering::AcqRel) != enc {
            self.health_since_ms.store(unix_ms_now(), Ordering::Relaxed);
        }
        self.probe_reason.store(reason.map(Arc::new));
        self.published.store(true, Ordering::Release);
    }
    pub(crate) fn record_success(&self) {
        self.last_success_ms.store(unix_ms_now(), Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.published.store(true, Ordering::Release);
    }
    pub(crate) fn record_failure(&self, kind: SinkFailureKind, message: String) {
        self.last_failure_ms.store(unix_ms_now(), Ordering::Relaxed);
        self.last_failure_kind
            .store(kind.as_u8() + 1, Ordering::Relaxed);
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.last_error.store(Some(Arc::new(message)));
        self.published.store(true, Ordering::Release);
    }
    pub(crate) fn evidence(&self) -> Option<SinkEvidence> {
        if !self.published.load(Ordering::Acquire) {
            return None;
        }
        let health = self.health.load(Ordering::Acquire);
        let kind = self.last_failure_kind.load(Ordering::Relaxed);
        Some(SinkEvidence {
            health: if health == 0 {
                None
            } else {
                SinkHealthCode::from_u8(health - 1)
            },
            health_since_unix_ms: opt_ms(self.health_since_ms.load(Ordering::Relaxed)),
            last_success_unix_ms: opt_ms(self.last_success_ms.load(Ordering::Relaxed)),
            last_failure_unix_ms: opt_ms(self.last_failure_ms.load(Ordering::Relaxed)),
            last_failure_kind: if kind == 0 {
                None
            } else {
                SinkFailureKind::from_u8(kind - 1)
            },
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            last_error: self.last_error.load_full().map(|s| (*s).clone()),
            probe_reason: self.probe_reason.load_full().map(|s| (*s).clone()),
        })
    }
}

// ── WAB (written only by the poll task) ──────────────────────────────────

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LagReport {
    pub undelivered_segments: u64,
    pub undelivered_records: u64,
    /// The oldest undelivered segment's header `created_at`, unix ms.
    pub oldest_created_ms: Option<u64>,
    /// `(shard, created_at unix ms)` of each active `.wab`.
    pub active_created_ms: Vec<(u16, u64)>,
    pub dead_letter_bytes: u64,
    pub dead_letter_files: u64,
    pub quarantine_bytes: u64,
    pub quarantine_files: u64,
    pub scanned_at_ms: u64,
}

pub(crate) struct WabCell {
    lag: ArcSwapOption<LagReport>,
    first_scan_ok_ms: AtomicU64,
    last_scan_ok_ms: AtomicU64,
    growth: AtomicU8, // 0 unknown, 1 no, 2 yes
}

impl WabCell {
    pub(crate) fn new() -> Self {
        WabCell {
            lag: ArcSwapOption::empty(),
            first_scan_ok_ms: AtomicU64::new(NONE_MS),
            last_scan_ok_ms: AtomicU64::new(NONE_MS),
            growth: AtomicU8::new(0),
        }
    }
    pub(crate) fn note_scan_ok(&self, now_ms: u64) {
        let _ = self.first_scan_ok_ms.compare_exchange(
            NONE_MS,
            now_ms,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.last_scan_ok_ms.store(now_ms, Ordering::Release);
    }
    pub(crate) fn first_scan_ok_ms(&self) -> Option<u64> {
        opt_ms(self.first_scan_ok_ms.load(Ordering::Relaxed))
    }
    pub(crate) fn last_scan_ok_ms(&self) -> Option<u64> {
        opt_ms(self.last_scan_ok_ms.load(Ordering::Acquire))
    }
    pub(crate) fn set_lag(&self, lag: LagReport) {
        self.lag.store(Some(Arc::new(lag)));
    }
    pub(crate) fn lag(&self) -> Option<Arc<LagReport>> {
        self.lag.load_full()
    }
    pub(crate) fn set_growth(&self, growing: bool) {
        self.growth
            .store(if growing { 2 } else { 1 }, Ordering::Relaxed);
    }
    pub(crate) fn growth(&self) -> Option<bool> {
        match self.growth.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v == 2),
        }
    }
}

// ── Admin and reload (PR 6 writes) ───────────────────────────────────────

pub(crate) struct AdminCell {
    enabled: AtomicBool,
    state: AtomicU8,
    reason: AtomicU8, // u8::MAX = none
    path: ArcSwapOption<String>,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 6 (admin socket) writes these
impl AdminCell {
    pub(crate) fn new() -> Self {
        AdminCell {
            enabled: AtomicBool::new(false),
            state: AtomicU8::new(AdminState::Disabled.as_u8()),
            reason: AtomicU8::new(u8::MAX),
            path: ArcSwapOption::empty(),
        }
    }
    pub(crate) fn set_bound(&self, path: String) {
        self.path.store(Some(Arc::new(path)));
        self.reason.store(u8::MAX, Ordering::Relaxed);
        self.enabled.store(true, Ordering::Relaxed);
        self.state
            .store(AdminState::Bound.as_u8(), Ordering::Release);
    }
    pub(crate) fn set_unavailable(&self, reason: AdminReason) {
        self.reason.store(reason.as_u8(), Ordering::Relaxed);
        self.enabled.store(true, Ordering::Relaxed);
        self.state
            .store(AdminState::Unavailable.as_u8(), Ordering::Release);
    }
    pub(crate) fn set_disabled(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        self.state
            .store(AdminState::Disabled.as_u8(), Ordering::Release);
    }
    pub(crate) fn evidence(&self) -> AdminEvidence {
        AdminEvidence {
            enabled: self.enabled.load(Ordering::Relaxed),
            state: AdminState::from_u8(self.state.load(Ordering::Acquire))
                .unwrap_or(AdminState::Disabled),
            reason: AdminReason::from_u8(self.reason.load(Ordering::Relaxed)),
            path: self.path.load_full().map(|p| (*p).clone()),
        }
    }
}

pub(crate) struct ReloadCell {
    last: ArcSwapOption<ReloadReport>,
    at_ms: AtomicU64,
    tls: AtomicU8, // 0 never attempted, 1 last attempt ok, 2 last attempt failed
}

#[cfg_attr(not(test), allow(dead_code))] // PR 6 (Reloader) publishes
impl ReloadCell {
    pub(crate) fn new() -> Self {
        ReloadCell {
            last: ArcSwapOption::empty(),
            at_ms: AtomicU64::new(NONE_MS),
            tls: AtomicU8::new(0),
        }
    }
    /// `tls_reload_failed` is sticky until a success (spec §9.3 code 29):
    /// only a report that names "tls" moves the TLS state.
    pub(crate) fn publish(&self, report: ReloadReport, now_ms: u64) {
        if report.changed.contains(&"tls") {
            self.tls.store(1, Ordering::Relaxed);
        } else if report.rejected.iter().any(|(item, _)| *item == "tls") {
            self.tls.store(2, Ordering::Relaxed);
        }
        self.at_ms.store(now_ms, Ordering::Relaxed);
        self.last.store(Some(Arc::new(report)));
    }
    pub(crate) fn evidence(&self) -> Option<ReloadEvidence> {
        let r = self.last.load_full()?;
        let mut items: Vec<ReloadItemEvidence> = r
            .changed
            .iter()
            .filter(|i| **i == "tls")
            .map(|_| ReloadItemEvidence {
                item: ReloadItem::TlsCertificates,
                outcome: ReloadItemOutcome::Reloaded,
                detail: None,
            })
            .collect();
        items.extend(
            r.rejected
                .iter()
                .filter(|(i, _)| *i == "tls")
                .map(|(_, why)| ReloadItemEvidence {
                    item: ReloadItem::TlsCertificates,
                    outcome: ReloadItemOutcome::Failed,
                    detail: Some(why.clone()),
                }),
        );
        Some(ReloadEvidence {
            trigger: match r.trigger {
                ReloadTrigger::Sighup => ReloadTriggerCode::Sighup,
                ReloadTrigger::Admin => ReloadTriggerCode::Admin,
            },
            at_unix_ms: self.at_ms.load(Ordering::Relaxed),
            outcome: match r.outcome {
                ReloadOutcome::Ok => ReloadOutcomeCode::Applied,
                ReloadOutcome::Noop => ReloadOutcomeCode::Noop,
                ReloadOutcome::Failed => ReloadOutcomeCode::Failed,
            },
            tls_last_failed: self.tls.load(Ordering::Relaxed) == 2,
            items,
            not_reloaded: vec![NotReloadedEvidence {
                item: ReloadItem::ConfigFile,
                reason_code: NotReloadedReason::RestartRequired,
            }],
        })
    }
}

// ── Condition latches (written only by the poll task) ────────────────────

/// First-seen unix ms of each active condition, indexed like `weir_status::ALL`.
pub(crate) struct ConditionLatches(Box<[AtomicU64]>);

impl ConditionLatches {
    pub(crate) fn new() -> Self {
        ConditionLatches(ALL.iter().map(|_| AtomicU64::new(NONE_MS)).collect())
    }
    fn index(code: ConditionCode) -> Option<usize> {
        ALL.iter().position(|c| *c == code)
    }
    pub(crate) fn update(&self, eval: &Evaluation, now_ms: u64) {
        for (i, code) in ALL.iter().enumerate() {
            let cell = &self.0[i];
            if eval.is_active(*code) {
                let _ = cell.compare_exchange(
                    NONE_MS,
                    now_ms.max(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            } else {
                cell.store(NONE_MS, Ordering::Relaxed);
            }
        }
    }
}

impl SinceLookup for ConditionLatches {
    fn since(&self, code: ConditionCode) -> Option<u64> {
        Self::index(code).and_then(|i| opt_ms(self.0[i].load(Ordering::Relaxed)))
    }
}

// ── The board ────────────────────────────────────────────────────────────

pub(crate) struct StaticInfo {
    pub version: &'static str,
    pub wire_version: u8,
    pub pid: u32,
    pub started_at_ms: u64,
    pub wab_dir: PathBuf,
    pub socket_path: PathBuf,
    pub wab_dir_dev: u64,
    pub wab_dir_ino: u64,
    pub config: Arc<ConfigView>,
}

impl StaticInfo {
    pub(crate) fn capture(config: &Config, view: Arc<ConfigView>) -> Self {
        #[cfg(unix)]
        let (dev, ino) = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&config.wab_dir)
                .map(|m| (m.dev(), m.ino()))
                .unwrap_or((0, 0))
        };
        #[cfg(not(unix))]
        let (dev, ino) = (0, 0);
        StaticInfo {
            version: env!("CARGO_PKG_VERSION"),
            wire_version: weir_core::WIRE_VERSION,
            pid: std::process::id(),
            started_at_ms: unix_ms_now(),
            wab_dir: config.wab_dir.clone(),
            socket_path: config.socket_path.clone(),
            wab_dir_dev: dev,
            wab_dir_ino: ino,
            config: view,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        StaticInfo {
            version: env!("CARGO_PKG_VERSION"),
            wire_version: weir_core::WIRE_VERSION,
            pid: std::process::id(),
            started_at_ms: unix_ms_now(),
            wab_dir: PathBuf::from("/var/lib/weir/wab"),
            socket_path: PathBuf::from("/run/weir/weir.sock"),
            wab_dir_dev: 1,
            wab_dir_ino: 2,
            config: Arc::new(ConfigView::empty_for_test()),
        }
    }
}

/// The wait-free live state (spec §5.2). No channel handle of any kind:
/// depth arrives as `len()` snapshots in `drain_depth` (PR 2/3 write it).
pub(crate) struct StatusBoard {
    pub(crate) statik: StaticInfo,
    pub(crate) life: LifecycleCell,
    pub(crate) listeners: ListenerCells,
    pub(crate) shards: Arc<ShardRegistry>,
    pub(crate) drain: DrainCell,
    pub(crate) sink: SinkCell,
    pub(crate) wab: WabCell,
    pub(crate) admin: AdminCell,
    pub(crate) reload: ReloadCell,
    pub(crate) latches: ConditionLatches,
    pub(crate) recent: ArcSwap<RecentCounters>,
    /// `QueueSender::len()` from the 500 ms task; `QUEUE_UNPUBLISHED` before it.
    pub(crate) queue_depth: AtomicU64,
    /// Drain-channel `len()` snapshots; `DEPTH_UNPUBLISHED` until PR 3's writers.
    pub(crate) drain_depth: Arc<AtomicUsize>,
    /// When the depth rose to >= the high-water threshold (500 ms task); 0 = below.
    pub(crate) channel_high_since_ms: AtomicU64,
    /// When the drain heartbeat last moved (5 s poll task); 0 = unknown.
    pub(crate) heartbeat_changed_ms: AtomicU64,
    /// When the drain entered its current busy operation (5 s poll task, from
    /// `DrainHeartbeat::busy()`); 0 = between operations or unknown. Written
    /// after `drain_busy_sink_call`, so a reader that sees a non-zero stamp
    /// sees that tick's kind (or, at worst, the neighbouring operation's, the
    /// same tolerance `DrainHeartbeat::busy` documents).
    pub(crate) drain_busy_since_ms: AtomicU64,
    /// The busy operation is a sink call (`BusyKind::is_sink_call`).
    pub(crate) drain_busy_sink_call: AtomicBool,
    pub(crate) wab_cap_rejecting: Arc<AtomicBool>,
    pub(crate) wab_bytes_now: Arc<AtomicU64>,
}

impl StatusBoard {
    pub(crate) fn new(
        statik: StaticInfo,
        shard_count: usize,
        worker_count: usize,
        tcp_configured: bool,
        wab_cap_rejecting: Arc<AtomicBool>,
        wab_bytes_now: Arc<AtomicU64>,
    ) -> Arc<Self> {
        let life = LifecycleCell::new(statik.started_at_ms);
        let shards = Arc::new(ShardRegistry::new(
            shard_count,
            worker_count,
            life.shutting_down_flag(),
        ));
        Arc::new(StatusBoard {
            statik,
            life,
            listeners: ListenerCells::new(cfg!(unix), tcp_configured),
            shards,
            drain: DrainCell::new(),
            sink: SinkCell::new(),
            wab: WabCell::new(),
            admin: AdminCell::new(),
            reload: ReloadCell::new(),
            latches: ConditionLatches::new(),
            recent: ArcSwap::from_pointee(RecentCounters::default()),
            queue_depth: AtomicU64::new(QUEUE_UNPUBLISHED),
            drain_depth: Arc::new(AtomicUsize::new(DEPTH_UNPUBLISHED)),
            channel_high_since_ms: AtomicU64::new(NONE_MS),
            heartbeat_changed_ms: AtomicU64::new(NONE_MS),
            drain_busy_since_ms: AtomicU64::new(NONE_MS),
            drain_busy_sink_call: AtomicBool::new(false),
            wab_cap_rejecting,
            wab_bytes_now,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
        Self::for_test_with(1, 1)
    }

    #[cfg(test)]
    pub(crate) fn for_test_with(shards: usize, workers: usize) -> Arc<Self> {
        Self::new(
            StaticInfo::for_test(),
            shards,
            workers,
            false,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::DrainStateValue;
    use weir_status::*;

    #[test]
    fn an_unpublished_drain_cell_is_unknown_not_healthy() {
        let d = DrainCell::new();
        assert!(d.evidence(None, None, None).is_none());
        assert_eq!(d.state(), None);
    }

    #[test]
    fn the_drain_cell_publishes_state_progress_and_exit() {
        let d = DrainCell::new();
        d.set_state(DrainStateValue::retrying_transient);
        let since = d.state_since_ms.load(Ordering::Relaxed);
        d.set_state(DrainStateValue::retrying_transient);
        assert_eq!(
            d.state_since_ms.load(Ordering::Relaxed),
            since,
            "since moves only on a change"
        );
        d.set_in_flight(Some(InFlight {
            segment: "shard_02/seg_00000007.wab.sealed".into(),
            attempt: 2,
            retries_left: 1,
            processed: 64,
            next_retry_at_ms: Some(9),
        }));
        d.commit_started();
        let busy = BusyEvidence {
            sink_call: true,
            since_unix_ms: 4,
        };
        let e = d.evidence(Some(1.5), Some(5), Some(busy)).unwrap();
        assert_eq!(
            e.busy,
            Some(busy),
            "the poll task's busy reading passes through"
        );
        assert_eq!(e.state, DrainStateCode::RetryingTransient);
        assert_eq!(
            e.in_flight.unwrap().segment,
            SegmentId {
                shard: 2,
                counter: 7
            }
        );
        assert!(e.commit_in_flight_since_unix_ms.is_some());
        assert!(
            e.last_progress_unix_ms.unwrap() >= 5,
            "max(heartbeat change, commit stamps)"
        );
        d.commit_finished();
        assert!(
            d.evidence(None, None, None)
                .unwrap()
                .commit_in_flight_since_unix_ms
                .is_none()
        );
        d.set_paused(123, true);
        d.set_pause_requested(true, Some(1000));
        let e = d.evidence(None, None, None).unwrap();
        assert!(e.paused && e.held && e.pause_requested);
        assert_eq!(
            (e.paused_since_unix_ms, e.paused_by_uid),
            (Some(123), Some(1000))
        );
        d.set_exit(DrainExitInfo {
            reason: DrainExitReason::GaveUp,
            abandoned: None,
            pending_left: 0,
            backlog_left: 0,
            last_panic: Some("boom".into()),
            at_ms: 1,
        });
        let e = d.evidence(None, None, None).unwrap();
        assert_eq!(e.exit, DrainExitCode::GaveUp);
        assert_eq!(e.last_panic.as_deref(), Some("boom"));
        d.set_exit(DrainExitInfo {
            reason: DrainExitReason::DeadlineAbandoned,
            abandoned: None,
            pending_left: 1,
            backlog_left: 2,
            last_panic: None,
            at_ms: 2,
        });
        assert_eq!(
            d.evidence(None, None, None).unwrap().exit,
            DrainExitCode::Stopped
        );
    }

    #[test]
    fn the_sink_cell_tracks_health_success_and_failure_streaks() {
        let s = SinkCell::new();
        assert!(s.evidence().is_none());
        s.set_health(
            &crate::sink::SinkHealth::Down("refused".into()),
            Some("refused".into()),
        );
        s.record_failure(SinkFailureKind::Timeout, "t1".into());
        s.record_failure(SinkFailureKind::Transient, "t2".into());
        let e = s.evidence().unwrap();
        assert_eq!(e.health, Some(SinkHealthCode::Down));
        assert_eq!(e.consecutive_failures, 2);
        assert_eq!(e.last_failure_kind, Some(SinkFailureKind::Transient));
        assert_eq!(
            (e.last_error.as_deref(), e.probe_reason.as_deref()),
            (Some("t2"), Some("refused"))
        );
        s.record_success();
        assert_eq!(s.evidence().unwrap().consecutive_failures, 0);
    }

    #[test]
    fn latches_keep_the_first_seen_time_until_the_condition_clears() {
        let l = ConditionLatches::new();
        let active = |codes: &[ConditionCode]| Evaluation {
            conditions: codes
                .iter()
                .map(|&code| Condition {
                    code,
                    severity: Severity::Warning,
                    affects: Affects::Ingest,
                    since_unix_ms: None,
                    summary: String::new(),
                    detail: None,
                    evidence: Default::default(),
                    runbook: "".into(),
                    gates: vec![],
                })
                .collect(),
            unevaluated: vec![],
        };
        l.update(&active(&[ConditionCode::SinkDown]), 100);
        l.update(&active(&[ConditionCode::SinkDown]), 200);
        assert_eq!(l.since(ConditionCode::SinkDown), Some(100));
        l.update(&active(&[]), 300);
        assert_eq!(l.since(ConditionCode::SinkDown), None);
        assert_eq!(l.since(ConditionCode::Unknown), None);
    }

    #[test]
    fn the_reload_cell_is_sticky_on_a_tls_failure_until_a_success() {
        use crate::reload::{ReloadOutcome, ReloadReport, ReloadTrigger};
        let r = ReloadCell::new();
        assert!(r.evidence().is_none());
        let report = |outcome,
                      changed: Vec<&'static str>,
                      rejected: Vec<(&'static str, String)>| ReloadReport {
            trigger: ReloadTrigger::Sighup,
            outcome,
            changed,
            rejected,
            note: None,
        };
        r.publish(
            report(
                ReloadOutcome::Failed,
                vec![],
                vec![("tls", "bad key".into())],
            ),
            10,
        );
        let e = r.evidence().unwrap();
        assert!(e.tls_last_failed);
        assert_eq!(e.items[0].detail.as_deref(), Some("bad key"));
        r.publish(report(ReloadOutcome::Noop, vec![], vec![]), 20);
        assert!(
            r.evidence().unwrap().tls_last_failed,
            "a noop does not clear it"
        );
        r.publish(report(ReloadOutcome::Ok, vec!["tls"], vec![]), 30);
        assert!(!r.evidence().unwrap().tls_last_failed);
        assert_eq!(
            r.evidence().unwrap().not_reloaded[0].item,
            ReloadItem::ConfigFile
        );
    }

    #[test]
    fn a_new_board_starts_initializing_with_unpublished_depths() {
        let b = StatusBoard::for_test_with(2, 1);
        assert_eq!(b.life.phase(), Phase::Initializing);
        assert_eq!(b.drain_depth.load(Ordering::Relaxed), DEPTH_UNPUBLISHED);
        assert_eq!(b.queue_depth.load(Ordering::Relaxed), QUEUE_UNPUBLISHED);
        assert_eq!(b.shards.shard_count(), 2);
        assert_eq!(b.admin.evidence().state, AdminState::Disabled);
        assert_eq!(*b.recent.load_full(), RecentCounters::default());
    }

    #[test]
    fn entering_serving_marks_ever_served_and_stamps_the_phase() {
        let life = LifecycleCell::new(1_000);
        assert_eq!(life.phase(), Phase::Initializing);
        assert!(!life.ever_served());
        assert!(life.enter(Phase::Recovering, 2_000));
        assert_eq!(
            (life.phase(), life.phase_since_ms()),
            (Phase::Recovering, 2_000)
        );
        assert!(life.enter(Phase::Serving, 3_000));
        assert!(life.ever_served());
    }

    #[test]
    fn nothing_leaves_shutting_down() {
        let life = LifecycleCell::new(0);
        life.enter(Phase::Binding, 1);
        assert!(life.begin_shutdown(5));
        assert!(
            !life.enter(Phase::Serving, 6),
            "a late startup step must not resurrect serving"
        );
        assert_eq!(life.phase(), Phase::ShuttingDown);
        assert!(!life.ever_served());
        assert_eq!(life.shutting_down_since_ms(), Some(5));
    }

    #[test]
    fn begin_shutdown_is_first_caller_wins_and_shares_its_flag() {
        let life = LifecycleCell::new(0);
        let flag = life.shutting_down_flag();
        assert!(!flag.load(Ordering::Acquire));
        assert!(life.begin_shutdown(10));
        assert!(!life.begin_shutdown(20));
        assert!(
            flag.load(Ordering::Acquire),
            "the registry sees the same flag"
        );
        assert_eq!(life.shutting_down_since_ms(), Some(10));
        assert_eq!(life.drain_deadline_ms(), None);
        life.set_drain_deadline(30_000, 10);
        assert_eq!(
            (life.drain_deadline_ms(), life.drain_budget_secs()),
            (Some(30_000), Some(10))
        );
    }

    #[test]
    fn shutting_down_and_forced_are_observable_and_sticky() {
        let life = LifecycleCell::new(0);
        assert!(!life.is_shutting_down());
        assert!(!life.forced());
        assert_eq!(life.drain_budget_secs(), None);
        life.begin_shutdown(1);
        life.mark_forced();
        assert!(life.is_shutting_down());
        assert!(life.forced());
        assert!(!life.begin_shutdown(2));
        assert!(life.forced(), "a later shutdown call does not clear forced");
    }

    #[test]
    fn listener_evidence_skips_unconfigured_listeners() {
        let l = ListenerCells::new(true, false);
        assert_eq!(
            l.evidence(),
            vec![ListenerEvidence {
                kind: ListenerKind::Unix,
                state: ListenerState::Pending
            }]
        );
        l.unix.set(ListenerState::Bound);
        l.tcp.set(ListenerState::Bound);
        assert_eq!(l.evidence().len(), 2);
        assert_eq!(l.unix.get(), ListenerState::Bound);
    }
}
