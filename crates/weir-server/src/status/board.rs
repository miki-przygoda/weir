//! The status board's cells (spec §5.2). Every cell has one writer; readers
//! only load.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use weir_status::{ListenerEvidence, ListenerKind, ListenerState, Phase};

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

#[cfg(test)]
mod tests {
    use super::*;

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
