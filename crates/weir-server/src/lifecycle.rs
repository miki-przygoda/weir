//! Process lifecycle: the shutdown level and the signals that raise it
//! (Foundations §5.6). Not an actor: `request_shutdown` does its work inline
//! on whichever task calls it (the signal task), and everything else only
//! reads the level from a `watch` channel.
//!
//! The level only ever rises: Running -> Graceful (first SIGTERM/SIGINT) ->
//! Forced (second). A third signal only logs. SIGHUP is NOT handled here; it
//! stays on `reload::spawn_sighup_task`, the one reload path the safety floor
//! built and the admin `reload` verb reuses.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

use tokio::sync::watch;
use tracing::{info, warn};
use weir_status::Phase;

use crate::drain::control::DrainControl;
use crate::metrics::Metrics;
use crate::status::StatusBoard;

/// How far shutdown has gone. Ordered: a later level implies the earlier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ShutdownLevel {
    Running,
    /// First SIGTERM/SIGINT: lame duck, bounded drain (§7 T0).
    Graceful,
    /// Second signal: connection drains abort, the drain deadline is now
    /// (§7 "second signal"). The worker and flusher joins still run.
    Forced,
}

pub(crate) struct Lifecycle {
    level: watch::Sender<ShutdownLevel>,
    /// How many shutdown requests arrived; picks the escalation step.
    requests: AtomicU32,
    board: Arc<StatusBoard>,
    metrics: Arc<Metrics>,
    drain: DrainControl,
    /// Read by every flusher's hand-off loop (PR 3's `hand_off`), at most
    /// once per 100 ms of blocking.
    flushers_shutting_down: Arc<AtomicBool>,
}

impl Lifecycle {
    pub(crate) fn new(
        board: Arc<StatusBoard>,
        metrics: Arc<Metrics>,
        drain: DrainControl,
        flushers_shutting_down: Arc<AtomicBool>,
    ) -> Self {
        let (level, _rx) = watch::channel(ShutdownLevel::Running);
        Self {
            level,
            requests: AtomicU32::new(0),
            board,
            metrics,
            drain,
            flushers_shutting_down,
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ShutdownLevel> {
        self.level.subscribe()
    }

    pub(crate) fn level(&self) -> ShutdownLevel {
        *self.level.borrow()
    }

    /// One shutdown request (a signal). The first is Graceful, the second
    /// Forced, the rest only log. Returns the level now in force.
    pub(crate) fn request_shutdown(&self, cause: &'static str) -> ShutdownLevel {
        // Saturating, so a signal storm can never wrap the count back to 0
        // and re-run the Graceful step. A compare-exchange loop, not
        // `fetch_update` (deprecated on newer stable; MSRV 1.88).
        let mut seen = self.requests.load(Ordering::Acquire);
        loop {
            match self.requests.compare_exchange_weak(
                seen,
                seen.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => seen = actual,
            }
        }
        match seen {
            0 => {
                info!(
                    signal = cause,
                    "shutdown requested: lame duck, then a bounded drain"
                );
                // §7 T0, in order: (a) readiness and (b) phase plus its gauge
                // (PR 1's StatusBoard::begin_shutdown), (c) drain absorb,
                // (d) flushers, (e) the broadcast that starts the lame duck.
                self.board.begin_shutdown(&self.metrics);
                self.drain.begin_shutdown();
                self.flushers_shutting_down.store(true, Ordering::Release);
                // send_replace, not send: send stores nothing when no
                // receiver is alive, and main may subscribe later.
                self.level.send_replace(ShutdownLevel::Graceful);
                ShutdownLevel::Graceful
            }
            1 => {
                warn!(
                    signal = cause,
                    "forced shutdown: aborting open connections and the drain now; the \
                     worker and flusher joins still run, so every acked record stays on disk"
                );
                self.drain.force_stop();
                self.board.life.mark_forced();
                self.level.send_replace(ShutdownLevel::Forced);
                ShutdownLevel::Forced
            }
            _ => {
                info!(signal = cause, "shutdown already forced; signal ignored");
                ShutdownLevel::Forced
            }
        }
    }

    /// Publishes a startup phase on the board and the one-hot
    /// `weir_startup_phase` gauge. Returns the phase actually in force:
    /// `ShuttingDown` once shutdown began, whatever was asked.
    pub(crate) fn publish_phase(&self, phase: Phase) -> Phase {
        // PR 1: moves the phase and publishes the one-hot gauge, or refuses
        // (and publishes nothing) once ShuttingDown is stored.
        self.board.enter_phase(phase, &self.metrics);
        self.board.life.phase()
    }

    /// Resolves once the level leaves `Running`.
    pub(crate) async fn wait_shutdown(&self) -> ShutdownLevel {
        let mut rx = self.subscribe();
        shutdown_requested(&mut rx).await
    }
}

/// Resolves with the level once it is past `Running`. Uses `wait_for`, which
/// checks the current value first, never `changed()`: a signal that lands
/// between a caller's "already shutting down?" check and its first poll is
/// never missed (§5.6). A dropped sender reads as Graceful, which matches the
/// `oneshot` it replaces ("fires or is dropped").
pub(crate) async fn shutdown_requested(rx: &mut watch::Receiver<ShutdownLevel>) -> ShutdownLevel {
    match rx.wait_for(|l| *l != ShutdownLevel::Running).await {
        Ok(l) => *l,
        Err(_) => ShutdownLevel::Graceful,
    }
}

/// Resolves once the level is `Forced`. A dropped sender never forces: it
/// pends, so a connection drain then ends by its own timeout.
pub(crate) async fn forced(rx: &mut watch::Receiver<ShutdownLevel>) {
    if rx.wait_for(|l| *l == ShutdownLevel::Forced).await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{StartupPhaseLabel, StartupPhaseValue};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    struct Fx {
        lc: Arc<Lifecycle>,
        board: Arc<StatusBoard>,
        drain: DrainControl,
        flag: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
    }

    fn fx() -> Fx {
        let board = StatusBoard::for_test_with(1, 1);
        let (m, _reg) = Metrics::new();
        let metrics = Arc::new(m);
        let drain = DrainControl::attached();
        let flag = Arc::new(AtomicBool::new(false));
        let lc = Arc::new(Lifecycle::new(
            Arc::clone(&board),
            Arc::clone(&metrics),
            drain.clone(),
            Arc::clone(&flag),
        ));
        Fx {
            lc,
            board,
            drain,
            flag,
            metrics,
        }
    }

    fn gauge(m: &Metrics, v: StartupPhaseValue) -> f64 {
        m.startup_phase
            .get_or_create(&StartupPhaseLabel { phase: v })
            .get()
    }

    #[test]
    fn first_request_is_graceful_and_sets_every_t0_flag() {
        let f = fx();
        let rx = f.lc.subscribe();
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Graceful);
        assert_eq!(*rx.borrow(), ShutdownLevel::Graceful, "T0 (e): the watch");
        assert!(f.board.life.is_shutting_down(), "T0 (a)");
        assert_eq!(f.board.life.phase(), Phase::ShuttingDown, "T0 (b)");
        assert!(f.drain.absorbing(), "T0 (c): absorb mode");
        assert!(
            !f.drain.stop_requested(),
            "T0 must not stop the drain: its budget starts after the flusher joins"
        );
        assert!(f.flag.load(Ordering::Relaxed), "T0 (d): the flushers' flag");
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::shutting_down), 1.0);
        assert!(!f.board.life.forced());
    }

    #[test]
    fn second_request_forces_and_third_only_logs() {
        let f = fx();
        let rx = f.lc.subscribe();
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Graceful);
        assert_eq!(f.lc.request_shutdown("SIGTERM"), ShutdownLevel::Forced);
        assert!(f.drain.stop_requested(), "force_stop sets stop");
        assert!(
            f.drain.deadline_passed(),
            "force_stop sets the deadline to now"
        );
        assert!(f.board.life.forced());
        assert_eq!(*rx.borrow(), ShutdownLevel::Forced);
        assert_eq!(f.lc.request_shutdown("SIGINT"), ShutdownLevel::Forced);
        assert_eq!(f.lc.level(), ShutdownLevel::Forced);
    }

    #[test]
    fn a_forced_deadline_survives_main_s_later_request_stop() {
        let f = fx();
        f.lc.request_shutdown("SIGTERM");
        f.lc.request_shutdown("SIGTERM");
        // Main's T2 runs after the joins, i.e. after the second signal here.
        f.drain.request_stop(Duration::from_secs(10));
        assert!(
            f.drain.deadline_passed(),
            "fetch_min: request_stop must not raise a forced deadline"
        );
    }

    #[test]
    fn the_level_is_set_even_with_no_receiver_alive() {
        let f = fx();
        // watch::Sender::send fails (and does not store) with no receivers;
        // request_shutdown must use send_replace.
        f.lc.request_shutdown("SIGTERM");
        assert_eq!(f.lc.level(), ShutdownLevel::Graceful);
        assert_eq!(*f.lc.subscribe().borrow(), ShutdownLevel::Graceful);
    }

    #[tokio::test]
    async fn a_receiver_subscribed_after_the_signal_still_sees_it() {
        let f = fx();
        f.lc.request_shutdown("SIGTERM");
        let mut rx = f.lc.subscribe();
        let level = tokio::time::timeout(Duration::from_millis(50), shutdown_requested(&mut rx))
            .await
            .expect("wait_for checks the current value first; a late subscriber must not hang");
        assert_eq!(level, ShutdownLevel::Graceful);
    }

    #[tokio::test]
    async fn forced_resolves_only_on_the_second_signal() {
        let f = fx();
        let mut rx = f.lc.subscribe();
        f.lc.request_shutdown("SIGTERM");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), forced(&mut rx))
                .await
                .is_err(),
            "Graceful is not Forced"
        );
        f.lc.request_shutdown("SIGTERM");
        tokio::time::timeout(Duration::from_millis(50), forced(&mut rx))
            .await
            .expect("Forced must resolve");
    }

    #[test]
    fn a_phase_published_after_shutdown_cannot_overwrite_it() {
        let f = fx();
        assert_eq!(f.lc.publish_phase(Phase::Recovering), Phase::Recovering);
        f.lc.request_shutdown("SIGTERM");
        assert_eq!(f.lc.publish_phase(Phase::Handoff), Phase::ShuttingDown);
        assert_eq!(f.board.life.phase(), Phase::ShuttingDown);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::handoff), 0.0);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::shutting_down), 1.0);
        assert_eq!(
            f.lc.publish_phase(Phase::Serving),
            Phase::ShuttingDown,
            "serving must be refused once shutting down"
        );
    }

    #[test]
    fn startup_phase_gauge_is_one_hot() {
        let f = fx();
        for p in [
            Phase::Initializing,
            Phase::Recovering,
            Phase::Handoff,
            Phase::Binding,
        ] {
            f.lc.publish_phase(p);
            let ones = StartupPhaseValue::ALL
                .into_iter()
                .filter(|&v| gauge(&f.metrics, v) == 1.0)
                .count();
            assert_eq!(ones, 1, "exactly one phase series is 1 after {p:?}");
        }
        assert_eq!(f.lc.publish_phase(Phase::Serving), Phase::Serving);
        assert_eq!(gauge(&f.metrics, StartupPhaseValue::serving), 1.0);
        assert!(
            f.board.life.ever_served(),
            "PR 1's enter(Serving) sets ever_served"
        );
    }
}
