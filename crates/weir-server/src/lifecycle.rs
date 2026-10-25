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
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{info, warn};
use weir_status::Phase;

use crate::drain::DrainExit;
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

    /// Installs the SIGTERM and SIGINT handlers (synchronously, so they are in
    /// place when this returns) and spawns the task that turns EVERY delivery
    /// into a `request_shutdown`. Replaces the one-shot task that took the
    /// first signal and then exited, after which a second SIGTERM was
    /// swallowed. Must be called inside the runtime. tokio coalesces
    /// deliveries that arrive before the task is polled, so two signals sent
    /// microseconds apart may count as one.
    #[cfg(unix)]
    pub(crate) fn spawn_signal_task(self: &Arc<Self>) -> std::io::Result<()> {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())?;
        let mut int = signal(SignalKind::interrupt())?;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let cause = tokio::select! {
                    s = term.recv() => match s { Some(()) => "SIGTERM", None => break },
                    s = int.recv() => match s { Some(()) => "SIGINT", None => break },
                };
                me.request_shutdown(cause);
            }
        });
        Ok(())
    }

    /// Non-Unix builds have no producer listener; Ctrl-C still escalates.
    #[cfg(not(unix))]
    pub(crate) fn spawn_signal_task(self: &Arc<Self>) -> std::io::Result<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            while tokio::signal::ctrl_c().await.is_ok() {
                me.request_shutdown("Ctrl-C");
            }
        });
        Ok(())
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

// ── Bounded shutdown: the drain wait and the exit summary (§7 T2-T3) ─────────

/// How long main waits for the drain thread past its deadline before taking
/// the orphan exit (§7 T2).
pub(crate) const DRAIN_EXIT_GRACE: Duration = Duration::from_secs(2);
/// `is_finished` poll cadence during that wait.
pub(crate) const DRAIN_JOIN_POLL: Duration = Duration::from_millis(50);
/// Startup step 10: how long to wait for every shard to leave `Starting`
/// (and every worker to start) before the producer listeners are bound.
/// Unsettled shards stay `Starting` and are not routed to.
pub(crate) const SETTLE_WAIT: Duration = Duration::from_secs(10);
/// T1: how long a work-queue sender may outlive ingest. A connection task
/// aborted mid-push can hold one inside `spawn_blocking` for up to
/// `QUEUE_PUSH_TIMEOUT` (5 s, socket/connection.rs).
pub(crate) const QUEUE_SENDERS_GRACE: Duration = Duration::from_secs(6);

/// Polls `done` every `poll` until it returns true (-> true) or `max` passes
/// (-> false). `done` is always called at least once. Blocking; for main's
/// thread only. (Moved from PR 3's main.rs.)
pub(crate) fn wait_until(max: Duration, poll: Duration, mut done: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + max;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(poll);
    }
}

/// How the bounded drain wait ended.
#[derive(Debug)]
pub(crate) enum DrainWait<T> {
    /// The drain thread ended; `None` if it panicked past its supervisor.
    Joined(Option<T>),
    /// Still running one grace past its deadline: a sink call that never
    /// yields. The handle is dropped (the thread is detached) and main takes
    /// the orphan exit.
    Orphaned,
}

/// Waits for the drain thread: until it ends, or until `grace` after the
/// DrainControl deadline. The deadline is re-read on every pass, because a
/// second signal lowers it (`force_stop`'s `fetch_min`) while main is already
/// waiting, and the wait must shrink with it (§7 "second signal").
///
/// Only polls `JoinHandle::is_finished` and an atomic deadline: it never
/// touches the drain's state, so it cannot block the drain.
pub(crate) fn wait_for_drain<T>(
    handle: std::thread::JoinHandle<T>,
    ctl: &DrainControl,
    grace: Duration,
    poll: Duration,
) -> DrainWait<T> {
    // When this wait first saw the deadline as passed; the grace runs from
    // there, so a deadline lowered mid-wait starts the grace at once.
    let mut deadline_seen: Option<Instant> = None;
    loop {
        if handle.is_finished() {
            return DrainWait::Joined(handle.join().ok());
        }
        // None means no stop was requested, which main never does here; treat
        // it as an expired deadline so the wait stays bounded regardless.
        let left = ctl.time_left().unwrap_or(Duration::ZERO);
        if left.is_zero() {
            let at = *deadline_seen.get_or_insert_with(Instant::now);
            if at.elapsed() >= grace {
                return DrainWait::Orphaned;
            }
        }
        // Inside the grace window poll at `poll`; before the deadline, never
        // oversleep it.
        std::thread::sleep(if left.is_zero() { poll } else { poll.min(left) });
    }
}

/// When the last WAB size scan ran, and what it found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OnDisk {
    pub bytes: u64,
    pub as_of_unix_ms: u64,
}

/// Everything the T3 exit line reports, gathered WITHOUT new I/O (§7 T3): no
/// `scan_unconfirmed_sealed`, which can quarantine through `check_confirmed`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShutdownFacts {
    /// False on the orphan exit: the drain thread never returned its
    /// `DrainExit`, so pending and backlog counts are unknown (reported as 0).
    pub drain_joined: bool,
    pub pending_left: usize,
    pub backlog_left: usize,
    /// The segment abandoned at the deadline and its committed record count.
    pub abandoned: Option<(String, u64)>,
    /// `weir_wab_segments_not_handed_off_total` at exit.
    pub not_handed_off: u64,
    pub on_disk: Option<OnDisk>,
}

impl ShutdownFacts {
    /// The drain joined. `exit` is `None` when it panicked past its
    /// supervisor and returned no `DrainExit`.
    pub(crate) fn from_exit(
        exit: Option<&DrainExit>,
        not_handed_off: u64,
        on_disk: Option<OnDisk>,
    ) -> Self {
        Self {
            drain_joined: true,
            pending_left: exit.map_or(0, |e| e.pending_left),
            backlog_left: exit.map_or(0, |e| e.backlog_left),
            abandoned: exit.and_then(|e| e.abandoned.clone()),
            not_handed_off,
            on_disk,
        }
    }

    /// The orphan exit: `in_flight` is what the board's `drain.in_flight` last
    /// showed (an atomic load, no I/O).
    pub(crate) fn orphaned(
        in_flight: Option<(String, u64)>,
        not_handed_off: u64,
        on_disk: Option<OnDisk>,
    ) -> Self {
        Self {
            drain_joined: false,
            pending_left: 0,
            backlog_left: 0,
            abandoned: in_flight,
            not_handed_off,
            on_disk,
        }
    }

    /// Segments the next start replays: pending + backlog + not handed off,
    /// plus the abandoned one.
    pub(crate) fn left_for_replay(&self) -> u64 {
        (self.pending_left as u64)
            .saturating_add(self.backlog_left as u64)
            .saturating_add(self.not_handed_off)
            .saturating_add(u64::from(self.abandoned.is_some()))
    }
}

/// The one exit line (§7 T3). States the duplicate window; never a count of
/// dead-lettered records (DrainExit does not carry one; README Q5).
pub(crate) fn shutdown_summary(f: &ShutdownFacts) -> String {
    use std::fmt::Write as _;

    let left = f.left_for_replay();
    if left == 0 {
        return "shutdown: every handed-off segment was delivered; nothing is left for replay"
            .to_string();
    }
    let mut s = format!("shutdown: {left} segment(s) left sealed+unconfirmed for replay");
    // `write!` into a String cannot fail.
    if let Some(d) = &f.on_disk {
        let _ = write!(s, " (WAB ~{} bytes, as of {})", d.bytes, d.as_of_unix_ms);
    }
    if let Some((seg, n)) = &f.abandoned {
        let _ = write!(
            s,
            "; abandoned {seg} after {n} records: those records will be re-delivered on \
             replay, and any of them the sink rejected permanently will be dead-lettered again"
        );
    }
    if f.not_handed_off > 0 {
        let _ = write!(
            s,
            "; {} segment(s) were not handed off before the 1 s budget and are durable on disk",
            f.not_handed_off
        );
    }
    s
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

    /// Two SIGTERMs to this very process: Graceful, then Forced. The handler
    /// is installed (synchronously) before the first kill, so the test binary
    /// is never terminated by the default disposition.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_sigterm_escalates_graceful_then_forced() {
        let f = fx();
        f.lc.spawn_signal_task()
            .expect("install the SIGTERM/SIGINT handlers");
        let mut rx = f.lc.subscribe();
        // SAFETY: kill(2) on our own pid with a handler installed above.
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
        let level = tokio::time::timeout(Duration::from_secs(1), shutdown_requested(&mut rx))
            .await
            .expect("the first SIGTERM must reach the lifecycle");
        assert_eq!(level, ShutdownLevel::Graceful);
        // tokio coalesces signals that arrive before the task is polled.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // SAFETY: as above.
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
        tokio::time::timeout(Duration::from_secs(1), forced(&mut rx))
            .await
            .expect("a second SIGTERM must force, not be swallowed");
    }

    use std::time::Instant;

    #[test]
    fn wait_for_drain_returns_as_soon_as_the_drain_ends() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_secs(5));
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            7u32
        });
        let t0 = Instant::now();
        match wait_for_drain(h, &ctl, Duration::from_secs(2), Duration::from_millis(10)) {
            DrainWait::Joined(Some(7)) => {}
            other => panic!("expected Joined(Some(7)), got {other:?}"),
        }
        assert!(t0.elapsed() < Duration::from_millis(400));
    }

    #[test]
    fn wait_for_drain_gives_up_one_grace_after_the_deadline() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_millis(200));
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(10)); // a sink call that never yields
            0u32
        });
        let t0 = Instant::now();
        let w = wait_for_drain(
            h,
            &ctl,
            Duration::from_millis(300),
            Duration::from_millis(10),
        );
        let took = t0.elapsed();
        assert!(matches!(w, DrainWait::Orphaned), "{w:?}");
        assert!(
            took >= Duration::from_millis(450) && took < Duration::from_millis(900),
            "deadline (200 ms) + grace (300 ms), took {took:?}"
        );
    }

    #[test]
    fn a_force_stop_during_the_wait_shrinks_it() {
        let ctl = DrainControl::attached();
        ctl.request_stop(Duration::from_secs(10));
        let c2 = ctl.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            c2.force_stop(); // the second SIGTERM, during T2
        });
        let h = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(10));
            0u32
        });
        let t0 = Instant::now();
        let w = wait_for_drain(
            h,
            &ctl,
            Duration::from_millis(300),
            Duration::from_millis(10),
        );
        assert!(matches!(w, DrainWait::Orphaned));
        assert!(
            t0.elapsed() < Duration::from_millis(700),
            "force_stop must cut the 10 s wait to ~grace, took {:?}",
            t0.elapsed()
        );
    }

    #[test]
    fn wait_until_reports_success_and_timeout() {
        let t0 = Instant::now();
        assert!(wait_until(
            Duration::from_secs(1),
            Duration::from_millis(5),
            || { t0.elapsed() > Duration::from_millis(30) }
        ));
        assert!(!wait_until(
            Duration::from_millis(50),
            Duration::from_millis(5),
            || false
        ));
    }

    /// PR 3's test, moved with the function from main.rs.
    #[test]
    fn wait_until_returns_true_as_soon_as_done_and_false_at_the_deadline() {
        let mut calls = 0;
        assert!(wait_until(
            Duration::from_secs(5),
            Duration::from_millis(1),
            || {
                calls += 1;
                calls == 3
            }
        ));
        assert_eq!(calls, 3);

        let t0 = std::time::Instant::now();
        assert!(!wait_until(
            Duration::from_millis(60),
            Duration::from_millis(5),
            || false
        ));
        let took = t0.elapsed();
        assert!(
            took >= Duration::from_millis(60) && took < Duration::from_millis(500),
            "{took:?}"
        );
    }

    #[test]
    fn summary_names_what_is_left_and_the_duplicate_window() {
        let f = ShutdownFacts {
            drain_joined: true,
            pending_left: 2,
            backlog_left: 3,
            abandoned: Some(("shard_00/seg_00000007".to_string(), 41)),
            not_handed_off: 1,
            on_disk: Some(OnDisk {
                bytes: 1_048_576,
                as_of_unix_ms: 1_700_000_000_000,
            }),
        };
        assert_eq!(
            f.left_for_replay(),
            7,
            "2 pending + 3 backlog + 1 not handed off + 1 abandoned"
        );
        let s = shutdown_summary(&f);
        for needle in [
            "7 segment(s)",
            "left sealed+unconfirmed for replay",
            "~1048576 bytes",
            "as of 1700000000000",
            "abandoned shard_00/seg_00000007 after 41 records",
            "re-delivered on replay",
            "dead-lettered again",
            "1 segment(s) were not handed off",
        ] {
            assert!(s.contains(needle), "missing {needle:?} in: {s}");
        }
    }

    #[test]
    fn a_complete_drain_says_nothing_is_left() {
        let f = ShutdownFacts {
            drain_joined: true,
            pending_left: 0,
            backlog_left: 0,
            abandoned: None,
            not_handed_off: 0,
            on_disk: None,
        };
        assert_eq!(f.left_for_replay(), 0);
        assert_eq!(
            shutdown_summary(&f),
            "shutdown: every handed-off segment was delivered; nothing is left for replay"
        );
    }
}
