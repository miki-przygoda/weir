//! Drain intent (spec §5.4): what the lifecycle or an operator WANTS the drain
//! to do. The drain reads it; nothing here observes the drain. What the drain
//! has actually done is published to the status board (`drain::publish`).
//!
//! Every field is an atomic on one `Arc`, so a control handle is cheap to clone
//! and never blocks the drain. There is no channel handle here, by design
//! (spec §4): the drain owns the only Receiver of the drain channel.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
    },
    time::{Duration, Instant},
};

use crate::status::{DrainExitReason, StatusBoard, unix_ms_now};

/// At most one operator-driven stranded-segment rescan per this interval;
/// requests inside it are answered from the previous scan (`coalesced: true`).
pub(crate) const REQUEUE_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// The drain's wait slice, the floor on every wait while a pause is in force
/// (so a hold can never spin on `recv_timeout(0)`), and the deadline poll.
pub(crate) const SLICE: Duration = Duration::from_millis(50);

/// `deadline_ms` value meaning "no deadline".
const NO_DEADLINE: u64 = u64::MAX;

/// How the drain treats its channel disconnecting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lifetime {
    /// Production (main, since PR 4): `Disconnected` is a non-event; the drain
    /// exits only on `request_stop` / `force_stop` (or when its panic budget is
    /// spent).
    Attached,
    /// The pre-Foundations semantics, kept for the drain's own tests:
    /// `Disconnected` means "deliver what is queued, then exit".
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "production is Attached; the drain's tests use it")
    )]
    Detached,
}

struct Inner {
    mode: Lifetime,
    /// Origin of `deadline_ms`; monotonic, so a wall-clock step cannot move a deadline.
    epoch: Instant,
    /// Wall-clock reading taken with `epoch`, to publish the deadline as unix ms.
    epoch_unix_ms: u64,
    absorb: AtomicBool,
    stop: AtomicBool,
    input_closed: AtomicBool,
    /// Milliseconds since `epoch`; `NO_DEADLINE` = none. Only ever lowered (`fetch_min`).
    deadline_ms: AtomicU64,
    pause: AtomicBool,
    pause_seq: AtomicU64,
    /// The newest requeue ticket handed out.
    requeue_req: AtomicU64,
    /// The newest ticket the drain has resolved.
    requeue_seen: AtomicU64,
    /// One bump per `drain_thread_with` start.
    generation: AtomicU64,
}

/// Shared drain intent. Clone freely; every clone is the same cell.
#[derive(Clone)]
pub(crate) struct DrainControl(Arc<Inner>);

impl DrainControl {
    fn new(mode: Lifetime) -> Self {
        let epoch_unix_ms = unix_ms_now();
        Self(Arc::new(Inner {
            mode,
            epoch: Instant::now(),
            epoch_unix_ms,
            absorb: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            input_closed: AtomicBool::new(false),
            deadline_ms: AtomicU64::new(NO_DEADLINE),
            pause: AtomicBool::new(false),
            pause_seq: AtomicU64::new(0),
            requeue_req: AtomicU64::new(0),
            requeue_seen: AtomicU64::new(0),
            generation: AtomicU64::new(0),
        }))
    }

    pub(crate) fn attached() -> Self {
        Self::new(Lifetime::Attached)
    }

    /// The pre-Foundations lifetime, for tests (production is `attached`).
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self::new(Lifetime::Detached)
    }

    pub(crate) fn mode(&self) -> Lifetime {
        self.0.mode
    }

    fn now_ms(&self) -> u64 {
        // Capped one below the sentinel so "now" can never read as "no deadline".
        u64::try_from(self.0.epoch.elapsed().as_millis())
            .unwrap_or(NO_DEADLINE - 1)
            .min(NO_DEADLINE - 1)
    }

    fn deadline_raw(&self) -> Option<u64> {
        let d = self.0.deadline_ms.load(Acquire);
        (d != NO_DEADLINE).then_some(d)
    }

    // ── Control side ─────────────────────────────────────────────────────────

    /// T0 (first SIGTERM): absorb mode — move channel arrivals into `pending`
    /// on every wait slice; skip probes, rescans and gauge walks.
    pub(crate) fn begin_shutdown(&self) {
        self.0.absorb.store(true, Release);
    }

    /// T2 (after the flusher joins): input is closed, deliver until `budget`
    /// from now, then abandon. The deadline only ever moves earlier, and is
    /// stored before `stop` so a drain that sees `stop` sees the deadline.
    pub(crate) fn request_stop(&self, budget: Duration) {
        self.0.input_closed.store(true, Release);
        let budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
        let at = self.now_ms().saturating_add(budget_ms).min(NO_DEADLINE - 1);
        self.0.deadline_ms.fetch_min(at, AcqRel);
        self.0.stop.store(true, Release);
    }

    /// Second signal: absorb, and a deadline of now. A later `request_stop`
    /// cannot raise it (both use `fetch_min`).
    pub(crate) fn force_stop(&self) {
        self.0.absorb.store(true, Release);
        self.0.deadline_ms.fetch_min(self.now_ms(), AcqRel);
        self.0.stop.store(true, Release);
    }

    /// Pause delivery before the next commit batch. Returns the request's sequence number.
    pub(crate) fn request_pause(&self) -> u64 {
        self.0.pause.store(true, Release);
        self.0.pause_seq.fetch_add(1, AcqRel) + 1
    }

    /// Resume delivery. Returns the request's sequence number.
    pub(crate) fn request_resume(&self) -> u64 {
        self.0.pause.store(false, Release);
        self.0.pause_seq.fetch_add(1, AcqRel) + 1
    }

    /// Ask for a stranded-segment rescan. A request made while an earlier
    /// ticket is still unobserved folds into it and returns that ticket, so a
    /// caller looping on this cannot queue more than one scan (spec §5.4).
    pub(crate) fn request_requeue(&self) -> u64 {
        loop {
            let req = self.0.requeue_req.load(Acquire);
            if req > self.0.requeue_seen.load(Acquire) {
                return req;
            }
            if self
                .0
                .requeue_req
                .compare_exchange(req, req + 1, AcqRel, Acquire)
                .is_ok()
            {
                return req + 1;
            }
        }
    }

    // ── Drain side ───────────────────────────────────────────────────────────

    pub(crate) fn absorbing(&self) -> bool {
        self.0.absorb.load(Acquire)
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.0.stop.load(Acquire)
    }

    pub(crate) fn input_closed(&self) -> bool {
        self.0.input_closed.load(Acquire)
    }

    pub(crate) fn deadline_passed(&self) -> bool {
        self.deadline_raw().is_some_and(|d| self.now_ms() >= d)
    }

    pub(crate) fn time_left(&self) -> Option<Duration> {
        self.deadline_raw()
            .map(|d| Duration::from_millis(d.saturating_sub(self.now_ms())))
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline_raw()
            .map(|d| self.0.epoch + Duration::from_millis(d))
    }

    pub(crate) fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_raw()
            .map(|d| self.0.epoch_unix_ms.saturating_add(d))
    }

    /// Resolves once the deadline has passed; pending forever without one.
    /// Polls every [`SLICE`] on the drain's current-thread runtime, so it is
    /// cheap to race against a sink commit in `select!`.
    pub(crate) async fn deadline_reached(&self) {
        loop {
            if self.deadline_passed() {
                return;
            }
            let nap = self
                .time_left()
                .map_or(SLICE, |left| left.min(SLICE))
                .max(Duration::from_millis(1));
            tokio::time::sleep(nap).await;
        }
    }

    pub(crate) fn pause_requested(&self) -> bool {
        self.0.pause.load(Acquire)
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "called by PR 4 (lifecycle) / PR 6 (admin); tests exercise it now"
        )
    )]
    pub(crate) fn pause_seq(&self) -> u64 {
        self.0.pause_seq.load(Acquire)
    }

    pub(crate) fn requeue_requested(&self) -> u64 {
        self.0.requeue_req.load(Acquire)
    }

    pub(crate) fn requeue_observed(&self) -> u64 {
        self.0.requeue_seen.load(Acquire)
    }

    /// The drain resolved `ticket` (and every ticket before it).
    pub(crate) fn observe_requeue(&self, ticket: u64) {
        self.0.requeue_seen.fetch_max(ticket, AcqRel);
    }

    /// Called once per `drain_thread_with` start; returns the new generation.
    pub(crate) fn begin_generation(&self) -> u64 {
        self.0.generation.fetch_add(1, AcqRel) + 1
    }

    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "called by PR 4 (lifecycle) / PR 6 (admin); tests exercise it now"
        )
    )]
    pub(crate) fn generation(&self) -> u64 {
        self.0.generation.load(Acquire)
    }
}

/// Everything the drain is started with besides its sink and config (spec §5.4).
/// Kept out of `DrainConfig` so the existing `DrainConfig { .. }` test literals
/// compile unchanged.
pub(crate) struct DrainRuntime {
    pub(crate) control: DrainControl,
    /// `None` = unobserved (tests); production passes the board from PR 1.
    pub(crate) board: Option<Arc<StatusBoard>>,
    /// The startup replay list, handed over in memory (PR 3/4). Used on the
    /// drain's FIRST start only; a respawn recovers it through its rescan.
    pub(crate) initial_backlog: Vec<PathBuf>,
}

impl DrainRuntime {
    /// Detached, no board, no backlog: the pre-Foundations test wiring.
    /// Production builds its runtime with the board (`main`).
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self {
            control: DrainControl::detached(),
            board: None,
            initial_backlog: Vec::new(),
        }
    }
}

/// Why and how the drain stopped. The input to main's T3 summary (PR 4),
/// built without new I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrainExit {
    pub(crate) reason: DrainExitReason,
    /// The segment left partly delivered (`segment_identity`, records processed).
    pub(crate) abandoned: Option<(String, u64)>,
    /// Paths still in `pending` plus what was left in the channel.
    pub(crate) pending_left: usize,
    pub(crate) backlog_left: usize,
    /// The last drain panic's message, sanitised (admin view only).
    pub(crate) last_panic: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*; // includes the module's `unix_ms_now` import

    #[test]
    fn a_fresh_control_carries_no_intent() {
        let c = DrainControl::detached();
        assert_eq!(c.mode(), Lifetime::Detached);
        assert_eq!(DrainControl::attached().mode(), Lifetime::Attached);
        assert!(!c.absorbing() && !c.stop_requested() && !c.input_closed());
        assert!(!c.pause_requested() && !c.deadline_passed());
        assert_eq!(c.time_left(), None);
        assert_eq!(c.deadline(), None);
        assert_eq!(c.deadline_unix_ms(), None);
        assert_eq!(
            (
                c.requeue_requested(),
                c.requeue_observed(),
                c.generation(),
                c.pause_seq()
            ),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn begin_shutdown_turns_on_absorb_and_nothing_else() {
        let c = DrainControl::attached();
        c.begin_shutdown();
        assert!(c.absorbing());
        assert!(!c.stop_requested() && !c.input_closed());
        assert_eq!(
            c.time_left(),
            None,
            "T0 sets no deadline; T2's request_stop does"
        );
    }

    #[test]
    fn request_stop_closes_input_and_sets_a_deadline() {
        let c = DrainControl::attached();
        c.request_stop(Duration::from_secs(10));
        assert!(c.stop_requested() && c.input_closed());
        assert!(
            !c.absorbing(),
            "request_stop does not imply absorb; begin_shutdown does"
        );
        let left = c.time_left().expect("a deadline");
        assert!(
            left > Duration::from_secs(9) && left <= Duration::from_secs(10),
            "{left:?}"
        );
        assert!(!c.deadline_passed());
    }

    #[test]
    fn force_stop_then_request_stop_keeps_the_forced_deadline() {
        let c = DrainControl::attached();
        c.force_stop();
        c.request_stop(Duration::from_secs(10));
        assert!(
            c.deadline_passed(),
            "fetch_min: main's T2 request_stop(10 s) must not raise a forced deadline"
        );
        assert!(c.absorbing() && c.stop_requested());
        assert_eq!(c.time_left(), Some(Duration::ZERO));
    }

    #[test]
    fn the_deadline_only_ever_moves_earlier() {
        let c = DrainControl::attached();
        c.request_stop(Duration::from_secs(10));
        c.request_stop(Duration::from_secs(1));
        assert!(c.time_left().unwrap() <= Duration::from_secs(1));
        c.request_stop(Duration::from_secs(60));
        assert!(
            c.time_left().unwrap() <= Duration::from_secs(1),
            "a longer budget never raises it"
        );
    }

    #[test]
    fn a_huge_budget_saturates_instead_of_wrapping() {
        let c = DrainControl::attached();
        c.request_stop(Duration::MAX);
        assert!(c.stop_requested());
        assert!(!c.deadline_passed());
        assert!(c.time_left().is_some());
    }

    #[test]
    fn the_deadline_is_published_in_unix_milliseconds() {
        let c = DrainControl::attached();
        let before = unix_ms_now();
        c.request_stop(Duration::from_secs(10));
        let after = unix_ms_now();
        let d = c.deadline_unix_ms().expect("a deadline");
        assert!(
            d + 50 >= before + 10_000 && d <= after + 10_000 + 50,
            "{d} vs [{before}, {after}]"
        );
    }

    #[test]
    fn deadline_reached_resolves_once_the_deadline_passes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let c = DrainControl::detached();
        c.request_stop(Duration::from_millis(120));
        let t = Instant::now();
        rt.block_on(c.deadline_reached());
        let took = t.elapsed();
        assert!(
            took >= Duration::from_millis(100) && took < Duration::from_millis(400),
            "{took:?}"
        );
    }

    #[test]
    fn deadline_reached_never_resolves_without_a_deadline() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let c = DrainControl::detached();
        let r = rt.block_on(async {
            tokio::time::timeout(Duration::from_millis(200), c.deadline_reached()).await
        });
        assert!(r.is_err(), "no deadline: the future must stay pending");
    }

    #[test]
    fn pause_and_resume_return_increasing_sequence_numbers() {
        let c = DrainControl::detached();
        let a = c.request_pause();
        assert!(c.pause_requested());
        let b = c.request_resume();
        assert!(!c.pause_requested());
        assert!(b > a);
        assert_eq!(c.pause_seq(), b);
        let again = c.request_resume();
        assert!(
            again > b,
            "idempotent in effect, but every request is sequenced"
        );
    }

    #[test]
    fn requeue_requests_fold_into_the_outstanding_ticket() {
        let c = DrainControl::detached();
        assert_eq!(c.request_requeue(), 1);
        assert_eq!(
            c.request_requeue(),
            1,
            "ticket 1 not yet observed: fold into it"
        );
        assert_eq!(c.request_requeue(), 1);
        c.observe_requeue(1);
        assert_eq!(
            c.request_requeue(),
            2,
            "after observation a request is a new ticket"
        );
        c.observe_requeue(1);
        assert_eq!(
            c.requeue_observed(),
            1,
            "observation is monotonic (fetch_max)"
        );
        c.observe_requeue(2);
        assert_eq!(c.requeue_observed(), 2);
    }

    #[test]
    fn generations_count_drain_starts() {
        let c = DrainControl::detached();
        assert_eq!(c.begin_generation(), 1);
        assert_eq!(c.begin_generation(), 2);
        assert_eq!(c.generation(), 2);
    }

    #[test]
    fn clones_share_one_intent() {
        let a = DrainControl::attached();
        let b = a.clone();
        b.request_pause();
        b.begin_shutdown();
        assert!(a.pause_requested() && a.absorbing());
    }
}
