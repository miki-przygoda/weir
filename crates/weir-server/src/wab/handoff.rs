//! The flusher → drain hand-off (spec §5.5).
//!
//! Every flusher send site (rotation, timed seal, shutdown seal) goes through
//! [`hand_off`]: a loop of 100 ms timed sends instead of one blocking send.
//!
//! # Invariant (spec §15, the only new branch next to the ack path)
//!
//! `sealed` is always a path returned by a successful `seal_current`, and the
//! seal includes the fsync. The acks for the segment's records are appended
//! after this returns (`flush_batch`'s `durable_acks.append`) and resolved
//! after that. So whatever this returns, those acks are true. A segment that is
//! NOT handed off is sealed, unconfirmed and replayed on the next start: the
//! same outcome as today's `SendError` arm. Never move the hand-off after the
//! ack resolution, and never call it with a path that was not sealed.
//!
//! # Why a loop
//!
//! A flusher that entered a send before SIGTERM re-reads `shutting_down` at
//! most one slice later; a plain `send` waits until the drain takes an item,
//! and the drain may be inside a commit lasting up to `commit_timeout`.
//! Outside shutdown the loop behaves exactly like a blocking send: the only
//! added cost is one Relaxed load per 100 ms *of blocking*.
//!
//! # Budget
//!
//! Once `shutting_down` is set, the budget is [`FINAL_HANDOFF_TIMEOUT`] of
//! wall time measured on the injected clock (`BlockingClock::now`, decision
//! Q7): `t0` is the start of the first slice that timed out after the flag was
//! seen, and the hand-off gives up once `now - t0 >= FINAL_HANDOFF_TIMEOUT`.
//! The budget is per flusher thread (`t0` is an `Arc<AtomicU64>` shared by
//! every panic-respawn of that flusher): once it is spent, each later hand-off
//! makes one slice and gives up. Total shutdown hand-off time per flusher is
//! ≤ 1 s + 100 ms per remaining rotation. Under `RealClock` that is real time;
//! under the DST `SimClock` it is virtual time, which only a full channel or a
//! flusher wait advances, so the bounded-channel seeds are deterministic.
//!
//! # Counting
//!
//! Both ways of giving up leave a sealed, unconfirmed segment for replay and
//! are counted in `weir_wab_segments_not_handed_off_total{reason}` (decision
//! Q7): `budget` when the shutdown budget ran out, `disconnected` when the
//! drain's Receiver was gone. The T3 shutdown summary reads the sum.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use crossbeam_channel::{SendTimeoutError, Sender};
use tracing::{info, warn};

use super::clock::BlockingClock;
use crate::metrics::{Metrics, NotHandedOffLabel, NotHandedOffReason};

/// One timed send. Also the latency with which a blocked flusher sees shutdown.
pub(crate) const HANDOFF_SLICE: Duration = Duration::from_millis(100);
/// The per-flusher shutdown budget (spec §13 constant), in wall time on the
/// injected clock.
pub(crate) const FINAL_HANDOFF_TIMEOUT: Duration = Duration::from_secs(1);

/// `shutdown_t0_ns` before the budget has started.
const BUDGET_NOT_STARTED: u64 = u64::MAX;

/// Where a hand-off reports "this shard's flusher is blocked on the drain".
/// Production: the StatusBoard's ShardRegistry (`handoff_blocked_since_ms`,
/// spec §5.3). `None` in [`HandoffConfig`] = detached (DST, unit tests).
pub(crate) trait HandoffMarker: Send + Sync {
    /// `since_unix_ms == 0` clears the marker.
    fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64);
}

impl HandoffMarker for crate::status::StatusBoard {
    fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64) {
        self.shards
            .set_handoff_blocked_since_ms(usize::from(shard), since_unix_ms);
    }
}

/// Shared by every flusher; turned into one [`HandoffState`] per shard.
#[derive(Clone)]
pub(crate) struct HandoffConfig {
    /// Set at T0 by the Lifecycle (PR 4). Nothing sets it in PR 3.
    pub(crate) shutting_down: Arc<AtomicBool>,
    /// `StatusBoard::drain_depth`: last-writer-wins `len()` snapshots.
    pub(crate) drain_depth: Arc<AtomicUsize>,
    pub(crate) marker: Option<Arc<dyn HandoffMarker>>,
}

impl HandoffConfig {
    #[cfg(any(test, feature = "dst"))]
    pub(crate) fn detached() -> Self {
        HandoffConfig {
            shutting_down: Arc::new(AtomicBool::new(false)),
            drain_depth: Arc::new(AtomicUsize::new(0)),
            marker: None,
        }
    }

    /// Called once per flusher THREAD (outside the panic-respawn factory), so
    /// the budget is shared by every respawn of that flusher.
    pub(crate) fn for_shard(&self, shard_id: u16, metrics: Arc<Metrics>) -> HandoffState {
        HandoffState {
            shard_id,
            shutting_down: Arc::clone(&self.shutting_down),
            drain_depth: Arc::clone(&self.drain_depth),
            marker: self.marker.clone(),
            metrics,
            shutdown_t0_ns: Arc::new(AtomicU64::new(BUDGET_NOT_STARTED)),
        }
    }
}

/// One flusher's view of the hand-off. Clones share the budget.
#[derive(Clone)]
pub(crate) struct HandoffState {
    shard_id: u16,
    shutting_down: Arc<AtomicBool>,
    drain_depth: Arc<AtomicUsize>,
    marker: Option<Arc<dyn HandoffMarker>>,
    metrics: Arc<Metrics>,
    /// `BlockingClock::now()` in ns at the end of the first slice that timed
    /// out while shutting down; [`BUDGET_NOT_STARTED`] until then.
    shutdown_t0_ns: Arc<AtomicU64>,
}

impl HandoffState {
    // Under `dst` without `test`, the only caller is Task 3.5's bounded-channel
    // seed family; remove this allowance there.
    #[cfg(any(test, feature = "dst"))]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(
        shard_id: u16,
        shutting_down: Arc<AtomicBool>,
        drain_depth: Arc<AtomicUsize>,
        marker: Option<Arc<dyn HandoffMarker>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        HandoffConfig {
            shutting_down,
            drain_depth,
            marker,
        }
        .for_shard(shard_id, metrics)
    }

    /// Never shutting down, private depth cell, no marker.
    #[cfg(any(test, feature = "dst"))]
    pub(crate) fn detached(shard_id: u16, metrics: Arc<Metrics>) -> Self {
        HandoffConfig::detached().for_shard(shard_id, metrics)
    }

    fn mark_blocked(&self) {
        if let Some(m) = &self.marker {
            m.set_handoff_blocked_since_ms(self.shard_id, crate::status::unix_ms_now().max(1));
        }
    }

    fn clear_blocked(&self) {
        if let Some(m) = &self.marker {
            m.set_handoff_blocked_since_ms(self.shard_id, 0);
        }
    }

    /// Called after a slice timed out while shutting down, with the clock's
    /// `now()`. True once [`FINAL_HANDOFF_TIMEOUT`] has passed since the start
    /// of the first such slice (that slice counts against the budget, hence
    /// the `+ HANDOFF_SLICE`). Only the owning flusher thread calls this (a
    /// respawn runs only after the previous body ended), so load-then-store
    /// is race-free.
    fn shutdown_budget_spent(&self, now: Duration) -> bool {
        let now_ns = u64::try_from(now.as_nanos()).unwrap_or(u64::MAX - 1);
        let t0 = match self.shutdown_t0_ns.load(Relaxed) {
            BUDGET_NOT_STARTED => {
                self.shutdown_t0_ns.store(now_ns, Relaxed);
                now_ns
            }
            t0 => t0,
        };
        now_ns.saturating_sub(t0) + HANDOFF_SLICE.as_nanos() as u64
            >= FINAL_HANDOFF_TIMEOUT.as_nanos() as u64
    }

    fn count_not_handed_off(&self, reason: NotHandedOffReason) {
        self.metrics
            .wab_segments_not_handed_off
            .get_or_create(&NotHandedOffLabel { reason })
            .inc();
    }
}

/// Which flusher site handed off; carried into the log line. A timed seal
/// keeps its `reason` (`idle` / `lifetime`), which the old send site logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandoffSite {
    Rotation,
    TimedSeal { reason: &'static str },
    ShutdownSeal,
}

impl HandoffSite {
    fn as_str(self) -> &'static str {
        match self {
            HandoffSite::Rotation => "rotation",
            HandoffSite::TimedSeal { .. } => "timed_seal",
            HandoffSite::ShutdownSeal => "shutdown_seal",
        }
    }

    /// The timed seal's reason; `None` for the other sites.
    fn reason(self) -> Option<&'static str> {
        match self {
            HandoffSite::TimedSeal { reason } => Some(reason),
            HandoffSite::Rotation | HandoffSite::ShutdownSeal => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HandOff {
    Delivered,
    NotHandedOff(NotHandedOff),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NotHandedOff {
    /// Shutting down and the per-flusher budget is spent.
    ShutdownBudget,
    /// The drain's Receiver is gone.
    Disconnected,
}

/// Hands one sealed segment to the drain. See the module docs for the
/// invariant that makes every return value safe for the acks.
pub(crate) fn hand_off<C: BlockingClock>(
    clock: &C,
    tx: &Sender<PathBuf>,
    sealed: PathBuf,
    sh: &HandoffState,
    site: HandoffSite,
) -> HandOff {
    let mut path = sealed;
    let mut marked = false;
    loop {
        match clock.send_timeout(tx, path, HANDOFF_SLICE) {
            Ok(()) => {
                if marked {
                    sh.clear_blocked();
                }
                sh.drain_depth.store(tx.len(), Relaxed);
                return HandOff::Delivered;
            }
            Err(SendTimeoutError::Timeout(back)) => {
                path = back;
                if !marked {
                    sh.mark_blocked();
                    marked = true;
                }
                if sh.shutting_down.load(Relaxed) && sh.shutdown_budget_spent(clock.now()) {
                    sh.clear_blocked();
                    sh.count_not_handed_off(NotHandedOffReason::budget);
                    warn!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %path.display(),
                        budget_ms = FINAL_HANDOFF_TIMEOUT.as_millis() as u64,
                        "shutting down and the drain channel stayed full; sealed segment not \
                         handed off. It is durable and will be delivered on restart"
                    );
                    return HandOff::NotHandedOff(NotHandedOff::ShutdownBudget);
                }
            }
            Err(SendTimeoutError::Disconnected(back)) => {
                if marked {
                    sh.clear_blocked();
                }
                sh.count_not_handed_off(NotHandedOffReason::disconnected);
                if sh.shutting_down.load(Relaxed) {
                    info!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %back.display(),
                        "drain channel closed on shutdown; sealed segment is durable and will \
                         be delivered on restart"
                    );
                } else {
                    warn!(
                        shard = sh.shard_id,
                        site = site.as_str(),
                        reason = site.reason().unwrap_or(""),
                        sealed = %back.display(),
                        "drain channel closed; sealed segment is durable and will be delivered \
                         on restart"
                    );
                }
                return HandOff::NotHandedOff(NotHandedOff::Disconnected);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{Receiver, RecvTimeoutError, TrySendError};
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::SeqCst},
    };
    use std::time::Instant;

    use crate::wab::clock::RealClock;

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    /// One `reason` series of `weir_wab_segments_not_handed_off_total`.
    fn not_handed_off(m: &Metrics, reason: NotHandedOffReason) -> u64 {
        m.wab_segments_not_handed_off
            .get_or_create(&NotHandedOffLabel { reason })
            .get()
    }

    /// Records every marker write, so a test can see "set, then cleared".
    #[derive(Default)]
    struct RecordingMarker(Mutex<Vec<(u16, u64)>>);
    impl HandoffMarker for RecordingMarker {
        fn set_handoff_blocked_since_ms(&self, shard: u16, since_unix_ms: u64) {
            self.0.lock().unwrap().push((shard, since_unix_ms));
        }
    }

    /// Counts send attempts and never waits; a full channel charges the
    /// timeout to a virtual clock, as `SimClock` does. Makes the wall-time
    /// budget exact in attempts.
    #[derive(Default)]
    struct CountingClock {
        attempts: AtomicU64,
        virtual_ms: AtomicU64,
    }
    impl BlockingClock for CountingClock {
        fn recv_timeout<T>(&self, rx: &Receiver<T>, t: Duration) -> Result<T, RecvTimeoutError> {
            rx.recv_timeout(t)
        }
        fn send_timeout<T>(
            &self,
            tx: &Sender<T>,
            v: T,
            t: Duration,
        ) -> Result<(), SendTimeoutError<T>> {
            self.attempts.fetch_add(1, SeqCst);
            match tx.try_send(v) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(v)) => {
                    self.virtual_ms.fetch_add(t.as_millis() as u64, SeqCst);
                    Err(SendTimeoutError::Timeout(v))
                }
                Err(TrySendError::Disconnected(v)) => Err(SendTimeoutError::Disconnected(v)),
            }
        }
        fn sleep(&self, _dur: Duration) {}
        fn now(&self) -> Duration {
            Duration::from_millis(self.virtual_ms.load(SeqCst))
        }
    }

    fn full_channel() -> (Sender<PathBuf>, Receiver<PathBuf>) {
        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
        tx.send(PathBuf::from("prefill")).unwrap();
        (tx, rx)
    }

    #[test]
    fn the_budget_is_one_second_per_flusher_then_one_slice_per_later_hand_off() {
        let m = metrics();
        let sh = HandoffState::new(
            3,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicUsize::new(0)),
            None,
            Arc::clone(&m),
        );
        let (tx, _rx) = full_channel();
        let clock = CountingClock::default();

        let first = hand_off(&clock, &tx, PathBuf::from("a"), &sh, HandoffSite::Rotation);
        assert_eq!(first, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert_eq!(
            clock.attempts.load(SeqCst),
            10,
            "the budget is 1 s of clock time: ten 100 ms slices"
        );
        assert_eq!(clock.now(), Duration::from_secs(1));

        // A clone shares the budget, as a panic-respawned flusher does.
        let respawned = sh.clone();
        let second = hand_off(
            &clock,
            &tx,
            PathBuf::from("b"),
            &respawned,
            HandoffSite::ShutdownSeal,
        );
        assert_eq!(second, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert_eq!(
            clock.attempts.load(SeqCst),
            11,
            "a spent budget allows exactly one more slice"
        );
        assert_eq!(not_handed_off(&m, NotHandedOffReason::budget), 2);
        assert_eq!(not_handed_off(&m, NotHandedOffReason::disconnected), 0);
        assert_eq!(m.wab_segments_not_handed_off_total(), 2);
    }

    #[test]
    fn outside_shutdown_a_full_channel_never_gives_up() {
        // Backpressure is unchanged: no flag, so the loop keeps trying. A
        // consumer frees the slot after 250 ms and the segment is delivered.
        let marker = Arc::new(RecordingMarker::default());
        let depth = Arc::new(AtomicUsize::new(0));
        let sh = HandoffState::new(
            1,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&depth),
            Some(Arc::clone(&marker) as Arc<dyn HandoffMarker>),
            metrics(),
        );
        let (tx, rx) = full_channel();
        let consumer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            let first = rx.recv().unwrap();
            let second = rx.recv().unwrap();
            (first, second)
        });
        let t0 = Instant::now();
        let out = hand_off(
            &RealClock,
            &tx,
            PathBuf::from("seg"),
            &sh,
            HandoffSite::Rotation,
        );
        assert_eq!(out, HandOff::Delivered);
        assert!(t0.elapsed() >= Duration::from_millis(200));
        let (first, second) = consumer.join().unwrap();
        assert_eq!(
            (first, second),
            (PathBuf::from("prefill"), PathBuf::from("seg"))
        );

        let writes = marker.0.lock().unwrap().clone();
        assert_eq!(
            writes.len(),
            2,
            "marked once on the first timeout, cleared once: {writes:?}"
        );
        assert_eq!(writes[0].0, 1);
        assert!(writes[0].1 > 0, "the mark is a wall-clock unix-ms stamp");
        assert_eq!(writes[1], (1, 0), "a delivered hand-off clears the marker");
        assert!(
            depth.load(SeqCst) <= 1,
            "depth is the channel length after the send"
        );
    }

    #[test]
    fn a_flag_raised_mid_block_is_seen_within_one_slice_and_bounds_the_wait() {
        let flag = Arc::new(AtomicBool::new(false));
        let sh = HandoffState::new(
            0,
            Arc::clone(&flag),
            Arc::new(AtomicUsize::new(0)),
            None,
            metrics(),
        );
        let (tx, _rx) = full_channel();
        let raiser = {
            let flag = Arc::clone(&flag);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                flag.store(true, SeqCst);
            })
        };
        let t0 = Instant::now();
        let out = hand_off(
            &RealClock,
            &tx,
            PathBuf::from("seg"),
            &sh,
            HandoffSite::ShutdownSeal,
        );
        let waited = t0.elapsed();
        raiser.join().unwrap();
        assert_eq!(out, HandOff::NotHandedOff(NotHandedOff::ShutdownBudget));
        assert!(
            waited >= Duration::from_millis(1200) && waited < Duration::from_millis(2000),
            "≈300 ms blocked + 1 s budget, got {waited:?}"
        );
    }

    #[test]
    fn a_closed_channel_gives_up_at_once_and_counts() {
        let m = metrics();
        let sh = HandoffState::detached(0, Arc::clone(&m));
        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drop(rx);
        let t0 = Instant::now();
        let out = hand_off(
            &RealClock,
            &tx,
            PathBuf::from("seg"),
            &sh,
            HandoffSite::TimedSeal { reason: "idle" },
        );
        assert_eq!(out, HandOff::NotHandedOff(NotHandedOff::Disconnected));
        assert!(t0.elapsed() < Duration::from_millis(100));
        assert_eq!(not_handed_off(&m, NotHandedOffReason::disconnected), 1);
        assert_eq!(
            not_handed_off(&m, NotHandedOffReason::budget),
            0,
            "a closed channel is not a spent budget"
        );
    }

    #[test]
    fn the_site_carries_the_timed_seal_reason_into_the_log_fields() {
        assert_eq!(
            HandoffSite::TimedSeal { reason: "lifetime" }.as_str(),
            "timed_seal"
        );
        assert_eq!(
            HandoffSite::TimedSeal { reason: "lifetime" }.reason(),
            Some("lifetime")
        );
        assert_eq!(HandoffSite::Rotation.reason(), None);
        assert_eq!(HandoffSite::ShutdownSeal.as_str(), "shutdown_seal");
    }

    #[test]
    fn a_delivered_hand_off_publishes_the_channel_depth() {
        let depth = Arc::new(AtomicUsize::new(0));
        let sh = HandoffState::new(
            0,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&depth),
            None,
            metrics(),
        );
        let (tx, _rx) = crossbeam_channel::bounded::<PathBuf>(4);
        tx.send(PathBuf::from("already")).unwrap();
        assert_eq!(
            hand_off(
                &RealClock,
                &tx,
                PathBuf::from("seg"),
                &sh,
                HandoffSite::Rotation
            ),
            HandOff::Delivered
        );
        assert_eq!(depth.load(SeqCst), 2);
    }
}
