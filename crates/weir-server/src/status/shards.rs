//! Per-shard and per-worker state (spec §5.3). The flusher supervisor is the
//! single writer of a shard's terminal state; the flusher writes only `Up`;
//! a worker's drop guard writes its own running flag. Readers (routing,
//! evidence) only load: O(shard_count <= 256) loads per `pick_live`.
use super::unix_ms_now;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use weir_status::{OfflineReason, ShardEvidence, ShardState, WorkerEvidence};

const NO_REASON: u8 = u8::MAX;

/// How a flusher supervisor ended (also SP-G's `SupervisorExit` seam).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SupervisorExit {
    InputClosed,
    CounterScanFailed,
    RespawnExhausted,
}

struct ShardCell {
    state: AtomicU8,
    reason: AtomicU8,
    since_ms: AtomicU64,
    respawns: AtomicU32,
    handoff_blocked_since_ms: AtomicU64,
}

impl ShardCell {
    fn new(now_ms: u64) -> Self {
        ShardCell {
            state: AtomicU8::new(ShardState::Starting.as_u8()),
            reason: AtomicU8::new(NO_REASON),
            since_ms: AtomicU64::new(now_ms),
            respawns: AtomicU32::new(0),
            handoff_blocked_since_ms: AtomicU64::new(0),
        }
    }
    fn set(&self, state: ShardState, reason: Option<OfflineReason>) {
        self.reason.store(
            reason.map_or(NO_REASON, OfflineReason::as_u8),
            Ordering::Relaxed,
        );
        self.since_ms.store(unix_ms_now(), Ordering::Relaxed);
        self.state.store(state.as_u8(), Ordering::Release);
    }
    fn state(&self) -> ShardState {
        ShardState::from_u8(self.state.load(Ordering::Acquire)).unwrap_or(ShardState::Offline)
    }
}

struct WorkerCell {
    running: AtomicBool,
    exited_at_ms: AtomicU64,
}

pub(crate) struct ShardRegistry {
    shards: Box<[ShardCell]>,
    workers: Box<[WorkerCell]>,
    shutting_down: Arc<AtomicBool>,
}

impl ShardRegistry {
    pub(crate) fn new(
        shard_count: usize,
        worker_count: usize,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        let now = unix_ms_now();
        ShardRegistry {
            shards: (0..shard_count).map(|_| ShardCell::new(now)).collect(),
            workers: (0..worker_count)
                .map(|_| WorkerCell {
                    running: AtomicBool::new(false),
                    exited_at_ms: AtomicU64::new(0),
                })
                .collect(),
            shutting_down,
        }
    }

    /// A one-shard, one-worker registry nothing else observes (spec §5.3:
    /// "DST passes a detached registry").
    #[cfg_attr(not(any(test, feature = "dst")), allow(dead_code))] // DST and tests only
    pub(crate) fn detached() -> Arc<Self> {
        Arc::new(Self::new(1, 1, Arc::new(AtomicBool::new(false))))
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 3/4 read it
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }
    #[cfg_attr(not(test), allow(dead_code))] // PR 3/4 read it
    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }
    pub(crate) fn state(&self, shard: usize) -> ShardState {
        self.shards[shard].state()
    }
    pub(crate) fn reason(&self, shard: usize) -> Option<OfflineReason> {
        OfflineReason::from_u8(self.shards[shard].reason.load(Ordering::Relaxed))
    }
    pub(crate) fn respawns(&self, shard: usize) -> u32 {
        self.shards[shard].respawns.load(Ordering::Relaxed)
    }

    pub(crate) fn mark_up(&self, shard: usize) {
        self.shards[shard].set(ShardState::Up, None);
    }

    pub(crate) fn mark_respawning(&self, shard: usize) {
        self.shards[shard].respawns.fetch_add(1, Ordering::Relaxed);
        self.shards[shard].set(ShardState::Respawning, None);
    }

    /// Spec §5.3: InputClosed is `Stopped` while shutting down, else
    /// `Offline(FlusherExited)`; the other two are `Offline(<reason>)`.
    pub(crate) fn mark_exit(&self, shard: usize, exit: SupervisorExit) {
        let (state, reason) = match exit {
            SupervisorExit::InputClosed if self.shutting_down.load(Ordering::Acquire) => {
                (ShardState::Stopped, None)
            }
            SupervisorExit::InputClosed => {
                (ShardState::Offline, Some(OfflineReason::FlusherExited))
            }
            SupervisorExit::CounterScanFailed => {
                (ShardState::Offline, Some(OfflineReason::CounterScanFailed))
            }
            SupervisorExit::RespawnExhausted => {
                (ShardState::Offline, Some(OfflineReason::RespawnExhausted))
            }
        };
        self.shards[shard].set(state, reason);
    }

    /// The hand-off-blocked marker (spec §5.3). The first timeout of an
    /// episode stamps it; later stamps keep the first; 0 clears it.
    #[cfg_attr(not(test), allow(dead_code))] // PR 3's hand_off writes this
    pub(crate) fn set_handoff_blocked_since_ms(&self, shard: usize, since_unix_ms: u64) {
        let cell = &self.shards[shard].handoff_blocked_since_ms;
        if since_unix_ms == 0 {
            cell.store(0, Ordering::Relaxed);
        } else {
            let _ = cell.compare_exchange(0, since_unix_ms, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    pub(crate) fn handoff_blocked_since_ms(&self, shard: usize) -> Option<u64> {
        let v = self.shards[shard]
            .handoff_blocked_since_ms
            .load(Ordering::Relaxed);
        (v != 0).then_some(v)
    }

    pub(crate) fn worker_running(&self, worker: usize) -> bool {
        self.workers
            .get(worker)
            .is_some_and(|w| w.running.load(Ordering::Acquire))
    }

    /// Spec §5.3: `Up|Respawning` AND the worker owning partition
    /// `shard % worker_count` is running.
    pub(crate) fn ingest_capable(&self, shard: usize) -> bool {
        let state = self.shards[shard].state();
        matches!(state, ShardState::Up | ShardState::Respawning)
            && self.worker_running(shard % self.workers.len().max(1))
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 3 routes with this
    pub(crate) fn ingest_capable_count(&self) -> usize {
        (0..self.shards.len())
            .filter(|&s| self.ingest_capable(s))
            .count()
    }

    /// The (n mod k)-th ingest-capable shard. A shard that changes state
    /// between the count and the walk is tolerated: the walk falls back to
    /// the first capable shard, or `None`.
    #[cfg_attr(not(test), allow(dead_code))] // PR 3 routes with this
    pub(crate) fn pick_live(&self, n: u64) -> Option<u32> {
        let k = self.ingest_capable_count();
        if k == 0 {
            return None;
        }
        let target = (n % k as u64) as usize;
        let mut capable = (0..self.shards.len()).filter(|&s| self.ingest_capable(s));
        capable
            .clone()
            .nth(target)
            .or_else(|| capable.next())
            .map(|s| s as u32)
    }

    #[cfg_attr(not(test), allow(dead_code))] // PR 3's settle wait reads this
    pub(crate) fn all_settled(&self) -> bool {
        self.shards
            .iter()
            .all(|s| s.state() != ShardState::Starting)
    }

    pub(crate) fn shard_evidence(&self) -> Vec<ShardEvidence> {
        (0..self.shards.len())
            .map(|s| ShardEvidence {
                id: s as u16,
                state: self.state(s),
                reason: self.reason(s),
                since_unix_ms: self.shards[s].since_ms.load(Ordering::Relaxed),
                respawns: self.respawns(s),
                worker_running: self.worker_running(s % self.workers.len().max(1)),
                handoff_blocked_since_unix_ms: self.handoff_blocked_since_ms(s),
                active_segment_age_secs: None,
            })
            .collect()
    }

    pub(crate) fn worker_evidence(&self) -> Vec<WorkerEvidence> {
        self.workers
            .iter()
            .enumerate()
            .map(|(i, w)| {
                let exited = w.exited_at_ms.load(Ordering::Acquire);
                WorkerEvidence {
                    id: i as u16,
                    running: w.running.load(Ordering::Acquire),
                    exited_at_unix_ms: (exited != 0).then_some(exited),
                }
            })
            .collect()
    }
}

/// Held by a worker thread for its whole life; `Drop` runs on a normal
/// exit and during a panic unwind alike.
pub(crate) struct WorkerGuard {
    registry: Arc<ShardRegistry>,
    worker: usize,
}

impl WorkerGuard {
    pub(crate) fn enter(registry: Arc<ShardRegistry>, worker: usize) -> Self {
        registry.workers[worker]
            .running
            .store(true, Ordering::Release);
        WorkerGuard { registry, worker }
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let cell = &self.registry.workers[self.worker];
        cell.exited_at_ms
            .store(unix_ms_now().max(1), Ordering::Release);
        cell.running.store(false, Ordering::Release);
    }
}

/// One shard's handle on the registry, for its supervisor and flusher.
#[derive(Clone)]
pub(crate) struct ShardSlot {
    registry: Arc<ShardRegistry>,
    shard: usize,
}

impl ShardSlot {
    pub(crate) fn new(registry: Arc<ShardRegistry>, shard: usize) -> Self {
        ShardSlot { registry, shard }
    }
    #[cfg_attr(not(any(test, feature = "dst")), allow(dead_code))] // DST and tests only
    pub(crate) fn detached() -> Self {
        Self::new(ShardRegistry::detached(), 0)
    }
    pub(crate) fn mark_up(&self) {
        self.registry.mark_up(self.shard);
    }
    pub(crate) fn mark_respawning(&self) {
        self.registry.mark_respawning(self.shard);
    }
    pub(crate) fn mark_exit(&self, exit: SupervisorExit) {
        self.registry.mark_exit(self.shard, exit);
    }
    #[cfg_attr(not(test), allow(dead_code))] // tests; PR 3's hand-off marker
    pub(crate) fn registry(&self) -> &Arc<ShardRegistry> {
        &self.registry
    }
    #[cfg_attr(not(test), allow(dead_code))] // PR 3's hand-off marker reads it
    pub(crate) fn shard(&self) -> usize {
        self.shard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(shards: usize, workers: usize) -> Arc<ShardRegistry> {
        Arc::new(ShardRegistry::new(
            shards,
            workers,
            Arc::new(AtomicBool::new(false)),
        ))
    }
    fn all_up(r: &Arc<ShardRegistry>) -> Vec<WorkerGuard> {
        (0..r.shard_count()).for_each(|s| r.mark_up(s));
        (0..r.worker_count())
            .map(|w| WorkerGuard::enter(Arc::clone(r), w))
            .collect()
    }

    #[test]
    fn everything_starts_starting_and_unsettled_with_no_live_shard() {
        let r = registry(2, 1);
        assert_eq!(r.state(0), ShardState::Starting);
        assert!(!r.all_settled());
        assert_eq!(r.ingest_capable_count(), 0);
        assert_eq!(r.pick_live(0), None);
    }

    #[test]
    fn pick_live_round_robins_over_ingest_capable_shards_only() {
        let r = registry(4, 2);
        let _workers = all_up(&r);
        assert!(r.all_settled());
        let picks: Vec<u32> = (0..8).map(|n| r.pick_live(n).unwrap()).collect();
        assert_eq!(picks, vec![0, 1, 2, 3, 0, 1, 2, 3]);
        r.mark_exit(1, SupervisorExit::RespawnExhausted);
        let picks: Vec<u32> = (0..6).map(|n| r.pick_live(n).unwrap()).collect();
        assert_eq!(picks, vec![0, 2, 3, 0, 2, 3]);
        r.mark_respawning(2);
        assert!(
            r.ingest_capable(2),
            "Respawning counts: the batch queue survives a respawn"
        );
        assert_eq!(r.respawns(2), 1);
    }

    #[test]
    fn a_dead_worker_takes_its_shards_out_of_routing() {
        let r = registry(4, 2);
        let mut workers = all_up(&r);
        drop(workers.remove(1)); // worker 1 owns shards 1 and 3 (shard % worker_count)
        assert!(!r.worker_running(1));
        let picks: Vec<u32> = (0..4).map(|n| r.pick_live(n).unwrap()).collect();
        assert_eq!(picks, vec![0, 2, 0, 2]);
        let w = r.worker_evidence();
        assert!(w[1].exited_at_unix_ms.is_some());
        assert!(w[0].running && w[0].exited_at_unix_ms.is_none());
    }

    #[test]
    fn supervisor_exits_map_to_terminal_states() {
        let flag = Arc::new(AtomicBool::new(false));
        let r = ShardRegistry::new(3, 1, Arc::clone(&flag));
        r.mark_exit(0, SupervisorExit::CounterScanFailed);
        r.mark_exit(1, SupervisorExit::InputClosed);
        assert_eq!(
            (r.state(0), r.reason(0)),
            (ShardState::Offline, Some(OfflineReason::CounterScanFailed))
        );
        assert_eq!(
            (r.state(1), r.reason(1)),
            (ShardState::Offline, Some(OfflineReason::FlusherExited))
        );
        flag.store(true, Ordering::Release);
        r.mark_exit(2, SupervisorExit::InputClosed);
        assert_eq!(
            (r.state(2), r.reason(2)),
            (ShardState::Stopped, None),
            "input closed during shutdown is a clean stop"
        );
        assert!(r.all_settled());
    }

    #[test]
    fn the_handoff_marker_keeps_its_first_timestamp_and_clears_on_zero() {
        let r = registry(1, 1);
        assert_eq!(r.handoff_blocked_since_ms(0), None);
        r.set_handoff_blocked_since_ms(0, 100);
        r.set_handoff_blocked_since_ms(0, 200);
        assert_eq!(
            r.handoff_blocked_since_ms(0),
            Some(100),
            "the first timeout stamps it"
        );
        r.set_handoff_blocked_since_ms(0, 0);
        assert_eq!(r.handoff_blocked_since_ms(0), None);
        assert_eq!(r.shard_evidence()[0].handoff_blocked_since_unix_ms, None);
    }

    #[test]
    fn a_detached_slot_is_independent() {
        let a = ShardSlot::detached();
        let b = ShardSlot::detached();
        a.mark_up();
        assert_eq!(a.registry().state(0), ShardState::Up);
        assert_eq!(b.registry().state(0), ShardState::Starting);
    }

    #[test]
    fn a_slot_writes_only_its_own_shard() {
        let r = registry(2, 1);
        let slot = ShardSlot::new(Arc::clone(&r), 1);
        assert_eq!(slot.shard(), 1);
        slot.mark_respawning();
        assert_eq!((r.state(1), r.respawns(1)), (ShardState::Respawning, 1));
        slot.mark_exit(SupervisorExit::RespawnExhausted);
        assert_eq!(
            (r.state(1), r.reason(1)),
            (ShardState::Offline, Some(OfflineReason::RespawnExhausted))
        );
        assert_eq!((r.state(0), r.respawns(0)), (ShardState::Starting, 0));
    }

    #[test]
    fn a_panicking_worker_still_marks_itself_exited() {
        let r = registry(1, 1);
        let r2 = Arc::clone(&r);
        let joined = std::thread::spawn(move || {
            let _g = WorkerGuard::enter(r2, 0);
            panic!("worker body panicked");
        })
        .join();
        assert!(joined.is_err());
        assert!(!r.worker_running(0));
        assert!(r.worker_evidence()[0].exited_at_unix_ms.is_some());
    }
}
