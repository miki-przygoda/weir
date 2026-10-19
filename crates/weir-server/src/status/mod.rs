//! Live status (spec §5.2-5.3, §5.11, §5.14): the wait-free board the
//! daemon's threads publish into, the shard registry, the poll tasks and
//! the renderers. Writers store atomics or swap `ArcSwap` cells; readers
//! only load. No blocking lock type appears anywhere under this directory
//! (spec §9.2 rule 5); `no_blocking_locks_under_status` enforces it.
pub(crate) mod board;
pub(crate) mod shards;

pub(crate) use shards::{ShardRegistry, ShardSlot, SupervisorExit, WorkerGuard};

/// Milliseconds since the Unix epoch. 0 on a clock before 1970.
pub(crate) fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
