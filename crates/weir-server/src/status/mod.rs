//! Live status (spec §5.2-5.3, §5.11, §5.14): the wait-free board the
//! daemon's threads publish into, the shard registry, the poll tasks and
//! the renderers. Writers store atomics or swap `ArcSwap` cells; readers
//! only load. No blocking lock type appears anywhere under this directory
//! (spec §9.2 rule 5); `no_blocking_locks_under_status` enforces it.
pub(crate) mod board;
pub(crate) mod config_view;
mod http;
pub(crate) mod poll;
pub(crate) mod render;
pub(crate) mod shards;

pub(crate) use board::StatusBoard;
#[cfg_attr(not(test), allow(unused_imports))] // PR 2 imports these from crate::status
pub(crate) use board::{DrainExitInfo, DrainExitReason, InFlight, RequeueResult};
#[cfg_attr(not(test), allow(unused_imports))] // PR 5 / PR 6 name it
pub(crate) use render::StatusSnapshot;
pub(crate) use shards::{ShardRegistry, ShardSlot, SupervisorExit, WorkerGuard};
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use weir_status::SinkFailureKind;

/// Milliseconds since the Unix epoch. 0 on a clock before 1970.
pub(crate) fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// A board with `shard_count` shards in `Starting` and no worker running.
#[cfg(test)]
pub(crate) fn test_board(shard_count: usize, worker_count: usize) -> std::sync::Arc<StatusBoard> {
    StatusBoard::for_test_with(shard_count, worker_count)
}

#[cfg(test)]
mod tests {
    /// Spec §9.2 rule 5: the status code takes no blocking lock, so a stalled
    /// reader can never delay an ack, a flusher or the drain. Walks every
    /// `.rs` file under src/status/ (subdirectories included), so a file a
    /// later task or PR adds (render.rs, poll.rs, PR 5's http.rs) is scanned
    /// without anyone listing it. Verified by mutation in Task 1.16 Step 4.
    #[test]
    fn no_blocking_locks_under_status() {
        let banned = [concat!("Mu", "tex"), concat!("Rw", "Lock")];
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/status");
        let mut pending = vec![dir.clone()];
        let mut scanned = 0;
        while let Some(d) = pending.pop() {
            for entry in std::fs::read_dir(&d).expect("src/status exists") {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let src = std::fs::read_to_string(&path).unwrap();
                    for b in banned {
                        assert!(
                            !src.contains(b),
                            "{} mentions {b}; status/ must stay wait-free (spec §9.2)",
                            path.display()
                        );
                    }
                    scanned += 1;
                }
            }
        }
        // mod.rs, board.rs, shards.rs and config_view.rs exist from this task on.
        assert!(
            scanned >= 4,
            "scanned only {scanned} files under {}",
            dir.display()
        );
    }
}
