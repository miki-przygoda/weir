//! Connection → shard assignment (spec §5.7). Pinning at accept is unchanged,
//! so per-connection ordering is unchanged; only WHICH shard is picked moves
//! from `n % shard_count` to "the (n mod k)-th ingest-capable shard".

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use tracing::warn;

use crate::metrics::Metrics;

/// At most one "no live shard" WARN per this interval, across both listeners.
pub(crate) const NO_LIVE_SHARD_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// The one question routing asks of the shard registry.
pub(crate) trait LiveShards: Send + Sync {
    fn pick_live(&self, n: u64) -> Option<u32>;
}

impl LiveShards for crate::status::StatusBoard {
    fn pick_live(&self, n: u64) -> Option<u32> {
        self.shards.pick_live(n)
    }
}

#[derive(Clone)]
enum Kind {
    /// Today's assignment. The `#[cfg(test)]` `socket::run` wrapper uses it.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the #[cfg(test)] socket::run wrapper is the only caller after Task 3.11"
        )
    )]
    RoundRobin { shard_count: u64 },
    /// Production from PR 3: only shards that can accept ingest.
    Live(Arc<dyn LiveShards>),
}

/// Shared by the Unix and TCP accept loops (clone it): one refusal limiter.
#[derive(Clone)]
pub(crate) struct ShardRouter {
    kind: Kind,
    warn_limiter: Arc<WarnLimiter>,
}

impl ShardRouter {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the #[cfg(test)] socket::run wrapper is the only caller after Task 3.11"
        )
    )]
    pub(crate) fn round_robin(shard_count: usize) -> Self {
        Self::with(Kind::RoundRobin {
            shard_count: shard_count.max(1) as u64,
        })
    }

    pub(crate) fn live(shards: Arc<dyn LiveShards>) -> Self {
        Self::with(Kind::Live(shards))
    }

    fn with(kind: Kind) -> Self {
        ShardRouter {
            kind,
            warn_limiter: Arc::new(WarnLimiter::new(NO_LIVE_SHARD_WARN_INTERVAL)),
        }
    }

    /// The shard for the `n`-th accepted connection, or `None` when no shard
    /// can take ingest (every one offline, or still starting).
    pub(crate) fn pick(&self, n: u64) -> Option<u32> {
        match &self.kind {
            Kind::RoundRobin { shard_count } => Some((n % shard_count) as u32),
            Kind::Live(shards) => shards.pick_live(n),
        }
    }

    /// Counts a refused connection and WARNs, rate-limited. The caller drops
    /// the stream: no frame has been read, so the producer sees a closed
    /// connection and retries, as with the connection cap.
    pub(crate) fn refuse(&self, metrics: &Metrics, transport: &'static str) {
        metrics.connections_refused_no_live_shard.inc();
        if self.refusal_warn_due() {
            warn!(
                transport,
                "refusing connection: no shard can accept ingest (every shard is offline or \
                 still starting); see weir_wab_shard_up and the flusher logs"
            );
        }
    }

    fn refusal_warn_due(&self) -> bool {
        self.warn_limiter.due()
    }
}

/// Lock-free "at most once per interval".
struct WarnLimiter {
    epoch: Instant,
    interval_ms: u64,
    /// Milliseconds since `epoch` before which no WARN is due; 0 = due now.
    next_ms: AtomicU64,
}

impl WarnLimiter {
    fn new(interval: Duration) -> Self {
        WarnLimiter {
            epoch: Instant::now(),
            interval_ms: interval.as_millis() as u64,
            next_ms: AtomicU64::new(0),
        }
    }

    /// True for exactly one caller per interval. A lost compare-exchange
    /// means another caller claimed this interval's WARN, so the loser stays
    /// quiet rather than retrying.
    fn due(&self) -> bool {
        let now = self.epoch.elapsed().as_millis() as u64;
        let next = self.next_ms.load(Relaxed);
        now >= next
            && self
                .next_ms
                .compare_exchange(next, now + self.interval_ms, Relaxed, Relaxed)
                .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shards 0..k; the ones in `down` are not ingest-capable.
    struct FakeShards {
        k: u32,
        down: Vec<u32>,
    }
    impl LiveShards for FakeShards {
        fn pick_live(&self, n: u64) -> Option<u32> {
            let live: Vec<u32> = (0..self.k).filter(|s| !self.down.contains(s)).collect();
            (!live.is_empty()).then(|| live[(n % live.len() as u64) as usize])
        }
    }

    #[test]
    fn round_robin_is_todays_assignment() {
        let r = ShardRouter::round_robin(3);
        let picks: Vec<_> = (0..6).map(|n| r.pick(n)).collect();
        assert_eq!(picks, [0, 1, 2, 0, 1, 2].map(Some));
        assert_eq!(
            ShardRouter::round_robin(0).pick(5),
            Some(0),
            "shard_count 0 is clamped to 1"
        );
    }

    #[test]
    fn live_routing_skips_an_offline_shard_and_reports_none_when_all_are_down() {
        let r = ShardRouter::live(Arc::new(FakeShards {
            k: 3,
            down: vec![1],
        }));
        let picks: Vec<_> = (0..100).filter_map(|n| r.pick(n)).collect();
        assert_eq!(picks.len(), 100);
        assert!(!picks.contains(&1));
        assert!(
            picks.contains(&0) && picks.contains(&2),
            "the live shards share the load"
        );

        let none = ShardRouter::live(Arc::new(FakeShards {
            k: 2,
            down: vec![0, 1],
        }));
        assert_eq!(none.pick(0), None);
    }

    #[test]
    fn a_refusal_always_counts_but_warns_at_most_once_per_interval() {
        let m = Metrics::new().0;
        let r = ShardRouter::round_robin(1);
        let shared = r.clone();
        assert!(r.refusal_warn_due(), "the first refusal warns");
        assert!(
            !shared.refusal_warn_due(),
            "clones share the limiter (Unix and TCP)"
        );
        r.refuse(&m, "unix");
        shared.refuse(&m, "tcp");
        assert_eq!(m.connections_refused_no_live_shard.get(), 2);
    }
}
