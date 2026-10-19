//! The facts the evaluator reads (spec §5.1 `Evidence`). Every sub-record is
//! `Option`: `None` means "not known", which makes the conditions that need
//! it unevaluated. Evidence may carry admin-only free text (`last_panic`,
//! `last_error`, paths); it deliberately does NOT implement `Serialize`, so it
//! cannot be sent anywhere except through a document type.
use crate::config::{ConfigValue, PublicConfig};
use crate::segment::SegmentId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A closed set of wire codes. Each enum is `Copy + Ord + Hash`, serialises
/// as its snake_case `as_str()`, and has a dense `u8` encoding (its index in
/// `ALL`) so a status-board cell can hold it in an `AtomicU8`.
macro_rules! closed_set {
    ($(#[$m:meta])* $name:ident { $($v:ident => $s:literal),* $(,)? }) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($v),* }

        impl $name {
            /// Every value, in declaration (and `u8`) order.
            pub const ALL: &'static [$name] = &[$($name::$v),*];

            /// The wire name (snake_case).
            pub fn as_str(self) -> &'static str { match self { $($name::$v => $s),* } }

            /// The dense encoding: the index of `self` in `ALL`.
            pub fn as_u8(self) -> u8 { self as u8 }

            /// The inverse of `as_u8`; `None` when out of range.
            pub fn from_u8(v: u8) -> Option<Self> { Self::ALL.get(usize::from(v)).copied() }
        }
    };
}

closed_set!(
    /// Startup and shutdown phases (spec §6), in order.
    Phase { Initializing => "initializing", Recovering => "recovering", Handoff => "handoff",
            Binding => "binding", Serving => "serving", ShuttingDown => "shutting_down" });
closed_set!(
    /// Where an `Evidence` came from: the live daemon, a metrics scrape, disk
    /// facts read offline, or a scrape plus disk.
    Source { Daemon => "daemon", Metrics => "metrics", Disk => "disk",
             MetricsAndDisk => "metrics_and_disk" });
closed_set!(
    /// A producer listener.
    ListenerKind { Unix => "unix", Tcp => "tcp" });
closed_set!(
    /// A producer listener's state.
    ListenerState { NotConfigured => "not_configured", Pending => "pending",
                    Bound => "bound", Closed => "closed", Failed => "failed" });
closed_set!(
    /// A shard's flusher state (spec §5.3).
    ShardState { Starting => "starting", Up => "up", Respawning => "respawning",
                 Offline => "offline", Stopped => "stopped" });
closed_set!(
    /// Why a shard went `Offline`.
    OfflineReason { RespawnExhausted => "respawn_exhausted",
                    CounterScanFailed => "counter_scan_failed",
                    FlusherExited => "flusher_exited" });
closed_set!(
    /// The drain's state machine (spec §8).
    DrainStateCode { Draining => "draining", RetryingTransient => "retrying_transient",
                     BlockedDeadLetterFull => "blocked_dead_letter_full",
                     Stopped => "stopped" });
closed_set!(
    /// The drain supervisor's exit status.
    DrainExitCode { Running => "running", Stopped => "stopped", GaveUp => "gave_up" });
closed_set!(
    /// The sink health probe's verdict.
    SinkHealthCode { Healthy => "healthy", Degraded => "degraded", Down => "down" });
closed_set!(
    /// The class of the last failed sink commit.
    SinkFailureKind { Transient => "transient", Permanent => "permanent",
                      Timeout => "timeout", ContractViolation => "contract_violation" });
closed_set!(
    /// The admin socket's state.
    AdminState { Disabled => "disabled", Bound => "bound", Unavailable => "unavailable" });
closed_set!(
    /// Why the admin socket is `Unavailable` (spec §9.3 `admin_unavailable`).
    AdminReason { ParentInsecure => "parent_insecure", PathTooLong => "path_too_long",
                  InUse => "in_use", BindFailed => "bind_failed" });
closed_set!(
    /// What asked for a reload.
    ReloadTriggerCode { Sighup => "sighup", Admin => "admin" });
closed_set!(
    /// A reload's overall outcome.
    ReloadOutcomeCode { Applied => "applied", Noop => "noop", Failed => "failed" });
closed_set!(
    /// One thing a reload can act on.
    ReloadItem { TlsCertificates => "tls_certificates", ConfigFile => "config_file" });
closed_set!(
    /// One reload item's outcome.
    ReloadItemOutcome { Reloaded => "reloaded", Failed => "failed",
                        NotConfigured => "not_configured" });
closed_set!(
    /// Why an item was not reloaded.
    NotReloadedReason { RestartRequired => "restart_required" });

impl Phase {
    /// `phase < serving` (spec §9.3 `starting`). `ShuttingDown` is not.
    pub fn before_serving(self) -> bool {
        self < Phase::Serving
    }
}

// closed_set! cannot place `#[default]` on a variant, so this one is by hand.
#[allow(clippy::derivable_impls)]
impl Default for Source {
    fn default() -> Self {
        Source::Daemon
    }
}

/// Everything the evaluator may read. `None` = not known (unevaluated).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Evidence {
    pub now_unix_ms: u64,
    pub source: Source,
    pub identity: Option<IdentityEvidence>,
    pub started_at_unix_ms: Option<u64>,
    pub phase: Option<Phase>,
    pub phase_since_unix_ms: Option<u64>,
    pub ever_served: Option<bool>,
    pub shutting_down: Option<bool>,
    pub shutting_down_since_unix_ms: Option<u64>,
    pub forced: Option<bool>,
    pub drain_deadline_unix_ms: Option<u64>,
    pub drain_budget_secs: Option<u64>,
    pub listeners: Option<Vec<ListenerEvidence>>,
    pub shards: Option<Vec<ShardEvidence>>,
    pub workers: Option<Vec<WorkerEvidence>>,
    pub queue_depth: Option<u64>,
    pub wab_cap: Option<CapEvidence>,
    pub channel: Option<ChannelEvidence>,
    pub drain: Option<DrainEvidence>,
    pub sink: Option<SinkEvidence>,
    pub backlog: Option<BacklogEvidence>,
    pub recent: Option<RecentCounters>,
    pub reload: Option<ReloadEvidence>,
    pub admin: Option<AdminEvidence>,
    pub config: Option<ConfigEvidence>,
}

impl Evidence {
    /// Nothing known except the time and where the (missing) facts came from.
    pub fn empty(now_unix_ms: u64, source: Source) -> Self {
        Evidence {
            now_unix_ms,
            source,
            ..Evidence::default()
        }
    }
}

/// Which daemon this is (admin only: carries paths).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityEvidence {
    pub version: String,
    pub wire_version: u8,
    pub pid: u32,
    pub started_at_unix_ms: u64,
    pub wab_dir: String,
    pub socket_path: String,
    pub wab_dir_dev: u64,
    pub wab_dir_ino: u64,
}

/// One producer listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenerEvidence {
    pub kind: ListenerKind,
    pub state: ListenerState,
}

/// One shard (spec §5.3).
#[derive(Clone, Debug, PartialEq)]
pub struct ShardEvidence {
    pub id: u16,
    pub state: ShardState,
    pub reason: Option<OfflineReason>,
    pub since_unix_ms: u64,
    pub respawns: u32,
    pub worker_running: bool,
    pub handoff_blocked_since_unix_ms: Option<u64>,
    pub active_segment_age_secs: Option<f64>,
}

impl ShardEvidence {
    /// Spec §5.3: `Up|Respawning` AND the owning worker is running.
    /// `Respawning` counts because the bounded batch queue survives a respawn.
    pub fn ingest_capable(&self) -> bool {
        matches!(self.state, ShardState::Up | ShardState::Respawning) && self.worker_running
    }
}

/// One worker thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerEvidence {
    pub id: u16,
    pub running: bool,
    /// Set once the worker thread has exited; `None` for one never started.
    pub exited_at_unix_ms: Option<u64>,
}

/// The `wab_max_bytes` cap and the size scans behind it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapEvidence {
    pub max_bytes: u64,
    pub low_water_bytes: u64,
    pub bytes_now: u64,
    pub rejecting: bool,
    pub first_scan_ok_unix_ms: Option<u64>,
    pub last_scan_ok_unix_ms: Option<u64>,
    /// The existing `should_warn_wab_growth` verdict on the last window.
    pub growing_unbounded: Option<bool>,
}

/// The flusher → drain channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelEvidence {
    pub depth: u64,
    pub capacity: u64,
    /// When the depth last rose to >= the high-water threshold; `None` below it.
    pub high_since_unix_ms: Option<u64>,
}

/// The drain (spec §5.4, §8).
#[derive(Clone, Debug, PartialEq)]
pub struct DrainEvidence {
    pub state: DrainStateCode,
    pub state_since_unix_ms: u64,
    pub pause_requested: bool,
    pub paused: bool,
    pub held: bool,
    pub paused_since_unix_ms: Option<u64>,
    pub paused_by_uid: Option<u32>,
    pub generation: u64,
    pub observed_requeue: u64,
    pub in_flight: Option<InFlightEvidence>,
    pub commit_in_flight_since_unix_ms: Option<u64>,
    /// The safety floor's busy signal (`DrainHeartbeat::busy()`): what the
    /// drain thread is inside right now; `None` between operations or unknown.
    pub busy: Option<BusyEvidence>,
    pub last_progress_unix_ms: Option<u64>,
    pub heartbeat_age_secs: Option<f64>,
    pub pending_segments: u64,
    pub backlog_segments: u64,
    pub replay_total: u64,
    pub replay_remaining: u64,
    pub blocked_since_unix_ms: Option<u64>,
    pub last_requeue: Option<RequeueEvidence>,
    pub exit: DrainExitCode,
    /// Admin-only free text: the last panic payload.
    pub last_panic: Option<String>,
}

/// One reading of the drain's busy signal. `sink_call` is true inside a sink
/// commit or health probe, false inside local I/O (segment read, fsync,
/// dead-letter write, quarantine move, directory scan), which is never a
/// wedge (weir-server `classify_drain_stall`, `SlowLocalIo`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BusyEvidence {
    pub sink_call: bool,
    pub since_unix_ms: u64,
}

/// The segment the drain is delivering right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InFlightEvidence {
    pub segment: SegmentId,
    pub attempt: u32,
    pub retries_left: u32,
    pub processed_records: u64,
    pub next_retry_at_unix_ms: Option<u64>,
}

/// The last `retry-stranded` request's result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequeueEvidence {
    pub ticket: u64,
    pub requeued: u64,
    pub already_queued: u64,
    pub backoff_cut: bool,
    pub coalesced: bool,
    /// Admin-only free text.
    pub scan_error: Option<String>,
    pub at_unix_ms: u64,
}

/// The sink, as the drain and its health probe see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkEvidence {
    pub health: Option<SinkHealthCode>,
    pub health_since_unix_ms: Option<u64>,
    pub last_success_unix_ms: Option<u64>,
    pub last_failure_unix_ms: Option<u64>,
    pub last_failure_kind: Option<SinkFailureKind>,
    pub consecutive_failures: u64,
    /// Admin-only free text (already redacted and capped by the writer).
    pub last_error: Option<String>,
    /// Admin-only free text.
    pub probe_reason: Option<String>,
}

/// What is buffered but not yet delivered (the read-only lag scan).
#[derive(Clone, Debug, PartialEq)]
pub struct BacklogEvidence {
    pub undelivered_segments: u64,
    pub undelivered_records: u64,
    /// Upper bound: measured from the segment header's `created_at`.
    pub oldest_undelivered_age_secs: Option<f64>,
    pub dead_letter_bytes: u64,
    pub dead_letter_max_bytes: Option<u64>,
    pub quarantine_bytes: u64,
    pub quarantine_files: u64,
    pub scanned_at_ms: u64,
}

/// Counter deltas over fixed windows, computed by the producer of the
/// evidence (the daemon's 5 s poll task). `None` = no window available
/// (a single offline scrape has no history).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecentCounters {
    pub ack_timeouts_5m: Option<u64>,
    pub fsync_failures_15m: Option<u64>,
    pub flusher_panics_15m: Option<u64>,
    pub stranded_since_last_success: Option<u64>,
    /// Absolute, this process.
    pub drain_panics_total: Option<u64>,
    pub recovery_segments_failed: Option<u64>,
    pub recovery_quarantine_copy_failed: Option<u64>,
    pub coalesce_ewma_us: Option<u64>,
}

/// The last reload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReloadEvidence {
    pub trigger: ReloadTriggerCode,
    pub at_unix_ms: u64,
    pub outcome: ReloadOutcomeCode,
    /// The last TLS reload attempt failed and none has succeeded since (sticky).
    pub tls_last_failed: bool,
    pub items: Vec<ReloadItemEvidence>,
    pub not_reloaded: Vec<NotReloadedEvidence>,
}

/// One item of the last reload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReloadItemEvidence {
    pub item: ReloadItem,
    pub outcome: ReloadItemOutcome,
    /// Admin-only free text.
    pub detail: Option<String>,
}

/// An item the last reload did not touch, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotReloadedEvidence {
    pub item: ReloadItem,
    pub reason_code: NotReloadedReason,
}

/// The admin socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminEvidence {
    pub enabled: bool,
    pub state: AdminState,
    pub reason: Option<AdminReason>,
    /// Admin-only (an absolute path).
    pub path: Option<String>,
}

/// The effective configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigEvidence {
    /// A closed-set code (`noop`, `http`, ...), not a URL.
    pub sink_type: String,
    /// `max(2 × sink_timeout_secs, 60)`, the drain's backstop.
    pub commit_timeout_secs: Option<u64>,
    pub public: PublicConfig,
    /// The full ConfigView (admin only).
    pub view: BTreeMap<String, ConfigValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_wire_names<T: Copy + Serialize + std::fmt::Debug>(
        all: &[T],
        name: fn(T) -> &'static str,
    ) {
        for v in all {
            assert_eq!(
                serde_json::to_string(v).unwrap(),
                format!("\"{}\"", name(*v)),
                "{v:?}"
            );
        }
    }

    #[test]
    fn closed_set_enums_serialise_as_their_as_str() {
        all_wire_names(Phase::ALL, Phase::as_str);
        all_wire_names(ListenerState::ALL, ListenerState::as_str);
        all_wire_names(ShardState::ALL, ShardState::as_str);
        all_wire_names(OfflineReason::ALL, OfflineReason::as_str);
        all_wire_names(DrainStateCode::ALL, DrainStateCode::as_str);
        all_wire_names(DrainExitCode::ALL, DrainExitCode::as_str);
        all_wire_names(SinkHealthCode::ALL, SinkHealthCode::as_str);
        all_wire_names(SinkFailureKind::ALL, SinkFailureKind::as_str);
        all_wire_names(AdminState::ALL, AdminState::as_str);
        all_wire_names(AdminReason::ALL, AdminReason::as_str);
        all_wire_names(ReloadItem::ALL, ReloadItem::as_str);
        assert_eq!(Phase::ShuttingDown.as_str(), "shutting_down");
        assert_eq!(
            SinkFailureKind::ContractViolation.as_str(),
            "contract_violation"
        );
    }

    #[test]
    fn u8_encoding_round_trips_and_rejects_out_of_range() {
        for p in Phase::ALL {
            assert_eq!(Phase::from_u8(p.as_u8()), Some(*p));
        }
        assert_eq!(Phase::from_u8(6), None);
        assert_eq!(
            ListenerState::from_u8(ListenerState::Failed.as_u8()),
            Some(ListenerState::Failed)
        );
    }

    #[test]
    fn only_the_four_startup_phases_are_before_serving() {
        let before: Vec<Phase> = Phase::ALL
            .iter()
            .copied()
            .filter(|p| p.before_serving())
            .collect();
        assert_eq!(
            before,
            vec![
                Phase::Initializing,
                Phase::Recovering,
                Phase::Handoff,
                Phase::Binding
            ]
        );
    }

    #[test]
    fn a_shard_is_ingest_capable_only_when_up_or_respawning_with_its_worker_running() {
        let shard = |state, worker_running| ShardEvidence {
            id: 0,
            state,
            reason: None,
            since_unix_ms: 0,
            respawns: 0,
            worker_running,
            handoff_blocked_since_unix_ms: None,
            active_segment_age_secs: None,
        };
        assert!(shard(ShardState::Up, true).ingest_capable());
        assert!(shard(ShardState::Respawning, true).ingest_capable());
        assert!(!shard(ShardState::Up, false).ingest_capable());
        for s in [
            ShardState::Starting,
            ShardState::Offline,
            ShardState::Stopped,
        ] {
            assert!(!shard(s, true).ingest_capable(), "{s:?}");
        }
    }

    #[test]
    fn empty_evidence_knows_only_the_time_and_the_source() {
        let e = Evidence::empty(1_000, Source::Disk);
        assert_eq!(e.now_unix_ms, 1_000);
        assert_eq!(e.source, Source::Disk);
        assert!(e.phase.is_none() && e.shards.is_none() && e.drain.is_none() && e.config.is_none());
        assert_eq!(Evidence::default().source, Source::Daemon);
    }
}
