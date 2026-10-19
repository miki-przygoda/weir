//! Fixtures shared by this crate's tests, weir-server's tests and weir-ctl's
//! tests. `#[doc(hidden)]`: not part of the supported API.
use crate::*;
use std::collections::BTreeMap;

/// A healthy two-shard daemon, ten minutes up, serving; every sub-record
/// present, so no condition is active and none is unevaluated.
pub fn serving_evidence(now: u64) -> Evidence {
    let mut view = BTreeMap::new();
    for (k, v) in [
        ("shard_count", 2u64),
        ("worker_count", 2),
        ("wab_max_bytes", 1_000_000_000),
        ("dead_letter_max_bytes", 100_000_000),
        ("wab_segment_max_bytes", 268_435_456),
        ("wab_segment_max_age_secs", 0),
        ("wab_segment_max_lifetime_secs", 0),
        ("sink_max_retries", 3),
        ("sink_timeout_secs", 30),
        ("health_poll_interval_secs", 30),
        ("shutdown_timeout_secs", 30),
        ("drain_shutdown_budget_secs", 10),
        ("max_connections", 1024),
    ] {
        view.insert(k.to_string(), ConfigValue::U64(v));
    }
    view.insert("peer_uid_check".to_string(), ConfigValue::Bool(true));
    view.insert("tls_enabled".to_string(), ConfigValue::Bool(false));
    view.insert("sink_type".to_string(), ConfigValue::Str("http".into()));
    Evidence {
        now_unix_ms: now,
        source: Source::Daemon,
        identity: Some(IdentityEvidence {
            version: "4.1.0".into(),
            wire_version: 1,
            pid: 4242,
            started_at_unix_ms: now - 600_000,
            wab_dir: "/var/lib/weir/wab".into(),
            socket_path: "/run/weir/weir.sock".into(),
            wab_dir_dev: 66_306,
            wab_dir_ino: 1_048_577,
        }),
        started_at_unix_ms: Some(now - 600_000),
        phase: Some(Phase::Serving),
        phase_since_unix_ms: Some(now - 590_000),
        ever_served: Some(true),
        shutting_down: Some(false),
        shutting_down_since_unix_ms: None,
        forced: Some(false),
        drain_deadline_unix_ms: None,
        drain_budget_secs: Some(10),
        listeners: Some(vec![ListenerEvidence {
            kind: ListenerKind::Unix,
            state: ListenerState::Bound,
        }]),
        shards: Some(
            (0..2)
                .map(|id| ShardEvidence {
                    id,
                    state: ShardState::Up,
                    reason: None,
                    since_unix_ms: now - 590_000,
                    respawns: 0,
                    worker_running: true,
                    handoff_blocked_since_unix_ms: None,
                    active_segment_age_secs: Some(12.0),
                })
                .collect(),
        ),
        workers: Some(
            (0..2)
                .map(|id| WorkerEvidence {
                    id,
                    running: true,
                    exited_at_unix_ms: None,
                })
                .collect(),
        ),
        queue_depth: Some(0),
        wab_cap: Some(CapEvidence {
            max_bytes: 1_000_000_000,
            low_water_bytes: 900_000_000,
            bytes_now: 10_000,
            rejecting: false,
            first_scan_ok_unix_ms: Some(now - 595_000),
            last_scan_ok_unix_ms: Some(now - 2_000),
            growing_unbounded: Some(false),
        }),
        channel: Some(ChannelEvidence {
            depth: 0,
            capacity: 256,
            high_since_unix_ms: None,
        }),
        drain: Some(DrainEvidence {
            state: DrainStateCode::Draining,
            state_since_unix_ms: now - 590_000,
            pause_requested: false,
            paused: false,
            held: false,
            paused_since_unix_ms: None,
            paused_by_uid: None,
            generation: 1,
            observed_requeue: 0,
            in_flight: None,
            commit_in_flight_since_unix_ms: None,
            busy: None,
            last_progress_unix_ms: Some(now - 1_000),
            heartbeat_age_secs: Some(1.0),
            pending_segments: 0,
            backlog_segments: 0,
            replay_total: 0,
            replay_remaining: 0,
            blocked_since_unix_ms: None,
            last_requeue: None,
            exit: DrainExitCode::Running,
            last_panic: None,
        }),
        sink: Some(SinkEvidence {
            health: Some(SinkHealthCode::Healthy),
            health_since_unix_ms: Some(now - 590_000),
            last_success_unix_ms: Some(now - 1_000),
            last_failure_unix_ms: None,
            last_failure_kind: None,
            consecutive_failures: 0,
            last_error: None,
            probe_reason: None,
        }),
        backlog: Some(BacklogEvidence {
            undelivered_segments: 0,
            undelivered_records: 0,
            oldest_undelivered_age_secs: None,
            dead_letter_bytes: 0,
            dead_letter_max_bytes: Some(100_000_000),
            quarantine_bytes: 0,
            quarantine_files: 0,
            scanned_at_ms: now - 2_000,
        }),
        recent: Some(RecentCounters {
            ack_timeouts_5m: Some(0),
            fsync_failures_15m: Some(0),
            flusher_panics_15m: Some(0),
            stranded_since_last_success: Some(0),
            drain_panics_total: Some(0),
            recovery_segments_failed: Some(0),
            recovery_quarantine_copy_failed: Some(0),
            coalesce_ewma_us: Some(200),
        }),
        reload: Some(ReloadEvidence {
            trigger: ReloadTriggerCode::Sighup,
            at_unix_ms: now - 300_000,
            outcome: ReloadOutcomeCode::Noop,
            tls_last_failed: false,
            items: vec![],
            not_reloaded: vec![NotReloadedEvidence {
                item: ReloadItem::ConfigFile,
                reason_code: NotReloadedReason::RestartRequired,
            }],
        }),
        admin: Some(AdminEvidence {
            enabled: true,
            state: AdminState::Bound,
            reason: None,
            path: Some("/var/lib/weir/wab.admin.sock".into()),
        }),
        config: Some(ConfigEvidence {
            sink_type: "http".into(),
            commit_timeout_secs: Some(60),
            public: PublicConfig::from_view(&view),
            view,
        }),
    }
}
