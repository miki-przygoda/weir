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

/// A latch table where every active condition was first seen at `.0`.
pub struct FixedLatches(pub u64);

impl SinceLookup for FixedLatches {
    fn since(&self, _code: ConditionCode) -> Option<u64> {
        Some(self.0)
    }
}

/// `serving_evidence` with every admin-only free-text field populated and
/// three conditions active (drain_paused, sink_failing, tls_reload_failed),
/// so the golden document exercises every Option PR 5's sentinel test plants
/// into. Strings are recognisable, never real secrets.
pub fn full_evidence(now: u64) -> Evidence {
    let mut e = serving_evidence(now);
    let d = e.drain.as_mut().unwrap();
    d.pause_requested = true;
    d.paused = true;
    d.held = true;
    d.paused_since_unix_ms = Some(now - 30_000);
    d.paused_by_uid = Some(1000);
    d.in_flight = Some(InFlightEvidence {
        segment: SegmentId {
            shard: 1,
            counter: 42,
        },
        attempt: 2,
        retries_left: 1,
        processed_records: 512,
        next_retry_at_unix_ms: Some(now + 4_000),
    });
    d.commit_in_flight_since_unix_ms = Some(now - 500);
    d.last_requeue = Some(RequeueEvidence {
        ticket: 3,
        requeued: 2,
        already_queued: 1,
        backoff_cut: true,
        coalesced: false,
        scan_error: Some("fixture scan error text".into()),
        at_unix_ms: now - 10_000,
    });
    d.last_panic = Some("fixture panic text".into());
    d.replay_total = 300;
    d.replay_remaining = 12;
    d.pending_segments = 3;
    d.backlog_segments = 12;
    let s = e.sink.as_mut().unwrap();
    s.consecutive_failures = 4;
    s.last_failure_unix_ms = Some(now - 2_000);
    s.last_failure_kind = Some(SinkFailureKind::Transient);
    s.last_error = Some("fixture sink error text".into());
    s.probe_reason = Some("fixture probe reason text".into());
    let b = e.backlog.as_mut().unwrap();
    b.undelivered_segments = 15;
    b.undelivered_records = 4_096;
    b.oldest_undelivered_age_secs = Some(120.5);
    b.dead_letter_bytes = 1_024;
    let r = e.reload.as_mut().unwrap();
    r.outcome = ReloadOutcomeCode::Failed;
    r.tls_last_failed = true;
    r.items = vec![ReloadItemEvidence {
        item: ReloadItem::TlsCertificates,
        outcome: ReloadItemOutcome::Failed,
        detail: Some("fixture reload detail text".into()),
    }];
    let c = e.config.as_mut().unwrap();
    c.view.insert(
        "wab_dir".into(),
        ConfigValue::Str("/var/lib/weir/wab".into()),
    );
    c.view.insert(
        "sink_url".into(),
        ConfigValue::Str("https://sink.example:443".into()),
    );
    c.view
        .insert("sink_url_path_redacted".into(), ConfigValue::Bool(true));
    e.queue_depth = Some(7);
    e.channel = Some(ChannelEvidence {
        depth: 17,
        capacity: 256,
        high_since_unix_ms: None,
    });
    e
}
