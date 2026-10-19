//! `weir.status/v1`, admin rendering (spec §9.1). The public rendering is a
//! separate type built from this one (PR 5). Conventions: times are unix-ms
//! integers, ages f64 seconds, an unknown value is `null`. Additive-only.
use crate::codes::ConditionCode;
use crate::condition::{Condition, Evaluation, Thresholds};
use crate::config::{ConfigValue, PublicConfig};
use crate::evidence::*;
use crate::probe::{LivezCheck, liveness, readiness, startup};
use crate::segment::SegmentId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCHEMA: &str = "weir.status/v1";
pub const CHANNEL_CAPACITY: u64 = 256;
pub const ADMIN_PROTOCOL: u32 = 1;
pub const ADMIN_VERBS: [&str; 5] = [
    "status",
    "reload",
    "drain.pause",
    "drain.resume",
    "drain.retry_stranded",
];
/// Spec §10: the document is bounded at 256 shards and 40 conditions.
pub const MAX_DOC_SHARDS: usize = 256;
pub const MAX_DOC_CONDITIONS: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
    Public,
    Admin,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdminStatusDoc {
    pub schema: String,
    pub view: View,
    pub version: String,
    pub wire_version: u8,
    pub generated_at_ms: u64,
    pub started_at_ms: u64,
    pub uptime_secs: f64,
    pub pid: u32,
    pub instance: InstanceView,
    pub verdict: Verdict,
    pub conditions: Vec<Condition>,
    pub unevaluated: Vec<ConditionCode>,
    pub startup: StartupView,
    pub lifecycle: LifecycleView,
    pub ingest: IngestView,
    pub shards: Vec<ShardView>,
    pub drain: Option<AdminDrainView>,
    pub sink: Option<AdminSinkView>,
    pub backlog: Option<BacklogView>,
    pub reload: AdminReloadView,
    pub admin: AdminSurfaceView,
    pub config: AdminConfigDoc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceView {
    pub wab_dir: String,
    pub socket_path: String,
    pub wab_dir_dev: u64,
    pub wab_dir_ino: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    pub ready: bool,
    pub live: bool,
    pub startup_complete: bool,
    pub livez_checks: Vec<LivezCheck>,
    pub readiness_failing: Vec<ConditionCode>,
    pub liveness_failing: Vec<ConditionCode>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayView {
    pub total: u64,
    pub remaining: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupView {
    pub phase: Option<Phase>,
    pub phase_since_ms: Option<u64>,
    pub replay: Option<ReplayView>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleView {
    pub shutting_down: Option<bool>,
    pub shutting_down_since_ms: Option<u64>,
    pub forced: Option<bool>,
    pub drain_deadline_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerView {
    pub kind: ListenerKind,
    pub state: ListenerState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WabCapView {
    pub max_bytes: u64,
    pub low_water_bytes: u64,
    pub bytes_now: u64,
    pub rejecting: bool,
    pub last_scan_ok_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestView {
    pub listeners: Vec<ListenerView>,
    pub queue_depth: Option<u64>,
    pub wab_cap: Option<WabCapView>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShardView {
    pub id: u16,
    pub state: ShardState,
    pub reason: Option<OfflineReason>,
    pub since_ms: u64,
    pub respawns: u32,
    pub worker_running: bool,
    pub handoff_blocked_since_ms: Option<u64>,
    pub active_segment_age_secs: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightView {
    pub segment: SegmentId,
    pub attempt: u32,
    pub retries_left: u32,
    pub processed_records: u64,
    pub next_retry_at_ms: Option<u64>,
    pub commit_in_flight_since_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminRequeueView {
    pub ticket: u64,
    pub requeued: u64,
    pub already_queued: u64,
    pub backoff_cut: bool,
    pub coalesced: bool,
    pub scan_failed: bool,
    pub at_ms: u64,
    pub scan_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdminDrainView {
    pub state: DrainStateCode,
    pub state_since_ms: Option<u64>,
    pub paused: bool,
    pub pause_requested: bool,
    pub held: bool,
    pub paused_since_ms: Option<u64>,
    pub paused_by_uid: Option<u32>,
    pub generation: u64,
    pub observed_requeue: u64,
    pub in_flight: Option<InFlightView>,
    pub pending_segments: u64,
    pub backlog_segments: u64,
    pub channel_depth: Option<u64>,
    pub channel_capacity: u64,
    pub heartbeat_age_secs: Option<f64>,
    pub blocked_since_ms: Option<u64>,
    pub last_requeue: Option<AdminRequeueView>,
    pub exit: DrainExitCode,
    pub last_panic: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSinkFailure {
    pub at_ms: u64,
    pub kind: Option<SinkFailureKind>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSinkView {
    #[serde(rename = "type")]
    pub sink_type: String,
    pub health: Option<SinkHealthCode>,
    pub health_since_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_failure: Option<AdminSinkFailure>,
    pub consecutive_failures: u64,
    pub probe_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BacklogView {
    pub undelivered_segments: u64,
    pub undelivered_records: u64,
    pub oldest_undelivered_age_secs: Option<f64>,
    pub dead_letter_bytes: u64,
    pub dead_letter_max_bytes: Option<u64>,
    pub quarantine_bytes: u64,
    pub quarantine_files: u64,
    pub scanned_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminReloadItem {
    pub item: String,
    pub outcome: ReloadItemOutcome,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotReloadedView {
    pub item: String,
    pub reason_code: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminReloadLast {
    pub trigger: ReloadTriggerCode,
    pub at_ms: u64,
    pub outcome: ReloadOutcomeCode,
    pub items: Vec<AdminReloadItem>,
    pub not_reloaded: Vec<NotReloadedView>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminReloadView {
    pub last: Option<AdminReloadLast>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSurfaceView {
    pub protocol: u32,
    pub verbs: Vec<String>,
    pub state: Option<AdminState>,
    pub reason_code: Option<String>,
    pub path: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AdminConfigDoc {
    pub public: PublicConfig,
    /// The full ConfigView (spec §5.14). Admin only by construction.
    pub view: BTreeMap<String, ConfigValue>,
}

impl AdminStatusDoc {
    /// Renders one admin document from one set of facts and its evaluation.
    /// `identity` is required: only the daemon itself builds this document.
    pub fn build(
        identity: &IdentityEvidence,
        e: &Evidence,
        eval: &Evaluation,
        t: &Thresholds,
        livez_checks: &[LivezCheck],
    ) -> Self {
        let ready = readiness(e);
        let live = liveness(e, t, livez_checks);
        let started = startup(e);
        let now = e.now_unix_ms;
        AdminStatusDoc {
            schema: SCHEMA.to_string(),
            view: View::Admin,
            version: identity.version.clone(),
            wire_version: identity.wire_version,
            generated_at_ms: now,
            started_at_ms: identity.started_at_unix_ms,
            uptime_secs: now.saturating_sub(identity.started_at_unix_ms) as f64 / 1000.0,
            pid: identity.pid,
            instance: InstanceView {
                wab_dir: identity.wab_dir.clone(),
                socket_path: identity.socket_path.clone(),
                wab_dir_dev: identity.wab_dir_dev,
                wab_dir_ino: identity.wab_dir_ino,
            },
            verdict: Verdict {
                ready: ready.ok,
                live: live.ok,
                startup_complete: started.ok,
                livez_checks: livez_checks.to_vec(),
                readiness_failing: ready.failing,
                liveness_failing: live.failing,
            },
            conditions: eval
                .conditions
                .iter()
                .take(MAX_DOC_CONDITIONS)
                .cloned()
                .collect(),
            unevaluated: eval.unevaluated.clone(),
            startup: StartupView {
                phase: e.phase,
                phase_since_ms: e.phase_since_unix_ms,
                replay: e.drain.as_ref().map(|d| ReplayView {
                    total: d.replay_total,
                    remaining: d.replay_remaining,
                }),
            },
            lifecycle: LifecycleView {
                shutting_down: e.shutting_down,
                shutting_down_since_ms: e.shutting_down_since_unix_ms,
                forced: e.forced,
                drain_deadline_ms: e.drain_deadline_unix_ms,
            },
            ingest: IngestView {
                listeners: e
                    .listeners
                    .iter()
                    .flatten()
                    .map(|l| ListenerView {
                        kind: l.kind,
                        state: l.state,
                    })
                    .collect(),
                queue_depth: e.queue_depth,
                wab_cap: e.wab_cap.map(|c| WabCapView {
                    max_bytes: c.max_bytes,
                    low_water_bytes: c.low_water_bytes,
                    bytes_now: c.bytes_now,
                    rejecting: c.rejecting,
                    last_scan_ok_ms: c.last_scan_ok_unix_ms,
                }),
            },
            shards: e
                .shards
                .iter()
                .flatten()
                .take(MAX_DOC_SHARDS)
                .map(shard_view)
                .collect(),
            drain: e.drain.as_ref().map(|d| drain_view(d, e.channel.as_ref())),
            sink: e.sink.as_ref().map(|s| sink_view(s, e.config.as_ref())),
            backlog: e.backlog.as_ref().map(|b| BacklogView {
                undelivered_segments: b.undelivered_segments,
                undelivered_records: b.undelivered_records,
                oldest_undelivered_age_secs: b.oldest_undelivered_age_secs,
                dead_letter_bytes: b.dead_letter_bytes,
                dead_letter_max_bytes: b.dead_letter_max_bytes,
                quarantine_bytes: b.quarantine_bytes,
                quarantine_files: b.quarantine_files,
                scanned_at_ms: b.scanned_at_ms,
            }),
            reload: AdminReloadView {
                last: e.reload.as_ref().map(reload_last),
            },
            admin: AdminSurfaceView {
                protocol: ADMIN_PROTOCOL,
                verbs: ADMIN_VERBS.iter().map(|v| v.to_string()).collect(),
                state: e.admin.as_ref().map(|a| a.state),
                reason_code: e
                    .admin
                    .as_ref()
                    .and_then(|a| a.reason)
                    .map(|r| r.as_str().to_string()),
                path: e.admin.as_ref().and_then(|a| a.path.clone()),
            },
            config: e
                .config
                .as_ref()
                .map(|c| AdminConfigDoc {
                    public: c.public.clone(),
                    view: c.view.clone(),
                })
                .unwrap_or_default(),
        }
    }
}

fn shard_view(s: &ShardEvidence) -> ShardView {
    ShardView {
        id: s.id,
        state: s.state,
        reason: s.reason,
        since_ms: s.since_unix_ms,
        respawns: s.respawns,
        worker_running: s.worker_running,
        handoff_blocked_since_ms: s.handoff_blocked_since_unix_ms,
        active_segment_age_secs: s.active_segment_age_secs,
    }
}

fn drain_view(d: &DrainEvidence, channel: Option<&ChannelEvidence>) -> AdminDrainView {
    AdminDrainView {
        state: d.state,
        state_since_ms: Some(d.state_since_unix_ms),
        paused: d.paused,
        pause_requested: d.pause_requested,
        held: d.held,
        paused_since_ms: d.paused_since_unix_ms,
        paused_by_uid: d.paused_by_uid,
        generation: d.generation,
        observed_requeue: d.observed_requeue,
        in_flight: d.in_flight.map(|i| InFlightView {
            segment: i.segment,
            attempt: i.attempt,
            retries_left: i.retries_left,
            processed_records: i.processed_records,
            next_retry_at_ms: i.next_retry_at_unix_ms,
            commit_in_flight_since_ms: d.commit_in_flight_since_unix_ms,
        }),
        pending_segments: d.pending_segments,
        backlog_segments: d.backlog_segments,
        channel_depth: channel.map(|c| c.depth),
        channel_capacity: CHANNEL_CAPACITY,
        heartbeat_age_secs: d.heartbeat_age_secs,
        blocked_since_ms: d.blocked_since_unix_ms,
        last_requeue: d.last_requeue.as_ref().map(|r| AdminRequeueView {
            ticket: r.ticket,
            requeued: r.requeued,
            already_queued: r.already_queued,
            backoff_cut: r.backoff_cut,
            coalesced: r.coalesced,
            scan_failed: r.scan_error.is_some(),
            at_ms: r.at_unix_ms,
            scan_error: r.scan_error.clone(),
        }),
        exit: d.exit,
        last_panic: d.last_panic.clone(),
    }
}

fn sink_view(s: &SinkEvidence, config: Option<&ConfigEvidence>) -> AdminSinkView {
    AdminSinkView {
        sink_type: config.map_or_else(|| "unknown".to_string(), |c| c.sink_type.clone()),
        health: s.health,
        health_since_ms: s.health_since_unix_ms,
        last_success_ms: s.last_success_unix_ms,
        last_failure: s.last_failure_unix_ms.map(|at| AdminSinkFailure {
            at_ms: at,
            kind: s.last_failure_kind,
            message: s.last_error.clone(),
        }),
        consecutive_failures: s.consecutive_failures,
        probe_reason: s.probe_reason.clone(),
    }
}

fn reload_last(r: &ReloadEvidence) -> AdminReloadLast {
    AdminReloadLast {
        trigger: r.trigger,
        at_ms: r.at_unix_ms,
        outcome: r.outcome,
        items: r
            .items
            .iter()
            .map(|i| AdminReloadItem {
                item: i.item.as_str().to_string(),
                outcome: i.outcome,
                detail: i.detail.clone(),
            })
            .collect(),
        not_reloaded: r
            .not_reloaded
            .iter()
            .map(|n| NotReloadedView {
                item: n.item.as_str().to_string(),
                reason_code: n.reason_code.as_str().to_string(),
            })
            .collect(),
    }
}
