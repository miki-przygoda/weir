//! The public rendering of `weir.status/v1` (spec §5.1 rules, §9.1).
//!
//! `PublicStatusDoc` is a separate type, not a filtered serialization of
//! `AdminStatusDoc`. Its only String-typed fields are closed-enum
//! renderings, condition codes, segment identities, the schema and version,
//! and the template-rendered `summary`. Panic messages, I/O and sink errors,
//! probe reasons, reload detail, uids, the pid, absolute paths and the sink
//! URL, host or bucket have no field here, so they cannot be serialized into
//! `/status`. Every admin struct is destructured without `..`: a field added
//! to the admin document does not compile until it is classified here. The
//! one exception is `AdminConfigDoc`, whose every field beyond `public` is
//! admin only by construction.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    AdminConfigDoc, AdminDrainView, AdminReloadItem, AdminReloadLast, AdminReloadView,
    AdminRequeueView, AdminSinkFailure, AdminSinkView, AdminState, AdminStatusDoc,
    AdminSurfaceView, Affects, BacklogView, Condition, ConditionCode, DrainExitCode,
    DrainStateCode, EvidenceValue, Gate, InFlightView, IngestView, LifecycleView,
    MAX_DOC_CONDITIONS, MAX_DOC_SHARDS, NotReloadedView, PublicConfig, ReloadItemOutcome,
    ReloadOutcomeCode, ReloadTriggerCode, SegmentId, Severity, ShardView, SinkFailureKind,
    SinkHealthCode, StartupView, Verdict, View,
};

/// `/status` is bounded (spec §10): `shard_count` is at most 256 already, and
/// the catalogue has 30 codes; the caps make the bound explicit. The same
/// bounds PR 1's admin document applies.
pub const MAX_PUBLIC_SHARDS: usize = MAX_DOC_SHARDS;
pub const MAX_PUBLIC_CONDITIONS: usize = MAX_DOC_CONDITIONS;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicStatusDoc {
    pub schema: String,
    pub view: View,
    pub version: String,
    pub wire_version: u8,
    pub generated_at_ms: u64,
    pub started_at_ms: u64,
    pub uptime_secs: f64,
    pub verdict: Verdict,
    pub conditions: Vec<PublicCondition>,
    pub unevaluated: Vec<ConditionCode>,
    pub startup: StartupView,
    pub lifecycle: LifecycleView,
    pub ingest: IngestView,
    pub shards: Vec<ShardView>,
    /// `None` until the drain publishes (PR 1: unknown is `null`).
    pub drain: Option<PublicDrainView>,
    pub sink: Option<PublicSinkView>,
    pub backlog: Option<BacklogView>,
    pub reload: PublicReloadView,
    pub admin: PublicAdminSurface,
    pub config: PublicConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicCondition {
    pub code: ConditionCode,
    pub severity: Severity,
    pub affects: Affects,
    pub since_ms: Option<u64>,
    pub summary: String,
    pub evidence: BTreeMap<String, PublicEvidenceValue>,
    pub runbook: String,
    pub gates: Vec<Gate>,
}

/// `EvidenceValue` without `Text`. The same serde shape as PR 1's
/// `EvidenceValue` (untagged, same variant order: a segment-id string reads
/// as `Segment`, any other string as `Code`) so weir-ctl parses both
/// renderings with one reader.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PublicEvidenceValue {
    U64(u64),
    F64(f64),
    Bool(bool),
    Segment(SegmentId),
    Shards(Vec<u16>),
    Code(String),
}

impl PublicEvidenceValue {
    /// `None` for admin-only evidence. Exhaustive on purpose: a new
    /// `EvidenceValue` variant fails to compile until it is classified.
    pub fn from_admin(v: &EvidenceValue) -> Option<Self> {
        Some(match v {
            EvidenceValue::U64(n) => Self::U64(*n),
            EvidenceValue::F64(x) => Self::F64(*x),
            EvidenceValue::Bool(b) => Self::Bool(*b),
            EvidenceValue::Code(c) => Self::Code(c.clone()),
            EvidenceValue::Segment(s) => Self::Segment(*s),
            EvidenceValue::Shards(s) => Self::Shards(s.clone()),
            EvidenceValue::Text(_) => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicDrainView {
    pub state: DrainStateCode,
    pub state_since_ms: Option<u64>,
    pub paused: bool,
    pub pause_requested: bool,
    pub held: bool,
    pub paused_since_ms: Option<u64>,
    pub generation: u64,
    pub observed_requeue: u64,
    pub in_flight: Option<InFlightView>,
    pub pending_segments: u64,
    pub backlog_segments: u64,
    pub channel_depth: Option<u64>,
    pub channel_capacity: u64,
    pub heartbeat_age_secs: Option<f64>,
    pub blocked_since_ms: Option<u64>,
    pub last_requeue: Option<PublicRequeueView>,
    pub exit: DrainExitCode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicRequeueView {
    pub ticket: u64,
    pub requeued: u64,
    pub already_queued: u64,
    pub backoff_cut: bool,
    pub coalesced: bool,
    pub scan_failed: bool,
    pub at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicSinkView {
    #[serde(rename = "type")]
    pub sink_type: String,
    pub health: Option<SinkHealthCode>,
    pub health_since_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_failure: Option<PublicSinkFailure>,
    pub consecutive_failures: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicSinkFailure {
    pub at_ms: u64,
    pub kind: Option<SinkFailureKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicReloadView {
    pub last: Option<PublicReloadLast>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicReloadLast {
    pub trigger: ReloadTriggerCode,
    pub at_ms: u64,
    pub outcome: ReloadOutcomeCode,
    pub items: Vec<PublicReloadItem>,
    pub not_reloaded: Vec<NotReloadedView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicReloadItem {
    pub item: String,
    pub outcome: ReloadItemOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicAdminSurface {
    pub protocol: u32,
    pub verbs: Vec<String>,
    pub state: Option<AdminState>,
    pub reason_code: Option<String>,
}

fn public_condition(c: &Condition) -> PublicCondition {
    let Condition {
        code,
        severity,
        affects,
        since_unix_ms,
        summary,
        detail: _,
        evidence,
        runbook,
        gates,
    } = c;
    PublicCondition {
        code: *code,
        severity: *severity,
        affects: *affects,
        since_ms: *since_unix_ms,
        summary: summary.clone(),
        evidence: evidence
            .iter()
            .filter_map(|(k, v)| PublicEvidenceValue::from_admin(v).map(|pv| (k.clone(), pv)))
            .collect(),
        runbook: runbook.to_string(),
        gates: gates.clone(),
    }
}

fn public_drain(d: &AdminDrainView) -> PublicDrainView {
    let AdminDrainView {
        state,
        state_since_ms,
        paused,
        pause_requested,
        held,
        paused_since_ms,
        paused_by_uid: _,
        generation,
        observed_requeue,
        in_flight,
        pending_segments,
        backlog_segments,
        channel_depth,
        channel_capacity,
        heartbeat_age_secs,
        blocked_since_ms,
        last_requeue,
        exit,
        last_panic: _,
    } = d;
    PublicDrainView {
        state: *state,
        state_since_ms: *state_since_ms,
        paused: *paused,
        pause_requested: *pause_requested,
        held: *held,
        paused_since_ms: *paused_since_ms,
        generation: *generation,
        observed_requeue: *observed_requeue,
        in_flight: *in_flight,
        pending_segments: *pending_segments,
        backlog_segments: *backlog_segments,
        channel_depth: *channel_depth,
        channel_capacity: *channel_capacity,
        heartbeat_age_secs: *heartbeat_age_secs,
        blocked_since_ms: *blocked_since_ms,
        last_requeue: last_requeue.as_ref().map(|r| {
            let AdminRequeueView {
                ticket,
                requeued,
                already_queued,
                backoff_cut,
                coalesced,
                scan_failed,
                at_ms,
                scan_error: _,
            } = r;
            PublicRequeueView {
                ticket: *ticket,
                requeued: *requeued,
                already_queued: *already_queued,
                backoff_cut: *backoff_cut,
                coalesced: *coalesced,
                scan_failed: *scan_failed,
                at_ms: *at_ms,
            }
        }),
        exit: *exit,
    }
}

fn public_sink(s: &AdminSinkView) -> PublicSinkView {
    let AdminSinkView {
        sink_type,
        health,
        health_since_ms,
        last_success_ms,
        last_failure,
        consecutive_failures,
        probe_reason: _,
    } = s;
    PublicSinkView {
        sink_type: sink_type.clone(),
        health: *health,
        health_since_ms: *health_since_ms,
        last_success_ms: *last_success_ms,
        last_failure: last_failure.as_ref().map(|f| {
            let AdminSinkFailure {
                at_ms,
                kind,
                message: _,
            } = f;
            PublicSinkFailure {
                at_ms: *at_ms,
                kind: *kind,
            }
        }),
        consecutive_failures: *consecutive_failures,
    }
}

fn public_reload(r: &AdminReloadView) -> PublicReloadView {
    let AdminReloadView { last } = r;
    PublicReloadView {
        last: last.as_ref().map(|l| {
            let AdminReloadLast {
                trigger,
                at_ms,
                outcome,
                items,
                not_reloaded,
            } = l;
            PublicReloadLast {
                trigger: *trigger,
                at_ms: *at_ms,
                outcome: *outcome,
                items: items
                    .iter()
                    .map(|i| {
                        let AdminReloadItem {
                            item,
                            outcome,
                            detail: _,
                        } = i;
                        PublicReloadItem {
                            item: item.clone(),
                            outcome: *outcome,
                        }
                    })
                    .collect(),
                not_reloaded: not_reloaded.clone(),
            }
        }),
    }
}

fn public_admin_surface(a: &AdminSurfaceView) -> PublicAdminSurface {
    let AdminSurfaceView {
        protocol,
        verbs,
        state,
        reason_code,
        path: _,
    } = a;
    PublicAdminSurface {
        protocol: *protocol,
        verbs: verbs.clone(),
        state: *state,
        reason_code: reason_code.clone(),
    }
}

/// The one way to build the public document (spec §5.1).
pub fn public(doc: &AdminStatusDoc) -> PublicStatusDoc {
    let AdminStatusDoc {
        schema,
        view: _,
        version,
        wire_version,
        generated_at_ms,
        started_at_ms,
        uptime_secs,
        pid: _,
        instance: _,
        verdict,
        conditions,
        unevaluated,
        startup,
        lifecycle,
        ingest,
        shards,
        drain,
        sink,
        backlog,
        reload,
        admin,
        config,
    } = doc;
    let AdminConfigDoc {
        public: public_config,
        ..
    } = config; // the full view is admin only
    PublicStatusDoc {
        schema: schema.clone(),
        view: View::Public,
        version: version.clone(),
        wire_version: *wire_version,
        generated_at_ms: *generated_at_ms,
        started_at_ms: *started_at_ms,
        uptime_secs: *uptime_secs,
        verdict: verdict.clone(),
        conditions: conditions
            .iter()
            .take(MAX_PUBLIC_CONDITIONS)
            .map(public_condition)
            .collect(),
        unevaluated: unevaluated.clone(),
        startup: *startup,
        lifecycle: *lifecycle,
        ingest: ingest.clone(),
        shards: shards.iter().take(MAX_PUBLIC_SHARDS).cloned().collect(),
        drain: drain.as_ref().map(public_drain),
        sink: sink.as_ref().map(public_sink),
        backlog: backlog.clone(),
        reload: public_reload(reload),
        admin: public_admin_surface(admin),
        config: public_config.clone(),
    }
}
