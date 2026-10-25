//! The Conditions catalogue (spec §9.3): one entry per code, in catalogue order.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// How bad an active condition is. Ordered: `Info < Warning < Critical`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// What a condition endangers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Affects {
    Ingest,
    Delivery,
    Durability,
}

/// Which probe a condition can fail: `Readiness` always forces `/readyz` to
/// 503; `Liveness` marks a check selectable in `livez_checks` (spec §9.3 R, L*).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Gate {
    Readiness,
    Liveness,
}

macro_rules! condition_codes {
    ($( $variant:ident => $name:literal ),* $(,)?) => {
        /// A condition code. `Unknown` is what an older reader makes of a code a
        /// newer daemon emits; it is shown verbatim (spec §5.1), never dropped.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum ConditionCode { $( $variant, )* Unknown }

        impl ConditionCode {
            /// The wire name (snake_case).
            pub fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $name, )* Self::Unknown => "unknown" }
            }

            /// Parses a wire name; anything unrecognised is `Unknown`.
            pub fn parse_lossy(s: &str) -> Self {
                match s { $( $name => Self::$variant, )* _ => Self::Unknown }
            }
        }
    };
}

condition_codes! {
    Starting => "starting",
    ShuttingDown => "shutting_down",
    ListenerDown => "listener_down",
    NoLiveShard => "no_live_shard",
    ShardOffline => "shard_offline",
    WorkerExited => "worker_exited",
    WabCapRejecting => "wab_cap_rejecting",
    DrainBackpressure => "drain_backpressure",
    AckTimeoutsRecent => "ack_timeouts_recent",
    WabScanStale => "wab_scan_stale",
    WabGrowingUnbounded => "wab_growing_unbounded",
    DrainStopped => "drain_stopped",
    DrainWedged => "drain_wedged",
    DrainPaused => "drain_paused",
    DrainBlockedDeadLetterFull => "drain_blocked_dead_letter_full",
    DeliveryStalled => "delivery_stalled",
    DeliveryLagging => "delivery_lagging",
    SegmentsStranded => "segments_stranded",
    SinkDown => "sink_down",
    SinkDegraded => "sink_degraded",
    SinkFailing => "sink_failing",
    DeadLetterNearCap => "dead_letter_near_cap",
    NoopSink => "noop_sink",
    ActiveSegmentAging => "active_segment_aging",
    FsyncFailuresRecent => "fsync_failures_recent",
    FlusherRespawnedRecent => "flusher_respawned_recent",
    QuarantineNonempty => "quarantine_nonempty",
    RecoveryIncomplete => "recovery_incomplete",
    TlsReloadFailed => "tls_reload_failed",
    AdminUnavailable => "admin_unavailable",
    DaemonUnreachable => "daemon_unreachable",
    RecoveryPending => "recovery_pending",
}

impl Serialize for ConditionCode {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ConditionCode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self::parse_lossy(&s))
    }
}

/// One catalogue row (spec §9.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogueEntry {
    pub code: ConditionCode,
    pub severity: Severity,
    pub affects: Affects,
    pub gates: &'static [Gate],
    pub runbook: &'static str,
}

const NONE: &[Gate] = &[];
const R: &[Gate] = &[Gate::Readiness];
const L: &[Gate] = &[Gate::Liveness];
const RL: &[Gate] = &[Gate::Readiness, Gate::Liveness];

macro_rules! entry {
    ($code:ident, $sev:ident, $aff:ident, $gates:expr, $anchor:literal) => {
        CatalogueEntry {
            code: ConditionCode::$code,
            severity: Severity::$sev,
            affects: Affects::$aff,
            gates: $gates,
            runbook: concat!("docs/monitoring.md#cond-", $anchor),
        }
    };
}

/// The 30 codes the daemon evaluates, in spec §9.3 order. Thresholds are
/// constants (owner decision), see `Thresholds`.
#[rustfmt::skip] // one row per line, so the table reads like spec §9.3
pub const CATALOGUE: &[CatalogueEntry] = &[
    entry!(Starting, Info, Ingest, R, "starting"),
    entry!(ShuttingDown, Info, Ingest, R, "shutting_down"),
    entry!(ListenerDown, Critical, Ingest, RL, "listener_down"),
    entry!(NoLiveShard, Critical, Ingest, RL, "no_live_shard"),
    entry!(ShardOffline, Critical, Ingest, NONE, "shard_offline"),
    entry!(WorkerExited, Critical, Ingest, L, "worker_exited"),
    entry!(WabCapRejecting, Critical, Ingest, R, "wab_cap_rejecting"),
    entry!(DrainBackpressure, Warning, Ingest, NONE, "drain_backpressure"),
    entry!(AckTimeoutsRecent, Warning, Ingest, NONE, "ack_timeouts_recent"),
    entry!(WabScanStale, Warning, Ingest, NONE, "wab_scan_stale"),
    entry!(WabGrowingUnbounded, Warning, Durability, NONE, "wab_growing_unbounded"),
    entry!(DrainStopped, Critical, Delivery, L, "drain_stopped"),
    entry!(DrainWedged, Critical, Delivery, L, "drain_wedged"),
    entry!(DrainPaused, Warning, Delivery, NONE, "drain_paused"),
    entry!(DrainBlockedDeadLetterFull, Critical, Delivery, NONE, "drain_blocked_dead_letter_full"),
    entry!(DeliveryStalled, Warning, Delivery, NONE, "delivery_stalled"),
    entry!(DeliveryLagging, Warning, Delivery, NONE, "delivery_lagging"),
    entry!(SegmentsStranded, Warning, Delivery, NONE, "segments_stranded"),
    entry!(SinkDown, Warning, Delivery, NONE, "sink_down"),
    entry!(SinkDegraded, Info, Delivery, NONE, "sink_degraded"),
    entry!(SinkFailing, Warning, Delivery, NONE, "sink_failing"),
    entry!(DeadLetterNearCap, Warning, Delivery, NONE, "dead_letter_near_cap"),
    entry!(NoopSink, Warning, Delivery, NONE, "noop_sink"),
    entry!(ActiveSegmentAging, Info, Delivery, NONE, "active_segment_aging"),
    entry!(FsyncFailuresRecent, Critical, Durability, NONE, "fsync_failures_recent"),
    entry!(FlusherRespawnedRecent, Warning, Ingest, NONE, "flusher_respawned_recent"),
    entry!(QuarantineNonempty, Warning, Durability, NONE, "quarantine_nonempty"),
    entry!(RecoveryIncomplete, Critical, Durability, NONE, "recovery_incomplete"),
    entry!(TlsReloadFailed, Warning, Ingest, NONE, "tls_reload_failed"),
    entry!(AdminUnavailable, Warning, Ingest, NONE, "admin_unavailable"),
];

/// Emitted only by `weir-ctl` when it evaluates offline (spec §9.3).
/// `recovery_pending` has no `affects` in §9.3; `durability` is chosen
/// because it reports unsealed segments that recovery will seal.
#[rustfmt::skip]
pub const OFFLINE_ONLY: &[CatalogueEntry] = &[
    entry!(DaemonUnreachable, Critical, Ingest, NONE, "daemon_unreachable"),
    entry!(RecoveryPending, Info, Durability, NONE, "recovery_pending"),
];

/// Every code the daemon evaluates, in catalogue order (spec §5.1 `ALL`).
pub const ALL: &[ConditionCode] = &[
    ConditionCode::Starting,
    ConditionCode::ShuttingDown,
    ConditionCode::ListenerDown,
    ConditionCode::NoLiveShard,
    ConditionCode::ShardOffline,
    ConditionCode::WorkerExited,
    ConditionCode::WabCapRejecting,
    ConditionCode::DrainBackpressure,
    ConditionCode::AckTimeoutsRecent,
    ConditionCode::WabScanStale,
    ConditionCode::WabGrowingUnbounded,
    ConditionCode::DrainStopped,
    ConditionCode::DrainWedged,
    ConditionCode::DrainPaused,
    ConditionCode::DrainBlockedDeadLetterFull,
    ConditionCode::DeliveryStalled,
    ConditionCode::DeliveryLagging,
    ConditionCode::SegmentsStranded,
    ConditionCode::SinkDown,
    ConditionCode::SinkDegraded,
    ConditionCode::SinkFailing,
    ConditionCode::DeadLetterNearCap,
    ConditionCode::NoopSink,
    ConditionCode::ActiveSegmentAging,
    ConditionCode::FsyncFailuresRecent,
    ConditionCode::FlusherRespawnedRecent,
    ConditionCode::QuarantineNonempty,
    ConditionCode::RecoveryIncomplete,
    ConditionCode::TlsReloadFailed,
    ConditionCode::AdminUnavailable,
];

/// The catalogue row for `code` (daemon or offline-only); `None` for `Unknown`.
pub fn entry(code: ConditionCode) -> Option<&'static CatalogueEntry> {
    CATALOGUE
        .iter()
        .chain(OFFLINE_ONLY)
        .find(|e| e.code == code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_has_the_thirty_daemon_codes_in_spec_order() {
        assert_eq!(CATALOGUE.len(), 30);
        assert_eq!(ALL.len(), 30);
        for (e, c) in CATALOGUE.iter().zip(ALL) {
            assert_eq!(e.code, *c, "ALL and CATALOGUE disagree at {c:?}");
        }
        assert_eq!(ALL[0], ConditionCode::Starting);
        assert_eq!(ALL[29], ConditionCode::AdminUnavailable);
    }

    #[test]
    fn every_code_round_trips_through_its_wire_name() {
        for c in ALL.iter().chain(OFFLINE_ONLY.iter().map(|e| &e.code)) {
            assert_eq!(ConditionCode::parse_lossy(c.as_str()), *c);
            let json = serde_json::to_string(c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_str()));
            assert_eq!(serde_json::from_str::<ConditionCode>(&json).unwrap(), *c);
        }
    }

    #[test]
    fn a_code_from_a_newer_daemon_parses_as_unknown() {
        let c: ConditionCode = serde_json::from_str("\"config_pending_restart\"").unwrap();
        assert_eq!(c, ConditionCode::Unknown);
        assert_eq!(
            ConditionCode::parse_lossy("tls_cert_expiring"),
            ConditionCode::Unknown
        );
    }

    #[test]
    fn runbook_anchor_is_derived_from_the_code() {
        for e in CATALOGUE.iter().chain(OFFLINE_ONLY) {
            assert_eq!(
                e.runbook,
                format!("docs/monitoring.md#cond-{}", e.code.as_str())
            );
        }
    }

    #[test]
    fn gates_match_spec_9_3() {
        use ConditionCode as C;
        let gated = |g: Gate| -> Vec<ConditionCode> {
            CATALOGUE
                .iter()
                .filter(|e| e.gates.contains(&g))
                .map(|e| e.code)
                .collect()
        };
        assert_eq!(
            gated(Gate::Readiness),
            vec![
                C::Starting,
                C::ShuttingDown,
                C::ListenerDown,
                C::NoLiveShard,
                C::WabCapRejecting
            ]
        );
        assert_eq!(
            gated(Gate::Liveness),
            vec![
                C::ListenerDown,
                C::NoLiveShard,
                C::WorkerExited,
                C::DrainStopped,
                C::DrainWedged
            ]
        );
    }

    #[test]
    fn severities_and_affects_match_spec_9_3() {
        use ConditionCode as C;
        let critical: Vec<ConditionCode> = CATALOGUE
            .iter()
            .filter(|e| e.severity == Severity::Critical)
            .map(|e| e.code)
            .collect();
        assert_eq!(
            critical,
            vec![
                C::ListenerDown,
                C::NoLiveShard,
                C::ShardOffline,
                C::WorkerExited,
                C::WabCapRejecting,
                C::DrainStopped,
                C::DrainWedged,
                C::DrainBlockedDeadLetterFull,
                C::FsyncFailuresRecent,
                C::RecoveryIncomplete,
            ]
        );
        let info: Vec<ConditionCode> = CATALOGUE
            .iter()
            .filter(|e| e.severity == Severity::Info)
            .map(|e| e.code)
            .collect();
        assert_eq!(
            info,
            vec![
                C::Starting,
                C::ShuttingDown,
                C::SinkDegraded,
                C::ActiveSegmentAging
            ]
        );
        assert_eq!(
            entry(C::WabGrowingUnbounded).unwrap().affects,
            Affects::Durability
        );
        assert_eq!(
            entry(C::FlusherRespawnedRecent).unwrap().affects,
            Affects::Ingest
        );
        assert_eq!(
            entry(C::DaemonUnreachable).unwrap().severity,
            Severity::Critical
        );
        assert!(entry(C::Unknown).is_none());
    }
}
