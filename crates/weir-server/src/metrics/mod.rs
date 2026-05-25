//! Prometheus metrics for the weir daemon.
//!
//! Call `Metrics::new()` once at startup to get `(Arc<Metrics>, Registry)`.
//! Pass `Arc<Metrics>` to every subsystem that needs to update counters/gauges.
//! Pass `Registry` to `metrics::server::spawn` for the `/metrics` exposition endpoint.
//!
//! # Counter naming
//!
//! Counters are registered **without** the `_total` suffix; `prometheus-client`
//! appends it automatically in the text output. All names in this file match the
//! plan's metric names after that suffix is accounted for.

pub(crate) mod server;

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use std::sync::atomic::AtomicU64;

// ── Label types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TierLabel {
    pub tier: TierValue,
}

/// Durability tier label value. Lowercase to match Prometheus naming conventions.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TierValue {
    sync,
    batched,
    buffered,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct NackLabel {
    pub tier: TierValue,
    pub reason: NackReason,
}

/// Nack reason label value. Names match the wire-protocol `NackReason` variant names.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum NackReason {
    bad_magic,
    version_mismatch,
    bad_header_crc,
    payload_too_large,
    bad_payload_crc,
    internal_error,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SegmentStateLabel {
    pub state: SegmentState,
}

// `open` is defined for metric completeness; wiring it requires threading metrics
// into ShardWriter in segment.rs, which is deferred.
#[allow(non_camel_case_types, dead_code)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum SegmentState {
    open,
    sealed,
    confirmed,
    quarantined,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabel {
    pub outcome: Outcome,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum Outcome {
    committed,
    retried,
    dead_lettered,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkHealthLabel {
    pub state: SinkHealthState,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum SinkHealthState {
    healthy,
    degraded,
    down,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DrainStateLabel {
    pub state: DrainStateValue,
}

/// Drain state label value. `blocked_dead_letter_full` matches the state machine name.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum DrainStateValue {
    draining,
    retrying_transient,
    blocked_dead_letter_full,
}

// ── Metrics struct ────────────────────────────────────────────────────────────

/// All Prometheus metrics for the weir daemon.
///
/// Constructed once via [`Metrics::new`]; cloned fields use `Arc` internally so
/// cloning `Metrics` or any individual field is cheap and shares the same atomic storage.
pub(crate) struct Metrics {
    // ── Socket / wire layer ───────────────────────────────────────────────────
    pub records_accepted: Family<TierLabel, Counter<u64, AtomicU64>>,
    pub records_ack: Family<TierLabel, Counter<u64, AtomicU64>>,
    pub records_nack: Family<NackLabel, Counter<u64, AtomicU64>>,

    // ── WAB ───────────────────────────────────────────────────────────────────
    pub wab_segments: Family<SegmentStateLabel, Counter<u64, AtomicU64>>,
    pub wab_bytes_on_disk: Gauge<f64, AtomicU64>,
    pub wab_fsync_duration: Histogram,

    // ── Sink / drain ──────────────────────────────────────────────────────────
    pub sink_commit_duration: Histogram,
    pub sink_commit_records: Family<OutcomeLabel, Counter<u64, AtomicU64>>,
    pub sink_health: Family<SinkHealthLabel, Gauge<f64, AtomicU64>>,
    pub queue_depth: Gauge<f64, AtomicU64>,

    // ── Recovery ──────────────────────────────────────────────────────────────
    pub recovery_records_replayed: Counter<u64, AtomicU64>,
    pub recovery_segments_quarantined: Counter<u64, AtomicU64>,

    // ── Dead letter / drain state ─────────────────────────────────────────────
    pub dead_letter_bytes_on_disk: Gauge<f64, AtomicU64>,
    /// Increments once per entry into `BlockedDeadLetterFull`, not once per wake cycle.
    /// Semantics: "how many distinct blocking events over daemon lifetime."
    pub dead_letter_full: Counter<u64, AtomicU64>,
    /// Gauge vector: exactly one state label value is 1 at any time; the others are 0.
    pub drain_state: Family<DrainStateLabel, Gauge<f64, AtomicU64>>,
    /// Seconds elapsed since the drain entered `BlockedDeadLetterFull`. Resets to 0
    /// on transition out. Alert if this gauge exceeds an operator-defined threshold (e.g. 300 s).
    pub dead_letter_blocked_duration: Gauge<f64, AtomicU64>,
}

/// Latency histogram buckets covering 1 ms–1 s; suitable for fsync and network round-trips.
const LATENCY_BUCKETS: &[f64] = &[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

impl Metrics {
    /// Creates all metrics, registers them with a fresh [`Registry`], and returns both.
    ///
    /// Pass the returned `Registry` (wrapped in `Arc`) to [`server::spawn`]; keep
    /// `Arc<Metrics>` for subsystems that update the counters and gauges.
    pub(crate) fn new() -> (Self, Registry) {
        let mut reg = Registry::default();

        macro_rules! reg {
            ($metric:expr, $name:literal, $help:literal) => {{
                let m = $metric;
                reg.register($name, $help, m.clone());
                m
            }};
        }

        let records_accepted = reg!(
            Family::<TierLabel, Counter<u64, AtomicU64>>::default(),
            "weir_records_accepted",
            "Total records accepted from producers, by durability tier"
        );
        let records_ack = reg!(
            Family::<TierLabel, Counter<u64, AtomicU64>>::default(),
            "weir_records_ack",
            "Total records acknowledged to producers, by durability tier"
        );
        let records_nack = reg!(
            Family::<NackLabel, Counter<u64, AtomicU64>>::default(),
            "weir_records_nack",
            "Total records rejected (Nack), by durability tier and rejection reason"
        );
        let wab_segments = reg!(
            Family::<SegmentStateLabel, Counter<u64, AtomicU64>>::default(),
            "weir_wab_segments",
            "Cumulative WAB segment transitions by state (open, sealed, confirmed, quarantined)"
        );
        let wab_bytes_on_disk = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_bytes_on_disk",
            "Current total bytes used by WAB segment files on disk"
        );
        let wab_fsync_duration = reg!(
            Histogram::new(LATENCY_BUCKETS.iter().copied()),
            "weir_wab_fsync_duration_seconds",
            "Wall-clock time of WAB fdatasync calls"
        );
        let sink_commit_duration = reg!(
            Histogram::new(LATENCY_BUCKETS.iter().copied()),
            "weir_sink_commit_duration_seconds",
            "Wall-clock time of Sink::commit calls"
        );
        let sink_commit_records = reg!(
            Family::<OutcomeLabel, Counter<u64, AtomicU64>>::default(),
            "weir_sink_commit_records",
            "Records processed by the drain, by outcome (committed, retried, dead_lettered)"
        );
        let sink_health = reg!(
            Family::<SinkHealthLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_sink_health",
            "Current sink health state (1 = active, 0 = inactive for each state label)"
        );
        let queue_depth = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_queue_depth",
            "Current number of records waiting in the work queue"
        );
        let recovery_records_replayed = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_recovery_records_replayed",
            "Records replayed from WAB segments during crash recovery"
        );
        let recovery_segments_quarantined = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_recovery_segments_quarantined",
            "WAB segments quarantined due to corruption detected during crash recovery"
        );
        let dead_letter_bytes_on_disk = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_dead_letter_bytes_on_disk",
            "Current total bytes used by dead-letter segment files on disk"
        );
        let dead_letter_full = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_dead_letter_full",
            "Number of times the drain entered BlockedDeadLetterFull state. Each increment \
             represents a distinct episode where a permanently-rejected record could not be \
             dead-lettered due to cap exhaustion. Operator intervention required."
        );
        let drain_state = reg!(
            Family::<DrainStateLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_state",
            "Current drain state (1 = active, 0 = inactive). Exactly one state label value \
             is 1 at any time; the others are 0."
        );
        let dead_letter_blocked_duration = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_dead_letter_blocked_duration_seconds",
            "Seconds elapsed since the drain entered BlockedDeadLetterFull. Resets to 0 on \
             transition out. Fire an alert if this gauge exceeds your operator-defined \
             threshold (e.g. 300 s)."
        );

        let metrics = Self {
            records_accepted,
            records_ack,
            records_nack,
            wab_segments,
            wab_bytes_on_disk,
            wab_fsync_duration,
            sink_commit_duration,
            sink_commit_records,
            sink_health,
            queue_depth,
            recovery_records_replayed,
            recovery_segments_quarantined,
            dead_letter_bytes_on_disk,
            dead_letter_full,
            drain_state,
            dead_letter_blocked_duration,
        };

        // Pre-initialise gauge families so all label combinations appear on the first scrape
        // even before any state transition has occurred.
        metrics
            .drain_state
            .get_or_create(&DrainStateLabel {
                state: DrainStateValue::draining,
            })
            .set(1.0);
        metrics
            .drain_state
            .get_or_create(&DrainStateLabel {
                state: DrainStateValue::retrying_transient,
            })
            .set(0.0);
        metrics
            .drain_state
            .get_or_create(&DrainStateLabel {
                state: DrainStateValue::blocked_dead_letter_full,
            })
            .set(0.0);

        metrics
            .sink_health
            .get_or_create(&SinkHealthLabel {
                state: SinkHealthState::healthy,
            })
            .set(1.0);
        metrics
            .sink_health
            .get_or_create(&SinkHealthLabel {
                state: SinkHealthState::degraded,
            })
            .set(0.0);
        metrics
            .sink_health
            .get_or_create(&SinkHealthLabel {
                state: SinkHealthState::down,
            })
            .set(0.0);

        (metrics, reg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_client::encoding::text::encode;

    fn encode_to_string(registry: &Registry) -> String {
        let mut buf = String::new();
        encode(&mut buf, registry).expect("encode failed");
        buf
    }

    #[test]
    fn counters_initialised_to_zero() {
        let (m, _reg) = Metrics::new();
        assert_eq!(m.recovery_records_replayed.get(), 0);
        assert_eq!(m.recovery_segments_quarantined.get(), 0);
        assert_eq!(m.dead_letter_full.get(), 0);
    }

    #[test]
    fn incrementing_counter_reflected_in_output() {
        let (m, reg) = Metrics::new();
        m.recovery_records_replayed.inc_by(42);
        let out = encode_to_string(&reg);
        assert!(
            out.contains("weir_recovery_records_replayed_total 42"),
            "expected counter value in output; got:\n{out}"
        );
    }

    #[test]
    fn all_metric_names_present_in_output() {
        let (_m, reg) = Metrics::new();
        let out = encode_to_string(&reg);
        // All registered metrics appear in # HELP / # TYPE lines using the base name
        // (no _total suffix — prometheus-client appends _total only to counter data lines).
        // Family counters with no initialized label sets produce HELP/TYPE lines only,
        // so checking the base name is correct and sufficient.
        let expected = [
            "weir_records_accepted",
            "weir_records_ack",
            "weir_records_nack",
            "weir_wab_segments",
            "weir_wab_bytes_on_disk",
            "weir_wab_fsync_duration_seconds",
            "weir_sink_commit_duration_seconds",
            "weir_sink_commit_records",
            "weir_sink_health",
            "weir_queue_depth",
            "weir_recovery_records_replayed",
            "weir_recovery_segments_quarantined",
            "weir_dead_letter_bytes_on_disk",
            "weir_dead_letter_full",
            "weir_drain_state",
            "weir_dead_letter_blocked_duration_seconds",
        ];
        for name in expected {
            assert!(
                out.contains(name),
                "metric {name:?} missing from output:\n{out}"
            );
        }
    }

    #[test]
    fn drain_state_and_sink_health_pre_initialised() {
        let (_m, reg) = Metrics::new();
        let out = encode_to_string(&reg);
        assert!(
            out.contains("draining"),
            "drain_state draining label missing"
        );
        assert!(
            out.contains("retrying_transient"),
            "drain_state retrying_transient label missing"
        );
        assert!(
            out.contains("blocked_dead_letter_full"),
            "drain_state blocked label missing"
        );
        assert!(out.contains("healthy"), "sink_health healthy label missing");
    }
}
