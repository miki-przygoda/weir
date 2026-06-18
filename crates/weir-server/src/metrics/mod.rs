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
    empty_payload,
    unknown_message,
    reserved_flags_set,
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

/// Info-style label carrying the configured sink type. The active sink's series
/// is set to 1.0 so operators (and `weir-ctl metrics`) can tell whether the sink
/// is a real downstream or the discard-everything `noop`. A `String` rather than
/// an enum so a feature-gated sink (`clickhouse`) needs no `cfg` here.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkInfoLabel {
    pub sink_type: String,
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TlsHandshakeFailureLabel {
    pub reason: TlsHandshakeFailureReason,
}

/// TLS handshake failure reason. Lowercase snake_case to match Prometheus naming conventions.
///
/// Constructed by the TCP+mTLS accept loop (`socket::tcp`, feature = "tls"); on
/// the default Unix-only build the variants are never constructed, so the
/// dead-code lint is suppressed only there.
#[allow(non_camel_case_types)]
#[cfg_attr(not(feature = "tls"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TlsHandshakeFailureReason {
    no_client_cert,
    bad_cert,
    timeout,
    other,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TlsReloadLabel {
    pub outcome: TlsReloadOutcome,
}

/// TLS config reload outcome. Lowercase to match Prometheus naming conventions.
///
/// Constructed by the SIGHUP reload task (`spawn_tls_reload_task`, feature = "tls"); on
/// the default Unix-only build the variants are never constructed, so the
/// dead-code lint is suppressed only there.
#[allow(non_camel_case_types)]
#[cfg_attr(not(feature = "tls"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TlsReloadOutcome {
    ok,
    failed,
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
    pub accept_latency: Histogram,
    /// Counts connections dropped because the handler sat in `read_exact`
    /// longer than `connection_read_timeout_secs` without receiving the next
    /// byte. Elevated values suggest slowloris-style activity or a flaky
    /// client.
    pub connection_idle_timeout: Counter<u64, AtomicU64>,
    /// Counts connections refused by the accept loop's peer-credential check
    /// (SO_PEERCRED on Linux, getpeereid on macOS) — peer uid did not match
    /// the daemon's, or the credential lookup failed. Should be zero on a
    /// healthy deployment; non-zero suggests an attempted bypass of the
    /// socket file's mode bits or a misconfigured producer.
    pub connection_rejected_peer_uid: Counter<u64, AtomicU64>,
    /// Counts `accept(2)` failures due to resource exhaustion (EMFILE/ENFILE/
    /// ENOBUFS/ENOMEM). On these the accept loop backs off briefly before
    /// retrying, because the pending connection stays in the kernel queue and
    /// an immediate retry would busy-spin. A rising value means the daemon is
    /// out of file descriptors or socket buffers — raise the fd ulimit or
    /// lower `max_connections`.
    pub accept_resource_exhaustion: Counter<u64, AtomicU64>,
    /// Counts in-flight connections that were force-aborted because they
    /// didn't drain within `shutdown_timeout_secs`. Each aborted connection
    /// may correspond to a push whose record was written to the WAB but
    /// whose producer never received an ack/nack — at-least-once retry
    /// territory. Non-zero values mean shutdown_timeout_secs is too tight
    /// for the deployment's tail latency.
    pub connections_aborted_at_shutdown: Counter<u64, AtomicU64>,
    /// Counts pushes that were nacked because the WAB ack channel did not
    /// fire within `ACK_TIMEOUT`. Triggered by a wedged flusher (stuck on
    /// a slow fsync, lock contention, etc.) — not by a normal slow disk.
    /// Any non-zero value indicates a shard is unhealthy even if it hasn't
    /// panicked; check `weir_wab_fsync_duration_seconds` for tail latency
    /// and `weir_wab_flusher_panics` for hard failures.
    pub ack_timeout: Counter<u64, AtomicU64>,

    // ── TLS ───────────────────────────────────────────────────────────────────
    /// TLS handshakes rejected, by reason. Always compiled and registered;
    /// incremented by the TCP+mTLS accept loop (`socket::tcp`, feature = "tls").
    /// On the default (Unix-only) build the field is never read, so the
    /// dead-code lint is suppressed only there.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_handshake_failures: Family<TlsHandshakeFailureLabel, Counter<u64, AtomicU64>>,
    /// SIGHUP TLS config reloads, by outcome. Always compiled and registered;
    /// incremented by the SIGHUP reload task (`spawn_tls_reload_task`, feature = "tls").
    /// On the default (Unix-only) build the field is never read, so the
    /// dead-code lint is suppressed only there.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_config_reloads: Family<TlsReloadLabel, Counter<u64, AtomicU64>>,

    // ── WAB ───────────────────────────────────────────────────────────────────
    pub wab_segments: Family<SegmentStateLabel, Counter<u64, AtomicU64>>,
    pub wab_bytes_on_disk: Gauge<f64, AtomicU64>,
    pub wab_fsync_duration: Histogram,
    /// WAB flusher thread panics. Once a flusher panics, its shard is offline
    /// (records routed to it receive Nack(InternalError)) until the daemon
    /// restarts — any non-zero value requires operator attention. Check logs
    /// for the shard_id and panic payload.
    pub wab_flusher_panics: Counter<u64, AtomicU64>,
    /// weir-drain thread panics caught and respawned by its supervisor. Sustained
    /// values indicate a logic bug in the sink/drain path; if the supervisor
    /// exhausts its respawn budget, delivery stops and the WAB accumulates on disk
    /// until the daemon restarts.
    pub drain_panics: Counter<u64, AtomicU64>,
    /// fsync / fdatasync calls that returned an error. A non-zero value is a
    /// durability hazard: the kernel buffered the write but couldn't push it
    /// to stable storage. Producers whose records were in the failed fsync
    /// receive Nack(InternalError); on Linux a second fsync after EIO may
    /// succeed but the data is generally lost ("fsyncgate"). Check logs for
    /// the shard_id and error string.
    pub wab_fsync_failures: Counter<u64, AtomicU64>,

    // ── Sink / drain ──────────────────────────────────────────────────────────
    pub sink_commit_duration: Histogram,
    pub sink_commit_records: Family<OutcomeLabel, Counter<u64, AtomicU64>>,
    pub sink_health: Family<SinkHealthLabel, Gauge<f64, AtomicU64>>,
    /// Configured sink type, set to 1.0 for the active sink (see [`SinkInfoLabel`]).
    pub sink_info: Family<SinkInfoLabel, Gauge<f64, AtomicU64>>,
    pub queue_depth: Gauge<f64, AtomicU64>,

    // ── Recovery ──────────────────────────────────────────────────────────────
    pub recovery_records_replayed: Counter<u64, AtomicU64>,
    pub recovery_segments_quarantined: Counter<u64, AtomicU64>,
    /// Counts WAB segment files seen during recovery whose permissions are
    /// not 0o600. Defense-in-depth signal for tampering or operator error.
    pub wab_unexpected_mode: Counter<u64, AtomicU64>,

    // ── Per-stage latency (bench-trace only) ─────────────────────────────────
    /// queue + coalesce wait: enqueue → worker flush. bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_queue: Histogram,
    /// bridge hop + flusher recv wait: worker flush → flusher dequeue. bench-trace only.
    /// This is where the Batched double-deadline anomaly will show up.
    #[cfg(feature = "bench-trace")]
    pub stage_bridge_wait: Histogram,
    /// record write: flusher dequeue → write_record done (pre-fsync). bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_write: Histogram,
    /// end-to-end server-side: enqueue → ack fired. bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_total: Histogram,

    // ── Dead letter / drain state ─────────────────────────────────────────────
    pub dead_letter_bytes_on_disk: Gauge<f64, AtomicU64>,
    /// Increments once per entry into `BlockedDeadLetterFull`, not once per wake cycle.
    /// Semantics: "how many distinct blocking events over daemon lifetime."
    pub dead_letter_full: Counter<u64, AtomicU64>,
    /// Increments once each time the drain abandons a segment after exhausting
    /// `max_retries` transient sink failures. The segment is left on disk and is
    /// re-drained when the sink recovers (see `weir_drain_segments_resumed_total`)
    /// or on daemon restart, so a rising value means delivery has stalled for at
    /// least one segment — the silent counterpart to the `error!` log on strand.
    pub drain_segments_stranded: Counter<u64, AtomicU64>,
    /// Increments per stranded segment re-queued when the sink health recovers
    /// (down→up). Pairs with `drain_segments_stranded`: convergence of the two
    /// means the backlog from an outage has been picked back up for delivery.
    pub drain_segments_resumed: Counter<u64, AtomicU64>,
    /// Gauge vector: exactly one state label value is 1 at any time; the others are 0.
    pub drain_state: Family<DrainStateLabel, Gauge<f64, AtomicU64>>,
    /// Seconds elapsed since the drain entered `BlockedDeadLetterFull`. Resets to 0
    /// on transition out. Alert if this gauge exceeds an operator-defined threshold (e.g. 300 s).
    pub dead_letter_blocked_duration: Gauge<f64, AtomicU64>,
}

/// Latency histogram buckets covering 1 ms–10 s; suitable for fsync and network
/// round-trips. The 2.5/5/10 s tail keeps a multi-second p99.9 quantifiable
/// instead of saturating at the old 1 s top bucket — a failing disk or saturated
/// network storage can push fsync well past a second and the critical alert
/// needs the real value (escalation #12).
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Buckets for `accept_latency`, which measures only accept→semaphore→spawn —
/// microsecond-scale CPU work that would all land in LATENCY_BUCKETS' first
/// (1 ms) bucket, making the histogram useless (F22). Spans 10 µs–10 ms so the
/// distribution is actually visible; always compiled (unlike the bench-trace
/// STAGE_BUCKETS).
const ACCEPT_BUCKETS: &[f64] = &[
    0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001, 0.0025, 0.005, 0.01,
];

/// Finer buckets for the per-stage breakdown — queue/write stages are tens of
/// microseconds, far below LATENCY_BUCKETS' 1 ms floor. Only used under bench-trace.
#[cfg(feature = "bench-trace")]
const STAGE_BUCKETS: &[f64] = &[
    0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001, 0.0025, 0.005, 0.01,
    0.025, 0.05, 0.1,
];

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
        let accept_latency = reg!(
            Histogram::new(ACCEPT_BUCKETS.iter().copied()),
            "weir_accept_latency_seconds",
            "Wall-clock time from socket accept to spawn of the connection handler"
        );
        let connection_idle_timeout = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_connection_idle_timeout",
            "Connections dropped because a frame read phase (header, payload, or CRC) \
             exceeded connection_read_timeout_secs — whether the client was idle or \
             merely streaming pathologically slowly"
        );
        let connection_rejected_peer_uid = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_connection_rejected_peer_uid",
            "Connections refused by the peer-credential check (peer uid != daemon uid, \
             or credential lookup failed). Any non-zero value suggests an attempted \
             bypass of the socket file's 0o600 mode or a misconfigured producer"
        );
        let accept_resource_exhaustion = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_accept_resource_exhaustion",
            "accept(2) failures due to resource exhaustion (EMFILE/ENFILE/ENOBUFS/ENOMEM). \
             The accept loop backs off briefly on each. A rising value means the daemon is \
             out of file descriptors or socket buffers"
        );
        let connections_aborted_at_shutdown = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_connections_aborted_at_shutdown",
            "Connections force-aborted at shutdown because they didn't drain within \
             shutdown_timeout_secs. Each may correspond to a push whose record was \
             written to the WAB but whose producer never received an ack/nack. \
             Increase shutdown_timeout_secs if this is non-zero on graceful shutdown"
        );
        let ack_timeout = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_ack_timeout",
            "Pushes nacked because the WAB ack channel did not fire within \
             ACK_TIMEOUT. Indicates a wedged flusher (slow fsync, lock contention) \
             that hasn't panicked. Investigate alongside weir_wab_fsync_duration_seconds"
        );
        let tls_handshake_failures = reg!(
            Family::<TlsHandshakeFailureLabel, Counter<u64, AtomicU64>>::default(),
            "weir_tls_handshake_failures",
            "TLS handshakes rejected, by reason"
        );
        let tls_config_reloads = reg!(
            Family::<TlsReloadLabel, Counter<u64, AtomicU64>>::default(),
            "weir_tls_config_reloads",
            "SIGHUP TLS config reloads, by outcome"
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
        let wab_flusher_panics = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_flusher_panics",
            "WAB flusher thread panics. A panicked flusher leaves its shard offline \
             (records routed to it receive Nack(InternalError)) until the daemon \
             restarts. Any non-zero value requires operator attention; check logs \
             for the shard_id and panic payload"
        );
        let drain_panics = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_panics",
            "weir-drain thread panics caught and respawned by its supervisor. \
             Sustained values indicate a logic bug in the sink/drain path; if the \
             supervisor exhausts its respawn budget, delivery stops and the WAB \
             accumulates on disk until restart"
        );
        let wab_fsync_failures = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_fsync_failures",
            "fsync/fdatasync calls that returned an error. Durability hazard: \
             the kernel buffered the write but couldn't push it to stable storage. \
             Producers whose records were in the failed fsync receive \
             Nack(InternalError). Check logs for the shard_id and error string"
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
        let sink_info = reg!(
            Family::<SinkInfoLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_sink_info",
            "Configured sink type, set to 1 for the active sink. sink_type=\"noop\" means \
             records are acked then DISCARDED (not forwarded downstream)"
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
        let wab_unexpected_mode = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_unexpected_mode",
            "WAB segment files seen during recovery whose permissions are not 0o600. \
             Non-zero values indicate tampering or operator error"
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
             dead-lettered due to cap exhaustion. Operator intervention required"
        );
        let drain_segments_stranded = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_segments_stranded",
            "WAB segments abandoned by the drain after exhausting max_retries transient \
             sink failures. The segment is left on disk and is re-drained automatically when \
             the sink health recovers (see weir_drain_segments_resumed) or on daemon restart, \
             so a rising value means delivery has stalled for at least one segment. Investigate \
             the sink (see weir_sink_health). Distinct from weir_dead_letter_full, which counts \
             PERMANENT rejections; this counts TRANSIENT failures that never succeeded"
        );
        let drain_segments_resumed = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_segments_resumed",
            "Stranded WAB segments re-queued for delivery after the sink health recovered \
             (down to up). Convergence with weir_drain_segments_stranded means an outage's \
             backlog has been picked back up; a persistent gap means segments are still \
             stranded (sink not yet recovered, or recovering then re-failing)"
        );
        let drain_state = reg!(
            Family::<DrainStateLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_state",
            "Current drain state (1 = active, 0 = inactive). Exactly one state label value \
             is 1 at any time; the others are 0"
        );
        let dead_letter_blocked_duration = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_dead_letter_blocked_duration_seconds",
            "Seconds elapsed since the drain entered BlockedDeadLetterFull. Resets to 0 on \
             transition out. Fire an alert if this gauge exceeds your operator-defined \
             threshold (e.g. 300 s)"
        );

        #[cfg(feature = "bench-trace")]
        let stage_queue = reg!(
            Histogram::new(STAGE_BUCKETS.iter().copied()),
            "weir_stage_queue_seconds",
            "Per-record queue + coalesce wait (enqueue → worker flush). bench-trace only"
        );
        #[cfg(feature = "bench-trace")]
        let stage_bridge_wait = reg!(
            Histogram::new(STAGE_BUCKETS.iter().copied()),
            "weir_stage_bridge_wait_seconds",
            "Per-record bridge hop + flusher recv wait (worker flush → flusher dequeue). bench-trace only"
        );
        #[cfg(feature = "bench-trace")]
        let stage_write = reg!(
            Histogram::new(STAGE_BUCKETS.iter().copied()),
            "weir_stage_write_seconds",
            "Per-record write time (flusher dequeue → write_record done, pre-fsync). bench-trace only"
        );
        #[cfg(feature = "bench-trace")]
        let stage_total = reg!(
            Histogram::new(STAGE_BUCKETS.iter().copied()),
            "weir_stage_total_seconds",
            "Per-record end-to-end server-side latency (enqueue → ack fired). bench-trace only"
        );

        let metrics = Self {
            records_accepted,
            records_ack,
            records_nack,
            accept_latency,
            connection_idle_timeout,
            connection_rejected_peer_uid,
            accept_resource_exhaustion,
            connections_aborted_at_shutdown,
            ack_timeout,
            tls_handshake_failures,
            tls_config_reloads,
            wab_segments,
            wab_bytes_on_disk,
            wab_fsync_duration,
            wab_flusher_panics,
            drain_panics,
            wab_fsync_failures,
            sink_commit_duration,
            sink_commit_records,
            sink_health,
            sink_info,
            queue_depth,
            recovery_records_replayed,
            recovery_segments_quarantined,
            wab_unexpected_mode,
            dead_letter_bytes_on_disk,
            dead_letter_full,
            drain_segments_stranded,
            drain_segments_resumed,
            drain_state,
            dead_letter_blocked_duration,
            #[cfg(feature = "bench-trace")]
            stage_queue,
            #[cfg(feature = "bench-trace")]
            stage_bridge_wait,
            #[cfg(feature = "bench-trace")]
            stage_write,
            #[cfg(feature = "bench-trace")]
            stage_total,
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
            "weir_tls_handshake_failures",
            "weir_tls_config_reloads",
            "weir_wab_segments",
            "weir_wab_bytes_on_disk",
            "weir_wab_fsync_duration_seconds",
            "weir_sink_commit_duration_seconds",
            "weir_sink_commit_records",
            "weir_sink_health",
            "weir_sink_info",
            "weir_queue_depth",
            "weir_recovery_records_replayed",
            "weir_recovery_segments_quarantined",
            "weir_dead_letter_bytes_on_disk",
            "weir_dead_letter_full",
            "weir_drain_segments_stranded",
            "weir_drain_segments_resumed",
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
