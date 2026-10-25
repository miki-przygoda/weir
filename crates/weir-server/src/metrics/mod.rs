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

pub(crate) mod router;
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

/// Durability tier label value. Lowercase to match Prometheus naming conventions,
/// and kept in lockstep with `weir_core::Durability`'s two variants — this is a
/// separate type only because `EncodeLabelValue` needs a concrete enum, but its
/// variant names must always equal `Durability`'s `Display` output.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TierValue {
    durable,
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
    bad_batch_framing,
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
///
/// `stopped` is terminal and is not one of the state machine's states: it is set
/// by the drain *supervisor* once it has exited for good — a clean shutdown, the
/// respawn budget exhausted, or the dead-letter writer failing to open at start.
/// All three converge on the same exit. The three running states are written only by the
/// drain thread, so without a terminal value the family would stay frozen at its
/// last reading — reporting `draining` while nothing drains.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum DrainStateValue {
    draining,
    retrying_transient,
    blocked_dead_letter_full,
    stopped,
}

// ── Drain ledger labels (safety floor, spec §4.0) ─────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct LocalIoStrandLabel {
    pub cause: LocalIoStrandCause,
}

/// Which local I/O stranded a segment: reading the sealed segment, or the
/// dead-letter write the drain needs before it may advance.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum LocalIoStrandCause {
    read,
    write,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ParkedLabel {
    pub reason: ParkedReason,
}

/// Why the drain set a segment aside for the rest of the run.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ParkedReason {
    local_read_failed,
    unsupported_version,
    drain_panic,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DispositionLabel {
    pub kind: DispositionKind,
}

/// A step still owed for a segment the drain has finished with: its confirm
/// (sidecar write or delete), or its quarantine move.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum DispositionKind {
    confirm,
    quarantine,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConfirmFailureLabel {
    pub stage: ConfirmFailureStage,
}

/// Which step of a delivered segment's confirm failed: the atomic `.confirmed`
/// sidecar write, or the delete of the segment after it.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ConfirmFailureStage {
    sidecar,
    delete,
}

// ── Drain liveness label (safety floor, spec §4.8) ───────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DrainBusyLabel {
    pub kind: DrainBusyKind,
}

/// What the drain thread is busy doing: a sink call, or local I/O. Mirrors the
/// drain's `BusyKind`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum DrainBusyKind {
    sink_commit,
    sink_probe,
    segment_read,
    local_fsync,
    dead_letter_write,
    quarantine_move,
    dir_scan,
}

impl DrainBusyKind {
    /// Every label value, so the family is complete from boot and a publish
    /// can zero the kinds that are not current.
    pub const ALL: [DrainBusyKind; 7] = [
        DrainBusyKind::sink_commit,
        DrainBusyKind::sink_probe,
        DrainBusyKind::segment_read,
        DrainBusyKind::local_fsync,
        DrainBusyKind::dead_letter_write,
        DrainBusyKind::quarantine_move,
        DrainBusyKind::dir_scan,
    ];
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
/// Constructed by `reload::Reloader` when TLS is registered (feature = "tls"); on
/// the default Unix-only build the variants are never constructed, so the
/// dead-code lint is suppressed only there.
#[allow(non_camel_case_types)]
#[cfg_attr(not(feature = "tls"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TlsReloadOutcome {
    ok,
    failed,
}

/// Labels for `weir_config_reloads_total` (safety floor, spec §3.2): what asked
/// for the reload and how it went.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConfigReloadLabel {
    pub trigger: ConfigReloadTrigger,
    pub outcome: ConfigReloadOutcome,
}

/// What asked for a config reload. Lowercase to match Prometheus naming
/// conventions.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ConfigReloadTrigger {
    /// SIGHUP.
    sighup,
    /// The admin socket's `reload` verb, which goes through the same
    /// `reload::Reloader`.
    admin,
}

/// How a config reload went. `dropped` is a request the bounded reload queue
/// (8 deep) had no room for, as in a SIGHUP flood: it never ran, and it is
/// counted so the flood is visible.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ConfigReloadOutcome {
    ok,
    failed,
    noop,
    dropped,
}

/// Labels for `weir_http_requests_dropped_total` (Foundations, spec §5.8,
/// §13): why the metrics/health listener closed a connection without a
/// response.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpDroppedLabel {
    pub reason: HttpDropReason,
}

/// Why a metrics/health listener connection was closed before a response.
/// Lowercase to match Prometheus naming conventions.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum HttpDropReason {
    /// No complete request head within 1 s (`HEAD_READ_TIMEOUT`): a stalled
    /// or slowloris peer.
    head_timeout,
    /// All 64 head-reading slots (`HEAD_READ_SLOTS`) were busy.
    head_pool_full,
    /// More concurrent scrapes than `metrics_max_connections`.
    metrics_pool_full,
    /// More than 4 (`PROBE_SLOTS`) concurrent `/livez`, `/readyz`,
    /// `/startupz` or `/status` requests.
    probe_pool_full,
}

/// Labels for `weir_admin_requests_total` (Foundations spec §11).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct AdminRequestLabel {
    pub verb: AdminVerbLabel,
    pub outcome: AdminOutcomeLabel,
}

/// The admin verb, spelled exactly as on the wire, plus `_connect` (a
/// connection refused before any request) and `_invalid` (a line that is
/// not a valid request). Hand-encoded because the wire spellings contain
/// dots, which a derived `EncodeLabelValue` cannot produce.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum AdminVerbLabel {
    Status,
    Reload,
    DrainPause,
    DrainResume,
    DrainRetryStranded,
    Connect,
    Invalid,
}

impl AdminVerbLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            AdminVerbLabel::Status => "status",
            AdminVerbLabel::Reload => "reload",
            AdminVerbLabel::DrainPause => "drain.pause",
            AdminVerbLabel::DrainResume => "drain.resume",
            AdminVerbLabel::DrainRetryStranded => "drain.retry_stranded",
            AdminVerbLabel::Connect => "_connect",
            AdminVerbLabel::Invalid => "_invalid",
        }
    }
}

impl EncodeLabelValue for AdminVerbLabel {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelValueEncoder,
    ) -> Result<(), std::fmt::Error> {
        use std::fmt::Write;
        encoder.write_str(self.as_str())
    }
}

/// `ok` (the verb answered `ok:true`), `error` (an error response; the exact
/// error code is in the daemon log), `refused` (`_connect`: the peer is
/// neither the daemon's uid nor root, or has no credentials), `busy`
/// (`_connect`: the 4-connection cap).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum AdminOutcomeLabel {
    ok,
    error,
    refused,
    busy,
}

// ── Foundations: status, lifecycle and conditions (spec §13) ────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BuildInfoLabel {
    pub version: String,
    pub wire_version: String,
}

/// `weir_startup_phase{phase}`: the lifecycle phase, one-hot.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum StartupPhaseValue {
    initializing,
    recovering,
    handoff,
    binding,
    serving,
    shutting_down,
}

impl StartupPhaseValue {
    pub const ALL: [StartupPhaseValue; 6] = [
        Self::initializing,
        Self::recovering,
        Self::handoff,
        Self::binding,
        Self::serving,
        Self::shutting_down,
    ];

    pub fn of(p: weir_status::Phase) -> Self {
        use weir_status::Phase;
        match p {
            Phase::Initializing => Self::initializing,
            Phase::Recovering => Self::recovering,
            Phase::Handoff => Self::handoff,
            Phase::Binding => Self::binding,
            Phase::Serving => Self::serving,
            Phase::ShuttingDown => Self::shutting_down,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StartupPhaseLabel {
    pub phase: StartupPhaseValue,
}

/// `weir_condition_active{code,severity}`: one pre-created series per
/// catalogue code. Strings because the code set lives in weir-status.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConditionLabel {
    pub code: String,
    pub severity: String,
}

impl ConditionLabel {
    pub fn of(e: &weir_status::CatalogueEntry) -> Self {
        let severity = match e.severity {
            weir_status::Severity::Info => "info",
            weir_status::Severity::Warning => "warning",
            weir_status::Severity::Critical => "critical",
        };
        ConditionLabel {
            code: e.code.as_str().to_string(),
            severity: severity.to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShardLabel {
    pub shard: String,
}

impl ShardLabel {
    pub fn of(id: usize) -> Self {
        ShardLabel {
            shard: id.to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WorkerLabel {
    pub worker: String,
}

impl WorkerLabel {
    pub fn of(id: usize) -> Self {
        WorkerLabel {
            worker: id.to_string(),
        }
    }
}

/// `weir_drain_pending_segments{queue}`: the live channel, or the backlog
/// (startup replay and rescans).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum DrainQueue {
    live,
    backlog,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DrainQueueLabel {
    pub queue: DrainQueue,
}

/// `weir_sink_last_failure_timestamp_seconds{kind}`: how a sink commit failed.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum SinkFailureValue {
    transient,
    permanent,
    timeout,
    contract_violation,
}

impl SinkFailureValue {
    pub const ALL: [SinkFailureValue; 4] = [
        Self::transient,
        Self::permanent,
        Self::timeout,
        Self::contract_violation,
    ];

    #[cfg_attr(not(test), allow(dead_code))] // PR 2 (the drain) writes the family
    pub fn of(k: weir_status::SinkFailureKind) -> Self {
        use weir_status::SinkFailureKind as K;
        match k {
            K::Transient => Self::transient,
            K::Permanent => Self::permanent,
            K::Timeout => Self::timeout,
            K::ContractViolation => Self::contract_violation,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkFailureLabel {
    pub kind: SinkFailureValue,
}

/// Why a transient `InternalError` nack went out (spec §5.13). The wire
/// reason is the same for all of them; only this label tells them apart.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum NackInternalCause {
    queue_full,
    write_failed,
    sender_dropped,
    ack_timeout,
    wab_cap,
    coordinate,
    ownership_lost,
}

impl NackInternalCause {
    pub const ALL: [NackInternalCause; 7] = [
        Self::queue_full,
        Self::write_failed,
        Self::sender_dropped,
        Self::ack_timeout,
        Self::wab_cap,
        Self::coordinate,
        Self::ownership_lost,
    ];
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct NackInternalLabel {
    pub cause: NackInternalCause,
}

/// Why a flusher could not hand a sealed segment to the drain (binding
/// decision Q7): the shutdown hand-off budget ran out, or the drain was gone.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum NotHandedOffReason {
    budget,
    disconnected,
}

impl NotHandedOffReason {
    pub const ALL: [NotHandedOffReason; 2] = [Self::budget, Self::disconnected];
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct NotHandedOffLabel {
    pub reason: NotHandedOffReason,
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
    /// incremented by `reload::Reloader` when TLS is registered (feature = "tls").
    /// On the default (Unix-only) build the field is never read, so the
    /// dead-code lint is suppressed only there.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub tls_config_reloads: Family<TlsReloadLabel, Counter<u64, AtomicU64>>,

    // ── WAB ───────────────────────────────────────────────────────────────────
    pub wab_segments: Family<SegmentStateLabel, Counter<u64, AtomicU64>>,
    pub wab_bytes_on_disk: Gauge<f64, AtomicU64>,
    /// Bytes currently held in the quarantine directory.
    pub quarantine_bytes_on_disk: Gauge<f64, AtomicU64>,
    pub wab_fsync_duration: Histogram,
    /// Records covered by ONE WAB group fsync — the fsync amortisation factor,
    /// measured rather than modelled.
    ///
    /// 1.0 means every record paid for its own fsync, which is the worst case
    /// and the one a single serial producer sees today. Nothing observed this
    /// before: the case for wire-level batching rests on this number being low
    /// in real deployments, and until now that was an assumption no deployment
    /// could check. An operator seeing values near `batch_size` is already
    /// getting the amortisation batching would buy.
    pub wab_group_commit_records: Histogram,
    /// Records carried by one `PushBatch`, as accepted at ingest.
    pub batch_records: Histogram,
    /// `PushBatch` frames answered with at least one clear bit.
    pub batch_partial: Counter<u64, AtomicU64>,
    /// WAB flusher thread panics. Once a flusher panics, its shard is offline
    /// (records routed to it receive Nack(InternalError)) until the daemon
    /// restarts — any non-zero value requires operator attention. Check logs
    /// for the shard_id and panic payload.
    pub wab_flusher_panics: Counter<u64, AtomicU64>,
    /// weir-drain thread panics caught and respawned by its supervisor. Sustained
    /// values indicate a logic bug in the sink/drain path; if the supervisor
    /// exhausts its respawn budget, delivery stops and the WAB accumulates on disk
    /// until the daemon restarts. A segment in flight for two panics is parked
    /// (`weir_drain_segments_parked{reason="drain_panic"}`) instead of spending
    /// the rest of the budget.
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
    /// Sealed segments waiting in the flusher → drain channel. Last-writer-wins
    /// `len()` snapshots from the flushers (after each hand-off) and the drain
    /// (each wait slice), published by the 500 ms poll task.
    pub drain_channel_depth: Gauge<f64, AtomicU64>,
    /// Capacity of that channel; constant `wab::DRAIN_CHANNEL_CAPACITY`.
    pub drain_channel_capacity: Gauge<f64, AtomicU64>,

    // ── Recovery ──────────────────────────────────────────────────────────────
    pub recovery_records_replayed: Counter<u64, AtomicU64>,
    pub recovery_segments_quarantined: Counter<u64, AtomicU64>,
    /// Segments whose crash recovery failed and were left for manual inspection.
    pub recovery_segments_failed: Counter<u64, AtomicU64>,
    /// Counts mid-file-corrupt WAB segments whose quarantine COPY failed during
    /// recovery (disk full / read-only mount / inode exhaustion). On this event
    /// recovery refuses to truncate the valid prefix — it leaves the segment in
    /// place to preserve any acked-durable records sitting after the corruption,
    /// rather than fail open and lose them. Alertable so the stuck-on-full-disk
    /// state is visible instead of silent.
    pub recovery_quarantine_copy_failed: Counter<u64, AtomicU64>,
    /// Pushes rejected because live WAB bytes exceeded `wab_max_bytes`.
    pub wab_cap_rejections: Counter<u64, AtomicU64>,
    /// Cumulative record payload bytes as accepted from producers, before any
    /// compression. Paired with [`Metrics::wab_record_stored_bytes`].
    pub wab_record_logical_bytes: Counter<u64, AtomicU64>,
    /// Cumulative record payload bytes as written to disk, after compression.
    pub wab_record_stored_bytes: Counter<u64, AtomicU64>,
    /// Counts WAB segment files seen during recovery whose permissions are
    /// not 0o600. Defense-in-depth signal for tampering or operator error.
    pub wab_unexpected_mode: Counter<u64, AtomicU64>,

    // ── Per-stage latency (bench-trace only) ─────────────────────────────────
    /// queue + coalesce wait: enqueue → worker flush. bench-trace only.
    #[cfg(feature = "bench-trace")]
    pub stage_queue: Histogram,
    /// bridge hop + flusher recv wait: worker flush → flusher dequeue. bench-trace only.
    /// This is where the (historical, disproven — see
    /// docs/benchmarks/phase3-results.md) Batched double-deadline anomaly
    /// hypothesis would have shown up.
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
    ///
    /// May transiently OVER-COUNT at a recovery-edge rescan: a segment still
    /// sitting in the drain channel can be re-queued and counted again, so
    /// convergence with `stranded` is approximate / eventually-consistent rather
    /// than exact. Deduplicating the COUNT is impractical — crossbeam channels
    /// are not enumerable, so the resume path cannot cheaply check whether a
    /// segment is already in flight. Double PROCESSING is filtered: both copies
    /// pass the drain ledger's admission check before they are processed.
    pub drain_segments_resumed: Counter<u64, AtomicU64>,
    /// Gauge vector: exactly one state label value is 1 at any time; the others are 0.
    pub drain_state: Family<DrainStateLabel, Gauge<f64, AtomicU64>>,
    /// Seconds elapsed since the drain entered `BlockedDeadLetterFull`. Resets to 0
    /// on transition out. Alert if this gauge exceeds an operator-defined threshold (e.g. 300 s).
    pub dead_letter_blocked_duration: Gauge<f64, AtomicU64>,

    // ── wab_dir ownership (safety floor, spec §3.1) ───────────────────────────
    /// 1 while this daemon holds the exclusive lock on `<wab_dir>/.weir.lock`.
    /// 0 under `wab_dir_lock = "best-effort"` on a filesystem that cannot lock
    /// (no guard against a second daemon), or when ownership was lost just
    /// before exit.
    pub wab_dir_lock_held: Gauge<f64, AtomicU64>,
    /// `weir_wab_dir_lock_integrity_failures_total`: integrity checks of the
    /// `wab_dir` lock that found it deleted or replaced (and re-locked it), or
    /// failed with an errno. Proven loss to another instance exits instead.
    pub wab_dir_lock_integrity_failures: Counter<u64, AtomicU64>,

    // ── Config reload (safety floor, spec §3.2) ───────────────────────────────
    /// `weir_config_reloads_total{trigger,outcome}`: every reload request, by
    /// what asked for it and how it went. `weir_tls_config_reloads_total` is
    /// unchanged and still counts the TLS part on its own.
    pub config_reloads: Family<ConfigReloadLabel, Counter<u64, AtomicU64>>,

    // ── Metrics / health listener (Foundations, spec §5.8) ───────────────────
    /// `weir_http_requests_dropped_total{reason}`: connections to the
    /// metrics/health listener closed without a response, by reason. All four
    /// reasons are pre-created at 0; the router's pools increment them.
    pub http_requests_dropped: Family<HttpDroppedLabel, Counter<u64, AtomicU64>>,

    // ── Admin socket (Foundations spec §11) ───────────────────────────────────
    /// `weir_admin_requests_total{verb,outcome}`.
    pub admin_requests: Family<AdminRequestLabel, Counter<u64, AtomicU64>>,
    /// 1 while the admin socket is bound and served, else 0.
    pub admin_socket_up: Gauge<f64, AtomicU64>,

    // ── Drain ledger (safety floor, spec §4.0) ────────────────────────────────
    /// Segments set aside after exhausting `max_retries` on a LOCAL I/O failure,
    /// by cause. Deliberately not `drain_segments_stranded`, which means "the
    /// sink is failing"; a local strand neither counts there nor marks the sink
    /// down.
    pub drain_local_io_strands: Family<LocalIoStrandLabel, Counter<u64, AtomicU64>>,
    /// Segments parked (not re-queued this run), by reason.
    pub drain_segments_parked: Family<ParkedLabel, Gauge<f64, AtomicU64>>,
    /// Finished segments with a confirm or quarantine step still outstanding.
    pub drain_segments_disposition_pending: Family<DispositionLabel, Gauge<f64, AtomicU64>>,
    /// Failed confirm steps for delivered segments, by stage. Each failure is
    /// retried on the next health poll without re-delivering the segment.
    pub drain_confirm_failures: Family<ConfirmFailureLabel, Counter<u64, AtomicU64>>,
    /// Segments whose move into `quarantine/` failed when the drain first tried
    /// it. Counted once per segment: the move is then retried on every health
    /// poll (the `quarantine` kind of `drain_segments_disposition_pending`)
    /// without re-delivering the segment's prefix.
    pub drain_quarantine_failures: Counter<u64, AtomicU64>,
    /// Failed writes of sink-rejected records into `dead_letter/` (spec §4.11).
    /// The sink commit already happened, so the retry redoes only the write.
    pub drain_dead_letter_write_failures: Counter<u64, AtomicU64>,

    // ── Drain liveness (safety floor, spec §4.8) ──────────────────────────────
    /// Seconds the drain thread has been inside its current sink call or local
    /// I/O operation, by kind; only the current kind is non-zero, and all are
    /// zero while the drain is between operations. Set by the 5 s poll task
    /// from the drain heartbeat's busy atomics.
    pub drain_busy_seconds: Family<DrainBusyLabel, Gauge<f64, AtomicU64>>,

    // ── Foundations: status, lifecycle and conditions (spec §13) ────────────
    // A field whose writer lands in a later task or PR carries a dead-code
    // allowance naming it; every family is registered and pre-created from
    // boot regardless.
    pub build_info: Family<BuildInfoLabel, Gauge<f64, AtomicU64>>,
    pub start_time_seconds: Gauge<f64, AtomicU64>,
    pub startup_phase: Family<StartupPhaseLabel, Gauge<f64, AtomicU64>>,
    pub ready: Gauge<f64, AtomicU64>,
    pub live: Gauge<f64, AtomicU64>,
    pub condition_active: Family<ConditionLabel, Gauge<f64, AtomicU64>>,
    pub wab_shard_up: Family<ShardLabel, Gauge<f64, AtomicU64>>,
    pub wab_handoff_blocked_seconds: Family<ShardLabel, Gauge<f64, AtomicU64>>,
    pub worker_up: Family<WorkerLabel, Gauge<f64, AtomicU64>>,
    pub wab_cap_rejecting: Gauge<f64, AtomicU64>,
    pub wab_undelivered_segments: Gauge<f64, AtomicU64>,
    pub wab_undelivered_records: Gauge<f64, AtomicU64>,
    pub wab_oldest_undelivered_age_seconds: Gauge<f64, AtomicU64>,
    pub wab_active_segment_age_seconds: Family<ShardLabel, Gauge<f64, AtomicU64>>,
    pub drain_pending_segments: Family<DrainQueueLabel, Gauge<f64, AtomicU64>>,
    #[allow(dead_code)] // written from PR 2 (the drain)
    pub drain_replay_remaining_segments: Gauge<f64, AtomicU64>,
    pub drain_heartbeat_age_seconds: Gauge<f64, AtomicU64>,
    pub drain_commit_in_flight_seconds: Gauge<f64, AtomicU64>,
    #[allow(dead_code)] // written from PR 2 (the drain)
    pub drain_paused: Gauge<f64, AtomicU64>,
    #[allow(dead_code)] // written from PR 2 (the drain)
    pub drain_generation: Gauge<f64, AtomicU64>,
    pub sink_last_success_timestamp_seconds: Gauge<f64, AtomicU64>,
    pub sink_last_failure_timestamp_seconds: Family<SinkFailureLabel, Gauge<f64, AtomicU64>>,
    pub sink_consecutive_failures: Gauge<f64, AtomicU64>,
    pub config_wab_max_bytes: Gauge<f64, AtomicU64>,
    pub config_dead_letter_max_bytes: Gauge<f64, AtomicU64>,
    pub config_wab_segment_max_bytes: Gauge<f64, AtomicU64>,
    pub config_segment_max_age_seconds: Gauge<f64, AtomicU64>,
    pub config_segment_max_lifetime_seconds: Gauge<f64, AtomicU64>,
    pub config_sink_max_retries: Gauge<f64, AtomicU64>,
    pub config_shutdown_timeout_seconds: Gauge<f64, AtomicU64>,
    /// Effective `drain_shutdown_budget_secs` (Foundations §7).
    pub config_drain_shutdown_budget_seconds: Gauge<f64, AtomicU64>,
    /// Unix time (seconds) at which a bounded shutdown abandons the drain's
    /// in-flight work. 0 until T2 (`request_stop`) sets it.
    pub drain_shutdown_deadline_seconds: Gauge<f64, AtomicU64>,
    pub records_nack_internal: Family<NackInternalLabel, Counter<u64, AtomicU64>>,
    #[allow(dead_code)] // written from PR 3 (the accept loop)
    pub connections_refused_no_live_shard: Counter<u64, AtomicU64>,
    #[allow(dead_code)] // written from PR 2 (the drain)
    pub drain_segments_abandoned: Counter<u64, AtomicU64>,
    pub wab_segments_not_handed_off: Family<NotHandedOffLabel, Counter<u64, AtomicU64>>,
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
            "Current bytes used by live WAB shard segments on disk: the open active \
             segment (.wab) plus sealed segments awaiting drain (.wab.sealed). Excludes \
             drained-marker segments (.wab.confirmed) and the dead_letter/ and quarantine/ \
             subdirs (which have their own accounting)"
        );
        let wab_fsync_duration = reg!(
            Histogram::new(LATENCY_BUCKETS.iter().copied()),
            "weir_wab_fsync_duration_seconds",
            "Wall-clock time of WAB fdatasync calls"
        );
        // Powers of two through the default batch_size (256) and past it: the
        // question this answers is an order of magnitude, not a percentile.
        let wab_group_commit_records = reg!(
            Histogram::new(
                [
                    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0
                ]
                .into_iter()
            ),
            "weir_wab_group_commit_records",
            "Records covered by one WAB group fsync (the fsync amortisation \
             factor; 1.0 means every record paid its own fsync)"
        );
        let batch_records = reg!(
            Histogram::new(
                [
                    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0
                ]
                .into_iter()
            ),
            "weir_batch_records",
            "Records carried by one PushBatch frame"
        );
        let batch_partial = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_batch_partial",
            "PushBatch frames answered with at least one record not durable"
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
        let drain_channel_depth = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_channel_depth",
            "Sealed segments waiting in the bounded flusher-to-drain channel (snapshot)"
        );
        let drain_channel_capacity = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_channel_capacity",
            "Capacity of the flusher-to-drain channel"
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
        let recovery_segments_failed = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_recovery_segments_failed",
            "WAB segments whose crash recovery failed AND left the segment at its \
             original path, so nothing will ever reach it. Deliberately excludes \
             segments that were quarantined: those are counted by \
             weir_recovery_segments_quarantined_total and are listed by \
             `weir-ctl quarantine`. Non-zero means acked records may be \
             unreachable — check the startup logs for the path and the cause"
        );
        let quarantine_bytes_on_disk = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_quarantine_bytes_on_disk",
            "Bytes held in the quarantine/ subdirectory: forensic copies of \
             corrupt segments preserved because acked records may sit after the \
             corruption. Unlike the quarantine counters this survives a restart. \
             Recover them with `weir-ctl quarantine requeue`"
        );
        let wab_record_logical_bytes = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_record_logical_bytes",
            "Cumulative record payload bytes as accepted from producers, before any \
             compression. Divide by weir_wab_record_stored_bytes_total for the live \
             compression ratio; the two are equal when compression is off"
        );
        let wab_record_stored_bytes = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_record_stored_bytes",
            "Cumulative record payload bytes as written to disk, after compression. \
             Excludes the 8-byte per-record framing, the segment header and the footer, \
             so it is not the segment file size"
        );
        let recovery_quarantine_copy_failed = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_recovery_quarantine_copy_failed",
            "Mid-file-corrupt WAB segments whose quarantine copy failed during \
             recovery; the valid prefix was left un-truncated to preserve any \
             acked-durable tail records (recovery is stuck until the operator clears \
             disk space / read-only state)"
        );
        let wab_cap_rejections = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_cap_rejections",
            "Pushes Nacked because live WAB bytes exceeded wab_max_bytes. These \
             surface to clients as NackReason::InternalError (the same byte as \
             queue saturation), so this counter is how the two are told apart"
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
             is 1 at any time; the others are 0. NOTE: state=\"draining\" does NOT imply \
             delivery progress — a segment stranded waiting on a fully-down sink still reads \
             draining. Watch weir_sink_health{state=\"down\"} and weir_drain_segments_stranded \
             for that case, not this gauge alone. state=\"stopped\" is terminal: the drain \
             supervisor has exited (clean shutdown, the respawn budget exhausted, or the \
             dead-letter writer failing to open at start) and NOTHING is being delivered \
             until the daemon restarts — alert on it"
        );
        let dead_letter_blocked_duration = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_dead_letter_blocked_duration_seconds",
            "Seconds elapsed since the drain entered BlockedDeadLetterFull. Resets to 0 on \
             transition out. Fire an alert if this gauge exceeds your operator-defined \
             threshold (e.g. 300 s)"
        );

        // ── wab_dir ownership (safety floor) ──────────────────────────────────
        let wab_dir_lock_held = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_dir_lock_held",
            "1 while this daemon holds the exclusive flock on <wab_dir>/.weir.lock, so no \
             second weir-server can recover or drain its live segments. 0 means it runs \
             WITHOUT that guard: wab_dir_lock = \"best-effort\" on a filesystem that cannot \
             lock, or ownership was lost just before exit. Alert on 0"
        );
        let wab_dir_lock_integrity_failures = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_dir_lock_integrity_failures",
            "Once-a-second integrity checks of <wab_dir>/.weir.lock that found the file \
             deleted or replaced (the daemon re-created and re-locked it) or that failed with \
             an error. Any increase means someone is tampering with the lock file: while it is \
             missing, a second weir-server can start on this wab_dir. Proven loss to another \
             instance does not count here; the daemon exits with status 3 instead"
        );

        // ── Config reload (safety floor) ──────────────────────────────────────
        let config_reloads = reg!(
            Family::<ConfigReloadLabel, Counter<u64, AtomicU64>>::default(),
            "weir_config_reloads",
            "Config reload requests, by trigger (sighup, or admin for the admin socket's reload \
             verb) and outcome: ok (something was reloaded), failed (a reload was attempted \
             and rejected; the \
             previous config stays in force), noop (nothing reloadable: the config is read \
             once at startup, and only TLS material reloads), dropped (the reload queue was \
             full, as in a SIGHUP flood, so the request never ran)"
        );

        // ── Metrics / health listener (Foundations, spec §5.8) ───────────────
        let http_requests_dropped = reg!(
            Family::<HttpDroppedLabel, Counter<u64, AtomicU64>>::default(),
            "weir_http_requests_dropped",
            "Connections to the metrics/health listener closed without a response, by reason: \
             head_timeout (no complete request head within 1 s), head_pool_full (all 64 \
             head slots busy), metrics_pool_full (more concurrent scrapes than \
             metrics_max_connections), probe_pool_full (more than 4 concurrent /livez, \
             /readyz, /startupz or /status requests)"
        );

        // ── Admin socket (Foundations spec §11) ───────────────────────────────
        let admin_requests = reg!(
            Family::<AdminRequestLabel, Counter<u64, AtomicU64>>::default(),
            "weir_admin_requests",
            "Admin socket requests, by verb (status, reload, drain.pause, drain.resume, \
             drain.retry_stranded; _connect for a connection refused before any request; \
             _invalid for a line that is not a valid request) and outcome (ok, error, \
             refused, busy)"
        );
        // The 13 meaningful series exist from the first scrape (spec §13).
        for verb in [
            AdminVerbLabel::Status,
            AdminVerbLabel::Reload,
            AdminVerbLabel::DrainPause,
            AdminVerbLabel::DrainResume,
            AdminVerbLabel::DrainRetryStranded,
        ] {
            for outcome in [AdminOutcomeLabel::ok, AdminOutcomeLabel::error] {
                let _ = admin_requests.get_or_create(&AdminRequestLabel { verb, outcome });
            }
        }
        for outcome in [AdminOutcomeLabel::refused, AdminOutcomeLabel::busy] {
            let _ = admin_requests.get_or_create(&AdminRequestLabel {
                verb: AdminVerbLabel::Connect,
                outcome,
            });
        }
        let _ = admin_requests.get_or_create(&AdminRequestLabel {
            verb: AdminVerbLabel::Invalid,
            outcome: AdminOutcomeLabel::error,
        });
        let admin_socket_up = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_admin_socket_up",
            "1 while the admin Unix socket is bound and served; 0 when it is disabled, \
             unavailable, or closed at exit"
        );

        // ── Drain ledger (safety floor, spec §4.0) ───────────────────────────
        let drain_local_io_strands = reg!(
            Family::<LocalIoStrandLabel, Counter<u64, AtomicU64>>::default(),
            "weir_drain_local_io_strands",
            "WAB segments the drain set aside after exhausting max_retries on a LOCAL I/O \
             failure rather than a sink failure: cause=\"read\" is reading the sealed segment, \
             cause=\"write\" is writing dead-letter records after a successful commit. Unlike \
             weir_drain_segments_stranded these do not mark the sink down; the idle rescan \
             resumes them past their committed prefix. Investigate the WAB and dead-letter \
             device (I/O errors, ENOSPC, permissions, descriptor exhaustion), not the sink"
        );
        let drain_segments_parked = reg!(
            Family::<ParkedLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_segments_parked",
            "WAB segments the drain has parked: kept on disk, not re-queued this run, and \
             replayed on the next start. reason=\"local_read_failed\" is a persistent \
             permission-class read error, \"unsupported_version\" a segment written by a newer \
             weir, \"drain_panic\" a segment the drain panicked on twice"
        );
        let drain_segments_disposition_pending = reg!(
            Family::<DispositionLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_segments_disposition_pending",
            "Delivered or set-aside WAB segments with a local step still outstanding and \
             retried on every health poll: kind=\"confirm\" is the .confirmed sidecar write or \
             the segment delete, kind=\"quarantine\" the move into quarantine/. These are \
             never re-delivered"
        );
        let drain_confirm_failures = reg!(
            Family::<ConfirmFailureLabel, Counter<u64, AtomicU64>>::default(),
            "weir_drain_confirm_failures",
            "Failed steps confirming a delivered WAB segment: stage=\"sidecar\" is the atomic \
             .confirmed write (tmp, fsync, rename, directory fsync), stage=\"delete\" the removal \
             of the segment after a durable sidecar. Each is retried on every health poll; the \
             segment is never re-delivered. A rising sidecar count usually means the WAB device \
             is full or read-only"
        );
        let drain_quarantine_failures = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_quarantine_failures",
            "WAB segments the drain could not move into quarantine/ (corrupt, truncated or \
             persistently unreadable segments whose readable prefix was already delivered). \
             Counted once per segment; the move is retried on every health poll, the bytes stay \
             in place, and the prefix is never re-delivered. Usually a full or read-only WAB \
             device, or something other than a directory at <wab_dir>/quarantine"
        );
        let drain_dead_letter_write_failures = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_dead_letter_write_failures",
            "Failed attempts to write sink-rejected records into dead_letter/ after the sink \
             commit succeeded. The drain keeps the rejected records in memory and retries only \
             the write, so the accepted records are not re-committed; the segment stays on disk \
             until the write lands. Usually a full or read-only WAB device (ENOSPC, EROFS, \
             EACCES)"
        );

        // ── Drain liveness (safety floor, spec §4.8) ─────────────────────────
        let drain_busy_seconds = reg!(
            Family::<DrainBusyLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_busy_seconds",
            "Seconds the drain thread has spent in its current operation, by kind: \
             sink_commit and sink_probe are calls into the sink; segment_read, local_fsync, \
             dead_letter_write, quarantine_move and dir_scan are local I/O on the WAB device \
             (dir_scan includes the rescan's orphan-file cleanup fsync). Only the current \
             kind is non-zero; all read 0 between operations. Sampled every 5 s. A large \
             local value means a slow disk, which a restart does not fix; a sink_commit \
             value past commit_timeout means a sink blocking the drain thread"
        );

        // ── Foundations: status, lifecycle and conditions (spec §13) ─────────
        let build_info = reg!(
            Family::<BuildInfoLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_build_info",
            "Always 1; the labels carry the daemon version and the wire protocol version"
        );
        let start_time_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_start_time_seconds",
            "Unix time the daemon process started"
        );
        let startup_phase = reg!(
            Family::<StartupPhaseLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_startup_phase",
            "Lifecycle phase, one-hot: initializing, recovering, handoff, binding, serving, \
             shutting_down"
        );
        let ready = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_ready",
            "1 when the daemon can accept ingest (the /readyz verdict), refreshed every 5 s"
        );
        let live = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_live",
            "1 unless a selected liveness check fails (the /livez verdict), refreshed every 5 s"
        );
        let condition_active = reg!(
            Family::<ConditionLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_condition_active",
            "1 while the condition is active, 0 otherwise (also 0 while unevaluated); one \
             series per catalogue code"
        );
        let wab_shard_up = reg!(
            Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_wab_shard_up",
            "1 while the shard's flusher is up or respawning, 0 while starting, offline or stopped"
        );
        let wab_handoff_blocked_seconds = reg!(
            Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_wab_handoff_blocked_seconds",
            "Seconds the shard's flusher has been blocked handing a sealed segment to a full \
             drain channel; 0 when not blocked"
        );
        let worker_up = reg!(
            Family::<WorkerLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_worker_up",
            "1 while the worker thread is running"
        );
        let wab_cap_rejecting = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_cap_rejecting",
            "1 while the wab_max_bytes cap is rejecting pushes"
        );
        let wab_undelivered_segments = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_undelivered_segments",
            "Sealed segments not yet confirmed delivered (5 s scan)"
        );
        let wab_undelivered_records = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_undelivered_records",
            "Records in sealed, undelivered segments, from segment footers (5 s scan)"
        );
        let wab_oldest_undelivered_age_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_wab_oldest_undelivered_age_seconds",
            "Upper bound on the age of the oldest undelivered record, from its segment header; \
             0 when none"
        );
        let wab_active_segment_age_seconds = reg!(
            Family::<ShardLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_wab_active_segment_age_seconds",
            "Age of the shard's active (open) segment; 0 when none"
        );
        let drain_pending_segments = reg!(
            Family::<DrainQueueLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_drain_pending_segments",
            "Segments queued in the drain, by queue (live channel items, backlog)"
        );
        let drain_replay_remaining_segments = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_replay_remaining_segments",
            "Startup replay segments not yet delivered"
        );
        let drain_heartbeat_age_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_heartbeat_age_seconds",
            "Seconds since the drain heartbeat last moved (5 s poll)"
        );
        let drain_commit_in_flight_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_commit_in_flight_seconds",
            "Seconds the current sink commit has been in flight; 0 when none"
        );
        let drain_paused = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_paused",
            "1 while delivery is paused by an operator"
        );
        let drain_generation = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_generation",
            "Drain thread starts in this process (1 + respawns)"
        );
        let sink_last_success_timestamp_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_sink_last_success_timestamp_seconds",
            "Unix time of the last successful sink commit; 0 before the first"
        );
        let sink_last_failure_timestamp_seconds = reg!(
            Family::<SinkFailureLabel, Gauge<f64, AtomicU64>>::default(),
            "weir_sink_last_failure_timestamp_seconds",
            "Unix time of the last failed sink commit, by failure kind"
        );
        let sink_consecutive_failures = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_sink_consecutive_failures",
            "Sink commits failed in a row since the last success"
        );
        let config_wab_max_bytes = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_wab_max_bytes",
            "Effective wab_max_bytes (0 = no cap)"
        );
        let config_dead_letter_max_bytes = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_dead_letter_max_bytes",
            "Effective dead_letter_max_bytes"
        );
        let config_wab_segment_max_bytes = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_wab_segment_max_bytes",
            "Effective wab_segment_max_bytes"
        );
        let config_segment_max_age_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_segment_max_age_seconds",
            "Effective wab_segment_max_age_secs (0 = idle seal off)"
        );
        let config_segment_max_lifetime_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_segment_max_lifetime_seconds",
            "Effective wab_segment_max_lifetime_secs (0 = lifetime seal off)"
        );
        let config_sink_max_retries = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_sink_max_retries",
            "Effective sink_max_retries"
        );
        let config_shutdown_timeout_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_shutdown_timeout_seconds",
            "Effective shutdown_timeout_secs"
        );
        let config_drain_shutdown_budget_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_config_drain_shutdown_budget_seconds",
            "Effective drain_shutdown_budget_secs"
        );
        let drain_shutdown_deadline_seconds = reg!(
            Gauge::<f64, AtomicU64>::default(),
            "weir_drain_shutdown_deadline_seconds",
            "Unix time at which a bounded shutdown abandons the drain's in-flight work \
             (0 = not shutting down)"
        );
        let records_nack_internal = reg!(
            Family::<NackInternalLabel, Counter<u64, AtomicU64>>::default(),
            "weir_records_nack_internal",
            "Transient InternalError nacks, by cause"
        );
        let connections_refused_no_live_shard = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_connections_refused_no_live_shard",
            "New connections refused because no shard could accept records"
        );
        let drain_segments_abandoned = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_drain_segments_abandoned",
            "Segments the drain thread left partly delivered when it exited on a stop (the \
             shutdown deadline, or a stop while the segment was paused or blocked); they stay \
             sealed and unconfirmed and are re-delivered from record 0 on the next start"
        );
        let wab_segments_not_handed_off = reg!(
            Family::<NotHandedOffLabel, Counter<u64, AtomicU64>>::default(),
            "weir_wab_segments_not_handed_off",
            "Sealed segments a flusher could not hand to the drain, by reason: budget (the 1 s \
             shutdown hand-off budget ran out) or disconnected (the drain was gone); durable, \
             delivered on restart"
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
            quarantine_bytes_on_disk,
            wab_fsync_duration,
            wab_group_commit_records,
            batch_records,
            batch_partial,
            wab_flusher_panics,
            drain_panics,
            wab_fsync_failures,
            sink_commit_duration,
            sink_commit_records,
            sink_health,
            sink_info,
            queue_depth,
            drain_channel_depth,
            drain_channel_capacity,
            recovery_records_replayed,
            recovery_segments_quarantined,
            recovery_segments_failed,
            recovery_quarantine_copy_failed,
            wab_cap_rejections,
            wab_record_logical_bytes,
            wab_record_stored_bytes,
            wab_unexpected_mode,
            dead_letter_bytes_on_disk,
            dead_letter_full,
            drain_segments_stranded,
            drain_segments_resumed,
            drain_state,
            dead_letter_blocked_duration,
            wab_dir_lock_held,
            wab_dir_lock_integrity_failures,
            config_reloads,
            http_requests_dropped,
            admin_requests,
            admin_socket_up,
            drain_local_io_strands,
            drain_segments_parked,
            drain_segments_disposition_pending,
            drain_confirm_failures,
            drain_quarantine_failures,
            drain_dead_letter_write_failures,
            drain_busy_seconds,
            build_info,
            start_time_seconds,
            startup_phase,
            ready,
            live,
            condition_active,
            wab_shard_up,
            wab_handoff_blocked_seconds,
            worker_up,
            wab_cap_rejecting,
            wab_undelivered_segments,
            wab_undelivered_records,
            wab_oldest_undelivered_age_seconds,
            wab_active_segment_age_seconds,
            drain_pending_segments,
            drain_replay_remaining_segments,
            drain_heartbeat_age_seconds,
            drain_commit_in_flight_seconds,
            drain_paused,
            drain_generation,
            sink_last_success_timestamp_seconds,
            sink_last_failure_timestamp_seconds,
            sink_consecutive_failures,
            config_wab_max_bytes,
            config_dead_letter_max_bytes,
            config_wab_segment_max_bytes,
            config_segment_max_age_seconds,
            config_segment_max_lifetime_seconds,
            config_sink_max_retries,
            config_shutdown_timeout_seconds,
            config_drain_shutdown_budget_seconds,
            drain_shutdown_deadline_seconds,
            records_nack_internal,
            connections_refused_no_live_shard,
            drain_segments_abandoned,
            wab_segments_not_handed_off,
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
            .drain_state
            .get_or_create(&DrainStateLabel {
                state: DrainStateValue::stopped,
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

        // Every metrics-listener drop reason exists at 0 from boot (spec §13),
        // so `rate()` works before the first drop.
        for reason in [
            HttpDropReason::head_timeout,
            HttpDropReason::head_pool_full,
            HttpDropReason::metrics_pool_full,
            HttpDropReason::probe_pool_full,
        ] {
            let _ = metrics
                .http_requests_dropped
                .get_or_create(&HttpDroppedLabel { reason });
        }

        // Every drain-ledger series exists at 0 from boot, so a scraper can
        // baseline and alert on them before the first strand or park.
        for cause in [LocalIoStrandCause::read, LocalIoStrandCause::write] {
            let _ = metrics
                .drain_local_io_strands
                .get_or_create(&LocalIoStrandLabel { cause });
        }
        for reason in [
            ParkedReason::local_read_failed,
            ParkedReason::unsupported_version,
            ParkedReason::drain_panic,
        ] {
            metrics
                .drain_segments_parked
                .get_or_create(&ParkedLabel { reason })
                .set(0.0);
        }
        for kind in [DispositionKind::confirm, DispositionKind::quarantine] {
            metrics
                .drain_segments_disposition_pending
                .get_or_create(&DispositionLabel { kind })
                .set(0.0);
        }
        for stage in [ConfirmFailureStage::sidecar, ConfirmFailureStage::delete] {
            let _ = metrics
                .drain_confirm_failures
                .get_or_create(&ConfirmFailureLabel { stage });
        }
        for kind in DrainBusyKind::ALL {
            metrics
                .drain_busy_seconds
                .get_or_create(&DrainBusyLabel { kind })
                .set(0.0);
        }

        // Pre-initialise the labeled nack counter family so the series exist at 0
        // on the first scrape — without this a scraper can't baseline them until
        // the first nack actually happens (unlike the pre-initialised gauges and
        // the unlabeled weir_ack_timeout counter, which exist at 0 from boot).
        // The decode-error reject path attributes its nack to tier=durable (the
        // durability tier isn't known until the header parses), so touching every
        // reason at tier=durable covers the always-reachable label combinations; the
        // per-tier payload_too_large/internal_error/empty_payload series still
        // appear on first occurrence as before.
        for reason in [
            NackReason::bad_magic,
            NackReason::version_mismatch,
            NackReason::bad_header_crc,
            NackReason::payload_too_large,
            NackReason::bad_payload_crc,
            NackReason::internal_error,
            NackReason::empty_payload,
            NackReason::unknown_message,
            NackReason::reserved_flags_set,
            NackReason::bad_batch_framing,
        ] {
            // get_or_create returns a guard; binding it to `_` is enough — the
            // series is registered at 0 by the lookup itself.
            let _ = metrics.records_nack.get_or_create(&NackLabel {
                tier: TierValue::durable,
                reason,
            });
        }

        // Foundations (spec §9.2 rule 6): every fixed label set exists at 0
        // from boot. The per-shard and per-worker series are created by
        // `init_topology` once the topology is known.
        for phase in StartupPhaseValue::ALL {
            metrics
                .startup_phase
                .get_or_create(&StartupPhaseLabel { phase })
                .set(0.0);
        }
        for e in weir_status::CATALOGUE {
            metrics
                .condition_active
                .get_or_create(&ConditionLabel::of(e))
                .set(0.0);
        }
        for queue in [DrainQueue::live, DrainQueue::backlog] {
            metrics
                .drain_pending_segments
                .get_or_create(&DrainQueueLabel { queue })
                .set(0.0);
        }
        for kind in SinkFailureValue::ALL {
            metrics
                .sink_last_failure_timestamp_seconds
                .get_or_create(&SinkFailureLabel { kind })
                .set(0.0);
        }
        for cause in NackInternalCause::ALL {
            let _ = metrics
                .records_nack_internal
                .get_or_create(&NackInternalLabel { cause });
        }
        for reason in NotHandedOffReason::ALL {
            let _ = metrics
                .wab_segments_not_handed_off
                .get_or_create(&NotHandedOffLabel { reason });
        }

        (metrics, reg)
    }
}

impl Metrics {
    /// Pre-creates the per-shard and per-worker series (spec §9.2 rule 6:
    /// after startup, `get_or_create` only ever takes the read lock).
    pub(crate) fn init_topology(&self, shard_count: usize, worker_count: usize) {
        for s in 0..shard_count {
            let l = ShardLabel::of(s);
            self.wab_shard_up.get_or_create(&l).set(0.0);
            self.wab_handoff_blocked_seconds.get_or_create(&l).set(0.0);
            self.wab_active_segment_age_seconds
                .get_or_create(&l)
                .set(0.0);
        }
        for w in 0..worker_count {
            self.worker_up.get_or_create(&WorkerLabel::of(w)).set(0.0);
        }
    }

    /// Both causes of `weir_wab_segments_not_handed_off_total` summed: what
    /// PR 3's DST invariant and PR 4's T3 summary read.
    #[cfg_attr(not(test), allow(dead_code))] // PR 3 / PR 4 call this
    pub(crate) fn wab_segments_not_handed_off_total(&self) -> u64 {
        NotHandedOffReason::ALL
            .into_iter()
            .map(|reason| {
                self.wab_segments_not_handed_off
                    .get_or_create(&NotHandedOffLabel { reason })
                    .get()
            })
            .sum()
    }

    /// Called by the 500 ms poll task with `board.drain_depth`. Skips PR 1's
    /// `DEPTH_UNPUBLISHED` sentinel (usize::MAX, the value before the first
    /// flusher hand-off or drain slice), which is "unknown", not a depth.
    pub(crate) fn publish_drain_channel_depth(&self, depth: usize) {
        if depth != crate::status::board::DEPTH_UNPUBLISHED {
            self.drain_channel_depth.set(depth as f64);
        }
    }

    /// `weir_build_info` and `weir_start_time_seconds`, once at startup.
    pub(crate) fn publish_build_info(&self, version: &str, wire_version: u8) {
        self.build_info
            .get_or_create(&BuildInfoLabel {
                version: version.to_string(),
                wire_version: wire_version.to_string(),
            })
            .set(1.0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        self.start_time_seconds.set(now);
    }

    /// The `weir_config_*` gauges: the effective values of the keys an
    /// operator most often needs next to a dashboard panel.
    pub(crate) fn publish_config(&self, c: &crate::config::Config) {
        self.config_wab_max_bytes.set(c.wab_max_bytes as f64);
        self.config_dead_letter_max_bytes
            .set(c.dead_letter_max_bytes as f64);
        self.config_wab_segment_max_bytes
            .set(c.wab_segment_max_bytes as f64);
        self.config_segment_max_age_seconds
            .set(c.wab_segment_max_age_secs as f64);
        self.config_segment_max_lifetime_seconds
            .set(c.wab_segment_max_lifetime_secs as f64);
        self.config_sink_max_retries
            .set(f64::from(c.sink_max_retries));
        self.config_shutdown_timeout_seconds
            .set(c.shutdown_timeout_secs as f64);
        self.config_drain_shutdown_budget_seconds
            .set(c.drain_shutdown_budget_secs as f64);
    }

    /// Sets `weir_startup_phase` one-hot on `phase`.
    pub(crate) fn publish_phase(&self, phase: weir_status::Phase) {
        let current = StartupPhaseValue::of(phase);
        for p in StartupPhaseValue::ALL {
            self.startup_phase
                .get_or_create(&StartupPhaseLabel { phase: p })
                .set(if p == current { 1.0 } else { 0.0 });
        }
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
    fn drain_channel_depth_and_capacity_are_exported() {
        let (m, reg) = Metrics::new();
        m.drain_channel_capacity
            .set(crate::wab::DRAIN_CHANNEL_CAPACITY as f64);
        m.publish_drain_channel_depth(7);
        let out = encode_to_string(&reg);
        assert!(out.contains("weir_drain_channel_depth 7"), "{out}");
        assert!(out.contains("weir_drain_channel_capacity 256"), "{out}");
        // PR 1's board holds DEPTH_UNPUBLISHED (usize::MAX) until the first
        // hand-off or drain slice stores a real len(): never export it
        // (README Review Focus 2).
        m.publish_drain_channel_depth(crate::status::board::DEPTH_UNPUBLISHED);
        let out = encode_to_string(&reg);
        assert!(
            out.contains("weir_drain_channel_depth 7"),
            "the unpublished sentinel leaked: {out}"
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
        //
        // Every `reg!` call in `Metrics::new`. The match
        // is on the whole `# HELP <name> ` line, not a substring, because many
        // names are prefixes of others (`weir_wab_segments` would otherwise be
        // satisfied by any `weir_wab_segments_*`).
        let mut expected = vec![
            "weir_records_accepted",
            "weir_records_ack",
            "weir_records_nack",
            "weir_accept_latency_seconds",
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
            "weir_drain_channel_depth",
            "weir_drain_channel_capacity",
            "weir_recovery_records_replayed",
            "weir_recovery_segments_quarantined",
            "weir_recovery_segments_failed",
            "weir_recovery_quarantine_copy_failed",
            "weir_quarantine_bytes_on_disk",
            "weir_dead_letter_bytes_on_disk",
            "weir_dead_letter_full",
            "weir_drain_segments_stranded",
            "weir_drain_segments_resumed",
            "weir_drain_state",
            "weir_dead_letter_blocked_duration_seconds",
            "weir_wab_fsync_failures",
            "weir_wab_flusher_panics",
            "weir_wab_cap_rejections",
            "weir_wab_unexpected_mode",
            "weir_wab_record_logical_bytes",
            "weir_wab_record_stored_bytes",
            "weir_wab_group_commit_records",
            "weir_wab_dir_lock_held",
            "weir_wab_dir_lock_integrity_failures",
            "weir_ack_timeout",
            "weir_batch_records",
            "weir_batch_partial",
            "weir_accept_resource_exhaustion",
            "weir_connection_idle_timeout",
            "weir_connection_rejected_peer_uid",
            "weir_connections_aborted_at_shutdown",
            "weir_config_reloads",
            "weir_drain_panics",
            "weir_drain_local_io_strands",
            "weir_drain_segments_parked",
            "weir_drain_segments_disposition_pending",
            "weir_drain_confirm_failures",
            "weir_drain_quarantine_failures",
            "weir_drain_dead_letter_write_failures",
            "weir_drain_busy_seconds",
            // Foundations PR 1 (status, lifecycle and conditions)
            "weir_build_info",
            "weir_start_time_seconds",
            "weir_startup_phase",
            "weir_ready",
            "weir_live",
            "weir_condition_active",
            "weir_wab_shard_up",
            "weir_wab_handoff_blocked_seconds",
            "weir_worker_up",
            "weir_wab_cap_rejecting",
            "weir_wab_undelivered_segments",
            "weir_wab_undelivered_records",
            "weir_wab_oldest_undelivered_age_seconds",
            "weir_wab_active_segment_age_seconds",
            "weir_drain_pending_segments",
            "weir_drain_replay_remaining_segments",
            "weir_drain_heartbeat_age_seconds",
            "weir_drain_commit_in_flight_seconds",
            "weir_drain_paused",
            "weir_drain_generation",
            "weir_sink_last_success_timestamp_seconds",
            "weir_sink_last_failure_timestamp_seconds",
            "weir_sink_consecutive_failures",
            "weir_config_wab_max_bytes",
            "weir_config_dead_letter_max_bytes",
            "weir_config_wab_segment_max_bytes",
            "weir_config_segment_max_age_seconds",
            "weir_config_segment_max_lifetime_seconds",
            "weir_config_sink_max_retries",
            "weir_config_shutdown_timeout_seconds",
            "weir_records_nack_internal",
            "weir_connections_refused_no_live_shard",
            "weir_drain_segments_abandoned",
            "weir_wab_segments_not_handed_off",
            "weir_config_drain_shutdown_budget_seconds",
            "weir_drain_shutdown_deadline_seconds",
            // Metrics / health listener (Foundations PR 5)
            "weir_http_requests_dropped",
            // Foundations PR 6 (admin socket)
            "weir_admin_requests",
            "weir_admin_socket_up",
        ];
        if cfg!(feature = "bench-trace") {
            expected.extend([
                "weir_stage_queue_seconds",
                "weir_stage_bridge_wait_seconds",
                "weir_stage_write_seconds",
                "weir_stage_total_seconds",
            ]);
        }
        for name in &expected {
            assert!(
                out.lines()
                    .any(|l| l.starts_with(&format!("# HELP {name} "))),
                "metric {name:?} missing from output:\n{out}"
            );
        }
        // And nothing registered that this list does not name: a new family
        // has to be added here, or this fails.
        let registered = out.lines().filter(|l| l.starts_with("# HELP ")).count();
        assert_eq!(
            registered,
            expected.len(),
            "Metrics::new registers {registered} families but this test names {}; \
             add the new family to `expected`",
            expected.len()
        );
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
        // The terminal state must exist at 0 from boot too, so a scraper can
        // baseline (and alert on) it before the drain has ever stopped.
        assert!(
            out.contains("weir_drain_state{state=\"stopped\"} 0"),
            "drain_state stopped label missing or not pre-initialised to 0:\n{out}"
        );
        assert!(out.contains("healthy"), "sink_health healthy label missing");
    }

    /// The label order is the contract operators query by:
    /// `weir_config_reloads_total{trigger="sighup",outcome="noop"}`.
    #[test]
    fn config_reloads_encodes_trigger_then_outcome() {
        let (m, reg) = Metrics::new();
        m.config_reloads
            .get_or_create(&ConfigReloadLabel {
                trigger: ConfigReloadTrigger::sighup,
                outcome: ConfigReloadOutcome::dropped,
            })
            .inc();
        let out = encode_to_string(&reg);
        assert!(
            out.contains("weir_config_reloads_total{trigger=\"sighup\",outcome=\"dropped\"} 1"),
            "{out}"
        );
    }

    /// The lock gauge starts at 0: `main` sets it to 1 only after the lock is
    /// actually held, so a scrape can never report a guard that is not there.
    #[test]
    fn wab_dir_lock_held_starts_at_zero() {
        let (m, reg) = Metrics::new();
        assert!(encode_to_string(&reg).contains("weir_wab_dir_lock_held 0"));
        m.wab_dir_lock_held.set(1.0);
        assert!(encode_to_string(&reg).contains("weir_wab_dir_lock_held 1"));
    }

    #[test]
    fn drain_ledger_series_are_pre_initialised_to_zero() {
        let (_m, reg) = Metrics::new();
        let out = encode_to_string(&reg);
        for series in [
            "weir_drain_local_io_strands_total{cause=\"read\"} 0",
            "weir_drain_local_io_strands_total{cause=\"write\"} 0",
            "weir_drain_segments_parked{reason=\"local_read_failed\"} 0",
            "weir_drain_segments_parked{reason=\"unsupported_version\"} 0",
            "weir_drain_segments_parked{reason=\"drain_panic\"} 0",
            "weir_drain_segments_disposition_pending{kind=\"confirm\"} 0",
            "weir_drain_segments_disposition_pending{kind=\"quarantine\"} 0",
            "weir_drain_confirm_failures_total{stage=\"sidecar\"} 0",
            "weir_drain_confirm_failures_total{stage=\"delete\"} 0",
            "weir_drain_quarantine_failures_total 0",
            "weir_drain_dead_letter_write_failures_total 0",
        ] {
            assert!(
                out.contains(series),
                "{series:?} missing or not pre-initialised to 0:\n{out}"
            );
        }
    }

    #[test]
    fn foundations_families_exist_from_boot_with_their_label_sets() {
        let (m, reg) = Metrics::new();
        m.init_topology(2, 1);
        let out = encode_to_string(&reg);
        for series in [
            "weir_startup_phase{phase=\"initializing\"} 0",
            "weir_startup_phase{phase=\"shutting_down\"} 0",
            "weir_ready 0",
            "weir_live 0",
            "weir_condition_active{code=\"starting\",severity=\"info\"} 0",
            "weir_condition_active{code=\"admin_unavailable\",severity=\"warning\"} 0",
            "weir_wab_shard_up{shard=\"1\"} 0",
            "weir_wab_handoff_blocked_seconds{shard=\"0\"} 0",
            "weir_wab_active_segment_age_seconds{shard=\"1\"} 0",
            "weir_worker_up{worker=\"0\"} 0",
            "weir_wab_cap_rejecting 0",
            "weir_drain_pending_segments{queue=\"live\"} 0",
            "weir_drain_pending_segments{queue=\"backlog\"} 0",
            "weir_sink_last_failure_timestamp_seconds{kind=\"contract_violation\"} 0",
            "weir_records_nack_internal_total{cause=\"queue_full\"} 0",
            "weir_records_nack_internal_total{cause=\"ownership_lost\"} 0",
            "weir_connections_refused_no_live_shard_total 0",
            "weir_drain_segments_abandoned_total 0",
            "weir_wab_segments_not_handed_off_total{reason=\"budget\"} 0",
            "weir_wab_segments_not_handed_off_total{reason=\"disconnected\"} 0",
        ] {
            assert!(out.contains(series), "{series:?} missing:\n{out}");
        }
        assert_eq!(
            out.matches("weir_condition_active{").count(),
            30,
            "one series per daemon code"
        );
        assert_eq!(m.wab_segments_not_handed_off_total(), 0);
    }

    #[test]
    fn build_info_config_and_phase_publish_once_and_one_hot() {
        let (m, reg) = Metrics::new();
        m.publish_build_info("4.1.0", 1);
        m.publish_phase(weir_status::Phase::Recovering);
        m.publish_phase(weir_status::Phase::Serving);
        let out = encode_to_string(&reg);
        assert!(
            out.contains("weir_build_info{version=\"4.1.0\",wire_version=\"1\"} 1"),
            "{out}"
        );
        assert!(out.contains("weir_startup_phase{phase=\"serving\"} 1"));
        assert!(out.contains("weir_startup_phase{phase=\"recovering\"} 0"));
        let start: f64 = out
            .lines()
            .find_map(|l| l.strip_prefix("weir_start_time_seconds "))
            .expect("start time series")
            .parse()
            .unwrap();
        assert!(start > 1_700_000_000.0, "start time is stamped: {start}");
    }

    /// The `weir_config_*` gauges carry the effective values, so a dashboard
    /// can draw the cap next to the usage it bounds.
    #[test]
    fn publish_config_sets_the_effective_values() {
        use crate::config::{Config, PartialConfig};
        let dir = crate::testutil::scratch_dir("metrics_publish_config");
        crate::testutil::mkdir_p(&dir);
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir),
                wab_max_bytes: Some(1 << 30),
                sink_max_retries: Some(7),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .expect("valid config");
        let (m, _reg) = Metrics::new();
        m.publish_config(&c);
        assert_eq!(m.config_wab_max_bytes.get(), (1u64 << 30) as f64);
        assert_eq!(m.config_sink_max_retries.get(), 7.0);
        assert_eq!(
            m.config_dead_letter_max_bytes.get(),
            c.dead_letter_max_bytes as f64
        );
        assert_eq!(
            m.config_wab_segment_max_bytes.get(),
            c.wab_segment_max_bytes as f64
        );
        assert_eq!(
            m.config_segment_max_age_seconds.get(),
            c.wab_segment_max_age_secs as f64
        );
        assert_eq!(
            m.config_segment_max_lifetime_seconds.get(),
            c.wab_segment_max_lifetime_secs as f64
        );
        assert_eq!(
            m.config_shutdown_timeout_seconds.get(),
            c.shutdown_timeout_secs as f64
        );
        assert_eq!(
            m.config_drain_shutdown_budget_seconds.get(),
            c.drain_shutdown_budget_secs as f64
        );
    }

    #[test]
    fn the_shutdown_budget_and_deadline_gauges_are_exported() {
        let (m, reg) = Metrics::new();
        m.config_drain_shutdown_budget_seconds.set(10.0);
        m.drain_shutdown_deadline_seconds.set(1_800_000_000.5);
        let out = encode_to_string(&reg);
        assert!(
            out.contains("weir_config_drain_shutdown_budget_seconds 10"),
            "{out}"
        );
        assert!(
            out.contains("weir_drain_shutdown_deadline_seconds 1800000000.5"),
            "{out}"
        );
    }

    /// The `kind` label value is the wire name weir-status gives the same
    /// failure kind, so `evidence_from_metrics` reads back what PR 2 writes.
    #[test]
    fn sink_failure_label_values_match_the_weir_status_wire_names() {
        for &k in weir_status::SinkFailureKind::ALL {
            let (m, reg) = Metrics::new();
            m.sink_last_failure_timestamp_seconds
                .get_or_create(&SinkFailureLabel {
                    kind: SinkFailureValue::of(k),
                })
                .set(42.0);
            let out = encode_to_string(&reg);
            let series = format!(
                "weir_sink_last_failure_timestamp_seconds{{kind=\"{}\"}} 42",
                k.as_str()
            );
            assert!(out.contains(&series), "{series:?} missing:\n{out}");
        }
    }

    /// All four drop reasons exist at 0 from boot (spec §13: every new family
    /// pre-creates its label set), so `rate()` works before the first drop.
    #[test]
    fn http_requests_dropped_series_are_pre_initialised_to_zero() {
        let (m, reg) = Metrics::new();
        let out = encode_to_string(&reg);
        for reason in [
            "head_timeout",
            "head_pool_full",
            "metrics_pool_full",
            "probe_pool_full",
        ] {
            let series = format!("weir_http_requests_dropped_total{{reason=\"{reason}\"}} 0");
            assert!(out.contains(&series), "{series:?} missing:\n{out}");
        }
        m.http_requests_dropped
            .get_or_create(&HttpDroppedLabel {
                reason: HttpDropReason::head_timeout,
            })
            .inc();
        assert!(
            encode_to_string(&reg)
                .contains("weir_http_requests_dropped_total{reason=\"head_timeout\"} 1")
        );
    }

    /// spec §11/§13: the verb label is bounded and spelled exactly as on the
    /// wire, and every meaningful series exists from the first scrape.
    #[test]
    fn admin_series_are_pre_created_with_wire_verb_names() {
        let (m, reg) = Metrics::new();
        let out = encode_to_string(&reg);
        for verb in [
            "status",
            "reload",
            "drain.pause",
            "drain.resume",
            "drain.retry_stranded",
        ] {
            for outcome in ["ok", "error"] {
                let s =
                    format!("weir_admin_requests_total{{verb=\"{verb}\",outcome=\"{outcome}\"}} 0");
                assert!(out.contains(&s), "missing {s}\n{out}");
            }
        }
        for s in [
            "weir_admin_requests_total{verb=\"_connect\",outcome=\"refused\"} 0",
            "weir_admin_requests_total{verb=\"_connect\",outcome=\"busy\"} 0",
            "weir_admin_requests_total{verb=\"_invalid\",outcome=\"error\"} 0",
            "weir_admin_socket_up 0",
        ] {
            assert!(out.contains(s), "missing {s}\n{out}");
        }
        m.admin_socket_up.set(1.0);
        assert!(encode_to_string(&reg).contains("weir_admin_socket_up 1"));
        let ok_status = AdminRequestLabel {
            verb: AdminVerbLabel::Status,
            outcome: AdminOutcomeLabel::ok,
        };
        m.admin_requests.get_or_create(&ok_status).inc();
        assert!(
            encode_to_string(&reg)
                .contains("weir_admin_requests_total{verb=\"status\",outcome=\"ok\"} 1")
        );
    }
}
