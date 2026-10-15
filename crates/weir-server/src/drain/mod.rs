//! Drain: reads sealed WAB segments and forwards them to a `Sink`.
//!
//! # State machine
//!
//! The drain thread operates in one of three states:
//!
//! ```text
//! Draining
//!   │  Transient sink error
//!   ▼
//! RetryingTransient  ──(MAX_RETRIES exhausted)──▶  Draining (next segment;
//!   │                                              stranded at once, held off
//!   │                                              by its not_before)
//!   │  success
//!   ▼
//! Draining
//!   │  Permanent error AND dead-letter cap exceeded
//!   ▼
//! BlockedDeadLetterFull  ──(the blocked write fits)──▶  RetryingTransient (same
//!                                            segment, RESUMING past already-processed
//!                                            sub-batches — see F05)
//! ```
//!
//! A sub-batch the sink committed but whose rejected records could not be
//! dead-lettered (the cap blocked the write, or the write failed) leaves those
//! records with the drain's ledger as a carry. The next attempt writes the
//! carry and resumes past the sub-batch, so its accepted records are committed
//! once per run, not once per retry (spec §4.5, §4.11).
//!
//! # Confirmed files
//!
//! After draining a segment the drain writes a `.confirmed` sidecar (atomically:
//! a fsynced `.confirmed.tmp` renamed into place) and deletes the sealed
//! segment. Crash recovery skips segments with a valid `.confirmed` file. A
//! crash between writing `.confirmed` and deleting the segment is safe: crash
//! recovery skips the segment (confirmed), and the orphan sealed file is deleted
//! by the startup scan or the stranded-segment rescan once this daemon owns the
//! `wab_dir` ([`DrainConfig::orphan_cleanup`]). A confirm step that fails at
//! runtime is retried by the ledger on every health poll; the segment is never
//! delivered again.

mod confirmed;
pub mod dead_letter;
pub(crate) mod ledger;

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_channel::RecvTimeoutError;
use tracing::{debug, error, info, warn};
use weir_core::Payload;
use weir_sink_sdk::RecordId;

use crate::{
    metrics::{
        DispositionKind, DispositionLabel, DrainStateLabel, DrainStateValue, LocalIoStrandCause,
        LocalIoStrandLabel, Metrics, Outcome, OutcomeLabel, ParkedLabel, ParkedReason,
        SinkHealthLabel, SinkHealthState,
    },
    sink::{Sink, SinkError, SinkHealth},
    wab::{SegmentReader, read_segment_record_count, segment::segment_identity},
};

use confirmed::confirm_and_delete;
use dead_letter::DeadLetterWriter;
use ledger::{Admit, DeadLetterCarry, SegmentLedger, TransientCause};
#[cfg(test)]
use ledger::{ErrClass, ParkReason};

// ── Constants ─────────────────────────────────────────────────────────────────

pub const MAX_RETRIES: u32 = 3;

/// How many strand cycles of segment-class local read failures (`Access` or
/// `Media`, spec §4.0 table) a segment gets before it is escalated: parked for
/// `Access`, quarantined for `Media`. A cycle is `max_retries + 1` attempts, so
/// the escalation fires on the `(max_retries + 1) × 3`-th failure. `Process`
/// errors never count (spec §4.1).
pub(crate) const LOCAL_READ_STRAND_LIMIT: u32 = 3;

/// The minimum time since a segment's FIRST counted local read failure in this
/// run before that segment may be escalated, on top of
/// [`LOCAL_READ_STRAND_LIMIT`] strand cycles (conf-regressions-8). Both must
/// hold. The cycle count alone escalated about a minute into a fault at the
/// defaults, so a transient device blip (a remount, an iSCSI or NBD path flap,
/// a `chown -R` or SELinux relabel during maintenance) quarantined a good
/// segment into manual recovery or parked it until a restart, where 4.0.1 kept
/// retrying and recovered by itself. Ten minutes outlasts any blip worth the
/// name while still setting a genuinely failed segment aside in the same
/// incident. The segment cycles meanwhile, a cool-down and one strand per
/// cycle, as a `Process` error does.
pub(crate) const LOCAL_READ_ESCALATION_MIN_AGE: Duration = Duration::from_secs(600);

/// Upper bound on a single transient-retry backoff once shutdown has been
/// observed (the drain channel disconnected). The backoff is clamped rather than
/// dropped: firing the remaining attempts back-to-back with no delay spends the
/// whole retry budget on what is effectively one attempt, since a sink that
/// needs milliseconds to answer fails the same way three times inside the same
/// instant (W2).
///
/// Why 250 ms:
///
///   * It is above the default `sink_retry_base_delay_ms` (100), so at default
///     config the shutdown schedule (100 / 200 / 250 ms) is all but identical to
///     the normal one (100 / 200 / 400 ms) — the clamp only bites on an operator's
///     long base delay or a server-supplied `Retry-After` (up to 5 minutes).
///   * With the default retry budget it covers a whole episode in
///     `MAX_RETRIES × 250 ms` = 750 ms (an episode serves one wait per retry;
///     the strand after the last attempt serves none, spec §4.6), inside the
///     1 s [`SHUTDOWN_RETRY_BACKOFF_BUDGET`] (the actual bound on total added
///     shutdown time). 1 s is small against the per-attempt
///     `commit_timeout` (at least 60 s: twice `sink_timeout_secs`, floored at
///     60) that the shutdown path already tolerates,
///     so this is never the term that decides how long shutdown takes.
///   * It is 5× the 50 ms channel-poll slice used while the channel is live, so
///     the wait is a real delay and not a rounding artifact of the poll loop.
const SHUTDOWN_RETRY_BACKOFF_CAP: Duration = Duration::from_millis(250);

/// Total time the drain may spend on [`SHUTDOWN_RETRY_BACKOFF_CAP`]-clamped
/// backoffs across the whole shutdown, after which shutdown retries fire
/// immediately again.
///
/// A per-retry clamp alone is not enough to bound shutdown. Stranding a segment
/// marks the sink down, so the Draining arm's next health poll sees a down→up
/// edge and re-queues that same segment (the 4a auto-resume) once its
/// `not_before` has elapsed, starting a fresh retry episode. Against a sink whose HEAD probe stays healthy while every POST
/// fails transiently, that cycle is unbounded — today it is only survivable
/// because zero-delay retries never let `health_poll_interval` elapse. Spending
/// from one budget keeps the worst case flat: shutdown grows by at most this
/// much in total, no matter how many segments or resume cycles are involved.
///
/// 1 s was sized as `(MAX_RETRIES + 1) × SHUTDOWN_RETRY_BACKOFF_CAP`, when an
/// episode still ended with a final wait before the strand; it now needs only
/// `MAX_RETRIES ×` the cap, so it covers one full retry episode at the default
/// retry budget with room to spare, which is the case worth
/// buying (the segment in flight when the operator sent SIGTERM). Anything after
/// that is a sink that has already failed four spaced-out attempts, which is not
/// worth delaying a shutdown for.
const SHUTDOWN_RETRY_BACKOFF_BUDGET: Duration = Duration::from_secs(1);

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DrainConfig {
    /// Root WAB directory (used to locate and create the dead-letter directory).
    pub wab_dir: PathBuf,
    /// Maximum total byte size of the `dead_letter/` directory. When the drain
    /// would exceed this, it transitions to `BlockedDeadLetterFull`.
    pub dead_letter_max_bytes: u64,
    /// How long `BlockedDeadLetterFull` sleeps between dead-letter size checks.
    pub dead_letter_check_interval: Duration,
    /// Initial delay before the first retry on a transient sink error. Doubles
    /// with each subsequent retry.
    pub base_retry_delay: Duration,
    /// Maximum number of retry attempts per segment before the segment is left
    /// on disk and the drain advances to the next segment.
    pub max_retries: u32,
    /// Maximum times the panic supervisor respawns the drain thread before
    /// giving up (delivery stops; the WAB accumulates until restart). Defaults to
    /// [`MAX_DRAIN_RESPAWNS`]; carried in config so the give-up path is testable
    /// without the real ~4.5 s of inter-respawn backoff.
    pub max_respawns: u32,
    /// Hard upper bound on a single `Sink::commit` call. A backstop against a
    /// sink that hangs without honouring its own internal timeout (notably
    /// third-party sinks built on `weir-sink-sdk`, which carry no built-in
    /// timeout). On elapse, the commit is treated as a transient error and the
    /// segment is retried.
    pub commit_timeout: Duration,
    /// How often the drain re-probes sink health and rescans for stranded
    /// segments to re-drain. Enforced on a wall-clock cadence so it fires even
    /// under sustained load (not only when the channel goes idle). Default
    /// [`HEALTH_POLL_INTERVAL`]; operator-tunable via `health_poll_interval_secs`.
    pub health_poll_interval: Duration,
    /// Timeout for the sink health *probe* (the HEAD/ping in `probe_health`),
    /// kept separate from — and much shorter than — `commit_timeout`: a probe is
    /// a liveness check, not a delivery, so it shouldn't inherit the long
    /// commit backstop. A hung probe past this is treated as `Down`.
    pub health_probe_timeout: Duration,
    /// Whether the stranded-segment rescan may delete an orphan: a sealed
    /// segment beside a valid `.confirmed` sidecar, left by a crash (or a failed
    /// delete) between the two confirm steps (spec §4.3, D7). True only while
    /// this daemon holds the `wab_dir` lock; without it another daemon may be
    /// mid-confirm on the same segment, so orphans are kept.
    pub orphan_cleanup: bool,
    /// [`LOCAL_READ_ESCALATION_MIN_AGE`] in production. Carried in config,
    /// like `max_respawns`, so the escalation tests need not wait ten minutes.
    pub local_read_escalation_min_age: Duration,
}

// ── Internal state machine types ──────────────────────────────────────────────

enum DrainState {
    Draining,
    RetryingTransient {
        segment: PathBuf,
        retries_left: u32,
        next_delay: Duration,
        /// Records of this segment already durably processed by earlier
        /// sub-batches; the retry resumes past them rather than re-committing /
        /// re-dead-lettering them (F05).
        processed: u64,
        /// What made the most recent attempt transient. Decides the strand
        /// bookkeeping once the budget runs out: only a sink cause counts as a
        /// sink strand and marks the sink down (D11).
        cause: TransientCause,
    },
    BlockedDeadLetterFull {
        segment: PathBuf,
        blocked_since: Instant,
        /// Records already durably processed before the block; the post-headroom
        /// retry resumes past them (F05).
        processed: u64,
        /// The estimated bytes of the dead-letter write that blocked. The drain
        /// unblocks only once that write fits under the cap — the same predicate
        /// that blocked it (spec §4.5) — never on a bare `total < cap`.
        needed: u64,
    },
}

enum ProcessResult {
    /// Segment fully processed. Confirm and delete it.
    Confirmed { record_count: u64 },
    /// Sink returned a transient error. Retry the segment after `retry_after`
    /// (if the sink supplied a hint, e.g. an HTTP Retry-After header) or
    /// after the drain's exponential-backoff delay (if `None`). `processed` is
    /// the number of records the *earlier* sub-batches of this attempt durably
    /// committed/dead-lettered — the retry skips them (F05).
    ///
    /// `carry` is set only for a dead-letter write that failed after the sink
    /// committed (`cause` is `LocalWrite`, spec §4.11): the rejected records the
    /// next attempt writes before re-reading, instead of re-committing the
    /// sub-batch.
    Transient {
        retry_after: Option<Duration>,
        processed: u64,
        cause: TransientCause,
        carry: Option<DeadLetterCarry>,
    },
    /// Dead-letter cap would be exceeded. Block until capacity frees. `processed`
    /// carries the durable progress so the post-headroom retry resumes past the
    /// already-handled sub-batches (F05). `needed` is the estimated size of the
    /// write that blocked, for the unblock predicate, and `carry` the rejected
    /// records the post-headroom attempt writes without re-committing the
    /// sub-batch (spec §4.5).
    BlockedDeadLetter {
        processed: u64,
        needed: u64,
        carry: Option<DeadLetterCarry>,
    },
    /// A record failed to read mid-segment. The readable prefix was delivered and
    /// the segment was quarantined for manual recovery — do not retry or delete.
    Quarantined,
    /// As `Quarantined`, but the move into `quarantine/` failed (already logged
    /// and counted). The segment is still on disk, and whatever prefix it had
    /// was delivered: the ledger takes it into `quarantine_pending`, and the
    /// health polls retry only the move — never the delivery (spec §4.4).
    QuarantinePending { reason: &'static str },
    /// The segment cannot be delivered by this binary but is not corrupt (its
    /// header names a newer format version). Nothing was delivered. Set it aside
    /// for the rest of the run, bytes untouched; the next start replays it.
    Parked { reason: ledger::ParkReason },
}

enum BatchResult {
    Ok,
    /// `owed` is set when the sink committed but the dead-letter write of its
    /// rejects failed (a `LocalWrite` cause, spec §4.11).
    Transient {
        retry_after: Option<Duration>,
        cause: TransientCause,
        owed: Option<DeadLetterOwed>,
    },
    /// The dead-letter write of the sink's rejects would exceed the cap; it
    /// needs `needed` bytes of headroom (spec §4.5).
    Blocked {
        needed: u64,
        owed: DeadLetterOwed,
    },
}

/// What a sub-batch still owes the dead-letter directory after its sink call:
/// the records the sink rejected, and how many it accepted (counted as
/// committed only once the rejects are written, S24).
struct DeadLetterOwed {
    payloads: Vec<Payload>,
    committed: u64,
}

impl DeadLetterOwed {
    /// As the ledger's carry for a sub-batch ending at record `through`.
    fn into_carry(self, through: u64) -> DeadLetterCarry {
        DeadLetterCarry {
            through,
            bytes: self.payloads.iter().map(|p| p.len() as u64).sum(),
            committed: self.committed,
            payloads: self.payloads,
        }
    }
}

/// Why [`dead_letter_records`] did not write.
enum DeadLetterFailure {
    /// The write would exceed the cap; it needs `needed` bytes.
    Blocked { needed: u64 },
    /// The write itself failed (already logged and counted).
    WriteFailed,
}

// ── Public interface ──────────────────────────────────────────────────────────

/// Spawns the drain thread. Returns a `JoinHandle` that completes once the
/// `drain_rx` channel closes (i.e. all WAB flusher threads have exited) and all
/// in-flight segments have been processed or explicitly abandoned.
pub fn spawn<S: Sink + 'static>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    heartbeat: DrainHeartbeat,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("weir-drain".into())
        .spawn(move || run_drain_supervised(drain_rx, sink, config, metrics, heartbeat))
        .expect("failed to spawn drain thread")
}

/// Maximum times the drain is respawned after a panic before giving up — past
/// this, delivery stops and the WAB accumulates on disk until restart (the same
/// policy as the WAB flusher supervisor).
///
/// A segment that keeps crashing the drain is parked at its second panic
/// ([`ledger::DRAIN_PANIC_PARK_LIMIT`], spec §4.12), so one poison segment
/// costs two respawns, not all of them. The budget is still per process and
/// never refilled: five distinct poison segments spend it and stop delivery.
pub(crate) const MAX_DRAIN_RESPAWNS: u32 = 10;

/// Runs [`drain_thread`] under panic supervision. A panic in the drain (a sink
/// impl, `process_segment`, `confirm_and_delete`, a `DeadLetterWriter::open`
/// failure, …) is caught and the drain is respawned with a fresh runtime +
/// dead-letter writer, reading from the same channel so buffered segments
/// survive. Without this, a single panic would kill delivery permanently while
/// producers keep being acked and the WAB grows unbounded.
///
/// The segment in flight at panic time stays durable on disk. The respawn's
/// first healthy probe is a recovery edge, so its rescan re-queues that
/// segment (and anything else `pending` lost) and it is retried this run,
/// resuming past whatever prefix the ledger's cursor already covers. A segment
/// that panics the drain a second time is parked as `drain_panic` instead
/// (spec §4.12): left in place, not retried this run, replayed from its first
/// record on the next start. The ledger outlives every respawn, so the count
/// and the park both hold across incarnations.
///
/// Behavioural consequence for a *permanently* unusable dead-letter directory
/// (the case this exists for is a stray file sitting where the directory
/// should be): `drain_state` only reaches `stopped` after the respawn ladder
/// is exhausted, i.e. up to `MAX_DRAIN_RESPAWNS` (10) attempts with
/// `100ms * attempts` backoff between them — roughly 4.5 s in the real
/// default, not immediate. That is the intended cost of routing this failure
/// through the same panic-supervision path as every other drain failure
/// (rather than a special-cased instant bail-out); an operator or readiness
/// script watching `drain_state`/`drain_panics` should expect that delay
/// rather than reading it as a hang.
fn run_drain_supervised<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    heartbeat: DrainHeartbeat,
) {
    let mut attempts = 0u32;
    // Owned here, not by `drain_thread`, so it survives a panic and respawn: a
    // respawned drain still knows which segments are owned by a disposition,
    // parked or cooling down, and where each stranded segment resumes.
    let mut ledger = SegmentLedger::new(Arc::new(ledger::LedgerCounts::default()));
    loop {
        let rx = drain_rx.clone();
        let sink = Arc::clone(&sink);
        let cfg = config.clone();
        let m = Arc::clone(&metrics);
        let hb = heartbeat.clone();
        // True on the first start; false on every respawn (a panic destroys
        // `pending`, so a respawn must treat its first healthy probe as a
        // recovery edge and rescan the disk — see `drain_thread`'s
        // `initial_health_ok`).
        let first_start = attempts == 0;
        let lent = &mut ledger;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drain_thread(rx, sink, cfg, m, first_start, hb, lent)
        }));
        // A clean Ok(()) return means the channel closed (all flushers gone) — done.
        let Err(_) = result else { break };
        attempts += 1;
        metrics.drain_panics.inc();
        // Charge the panic to the segment in flight, if any, and park it at the
        // second (spec §4.12). Without this the respawn's first healthy probe
        // rescans, re-queues the same segment, and a deterministic panic
        // repeats until the budget below is spent and delivery stops for
        // every segment.
        if let Some((path, panics)) = ledger.note_drain_panic() {
            if panics >= ledger::DRAIN_PANIC_PARK_LIMIT {
                error!(
                    path = %path.display(),
                    panics,
                    "drain: this segment crashed the drain {panics} times; parked (left in place, \
                     not retried this run) — weir_drain_segments_parked{{reason=\"drain_panic\"}}; \
                     other segments keep delivering. It replays from its first record on the next \
                     start: fix the sink (or the record it trips on) before restarting"
                );
            } else {
                warn!(
                    path = %path.display(),
                    panics,
                    "drain: panicked while delivering this segment; it is retried after the \
                     respawn and parked if it crashes the drain again"
                );
            }
            publish_ledger_counts(ledger.counts(), &metrics);
        }
        if attempts >= config.max_respawns {
            error!(
                attempts,
                "drain thread panicked too many times; giving up — delivery is \
                 stopped and the WAB will accumulate on disk until restart"
            );
            break;
        }
        error!(
            attempts,
            max = config.max_respawns,
            "drain thread panicked; respawning"
        );
        std::thread::sleep(Duration::from_millis(
            100u64.saturating_mul(attempts as u64),
        ));
    }
    // EVERY exit from the loop above lands here — there is no other way out of
    // it. The `Ok(())` arm breaks on a clean channel close (shutdown — all WAB
    // flushers exited), and the respawn-budget arm breaks once panics (which
    // now include a `DeadLetterWriter::open` failure — see `drain_thread` —
    // as well as a sink impl / `process_segment` / `confirm_and_delete` panic)
    // exhaust `MAX_DRAIN_RESPAWNS`. So one assignment covers both.
    //
    // `drain_state` is written ONLY by the drain thread, and it is pre-initialised
    // to `draining = 1`. Left untouched on exit it stays frozen there: an operator
    // scraping /metrics sees a daemon that is draining while nothing is being
    // delivered, and the WAB growth warning — which reads this gauge — stays
    // silent in precisely the scenario it exists for. Publishing the terminal
    // state is the truth on all three paths, clean shutdown included: the drain
    // genuinely is no longer draining.
    //
    // `sink_health` is deliberately NOT forced here. Once nobody is probing the
    // sink its health is *unknown*, not down — and on the clean-shutdown path the
    // sink is usually perfectly healthy, so writing `down` would fire a false
    // "sink is down" signal on every SIGTERM. `drain_state{stopped}` is the
    // honest signal, and it tells the operator the health reading is stale.
    set_drain_state(&metrics, DrainStateValue::stopped);
    info!("drain supervisor exiting");
}

// ── Drain state helper ────────────────────────────────────────────────────────

fn set_drain_state(metrics: &Metrics, active: DrainStateValue) {
    // Every label value in the family, so the one-hot invariant holds no matter
    // which is active — omitting one here would leave it stuck at its last
    // reading while another state claims to be active.
    let states = [
        DrainStateValue::draining,
        DrainStateValue::retrying_transient,
        DrainStateValue::blocked_dead_letter_full,
        DrainStateValue::stopped,
    ];
    for s in states {
        let v = if s == active { 1.0 } else { 0.0 };
        metrics
            .drain_state
            .get_or_create(&DrainStateLabel { state: s })
            .set(v);
    }
}

fn set_sink_health(metrics: &Metrics, health: SinkHealth) {
    // Map the health to its one-hot active state (logging the reason on the
    // off-nominal arms), then drive the gauge family the same way set_drain_state
    // does: exactly one label set to 1.0, the rest 0.0.
    let active = match &health {
        SinkHealth::Healthy => SinkHealthState::healthy,
        SinkHealth::Degraded(reason) => {
            warn!(reason = %reason, "sink health: degraded");
            SinkHealthState::degraded
        }
        SinkHealth::Down(reason) => {
            error!(reason = %reason, "sink health: down");
            SinkHealthState::down
        }
        // SinkHealth is #[non_exhaustive] (F48): report an unrecognised future
        // state as degraded so the gauge still moves off "healthy" and operators
        // get a signal rather than a silently-stuck reading.
        _ => {
            warn!("sink health: unrecognised state; reporting as degraded");
            SinkHealthState::degraded
        }
    };
    let states = [
        SinkHealthState::healthy,
        SinkHealthState::degraded,
        SinkHealthState::down,
    ];
    for s in states {
        let v = if s == active { 1.0 } else { 0.0 };
        metrics
            .sink_health
            .get_or_create(&SinkHealthLabel { state: s })
            .set(v);
    }
}

/// Publishes the ledger's wait-free counts to the drain-ledger gauges. Reads only
/// the atomics, never the ledger's maps.
fn publish_ledger_counts(counts: &ledger::LedgerCounts, metrics: &Metrics) {
    use std::sync::atomic::Ordering::Relaxed;
    for (reason, why) in [
        (
            ParkedReason::local_read_failed,
            ledger::ParkReason::LocalReadFailed,
        ),
        (
            ParkedReason::unsupported_version,
            ledger::ParkReason::UnsupportedVersion,
        ),
        (ParkedReason::drain_panic, ledger::ParkReason::DrainPanic),
    ] {
        metrics
            .drain_segments_parked
            .get_or_create(&ParkedLabel { reason })
            .set(counts.parked_by_reason[why.index()].load(Relaxed) as f64);
    }
    for (kind, n) in [
        (DispositionKind::confirm, &counts.confirm_pending),
        (DispositionKind::quarantine, &counts.quarantine_pending),
    ] {
        metrics
            .drain_segments_disposition_pending
            .get_or_create(&DispositionLabel { kind })
            .set(n.load(Relaxed) as f64);
    }
}

/// Probes [`Sink::health`] under a timeout backstop and maps a hang to `Down`.
///
/// The drain runs on a single-threaded runtime; an un-timed `health()` that
/// hangs would wedge the entire drain — the idle loop would never return to
/// `recv` (missing shutdown), and the blocked-state poll would never reach its
/// `channel_closed` check. Mirrors the `commit()` timeout backstop so a
/// misbehaving sink degrades the health gauge instead of stalling delivery.
fn probe_health<S: Sink>(
    rt: &tokio::runtime::Runtime,
    sink: &S,
    timeout: Duration,
    hb: &DrainHeartbeat,
) -> SinkHealth {
    // Outside the timeout: a probe that blocks the thread keeps it set.
    let _busy = BusyGuard::new(hb, BusyKind::SinkProbe);
    match rt.block_on(async { tokio::time::timeout(timeout, sink.health()).await }) {
        Ok(health) => health,
        Err(_elapsed) => SinkHealth::Down(format!(
            "health probe exceeded {}s backstop",
            timeout.as_secs()
        )),
    }
}

/// Probes sink health, updates the `weir_sink_health` gauge, and — on a down→up
/// recovery edge (was not-healthy, now healthy) — rescans the WAB for stranded
/// (sealed-but-unconfirmed) segments and re-queues any not already pending, so a
/// recovered sink drains its backlog automatically rather than waiting for a
/// daemon restart. Returns the new health-ok state for the caller's
/// `prev_health_ok`.
///
/// `in_flight` is the segment the caller is *currently* holding outside `pending`
/// — the one being retried (`RetryingTransient`) or parked (`BlockedDeadLetterFull`).
/// It is excluded from the re-queue so the rescan can run from those arms (M2)
/// without double-queuing the segment the arm is about to resume on its own. The
/// `Draining` caller holds nothing across the rescan and passes `None`.
///
/// `exclude` is the drain ledger's `excludes` (spec §4.0): segments owned by an
/// outstanding disposition, parked, or cooling down. The scan skips them before
/// its confirmed-check, so they are neither re-queued nor touched.
#[allow(clippy::too_many_arguments)]
fn probe_and_resume_stranded<S: Sink>(
    rt: &tokio::runtime::Runtime,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    pending: &mut VecDeque<PathBuf>,
    prev_health_ok: bool,
    in_flight: Option<&Path>,
    exclude: &dyn Fn(&Path) -> bool,
    hb: &DrainHeartbeat,
) -> bool {
    let health = probe_health(rt, sink, config.health_probe_timeout, hb);
    // Deliverable, not perfect. `Degraded` means "reachable but not fully
    // healthy" — the HTTP probe returns it for any 4xx outside 401/403/405/501,
    // which includes 404, what a method-routed framework answers for HEAD on a
    // POST-only ingest path (`sink/http.rs:645`). Commits over such a sink
    // succeed; only the probe 4xxes. Requiring `Healthy` here made Degraded a
    // STICKIER failure than Down — a sink that went down and came back healthy
    // un-stranded its backlog, while one that came back degraded never did, for
    // the life of the process. Only `Down` means "do not bother".
    let now_ok = !matches!(health, SinkHealth::Down(_));
    set_sink_health(metrics, health);

    // Recovered, or idle. The edge alone is not enough: a segment strands after
    // `sink_max_retries` transient COMMIT failures, which need not change what
    // the PROBE answers, so a sink that holds one health state across the whole
    // episode strands a segment with no edge on either side of it. An idle drain
    // is precisely when such a segment is invisible and when a directory walk
    // costs nothing, so it rescans unconditionally. The edge stays as the cost
    // control for the busy case: with `pending` non-empty the scan is skipped,
    // and the segments in it are about to be processed anyway.
    if now_ok && (!prev_health_ok || pending.is_empty()) {
        // `None` shard_count: skip the beyond-configured-count advisory — that's
        // a startup-replay concern, not a recovery one.
        let scan = {
            let _busy = BusyGuard::new(hb, BusyKind::DirScan);
            crate::wab::scan_unconfirmed_sealed(
                &config.wab_dir,
                None,
                metrics,
                exclude,
                config.orphan_cleanup,
            )
        };
        match scan {
            Ok(crate::wab::SealedScan {
                segments: sealed,
                incomplete,
            }) => {
                // Dedup against what's already queued via a HashSet membership view
                // (O(n+m)) rather than VecDeque::contains per segment (O(n·m)) —
                // matters when a long outage strands many segments (F-rescan). The
                // caller's in-flight segment (if any) is folded into the same view
                // so it is never re-queued from under the arm resuming it (M2).
                let mut already: std::collections::HashSet<&Path> =
                    pending.iter().map(PathBuf::as_path).collect();
                if let Some(seg) = in_flight {
                    already.insert(seg);
                }
                let fresh: Vec<PathBuf> = sealed
                    .into_iter()
                    .filter(|seg| !already.contains(seg.as_path()))
                    .collect();
                let mut resumed = 0u64;
                for seg in fresh {
                    pending.push_back(seg);
                    resumed += 1;
                }
                if resumed > 0 {
                    metrics.drain_segments_resumed.inc_by(resumed);
                    info!(
                        resumed,
                        "drain: sink recovered; re-queued stranded segment(s) for delivery"
                    );
                }
                // A shard could not be listed (already logged by the scan): the
                // readable shards are resumed above, and the edge is held live
                // exactly as for a failed scan (H1, below), so the next poll
                // scans again and picks up the skipped shard once it reads.
                if incomplete {
                    return prev_health_ok;
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "drain: sink recovered but the stranded-segment rescan failed; will retry on the next recovery edge"
                );
                // Keep the recovery edge LIVE: return the caller's previous
                // health-ok state (still `false` here, since we only reach this
                // arm on a down→up edge) rather than `now_ok`. If we returned
                // `now_ok` (true), `prev_health_ok` would flip true and the
                // `now_ok && !prev_health_ok` edge would never fire again, so a
                // transient rescan failure would strand segments until restart —
                // contradicting the "will retry on the next recovery edge"
                // promise (H1). The gauge above already reflects the healthy
                // probe; only the edge-tracking state is held back.
                return prev_health_ok;
            }
        }
    }
    now_ok
}

// ── Heartbeat (liveness observed from outside the drain's own runtime) ────────

/// A monotonic counter the drain advances whenever it is alive.
///
/// Bumped at the top of the outer loop, inside every deliberate wait (the
/// retry backoff and the dead-letter-full block), after every sink commit and
/// every [`READ_HEARTBEAT_EVERY`] records read, because a drain that is merely
/// WAITING, or working through one large segment, is not wedged. A single
/// retry backoff can legitimately run to `MAX_RETRY_DELAY` (300 s), and the
/// blocked arm parks indefinitely — both far past the stall warning's
/// `health_poll_interval + 15 s` threshold — so bumping only at the loop head
/// would report a slow-but-healthy sink as a blocked OS thread.
///
/// The drain runs on a current-thread runtime, so a sink that BLOCKS the
/// thread (a blocking DB driver, `std::fs`, a spin loop) defeats both
/// `tokio::time::timeout` backstops — `commit_timeout` and
/// `health_probe_timeout` can only fire if the awaited future yields.
/// Measured: `commit_timeout` = 50 ms against a 1.2 s blocking commit elapsed
/// 1.214 s and still confirmed the batch; a 50 ms health-probe timeout
/// against a 1.5 s blocking health elapsed 1.504 s and returned healthy.
/// Liveness therefore cannot be observed from inside the drain — this counter
/// lets a task on another runtime (see `main.rs`'s WAB-growth poll) see it.
///
/// Deliberately NOT a fix: the drain still wedges forever against a sink that
/// never yields. This only makes that state visible in the logs instead of
/// silent. `drain_state` is untouched by this — see the callers.
///
/// # Busy signal (spec §4.8)
///
/// A frozen counter alone cannot tell a wedge from a drain that is merely
/// inside one long operation: a 60 s sink commit that yields (and that
/// `commit_timeout` can still cancel), or a sidecar fsync on a disk that
/// stalls for a minute. Both used to read as "wedged, restart it", and a
/// restart re-delivers from record 0 and fixes neither. So the counter is
/// bumped after every sink commit and every [`READ_HEARTBEAT_EVERY`] records
/// read, and every long operation runs under a [`BusyGuard`] naming what it
/// is ([`BusyKind`]) and since when. The poll task in `main.rs` reads both
/// ([`DrainHeartbeat::busy`]) to tell a slow sink call or slow local I/O from
/// a genuinely wedged thread.
///
/// Every write is a wait-free `Relaxed` store on the drain thread, and nothing
/// here is shared with the ack path.
#[derive(Clone, Default)]
pub(crate) struct DrainHeartbeat(std::sync::Arc<HeartbeatCells>);

#[derive(Default)]
struct HeartbeatCells {
    beats: std::sync::atomic::AtomicU64,
    /// When the current operation started: milliseconds since
    /// [`busy_clock_ms`]'s process epoch, plus 1. Zero means idle.
    busy_since: std::sync::atomic::AtomicU64,
    /// The current operation, a [`BusyKind`] discriminant. Zero means idle.
    busy_kind: std::sync::atomic::AtomicU8,
}

impl DrainHeartbeat {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn bump(&self) {
        self.0
            .beats
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.beats.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// What the drain is busy doing and for how long, or `None` between
    /// operations. Read from another thread: the two atomics are loaded
    /// separately, so a reading that straddles a transition may pair one
    /// operation's kind with its neighbour's start — off by one operation at
    /// worst, which a poll every 5 s cannot tell apart anyway.
    pub(crate) fn busy(&self) -> Option<(BusyKind, Duration)> {
        use std::sync::atomic::Ordering::Relaxed;
        let since = self.0.busy_since.load(Relaxed);
        if since == 0 {
            return None;
        }
        let kind = BusyKind::from_u8(self.0.busy_kind.load(Relaxed))?;
        let elapsed_ms = busy_clock_ms().saturating_sub(since - 1);
        Some((kind, Duration::from_millis(elapsed_ms)))
    }

    fn set_busy(&self, kind: BusyKind) {
        use std::sync::atomic::Ordering::Relaxed;
        self.0.busy_kind.store(kind as u8, Relaxed);
        self.0
            .busy_since
            .store(busy_clock_ms().saturating_add(1), Relaxed);
    }

    fn clear_busy(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.0.busy_since.store(0, Relaxed);
        self.0.busy_kind.store(0, Relaxed);
    }

    fn busy_kind(&self) -> Option<BusyKind> {
        BusyKind::from_u8(self.0.busy_kind.load(std::sync::atomic::Ordering::Relaxed))
    }
}

/// Milliseconds since a process-wide epoch, the clock of
/// [`DrainHeartbeat`]'s `busy_since`. Monotonic (an `Instant`), so a wall-clock
/// step cannot make an operation look minutes long or negative.
fn busy_clock_ms() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let ms = EPOCH.get_or_init(Instant::now).elapsed().as_millis();
    u64::try_from(ms).unwrap_or(u64::MAX)
}

/// How often the reader loop bumps the heartbeat, in records read. A resumed
/// attempt re-walks its whole durable prefix (G13), up to a 256 MiB segment of
/// small records, so reads bump it as well as commits.
pub(crate) const READ_HEARTBEAT_EVERY: u64 = 1024;

/// What a [`BusyGuard`] says the drain thread is doing. The first two are
/// calls into the sink, which `commit_timeout` / `health_probe_timeout` bound
/// when the sink yields; the rest are local I/O on the WAB device, which
/// nothing bounds and a restart does not speed up.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BusyKind {
    SinkCommit = 1,
    SinkProbe = 2,
    SegmentRead = 3,
    LocalFsync = 4,
    DeadLetterWrite = 5,
    QuarantineMove = 6,
    /// A directory walk: the stranded-segment rescan, the dead-letter rescan
    /// or the quarantine size walk. The rescan's orphan-file cleanup fsync
    /// (`wab::scan_unconfirmed_sealed`) also reads as this kind rather than
    /// `LocalFsync`: it runs inside the walk, and both are local I/O, so the
    /// verdict is the same.
    DirScan = 7,
}

impl BusyKind {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Self::SinkCommit,
            2 => Self::SinkProbe,
            3 => Self::SegmentRead,
            4 => Self::LocalFsync,
            5 => Self::DeadLetterWrite,
            6 => Self::QuarantineMove,
            7 => Self::DirScan,
            _ => return None,
        })
    }

    /// True for a call into the sink, false for local I/O.
    pub(crate) fn is_sink_call(self) -> bool {
        matches!(self, Self::SinkCommit | Self::SinkProbe)
    }

    /// The `weir_drain_busy_seconds{kind}` label value.
    pub(crate) fn metric_label(self) -> crate::metrics::DrainBusyKind {
        use crate::metrics::DrainBusyKind as L;
        match self {
            Self::SinkCommit => L::sink_commit,
            Self::SinkProbe => L::sink_probe,
            Self::SegmentRead => L::segment_read,
            Self::LocalFsync => L::local_fsync,
            Self::DeadLetterWrite => L::dead_letter_write,
            Self::QuarantineMove => L::quarantine_move,
            Self::DirScan => L::dir_scan,
        }
    }
}

/// Marks the drain thread busy with one [`BusyKind`] for the guard's lifetime.
///
/// Set on `new`, cleared on drop — including a drop during a panic unwind, so a
/// panicking sink or disk helper cannot leave a respawned drain reporting the
/// dead one's operation. Guards nest: the reader loop's `SegmentRead` guard
/// holds a `SinkCommit` guard around each commit, and dropping the inner guard
/// hands the signal back to the outer kind, timed afresh from that moment (the
/// question the verdict asks is how long the CURRENT stretch has run).
pub(crate) struct BusyGuard<'a> {
    hb: &'a DrainHeartbeat,
    outer: Option<BusyKind>,
}

impl<'a> BusyGuard<'a> {
    pub(crate) fn new(hb: &'a DrainHeartbeat, kind: BusyKind) -> Self {
        let outer = hb.busy_kind();
        hb.set_busy(kind);
        Self { hb, outer }
    }
}

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        match self.outer {
            Some(kind) => self.hb.set_busy(kind),
            None => self.hb.clear_busy(),
        }
    }
}

/// True when the heartbeat has not moved between two observations.
///
/// A single unchanged pair is not on its own proof of a wedge — see
/// `main.rs`'s poll task, which requires this to hold for several
/// consecutive polls (and past the drain's own `health_poll_interval`, since
/// an idle-but-healthy drain only bumps the heartbeat that often) before it
/// warns. Nor is a long freeze on its own proof of a WEDGE specifically: a
/// drain that has exited leaves the heartbeat frozen forever too, which is why
/// the poll task also reads `drain_state` (`main.rs`'s `drain_stall_verdict`).
pub(crate) fn drain_appears_stalled(previous: u64, current: u64) -> bool {
    previous == current
}

// ── Drain thread ──────────────────────────────────────────────────────────────

/// Total bytes currently held in `<wab_dir>/quarantine`: forensic copies of
/// corrupt WAB segments preserved by crash recovery (see `wab::recovery`)
/// because acked records may sit after the corruption.
///
/// Same shape as `dead_letter`'s directory-sizing scan and `main.rs`'s
/// `compute_wab_bytes_on_disk`: walk the directory, sum regular-file sizes,
/// and read a missing directory as `0` rather than an error. An absent
/// quarantine dir is the normal, healthy case — nothing has ever been
/// quarantined — and must not be surfaced as a scan failure.
///
/// Refreshed on the same wall-clock health-poll cadence as
/// `dead_letter_bytes_on_disk` (see the call site below), so the gauge is a
/// periodic snapshot, not a live value — same staleness bound as
/// `weir_wab_bytes_on_disk`.
fn quarantine_bytes_on_disk(wab_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(wab_dir.join("quarantine")) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        if let Ok(meta) = entry.metadata()
            && meta.is_file()
        {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

fn drain_thread<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
    // False on every respawn. A panic takes `pending` with it, and the only
    // in-run rescan is the `now_ok && !prev_health_ok` edge — starting at
    // `true` with a healthy sink means that edge never fires again and the
    // backlog waits for a process restart. Starting at `false` makes the
    // first healthy probe an edge, which re-queues everything still on disk.
    initial_health_ok: bool,
    // Bumped once per outer-loop iteration below, and again on every 50 ms
    // slice of the two deliberate waits (retry backoff, dead-letter-full
    // block) so that waiting-but-alive still reads as alive. See
    // `DrainHeartbeat` for why this exists: it's the only liveness signal that
    // survives a sink call that blocks this thread outright rather than
    // yielding.
    heartbeat: DrainHeartbeat,
    // Owned by `run_drain_supervised` so it outlives a panic; see `ledger`.
    ledger: &mut SegmentLedger,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("drain tokio runtime");

    let mut dead_letter = match DeadLetterWriter::open(&config.wab_dir) {
        Ok(dl) => dl,
        Err(e) => {
            // NOT a bare `return`. run_drain_supervised reads a normal return
            // as "the channel closed, delivery is finished" — so it neither
            // respawns nor counts a panic, and the daemon goes on acking
            // producers while nothing is ever delivered. Panicking routes this
            // into the supervisor, which retries and, if it is persistent,
            // exhausts max_respawns and logs the loud "delivery is stopped"
            // error an operator can act on. A transient ENOENT inside scan_dir
            // (a file vanishing between read_dir and stat) is enough to reach
            // this path.
            error!(error = %e, "drain: failed to open dead-letter writer");
            panic!("drain: dead-letter writer unavailable: {e}");
        }
    };

    let mut state = DrainState::Draining;
    let mut pending: VecDeque<PathBuf> = VecDeque::new();

    // The start of the current dead-letter-full blocking episode, if any. An
    // episode spans a flapping cap (unblock → reblock the same segment without
    // intervening progress): `enter_blocked` increments `dead_letter_full` and
    // stamps `blocked_since` only when this is `None`, and reuses the stored
    // instant otherwise, so a flap counts as ONE episode and the blocked-duration
    // gauge keeps climbing rather than resetting. Cleared (and the gauge reset)
    // when a segment is actually confirmed, quarantined or parked — the drain
    // moved past a segment for good this run, which is genuine progress (F09).
    let mut block_episode: Option<Instant> = None;

    // Tracks the sink's last-observed health so we can detect a down→up recovery
    // edge and re-drain stranded segments. On a fresh start this is `true`
    // (startup replay already handled any pre-existing unconfirmed segments, so
    // the first healthy probe is not a recovery); on a respawn it is `false` (see
    // `initial_health_ok`), so the first healthy probe IS treated as a recovery
    // edge and rescans the disk for whatever `pending` lost to the panic. Set
    // `false` again whenever a segment strands.
    let mut prev_health_ok = initial_health_ok;

    // Wall-clock anchor for the health-poll + stranded-segment rescan. Driving it
    // off elapsed time (not channel idleness) means recovery fires even under
    // sustained ingest — a busy drain channel can no longer starve it (the idle
    // recv_timeout branch alone never fired while segments kept arriving).
    let mut last_health_poll = Instant::now();

    // Total time the drain may still spend on clamped transient-retry backoffs
    // after shutdown has been observed. Consumed by the `RetryingTransient` arm;
    // once exhausted, shutdown retries revert to firing immediately. See
    // [`SHUTDOWN_RETRY_BACKOFF_BUDGET`] for why this is bounded in aggregate and
    // not just per-retry.
    let mut shutdown_backoff_left = SHUTDOWN_RETRY_BACKOFF_BUDGET;

    'outer: loop {
        // See `DrainHeartbeat`: bumped unconditionally on every pass through
        // this loop, whatever state does or doesn't happen below — a sink
        // call that blocks this thread (rather than yielding) is exactly the
        // case that stops this line from ever running again. The two arms that
        // deliberately park for a long time bump it from inside their own wait
        // loops as well, so only a genuinely blocked thread freezes it.
        heartbeat.bump();
        // Every ledger mutation happens inside an arm below and is followed by
        // another pass through here, so publishing at the loop head keeps the
        // gauges current without threading `metrics` into the ledger.
        publish_ledger_counts(ledger.counts(), &metrics);
        state = match state {
            // ── Draining ─────────────────────────────────────────────────────
            DrainState::Draining => {
                set_drain_state(&metrics, DrainStateValue::draining);

                // Wall-clock health poll + stranded-segment rescan. Runs on the
                // `health_poll_interval` cadence regardless of channel activity, so
                // a recovered sink re-drains its stranded backlog even while ingest
                // is saturating the drain channel (the old idle-only poll never
                // fired under sustained load). `pending` is drained in submission
                // order, so re-queued stranded segments interleave with new ones.
                //
                // Order (spec §4.0): outstanding dispositions first — they never
                // deliver, and finishing one can change what the rescan may
                // pick up — then the probe + rescan, then the gauges.
                if last_health_poll.elapsed() >= config.health_poll_interval {
                    ledger.retry_dispositions(&config.wab_dir, &metrics, &heartbeat);
                    let now = Instant::now();
                    prev_health_ok = probe_and_resume_stranded(
                        &rt,
                        &*sink,
                        &config,
                        &metrics,
                        &mut pending,
                        prev_health_ok,
                        None,
                        &|p| ledger.excludes(p, now),
                        &heartbeat,
                    );
                    // Refresh the dead-letter size gauge too, so an out-of-band
                    // `weir-ctl dl drop` (an operator deleting files while the
                    // daemon runs) is reflected without waiting for the blocked
                    // state or a restart.
                    let rescanned = {
                        let _busy = BusyGuard::new(&heartbeat, BusyKind::DirScan);
                        dead_letter.rescan()
                    };
                    if rescanned.is_ok() {
                        metrics
                            .dead_letter_bytes_on_disk
                            .set(dead_letter.total_bytes() as f64);
                    }
                    // Same idea for quarantine: an operator can run `weir-ctl
                    // quarantine requeue` (or manually clear the dir) while the
                    // daemon is up, and the gauge should catch up on this same
                    // health-poll cadence rather than only at the next restart.
                    let quarantined = {
                        let _busy = BusyGuard::new(&heartbeat, BusyKind::DirScan);
                        quarantine_bytes_on_disk(&config.wab_dir)
                    };
                    metrics.quarantine_bytes_on_disk.set(quarantined as f64);
                    last_health_poll = Instant::now();
                }

                // Both sources pass the ledger's admission check (spec §4.0):
                // the channel is not enumerable, so a path can sit in it AND in
                // `pending` (re-queued by a rescan, or buffered by one of the
                // waits below). A copy the ledger owns is dropped here; the
                // rescan is its way back once it becomes admissible.
                let next_segment = loop {
                    if let Some(p) = pending.pop_front() {
                        match ledger.admit(&p, Instant::now()) {
                            Admit::Process { skip } => break Some((p, skip)),
                            Admit::Drop(owner) => {
                                debug!(path = %p.display(), owner, "drain: not admitting queued segment");
                                continue;
                            }
                        }
                    }
                    // Idle: wait for a new segment, but no longer than the time
                    // left until the next health poll, then re-enter Draining so
                    // the wall-clock poll above runs. Channel closure (all WAB
                    // flushers exited) still ends the loop.
                    let until_next_poll = config
                        .health_poll_interval
                        .saturating_sub(last_health_poll.elapsed())
                        .max(Duration::from_millis(1));
                    match drain_rx.recv_timeout(until_next_poll) {
                        Ok(p) => match ledger.admit(&p, Instant::now()) {
                            Admit::Process { skip } => break Some((p, skip)),
                            Admit::Drop(owner) => {
                                debug!(path = %p.display(), owner, "drain: not admitting segment from the channel");
                                break None;
                            }
                        },
                        Err(RecvTimeoutError::Timeout) => break None,
                        Err(RecvTimeoutError::Disconnected) => break 'outer,
                    }
                };

                match next_segment {
                    // Idle timeout with nothing queued (or a dropped channel
                    // copy): re-enter Draining so the wall-clock health poll
                    // above runs on schedule.
                    None => DrainState::Draining,
                    Some((segment, skip)) => {
                        // A fresh segment starts at 0; one the ledger holds a
                        // cursor for (it stranded earlier in this run) resumes
                        // past its durable prefix. The health gauge + rescan are
                        // refreshed by the wall-clock poll above, so we no longer
                        // probe the sink after every segment (which, under
                        // sustained load, meant a HEAD request per segment).
                        let result = attempt_segment(
                            &rt,
                            &segment,
                            &*sink,
                            &config,
                            &metrics,
                            &mut dead_letter,
                            ledger,
                            skip,
                            &heartbeat,
                        );
                        transition_from_draining(
                            segment,
                            result,
                            &config,
                            &metrics,
                            &mut block_episode,
                            ledger,
                            &heartbeat,
                        )
                    }
                }
            }

            // ── RetryingTransient, budget spent: strand now ───────────────────
            //
            // Checked BEFORE any wait (spec §4.6). The arm below used to serve
            // the whole backoff, rescan, and only then notice the budget was
            // gone, so every strand ended with a dead wait on the single drain
            // thread: `8 × base` at the defaults, up to `MAX_RETRY_DELAY`
            // (300 s) under a server's Retry-After, while every other segment
            // queued behind it.
            //
            // The wait is not dropped, it moves off the thread: the delay this
            // attempt would have served becomes the segment's `not_before`.
            // Stranding alone would let the next health poll re-queue it at
            // once (a sink strand marks the sink down, and a healthy probe is a
            // recovery edge), and a copy of the path still in the channel would
            // skip the rescan altogether — either one violating a Retry-After.
            // Admission and the rescan exclusion both honour `not_before`, so
            // the segment is held off until then while other segments move.
            DrainState::RetryingTransient {
                segment,
                retries_left: 0,
                next_delay,
                processed,
                cause,
            } => {
                ledger.set_not_before(&segment, Instant::now() + next_delay);
                // The `processed` cursor SURVIVES the strand (spec §4.0,
                // revising H2): every transient result already recorded it
                // in the ledger, pinned to the file's (dev, ino), so when the
                // rescan re-queues this path, admission resumes it past the
                // committed prefix instead of re-delivering it. It is in
                // memory only — a restart still replays the whole segment,
                // the at-least-once trade-off the SinkSDK mandates for crash
                // recovery (sinks must dedupe).
                //
                // The bookkeeping depends on WHAT failed (D11).
                match cause {
                    TransientCause::Sink => {
                        metrics.drain_segments_stranded.inc();
                        // The sink is failing; mark it down so the next
                        // healthy probe registers as a down→up recovery edge
                        // and re-drains this stranded segment without
                        // waiting for a restart.
                        prev_health_ok = false;
                        error!(
                            path = %segment.display(),
                            processed,
                            cool_down = ?next_delay,
                            "drain: max retries exhausted; segment left on disk (stranded) — \
                             weir_drain_segments_stranded_total incremented; re-drained \
                             automatically once its cool-down has passed and the sink answers \
                             (resuming past the records already delivered), or on restart (from the start — duplicates \
                             handled by the sink's idempotency)"
                        );
                    }
                    // A local cause says nothing about the sink: do not fake
                    // a sink-down edge, and keep it out of the sink-oriented
                    // stranded counter. The idle rescan resumes it.
                    TransientCause::LocalRead { class, errno } => {
                        metrics
                            .drain_local_io_strands
                            .get_or_create(&LocalIoStrandLabel {
                                cause: LocalIoStrandCause::read,
                            })
                            .inc();
                        error!(
                            path = %segment.display(),
                            ?class,
                            errno,
                            processed,
                            cool_down = ?next_delay,
                            "drain: max retries exhausted reading the segment; left on disk — \
                             weir_drain_local_io_strands_total{{cause=\"read\"}} incremented; \
                             retried by the idle rescan. Check the WAB device, not the sink"
                        );
                    }
                    TransientCause::LocalWrite => {
                        metrics
                            .drain_local_io_strands
                            .get_or_create(&LocalIoStrandLabel {
                                cause: LocalIoStrandCause::write,
                            })
                            .inc();
                        error!(
                            path = %segment.display(),
                            processed,
                            cool_down = ?next_delay,
                            "drain: max retries exhausted writing dead-letter records; segment \
                             left on disk — weir_drain_local_io_strands_total{{cause=\"write\"}} \
                             incremented; retried by the idle rescan. Check the dead_letter/ \
                             device (space, permissions), not the sink"
                        );
                    }
                }

                DrainState::Draining
            }

            // ── RetryingTransient ─────────────────────────────────────────────
            // `retries_left >= 1` here; a spent budget matched the arm above.
            // The previous attempt's cause only matters to a strand, so the
            // retry ignores it and the attempt below reports its own.
            DrainState::RetryingTransient {
                segment,
                retries_left,
                next_delay,
                processed,
                cause: _,
            } => {
                set_drain_state(&metrics, DrainStateValue::retrying_transient);

                // Interruptible backoff: consume next_delay in short slices via
                // recv_timeout so a shutdown — which the drain only learns of as the
                // drain channel disconnecting (all WAB flushers have exited) — cuts
                // the wait short instead of sleeping out a multi-minute Retry-After.
                // On disconnect we shorten the wait but STILL run the retry below
                // (its commit is bounded by commit_timeout), preserving the existing
                // "complete in-flight work on shutdown" behaviour; the Draining loop
                // then observes the same disconnect and exits. In production the
                // channel only closes on shutdown, so the Retry-After delay is only
                // ever cut at shutdown, never during normal operation. Segments that
                // arrive during the wait are buffered (S15).
                //
                // W2: the shutdown wait is CLAMPED, not eliminated. Breaking outright
                // made every remaining attempt fire back-to-back with zero delay —
                // the chaos harness measured four segments each burning all
                // MAX_RETRIES inside 0.4 ms before being logged as stranded. Three
                // attempts inside one instant against a sink that needs milliseconds
                // to answer fail identically for the same reason, so that spent the
                // whole budget on what was effectively one attempt and forced a
                // strand a brief delay might have avoided — neither "retry properly"
                // nor "don't retry". Sleeping the clamped remainder makes each retry
                // an independent sample again. `thread::sleep` rather than another
                // recv_timeout because a disconnected channel returns immediately and
                // would spin; nothing is missed, since no further segment can arrive.
                //
                // The clamp is drawn from a per-process budget so the added shutdown
                // time cannot compound: a segment that strands makes the Draining
                // arm's health poll re-queue it (the 4a auto-resume edge), which
                // starts another retry episode, and with an unbudgeted per-retry
                // clamp a HEAD-healthy / POST-transient-failing sink would keep that
                // cycle alive and shutdown would never finish. Once the budget is
                // spent the waits go to zero and the loop degenerates to the previous
                // behaviour, which terminates.
                let wait_end = Instant::now() + next_delay;
                loop {
                    // WAITING IS NOT WEDGED. `heartbeat.bump()` at the top of
                    // `'outer` is never reached while this arm holds the thread,
                    // and this single wait can legitimately run to
                    // `MAX_RETRY_DELAY` (300 s) on a `Retry-After`-honouring
                    // sink — far past the stall warning's
                    // `health_poll_interval + 15 s` threshold (45 s at the
                    // defaults). Without a bump here a merely-SLOW sink is
                    // reported as a drain "wedged inside a sink call that blocks
                    // its OS thread", which is the worst possible misdirection
                    // during an incident. Bumping on each 50 ms slice keeps the
                    // heartbeat meaning what it claims: only a thread that
                    // cannot reach its own loop at all freezes it.
                    heartbeat.bump();
                    let remaining = wait_end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let poll = remaining.min(Duration::from_millis(50));
                    match drain_rx.recv_timeout(poll) {
                        Ok(new_seg) => pending.push_back(new_seg),
                        Err(RecvTimeoutError::Timeout) => {}
                        // Channel closed (shutdown): serve the clamped remainder of
                        // the backoff, then finish the retry.
                        Err(RecvTimeoutError::Disconnected) => {
                            let left = wait_end
                                .saturating_duration_since(Instant::now())
                                .min(SHUTDOWN_RETRY_BACKOFF_CAP)
                                .min(shutdown_backoff_left);
                            shutdown_backoff_left -= left;
                            std::thread::sleep(left);
                            break;
                        }
                    }
                }

                // Rescan for OTHER stranded segments before the retry (M2). A long
                // backoff here would otherwise starve the Draining-arm rescan, so a
                // sink that recovers mid-backoff wouldn't re-drain segments stranded
                // by an earlier outage until this segment resolved. Excludes the
                // segment being retried (`segment`) — this arm processes it directly
                // below, so re-queuing it would double-process. The borrow ends
                // before `segment` is moved into process_segment / the next state.
                // Ledger-owned paths are skipped too (spec §4.0).
                let now = Instant::now();
                prev_health_ok = probe_and_resume_stranded(
                    &rt,
                    &*sink,
                    &config,
                    &metrics,
                    &mut pending,
                    prev_health_ok,
                    Some(&segment),
                    &|p| ledger.excludes(p, now),
                    &heartbeat,
                );

                // Resume past the sub-batches already durably processed (F05).
                let result = attempt_segment(
                    &rt,
                    &segment,
                    &*sink,
                    &config,
                    &metrics,
                    &mut dead_letter,
                    ledger,
                    processed,
                    &heartbeat,
                );
                // Subsequent failure: spend one retry and double the delay
                // (exponential backoff). retries_left is >= 1 here — a spent
                // budget is the arm above, which strands without waiting.
                next_state_after_process(
                    segment,
                    result,
                    &config,
                    &metrics,
                    &mut block_episode,
                    ledger,
                    &heartbeat,
                    |segment, retry_after, processed, cause| DrainState::RetryingTransient {
                        segment,
                        retries_left: retries_left - 1,
                        next_delay: next_retry_delay(next_delay * 2, retry_after),
                        processed,
                        cause,
                    },
                )
            }

            // ── BlockedDeadLetterFull ─────────────────────────────────────────
            DrainState::BlockedDeadLetterFull {
                segment,
                blocked_since,
                processed,
                needed,
            } => {
                set_drain_state(&metrics, DrainStateValue::blocked_dead_letter_full);
                metrics
                    .dead_letter_blocked_duration
                    .set(blocked_since.elapsed().as_secs_f64());

                // Drain new segments into the pending buffer while waiting.
                // Use recv_timeout in short slices to detect channel disconnection.
                let mut channel_closed = false;
                let check_end = Instant::now() + config.dead_letter_check_interval;
                loop {
                    // Same reasoning as the retry-backoff wait above: this arm
                    // parks for `dead_letter_check_interval` (30 s by default)
                    // and can re-park indefinitely while an operator frees
                    // headroom, all inside iterations of `'outer` whose head is
                    // never reached. A drain that is blocked and SAYING SO via
                    // `drain_state{blocked_dead_letter_full}` is alive; only a
                    // thread that cannot reach its own loop is wedged.
                    heartbeat.bump();
                    let remaining = check_end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let poll = remaining.min(Duration::from_millis(50));
                    match drain_rx.recv_timeout(poll) {
                        Ok(new_seg) => pending.push_back(new_seg),
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            channel_closed = true;
                            break;
                        }
                    }
                }

                // Rescan so external deletions (e.g. operator cleanup) are reflected.
                // A deleted dead_letter/ is recreated and counts as empty (spec
                // §4.5). Any other failed rescan leaves total_bytes stale at its
                // at-cap value, so the unblock check below can't clear and the
                // drain stays blocked even after the operator frees the dir —
                // surface it rather than swallowing (F08).
                let rescanned = {
                    let _busy = BusyGuard::new(&heartbeat, BusyKind::DirScan);
                    dead_letter.rescan()
                };
                if let Err(e) = rescanned {
                    warn!(error = %e, "drain: dead-letter rescan failed while blocked; total_bytes may be stale until the next successful rescan");
                }
                metrics
                    .dead_letter_bytes_on_disk
                    .set(dead_letter.total_bytes() as f64);

                // Refresh sink health every wake-cycle AND rescan for OTHER
                // stranded segments (M2). Operators monitoring weir_sink_health
                // need to know if the sink came back up while we were waiting on
                // dead-letter headroom; without this poll the gauge would be stuck
                // at whatever value the last segment commit produced. And because
                // this arm can park indefinitely (the cap stays full → loops back
                // to itself, line below), the wall-clock rescan that lives in the
                // Draining arm would otherwise be starved here: a sink that
                // recovers while we're parked would not re-drain segments stranded
                // by an *earlier* outage until the cap cleared. Reusing
                // probe_and_resume_stranded does the same gauge update plus the
                // down→up re-queue, excluding `segment` (this arm resumes it on its
                // own via the RetryingTransient transition below, so re-queuing it
                // would double-process). Ledger-owned paths are skipped too.
                let now = Instant::now();
                prev_health_ok = probe_and_resume_stranded(
                    &rt,
                    &*sink,
                    &config,
                    &metrics,
                    &mut pending,
                    prev_health_ok,
                    Some(&segment),
                    &|p| ledger.excludes(p, now),
                    &heartbeat,
                );
                // Unblock on the SAME predicate that blocked: the blocked write
                // fits. A bare `total < cap` unblocked while the total sat in
                // `[cap - needed, cap)`, so the retry blocked again at once —
                // and, before the carry, re-committed the sub-batch's accepted
                // records on every check interval (spec §4.5). A blocked state
                // always has `needed <= cap` (a write larger than the whole cap
                // is written anyway, see `dead_letter_records`), so this can
                // always clear.
                if !dead_letter.would_exceed_cap(needed, config.dead_letter_max_bytes) {
                    // Headroom available — retry the preserved segment, RESUMING
                    // past the sub-batches already durably processed (skip =
                    // processed) rather than from record 0, so earlier sub-batches
                    // aren't re-committed/re-dead-lettered (F05). The ledger holds
                    // the blocked sub-batch's rejects as a carry, so the retry
                    // writes them first and does not re-commit that sub-batch
                    // either. Route through
                    // RetryingTransient — the state that carries `processed`; fresh
                    // segments still arrive via pending/the channel and are picked
                    // up by Draining.
                    //
                    // Seed next_delay with base_retry_delay (NOT zero): the retry
                    // fires after the base delay, and crucially, if it then hits a
                    // transient SINK error, next_state_after_process doubles a
                    // non-zero base into real exponential backoff — a zero seed
                    // would double to zero and busy-loop with no backoff (G12).
                    //
                    // Do NOT clear the episode or reset the duration gauge here: if
                    // the retry immediately re-blocks (a flapping cap) it's the same
                    // episode and the duration must keep climbing. The gauge resets
                    // only when a segment is actually confirmed/quarantined — see
                    // next_state_after_process (F09).
                    DrainState::RetryingTransient {
                        segment,
                        retries_left: config.max_retries,
                        next_delay: config.base_retry_delay,
                        processed,
                        cause: TransientCause::Sink,
                    }
                } else if channel_closed {
                    // No headroom and shutdown requested — leave segment unconfirmed.
                    // Crash recovery will replay it on next start.
                    info!(
                        path = %segment.display(),
                        "drain: shutdown while blocked; segment not confirmed"
                    );
                    break 'outer;
                } else {
                    DrainState::BlockedDeadLetterFull {
                        segment,
                        blocked_since,
                        processed,
                        needed,
                    }
                }
            }
        };
    }

    // One best-effort pass over outstanding dispositions on the way out: a
    // sidecar or quarantine move that can complete now saves a re-delivery on
    // the next start. It never delivers.
    ledger.retry_dispositions(&config.wab_dir, &metrics, &heartbeat);
    publish_ledger_counts(ledger.counts(), &metrics);
    info!("drain thread exiting");
}

/// Maps a [`ProcessResult`] to the next [`DrainState`]. The three terminal
/// outcomes are identical whether we arrived from a first attempt or a retry —
/// `Confirmed` confirms+deletes and moves on, `BlockedDeadLetter` enters the
/// blocked state, `Quarantined` (handled inside `process_segment`) moves on, and
/// so does `QuarantinePending`, whose move the ledger retries.
/// Only the `Transient → RetryingTransient` backoff differs between the two
/// callers (fresh budget vs. decremented/exponential), so each supplies it via
/// `on_transient`. This is the single source of truth for the non-retry
/// transitions — see the `Draining` and `RetryingTransient` arms in `run`.
///
/// It also keeps the ledger's per-segment progress in step (spec §4.0): a
/// transient or blocked result records the durable cursor, which survives a
/// strand; a confirmed or quarantined segment is forgotten.
#[allow(clippy::too_many_arguments)]
fn next_state_after_process(
    segment: PathBuf,
    result: ProcessResult,
    config: &DrainConfig,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
    ledger: &mut SegmentLedger,
    hb: &DrainHeartbeat,
    on_transient: impl FnOnce(PathBuf, Option<Duration>, u64, TransientCause) -> DrainState,
) -> DrainState {
    match result {
        ProcessResult::Confirmed { record_count } => {
            // A confirm that did not finish is owned by the ledger from here:
            // the polls retry only the outstanding step, and neither the
            // channel nor the rescan delivers the segment again (spec §4.3).
            let outcome = {
                let _busy = BusyGuard::new(hb, BusyKind::LocalFsync);
                confirm_and_delete(&segment, record_count, metrics)
            };
            ledger.forget(&segment);
            ledger.note_confirm_outcome(&segment, outcome, record_count);
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
        ProcessResult::Transient {
            retry_after,
            processed,
            cause,
            carry,
        } => {
            if let TransientCause::LocalRead { class, errno } = cause
                && let Some(next) = escalate_local_read(
                    &segment,
                    class,
                    errno,
                    config,
                    metrics,
                    block_episode,
                    ledger,
                    hb,
                )
            {
                return next;
            }
            ledger.record_progress(&segment, processed);
            store_dl_carry(&segment, carry, ledger);
            on_transient(segment, retry_after, processed, cause)
        }
        ProcessResult::BlockedDeadLetter {
            processed,
            needed,
            carry,
        } => {
            ledger.record_progress(&segment, processed);
            store_dl_carry(&segment, carry, ledger);
            enter_blocked(segment, metrics, block_episode, processed, needed)
        }
        // Not deliverable by this binary, not corrupt: set aside for the run.
        ProcessResult::Parked { reason } => {
            ledger.park(&segment, reason);
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
        // Segment was quarantined inside process_segment — move on.
        ProcessResult::Quarantined => {
            ledger.forget(&segment);
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
        // The quarantine move failed. The ledger owns the segment from here:
        // admission drops every copy of it, the rescan skips it, and the polls
        // retry only the move. Its readable prefix was delivered once and is
        // never delivered again this run (spec §4.4).
        ProcessResult::QuarantinePending { reason } => {
            ledger.note_quarantine_pending(&segment, reason);
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
    }
}

/// Hands a dead-letter carry to the ledger. A carry the ledger refuses (over
/// [`ledger::DL_CARRY_MAX_BYTES`] in total) is dropped here, and the segment
/// falls back to re-committing that sub-batch once on its next attempt: its
/// cursor still ends before it. That costs one duplicate commit, never a record.
fn store_dl_carry(segment: &Path, carry: Option<DeadLetterCarry>, ledger: &mut SegmentLedger) {
    let Some(carry) = carry else { return };
    let (records, bytes) = (carry.payloads.len(), carry.bytes);
    if !ledger.set_dl_carry(segment, carry) {
        warn!(
            path = %segment.display(),
            records,
            bytes,
            max_bytes = ledger::DL_CARRY_MAX_BYTES,
            "drain: cannot hold this sub-batch's dead-lettered records in memory (carry bound \
             reached, or the segment cannot be stat'ed); the sub-batch will be re-committed once \
             when the segment is retried"
        );
    }
}

/// One attempt at `segment`, resuming past its first `skip` records.
///
/// A dead-letter write an earlier attempt still owes (the ledger's carry) is
/// settled first: written, then the attempt resumes past the sub-batch it came
/// from, so the sink never sees that sub-batch again (spec §4.5, §4.11). If the
/// write blocks or fails again, the carry goes back to the ledger with the
/// result and nothing is sent to the sink.
///
/// The whole attempt runs with `segment` marked in flight in the ledger, so a
/// panic anywhere inside it (a sink, the reader, the dead-letter writer) is
/// charged to this segment by the supervisor, which parks it at the second
/// (spec §4.12). The mark is cleared only on a normal return; an unwinding
/// panic leaves it for the supervisor to read.
#[allow(clippy::too_many_arguments)]
fn attempt_segment<S: Sink>(
    rt: &tokio::runtime::Runtime,
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    ledger: &mut SegmentLedger,
    skip: u64,
    hb: &DrainHeartbeat,
) -> ProcessResult {
    ledger.begin_attempt(segment);
    let result = 'attempt: {
        // `skip` may come from a state rather than from admission; either
        // way, a cursor measured on a file since replaced does not apply.
        let skip = ledger.checked_skip(segment, skip);
        let skip = match ledger.take_dl_carry(segment) {
            None => skip,
            Some(carry) => {
                match settle_dl_carry(segment, skip, carry, dead_letter, config, metrics, hb) {
                    Ok(through) => through,
                    Err(result) => break 'attempt result,
                }
            }
        };
        let last_chance = local_read_escalates_next(segment, config, ledger);
        rt.block_on(process_segment(
            segment,
            sink,
            config,
            metrics,
            dead_letter,
            skip,
            last_chance,
            hb,
        ))
    };
    ledger.end_attempt();
    result
}

/// Writes a carried sub-batch's rejected records. On success returns the record
/// to resume from (the end of that sub-batch) and counts the sub-batch's
/// accepted records as committed — the count its commit deferred (S24). On a
/// block or a failed write returns the result to report, carry included, with
/// `processed` unchanged.
fn settle_dl_carry(
    segment: &Path,
    skip: u64,
    carry: DeadLetterCarry,
    dead_letter: &mut DeadLetterWriter,
    config: &DrainConfig,
    metrics: &Metrics,
    hb: &DrainHeartbeat,
) -> Result<u64, ProcessResult> {
    match dead_letter_records(&carry.payloads, dead_letter, config, metrics, hb) {
        Ok(()) => {
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::committed,
                })
                .inc_by(carry.committed);
            info!(
                path = %segment.display(),
                dead_lettered = carry.payloads.len(),
                resume_from = carry.through,
                "drain: wrote the dead-letter records a previous attempt owed; resuming without \
                 re-committing their sub-batch"
            );
            Ok(carry.through.max(skip))
        }
        Err(DeadLetterFailure::Blocked { needed }) => Err(ProcessResult::BlockedDeadLetter {
            processed: skip,
            needed,
            carry: Some(carry),
        }),
        Err(DeadLetterFailure::WriteFailed) => Err(ProcessResult::Transient {
            retry_after: None,
            processed: skip,
            cause: TransientCause::LocalWrite,
            carry: Some(carry),
        }),
    }
}

/// Counts one local read failure of `segment` toward its escalation and, once
/// the count reaches `(max_retries + 1) × LOCAL_READ_STRAND_LIMIT` (three strand
/// cycles) AND the segment has been failing for the escalation min age
/// ([`local_read_escalation_due`]), escalates it (spec §4.1): an `Access` error
/// parks the segment as `local_read_failed`, a `Media` error quarantines it.
/// Returns the next state when it escalated, `None` to carry on retrying.
///
/// `Process`-class errors are daemon-wide (descriptors, memory), not a property
/// of the segment: they never count and never escalate, so a segment under fd
/// pressure keeps cycling exactly as before and heals when the pressure clears.
#[allow(clippy::too_many_arguments)]
fn escalate_local_read(
    segment: &Path,
    class: ledger::ErrClass,
    errno: Option<i32>,
    config: &DrainConfig,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
    ledger: &mut SegmentLedger,
    hb: &DrainHeartbeat,
) -> Option<DrainState> {
    let failures = ledger.note_local_read_failure(segment, class);
    if !local_read_escalation_due(segment, failures, config, ledger) {
        return None;
    }
    match class {
        ledger::ErrClass::Process => return None,
        ledger::ErrClass::Access => {
            error!(
                path = %segment.display(),
                errno,
                failures,
                failing_for = ?ledger.local_read_failing_for(segment),
                "drain: segment still unreadable (access) after {LOCAL_READ_STRAND_LIMIT} strand \
                 cycles and the escalation min age; parked (left in place, not retried this run) — \
                 weir_drain_segments_parked{{reason=\"local_read_failed\"}}; fix its permissions or \
                 type, it replays on the next start"
            );
            ledger.park(segment, ledger::ParkReason::LocalReadFailed);
        }
        ledger::ErrClass::Media => {
            error!(
                path = %segment.display(),
                errno,
                failures,
                failing_for = ?ledger.local_read_failing_for(segment),
                "drain: segment still unreadable (media) after {LOCAL_READ_STRAND_LIMIT} strand \
                 cycles and the escalation min age; quarantining"
            );
            // A failed move hands the segment to `quarantine_pending` rather
            // than forgetting it: forgotten, it would be re-opened for three
            // more strand cycles before the move was even tried again.
            const REASON: &str = "drain: persistent local read error";
            if quarantine_segment(segment, config, metrics, REASON, hb) {
                ledger.forget(segment);
            } else {
                ledger.note_quarantine_pending(segment, REASON);
            }
        }
    }
    end_block_episode(block_episode, metrics);
    Some(DrainState::Draining)
}

/// The local read failure count at which a segment escalates:
/// `(max_retries + 1) × LOCAL_READ_STRAND_LIMIT`, i.e. three strand cycles.
fn local_read_limit(config: &DrainConfig) -> u32 {
    config
        .max_retries
        .saturating_add(1)
        .saturating_mul(LOCAL_READ_STRAND_LIMIT)
}

/// True when a segment with `failures` segment-class local read failures is
/// due for escalation: the count has reached [`local_read_limit`] AND the
/// segment had been failing for at least `local_read_escalation_min_age`
/// ([`LOCAL_READ_ESCALATION_MIN_AGE`] in production) when the latest attempt
/// began. Both, so neither a fast burn through the retries nor a slow trickle
/// of failures alone sets a segment aside (conf-regressions-8).
fn local_read_escalation_due(
    segment: &Path,
    failures: u32,
    config: &DrainConfig,
    ledger: &SegmentLedger,
) -> bool {
    failures >= local_read_limit(config)
        && ledger.local_read_failing_for(segment) >= config.local_read_escalation_min_age
}

/// True when one more segment-class local read failure of `segment` would
/// escalate it — the `last_local_read_chance` of [`process_segment`]. It is
/// evaluated after [`SegmentLedger::begin_attempt`], against the same attempt
/// start [`escalate_local_read`] then ages the failures to, so the two always
/// agree on which attempt is the escalating one.
fn local_read_escalates_next(segment: &Path, config: &DrainConfig, ledger: &SegmentLedger) -> bool {
    local_read_escalation_due(
        segment,
        ledger.local_read_failures(segment).saturating_add(1),
        config,
        ledger,
    )
}

fn transition_from_draining(
    segment: PathBuf,
    result: ProcessResult,
    config: &DrainConfig,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
    ledger: &mut SegmentLedger,
    hb: &DrainHeartbeat,
) -> DrainState {
    // First failure for this segment: a fresh retry budget and the base delay.
    next_state_after_process(
        segment,
        result,
        config,
        metrics,
        block_episode,
        ledger,
        hb,
        |segment, retry_after, processed, cause| DrainState::RetryingTransient {
            segment,
            retries_left: config.max_retries,
            next_delay: next_retry_delay(config.base_retry_delay, retry_after),
            processed,
            cause,
        },
    )
}

/// Ends the current dead-letter-full blocking episode (if one is open) because
/// a segment was just confirmed, quarantined or parked (set aside for the run,
/// so the drain moved past it like a quarantine) — genuine forward progress. The
/// blocked-duration gauge is reset to zero only here, never on a transient
/// unblock, so a flapping cap doesn't repeatedly zero it (F09).
fn end_block_episode(block_episode: &mut Option<Instant>, metrics: &Metrics) {
    if block_episode.take().is_some() {
        metrics.dead_letter_blocked_duration.set(0.0);
    }
}

/// Default for [`DrainConfig::health_poll_interval`]: how often the idle drain
/// re-probes `Sink::health()` (keeping the `weir_sink_health{state}` gauge fresh
/// and driving the stranded-segment recovery rescan). 30 s matches the default
/// `dead_letter_check_interval_secs` for symmetry.
pub const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Picks the next retry delay. If the sink supplied a `Retry-After`-style
/// hint, honour it; otherwise fall back to `default` (typically the
/// exponential-backoff value). Caps at 5 minutes regardless of source so a
/// misbehaving server can't stall the drain indefinitely.
fn next_retry_delay(default: Duration, hint: Option<Duration>) -> Duration {
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
    let chosen = hint.unwrap_or(default);
    chosen.min(MAX_RETRY_DELAY)
}

fn enter_blocked(
    segment: PathBuf,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
    processed: u64,
    needed: u64,
) -> DrainState {
    // Increment the counter and stamp the episode start only on a genuine new
    // episode. If `block_episode` is already set we re-entered blocked after a
    // transient unblock of the same pressure (a flapping cap) — reuse the stored
    // instant so `dead_letter_full` counts distinct episodes and the duration
    // gauge keeps climbing rather than resetting (F09).
    let blocked_since = *block_episode.get_or_insert_with(|| {
        metrics.dead_letter_full.inc();
        Instant::now()
    });
    set_drain_state(metrics, DrainStateValue::blocked_dead_letter_full);
    DrainState::BlockedDeadLetterFull {
        segment,
        blocked_since,
        processed,
        needed,
    }
}

// ── Segment processing ────────────────────────────────────────────────────────

/// Delivers one sealed segment, resuming past its first `skip` records.
///
/// `last_local_read_chance` is true when one more segment-class local read
/// failure would reach the escalation limit (see [`local_read_escalates_next`]).
/// It matters only to a `Media`-class read error mid-segment: there, and only
/// there, the escalation must also deliver the partial batch it already read
/// before quarantining (spec §4.2), which needs the sink — so it happens here
/// rather than in [`escalate_local_read`].
#[allow(clippy::too_many_arguments)]
async fn process_segment<S: Sink>(
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    skip: u64,
    last_local_read_chance: bool,
    hb: &DrainHeartbeat,
) -> ProcessResult {
    // The whole attempt is reading the segment, except where a nested guard
    // says otherwise: each commit (`SinkCommit`), a dead-letter write, a
    // quarantine move. The heartbeat itself moves after every commit and every
    // READ_HEARTBEAT_EVERY records (spec §4.8), so one large segment no longer
    // freezes it for as long as the whole segment takes.
    let _busy = BusyGuard::new(hb, BusyKind::SegmentRead);
    let reader = match open_segment(segment) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Already gone (deleted out-of-band, or a double-send). Nothing to
            // deliver and nothing to preserve — a genuine no-op.
            warn!(path = %segment.display(), "drain: segment not found; treating as already-consumed");
            return ProcessResult::Confirmed { record_count: 0 };
        }
        // The reader rejected the header itself (bad magic, a version or a
        // flags bit it does not read, a file too short to hold one). That is a
        // statement about the BYTES, which no retry changes — so it is decided
        // now, not retried (spec §4.1), unless the re-read cannot reproduce the
        // rejection. Nothing was delivered either way.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            match inspect_rejected_header(segment) {
                HeaderVerdict::NewerFormat { version, flags } => {
                    // Written by a newer weir, not corrupt. Quarantining it would
                    // break a rollback (a downgraded daemon would push every
                    // newer-format segment into manual recovery), so it is set
                    // aside for this run, bytes untouched, and replayed on the
                    // next start — where a newer binary delivers it.
                    error!(
                        path = %segment.display(),
                        version,
                        flags = ?flags,
                        max_supported = weir_wab::format::FORMAT_VERSION_MAX_SUPPORTED,
                        error = %e,
                        "drain: segment was written in a newer WAB format (version or header \
                         flags) than this binary reads; parked (left in place, not retried this \
                         run) — upgrade weir to deliver it; it replays on the next start"
                    );
                    return ProcessResult::Parked {
                        reason: ledger::ParkReason::UnsupportedVersion,
                    };
                }
                HeaderVerdict::Corrupt => {
                    error!(path = %segment.display(), error = %e, "drain: segment header is corrupt; quarantining without retrying");
                    return quarantine_result(
                        segment,
                        config,
                        metrics,
                        "drain: corrupt segment header",
                        hb,
                    );
                }
                HeaderVerdict::Gone => {
                    warn!(path = %segment.display(), "drain: segment not found; treating as already-consumed");
                    return ProcessResult::Confirmed { record_count: 0 };
                }
                // Re-reading the header to tell the two apart failed with a
                // plain I/O error: undecided, so it is a local read like any
                // other (below).
                HeaderVerdict::Unreadable(re) => return local_read_failure(segment, re, skip),
            }
        }
        // Any other open failure is local I/O — fd exhaustion, ENOMEM, a
        // permissions problem, a device error. Confirming here would DELETE a
        // segment full of undelivered records on a transient blip, so it is
        // preserved and retried. The error's class (spec §4.0 table) decides
        // what persistence means: a daemon-wide `Process` error cycles until it
        // clears, while `Access` and `Media` errors escalate (park, quarantine)
        // after LOCAL_READ_STRAND_LIMIT strand cycles once the segment has
        // been failing for LOCAL_READ_ESCALATION_MIN_AGE — see
        // `next_state_after_process`.
        Err(e) => return local_read_failure(segment, e, skip),
    };

    // Read once from the header the reader already parsed. Stable across a
    // crash-replay, unlike wall-clock — see SinkBatch::with_segment_context.
    let segment_created_at = reader.header().created_at;

    let max_batch = sink.max_batch_size().max(1);
    // `read_index` counts every record the reader yields (including the `skip`
    // prefix already handled on a prior attempt). `durable_through` is how many
    // records have been durably committed/dead-lettered — it starts at `skip`
    // and advances only when a sub-batch returns Ok, so a Transient/Blocked
    // result reports exactly the prefix the next retry can skip (F05).
    let mut read_index: u64 = 0;
    let mut durable_through: u64 = skip;
    let mut batch: Vec<Payload> = Vec::with_capacity(max_batch);

    // Per-record identity for the sink's idempotency key: the segment's address
    // in the buffer plus the record's index within it, which is what tells two
    // records carrying identical bytes apart — a content hash cannot. Kept
    // parallel to `batch` rather than folded into `Payload`, so nothing about
    // the record type changes.
    //
    // `segment_identity` is the WAB's, not the drain's: the flusher predicts the
    // same string at ack time for a tracked push's `RecordCoordinate`, and the
    // two must agree or the coordinate names a record the sink never keyed.
    let segment_name = segment_identity(segment);
    let mut batch_ids: Vec<RecordId> = Vec::with_capacity(max_batch);

    // Set when the record loop stops on a record it cannot read and the segment
    // must be quarantined once the readable prefix is delivered; the reason is
    // the one recorded in quarantine/.
    let mut quarantine_after_prefix: Option<&'static str> = None;
    for result in reader {
        #[cfg(test)]
        let result = read_hook::on_record(segment, read_index, result);
        let payload = match result {
            Ok(p) => p,
            // Corruption, as the reader documents it: a CRC mismatch or an
            // oversized length (`InvalidData`), or a record cut short
            // (`UnexpectedEof`). The record and everything after it are
            // unreachable via the sequential reader and no retry changes the
            // bytes, so we deliver the readable prefix and then quarantine the
            // segment below — never confirm+delete it, which would silently drop
            // the unread tail.
            Err(e) if is_corruption(&e) => {
                error!(path = %segment.display(), error = %e, "drain: segment read error; will quarantine after delivering the readable prefix");
                quarantine_after_prefix = Some("drain: mid-segment read error");
                break;
            }
            // Any other kind is the READ failing, not the bytes being bad (an
            // EIO blip, a stale handle, fd pressure). Quarantining would push a
            // good segment into manual recovery, where a requeue re-sends the
            // prefix. Instead the attempt is a local transient (spec §4.2)
            // reporting exactly the durable prefix: the partial batch read so
            // far is NOT committed, and the ledger's cursor makes every later
            // attempt, across strands too, resume past what was committed — so
            // nothing is sent twice. The error's class decides persistence:
            // `Process` cycles until it clears, `Access` parks at the limit,
            // `Media` takes the corruption path at the limit (below).
            Err(e) => {
                let class = ledger::classify(&e);
                if last_local_read_chance && class == ledger::ErrClass::Media {
                    error!(
                        path = %segment.display(),
                        error = %e,
                        errno = e.raw_os_error(),
                        "drain: segment still unreadable (media) mid-segment after \
                         {LOCAL_READ_STRAND_LIMIT} strand cycles and the escalation min age; \
                         quarantining after delivering the readable prefix"
                    );
                    quarantine_after_prefix =
                        Some("drain: persistent local read error mid-segment");
                    break;
                }
                error!(
                    path = %segment.display(),
                    error = %e,
                    ?class,
                    durable_through,
                    "drain: segment read error mid-segment (not corruption); preserving for retry \
                     past the records already delivered"
                );
                return ProcessResult::Transient {
                    retry_after: None,
                    processed: durable_through,
                    cause: TransientCause::LocalRead {
                        class,
                        errno: e.raw_os_error(),
                    },
                    carry: None,
                };
            }
        };
        read_index += 1;
        if read_index.is_multiple_of(READ_HEARTBEAT_EVERY) {
            hb.bump();
        }
        // Skip the prefix already durably processed by a previous attempt so we
        // don't re-commit / re-dead-letter it (F05).
        //
        // The SegmentReader has already decoded + CRC-verified this skipped record
        // by the time we drop it, so a resumed retry re-walks (re-reads + re-CRCs)
        // the whole processed prefix (G13). That's accepted: retries are the
        // uncommon path and bounded by max_retries, and the work is a cheap
        // sequential scan — not worth a seek/fast-forward API on the
        // durability-critical reader right before the 1.0 freeze.
        if read_index <= skip {
            continue;
        }
        batch_ids.push(RecordId::for_record(&segment_name, read_index, &payload));
        batch.push(payload);

        if batch.len() >= max_batch {
            let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(max_batch));
            let full_ids = std::mem::replace(&mut batch_ids, Vec::with_capacity(max_batch));
            let n = full_batch.len() as u64;
            let result = commit_batch(
                &full_batch,
                &full_ids,
                segment_created_at,
                sink,
                config,
                metrics,
                dead_letter,
                hb,
            )
            .await;
            hb.bump();
            if let Err(setback) = apply_batch(result, &mut durable_through, n) {
                return setback;
            }
        }
    }

    if !batch.is_empty() {
        let n = batch.len() as u64;
        let result = commit_batch(
            &batch,
            &batch_ids,
            segment_created_at,
            sink,
            config,
            metrics,
            dead_letter,
            hb,
        )
        .await;
        hb.bump();
        if let Err(setback) = apply_batch(result, &mut durable_through, n) {
            return setback;
        }
    }
    if let Some(reason) = quarantine_after_prefix {
        // Readable prefix delivered; move the segment (with its unreadable tail) to
        // the quarantine dir for manual recovery rather than deleting it.
        return quarantine_result(segment, config, metrics, reason, hb);
    }

    // Completeness check before confirming (S01). A sealed segment ends with a
    // zero-length sentinel followed by a footer whose `record_count` is
    // authoritative. The sequential reader cannot tell "reached the sentinel"
    // from "the file just ended": a post-seal tail truncation (partial media
    // loss, a torn block, an at-rest truncation while the segment waits out a
    // long sink outage) drops trailing records AND the sentinel/footer, and the
    // reader returns a short, error-free stream. Confirming that would delete
    // records that were acked durable to producers but never delivered — a silent
    // crown-invariant violation. Cross-check the count we actually read against
    // the footer; on any mismatch (or an unreadable footer, i.e. the footer
    // itself was truncated) quarantine rather than confirm+delete. This turns a
    // previously-undetected silent loss into an operator-visible quarantine.
    match read_footer_count(segment) {
        Ok(footer_count) if footer_count == read_index => {}
        Ok(footer_count) => {
            error!(
                path = %segment.display(),
                read = read_index,
                footer = footer_count,
                "drain: segment record count disagrees with its footer (truncated tail?); quarantining instead of confirming"
            );
            return quarantine_result(
                segment,
                config,
                metrics,
                "drain: record count mismatch vs footer (truncated tail)",
                hb,
            );
        }
        // A footer cut short (the file ends inside it, or is shorter than one):
        // the footer itself was truncated, so completeness is unverifiable from
        // the bytes and no retry changes that.
        Err(e) if is_corruption(&e) => {
            error!(
                path = %segment.display(),
                error = %e,
                "drain: cannot read segment footer to verify completeness; quarantining instead of confirming"
            );
            return quarantine_result(
                segment,
                config,
                metrics,
                "drain: footer unreadable, completeness unverifiable",
                hb,
            );
        }
        // Gone between the record loop and the footer read: like the open-time
        // NotFound, already consumed. Every record was delivered above, so
        // there is nothing left to deliver or preserve.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!(path = %segment.display(), "drain: segment vanished before its footer was read; treating as already-consumed");
            return ProcessResult::Confirmed {
                record_count: read_index,
            };
        }
        // The footer READ failed (EIO, fd pressure, …), which says nothing about
        // whether the segment is complete. Every record is already durable, so
        // this is a local transient with `processed` = all of them: each retry,
        // across strands too, re-reads the segment (G13) and re-checks the
        // footer but re-commits nothing. At the escalation limit `Media`
        // quarantines and `Access` parks (`escalate_local_read`); nothing is
        // left to deliver first. Before spec §4.2 any footer error quarantined a
        // complete, delivered segment on the first attempt.
        Err(e) => {
            error!(
                path = %segment.display(),
                error = %e,
                records = read_index,
                "drain: cannot read segment footer (not corruption); every record is delivered — \
                 retrying the completeness check"
            );
            return ProcessResult::Transient {
                retry_after: None,
                processed: durable_through,
                cause: TransientCause::LocalRead {
                    class: ledger::classify(&e),
                    errno: e.raw_os_error(),
                },
                carry: None,
            };
        }
    }

    // Clean path only: every readable record is now durably processed
    // (durable_through == the full count). Asserted AFTER the quarantine
    // early-return — on a resumed segment (skip > 0) that then hits a mid-prefix
    // read error, durable_through == skip < read_index, which is expected and
    // handled above by quarantine; asserting before that check would trip on a
    // legitimate state (G10).
    debug_assert_eq!(durable_through, read_index);

    ProcessResult::Confirmed {
        record_count: read_index,
    }
}

/// Applies a sub-batch of `n` records, starting after record `*durable_through`,
/// to the cursor: `Ok` advances it past the sub-batch; anything else leaves it
/// and returns the [`ProcessResult`] to report. What the sub-batch still owes
/// the dead-letter directory becomes a carry ending at `durable_through + n`, so
/// the next attempt writes it and resumes past the sub-batch without
/// re-committing it.
fn apply_batch(
    result: BatchResult,
    durable_through: &mut u64,
    n: u64,
) -> Result<(), ProcessResult> {
    let processed = *durable_through;
    match result {
        BatchResult::Ok => {
            *durable_through += n;
            Ok(())
        }
        BatchResult::Transient {
            retry_after,
            cause,
            owed,
        } => Err(ProcessResult::Transient {
            retry_after,
            processed,
            cause,
            carry: owed.map(|o| o.into_carry(processed + n)),
        }),
        BatchResult::Blocked { needed, owed } => Err(ProcessResult::BlockedDeadLetter {
            processed,
            needed,
            carry: Some(owed.into_carry(processed + n)),
        }),
    }
}

/// True for the error kinds the segment reader uses for bad BYTES (spec §4.2):
/// `InvalidData` (CRC mismatch, oversized length) and `UnexpectedEof` (a record
/// or footer cut short). Every other kind is the read itself failing.
fn is_corruption(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
    )
}

/// A local read error at open, as a transient result. The cause is LOCAL, not
/// the sink: a strand from here must not mark the sink down or count as a sink
/// strand (D11).
fn local_read_failure(segment: &Path, e: std::io::Error, skip: u64) -> ProcessResult {
    error!(path = %segment.display(), error = %e, "drain: cannot open segment; preserving for retry");
    ProcessResult::Transient {
        retry_after: None,
        processed: skip,
        cause: TransientCause::LocalRead {
            class: ledger::classify(&e),
            errno: e.raw_os_error(),
        },
        carry: None,
    }
}

/// What a header the reader rejected as `InvalidData` actually is.
#[derive(Debug)]
enum HeaderVerdict {
    /// Written by a newer weir: the magic is right and either the version byte
    /// is above what this build reads, or it is a v2 header carrying a flags bit
    /// outside the set this build understands (`flags` is then `Some`).
    NewerFormat { version: u8, flags: Option<u8> },
    /// Bad magic, version 0, a v1 header with its reserved byte set, or too
    /// short to hold a header.
    Corrupt,
    /// The file vanished between the open and the re-read.
    Gone,
    /// The re-read itself failed with an I/O error, or found a header that now
    /// parses (the bytes changed under us) — undecided either way.
    Unreadable(std::io::Error),
}

/// Re-reads the header of a segment [`SegmentReader::open`] rejected, to tell a
/// newer format from corruption.
///
/// The magic is checked first: a version byte only means something under the
/// right magic (`b"NOTWEIR…"` has `'W'` = 0x57 at byte 4, which is no version).
/// A version above the max is newer whatever the file's length, since a newer
/// format may define its own header. Versions this build knows are judged by
/// [`weir_wab::format::parse_segment_header`] on the full fixed-size header: a
/// v2 flags bit outside the known set is what a newer weir writes to add a
/// feature without bumping the version, so it is newer, not corrupt; a set bit
/// in v1's reserved byte is corrupt (v1 documents it as zero).
fn inspect_rejected_header(segment: &Path) -> HeaderVerdict {
    use std::io::Read;
    use weir_wab::format::{
        FORMAT_VERSION_MAX_SUPPORTED, FORMAT_VERSION_V2, SEGMENT_HEADER_LEN, SEGMENT_MAGIC,
        SegmentHeaderParseError, parse_segment_header,
    };

    let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
    let read = std::fs::File::open(segment)
        .and_then(|f| f.take(SEGMENT_HEADER_LEN as u64).read_to_end(&mut header));
    match read {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HeaderVerdict::Gone,
        Err(e) => return HeaderVerdict::Unreadable(e),
    }
    if header.len() < 5 || header[0..4] != SEGMENT_MAGIC {
        return HeaderVerdict::Corrupt;
    }
    let version = header[4];
    if version > FORMAT_VERSION_MAX_SUPPORTED {
        return HeaderVerdict::NewerFormat {
            version,
            flags: header.get(5).copied(),
        };
    }
    match parse_segment_header(&header) {
        Err(SegmentHeaderParseError::ReservedFlagsSet {
            version: FORMAT_VERSION_V2,
            flags,
        }) => HeaderVerdict::NewerFormat {
            version: FORMAT_VERSION_V2,
            flags: Some(flags),
        },
        // Version 0, v1 reserved byte set, or a known version in a file shorter
        // than the full header.
        Err(_) => HeaderVerdict::Corrupt,
        // The open rejected these bytes but they parse now: the file changed
        // between the two reads. Do not quarantine on a guess — retry it as a
        // local read, which escalates if it keeps happening.
        Ok(_) => HeaderVerdict::Unreadable(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "segment header was rejected on open but parses on re-read",
        )),
    }
}

/// [`SegmentReader::open`], behind the test-only open-fault hook.
fn open_segment(segment: &Path) -> std::io::Result<SegmentReader> {
    #[cfg(test)]
    if let Some(e) = open_hook::on_open(segment) {
        return Err(e);
    }
    SegmentReader::open(segment)
}

/// Test-only fault injection and observation for segment opens, keyed by the
/// segment path so concurrently running tests (each in its own scratch dir)
/// never see each other's faults. Only registered paths are tracked.
#[cfg(test)]
pub(crate) mod open_hook {
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    struct Entry {
        errno: i32,
        fail_left: u32,
        attempts: u64,
    }

    static HOOKS: Mutex<Option<HashMap<PathBuf, Entry>>> = Mutex::new(None);

    fn with<R>(f: impl FnOnce(&mut HashMap<PathBuf, Entry>) -> R) -> R {
        let mut guard = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        f(guard.get_or_insert_with(HashMap::new))
    }

    /// Counts opens of `path` without failing any.
    pub(crate) fn watch(path: &Path) {
        fail(path, 0, 0);
    }

    /// Fails the next `times` opens of `path` with `errno`, counting every open.
    pub(crate) fn fail(path: &Path, errno: i32, times: u32) {
        with(|m| {
            m.insert(
                path.to_path_buf(),
                Entry {
                    errno,
                    fail_left: times,
                    attempts: 0,
                },
            )
        });
    }

    /// How many times the drain tried to open `path` since it was registered.
    pub(crate) fn attempts(path: &Path) -> u64 {
        with(|m| m.get(path).map_or(0, |e| e.attempts))
    }

    /// Stops tracking `path`.
    pub(crate) fn forget(path: &Path) {
        with(|m| m.remove(path));
    }

    /// Called on every segment open: counts it, and returns the injected error
    /// while the fault lasts.
    pub(super) fn on_open(path: &Path) -> Option<io::Error> {
        with(|m| {
            let e = m.get_mut(path)?;
            e.attempts += 1;
            if e.fail_left == 0 {
                return None;
            }
            e.fail_left -= 1;
            Some(io::Error::from_raw_os_error(e.errno))
        })
    }
}

/// The footer's record count, behind the test-only read-fault hook.
fn read_footer_count(segment: &Path) -> std::io::Result<u64> {
    #[cfg(test)]
    if let Some(e) = read_hook::on_footer(segment) {
        return Err(e);
    }
    read_segment_record_count(segment)
}

/// Test-only fault injection for the reads AFTER a segment opened: one record
/// of the sequential record loop, or the footer read of the completeness check.
/// Keyed by segment path like [`open_hook`], so concurrent tests never see each
/// other's faults.
///
/// The fault is an errno (not an `ErrorKind`) because the drain classifies a
/// local read error by its errno first (spec §4.0 table); EIO, say, has no
/// stable `ErrorKind` of its own.
#[cfg(test)]
pub(crate) mod read_hook {
    use std::{
        collections::HashMap,
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    use weir_core::Payload;

    /// Where the fault fires.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum FaultAt {
        /// The 0-based `n`-th record of the segment: records `0..n` read fine,
        /// record `n` fails instead of being yielded.
        Record(u64),
        /// The footer read that verifies the record count.
        Footer,
    }

    struct Entry {
        at: FaultAt,
        errno: i32,
        fail_left: u32,
        fired: u64,
    }

    static HOOKS: Mutex<Option<HashMap<PathBuf, Entry>>> = Mutex::new(None);

    fn with<R>(f: impl FnOnce(&mut HashMap<PathBuf, Entry>) -> R) -> R {
        let mut guard = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        f(guard.get_or_insert_with(HashMap::new))
    }

    /// Fails the next `times` reads of `at` in `path` with `errno`. Each
    /// attempt at the segment reaches `at` at most once, so `times` counts
    /// attempts (`u32::MAX` = never clears).
    pub(crate) fn fail(path: &Path, at: FaultAt, errno: i32, times: u32) {
        with(|m| {
            m.insert(
                path.to_path_buf(),
                Entry {
                    at,
                    errno,
                    fail_left: times,
                    fired: 0,
                },
            )
        });
    }

    /// How many times the fault has fired for `path`.
    pub(crate) fn fired(path: &Path) -> u64 {
        with(|m| m.get(path).map_or(0, |e| e.fired))
    }

    /// Stops faulting `path`.
    pub(crate) fn forget(path: &Path) {
        with(|m| m.remove(path));
    }

    fn fire(path: &Path, at: FaultAt) -> Option<io::Error> {
        with(|m| {
            let e = m.get_mut(path)?;
            if e.at != at || e.fail_left == 0 {
                return None;
            }
            e.fail_left -= 1;
            e.fired += 1;
            Some(io::Error::from_raw_os_error(e.errno))
        })
    }

    /// Called for the `index`-th (0-based) record the reader yields.
    pub(super) fn on_record(
        path: &Path,
        index: u64,
        result: io::Result<Payload>,
    ) -> io::Result<Payload> {
        match fire(path, FaultAt::Record(index)) {
            Some(e) => Err(e),
            None => result,
        }
    }

    /// Called before the footer read.
    pub(super) fn on_footer(path: &Path) -> Option<io::Error> {
        fire(path, FaultAt::Footer)
    }
}

/// Moves a segment that cannot be safely confirmed into the quarantine dir for
/// manual recovery, rather than deleting it. Returns whether the move landed.
///
/// If the move fails the segment is left on disk — never silently dropped — and
/// `weir_drain_quarantine_failures_total` counts it. The caller then hands the
/// segment to the ledger's `quarantine_pending`, whose health-poll retries redo
/// only the move (spec §4.4); before that, the idle rescan re-delivered the
/// segment's readable prefix on every poll for as long as the fault lasted.
fn quarantine_segment(
    segment: &Path,
    config: &DrainConfig,
    metrics: &Metrics,
    reason: &'static str,
    hb: &DrainHeartbeat,
) -> bool {
    let moved = {
        let _busy = BusyGuard::new(hb, BusyKind::QuarantineMove);
        move_to_quarantine(segment, &config.wab_dir, metrics, reason)
    };
    match moved {
        Ok(()) => true,
        Err(qe) => {
            metrics.drain_quarantine_failures.inc();
            error!(
                path = %segment.display(),
                error = %qe,
                reason,
                "drain: failed to quarantine segment; left on disk, not re-delivered — the move \
                 is retried on every health poll (weir_drain_quarantine_failures_total)"
            );
            false
        }
    }
}

/// [`quarantine_segment`] as the [`ProcessResult`] `process_segment` returns:
/// `Quarantined` when the move landed, `QuarantinePending` when it did not.
fn quarantine_result(
    segment: &Path,
    config: &DrainConfig,
    metrics: &Metrics,
    reason: &'static str,
    hb: &DrainHeartbeat,
) -> ProcessResult {
    if quarantine_segment(segment, config, metrics, reason, hb) {
        ProcessResult::Quarantined
    } else {
        ProcessResult::QuarantinePending { reason }
    }
}

/// One attempt at the quarantine move, shared by the first try
/// ([`quarantine_segment`]) and the ledger's retries. On success it bumps the
/// quarantine series exactly as every other quarantine site does.
///
/// A segment that no longer exists counts as done (ENOENT, spec §4.4): an
/// operator removed or requeued it, so there is nothing left to preserve, and
/// nothing is counted as quarantined.
fn move_to_quarantine(
    segment: &Path,
    wab_dir: &Path,
    metrics: &Metrics,
    reason: &str,
) -> std::io::Result<()> {
    match crate::wab::recovery::quarantine(segment, wab_dir, reason) {
        Ok(()) => {
            // BOTH series, per the invariant asserted on
            // `wab::recovery::quarantine_and_count`: "Every segment-quarantine
            // site funnels through here so the recovery_segments_quarantined
            // counter and the wab_segments{state=quarantined} gauge are ALWAYS
            // bumped ... If you add a third, route it here."
            //
            // This IS that third site, and it bumped only the flat counter — so
            // a drain-time quarantine moved one series while a recovery-time
            // quarantine moved two, and an operator reading the gap could not
            // tell "the drain quarantined something" from "a counter is broken".
            // It does not call quarantine_and_count itself only because that
            // helper's contract is to RETURN an io::Error describing the
            // quarantine, which this caller has no use for.
            metrics.recovery_segments_quarantined.inc();
            metrics
                .wab_segments
                .get_or_create(&crate::metrics::SegmentStateLabel {
                    state: crate::metrics::SegmentState::quarantined,
                })
                .inc();
            Ok(())
        }
        Err(qe) => match std::fs::symlink_metadata(segment) {
            Err(gone) if gone.kind() == std::io::ErrorKind::NotFound => {
                warn!(
                    path = %segment.display(),
                    reason,
                    "drain: segment vanished before it could be quarantined; nothing left to \
                     preserve"
                );
                Ok(())
            }
            _ => Err(qe),
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_batch<S: Sink>(
    payloads: &[Payload],
    record_ids: &[RecordId],
    segment_created_at: i64,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    hb: &DrainHeartbeat,
) -> BatchResult {
    // Derive the batch's dedup token. This runs on the drain thread — off the
    // fsync path entirely — and costs ~170 us per 256 KiB against a sink commit
    // measured in milliseconds.
    //
    // Batch-scoped, not segment-scoped: `process_segment` splits a segment into
    // `max_batch_size` chunks and calls this once per chunk, so a token shared
    // across chunks would make a dedup-capable sink discard every chunk after
    // the first.
    let dedup_token = weir_sink_sdk::DedupToken::for_payloads(payloads.iter());

    // Clone into an owned Vec so the caller's `payloads` stay available for
    // dead-lettering on a Permanent error. `Payload` is `Bytes`, so each clone
    // is a refcount bump. (weir 1.x also ran every payload through
    // `SinkRecord::from_payload` here; that conversion was the identity in every
    // implementation that ever existed, and 2.0 removes it.)
    // `segment_created_at` comes from the owning segment's WAB header, which is
    // written once and read back unchanged after a crash-replay. A sink that
    // derives a partition path or object key from it therefore puts a replayed
    // batch in the same place; deriving one from wall-clock would scatter
    // replays across partitions and duplicate them downstream.
    let batch = weir_sink_sdk::SinkBatch::with_segment_context(
        payloads.to_vec(),
        dedup_token,
        record_ids.to_vec(),
        segment_created_at,
    );

    let t = std::time::Instant::now();
    // Backstop timeout: a sink that hangs (e.g. a third-party sink with no
    // internal timeout) must not stall the drain forever. On elapse, treat it
    // as a transient error so the segment is retried.
    //
    // The busy guard sits OUTSIDE the timeout (spec §4.8): a sink that blocks
    // the thread instead of yielding defeats the timeout, and the guard staying
    // set past `commit_timeout` is how the poll task tells that wedge from a
    // merely slow commit. It is dropped before the dead-letter write below.
    let timed = {
        let _busy = BusyGuard::new(hb, BusyKind::SinkCommit);
        tokio::time::timeout(config.commit_timeout, sink.commit(batch)).await
    };
    let commit = match timed {
        Ok(inner) => inner,
        Err(_elapsed) => {
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::retried,
                })
                .inc_by(payloads.len() as u64);
            warn!(
                timeout_secs = config.commit_timeout.as_secs(),
                "drain: sink commit exceeded commit_timeout; treating as transient, retrying segment"
            );
            return BatchResult::Transient {
                retry_after: None,
                cause: TransientCause::Sink,
                owed: None,
            };
        }
    };
    match commit {
        Ok(commit_result) => {
            // The Sink contract is that committed ∪ dead_lettered partitions the
            // input batch. A (non-conforming, third-party) sink that drops a
            // record from BOTH vectors would otherwise have the segment
            // confirmed-and-deleted with that record neither delivered nor
            // dead-lettered — a silent false ack with zero detection (F02).
            // Refuse to confirm: preserve the segment, retry, and surface it
            // loudly. Built-in sinks always partition, so this never fires for
            // them. (The deeper fix — encoding the invariant in the SDK type — is
            // tracked as F41.)
            let accounted = commit_result.committed.len() + commit_result.dead_lettered.len();
            if accounted != payloads.len() {
                error!(
                    input = payloads.len(),
                    committed = commit_result.committed.len(),
                    dead_lettered = commit_result.dead_lettered.len(),
                    "drain: sink CommitResult does not account for every record; preserving \
                     segment instead of confirming (likely a Sink contract violation)"
                );
                return BatchResult::Transient {
                    retry_after: None,
                    cause: TransientCause::Sink,
                    owed: None,
                };
            }
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            // The committed-records count is bumped at the END, only on the path
            // that actually returns BatchResult::Ok — a Blocked or failed-write
            // Transient return below defers it to whoever finishes the
            // sub-batch: the carry's write, or (when no carry could be held) the
            // re-commit. Counting here would double-count it (S24).
            let committed_count = commit_result.committed.len() as u64;

            if !commit_result.dead_lettered.is_empty() {
                let dead_payloads: Vec<Payload> = commit_result
                    .dead_lettered
                    .into_iter()
                    .map(|(payload, _reason)| payload)
                    .collect();
                // A failed dead-letter write (or a cap block) must NOT fall through
                // to Ok: confirming would delete the segment with these records
                // neither delivered nor dead-lettered — silent data loss (B1/F02).
                // The rejects travel with the result as what the sub-batch owes,
                // so the retry writes them instead of re-committing the accepted
                // records (spec §4.5, §4.11).
                if let Err(failure) =
                    dead_letter_records(&dead_payloads, dead_letter, config, metrics, hb)
                {
                    return owed_result(failure, dead_payloads, committed_count);
                }
            }

            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::committed,
                })
                .inc_by(committed_count);
            BatchResult::Ok
        }

        Err(e) if e.is_transient() => {
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::retried,
                })
                .inc_by(payloads.len() as u64);
            let retry_after = e.retry_after();
            warn!(
                error = %e,
                retry_after_secs = retry_after.map(|d| d.as_secs()),
                "drain: transient sink error; will retry segment"
            );
            BatchResult::Transient {
                retry_after,
                cause: TransientCause::Sink,
                owed: None,
            }
        }

        Err(e) => {
            // Permanent error — dead-letter the whole batch.
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            error!(error = %e, "drain: permanent sink error; dead-lettering batch");

            // Same silent-data-loss guard as the partial-rejection path above: a
            // failed dead-letter write (or a cap block) preserves the segment for
            // retry instead of confirming + deleting it (B1/F02).
            if let Err(failure) = dead_letter_records(payloads, dead_letter, config, metrics, hb) {
                return owed_result(failure, payloads.to_vec(), 0);
            }

            // Dead-letter write succeeded: the records are durably re-homed, so the
            // segment can be confirmed. Leaving it unconsumed would replay the same
            // permanent-error records on every restart.
            BatchResult::Ok
        }
    }
}

/// Dead-letters `records` under the `dead_letter_max_bytes` cap, bumping the
/// `dead_lettered` outcome counter and the `dead_letter_bytes_on_disk` gauge on
/// success. Single source of truth for the cap pre-check + write + accounting,
/// shared by the partial-rejection and whole-batch-permanent paths in
/// `commit_batch` (previously duplicated byte-for-byte).
///
/// On failure the caller must NOT confirm the segment:
/// - `Err(DeadLetterFailure::Blocked)` when the write would exceed the cap and the
///   batch is not itself larger than the whole cap (a one-off oversized batch is
///   written anyway — overshoot once — to avoid a permanent block↔retry livelock, F03).
///   `needed` is the write's estimated size, which the blocked state's unblock
///   predicate waits on (spec §4.5).
/// - `Err(DeadLetterFailure::WriteFailed)` when the dead-letter write itself fails
///   (e.g. ENOSPC), counted in `weir_drain_dead_letter_write_failures_total`: the
///   segment is preserved and retried rather than confirmed+deleted with the
///   records neither delivered nor dead-lettered — silent data loss (B1/F02).
///
/// Shared by `commit_batch` and by the write of a carried sub-batch's rejects
/// ([`settle_dl_carry`]), so both block and fail on exactly the same terms.
fn dead_letter_records(
    records: &[Payload],
    dead_letter: &mut DeadLetterWriter,
    config: &DrainConfig,
    metrics: &Metrics,
    hb: &DrainHeartbeat,
) -> Result<(), DeadLetterFailure> {
    let estimated = estimated_write_bytes(records);
    if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
        if estimated > config.dead_letter_max_bytes {
            warn!(
                estimated,
                cap = config.dead_letter_max_bytes,
                "drain: dead-letter batch alone exceeds dead_letter_max_bytes; writing it anyway to avoid a permanent block"
            );
        } else {
            return Err(DeadLetterFailure::Blocked { needed: estimated });
        }
    }

    let written = {
        let _busy = BusyGuard::new(hb, BusyKind::DeadLetterWrite);
        dead_letter.write_records(records)
    };
    match written {
        Ok(()) => {
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::dead_lettered,
                })
                .inc_by(records.len() as u64);
            metrics
                .dead_letter_bytes_on_disk
                .set(dead_letter.total_bytes() as f64);
            Ok(())
        }
        Err(e) => {
            // The sink did its part; the WAB device did not. A local write
            // cause, so a strand from here neither marks the sink down nor
            // counts as a sink strand (D11, spec §4.11).
            metrics.drain_dead_letter_write_failures.inc();
            error!(
                error = %e,
                records = records.len(),
                "drain: failed to write dead-letter records; preserving segment and retrying only \
                 the dead-letter write (weir_drain_dead_letter_write_failures_total)"
            );
            Err(DeadLetterFailure::WriteFailed)
        }
    }
}

/// The [`BatchResult`] for a sub-batch the sink handled but whose rejects could
/// not be dead-lettered: `payloads` are the rejects it still owes, `committed`
/// the records the sink accepted.
fn owed_result(failure: DeadLetterFailure, payloads: Vec<Payload>, committed: u64) -> BatchResult {
    let owed = DeadLetterOwed {
        payloads,
        committed,
    };
    match failure {
        DeadLetterFailure::Blocked { needed } => BatchResult::Blocked { needed, owed },
        DeadLetterFailure::WriteFailed => BatchResult::Transient {
            retry_after: None,
            cause: TransientCause::LocalWrite,
            owed: Some(owed),
        },
    }
}

/// Byte estimate for the dead-letter segment that `payloads` would seal into —
/// header + per-record overhead + payloads + sentinel + footer. Delegates to the
/// single source of truth so the `would_exceed_cap` pre-check matches the real
/// sealed file size; the previous estimate omitted the ~60-byte segment framing,
/// letting each write creep past `dead_letter_max_bytes` (F07).
fn estimated_write_bytes(payloads: &[Payload]) -> u64 {
    dead_letter::estimated_segment_bytes(payloads)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::SinkBatch;
    use crate::wab::format::Compression;
    use crate::{
        metrics::Metrics,
        sink::{CommitResult, SinkHealth},
        wab::segment::{WabSegment, segment_path},
    };
    use std::{
        collections::VecDeque,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    // ── DrainHeartbeat / drain_appears_stalled ──────────────────────────────

    #[test]
    fn a_stalled_drain_is_visible_to_an_outside_observer() {
        // tokio::time::timeout cannot fire against a sink that blocks the drain's
        // current-thread runtime, so liveness has to be observed from outside.
        let hb = DrainHeartbeat::new();
        let first = hb.get();
        hb.bump();
        assert!(hb.get() > first, "a live drain advances the heartbeat");

        let stalled = hb.get();
        // Simulate a wedged drain: no bumps at all.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(
            hb.get(),
            stalled,
            "a wedged drain leaves the heartbeat frozen"
        );
        assert!(
            drain_appears_stalled(stalled, hb.get()),
            "an unchanged heartbeat across a poll interval must read as stalled"
        );
    }

    /// The isolated test above only proves `DrainHeartbeat` counts correctly in
    /// the abstract. This exercises the real `drain_thread`: an idle-but-alive
    /// drain (nothing queued, so its only activity is waking on its own
    /// `health_poll_interval` cadence) still advances the heartbeat over time.
    #[test]
    fn a_real_idle_drain_thread_advances_the_heartbeat() {
        let dir = tmp_dir("heartbeat_idle");
        let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let heartbeat = DrainHeartbeat::new();
        let config = DrainConfig {
            // Short so the idle wake cadence is observable inside a fast test.
            health_poll_interval: Duration::from_millis(20),
            ..fast_config(dir.clone())
        };
        let hb_for_thread = heartbeat.clone();
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink, config, metrics, true, hb_for_thread)
        });

        let before = heartbeat.get();
        std::thread::sleep(Duration::from_millis(200));
        let after = heartbeat.get();
        assert!(
            after > before,
            "an idle-but-alive drain must still advance its heartbeat on the \
             health-poll cadence, or a merely-idle drain would be indistinguishable \
             from a wedged one"
        );

        drop(tx);
        handle.join().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    /// The other half: a drain genuinely wedged inside a sink call that blocks
    /// its OS thread (`std::thread::sleep`, never `.await`ed — see
    /// `MockSink::with_blocking_commit`) must show a FROZEN heartbeat for the
    /// duration of the block. This is deliberately bounded (300 ms), unlike the
    /// real-world case this models, which is exactly why the heartbeat has to
    /// exist: nothing inside the drain can time this out.
    #[test]
    fn a_drain_wedged_inside_a_blocking_sink_call_freezes_the_heartbeat() {
        let dir = tmp_dir("heartbeat_wedge");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_blocking_commit(300, []));
        let metrics = noop_metrics();
        let heartbeat = DrainHeartbeat::new();
        let config = DrainConfig {
            health_poll_interval: Duration::from_millis(20),
            commit_timeout: Duration::from_secs(30),
            ..fast_config(dir.clone())
        };
        let hb_for_thread = heartbeat.clone();
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink, config, metrics, true, hb_for_thread)
        });

        // Give the drain a moment to pick up the segment and enter the blocking
        // commit, then sample twice while the 300 ms block is still ongoing.
        std::thread::sleep(Duration::from_millis(50));
        let during_a = heartbeat.get();
        std::thread::sleep(Duration::from_millis(100));
        let during_b = heartbeat.get();
        assert_eq!(
            during_a, during_b,
            "a sink that blocks the OS thread must freeze the heartbeat — this is \
             exactly the case tokio::time::timeout cannot catch, since the timer \
             never gets polled either"
        );

        drop(tx);
        handle.join().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    /// F2(b): the drain is ALSO alive while it is deliberately waiting — and
    /// waiting is not rare. `heartbeat.bump()` sits at the top of `'outer`, but
    /// a single iteration of the RetryingTransient arm can park for as long as
    /// `next_retry_delay` allows: a `Retry-After`-honouring sink can push that
    /// to `MAX_RETRY_DELAY` (300 s), and even without a hint `commit_timeout`
    /// defaults to 60 s. The stall warning's threshold is
    /// `health_poll_interval + 15 s` = 45 s at the defaults, so a merely-SLOW
    /// sink was reported to the operator as a drain "wedged inside a sink call
    /// that blocks its OS thread" — a confident, wrong diagnosis delivered
    /// during an incident.
    ///
    /// The waiting loop slices at 50 ms so it can notice a channel disconnect;
    /// bumping there costs nothing and makes the heartbeat mean what it claims
    /// to mean: only a thread that cannot reach its own loop head freezes it.
    #[test]
    fn a_drain_in_a_long_retry_backoff_keeps_advancing_the_heartbeat() {
        let dir = tmp_dir("heartbeat_backoff");
        let sealed = make_sealed_segment(&dir, 0, &[b"hello"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // A Retry-After hint long enough that the whole sampling window below
        // sits inside ONE RetryingTransient iteration — the shape of the real
        // 60 s/300 s waits, compressed to keep the test fast.
        const HINT: Duration = Duration::from_millis(1500);
        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::TransientWithRetryAfter(HINT)),
            MockSink::ok(vec![Payload::from(b"hello".as_ref())]),
        ]));
        let metrics = noop_metrics();
        let heartbeat = DrainHeartbeat::new();
        let hb_for_thread = heartbeat.clone();
        let config = fast_config(dir.clone());
        let sink_for_thread = Arc::clone(&sink);
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink_for_thread, config, metrics, true, hb_for_thread)
        });

        // Wait for the first commit to have returned its transient error: from
        // that point the drain is inside the backoff, not still draining.
        let deadline = Instant::now() + Duration::from_secs(5);
        while sink.call_count() < 1 {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for the first commit"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        std::thread::sleep(Duration::from_millis(150));
        let during_a = heartbeat.get();
        std::thread::sleep(Duration::from_millis(400));
        let during_b = heartbeat.get();
        assert!(
            during_b > during_a,
            "a drain WAITING on a retry backoff is alive and must keep advancing its \
             heartbeat ({during_a} -> {during_b}); freezing it makes a merely-slow sink \
             indistinguishable from a thread blocked inside a sink call"
        );

        drop(tx);
        handle.join().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    /// F2(b), the other arm: BlockedDeadLetterFull parks on the same 50 ms
    /// slicing while it waits for an operator to free dead-letter headroom.
    /// That wait is unbounded by design, so without a bump there it is the
    /// single easiest way to trip the "wedged inside a sink call" warning while
    /// the drain is in fact perfectly alive and reporting
    /// `drain_state{blocked_dead_letter_full}`.
    #[test]
    fn a_drain_blocked_on_dead_letter_headroom_keeps_advancing_the_heartbeat() {
        let dir = tmp_dir("heartbeat_dl_blocked");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);
        // Pre-fill the dead-letter dir past the cap so the first permanent
        // rejection has nowhere to go and the drain parks in the blocked wait.
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let metrics = noop_metrics();
        let heartbeat = DrainHeartbeat::new();
        let hb_for_thread = heartbeat.clone();
        let config = DrainConfig {
            // Long enough that the sampling window sits inside ONE wait.
            dead_letter_check_interval: Duration::from_millis(1500),
            ..tight_dl_config(dir.clone(), 100)
        };
        let metrics_for_thread = Arc::clone(&metrics);
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink, config, metrics_for_thread, true, hb_for_thread)
        });

        // Let it commit, get the reject, and enter the blocked wait.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !drain_is_blocked(&metrics) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for the drain to block on dead-letter headroom"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let during_a = heartbeat.get();
        std::thread::sleep(Duration::from_millis(400));
        let during_b = heartbeat.get();
        assert!(
            drain_is_blocked(&metrics),
            "the sampling window must sit inside ONE blocked wait, or this proves nothing"
        );
        assert!(
            during_b > during_a,
            "a drain blocked on dead-letter headroom is alive and must keep advancing \
             its heartbeat ({during_a} -> {during_b})"
        );

        drop(tx);
        handle.join().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    // ── next_retry_delay (Retry-After honoring) ─────────────────────────────

    #[test]
    fn next_retry_delay_uses_hint_when_present() {
        // Hint overrides the default — the server knows its load better
        // than our exponential backoff does.
        let default = Duration::from_millis(100);
        let hint = Some(Duration::from_secs(5));
        assert_eq!(next_retry_delay(default, hint), Duration::from_secs(5));
    }

    #[test]
    fn next_retry_delay_falls_back_to_default_without_hint() {
        let default = Duration::from_millis(500);
        assert_eq!(next_retry_delay(default, None), Duration::from_millis(500));
    }

    #[test]
    fn next_retry_delay_caps_at_5_minutes() {
        // A malicious or misbehaving server could send a huge Retry-After;
        // we cap so the drain isn't stalled indefinitely.
        let hint = Some(Duration::from_secs(86_400)); // 1 day
        assert_eq!(
            next_retry_delay(Duration::from_millis(100), hint),
            Duration::from_secs(300)
        );
        // Same cap applies to the default path (exponential backoff can
        // theoretically run away too).
        let default = Duration::from_secs(10_000);
        assert_eq!(next_retry_delay(default, None), Duration::from_secs(300));
    }
    use weir_core::Payload;

    // ── Mock sink ─────────────────────────────────────────────────────────────

    #[derive(Debug)]
    enum MockError {
        Transient,
        /// Transient error that supplies a `retry_after` hint. Used to
        /// verify the drain honours the hint over its default backoff.
        TransientWithRetryAfter(Duration),
        Permanent,
    }

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                MockError::Transient => write!(f, "transient error"),
                MockError::TransientWithRetryAfter(d) => {
                    write!(f, "transient error (retry-after: {d:?})")
                }
                MockError::Permanent => write!(f, "permanent error"),
            }
        }
    }

    impl std::error::Error for MockError {}

    impl crate::sink::SinkError for MockError {
        fn is_transient(&self) -> bool {
            matches!(
                self,
                MockError::Transient | MockError::TransientWithRetryAfter(_)
            )
        }

        fn retry_after(&self) -> Option<Duration> {
            match self {
                MockError::TransientWithRetryAfter(d) => Some(*d),
                _ => None,
            }
        }
    }

    type MockResult = Result<CommitResult, MockError>;

    struct MockSink {
        responses: Mutex<VecDeque<MockResult>>,
        call_count: AtomicU64,
        /// Number of initial `commit()` calls that hang forever (await
        /// `pending()`), modelling a sink with no internal timeout. The drain's
        /// backstop `commit_timeout` must cancel these.
        hang_calls: u64,
        /// A single 1-based `commit()` call that hangs forever (in addition to
        /// `hang_calls`), so a test can hang a *later* sub-batch — e.g. succeed on
        /// the first sub-batch, then time out on the second — to exercise the
        /// commit-timeout-resumes-past-the-cursor path.
        hang_on_call: Option<u64>,
        /// Number of initial `commit()` calls that panic, to drive the drain's
        /// panic supervisor.
        panic_calls: u64,
        /// A payload whose presence in a batch makes `commit()` panic, every
        /// time: a deterministic poison record (spec §4.12).
        panic_on_payload: Option<&'static [u8]>,
        /// Milliseconds every `commit()` call spends in a *genuine* blocking
        /// `std::thread::sleep` — never `.await`ed, unlike `hang_calls`'s
        /// `std::future::pending().await`. That distinction is the whole point:
        /// a future that yields still lets `tokio::time::timeout` observe
        /// elapsed time and fire, but a call that blocks the OS thread itself
        /// starves the runtime's own timer, so `commit_timeout` can never fire.
        /// Models the measured blocking-commit case documented on
        /// `DrainHeartbeat`. Zero (the default) disables this entirely.
        blocking_millis: u64,
        /// How long every `commit()` call spends in a YIELDING sleep
        /// (`tokio::time::sleep(..).await`) — a slow sink, as opposed to the
        /// thread-blocking `blocking_millis`. Zero (the default) disables it.
        yield_for: Duration,
        max_batch: usize,
        /// Wall-clock instant of every `commit()` call. Tests that need to
        /// measure inter-call gaps (e.g. retry-after observance) read this
        /// after `run_drain` returns.
        call_timestamps: Mutex<Vec<Instant>>,
        /// Every payload the sink claimed to commit (i.e. landed in a
        /// successful `commit_result.committed`). Tests can introspect to
        /// verify what got through — the metric `records_committed` only
        /// gives a count.
        committed_records: Mutex<Vec<Payload>>,
        /// Every payload the sink permanently rejected, paired with the
        /// reason string. Tests use this to verify dead-letter *contents*,
        /// not just the dead-letter file count.
        dead_lettered_records: Mutex<Vec<(Payload, String)>>,
        /// Scripted health responses, popped one per `health()` call. Once
        /// exhausted (or if empty), `health()` falls back to `Healthy` — so
        /// sinks built without a script behave exactly as before. Used by the
        /// stranded-segment recovery-edge tests to drive a Down→Healthy
        /// transition deterministically.
        health_script: Mutex<VecDeque<SinkHealth>>,
        /// Every `RecordId` the drain handed over, flattened across all
        /// `commit()` calls in arrival order. This is what a dedup-capable
        /// sink keys on, so it is the thing a re-batching test has to compare.
        seen_record_ids: Mutex<Vec<RecordId>>,
        /// Every `DedupToken` the drain handed over, one per `commit()` call.
        /// Recorded alongside the ids so the two can be compared under the
        /// same re-batching — they behave differently, deliberately.
        seen_dedup_tokens: Mutex<Vec<weir_sink_sdk::DedupToken>>,
    }

    impl MockSink {
        fn with_responses(responses: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                call_count: AtomicU64::new(0),
                hang_calls: 0,
                panic_calls: 0,
                panic_on_payload: None,
                blocking_millis: 0,
                yield_for: Duration::ZERO,
                hang_on_call: None,
                max_batch: 1000,
                call_timestamps: Mutex::new(Vec::new()),
                committed_records: Mutex::new(Vec::new()),
                dead_lettered_records: Mutex::new(Vec::new()),
                health_script: Mutex::new(VecDeque::new()),
                seen_record_ids: Mutex::new(Vec::new()),
                seen_dedup_tokens: Mutex::new(Vec::new()),
            }
        }

        /// A sink whose `health()` returns `script` in order (one per call),
        /// then falls back to `Healthy` once exhausted. Drives the down→up
        /// recovery-edge logic in `probe_and_resume_stranded`. `commit()`
        /// behaves like `with_responses([])` (commits whatever it is given).
        fn with_health_script(script: impl IntoIterator<Item = SinkHealth>) -> Self {
            Self {
                health_script: Mutex::new(script.into_iter().collect()),
                ..Self::with_responses([])
            }
        }

        fn with_batch_size(
            max_batch: usize,
            responses: impl IntoIterator<Item = MockResult>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                call_count: AtomicU64::new(0),
                hang_calls: 0,
                panic_calls: 0,
                panic_on_payload: None,
                blocking_millis: 0,
                yield_for: Duration::ZERO,
                hang_on_call: None,
                max_batch,
                call_timestamps: Mutex::new(Vec::new()),
                committed_records: Mutex::new(Vec::new()),
                dead_lettered_records: Mutex::new(Vec::new()),
                health_script: Mutex::new(VecDeque::new()),
                seen_record_ids: Mutex::new(Vec::new()),
                seen_dedup_tokens: Mutex::new(Vec::new()),
            }
        }

        /// A sink whose first `hang_calls` `commit()` invocations hang forever,
        /// then falls back to `responses` (empty = commit the whole batch).
        fn with_hang(hang_calls: u64, responses: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                hang_calls,
                ..Self::with_responses(responses)
            }
        }

        /// A sink whose first `panic_calls` `commit()` invocations panic, to drive
        /// the drain's panic supervisor (then falls back to `responses`).
        fn with_panic(panic_calls: u64, responses: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                panic_calls,
                ..Self::with_responses(responses)
            }
        }

        /// A sink that panics on every `commit()` whose batch contains
        /// `poison`, and commits every other batch whole.
        fn panicking_on(poison: &'static [u8]) -> Self {
            Self {
                panic_on_payload: Some(poison),
                ..Self::with_responses([])
            }
        }

        /// A sink whose EVERY `commit()` call spends `millis` in a genuine
        /// blocking `std::thread::sleep` before doing anything else — see
        /// `blocking_millis`. Falls back to `responses` afterward (empty =
        /// commit the whole batch).
        fn with_blocking_commit(
            millis: u64,
            responses: impl IntoIterator<Item = MockResult>,
        ) -> Self {
            Self {
                blocking_millis: millis,
                ..Self::with_responses(responses)
            }
        }

        /// A sink whose every `commit()` spends `per_commit` in a yielding
        /// sleep, then commits the whole batch; `max_batch` records per call.
        fn with_yielding_commit(per_commit: Duration, max_batch: usize) -> Self {
            Self {
                yield_for: per_commit,
                max_batch,
                ..Self::with_responses([])
            }
        }

        fn call_count(&self) -> u64 {
            self.call_count.load(Ordering::Relaxed)
        }

        /// Snapshot of every `commit()` invocation's `Instant`. Used by
        /// the retry-after test to verify the drain slept the hinted
        /// duration between calls.
        fn call_timestamps(&self) -> Vec<Instant> {
            self.call_timestamps.lock().unwrap().clone()
        }

        /// Snapshot of every payload the sink reported as committed.
        #[allow(dead_code)]
        fn committed_records(&self) -> Vec<Payload> {
            self.committed_records.lock().unwrap().clone()
        }

        /// Snapshot of every payload the sink reported as permanently
        /// rejected, paired with its reason string. Used by the
        /// dead-letter-contents test.
        fn dead_lettered_records(&self) -> Vec<(Payload, String)> {
            self.dead_lettered_records.lock().unwrap().clone()
        }

        /// Every `RecordId` the drain delivered, flattened across `commit()`
        /// calls in arrival order — deliberately NOT grouped by batch, because
        /// the whole point of the identity is that it does not depend on how
        /// the records were grouped.
        fn seen_record_ids(&self) -> Vec<RecordId> {
            self.seen_record_ids.lock().unwrap().clone()
        }

        /// Every `DedupToken` the drain delivered, one per `commit()` call.
        fn seen_dedup_tokens(&self) -> Vec<weir_sink_sdk::DedupToken> {
            self.seen_dedup_tokens.lock().unwrap().clone()
        }

        /// Hang the `nth` (1-based) `commit()` call forever — for hanging a
        /// *later* sub-batch (the first N succeed, then call N+1 times out).
        fn hanging_on_call(mut self, nth: u64) -> Self {
            self.hang_on_call = Some(nth);
            self
        }

        fn ok(batch: Vec<Payload>) -> MockResult {
            Ok(CommitResult::new(batch, vec![]))
        }

        fn ok_with_dead_letter(committed: Vec<Payload>, dead: Vec<Payload>) -> MockResult {
            Ok(CommitResult::new(
                committed,
                dead.into_iter().map(|p| (p, "rejected".into())).collect(),
            ))
        }
    }

    impl crate::sink::Sink for MockSink {
        type Error = MockError;

        async fn commit(&self, batch: SinkBatch) -> MockResult {
            // Capture the identity the drain supplied BEFORE consuming the
            // batch, and before any hang/panic arm — a call that the drain
            // later cancels still delivered these, and a test asserting what
            // reached the sink wants to see them.
            self.seen_dedup_tokens
                .lock()
                .unwrap()
                .push(*batch.dedup_token());
            if let Some(ids) = batch.record_ids() {
                self.seen_record_ids.lock().unwrap().extend_from_slice(ids);
            }
            let batch = batch.into_records();
            let nth = self.call_count.fetch_add(1, Ordering::Relaxed) + 1;
            self.call_timestamps.lock().unwrap().push(Instant::now());
            if nth <= self.panic_calls {
                panic!("mock sink: induced panic on commit #{nth}");
            }
            if let Some(poison) = self.panic_on_payload
                && batch.iter().any(|p| &p[..] == poison)
            {
                panic!("mock sink: induced panic on a poison payload (commit #{nth})");
            }
            if nth <= self.hang_calls || self.hang_on_call == Some(nth) {
                // Hang forever; the drain's backstop timeout cancels this future.
                std::future::pending::<()>().await;
            }
            if self.blocking_millis > 0 {
                // Deliberately NOT `.await`ed — a genuine OS-thread block, the
                // one shape `tokio::time::timeout` cannot cancel (see
                // `blocking_millis`).
                std::thread::sleep(Duration::from_millis(self.blocking_millis));
            }
            if !self.yield_for.is_zero() {
                tokio::time::sleep(self.yield_for).await;
            }
            let mut responses = self.responses.lock().unwrap();
            let response = responses
                .pop_front()
                .unwrap_or_else(|| Ok(CommitResult::new(batch.clone(), vec![])));
            // Capture for introspection BEFORE returning the response so
            // tests see the same view the drain is about to act on.
            if let Ok(ref result) = response {
                self.committed_records
                    .lock()
                    .unwrap()
                    .extend(result.committed.iter().cloned());
                self.dead_lettered_records
                    .lock()
                    .unwrap()
                    .extend(result.dead_lettered.iter().cloned());
            }
            response
        }

        fn max_batch_size(&self) -> usize {
            self.max_batch
        }

        async fn health(&self) -> SinkHealth {
            // Pop the next scripted health, else fall back to Healthy so sinks
            // built without a script behave exactly as before.
            self.health_script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(SinkHealth::Healthy)
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// The defect `segment_identity` exists to fix: every ShardWriter starts its
    /// counter at 1 and names files `seg_{counter:08}` inside its own shard dir,
    /// so the BASENAME repeats across shards. Hashing the basename alone gave two
    /// genuinely distinct records the same RecordId whenever shard_count > 1 and
    /// their payloads matched — which the HTTP sink sends as Idempotency-Key, so
    /// a correct endpoint drops one. Shipped in 2.0.3 and 2.0.4.
    #[test]
    fn record_ids_differ_across_shards_for_the_same_segment_number() {
        let a = Path::new("/var/lib/weir/wab/shard_00/seg_00000001.wab.sealed");
        let b = Path::new("/var/lib/weir/wab/shard_01/seg_00000001.wab.sealed");
        assert_eq!(
            a.file_name(),
            b.file_name(),
            "precondition: the basenames really do collide, which is the trap"
        );
        assert_ne!(
            segment_identity(a),
            segment_identity(b),
            "two shards' segment #1 must not share an identity"
        );

        let payload = Payload::from(b"heartbeat".as_ref());
        assert_ne!(
            RecordId::for_record(&segment_identity(a), 1, &payload),
            RecordId::for_record(&segment_identity(b), 1, &payload),
            "identical bytes at index 1 of each shard's segment #1 must not \
             produce the same idempotency key"
        );
    }

    /// The property that must survive the fix: a re-read of the same segment
    /// yields the same identity, so the drain's own retry still deduplicates.
    #[test]
    fn segment_identity_is_stable_for_the_same_segment() {
        let p = Path::new("/var/lib/weir/wab/shard_03/seg_00000042.wab.sealed");
        assert_eq!(segment_identity(p), segment_identity(p));
        assert_eq!(segment_identity(p), "shard_03/seg_00000042.wab.sealed");
    }

    /// A bare relative path (tests only) has no parent component; it must not
    /// panic or produce a leading separator.
    #[test]
    fn segment_identity_tolerates_a_bare_file_name() {
        assert_eq!(
            segment_identity(Path::new("seg_00000001.wab")),
            "seg_00000001.wab"
        );
    }

    /// Drains one sealed segment through a sink whose `max_batch_size` is
    /// `max_batch`, and returns the identity that sink observed: every
    /// `RecordId` in arrival order, plus one `DedupToken` per `commit()` call.
    ///
    /// `process_segment` neither deletes nor rewrites the segment (confirmation
    /// and unlink happen in the caller), so the same file can be drained twice
    /// at two different batch sizes — which is exactly the scenario an operator
    /// creates by editing `sink_max_batch_size` while a segment is unconfirmed.
    fn drain_identity(
        dir: &Path,
        segment: &Path,
        max_batch: usize,
        expect_records: u64,
    ) -> (Vec<RecordId>, Vec<weir_sink_sdk::DedupToken>) {
        use super::dead_letter::DeadLetterWriter;
        let sink = MockSink::with_batch_size(max_batch, []);
        let mut dl = DeadLetterWriter::open(dir).unwrap();
        let metrics = noop_metrics();
        let config = fast_config(dir.to_path_buf());
        let result = block_on(process_segment(
            segment, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Confirmed { record_count } if record_count == expect_records),
            "the segment must drain cleanly at max_batch_size={max_batch}, \
             confirming all {expect_records} records"
        );
        (sink.seen_record_ids(), sink.seen_dedup_tokens())
    }

    /// **The guarantee this branch exists to establish.** A sink must be able to
    /// recognise a record it has already committed even if `sink_max_batch_size`
    /// changed between the two attempts.
    ///
    /// The drain re-reads `sink.max_batch_size()` on every call, so an operator
    /// who edits the knob (or a redeployed pod carrying a different config)
    /// re-splits an unconfirmed segment at different boundaries on replay.
    /// Anything derived from the batch moves with those boundaries; the record's
    /// WAB coordinate does not. `RecordId` is built from that coordinate —
    /// `segment_identity` plus `read_index`, the record's ABSOLUTE ordinal
    /// within the segment — so the ids a sink sees are a property of the
    /// segment, not of the split.
    ///
    /// Falsified by changing `process_segment`'s
    /// `RecordId::for_record(&segment_name, read_index, &payload)` to a
    /// batch-relative index (`batch.len() as u64`), which is the plausible
    /// refactor — the index is already in scope three lines away. The final
    /// assertion then diverges at the fourth id: records 1-3 land in the first
    /// sub-batch under both splits and agree by accident, and everything after
    /// the size-3 boundary disagrees. A test that compared only the first
    /// record would have passed the broken drain.
    #[test]
    fn record_ids_survive_a_changed_sink_max_batch_size() {
        let dir = tmp_dir("rebatch_ids");
        let payloads: Vec<&[u8]> = vec![
            b"r01", b"r02", b"r03", b"r04", b"r05", b"r06", b"r07", b"r08", b"r09", b"r10",
        ];
        let segment = make_sealed_segment(&dir, 0, &payloads);

        let (ids_at_3, tokens_at_3) = drain_identity(&dir, &segment, 3, 10);
        let (ids_at_7, tokens_at_7) = drain_identity(&dir, &segment, 7, 10);

        // Precondition: the boundaries really did move. Ten records split four
        // ways at size 3 (3+3+3+1) and two ways at size 7 (7+3). Without this
        // the test would also pass against a drain that ignored max_batch_size.
        assert_eq!(tokens_at_3.len(), 4, "expected 4 sub-batches at size 3");
        assert_eq!(tokens_at_7.len(), 2, "expected 2 sub-batches at size 7");

        assert_eq!(
            ids_at_3.len(),
            10,
            "every record must arrive with an id — the drain never sends a batch without them"
        );
        assert_eq!(
            ids_at_3, ids_at_7,
            "the per-record identity must not depend on how the drain grouped the \
             records: a sink that committed these at sink_max_batch_size=3 has to \
             recognise the same records at 7, or a replay double-charges"
        );
    }

    /// The companion, pinning a limitation that is CORRECT and must not be
    /// "fixed" by making the token segment-scoped.
    ///
    /// A `DedupToken` names a batch. Re-batching produces different batches, so
    /// it produces different tokens — that is what batch-scoped means, and no
    /// reformulation of the digest changes it. Making the token segment-scoped
    /// instead would hand every sub-batch of a segment the same value, and a
    /// dedup-capable sink would discard sub-batches 2..N as duplicates: silent
    /// LOSS caused by the feature meant to prevent duplication. (That is the
    /// argument recorded in the 2026-08-09 design spec, §3.1.)
    ///
    /// **If you are here because this test failed:** you have changed the
    /// token's scope. Do not relax the assertion. The batch-size-stable
    /// idempotency key is `SinkBatch::record_ids()`, pinned by the test above;
    /// a sink that needs re-batch survival keys on that instead.
    #[test]
    fn the_dedup_token_deliberately_does_not_survive_a_changed_batch_size() {
        let dir = tmp_dir("rebatch_token");
        let payloads: Vec<&[u8]> = vec![
            b"r01", b"r02", b"r03", b"r04", b"r05", b"r06", b"r07", b"r08", b"r09", b"r10",
        ];
        let segment = make_sealed_segment(&dir, 0, &payloads);

        let (_, tokens_at_3) = drain_identity(&dir, &segment, 3, 10);
        let (_, tokens_at_7) = drain_identity(&dir, &segment, 7, 10);

        assert_ne!(
            tokens_at_3, tokens_at_7,
            "a batch token is a function of the batch; if re-batching left it \
             unchanged the token would no longer distinguish sub-batches"
        );
        // Sharper than length inequality: even the FIRST sub-batch differs,
        // because three records and seven records are different batches. There
        // is no shared prefix a downstream could match on.
        assert_ne!(
            tokens_at_3[0], tokens_at_7[0],
            "not even the first sub-batch survives a re-split"
        );
    }

    /// Ids for a test batch, at a synthetic coordinate. `commit_batch` only
    /// forwards these, so the exact segment name is immaterial — what matters is
    /// that the count matches, which `SinkBatch::with_segment_context` enforces.
    fn test_record_ids(payloads: &[Payload]) -> Vec<RecordId> {
        payloads
            .iter()
            .enumerate()
            .map(|(i, p)| RecordId::for_record("test-segment.wab", i as u64, p))
            .collect()
    }

    fn noop_metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    fn segments_confirmed(m: &Metrics) -> u64 {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        m.wab_segments
            .get_or_create(&SegmentStateLabel {
                state: SegmentState::confirmed,
            })
            .get()
    }

    fn segments_quarantined_family(m: &Metrics) -> u64 {
        use crate::metrics::{SegmentState, SegmentStateLabel};
        m.wab_segments
            .get_or_create(&SegmentStateLabel {
                state: SegmentState::quarantined,
            })
            .get()
    }

    #[test]
    fn drain_quarantine_bumps_both_counters_like_every_other_site() {
        // `wab::recovery::quarantine_and_count`'s docstring asserts the
        // invariant: "Every segment-quarantine site funnels through here so the
        // recovery_segments_quarantined counter and the
        // wab_segments{state=quarantined} gauge are ALWAYS bumped ... If you
        // add a third, route it here."
        //
        // `quarantine_segment` IS that third site, and it called the bare
        // `quarantine()`, bumping only the flat counter. So a drain-time
        // quarantine moved one series and a recovery-time quarantine moved two,
        // and the two silently diverged — with no way to tell from the metrics
        // whether the gap meant "drain quarantined something" or "a counter is
        // broken".
        let wab_dir = tmp_dir("quarantine_both_counters");
        let seg = wab_dir.join("shard_00__seg_00000007.wab.sealed");
        std::fs::write(&seg, b"corrupt").unwrap();

        let metrics = noop_metrics();
        let config = fast_config(wab_dir.clone());

        let before_flat = metrics.recovery_segments_quarantined.get();
        let before_family = segments_quarantined_family(&metrics);

        assert!(quarantine_segment(&seg, &config, &metrics, "test"));

        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            before_flat + 1,
            "the flat counter must move"
        );
        assert_eq!(
            segments_quarantined_family(&metrics),
            before_family + 1,
            "wab_segments{{state=quarantined}} must move too, or the two series diverge"
        );
    }

    fn records_committed(m: &Metrics) -> u64 {
        m.sink_commit_records
            .get_or_create(&OutcomeLabel {
                outcome: Outcome::committed,
            })
            .get()
    }

    fn records_dead_lettered(m: &Metrics) -> u64 {
        m.sink_commit_records
            .get_or_create(&OutcomeLabel {
                outcome: Outcome::dead_lettered,
            })
            .get()
    }

    fn records_retried(m: &Metrics) -> u64 {
        m.sink_commit_records
            .get_or_create(&OutcomeLabel {
                outcome: Outcome::retried,
            })
            .get()
    }

    fn dl_full_count(m: &Metrics) -> u64 {
        m.dead_letter_full.get()
    }

    fn drain_panics(m: &Metrics) -> u64 {
        m.drain_panics.get()
    }

    fn drain_is_blocked(m: &Metrics) -> bool {
        m.drain_state
            .get_or_create(&DrainStateLabel {
                state: DrainStateValue::blocked_dead_letter_full,
            })
            .get()
            == 1.0
    }

    fn drain_state_of(m: &Metrics, state: DrainStateValue) -> f64 {
        m.drain_state
            .get_or_create(&DrainStateLabel { state })
            .get()
    }

    /// Asserts the drain gauge reports the terminal state — i.e. the supervisor
    /// told the truth on its way out instead of leaving `draining` frozen at 1.
    fn assert_drain_reports_stopped(m: &Metrics, context: &str) {
        assert_eq!(
            drain_state_of(m, DrainStateValue::stopped),
            1.0,
            "{context}: weir_drain_state{{state=\"stopped\"}} must be 1 once the \
             supervisor has exited"
        );
        assert_eq!(
            drain_state_of(m, DrainStateValue::draining),
            0.0,
            "{context}: weir_drain_state{{state=\"draining\"}} must be 0 once the \
             supervisor has exited — a frozen 1 tells operators (and the WAB \
             growth warning) that delivery is still happening"
        );
    }

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = crate::testutil::scratch_dir(&format!("drain_{label}"));
        crate::testutil::mkdir_p(&dir);
        dir
    }

    fn make_sealed_segment(dir: &Path, shard_id: u16, payloads: &[&[u8]]) -> PathBuf {
        // Put sealed segments in a shard subdirectory so confirmed_path works.
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let path = segment_path(&shard_dir, 1);
        let mut seg = WabSegment::create(&path, shard_id, Compression::None).unwrap();
        for p in payloads {
            seg.write_record(p).unwrap();
        }
        seg.seal().unwrap()
    }

    fn fast_config(wab_dir: PathBuf) -> DrainConfig {
        DrainConfig {
            wab_dir,
            dead_letter_max_bytes: 1024 * 1024,
            dead_letter_check_interval: Duration::from_millis(10),
            base_retry_delay: Duration::from_millis(1),
            max_retries: MAX_RETRIES,
            max_respawns: MAX_DRAIN_RESPAWNS,
            commit_timeout: Duration::from_secs(30),
            // Short so the stranded-segment recovery rescan fires promptly in tests.
            health_poll_interval: Duration::from_millis(50),
            health_probe_timeout: Duration::from_secs(5),
            // As before the wab_dir lock: orphans are kept unless a test opts in.
            orphan_cleanup: false,
            // The count alone decides, so the escalation tests run in
            // milliseconds; `min_age_config` opts into a real min age.
            local_read_escalation_min_age: Duration::ZERO,
        }
    }

    fn tight_dl_config(wab_dir: PathBuf, max_bytes: u64) -> DrainConfig {
        DrainConfig {
            dead_letter_max_bytes: max_bytes,
            dead_letter_check_interval: Duration::from_millis(10),
            ..fast_config(wab_dir)
        }
    }

    /// Runs the drain until drain_tx is dropped, then joins.
    fn run_drain<S: Sink + 'static>(
        drain_rx: crossbeam_channel::Receiver<PathBuf>,
        drain_tx: crossbeam_channel::Sender<PathBuf>,
        sink: Arc<S>,
        config: DrainConfig,
        metrics: Arc<Metrics>,
    ) {
        // Most tests don't care about the heartbeat, so `run_drain` builds its
        // own throwaway one rather than making every one of its many callers
        // thread it through. Tests that DO care (the heartbeat tests below)
        // call `spawn`/`drain_thread` directly with their own instance.
        let handle = spawn(drain_rx, sink, config, metrics, DrainHeartbeat::new());
        drop(drain_tx);
        handle.join().unwrap();
    }

    fn get_confirmed_path(sealed: &Path) -> PathBuf {
        super::confirmed::confirmed_path(sealed)
    }

    // ── Ledger-era call shapes ────────────────────────────────────────────────
    //
    // `drain_thread` now borrows the supervisor-owned `SegmentLedger`, and
    // `probe_and_resume_stranded` takes the ledger's rescan exclusion. These
    // shadow the glob-imported originals with their previous call shape, so
    // every test written before the ledger runs unchanged against a fresh
    // ledger (no exclusions) — exactly what a first-start drain has. Tests that
    // need to seed or inspect the ledger call `super::drain_thread` directly.

    fn drain_thread<S: Sink>(
        drain_rx: crossbeam_channel::Receiver<PathBuf>,
        sink: Arc<S>,
        config: DrainConfig,
        metrics: Arc<Metrics>,
        initial_health_ok: bool,
        heartbeat: DrainHeartbeat,
    ) {
        let mut ledger = fresh_ledger();
        super::drain_thread(
            drain_rx,
            sink,
            config,
            metrics,
            initial_health_ok,
            heartbeat,
            &mut ledger,
        );
    }

    fn probe_and_resume_stranded<S: Sink>(
        rt: &tokio::runtime::Runtime,
        sink: &S,
        config: &DrainConfig,
        metrics: &Metrics,
        pending: &mut VecDeque<PathBuf>,
        prev_health_ok: bool,
        in_flight: Option<&Path>,
    ) -> bool {
        super::probe_and_resume_stranded(
            rt,
            sink,
            config,
            metrics,
            pending,
            prev_health_ok,
            in_flight,
            &|_| false,
            &DrainHeartbeat::new(),
        )
    }

    /// `process_segment` gained `last_local_read_chance` (spec §4.2); every test
    /// written before it runs a segment with no prior local read failures,
    /// which is `false`. Tests of the escalating attempt call
    /// `super::process_segment` directly.
    async fn process_segment<S: Sink>(
        segment: &Path,
        sink: &S,
        config: &DrainConfig,
        metrics: &Metrics,
        dead_letter: &mut DeadLetterWriter,
        skip: u64,
    ) -> ProcessResult {
        super::process_segment(
            segment,
            sink,
            config,
            metrics,
            dead_letter,
            skip,
            false,
            &DrainHeartbeat::new(),
        )
        .await
    }

    /// `commit_batch` gained the drain heartbeat (spec §4.8); tests that drive
    /// one sub-batch directly do not observe it.
    async fn commit_batch<S: Sink>(
        payloads: &[Payload],
        record_ids: &[RecordId],
        segment_created_at: i64,
        sink: &S,
        config: &DrainConfig,
        metrics: &Metrics,
        dead_letter: &mut DeadLetterWriter,
    ) -> BatchResult {
        super::commit_batch(
            payloads,
            record_ids,
            segment_created_at,
            sink,
            config,
            metrics,
            dead_letter,
            &DrainHeartbeat::new(),
        )
        .await
    }

    /// `quarantine_segment` gained the drain heartbeat (spec §4.8).
    fn quarantine_segment(
        segment: &Path,
        config: &DrainConfig,
        metrics: &Metrics,
        reason: &'static str,
    ) -> bool {
        super::quarantine_segment(segment, config, metrics, reason, &DrainHeartbeat::new())
    }

    /// `enter_blocked` gained `needed` (spec §4.5); the episode tests predate
    /// it and do not depend on it.
    fn enter_blocked(
        segment: PathBuf,
        metrics: &Metrics,
        block_episode: &mut Option<Instant>,
        processed: u64,
    ) -> DrainState {
        super::enter_blocked(segment, metrics, block_episode, processed, 0)
    }

    fn fresh_ledger() -> SegmentLedger {
        SegmentLedger::new(Arc::new(ledger::LedgerCounts::default()))
    }

    fn local_io_strands(m: &Metrics, cause: crate::metrics::LocalIoStrandCause) -> u64 {
        m.drain_local_io_strands
            .get_or_create(&crate::metrics::LocalIoStrandLabel { cause })
            .get()
    }

    fn parked_gauge(m: &Metrics, reason: crate::metrics::ParkedReason) -> f64 {
        m.drain_segments_parked
            .get_or_create(&crate::metrics::ParkedLabel { reason })
            .get()
    }

    // ── SegmentLedger: one admission filter on both sources (spec §4.0) ──────

    /// The double-queue hole. The crossbeam channel is not enumerable, so the
    /// same path can sit in the channel AND be found on disk by the idle rescan.
    /// Before the ledger that was harmless only because a delivered segment was
    /// always deleted; any state that keeps a delivered segment on disk (here:
    /// its `.confirmed` sidecar cannot be written) turned every channel copy and
    /// every idle rescan into a full re-delivery.
    ///
    /// The ledger owns such a segment (`confirm_pending`, spec §4.3: delivered,
    /// sidecar outstanding), and both sources must respect that: the second
    /// channel copy is dropped at admission, and the rescan skips the path
    /// before `check_confirmed` can look at it.
    #[test]
    fn same_path_in_channel_and_rescan_is_delivered_once() {
        let dir = tmp_dir("admit_once");
        let sealed = make_sealed_segment(&dir, 0, &[b"once"]);
        // The sidecar write fails: a directory sits where `.confirmed` goes, so
        // the atomic write's rename fails (EISDIR) and the delivered segment
        // stays sealed and unconfirmed on disk.
        crate::testutil::mkdir_p(get_confirmed_path(&sealed));

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        // A second copy of the same path, as the channel can hold after a
        // rescan re-queued what a flusher had also sent.
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let mut ledger = fresh_ledger();
        let config = fast_config(dir.clone()); // 50 ms health poll + idle rescan
        let (sink_t, metrics_t) = (Arc::clone(&sink), Arc::clone(&metrics));
        let handle = std::thread::spawn(move || {
            super::drain_thread(
                rx,
                sink_t,
                config,
                metrics_t,
                true,
                DrainHeartbeat::new(),
                &mut ledger,
            );
            ledger
        });

        // Many idle rescans: each would find the segment unconfirmed on disk.
        std::thread::sleep(Duration::from_millis(500));
        drop(tx);
        let ledger = handle.join().unwrap();

        assert_eq!(
            sink.call_count(),
            1,
            "a ledger-owned segment must reach the sink once: neither the channel \
             copy nor the idle rescan may re-deliver it"
        );
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            0,
            "the rescan must exclude a ledger-owned path, not re-queue it"
        );
        assert!(sealed.exists(), "the segment's bytes stay on disk");
        assert_eq!(
            metrics.recovery_segments_quarantined.get(),
            0,
            "the rescan must skip the path before check_confirmed, never quarantine it"
        );
        assert_eq!(
            ledger.confirm_stage(&sealed),
            Some(ledger::ConfirmStage::Sidecar),
            "the ledger owns the delivered segment until its sidecar lands"
        );
        assert_eq!(
            ledger
                .counts()
                .confirm_pending
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            confirm_pending_gauge(&metrics),
            1.0,
            "the drain publishes the ledger's confirm_pending tally to the gauge"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// F05 across a strand. A segment of five 1-record batches commits batches
    /// 1-2, then batch 3 fails transiently for longer than `max_retries`, so the
    /// segment strands. When the sink recovers, the rescan re-queues the path
    /// and admission resumes it past the committed prefix: the sink never sees
    /// batches 1-2 again.
    #[test]
    fn cursor_survives_a_strand_within_the_run() {
        let dir = tmp_dir("cursor_strand");
        let records: [&[u8]; 5] = [b"r1", b"r2", b"r3", b"r4", b"r5"];
        let sealed = make_sealed_segment(&dir, 0, &records);
        let payload = |r: &[u8]| Payload::from(r.to_vec());

        let mut responses: Vec<MockResult> = vec![
            MockSink::ok(vec![payload(records[0])]),
            MockSink::ok(vec![payload(records[1])]),
        ];
        responses.extend((0..=MAX_RETRIES).map(|_| Err(MockError::Transient)));
        // Then responses run out and the sink commits whatever it is given.
        let sink = Arc::new(MockSink::with_batch_size(1, responses));
        let metrics = noop_metrics();

        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !get_confirmed_path(&sealed).exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        handle.join().unwrap();

        assert_eq!(metrics.drain_segments_stranded.get(), 1, "one strand");
        assert!(
            get_confirmed_path(&sealed).exists(),
            "the resumed segment is delivered and confirmed"
        );
        let all: Vec<Payload> = records.iter().map(|r| payload(r)).collect();
        assert_eq!(
            sink.committed_records(),
            all,
            "each record committed exactly once, in order"
        );
        let ids = sink.seen_record_ids();
        for (i, r) in records.iter().enumerate().take(2) {
            let id = RecordId::for_record(&segment_identity(&sealed), i as u64 + 1, &payload(r));
            assert_eq!(
                ids.iter().filter(|seen| **seen == id).count(),
                1,
                "batch {} was committed before the strand and must never reach the \
                 sink again",
                i + 1
            );
        }
        assert_eq!(
            sink.call_count(),
            2 + u64::from(MAX_RETRIES + 1) + 3,
            "2 committed + {} failed attempts at batch 3 + batches 3-5 after resume",
            MAX_RETRIES + 1
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// D11: a strand caused by reading the segment is not a sink strand. It
    /// counts in `weir_drain_local_io_strands_total{cause="read"}`, and the
    /// sink-oriented `weir_drain_segments_stranded_total` (whose help text sends
    /// the operator to the sink) stays at 0.
    ///
    /// A directory at the sealed path opens but cannot be read (EISDIR), which
    /// works even as root.
    #[test]
    fn a_local_read_strand_is_not_counted_as_a_sink_strand() {
        let dir = tmp_dir("local_read_strand");
        let shard_dir = dir.join("shard_00");
        let sealed = segment_path(&shard_dir, 1).with_extension("wab.sealed");
        crate::testutil::mkdir_p(&sealed);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let config = DrainConfig {
            // No rescan inside the test: it would re-find the "segment".
            health_poll_interval: Duration::from_secs(3600),
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, Arc::clone(&sink), config, Arc::clone(&metrics));

        assert_eq!(
            sink.call_count(),
            0,
            "nothing was readable, nothing delivered"
        );
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            1
        );
        assert_eq!(
            metrics.drain_segments_stranded.get(),
            0,
            "a local read failure must not be reported as a sink strand"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// The other local cause: the sink committed (here: rejected permanently),
    /// but writing the rejected records to `dead_letter/` failed, so the drain
    /// may not advance. That is the WAB device, not the sink.
    #[test]
    fn a_dead_letter_write_strand_is_not_counted_as_a_sink_strand() {
        let dir = tmp_dir("local_write_strand");
        let sealed = make_sealed_segment(&dir, 0, &[b"rejected"]);
        let responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Permanent))
            .collect();
        let sink = Arc::new(MockSink::with_responses(responses));
        let metrics = noop_metrics();
        let config = DrainConfig {
            health_poll_interval: Duration::from_secs(3600),
            ..fast_config(dir.clone())
        };

        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        // The drain opens dead_letter/ at start. Swap it for a regular file so
        // every dead-letter write fails (ENOTDIR).
        let dl = dir.join("dead_letter");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dl.is_dir() {
            assert!(Instant::now() < deadline, "drain never opened dead_letter/");
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(20));
        std::fs::remove_dir_all(&dl).unwrap();
        std::fs::write(&dl, b"not a directory").unwrap();
        tx.send(sealed.clone()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::write) == 0
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::write),
            1
        );
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        assert!(
            sealed.exists(),
            "the records were never re-homed; keep them"
        );
        assert!(!get_confirmed_path(&sealed).exists());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn transient_results_carry_their_cause() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("transient_cause");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let payloads = vec![Payload::from(b"r1".as_ref())];
        let ids = test_record_ids(&payloads);

        let sink = MockSink::with_responses([Err(MockError::Transient)]);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(matches!(
            result,
            BatchResult::Transient {
                cause: TransientCause::Sink,
                ..
            }
        ));

        squat_dead_letter_dir(&dir);
        let sink = MockSink::with_responses([Err(MockError::Permanent)]);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(matches!(
            result,
            BatchResult::Transient {
                cause: TransientCause::LocalWrite,
                ..
            }
        ));

        // An unreadable segment at open: a local read, classified by errno.
        let not_a_file = dir.join("shard_00").join("seg_00000009.wab.sealed");
        crate::testutil::mkdir_p(&not_a_file);
        let result = block_on(process_segment(
            &not_a_file,
            &sink,
            &config,
            &metrics,
            &mut dl,
            0,
        ));
        assert!(
            matches!(
                result,
                ProcessResult::Transient {
                    cause: TransientCause::LocalRead {
                        class: ErrClass::Access,
                        ..
                    },
                    ..
                }
            ),
            "a directory at the sealed path is an Access-class local read"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── A corrupt or newer-format header at open (spec §4.1) ─────────────────

    /// A sealed path whose bytes are whatever the test writes, in `shard_00`.
    fn write_raw_sealed(dir: &Path, counter: u64, bytes: &[u8]) -> PathBuf {
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let sealed = segment_path(&shard_dir, counter).with_extension("wab.sealed");
        std::fs::write(&sealed, bytes).unwrap();
        sealed
    }

    /// Where the drain's quarantine puts `sealed` (shard-prefixed name).
    fn quarantined_copy(dir: &Path, sealed: &Path) -> PathBuf {
        let shard = sealed
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy();
        let name = sealed.file_name().unwrap().to_string_lossy();
        dir.join("quarantine").join(format!("{shard}__{name}"))
    }

    /// Polls `cond` for up to `secs`, panicking with `what` if it never holds.
    fn wait_for(secs: u64, what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Bad magic is corruption, not a transient read: it can never succeed on a
    /// retry. Before §4.1 it was retried `max_retries+1` times, stranded, and
    /// re-queued by every rescan for the life of the process (and replayed on
    /// every restart, because recovery only looks at `.wab`). Now it is
    /// quarantined on the FIRST attempt, never counted as a strand of either
    /// kind, and never opened again.
    #[test]
    fn a_bad_magic_header_is_quarantined_on_the_first_attempt() {
        let dir = tmp_dir("hdr_bad_magic");
        let bytes = b"NOTWEIR-not-a-valid-segment-header-xxxx";
        let sealed = write_raw_sealed(&dir, 1, bytes);
        open_hook::watch(&sealed);

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()), // 50 ms idle rescan
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        let q = quarantined_copy(&dir, &sealed);
        wait_for(5, "the corrupt segment in quarantine/", || q.exists());
        // Several idle rescans: none may find (or re-open) it.
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            open_hook::attempts(&sealed),
            1,
            "opened once, never retried"
        );
        assert!(!sealed.exists(), "moved out of the shard dir");
        assert_eq!(
            std::fs::read(&q).unwrap(),
            bytes,
            "the bytes are kept intact"
        );
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            0,
            "corruption is not a local I/O strand either"
        );
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        assert_eq!(sink.call_count(), 0);
        open_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Every header the reader rejects as `InvalidData` that is NOT a newer
    /// format is corruption: bad magic (even when byte 4 happens to look like a
    /// big version number), version 0, a v1 header with its reserved byte set,
    /// and a file too short to hold a header. A newer format is either a version
    /// above the max OR a v2 header with a flags bit this build does not know —
    /// the flags byte is where a later weir adds a feature without bumping the
    /// version (`weir_wab::format::KNOWN_FLAGS`), so both park.
    #[test]
    fn corrupt_header_shapes_quarantine_and_a_newer_version_parks() {
        use super::dead_letter::DeadLetterWriter;
        use weir_wab::format::{FLAG_ZSTD, FORMAT_VERSION_MAX_SUPPORTED, FORMAT_VERSION_V2};
        let dir = tmp_dir("hdr_shapes");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        let good = make_sealed_segment(&dir, 0, &[b"x"]);
        let good_bytes = std::fs::read(&good).unwrap();
        std::fs::remove_file(&good).unwrap();
        assert_eq!(good_bytes[4], 1, "fixture is a v1 segment");
        let with_byte4 = |v: u8| {
            let mut b = good_bytes.clone();
            b[4] = v;
            b
        };
        let with_v2_flags = |flags: u8| {
            let mut b = with_byte4(FORMAT_VERSION_V2);
            b[5] = flags;
            b
        };

        let cases: [(&str, Vec<u8>); 7] = [
            // 'W' (0x57) at byte 4 — a version byte far above the max, but the
            // magic is wrong, so this is not a newer weir's segment.
            (
                "bad magic",
                b"NOTWEIR-not-a-valid-header-xxxxxxxxxxxx".to_vec(),
            ),
            ("version 0", with_byte4(0)),
            ("empty", Vec::new()),
            ("short, magic only", b"WEI".to_vec()),
            ("short, magic + version", good_bytes[..6].to_vec()),
            // v1 documents byte 5 as zero-on-write: a set bit is corruption,
            // not a feature.
            ("v1 with a flags bit set", {
                let mut b = good_bytes.clone();
                b[5] = 0b0000_0010;
                b
            }),
            // A v2 header is a fixed 24 bytes; a file too short to hold one is
            // corrupt whatever its flags byte says.
            (
                "short v2 with an unknown flag",
                with_v2_flags(0b0000_0010)[..8].to_vec(),
            ),
        ];
        for (i, (what, bytes)) in cases.into_iter().enumerate() {
            let sealed = write_raw_sealed(&dir, 10 + i as u64, &bytes);
            let result = block_on(process_segment(
                &sealed, &sink, &config, &metrics, &mut dl, 0,
            ));
            assert!(
                matches!(result, ProcessResult::Quarantined),
                "{what}: a corrupt header is quarantined"
            );
            assert!(!sealed.exists(), "{what}: moved out of the shard dir");
            assert_eq!(
                std::fs::read(quarantined_copy(&dir, &sealed)).unwrap(),
                bytes,
                "{what}: bytes kept"
            );
        }

        let newer_cases: [(&str, Vec<u8>); 3] = [
            (
                "version above the max",
                with_byte4(FORMAT_VERSION_MAX_SUPPORTED + 1),
            ),
            ("v2 with an unknown flag bit", with_v2_flags(0b0000_0010)),
            (
                "v2 with zstd plus an unknown flag bit",
                with_v2_flags(FLAG_ZSTD | 0b1000_0000),
            ),
        ];
        for (i, (what, bytes)) in newer_cases.into_iter().enumerate() {
            let newer = write_raw_sealed(&dir, 20 + i as u64, &bytes);
            let result = block_on(process_segment(
                &newer, &sink, &config, &metrics, &mut dl, 0,
            ));
            assert!(
                matches!(
                    result,
                    ProcessResult::Parked {
                        reason: ParkReason::UnsupportedVersion
                    }
                ),
                "{what}: a newer format is parked, not quarantined"
            );
            assert!(newer.exists(), "{what}: a parked segment stays where it is");
            assert!(
                !quarantined_copy(&dir, &newer).exists(),
                "{what}: not moved to quarantine"
            );
            assert_eq!(
                std::fs::read(&newer).unwrap(),
                bytes,
                "{what}: bytes untouched"
            );
        }
        assert_eq!(sink.call_count(), 0);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A segment a NEWER weir wrote is not corrupt. Quarantining it would break
    /// a rollback: a downgraded daemon would push every newer-format segment
    /// into manual recovery. It is parked instead — left in place, not
    /// re-queued this run, and replayed on the next start, when a newer binary
    /// delivers it.
    #[test]
    fn a_newer_format_version_is_parked_not_moved() {
        use weir_wab::format::FORMAT_VERSION_MAX_SUPPORTED;
        let dir = tmp_dir("hdr_newer");
        let sealed = make_sealed_segment(&dir, 0, &[b"from the future"]);
        let mut bytes = std::fs::read(&sealed).unwrap();
        bytes[4] = FORMAT_VERSION_MAX_SUPPORTED + 1;
        std::fs::write(&sealed, &bytes).unwrap();
        open_hook::watch(&sealed);

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(5, "the unsupported_version park", || {
            parked_gauge(&metrics, crate::metrics::ParkedReason::unsupported_version) == 1.0
        });
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            open_hook::attempts(&sealed),
            1,
            "parked, so never re-queued"
        );
        assert_eq!(
            std::fs::read(&sealed).unwrap(),
            bytes,
            "left in place, untouched"
        );
        assert!(!quarantined_copy(&dir, &sealed).exists(), "not quarantined");
        assert!(!get_confirmed_path(&sealed).exists(), "not confirmed");
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            0
        );
        assert_eq!(sink.call_count(), 0);
        open_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Access class (spec §4.0 table): a directory at the sealed path opens but
    /// every read fails with EISDIR — which works even as root in CI. The
    /// failure counts toward escalation, and once it reaches
    /// `(max_retries+1) × LOCAL_READ_STRAND_LIMIT` attempts (three strand
    /// cycles) the segment is parked as `local_read_failed`: not re-queued by
    /// any later rescan, and never counted as a sink strand.
    #[cfg(unix)]
    #[test]
    fn an_access_class_read_failure_parks_after_three_strand_cycles() {
        let dir = tmp_dir("hdr_eisdir");
        let shard_dir = dir.join("shard_00");
        let sealed = segment_path(&shard_dir, 1).with_extension("wab.sealed");
        crate::testutil::mkdir_p(&sealed);
        open_hook::watch(&sealed);

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()), // the 50 ms idle rescan re-queues each strand
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(10, "the local_read_failed park", || {
            parked_gauge(&metrics, crate::metrics::ParkedReason::local_read_failed) == 1.0
        });
        // Many idle rescans after the park: none may re-queue it.
        std::thread::sleep(Duration::from_millis(400));
        drop(tx);
        handle.join().unwrap();

        let per_cycle = u64::from(MAX_RETRIES + 1);
        assert_eq!(
            open_hook::attempts(&sealed),
            per_cycle * u64::from(LOCAL_READ_STRAND_LIMIT),
            "three cycles of attempts, then parked and never opened again"
        );
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            u64::from(LOCAL_READ_STRAND_LIMIT - 1),
            "the first two cycles strand; the third ends in the park"
        );
        assert_eq!(
            metrics.drain_segments_stranded.get(),
            0,
            "a local read failure is never a sink strand"
        );
        assert!(sealed.is_dir(), "a parked path is left where it is");
        assert!(!quarantined_copy(&dir, &sealed).exists());
        assert_eq!(sink.call_count(), 0);
        open_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Process class (spec §4.0 table): EMFILE is daemon-wide descriptor
    /// pressure, not a property of the segment. It never counts toward
    /// escalation, however long it lasts: the segments keep stranding and
    /// resuming exactly as before, nothing is parked or quarantined, and every
    /// segment delivers once the pressure clears.
    #[cfg(unix)]
    #[test]
    fn a_process_class_open_failure_never_parks_and_heals() {
        const CYCLES: u32 = 30;
        let dir = tmp_dir("hdr_emfile");
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let mk = |counter: u64, payload: &[u8]| {
            let mut s =
                WabSegment::create(&segment_path(&shard_dir, counter), 0, Compression::None)
                    .unwrap();
            s.write_record(payload).unwrap();
            s.seal().unwrap()
        };
        let segs = [mk(1, b"first"), mk(2, b"second")];
        let failing = CYCLES * (MAX_RETRIES + 1);
        for seg in &segs {
            open_hook::fail(seg, libc::EMFILE, failing);
        }

        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        for seg in &segs {
            tx.send(seg.clone()).unwrap();
        }
        wait_for(60, "both segments confirmed once EMFILE clears", || {
            segs.iter().all(|s| get_confirmed_path(s).exists())
        });
        drop(tx);
        handle.join().unwrap();

        for seg in &segs {
            assert_eq!(
                open_hook::attempts(seg),
                u64::from(failing) + 1,
                "{} failing attempts, then the one that delivers",
                failing
            );
            open_hook::forget(seg);
        }
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            u64::from(CYCLES) * segs.len() as u64,
            "every cycle strands and resumes, as before"
        );
        for reason in [
            crate::metrics::ParkedReason::local_read_failed,
            crate::metrics::ParkedReason::unsupported_version,
            crate::metrics::ParkedReason::drain_panic,
        ] {
            assert_eq!(parked_gauge(&metrics, reason), 0.0, "nothing parked");
        }
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        let mut committed: Vec<Vec<u8>> = sink
            .committed_records()
            .iter()
            .map(|p| p.to_vec())
            .collect();
        committed.sort();
        assert_eq!(
            committed,
            vec![b"first".to_vec(), b"second".to_vec()],
            "everything delivers exactly once after the hook clears"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// `fast_config` with a real escalation min age, for the tests that
    /// exercise it; every other test escalates on the count alone.
    fn min_age_config(dir: PathBuf, min_age: Duration) -> DrainConfig {
        DrainConfig {
            local_read_escalation_min_age: min_age,
            ..fast_config(dir)
        }
    }

    /// conf-regressions-8: a WAB-device blip (a remount, a `chown -R`, an
    /// iSCSI path flap) that fails a segment's reads for MORE than three strand
    /// cycles but less than `LOCAL_READ_ESCALATION_MIN_AGE` is not escalated.
    /// The count alone used to quarantine (`Media`) or park until restart
    /// (`Access`) about a minute into such a blip at the defaults, where 4.0.1
    /// retried and recovered by itself. The segment keeps cycling and delivers,
    /// whole and once, as soon as the fault clears.
    #[cfg(unix)]
    #[test]
    fn a_local_read_blip_past_three_strand_cycles_but_under_the_min_age_heals() {
        const MIN_AGE: Duration = Duration::from_secs(5);
        const CYCLES: u32 = LOCAL_READ_STRAND_LIMIT + 2;
        for (errno, label) in [(libc::EIO, "media"), (libc::EACCES, "access")] {
            let dir = tmp_dir(&format!("read_blip_{label}"));
            let (sealed, records) = make_numbered_segment(&dir, 3);
            let failing = CYCLES * (MAX_RETRIES + 1);
            assert!(failing > (MAX_RETRIES + 1) * LOCAL_READ_STRAND_LIMIT);
            open_hook::fail(&sealed, errno, failing);

            let sink = Arc::new(MockSink::with_responses([]));
            let metrics = noop_metrics();
            let (tx, rx) = crossbeam_channel::unbounded();
            let handle = spawn(
                rx,
                Arc::clone(&sink),
                min_age_config(dir.clone(), MIN_AGE),
                Arc::clone(&metrics),
                DrainHeartbeat::new(),
            );
            let started = Instant::now();
            tx.send(sealed.clone()).unwrap();
            wait_for(10, "the segment confirmed once the blip clears", || {
                get_confirmed_path(&sealed).exists()
            });
            assert!(
                started.elapsed() < MIN_AGE,
                "{label}: the blip outlasted the min age, so this run proves nothing"
            );
            drop(tx);
            handle.join().unwrap();

            assert_eq!(
                open_hook::attempts(&sealed),
                u64::from(failing) + 1,
                "{label}: every failing attempt, then the one that delivers"
            );
            assert_eq!(
                local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
                u64::from(CYCLES),
                "{label}: each failing cycle strands and resumes"
            );
            assert_eq!(
                parked_gauge(&metrics, crate::metrics::ParkedReason::local_read_failed),
                0.0,
                "{label}: not parked"
            );
            assert_eq!(metrics.recovery_segments_quarantined.get(), 0, "{label}");
            assert!(!quarantined_copy(&dir, &sealed).exists(), "{label}");
            assert_eq!(committed_bytes(&sink), records, "{label}: whole, once");
            open_hook::forget(&sealed);
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// conf-regressions-8, the other side: a fault that persists past BOTH
    /// thresholds (three strand cycles and the min age since the segment's
    /// first local read failure) escalates by class, exactly as before: a
    /// `Media` error at open or mid-segment quarantines (mid-segment after
    /// delivering the readable prefix once), an `Access` error parks. Not
    /// before the min age, and never again after.
    #[cfg(unix)]
    #[test]
    fn a_local_read_fault_past_both_thresholds_escalates_by_class() {
        const MIN_AGE: Duration = Duration::from_millis(1500);
        const N: u64 = 7;
        let limit = u64::from((MAX_RETRIES + 1) * LOCAL_READ_STRAND_LIMIT);
        for case in ["open_media", "mid_segment_media", "open_access"] {
            let dir = tmp_dir(&format!("read_persistent_{case}"));
            let (sealed, records) = make_numbered_segment(&dir, 10);
            let attempts = || match case {
                "mid_segment_media" => read_hook::fired(&sealed),
                _ => open_hook::attempts(&sealed),
            };
            match case {
                "open_media" => open_hook::fail(&sealed, libc::EIO, u32::MAX),
                "open_access" => open_hook::fail(&sealed, libc::EACCES, u32::MAX),
                _ => read_hook::fail(&sealed, read_hook::FaultAt::Record(N), libc::EIO, u32::MAX),
            }

            let sink = Arc::new(MockSink::with_batch_size(3, []));
            let metrics = noop_metrics();
            let (tx, rx) = crossbeam_channel::unbounded();
            let handle = spawn(
                rx,
                Arc::clone(&sink),
                min_age_config(dir.clone(), MIN_AGE),
                Arc::clone(&metrics),
                DrainHeartbeat::new(),
            );
            let started = Instant::now();
            tx.send(sealed.clone()).unwrap();
            let quarantined = quarantined_copy(&dir, &sealed);
            wait_for(15, "the escalation", || match case {
                "open_access" => {
                    parked_gauge(&metrics, crate::metrics::ParkedReason::local_read_failed) == 1.0
                }
                _ => quarantined.exists(),
            });
            let elapsed = started.elapsed();
            assert!(elapsed >= MIN_AGE, "{case}: escalated after {elapsed:?}");
            let at_escalation = attempts();
            assert!(
                at_escalation > limit,
                "{case}: the count limit ({limit}) alone did not escalate ({at_escalation})"
            );
            // Idle rescans after the escalation: none may touch it again.
            std::thread::sleep(Duration::from_millis(300));
            drop(tx);
            handle.join().unwrap();
            assert_eq!(attempts(), at_escalation, "{case}: never read again");

            match case {
                "open_access" => {
                    assert!(sealed.exists(), "a parked segment is left in place");
                    assert!(!quarantined.exists());
                    assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
                    assert_eq!(sink.call_count(), 0);
                }
                "open_media" => {
                    assert!(!sealed.exists());
                    assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
                    assert_eq!(sink.call_count(), 0);
                }
                _ => {
                    assert!(!sealed.exists());
                    assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
                    assert_eq!(
                        committed_bytes(&sink),
                        records[..N as usize],
                        "the readable prefix exactly once, the partial batch on the \
                         escalating attempt only"
                    );
                }
            }
            open_hook::forget(&sealed);
            read_hook::forget(&sealed);
            std::fs::remove_dir_all(dir).ok();
        }
    }

    // ── Transient I/O mid-segment and at the footer (spec §4.2) ──────────────

    /// `n` sealed records `r00`, `r01`, … in one segment of `dir`.
    fn make_numbered_segment(dir: &Path, n: usize) -> (PathBuf, Vec<Vec<u8>>) {
        let records: Vec<Vec<u8>> = (0..n).map(|i| format!("r{i:02}").into_bytes()).collect();
        let refs: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
        (make_sealed_segment(dir, 0, &refs), records)
    }

    fn committed_bytes(sink: &MockSink) -> Vec<Vec<u8>> {
        sink.committed_records()
            .iter()
            .map(|p| p.to_vec())
            .collect()
    }

    /// One attempt that hits a non-corruption read error mid-segment is a local
    /// transient, not a quarantine: the sub-batches already committed are the
    /// durable cursor, and the partial in-memory batch (read but not yet sent)
    /// is NOT committed — the retry resumes exactly past the durable prefix and
    /// sends it then, once. Before §4.2 the prefix was committed and the good
    /// segment quarantined, so a blip pushed it into manual recovery.
    #[cfg(unix)]
    #[test]
    fn a_mid_segment_eio_is_transient_and_commits_no_partial_batch() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("mid_eio_once");
        let (sealed, records) = make_numbered_segment(&dir, 5);
        read_hook::fail(&sealed, read_hook::FaultAt::Record(3), libc::EIO, 1);

        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_batch_size(2, []);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(
                result,
                ProcessResult::Transient {
                    processed: 2,
                    cause: TransientCause::LocalRead {
                        class: ErrClass::Media,
                        errno: Some(libc::EIO),
                    },
                    ..
                }
            ),
            "EIO at record 3 with batches of 2: transient, durable through 2"
        );
        assert_eq!(
            committed_bytes(&sink),
            records[..2],
            "r02 was read into the partial batch but must not be committed"
        );
        assert!(sealed.exists(), "a blip must not move the segment");
        assert!(!dir.join("quarantine").exists(), "nothing quarantined");

        // The retry resumes past the durable prefix and finishes the segment.
        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 2,
        ));
        assert!(matches!(
            result,
            ProcessResult::Confirmed { record_count: 5 }
        ));
        assert_eq!(committed_bytes(&sink), records, "every record exactly once");
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The escalating attempt (`last_local_read_chance`) changes only a `Media`
    /// read mid-segment: that one delivers the partial batch and quarantines.
    /// `Process` (never escalates) and `Access` (parks, in the state machine)
    /// stay transient and still commit no partial batch.
    #[cfg(unix)]
    #[test]
    fn the_last_local_read_chance_only_changes_a_media_read() {
        use super::dead_letter::DeadLetterWriter;
        for (errno, quarantines) in [
            (libc::EMFILE, false),
            (libc::EACCES, false),
            (libc::EIO, true),
        ] {
            let dir = tmp_dir(&format!("last_chance_{errno}"));
            let (sealed, records) = make_numbered_segment(&dir, 5);
            read_hook::fail(&sealed, read_hook::FaultAt::Record(3), errno, 1);
            let mut dl = DeadLetterWriter::open(&dir).unwrap();
            let sink = MockSink::with_batch_size(2, []);
            let metrics = noop_metrics();
            let config = fast_config(dir.clone());

            let result = block_on(super::process_segment(
                &sealed,
                &sink,
                &config,
                &metrics,
                &mut dl,
                0,
                true,
                &DrainHeartbeat::new(),
            ));
            if quarantines {
                assert!(
                    matches!(result, ProcessResult::Quarantined),
                    "errno {errno}"
                );
                assert_eq!(committed_bytes(&sink), records[..3], "prefix incl. r02");
                assert!(quarantined_copy(&dir, &sealed).exists());
            } else {
                assert!(
                    matches!(
                        result,
                        ProcessResult::Transient {
                            processed: 2,
                            cause: TransientCause::LocalRead { .. },
                            ..
                        }
                    ),
                    "errno {errno}"
                );
                assert_eq!(committed_bytes(&sink), records[..2], "errno {errno}");
                assert!(sealed.exists());
            }
            read_hook::forget(&sealed);
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// Spec §4.2: the fault reader fails at record N and STAYS failing past the
    /// escalation limit. Every record below N reaches the sink exactly once in
    /// the run — the full sub-batches on the first attempt, the partial batch
    /// only on the escalating attempt — and the segment ends in `quarantine/`
    /// after three strand cycles, never counted as a sink strand.
    #[cfg(unix)]
    #[test]
    fn a_persistent_mid_segment_eio_delivers_the_prefix_once_then_quarantines() {
        const N: u64 = 7;
        let dir = tmp_dir("mid_eio_persistent");
        let (sealed, records) = make_numbered_segment(&dir, 10);
        read_hook::fail(&sealed, read_hook::FaultAt::Record(N), libc::EIO, u32::MAX);

        let sink = Arc::new(MockSink::with_batch_size(3, []));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()), // the 50 ms idle rescan re-queues each strand
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        let quarantined = quarantined_copy(&dir, &sealed);
        wait_for(10, "the media escalation quarantine", || {
            quarantined.exists()
        });
        // Idle rescans after the quarantine: nothing may re-deliver.
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            committed_bytes(&sink),
            records[..N as usize],
            "each record below N exactly once, in order — no strand re-sent the prefix"
        );
        assert_eq!(
            read_hook::fired(&sealed),
            u64::from(MAX_RETRIES + 1) * u64::from(LOCAL_READ_STRAND_LIMIT),
            "three strand cycles of attempts, then quarantined"
        );
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            u64::from(LOCAL_READ_STRAND_LIMIT - 1),
            "the first two cycles strand; the third ends in the quarantine"
        );
        assert_eq!(
            metrics.drain_segments_stranded.get(),
            0,
            "not a sink strand"
        );
        assert!(!sealed.exists(), "moved out of the shard dir");
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The `Access` twin of the test above: a mid-segment EACCES that persists
    /// parks the segment (bytes left in place) instead of quarantining it, and
    /// the partial batch is never committed — the next start replays the
    /// segment, and the at-least-once contract covers the prefix.
    #[cfg(unix)]
    #[test]
    fn a_persistent_mid_segment_eacces_parks_without_committing_the_partial_batch() {
        let dir = tmp_dir("mid_eacces_persistent");
        let (sealed, records) = make_numbered_segment(&dir, 10);
        read_hook::fail(
            &sealed,
            read_hook::FaultAt::Record(7),
            libc::EACCES,
            u32::MAX,
        );

        let sink = Arc::new(MockSink::with_batch_size(3, []));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(10, "the local_read_failed park", || {
            parked_gauge(&metrics, crate::metrics::ParkedReason::local_read_failed) == 1.0
        });
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            committed_bytes(&sink),
            records[..6],
            "the two full sub-batches exactly once; the partial batch never"
        );
        assert_eq!(
            read_hook::fired(&sealed),
            u64::from(MAX_RETRIES + 1) * u64::from(LOCAL_READ_STRAND_LIMIT),
            "three strand cycles of attempts, then parked and never read again"
        );
        assert!(sealed.exists(), "a parked segment is left in place");
        assert!(!quarantined_copy(&dir, &sealed).exists());
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.2: a footer read fault (EIO) held for two strand cycles, then
    /// cleared. Every record was committed before the footer check, so each
    /// retry resumes with `skip = record count` and re-commits nothing: the sink
    /// count equals the record count exactly, and the segment is confirmed.
    /// Before §4.2 any footer read error quarantined the (complete, delivered)
    /// segment on the first attempt.
    #[cfg(unix)]
    #[test]
    fn a_footer_read_blip_is_retried_and_confirms_without_redelivery() {
        const CYCLES: u32 = 2;
        let dir = tmp_dir("footer_eio_blip");
        let (sealed, records) = make_numbered_segment(&dir, 4);
        let failing = CYCLES * (MAX_RETRIES + 1);
        read_hook::fail(&sealed, read_hook::FaultAt::Footer, libc::EIO, failing);

        let sink = Arc::new(MockSink::with_batch_size(3, []));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(10, "the segment confirmed once the footer reads", || {
            get_confirmed_path(&sealed).exists()
        });
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            committed_bytes(&sink),
            records,
            "sink count == record count: the footer retries re-committed nothing"
        );
        assert_eq!(read_hook::fired(&sealed), u64::from(failing));
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::read),
            u64::from(CYCLES),
            "each failing cycle is a local read strand"
        );
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
        assert!(!sealed.exists(), "confirmed and deleted");
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A footer EIO that never clears escalates like any `Media` read: three
    /// strand cycles, then quarantine — and still with every record delivered
    /// exactly once, because each retry resumed past all of them.
    #[cfg(unix)]
    #[test]
    fn a_persistent_footer_eio_quarantines_after_three_cycles_without_redelivery() {
        let dir = tmp_dir("footer_eio_persistent");
        let (sealed, records) = make_numbered_segment(&dir, 4);
        read_hook::fail(&sealed, read_hook::FaultAt::Footer, libc::EIO, u32::MAX);

        let sink = Arc::new(MockSink::with_batch_size(3, []));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        let quarantined = quarantined_copy(&dir, &sealed);
        wait_for(10, "the footer media escalation quarantine", || {
            quarantined.exists()
        });
        std::thread::sleep(Duration::from_millis(300));
        drop(tx);
        handle.join().unwrap();

        assert_eq!(committed_bytes(&sink), records, "every record exactly once");
        assert_eq!(
            read_hook::fired(&sealed),
            u64::from(MAX_RETRIES + 1) * u64::from(LOCAL_READ_STRAND_LIMIT)
        );
        assert_eq!(metrics.drain_segments_stranded.get(), 0);
        assert!(!sealed.exists());
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A footer read that finds the file gone (NotFound) is treated like the
    /// open-time NotFound: already consumed. Every record was delivered before
    /// the footer check, so there is nothing to preserve.
    #[cfg(unix)]
    #[test]
    fn a_vanished_footer_counts_as_consumed() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("footer_enoent");
        let (sealed, records) = make_numbered_segment(&dir, 3);
        read_hook::fail(&sealed, read_hook::FaultAt::Footer, libc::ENOENT, 1);

        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Confirmed { record_count: 3 }),
            "a footer NotFound is already consumed, not a quarantine"
        );
        assert_eq!(committed_bytes(&sink), records);
        assert!(!dir.join("quarantine").exists());
        read_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    // ── Atomic sidecar, confirm_pending, orphan deletion (spec §4.3) ─────────

    /// Runs a first-start drain on its own thread with a fresh ledger, handing
    /// the ledger back on join so a test can inspect what it still owns.
    fn spawn_with_ledger<S: Sink + 'static>(
        rx: crossbeam_channel::Receiver<PathBuf>,
        sink: Arc<S>,
        config: DrainConfig,
        metrics: Arc<Metrics>,
    ) -> std::thread::JoinHandle<SegmentLedger> {
        std::thread::spawn(move || {
            let mut ledger = fresh_ledger();
            super::drain_thread(
                rx,
                sink,
                config,
                metrics,
                true,
                DrainHeartbeat::new(),
                &mut ledger,
            );
            ledger
        })
    }

    fn confirm_failures(m: &Metrics, stage: crate::metrics::ConfirmFailureStage) -> u64 {
        m.drain_confirm_failures
            .get_or_create(&crate::metrics::ConfirmFailureLabel { stage })
            .get()
    }

    fn confirm_pending_gauge(m: &Metrics) -> f64 {
        m.drain_segments_disposition_pending
            .get_or_create(&DispositionLabel {
                kind: DispositionKind::confirm,
            })
            .get()
    }

    fn tmp_sidecar(sealed: &Path) -> PathBuf {
        crate::wab::confirmed_tmp_path(&get_confirmed_path(sealed))
    }

    /// A sidecar that cannot be written used to send the WHOLE delivered
    /// segment back to the sink on every idle rescan (every
    /// `health_poll_interval`), for as long as the fault lasted. Now the segment
    /// is delivered once and only the sidecar is retried.
    ///
    /// A directory squatting on `seg_X.wab.confirmed.tmp` makes the sidecar's
    /// create fail with EISDIR, which works even as root.
    #[test]
    fn a_directory_at_the_sidecar_tmp_retries_the_confirm_not_the_delivery() {
        use crate::metrics::ConfirmFailureStage::sidecar;
        let dir = tmp_dir("confirm_tmp_eisdir");
        let sealed = make_sealed_segment(&dir, 0, &[b"once"]);
        let tmp = tmp_sidecar(&sealed);
        crate::testutil::mkdir_p(&tmp);

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );

        wait_for(5, "several failed sidecar attempts", || {
            confirm_failures(&metrics, sidecar) >= 3
        });
        wait_for(2, "the confirm_pending gauge", || {
            confirm_pending_gauge(&metrics) == 1.0
        });
        assert_eq!(
            sink.call_count(),
            1,
            "the segment is delivered once; the polls retry only its sidecar"
        );
        assert!(sealed.exists(), "the segment is kept until it is confirmed");
        assert!(!get_confirmed_path(&sealed).exists());
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);

        std::fs::remove_dir(&tmp).unwrap();
        wait_for(5, "the retried confirm deletes the segment", || {
            !sealed.exists()
        });
        assert!(get_confirmed_path(&sealed).exists());
        assert_eq!(sink.call_count(), 1, "still delivered exactly once");
        assert_eq!(segments_confirmed(&metrics), 1);

        drop(tx);
        let ledger = handle.join().unwrap();
        assert_eq!(
            ledger
                .counts()
                .confirm_pending
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(confirm_pending_gauge(&metrics), 0.0);
        std::fs::remove_dir_all(dir).ok();
    }

    /// ENOSPC mid-write, held across several polls. The in-place write left a
    /// torn sidecar under the final name, which the next rescan read as
    /// corruption and QUARANTINED the delivered segment for (an operator's
    /// `quarantine requeue` then re-sent all of it). Now nothing appears under
    /// the final name, nothing is quarantined, and the sink sees the segment
    /// once.
    #[test]
    fn a_short_sidecar_write_never_leaves_a_torn_sidecar_or_quarantines() {
        use crate::metrics::ConfirmFailureStage::sidecar;
        use confirmed::confirm_hook::{self, Fault};
        let dir = tmp_dir("confirm_short_write");
        let sealed = make_sealed_segment(&dir, 0, &[b"once"]);
        confirm_hook::fail(&sealed, Fault::ShortWrite, libc::ENOSPC, u32::MAX);

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );

        wait_for(5, "several short sidecar writes", || {
            confirm_failures(&metrics, sidecar) >= 3
        });
        assert!(
            !get_confirmed_path(&sealed).exists(),
            "no sidecar under the final name"
        );
        assert!(!tmp_sidecar(&sealed).exists(), "no partial tmp left behind");
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
        assert!(sealed.exists());
        assert_eq!(sink.call_count(), 1);

        confirm_hook::clear(&sealed);
        wait_for(5, "the confirm completes once the disk has room", || {
            !sealed.exists()
        });
        let sidecar_bytes = std::fs::read(get_confirmed_path(&sealed)).unwrap();
        let meta = crate::wab::format::parse_confirmed(&sidecar_bytes).unwrap();
        assert_eq!(meta.record_count, 1);
        assert_eq!(sink.call_count(), 1);

        drop(tx);
        handle.join().unwrap();
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A failed `sync_all` on the sidecar: nothing is renamed or deleted on the
    /// strength of a page-cache-only sidecar, and the retry, not a
    /// re-delivery, finishes the confirm.
    #[test]
    fn a_failed_sidecar_fsync_is_retried_without_redelivery() {
        use crate::metrics::ConfirmFailureStage::sidecar;
        use confirmed::confirm_hook::{self, Fault};
        let dir = tmp_dir("confirm_sync_fail");
        let sealed = make_sealed_segment(&dir, 0, &[b"once"]);
        confirm_hook::fail(&sealed, Fault::Sync, libc::EIO, 2);

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );
        wait_for(5, "the confirm completes after two failed fsyncs", || {
            !sealed.exists()
        });
        drop(tx);
        handle.join().unwrap();

        assert_eq!(confirm_failures(&metrics, sidecar), 2);
        assert_eq!(sink.call_count(), 1);
        assert!(get_confirmed_path(&sealed).exists());
        assert_eq!(confirm_hook::sidecar_writes(&sealed), 3);
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// A delete that fails after a durable sidecar is the cheapest stage to
    /// retry: only `remove_file`, never the sidecar again, never the delivery.
    /// Before, it only WARNed, and the orphan stayed on disk (and in the WAB
    /// byte count) for the life of the process.
    #[test]
    fn a_failed_delete_retries_only_the_delete() {
        use crate::metrics::ConfirmFailureStage::{delete, sidecar};
        use confirmed::confirm_hook::{self, Fault};
        let dir = tmp_dir("confirm_delete_fail");
        let sealed = make_sealed_segment(&dir, 0, &[b"once"]);
        confirm_hook::fail(&sealed, Fault::Delete, libc::EIO, u32::MAX);

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );
        wait_for(5, "several failed deletes", || {
            confirm_failures(&metrics, delete) >= 3
        });
        drop(tx);
        let mut ledger = handle.join().unwrap();

        assert_eq!(sink.call_count(), 1, "never delivered again");
        assert_eq!(
            ledger.confirm_stage(&sealed),
            Some(ledger::ConfirmStage::Delete)
        );
        assert_eq!(
            confirm_hook::sidecar_writes(&sealed),
            1,
            "the durable sidecar is not rewritten; only the delete is retried"
        );
        assert_eq!(confirm_failures(&metrics, sidecar), 0);
        assert!(get_confirmed_path(&sealed).exists());
        assert!(sealed.exists());
        assert_eq!(
            segments_confirmed(&metrics),
            1,
            "confirmed once, not per retry"
        );

        confirm_hook::clear(&sealed);
        ledger.retry_dispositions(&dir, &metrics, &DrainHeartbeat::new());
        assert!(!sealed.exists());
        assert_eq!(ledger.confirm_stage(&sealed), None);
        assert_eq!(segments_confirmed(&metrics), 1);
        confirm_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    /// An orphan pre-seeded on disk (a sealed segment beside a valid sidecar)
    /// is deleted by the drain's idle rescan when the daemon owns the
    /// `wab_dir`, and kept when it does not. Either way it is never delivered.
    #[test]
    fn the_idle_rescan_deletes_an_orphan_only_when_the_wab_dir_is_owned() {
        for orphan_cleanup in [true, false] {
            let dir = tmp_dir(&format!("rescan_orphan_{orphan_cleanup}"));
            let orphan = make_sealed_segment(&dir, 0, &[b"delivered last run"]);
            std::fs::write(
                get_confirmed_path(&orphan),
                crate::wab::format::build_confirmed(0, 1, 1),
            )
            .unwrap();

            let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
            let sink = Arc::new(MockSink::with_responses([]));
            let config = DrainConfig {
                orphan_cleanup,
                ..fast_config(dir.clone())
            };
            let handle = spawn_with_ledger(rx, Arc::clone(&sink), config, noop_metrics());
            if orphan_cleanup {
                wait_for(5, "the rescan deletes the orphan", || !orphan.exists());
            } else {
                std::thread::sleep(Duration::from_millis(300));
                assert!(orphan.exists(), "without the lock the orphan is kept");
            }
            drop(tx);
            handle.join().unwrap();
            assert_eq!(sink.call_count(), 0, "an orphan is never delivered");
            assert!(get_confirmed_path(&orphan).exists(), "the tombstone stays");
            std::fs::remove_dir_all(dir).ok();
        }
    }

    /// Review Focus 4: a 0-byte `.confirmed` that 4.0.x's in-place write tore
    /// is quarantined once by the rescan, exactly as today — not deleted as an
    /// orphan (it proves nothing), not delivered, and not touched again by the
    /// polls after it.
    #[test]
    fn a_legacy_torn_sidecar_is_quarantined_once_by_the_rescan() {
        let dir = tmp_dir("rescan_legacy_torn");
        let sealed = make_sealed_segment(&dir, 0, &[b"maybe delivered"]);
        std::fs::write(get_confirmed_path(&sealed), b"").unwrap();

        let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let config = DrainConfig {
            orphan_cleanup: true,
            ..fast_config(dir.clone())
        };
        let handle = spawn_with_ledger(rx, Arc::clone(&sink), config, Arc::clone(&metrics));
        wait_for(5, "the torn sidecar's segment is quarantined", || {
            quarantined_copy(&dir, &sealed).exists()
        });
        std::thread::sleep(Duration::from_millis(300)); // several more polls
        drop(tx);
        handle.join().unwrap();

        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);
        assert_eq!(sink.call_count(), 0);
        assert!(!sealed.exists());
        std::fs::remove_dir_all(dir).ok();
    }

    // ── quarantine_pending (spec §4.4) ────────────────────────────────────────

    fn quarantine_pending_gauge(m: &Metrics) -> f64 {
        m.drain_segments_disposition_pending
            .get_or_create(&DispositionLabel {
                kind: DispositionKind::quarantine,
            })
            .get()
    }

    fn health_polls_left(sink: &MockSink) -> usize {
        sink.health_script.lock().unwrap().len()
    }

    /// A quarantine move that fails (here: a regular file squats on
    /// `<wab_dir>/quarantine`, so the directory cannot be created) used to
    /// leave the segment sealed and unconfirmed, and every idle rescan sent its
    /// readable prefix to the sink again, once per `health_poll_interval`, for
    /// as long as the fault lasted. Now the prefix is delivered once, the
    /// failure is counted once, and the polls retry only the move.
    #[test]
    fn a_failed_quarantine_move_is_retried_not_redelivered() {
        let dir = tmp_dir("quarantine_pending_crc");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2"]);
        // Record 2's payload starts at byte 42 (24 header + 10 for record 1):
        // the reader yields r1, then a CRC mismatch.
        let mut bytes = std::fs::read(&sealed).unwrap();
        bytes[42] ^= 0xff;
        std::fs::write(&sealed, &bytes).unwrap();
        let squatter = dir.join("quarantine");
        std::fs::write(&squatter, b"not a directory").unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_health_script(
            (0..5).map(|_| SinkHealth::Healthy),
        ));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );

        wait_for(5, "five health polls", || health_polls_left(&sink) == 0);
        assert_eq!(
            committed_bytes(&sink),
            vec![b"r1".to_vec()],
            "the readable prefix is delivered once across five polls, not once per poll"
        );
        assert_eq!(
            metrics.drain_quarantine_failures.get(),
            1,
            "the failed move is counted once; its retries are not new failures"
        );
        assert_eq!(quarantine_pending_gauge(&metrics), 1.0);
        assert!(
            sealed.exists(),
            "the bytes stay on disk until the move lands"
        );
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);
        assert_eq!(segments_quarantined_family(&metrics), 0);

        std::fs::remove_file(&squatter).unwrap();
        // Wait on the quarantine series, not on the file vanishing: the rename
        // lands before the drain thread bumps the counters, so "file gone" can
        // be observed while both series still read 0.
        wait_for(
            5,
            "the next poll completes the move and bumps the quarantine series \
             like any other site",
            || {
                metrics.recovery_segments_quarantined.get() == 1
                    && segments_quarantined_family(&metrics) == 1
            },
        );
        assert!(!sealed.exists());
        assert!(quarantined_copy(&dir, &sealed).exists());
        assert_eq!(committed_bytes(&sink), vec![b"r1".to_vec()]);
        assert_eq!(metrics.drain_quarantine_failures.get(), 1);

        drop(tx);
        let ledger = handle.join().unwrap();
        assert_eq!(
            ledger
                .counts()
                .quarantine_pending
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(quarantine_pending_gauge(&metrics), 0.0);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The escalation site (spec §4.1): a persistent `Media`-class open error
    /// quarantines after three strand cycles. When that move fails too, the
    /// segment is handed to `quarantine_pending` — not forgotten and run
    /// through three more strand cycles of opens — and the polls retry only the
    /// move.
    #[test]
    fn a_failed_escalation_quarantine_is_retried_without_reopening() {
        let dir = tmp_dir("quarantine_pending_escalate");
        let sealed = make_sealed_segment(&dir, 0, &[b"never readable"]);
        open_hook::fail(&sealed, libc::EIO, u32::MAX);
        let squatter = dir.join("quarantine");
        std::fs::write(&squatter, b"not a directory").unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let handle = spawn_with_ledger(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
        );

        wait_for(10, "the escalated quarantine move fails", || {
            metrics.drain_quarantine_failures.get() == 1
        });
        let opens = open_hook::attempts(&sealed);
        std::thread::sleep(Duration::from_millis(300)); // several more polls
        assert_eq!(
            open_hook::attempts(&sealed),
            opens,
            "a segment awaiting its quarantine move is never opened again"
        );
        assert_eq!(metrics.drain_quarantine_failures.get(), 1);
        assert_eq!(quarantine_pending_gauge(&metrics), 1.0);
        assert!(sealed.exists());

        std::fs::remove_file(&squatter).unwrap();
        // As above: the counters are bumped after the rename, so wait on them.
        wait_for(5, "the next poll completes the move", || {
            metrics.recovery_segments_quarantined.get() == 1
                && segments_quarantined_family(&metrics) == 1
        });
        assert!(!sealed.exists());
        assert!(quarantined_copy(&dir, &sealed).exists());
        assert_eq!(open_hook::attempts(&sealed), opens);
        assert_eq!(sink.call_count(), 0);

        drop(tx);
        handle.join().unwrap();
        open_hook::forget(&sealed);
        std::fs::remove_dir_all(dir).ok();
    }

    // ── CommitResult ──────────────────────────────────────────────────────────

    #[test]
    fn commit_result_separates_committed_and_dead_lettered() {
        let p: Payload = Payload::from(b"hello".as_ref());
        let result: CommitResult =
            CommitResult::new(vec![p.clone()], vec![(p.clone(), "reason".into())]);
        assert_eq!(result.committed.len(), 1);
        assert_eq!(result.dead_lettered.len(), 1);
        assert_eq!(result.dead_lettered[0].0, p);
    }

    // ── Successful drain ──────────────────────────────────────────────────────

    #[test]
    fn successful_drain_writes_confirmed_and_deletes_segment() {
        let dir = tmp_dir("confirm");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([MockSink::ok(vec![
            Payload::from(b"r1".as_ref()),
            Payload::from(b"r2".as_ref()),
        ])]));
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(
            get_confirmed_path(&sealed).exists(),
            "confirmed file must exist"
        );
        assert!(
            !sealed.exists(),
            "sealed segment must be deleted after drain"
        );
        assert_eq!(segments_confirmed(&metrics), 1);
        assert_eq!(records_committed(&metrics), 2);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn confirmed_segment_not_replayed_on_restart() {
        let dir = tmp_dir("no_replay");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        // Confirmed file should exist; check_confirmed should return true.
        let confirmed = get_confirmed_path(&sealed);
        assert!(confirmed.exists());
        let _ok = crate::wab::recovery::check_confirmed(&sealed, &dir, &noop_metrics()).unwrap();
        let bytes = std::fs::read(&confirmed).unwrap();
        assert!(crate::wab::format::parse_confirmed(&bytes).is_ok());

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Transient retry ───────────────────────────────────────────────────────

    #[test]
    fn transient_success_on_retry_writes_confirmed() {
        let dir = tmp_dir("retry_ok");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Transient),
            MockSink::ok(vec![Payload::from(b"data".as_ref())]),
        ]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        assert!(
            get_confirmed_path(&sealed).exists(),
            "confirmed after successful retry"
        );
        assert!(!sealed.exists());

        std::fs::remove_dir_all(dir).ok();
    }

    /// F05: a retried multi-batch segment must NOT re-commit / re-dead-letter the
    /// sub-batches that already succeeded. With max_batch_size = 1 the two records
    /// are separate sub-batches: A is dead-lettered, B is transient (segment
    /// retried). The retry must RESUME at B — the sink sees A, B, B (3 commits),
    /// not a restart at A (which would be 4) — so A is neither re-committed nor
    /// re-dead-lettered.
    #[test]
    fn retry_resumes_past_already_processed_sub_batches() {
        let dir = tmp_dir("f05_resume");
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // Attempt 1: commit([A]) → permanent (dead-letter A); commit([B]) →
        // transient (retry). Attempt 2 resumes at B: commit([B]) → ok.
        let sink = Arc::new(MockSink::with_batch_size(
            1,
            [
                Err(MockError::Permanent),
                Err(MockError::Transient),
                MockSink::ok(vec![Payload::from(b"B".as_ref())]),
            ],
        ));
        let metrics = noop_metrics();
        run_drain(
            rx,
            tx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            metrics.clone(),
        );

        assert_eq!(
            sink.call_count(),
            3,
            "retry must resume at B (A,B,B = 3 commits), not restart at A (4)"
        );
        assert_eq!(
            records_dead_lettered(&metrics),
            1,
            "A must be dead-lettered exactly once, not again on the retry"
        );
        assert_eq!(records_committed(&metrics), 1, "B committed once");
        assert!(
            get_confirmed_path(&sealed).exists(),
            "segment confirmed after resuming the retry"
        );
        assert!(!sealed.exists(), "sealed segment deleted after confirm");

        std::fs::remove_dir_all(dir).ok();
    }

    /// The commit-timeout backstop interacting with the F05 sub-batch cursor:
    /// when the timeout fires on a LATER sub-batch (not the first), the retry must
    /// resume PAST the already-committed prefix, not re-commit it. max_batch=1 so
    /// each record is its own sub-batch: commit([A]) ok (cursor=1); commit([B])
    /// hangs → backstop timeout → Transient{processed=1}; the retry resumes at B.
    /// (hung_sink_commit_times_out covers the timeout on the *first* sub-batch;
    /// retry_resumes_past_already_processed_sub_batches covers the cursor under a
    /// permanent/transient response — this pins their intersection.)
    #[test]
    fn commit_timeout_on_a_later_sub_batch_resumes_past_the_committed_prefix() {
        let dir = tmp_dir("timeout_later_subbatch");
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // call 1 → commit([A]) ok; call 2 → commit([B]) HANGS (timeout); after the
        // transient retry, call 3 → commit([B]) ok. The hang on call 2 consumes no
        // response, so the queue is [A-ok, B-ok].
        let sink = Arc::new(
            MockSink::with_batch_size(
                1,
                [
                    MockSink::ok(vec![Payload::from(b"A".as_ref())]),
                    MockSink::ok(vec![Payload::from(b"B".as_ref())]),
                ],
            )
            .hanging_on_call(2),
        );
        let metrics = noop_metrics();
        let mut config = fast_config(dir.clone());
        config.commit_timeout = Duration::from_millis(50);
        run_drain(rx, tx, Arc::clone(&sink), config, metrics.clone());

        assert_eq!(
            sink.call_count(),
            3,
            "A, B(timeout), B = 3 commits — the retry resumed at B, not restarted at A (4)"
        );
        assert_eq!(
            records_committed(&metrics),
            2,
            "A and B each committed once"
        );
        assert!(
            get_confirmed_path(&sealed).exists(),
            "segment confirmed after the commit-timeout retry resumed past the cursor"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// Concern #2: a sink whose `commit()` hangs (a third-party sink with no
    /// internal timeout) must not stall the drain forever. The backstop
    /// `commit_timeout` fires → the batch is treated as transient → the segment
    /// is retried. Here the sink hangs on its first commit, then succeeds: the
    /// segment must still be confirmed, with the timeout charged to `retried`.
    #[test]
    fn hung_sink_commit_times_out_then_segment_retried_and_confirmed() {
        let dir = tmp_dir("hung_sink");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let metrics = noop_metrics();
        // Hang on the 1st commit; the 2nd falls back to the default response
        // (commits the whole batch).
        let sink = Arc::new(MockSink::with_hang(1, []));
        let mut config = fast_config(dir.clone());
        // Short backstop so the hung commit is cancelled quickly.
        config.commit_timeout = Duration::from_millis(50);
        run_drain(rx, tx, sink.clone(), config, metrics.clone());

        assert!(
            sink.call_count() >= 2,
            "the hung commit must be retried (calls={})",
            sink.call_count()
        );
        assert!(
            records_retried(&metrics) >= 2,
            "the timeout path must charge the records to the `retried` outcome"
        );
        assert_eq!(
            segments_confirmed(&metrics),
            1,
            "the segment is confirmed once the retry succeeds — no record lost, no deadlock"
        );
        assert!(get_confirmed_path(&sealed).exists());
        assert!(!sealed.exists());

        std::fs::remove_dir_all(dir).ok();
    }

    // ── B1: a failed dead-letter write must NOT confirm the segment ──────────
    // Regression for a silent-data-loss bug: when the sink permanently rejects
    // records AND the dead-letter write also fails (e.g. ENOSPC on the dead-letter
    // dir — exactly when dead-letter pressure peaks), the old code logged the error
    // and fell through to BatchResult::Ok → the segment was confirmed and DELETED
    // with the records neither delivered nor dead-lettered. The fix returns
    // Transient so the segment is preserved and retried (and left on disk if
    // retries exhaust), never silently dropped. We fault the dead-letter store by
    // putting a regular file where its directory was, so write_records fails
    // deterministically (ENOTDIR). Removing the directory no longer works: since
    // spec §4.9 a missing dead_letter/ is recreated and the write retried.

    /// Replaces the dead-letter directory with a regular file, so every write
    /// into it fails with ENOTDIR.
    fn squat_dead_letter_dir(dir: &Path) {
        let dl = dir.join("dead_letter");
        std::fs::remove_dir_all(&dl).unwrap();
        std::fs::write(&dl, b"not a directory").unwrap();
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn permanent_error_with_failed_dead_letter_write_is_transient_not_ok() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("b1_perm_dlfail");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        squat_dead_letter_dir(&dir);

        let sink = MockSink::with_responses([Err(MockError::Permanent)]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let payloads = vec![Payload::from(b"r1".as_ref()), Payload::from(b"r2".as_ref())];

        let ids = test_record_ids(&payloads);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(
            matches!(result, BatchResult::Transient { .. }),
            "permanent sink error + failed dead-letter write must be Transient (preserve \
             segment), not Ok (which confirms + deletes it → silent data loss)"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn partial_dead_letter_with_failed_write_is_transient_not_ok() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("b1_partial_dlfail");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        squat_dead_letter_dir(&dir);

        // commit() succeeds but reports one record permanently rejected; the
        // follow-up dead-letter write then fails.
        let sink = MockSink::with_responses([MockSink::ok_with_dead_letter(
            vec![Payload::from(b"ok".as_ref())],
            vec![Payload::from(b"bad".as_ref())],
        )]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let payloads = vec![
            Payload::from(b"ok".as_ref()),
            Payload::from(b"bad".as_ref()),
        ];

        let ids = test_record_ids(&payloads);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(
            matches!(result, BatchResult::Transient { .. }),
            "successful commit with a failed dead-letter write must be Transient \
             (preserve segment), not Ok"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── S03: the CommitResult partition guard (F02 false-ack defense) ────────────
    #[test]
    fn under_accounting_commit_result_is_transient_not_ok() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("s03_underaccount");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        // A non-conforming sink whose committed ∪ dead_lettered covers only 1 of
        // the 2 input records. The drain must refuse to confirm (Transient), never
        // Ok — confirming would delete the segment with the dropped record neither
        // delivered nor dead-lettered (the F02 partition guard).
        let sink = MockSink::with_responses([Ok(CommitResult::new(
            vec![Payload::from(b"only-one".as_ref())],
            vec![],
        ))]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let payloads = vec![Payload::from(b"r1".as_ref()), Payload::from(b"r2".as_ref())];

        let ids = test_record_ids(&payloads);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(
            matches!(result, BatchResult::Transient { .. }),
            "an under-accounting CommitResult must be Transient (preserve segment), not Ok"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── B2: an open failure must not silently confirm+delete a good segment ──
    // SegmentReader::open fails on transient I/O (fd exhaustion, ENOMEM) as well
    // as on permanent corruption. The old code returned Confirmed{0} for ANY open
    // error → the segment was deleted, discarding undelivered records on a
    // transient blip. The fix preserves the segment (Transient) for everything
    // except NotFound (already gone → genuine no-op).

    #[test]
    fn corrupt_segment_open_is_preserved_not_deleted() {
        let dir = tmp_dir("b2_corrupt");
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        // A file with bad magic → SegmentReader::open errors (InvalidData),
        // standing in for any non-NotFound open failure (transient or corrupt).
        let seg = segment_path(&shard_dir, 1);
        let sealed = seg.with_extension("wab.sealed");
        std::fs::write(&sealed, b"NOTWEIR-not-a-valid-segment-header-xxxx").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        // Spec §4.1 changed WHERE a corrupt header is preserved: it used to be
        // left at the sealed path (and retried forever); it is now moved to
        // quarantine/ on the first attempt. It is never deleted.
        // Transient open failures are still left in place and retried — see
        // the §4.1 tests and `a_local_read_strand_is_not_counted_as_a_sink_strand`.
        assert!(
            !sealed.exists() && quarantined_copy(&dir, &sealed).exists(),
            "a corrupt-header segment is moved to quarantine/ for recovery, not deleted \
             and not left at the sealed path to be retried"
        );
        assert!(
            !get_confirmed_path(&sealed).exists(),
            "an unopenable segment must not be confirmed"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// The S3 sink derives its object-key partition from this value. If the
    /// drain stops populating it, a replayed batch lands under a fresh
    /// wall-clock partition and duplicates the object — silently.
    #[test]
    fn the_drain_passes_the_segment_creation_time_to_the_sink() {
        use super::dead_letter::DeadLetterWriter;
        use std::sync::Mutex as StdMutex;

        /// Records what `segment_created_at` each commit carried.
        struct SpySink(StdMutex<Vec<Option<i64>>>);

        impl crate::sink::Sink for SpySink {
            type Error = MockError;

            async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, MockError> {
                self.0.lock().unwrap().push(batch.segment_created_at());
                Ok(CommitResult::new(batch.into_records(), Vec::new()))
            }

            async fn health(&self) -> SinkHealth {
                SinkHealth::Healthy
            }

            /// Forces `process_segment` to flush a full sub-batch mid-loop
            /// rather than only at the tail. Without this the default of 1000
            /// means a 2-record segment only ever exercises the tail flush, and
            /// the batched `commit_batch` call site goes untested — verified by
            /// mutation: passing 0 there alone left 440/440 green.
            fn max_batch_size(&self) -> usize {
                1
            }
        }

        let dir = tmp_dir("segment_created_at");
        let segment = make_sealed_segment(&dir, 0, &[b"a", b"b"]);

        // Read the header directly — this is the value the sink must observe.
        let expected = weir_wab::SegmentReader::open(&segment)
            .unwrap()
            .header()
            .created_at;

        let sink = SpySink(StdMutex::new(Vec::new()));
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        let result = block_on(process_segment(
            &segment, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Confirmed { .. }),
            "expected the segment to be confirmed"
        );

        let observed = sink.0.lock().unwrap().clone();
        // With max_batch_size = 1 and two records, this is one batched flush
        // plus one tail flush — so both `commit_batch` call sites are covered.
        assert_eq!(
            observed.len(),
            2,
            "expected both the batched and the tail commit_batch call sites to fire"
        );
        for seen in observed {
            assert_eq!(
                seen,
                Some(expected),
                "the drain must pass the segment header's created_at, not None or wall-clock"
            );
        }
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn missing_segment_open_is_confirmed_noop() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("b2_missing");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let missing = dir.join("shard_00").join("does_not_exist.wab.sealed");

        let result = block_on(process_segment(
            &missing, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Confirmed { record_count: 0 }),
            "a NotFound segment is already gone → Confirmed no-op (nothing to preserve)"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── B7: the drain survives a sink panic and keeps delivering ─────────────
    #[test]
    fn drain_survives_sink_panic_and_keeps_delivering() {
        let dir = tmp_dir("b7_panic");
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let mk = |counter: u64, payload: &[u8]| {
            let p = segment_path(&shard_dir, counter);
            let mut s = WabSegment::create(&p, 0, Compression::None).unwrap();
            s.write_record(payload).unwrap();
            s.seal().unwrap()
        };
        let _seg_a = mk(1, b"a");
        let seg_b = mk(2, b"b");

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(_seg_a.clone()).unwrap();
        tx.send(seg_b.clone()).unwrap();

        // Panic on the first commit (segment A); the supervisor must catch it,
        // respawn the drain, and still deliver segment B. (A is durable on disk
        // and would be replayed on a real restart — it is not re-delivered here.)
        let sink = Arc::new(MockSink::with_panic(1, []));
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(
            drain_panics(&metrics) >= 1,
            "the sink panic must be counted"
        );
        assert!(
            get_confirmed_path(&seg_b).exists(),
            "the drain must survive the panic and confirm the following segment"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── A respawned drain must rescan the disk ──────────────────────────────
    //
    // After a panic, `pending` is gone and the only in-run rescan is gated on
    // the `now_ok && !prev_health_ok` health edge. Starting a respawn's
    // `prev_health_ok` at `true` means that edge never fires again against a
    // healthy sink, so a segment stranded before the panic (never sent through
    // the channel — exactly like one left over from an earlier crash) is never
    // rescanned in this run; it waits for a process restart while `drain_state`
    // still reads `draining`. Exercised directly against `drain_thread`, using
    // `initial_health_ok = false` to simulate exactly the state a respawn
    // resumes into, rather than forcing a real panic through the supervisor.

    #[test]
    fn a_respawned_drain_rescans_the_disk_for_stranded_segments() {
        let dir = tmp_dir("respawn_rescan");
        // Sealed-but-unconfirmed on disk from the start — never sent through
        // the channel, exactly like a segment left stranded by an earlier
        // crash that a respawn must pick back up.
        let stranded = make_sealed_segment(&dir, 0, &[b"stranded"]);

        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let metrics_for_thread = metrics.clone();
        // A respawn starts here: no in-memory pending, healthy sink,
        // initial_health_ok = false (what run_drain_supervised passes on every
        // respawn). Run on its own thread so the test can keep `tx` alive long
        // enough for the thread's wall-clock health poll to fire (an
        // already-disconnected channel would make it exit before ever polling).
        let handle = std::thread::spawn(move || {
            drain_thread(
                rx,
                sink,
                config,
                metrics_for_thread,
                false,
                DrainHeartbeat::new(),
            );
        });

        // Bounded poll for the rescan's effect. health_poll_interval is 50 ms
        // in fast_config, so this resolves quickly once the fix is in place;
        // the cap just bounds the pre-fix (never happens) case.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && !get_confirmed_path(&stranded).exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        handle.join().unwrap();

        assert!(
            get_confirmed_path(&stranded).exists(),
            "a respawn (initial_health_ok = false) must treat its first \
             healthy probe as a down\u{2192}up edge and rescan the disk for \
             stranded segments, or the backlog waits for a process restart"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// A deterministic drain panic on one segment (spec §4.12). Before, each
    /// respawn rescanned the disk, re-queued the same segment, and panicked
    /// again, until `max_respawns` stopped delivery for EVERY segment. Now the
    /// segment in flight when the drain panics is parked once it has crashed
    /// the drain twice: the others deliver, the respawn budget is mostly
    /// unspent, and `drain_state` never reads `stopped` while the channel is
    /// live. The parked segment stays on disk, untouched, for the next start.
    #[test]
    fn a_segment_that_panics_the_drain_twice_is_parked_and_the_rest_deliver() {
        let dir = tmp_dir("drain_panic_park");
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let mk = |counter: u64, payload: &[u8]| {
            let p = segment_path(&shard_dir, counter);
            let mut s = WabSegment::create(&p, 0, Compression::None).unwrap();
            s.write_record(payload).unwrap();
            s.seal().unwrap()
        };
        let before = mk(1, b"before");
        let poison = mk(2, b"poison");
        let after = mk(3, b"after");

        let (tx, rx) = crossbeam_channel::unbounded();
        for seg in [&before, &poison, &after] {
            tx.send(seg.clone()).unwrap();
        }
        let sink = Arc::new(MockSink::panicking_on(b"poison"));
        let metrics = noop_metrics();
        // The real respawn budget (10): the point is that it is NOT exhausted.
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            metrics.clone(),
            DrainHeartbeat::new(),
        );

        let parked = || parked_gauge(&metrics, crate::metrics::ParkedReason::drain_panic);
        let stopped = || drain_state_of(&metrics, DrainStateValue::stopped) == 1.0;
        let mut ever_stopped = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            ever_stopped |= stopped();
            if ever_stopped
                || (parked() == 1.0
                    && get_confirmed_path(&before).exists()
                    && get_confirmed_path(&after).exists())
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        // Hold the channel open across several more health polls (50 ms each):
        // a parked segment must not be re-queued by the rescan and crash the
        // drain a third time.
        let hold = Instant::now() + Duration::from_millis(500);
        while Instant::now() < hold {
            ever_stopped |= stopped();
            std::thread::sleep(Duration::from_millis(10));
        }
        let panics_while_live = drain_panics(&metrics);
        let parked_while_live = parked();
        drop(tx);
        handle.join().unwrap();

        assert!(
            !ever_stopped,
            "one poison segment must not stop delivery for every segment"
        );
        assert_eq!(
            panics_while_live, 2,
            "the poison segment crashes the drain exactly twice, then is parked"
        );
        assert_eq!(
            parked_while_live, 1.0,
            "weir_drain_segments_parked{{reason=\"drain_panic\"}} names the parked segment"
        );
        assert!(
            get_confirmed_path(&before).exists() && get_confirmed_path(&after).exists(),
            "every other segment is delivered"
        );
        assert!(
            poison.exists() && !get_confirmed_path(&poison).exists(),
            "the parked segment is left in place, unconfirmed, for the next start"
        );
        assert!(
            !dir.join("quarantine").exists()
                || std::fs::read_dir(dir.join("quarantine")).unwrap().count() == 0,
            "a drain panic is not corruption: nothing is quarantined"
        );
        let committed = sink.committed_records();
        for want in [&b"before"[..], &b"after"[..]] {
            assert_eq!(
                committed.iter().filter(|p| &p[..] == want).count(),
                1,
                "{} is delivered exactly once",
                String::from_utf8_lossy(want)
            );
        }
        std::fs::remove_dir_all(dir).ok();
    }

    /// The terminal end of B7: if the drain keeps panicking, the supervisor gives
    /// up after `max_respawns` rather than respawning forever — delivery stops and
    /// the WAB accumulates until restart. Driven with `max_respawns = 2` so the
    /// give-up fires after a single ~100 ms backoff instead of the real ~4.5 s
    /// 10-respawn ladder.
    #[test]
    fn drain_supervisor_gives_up_after_max_respawns() {
        let dir = tmp_dir("drain_giveup");
        // Two segments (distinct counters) so each respawn has a fresh segment to
        // panic on.
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let mk = |counter: u64, payload: &[u8]| {
            let p = segment_path(&shard_dir, counter);
            let mut s = WabSegment::create(&p, 0, Compression::None).unwrap();
            s.write_record(payload).unwrap();
            s.seal().unwrap()
        };
        let sa = mk(1, b"a");
        let sb = mk(2, b"b");
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sa.clone()).unwrap();
        tx.send(sb.clone()).unwrap();

        let sink = Arc::new(MockSink::with_panic(5, [])); // panics on every commit
        let metrics = noop_metrics();
        let config = DrainConfig {
            max_respawns: 2,
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, sink, config, metrics.clone());

        assert_eq!(
            drain_panics(&metrics),
            2,
            "the supervisor counts one panic per respawn attempt, then gives up at the cap"
        );
        assert!(
            !get_confirmed_path(&sa).exists() && !get_confirmed_path(&sb).exists(),
            "after giving up, delivery stops — no segment is confirmed"
        );
        assert_drain_reports_stopped(&metrics, "respawn budget exhausted");
        std::fs::remove_dir_all(dir).ok();
    }

    /// The second permanent-stop path, and the one the whole-branch review
    /// reproduced against a live daemon: `DeadLetterWriter::open` fails. This
    /// now `panic!`s (see `drain_thread`) rather than returning cleanly, so the
    /// supervisor catches it, respawns, and — because this directory never
    /// becomes openable — exhausts `max_respawns` and gives up: `drain_panics`
    /// counts one per attempt, and only THEN does `drain_state` settle at
    /// `stopped`. (Before this fix, `drain_thread` returned *cleanly* before
    /// its loop ever started, the supervisor read that `Ok(())` as "the channel
    /// closed, delivery is finished," and neither respawned nor counted a
    /// panic; before an earlier, separate fix, `drain_state` also stayed
    /// pre-initialised at `draining = 1` forever, which kept `drain_is_healthy`
    /// (main.rs) true and the WAB growth warning permanently silent.)
    #[test]
    fn drain_supervisor_publishes_stopped_when_the_dead_letter_writer_cannot_open() {
        let dir = tmp_dir("drain_dl_open_fails");
        // A regular file where the dead-letter DIRECTORY must be: create_dir_private
        // fails with EEXIST, exactly as the reviewer's reproduction did.
        let blocker = dir.join("dead_letter");
        std::fs::remove_dir_all(&blocker).ok();
        std::fs::write(&blocker, b"not a directory").unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        // Tight max_respawns (like drain_supervisor_gives_up_after_max_respawns)
        // so the give-up fires after a single ~100 ms backoff instead of the
        // real ~4.5 s 10-respawn ladder — this directory never becomes
        // openable, so every respawn panics again.
        let config = DrainConfig {
            max_respawns: 2,
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, sink, config, metrics.clone());

        assert_eq!(
            drain_panics(&metrics),
            2,
            "an open failure must now panic — the supervisor sees it, \
             respawns, and (since this directory never becomes openable) \
             exhausts max_respawns rather than silently exiting after zero \
             panics"
        );
        assert_drain_reports_stopped(&metrics, "dead-letter writer would not open");
        std::fs::remove_dir_all(dir).ok();
    }

    /// Direct check, one level below the supervisor: `DeadLetterWriter::open`
    /// failing must surface as a panic that `catch_unwind` can see, not as a
    /// clean, normal return. `run_drain_supervised` reads `Ok(())` as "the
    /// channel closed, delivery is finished" — a bare `return` on this path
    /// would make it neither respawn nor count a panic, and the daemon would
    /// boot, bind, and ack producers forever while nothing is ever delivered.
    #[test]
    fn a_dead_letter_open_failure_is_not_mistaken_for_a_clean_shutdown() {
        let dir = tmp_dir("dl_open_failure_direct");
        // Make <wab_dir>/dead_letter un-openable as a directory.
        let dl = dir.join("dead_letter");
        std::fs::write(&dl, b"not a directory").unwrap();

        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
        drop(tx);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drain_thread(
                rx,
                Arc::new(MockSink::with_responses([])),
                fast_config(dir.clone()),
                metrics.clone(),
                true,
                DrainHeartbeat::new(),
            )
        }));

        assert!(
            result.is_err(),
            "an unusable dead-letter store must surface as a panic the \
             supervisor can see, not as a clean channel-closed exit"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// The third exit: an ordinary clean shutdown (drain channel closed). The
    /// drain genuinely is no longer draining, so it must say so here too —
    /// otherwise a scrape taken during shutdown still reads `draining = 1`.
    #[test]
    fn drain_supervisor_publishes_stopped_on_clean_shutdown() {
        let dir = tmp_dir("drain_clean_stop");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed).unwrap();
        let sink = Arc::new(MockSink::with_responses([]));
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert_drain_reports_stopped(&metrics, "clean shutdown");
        std::fs::remove_dir_all(dir).ok();
    }

    // ── B5: a mid-segment read error quarantines, never confirm+deletes ──────
    #[test]
    fn mid_segment_read_error_quarantines_not_deletes() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("b5_midcorrupt");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2"]);
        // Corrupt record 2's payload so the reader yields record 1 then errors on
        // record 2. Layout: 24 header + record1 [4 len + 4 crc + 2 payload] = 34;
        // record2 payload begins at 42.
        let mut bytes = std::fs::read(&sealed).unwrap();
        bytes[42] ^= 0xff;
        std::fs::write(&sealed, &bytes).unwrap();

        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]); // commits whatever prefix it's given
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Quarantined),
            "a mid-segment read error must quarantine, not confirm+delete the tail"
        );
        assert!(
            !sealed.exists(),
            "the unreadable segment must be moved out of the shard dir (quarantined)"
        );
        let q = dir.join("quarantine");
        assert!(
            q.is_dir() && std::fs::read_dir(&q).unwrap().count() == 1,
            "the segment must be preserved in the quarantine dir for manual recovery"
        );
        assert_eq!(
            records_dead_lettered(&metrics),
            0,
            "no dead-lettering — the prefix is delivered, the tail is quarantined"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T11 / G10): a RESUMED segment (skip > 0) that then hits a
    /// read error must still quarantine. The read_failed early-return fires before
    /// the debug_assert_eq!(durable_through, read_index), so a resume into a
    /// now-corrupt segment never panics in debug and never confirm+deletes the
    /// unread tail. The existing F05 resume test only covers the clean-resume case;
    /// the existing read-error quarantine test only covers skip == 0.
    #[test]
    fn resumed_segment_with_read_error_quarantines_not_panics() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("g10_resume_corrupt");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2", b"r3"]);
        // Corrupt record 2's payload so the reader yields r1 then errors at r2
        // (offset 42 = start of record 2's payload, per the skip==0 sibling test).
        let mut bytes = std::fs::read(&sealed).unwrap();
        bytes[42] ^= 0xff;
        std::fs::write(&sealed, &bytes).unwrap();

        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        // Resume as if record 1 was already durably processed on a prior attempt.
        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 1,
        ));
        assert!(
            matches!(result, ProcessResult::Quarantined),
            "a resumed segment that hits a read error must quarantine, not confirm or panic"
        );
        assert!(!sealed.exists(), "the segment must be moved to quarantine");
        assert_eq!(
            records_dead_lettered(&metrics),
            0,
            "the skipped prefix must not be re-dead-lettered; the tail is quarantined"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── S01: a post-seal tail truncation must quarantine, never confirm+delete ──
    #[test]
    fn post_seal_tail_truncation_quarantines_not_confirms() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("s01_tailtrunc");
        // Seal a 3-record segment, then truncate so records 1-2 stay fully
        // readable but record 3 + the zero-length sentinel + the footer are gone.
        // Layout: 24-byte header + per-record [4 len + 4 crc + 2 payload = 10] →
        // keep the first 44 bytes (header + 2 records). The sequential reader then
        // reads r1, r2 and sees the missing length field as a clean end of stream.
        let sealed = make_sealed_segment(&dir, 0, &[b"aa", b"bb", b"cc"]);
        let bytes = std::fs::read(&sealed).unwrap();
        assert!(
            bytes.len() > 44,
            "a sealed 3-record segment should exceed 44 bytes, was {}",
            bytes.len()
        );
        std::fs::write(&sealed, &bytes[..44]).unwrap();

        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        let sink = MockSink::with_responses([]); // commits whatever prefix it is given
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());

        let result = block_on(process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 0,
        ));
        assert!(
            matches!(result, ProcessResult::Quarantined),
            "a post-seal tail truncation must quarantine, not confirm+delete the lost tail"
        );
        assert!(
            !sealed.exists(),
            "the truncated segment must be moved to quarantine, not left to be confirmed+deleted"
        );
        let q = dir.join("quarantine");
        assert!(
            q.is_dir() && std::fs::read_dir(&q).unwrap().count() == 1,
            "the truncated segment must be preserved in quarantine for manual recovery"
        );
        assert_eq!(
            records_dead_lettered(&metrics),
            0,
            "no dead-lettering — the readable prefix is delivered, the truncated tail is quarantined"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── B6: a failed .confirmed write must not delete the segment ────────────
    #[test]
    fn confirmed_write_failure_preserves_segment() {
        let dir = tmp_dir("b6_confirm_fail");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1"]);
        // Block the .confirmed write: put a directory where the sidecar file would
        // go, so File::create fails deterministically.
        let confirmed = get_confirmed_path(&sealed);
        crate::testutil::mkdir_p(&confirmed);

        let metrics = noop_metrics();
        super::confirmed::confirm_and_delete(&sealed, 1, &metrics);

        assert!(
            sealed.exists(),
            "segment must be preserved when the .confirmed write fails (re-drained on restart)"
        );
        assert_eq!(
            segments_confirmed(&metrics),
            0,
            "must not mark the segment confirmed when the .confirmed write failed"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    // ── Strand timing and the per-segment cool-down (spec §4.6) ──────────────

    /// A sealed one-shard segment numbered `counter`, so a test can hold two
    /// distinct segments in the same shard.
    fn make_sealed_segment_numbered(dir: &Path, counter: u64, payloads: &[&[u8]]) -> PathBuf {
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);
        let mut seg =
            WabSegment::create(&segment_path(&shard_dir, counter), 0, Compression::None).unwrap();
        for p in payloads {
            seg.write_record(p).unwrap();
        }
        seg.seal().unwrap()
    }

    /// The final backoff was served for nobody. The RetryingTransient arm
    /// waited out the whole delay, rescanned, and only THEN noticed the budget
    /// was spent and stranded the segment: with `base=200ms` and
    /// `max_retries=2` the attempts land at 0, 200 and 600 ms and the strand at
    /// 1.4 s, the last 800 ms a dead wait on the single drain thread. The
    /// strand now happens before the wait, at ~0.6 s, and the delay it would
    /// have served becomes the segment's `not_before` instead.
    #[test]
    fn a_segment_strands_without_serving_the_final_backoff() {
        let dir = tmp_dir("strand_no_final_wait");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let sink = Arc::new(MockSink::with_responses(
            (0..1000).map(|_| Err(MockError::Transient)),
        ));
        let metrics = noop_metrics();
        let config = DrainConfig {
            base_retry_delay: Duration::from_millis(200),
            max_retries: 2,
            ..fast_config(dir.clone())
        };
        let mut ledger = fresh_ledger();
        let counts = Arc::clone(ledger.counts());
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let (sink_t, metrics_t) = (Arc::clone(&sink), Arc::clone(&metrics));
        let handle = std::thread::spawn(move || {
            super::drain_thread(
                rx,
                sink_t,
                config,
                metrics_t,
                true,
                DrainHeartbeat::new(),
                &mut ledger,
            );
        });

        wait_for(5, "the first commit", || sink.call_count() >= 1);
        wait_for(5, "the strand", || {
            metrics.drain_segments_stranded.get() >= 1
        });
        let stranded_at = Instant::now();
        let first_failure = sink.call_timestamps()[0];
        let attempts = sink.call_count();
        let cooling = counts.cooling_down.load(Ordering::Relaxed);
        drop(tx);
        handle.join().unwrap();

        let took = stranded_at.duration_since(first_failure);
        assert!(
            took < Duration::from_millis(1000),
            "the strand must not wait out the final 800 ms backoff first (first failure \
             to strand took {took:?}; 1.4 s means the dead final wait is back)"
        );
        assert_eq!(attempts, 3, "max_retries=2 is still three attempts");
        assert_eq!(
            cooling, 1,
            "the wait the strand skipped becomes the segment's not_before"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// A server-supplied `Retry-After` survives the strand. Stranding alone
    /// would let the very next health poll re-queue the segment (the sink is
    /// marked down and the probe comes back healthy), and a copy of the path
    /// still sitting in the channel would bypass the rescan entirely. The
    /// ledger's `not_before` holds both off until the hint has elapsed, and
    /// the drain thread is free meanwhile: another segment is delivered during
    /// the cool-down instead of after it.
    #[test]
    fn retry_after_is_honoured_across_a_strand_even_for_a_channel_copy() {
        const HINT: Duration = Duration::from_secs(2);
        let dir = tmp_dir("strand_retry_after");
        let a = make_sealed_segment_numbered(&dir, 1, &[b"a"]);
        let b = make_sealed_segment_numbered(&dir, 2, &[b"b"]);
        // One rejection with a 2 s Retry-After, then the sink takes anything.
        let sink = Arc::new(MockSink::with_responses([Err(
            MockError::TransientWithRetryAfter(HINT),
        )]));
        let metrics = noop_metrics();
        // No retries: the first failure strands, so the whole hint is the
        // cool-down. The 50 ms health poll + idle rescan of `fast_config` would
        // re-queue the segment at once if nothing held it back.
        let config = DrainConfig {
            max_retries: 0,
            ..fast_config(dir.clone())
        };
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(a.clone()).unwrap();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );

        wait_for(5, "the first commit", || sink.call_count() >= 1);
        let first_failure = sink.call_timestamps()[0];
        wait_for(5, "the strand", || {
            metrics.drain_segments_stranded.get() >= 1
        });
        // A copy of the cooling-down path in the channel (a flusher or an
        // earlier rescan put it there), then another segment behind it.
        tx.send(a.clone()).unwrap();
        tx.send(b.clone()).unwrap();

        wait_for(5, "the other segment to deliver", || {
            get_confirmed_path(&b).exists()
        });
        let b_done = first_failure.elapsed();
        assert!(
            b_done < Duration::from_millis(1500),
            "the drain thread must not sit out the Retry-After: another segment is \
             delivered during the cool-down (it took {b_done:?})"
        );
        wait_for(10, "the cooled-down segment to deliver", || {
            get_confirmed_path(&a).exists()
        });
        drop(tx);
        handle.join().unwrap();

        let calls = sink.call_timestamps();
        let committed = committed_bytes(&sink);
        assert_eq!(
            committed,
            vec![b"b".to_vec(), b"a".to_vec()],
            "each segment is committed once; the channel copy of the cooling-down \
             segment is dropped at admission, not delivered"
        );
        assert_eq!(
            calls.len(),
            3,
            "a's failure, b, then a once the hint elapsed"
        );
        let a_retry = calls[2].duration_since(first_failure);
        assert!(
            a_retry >= HINT,
            "the segment was re-processed {a_retry:?} after a Retry-After of {HINT:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn transient_max_retries_exhausted_leaves_segment_on_disk() {
        let dir = tmp_dir("max_retry");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        let sink = Arc::new(MockSink::with_responses(responses));
        let metrics = noop_metrics();
        // Push the health poll out of reach. `fast_config`'s 50 ms cadence races
        // this assertion: stranding the segment marks the sink down, the next
        // poll sees MockSink report healthy again, and that down→up edge is the
        // 4a auto-resume — it rescans, re-queues this very segment, and by then
        // the scripted responses are exhausted so the redelivery SUCCEEDS and the
        // segment is confirmed and deleted. Whether the drain exits before the
        // poll fires is pure timing, which made this test machine-dependent. The
        // resume behaviour has its own test
        // (`stranded_segment_resumes_when_sink_recovers`); this one is about what
        // happens when the retry budget runs out.
        let config = DrainConfig {
            health_poll_interval: Duration::from_secs(3600),
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, sink, config, Arc::clone(&metrics));

        assert!(
            sealed.exists(),
            "segment must remain on disk after max retries"
        );
        assert!(!get_confirmed_path(&sealed).exists(), "no confirmed file");
        // The strand must be observable: an operator alerts on this counter, not
        // on a one-time error! log line.
        assert_eq!(
            metrics.drain_segments_stranded.get(),
            1,
            "stranding a segment must increment weir_drain_segments_stranded_total"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// W2. At shutdown the retry backoff is CLAMPED, not eliminated.
    ///
    /// The drain only learns of shutdown as the drain channel disconnecting, and
    /// it used to `break` out of the interruptible backoff on that signal — so
    /// every remaining attempt fired back-to-back with zero delay (the chaos
    /// harness measured four segments burning all `MAX_RETRIES` in 0.4 ms). Three
    /// attempts inside one instant against a sink that needs milliseconds to
    /// answer fail identically for the same reason: the budget bought one
    /// effective attempt, not three, and forced a strand a brief delay might have
    /// avoided.
    ///
    /// With a 5 s base delay the unclamped schedule would be 5 + 10 + 20 + 40 s.
    /// The two bounds below are the whole property: **not zero** (each retry is a
    /// genuinely independent sample) and **far below one full backoff** (shutdown
    /// stays prompt).
    #[test]
    fn shutdown_clamps_the_retry_backoff_instead_of_zeroing_it() {
        let dir = tmp_dir("shutdown_backoff");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        let sink = Arc::new(MockSink::with_responses(responses));
        let metrics = noop_metrics();
        // A base delay far larger than the clamp, so an unclamped wait is
        // unmistakable and a zeroed one equally so.
        let config = DrainConfig {
            base_retry_delay: Duration::from_secs(5),
            ..fast_config(dir.clone())
        };

        // run_drain drops the channel immediately — i.e. the whole retry episode
        // runs in the shutdown regime.
        let started = Instant::now();
        run_drain(rx, tx, sink, config, Arc::clone(&metrics));
        let elapsed = started.elapsed();

        assert_eq!(
            metrics.drain_segments_stranded.get(),
            1,
            "the segment still strands once the budget is spent"
        );
        // The RetryingTransient arm waits once per retry, MAX_RETRIES times; the
        // give-up pass strands without waiting (spec §4.6), so the expected
        // total is ~3 × the clamp.
        assert!(
            elapsed >= Duration::from_millis(600),
            "shutdown retries must still be spaced by the clamped backoff, not fired \
             back-to-back with zero delay (elapsed {elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown must not sleep out a full base_retry_delay, let alone the \
             5 + 10 + 20 + 40 s exponential schedule (elapsed {elapsed:?})"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn stranded_segment_resumes_when_sink_recovers() {
        // 4a auto-resume: a segment strands (transient failures exhaust
        // max_retries), then the sink recovers — the drain's down→up recovery
        // rescan must re-queue and deliver it WITHOUT a daemon restart.
        let dir = tmp_dir("resume");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let (tx, rx) = crossbeam_channel::unbounded();

        // initial + MAX_RETRIES transient commits → strand; the resumed commit
        // then gets MockSink's default Ok (responses exhausted) → delivered.
        let responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        let sink = Arc::new(MockSink::with_responses(responses));
        let metrics = noop_metrics();

        // tx is kept OPEN (unlike run_drain) so the drain reaches the idle
        // health-poll where the recovery rescan runs.
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();

        // Wait for strand → recovery → delivery (idle health-poll is 50ms).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while records_committed(&metrics) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            metrics.drain_segments_stranded.get(),
            1,
            "segment must strand once before recovery"
        );
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "recovery must re-queue the stranded segment (weir_drain_segments_resumed_total)"
        );
        assert_eq!(
            records_committed(&metrics),
            1,
            "resumed segment must be delivered to the sink EXACTLY once \
             (>= 1 would miss a double-delivery of the re-queued segment)"
        );
        assert!(
            get_confirmed_path(&sealed).exists(),
            "resumed segment must be confirmed after delivery"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// The strand boundary no longer drops the F05 cursor (spec §4.0).
    ///
    /// `retry_resumes_past_already_processed_sub_batches` and
    /// `blocked_segment_resumes_past_processed_when_cap_clears` pin the two
    /// boundaries where the `processed` cursor always survived. Until the
    /// safety floor this test pinned the one where it was dropped on purpose
    /// (H2, the `max_retries` exhausted arm): the rescan re-queued only a
    /// `PathBuf`, so the resumed segment restarted at record 0 and re-committed
    /// the prefix that already landed — the billable duplicate behind the
    /// locked-bucket advice in `docs/sinks/s3.md`.
    ///
    /// The drain's `SegmentLedger` now keeps the cursor across the strand,
    /// pinned to the file's `(dev, ino)`, and admission resumes the re-queued
    /// path past it. It is still in memory only: a RESTART replays the whole
    /// segment, which `docs/sinks/s3.md` now says instead.
    ///
    /// Every other strand test uses a one-record segment, where a
    /// restart-at-zero and a resume-past-the-prefix are indistinguishable.
    #[test]
    fn a_strand_no_longer_re_delivers_the_prefix_it_already_committed() {
        let dir = tmp_dir("strand_reprefix");
        // batch_size = 1 → A and B commit as separate sub-batches, so there IS
        // a committed prefix the strand boundary could lose.
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);

        // call 1:            commit([A]) → Ok         (processed → 1)
        // calls 2..2+RETRIES: commit([B]) → Transient  (retries exhaust → STRAND)
        // then responses are exhausted, so MockSink defaults to Ok:
        // next call:         commit([B]) → Ok         → confirm (A is skipped)
        let mut responses: Vec<MockResult> = vec![MockSink::ok(vec![Payload::from(b"A".as_ref())])];
        responses.extend((0..=MAX_RETRIES).map(|_| Err(MockError::Transient)));
        let sink = Arc::new(MockSink::with_batch_size(1, responses));
        let metrics = noop_metrics();

        let (tx, rx) = crossbeam_channel::unbounded();
        // tx stays open so the drain reaches the idle health-poll where the
        // down→up recovery rescan runs.
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !get_confirmed_path(&sealed).exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            metrics.drain_segments_stranded.get(),
            1,
            "the segment must strand once before recovery re-queues it"
        );
        assert!(
            get_confirmed_path(&sealed).exists(),
            "the resumed segment must be confirmed after delivery"
        );

        let committed = sink.committed_records();
        let a = Payload::from(b"A".as_ref());
        let b = Payload::from(b"B".as_ref());
        assert_eq!(
            committed,
            vec![a, b],
            "A was committed before the strand; the resume must start past it. \
             Getting [A, A, B] means the cursor is dropped at the strand boundary \
             again, which re-opens the duplicate-version cost documented in \
             docs/sinks/s3.md"
        );
        assert_eq!(
            records_committed(&metrics),
            2,
            "a 2-record segment delivers 2 records across a strand within one run"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    // ── probe_and_resume_stranded: recovery-edge contract (H1) ─────────────────

    /// Reads the one-hot `weir_sink_health` gauge value for a single state.
    fn sink_health_gauge(m: &Metrics, state: crate::metrics::SinkHealthState) -> f64 {
        m.sink_health
            .get_or_create(&crate::metrics::SinkHealthLabel { state })
            .get()
    }

    /// A sink that comes back DEGRADED rather than Healthy must still un-strand
    /// its backlog. `Degraded` means "reachable but not fully healthy" — the
    /// HTTP probe returns it for any 4xx that is not 401/403/405/501, which
    /// includes **404**, what a method-routed framework answers for HEAD on a
    /// POST-only ingest path (`sink/http.rs:645`, pinned by
    /// `health_treats_other_4xx_as_degraded_and_5xx_as_down`). Commits over that
    /// sink succeed; only the probe 4xxes.
    ///
    /// Treating Degraded as not-recovered makes it a *stickier* failure than
    /// Down: a sink that goes Down and returns Healthy resumes, while one that
    /// returns Degraded never does, for the life of the process. That inversion
    /// is the defect.
    #[test]
    fn stranded_segments_resume_when_the_sink_comes_back_degraded() {
        let dir = tmp_dir("edge_degraded");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        let sink = MockSink::with_health_script([
            SinkHealth::Down("offline".into()),
            SinkHealth::Degraded("HEAD returned 404 Not Found".into()),
        ]);

        let after1 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, true, None);
        assert!(!after1, "Down probe must return now_ok = false");

        let after2 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after1, None);

        assert!(
            after2,
            "a Degraded sink is reachable and delivering; it must count as recovered"
        );
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "a sink that returns Degraded must re-queue its stranded segments"
        );
        assert_eq!(pending.len(), 1, "exactly one segment re-queued");
        assert_eq!(
            pending[0], sealed,
            "the re-queued segment is the stranded one"
        );
    }

    /// An edge cannot fire if the health never changes. A segment strands after
    /// `sink_max_retries` transient commit failures, which does not require the
    /// *probe* to have changed its answer — a sink whose HEAD 404s throughout
    /// (Degraded) while its POSTs fail transiently and then recover strands a
    /// segment with no edge on either side of it. The same is true of a Healthy
    /// sink with flaky commits.
    ///
    /// So when the drain has nothing queued, the rescan must not depend on an
    /// edge. Idle is exactly when a stranded segment is invisible and exactly
    /// when a directory walk is free; the edge remains the cost control for the
    /// busy case, where `pending` is non-empty and the scan is skipped.
    #[test]
    fn an_idle_drain_rescans_without_a_health_edge() {
        let dir = tmp_dir("idle_rescan");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        // Health never changes: Degraded throughout, so there is no edge at all.
        let sink = MockSink::with_health_script([
            SinkHealth::Degraded("HEAD returned 404 Not Found".into()),
            SinkHealth::Degraded("HEAD returned 404 Not Found".into()),
        ]);

        // prev_health_ok stays true across both polls — never a down→up edge.
        let after1 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, true, None);
        assert!(after1, "a Degraded sink is deliverable");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "an idle drain must find the stranded segment without a health edge"
        );
        assert_eq!(pending.len(), 1, "exactly one segment re-queued");
        assert_eq!(
            pending[0], sealed,
            "the re-queued segment is the stranded one"
        );

        // Second poll: pending is no longer empty, so the scan is skipped and
        // nothing is queued twice.
        probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after1, None);
        assert_eq!(
            pending.len(),
            1,
            "a busy drain skips the rescan; no segment is queued twice"
        );
    }

    /// The recovery edge (`now_ok && !prev_health_ok`) must fire EXACTLY once on
    /// the down→up transition, re-queueing the stranded segment, and must NOT
    /// fire again on subsequent steady-Healthy polls (no double re-queue).
    #[test]
    fn probe_and_resume_stranded_edge_fires_once() {
        use crate::metrics::SinkHealthState;

        let dir = tmp_dir("edge_once");
        // A real sealed-but-unconfirmed segment on disk for the rescan to find.
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        // Script: Down (no edge — sink still down) → Healthy (down→up edge) →
        // Healthy (steady, must NOT re-fire).
        let sink = MockSink::with_health_script([
            SinkHealth::Down("offline".into()),
            SinkHealth::Healthy,
            SinkHealth::Healthy,
        ]);

        // Poll 1: Down. prev was true (startup), now false. No edge → no resume.
        let after1 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, true, None);
        assert!(!after1, "Down probe must return now_ok = false");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            0,
            "no resume while the sink is down"
        );
        assert!(pending.is_empty(), "no segment queued while down");
        // Gauge reflects the (down) probe.
        assert_eq!(sink_health_gauge(&metrics, SinkHealthState::down), 1.0);

        // Poll 2: Healthy. down→up edge fires → exactly one segment re-queued.
        let after2 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after1, None);
        assert!(after2, "Healthy probe must return now_ok = true");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "the down→up edge must re-queue the stranded segment exactly once"
        );
        assert_eq!(
            pending.len(),
            1,
            "exactly one segment re-queued on the edge"
        );
        assert_eq!(
            pending[0], sealed,
            "the re-queued segment is the stranded one"
        );
        // Healthy probe drives the gauge to healthy.
        assert_eq!(sink_health_gauge(&metrics, SinkHealthState::healthy), 1.0);

        // Poll 3: Healthy again, prev now true → NO edge → NO further resume and
        // no second copy enqueued (dedup against `pending` also guards this).
        let after3 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after2, None);
        assert!(after3, "steady Healthy probe stays now_ok = true");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "steady-Healthy polls must NOT re-fire the recovery edge"
        );
        assert_eq!(pending.len(), 1, "no duplicate enqueue on steady Healthy");

        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.9: an unreadable shard directory must not block the rescan's
    /// resume of the other shards. The readable shards' segments are re-queued
    /// at once, and the partial scan is treated like a failed one for edge
    /// tracking (H1): the caller's `prev_health_ok` comes back, so the edge
    /// stays live and the next poll scans again, picking up the hidden shard
    /// once it is readable.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_shard_does_not_block_the_rescan_of_the_others() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir("rescan_unreadable_shard");
        let mk = |shard: &str, payload: &[u8]| {
            let sd = dir.join(shard);
            crate::testutil::mkdir_p(&sd);
            let mut s = WabSegment::create(&segment_path(&sd, 1), 0, Compression::None).unwrap();
            s.write_record(payload).unwrap();
            s.seal().unwrap()
        };
        let first = mk("shard_00", b"first");
        let hidden = mk("shard_01", b"hidden");
        let last = mk("shard_02", b"last");
        let bad = dir.join("shard_01");
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_dir(&bad).is_ok() {
            std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o700)).unwrap();
            eprintln!("skipping: a mode-000 directory is still readable (root?)");
            std::fs::remove_dir_all(dir).ok();
            return;
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let sink = MockSink::with_responses([]);
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        // On a recovery edge (prev_health_ok = false), healthy probe.
        let after1 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, false, None);
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            Vec::from(pending.clone()),
            vec![first.clone(), last.clone()],
            "the readable shards are resumed despite the unreadable one"
        );
        assert!(
            !after1,
            "a partial rescan keeps the recovery edge live, like a failed one"
        );

        // Next poll, shard readable again: the edge fires again and the hidden
        // segment is queued; the two already pending are not queued twice.
        let after2 =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after1, None);
        assert!(after2, "a complete rescan settles the edge");
        assert_eq!(Vec::from(pending), vec![first, last, hidden]);
        assert_eq!(metrics.drain_segments_resumed.get(), 3);
        std::fs::remove_dir_all(dir).ok();
    }

    /// H1 regression: when the WAB rescan returns `Err` on the recovery edge,
    /// `probe_and_resume_stranded` must return the caller's `prev_health_ok`
    /// (kept `false`) rather than `now_ok` (`true`), so the edge stays live and a
    /// later poll retries the rescan and resumes — instead of stranding the
    /// segment until restart. Faults the rescan by deleting `wab_dir` so the
    /// `fs::read_dir` inside `scan_unconfirmed_sealed` returns `Err`, then
    /// restores it and re-polls.
    #[test]
    fn probe_and_resume_stranded_err_path_keeps_edge_live() {
        let dir = tmp_dir("edge_err");
        // Start from a clean slate so the directory truly does not exist.
        std::fs::remove_dir_all(&dir).ok();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        // Every probe is Healthy; the recovery edge is driven by the caller's
        // prev_health_ok = false (as the drain sets it when a segment strands).
        let sink = MockSink::with_health_script([SinkHealth::Healthy, SinkHealth::Healthy]);

        // Poll on the edge while wab_dir is MISSING → scan_unconfirmed_sealed
        // returns Err. The contract: return the passed-in prev_health_ok (false),
        // NOT now_ok (true), and resume nothing.
        assert!(
            !dir.exists(),
            "precondition: wab_dir must be absent so the rescan faults"
        );
        let after_err =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, false, None);
        assert!(
            !after_err,
            "H1: on rescan Err the function must return the passed-in prev_health_ok (false), \
             not now_ok (true) — otherwise the recovery edge dies and segments strand until restart"
        );
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            0,
            "nothing resumes when the rescan failed"
        );
        assert!(pending.is_empty(), "no segment queued on rescan failure");
        // The gauge still reflects the healthy probe (FIX 1 leaves set_sink_health
        // as-is; only the edge-tracking return value is held back).
        assert_eq!(
            sink_health_gauge(&metrics, crate::metrics::SinkHealthState::healthy),
            1.0,
            "the health gauge must still show healthy even when the rescan failed"
        );

        // Now restore wab_dir with a stranded segment. Because after_err is still
        // false, the NEXT poll is again a down→up edge → the rescan retries,
        // succeeds, and resumes the segment. (If H1 returned now_ok=true, prev
        // would be true here and the edge would never fire again.)
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let after_retry =
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after_err, None);
        assert!(after_retry, "the retry poll is Healthy → now_ok = true");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "H1: the live edge lets a later poll retry the rescan and resume the segment"
        );
        assert_eq!(
            pending.len(),
            1,
            "the stranded segment is re-queued on retry"
        );
        assert_eq!(pending[0], sealed);

        std::fs::remove_dir_all(dir).ok();
    }

    /// M2: when called from the Blocked/Retrying arms, the rescan must re-queue
    /// OTHER stranded segments on a recovery edge but NOT the one the caller is
    /// holding in-flight (passed as `in_flight`) — otherwise that segment would be
    /// processed twice (once via the arm resuming it, once via `pending`).
    #[test]
    fn probe_and_resume_stranded_excludes_in_flight_segment() {
        let dir = tmp_dir("inflight_excl");
        // Two stranded segments on disk: `held` is the in-flight one; `other` was
        // stranded by an earlier outage and should resume.
        let held = make_sealed_segment(&dir, 0, &[b"held"]);
        let shard_dir = dir.join("shard_00");
        let other_active = segment_path(&shard_dir, 2);
        let mut s = WabSegment::create(&other_active, 0, Compression::None).unwrap();
        s.write_record(b"other").unwrap();
        let other = s.seal().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = fast_config(dir.clone());
        let metrics = noop_metrics();
        let mut pending: VecDeque<PathBuf> = VecDeque::new();

        // Healthy probe with prev=false → a down→up recovery edge.
        let sink = MockSink::with_health_script([SinkHealth::Healthy]);
        let after = probe_and_resume_stranded(
            &rt,
            &sink,
            &config,
            &metrics,
            &mut pending,
            false,
            Some(&held),
        );

        assert!(after, "Healthy probe → now_ok = true");
        assert_eq!(
            pending.len(),
            1,
            "exactly the OTHER stranded segment is re-queued, not the in-flight one"
        );
        assert_eq!(
            pending[0], other,
            "the re-queued segment is `other`, not `held`"
        );
        assert!(
            !pending.contains(&held),
            "the in-flight segment must be excluded from the re-queue (no double-process)"
        );
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "only the non-in-flight stranded segment counts as resumed"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn multiple_segments_second_processed_after_first_exhausts_retries() {
        let dir = tmp_dir("two_segs");
        let shard_dir = dir.join("shard_00");
        crate::testutil::mkdir_p(&shard_dir);

        let seg1_active = segment_path(&shard_dir, 1);
        let mut s1 = WabSegment::create(&seg1_active, 0, Compression::None).unwrap();
        s1.write_record(b"seg1").unwrap();
        let seg1 = s1.seal().unwrap();

        let seg2_active = segment_path(&shard_dir, 2);
        let mut s2 = WabSegment::create(&seg2_active, 0, Compression::None).unwrap();
        s2.write_record(b"seg2").unwrap();
        let seg2 = s2.seal().unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(seg1.clone()).unwrap();
        tx.send(seg2.clone()).unwrap();

        let mut responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        responses.push(MockSink::ok(vec![Payload::from(b"seg2".as_ref())]));
        // seg1's resume (after seg2 confirms) gets MockSink's default Ok.
        let sink = Arc::new(MockSink::with_responses(responses));
        // The stranded-resume rescan runs at the idle health poll. This test
        // asserts seg1 is STILL stranded when the drain exits, so the poll must
        // not fire during the run. fast_config's 50 ms is short enough that
        // under parallel load the clamped shutdown backoff (d522ce2 / W2) lets
        // the poll fire, resume seg1, and delete it — the assertion below then
        // fails ~2 runs in 3. The resume path is covered separately by
        // `stranded_segment_resumes_when_sink_recovers`.
        let config = DrainConfig {
            health_poll_interval: Duration::from_secs(3600),
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, sink, config, noop_metrics());

        // seg2 is delivered without being blocked by seg1 exhausting its retries
        // (a stranded segment doesn't stall the queue) — that is what this test
        // is for. seg1 stays stranded because the health poll is pinned above so
        // the sink-recovery rescan cannot run (that path is covered by
        // stranded_segment_resumes_when_sink_recovers).
        assert!(seg1.exists(), "seg1 left on disk after exhausted retries");
        assert!(!seg2.exists(), "seg2 confirmed and deleted");
        assert!(get_confirmed_path(&seg2).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Permanent error / dead-letter ─────────────────────────────────────────

    #[test]
    fn permanent_error_dead_letters_records_and_confirms_segment() {
        let dir = tmp_dir("perm_dl");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(get_confirmed_path(&sealed).exists());
        assert!(!sealed.exists());
        assert_eq!(records_dead_lettered(&metrics), 1);

        let dl_dir = dir.join("dead_letter");
        let dl_files: Vec<_> = std::fs::read_dir(&dl_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !dl_files.is_empty(),
            "dead-letter directory must not be empty"
        );

        for entry in &dl_files {
            let path = entry.path();
            if path.to_str().unwrap_or("").ends_with(".wab.sealed") {
                let records: Vec<_> = crate::wab::SegmentReader::open(&path)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert!(!records.is_empty());
            }
        }

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn commit_result_dead_lettered_records_written_to_dead_letter_dir() {
        let dir = tmp_dir("dl_partial");
        let sealed = make_sealed_segment(&dir, 0, &[b"ok_record", b"bad_record"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([MockSink::ok_with_dead_letter(
            vec![Payload::from(b"ok_record".as_ref())],
            vec![Payload::from(b"bad_record".as_ref())],
        )]));
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert_eq!(records_committed(&metrics), 1);
        assert_eq!(records_dead_lettered(&metrics), 1);
        assert!(get_confirmed_path(&sealed).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    // ── BlockedDeadLetterFull ─────────────────────────────────────────────────

    #[test]
    fn blocked_when_permanent_error_and_dead_letter_cap_exceeded() {
        let dir = tmp_dir("blocked");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(rx, sink, config, metrics.clone(), DrainHeartbeat::new());

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for blocked state"
            );
        }

        assert_eq!(dl_full_count(&metrics), 1);
        assert!(!get_confirmed_path(&sealed).exists());

        drop(tx);
        handle.join().unwrap();

        assert!(
            !get_confirmed_path(&sealed).exists(),
            "segment must not be confirmed on blocked shutdown"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// Coverage gap (T00 / F03): a single permanently-rejected batch whose sealed
    /// size ALONE exceeds dead_letter_max_bytes must be written anyway (overshoot
    /// once) and return Ok — NOT Blocked. Blocking on it would livelock: even an
    /// empty dir can't hold it. The existing blocked test only covers a batch that
    /// fits once the dir drains; the over-cap-single-batch arm was untested.
    #[test]
    fn oversized_dead_letter_batch_overshoots_cap_instead_of_blocking() {
        use super::dead_letter::DeadLetterWriter;
        let dir = tmp_dir("f03_overshoot_perm");
        let mut dl = DeadLetterWriter::open(&dir).unwrap();
        // Cap far below the minimum sealed-segment framing (~60 bytes) so any
        // non-empty batch is "oversized".
        let config = tight_dl_config(dir.clone(), 10);
        let metrics = noop_metrics();
        let sink = MockSink::with_responses([Err(MockError::Permanent)]);
        let payloads = vec![Payload::from(b"a-record".as_ref())];

        let ids = test_record_ids(&payloads);
        let result = block_on(commit_batch(
            &payloads, &ids, 0, &sink, &config, &metrics, &mut dl,
        ));
        assert!(
            matches!(result, BatchResult::Ok),
            "a single batch larger than the cap must be written anyway (overshoot), not Blocked"
        );
        assert_eq!(
            records_dead_lettered(&metrics),
            1,
            "the oversized batch must actually be written"
        );
        assert!(
            dl.total_bytes() > config.dead_letter_max_bytes,
            "the dir is intentionally over-cap after the one-time overshoot"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn blocked_does_not_call_commit_while_waiting() {
        let dir = tmp_dir("blocked_no_commit");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(
            rx,
            sink.clone(),
            config,
            metrics.clone(),
            DrainHeartbeat::new(),
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for blocked state"
            );
        }

        let calls_at_block = sink.call_count();
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(
            sink.call_count(),
            calls_at_block,
            "commit must not be called while blocked"
        );

        drop(tx);
        handle.join().unwrap();

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn blocked_unblocks_and_retries_same_segment() {
        let dir = tmp_dir("unblock");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Permanent),
            MockSink::ok(vec![Payload::from(b"record".as_ref())]),
        ]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(
            rx,
            sink.clone(),
            config,
            metrics.clone(),
            DrainHeartbeat::new(),
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out entering blocked"
            );
        }

        std::fs::remove_file(&blocking_file).unwrap();

        drop(tx);
        handle.join().unwrap();

        assert!(
            get_confirmed_path(&sealed).exists(),
            "segment must be confirmed after unblock"
        );
        assert!(!sealed.exists());
        // Spec §4.5 changed this from 2: the record the sink rejected is
        // carried across the block and dead-lettered on unblock, exactly as it
        // would have been with headroom; the sink is not asked again.
        assert_eq!(
            sink.call_count(),
            1,
            "commit called once: the unblocked retry writes the carried reject"
        );
        assert_eq!(records_dead_lettered(&metrics), 1);

        std::fs::remove_dir_all(dir).ok();
    }

    /// F05 across the BlockedDeadLetterFull → resume boundary. The existing
    /// `retry_resumes_past_already_processed_sub_batches` covers resume through
    /// `RetryingTransient`; this covers the *block path*: a segment that commits
    /// an early sub-batch, then blocks on a permanent reject because the
    /// dead-letter cap is full, must — once the cap clears — RESUME at the blocked
    /// sub-batch (skip = `processed`) rather than restart at record 0. Restarting
    /// would re-commit the already-delivered prefix (a duplicate beyond
    /// at-least-once *within a single run*). `processed` is carried through
    /// `BlockedDeadLetterFull` and back into `RetryingTransient` on unblock.
    #[test]
    fn blocked_segment_resumes_past_processed_when_cap_clears() {
        let dir = tmp_dir("blocked_resume_f05");
        // batch_size = 1 → A and B are committed as separate sub-batches.
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);

        // Pre-fill the dead-letter dir past the (tight) cap so the first permanent
        // reject of B blocks rather than dead-letters.
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // call 1: commit([A]) → Ok               (A committed, processed → 1)
        // call 2: commit([B]) → Permanent        (dead-letter B, but cap full → BLOCK at processed=1,
        //                                          B carried)
        // cap cleared → the carried B is dead-lettered → resume past B → confirm (spec §4.5;
        // before the carry, a third call re-committed [B]).
        // A restart-at-0 bug would instead re-commit [A] after the block.
        let sink = Arc::new(MockSink::with_batch_size(
            1,
            [
                MockSink::ok(vec![Payload::from(b"A".as_ref())]),
                Err(MockError::Permanent),
                Err(MockError::Permanent),
            ],
        ));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            metrics.clone(),
            DrainHeartbeat::new(),
        );

        // Park in BlockedDeadLetterFull (A done, B blocked).
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out entering blocked"
            );
        }
        assert_eq!(
            records_committed(&metrics),
            1,
            "A is committed before the block point"
        );

        // Free the cap; the blocked arm's rescan sees headroom and resumes at B.
        std::fs::remove_file(&blocking_file).unwrap();

        drop(tx);
        handle.join().unwrap();

        assert!(
            get_confirmed_path(&sealed).exists(),
            "segment confirmed after the block-path resume"
        );
        assert!(!sealed.exists(), "sealed segment deleted after confirm");
        assert_eq!(
            sink.call_count(),
            2,
            "resume must continue past the carried B (A,B = 2 commits), not restart at A"
        );
        // The crux: only A was ever committed. A restart-at-0 would re-read from
        // record 0, re-committing A on the resume (committed = [A, B], 4 commits).
        assert_eq!(
            sink.committed_records(),
            vec![Payload::from(b"A".as_ref())],
            "A committed exactly once — the resume skipped it rather than re-committing (F05)"
        );
        // B is dead-lettered by the drain (the sink returns Err(Permanent), so it
        // shows up in the records-dead-lettered metric, not the sink's CommitResult
        // introspection). Exactly one record dead-lettered: B once, never A.
        assert_eq!(
            records_dead_lettered(&metrics),
            1,
            "exactly one record (B) dead-lettered; A was never dead-lettered (no restart-at-0)"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// Replaces the sealed segment at `sealed` with a different file (a new
    /// inode) holding `payloads`, the way an operator restoring or overwriting
    /// a segment would. Returns nothing; asserts the inode changed.
    #[cfg(unix)]
    fn replace_segment_file(sealed: &Path, label: &str, payloads: &[&[u8]]) {
        use std::os::unix::fs::MetadataExt;
        let src_dir = tmp_dir(label);
        let replacement = make_sealed_segment(&src_dir, 0, payloads);
        let old_ino = std::fs::metadata(sealed).unwrap().ino();
        std::fs::rename(&replacement, sealed).unwrap();
        assert_ne!(
            std::fs::metadata(sealed).unwrap().ino(),
            old_ino,
            "the replacement is a different file"
        );
        std::fs::remove_dir_all(src_dir).ok();
    }

    /// cont-durability-1, the `RetryingTransient` path. The retry resumes from
    /// the `processed` its state carries, and before the fix nothing checked
    /// that against the `(dev, ino)` pin that admission checks: a file replaced
    /// between attempts had its first `processed` records skipped, and the
    /// segment was then confirmed and deleted, so they were never delivered.
    /// Now a cursor that no longer describes the file at the path resumes the
    /// new file from record 0.
    #[cfg(unix)]
    #[test]
    fn a_retry_resumes_a_replaced_file_from_its_first_record() {
        let dir = tmp_dir("retry_replaced_file");
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);
        // commit([A]) ok (cursor 1, pinned to this file); commit([B]) transient.
        // Everything after commits whole.
        let sink = Arc::new(MockSink::with_batch_size(
            1,
            [
                MockSink::ok(vec![Payload::from(b"A".as_ref())]),
                Err(MockError::Transient),
            ],
        ));
        let config = DrainConfig {
            // Long enough to replace the file before the retry fires.
            base_retry_delay: Duration::from_millis(400),
            ..fast_config(dir.clone())
        };
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(5, "the transient commit of B", || sink.call_count() >= 2);
        replace_segment_file(&sealed, "retry_replaced_file_src", &[b"X", b"Y"]);

        wait_for(5, "the replaced segment confirmed", || {
            get_confirmed_path(&sealed).exists()
        });
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            committed_bytes(&sink),
            vec![b"A".to_vec(), b"X".to_vec(), b"Y".to_vec()],
            "the new file is delivered from its first record; skipping X would lose it"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// cont-durability-1, the `BlockedDeadLetterFull` → unblock path: the same
    /// state-carried `processed`, plus a dead-letter carry pinned to the old
    /// file. The carry is discarded (it names the old file's records) and the
    /// new file is processed from record 0, as the discard's WARN says.
    #[cfg(unix)]
    #[test]
    fn an_unblocked_segment_resumes_a_replaced_file_from_its_first_record() {
        let dir = tmp_dir("blocked_replaced_file");
        let sealed = make_sealed_segment(&dir, 0, &[b"A", b"B"]);
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        // commit([A]) ok (cursor 1); commit([B]) permanent → the cap is full →
        // blocked at processed=1 with B carried. Everything after commits whole.
        let sink = Arc::new(MockSink::with_batch_size(
            1,
            [
                MockSink::ok(vec![Payload::from(b"A".as_ref())]),
                Err(MockError::Permanent),
            ],
        ));
        let metrics = noop_metrics();
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            tight_dl_config(dir.clone(), 100),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        tx.send(sealed.clone()).unwrap();
        wait_for(5, "blocked on dead-letter headroom", || {
            drain_is_blocked(&metrics)
        });
        replace_segment_file(&sealed, "blocked_replaced_file_src", &[b"X", b"Y"]);
        std::fs::remove_file(&blocking_file).unwrap();

        wait_for(5, "the replaced segment confirmed", || {
            get_confirmed_path(&sealed).exists()
        });
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            committed_bytes(&sink),
            vec![b"A".to_vec(), b"X".to_vec(), b"Y".to_vec()],
            "the new file is delivered from its first record; skipping X would lose it"
        );
        assert_eq!(
            records_dead_lettered(&metrics),
            0,
            "the old file's carried reject is not written for the new file"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dead_letter_full_total_increments_once_per_entry_not_per_wake() {
        let dir = tmp_dir("dl_total");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();
        let handle = spawn(rx, sink, config, metrics.clone(), DrainHeartbeat::new());

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(std::time::Instant::now() < deadline, "timed out");
        }
        std::thread::sleep(Duration::from_millis(60));

        assert_eq!(
            dl_full_count(&metrics),
            1,
            "counter must increment exactly once per entry into blocked, not per wake"
        );

        drop(tx);
        handle.join().unwrap();

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn blocked_duration_set_on_entry_and_cleared_on_exit() {
        let dir = tmp_dir("blocked_duration");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Permanent),
            MockSink::ok(vec![Payload::from(b"r".as_ref())]),
        ]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();
        let handle = spawn(rx, sink, config, metrics.clone(), DrainHeartbeat::new());

        // Wait for drain to enter blocked state.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(std::time::Instant::now() < deadline, "timed out");
        }

        // Clear blocking file → drain unblocks.
        std::fs::remove_file(&blocking_file).unwrap();
        drop(tx);
        handle.join().unwrap();

        // After unblocking, drain_state{blocked} must be 0 and blocked_duration reset to 0.
        assert!(
            !drain_is_blocked(&metrics),
            "drain must not be blocked after unblocking"
        );
        assert_eq!(
            metrics.dead_letter_blocked_duration.get(),
            0.0,
            "blocked_duration must be zero after unblocking"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Dead-letter headroom and carry (spec §4.5, §4.11) ────────────────────

    /// Every record in the dead-letter directory's sealed segments, in file
    /// order. Files that are not segments (the tests' zero-filled cap fillers)
    /// are skipped.
    fn dead_letter_contents(wab_dir: &Path) -> Vec<Payload> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(wab_dir.join("dead_letter"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
            .collect();
        files.sort();
        files
            .iter()
            .filter_map(|f| crate::wab::SegmentReader::open(f).ok())
            .flat_map(|r| r.map(Result::unwrap))
            .collect()
    }

    fn partial_reject(ok: &'static [u8], bad: &'static [u8]) -> MockResult {
        MockSink::ok_with_dead_letter(
            vec![Payload::from_static(ok)],
            vec![Payload::from_static(bad)],
        )
    }

    /// Spec §4.5. The block predicate is `total + needed > cap`, but the
    /// unblock predicate was `total < cap`. With the total anywhere in
    /// `[cap - needed, cap)` the drain unblocked on every check interval,
    /// re-committed the sub-batch's ACCEPTED records, and blocked again: one
    /// duplicate commit per `dead_letter_check_interval` for as long as the
    /// headroom stayed short.
    ///
    /// Now the drain unblocks only when `needed` fits, and the rejected records
    /// are carried, so the resume writes them without asking the sink again.
    #[test]
    fn a_dead_letter_block_short_of_headroom_commits_the_sub_batch_once() {
        let dir = tmp_dir("dl_headroom_once");
        let sealed = make_sealed_segment(&dir, 0, &[b"ok", b"bad"]);
        let needed = dead_letter::estimated_segment_bytes(&[Payload::from_static(b"bad")]);
        let cap = 4096;
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let filler = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&filler, vec![0u8; (cap - needed / 2) as usize]).unwrap();

        // Every commit accepts "ok" and rejects "bad" — as a real sink would on
        // every re-send.
        let sink = Arc::new(MockSink::with_responses(
            (0..50).map(|_| partial_reject(b"ok", b"bad")),
        ));
        let metrics = noop_metrics();
        let config = DrainConfig {
            dead_letter_check_interval: Duration::from_millis(50),
            ..tight_dl_config(dir.clone(), cap)
        };
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );

        wait_for(5, "the block", || drain_is_blocked(&metrics));
        // Ten check intervals with the total still short of `needed`.
        std::thread::sleep(Duration::from_millis(10 * 50 + 50));
        assert_eq!(
            sink.call_count(),
            1,
            "headroom short of what the blocked write needs must not unblock the drain"
        );
        assert!(drain_is_blocked(&metrics));

        // Free space beyond `needed`: the drain resumes once and writes the carry.
        std::fs::remove_file(&filler).unwrap();
        wait_for(5, "the confirm", || get_confirmed_path(&sealed).exists());
        drop(tx);
        handle.join().unwrap();

        assert_eq!(
            sink.call_count(),
            1,
            "the resume writes the carried rejects; it does not re-commit the sub-batch"
        );
        assert_eq!(sink.committed_records(), vec![Payload::from_static(b"ok")]);
        assert_eq!(
            dead_letter_contents(&dir),
            vec![Payload::from_static(b"bad")]
        );
        assert_eq!(
            records_committed(&metrics),
            1,
            "the accepted record counts once"
        );
        assert_eq!(records_dead_lettered(&metrics), 1);
        assert_eq!(dl_full_count(&metrics), 1, "one blocking episode");
        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.5: `rm -rf dead_letter/` is the obvious way to free headroom.
    /// The rescan used to fail on the missing directory, leaving the total at
    /// its at-cap value, so the drain never unblocked. Now the rescan recreates
    /// the directory and the drain resumes within one check interval.
    #[test]
    fn deleting_the_dead_letter_dir_while_blocked_resumes_within_one_interval() {
        let dir = tmp_dir("dl_dir_deleted");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let interval = Duration::from_millis(400);
        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let metrics = noop_metrics();
        let config = DrainConfig {
            dead_letter_check_interval: interval,
            ..tight_dl_config(dir.clone(), 100)
        };
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            config,
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        wait_for(5, "the block", || drain_is_blocked(&metrics));

        let deleted_at = Instant::now();
        std::fs::remove_dir_all(&dl_dir).unwrap();
        let deadline = deleted_at + Duration::from_secs(3);
        while !get_confirmed_path(&sealed).exists() {
            assert!(
                Instant::now() < deadline,
                "a drain blocked on dead-letter headroom must resume once dead_letter/ is deleted"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let took = deleted_at.elapsed();
        drop(tx);
        handle.join().unwrap();

        assert!(
            took < interval + Duration::from_millis(500),
            "resumed after {took:?}; expected within one {interval:?} check interval"
        );
        assert!(dl_dir.is_dir(), "dead_letter/ is recreated");
        assert_eq!(
            dead_letter_contents(&dir),
            vec![Payload::from_static(b"record")]
        );
        assert_eq!(sink.call_count(), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Spec §4.11. The sink commits first and the rejected records are written
    /// to `dead_letter/` afterwards. When that write failed (ENOSPC on the WAB
    /// device), the attempt was transient with the cursor unmoved, so every
    /// retry and every strand cycle re-committed the accepted records — with
    /// no bound. Now the rejected records are carried and each retry redoes
    /// only the write.
    #[test]
    fn a_failed_dead_letter_write_redoes_only_the_write() {
        let dir = tmp_dir("dl_write_enospc");
        let sealed = make_sealed_segment(&dir, 0, &[b"ok", b"bad"]);
        let sink = Arc::new(MockSink::with_responses(
            (0..50).map(|_| partial_reject(b"ok", b"bad")),
        ));
        let metrics = noop_metrics();
        // 10 failed writes span more than two strand cycles at max_retries = 3
        // (4 attempts each), so the carry has to survive strands and the idle
        // rescan's re-admission, not just the in-arm retries.
        let dl_dir = dir.join("dead_letter");
        dead_letter::write_hook::fail(&dl_dir, libc::ENOSPC, 10);

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let handle = spawn(
            rx,
            Arc::clone(&sink),
            fast_config(dir.clone()),
            Arc::clone(&metrics),
            DrainHeartbeat::new(),
        );
        wait_for(10, "the confirm", || get_confirmed_path(&sealed).exists());
        drop(tx);
        handle.join().unwrap();
        let fired = dead_letter::write_hook::fired(&dl_dir);
        dead_letter::write_hook::forget(&dl_dir);

        assert_eq!(fired, 10, "the hook failed ten writes");
        assert_eq!(
            sink.call_count(),
            1,
            "the sub-batch is committed once; only the dead-letter write is retried"
        );
        assert_eq!(sink.committed_records(), vec![Payload::from_static(b"ok")]);
        assert_eq!(
            dead_letter_contents(&dir),
            vec![Payload::from_static(b"bad")],
            "the dead-letter directory holds each rejected record exactly once"
        );
        assert_eq!(metrics.drain_dead_letter_write_failures.get(), 10);
        assert_eq!(
            local_io_strands(&metrics, crate::metrics::LocalIoStrandCause::write),
            2,
            "two strand cycles of four failed writes each"
        );
        assert_eq!(
            metrics.drain_segments_stranded.get(),
            0,
            "not a sink strand"
        );
        assert_eq!(records_committed(&metrics), 1);
        assert_eq!(records_dead_lettered(&metrics), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The carry bound (spec §4.5): a carry that would push the in-memory total
    /// over the bound is not stored, and that segment falls back to the old
    /// behaviour — the sub-batch is re-committed once when the drain resumes —
    /// rather than holding unbounded payloads. Bytes are never dropped.
    ///
    /// Without a carry, the unblock predicate is the only thing between a short
    /// headroom and a re-commit per check interval, so this is also the test
    /// that pins the predicate itself: ten intervals with the total in
    /// `[cap - needed, cap)` must not unblock the drain.
    #[test]
    fn a_carry_over_the_bound_falls_back_to_recommitting_once() {
        let dir = tmp_dir("dl_carry_over_bound");
        let sealed = make_sealed_segment(&dir, 0, &[b"ok", b"bad"]);
        let needed = dead_letter::estimated_segment_bytes(&[Payload::from_static(b"bad")]);
        let cap = 4096;
        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        let filler = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&filler, vec![0u8; (cap - needed / 2) as usize]).unwrap();

        let sink = Arc::new(MockSink::with_responses(
            (0..50).map(|_| partial_reject(b"ok", b"bad")),
        ));
        let metrics = noop_metrics();
        let config = DrainConfig {
            dead_letter_check_interval: Duration::from_millis(50),
            ..tight_dl_config(dir.clone(), cap)
        };
        // "bad" is 3 bytes; a 2-byte bound refuses it.
        let mut ledger = fresh_ledger().with_dl_carry_max_bytes(2);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let (sink_t, metrics_t) = (Arc::clone(&sink), Arc::clone(&metrics));
        let handle = std::thread::spawn(move || {
            super::drain_thread(
                rx,
                sink_t,
                config,
                metrics_t,
                true,
                DrainHeartbeat::new(),
                &mut ledger,
            );
            ledger
        });

        wait_for(5, "the block", || drain_is_blocked(&metrics));
        std::thread::sleep(Duration::from_millis(10 * 50 + 50));
        assert_eq!(
            sink.call_count(),
            1,
            "headroom short of `needed` must not unblock the drain, or every check \
             interval re-commits the sub-batch"
        );
        std::fs::remove_file(&filler).unwrap();
        wait_for(5, "the confirm", || get_confirmed_path(&sealed).exists());
        drop(tx);
        let ledger = handle.join().unwrap();

        assert_eq!(
            sink.call_count(),
            2,
            "without a stored carry the sub-batch is re-committed exactly once"
        );
        assert_eq!(
            sink.committed_records(),
            vec![Payload::from_static(b"ok"), Payload::from_static(b"ok")]
        );
        assert_eq!(
            dead_letter_contents(&dir),
            vec![Payload::from_static(b"bad")]
        );
        assert_eq!(
            ledger
                .counts()
                .dl_carry_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// F09: a flapping cap (unblock → reblock the same pressure without
    /// intervening progress) must count as ONE episode — `dead_letter_full`
    /// increments once and `blocked_since` is preserved — while a genuine
    /// confirm ends the episode and resets the duration gauge.
    #[test]
    fn block_episode_counts_once_across_flap_then_resets_on_progress() {
        let (m, _reg) = Metrics::new();
        let mut episode: Option<Instant> = None;
        let seg = PathBuf::from("/tmp/weir-f09-seg");

        // First genuine entry: counts and stamps the episode start.
        let _ = enter_blocked(seg.clone(), &m, &mut episode, 0);
        assert_eq!(m.dead_letter_full.get(), 1);
        let first_start = episode.expect("episode stamped on entry");

        // Re-enter blocked after a transient unblock (no confirm in between):
        // must NOT re-increment and must reuse the original start instant.
        let _ = enter_blocked(seg.clone(), &m, &mut episode, 0);
        assert_eq!(m.dead_letter_full.get(), 1, "flap must not re-increment");
        assert_eq!(
            episode.expect("episode still open"),
            first_start,
            "blocked_since preserved across flap"
        );

        // Genuine progress (a segment confirmed): end the episode and reset the
        // duration gauge to zero.
        m.dead_letter_blocked_duration.set(5.0);
        end_block_episode(&mut episode, &m);
        assert!(episode.is_none(), "episode cleared on progress");
        assert_eq!(m.dead_letter_blocked_duration.get(), 0.0);

        // A brand-new episode after a real end counts again.
        let _ = enter_blocked(seg, &m, &mut episode, 0);
        assert_eq!(m.dead_letter_full.get(), 2, "new episode after end counts");
    }

    #[test]
    fn drain_state_gauge_reflects_active_state() {
        // Structural check: set_drain_state sets exactly one gauge to 1.0.
        let (m, _reg) = Metrics::new();
        set_drain_state(&m, DrainStateValue::draining);
        assert_eq!(
            m.drain_state
                .get_or_create(&DrainStateLabel {
                    state: DrainStateValue::draining
                })
                .get(),
            1.0
        );
        assert_eq!(
            m.drain_state
                .get_or_create(&DrainStateLabel {
                    state: DrainStateValue::retrying_transient
                })
                .get(),
            0.0
        );
        assert_eq!(
            m.drain_state
                .get_or_create(&DrainStateLabel {
                    state: DrainStateValue::blocked_dead_letter_full
                })
                .get(),
            0.0
        );

        set_drain_state(&m, DrainStateValue::blocked_dead_letter_full);
        assert_eq!(
            m.drain_state
                .get_or_create(&DrainStateLabel {
                    state: DrainStateValue::draining
                })
                .get(),
            0.0
        );
        assert_eq!(
            m.drain_state
                .get_or_create(&DrainStateLabel {
                    state: DrainStateValue::blocked_dead_letter_full
                })
                .get(),
            1.0
        );
    }

    // ── max_batch_size ────────────────────────────────────────────────────────

    #[test]
    fn max_batch_size_respected_for_large_segment() {
        let dir = tmp_dir("batch_size");
        let sealed = make_sealed_segment(&dir, 0, &[b"a", b"b", b"c", b"d", b"e"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_batch_size(2, []));
        let metrics = noop_metrics();
        run_drain(
            rx,
            tx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
        );

        assert_eq!(
            sink.call_count(),
            3,
            "5 records with batch=2 → 3 commit calls"
        );
        assert_eq!(records_committed(&metrics), 5);

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Dead-letter file format ───────────────────────────────────────────────

    #[test]
    fn dead_letter_segment_readable_with_valid_crcs() {
        let dir = tmp_dir("dl_readable");
        let sealed = make_sealed_segment(&dir, 0, &[b"dead1", b"dead2"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        let dl_dir = dir.join("dead_letter");
        let dl_files: Vec<PathBuf> = std::fs::read_dir(&dl_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_str().unwrap_or("").ends_with(".wab.sealed"))
            .collect();

        assert!(!dl_files.is_empty(), "dead-letter files must exist");
        for path in &dl_files {
            let records: Vec<Payload> = crate::wab::SegmentReader::open(path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .expect("dead-letter records must have valid CRCs");
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], b"dead1" as &[u8]);
            assert_eq!(records[1], b"dead2" as &[u8]);
        }

        std::fs::remove_dir_all(dir).ok();
    }

    // ── End-to-end behaviour (extended MockSink: retry_after hints, captures) ──

    /// `dead_letter_segment_readable_with_valid_crcs` already verifies the
    /// dead-letter file's *bytes*. This test verifies the complete payload
    /// pass-through view: the drain hands the sink exactly the records
    /// from the segment, the sink's reported committed / dead-lettered
    /// split reaches the dead-letter file on disk, and the metric counts
    /// agree with the mock's introspection capture. Catches a regression
    /// where the drain duplicates, drops, or reorders records between
    /// segment-read and sink-commit.
    #[test]
    fn mock_captures_show_exact_payloads_pass_through_drain() {
        let dir = tmp_dir("payload_passthrough");
        let sealed = make_sealed_segment(&dir, 0, &[b"alpha", b"beta", b"gamma"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([MockSink::ok_with_dead_letter(
            vec![
                Payload::from(b"alpha".as_ref()),
                Payload::from(b"gamma".as_ref()),
            ],
            vec![Payload::from(b"beta".as_ref())],
        )]));
        let metrics = noop_metrics();
        run_drain(
            rx,
            tx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
        );

        // Mock-captured view matches what the sink actually saw.
        let committed = sink.committed_records();
        let dead_lettered = sink.dead_lettered_records();
        assert_eq!(committed, vec![b"alpha".to_vec(), b"gamma".to_vec()]);
        assert_eq!(dead_lettered.len(), 1);
        assert_eq!(dead_lettered[0].0, b"beta" as &[u8]);

        // Metric counts agree with the capture.
        assert_eq!(records_committed(&metrics), committed.len() as u64);
        assert_eq!(records_dead_lettered(&metrics), dead_lettered.len() as u64);

        // And the dead-letter file on disk contains exactly the records the
        // sink reported as permanently rejected — end-to-end agreement.
        let dl_dir = dir.join("dead_letter");
        let dl_files: Vec<PathBuf> = std::fs::read_dir(&dl_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_str().unwrap_or("").ends_with(".wab.sealed"))
            .collect();
        assert_eq!(dl_files.len(), 1, "expected one dead-letter file");
        let dl_records: Vec<Payload> = crate::wab::SegmentReader::open(&dl_files[0])
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(dl_records, vec![b"beta".to_vec()]);

        // Segment was confirmed (and so deleted from the WAB dir).
        assert!(get_confirmed_path(&sealed).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    /// `next_retry_delay`'s unit tests verify the helper picks the hint
    /// over the default. This test verifies the drain ACTUALLY SLEEPS
    /// the hinted duration — the wall-clock gap between the failing
    /// commit and the retry must be ≥ the hint, not the fast_config
    /// default (1 ms).
    #[test]
    fn drain_waits_retry_after_hint_before_retrying() {
        let dir = tmp_dir("retry_after");
        let sealed = make_sealed_segment(&dir, 0, &[b"hello"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // 75 ms hint — distinguishable from the 1 ms fast_config default
        // by more than scheduling jitter.
        const HINT: Duration = Duration::from_millis(75);
        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::TransientWithRetryAfter(HINT)),
            MockSink::ok(vec![Payload::from(b"hello".as_ref())]),
        ]));
        let metrics = noop_metrics();

        // Hold the channel OPEN across the backoff. In production the drain channel
        // only disconnects on shutdown, and the interruptible backoff (S15) cuts
        // the Retry-After short ONLY on disconnect — so this exercises the
        // normal-operation timing path. Dropping `tx` during the wait would
        // (correctly) model a shutdown and cut the backoff; we drop it only after
        // both commit attempts have happened.
        let handle = spawn(
            rx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
            DrainHeartbeat::new(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if sink.call_timestamps().len() >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for the retry"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(tx);
        handle.join().unwrap();

        let timestamps = sink.call_timestamps();
        assert_eq!(timestamps.len(), 2, "expected exactly two commit calls");
        let gap = timestamps[1].duration_since(timestamps[0]);
        // Loose lower bound (60 ms) accounts for the drain's coarse
        // sleep granularity and any sandbox scheduling jitter — well
        // above the 1 ms default it would have used without the hint.
        assert!(
            gap >= Duration::from_millis(60),
            "expected gap >= 60 ms (hinted 75 ms), got {gap:?}"
        );
        // Final state: committed, confirmed.
        assert_eq!(records_committed(&metrics), 1);
        assert!(get_confirmed_path(&sealed).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    /// S15: a long Retry-After must not stall shutdown. Once the drain is in the
    /// backoff, closing the channel (the only shutdown signal the drain sees) must
    /// interrupt the wait — the thread joins well within the 30s hint, leaving the
    /// segment unconfirmed for replay on restart.
    #[test]
    fn retry_backoff_is_interrupted_by_shutdown() {
        let dir = tmp_dir("retry_interrupt");
        let sealed = make_sealed_segment(&dir, 0, &[b"x"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        const LONG: Duration = Duration::from_secs(30);
        // All-transient with a long hint: without interruption the drain would
        // sleep ~30s per attempt. With enough responses it can never confirm.
        let responses: Vec<MockResult> = (0..=MAX_RETRIES + 1)
            .map(|_| Err(MockError::TransientWithRetryAfter(LONG)))
            .collect();
        // The probe reports Down for the whole test, matching the scenario: a sink
        // that transiently fails every POST is a sink that is out. This has to be
        // explicit now that the shutdown backoff is clamped rather than dropped
        // (W2) — the retry episode outlives `health_poll_interval`, so a probe that
        // answered Healthy would give the Draining arm a down→up edge, re-queue the
        // stranded segment via the 4a auto-resume and deliver it before exit. That
        // is a legitimate (better) outcome, but it is not the S15 property, and
        // pinning it here would make this test about auto-resume instead of about
        // shutdown latency.
        let sink = Arc::new(MockSink {
            health_script: Mutex::new(
                std::iter::repeat_n(SinkHealth::Down("test".into()), 64).collect(),
            ),
            ..MockSink::with_responses(responses)
        });
        let metrics = noop_metrics();
        let handle = spawn(
            rx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
            DrainHeartbeat::new(),
        );

        // Wait until the first commit attempt has happened (drain is now in backoff).
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if !sink.call_timestamps().is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for the first commit attempt"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        // Shutdown: drop the channel and assert the drain does NOT wait out the 30s
        // backoff (it interrupts and unwinds its retries promptly).
        let t = std::time::Instant::now();
        drop(tx);
        handle.join().unwrap();
        let elapsed = t.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown must interrupt the 30s backoff, took {elapsed:?}"
        );
        assert!(
            !get_confirmed_path(&sealed).exists(),
            "segment must be left unconfirmed on shutdown"
        );
        assert!(
            sealed.exists(),
            "segment preserved on disk for replay on restart"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    /// The `.confirmed` sidecar marks the segment as successfully drained
    /// and consumed. It must NOT appear after a transient error — only
    /// after the final, successful commit. Catches a regression where
    /// the drain writes `.confirmed` optimistically before the sink
    /// actually acks, which would silently lose data on a real-world
    /// retry-then-give-up scenario.
    #[test]
    fn confirmed_file_only_appears_after_successful_commit_not_during_retries() {
        let dir = tmp_dir("confirmed_only_after_success");
        let sealed = make_sealed_segment(&dir, 0, &[b"r1", b"r2"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // Three transient failures, then success. The confirmed path
        // must not exist after any of the failures — only after the
        // last (successful) call.
        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Transient),
            Err(MockError::Transient),
            Err(MockError::Transient),
            MockSink::ok(vec![
                Payload::from(b"r1".as_ref()),
                Payload::from(b"r2".as_ref()),
            ]),
        ]));
        let metrics = noop_metrics();
        run_drain(
            rx,
            tx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
        );

        // Sink saw all four calls.
        assert_eq!(sink.call_count(), 4);
        // Only the last call committed; the prior three were transient
        // errors — committed_records should reflect only the success.
        assert_eq!(
            sink.committed_records(),
            vec![b"r1".to_vec(), b"r2".to_vec()]
        );
        // Confirmed file present, segment file deleted — final-state
        // invariant.
        assert!(get_confirmed_path(&sealed).exists());
        assert!(
            !sealed.exists(),
            "segment file should be deleted after confirm"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    #[test]
    fn drain_exits_when_channel_closes() {
        let dir = tmp_dir("exit");
        let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
        let sink = Arc::new(MockSink::with_responses([]));
        let handle = spawn(
            rx,
            sink,
            fast_config(dir.clone()),
            noop_metrics(),
            DrainHeartbeat::new(),
        );
        drop(tx);
        let done = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < done {
            if handle.is_finished() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("drain thread did not exit within 3 seconds after channel close");
    }

    #[test]
    fn shutdown_while_blocked_does_not_confirm_segment() {
        let dir = tmp_dir("blocked_shutdown");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);

        let dl_dir = dir.join("dead_letter");
        crate::testutil::mkdir_p(&dl_dir);
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();
        let handle = spawn(rx, sink, config, metrics.clone(), DrainHeartbeat::new());

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if drain_is_blocked(&metrics) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(std::time::Instant::now() < deadline, "timed out");
        }

        drop(tx);
        handle.join().unwrap();

        assert!(
            !get_confirmed_path(&sealed).exists(),
            "segment must NOT be confirmed on blocked shutdown"
        );
        assert!(
            sealed.exists(),
            "segment must still exist after blocked shutdown"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    // ── Truthful drain liveness (spec §4.8) ──────────────────────────────────

    #[test]
    fn an_idle_heartbeat_reports_nothing_busy() {
        let hb = DrainHeartbeat::new();
        assert_eq!(hb.busy(), None);
        hb.bump();
        assert_eq!(hb.busy(), None, "a bump is liveness, not a busy operation");
    }

    #[test]
    fn a_busy_guard_names_its_kind_and_clears_on_drop() {
        let hb = DrainHeartbeat::new();
        // The poll task reads a clone; the signal must be shared, not copied.
        let observer = hb.clone();
        {
            let _busy = BusyGuard::new(&hb, BusyKind::LocalFsync);
            std::thread::sleep(Duration::from_millis(30));
            let (kind, busy_for) = observer.busy().expect("busy inside the guard");
            assert_eq!(kind, BusyKind::LocalFsync);
            assert!(
                busy_for >= Duration::from_millis(25),
                "busy_for counts from the guard's creation: {busy_for:?}"
            );
        }
        assert_eq!(
            observer.busy(),
            None,
            "dropping the guard clears the signal"
        );
    }

    /// The guard is the only thing that clears the busy signal, so a panic
    /// inside a guarded call (a sink impl, a disk helper) must clear it on the
    /// way out. A respawned drain otherwise starts life reporting the dead
    /// drain's operation as still in flight, which the verdict would read as a
    /// slow call instead of what it is.
    #[test]
    fn a_busy_guard_clears_on_a_panic_unwind() {
        let hb = DrainHeartbeat::new();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _busy = BusyGuard::new(&hb, BusyKind::SinkCommit);
            assert!(hb.busy().is_some());
            panic!("induced panic inside a guarded sink call");
        }));
        assert!(unwound.is_err(), "precondition: the closure panicked");
        assert_eq!(
            hb.busy(),
            None,
            "a guard dropped by a panic unwind must clear the busy signal"
        );
    }

    /// The reader loop holds a `SegmentRead` guard and switches to `SinkCommit`
    /// around each commit. Leaving the commit hands the signal back to the
    /// read, timed from when the read resumed, not from the start of the
    /// segment.
    #[test]
    fn a_nested_busy_guard_restores_the_outer_kind() {
        let hb = DrainHeartbeat::new();
        let _read = BusyGuard::new(&hb, BusyKind::SegmentRead);
        std::thread::sleep(Duration::from_millis(60));
        {
            let _commit = BusyGuard::new(&hb, BusyKind::SinkCommit);
            let (kind, busy_for) = hb.busy().unwrap();
            assert_eq!(kind, BusyKind::SinkCommit);
            assert!(busy_for < Duration::from_millis(50), "{busy_for:?}");
        }
        let (kind, busy_for) = hb.busy().unwrap();
        assert_eq!(kind, BusyKind::SegmentRead, "the read resumes");
        assert!(
            busy_for < Duration::from_millis(50),
            "the resumed read is timed from its resumption: {busy_for:?}"
        );
        drop(_read);
        assert_eq!(hb.busy(), None);
    }

    /// A sink that records what the drain's busy signal says while each of its
    /// calls is in progress.
    struct BusyProbeSink {
        hb: DrainHeartbeat,
        in_commit: Mutex<Vec<Option<BusyKind>>>,
        in_health: Mutex<Vec<Option<BusyKind>>>,
    }

    impl crate::sink::Sink for BusyProbeSink {
        type Error = MockError;

        async fn commit(&self, batch: SinkBatch) -> MockResult {
            self.in_commit
                .lock()
                .unwrap()
                .push(self.hb.busy().map(|(k, _)| k));
            Ok(CommitResult::new(batch.into_records(), vec![]))
        }

        fn max_batch_size(&self) -> usize {
            2
        }

        async fn health(&self) -> SinkHealth {
            self.in_health
                .lock()
                .unwrap()
                .push(self.hb.busy().map(|(k, _)| k));
            SinkHealth::Healthy
        }
    }

    #[test]
    fn the_busy_signal_names_the_sink_call_in_flight() {
        let dir = tmp_dir("busy_sink_calls");
        let (sealed, _) = make_numbered_segment(&dir, 5);
        let hb = DrainHeartbeat::new();
        let sink = BusyProbeSink {
            hb: hb.clone(),
            in_commit: Mutex::new(Vec::new()),
            in_health: Mutex::new(Vec::new()),
        };
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let mut dl = DeadLetterWriter::open(&dir).unwrap();

        let result = block_on(super::process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, 0, false, &hb,
        ));
        assert!(matches!(
            result,
            ProcessResult::Confirmed { record_count: 5 }
        ));
        assert_eq!(
            *sink.in_commit.lock().unwrap(),
            vec![Some(BusyKind::SinkCommit); 3],
            "every commit runs under SinkCommit, not the surrounding SegmentRead"
        );
        assert_eq!(hb.busy(), None, "nothing is busy once the segment is done");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        super::probe_health(&rt, &sink, Duration::from_secs(5), &hb);
        assert_eq!(
            *sink.in_health.lock().unwrap(),
            vec![Some(BusyKind::SinkProbe)]
        );
        assert_eq!(hb.busy(), None);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The heartbeat used to move only at the loop head and inside the two
    /// waits, so one segment (up to 256 MiB, delivered inside a single
    /// `block_on`) froze it for as long as the whole segment took. It now moves
    /// after every commit and every 1,024 records read — the resumed prefix a
    /// retry re-walks included.
    #[test]
    fn reading_and_committing_advance_the_heartbeat() {
        let dir = tmp_dir("heartbeat_reads");
        let records: Vec<Vec<u8>> = (0..2 * READ_HEARTBEAT_EVERY)
            .map(|i| (i as u32).to_le_bytes().to_vec())
            .collect();
        let refs: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
        let sealed = make_sealed_segment(&dir, 0, &refs);
        let sink = MockSink::with_batch_size(1, []);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let mut dl = DeadLetterWriter::open(&dir).unwrap();

        // Every record is already durable: the attempt only re-walks them.
        let reads = DrainHeartbeat::new();
        let skip = records.len() as u64;
        let result = block_on(super::process_segment(
            &sealed, &sink, &config, &metrics, &mut dl, skip, false, &reads,
        ));
        assert!(matches!(result, ProcessResult::Confirmed { .. }));
        assert_eq!(sink.call_count(), 0, "precondition: nothing was committed");
        assert_eq!(
            reads.get(),
            2,
            "one bump per {READ_HEARTBEAT_EVERY} records read"
        );

        // Three one-record commits past the same prefix: one bump each.
        let commits = DrainHeartbeat::new();
        let result = block_on(super::process_segment(
            &sealed,
            &sink,
            &config,
            &metrics,
            &mut dl,
            skip - 3,
            false,
            &commits,
        ));
        assert!(matches!(result, ProcessResult::Confirmed { .. }));
        assert_eq!(sink.call_count(), 3);
        assert_eq!(commits.get(), 2 + 3, "two read bumps plus one per commit");
        std::fs::remove_dir_all(dir).ok();
    }

    /// The case the old verdict got wrong. A sink that takes 2 s per commit and
    /// YIELDS while it does is slow, not wedged — `commit_timeout` can fire on
    /// it — yet with the heartbeat moving only at the loop head, a 10-batch
    /// segment froze it for 20 s, past the 16 s threshold at a 1 s
    /// `health_poll_interval`, and the operator was told to restart. Observed
    /// here as `main.rs`'s poll task observes it, at 50 ms instead of 5 s:
    /// against the production thresholds nothing is ever reported, and against
    /// thresholds scaled below one commit the freeze is reported as what it is,
    /// a slow sink call.
    #[test]
    fn a_slow_but_yielding_sink_never_reads_as_a_wedged_drain() {
        const COMMIT: Duration = Duration::from_secs(2);
        const BATCHES: usize = 10;
        let dir = tmp_dir("liveness_slow_sink");
        let (sealed, _) = make_numbered_segment(&dir, BATCHES);
        let sink = Arc::new(MockSink::with_yielding_commit(COMMIT, 1));
        let heartbeat = DrainHeartbeat::new();
        let config = DrainConfig {
            health_poll_interval: Duration::from_secs(1),
            commit_timeout: Duration::from_secs(30),
            ..fast_config(dir.clone())
        };
        let (interval, commit_timeout) = (config.health_poll_interval, config.commit_timeout);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let (sink_t, hb_t) = (Arc::clone(&sink), heartbeat.clone());
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink_t, config, noop_metrics(), true, hb_t)
        });

        let mut last = heartbeat.get();
        let mut changed_at = Instant::now();
        let mut longest_freeze = Duration::ZERO;
        let mut slow_sink_calls = 0;
        let deadline = Instant::now() + Duration::from_secs(60);
        while sealed.exists() {
            assert!(
                Instant::now() < deadline,
                "timed out delivering the segment"
            );
            std::thread::sleep(Duration::from_millis(50));
            let now = Instant::now();
            let beat = heartbeat.get();
            if beat != last {
                (last, changed_at) = (beat, now);
                continue;
            }
            let frozen_for = now.duration_since(changed_at);
            longest_freeze = longest_freeze.max(frozen_for);
            let busy = heartbeat.busy();
            assert_ne!(
                crate::drain_stall_verdict(frozen_for, interval, false, busy, commit_timeout),
                Some(crate::DrainStallVerdict::Wedged),
                "a slow, yielding sink read as a wedged drain (frozen {frozen_for:?}, busy \
                 {busy:?})"
            );
            let scaled = crate::classify_drain_stall(
                frozen_for >= Duration::from_millis(500),
                false,
                busy,
                commit_timeout + Duration::from_secs(1),
            );
            assert_ne!(
                scaled,
                Some(crate::DrainStallVerdict::Wedged),
                "frozen {frozen_for:?}, busy {busy:?}"
            );
            if matches!(scaled, Some(crate::DrainStallVerdict::SlowSinkCall { .. })) {
                slow_sink_calls += 1;
            }
        }
        drop(tx);
        handle.join().unwrap();

        assert_eq!(sink.call_count(), BATCHES as u64);
        assert!(
            longest_freeze < COMMIT + Duration::from_secs(1),
            "the heartbeat must advance between commits, not once per segment \
             (longest freeze {longest_freeze:?})"
        );
        assert!(
            slow_sink_calls > 0,
            "a freeze inside a commit must be reported as a slow sink call"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// The backstop the busy signal cannot replace. A sink that blocks the
    /// drain's OS thread defeats `commit_timeout` (the timer is never polled),
    /// so the `SinkCommit` guard sits OUTSIDE the timeout and stays set for the
    /// whole block. Past `commit_timeout` plus the grace that a timeout which
    /// could fire would have fired, so the verdict turns from a slow sink call
    /// into a wedge.
    #[test]
    fn a_sink_blocking_the_thread_past_commit_timeout_plus_grace_reads_as_wedged() {
        const STALL_AFTER: Duration = Duration::from_millis(200);
        const GRACE: Duration = Duration::from_millis(400);
        let dir = tmp_dir("liveness_blocking_sink");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);
        let sink = Arc::new(MockSink::with_blocking_commit(1500, []));
        let heartbeat = DrainHeartbeat::new();
        let config = DrainConfig {
            commit_timeout: Duration::from_millis(50),
            ..fast_config(dir.clone())
        };
        let limit = config.commit_timeout + GRACE;
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();
        let (sink_t, hb_t) = (Arc::clone(&sink), heartbeat.clone());
        let handle = std::thread::spawn(move || {
            drain_thread(rx, sink_t, config, noop_metrics(), true, hb_t)
        });

        wait_for(5, "the blocking commit to start", || sink.call_count() >= 1);
        let frozen_at = heartbeat.get();
        let since = Instant::now();
        let mut verdicts = Vec::new();
        while heartbeat.get() == frozen_at && since.elapsed() < Duration::from_millis(1400) {
            std::thread::sleep(Duration::from_millis(20));
            let busy = heartbeat.busy();
            if let Some(v) =
                crate::classify_drain_stall(since.elapsed() >= STALL_AFTER, false, busy, limit)
            {
                if verdicts.last() != Some(&std::mem::discriminant(&v)) {
                    verdicts.push(std::mem::discriminant(&v));
                }
                if v == crate::DrainStallVerdict::Wedged {
                    let (kind, busy_for) = busy.expect("the blocked commit is still busy");
                    assert_eq!(kind, BusyKind::SinkCommit);
                    assert!(busy_for > limit, "{busy_for:?}");
                    break;
                }
            }
        }
        let slow = std::mem::discriminant(&crate::DrainStallVerdict::SlowSinkCall {
            kind: BusyKind::SinkCommit,
            busy_for: Duration::ZERO,
        });
        let wedged = std::mem::discriminant(&crate::DrainStallVerdict::Wedged);
        assert_eq!(
            verdicts,
            vec![slow, wedged],
            "a slow sink call first, then a wedge once it outlives commit_timeout + grace"
        );

        drop(tx);
        handle.join().unwrap();
        assert_eq!(heartbeat.busy(), None);
        std::fs::remove_dir_all(dir).ok();
    }
}
