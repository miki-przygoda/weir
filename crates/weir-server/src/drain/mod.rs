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
//! RetryingTransient  ──(MAX_RETRIES exhausted)──▶  Draining (next segment)
//!   │  success
//!   ▼
//! Draining
//!   │  Permanent error AND dead-letter cap exceeded
//!   ▼
//! BlockedDeadLetterFull  ──(cap clears)──▶  RetryingTransient (same segment,
//!                                            RESUMING past already-processed
//!                                            sub-batches — see F05)
//! ```
//!
//! # Confirmed files
//!
//! After draining a segment the drain writes a `.confirmed` sidecar and deletes
//! the sealed segment. Crash recovery skips segments with a valid `.confirmed` file.
//! A crash between writing `.confirmed` and deleting the segment is safe: crash
//! recovery will skip the segment (confirmed), and the orphan sealed file can be
//! cleaned up on the next startup.

mod confirmed;
pub mod dead_letter;

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_channel::RecvTimeoutError;
use tracing::{error, info, warn};
use weir_core::Payload;

use crate::{
    metrics::{
        DrainStateLabel, DrainStateValue, Metrics, Outcome, OutcomeLabel, SinkHealthLabel,
        SinkHealthState,
    },
    sink::{Sink, SinkError, SinkHealth, SinkRecord},
    wab::{SegmentReader, read_segment_record_count},
};

use confirmed::confirm_and_delete;
use dead_letter::DeadLetterWriter;

// ── Constants ─────────────────────────────────────────────────────────────────

pub const MAX_RETRIES: u32 = 3;

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
    },
    BlockedDeadLetterFull {
        segment: PathBuf,
        blocked_since: Instant,
        /// Records already durably processed before the block; the post-headroom
        /// retry resumes past them (F05).
        processed: u64,
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
    Transient {
        retry_after: Option<Duration>,
        processed: u64,
    },
    /// Dead-letter cap would be exceeded. Block until capacity frees. `processed`
    /// carries the durable progress so the post-headroom retry resumes past the
    /// already-handled sub-batches (F05).
    BlockedDeadLetter { processed: u64 },
    /// A record failed to read mid-segment. The readable prefix was delivered and
    /// the segment was quarantined for manual recovery — do not retry or delete.
    Quarantined,
}

enum BatchResult {
    Ok,
    Transient { retry_after: Option<Duration> },
    Blocked,
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
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("weir-drain".into())
        .spawn(move || run_drain_supervised(drain_rx, sink, config, metrics))
        .expect("failed to spawn drain thread")
}

/// Maximum times the drain is respawned after a panic before giving up — past
/// this, delivery stops and the WAB accumulates on disk until restart (the same
/// policy as the WAB flusher supervisor).
const MAX_DRAIN_RESPAWNS: u32 = 10;

/// Runs [`drain_thread`] under panic supervision. A panic in the drain (a sink
/// impl, `process_segment`, `confirm_and_delete`, …) is caught and the drain is
/// respawned with a fresh runtime + dead-letter writer, reading from the same
/// channel so buffered segments survive. Without this, a single panic would kill
/// delivery permanently while producers keep being acked and the WAB grows
/// unbounded. The segment in flight at panic time is not re-delivered this run,
/// but it is durable on disk and replayed on the next restart.
fn run_drain_supervised<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
) {
    let mut attempts = 0u32;
    loop {
        let rx = drain_rx.clone();
        let sink = Arc::clone(&sink);
        let cfg = config.clone();
        let m = Arc::clone(&metrics);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drain_thread(rx, sink, cfg, m)
        }));
        // A clean Ok(()) return means the channel closed (all flushers gone) — done.
        let Err(_) = result else { break };
        attempts += 1;
        metrics.drain_panics.inc();
        if attempts >= MAX_DRAIN_RESPAWNS {
            error!(
                attempts,
                "drain thread panicked too many times; giving up — delivery is \
                 stopped and the WAB will accumulate on disk until restart"
            );
            break;
        }
        error!(
            attempts,
            max = MAX_DRAIN_RESPAWNS,
            "drain thread panicked; respawning"
        );
        std::thread::sleep(Duration::from_millis(
            100u64.saturating_mul(attempts as u64),
        ));
    }
    info!("drain supervisor exiting");
}

// ── Drain state helper ────────────────────────────────────────────────────────

fn set_drain_state(metrics: &Metrics, active: DrainStateValue) {
    let states = [
        DrainStateValue::draining,
        DrainStateValue::retrying_transient,
        DrainStateValue::blocked_dead_letter_full,
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

/// Probes [`Sink::health`] under a timeout backstop and maps a hang to `Down`.
///
/// The drain runs on a single-threaded runtime; an un-timed `health()` that
/// hangs would wedge the entire drain — the idle loop would never return to
/// `recv` (missing shutdown), and the blocked-state poll would never reach its
/// `channel_closed` check. Mirrors the `commit()` timeout backstop so a
/// misbehaving sink degrades the health gauge instead of stalling delivery.
fn probe_health<S: Sink>(rt: &tokio::runtime::Runtime, sink: &S, timeout: Duration) -> SinkHealth {
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
fn probe_and_resume_stranded<S: Sink>(
    rt: &tokio::runtime::Runtime,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    pending: &mut VecDeque<PathBuf>,
    prev_health_ok: bool,
) -> bool {
    let health = probe_health(rt, sink, config.health_probe_timeout);
    let now_ok = matches!(health, SinkHealth::Healthy);
    set_sink_health(metrics, health);

    if now_ok && !prev_health_ok {
        // `None` shard_count: skip the beyond-configured-count advisory — that's
        // a startup-replay concern, not a recovery one.
        match crate::wab::scan_unconfirmed_sealed(&config.wab_dir, None) {
            Ok(sealed) => {
                // Dedup against what's already queued via a HashSet membership view
                // (O(n+m)) rather than VecDeque::contains per segment (O(n·m)) —
                // matters when a long outage strands many segments (F-rescan).
                let already: std::collections::HashSet<&PathBuf> = pending.iter().collect();
                let fresh: Vec<PathBuf> = sealed
                    .into_iter()
                    .filter(|seg| !already.contains(seg))
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

// ── Drain thread ──────────────────────────────────────────────────────────────

fn drain_thread<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<Metrics>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("drain tokio runtime");

    let mut dead_letter = match DeadLetterWriter::open(&config.wab_dir) {
        Ok(dl) => dl,
        Err(e) => {
            error!(error = %e, "drain: failed to open dead-letter writer; exiting");
            return;
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
    // when a segment is actually confirmed or quarantined — genuine progress (F09).
    let mut block_episode: Option<Instant> = None;

    // Tracks the sink's last-observed health so we can detect a down→up recovery
    // edge and re-drain stranded segments. Starts `true` (startup replay already
    // handled any pre-existing unconfirmed segments, so the first healthy probe
    // is not a recovery). Set `false` whenever a segment strands.
    let mut prev_health_ok = true;

    // Wall-clock anchor for the health-poll + stranded-segment rescan. Driving it
    // off elapsed time (not channel idleness) means recovery fires even under
    // sustained ingest — a busy drain channel can no longer starve it (the idle
    // recv_timeout branch alone never fired while segments kept arriving).
    let mut last_health_poll = Instant::now();

    'outer: loop {
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
                if last_health_poll.elapsed() >= config.health_poll_interval {
                    prev_health_ok = probe_and_resume_stranded(
                        &rt,
                        &*sink,
                        &config,
                        &metrics,
                        &mut pending,
                        prev_health_ok,
                    );
                    // Refresh the dead-letter size gauge too, so an out-of-band
                    // `weir-ctl dl drop` (an operator deleting files while the
                    // daemon runs) is reflected without waiting for the blocked
                    // state or a restart.
                    if dead_letter.rescan().is_ok() {
                        metrics
                            .dead_letter_bytes_on_disk
                            .set(dead_letter.total_bytes() as f64);
                    }
                    last_health_poll = Instant::now();
                }

                let next_segment = if let Some(p) = pending.pop_front() {
                    Some(p)
                } else {
                    // Idle: wait for a new segment, but no longer than the time
                    // left until the next health poll, then re-enter Draining so
                    // the wall-clock poll above runs. Channel closure (all WAB
                    // flushers exited) still ends the loop.
                    let until_next_poll = config
                        .health_poll_interval
                        .saturating_sub(last_health_poll.elapsed())
                        .max(Duration::from_millis(1));
                    match drain_rx.recv_timeout(until_next_poll) {
                        Ok(p) => Some(p),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => break 'outer,
                    }
                };

                match next_segment {
                    // Idle timeout with nothing queued: re-enter Draining so the
                    // wall-clock health poll above runs on schedule.
                    None => DrainState::Draining,
                    Some(segment) => {
                        // Fresh segment → process from the start (skip = 0). The
                        // health gauge + rescan are refreshed by the wall-clock
                        // poll above, so we no longer probe the sink after every
                        // segment (which, under sustained load, meant a HEAD
                        // request per segment).
                        let result = rt.block_on(process_segment(
                            &segment,
                            &*sink,
                            &config,
                            &metrics,
                            &mut dead_letter,
                            0,
                        ));
                        transition_from_draining(
                            segment,
                            result,
                            &config,
                            &metrics,
                            &mut block_episode,
                        )
                    }
                }
            }

            // ── RetryingTransient ─────────────────────────────────────────────
            DrainState::RetryingTransient {
                segment,
                retries_left,
                next_delay,
                processed,
            } => {
                set_drain_state(&metrics, DrainStateValue::retrying_transient);

                // Interruptible backoff: consume next_delay in short slices via
                // recv_timeout so a shutdown — which the drain only learns of as the
                // drain channel disconnecting (all WAB flushers have exited) — cuts
                // the wait short instead of sleeping out a multi-minute Retry-After.
                // On disconnect we stop waiting but STILL run the retry below (its
                // commit is bounded by commit_timeout), preserving the existing
                // "complete in-flight work on shutdown" behaviour; the Draining loop
                // then observes the same disconnect and exits. In production the
                // channel only closes on shutdown, so the Retry-After delay is only
                // ever cut at shutdown, never during normal operation. Segments that
                // arrive during the wait are buffered (S15).
                let wait_end = Instant::now() + next_delay;
                loop {
                    let remaining = wait_end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let poll = remaining.min(Duration::from_millis(50));
                    match drain_rx.recv_timeout(poll) {
                        Ok(new_seg) => pending.push_back(new_seg),
                        Err(RecvTimeoutError::Timeout) => {}
                        // Channel closed (shutdown): stop waiting, finish the retry.
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }

                if retries_left == 0 {
                    metrics.drain_segments_stranded.inc();
                    // The sink is failing; mark it down so the next healthy probe
                    // registers as a down→up recovery edge and re-drains this
                    // stranded segment without waiting for a restart.
                    prev_health_ok = false;
                    error!(
                        path = %segment.display(),
                        "drain: max retries exhausted; segment left on disk (stranded) — \
                         weir_drain_segments_stranded_total incremented; re-drained automatically \
                         when the sink recovers, or on restart"
                    );
                    DrainState::Draining
                } else {
                    // Resume past the sub-batches already durably processed (F05).
                    let result = rt.block_on(process_segment(
                        &segment,
                        &*sink,
                        &config,
                        &metrics,
                        &mut dead_letter,
                        processed,
                    ));
                    // Subsequent failure: spend one retry and double the delay
                    // (exponential backoff). retries_left is >= 1 here — the
                    // == 0 case returned above.
                    next_state_after_process(
                        segment,
                        result,
                        &metrics,
                        &mut block_episode,
                        |segment, retry_after, processed| DrainState::RetryingTransient {
                            segment,
                            retries_left: retries_left - 1,
                            next_delay: next_retry_delay(next_delay * 2, retry_after),
                            processed,
                        },
                    )
                }
            }

            // ── BlockedDeadLetterFull ─────────────────────────────────────────
            DrainState::BlockedDeadLetterFull {
                segment,
                blocked_since,
                processed,
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
                // A failed rescan leaves total_bytes stale at its at-cap value, so
                // the unblock check below can't clear and the drain stays blocked
                // even after the operator frees the dir — surface it rather than
                // swallowing (F08).
                if let Err(e) = dead_letter.rescan() {
                    warn!(error = %e, "drain: dead-letter rescan failed while blocked; total_bytes may be stale until the next successful rescan");
                }
                metrics
                    .dead_letter_bytes_on_disk
                    .set(dead_letter.total_bytes() as f64);

                // Refresh sink health every wake-cycle. Operators monitoring
                // weir_sink_health need to know if the sink came back up
                // while we were waiting on dead-letter headroom; without
                // this poll the gauge would be stuck at whatever value the
                // last segment commit produced.
                let health = probe_health(&rt, &*sink, config.health_probe_timeout);
                set_sink_health(&metrics, health);
                if dead_letter.total_bytes() < config.dead_letter_max_bytes {
                    // Headroom available — retry the preserved segment, RESUMING
                    // past the sub-batches already durably processed (skip =
                    // processed) rather than from record 0, so earlier sub-batches
                    // aren't re-committed/re-dead-lettered (F05). Route through
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
                    }
                }
            }
        };
    }

    info!("drain thread exiting");
}

/// Maps a [`ProcessResult`] to the next [`DrainState`]. The three terminal
/// outcomes are identical whether we arrived from a first attempt or a retry —
/// `Confirmed` confirms+deletes and moves on, `BlockedDeadLetter` enters the
/// blocked state, `Quarantined` (handled inside `process_segment`) moves on.
/// Only the `Transient → RetryingTransient` backoff differs between the two
/// callers (fresh budget vs. decremented/exponential), so each supplies it via
/// `on_transient`. This is the single source of truth for the non-retry
/// transitions — see the `Draining` and `RetryingTransient` arms in `run`.
fn next_state_after_process(
    segment: PathBuf,
    result: ProcessResult,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
    on_transient: impl FnOnce(PathBuf, Option<Duration>, u64) -> DrainState,
) -> DrainState {
    match result {
        ProcessResult::Confirmed { record_count } => {
            confirm_and_delete(&segment, record_count, metrics);
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
        ProcessResult::Transient {
            retry_after,
            processed,
        } => on_transient(segment, retry_after, processed),
        ProcessResult::BlockedDeadLetter { processed } => {
            enter_blocked(segment, metrics, block_episode, processed)
        }
        // Segment was quarantined inside process_segment — move on.
        ProcessResult::Quarantined => {
            end_block_episode(block_episode, metrics);
            DrainState::Draining
        }
    }
}

fn transition_from_draining(
    segment: PathBuf,
    result: ProcessResult,
    config: &DrainConfig,
    metrics: &Metrics,
    block_episode: &mut Option<Instant>,
) -> DrainState {
    // First failure for this segment: a fresh retry budget and the base delay.
    next_state_after_process(
        segment,
        result,
        metrics,
        block_episode,
        |segment, retry_after, processed| DrainState::RetryingTransient {
            segment,
            retries_left: config.max_retries,
            next_delay: next_retry_delay(config.base_retry_delay, retry_after),
            processed,
        },
    )
}

/// Ends the current dead-letter-full blocking episode (if one is open) because
/// a segment was just confirmed or quarantined — genuine forward progress. The
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
    }
}

// ── Segment processing ────────────────────────────────────────────────────────

async fn process_segment<S: Sink>(
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
    skip: u64,
) -> ProcessResult {
    let reader = match SegmentReader::open(segment) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Already gone (deleted out-of-band, or a double-send). Nothing to
            // deliver and nothing to preserve — a genuine no-op.
            warn!(path = %segment.display(), "drain: segment not found; treating as already-consumed");
            return ProcessResult::Confirmed { record_count: 0 };
        }
        Err(e) => {
            // Any other open failure — transient I/O (fd exhaustion, ENOMEM, a
            // momentary read error) or post-seal corruption (bad magic/version).
            // Confirming here would DELETE a segment full of undelivered records on
            // a transient blip. Preserve it: a transient error clears on retry, and
            // a genuinely corrupt segment ends up left on disk for manual recovery
            // (and is quarantined by crash recovery on the next restart).
            error!(path = %segment.display(), error = %e, "drain: cannot open segment; preserving for retry");
            return ProcessResult::Transient {
                retry_after: None,
                processed: skip,
            };
        }
    };

    let max_batch = sink.max_batch_size().max(1);
    // `read_index` counts every record the reader yields (including the `skip`
    // prefix already handled on a prior attempt). `durable_through` is how many
    // records have been durably committed/dead-lettered — it starts at `skip`
    // and advances only when a sub-batch returns Ok, so a Transient/Blocked
    // result reports exactly the prefix the next retry can skip (F05).
    let mut read_index: u64 = 0;
    let mut durable_through: u64 = skip;
    let mut batch: Vec<Payload> = Vec::with_capacity(max_batch);

    let mut read_failed = false;
    for result in reader {
        let payload = match result {
            Ok(p) => p,
            Err(e) => {
                // A record failed to read mid-stream (post-seal corruption, or a
                // transient read error). The corrupt record and everything after it
                // are unreachable via the sequential reader, so we deliver the
                // readable prefix and then quarantine the segment below — never
                // confirm+delete it, which would silently drop the unread tail.
                error!(path = %segment.display(), error = %e, "drain: segment read error; will quarantine after delivering the readable prefix");
                read_failed = true;
                break;
            }
        };
        read_index += 1;
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
        batch.push(payload);

        if batch.len() >= max_batch {
            let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(max_batch));
            let n = full_batch.len() as u64;
            match commit_batch(&full_batch, sink, config, metrics, dead_letter).await {
                BatchResult::Ok => durable_through += n,
                BatchResult::Transient { retry_after } => {
                    return ProcessResult::Transient {
                        retry_after,
                        processed: durable_through,
                    };
                }
                BatchResult::Blocked => {
                    return ProcessResult::BlockedDeadLetter {
                        processed: durable_through,
                    };
                }
            }
        }
    }

    if !batch.is_empty() {
        let n = batch.len() as u64;
        match commit_batch(&batch, sink, config, metrics, dead_letter).await {
            BatchResult::Ok => durable_through += n,
            BatchResult::Transient { retry_after } => {
                return ProcessResult::Transient {
                    retry_after,
                    processed: durable_through,
                };
            }
            BatchResult::Blocked => {
                return ProcessResult::BlockedDeadLetter {
                    processed: durable_through,
                };
            }
        }
    }
    if read_failed {
        // Readable prefix delivered; move the segment (with its unreadable tail) to
        // the quarantine dir for manual recovery rather than deleting it.
        quarantine_segment(segment, config, metrics, "drain: mid-segment read error");
        return ProcessResult::Quarantined;
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
    match read_segment_record_count(segment) {
        Ok(footer_count) if footer_count == read_index => {}
        Ok(footer_count) => {
            error!(
                path = %segment.display(),
                read = read_index,
                footer = footer_count,
                "drain: segment record count disagrees with its footer (truncated tail?); quarantining instead of confirming"
            );
            quarantine_segment(
                segment,
                config,
                metrics,
                "drain: record count mismatch vs footer (truncated tail)",
            );
            return ProcessResult::Quarantined;
        }
        Err(e) => {
            error!(
                path = %segment.display(),
                error = %e,
                "drain: cannot read segment footer to verify completeness; quarantining instead of confirming"
            );
            quarantine_segment(
                segment,
                config,
                metrics,
                "drain: footer unreadable, completeness unverifiable",
            );
            return ProcessResult::Quarantined;
        }
    }

    // Clean path only: every readable record is now durably processed
    // (durable_through == the full count). Asserted AFTER the read_failed
    // early-return — on a resumed segment (skip > 0) that then hits a mid-prefix
    // read error, durable_through == skip < read_index, which is expected and
    // handled above by quarantine; asserting before that check would trip on a
    // legitimate state (G10).
    debug_assert_eq!(durable_through, read_index);

    ProcessResult::Confirmed {
        record_count: read_index,
    }
}

/// Moves a segment that cannot be safely confirmed into the quarantine dir for
/// manual recovery, rather than deleting it. If the move itself fails the segment
/// is left on disk (crash recovery re-encounters it) — never silently dropped.
fn quarantine_segment(segment: &Path, config: &DrainConfig, metrics: &Metrics, reason: &str) {
    match crate::wab::recovery::quarantine(segment, &config.wab_dir, reason) {
        Ok(()) => {
            metrics.recovery_segments_quarantined.inc();
        }
        Err(qe) => {
            error!(path = %segment.display(), error = %qe, reason, "drain: failed to quarantine segment; left on disk");
        }
    }
}

async fn commit_batch<S: Sink>(
    payloads: &[Payload],
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
) -> BatchResult {
    // Convert payloads to the sink's record type. Cloning here keeps the original
    // payloads available for dead-lettering on a Permanent error.
    let records: Vec<S::Record> = payloads
        .iter()
        .cloned()
        .map(S::Record::from_payload)
        .collect();

    let t = std::time::Instant::now();
    // Backstop timeout: a sink that hangs (e.g. a third-party sink with no
    // internal timeout) must not stall the drain forever. On elapse, treat it
    // as a transient error so the segment is retried.
    let commit = match tokio::time::timeout(config.commit_timeout, sink.commit(records)).await {
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
            return BatchResult::Transient { retry_after: None };
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
                return BatchResult::Transient { retry_after: None };
            }
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            // The committed-records count is bumped at the END, only on the path
            // that actually returns BatchResult::Ok — a Blocked or failed-write
            // Transient return below leaves the batch to be re-committed on retry,
            // and counting here would double-count it (S24).
            let committed_count = commit_result.committed.len() as u64;

            if !commit_result.dead_lettered.is_empty() {
                let dead_payloads: Vec<Payload> = commit_result
                    .dead_lettered
                    .into_iter()
                    .map(|(r, _reason)| r.into_payload())
                    .collect();
                // A failed dead-letter write (or a cap block) must NOT fall through
                // to Ok: confirming would delete the segment with these records
                // neither delivered nor dead-lettered — silent data loss (B1/F02).
                if let Err(failure) =
                    dead_letter_records(&dead_payloads, dead_letter, config, metrics)
                {
                    return failure;
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
            BatchResult::Transient { retry_after }
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
            if let Err(failure) = dead_letter_records(payloads, dead_letter, config, metrics) {
                return failure;
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
/// On failure it returns the `BatchResult` the caller must propagate WITHOUT
/// confirming the segment:
/// - `Err(BatchResult::Blocked)` when the write would exceed the cap and the
///   batch is not itself larger than the whole cap (a one-off oversized batch is
///   written anyway — overshoot once — to avoid a permanent block↔retry livelock, F03).
/// - `Err(BatchResult::Transient)` when the dead-letter write itself fails (e.g.
///   ENOSPC): the segment is preserved and retried rather than confirmed+deleted
///   with the records neither delivered nor dead-lettered — silent data loss (B1/F02).
fn dead_letter_records(
    records: &[Payload],
    dead_letter: &mut DeadLetterWriter,
    config: &DrainConfig,
    metrics: &Metrics,
) -> Result<(), BatchResult> {
    let estimated = estimated_write_bytes(records);
    if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
        if estimated > config.dead_letter_max_bytes {
            warn!(
                estimated,
                cap = config.dead_letter_max_bytes,
                "drain: dead-letter batch alone exceeds dead_letter_max_bytes; writing it anyway to avoid a permanent block"
            );
        } else {
            return Err(BatchResult::Blocked);
        }
    }

    match dead_letter.write_records(records) {
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
            error!(error = %e, "drain: failed to write dead-letter records; preserving segment for retry");
            Err(BatchResult::Transient { retry_after: None })
        }
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

    type MockResult = Result<CommitResult<Payload>, MockError>;

    struct MockSink {
        responses: Mutex<VecDeque<MockResult>>,
        call_count: AtomicU64,
        /// Number of initial `commit()` calls that hang forever (await
        /// `pending()`), modelling a sink with no internal timeout. The drain's
        /// backstop `commit_timeout` must cancel these.
        hang_calls: u64,
        /// Number of initial `commit()` calls that panic, to drive the drain's
        /// panic supervisor.
        panic_calls: u64,
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
    }

    impl MockSink {
        fn with_responses(responses: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                call_count: AtomicU64::new(0),
                hang_calls: 0,
                panic_calls: 0,
                max_batch: 1000,
                call_timestamps: Mutex::new(Vec::new()),
                committed_records: Mutex::new(Vec::new()),
                dead_lettered_records: Mutex::new(Vec::new()),
                health_script: Mutex::new(VecDeque::new()),
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
                max_batch,
                call_timestamps: Mutex::new(Vec::new()),
                committed_records: Mutex::new(Vec::new()),
                dead_lettered_records: Mutex::new(Vec::new()),
                health_script: Mutex::new(VecDeque::new()),
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
        type Record = Payload;
        type Error = MockError;

        async fn commit(&self, batch: Vec<Payload>) -> MockResult {
            let nth = self.call_count.fetch_add(1, Ordering::Relaxed) + 1;
            self.call_timestamps.lock().unwrap().push(Instant::now());
            if nth <= self.panic_calls {
                panic!("mock sink: induced panic on commit #{nth}");
            }
            if nth <= self.hang_calls {
                // Hang forever; the drain's backstop timeout cancels this future.
                std::future::pending::<()>().await;
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

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weir_drain_{label}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_sealed_segment(dir: &Path, shard_id: u16, payloads: &[&[u8]]) -> PathBuf {
        // Put sealed segments in a shard subdirectory so confirmed_path works.
        let shard_dir = dir.join("shard_00");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let path = segment_path(&shard_dir, 1);
        let mut seg = WabSegment::create(&path, shard_id).unwrap();
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
            commit_timeout: Duration::from_secs(30),
            // Short so the stranded-segment recovery rescan fires promptly in tests.
            health_poll_interval: Duration::from_millis(50),
            health_probe_timeout: Duration::from_secs(5),
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
        let handle = spawn(drain_rx, sink, config, metrics);
        drop(drain_tx);
        handle.join().unwrap();
    }

    fn get_confirmed_path(sealed: &Path) -> PathBuf {
        super::confirmed::confirmed_path(sealed)
    }

    // ── CommitResult ──────────────────────────────────────────────────────────

    #[test]
    fn commit_result_separates_committed_and_dead_lettered() {
        let p: Payload = Payload::from(b"hello".as_ref());
        let result: CommitResult<Payload> =
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
        let _ok = crate::wab::recovery::check_confirmed(&sealed, &dir).unwrap();
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
    // removing its directory so write_records fails deterministically.

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
        std::fs::remove_dir_all(dir.join("dead_letter")).unwrap();

        let sink = MockSink::with_responses([Err(MockError::Permanent)]);
        let metrics = noop_metrics();
        let config = fast_config(dir.clone());
        let payloads = vec![Payload::from(b"r1".as_ref()), Payload::from(b"r2".as_ref())];

        let result = block_on(commit_batch(&payloads, &sink, &config, &metrics, &mut dl));
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
        std::fs::remove_dir_all(dir.join("dead_letter")).unwrap();

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

        let result = block_on(commit_batch(&payloads, &sink, &config, &metrics, &mut dl));
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

        let result = block_on(commit_batch(&payloads, &sink, &config, &metrics, &mut dl));
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
        std::fs::create_dir_all(&shard_dir).unwrap();
        // A file with bad magic → SegmentReader::open errors (InvalidData),
        // standing in for any non-NotFound open failure (transient or corrupt).
        let seg = segment_path(&shard_dir, 1);
        let sealed = seg.with_extension("wab.sealed");
        std::fs::write(&sealed, b"NOTWEIR-not-a-valid-segment-header-xxxx").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        assert!(
            sealed.exists(),
            "an unopenable segment must be left on disk for recovery, not deleted"
        );
        assert!(
            !get_confirmed_path(&sealed).exists(),
            "an unopenable segment must not be confirmed"
        );
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
        std::fs::create_dir_all(&shard_dir).unwrap();
        let mk = |counter: u64, payload: &[u8]| {
            let p = segment_path(&shard_dir, counter);
            let mut s = WabSegment::create(&p, 0).unwrap();
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
        std::fs::create_dir_all(&confirmed).unwrap();

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
        run_drain(rx, tx, sink, fast_config(dir.clone()), Arc::clone(&metrics));

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

    // ── probe_and_resume_stranded: recovery-edge contract (H1) ─────────────────

    /// Reads the one-hot `weir_sink_health` gauge value for a single state.
    fn sink_health_gauge(m: &Metrics, state: crate::metrics::SinkHealthState) -> f64 {
        m.sink_health
            .get_or_create(&crate::metrics::SinkHealthLabel { state })
            .get()
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
        let after1 = probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, true);
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
        let after2 = probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after1);
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
        let after3 = probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after2);
        assert!(after3, "steady Healthy probe stays now_ok = true");
        assert_eq!(
            metrics.drain_segments_resumed.get(),
            1,
            "steady-Healthy polls must NOT re-fire the recovery edge"
        );
        assert_eq!(pending.len(), 1, "no duplicate enqueue on steady Healthy");

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
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, false);
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
            probe_and_resume_stranded(&rt, &sink, &config, &metrics, &mut pending, after_err);
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

    #[test]
    fn multiple_segments_second_processed_after_first_exhausts_retries() {
        let dir = tmp_dir("two_segs");
        let shard_dir = dir.join("shard_00");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let seg1_active = segment_path(&shard_dir, 1);
        let mut s1 = WabSegment::create(&seg1_active, 0).unwrap();
        s1.write_record(b"seg1").unwrap();
        let seg1 = s1.seal().unwrap();

        let seg2_active = segment_path(&shard_dir, 2);
        let mut s2 = WabSegment::create(&seg2_active, 0).unwrap();
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
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        // seg2 is delivered without being blocked by seg1 exhausting its retries
        // (a stranded segment doesn't stall the queue). seg1 stays stranded here
        // because run_drain drops the channel, so the drain never reaches the idle
        // poll where the sink-recovery rescan runs (that path is covered by
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
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(rx, sink, config, metrics.clone());

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

        let result = block_on(commit_batch(&payloads, &sink, &config, &metrics, &mut dl));
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
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();

        let handle = spawn(rx, sink.clone(), config, metrics.clone());

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
        std::fs::create_dir_all(&dl_dir).unwrap();
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

        let handle = spawn(rx, sink.clone(), config, metrics.clone());

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
        assert_eq!(
            sink.call_count(),
            2,
            "commit called exactly twice: first attempt + retry"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dead_letter_full_total_increments_once_per_entry_not_per_wake() {
        let dir = tmp_dir("dl_total");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);

        let dl_dir = dir.join("dead_letter");
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();
        let handle = spawn(rx, sink, config, metrics.clone());

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
        std::fs::create_dir_all(&dl_dir).unwrap();
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
        let handle = spawn(rx, sink, config, metrics.clone());

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
        let handle = spawn(rx, sink.clone(), fast_config(dir.clone()), metrics.clone());
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
        let sink = Arc::new(MockSink::with_responses(responses));
        let metrics = noop_metrics();
        let handle = spawn(rx, sink.clone(), fast_config(dir.clone()), metrics.clone());

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
        let handle = spawn(rx, sink, fast_config(dir.clone()), noop_metrics());
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
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = noop_metrics();
        let handle = spawn(rx, sink, config, metrics.clone());

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
}
