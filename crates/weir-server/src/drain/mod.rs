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
        match result {
            // Clean exit: the channel closed (all flushers gone). Done.
            Ok(()) => break,
            Err(_) => {
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
        }
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
    let (healthy, degraded, down) = match &health {
        SinkHealth::Healthy => (1.0, 0.0, 0.0),
        SinkHealth::Degraded(reason) => {
            warn!(reason = %reason, "sink health: degraded");
            (0.0, 1.0, 0.0)
        }
        SinkHealth::Down(reason) => {
            error!(reason = %reason, "sink health: down");
            (0.0, 0.0, 1.0)
        }
        // SinkHealth is #[non_exhaustive] (F48): report an unrecognised future
        // state as degraded so the gauge still moves off "healthy" and operators
        // get a signal rather than a silently-stuck reading.
        _ => {
            warn!("sink health: unrecognised state; reporting as degraded");
            (0.0, 1.0, 0.0)
        }
    };
    metrics
        .sink_health
        .get_or_create(&SinkHealthLabel {
            state: SinkHealthState::healthy,
        })
        .set(healthy);
    metrics
        .sink_health
        .get_or_create(&SinkHealthLabel {
            state: SinkHealthState::degraded,
        })
        .set(degraded);
    metrics
        .sink_health
        .get_or_create(&SinkHealthLabel {
            state: SinkHealthState::down,
        })
        .set(down);
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

    'outer: loop {
        state = match state {
            // ── Draining ─────────────────────────────────────────────────────
            DrainState::Draining => {
                set_drain_state(&metrics, DrainStateValue::draining);

                let segment = if let Some(p) = pending.pop_front() {
                    p
                } else {
                    // Wait for a new segment, but wake every
                    // HEALTH_POLL_INTERVAL to refresh sink health so the
                    // gauge stays current even on an idle daemon. Channel
                    // closure (all WAB flushers exited) still ends the loop.
                    loop {
                        match drain_rx.recv_timeout(HEALTH_POLL_INTERVAL) {
                            Ok(p) => break p,
                            Err(RecvTimeoutError::Timeout) => {
                                let health = probe_health(&rt, &*sink, config.commit_timeout);
                                set_sink_health(&metrics, health);
                            }
                            Err(RecvTimeoutError::Disconnected) => break 'outer,
                        }
                    }
                };

                // Fresh segment → process from the start (skip = 0).
                let result = rt.block_on(process_segment(
                    &segment,
                    &*sink,
                    &config,
                    &metrics,
                    &mut dead_letter,
                    0,
                ));

                let health = probe_health(&rt, &*sink, config.commit_timeout);
                set_sink_health(&metrics, health);

                transition_from_draining(segment, result, &config, &metrics, &mut block_episode)
            }

            // ── RetryingTransient ─────────────────────────────────────────────
            DrainState::RetryingTransient {
                segment,
                retries_left,
                next_delay,
                processed,
            } => {
                set_drain_state(&metrics, DrainStateValue::retrying_transient);
                std::thread::sleep(next_delay);

                if retries_left == 0 {
                    error!(
                        path = %segment.display(),
                        "drain: max retries exhausted; segment left on disk for manual recovery"
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
                let health = probe_health(&rt, &*sink, config.commit_timeout);
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

/// How often the drain polls `Sink::health()` while idle (waiting on the
/// channel) or while blocked on a full dead-letter directory. Keeps the
/// `weir_sink_health{state}` gauge fresh even when no segments are flowing.
/// Not currently configurable; 30 s matches the default
/// `dead_letter_check_interval_secs` for symmetry.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(30);

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
    let blocked_since = match *block_episode {
        Some(started) => started,
        None => {
            let now = Instant::now();
            metrics.dead_letter_full.inc();
            *block_episode = Some(now);
            now
        }
    };
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

                let estimated = estimated_write_bytes(&dead_payloads);
                if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
                    if estimated > config.dead_letter_max_bytes {
                        // This batch alone exceeds the cap, so blocking could never
                        // clear (even an empty dir won't fit it) — that would wedge
                        // the drain forever in a block↔retry livelock (F03). The cap
                        // bounds steady-state growth, not a single oversized
                        // permanent rejection; write it anyway (overshoot once).
                        warn!(
                            estimated,
                            cap = config.dead_letter_max_bytes,
                            "drain: dead-letter batch alone exceeds dead_letter_max_bytes; writing it anyway to avoid a permanent block"
                        );
                    } else {
                        return BatchResult::Blocked;
                    }
                }

                match dead_letter.write_records(&dead_payloads) {
                    Ok(()) => {
                        metrics
                            .sink_commit_records
                            .get_or_create(&OutcomeLabel {
                                outcome: Outcome::dead_lettered,
                            })
                            .inc_by(dead_payloads.len() as u64);
                        metrics
                            .dead_letter_bytes_on_disk
                            .set(dead_letter.total_bytes() as f64);
                    }
                    Err(e) => {
                        // The records were rejected by the sink AND could not be
                        // dead-lettered (e.g. ENOSPC on the dead-letter dir — exactly
                        // when pressure peaks). Falling through to Ok would confirm
                        // and DELETE the segment with these records neither delivered
                        // nor dead-lettered: silent data loss. Treat it as transient
                        // so the segment is preserved and retried (the already-
                        // committed records re-send under the at-least-once contract).
                        error!(error = %e, "drain: failed to write dead-letter records; preserving segment for retry");
                        return BatchResult::Transient { retry_after: None };
                    }
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

            let estimated = estimated_write_bytes(payloads);
            if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
                if estimated > config.dead_letter_max_bytes {
                    // Batch alone exceeds the cap — blocking could never clear, so
                    // write it anyway rather than livelock the drain (F03). The cap
                    // bounds steady-state growth, not one oversized permanent batch.
                    warn!(
                        estimated,
                        cap = config.dead_letter_max_bytes,
                        "drain: dead-letter batch alone exceeds dead_letter_max_bytes; writing it anyway to avoid a permanent block"
                    );
                } else {
                    return BatchResult::Blocked;
                }
            }

            match dead_letter.write_records(payloads) {
                Ok(()) => {
                    metrics
                        .sink_commit_records
                        .get_or_create(&OutcomeLabel {
                            outcome: Outcome::dead_lettered,
                        })
                        .inc_by(payloads.len() as u64);
                    metrics
                        .dead_letter_bytes_on_disk
                        .set(dead_letter.total_bytes() as f64);
                }
                Err(dl_err) => {
                    // The batch hit a permanent sink error AND could not be
                    // dead-lettered. Confirming here would DELETE the segment with
                    // the records neither delivered nor dead-lettered — silent data
                    // loss. Preserve the segment by retrying; the dead-letter write
                    // typically succeeds once disk pressure clears, or the drain
                    // blocks on the dead-letter cap (would_exceed_cap above).
                    error!(error = %dl_err, "drain: failed to dead-letter records for permanent error; preserving segment for retry");
                    return BatchResult::Transient { retry_after: None };
                }
            }

            // Dead-letter write succeeded: the records are durably re-homed, so the
            // segment can be confirmed. Leaving it unconsumed would replay the same
            // permanent-error records on every restart.
            BatchResult::Ok
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
            SinkHealth::Healthy
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
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        assert!(
            sealed.exists(),
            "segment must remain on disk after max retries"
        );
        assert!(!get_confirmed_path(&sealed).exists(), "no confirmed file");

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
        let sink = Arc::new(MockSink::with_responses(responses));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

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
        run_drain(
            rx,
            tx,
            sink.clone(),
            fast_config(dir.clone()),
            metrics.clone(),
        );

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
