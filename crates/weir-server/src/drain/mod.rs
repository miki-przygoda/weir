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
//! BlockedDeadLetterFull  ──(cap clears)──▶  Draining (same segment, retry from start)
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
    wab::SegmentReader,
};

use confirmed::confirm_and_delete;
use dead_letter::DeadLetterWriter;

// ── Constants ─────────────────────────────────────────────────────────────────

pub const MAX_RETRIES: u32 = 3;

// ── Configuration ─────────────────────────────────────────────────────────────

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
    },
    BlockedDeadLetterFull {
        segment: PathBuf,
        blocked_since: Instant,
    },
}

enum ProcessResult {
    /// Segment fully processed. Confirm and delete it.
    Confirmed { record_count: u64 },
    /// Sink returned a transient error. Retry the segment after `retry_after`
    /// (if the sink supplied a hint, e.g. an HTTP Retry-After header) or
    /// after the drain's exponential-backoff delay (if `None`).
    Transient { retry_after: Option<Duration> },
    /// Dead-letter cap would be exceeded. Block until capacity frees.
    BlockedDeadLetter,
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
        .spawn(move || drain_thread(drain_rx, sink, config, metrics))
        .expect("failed to spawn drain thread")
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
                                let health = rt.block_on(sink.health());
                                set_sink_health(&metrics, health);
                            }
                            Err(RecvTimeoutError::Disconnected) => break 'outer,
                        }
                    }
                };

                let result = rt.block_on(process_segment(
                    &segment,
                    &*sink,
                    &config,
                    &metrics,
                    &mut dead_letter,
                ));

                let health = rt.block_on(sink.health());
                set_sink_health(&metrics, health);

                transition_from_draining(segment, result, &config, &metrics)
            }

            // ── RetryingTransient ─────────────────────────────────────────────
            DrainState::RetryingTransient {
                segment,
                retries_left,
                next_delay,
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
                    let result = rt.block_on(process_segment(
                        &segment,
                        &*sink,
                        &config,
                        &metrics,
                        &mut dead_letter,
                    ));
                    match result {
                        ProcessResult::Confirmed { record_count } => {
                            confirm_and_delete(&segment, record_count, &metrics);
                            DrainState::Draining
                        }
                        ProcessResult::Transient { retry_after } => DrainState::RetryingTransient {
                            segment,
                            retries_left: retries_left - 1,
                            next_delay: next_retry_delay(next_delay * 2, retry_after),
                        },
                        ProcessResult::BlockedDeadLetter => enter_blocked(segment, &metrics),
                    }
                }
            }

            // ── BlockedDeadLetterFull ─────────────────────────────────────────
            DrainState::BlockedDeadLetterFull {
                segment,
                blocked_since,
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
                let _ = dead_letter.rescan();
                metrics
                    .dead_letter_bytes_on_disk
                    .set(dead_letter.total_bytes() as f64);

                // Refresh sink health every wake-cycle. Operators monitoring
                // weir_sink_health need to know if the sink came back up
                // while we were waiting on dead-letter headroom; without
                // this poll the gauge would be stuck at whatever value the
                // last segment commit produced.
                let health = rt.block_on(sink.health());
                set_sink_health(&metrics, health);
                if dead_letter.total_bytes() < config.dead_letter_max_bytes {
                    // Headroom available — retry the preserved segment from the beginning.
                    metrics.dead_letter_blocked_duration.set(0.0);
                    pending.push_front(segment);
                    DrainState::Draining
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
                    }
                }
            }
        };
    }

    info!("drain thread exiting");
}

fn transition_from_draining(
    segment: PathBuf,
    result: ProcessResult,
    config: &DrainConfig,
    metrics: &Metrics,
) -> DrainState {
    match result {
        ProcessResult::Confirmed { record_count } => {
            confirm_and_delete(&segment, record_count, metrics);
            DrainState::Draining
        }
        ProcessResult::Transient { retry_after } => DrainState::RetryingTransient {
            segment,
            retries_left: config.max_retries,
            next_delay: next_retry_delay(config.base_retry_delay, retry_after),
        },
        ProcessResult::BlockedDeadLetter => enter_blocked(segment, metrics),
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

fn enter_blocked(segment: PathBuf, metrics: &Metrics) -> DrainState {
    let blocked_since = Instant::now();
    metrics.dead_letter_full.inc();
    set_drain_state(metrics, DrainStateValue::blocked_dead_letter_full);
    DrainState::BlockedDeadLetterFull {
        segment,
        blocked_since,
    }
}

// ── Segment processing ────────────────────────────────────────────────────────

async fn process_segment<S: Sink>(
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &Metrics,
    dead_letter: &mut DeadLetterWriter,
) -> ProcessResult {
    let reader = match SegmentReader::open(segment) {
        Ok(r) => r,
        Err(e) => {
            error!(path = %segment.display(), error = %e, "drain: cannot open segment; skipping");
            return ProcessResult::Confirmed { record_count: 0 };
        }
    };

    let max_batch = sink.max_batch_size().max(1);
    let mut total_records: u64 = 0;
    let mut batch: Vec<Payload> = Vec::with_capacity(max_batch);

    for result in reader {
        let payload = match result {
            Ok(p) => p,
            Err(e) => {
                error!(path = %segment.display(), error = %e, "drain: segment read error; stopping here");
                break;
            }
        };
        total_records += 1;
        batch.push(payload);

        if batch.len() >= max_batch {
            let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(max_batch));
            match commit_batch(&full_batch, sink, config, metrics, dead_letter).await {
                BatchResult::Ok => {}
                BatchResult::Transient { retry_after } => {
                    return ProcessResult::Transient { retry_after };
                }
                BatchResult::Blocked => return ProcessResult::BlockedDeadLetter,
            }
        }
    }

    if !batch.is_empty() {
        match commit_batch(&batch, sink, config, metrics, dead_letter).await {
            BatchResult::Ok => {}
            BatchResult::Transient { retry_after } => {
                return ProcessResult::Transient { retry_after };
            }
            BatchResult::Blocked => return ProcessResult::BlockedDeadLetter,
        }
    }

    ProcessResult::Confirmed {
        record_count: total_records,
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
            metrics
                .sink_commit_duration
                .observe(t.elapsed().as_secs_f64());
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel {
                    outcome: Outcome::committed,
                })
                .inc_by(commit_result.committed.len() as u64);

            if !commit_result.dead_lettered.is_empty() {
                let dead_payloads: Vec<Payload> = commit_result
                    .dead_lettered
                    .into_iter()
                    .map(|(r, _reason)| r.into_payload())
                    .collect();

                let estimated = estimated_write_bytes(&dead_payloads);
                if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
                    return BatchResult::Blocked;
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
                        error!(error = %e, "drain: failed to write dead-letter records");
                    }
                }
            }

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
                return BatchResult::Blocked;
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
                    error!(error = %dl_err, "drain: failed to write dead-letter records for permanent error");
                }
            }

            // Segment can be confirmed: we've either dead-lettered the records or
            // logged the failure. Leaving the segment unconsumed would replay the
            // same permanent-error records on every restart.
            BatchResult::Ok
        }
    }
}

/// Rough byte estimate for WAB record overhead + payload.
fn estimated_write_bytes(payloads: &[Payload]) -> u64 {
    payloads.iter().map(|p| p.len() as u64 + 8).sum()
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
            Ok(CommitResult {
                committed: batch,
                dead_lettered: vec![],
            })
        }

        fn ok_with_dead_letter(committed: Vec<Payload>, dead: Vec<Payload>) -> MockResult {
            Ok(CommitResult {
                committed,
                dead_lettered: dead.into_iter().map(|p| (p, "rejected".into())).collect(),
            })
        }
    }

    impl crate::sink::Sink for MockSink {
        type Record = Payload;
        type Error = MockError;

        async fn commit(&self, batch: Vec<Payload>) -> MockResult {
            let nth = self.call_count.fetch_add(1, Ordering::Relaxed) + 1;
            self.call_timestamps.lock().unwrap().push(Instant::now());
            if nth <= self.hang_calls {
                // Hang forever; the drain's backstop timeout cancels this future.
                std::future::pending::<()>().await;
            }
            let mut responses = self.responses.lock().unwrap();
            let response = responses.pop_front().unwrap_or_else(|| {
                Ok(CommitResult {
                    committed: batch.clone(),
                    dead_lettered: vec![],
                })
            });
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
        let result: CommitResult<Payload> = CommitResult {
            committed: vec![p.clone()],
            dead_lettered: vec![(p.clone(), "reason".into())],
        };
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
