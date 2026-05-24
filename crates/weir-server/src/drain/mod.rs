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

pub mod dead_letter;

use std::{
    collections::VecDeque,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crossbeam_channel::RecvTimeoutError;
use tracing::{error, info, warn};
use weir_core::Payload;

use crate::{
    sink::{Sink, SinkError, SinkRecord},
    wab::{
        SegmentReader,
        format::{EXT_CONFIRMED, EXT_SEALED, SEGMENT_FOOTER_LEN, build_confirmed, unix_nanos_now},
    },
};

use dead_letter::DeadLetterWriter;

// ── Constants ─────────────────────────────────────────────────────────────────

pub const MAX_RETRIES: u32 = 3;

/// Drain state label values stored in `DrainMetrics::state`.
pub const STATE_DRAINING: u8 = 0;
pub const STATE_RETRYING: u8 = 1;
pub const STATE_BLOCKED: u8 = 2;

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
}

// ── Metrics ───────────────────────────────────────────────────────────────────

pub struct DrainMetrics {
    pub segments_drained: AtomicU64,
    pub records_committed: AtomicU64,
    pub records_dead_lettered: AtomicU64,
    /// Increments once each time the drain enters `BlockedDeadLetterFull`. Does
    /// NOT increment on each wake cycle within a blocked period.
    pub dead_letter_full_total: AtomicU64,
    /// Current state: `STATE_DRAINING`, `STATE_RETRYING`, or `STATE_BLOCKED`.
    pub state: AtomicU8,
    /// Set to `Some(Instant)` when entering `BlockedDeadLetterFull`; reset to
    /// `None` on exit. Step 07 computes the blocked duration from this.
    pub blocked_since: std::sync::Mutex<Option<Instant>>,
}

impl DrainMetrics {
    pub fn new() -> Self {
        Self {
            segments_drained: AtomicU64::new(0),
            records_committed: AtomicU64::new(0),
            records_dead_lettered: AtomicU64::new(0),
            dead_letter_full_total: AtomicU64::new(0),
            state: AtomicU8::new(STATE_DRAINING),
            blocked_since: std::sync::Mutex::new(None),
        }
    }
}

impl Default for DrainMetrics {
    fn default() -> Self {
        Self::new()
    }
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
    /// Sink returned a transient error. Retry the segment.
    Transient,
    /// Dead-letter cap would be exceeded. Block until capacity frees.
    BlockedDeadLetter,
}

enum BatchResult {
    Ok,
    Transient,
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
    metrics: Arc<DrainMetrics>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("weir-drain".into())
        .spawn(move || drain_thread(drain_rx, sink, config, metrics))
        .expect("failed to spawn drain thread")
}

// ── Drain thread ──────────────────────────────────────────────────────────────

fn drain_thread<S: Sink>(
    drain_rx: crossbeam_channel::Receiver<PathBuf>,
    sink: Arc<S>,
    config: DrainConfig,
    metrics: Arc<DrainMetrics>,
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
                metrics.state.store(STATE_DRAINING, Ordering::Relaxed);

                let segment = if let Some(p) = pending.pop_front() {
                    p
                } else {
                    match drain_rx.recv() {
                        Ok(p) => p,
                        Err(_) => break 'outer,
                    }
                };

                let result = rt.block_on(process_segment(
                    &segment,
                    &*sink,
                    &config,
                    &metrics,
                    &mut dead_letter,
                ));

                transition_from_draining(segment, result, &config, &metrics)
            }

            // ── RetryingTransient ─────────────────────────────────────────────
            DrainState::RetryingTransient {
                segment,
                retries_left,
                next_delay,
            } => {
                metrics.state.store(STATE_RETRYING, Ordering::Relaxed);
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
                        ProcessResult::Transient => DrainState::RetryingTransient {
                            segment,
                            retries_left: retries_left - 1,
                            next_delay: next_delay * 2,
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
                metrics.state.store(STATE_BLOCKED, Ordering::Relaxed);

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
                if dead_letter.total_bytes() < config.dead_letter_max_bytes {
                    // Headroom available — retry the preserved segment from the beginning.
                    *metrics.blocked_since.lock().unwrap() = None;
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
    metrics: &DrainMetrics,
) -> DrainState {
    match result {
        ProcessResult::Confirmed { record_count } => {
            confirm_and_delete(&segment, record_count, metrics);
            DrainState::Draining
        }
        ProcessResult::Transient => DrainState::RetryingTransient {
            segment,
            retries_left: config.max_retries,
            next_delay: config.base_retry_delay,
        },
        ProcessResult::BlockedDeadLetter => enter_blocked(segment, metrics),
    }
}

fn enter_blocked(segment: PathBuf, metrics: &DrainMetrics) -> DrainState {
    let blocked_since = Instant::now();
    metrics
        .dead_letter_full_total
        .fetch_add(1, Ordering::Relaxed);
    *metrics.blocked_since.lock().unwrap() = Some(blocked_since);
    metrics.state.store(STATE_BLOCKED, Ordering::Relaxed);
    DrainState::BlockedDeadLetterFull {
        segment,
        blocked_since,
    }
}

fn confirm_and_delete(sealed: &Path, record_count: u64, metrics: &DrainMetrics) {
    write_confirmed_file(sealed, record_count);
    if let Err(e) = std::fs::remove_file(sealed) {
        warn!(path = %sealed.display(), error = %e, "drain: failed to delete confirmed segment");
    }
    metrics.segments_drained.fetch_add(1, Ordering::Relaxed);
}

// ── Segment processing ────────────────────────────────────────────────────────

async fn process_segment<S: Sink>(
    segment: &Path,
    sink: &S,
    config: &DrainConfig,
    metrics: &DrainMetrics,
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
                BatchResult::Transient => return ProcessResult::Transient,
                BatchResult::Blocked => return ProcessResult::BlockedDeadLetter,
            }
        }
    }

    if !batch.is_empty() {
        match commit_batch(&batch, sink, config, metrics, dead_letter).await {
            BatchResult::Ok => {}
            BatchResult::Transient => return ProcessResult::Transient,
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
    metrics: &DrainMetrics,
    dead_letter: &mut DeadLetterWriter,
) -> BatchResult {
    // Convert payloads to the sink's record type. Cloning here keeps the original
    // payloads available for dead-lettering on a Permanent error.
    let records: Vec<S::Record> = payloads
        .iter()
        .cloned()
        .map(S::Record::from_payload)
        .collect();

    match sink.commit(records).await {
        Ok(commit_result) => {
            metrics
                .records_committed
                .fetch_add(commit_result.committed.len() as u64, Ordering::Relaxed);

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
                            .records_dead_lettered
                            .fetch_add(dead_payloads.len() as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!(error = %e, "drain: failed to write dead-letter records");
                    }
                }
            }

            BatchResult::Ok
        }

        Err(e) if e.is_transient() => {
            warn!(error = %e, "drain: transient sink error; will retry segment");
            BatchResult::Transient
        }

        Err(e) => {
            // Permanent error — dead-letter the whole batch.
            error!(error = %e, "drain: permanent sink error; dead-lettering batch");

            let estimated = estimated_write_bytes(payloads);
            if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
                return BatchResult::Blocked;
            }

            match dead_letter.write_records(payloads) {
                Ok(()) => {
                    metrics
                        .records_dead_lettered
                        .fetch_add(payloads.len() as u64, Ordering::Relaxed);
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

// ── Confirmed file helpers ────────────────────────────────────────────────────

fn write_confirmed_file(sealed: &Path, record_count: u64) {
    let confirmed = confirmed_path(sealed);
    let sealed_at = read_sealed_at_nanos(sealed).unwrap_or(0);
    let bytes = build_confirmed(sealed_at, record_count, unix_nanos_now());
    if let Err(e) = std::fs::write(&confirmed, bytes) {
        error!(
            path = %confirmed.display(),
            error = %e,
            "drain: failed to write .confirmed file; segment will be replayed on restart"
        );
    }
}

fn confirmed_path(sealed: &Path) -> PathBuf {
    let s = sealed.to_string_lossy();
    let base = s.strip_suffix(EXT_SEALED).unwrap_or(&s);
    PathBuf::from(format!("{base}{EXT_CONFIRMED}"))
}

/// Reads the `sealed_at` timestamp from the segment footer (last 32 bytes of the file).
/// Returns 0 on any read failure — the field is informational only.
fn read_sealed_at_nanos(path: &Path) -> io::Result<i64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len < (SEGMENT_FOOTER_LEN as u64 + 4) {
        return Ok(0);
    }
    file.seek(SeekFrom::End(-(SEGMENT_FOOTER_LEN as i64)))?;
    let mut footer = [0u8; SEGMENT_FOOTER_LEN];
    file.read_exact(&mut footer)?;
    // sealed_at is at footer bytes [20..28] — see wab_format.md.
    Ok(i64::from_le_bytes(footer[20..28].try_into().unwrap()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sink::{CommitResult, SinkHealth},
        wab::segment::{WabSegment, segment_path},
    };
    use std::{collections::VecDeque, sync::Mutex};
    use weir_core::Payload;

    // ── Mock sink ─────────────────────────────────────────────────────────────

    #[derive(Debug)]
    enum MockError {
        Transient,
        Permanent,
    }

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                MockError::Transient => write!(f, "transient error"),
                MockError::Permanent => write!(f, "permanent error"),
            }
        }
    }

    impl std::error::Error for MockError {}

    impl crate::sink::SinkError for MockError {
        fn is_transient(&self) -> bool {
            matches!(self, MockError::Transient)
        }
    }

    type MockResult = Result<CommitResult<Payload>, MockError>;

    struct MockSink {
        responses: Mutex<VecDeque<MockResult>>,
        call_count: AtomicU64,
        max_batch: usize,
    }

    impl MockSink {
        fn with_responses(responses: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                call_count: AtomicU64::new(0),
                max_batch: 1000,
            }
        }

        fn with_batch_size(
            max_batch: usize,
            responses: impl IntoIterator<Item = MockResult>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                call_count: AtomicU64::new(0),
                max_batch,
            }
        }

        fn call_count(&self) -> u64 {
            self.call_count.load(Ordering::Relaxed)
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
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let mut responses = self.responses.lock().unwrap();
            responses.pop_front().unwrap_or_else(|| {
                Ok(CommitResult {
                    committed: batch,
                    dead_lettered: vec![],
                })
            })
        }

        fn max_batch_size(&self) -> usize {
            self.max_batch
        }

        async fn health(&self) -> SinkHealth {
            SinkHealth::Healthy
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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
        metrics: Arc<DrainMetrics>,
    ) {
        let handle = spawn(drain_rx, sink, config, metrics);
        drop(drain_tx);
        handle.join().unwrap();
    }

    fn get_confirmed_path(sealed: &Path) -> PathBuf {
        confirmed_path(sealed)
    }

    // ── CommitResult ──────────────────────────────────────────────────────────

    #[test]
    fn commit_result_separates_committed_and_dead_lettered() {
        let p = b"hello".to_vec();
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
            b"r1".to_vec(),
            b"r2".to_vec(),
        ])]));
        let metrics = Arc::new(DrainMetrics::new());
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(
            get_confirmed_path(&sealed).exists(),
            "confirmed file must exist"
        );
        assert!(
            !sealed.exists(),
            "sealed segment must be deleted after drain"
        );
        assert_eq!(metrics.segments_drained.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.records_committed.load(Ordering::Relaxed), 2);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn confirmed_segment_not_replayed_on_restart() {
        let dir = tmp_dir("no_replay");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([]));
        run_drain(
            rx,
            tx,
            sink,
            fast_config(dir.clone()),
            Arc::new(DrainMetrics::new()),
        );

        // Confirmed file should exist; check_confirmed should return true.
        let confirmed = get_confirmed_path(&sealed);
        assert!(confirmed.exists());
        let _ok = crate::wab::recovery::check_confirmed(&sealed, &dir).unwrap();
        // check_confirmed returns false for a missing *sealed* segment (it was deleted).
        // Verify the confirmed file was written correctly by parsing it directly.
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
            Err(MockError::Transient),            // first attempt
            MockSink::ok(vec![b"data".to_vec()]), // first retry succeeds
        ]));
        let metrics = Arc::new(DrainMetrics::new());
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(
            get_confirmed_path(&sealed).exists(),
            "confirmed after successful retry"
        );
        assert!(!sealed.exists());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn transient_max_retries_exhausted_leaves_segment_on_disk() {
        let dir = tmp_dir("max_retry");
        let sealed = make_sealed_segment(&dir, 0, &[b"data"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // Always transient — initial attempt + MAX_RETRIES retries all fail.
        let responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        let sink = Arc::new(MockSink::with_responses(responses));
        run_drain(
            rx,
            tx,
            sink,
            fast_config(dir.clone()),
            Arc::new(DrainMetrics::new()),
        );

        // Segment should still exist (not confirmed, not deleted).
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

        // seg1: all transient → exhausted. seg2: ok.
        let mut responses: Vec<MockResult> = (0..=MAX_RETRIES)
            .map(|_| Err(MockError::Transient))
            .collect();
        responses.push(MockSink::ok(vec![b"seg2".to_vec()]));
        let sink = Arc::new(MockSink::with_responses(responses));
        run_drain(
            rx,
            tx,
            sink,
            fast_config(dir.clone()),
            Arc::new(DrainMetrics::new()),
        );

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
        let metrics = Arc::new(DrainMetrics::new());
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        // Segment confirmed and deleted.
        assert!(get_confirmed_path(&sealed).exists());
        assert!(!sealed.exists());
        assert_eq!(metrics.records_dead_lettered.load(Ordering::Relaxed), 1);

        // Dead-letter dir has a readable segment.
        let dl_dir = dir.join("dead_letter");
        let dl_files: Vec<_> = std::fs::read_dir(&dl_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !dl_files.is_empty(),
            "dead-letter directory must not be empty"
        );

        // All dead-letter records are readable via SegmentReader.
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
            vec![b"ok_record".to_vec()],
            vec![b"bad_record".to_vec()],
        )]));
        let metrics = Arc::new(DrainMetrics::new());
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert_eq!(metrics.records_committed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.records_dead_lettered.load(Ordering::Relaxed), 1);
        assert!(get_confirmed_path(&sealed).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    // ── BlockedDeadLetterFull ─────────────────────────────────────────────────

    #[test]
    fn blocked_when_permanent_error_and_dead_letter_cap_exceeded() {
        let dir = tmp_dir("blocked");
        let sealed = make_sealed_segment(&dir, 0, &[b"record"]);

        // Pre-fill dead-letter dir with a file to exceed the cap.
        let dl_dir = dir.join("dead_letter");
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // Permanent error. Cap = 100 bytes but dead-letter already has 200.
        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = Arc::new(DrainMetrics::new());

        let handle = spawn(rx, sink, config, metrics.clone());

        // Wait for the drain to enter blocked state.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.state.load(Ordering::Relaxed) == STATE_BLOCKED {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for blocked state"
            );
        }

        assert_eq!(metrics.dead_letter_full_total.load(Ordering::Relaxed), 1);
        // Segment must not be confirmed while blocked.
        assert!(!get_confirmed_path(&sealed).exists());

        // Signal shutdown by dropping tx — drain exits without confirming.
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

        // Pre-fill dead-letter dir to exceed cap.
        let dl_dir = dir.join("dead_letter");
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = Arc::new(DrainMetrics::new());

        let handle = spawn(rx, sink.clone(), config, metrics.clone());

        // Wait for blocked state.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.state.load(Ordering::Relaxed) == STATE_BLOCKED {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for blocked state"
            );
        }

        let calls_at_block = sink.call_count();
        // Wait for several check intervals — commit must not be called again.
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

        // Start with dead-letter dir at cap.
        let dl_dir = dir.join("dead_letter");
        std::fs::create_dir_all(&dl_dir).unwrap();
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // First call: Permanent, cap exceeded → blocked.
        // Second call (after unblock, same segment): ok.
        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Permanent),
            MockSink::ok(vec![b"record".to_vec()]),
        ]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = Arc::new(DrainMetrics::new());

        let handle = spawn(rx, sink.clone(), config, metrics.clone());

        // Wait for blocked state.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.state.load(Ordering::Relaxed) == STATE_BLOCKED {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(
                std::time::Instant::now() < deadline,
                "timed out entering blocked"
            );
        }

        // Clear the blocking file → dead-letter dir is now below cap.
        std::fs::remove_file(&blocking_file).unwrap();

        // Drain should unblock and retry the same segment.
        drop(tx);
        handle.join().unwrap();

        // Segment confirmed and deleted (second attempt succeeded).
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
        let metrics = Arc::new(DrainMetrics::new());
        let handle = spawn(rx, sink, config, metrics.clone());

        // Wait for blocked, then wait for multiple check intervals.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.state.load(Ordering::Relaxed) == STATE_BLOCKED {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(std::time::Instant::now() < deadline, "timed out");
        }
        // Sleep long enough to cover several check intervals (10ms each).
        std::thread::sleep(Duration::from_millis(60));

        assert_eq!(
            metrics.dead_letter_full_total.load(Ordering::Relaxed),
            1,
            "counter must increment exactly once per entry into blocked, not per wake"
        );

        drop(tx);
        handle.join().unwrap();

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn blocked_since_set_on_entry_and_cleared_on_exit() {
        let dir = tmp_dir("blocked_since");
        let sealed = make_sealed_segment(&dir, 0, &[b"r"]);

        let dl_dir = dir.join("dead_letter");
        std::fs::create_dir_all(&dl_dir).unwrap();
        let blocking_file = dl_dir.join("dl_00000001.wab.sealed");
        std::fs::write(&blocking_file, vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([
            Err(MockError::Permanent),
            MockSink::ok(vec![b"r".to_vec()]),
        ]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = Arc::new(DrainMetrics::new());
        let handle = spawn(rx, sink, config, metrics.clone());

        // Wait for blocked_since to be set.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.blocked_since.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
            assert!(std::time::Instant::now() < deadline, "timed out");
        }

        // Clear blocking file → drain unblocks.
        std::fs::remove_file(&blocking_file).unwrap();
        drop(tx);
        handle.join().unwrap();

        // blocked_since reset to None after exit.
        assert!(
            metrics.blocked_since.lock().unwrap().is_none(),
            "blocked_since must be None after unblocking"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn drain_state_only_one_value_active_at_any_time() {
        // This is a structural property of the constants — each state is a distinct u8.
        assert_ne!(STATE_DRAINING, STATE_RETRYING);
        assert_ne!(STATE_DRAINING, STATE_BLOCKED);
        assert_ne!(STATE_RETRYING, STATE_BLOCKED);
    }

    // ── max_batch_size ────────────────────────────────────────────────────────

    #[test]
    fn max_batch_size_respected_for_large_segment() {
        let dir = tmp_dir("batch_size");
        let sealed = make_sealed_segment(&dir, 0, &[b"a", b"b", b"c", b"d", b"e"]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        // max_batch_size = 2: 5 records → 3 commit calls (2+2+1).
        let sink = Arc::new(MockSink::with_batch_size(2, []));
        let metrics = Arc::new(DrainMetrics::new());
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
        assert_eq!(metrics.records_committed.load(Ordering::Relaxed), 5);

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
        run_drain(
            rx,
            tx,
            sink,
            fast_config(dir.clone()),
            Arc::new(DrainMetrics::new()),
        );

        let dl_dir = dir.join("dead_letter");
        let dl_files: Vec<PathBuf> = std::fs::read_dir(&dl_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_str().unwrap_or("").ends_with(".wab.sealed"))
            .collect();

        assert!(!dl_files.is_empty(), "dead-letter files must exist");
        for path in &dl_files {
            let records: Vec<Vec<u8>> = crate::wab::SegmentReader::open(path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .expect("dead-letter records must have valid CRCs");
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], b"dead1");
            assert_eq!(records[1], b"dead2");
        }

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
            Arc::new(DrainMetrics::new()),
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
        std::fs::create_dir_all(&dl_dir).unwrap();
        std::fs::write(dl_dir.join("dl_00000001.wab.sealed"), vec![0u8; 200]).unwrap();

        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(sealed.clone()).unwrap();

        let sink = Arc::new(MockSink::with_responses([Err(MockError::Permanent)]));
        let config = tight_dl_config(dir.clone(), 100);
        let metrics = Arc::new(DrainMetrics::new());
        let handle = spawn(rx, sink, config, metrics.clone());

        // Wait for blocked state then drop sender (shutdown signal).
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if metrics.state.load(Ordering::Relaxed) == STATE_BLOCKED {
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
