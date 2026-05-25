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
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_channel::RecvTimeoutError;
use tracing::{error, info, warn};
use weir_core::Payload;

use crate::{
    metrics::{
        DrainStateLabel, DrainStateValue, Metrics, OutcomeLabel, Outcome,
        SegmentStateLabel, SegmentState, SinkHealthLabel, SinkHealthState,
    },
    sink::{Sink, SinkError, SinkHealth, SinkRecord},
    wab::{
        SegmentReader,
        format::{EXT_CONFIRMED, EXT_SEALED, SEGMENT_FOOTER_LEN, build_confirmed, unix_nanos_now},
    },
};

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
        metrics.drain_state.get_or_create(&DrainStateLabel { state: s }).set(v);
    }
}

fn set_sink_health(metrics: &Metrics, health: SinkHealth) {
    let (healthy, degraded, down) = match health {
        SinkHealth::Healthy => (1.0, 0.0, 0.0),
        SinkHealth::Degraded(_) => (0.0, 1.0, 0.0),
        SinkHealth::Down(_) => (0.0, 0.0, 1.0),
    };
    metrics.sink_health.get_or_create(&SinkHealthLabel { state: SinkHealthState::healthy }).set(healthy);
    metrics.sink_health.get_or_create(&SinkHealthLabel { state: SinkHealthState::degraded }).set(degraded);
    metrics.sink_health.get_or_create(&SinkHealthLabel { state: SinkHealthState::down }).set(down);
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
                set_drain_state(&metrics, DrainStateValue::blocked_dead_letter_full);
                metrics.dead_letter_blocked_duration.set(blocked_since.elapsed().as_secs_f64());

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
                metrics.dead_letter_bytes_on_disk.set(dead_letter.total_bytes() as f64);
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
        ProcessResult::Transient => DrainState::RetryingTransient {
            segment,
            retries_left: config.max_retries,
            next_delay: config.base_retry_delay,
        },
        ProcessResult::BlockedDeadLetter => enter_blocked(segment, metrics),
    }
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

fn confirm_and_delete(sealed: &Path, record_count: u64, metrics: &Metrics) {
    write_confirmed_file(sealed, record_count);
    if let Err(e) = std::fs::remove_file(sealed) {
        warn!(path = %sealed.display(), error = %e, "drain: failed to delete confirmed segment");
    }
    metrics.wab_segments.get_or_create(&SegmentStateLabel { state: SegmentState::confirmed }).inc();
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
    match sink.commit(records).await {
        Ok(commit_result) => {
            metrics.sink_commit_duration.observe(t.elapsed().as_secs_f64());
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel { outcome: Outcome::committed })
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
                            .get_or_create(&OutcomeLabel { outcome: Outcome::dead_lettered })
                            .inc_by(dead_payloads.len() as u64);
                        metrics.dead_letter_bytes_on_disk.set(dead_letter.total_bytes() as f64);
                    }
                    Err(e) => {
                        error!(error = %e, "drain: failed to write dead-letter records");
                    }
                }
            }

            BatchResult::Ok
        }

        Err(e) if e.is_transient() => {
            metrics.sink_commit_duration.observe(t.elapsed().as_secs_f64());
            metrics
                .sink_commit_records
                .get_or_create(&OutcomeLabel { outcome: Outcome::retried })
                .inc_by(payloads.len() as u64);
            warn!(error = %e, "drain: transient sink error; will retry segment");
            BatchResult::Transient
        }

        Err(e) => {
            // Permanent error — dead-letter the whole batch.
            metrics.sink_commit_duration.observe(t.elapsed().as_secs_f64());
            error!(error = %e, "drain: permanent sink error; dead-lettering batch");

            let estimated = estimated_write_bytes(payloads);
            if dead_letter.would_exceed_cap(estimated, config.dead_letter_max_bytes) {
                return BatchResult::Blocked;
            }

            match dead_letter.write_records(payloads) {
                Ok(()) => {
                    metrics
                        .sink_commit_records
                        .get_or_create(&OutcomeLabel { outcome: Outcome::dead_lettered })
                        .inc_by(payloads.len() as u64);
                    metrics.dead_letter_bytes_on_disk.set(dead_letter.total_bytes() as f64);
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

    fn noop_metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    fn segments_confirmed(m: &Metrics) -> u64 {
        m.wab_segments
            .get_or_create(&SegmentStateLabel { state: SegmentState::confirmed })
            .get()
    }

    fn records_committed(m: &Metrics) -> u64 {
        m.sink_commit_records
            .get_or_create(&OutcomeLabel { outcome: Outcome::committed })
            .get()
    }

    fn records_dead_lettered(m: &Metrics) -> u64 {
        m.sink_commit_records
            .get_or_create(&OutcomeLabel { outcome: Outcome::dead_lettered })
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
        let metrics = noop_metrics();
        run_drain(rx, tx, sink, fast_config(dir.clone()), metrics.clone());

        assert!(get_confirmed_path(&sealed).exists(), "confirmed file must exist");
        assert!(!sealed.exists(), "sealed segment must be deleted after drain");
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
            MockSink::ok(vec![b"data".to_vec()]),
        ]));
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());

        assert!(get_confirmed_path(&sealed).exists(), "confirmed after successful retry");
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

        assert!(sealed.exists(), "segment must remain on disk after max retries");
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
        responses.push(MockSink::ok(vec![b"seg2".to_vec()]));
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
        assert!(!dl_files.is_empty(), "dead-letter directory must not be empty");

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
            assert!(std::time::Instant::now() < deadline, "timed out waiting for blocked state");
        }

        assert_eq!(dl_full_count(&metrics), 1);
        assert!(!get_confirmed_path(&sealed).exists());

        drop(tx);
        handle.join().unwrap();

        assert!(!get_confirmed_path(&sealed).exists(), "segment must not be confirmed on blocked shutdown");

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
            assert!(std::time::Instant::now() < deadline, "timed out waiting for blocked state");
        }

        let calls_at_block = sink.call_count();
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(sink.call_count(), calls_at_block, "commit must not be called while blocked");

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
            MockSink::ok(vec![b"record".to_vec()]),
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
            assert!(std::time::Instant::now() < deadline, "timed out entering blocked");
        }

        std::fs::remove_file(&blocking_file).unwrap();

        drop(tx);
        handle.join().unwrap();

        assert!(get_confirmed_path(&sealed).exists(), "segment must be confirmed after unblock");
        assert!(!sealed.exists());
        assert_eq!(sink.call_count(), 2, "commit called exactly twice: first attempt + retry");

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
            MockSink::ok(vec![b"r".to_vec()]),
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
        assert!(!drain_is_blocked(&metrics), "drain must not be blocked after unblocking");
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
        assert_eq!(m.drain_state.get_or_create(&DrainStateLabel { state: DrainStateValue::draining }).get(), 1.0);
        assert_eq!(m.drain_state.get_or_create(&DrainStateLabel { state: DrainStateValue::retrying_transient }).get(), 0.0);
        assert_eq!(m.drain_state.get_or_create(&DrainStateLabel { state: DrainStateValue::blocked_dead_letter_full }).get(), 0.0);

        set_drain_state(&m, DrainStateValue::blocked_dead_letter_full);
        assert_eq!(m.drain_state.get_or_create(&DrainStateLabel { state: DrainStateValue::draining }).get(), 0.0);
        assert_eq!(m.drain_state.get_or_create(&DrainStateLabel { state: DrainStateValue::blocked_dead_letter_full }).get(), 1.0);
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
        run_drain(rx, tx, sink.clone(), fast_config(dir.clone()), metrics.clone());

        assert_eq!(sink.call_count(), 3, "5 records with batch=2 → 3 commit calls");
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

        assert!(!get_confirmed_path(&sealed).exists(), "segment must NOT be confirmed on blocked shutdown");
        assert!(sealed.exists(), "segment must still exist after blocked shutdown");

        std::fs::remove_dir_all(dir).ok();
    }
}
