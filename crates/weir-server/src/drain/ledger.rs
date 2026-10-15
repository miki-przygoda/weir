//! Drain-owned, in-memory bookkeeping keyed by segment path (spec §4.0).
//!
//! The drain has two entry points for a segment path — the crossbeam channel
//! the WAB flushers feed, and its own in-memory `pending` queue (filled by the
//! stranded-segment rescan and by the waits that buffer channel arrivals). The
//! channel is not enumerable, so the same path can sit in both at once: up to
//! 256 copies of a path can still be in the channel when the idle rescan finds
//! the same file on disk. Before this ledger the second copy was a harmless
//! NotFound no-op only because a delivered segment was always deleted. Every
//! state that keeps a delivered or set-aside segment ON DISK (a sidecar still
//! to write, a quarantine move still to do, a parked segment, a cool-down)
//! therefore needs one choke point that both sources pass through. That choke
//! point is [`SegmentLedger::admit`]; the rescan's equivalent, applied before
//! any side effect of the confirmed-check, is [`SegmentLedger::excludes`].
//!
//! The ledger also carries the resume cursor across strands: a segment that
//! strands after committing a prefix resumes past that prefix when it is
//! re-admitted within the same run, instead of re-delivering it.
//!
//! # Ownership
//!
//! `run_drain_supervised` owns the ledger and lends it `&mut` to each
//! `drain_thread` incarnation, so it survives a drain panic and respawn. It is
//! touched only on the drain thread. Nothing here is persisted: a restart falls
//! back to replay-from-record-0, which is the at-least-once contract.
//!
//! Readers on other threads (metrics, and later a status endpoint) never touch
//! the maps; they read [`LedgerCounts`], which is refreshed after every mutation.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use tracing::{info, warn};
use weir_core::Payload;

use super::confirmed::{self, ConfirmOutcome};
use crate::metrics::Metrics;

// ── Public types ──────────────────────────────────────────────────────────────

/// How far into a segment the drain has durably processed (committed or
/// dead-lettered), pinned to the file it was measured on.
///
/// `(dev, ino)` is what makes the cursor safe to reuse. Sealed segments are
/// immutable and names are unique within a run (the writer's counter only
/// grows), but an operator can still replace a file at the same path — a
/// `weir-ctl quarantine requeue`, a restore from backup. A cursor whose
/// identity no longer matches `stat(path)` describes a different file and is
/// discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ResumeCursor {
    pub processed: u64,
    pub dev: u64,
    pub ino: u64,
}

/// The verdict of [`SegmentLedger::admit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    /// Process the segment, skipping the first `skip` records (already durably
    /// handled earlier in this run).
    Process { skip: u64 },
    /// Do not process this copy of the path now. The `&'static str` names the
    /// ledger state that owns it, for the debug log. The rescan is the re-entry
    /// point once the path becomes admissible again.
    Drop(&'static str),
}

/// Why a segment is parked: set aside for the rest of this run, bytes kept on
/// disk, replayed on the next start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ParkReason {
    /// A segment-class local read error (`Access`) persisted past the
    /// escalation limit (§4.1).
    LocalReadFailed,
    /// The header names a format version newer than this binary reads (§4.1).
    /// Not corrupt: a newer weir delivers it after an upgrade.
    UnsupportedVersion,
    /// The drain panicked twice while this segment was in flight (§4.12).
    DrainPanic,
}

impl ParkReason {
    /// Index into [`LedgerCounts::parked_by_reason`].
    pub(crate) fn index(self) -> usize {
        match self {
            ParkReason::LocalReadFailed => 0,
            ParkReason::UnsupportedVersion => 1,
            ParkReason::DrainPanic => 2,
        }
    }
}

/// The class of a local I/O error, which decides whether it counts toward a
/// segment's escalation (spec §4.0 table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrClass {
    /// Daemon-wide resource pressure (descriptors, memory, buffers, a signal).
    /// Not a property of the segment, so it never counts toward escalation: the
    /// segment strands and resumes, cursor kept, and self-heals when the
    /// pressure clears.
    Process,
    /// The path cannot be read as a file by this process (permissions, a
    /// directory or a symlink loop in its place). Escalates to park.
    Access,
    /// The bytes cannot be read (I/O error, stale handle, bad message) and
    /// everything unrecognised. Escalates to quarantine.
    Media,
}

/// What made a segment attempt transient. Decides the strand bookkeeping:
/// only a `Sink` cause marks the sink down and counts as a sink strand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransientCause {
    /// The sink failed transiently (or timed out, or broke its contract).
    Sink,
    /// Reading the sealed segment failed with a non-corruption error.
    LocalRead { class: ErrClass, errno: Option<i32> },
    /// A local write the drain needs before it may advance failed — the
    /// dead-letter write after a successful sink commit (§4.11).
    LocalWrite,
}

/// How many drain panics a segment may be in flight for before it is parked as
/// `drain_panic` (spec §4.12). One panic can be bad luck (a sink bug tripped by
/// timing, a transient allocation failure); a second on the same segment, after
/// a respawn, is deterministic enough to stop retrying it this run. Without the
/// park every respawn's rescan re-queues the segment, it panics again, and once
/// the supervisor's respawn budget is spent delivery stops for every segment.
pub(crate) const DRAIN_PANIC_PARK_LIMIT: u32 = 2;

/// Upper bound on the payload bytes all [`DeadLetterCarry`]s may hold in memory
/// at once (spec §4.5). A carry that would exceed it is not stored, and that
/// segment falls back to re-committing its blocked sub-batch once, with a WARN.
pub(crate) const DL_CARRY_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Dead-letter payloads a sub-batch still owes after its sink commit, so the
/// retry redoes only the dead-letter write instead of re-committing the
/// accepted records (§4.5: the cap blocked the write; §4.11: the write failed).
#[derive(Debug)]
pub(crate) struct DeadLetterCarry {
    /// The cursor to advance to once `payloads` are written: the end of the
    /// sub-batch they came from.
    pub through: u64,
    /// The records the sink rejected, to be dead-lettered.
    pub payloads: Vec<Payload>,
    /// Sum of the payload lengths: what the carry holds in memory, counted
    /// against [`DL_CARRY_MAX_BYTES`].
    pub bytes: u64,
    /// Records of the same sub-batch the sink accepted. They are counted as
    /// committed only once the carry is written, exactly as an unblocked
    /// sub-batch counts them only once its dead-letter write lands.
    pub committed: u64,
}

/// A carry pinned to the `(dev, ino)` of the file it was measured on, like
/// [`ResumeCursor`]: a carry for a replaced file names another segment's
/// records and must never be written.
#[derive(Debug)]
struct PinnedCarry {
    carry: DeadLetterCarry,
    dev: u64,
    ino: u64,
}

/// Which step of a confirm is still outstanding for a delivered segment (§4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfirmStage {
    Sidecar,
    Delete,
}

/// A delivered segment whose `.confirmed` sidecar or delete is outstanding
/// (§4.3). Delivery is never retried for it; only the confirm is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfirmPending {
    pub stage: ConfirmStage,
    pub record_count: u64,
}

/// Wait-free mirrors of the ledger's sizes, for readers off the drain thread
/// (the metrics gauges, and the status seam in spec §5). Updated by the drain
/// after every ledger mutation; nobody but the drain ever touches the maps.
#[derive(Default, Debug)]
pub(crate) struct LedgerCounts {
    pub confirm_pending: AtomicUsize,
    pub quarantine_pending: AtomicUsize,
    /// Indexed by [`ParkReason::index`].
    pub parked_by_reason: [AtomicUsize; 3],
    pub cooling_down: AtomicUsize,
    pub resume_cursors: AtomicUsize,
    pub dl_carry_bytes: AtomicU64,
}

// ── The ledger ────────────────────────────────────────────────────────────────

pub(crate) struct SegmentLedger {
    /// Durable progress per segment; survives a strand within a run (F05).
    resume_from: HashMap<PathBuf, ResumeCursor>,
    /// Dead-letter writes a blocked sub-batch still owes (§4.5, §4.11).
    dl_carry: HashMap<PathBuf, PinnedCarry>,
    /// The bound on the sum of `dl_carry` bytes ([`DL_CARRY_MAX_BYTES`]; a
    /// field only so tests can lower it).
    dl_carry_max_bytes: u64,
    /// Delivered; sidecar or delete outstanding (§4.3).
    confirm_pending: HashMap<PathBuf, ConfirmPending>,
    /// Quarantine move outstanding (§4.4).
    quarantine_pending: HashMap<PathBuf, &'static str>,
    /// Not re-queued this run.
    parked: HashMap<PathBuf, ParkReason>,
    /// Segment-class local read failures (§4.1, §4.2).
    local_read_failures: HashMap<PathBuf, LocalReadFailures>,
    /// Per-segment cool-down (§4.6).
    not_before: HashMap<PathBuf, Instant>,
    /// Drain panics while this segment was in flight (§4.12).
    panic_count: HashMap<PathBuf, u32>,
    /// The segment being processed when a panic unwinds (§4.12).
    in_flight: Option<PathBuf>,
    /// When the latest attempt began. A segment's local read failures are aged
    /// to this instant, not to "now", so the escalation decided after an
    /// attempt and the `last_local_read_chance` decided before it always agree.
    attempt_started: Instant,
    counts: Arc<LedgerCounts>,
}

/// A segment's segment-class local read failures in this run.
#[derive(Clone, Copy, Debug)]
struct LocalReadFailures {
    count: u32,
    /// When the first of them was counted: escalation also needs this to be
    /// old enough (conf-regressions-8), so a short blip never escalates.
    first_at: Instant,
}

impl SegmentLedger {
    pub(crate) fn new(counts: Arc<LedgerCounts>) -> Self {
        let ledger = Self {
            resume_from: HashMap::new(),
            dl_carry: HashMap::new(),
            dl_carry_max_bytes: DL_CARRY_MAX_BYTES,
            confirm_pending: HashMap::new(),
            quarantine_pending: HashMap::new(),
            parked: HashMap::new(),
            local_read_failures: HashMap::new(),
            not_before: HashMap::new(),
            panic_count: HashMap::new(),
            in_flight: None,
            attempt_started: Instant::now(),
            counts,
        };
        ledger.sync_counts();
        ledger
    }

    /// The single admission check, consulted for every path the Draining arm is
    /// about to process, whichever source it came from (the channel or
    /// `pending`).
    ///
    /// A path owned by a disposition (`confirm_pending`, `quarantine_pending`),
    /// parked, or still inside its `not_before` cool-down is dropped. An elapsed
    /// cool-down is cleared. An admissible path resumes past its cursor when the
    /// cursor still describes the file at `path`; a stale cursor is discarded.
    pub(crate) fn admit(&mut self, path: &Path, now: Instant) -> Admit {
        if self.confirm_pending.contains_key(path) {
            return Admit::Drop("confirm_pending");
        }
        if self.quarantine_pending.contains_key(path) {
            return Admit::Drop("quarantine_pending");
        }
        if self.parked.contains_key(path) {
            return Admit::Drop("parked");
        }
        if let Some(&at) = self.not_before.get(path) {
            if now < at {
                return Admit::Drop("cooling_down");
            }
            self.not_before.remove(path);
            self.sync_counts();
        }
        let Some(cursor) = self.resume_from.get(path).copied() else {
            return Admit::Process { skip: 0 };
        };
        if file_identity(path) == Some((cursor.dev, cursor.ino)) {
            Admit::Process {
                skip: cursor.processed,
            }
        } else {
            self.resume_from.remove(path);
            self.sync_counts();
            Admit::Process { skip: 0 }
        }
    }

    /// True when the rescan must skip `path` before any side effect: it is owned
    /// by a disposition, parked, or still cooling down. The rescan applies this
    /// BEFORE the confirmed-check, whose quarantine side effects must never touch
    /// a ledger-owned segment.
    pub(crate) fn excludes(&self, path: &Path, now: Instant) -> bool {
        self.confirm_pending.contains_key(path)
            || self.quarantine_pending.contains_key(path)
            || self.parked.contains_key(path)
            || self.not_before.get(path).is_some_and(|&at| now < at)
    }

    /// Checks a resume point the drain carried OUTSIDE the ledger against the
    /// cursor's `(dev, ino)` pin, as [`Self::admit`] does for a path it admits
    /// (cont-durability-1). The `RetryingTransient` and `BlockedDeadLetterFull`
    /// states carry `processed` themselves and never pass through admission, so
    /// without this a file replaced between attempts had its first `skip`
    /// records skipped, and was then confirmed and deleted without them.
    ///
    /// Returns `skip` unchanged unless the cursor's pin positively names another
    /// file: then the cursor is discarded and the new file starts at record 0.
    /// A missing cursor (the stat failed when it was recorded) or a file that
    /// cannot be stat'ed now keeps `skip`, as before: resetting on an
    /// unproven mismatch would re-deliver the prefix of the SAME file.
    pub(crate) fn checked_skip(&mut self, path: &Path, skip: u64) -> u64 {
        if skip == 0 {
            return 0;
        }
        let Some(cursor) = self.resume_from.get(path).copied() else {
            return skip;
        };
        match file_identity(path) {
            Some(id) if id != (cursor.dev, cursor.ino) => {
                warn!(
                    path = %path.display(),
                    skip,
                    "drain: the segment file was replaced since its resume cursor was recorded; \
                     the new file is processed from its first record"
                );
                self.resume_from.remove(path);
                self.sync_counts();
                0
            }
            _ => skip,
        }
    }

    /// Records that the first `processed` records of `path` are durably handled,
    /// pinned to the file's current `(dev, ino)`. Called whenever an attempt
    /// returns transient or blocked; the cursor then survives a strand, so a
    /// re-admitted segment resumes past its committed prefix.
    ///
    /// `processed == 0` records nothing. A file that cannot be stat'ed gets no
    /// cursor (and loses any old one): admission would discard it anyway.
    pub(crate) fn record_progress(&mut self, path: &Path, processed: u64) {
        if processed == 0 {
            return;
        }
        match file_identity(path) {
            Some((dev, ino)) => {
                self.resume_from.insert(
                    path.to_path_buf(),
                    ResumeCursor {
                        processed,
                        dev,
                        ino,
                    },
                );
            }
            None => {
                self.resume_from.remove(path);
            }
        }
        self.sync_counts();
    }

    /// Drops every per-segment progress entry for `path` (cursor, carry, read
    /// failures, cool-down, panic count): it was confirmed or quarantined, so
    /// nothing about it is worth remembering. Disposition and park entries are
    /// left alone; they are set and cleared explicitly.
    pub(crate) fn forget(&mut self, path: &Path) {
        self.resume_from.remove(path);
        self.dl_carry.remove(path);
        self.local_read_failures.remove(path);
        self.not_before.remove(path);
        self.panic_count.remove(path);
        self.sync_counts();
    }

    /// Sets `path` aside for the rest of this run. Its progress is forgotten: a
    /// parked segment replays from record 0 on the next start (or when an
    /// operator un-parks it), so no cursor may outlive the park.
    pub(crate) fn park(&mut self, path: &Path, why: ParkReason) {
        self.parked.insert(path.to_path_buf(), why);
        self.forget(path);
    }

    /// Marks `path` as the segment in flight, for the supervisor to blame if
    /// the drain panics before [`Self::end_attempt`] (spec §4.12). The drain
    /// sets it around each attempt: the dead-letter carry write, the read and
    /// the sink commits.
    pub(crate) fn begin_attempt(&mut self, path: &Path) {
        self.in_flight = Some(path.to_path_buf());
        self.attempt_started = Instant::now();
    }

    /// Clears the in-flight segment: the attempt returned without panicking.
    pub(crate) fn end_attempt(&mut self) {
        self.in_flight = None;
    }

    /// Called by the panic supervisor once a drain incarnation has unwound.
    /// Charges the panic to the segment that was in flight, if any, and parks
    /// that segment as [`ParkReason::DrainPanic`] once it reaches
    /// [`DRAIN_PANIC_PARK_LIMIT`]. Returns the segment and its panic count, or
    /// `None` when the drain panicked outside an attempt (a probe, a confirm,
    /// opening the dead-letter writer): nothing to blame, nothing parked.
    ///
    /// Parking forgets the segment's progress like every park, so the next
    /// start replays it from record 0. Below the limit its cursor is kept: a
    /// panic records no progress of its own, so the retry resumes from
    /// wherever the last transient or blocked attempt left it.
    pub(crate) fn note_drain_panic(&mut self) -> Option<(PathBuf, u32)> {
        let path = self.in_flight.take()?;
        let n = self.panic_count.entry(path.clone()).or_insert(0);
        *n = n.saturating_add(1);
        let panics = *n;
        if panics >= DRAIN_PANIC_PARK_LIMIT {
            self.park(&path, ParkReason::DrainPanic);
        }
        Some((path, panics))
    }

    /// Counts one local read failure of `path` and returns the running total.
    /// `Process`-class errors are daemon-wide, not a property of the segment,
    /// so they never count.
    ///
    /// The first counted failure of `path` also records when the failing
    /// began, for [`Self::local_read_failing_for`].
    pub(crate) fn note_local_read_failure(&mut self, path: &Path, class: ErrClass) -> u32 {
        if class == ErrClass::Process {
            return self.local_read_failures(path);
        }
        let f = self
            .local_read_failures
            .entry(path.to_path_buf())
            .or_insert(LocalReadFailures {
                count: 0,
                first_at: Instant::now(),
            });
        f.count = f.count.saturating_add(1);
        f.count
    }

    /// The running count of segment-class local read failures of `path`, without
    /// counting one. The drain asks before an attempt whether the next failure
    /// would escalate (spec §4.2).
    pub(crate) fn local_read_failures(&self, path: &Path) -> u32 {
        self.local_read_failures.get(path).map_or(0, |f| f.count)
    }

    /// How long `path` had been failing local reads when the latest attempt
    /// began: from its first counted failure in this run to the start of the
    /// attempt. Zero when it has none, or when the first one came during that
    /// attempt. Escalation needs this to reach the drain's min age as well as
    /// the count to reach its limit (conf-regressions-8).
    pub(crate) fn local_read_failing_for(&self, path: &Path) -> std::time::Duration {
        self.local_read_failures
            .get(path)
            .map_or(std::time::Duration::ZERO, |f| {
                self.attempt_started.saturating_duration_since(f.first_at)
            })
    }

    /// Stores what `path` still owes the dead-letter directory, pinned to the
    /// file's current `(dev, ino)`, replacing any earlier carry of `path`.
    ///
    /// Returns false, storing nothing, when the carry would push the sum of all
    /// carries over the bound, or when the file cannot be stat'ed (nothing to
    /// pin it to). The caller then falls back to re-committing the sub-batch on
    /// the next attempt — the behaviour before carries existed — so a refusal
    /// costs one duplicate commit, never a record.
    pub(crate) fn set_dl_carry(&mut self, path: &Path, carry: DeadLetterCarry) -> bool {
        let others: u64 = self
            .dl_carry
            .iter()
            .filter(|(p, _)| p.as_path() != path)
            .map(|(_, pinned)| pinned.carry.bytes)
            .sum();
        let stored = match file_identity(path) {
            Some((dev, ino)) if others.saturating_add(carry.bytes) <= self.dl_carry_max_bytes => {
                self.dl_carry
                    .insert(path.to_path_buf(), PinnedCarry { carry, dev, ino });
                true
            }
            _ => {
                self.dl_carry.remove(path);
                false
            }
        };
        self.sync_counts();
        stored
    }

    /// Takes the carry of `path`, if any, for the attempt about to process it.
    /// A carry whose pin no longer matches the file at `path` is discarded: it
    /// describes another segment's records.
    pub(crate) fn take_dl_carry(&mut self, path: &Path) -> Option<DeadLetterCarry> {
        let pinned = self.dl_carry.remove(path)?;
        self.sync_counts();
        if file_identity(path) == Some((pinned.dev, pinned.ino)) {
            Some(pinned.carry)
        } else {
            warn!(
                path = %path.display(),
                "drain: discarding a dead-letter carry measured on a file that has since been \
                 replaced; the new file is processed from its own start"
            );
            None
        }
    }

    /// Lowers the carry bound, for tests that cannot hold 64 MiB.
    #[cfg(test)]
    pub(crate) fn with_dl_carry_max_bytes(mut self, max: u64) -> Self {
        self.dl_carry_max_bytes = max;
        self
    }

    /// Keeps `path` out of admission and the rescan until `at`.
    pub(crate) fn set_not_before(&mut self, path: &Path, at: Instant) {
        self.not_before.insert(path.to_path_buf(), at);
        self.sync_counts();
    }

    /// Takes ownership of a delivered segment whose confirm did not finish
    /// (§4.3): from here on admission drops every copy of `path`, the rescan
    /// skips it, and [`Self::retry_dispositions`] retries only the outstanding
    /// stage. `Done` releases nothing because nothing was taken.
    pub(crate) fn note_confirm_outcome(
        &mut self,
        path: &Path,
        outcome: ConfirmOutcome,
        record_count: u64,
    ) {
        let stage = match outcome {
            ConfirmOutcome::Done => return,
            ConfirmOutcome::SidecarFailed => ConfirmStage::Sidecar,
            ConfirmOutcome::DeleteFailed => ConfirmStage::Delete,
        };
        self.confirm_pending.insert(
            path.to_path_buf(),
            ConfirmPending {
                stage,
                record_count,
            },
        );
        self.sync_counts();
    }

    /// Takes ownership of a segment whose quarantine move failed (§4.4): from
    /// here on admission drops every copy of `path`, the rescan skips it, and
    /// [`Self::retry_dispositions`] retries only the move. Its progress is
    /// forgotten: whatever prefix it had was delivered, and it is never
    /// resumed, only moved. The failure itself was counted by the first move.
    pub(crate) fn note_quarantine_pending(&mut self, path: &Path, reason: &'static str) {
        self.quarantine_pending.insert(path.to_path_buf(), reason);
        self.forget(path);
    }

    /// Retries outstanding dispositions — the sidecar write and delete of
    /// `confirm_pending`, the move of `quarantine_pending` into
    /// `<wab_dir>/quarantine/`. **It never delivers.** Runs first in every
    /// health poll and once on drain exit.
    ///
    /// A `Sidecar` entry redoes the whole confirm (the atomic sidecar, then the
    /// delete); a `Delete` entry, whose sidecar is already durable, redoes only
    /// the delete. A segment that has vanished meanwhile (an operator removed
    /// it, or a concurrent scan deleted it as an orphan) completes as `Done`.
    ///
    /// A quarantine entry redoes only the move. A landed move bumps the
    /// quarantine series like any other quarantine; a segment that vanished
    /// meanwhile is done; a move that still fails is logged but not counted
    /// again (`weir_drain_quarantine_failures_total` counts segments, and the
    /// disposition gauge shows how many are still waiting).
    ///
    /// Each step runs under the drain's busy signal (`LocalFsync` for a
    /// confirm, `QuarantineMove` for a move, spec §4.8), so a slow disk here
    /// reads as slow local I/O rather than a wedged drain.
    pub(crate) fn retry_dispositions(
        &mut self,
        wab_dir: &Path,
        metrics: &Metrics,
        hb: &super::DrainHeartbeat,
    ) {
        if self.confirm_pending.is_empty() && self.quarantine_pending.is_empty() {
            return;
        }
        self.retry_confirms(metrics, hb);
        self.retry_quarantines(wab_dir, metrics, hb);
        self.sync_counts();
    }

    fn retry_quarantines(&mut self, wab_dir: &Path, metrics: &Metrics, hb: &super::DrainHeartbeat) {
        let mut moved = Vec::new();
        for (path, &reason) in &self.quarantine_pending {
            let result = {
                let _busy = super::BusyGuard::new(hb, super::BusyKind::QuarantineMove);
                super::move_to_quarantine(path, wab_dir, metrics, reason)
            };
            match result {
                Ok(()) => {
                    info!(
                        path = %path.display(),
                        reason,
                        "drain: outstanding quarantine move completed without re-delivery"
                    );
                    moved.push(path.clone());
                }
                Err(e) => warn!(
                    path = %path.display(),
                    error = %e,
                    reason,
                    "drain: quarantine move still failing; segment kept on disk, not re-delivered, \
                     retried next health poll"
                ),
            }
        }
        for path in moved {
            self.quarantine_pending.remove(&path);
        }
    }

    fn retry_confirms(&mut self, metrics: &Metrics, hb: &super::DrainHeartbeat) {
        let mut done = Vec::new();
        for (path, pending) in self.confirm_pending.iter_mut() {
            let _busy = super::BusyGuard::new(hb, super::BusyKind::LocalFsync);
            let outcome = match pending.stage {
                ConfirmStage::Sidecar => {
                    confirmed::confirm_and_delete(path, pending.record_count, metrics)
                }
                ConfirmStage::Delete => confirmed::delete_confirmed_segment(path, metrics),
            };
            match outcome {
                ConfirmOutcome::Done => {
                    info!(
                        path = %path.display(),
                        "drain: outstanding confirm completed; segment confirmed and deleted \
                         without re-delivery"
                    );
                    done.push(path.clone());
                }
                // Still failing; counted by the confirm itself. Retried next poll.
                ConfirmOutcome::SidecarFailed => {}
                // The sidecar landed: from now on only the delete is owed.
                ConfirmOutcome::DeleteFailed => pending.stage = ConfirmStage::Delete,
            }
        }
        for path in done {
            self.confirm_pending.remove(&path);
        }
    }

    /// The outstanding confirm stage of `path`, if the ledger owns it.
    #[cfg(test)]
    pub(crate) fn confirm_stage(&self, path: &Path) -> Option<ConfirmStage> {
        self.confirm_pending.get(path).map(|c| c.stage)
    }

    /// The shared count mirrors.
    pub(crate) fn counts(&self) -> &Arc<LedgerCounts> {
        &self.counts
    }

    /// Re-publishes every size into [`LedgerCounts`]. Called after each
    /// mutation; the maps are small (a handful of set-aside segments), so the
    /// park tally is recomputed rather than tracked incrementally.
    fn sync_counts(&self) {
        let c = &self.counts;
        c.confirm_pending
            .store(self.confirm_pending.len(), Ordering::Relaxed);
        c.quarantine_pending
            .store(self.quarantine_pending.len(), Ordering::Relaxed);
        let mut parked = [0usize; 3];
        for why in self.parked.values() {
            parked[why.index()] += 1;
        }
        for (slot, n) in c.parked_by_reason.iter().zip(parked) {
            slot.store(n, Ordering::Relaxed);
        }
        c.cooling_down
            .store(self.not_before.len(), Ordering::Relaxed);
        c.resume_cursors
            .store(self.resume_from.len(), Ordering::Relaxed);
        c.dl_carry_bytes.store(
            self.dl_carry
                .values()
                .map(|pinned| pinned.carry.bytes)
                .sum(),
            Ordering::Relaxed,
        );
    }
}

/// `(dev, ino)` of the file at `path`, or `None` when it cannot be stat'ed.
#[cfg(unix)]
fn file_identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}

/// weir-server has no ingest path off Unix, so a WAB is never drained there;
/// without an inode a cursor can never be proven current, so none is kept.
#[cfg(not(unix))]
fn file_identity(_path: &Path) -> Option<(u64, u64)> {
    None
}

/// Classifies a local read error per the spec §4.0 table, by errno when there
/// is one. An error without an errno (one a reader synthesised) is classified
/// by its kind, falling back to `Media` like any unrecognised errno.
pub(crate) fn classify(err: &io::Error) -> ErrClass {
    #[cfg(unix)]
    if let Some(errno) = err.raw_os_error() {
        return match errno {
            libc::EMFILE
            | libc::ENFILE
            | libc::ENOMEM
            | libc::ENOBUFS
            | libc::EAGAIN
            | libc::EINTR => ErrClass::Process,
            libc::EACCES | libc::EPERM | libc::EISDIR | libc::ENOTDIR | libc::ELOOP => {
                ErrClass::Access
            }
            _ => ErrClass::Media,
        };
    }
    match err.kind() {
        io::ErrorKind::OutOfMemory | io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock => {
            ErrClass::Process
        }
        io::ErrorKind::PermissionDenied
        | io::ErrorKind::IsADirectory
        | io::ErrorKind::NotADirectory => ErrClass::Access,
        _ => ErrClass::Media,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ledger() -> SegmentLedger {
        SegmentLedger::new(Arc::new(LedgerCounts::default()))
    }

    fn file(label: &str) -> (PathBuf, PathBuf) {
        let dir = crate::testutil::scratch_dir(&format!("ledger_{label}"));
        crate::testutil::mkdir_p(&dir);
        let path = dir.join("seg_00000001.wab.sealed");
        std::fs::write(&path, b"sealed bytes").unwrap();
        (dir, path)
    }

    #[test]
    fn admit_skips_parked_confirm_and_quarantine_pending() {
        let (dir, path) = file("admit_owned");
        let now = Instant::now();

        let mut l = ledger();
        assert_eq!(l.admit(&path, now), Admit::Process { skip: 0 });
        assert!(!l.excludes(&path, now));

        let mut l = ledger();
        l.confirm_pending.insert(
            path.clone(),
            ConfirmPending {
                stage: ConfirmStage::Sidecar,
                record_count: 1,
            },
        );
        assert!(matches!(l.admit(&path, now), Admit::Drop(_)));
        assert!(l.excludes(&path, now));

        let mut l = ledger();
        l.quarantine_pending.insert(path.clone(), "test");
        assert!(matches!(l.admit(&path, now), Admit::Drop(_)));
        assert!(l.excludes(&path, now));

        let mut l = ledger();
        l.park(&path, ParkReason::LocalReadFailed);
        assert!(matches!(l.admit(&path, now), Admit::Drop(_)));
        assert!(l.excludes(&path, now));
        assert_eq!(
            l.counts().parked_by_reason[ParkReason::LocalReadFailed.index()]
                .load(Ordering::Relaxed),
            1,
            "parking must be mirrored in the wait-free counts"
        );

        // A cool-down drops the path until it is due, then admits it again.
        let mut l = ledger();
        l.set_not_before(&path, now + Duration::from_secs(60));
        assert!(matches!(l.admit(&path, now), Admit::Drop(_)));
        assert!(l.excludes(&path, now));
        assert_eq!(l.counts().cooling_down.load(Ordering::Relaxed), 1);
        let later = now + Duration::from_secs(61);
        assert!(!l.excludes(&path, later));
        assert_eq!(l.admit(&path, later), Admit::Process { skip: 0 });
        assert_eq!(
            l.counts().cooling_down.load(Ordering::Relaxed),
            0,
            "an elapsed cool-down is cleared once the path is admitted"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn admit_resumes_from_cursor_when_inode_matches() {
        let (dir, path) = file("admit_cursor");
        let mut l = ledger();
        l.record_progress(&path, 7);
        assert_eq!(l.counts().resume_cursors.load(Ordering::Relaxed), 1);
        assert_eq!(l.admit(&path, Instant::now()), Admit::Process { skip: 7 });
        // Admission does not consume the cursor: a second admission (a channel
        // copy after a strand) resumes from the same place.
        assert_eq!(l.admit(&path, Instant::now()), Admit::Process { skip: 7 });

        // Progress only moves forward with the caller's value.
        l.record_progress(&path, 9);
        assert_eq!(l.admit(&path, Instant::now()), Admit::Process { skip: 9 });

        // Confirmed / quarantined: the cursor goes.
        l.forget(&path);
        assert_eq!(l.admit(&path, Instant::now()), Admit::Process { skip: 0 });
        assert_eq!(l.counts().resume_cursors.load(Ordering::Relaxed), 0);

        // Parking drops the cursor too: a parked segment replays from record 0
        // on the next start, so a stale cursor must not outlive the park.
        l.record_progress(&path, 3);
        l.park(&path, ParkReason::DrainPanic);
        assert_eq!(l.counts().resume_cursors.load(Ordering::Relaxed), 0);

        // processed == 0 records nothing.
        let mut l = ledger();
        l.record_progress(&path, 0);
        assert_eq!(l.counts().resume_cursors.load(Ordering::Relaxed), 0);

        std::fs::remove_dir_all(dir).ok();
    }

    /// cont-durability-1: the same pin check for a resume point a drain state
    /// carried, which never passes through `admit`. Only a positive mismatch
    /// resets: with no cursor to compare against, the carried value stands.
    #[test]
    fn checked_skip_resets_only_a_skip_whose_cursor_names_another_file() {
        let (dir, path) = file("checked_skip");
        let mut l = ledger();
        assert_eq!(l.checked_skip(&path, 0), 0);
        assert_eq!(
            l.checked_skip(&path, 4),
            4,
            "no cursor: nothing to disprove"
        );

        l.record_progress(&path, 4);
        assert_eq!(l.checked_skip(&path, 4), 4, "same file: resume past it");
        assert_eq!(l.counts().resume_cursors.load(Ordering::Relaxed), 1);

        let tmp = dir.join("replacement.tmp");
        std::fs::write(&tmp, b"a different segment").unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        assert_eq!(
            l.checked_skip(&path, 4),
            0,
            "a replaced file is processed from its first record"
        );
        assert_eq!(
            l.counts().resume_cursors.load(Ordering::Relaxed),
            0,
            "the stale cursor is discarded"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn stale_cursor_for_replaced_file_resets_to_zero() {
        let (dir, path) = file("stale_cursor");
        let mut l = ledger();
        l.record_progress(&path, 5);

        // Replace the file at the same path with a different inode. The new file
        // is created while the old one still exists, so the inode cannot be
        // reused.
        let tmp = dir.join("replacement.tmp");
        std::fs::write(&tmp, b"a different segment").unwrap();
        std::fs::rename(&tmp, &path).unwrap();

        assert_eq!(
            l.admit(&path, Instant::now()),
            Admit::Process { skip: 0 },
            "a cursor measured on a replaced file must not skip records of the new one"
        );
        assert_eq!(
            l.counts().resume_cursors.load(Ordering::Relaxed),
            0,
            "the stale cursor is discarded, not kept"
        );

        // A vanished file also resets (process_segment then sees NotFound).
        l.record_progress(&path, 5);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(l.admit(&path, Instant::now()), Admit::Process { skip: 0 });

        std::fs::remove_dir_all(dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn classify_maps_errnos_per_table() {
        let class = |errno: i32| classify(&io::Error::from_raw_os_error(errno));
        for errno in [
            libc::EMFILE,
            libc::ENFILE,
            libc::ENOMEM,
            libc::ENOBUFS,
            libc::EAGAIN,
            libc::EINTR,
        ] {
            assert_eq!(class(errno), ErrClass::Process, "errno {errno}");
        }
        for errno in [
            libc::EACCES,
            libc::EPERM,
            libc::EISDIR,
            libc::ENOTDIR,
            libc::ELOOP,
        ] {
            assert_eq!(class(errno), ErrClass::Access, "errno {errno}");
        }
        for errno in [
            libc::EIO,
            libc::ENXIO,
            libc::ESTALE,
            libc::EBADMSG,
            libc::ENOSPC,
        ] {
            assert_eq!(class(errno), ErrClass::Media, "errno {errno}");
        }
        // No errno (a synthetic error, e.g. a reader's InvalidData): any other
        // kind is Media, and the kinds that name a table row map to it.
        assert_eq!(
            classify(&io::Error::new(io::ErrorKind::InvalidData, "bad")),
            ErrClass::Media
        );
        assert_eq!(classify(&io::Error::other("eio")), ErrClass::Media);
        assert_eq!(
            classify(&io::Error::from(io::ErrorKind::PermissionDenied)),
            ErrClass::Access
        );
        assert_eq!(
            classify(&io::Error::from(io::ErrorKind::Interrupted)),
            ErrClass::Process
        );
    }

    /// The confirm_pending lifecycle (spec §4.3): an unfinished confirm makes
    /// the ledger own the path (admission drops it, the rescan excludes it, the
    /// gauge mirror counts it), a `Done` outcome takes nothing, and a segment
    /// that vanished while its sidecar was outstanding is released without
    /// writing a sidecar for a file that no longer exists.
    #[test]
    fn a_confirm_pending_entry_owns_the_path_until_the_confirm_finishes() {
        let (dir, path) = file("confirm_pending");
        let metrics = Metrics::new().0;
        let now = Instant::now();

        let mut l = ledger();
        l.note_confirm_outcome(&path, ConfirmOutcome::Done, 1);
        assert_eq!(l.confirm_stage(&path), None, "Done takes nothing");

        l.note_confirm_outcome(&path, ConfirmOutcome::SidecarFailed, 1);
        assert_eq!(l.confirm_stage(&path), Some(ConfirmStage::Sidecar));
        assert_eq!(l.admit(&path, now), Admit::Drop("confirm_pending"));
        assert!(l.excludes(&path, now));
        assert_eq!(l.counts().confirm_pending.load(Ordering::Relaxed), 1);

        std::fs::remove_file(&path).unwrap();
        l.retry_dispositions(&dir, &metrics, &super::super::DrainHeartbeat::new());
        assert_eq!(l.confirm_stage(&path), None);
        assert_eq!(l.counts().confirm_pending.load(Ordering::Relaxed), 0);
        assert!(
            !crate::wab::format::confirmed_path_for(&path).exists(),
            "no sidecar is written for a segment that is gone"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// The quarantine_pending lifecycle (spec §4.4): a failed move makes the
    /// ledger own the path (admission drops it, the rescan excludes it, the
    /// gauge mirror counts it, its cursor goes), each retry redoes only the
    /// move, and a segment that vanished meanwhile is released as done.
    #[test]
    fn a_quarantine_pending_entry_owns_the_path_until_the_move_lands() {
        let (dir, path) = file("quarantine_pending");
        let squatter = dir.join("quarantine");
        std::fs::write(&squatter, b"not a directory").unwrap();
        let metrics = Metrics::new().0;
        let now = Instant::now();

        let mut l = ledger();
        l.record_progress(&path, 3);
        l.note_quarantine_pending(&path, "test: unreadable tail");
        assert_eq!(l.admit(&path, now), Admit::Drop("quarantine_pending"));
        assert!(l.excludes(&path, now));
        assert_eq!(l.counts().quarantine_pending.load(Ordering::Relaxed), 1);
        assert_eq!(
            l.counts().resume_cursors.load(Ordering::Relaxed),
            0,
            "a segment awaiting quarantine is never resumed, so its cursor goes"
        );

        l.retry_dispositions(&dir, &metrics, &super::super::DrainHeartbeat::new());
        assert_eq!(
            l.counts().quarantine_pending.load(Ordering::Relaxed),
            1,
            "still failing: still owned"
        );
        assert!(path.exists());
        assert_eq!(metrics.recovery_segments_quarantined.get(), 0);

        std::fs::remove_file(&squatter).unwrap();
        l.retry_dispositions(&dir, &metrics, &super::super::DrainHeartbeat::new());
        assert_eq!(l.counts().quarantine_pending.load(Ordering::Relaxed), 0);
        assert!(!path.exists(), "the retry moved the segment");
        assert!(squatter.is_dir());
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);

        // ENOENT counts as done: nothing is left to preserve, nothing is counted.
        let gone = dir.join("seg_00000002.wab.sealed");
        l.note_quarantine_pending(&gone, "test: vanished");
        l.retry_dispositions(&dir, &metrics, &super::super::DrainHeartbeat::new());
        assert_eq!(l.counts().quarantine_pending.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.recovery_segments_quarantined.get(), 1);

        std::fs::remove_dir_all(dir).ok();
    }

    fn carry(through: u64, payloads: &[&'static [u8]]) -> DeadLetterCarry {
        let payloads: Vec<Payload> = payloads.iter().map(|p| Payload::from_static(p)).collect();
        DeadLetterCarry {
            through,
            bytes: payloads.iter().map(|p| p.len() as u64).sum(),
            committed: 1,
            payloads,
        }
    }

    /// The dead-letter carry (spec §4.5, §4.11): stored per segment, pinned to
    /// the file it was measured on, mirrored in `dl_carry_bytes`, taken (not
    /// copied) when the segment is next processed, and dropped with the rest
    /// of the segment's progress.
    #[test]
    fn a_dl_carry_is_pinned_to_its_file_and_taken_once() {
        let (dir, path) = file("dl_carry");
        let mut l = ledger();
        assert!(l.set_dl_carry(&path, carry(3, &[b"bad1", b"bad22"])));
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 9);

        let taken = l.take_dl_carry(&path).expect("the carry is there");
        assert_eq!(taken.through, 3);
        assert_eq!(taken.payloads.len(), 2);
        assert_eq!(taken.committed, 1);
        assert!(l.take_dl_carry(&path).is_none(), "taken, not copied");
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 0);

        // Confirmed, quarantined or parked: the carry goes with the cursor.
        assert!(l.set_dl_carry(&path, carry(3, &[b"bad"])));
        l.forget(&path);
        assert!(l.take_dl_carry(&path).is_none());
        assert!(l.set_dl_carry(&path, carry(3, &[b"bad"])));
        l.park(&path, ParkReason::DrainPanic);
        assert!(l.take_dl_carry(&path).is_none());
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 0);

        // A carry measured on a file that has since been replaced describes
        // records of a different segment: it is discarded, never written.
        let mut l = ledger();
        assert!(l.set_dl_carry(&path, carry(3, &[b"bad"])));
        let tmp = dir.join("replacement.tmp");
        std::fs::write(&tmp, b"a different segment").unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        assert!(l.take_dl_carry(&path).is_none());
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 0);

        // No file, no identity: nothing to pin the carry to, so it is not kept.
        std::fs::remove_file(&path).unwrap();
        assert!(!l.set_dl_carry(&path, carry(3, &[b"bad"])));
        std::fs::remove_dir_all(dir).ok();
    }

    /// The memory bound: the sum of all carries stays at or under the cap. A
    /// carry that would push it over is refused (the caller falls back to
    /// re-committing that sub-batch once), and refusing it leaves the others.
    #[test]
    fn dl_carries_are_bounded_in_total() {
        let (dir, path) = file("dl_carry_cap");
        let other = dir.join("seg_00000002.wab.sealed");
        std::fs::write(&other, b"sealed bytes").unwrap();
        assert_eq!(DL_CARRY_MAX_BYTES, 64 * 1024 * 1024);

        let mut l = ledger().with_dl_carry_max_bytes(10);
        assert!(l.set_dl_carry(&path, carry(1, &[b"123456"])));
        assert!(
            !l.set_dl_carry(&other, carry(1, &[b"12345"])),
            "6 + 5 > 10: refused"
        );
        assert!(l.take_dl_carry(&other).is_none());
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 6);
        assert!(l.set_dl_carry(&other, carry(1, &[b"1234"])), "6 + 4 fits");
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 10);

        // Replacing a segment's own carry does not count the old one twice.
        assert!(l.set_dl_carry(&path, carry(2, &[b"654321"])));
        assert_eq!(l.counts().dl_carry_bytes.load(Ordering::Relaxed), 10);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The drain-panic bookkeeping (spec §4.12): a panic is charged to the
    /// segment in flight, the second one parks it, a panic outside any attempt
    /// blames nothing, and a returned attempt clears the in-flight mark.
    #[test]
    fn a_segment_in_flight_for_two_drain_panics_is_parked() {
        let (dir, path) = file("drain_panic");
        let now = Instant::now();
        let mut l = ledger();
        assert_eq!(DRAIN_PANIC_PARK_LIMIT, 2);

        // Nothing in flight: nothing to blame.
        assert_eq!(l.note_drain_panic(), None);

        // An attempt that returned clears the mark.
        l.begin_attempt(&path);
        l.end_attempt();
        assert_eq!(l.note_drain_panic(), None);

        // First panic: counted, not parked, cursor kept, still admissible.
        l.record_progress(&path, 4);
        l.begin_attempt(&path);
        assert_eq!(l.note_drain_panic(), Some((path.clone(), 1)));
        assert_eq!(l.admit(&path, now), Admit::Process { skip: 4 });
        assert_eq!(
            l.note_drain_panic(),
            None,
            "the in-flight mark is consumed by the panic it is charged to"
        );

        // Second panic on the same segment: parked as drain_panic.
        l.begin_attempt(&path);
        assert_eq!(l.note_drain_panic(), Some((path.clone(), 2)));
        assert_eq!(l.admit(&path, now), Admit::Drop("parked"));
        assert!(l.excludes(&path, now));
        assert_eq!(
            l.counts().parked_by_reason[ParkReason::DrainPanic.index()].load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            l.counts().resume_cursors.load(Ordering::Relaxed),
            0,
            "a parked segment replays from record 0 on the next start"
        );

        // A segment that is confirmed between panics starts its count over.
        let other = dir.join("seg_00000002.wab.sealed");
        std::fs::write(&other, b"sealed bytes").unwrap();
        l.begin_attempt(&other);
        assert_eq!(l.note_drain_panic(), Some((other.clone(), 1)));
        l.forget(&other);
        l.begin_attempt(&other);
        assert_eq!(l.note_drain_panic(), Some((other.clone(), 1)));
        assert_eq!(l.admit(&other, now), Admit::Process { skip: 0 });

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn process_class_read_failures_never_count() {
        let (dir, path) = file("lrf_count");
        let mut l = ledger();
        assert_eq!(l.note_local_read_failure(&path, ErrClass::Process), 0);
        assert_eq!(l.note_local_read_failure(&path, ErrClass::Media), 1);
        assert_eq!(l.note_local_read_failure(&path, ErrClass::Access), 2);
        assert_eq!(l.note_local_read_failure(&path, ErrClass::Process), 2);
        l.forget(&path);
        assert_eq!(l.note_local_read_failure(&path, ErrClass::Media), 1);
        std::fs::remove_dir_all(dir).ok();
    }
}
