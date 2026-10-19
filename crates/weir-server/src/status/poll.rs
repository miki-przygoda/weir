//! The status poll tasks (spec §5.11), moved out of main.rs unchanged: the
//! 5 s WAB-size scan feeding the `wab_max_bytes` cap and the growth warning,
//! and the drain-heartbeat stall check with its busy gauge. Later sections
//! of this file add the cap refresh, the lag scan, the counter windows, the
//! evaluation and the 500 ms queue-depth task.
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::drain::{self, DrainHeartbeat, drain_appears_stalled};
use crate::metrics::{
    ConditionLabel, Metrics, ShardLabel, SinkFailureLabel, SinkFailureValue, WorkerLabel,
};
use crate::models::WorkUnit;
use crate::queue::QueueSender;
use crate::status::board::LagReport;
use crate::status::{StatusBoard, unix_ms_now};
use weir_status::{ConditionCode, Evaluation, Evidence, RecentCounters, Severity, ShardState};
use weir_wab::format::{
    EXT_ACTIVE, EXT_SEALED, SEGMENT_HEADER_LEN, confirmed_path_for, parse_segment_header,
};

/// Total live shard-segment bytes, or `None` if the WAB could not be scanned.
///
/// `None` is not "empty" — it is "we do not know", and the two must never
/// collapse. This value feeds the atomic that the `wab_max_bytes` cap is checked
/// against, so a scan error reported as `0` reads as an empty WAB and lifts the
/// cap entirely until the next tick. `apply_wab_scan` already refuses to do that
/// for a join failure (see its doc comment, which names this as "failing open on
/// the one feature whose job is to fail closed"); returning `Option` here closes
/// the same hole on the path that produces the number.
///
/// Any error makes the whole answer untrustworthy, including a per-shard one and
/// a single dirent the directory iterator failed to return: an unreadable shard
/// or entry is an undercount, and an undercount lifts the cap by exactly the
/// bytes we failed to see. The single exception is a file that vanished
/// between `read_dir` and `metadata` — the drain confirms and deletes segments
/// continuously, so that race is routine and the file genuinely occupies nothing.
pub(crate) fn compute_wab_bytes_on_disk(wab_dir: &Path) -> Option<u64> {
    let Ok(dir) = std::fs::read_dir(wab_dir) else {
        return None;
    };
    let mut total = 0u64;
    for entry in dir {
        // Matched, never `.flatten()`ed: a dirent error is an entry we could not
        // see, which is an undercount (spec §4.9), not an absence.
        let Ok(entry) = dirent_fault_hook(wab_dir, entry) else {
            return None;
        };
        let shard_path = entry.path();
        if !shard_path.is_dir() {
            continue;
        }
        // Skip the daemon's reserved subdirs — they aren't shards. dead_letter/
        // (dl_*.wab.sealed) has its own weir_dead_letter_bytes_on_disk gauge, and
        // quarantine/ holds forensic copies (both `.wab` and `.wab.sealed`,
        // depending on whether crash recovery or the drain preserved them; see
        // docs/wab_format.md); counting either here would double-count
        // live-segment bytes (G15, mirrors recovery's skip).
        let dir_name = shard_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if dir_name == "dead_letter" || dir_name == "quarantine" {
            continue;
        }
        let Ok(shard_dir) = std::fs::read_dir(&shard_path) else {
            return None;
        };
        for file in shard_dir {
            let Ok(file) = dirent_fault_hook(&shard_path, file) else {
                return None;
            };
            let fpath = file.path();
            let name = fpath.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".wab") || name.ends_with(".wab.sealed") {
                match std::fs::metadata(&fpath) {
                    Ok(m) => total += m.len(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => return None,
                }
            }
        }
    }
    Some(total)
}

/// The `cfg(test)` dirent fault hook: a real `readdir` error cannot be provoked
/// from a test, so a test arms a directory and every entry read from it comes
/// back as an I/O error.
#[cfg(test)]
fn dirent_fault_hook(
    dir: &Path,
    entry: std::io::Result<std::fs::DirEntry>,
) -> std::io::Result<std::fs::DirEntry> {
    dirent_fault::apply(dir, entry)
}

#[cfg(not(test))]
#[inline(always)]
fn dirent_fault_hook(
    _dir: &Path,
    entry: std::io::Result<std::fs::DirEntry>,
) -> std::io::Result<std::fs::DirEntry> {
    entry
}

#[cfg(test)]
mod dirent_fault {
    use std::{
        cell::RefCell,
        io,
        path::{Path, PathBuf},
    };

    thread_local! {
        static ARMED: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// Fails every dirent read from `dir` on this thread until [`disarm`].
    pub(super) fn arm(dir: &Path) {
        ARMED.with(|a| *a.borrow_mut() = Some(dir.to_path_buf()));
    }

    pub(super) fn disarm() {
        ARMED.with(|a| *a.borrow_mut() = None);
    }

    pub(super) fn apply(
        dir: &Path,
        entry: io::Result<std::fs::DirEntry>,
    ) -> io::Result<std::fs::DirEntry> {
        ARMED.with(|a| {
            if a.borrow().as_deref() == Some(dir) {
                Err(io::Error::other("injected dirent error"))
            } else {
                entry
            }
        })
    }
}

/// Number of 5 s samples the growth warning considers (60 s).
const WAB_GROWTH_WINDOW: usize = 12;

/// How long the WAB growth warning stays quiet after firing. The polling task
/// evaluates the warning every 5 s, so without this it would log 60 times a
/// minute for as long as the condition held.
const WAB_GROWTH_WARN_INTERVAL: Duration = Duration::from_secs(300);

/// Consecutive 5 s polls — beyond the grace period in
/// [`drain_heartbeat_looks_wedged`] — with no movement on
/// [`drain::DrainHeartbeat`] before the drain is treated as possibly wedged.
/// 3 polls = 15 s.
const DRAIN_STALL_CONSECUTIVE_POLLS: u32 = 3;

/// Whether an unchanged drain heartbeat has been frozen long enough to call
/// it a likely wedge rather than ordinary idling.
///
/// A perfectly healthy but IDLE drain (nothing queued) only re-enters its own
/// loop — and so only bumps [`drain::DrainHeartbeat`] — once per
/// `health_poll_interval` (operator-tunable; 30 s by default): its idle wait
/// is bounded by that interval, and nothing else wakes it. Applying the
/// brief 3-poll/15 s rule from the moment the heartbeat froze would therefore
/// fire on every idle window past 15 s at the default config — indistinguishable,
/// to a poll-counter alone, from a genuine wedge (which just never recovers).
/// So the clock only starts once `health_poll_interval` has already elapsed
/// with no bump — the longest a healthy idle drain should ever go quiet — and
/// [`DRAIN_STALL_CONSECUTIVE_POLLS`] more 5 s polls beyond THAT must also show
/// no movement. A real wedge stays frozen forever and still trips this; a
/// merely-idle drain resumes on its own cadence and never does.
fn drain_heartbeat_looks_wedged(frozen_for: Duration, health_poll_interval: Duration) -> bool {
    frozen_for >= health_poll_interval + Duration::from_secs(5) * DRAIN_STALL_CONSECUTIVE_POLLS
}

/// How long past `commit_timeout` a sink call may stay busy before the stall
/// verdict calls it a wedge (spec §4.8). A sink that yields is cancelled by
/// `commit_timeout`; one still busy this long after it is blocking the drain's
/// OS thread, where no timeout can fire. Matches the 3-poll/15 s grace of
/// [`drain_heartbeat_looks_wedged`].
const SINK_CALL_GRACE: Duration = Duration::from_secs(15);

/// What a frozen drain heartbeat actually means. A freeze past the threshold is
/// only part of the diagnosis — the `drain_state` gauge and the heartbeat's
/// busy signal ([`drain::DrainHeartbeat::busy`]) supply the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainStallVerdict {
    /// The drain thread is GONE: the supervisor exhausted its respawn budget
    /// (or the daemon is shutting down) and published `drain_state{stopped}` on
    /// its way out. The heartbeat is frozen because nobody is left to bump it.
    /// Nothing is wedged; delivery has ended and only a restart resumes it.
    Stopped,
    /// The drain still claims a live state, yet cannot reach its own loop head:
    /// either nothing is marked busy (the thread is stuck somewhere no guard
    /// covers), or a sink call has stayed busy past `commit_timeout` +
    /// [`SINK_CALL_GRACE`] — a sink blocking the drain's OS thread instead of
    /// yielding, which no timeout inside the drain can fire against.
    Wedged,
    /// Inside a sink call (commit or health probe) that has not yet outlived
    /// `commit_timeout` + [`SINK_CALL_GRACE`]: a slow sink, which the timeout
    /// still bounds. Logged at INFO with no restart advice.
    SlowSinkCall {
        kind: drain::BusyKind,
        busy_for: Duration,
    },
    /// Inside local I/O on the WAB device (a segment read, an fsync, a
    /// dead-letter write, a quarantine move, a directory scan), for however
    /// long. A slow disk: a restart re-delivers from record 0 and leaves the
    /// disk exactly as slow, so this is never a wedge.
    SlowLocalIo {
        kind: drain::BusyKind,
        busy_for: Duration,
    },
}

/// Composes the freeze duration, the live `drain_state` gauge and the busy
/// signal into the one diagnosis the operator is shown (spec §4.8):
///
/// | condition | verdict |
/// |---|---|
/// | `drain_state{stopped}` | `Stopped` |
/// | busy in a sink call for ≤ `commit_timeout + 15 s` | `SlowSinkCall` |
/// | busy in local I/O, any duration | `SlowLocalIo` |
/// | busy in a sink call for > `commit_timeout + 15 s` | `Wedged` |
/// | nothing busy | `Wedged` |
///
/// Split out — like [`drain_is_healthy`] and [`should_warn_wab_growth`] before
/// it — because the composition, not the duration predicate alone, is where the
/// bugs were: the warning fired the "wedged inside a sink call" text on a drain
/// whose own gauge said `stopped` (the dead-letter-open panic exhausts the
/// respawn ladder before the first `heartbeat.bump()` ever runs), and on a
/// drain that was merely inside one slow commit or one slow fsync, advising a
/// restart that re-delivers from record 0 and fixes neither.
///
/// Returns `None` while the freeze is still inside the grace period.
// pub(crate): the drain module's stall tests call it (it was crate-root
// private in main.rs, which every module could already reach).
pub(crate) fn drain_stall_verdict(
    frozen_for: Duration,
    health_poll_interval: Duration,
    drain_stopped: bool,
    busy: Option<(drain::BusyKind, Duration)>,
    commit_timeout: Duration,
) -> Option<DrainStallVerdict> {
    classify_drain_stall(
        drain_heartbeat_looks_wedged(frozen_for, health_poll_interval),
        drain_stopped,
        busy,
        commit_timeout + SINK_CALL_GRACE,
    )
}

/// [`drain_stall_verdict`] with its two thresholds already applied, so a test
/// can scale them below the production seconds: `frozen_past_threshold` is
/// [`drain_heartbeat_looks_wedged`]'s answer, and `sink_call_limit` how long a
/// sink call may stay busy before it reads as a wedge.
// pub(crate): the drain module's stall tests call it (it was crate-root
// private in main.rs, which every module could already reach).
pub(crate) fn classify_drain_stall(
    frozen_past_threshold: bool,
    drain_stopped: bool,
    busy: Option<(drain::BusyKind, Duration)>,
    sink_call_limit: Duration,
) -> Option<DrainStallVerdict> {
    if !frozen_past_threshold {
        return None;
    }
    if drain_stopped {
        return Some(DrainStallVerdict::Stopped);
    }
    Some(match busy {
        Some((kind, busy_for)) if kind.is_sink_call() && busy_for <= sink_call_limit => {
            DrainStallVerdict::SlowSinkCall { kind, busy_for }
        }
        Some((kind, busy_for)) if !kind.is_sink_call() => {
            DrainStallVerdict::SlowLocalIo { kind, busy_for }
        }
        _ => DrainStallVerdict::Wedged,
    })
}

/// Publishes `weir_drain_busy_seconds{kind}` from one reading of the busy
/// signal: the current kind carries its duration, every other kind 0, and all
/// are 0 while the drain is between operations.
fn publish_drain_busy(
    metrics: &crate::metrics::Metrics,
    busy: Option<(drain::BusyKind, Duration)>,
) {
    let current = busy.map(|(kind, busy_for)| (kind.metric_label(), busy_for.as_secs_f64()));
    for kind in crate::metrics::DrainBusyKind::ALL {
        let secs = match current {
            Some((k, secs)) if k == kind => secs,
            _ => 0.0,
        };
        metrics
            .drain_busy_seconds
            .get_or_create(&crate::metrics::DrainBusyLabel { kind })
            .set(secs);
    }
}

/// Logs one stall verdict. Only `Stopped` and `Wedged` carry restart advice;
/// a slow sink call is INFO, and slow local I/O a WARN that points at the disk.
fn log_drain_stall(verdict: DrainStallVerdict, frozen_for: Duration) {
    let frozen_secs = frozen_for.as_secs();
    match verdict {
        DrainStallVerdict::Wedged => warn!(
            frozen_secs,
            "drain heartbeat has not advanced in over {frozen_secs}s — the \
             drain thread may be wedged inside a sink call that blocks its \
             OS thread (a synchronous DB driver, std::fs, or a spin loop) \
             rather than yielding. That defeats both commit_timeout and \
             health_probe_timeout, which can only fire when the awaited \
             future actually yields on the drain's current-thread runtime. \
             drain_state still reports a live state, so this is a wedge \
             rather than a stopped drain; if delivery has genuinely \
             stalled, an operator restart is the only recovery."
        ),
        DrainStallVerdict::Stopped => warn!(
            frozen_secs,
            "drain heartbeat has not advanced in over {frozen_secs}s and \
             drain_state reports `stopped` — the drain thread is GONE, not \
             wedged. Either the supervisor exhausted its respawn budget \
             (look for the earlier \"drain thread panicked too many \
             times\" ERROR) or the daemon is shutting down. Delivery has \
             ended and the WAB will keep growing on disk; only a process \
             restart resumes it. Do NOT go looking for a blocked sink \
             call — there is no drain thread left to block."
        ),
        DrainStallVerdict::SlowSinkCall { kind, busy_for } => info!(
            frozen_secs,
            busy_kind = ?kind,
            busy_secs = busy_for.as_secs(),
            "drain is inside a slow sink call ({kind:?}, {}s so far); it is still within \
             commit_timeout plus grace, so the drain is waiting on the sink, not wedged. No \
             action needed unless it persists — check the sink's latency",
            busy_for.as_secs()
        ),
        DrainStallVerdict::SlowLocalIo { kind, busy_for } => warn!(
            frozen_secs,
            busy_kind = ?kind,
            busy_secs = busy_for.as_secs(),
            "drain has been inside one local I/O operation ({kind:?}) for {}s — the WAB \
             device is slow or stalled. This is not a wedged drain and a restart will not \
             help: it re-delivers from record 0 and leaves the disk exactly as slow. Check \
             the WAB device (I/O latency, a failing or saturated disk, a stalled network \
             filesystem)",
            busy_for.as_secs()
        ),
    }
}

/// Whether the drain supervisor has published its terminal `stopped` state.
///
/// Reads the same one-hot `weir_drain_state` family `drain::set_drain_state`
/// writes, exactly as [`drain_is_healthy`] reads `draining`.
fn drain_is_stopped(metrics: &crate::metrics::Metrics) -> bool {
    metrics
        .drain_state
        .get_or_create(&crate::metrics::DrainStateLabel {
            state: crate::metrics::DrainStateValue::stopped,
        })
        .get()
        > 0.0
}

/// Whether the growth warning is allowed to fire again at `now`.
///
/// Extracted from the polling task's closure for the same reason
/// [`should_warn_wab_growth`] and [`drain_is_healthy`] were: it is the third
/// piece of the warning's decision and the only one that was left inline, so it
/// was the only one no test could reach. `saturating_duration_since` rather than
/// `elapsed`/subtraction so a `now` behind `last_warned` reads as "not yet due"
/// instead of panicking.
fn growth_warning_is_due(last_warned: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    match last_warned {
        // Never warned: the first qualifying window fires immediately.
        None => true,
        Some(t) => now.saturating_duration_since(t) >= WAB_GROWTH_WARN_INTERVAL,
    }
}

impl DrainStallVerdict {
    /// How urgent the verdict is, for the log rate limit in
    /// [`stall_log_is_due`]. `Stopped` ranks above `Wedged` because it carries
    /// the opposite advice (there is no sink call to hunt), so a wedge that
    /// turns into a dead drain is logged at once too.
    fn severity(self) -> u8 {
        match self {
            Self::SlowSinkCall { .. } => 0,
            Self::SlowLocalIo { .. } => 1,
            Self::Wedged => 2,
            Self::Stopped => 3,
        }
    }
}

/// Whether a stall verdict may be logged at `now`, given the last one logged.
///
/// A repeat, or a milder verdict, waits out [`WAB_GROWTH_WARN_INTERVAL`] like
/// any other periodic warning. An ESCALATION is logged at once: a sink that
/// blocks the drain's OS thread always reads first as a slow sink call (an INFO
/// that says no action is needed) and only becomes `Wedged` once it outlives
/// `commit_timeout` + [`SINK_CALL_GRACE`] — about 35 s later at defaults. With
/// one shared rate limit, that INFO held the wedge WARN back for five minutes,
/// in exactly the case the stall check exists for.
fn stall_log_is_due(
    last_logged: Option<(std::time::Instant, DrainStallVerdict)>,
    verdict: DrainStallVerdict,
    now: std::time::Instant,
) -> bool {
    match last_logged {
        None => true,
        Some((at, last)) => {
            verdict.severity() > last.severity() || growth_warning_is_due(Some(at), now)
        }
    }
}

/// The heartbeat-stall check's state across 5 s polls: the last heartbeat
/// seen, when it last moved, and the last verdict logged (for
/// [`stall_log_is_due`]).
pub(crate) struct DrainLiveness {
    health_poll_interval: Duration,
    commit_timeout: Duration,
    last_heartbeat: Option<u64>,
    heartbeat_changed_at: std::time::Instant,
    last_logged: Option<(std::time::Instant, DrainStallVerdict)>,
}

impl DrainLiveness {
    pub(crate) fn new(
        now: std::time::Instant,
        health_poll_interval: Duration,
        commit_timeout: Duration,
    ) -> Self {
        Self {
            health_poll_interval,
            commit_timeout,
            last_heartbeat: None,
            heartbeat_changed_at: now,
            last_logged: None,
        }
    }

    /// How long the heartbeat has been frozen as of `now` (zero right after
    /// it moved). The status steps publish it as `heartbeat_changed_ms`.
    pub(crate) fn frozen_for(&self, now: std::time::Instant) -> Duration {
        now.saturating_duration_since(self.heartbeat_changed_at)
    }

    /// Folds one poll's reading in. Returns the verdict to log on this poll,
    /// with how long the heartbeat has been frozen, or `None` when the drain
    /// is moving, still inside the grace period, or rate-limited.
    fn observe(
        &mut self,
        now: std::time::Instant,
        heartbeat: u64,
        drain_stopped: bool,
        busy: Option<(drain::BusyKind, Duration)>,
    ) -> Option<(DrainStallVerdict, Duration)> {
        match self.last_heartbeat.replace(heartbeat) {
            Some(prev) if drain_appears_stalled(prev, heartbeat) => {
                let frozen_for = now.saturating_duration_since(self.heartbeat_changed_at);
                let verdict = drain_stall_verdict(
                    frozen_for,
                    self.health_poll_interval,
                    drain_stopped,
                    busy,
                    self.commit_timeout,
                )?;
                if !stall_log_is_due(self.last_logged, verdict, now) {
                    return None;
                }
                self.last_logged = Some((now, verdict));
                Some((verdict, frozen_for))
            }
            _ => {
                self.heartbeat_changed_at = now;
                None
            }
        }
    }
}

/// The 5 s polling task's state: the WAB-size scan feeding the
/// `wab_max_bytes` cap and the growth warning, and the drain-heartbeat stall
/// check with its busy gauge.
pub(crate) struct WabPoll {
    metrics: Arc<crate::metrics::Metrics>,
    wab_bytes_now: Arc<std::sync::atomic::AtomicU64>,
    wab_max_bytes: u64,
    heartbeat: DrainHeartbeat,
    liveness: DrainLiveness,
    /// Rolling window of on-disk-byte samples feeding `should_warn_wab_growth`.
    samples: Vec<u64>,
    /// The last time the growth warning fired (rate-limited).
    last_growth_warned: Option<std::time::Instant>,
    /// The status half of the task (spec §5.11). `None` in the unit tests
    /// that exercise the warning logic alone.
    status: Option<PollStatus>,
}

impl WabPoll {
    pub(crate) fn new(
        metrics: Arc<crate::metrics::Metrics>,
        wab_bytes_now: Arc<std::sync::atomic::AtomicU64>,
        wab_max_bytes: u64,
        heartbeat: DrainHeartbeat,
        liveness: DrainLiveness,
    ) -> Self {
        Self {
            metrics,
            wab_bytes_now,
            wab_max_bytes,
            heartbeat,
            liveness,
            samples: Vec::with_capacity(WAB_GROWTH_WINDOW + 1),
            last_growth_warned: None,
            status: None,
        }
    }

    /// Attaches the status half: the board the scan stamps, the cap flag and
    /// the growth verdict are published on.
    #[cfg_attr(not(test), allow(dead_code))] // Task 1.23 wires the 5 s task
    pub(crate) fn with_status(mut self, status: PollStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// One tick. Returns the stall verdict logged on it, if any.
    ///
    /// The drain-liveness check runs FIRST, before the WAB scan can end the
    /// tick: a failing scan (an unreadable shard, a dirent error) must not
    /// silence the one signal that says whether the drain is alive (spec
    /// §4.8). `scan` is a closure rather than a started task so it begins only
    /// where it is awaited, and so a test can hand in a failing one.
    pub(crate) async fn tick<F, Fut>(
        &mut self,
        now: std::time::Instant,
        scan: F,
    ) -> Option<DrainStallVerdict>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<u64>, tokio::task::JoinError>>,
    {
        let logged = self.check_drain_liveness(now);

        // On a failed scan the last known size stands, the cap flag is left
        // as it is (fail-closed) and this tick contributes no growth sample;
        // see `apply_wab_scan` for why neither 0 nor a repeated value is safe
        // here. The status steps below still run.
        if let Some(bytes) = apply_wab_scan(
            scan().await,
            &mut self.samples,
            &self.metrics,
            &self.wab_bytes_now,
        ) {
            if let Some(st) = &self.status {
                st.board.wab.note_scan_ok(unix_ms_now());
                // Spec §5.5: the refresh that clears the flag without traffic.
                let rejecting = crate::wab::cap::evaluate(
                    self.wab_max_bytes,
                    bytes,
                    &st.board.wab_cap_rejecting,
                );
                self.metrics.wab_cap_rejecting.set(flag(rejecting));
            }
            let drain_healthy = drain_is_healthy(&self.metrics);
            let growing = should_warn_wab_growth(&self.samples, self.wab_max_bytes, drain_healthy);
            if let Some(st) = &self.status {
                st.board.wab.set_growth(growing);
            }
            if growing && growth_warning_is_due(self.last_growth_warned, std::time::Instant::now())
            {
                warn!(
                    wab_bytes = bytes,
                    growth_bytes = bytes.saturating_sub(self.samples[0]),
                    window_secs = WAB_GROWTH_WINDOW * 5,
                    "WAB is growing while the sink is not healthy and no wab_max_bytes \
                     is set — producers are still being acked into an unbounded buffer. \
                     Set wab_max_bytes to bound it."
                );
                self.last_growth_warned = Some(std::time::Instant::now());
            }
        }
        // Status steps (spec §5.11 3-6). They run on every tick, a failed size
        // scan included: the lag scan and the counters are independent of it.
        if let Some(mut st) = self.status.take() {
            let now_ms = unix_ms_now();
            let frozen = self.liveness.frozen_for(now);
            st.board.heartbeat_changed_ms.store(
                now_ms
                    .saturating_sub(u64::try_from(frozen.as_millis()).unwrap_or(u64::MAX))
                    .max(1),
                Ordering::Relaxed,
            );
            // The busy signal, read after the size scan so the stamp is current; the
            // same reading `check_drain_liveness` classifies, one tick later at worst.
            match self.heartbeat.busy() {
                Some((kind, busy_for)) => {
                    st.board
                        .drain_busy_sink_call
                        .store(kind.is_sink_call(), Ordering::Relaxed);
                    let since = now_ms
                        .saturating_sub(u64::try_from(busy_for.as_millis()).unwrap_or(u64::MAX))
                        .max(1);
                    st.board.drain_busy_since_ms.store(since, Ordering::Release);
                }
                None => st.board.drain_busy_since_ms.store(0, Ordering::Release),
            }
            self.metrics
                .drain_heartbeat_age_seconds
                .set(frozen.as_secs_f64());
            st.refresh_lag(now_ms).await;
            st.refresh_recent(now, &self.metrics);
            st.publish(now_ms, &self.metrics);
            self.status = Some(st);
        }
        logged
    }

    /// Drain-heartbeat stall check, and the busy gauge.
    ///
    /// Deliberately independent of `drain_is_healthy` — a wedged drain (a sink
    /// call that blocks the drain's OS thread instead of yielding) is exactly
    /// the state where `drain_state` / `sink_health` stay frozen at their last
    /// (healthy-looking) reading, which is what silences the WAB-growth
    /// warning. This check reads the heartbeat directly instead, so it isn't
    /// fooled by the same stale gauges. `drain_state` is still consulted for ONE
    /// thing — the `stopped` terminal state, the one value the supervisor
    /// writes that is never stale — because a dead drain freezes the heartbeat
    /// exactly like a wedged one and needs the opposite advice. The busy signal
    /// tells a wedge from a drain that is merely inside one slow sink call or
    /// one slow disk operation. See `drain_stall_verdict`.
    fn check_drain_liveness(&mut self, now: std::time::Instant) -> Option<DrainStallVerdict> {
        let busy = self.heartbeat.busy();
        publish_drain_busy(&self.metrics, busy);
        let (verdict, frozen_for) = self.liveness.observe(
            now,
            self.heartbeat.get(),
            drain_is_stopped(&self.metrics),
            busy,
        )?;
        log_drain_stall(verdict, frozen_for);
        Some(verdict)
    }
}

/// Fold one WAB-size scan into the polling task's state, returning the size on
/// success and `None` when the scan failed to join.
///
/// The `Err` arm lives in here rather than at the call site on purpose. The
/// defect this replaced was `.await.unwrap_or(0)`, which is a call-site
/// substitution — a test of a function that already takes `Option<u64>` would
/// pass with that bug fully reintroduced. Taking the `JoinResult` itself is what
/// makes the test load-bearing.
///
/// A join failure must not be papered over in either direction. Storing 0 reads
/// as "the WAB is empty" and lifts the cap entirely until the next tick, failing
/// open on the one feature whose job is to fail closed; pushing the previous
/// value again reads as "not growing" and suppresses the growth warning for a
/// whole window. So neither the gauge, the shared atomic, nor the window moves —
/// the last known size stands, 5 s stale, which is exactly the staleness the cap
/// is already documented to carry.
fn apply_wab_scan(
    scan: Result<Option<u64>, tokio::task::JoinError>,
    samples: &mut Vec<u64>,
    metrics: &crate::metrics::Metrics,
    wab_bytes_now: &std::sync::atomic::AtomicU64,
) -> Option<u64> {
    let bytes = match scan {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            warn!(
                "WAB byte scan could not read the WAB directory; keeping the last \
                 known size. The wab_max_bytes cap and weir_wab_bytes_on_disk are \
                 stale until the next tick."
            );
            return None;
        }
        Err(e) => {
            warn!(
                error = %e,
                "WAB byte scan failed to join; keeping the last known size. The \
                 wab_max_bytes cap and weir_wab_bytes_on_disk are stale until the \
                 next tick."
            );
            return None;
        }
    };
    metrics.wab_bytes_on_disk.set(bytes as f64);
    wab_bytes_now.store(bytes, std::sync::atomic::Ordering::Relaxed);
    samples.push(bytes);
    if samples.len() > WAB_GROWTH_WINDOW {
        samples.remove(0);
    }
    Some(bytes)
}

/// Whether to warn that the WAB is growing unbounded.
///
/// All three must hold:
/// 1. no cap is set — with one set, the cap handles it and the warning is noise;
/// 2. the window is full and shows net growth that never gave ground;
/// 3. the sink is unhealthy or the drain is not draining.
///
/// Condition 3 is the one that matters. Sustained growth under a healthy drain
/// is just a fast producer; warning on that teaches operators to ignore the
/// message, which defeats the purpose.
///
/// Condition 2 is deliberately NOT "every step was non-decreasing". A *degraded*
/// sink — one delivering some batches and failing others, which the spec names as
/// a case this must catch — deletes a confirmed segment every so often, and each
/// deletion drops the byte count for one sample. Under a strict per-step rule a
/// single such dip anywhere in the 12-sample window resets the whole detector, so
/// the warning would only ever fire for a *fully* dead sink. The rule here is
/// weaker per-step but just as strong end-to-end: the window must end above where
/// it started and must never have fallen below where it started. A WAB that is
/// genuinely draining dips under its own starting point and still does not warn.
fn should_warn_wab_growth(samples: &[u64], wab_max_bytes: u64, drain_healthy: bool) -> bool {
    if wab_max_bytes != 0 || drain_healthy || samples.len() < WAB_GROWTH_WINDOW {
        return false;
    }
    let window = &samples[samples.len() - WAB_GROWTH_WINDOW..];
    let first = window[0];
    let last = window[window.len() - 1];
    last > first && window.iter().all(|&s| s >= first)
}

/// Reads the live `weir_sink_health` and `weir_drain_state` gauges and
/// composes them into condition 3 of `should_warn_wab_growth`: is the drain
/// healthy right now.
///
/// Returns `true` only when the sink is healthy AND the drain is actively
/// draining. That `&&` is the whole point: condition 3 is "sink unhealthy OR
/// not draining", and by De Morgan that is exactly NOT(healthy AND draining).
/// The negation happens in the caller — `should_warn_wab_growth` takes this
/// value as `drain_healthy` and returns `false` early when it is `true`. So a
/// swapped `&&`/`||` here silently inverts which failures the warning covers.
///
/// Pulled out of the background task's closure (rather than left inline) so this
/// composition — not just the pure predicate — is unit-tested; a swapped
/// `&&`/`||` or a wrong label variant here would previously compile and pass
/// every existing test.
fn drain_is_healthy(metrics: &crate::metrics::Metrics) -> bool {
    // `sink_health` and `drain_state` are Gauge families with the current
    // state's series set to 1.0, so the live value reads back without any new
    // plumbing.
    let sink_healthy = metrics
        .sink_health
        .get_or_create(&crate::metrics::SinkHealthLabel {
            state: crate::metrics::SinkHealthState::healthy,
        })
        .get()
        > 0.0;
    let draining = metrics
        .drain_state
        .get_or_create(&crate::metrics::DrainStateLabel {
            state: crate::metrics::DrainStateValue::draining,
        })
        .get()
        > 0.0;
    sink_healthy && draining
}

/// The status half of the 5 s task (spec §5.11). `None` in the moved unit
/// tests, which exercise the warning logic alone.
pub(crate) struct PollStatus {
    board: Arc<StatusBoard>,
    wab_dir: PathBuf,
    thresholds: weir_status::Thresholds,
    coalesce_hint: Arc<AtomicU64>,
    /// `None` only while a lag scan holds it on the blocking pool.
    footer_cache: Option<FooterCache>,
    /// Whether the last lag scan failed: the WARN fires once per episode.
    lag_failing: bool,
    /// The sliding windows behind `RecentCounters`' 5 m / 15 m deltas.
    windows: RecentWindows,
    /// The conditions active at the last evaluation (transition logging).
    prev_active: BTreeSet<ConditionCode>,
    /// Spec §9.3 code 18 (`segments_stranded`): the stranded count at the last
    /// new sink success, the count at the previous tick, and the success stamp
    /// last seen.
    stranded_baseline: u64,
    stranded_prev: u64,
    seen_success_ms: Option<u64>,
}

#[cfg_attr(not(test), allow(dead_code))] // Task 1.23 wires the 5 s task
impl PollStatus {
    pub(crate) fn new(
        board: Arc<StatusBoard>,
        wab_dir: PathBuf,
        coalesce_hint: Arc<AtomicU64>,
    ) -> Self {
        PollStatus {
            board,
            wab_dir,
            thresholds: weir_status::Thresholds::DEFAULT,
            coalesce_hint,
            footer_cache: Some(FooterCache::default()),
            lag_failing: false,
            windows: RecentWindows::new(Instant::now()),
            prev_active: BTreeSet::new(),
            stranded_baseline: 0,
            stranded_prev: 0,
            seen_success_ms: None,
        }
    }

    /// Spec §5.11 step 5: counter deltas over their windows, and the
    /// absolute counters, onto the board.
    pub(crate) fn refresh_recent(&mut self, now: Instant, metrics: &Metrics) {
        // Strandings since the last sink success. When a new success shows up,
        // rebase on the count at the previous tick, so a strand inside this
        // window is never hidden (errs toward firing). `None` while the sink
        // cell is unpublished: the rule then stays unevaluated. PR 2 Task 2.2
        // (`Publisher::sink_success`) publishes the cell; nothing else is needed.
        let stranded_now = metrics.drain_segments_stranded.get();
        let stranded = self.board.sink.evidence().map(|s| {
            if s.last_success_unix_ms != self.seen_success_ms {
                self.seen_success_ms = s.last_success_unix_ms;
                self.stranded_baseline = self.stranded_prev;
            }
            stranded_now.saturating_sub(self.stranded_baseline)
        });
        self.stranded_prev = stranded_now;
        let w = &mut self.windows;
        let recent = RecentCounters {
            ack_timeouts_5m: Some(w.ack_timeouts.delta(
                now,
                metrics.ack_timeout.get(),
                Duration::from_secs(weir_status::ACK_TIMEOUT_WINDOW_SECS),
            )),
            fsync_failures_15m: Some(w.fsync_failures.delta(
                now,
                metrics.wab_fsync_failures.get(),
                Duration::from_secs(weir_status::FSYNC_WINDOW_SECS),
            )),
            flusher_panics_15m: Some(w.flusher_panics.delta(
                now,
                metrics.wab_flusher_panics.get(),
                Duration::from_secs(weir_status::PANIC_WINDOW_SECS),
            )),
            stranded_since_last_success: stranded,
            drain_panics_total: Some(metrics.drain_panics.get()),
            recovery_segments_failed: Some(metrics.recovery_segments_failed.get()),
            recovery_quarantine_copy_failed: Some(metrics.recovery_quarantine_copy_failed.get()),
            coalesce_ewma_us: Some(self.coalesce_hint.load(Ordering::Relaxed)),
        };
        self.board.recent.store(Arc::new(recent));
    }

    /// Spec §5.11 steps 6-7: evaluate, latch, publish, log transitions.
    pub(crate) fn publish(&mut self, now_ms: u64, metrics: &Metrics) -> Evaluation {
        let ev = self.board.evidence_at(now_ms);
        let eval = weir_status::evaluate(&ev, &self.thresholds, &self.board.latches);
        self.board.latches.update(&eval, now_ms);
        let ready = weir_status::readiness(&ev).ok;
        // PR 5 passes the configured livez_checks; none exist yet, so `[]`.
        let live = weir_status::liveness(&ev, &self.thresholds, &[]).ok;
        publish_gauges(metrics, &ev, &eval, ready, live);
        self.log_transitions(&eval);
        eval
    }

    /// Raising a warning or critical condition WARNs; raising an info one
    /// (e.g. `starting` on every boot) and every clear log at INFO.
    fn log_transitions(&mut self, eval: &Evaluation) {
        let now: BTreeSet<ConditionCode> = eval.conditions.iter().map(|c| c.code).collect();
        for c in eval
            .conditions
            .iter()
            .filter(|c| !self.prev_active.contains(&c.code))
        {
            if c.severity == Severity::Info {
                info!(
                    code = c.code.as_str(),
                    severity = "info",
                    "condition raised: {}",
                    c.summary
                );
            } else {
                warn!(
                    code = c.code.as_str(),
                    severity = ?c.severity,
                    "condition raised: {}",
                    c.summary
                );
            }
        }
        for code in self.prev_active.difference(&now) {
            info!(code = code.as_str(), "condition cleared");
        }
        self.prev_active = now;
    }

    /// Runs the lag scan off the runtime (`spawn_blocking`). The cache moves
    /// into the blocking task and back. A failure keeps the last report
    /// (stale, never zero) and WARNs once per failing episode.
    pub(crate) async fn refresh_lag(&mut self, now_ms: u64) {
        let mut cache = self.footer_cache.take().unwrap_or_default();
        let dir = self.wab_dir.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let result = scan_wab_lag(&dir, &mut cache, now_ms);
            (cache, result)
        })
        .await;
        match joined {
            Ok((cache, Ok(lag))) => {
                self.footer_cache = Some(cache);
                self.board.wab.set_lag(lag);
                self.lag_failing = false;
            }
            Ok((cache, Err(e))) => {
                self.footer_cache = Some(cache);
                if !self.lag_failing {
                    warn!(
                        error = %e,
                        "WAB lag scan failed; backlog figures stay at their last value \
                         until it succeeds"
                    );
                }
                self.lag_failing = true;
            }
            // Cancelled at runtime shutdown; the cache is rebuilt on the next scan.
            Err(_) => {}
        }
    }
}

/// The footer cache's bound (spec §20 (c)).
pub(crate) const FOOTER_CACHE_CAP: usize = 65_536;

#[derive(Clone, Copy)]
struct SealedMeta {
    records: u64,
    created_ms: Option<u64>,
}

/// Footer and header facts of sealed segments, keyed by path. A sealed
/// segment never changes, so one read per segment lifetime. Entries not seen
/// by a scan are dropped (the segment was delivered and deleted); new
/// entries stop being cached at [`FOOTER_CACHE_CAP`] (spec §20 (c)).
#[derive(Default)]
pub(crate) struct FooterCache {
    entries: HashMap<PathBuf, SealedMeta>,
}

impl FooterCache {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// `created_at` from a segment header, as unix ms. `None` for a header not
/// fully written yet (a just-created active segment) or one that does not
/// parse.
fn header_created_ms(path: &Path) -> io::Result<Option<u64>> {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    let mut f = std::fs::File::open(path)?;
    if f.read_exact(&mut buf).is_err() {
        return Ok(None);
    }
    Ok(parse_segment_header(&buf)
        .ok()
        .and_then(|h| u64::try_from(h.created_at).ok())
        .map(|ns| ns / 1_000_000))
}

/// Regular files and their total bytes directly inside `dir`. A file that
/// vanishes between `read_dir` and `metadata` is skipped.
fn dir_files_and_bytes(dir: &Path) -> io::Result<(u64, u64)> {
    let mut files = 0;
    let mut bytes = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        match entry.metadata() {
            Ok(m) if m.is_file() => {
                files += 1;
                bytes += m.len();
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok((files, bytes))
}

/// The read-only lag scan (spec §5.11 step 3). It never feeds `wab_bytes_now`
/// and never calls `check_confirmed` (whose quarantine side effects belong to
/// the drain); a sealed segment with a `.confirmed` sidecar beside it is
/// delivered and is skipped. Files vanishing mid-scan (the drain deletes
/// confirmed segments continuously) are tolerated.
pub(crate) fn scan_wab_lag(
    wab_dir: &Path,
    cache: &mut FooterCache,
    now_ms: u64,
) -> io::Result<LagReport> {
    let mut r = LagReport {
        scanned_at_ms: now_ms,
        ..LagReport::default()
    };
    let mut seen = HashSet::new();
    for entry in std::fs::read_dir(wab_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        match name.as_ref() {
            "dead_letter" => {
                (r.dead_letter_files, r.dead_letter_bytes) = dir_files_and_bytes(&path)?;
            }
            "quarantine" => {
                (r.quarantine_files, r.quarantine_bytes) = dir_files_and_bytes(&path)?;
            }
            n => {
                let Some(shard) = n.strip_prefix("shard_").and_then(|s| s.parse::<u16>().ok())
                else {
                    continue;
                };
                scan_shard(&path, shard, cache, &mut seen, &mut r)?;
            }
        }
    }
    cache.entries.retain(|p, _| seen.contains(p));
    Ok(r)
}

fn scan_shard(
    dir: &Path,
    shard: u16,
    cache: &mut FooterCache,
    seen: &mut HashSet<PathBuf>,
    r: &mut LagReport,
) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(file) = path.file_name().and_then(|f| f.to_str()) else {
            continue;
        };
        if file.ends_with(EXT_SEALED) {
            if confirmed_path_for(&path).exists() {
                continue; // delivered; the drain deletes it after its sidecar
            }
            let meta = match cache.entries.get(&path) {
                Some(m) => *m,
                None => {
                    let (records, footer_ok) = match crate::wab::read_segment_record_count(&path) {
                        Ok(n) => (n, true),
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        // Unreadable footer: count the segment, not its records.
                        Err(_) => (0, false),
                    };
                    let created_ms = header_created_ms(&path).unwrap_or(None);
                    let m = SealedMeta {
                        records,
                        created_ms,
                    };
                    // Only a complete reading is cached: a transient read
                    // error is retried on the next scan rather than pinned
                    // for the segment's lifetime.
                    if footer_ok && created_ms.is_some() && cache.len() < FOOTER_CACHE_CAP {
                        cache.entries.insert(path.clone(), m);
                    }
                    m
                }
            };
            seen.insert(path);
            r.undelivered_segments += 1;
            r.undelivered_records += meta.records;
            if let Some(c) = meta.created_ms {
                r.oldest_created_ms = Some(r.oldest_created_ms.map_or(c, |o| o.min(c)));
            }
        } else if file.ends_with(EXT_ACTIVE)
            && let Ok(Some(c)) = header_created_ms(&path)
        {
            // A let-chain, not a nested `if let`: clippy's `collapsible_if`
            // lints the nested form on edition 2024 / MSRV 1.88.
            r.active_created_ms.push((shard, c));
        }
    }
    Ok(())
}

fn flag(v: bool) -> f64 {
    if v { 1.0 } else { 0.0 }
}

/// Counter increase over a sliding window, from 5 s samples. A baseline
/// sample (start, 0) makes "younger than the window" count from process
/// start, which is when every counter was 0.
pub(crate) struct RecentWindow {
    samples: VecDeque<(Instant, u64)>,
}

impl RecentWindow {
    pub(crate) fn new(start: Instant) -> Self {
        RecentWindow {
            samples: VecDeque::from([(start, 0)]),
        }
    }

    /// The increase of `current` over the last `window`: against the newest
    /// sample at least `window` old, or the baseline while none is. Samples
    /// older than that one are dropped, so the deque holds one window.
    pub(crate) fn delta(&mut self, now: Instant, current: u64, window: Duration) -> u64 {
        self.samples.push_back((now, current));
        if let Some(cutoff) = now.checked_sub(window) {
            while self.samples.len() >= 2 && self.samples[1].0 <= cutoff {
                self.samples.pop_front();
            }
        }
        current.saturating_sub(self.samples[0].1)
    }
}

struct RecentWindows {
    ack_timeouts: RecentWindow,
    fsync_failures: RecentWindow,
    flusher_panics: RecentWindow,
}

impl RecentWindows {
    fn new(start: Instant) -> Self {
        RecentWindows {
            ack_timeouts: RecentWindow::new(start),
            fsync_failures: RecentWindow::new(start),
            flusher_panics: RecentWindow::new(start),
        }
    }
}

fn age_secs(now_ms: u64, then_ms: u64) -> f64 {
    now_ms.saturating_sub(then_ms) as f64 / 1000.0
}

/// Every gauge the evaluation and the evidence feed (spec §5.11 step 6).
pub(crate) fn publish_gauges(
    m: &Metrics,
    ev: &Evidence,
    eval: &Evaluation,
    ready: bool,
    live: bool,
) {
    let now = ev.now_unix_ms;
    m.ready.set(flag(ready));
    m.live.set(flag(live));
    for e in weir_status::CATALOGUE {
        m.condition_active
            .get_or_create(&ConditionLabel::of(e))
            .set(flag(eval.is_active(e.code)));
    }
    for s in ev.shards.iter().flatten() {
        let l = ShardLabel::of(usize::from(s.id));
        m.wab_shard_up.get_or_create(&l).set(flag(matches!(
            s.state,
            ShardState::Up | ShardState::Respawning
        )));
        m.wab_handoff_blocked_seconds.get_or_create(&l).set(
            s.handoff_blocked_since_unix_ms
                .map_or(0.0, |t| age_secs(now, t)),
        );
        m.wab_active_segment_age_seconds
            .get_or_create(&l)
            .set(s.active_segment_age_secs.unwrap_or(0.0));
    }
    for w in ev.workers.iter().flatten() {
        m.worker_up
            .get_or_create(&WorkerLabel::of(usize::from(w.id)))
            .set(flag(w.running));
    }
    if let Some(c) = &ev.wab_cap {
        m.wab_cap_rejecting.set(flag(c.rejecting));
    }
    if let Some(b) = &ev.backlog {
        m.wab_undelivered_segments
            .set(b.undelivered_segments as f64);
        m.wab_undelivered_records.set(b.undelivered_records as f64);
        m.wab_oldest_undelivered_age_seconds
            .set(b.oldest_undelivered_age_secs.unwrap_or(0.0));
        // Cadence change (spec §13): the 5 s task now also sets these.
        m.dead_letter_bytes_on_disk.set(b.dead_letter_bytes as f64);
        m.quarantine_bytes_on_disk.set(b.quarantine_bytes as f64);
    }
    if let Some(d) = &ev.drain {
        m.drain_commit_in_flight_seconds.set(
            d.commit_in_flight_since_unix_ms
                .map_or(0.0, |t| age_secs(now, t)),
        );
    }
    if let Some(s) = &ev.sink {
        m.sink_last_success_timestamp_seconds
            .set(s.last_success_unix_ms.map_or(0.0, |t| t as f64 / 1000.0));
        m.sink_consecutive_failures
            .set(s.consecutive_failures as f64);
        if let (Some(t), Some(k)) = (s.last_failure_unix_ms, s.last_failure_kind) {
            m.sink_last_failure_timestamp_seconds
                .get_or_create(&SinkFailureLabel {
                    kind: SinkFailureValue::of(k),
                })
                .set(t as f64 / 1000.0);
        }
    }
}

/// Stamps when the drain channel rose to `high_depth` and clears it below
/// (spec §9.3 code 8's "≥ 230 for 10 s"). Unpublished depth is not high.
pub(crate) fn update_channel_high(board: &StatusBoard, now_ms: u64, high_depth: u64) {
    let depth = board.drain_depth.load(Ordering::Relaxed);
    if depth != crate::status::board::DEPTH_UNPUBLISHED && depth as u64 >= high_depth {
        let _ = board.channel_high_since_ms.compare_exchange(
            0,
            now_ms.max(1),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    } else {
        board.channel_high_since_ms.store(0, Ordering::Relaxed);
    }
}

/// The 500 ms task (spec §5.11): queue depth into the gauge and the board,
/// and the channel high-water stamp. Holds a `queue_tx` clone: main aborts
/// and awaits it at shutdown (from PR 4; today `drop(rt)` releases it).
#[allow(dead_code)] // Task 1.23 replaces main's 500 ms task with it
pub(crate) fn spawn_queue_depth_task(
    queue_tx: QueueSender<WorkUnit>,
    metrics: Arc<Metrics>,
    board: Arc<StatusBoard>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let high = weir_status::Thresholds::DEFAULT.channel_high_depth;
        loop {
            interval.tick().await;
            let depth = queue_tx.len();
            metrics.queue_depth.set(depth as f64);
            board.queue_depth.store(depth as u64, Ordering::Relaxed);
            update_channel_high(&board, unix_ms_now(), high);
        }
    })
}

/// Spec §6 step 9: the first size scan and cap evaluation run synchronously,
/// after recovery and before any producer listener exists. Without it a
/// daemon restarted over its cap reads `wab_bytes_now = 0`, reports ready
/// and admits pushes until the first asynchronous scan. Fail-closed: if the
/// scan fails and a cap is set, start rejecting until a poll scan succeeds.
///
/// Returns the scanned size, or `None` when the scan failed.
#[cfg_attr(not(test), allow(dead_code))] // Task 1.23 calls it from main
pub(crate) fn startup_cap_gate(
    wab_dir: &Path,
    wab_max_bytes: u64,
    board: &StatusBoard,
    metrics: &Metrics,
) -> Option<u64> {
    match compute_wab_bytes_on_disk(wab_dir) {
        Some(bytes) => {
            metrics.wab_bytes_on_disk.set(bytes as f64);
            board.wab_bytes_now.store(bytes, Ordering::Relaxed);
            board.wab.note_scan_ok(unix_ms_now());
            let rejecting =
                crate::wab::cap::evaluate(wab_max_bytes, bytes, &board.wab_cap_rejecting);
            metrics.wab_cap_rejecting.set(flag(rejecting));
            if rejecting {
                warn!(
                    wab_bytes = bytes,
                    wab_max_bytes,
                    "the WAB is over wab_max_bytes at startup; pushes nack until delivery \
                     brings it below {}",
                    crate::wab::cap::low_water(wab_max_bytes)
                );
            }
            Some(bytes)
        }
        None => {
            if wab_max_bytes > 0 {
                board.wab_cap_rejecting.store(true, Ordering::Relaxed);
                metrics.wab_cap_rejecting.set(1.0);
                warn!(
                    wab_dir = %wab_dir.display(),
                    "the startup WAB size scan failed; with wab_max_bytes set, pushes nack \
                     (fail-closed) until a scan succeeds (every 5 s)"
                );
            }
            None
        }
    }
}

#[cfg(test)]
mod wab_bytes_tests {
    use super::compute_wab_bytes_on_disk;

    /// G15: the gauge counts only live shard-segment bytes — the dead_letter/ and
    /// quarantine/ reserved subdirs (which carry .wab.sealed files too) must be
    /// skipped, or their bytes would be double-counted against their own gauges.
    #[test]
    fn compute_wab_bytes_skips_dead_letter_and_quarantine() {
        let root = crate::testutil::scratch_dir("g15");
        let shard = root.join("shard_00");
        let dl = root.join("dead_letter");
        let q = root.join("quarantine");
        for d in [&shard, &dl, &q] {
            crate::testutil::mkdir_p(d);
        }
        // 100 live shard bytes; 999 in dead_letter; 999 in quarantine.
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 100]).unwrap();
        std::fs::write(dl.join("dl_00000001.wab.sealed"), vec![0u8; 999]).unwrap();
        std::fs::write(q.join("shard_00__seg_00000001.wab.sealed"), vec![0u8; 999]).unwrap();

        assert_eq!(
            compute_wab_bytes_on_disk(&root),
            Some(100),
            "only live shard segments count; dead_letter + quarantine are skipped"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// `<wab_dir>/.weir.lock` sits at the root as a regular file. The gauge
    /// counts live shard-segment bytes only, so the lock file's bytes are NOT
    /// counted (the root walk skips every non-directory). Pinned so a future
    /// walker change cannot start counting it against `wab_max_bytes`.
    #[test]
    fn compute_wab_bytes_does_not_count_the_wab_dir_lock_file() {
        let root = crate::testutil::scratch_dir("wab_bytes_lock");
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 100]).unwrap();
        std::fs::write(root.join(crate::wab::lock::LOCK_FILE_NAME), vec![b'x'; 77]).unwrap();
        assert_eq!(compute_wab_bytes_on_disk(&root), Some(100));
        std::fs::remove_dir_all(&root).ok();
    }

    /// A stale `seg_X.wab.confirmed.tmp` (the atomic sidecar write's transient
    /// file, spec §4.3) holds no record bytes and must not count toward the cap.
    #[test]
    fn compute_wab_bytes_ignores_a_confirmed_tmp_file() {
        let root = crate::testutil::scratch_dir("wab_bytes_tmp");
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 100]).unwrap();
        std::fs::write(shard.join("seg_00000002.wab.confirmed.tmp"), vec![0u8; 999]).unwrap();
        assert_eq!(compute_wab_bytes_on_disk(&root), Some(100));
        std::fs::remove_dir_all(&root).ok();
    }

    /// Spec §4.9: a dirent error (an `Err` from the `read_dir` iterator) is an
    /// unknown, not an absence. `.flatten()` used to drop it, so the entry —
    /// a whole shard at the top level, a segment inside a shard — was simply
    /// not counted: an undercount, which lifts the `wab_max_bytes` cap by
    /// exactly the bytes it failed to see. Both levels now report `None`.
    #[test]
    fn a_dirent_error_makes_the_size_unknown() {
        let root = crate::testutil::scratch_dir("wab_bytes_dirent");
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 100]).unwrap();
        assert_eq!(compute_wab_bytes_on_disk(&root), Some(100));

        for armed in [&root, &shard] {
            super::dirent_fault::arm(armed);
            let got = compute_wab_bytes_on_disk(&root);
            super::dirent_fault::disarm();
            assert_eq!(
                got,
                None,
                "a dirent error while listing {} must make the size unknown",
                armed.display()
            );
        }
        assert_eq!(compute_wab_bytes_on_disk(&root), Some(100));
        std::fs::remove_dir_all(&root).ok();
    }

    /// The cap's job is to fail CLOSED. A scan that cannot read the WAB
    /// directory knows nothing about its size, and must not be able to report
    /// the same answer as a directory it read successfully and found empty --
    /// that answer is 0, which `apply_wab_scan` takes as a success and writes
    /// into the atomic the cap is checked against, lifting the cap entirely.
    ///
    /// `apply_wab_scan` already refuses to do this for a JoinError, and says so
    /// by name: "failing open on the one feature whose job is to fail closed".
    /// The hole is upstream of that guard, in the value this function produces.
    #[test]
    fn an_unreadable_wab_dir_is_not_reported_as_empty() {
        let empty = crate::testutil::scratch_dir("scanfail_ok");
        crate::testutil::mkdir_p(&empty);
        let unreadable = empty.join("no_such_wab_dir");
        assert!(!unreadable.exists(), "the failure case must actually fail");

        assert_eq!(
            compute_wab_bytes_on_disk(&unreadable),
            None,
            "a WAB directory that could not be scanned reports no size at all"
        );
        assert_eq!(
            compute_wab_bytes_on_disk(&empty),
            Some(0),
            "a directory that WAS scanned and found empty still reports a size"
        );
        std::fs::remove_dir_all(&empty).ok();
    }
}

#[cfg(test)]
mod growth_warning_tests {
    use super::{WAB_GROWTH_WARN_INTERVAL, growth_warning_is_due, should_warn_wab_growth};
    use std::time::{Duration, Instant};

    /// Spec §7: "rate limiting holds across repeated polls." The warning's other
    /// two inputs were extracted for unit testing; this one stayed inline in the
    /// polling closure and so went untested until now.
    #[test]
    fn growth_warning_rate_limit_gates_on_the_full_interval() {
        let base = Instant::now();
        assert!(
            growth_warning_is_due(None, base),
            "the first qualifying window must warn immediately"
        );
        assert!(
            !growth_warning_is_due(Some(base), base),
            "a warning just emitted must not repeat on the same instant"
        );
        assert!(
            !growth_warning_is_due(
                Some(base),
                base + WAB_GROWTH_WARN_INTERVAL - Duration::from_secs(1)
            ),
            "one second short of the interval is not due"
        );
        assert!(
            growth_warning_is_due(Some(base), base + WAB_GROWTH_WARN_INTERVAL),
            "exactly the interval is due"
        );
        assert!(growth_warning_is_due(
            Some(base),
            base + WAB_GROWTH_WARN_INTERVAL * 10
        ));
    }

    /// The shape the rate limit actually has to survive: the task polls every
    /// 5 s, and the warning conditions can stay satisfied for hours. Ten minutes
    /// of ticks with the condition permanently true must produce three warnings
    /// (t=0, t=300, t=600), not 121.
    #[test]
    fn growth_warning_rate_limit_holds_across_repeated_polls() {
        let base = Instant::now();
        let mut last_warned: Option<Instant> = None;
        let mut fired = 0u32;
        for tick in 0..=120u64 {
            let now = base + Duration::from_secs(tick * 5);
            if growth_warning_is_due(last_warned, now) {
                fired += 1;
                last_warned = Some(now);
            }
        }
        assert_eq!(
            fired, 3,
            "600 s of 5 s polls with the condition always true must warn at \
             t=0/300/600 only"
        );
    }

    #[test]
    fn growth_warning_fires_only_when_all_three_conditions_hold() {
        // Growing + unhealthy + no cap => warn.
        assert!(should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            0,
            false,
        ));
        // Growing + HEALTHY => no warning. This is the condition that keeps the
        // message trustworthy: a fast producer must not trigger it.
        assert!(!should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            0,
            true,
        ));
        // Growing + unhealthy but a cap IS set => no warning; the cap handles it.
        assert!(!should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            64 * 1024 * 1024,
            false,
        ));
        // Flat or shrinking => no warning.
        assert!(!should_warn_wab_growth(
            &[500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500],
            0,
            false,
        ));
        assert!(!should_warn_wab_growth(
            &[
                1200, 1100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100
            ],
            0,
            false,
        ));
        // Not enough samples yet => no warning.
        assert!(!should_warn_wab_growth(&[100, 200, 300], 0, false));
    }

    /// The shape a *degraded* sink produces: net growth interrupted by dips
    /// wherever a batch did get delivered and its segment was confirmed and
    /// deleted. The spec names "slow or degraded sink" as a case this must
    /// catch, and a strict per-step non-decrease rule never fires on it — one
    /// dip anywhere in the window resets the detector.
    #[test]
    fn growth_warning_fires_on_a_dipping_but_net_growing_window() {
        assert!(should_warn_wab_growth(
            &[
                100, 300, 250, 500, 420, 700, 650, 900, 850, 1100, 1000, 1300
            ],
            0,
            false,
        ));
        // A dip exactly back to the starting value is still not giving ground.
        assert!(should_warn_wab_growth(
            &[100, 200, 100, 300, 200, 400, 300, 500, 400, 600, 500, 700],
            0,
            false,
        ));
    }

    /// The counter-case that keeps the weaker rule honest: a WAB that is really
    /// draining goes BELOW where the window started, and must not warn however
    /// high it ends up. Without this, "net growth" alone would fire on a sawtooth
    /// that is comfortably keeping up.
    #[test]
    fn growth_warning_stays_silent_when_the_window_dips_below_its_start() {
        assert!(!should_warn_wab_growth(
            &[
                1000, 1200, 900, 1400, 800, 1600, 700, 1800, 600, 2000, 500, 2200
            ],
            0,
            false,
        ));
        // A single sample one byte under the start is enough to disqualify it.
        assert!(!should_warn_wab_growth(
            &[100, 200, 300, 400, 500, 99, 700, 800, 900, 1000, 1100, 1200],
            0,
            false,
        ));
    }
}

#[cfg(test)]
mod drain_is_healthy_tests {
    use super::drain_is_healthy;
    use crate::metrics::{
        DrainStateLabel, DrainStateValue, Metrics, SinkHealthLabel, SinkHealthState,
    };

    /// Drives the `sink_health` gauge family exactly the way
    /// `drain::set_sink_health` does in production: one-hot, active state at
    /// 1.0, the rest 0.0.
    fn set_sink_health(m: &Metrics, active: SinkHealthState) {
        for s in [
            SinkHealthState::healthy,
            SinkHealthState::degraded,
            SinkHealthState::down,
        ] {
            let v = if s == active { 1.0 } else { 0.0 };
            m.sink_health
                .get_or_create(&SinkHealthLabel { state: s })
                .set(v);
        }
    }

    /// Drives the `drain_state` gauge family exactly the way
    /// `drain::set_drain_state` does in production: one-hot, active state at
    /// 1.0, the rest 0.0.
    fn set_drain_state(m: &Metrics, active: DrainStateValue) {
        for s in [
            DrainStateValue::draining,
            DrainStateValue::retrying_transient,
            DrainStateValue::blocked_dead_letter_full,
            DrainStateValue::stopped,
        ] {
            let v = if s == active { 1.0 } else { 0.0 };
            m.drain_state
                .get_or_create(&DrainStateLabel { state: s })
                .set(v);
        }
    }

    #[test]
    fn sink_healthy_and_draining_is_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::draining);
        assert!(
            drain_is_healthy(&m),
            "healthy sink + actively draining must compose to healthy (no warning)"
        );
    }

    #[test]
    fn sink_healthy_but_not_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::retrying_transient);
        assert!(
            !drain_is_healthy(&m),
            "a healthy sink that isn't actively draining must not compose to healthy"
        );
    }

    #[test]
    fn sink_unhealthy_but_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::down);
        set_drain_state(&m, DrainStateValue::draining);
        assert!(
            !drain_is_healthy(&m),
            "an unhealthy sink must not compose to healthy even while draining"
        );
    }

    #[test]
    fn sink_unhealthy_and_not_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::down);
        set_drain_state(&m, DrainStateValue::blocked_dead_letter_full);
        assert!(!drain_is_healthy(&m));
    }

    /// The case that made the growth warning structurally unable to fire: the
    /// drain thread is gone, so nothing updates these gauges any more. The
    /// supervisor now publishes `stopped` on its way out, and a stopped drain
    /// must not read as healthy however healthy the *sink* last looked — that
    /// last reading is stale by definition once nobody is probing it.
    #[test]
    fn stopped_drain_is_not_healthy_even_with_a_healthy_sink() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::stopped);
        assert!(
            !drain_is_healthy(&m),
            "a drain that has exited must never compose to healthy — this is what \
             lets the WAB growth warning fire when delivery has stopped for good"
        );
    }
}

#[cfg(test)]
mod apply_wab_scan_tests {
    use super::{WAB_GROWTH_WINDOW, apply_wab_scan};
    use crate::metrics::Metrics;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A real `JoinError`. Aborting a task is the only way to construct one —
    /// the type has no public constructor — and it is also the realistic cause:
    /// the blocking scan is cancelled at runtime shutdown.
    async fn join_error() -> tokio::task::JoinError {
        let h = tokio::spawn(async { std::future::pending::<()>().await });
        h.abort();
        h.await.expect_err("an aborted task must yield a JoinError")
    }

    #[tokio::test]
    async fn failed_scan_leaves_the_cap_reading_the_last_known_size() {
        let m = Metrics::new().0;
        let mut samples = vec![4_096];
        // What the cap actually reads. If a failed scan ever writes 0 here, the
        // cap is lifted entirely until the next tick — the fail-open this test
        // exists to prevent.
        let live = AtomicU64::new(4_096);

        let out = apply_wab_scan(Err(join_error().await), &mut samples, &m, &live);

        assert_eq!(out, None, "a failed scan yields no size");
        assert_eq!(
            live.load(Ordering::Relaxed),
            4_096,
            "a failed scan must not move the value the WAB cap is checked against"
        );
        assert_eq!(
            samples,
            vec![4_096],
            "a failed scan must not push a sample — a repeated value reads as \
             'not growing' and would suppress the growth warning for a window"
        );
    }

    /// The sibling of `failed_scan_leaves_the_cap_reading_the_last_known_size`,
    /// for the other way a scan can fail to produce a number. `compute_wab_bytes_on_disk`
    /// returns `None` when it could not read the WAB directory; treating that as a
    /// size would write it into `wab_bytes_now` and lift the cap. Mutation-checked:
    /// making the `Ok(None)` arm fall through to 0 fails this test on both asserts.
    #[tokio::test]
    async fn incomplete_scan_leaves_the_cap_reading_the_last_known_size() {
        let m = Metrics::new().0;
        let mut samples = vec![4_096];
        let live = AtomicU64::new(4_096);

        let out = apply_wab_scan(Ok(None), &mut samples, &m, &live);

        assert_eq!(out, None, "an incomplete scan yields no size");
        assert_eq!(
            live.load(Ordering::Relaxed),
            4_096,
            "an unreadable WAB directory must not move the value the cap is \
             checked against -- reporting 0 there lifts the cap entirely"
        );
        assert_eq!(
            samples,
            vec![4_096],
            "an incomplete scan must not push a sample"
        );
    }

    #[tokio::test]
    async fn successful_scan_publishes_the_size_and_extends_the_window() {
        let m = Metrics::new().0;
        let mut samples = Vec::new();
        let live = AtomicU64::new(0);

        let out = apply_wab_scan(Ok(Some(8_192)), &mut samples, &m, &live);

        assert_eq!(out, Some(8_192));
        assert_eq!(live.load(Ordering::Relaxed), 8_192);
        assert_eq!(samples, vec![8_192]);
    }

    #[tokio::test]
    async fn the_window_is_bounded_and_drops_from_the_front() {
        let m = Metrics::new().0;
        let mut samples = Vec::new();
        let live = AtomicU64::new(0);

        // One more than the window holds, so the oldest must fall off.
        for i in 0..=WAB_GROWTH_WINDOW as u64 {
            apply_wab_scan(Ok(Some(i)), &mut samples, &m, &live);
        }

        assert_eq!(samples.len(), WAB_GROWTH_WINDOW);
        assert_eq!(
            samples[0], 1,
            "the oldest sample is evicted, not the newest"
        );
        assert_eq!(samples[samples.len() - 1], WAB_GROWTH_WINDOW as u64);
    }
}

#[cfg(test)]
mod drain_heartbeat_stall_tests {
    use super::{
        DRAIN_STALL_CONSECUTIVE_POLLS, DrainStallVerdict, SINK_CALL_GRACE,
        drain_heartbeat_looks_wedged, drain_is_stopped, drain_stall_verdict, publish_drain_busy,
    };
    use crate::drain::BusyKind;
    use crate::metrics::{
        DrainBusyKind, DrainBusyLabel, DrainStateLabel, DrainStateValue, Metrics,
    };
    use std::time::Duration;

    /// The production default: `max(2 × sink_timeout, 60 s)`.
    const COMMIT_TIMEOUT: Duration = Duration::from_secs(60);

    fn set_drain_state(m: &Metrics, active: DrainStateValue) {
        for s in [
            DrainStateValue::draining,
            DrainStateValue::retrying_transient,
            DrainStateValue::blocked_dead_letter_full,
            DrainStateValue::stopped,
        ] {
            let v = if s == active { 1.0 } else { 0.0 };
            m.drain_state
                .get_or_create(&DrainStateLabel { state: s })
                .set(v);
        }
    }

    /// F2(a): once the supervisor exhausts its respawn budget it publishes
    /// `drain_state{stopped}` and the heartbeat freezes FOREVER — so the stall
    /// check fires ~45-50 s later and, until this fix, asserted "drain_state is
    /// left unchanged (a wedge is not `stopped`)". That is exactly backwards,
    /// and it is the deterministic outcome of the dead-letter-open panic path:
    /// the panic precedes the first `heartbeat.bump()`, so the heartbeat is 0
    /// for the whole respawn ladder. The operator is then sent hunting a live
    /// wedged sink while the drain is simply dead.
    #[test]
    fn a_stopped_drain_is_diagnosed_as_stopped_not_wedged() {
        let m = Metrics::new().0;
        set_drain_state(&m, DrainStateValue::stopped);
        let interval = Duration::from_secs(30);
        let frozen_for = Duration::from_secs(50); // past the 45 s threshold
        assert!(
            drain_heartbeat_looks_wedged(frozen_for, interval),
            "precondition: the freeze is long enough that something is reported"
        );
        assert_eq!(
            drain_stall_verdict(
                frozen_for,
                interval,
                drain_is_stopped(&m),
                None,
                COMMIT_TIMEOUT
            ),
            Some(DrainStallVerdict::Stopped),
            "a drain whose own gauge says `stopped` is dead, not wedged — reporting a              wedge here misdirects the operator during an incident"
        );
    }

    /// The other half: with the drain still claiming a live state, a frozen
    /// heartbeat IS the wedge the warning was written for, and must still fire.
    #[test]
    fn a_frozen_heartbeat_on_a_live_drain_state_is_still_a_wedge() {
        let m = Metrics::new().0;
        for live in [
            DrainStateValue::draining,
            DrainStateValue::retrying_transient,
            DrainStateValue::blocked_dead_letter_full,
        ] {
            set_drain_state(&m, live.clone());
            assert_eq!(
                drain_stall_verdict(
                    Duration::from_secs(50),
                    Duration::from_secs(30),
                    drain_is_stopped(&m),
                    None,
                    COMMIT_TIMEOUT,
                ),
                Some(DrainStallVerdict::Wedged),
                "{live:?} is a live state; a frozen heartbeat under it is a real wedge"
            );
        }
    }

    /// Below the threshold nothing is reported at all, stopped or not — the
    /// grace period is not bypassed by the new branch.
    #[test]
    fn nothing_is_reported_below_the_stall_threshold() {
        let interval = Duration::from_secs(30);
        for stopped in [true, false] {
            assert_eq!(
                drain_stall_verdict(
                    Duration::from_secs(20),
                    interval,
                    stopped,
                    None,
                    COMMIT_TIMEOUT
                ),
                None
            );
        }
    }

    /// `drain_is_stopped` reads the same one-hot gauge family the drain
    /// supervisor writes. A wrong label variant here would silently restore
    /// the false diagnosis, so read it back explicitly.
    #[test]
    fn drain_is_stopped_reads_the_drain_state_gauge() {
        let m = Metrics::new().0;
        set_drain_state(&m, DrainStateValue::draining);
        assert!(!drain_is_stopped(&m));
        set_drain_state(&m, DrainStateValue::stopped);
        assert!(drain_is_stopped(&m));
    }

    /// The false positive this function exists to prevent: at the default
    /// `health_poll_interval` (30 s), a healthy but fully idle drain only
    /// bumps its heartbeat once per interval (its idle wait is bounded by
    /// that interval, and nothing else wakes it) — so a gap anywhere up to
    /// that interval must never read as wedged, however suggestive the
    /// brief's literal "3 polls / 15 s" figure looks in isolation.
    #[test]
    fn a_healthy_idle_drain_at_the_default_poll_interval_never_reads_as_wedged() {
        let health_poll_interval = Duration::from_secs(30);
        // 15 s (the brief's literal figure) is well within one idle cycle.
        assert!(!drain_heartbeat_looks_wedged(
            Duration::from_secs(15),
            health_poll_interval
        ));
        // So is a gap right up to (but not past) the interval itself.
        assert!(!drain_heartbeat_looks_wedged(
            health_poll_interval,
            health_poll_interval
        ));
    }

    /// Past one full idle cycle, the brief's 3-poll/15 s rule takes over:
    /// three more silent 5 s polls beyond the grace period reads as wedged.
    #[test]
    fn a_stall_past_one_full_idle_cycle_plus_three_polls_reads_as_wedged() {
        let health_poll_interval = Duration::from_secs(30);
        let threshold =
            health_poll_interval + Duration::from_secs(5) * DRAIN_STALL_CONSECUTIVE_POLLS;
        assert!(!drain_heartbeat_looks_wedged(
            threshold - Duration::from_secs(1),
            health_poll_interval
        ));
        assert!(drain_heartbeat_looks_wedged(
            threshold,
            health_poll_interval
        ));
        // A real wedge never recovers, so this must keep reading true forever,
        // not just at the moment it first crosses the threshold.
        assert!(drain_heartbeat_looks_wedged(
            threshold * 10,
            health_poll_interval
        ));
    }

    /// A short, test-style `health_poll_interval` still gets the full
    /// three-poll grace ON TOP of it, not just the literal 15 s figure taken
    /// in isolation.
    #[test]
    fn the_grace_period_scales_with_a_short_health_poll_interval() {
        let health_poll_interval = Duration::from_secs(5);
        assert!(!drain_heartbeat_looks_wedged(
            Duration::from_secs(19),
            health_poll_interval
        ));
        assert!(drain_heartbeat_looks_wedged(
            Duration::from_secs(20),
            health_poll_interval
        ));
    }

    // ── The busy-aware verdict table (spec §4.8) ─────────────────────────────

    const LOCAL_KINDS: [BusyKind; 5] = [
        BusyKind::SegmentRead,
        BusyKind::LocalFsync,
        BusyKind::DeadLetterWrite,
        BusyKind::QuarantineMove,
        BusyKind::DirScan,
    ];

    /// One row per line of the spec's table, at the default 30 s poll interval
    /// and 60 s `commit_timeout`, with the heartbeat frozen past the 45 s
    /// threshold.
    #[test]
    fn the_verdict_table() {
        let interval = Duration::from_secs(30);
        let frozen = Duration::from_secs(50);
        let limit = COMMIT_TIMEOUT + SINK_CALL_GRACE;
        let verdict =
            |stopped, busy| drain_stall_verdict(frozen, interval, stopped, busy, COMMIT_TIMEOUT);

        // `drain_state{stopped}`: Stopped, whatever the busy atomics say.
        assert_eq!(verdict(true, None), Some(DrainStallVerdict::Stopped));
        assert_eq!(
            verdict(true, Some((BusyKind::SinkCommit, frozen))),
            Some(DrainStallVerdict::Stopped)
        );

        // Busy in a sink call up to commit_timeout + 15 s: a slow sink call.
        for kind in [BusyKind::SinkCommit, BusyKind::SinkProbe] {
            for busy_for in [frozen, limit] {
                assert_eq!(
                    verdict(false, Some((kind, busy_for))),
                    Some(DrainStallVerdict::SlowSinkCall { kind, busy_for }),
                    "{kind:?} for {busy_for:?}"
                );
            }
            // Past it, a timeout that could fire would have: the sink is
            // blocking the thread.
            let busy_for = limit + Duration::from_millis(1);
            assert_eq!(
                verdict(false, Some((kind, busy_for))),
                Some(DrainStallVerdict::Wedged),
                "{kind:?} for {busy_for:?}"
            );
        }

        // Busy in local I/O for ANY duration: slow local I/O, never a wedge
        // — a restart does not fix a disk.
        for kind in LOCAL_KINDS {
            for busy_for in [frozen, Duration::from_secs(3600)] {
                assert_eq!(
                    verdict(false, Some((kind, busy_for))),
                    Some(DrainStallVerdict::SlowLocalIo { kind, busy_for }),
                    "{kind:?} for {busy_for:?}"
                );
            }
        }

        // Frozen with nothing busy: the thread is stuck somewhere no guard
        // covers — a wedge.
        assert_eq!(verdict(false, None), Some(DrainStallVerdict::Wedged));
    }

    /// The slow-disk case the beast research measured: one sidecar fsync that
    /// takes a minute. Before §4.8 this froze the heartbeat past the 45 s
    /// threshold with nothing to tell it from a wedge, and the operator was
    /// advised to restart, which re-delivers the segment from record 0 and
    /// leaves the disk exactly as slow.
    #[test]
    fn a_sixty_second_sidecar_fsync_is_slow_local_io_not_a_wedge() {
        let fsync = Duration::from_secs(60);
        assert_eq!(
            drain_stall_verdict(
                fsync,
                Duration::from_secs(30),
                false,
                Some((BusyKind::LocalFsync, fsync)),
                COMMIT_TIMEOUT,
            ),
            Some(DrainStallVerdict::SlowLocalIo {
                kind: BusyKind::LocalFsync,
                busy_for: fsync,
            })
        );
    }

    /// The grace period still comes first: below the freeze threshold nothing
    /// is reported, whatever is busy and however long for.
    #[test]
    fn nothing_busy_or_not_is_reported_below_the_stall_threshold() {
        let interval = Duration::from_secs(30);
        let frozen = Duration::from_secs(44);
        let mut shapes = vec![None];
        for kind in LOCAL_KINDS
            .into_iter()
            .chain([BusyKind::SinkCommit, BusyKind::SinkProbe])
        {
            shapes.push(Some((kind, Duration::from_secs(3600))));
        }
        for busy in shapes {
            for stopped in [true, false] {
                assert_eq!(
                    drain_stall_verdict(frozen, interval, stopped, busy, COMMIT_TIMEOUT),
                    None,
                    "{busy:?}, stopped={stopped}"
                );
            }
        }
    }

    fn busy_gauge(m: &Metrics, kind: DrainBusyKind) -> f64 {
        m.drain_busy_seconds
            .get_or_create(&DrainBusyLabel { kind })
            .get()
    }

    const ALL_GAUGE_KINDS: [DrainBusyKind; 7] = [
        DrainBusyKind::sink_commit,
        DrainBusyKind::sink_probe,
        DrainBusyKind::segment_read,
        DrainBusyKind::local_fsync,
        DrainBusyKind::dead_letter_write,
        DrainBusyKind::quarantine_move,
        DrainBusyKind::dir_scan,
    ];

    /// `weir_drain_busy_seconds{kind}`: only the current kind is non-zero, and
    /// an idle drain reads zero everywhere — including the kind that was busy
    /// on the previous poll.
    #[test]
    fn the_busy_gauge_shows_only_the_current_kind() {
        let m = Metrics::new().0;
        for kind in ALL_GAUGE_KINDS {
            assert_eq!(busy_gauge(&m, kind), 0.0, "{kind:?} exists at 0 from boot");
        }

        publish_drain_busy(
            &m,
            Some((BusyKind::LocalFsync, Duration::from_millis(2500))),
        );
        for kind in ALL_GAUGE_KINDS {
            let want = if kind == DrainBusyKind::local_fsync {
                2.5
            } else {
                0.0
            };
            assert_eq!(busy_gauge(&m, kind), want, "{kind:?}");
        }

        publish_drain_busy(&m, Some((BusyKind::SinkCommit, Duration::from_secs(1))));
        assert_eq!(busy_gauge(&m, DrainBusyKind::sink_commit), 1.0);
        assert_eq!(busy_gauge(&m, DrainBusyKind::local_fsync), 0.0);

        publish_drain_busy(&m, None);
        for kind in ALL_GAUGE_KINDS {
            assert_eq!(busy_gauge(&m, kind), 0.0, "{kind:?}");
        }
    }

    /// Each `BusyKind` maps to its own gauge label: a crossed wire would put a
    /// slow disk on the sink's series.
    #[test]
    fn every_busy_kind_has_its_own_gauge_label() {
        let m = Metrics::new().0;
        let kinds = [
            BusyKind::SinkCommit,
            BusyKind::SinkProbe,
            BusyKind::SegmentRead,
            BusyKind::LocalFsync,
            BusyKind::DeadLetterWrite,
            BusyKind::QuarantineMove,
            BusyKind::DirScan,
        ];
        for (kind, label) in kinds.into_iter().zip(ALL_GAUGE_KINDS) {
            publish_drain_busy(&m, Some((kind, Duration::from_secs(7))));
            assert_eq!(busy_gauge(&m, label), 7.0, "{kind:?} -> {label:?}");
        }
    }
}

#[cfg(test)]
mod drain_stall_log_tests {
    use super::{
        DrainLiveness, DrainStallVerdict, WAB_GROWTH_WARN_INTERVAL, WabPoll, stall_log_is_due,
    };
    use crate::drain::{BusyGuard, BusyKind, DrainHeartbeat};
    use crate::metrics::{DrainBusyKind, DrainBusyLabel, Metrics};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const SLOW_SINK: DrainStallVerdict = DrainStallVerdict::SlowSinkCall {
        kind: BusyKind::SinkCommit,
        busy_for: Duration::from_secs(45),
    };
    const SLOW_DISK: DrainStallVerdict = DrainStallVerdict::SlowLocalIo {
        kind: BusyKind::LocalFsync,
        busy_for: Duration::from_secs(45),
    };

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// The review's sequence at defaults: a sink that blocks the drain's OS
    /// thread reads as a slow sink call (INFO, "no action needed") at 45 s and
    /// becomes a wedge once it outlives commit_timeout + 15 s. The wedge WARN
    /// must go out on that poll, not five minutes after the INFO.
    #[test]
    fn a_wedge_right_after_a_slow_sink_call_info_is_logged_at_once() {
        let t0 = Instant::now();
        assert!(stall_log_is_due(None, SLOW_SINK, t0 + secs(45)));
        let last = Some((t0 + secs(45), SLOW_SINK));
        assert!(
            stall_log_is_due(last, DrainStallVerdict::Wedged, t0 + secs(80)),
            "an escalation to Wedged must bypass the rate limit"
        );
    }

    #[test]
    fn every_escalation_bypasses_the_rate_limit() {
        let t0 = Instant::now();
        let at = |v| Some((t0, v));
        let soon = t0 + secs(5);
        assert!(stall_log_is_due(at(SLOW_SINK), SLOW_DISK, soon));
        assert!(stall_log_is_due(
            at(SLOW_SINK),
            DrainStallVerdict::Wedged,
            soon
        ));
        assert!(stall_log_is_due(
            at(SLOW_DISK),
            DrainStallVerdict::Wedged,
            soon
        ));
        assert!(stall_log_is_due(
            at(SLOW_SINK),
            DrainStallVerdict::Stopped,
            soon
        ));
        assert!(
            stall_log_is_due(
                at(DrainStallVerdict::Wedged),
                DrainStallVerdict::Stopped,
                soon
            ),
            "Stopped carries the opposite advice to Wedged (no sink call to hunt)"
        );
    }

    /// The rate limit itself still holds for a repeat or a de-escalation.
    #[test]
    fn a_repeat_or_a_milder_verdict_waits_out_the_interval() {
        let t0 = Instant::now();
        let just_before = t0 + WAB_GROWTH_WARN_INTERVAL - secs(1);
        let at_interval = t0 + WAB_GROWTH_WARN_INTERVAL;
        for (last, next) in [
            (DrainStallVerdict::Wedged, DrainStallVerdict::Wedged),
            (DrainStallVerdict::Stopped, DrainStallVerdict::Stopped),
            (SLOW_SINK, SLOW_SINK),
            (SLOW_DISK, SLOW_DISK),
            (DrainStallVerdict::Wedged, SLOW_SINK),
            (DrainStallVerdict::Wedged, SLOW_DISK),
            (DrainStallVerdict::Stopped, DrainStallVerdict::Wedged),
            (SLOW_DISK, SLOW_SINK),
        ] {
            assert!(
                !stall_log_is_due(Some((t0, last)), next, just_before),
                "{last:?} then {next:?} inside the interval"
            );
            assert!(
                stall_log_is_due(Some((t0, last)), next, at_interval),
                "{last:?} then {next:?} once the interval has passed"
            );
        }
    }

    /// A different busy duration is the same verdict class, not an escalation:
    /// otherwise a slow sink call would log on every 5 s poll.
    #[test]
    fn a_longer_busy_duration_is_not_an_escalation() {
        let t0 = Instant::now();
        let longer = DrainStallVerdict::SlowSinkCall {
            kind: BusyKind::SinkProbe,
            busy_for: secs(70),
        };
        assert!(!stall_log_is_due(
            Some((t0, SLOW_SINK)),
            longer,
            t0 + secs(5)
        ));
    }

    /// The poll task's state machine end to end, at production defaults
    /// (health_poll_interval 30 s, commit_timeout 60 s): a sink commit that
    /// blocks the thread forever, polled every 5 s for two minutes.
    #[test]
    fn a_thread_blocking_commit_logs_info_then_the_wedge_without_delay() {
        let t0 = Instant::now();
        let mut liveness = DrainLiveness::new(t0, secs(30), secs(60));
        let mut logged = Vec::new();
        for tick in 0..=24u64 {
            let elapsed = secs(tick * 5);
            let busy = Some((BusyKind::SinkCommit, elapsed));
            if let Some((verdict, _frozen)) = liveness.observe(t0 + elapsed, 7, false, busy) {
                logged.push((elapsed.as_secs(), verdict));
            }
        }
        assert_eq!(
            logged,
            vec![
                (
                    45,
                    DrainStallVerdict::SlowSinkCall {
                        kind: BusyKind::SinkCommit,
                        busy_for: secs(45)
                    }
                ),
                (80, DrainStallVerdict::Wedged),
            ],
            "INFO at the stall threshold, the wedge WARN on the first poll past 75 s, \
             and nothing else inside the 300 s rate limit"
        );
    }

    /// A moving heartbeat resets the freeze clock, as before the extraction.
    #[test]
    fn a_moving_heartbeat_never_reports() {
        let t0 = Instant::now();
        let mut liveness = DrainLiveness::new(t0, secs(30), secs(60));
        for tick in 0..=40u64 {
            assert_eq!(
                liveness.observe(t0 + secs(tick * 5), tick, false, None),
                None
            );
        }
    }

    /// Spec §4.8 poll placement: the busy gauge and the stall verdict run
    /// BEFORE the WAB scan can end the tick, so a failing scan does not
    /// silence the one signal that says whether the drain is alive.
    #[tokio::test]
    async fn a_failing_wab_scan_does_not_silence_the_stall_check_or_the_busy_gauge() {
        let metrics = Arc::new(Metrics::new().0);
        let hb = DrainHeartbeat::new();
        let _busy = BusyGuard::new(&hb, BusyKind::LocalFsync);
        std::thread::sleep(Duration::from_millis(20));
        let t0 = Instant::now();
        let mut poll = WabPoll::new(
            Arc::clone(&metrics),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            0,
            hb.clone(),
            DrainLiveness::new(t0, secs(30), secs(60)),
        );
        let failing_scan = || async { Ok(None) };
        assert_eq!(poll.tick(t0, failing_scan).await, None);
        let verdict = poll.tick(t0 + secs(50), failing_scan).await;
        assert!(
            matches!(
                verdict,
                Some(DrainStallVerdict::SlowLocalIo {
                    kind: BusyKind::LocalFsync,
                    ..
                })
            ),
            "the stall check ran despite the failing scan: {verdict:?}"
        );
        let gauge = metrics
            .drain_busy_seconds
            .get_or_create(&DrainBusyLabel {
                kind: DrainBusyKind::local_fsync,
            })
            .get();
        assert!(gauge > 0.0, "the busy gauge was published: {gauge}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::status::StatusBoard;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn wab_with_bytes(label: &str, bytes: usize) -> std::path::PathBuf {
        let root = crate::testutil::scratch_dir(&format!("poll_{label}"));
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; bytes]).unwrap();
        root
    }

    fn poll_for(
        board: &Arc<StatusBoard>,
        metrics: &Arc<Metrics>,
        cap: u64,
        dir: std::path::PathBuf,
    ) -> WabPoll {
        WabPoll::new(
            Arc::clone(metrics),
            Arc::clone(&board.wab_bytes_now),
            cap,
            DrainHeartbeat::new(),
            DrainLiveness::new(Instant::now(), secs(30), secs(60)),
        )
        .with_status(PollStatus::new(
            Arc::clone(board),
            dir,
            Arc::new(AtomicU64::new(200)),
        ))
    }

    #[test]
    fn startup_gate_rejects_a_restart_over_the_cap_before_any_listener_exists() {
        let dir = wab_with_bytes("gate_over", 2_000);
        let board = StatusBoard::for_test();
        let m = Metrics::new().0;
        assert_eq!(startup_cap_gate(&dir, 1_000, &board, &m), Some(2_000));
        assert!(board.wab_cap_rejecting.load(Ordering::Relaxed));
        assert_eq!(board.wab_bytes_now.load(Ordering::Relaxed), 2_000);
        assert!(board.wab.first_scan_ok_ms().is_some());
        assert_eq!(m.wab_cap_rejecting.get(), 1.0);
        let under = StatusBoard::for_test();
        assert_eq!(startup_cap_gate(&dir, 10_000, &under, &m), Some(2_000));
        assert!(!under.wab_cap_rejecting.load(Ordering::Relaxed));
    }

    #[test]
    fn a_failed_startup_scan_fails_closed_only_when_a_cap_is_set() {
        let missing = crate::testutil::scratch_dir("poll_gate_missing").join("no_such_wab_dir");
        let m = Metrics::new().0;
        let capped = StatusBoard::for_test();
        assert_eq!(startup_cap_gate(&missing, 1_000, &capped, &m), None);
        assert!(
            capped.wab_cap_rejecting.load(Ordering::Relaxed),
            "fail-closed: nack until a scan succeeds"
        );
        assert!(capped.wab.first_scan_ok_ms().is_none());
        let uncapped = StatusBoard::for_test();
        startup_cap_gate(&missing, 0, &uncapped, &m);
        assert!(!uncapped.wab_cap_rejecting.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn the_poll_clears_the_cap_with_zero_pushes() {
        let board = StatusBoard::for_test();
        let m = Arc::new(Metrics::new().0);
        board.wab_cap_rejecting.store(true, Ordering::Relaxed);
        let mut poll = poll_for(&board, &m, 1_000, std::path::PathBuf::from("/nonexistent"));
        poll.tick(Instant::now(), || async { Ok(Some(100)) }).await;
        assert!(
            !board.wab_cap_rejecting.load(Ordering::Relaxed),
            "drained below low water: released without a push"
        );
        assert!(board.wab.last_scan_ok_ms().is_some());
        assert_eq!(m.wab_cap_rejecting.get(), 0.0);
    }

    #[tokio::test]
    async fn a_failed_poll_scan_leaves_the_cap_and_the_scan_stamp_alone() {
        let board = StatusBoard::for_test();
        let m = Arc::new(Metrics::new().0);
        board.wab_cap_rejecting.store(true, Ordering::Relaxed);
        let mut poll = poll_for(&board, &m, 1_000, std::path::PathBuf::from("/nonexistent"));
        poll.tick(Instant::now(), || async { Ok(None) }).await;
        assert!(
            board.wab_cap_rejecting.load(Ordering::Relaxed),
            "fail-closed"
        );
        assert!(board.wab.last_scan_ok_ms().is_none());
    }

    #[tokio::test]
    async fn the_growth_verdict_is_published_on_every_successful_scan() {
        let board = StatusBoard::for_test();
        let m = Arc::new(Metrics::new().0);
        let mut poll = poll_for(&board, &m, 0, std::path::PathBuf::from("/nonexistent"));
        assert_eq!(board.wab.growth(), None);
        poll.tick(Instant::now(), || async { Ok(Some(10)) }).await;
        assert_eq!(
            board.wab.growth(),
            Some(false),
            "a one-sample window is not growth"
        );
    }

    use crate::wab::segment::{WabSegment, segment_path};
    use weir_wab::format::{Compression, confirmed_path_for};

    fn sealed(shard_dir: &Path, counter: u64, records: usize) -> std::path::PathBuf {
        let mut seg =
            WabSegment::create(&segment_path(shard_dir, counter), 0, Compression::None).unwrap();
        for i in 0..records {
            seg.write_record(format!("r{i}").as_bytes()).unwrap();
        }
        seg.seal().unwrap()
    }

    #[test]
    fn the_lag_scan_counts_undelivered_work_and_never_touches_the_wab() {
        let root = crate::testutil::scratch_dir("poll_lag");
        let _ = std::fs::remove_dir_all(&root);
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        sealed(&shard, 1, 3);
        let delivered = sealed(&shard, 2, 5);
        std::fs::write(confirmed_path_for(&delivered), b"sidecar").unwrap();
        let mut active =
            WabSegment::create(&segment_path(&shard, 3), 0, Compression::None).unwrap();
        active.write_record(b"open").unwrap();
        for (dir, files) in [("dead_letter", 1usize), ("quarantine", 2)] {
            crate::testutil::mkdir_p(root.join(dir));
            for i in 0..files {
                std::fs::write(root.join(dir).join(format!("f{i}")), vec![0u8; 10]).unwrap();
            }
        }
        let listing = || {
            let mut v: Vec<_> = std::fs::read_dir(&shard)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            v.sort();
            v
        };
        let before = listing();
        let mut cache = FooterCache::default();
        let now = unix_ms_now() + 60_000;
        let lag = scan_wab_lag(&root, &mut cache, now).unwrap();
        assert_eq!(
            (lag.undelivered_segments, lag.undelivered_records),
            (1, 3),
            "the confirmed one is delivered"
        );
        assert!(lag.oldest_created_ms.is_some_and(|c| now - c >= 60_000));
        assert_eq!(lag.active_created_ms.len(), 1);
        assert_eq!((lag.dead_letter_files, lag.dead_letter_bytes), (1, 10));
        assert_eq!((lag.quarantine_files, lag.quarantine_bytes), (2, 20));
        assert_eq!(lag.scanned_at_ms, now);
        assert_eq!(
            before,
            listing(),
            "read-only: nothing created, renamed or deleted"
        );
        assert_eq!(cache.len(), 1);
        drop(active);
    }

    #[test]
    fn the_footer_cache_forgets_deleted_segments() {
        let root = crate::testutil::scratch_dir("poll_lag_evict");
        let _ = std::fs::remove_dir_all(&root);
        let shard = root.join("shard_00");
        crate::testutil::mkdir_p(&shard);
        let seg = sealed(&shard, 1, 2);
        let mut cache = FooterCache::default();
        scan_wab_lag(&root, &mut cache, unix_ms_now()).unwrap();
        assert_eq!(cache.len(), 1);
        std::fs::remove_file(&seg).unwrap();
        let lag = scan_wab_lag(&root, &mut cache, unix_ms_now()).unwrap();
        assert_eq!((lag.undelivered_segments, cache.len()), (0, 0));
    }

    #[tokio::test]
    async fn refresh_lag_publishes_onto_the_board_and_keeps_the_last_report_on_failure() {
        let root = crate::testutil::scratch_dir("poll_lag_board");
        let _ = std::fs::remove_dir_all(&root);
        crate::testutil::mkdir_p(root.join("shard_00"));
        sealed(&root.join("shard_00"), 1, 4);
        let board = StatusBoard::for_test();
        let mut st = PollStatus::new(
            Arc::clone(&board),
            root.clone(),
            Arc::new(AtomicU64::new(200)),
        );
        st.refresh_lag(unix_ms_now()).await;
        assert_eq!(board.wab.lag().unwrap().undelivered_records, 4);
        std::fs::remove_dir_all(&root).unwrap();
        st.refresh_lag(unix_ms_now()).await;
        assert_eq!(
            board.wab.lag().unwrap().undelivered_records,
            4,
            "a failed scan leaves the last report (stale, not zero)"
        );
    }

    use weir_status::{ConditionCode, ListenerState, Phase, SinceLookup};

    #[test]
    fn a_window_counts_increases_within_it_and_forgets_older_ones() {
        let t0 = Instant::now();
        let mut w = RecentWindow::new(t0);
        assert_eq!(
            w.delta(t0 + secs(5), 2, secs(300)),
            2,
            "younger than the window: counted since process start"
        );
        assert_eq!(w.delta(t0 + secs(200), 2, secs(300)), 2);
        assert_eq!(
            w.delta(t0 + secs(400), 2, secs(300)),
            0,
            "no increase in the last 300 s"
        );
        assert_eq!(w.delta(t0 + secs(405), 5, secs(300)), 3);
    }

    #[test]
    fn the_channel_high_water_mark_is_stamped_once_and_cleared_below() {
        let b = StatusBoard::for_test();
        update_channel_high(&b, 100, 230);
        assert_eq!(
            b.channel_high_since_ms.load(Ordering::Relaxed),
            0,
            "unpublished depth is not high"
        );
        b.drain_depth.store(240, Ordering::Relaxed);
        update_channel_high(&b, 100, 230);
        update_channel_high(&b, 200, 230);
        assert_eq!(b.channel_high_since_ms.load(Ordering::Relaxed), 100);
        b.drain_depth.store(10, Ordering::Relaxed);
        update_channel_high(&b, 300, 230);
        assert_eq!(b.channel_high_since_ms.load(Ordering::Relaxed), 0);
    }

    fn serving_board() -> Arc<StatusBoard> {
        let b = crate::status::test_board(2, 1);
        b.shards.mark_up(0);
        b.shards.mark_up(1);
        let guard = crate::status::WorkerGuard::enter(Arc::clone(&b.shards), 0);
        std::mem::forget(guard); // keep the worker "running" for the test's life
        b.listeners.unix.set(ListenerState::Bound);
        b.life.enter(Phase::Serving, unix_ms_now());
        b
    }

    #[tokio::test]
    async fn one_tick_evaluates_publishes_gauges_and_latches() {
        let root = crate::testutil::scratch_dir("poll_full_tick");
        let _ = std::fs::remove_dir_all(&root);
        crate::testutil::mkdir_p(root.join("shard_00"));
        sealed(&root.join("shard_00"), 1, 2);
        let board = serving_board();
        let (m, reg) = Metrics::new();
        m.init_topology(2, 1);
        let m = Arc::new(m);
        let mut poll = poll_for(&board, &m, 0, root);
        poll.tick(Instant::now(), || async { Ok(Some(10)) }).await;
        assert_eq!(m.ready.get(), 1.0);
        assert_eq!(m.live.get(), 1.0);
        assert_eq!(m.wab_undelivered_segments.get(), 1.0);
        assert!(board.heartbeat_changed_ms.load(Ordering::Relaxed) > 0);
        assert_eq!(
            board.drain_busy_since_ms.load(Ordering::Relaxed),
            0,
            "an idle drain publishes no busy stamp"
        );
        assert_eq!(board.recent.load_full().ack_timeouts_5m, Some(0));
        // The test board's ConfigView is the noop sink: one active condition.
        assert!(board.latches.since(ConditionCode::NoopSink).is_some());
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &reg).unwrap();
        assert!(
            out.contains("weir_condition_active{code=\"noop_sink\",severity=\"warning\"} 1"),
            "{out}"
        );
        assert!(out.contains("weir_wab_shard_up{shard=\"1\"} 1"));
        assert!(out.contains("weir_worker_up{worker=\"0\"} 1"));
    }

    #[tokio::test]
    async fn condition_transitions_are_logged_once_each_way() {
        let board = serving_board();
        let m = Arc::new(Metrics::new().0);
        let mut st = PollStatus::new(
            Arc::clone(&board),
            std::path::PathBuf::from("/nonexistent"),
            Arc::new(AtomicU64::new(200)),
        );
        st.refresh_recent(Instant::now(), &m);
        let (_, first) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
        assert!(
            first.contains("condition raised") && first.contains("noop_sink"),
            "{first}"
        );
        let (_, second) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
        assert!(
            !second.contains("condition raised"),
            "no repeat while it stays active: {second}"
        );
        board.wab_cap_rejecting.store(true, Ordering::Relaxed);
        let (_, third) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
        assert!(
            third.contains("WARN") && third.contains("wab_cap_rejecting"),
            "{third}"
        );
        board.wab_cap_rejecting.store(false, Ordering::Relaxed);
        let (_, fourth) = crate::testutil::capture_logs(|| st.publish(unix_ms_now(), &m));
        assert!(
            fourth.contains("condition cleared") && fourth.contains("wab_cap_rejecting"),
            "{fourth}"
        );
        assert_eq!(m.ready.get(), 1.0);
    }

    #[test]
    fn stranded_since_last_success_rebases_on_each_new_success() {
        let board = StatusBoard::for_test();
        let m = Metrics::new().0;
        let mut st = PollStatus::new(
            Arc::clone(&board),
            std::path::PathBuf::from("/nonexistent"),
            Arc::new(AtomicU64::new(200)),
        );
        st.refresh_recent(Instant::now(), &m);
        assert_eq!(
            board.recent.load_full().stranded_since_last_success,
            None,
            "unpublished sink cell"
        );
        board.sink.record_success();
        m.drain_segments_stranded.inc();
        st.refresh_recent(Instant::now(), &m);
        assert_eq!(
            board.recent.load_full().stranded_since_last_success,
            Some(1)
        );
        // `record_success` stamps whole milliseconds; make the second one distinct.
        std::thread::sleep(Duration::from_millis(2));
        board.sink.record_success();
        st.refresh_recent(Instant::now(), &m);
        st.refresh_recent(Instant::now(), &m);
        assert_eq!(
            board.recent.load_full().stranded_since_last_success,
            Some(0)
        );
    }

    #[tokio::test]
    async fn a_failed_size_scan_still_evaluates() {
        let board = serving_board();
        let m = Arc::new(Metrics::new().0);
        let mut poll = poll_for(&board, &m, 0, std::path::PathBuf::from("/nonexistent"));
        poll.tick(Instant::now(), || async { Ok(None) }).await;
        assert!(
            board.latches.since(ConditionCode::WabScanStale).is_some(),
            "the status steps ran without a size scan"
        );
    }
}
