//! Board → `Evidence` → document (spec §5.2 `snapshot`, `evidence`,
//! `admin_doc`). Loads only: no I/O, no lock. Disk facts are at most one
//! poll interval (5 s) old (spec §9.2 rule 8).
use super::board::{DEPTH_UNPUBLISHED, QUEUE_UNPUBLISHED, StatusBoard};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use weir_status::{
    AdminStatusDoc, BacklogEvidence, BusyEvidence, CapEvidence, ChannelEvidence, Evaluation,
    Evidence, IdentityEvidence, Source, Thresholds,
};

pub(crate) struct StatusSnapshot {
    evidence: Evidence,
    identity: IdentityEvidence,
}

#[cfg_attr(not(test), allow(dead_code))] // PR 5 / PR 6 call these
impl StatusSnapshot {
    pub(crate) fn evidence(&self) -> Evidence {
        self.evidence.clone()
    }
    /// The admin document with no liveness checks selected and default
    /// thresholds; PR 5's router overwrites the three liveness fields.
    pub(crate) fn admin_doc(&self, eval: &Evaluation) -> AdminStatusDoc {
        AdminStatusDoc::build(
            &self.identity,
            &self.evidence,
            eval,
            &Thresholds::DEFAULT,
            &[],
        )
    }
}

/// One snapshot, one evaluation, the configured liveness checks. Loads only.
/// PR 5 (`/status`, `status::http`) and PR 6 (admin `status`) are its
/// callers.
pub(crate) fn render_admin_doc(
    board: &StatusBoard,
    thresholds: &Thresholds,
    livez_checks: &[weir_status::LivezCheck],
    now: SystemTime,
) -> AdminStatusDoc {
    let snap = board.snapshot(now);
    let eval = weir_status::evaluate(&snap.evidence, thresholds, &board.latches);
    AdminStatusDoc::build(
        &snap.identity,
        &snap.evidence,
        &eval,
        thresholds,
        livez_checks,
    )
}

fn opt(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

impl StatusBoard {
    pub(crate) fn identity(&self) -> IdentityEvidence {
        let s = &self.statik;
        IdentityEvidence {
            version: s.version.to_string(),
            wire_version: s.wire_version,
            pid: s.pid,
            started_at_unix_ms: s.started_at_ms,
            wab_dir: s.wab_dir.display().to_string(),
            socket_path: s.socket_path.display().to_string(),
            wab_dir_dev: s.wab_dir_dev,
            wab_dir_ino: s.wab_dir_ino,
        }
    }

    pub(crate) fn snapshot(&self, now: SystemTime) -> StatusSnapshot {
        let now_ms = now
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        StatusSnapshot {
            evidence: self.evidence_at(now_ms),
            identity: self.identity(),
        }
    }

    pub(crate) fn evidence_at(&self, now_ms: u64) -> Evidence {
        let life = &self.life;
        let lag = self.wab.lag();
        let mut shards = self.shards.shard_evidence();
        if let Some(lag) = &lag {
            for &(id, created) in &lag.active_created_ms {
                if let Some(s) = shards.get_mut(usize::from(id)) {
                    s.active_segment_age_secs =
                        Some(now_ms.saturating_sub(created) as f64 / 1000.0);
                }
            }
        }
        let config = self.statik.config.evidence();
        let max = config.public.wab_max_bytes.unwrap_or(0);
        let heartbeat_changed = opt(self.heartbeat_changed_ms.load(Ordering::Relaxed));
        let heartbeat_age = heartbeat_changed.map(|t| now_ms.saturating_sub(t) as f64 / 1000.0);
        let busy = opt(self.drain_busy_since_ms.load(Ordering::Acquire)).map(|since_unix_ms| {
            BusyEvidence {
                sink_call: self.drain_busy_sink_call.load(Ordering::Relaxed),
                since_unix_ms,
            }
        });
        let depth = self.drain_depth.load(Ordering::Relaxed);
        let queue = self.queue_depth.load(Ordering::Relaxed);
        Evidence {
            now_unix_ms: now_ms,
            source: Source::Daemon,
            identity: Some(self.identity()),
            started_at_unix_ms: Some(self.statik.started_at_ms),
            phase: Some(life.phase()),
            phase_since_unix_ms: Some(life.phase_since_ms()),
            ever_served: Some(life.ever_served()),
            shutting_down: Some(life.is_shutting_down()),
            shutting_down_since_unix_ms: life.shutting_down_since_ms(),
            forced: Some(life.forced()),
            drain_deadline_unix_ms: life.drain_deadline_ms(),
            drain_budget_secs: life.drain_budget_secs(),
            listeners: Some(self.listeners.evidence()),
            shards: Some(shards),
            workers: Some(self.shards.worker_evidence()),
            queue_depth: (queue != QUEUE_UNPUBLISHED).then_some(queue),
            wab_cap: Some(CapEvidence {
                max_bytes: max,
                low_water_bytes: crate::wab::cap::low_water(max),
                bytes_now: self.wab_bytes_now.load(Ordering::Relaxed),
                rejecting: self.wab_cap_rejecting.load(Ordering::Relaxed),
                first_scan_ok_unix_ms: self.wab.first_scan_ok_ms(),
                last_scan_ok_unix_ms: self.wab.last_scan_ok_ms(),
                growing_unbounded: self.wab.growth(),
            }),
            channel: (depth != DEPTH_UNPUBLISHED).then(|| ChannelEvidence {
                depth: depth as u64,
                capacity: weir_status::CHANNEL_CAPACITY,
                high_since_unix_ms: opt(self.channel_high_since_ms.load(Ordering::Relaxed)),
            }),
            drain: self.drain.evidence(heartbeat_age, heartbeat_changed, busy),
            sink: self.sink.evidence(),
            backlog: lag.as_ref().map(|l| BacklogEvidence {
                undelivered_segments: l.undelivered_segments,
                undelivered_records: l.undelivered_records,
                oldest_undelivered_age_secs: l
                    .oldest_created_ms
                    .map(|c| now_ms.saturating_sub(c) as f64 / 1000.0),
                dead_letter_bytes: l.dead_letter_bytes,
                dead_letter_max_bytes: config.public.dead_letter_max_bytes,
                quarantine_bytes: l.quarantine_bytes,
                quarantine_files: l.quarantine_files,
                scanned_at_ms: l.scanned_at_ms,
            }),
            recent: Some((*self.recent.load_full()).clone()),
            reload: self.reload.evidence(),
            admin: Some(self.admin.evidence()),
            config: Some(config),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::DrainStateValue;
    use std::sync::atomic::Ordering;
    use weir_status::*;

    #[test]
    fn a_fresh_board_reports_initializing_and_leaves_unwritten_cells_unknown() {
        let b = crate::status::test_board(2, 1);
        let e = b.evidence_at(1_800_000_000_000);
        assert_eq!(e.source, Source::Daemon);
        assert_eq!(e.phase, Some(Phase::Initializing));
        assert_eq!(e.shutting_down, Some(false));
        assert_eq!(e.shards.as_ref().unwrap().len(), 2);
        assert!(e.drain.is_none(), "drain cell unpublished (PR 2)");
        assert!(e.sink.is_none(), "sink cell unpublished (PR 2)");
        assert!(e.channel.is_none(), "drain depth unpublished (PR 3)");
        assert!(e.backlog.is_none(), "no lag scan yet");
        assert_eq!(e.queue_depth, None);
        assert_eq!(e.wab_cap.unwrap().last_scan_ok_unix_ms, None);
        assert_eq!(e.config.as_ref().unwrap().sink_type, "noop");
        assert_eq!(e.identity.as_ref().unwrap().wab_dir, "/var/lib/weir/wab");
        let ev = evaluate(&e, &Thresholds::DEFAULT, &b.latches);
        for c in [
            ConditionCode::DrainBackpressure,
            ConditionCode::DrainStopped,
            ConditionCode::SinkDown,
            ConditionCode::DeliveryStalled,
            ConditionCode::SegmentsStranded,
        ] {
            assert!(
                ev.unevaluated.contains(&c),
                "{c:?} must be unevaluated, not healthy"
            );
        }
        assert!(ev.is_active(ConditionCode::Starting));
        assert!(ev.is_active(ConditionCode::NoopSink));
    }

    #[test]
    fn published_cells_become_evidence() {
        let b = crate::status::test_board(1, 1);
        b.drain.set_state(DrainStateValue::draining);
        b.sink.record_success();
        b.drain_depth.store(240, Ordering::Relaxed);
        b.channel_high_since_ms.store(5, Ordering::Relaxed);
        b.queue_depth.store(3, Ordering::Relaxed);
        b.wab.set_lag(crate::status::board::LagReport {
            undelivered_segments: 2,
            undelivered_records: 7,
            oldest_created_ms: Some(1_000),
            active_created_ms: vec![(0, 2_000)],
            dead_letter_bytes: 9,
            dead_letter_files: 1,
            quarantine_bytes: 0,
            quarantine_files: 0,
            scanned_at_ms: 3_000,
        });
        let e = b.evidence_at(10_000);
        assert!(e.drain.is_some() && e.sink.is_some());
        assert_eq!(
            e.channel,
            Some(ChannelEvidence {
                depth: 240,
                capacity: 256,
                high_since_unix_ms: Some(5)
            })
        );
        assert_eq!(e.queue_depth, Some(3));
        let backlog = e.backlog.unwrap();
        assert_eq!(
            (
                backlog.undelivered_records,
                backlog.oldest_undelivered_age_secs
            ),
            (7, Some(9.0))
        );
        assert_eq!(e.shards.unwrap()[0].active_segment_age_secs, Some(8.0));
    }

    #[test]
    fn admin_doc_renders_the_daemon_identity() {
        let b = crate::status::test_board(1, 1);
        // Through the crate::status re-export PR 5 / PR 6 name.
        let snap: crate::status::StatusSnapshot = b.snapshot(std::time::SystemTime::now());
        let ev = evaluate(&snap.evidence(), &Thresholds::DEFAULT, &b.latches);
        let doc = snap.admin_doc(&ev);
        assert_eq!(doc.view, View::Admin);
        assert_eq!(doc.instance.socket_path, "/run/weir/weir.sock");
        assert_eq!(doc.pid, std::process::id());
        assert!(!doc.verdict.ready, "a board that never served is not ready");
    }

    /// `/status` (PR 5) and the admin `status` verb (PR 6) render through this
    /// one function; its verdict is the same predicate `/readyz` uses (G2),
    /// and it carries the configured liveness checks.
    #[test]
    fn render_admin_doc_shares_readyz_s_predicate_and_carries_the_livez_checks() {
        // A serving board, so the verdict is exercised on both sides: a
        // never-served board is not ready either way, which would let a
        // hard-coded `ready: false` pass.
        let b = crate::status::test_board(1, 1);
        b.shards.mark_up(0);
        std::mem::forget(crate::status::WorkerGuard::enter(
            std::sync::Arc::clone(&b.shards),
            0,
        ));
        b.listeners.unix.set(ListenerState::Bound);
        b.life.enter(Phase::Serving, crate::status::unix_ms_now());
        b.wab.note_scan_ok(crate::status::unix_ms_now());
        let now = std::time::SystemTime::now();

        let ev = b.snapshot(now).evidence();
        assert!(
            readiness(&ev).ok,
            "precondition: a serving board is ready: {:?}",
            readiness(&ev).failing
        );
        let doc = render_admin_doc(&b, &Thresholds::DEFAULT, &[LivezCheck::NoLiveShard], now);
        assert!(doc.verdict.ready);
        assert_eq!(doc.verdict.livez_checks, vec![LivezCheck::NoLiveShard]);
        assert_eq!(doc.view, View::Admin);

        b.wab_cap_rejecting.store(true, Ordering::Relaxed);
        let ev = b.snapshot(now).evidence();
        let doc = render_admin_doc(&b, &Thresholds::DEFAULT, &[LivezCheck::NoLiveShard], now);
        assert!(!readiness(&ev).ok);
        assert!(!doc.verdict.ready);
        assert_eq!(doc.verdict.readiness_failing, readiness(&ev).failing);
    }

    /// Spec §16 "concurrent writer/reader stress test": writers hammer every
    /// cell while readers build evidence. Nothing blocks, nothing panics,
    /// and readers see writes. Progress, not time, ends each loop: every
    /// thread does at least 1,000 iterations (readers: and has seen the
    /// drain publication), so a thread the scheduler starts late still
    /// passes; a 10 s deadline turns real starvation into a failure.
    #[test]
    fn concurrent_writers_and_readers_never_block_each_other() {
        let b = crate::status::test_board(4, 2);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = Vec::new();
        for t in 0..2 {
            let (b, stop) = (b.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let mut i = 0u64;
                while i < 1_000 || !stop.load(Ordering::Relaxed) {
                    i += 1;
                    if t == 0 {
                        b.drain.set_state(if i.is_multiple_of(2) {
                            DrainStateValue::draining
                        } else {
                            DrainStateValue::retrying_transient
                        });
                        b.drain.commit_started();
                        b.drain.commit_finished();
                        b.sink
                            .record_failure(SinkFailureKind::Transient, format!("e{i}"));
                    } else {
                        b.shards.mark_up((i % 4) as usize);
                        b.shards.mark_respawning(((i + 1) % 4) as usize);
                        b.drain_depth.store((i % 256) as usize, Ordering::Relaxed);
                        b.life.enter(Phase::Serving, i);
                    }
                }
                i
            }));
        }
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let (b, stop) = (b.clone(), stop.clone());
                std::thread::spawn(move || {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    let (mut saw_drain, mut reads) = (false, 0u64);
                    loop {
                        let e = b.evidence_at(super::super::unix_ms_now());
                        let _ = evaluate(&e, &Thresholds::DEFAULT, &b.latches);
                        saw_drain |= e.drain.is_some();
                        reads += 1;
                        if saw_drain && reads >= 1_000 && stop.load(Ordering::Relaxed) {
                            break saw_drain;
                        }
                        assert!(
                            std::time::Instant::now() < deadline,
                            "reader starved for 10 s"
                        );
                    }
                })
            })
            .collect();
        // Minimum overlap only; correctness no longer depends on it.
        std::thread::sleep(std::time::Duration::from_millis(200));
        stop.store(true, Ordering::Relaxed);
        for t in threads {
            assert!(t.join().unwrap() > 0, "every writer made progress");
        }
        for r in readers {
            assert!(
                r.join().unwrap(),
                "readers observed the writers' publications"
            );
        }
    }
}
