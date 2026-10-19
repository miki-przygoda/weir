//! The status board as the HTTP router's source (spec §5.8, §10, §12).
//!
//! Loads and a pure evaluation only: no lock, no I/O, no await (§9.2).
//! Readiness and liveness come from `weir_status::{readiness, liveness,
//! startup}`, the same predicates `weir_ready`, `weir_live`, admin `status`
//! and offline `weir-ctl status` use (G2), evaluated live on every request.

use std::time::SystemTime;

use weir_status::{Evidence, LivezCheck, Phase, PublicStatusDoc, Thresholds};

use crate::metrics::router::{Endpoint, ProbeAnswer, StartupFacts, StatusSource};
use crate::status::StatusBoard;

/// What `/startupz` prints while startup is not complete: the phase, its
/// age in whole seconds and the replay count still ahead of the drain.
pub(crate) fn startup_facts(ev: &Evidence) -> StartupFacts {
    StartupFacts {
        phase: ev.phase.map_or("unknown", Phase::as_str),
        phase_age_secs: ev
            .phase_since_unix_ms
            .map_or(0, |since| ev.now_unix_ms.saturating_sub(since) / 1000),
        replay_remaining: ev.drain.as_ref().map(|d| d.replay_remaining),
    }
}

impl StatusSource for StatusBoard {
    fn probe(
        &self,
        endpoint: Endpoint,
        now: SystemTime,
        checks: &[LivezCheck],
        t: &Thresholds,
    ) -> ProbeAnswer {
        let ev = self.snapshot(now).evidence();
        let verdict = match endpoint {
            Endpoint::Livez => weir_status::liveness(&ev, t, checks),
            Endpoint::Readyz => weir_status::readiness(&ev),
            Endpoint::Startupz | Endpoint::Status => weir_status::startup(&ev),
        };
        // The 200 path is a handful of loads; summaries are rendered only
        // for a failing probe, and only for its failing codes.
        let summaries = if verdict.ok {
            Vec::new()
        } else {
            weir_status::evaluate(&ev, t, &self.latches)
                .conditions
                .into_iter()
                .filter(|c| verdict.failing.contains(&c.code))
                .map(|c| (c.code, c.summary))
                .collect()
        };
        ProbeAnswer {
            verdict,
            summaries,
            startup: startup_facts(&ev),
        }
    }

    fn public_doc(
        &self,
        now: SystemTime,
        checks: &[LivezCheck],
        t: &Thresholds,
    ) -> PublicStatusDoc {
        // PR 1's one admin rendering (the admin `status` verb, PR 6, uses
        // the same function), then the type-enforced public projection.
        weir_status::public(&crate::status::render::render_admin_doc(
            self, t, checks, now,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::SystemTime;

    use weir_status::{ConditionCode, ListenerState, LivezCheck, Phase, Thresholds};

    use crate::metrics::router::{
        Endpoint, StatusSource, render_livez, render_readyz, render_startupz,
    };
    use crate::sink::SinkHealth;
    use crate::status::{StatusBoard, SupervisorExit, WorkerGuard, test_board};

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// A board after a clean PR 4 startup: phase serving, unix listener bound,
    /// every shard Up and every worker running.
    fn serving_board(shards: usize, workers: usize) -> Arc<StatusBoard> {
        let b = test_board(shards, workers);
        for id in 0..shards {
            b.shards.mark_up(id);
        }
        for w in 0..workers {
            // Keep each worker "running" for the test's life (PR 1's poll tests do the same).
            std::mem::forget(WorkerGuard::enter(Arc::clone(&b.shards), w));
        }
        b.listeners.unix.set(ListenerState::Bound);
        b.life.enter(Phase::Serving, now_ms()); // also sets ever_served
        b
    }

    fn body(r: crate::metrics::router::Response) -> (u16, String) {
        (r.status, String::from_utf8(r.body).unwrap())
    }

    fn readyz(b: &StatusBoard) -> (u16, String) {
        body(render_readyz(
            &b.probe(
                Endpoint::Readyz,
                SystemTime::now(),
                &[],
                &Thresholds::default(),
            ),
            false,
        ))
    }

    #[test]
    fn a_fresh_board_is_live_but_neither_ready_nor_started() {
        let b = test_board(2, 2);
        let t = Thresholds::default();
        let checks = [LivezCheck::NoLiveShard, LivezCheck::ListenerDown];
        // Liveness checks run only while serving: startup never fails /livez (spec §10, §12).
        assert_eq!(
            body(render_livez(
                &b.probe(Endpoint::Livez, SystemTime::now(), &checks, &t),
                false
            )),
            (200, "ok\n".into())
        );
        let (s, text) = readyz(&b);
        assert_eq!(s, 503);
        assert!(
            text.starts_with("not ready: ") && text.contains("starting"),
            "{text}"
        );
        let (s, text) = body(render_startupz(&b.probe(
            Endpoint::Startupz,
            SystemTime::now(),
            &[],
            &t,
        )));
        assert_eq!(s, 503);
        assert!(
            text.starts_with("starting: phase=initializing since="),
            "{text}"
        );
    }

    #[test]
    fn a_serving_board_is_ready_and_stays_started_through_shutdown() {
        let b = serving_board(2, 2);
        assert_eq!(readyz(&b), (200, "ok\n".into()));
        b.life.begin_shutdown(now_ms());
        let (s, text) = readyz(&b);
        assert_eq!(s, 503);
        assert!(text.contains("shutting_down"), "{text}");
        // /startupz is sticky once served (spec §10); /livez never fails on shutdown.
        let t = Thresholds::default();
        assert_eq!(
            body(render_startupz(&b.probe(
                Endpoint::Startupz,
                SystemTime::now(),
                &[],
                &t
            )))
            .0,
            200
        );
        assert_eq!(
            body(render_livez(
                &b.probe(
                    Endpoint::Livez,
                    SystemTime::now(),
                    &[LivezCheck::NoLiveShard],
                    &t
                ),
                false
            ))
            .0,
            200
        );
    }

    #[test]
    fn readiness_is_ingest_capability_only() {
        // Cap rejecting gates readiness ...
        let b = serving_board(1, 1);
        b.wab_cap_rejecting.store(true, Ordering::Relaxed);
        let (s, text) = readyz(&b);
        assert_eq!(s, 503);
        assert!(text.contains("wab_cap_rejecting"), "{text}");
        // ... as does losing every shard ...
        let b = serving_board(2, 2);
        b.shards.mark_exit(0, SupervisorExit::RespawnExhausted);
        assert_eq!(
            readyz(&b).0,
            200,
            "one live shard is enough (owner decision 4)"
        );
        b.shards.mark_exit(1, SupervisorExit::CounterScanFailed);
        let (s, text) = readyz(&b);
        assert_eq!(s, 503);
        assert!(text.contains("no_live_shard"), "{text}");
        // ... and a failed producer listener ...
        let b = serving_board(1, 1);
        b.listeners.unix.set(ListenerState::Failed);
        assert!(readyz(&b).1.contains("listener_down"));
        // ... but a down sink never does (spec §12).
        let b = serving_board(1, 1);
        b.sink
            .last_error
            .store(Some(Arc::new("connection refused".into())));
        b.sink.set_health(
            &SinkHealth::Down("connection refused".into()),
            Some("connection refused".into()),
        );
        assert_eq!(readyz(&b), (200, "ok\n".into()));
    }

    #[test]
    fn liveness_fails_only_on_selected_restart_fixable_checks() {
        let b = serving_board(1, 1);
        b.shards.mark_exit(0, SupervisorExit::InputClosed); // not shutting down: Offline(FlusherExited)
        let t = Thresholds::default();
        let none = b.probe(Endpoint::Livez, SystemTime::now(), &[], &t);
        assert_eq!(
            body(render_livez(&none, false)),
            (200, "ok\n".into()),
            "livez_checks = [] never fails"
        );
        let sel = b.probe(
            Endpoint::Livez,
            SystemTime::now(),
            &[LivezCheck::NoLiveShard],
            &t,
        );
        let (s, text) = body(render_livez(&sel, true));
        assert_eq!(s, 503);
        assert!(text.contains("[-]no_live_shard failed: "), "{text}");
        assert!(text.ends_with("not live: no_live_shard\n"), "{text}");
        assert!(
            sel.summaries
                .iter()
                .any(|(c, _)| *c == ConditionCode::NoLiveShard)
        );
    }

    /// Spec §16 sentinel test, board half: a sentinel in every String-typed
    /// board field (and the pause uid) goes through every board writer, then
    /// through `render_admin_doc` and the evaluator, and reaches neither
    /// `/status` nor the `?verbose` probe lines. Task 5.1 covers `public()` on a
    /// pre-built document; this covers the leaks only a live board can show.
    #[test]
    fn board_free_text_never_reaches_status_or_verbose_probes() {
        const S: &str = "SENTINEL-board-free-text";
        const UID: u32 = 2_718_281_828;
        let b = StatusBoard::new(
            crate::status::board::StaticInfo {
                wab_dir: std::path::PathBuf::from(format!("/{S}-wab")),
                socket_path: std::path::PathBuf::from(format!("/{S}-sock")),
                ..crate::status::board::StaticInfo::for_test()
            },
            1,
            1,
            false,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        );
        b.shards.mark_up(0);
        std::mem::forget(WorkerGuard::enter(Arc::clone(&b.shards), 0));
        b.life.enter(Phase::Serving, now_ms());
        b.listeners.unix.set(ListenerState::Failed);
        b.sink.set_health(
            &SinkHealth::Down(format!("{S} down")),
            Some(format!("{S} probe_reason")),
        );
        b.sink.record_failure(
            weir_status::SinkFailureKind::Transient,
            format!("{S} commit error"),
        );
        b.drain.set_state(crate::metrics::DrainStateValue::draining);
        b.drain.set_pause_requested(true, Some(UID));
        b.drain.set_last_requeue(crate::status::RequeueResult {
            ticket: 1,
            requeued: 0,
            already_queued: 0,
            scan_error: Some(format!("{S} scan_error")),
            backoff_cut: false,
            coalesced: false,
            at_ms: now_ms(),
        });
        b.drain.set_exit(crate::status::DrainExitInfo {
            reason: crate::status::DrainExitReason::GaveUp,
            abandoned: None,
            pending_left: 0,
            backlog_left: 0,
            last_panic: Some(format!("{S} panic")),
            at_ms: now_ms(),
        });
        // PR 6 Task 6.8 adds `at_unix_ms` to `ReloadReport`; it adds `at_unix_ms: 0` here.
        b.reload.publish(
            crate::reload::ReloadReport {
                trigger: crate::reload::ReloadTrigger::Sighup,
                outcome: crate::reload::ReloadOutcome::Failed,
                changed: vec![],
                rejected: vec![("tls", format!("{S} reload detail"))],
                note: None,
            },
            now_ms(),
        );
        let t = Thresholds::default();
        let checks = [LivezCheck::ListenerDown, LivezCheck::DrainStopped];
        let doc = serde_json::to_string(&b.public_doc(SystemTime::now(), &checks, &t)).unwrap();
        assert!(
            !doc.contains("SENTINEL") && !doc.contains(&UID.to_string()),
            "{doc}"
        );
        for (endpoint, r) in [
            (
                Endpoint::Livez,
                render_livez(
                    &b.probe(Endpoint::Livez, SystemTime::now(), &checks, &t),
                    true,
                ),
            ),
            (
                Endpoint::Readyz,
                render_readyz(
                    &b.probe(Endpoint::Readyz, SystemTime::now(), &checks, &t),
                    true,
                ),
            ),
        ] {
            let text = String::from_utf8(r.body).unwrap();
            assert!(
                !text.contains("SENTINEL") && !text.contains(&UID.to_string()),
                "{endpoint:?}: {text}"
            );
        }
        // Vacuity guard: the admin rendering of the same board carries every
        // planted value, so each writer above really reached the document.
        let admin = serde_json::to_string(&crate::status::render::render_admin_doc(
            &b,
            &t,
            &checks,
            SystemTime::now(),
        ))
        .unwrap();
        for needle in [
            format!("{S} panic"),
            format!("{S} scan_error"),
            format!("{S} reload detail"),
            format!("/{S}-wab"),
            format!("{S} commit error"),
            UID.to_string(),
        ] {
            assert!(admin.contains(&needle), "admin doc lacks {needle}: {admin}");
        }
        assert!(
            doc.contains("\"livez_checks\":[\"listener_down\",\"drain_stopped\"]"),
            "verdict carries the selected checks: {doc}"
        );
    }
}
