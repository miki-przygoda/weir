//! The admin verbs (spec §11) and the state they act on. `status` reads the
//! board; `reload` goes through `reload.rs`'s single loop; the drain verbs
//! store intent in `DrainControl` and wait for the drain's observation on
//! the board. Neither the drain nor the Lifecycle ever awaits a client.

use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use serde_json::{Map, Value};
use tokio::sync::{Mutex, mpsc, watch};
use weir_status::{
    LivezCheck, Thresholds,
    admin::{
        AdminError, DEFAULT_PAUSE_WAIT_MS, ErrorCode, HeldSegment, InstanceId, PAUSE_NOTE,
        PauseResult, ResumeResult, Verb, WARN_UNBOUNDED_WAB,
    },
};

use super::framing::wait_arg;
use crate::{
    drain::control::DrainControl,
    lifecycle::ShutdownLevel,
    metrics::{AdminVerbLabel, Metrics},
    reload::{self, ReloadRequest, ReloadTrigger, ReloadWaitError},
    status::{DrainExitReason, RequeueResult, StatusBoard, unix_ms_now},
};

/// How often a mutating verb re-reads the board while it waits (spec §11).
pub(crate) const OBSERVE_POLL: Duration = Duration::from_millis(20);

/// Polls `done` every [`OBSERVE_POLL`] until it holds or `wait` passes.
/// Checks once even when `wait` is zero. Returns whether `done` held.
pub(crate) async fn wait_until(wait: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if done() {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::time::sleep((deadline - now).min(OBSERVE_POLL)).await;
    }
}

/// Everything the drain verbs read from the board. [`drain_obs`] is the ONLY
/// code in the admin module that names `DrainCell` fields (PR 1 / PR 2), so a
/// rename there is a one-function edit here.
struct DrainObs {
    generation: u64,
    paused_since_ms: u64,
    held: bool,
    held_segment: Option<HeldSegment>,
    observed_requeue: u64,
    last_requeue: Option<Arc<RequeueResult>>,
    gave_up: bool,
}

impl DrainObs {
    /// Paused at the Draining head, or held mid-segment (spec §8).
    fn paused(&self) -> bool {
        self.paused_since_ms != 0 || self.held
    }
}

/// Loads only (atomics and `ArcSwap`): never blocks the drain.
fn drain_obs(board: &StatusBoard) -> DrainObs {
    let d = &board.drain;
    let held = d.held.load(Ordering::Acquire);
    DrainObs {
        generation: d.generation.load(Ordering::Acquire),
        paused_since_ms: d.paused_since_ms.load(Ordering::Acquire),
        held,
        held_segment: if held {
            d.in_flight.load_full().map(|f| HeldSegment {
                segment: f.segment.clone(),
                processed_records: f.processed,
            })
        } else {
            None
        },
        observed_requeue: d.observed_requeue.load(Ordering::Acquire),
        last_requeue: d.last_requeue.load_full(),
        gave_up: d
            .exit
            .load_full()
            .is_some_and(|x| x.reason == DrainExitReason::GaveUp),
    }
}

/// Everything the admin verbs act on. Built once in `main` (Task 6.14) and
/// shared by every connection as `Arc<AdminState>`.
pub(crate) struct AdminState {
    pub(crate) board: Arc<StatusBoard>,
    pub(crate) drain_ctl: DrainControl,
    /// A clone of the reload queue SIGHUP feeds (spec §5.10: one path).
    pub(crate) reload_tx: mpsc::Sender<ReloadRequest>,
    /// The Lifecycle's shutdown level (plan reconciliation 6: the level, not
    /// the Lifecycle itself).
    pub(crate) level_rx: watch::Receiver<ShutdownLevel>,
    pub(crate) thresholds: Arc<Thresholds>,
    pub(crate) livez_checks: Arc<[LivezCheck]>,
    pub(crate) instance: InstanceId,
    pub(crate) metrics: Arc<Metrics>,
    /// For the pause warning (spec §11): 0 means the WAB is unbounded.
    pub(crate) wab_max_bytes: u64,
    pub(crate) daemon_euid: u32,
    /// The admin mutation guard (spec §8): held only for the intent store,
    /// released before any observation wait.
    pub(crate) mutate: Mutex<()>,
}

/// The metric label for a parsed verb: the exact verb string (Task 6.3).
pub(crate) fn verb_label(v: Verb) -> AdminVerbLabel {
    match v {
        Verb::Status => AdminVerbLabel::Status,
        Verb::Reload => AdminVerbLabel::Reload,
        Verb::DrainPause => AdminVerbLabel::DrainPause,
        Verb::DrainResume => AdminVerbLabel::DrainResume,
        Verb::DrainRetryStranded => AdminVerbLabel::DrainRetryStranded,
    }
}

fn encode<T: serde::Serialize>(v: &T) -> Result<Value, AdminError> {
    serde_json::to_value(v).map_err(|e| {
        AdminError::new(
            ErrorCode::Internal,
            format!("could not encode the result: {e}"),
        )
    })
}

impl AdminState {
    /// The admin view of the status document: the same model and evaluator
    /// as `/status` (PR 1's renderer, PR 5's route). Loads only: never blocks
    /// the drain, a flusher or an ack.
    fn status(&self) -> Result<Value, AdminError> {
        let doc = crate::status::render::render_admin_doc(
            &self.board,
            &self.thresholds,
            &self.livez_checks,
            SystemTime::now(),
        );
        encode(&doc)
    }

    /// Runs one reload through the loop SIGHUP uses and answers with its
    /// wire report. A failed TLS reload is a successful request whose
    /// outcome is `failed` (spec §11); only a reload that could not run, or
    /// whose answer never came, is an error.
    async fn reload(&self) -> Result<Value, AdminError> {
        let report = reload::request_reload_and_wait(
            &self.reload_tx,
            ReloadTrigger::Admin,
            &self.metrics,
            reload::ADMIN_ENQUEUE_TIMEOUT,
            reload::ADMIN_REPLY_TIMEOUT,
        )
        .await
        .map_err(|e| match e {
            ReloadWaitError::QueueFull => AdminError::new(
                ErrorCode::Busy,
                "the reload queue is full (a reload burst is in progress); try again",
            ),
            ReloadWaitError::LoopGone => {
                AdminError::new(ErrorCode::Unavailable, "the reload loop has stopped")
            }
            ReloadWaitError::NoAnswer => AdminError::new(
                ErrorCode::Internal,
                "the reload did not complete; the previous configuration stays in force",
            ),
            ReloadWaitError::TimedOut => AdminError::new(
                ErrorCode::Internal,
                format!(
                    "no reload result within {} s; it may still complete, see `status`",
                    reload::ADMIN_REPLY_TIMEOUT.as_secs()
                ),
            ),
        })?;
        encode(&report.to_wire())
    }

    /// spec §8 "Refusals": after T0 → `shutting_down`; the drain gave up →
    /// `unavailable`. Checked before any intent is stored.
    fn refuse_mutation(&self) -> Result<(), AdminError> {
        if *self.level_rx.borrow() != ShutdownLevel::Running {
            return Err(AdminError::new(
                ErrorCode::ShuttingDown,
                "the daemon is shutting down; drain verbs are refused (status and reload still work)",
            ));
        }
        if drain_obs(&self.board).gave_up {
            return Err(AdminError::new(
                ErrorCode::Unavailable,
                "the drain stopped after exhausting its restart budget; see `status`",
            ));
        }
        Ok(())
    }

    /// Stores the pause intent (and its uid on the board, admin view only),
    /// releases the guard, then waits up to `wait_ms` for the drain to be
    /// observed paused or held. An unobserved pause is still a success with
    /// `paused: false`: the intent stays set.
    async fn drain_pause(
        &self,
        args: &Map<String, Value>,
        peer_uid: u32,
    ) -> Result<Value, AdminError> {
        let wait = wait_arg(args, DEFAULT_PAUSE_WAIT_MS)?;
        self.refuse_mutation()?;
        {
            let _g = self.mutate.lock().await;
            self.drain_ctl.request_pause();
            // PR 1's mirror of the intent and its uid (admin view only); it
            // also answers PR 2's question of who puts `pause_requested` on
            // the board: the admin server does.
            self.board.drain.set_pause_requested(true, Some(peer_uid));
        } // released BEFORE the wait (spec §8)
        wait_until(wait, || drain_obs(&self.board).paused()).await;
        let o = drain_obs(&self.board);
        let mut warnings = Vec::new();
        if self.wab_max_bytes == 0 {
            warnings.push(WARN_UNBOUNDED_WAB.to_string());
        }
        encode(&PauseResult {
            generation: o.generation,
            pause_requested: self.drain_ctl.pause_requested(),
            paused: o.paused(),
            held: o.held,
            paused_since_ms: (o.paused_since_ms != 0).then_some(o.paused_since_ms),
            held_segment: o.held_segment,
            warnings,
            note: PAUSE_NOTE.to_string(),
        })
    }

    /// Clears the pause intent, releases the guard, then waits up to
    /// `wait_ms` for the drain to be observed neither paused nor held.
    async fn drain_resume(&self, args: &Map<String, Value>) -> Result<Value, AdminError> {
        let wait = wait_arg(args, DEFAULT_PAUSE_WAIT_MS)?;
        self.refuse_mutation()?;
        let (was_paused, since) = {
            let _g = self.mutate.lock().await;
            let was = self.drain_ctl.pause_requested();
            let since = drain_obs(&self.board).paused_since_ms;
            self.drain_ctl.request_resume();
            self.board.drain.set_pause_requested(false, None);
            (was, since)
        };
        let resumed = wait_until(wait, || !drain_obs(&self.board).paused()).await;
        let o = drain_obs(&self.board);
        encode(&ResumeResult {
            generation: o.generation,
            pause_requested: self.drain_ctl.pause_requested(),
            was_paused,
            paused_for_secs: (since != 0)
                .then(|| unix_ms_now().saturating_sub(since) as f64 / 1000.0),
            resumed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::test_support::{Harness, ReloadMode, args, harness, uid};
    use crate::metrics::{ConfigReloadLabel, ConfigReloadOutcome, ConfigReloadTrigger};
    use crate::status::unix_ms_now;
    use serde_json::json;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[tokio::test]
    async fn status_is_the_admin_view_of_the_status_document() {
        let h = harness(ReloadMode::Live);
        let doc = h.st.status().unwrap();
        assert_eq!(doc["schema"], "weir.status/v1");
        assert_eq!(doc["view"], "admin");
    }

    #[tokio::test]
    async fn reload_goes_through_the_one_reload_loop() {
        let h = harness(ReloadMode::Live);
        let res = h.st.reload().await.unwrap();
        assert_eq!(res["trigger"], "admin");
        assert_eq!(res["outcome"], "noop");
        assert_eq!(res["items"][0]["item"], "tls_certificates");
        assert_eq!(res["items"][0]["outcome"], "not_configured");
        assert_eq!(res["not_reloaded"][0]["item"], "config_file");
        let n =
            h.st.metrics
                .config_reloads
                .get_or_create(&ConfigReloadLabel {
                    trigger: ConfigReloadTrigger::admin,
                    outcome: ConfigReloadOutcome::noop,
                })
                .get();
        assert_eq!(n, 1, "counted once, under trigger=admin");
    }

    #[tokio::test]
    async fn reload_on_a_stalled_loop_is_busy_and_retryable() {
        let h = harness(ReloadMode::Stalled);
        for _ in 0..reload::RELOAD_QUEUE_DEPTH {
            assert!(reload::request_reload(
                &h.st.reload_tx,
                ReloadTrigger::Sighup,
                &h.st.metrics
            ));
        }
        let e = h.st.reload().await.unwrap_err(); // waits ADMIN_ENQUEUE_TIMEOUT (1 s)
        assert_eq!(e.code, ErrorCode::Busy);
        assert!(e.retryable);
    }

    /// Stands in for the drain thread: observes pause intent on the board the
    /// way PR 2's Draining head does, every 5 ms.
    fn fake_drain(h: &Harness) -> tokio::task::JoinHandle<()> {
        let st = Arc::clone(&h.st);
        tokio::spawn(async move {
            loop {
                let d = &st.board.drain;
                if st.drain_ctl.pause_requested() {
                    if d.paused_since_ms.load(Ordering::Acquire) == 0 {
                        d.paused_since_ms.store(unix_ms_now(), Ordering::Release);
                    }
                } else {
                    d.paused_since_ms.store(0, Ordering::Release);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    #[tokio::test]
    async fn pause_is_observed_and_reported_with_its_caveats() {
        let h = harness(ReloadMode::Live);
        let drain = fake_drain(&h);
        let res =
            h.st.drain_pause(&args(json!({"wait_ms": 2000})), uid())
                .await
                .unwrap();
        assert_eq!(res["pause_requested"], true);
        assert_eq!(res["paused"], true);
        assert!(res["paused_since_ms"].as_u64().unwrap() > 0);
        assert_eq!(res["held"], false);
        assert_eq!(res["held_segment"], Value::Null);
        assert!(res["note"].as_str().unwrap().contains("in-memory only"));
        assert_eq!(
            res["warnings"],
            json!([weir_status::admin::WARN_UNBOUNDED_WAB]),
            "wab_max_bytes = 0 in the harness"
        );
        assert_eq!(
            h.st.board.drain.paused_by_uid.load(Ordering::Acquire),
            u64::from(uid())
        );
        assert!(
            h.st.board.drain.pause_requested.load(Ordering::Acquire),
            "the intent is mirrored on the board"
        );
        drain.abort();
    }

    #[tokio::test]
    async fn an_unobserved_pause_returns_at_its_deadline() {
        let h = harness(ReloadMode::Live); // no drain: nothing observes
        let t0 = std::time::Instant::now();
        let res =
            h.st.drain_pause(&args(json!({"wait_ms": 0})), uid())
                .await
                .unwrap();
        assert!(
            t0.elapsed() < Duration::from_millis(100),
            "{:?}",
            t0.elapsed()
        );
        assert_eq!(res["pause_requested"], true);
        assert_eq!(
            res["paused"], false,
            "requested, not yet effective (weir-ctl exits 4)"
        );
        assert!(h.st.drain_ctl.pause_requested(), "the intent stays set");
    }

    /// spec §8: the guard is released before the observation wait, so a
    /// long-waiting pause never delays a resume.
    #[tokio::test]
    async fn a_waiting_pause_does_not_delay_resume() {
        let h = harness(ReloadMode::Live);
        let st = Arc::clone(&h.st);
        let pause = tokio::spawn(async move {
            st.drain_pause(&args(json!({"wait_ms": 30000})), uid())
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let t0 = std::time::Instant::now();
        let res =
            h.st.drain_resume(&args(json!({"wait_ms": 0})))
                .await
                .unwrap();
        assert!(
            t0.elapsed() < Duration::from_millis(200),
            "resume took {:?}",
            t0.elapsed()
        );
        assert_eq!(res["was_paused"], true);
        assert!(!h.st.drain_ctl.pause_requested(), "last writer wins");
        pause.abort();
    }

    #[tokio::test]
    async fn resume_reports_how_long_it_was_paused() {
        let h = harness(ReloadMode::Live);
        let drain = fake_drain(&h);
        h.st.drain_pause(&args(json!({"wait_ms": 2000})), uid())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let res =
            h.st.drain_resume(&args(json!({"wait_ms": 2000})))
                .await
                .unwrap();
        assert_eq!(res["pause_requested"], false);
        assert_eq!(res["was_paused"], true);
        assert_eq!(res["resumed"], true);
        assert!(res["paused_for_secs"].as_f64().unwrap() >= 0.1, "{res}");
        assert_eq!(
            h.st.board.drain.paused_by_uid.load(Ordering::Acquire),
            u64::MAX,
            "PR 1's NO_UID: no pause in force"
        );
        drain.abort();
    }

    /// spec §7 (f) / §8: after T0 the drain verbs refuse, storing no intent.
    #[tokio::test]
    async fn pause_and_resume_are_refused_after_t0() {
        let h = harness(ReloadMode::Live);
        h.level_tx.send(ShutdownLevel::Graceful).unwrap();
        let e =
            h.st.drain_pause(&args(json!({"wait_ms": 0})), uid())
                .await
                .unwrap_err();
        assert_eq!(e.code, ErrorCode::ShuttingDown);
        assert!(
            !h.st.drain_ctl.pause_requested(),
            "a refused pause stores no intent"
        );
        let e =
            h.st.drain_resume(&args(json!({"wait_ms": 0})))
                .await
                .unwrap_err();
        assert_eq!(e.code, ErrorCode::ShuttingDown);
    }

    #[tokio::test]
    async fn drain_verbs_are_unavailable_once_the_drain_gave_up() {
        let h = harness(ReloadMode::Live);
        h.st.board
            .drain
            .exit
            .store(Some(Arc::new(crate::status::DrainExitInfo {
                reason: crate::status::DrainExitReason::GaveUp,
                abandoned: None,
                pending_left: 0,
                backlog_left: 0,
                last_panic: None,
                at_ms: unix_ms_now(),
            })));
        let e =
            h.st.drain_pause(&args(json!({"wait_ms": 0})), uid())
                .await
                .unwrap_err();
        assert_eq!(e.code, ErrorCode::Unavailable);
        assert!(e.retryable);
    }

    #[tokio::test]
    async fn wait_ms_out_of_range_is_a_bad_request_and_stores_nothing() {
        let h = harness(ReloadMode::Live);
        let e =
            h.st.drain_pause(&args(json!({"wait_ms": 30001})), uid())
                .await
                .unwrap_err();
        assert_eq!(e.code, ErrorCode::BadRequest);
        assert!(!h.st.drain_ctl.pause_requested());
    }
}
