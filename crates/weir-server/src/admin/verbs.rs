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
use tracing::{debug, info};
use weir_status::{
    LivezCheck, Thresholds,
    admin::{
        AdminError, DEFAULT_PAUSE_WAIT_MS, DEFAULT_RETRY_WAIT_MS, ErrorCode, HeldSegment,
        InstanceId, PAUSE_NOTE, PauseResult, RETRY_NOTE, Response, ResumeResult,
        RetryStrandedResult, Verb, WARN_UNBOUNDED_WAB,
    },
};

use super::framing::{Dispatch, parse_request, wait_arg};
use crate::{
    drain::control::DrainControl,
    lifecycle::ShutdownLevel,
    metrics::{AdminOutcomeLabel, AdminRequestLabel, AdminVerbLabel, Metrics},
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
    /// One increment of `weir_admin_requests_total{verb,outcome}`. Every
    /// label set is pre-created by `Metrics::new` (Task 6.3), so this never
    /// adds a series.
    fn count(&self, verb: AdminVerbLabel, outcome: AdminOutcomeLabel) {
        self.metrics
            .admin_requests
            .get_or_create(&AdminRequestLabel { verb, outcome })
            .inc();
    }

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

    /// Asks the drain for a stranded-segment rescan and waits up to
    /// `wait_ms` for its ticket to be observed. A request made while an
    /// earlier ticket is unobserved folds into it (spec §5.4 "Coalescing"),
    /// so a client looping on this verb never queues more than one scan.
    async fn drain_retry_stranded(&self, args: &Map<String, Value>) -> Result<Value, AdminError> {
        let wait = wait_arg(args, DEFAULT_RETRY_WAIT_MS)?;
        self.refuse_mutation()?;
        let ticket = {
            let _g = self.mutate.lock().await;
            self.drain_ctl.request_requeue()
        }; // released BEFORE the wait (spec §8)
        let completed =
            wait_until(wait, || drain_obs(&self.board).observed_requeue >= ticket).await;
        let o = drain_obs(&self.board);
        // The result that answered this ticket: its own, or a later scan's
        // (coalesced). The drain stores `last_requeue` before it bumps
        // `observed_requeue`, so a completed ticket always finds it. An
        // unobserved ticket reports no counts.
        let r = if completed {
            o.last_requeue.filter(|r| r.ticket >= ticket)
        } else {
            None
        };
        encode(&RetryStrandedResult {
            ticket,
            completed,
            coalesced: r.as_ref().is_some_and(|r| r.coalesced || r.ticket > ticket),
            requeued: r.as_ref().map(|r| r.requeued),
            already_queued: r.as_ref().map(|r| r.already_queued),
            backoff_cut: r.as_ref().is_some_and(|r| r.backoff_cut),
            scan_error: r.as_ref().and_then(|r| r.scan_error.clone()),
            paused: self.drain_ctl.pause_requested(),
            note: RETRY_NOTE.to_string(),
        })
    }
}

/// The one dispatcher (spec §11): parse the envelope, run the verb, count the
/// request, log it, and answer with the daemon's identity on every reply
/// (errors included; this peer already passed the uid check in the accept
/// loop). The metric carries only `ok | error`; the precise error code goes
/// to the log line (plan reconciliation 7).
impl Dispatch for AdminState {
    async fn dispatch(&self, line: &[u8], peer_uid: u32) -> Response {
        let req = match parse_request(line) {
            Ok(r) => r,
            Err((id, e)) => {
                self.count(AdminVerbLabel::Invalid, AdminOutcomeLabel::error);
                // DEBUG, and the code only: the request text is never logged.
                debug!(peer_uid, code = e.code.as_str(), "admin: invalid request");
                return Response::failure(id, Some(self.instance.clone()), e);
            }
        };
        let result = match req.verb {
            Verb::Status => self.status(),
            Verb::Reload => self.reload().await,
            Verb::DrainPause => self.drain_pause(&req.args, peer_uid).await,
            Verb::DrainResume => self.drain_resume(&req.args).await,
            Verb::DrainRetryStranded => self.drain_retry_stranded(&req.args).await,
        };
        let outcome = match &result {
            Ok(_) => "ok",
            Err(e) => e.code.as_str(),
        };
        self.count(
            verb_label(req.verb),
            if result.is_ok() {
                AdminOutcomeLabel::ok
            } else {
                AdminOutcomeLabel::error
            },
        );
        if req.verb == Verb::Status {
            // A poller may call `status` every few seconds: keep INFO quiet.
            debug!(peer_uid, outcome, "admin status");
        } else {
            // spec §11: every mutating verb is logged at INFO with
            // {verb, peer_uid, outcome}; reload is logged the same way.
            info!(verb = req.verb.as_str(), peer_uid, outcome, "admin request");
        }
        match result {
            Ok(v) => Response::success(req.id, self.instance.clone(), v),
            Err(e) => Response::failure(req.id, Some(self.instance.clone()), e),
        }
    }

    fn instance(&self) -> Option<InstanceId> {
        Some(self.instance.clone())
    }

    /// An oversized line or a missed request deadline never reaches
    /// [`parse_request`], so it is counted here, under `_invalid`.
    fn on_framing_error(&self, _code: ErrorCode) {
        self.count(AdminVerbLabel::Invalid, AdminOutcomeLabel::error);
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

    /// Stands in for the drain's requeue observation (PR 2): publishes a
    /// RequeueResult for each new ticket.
    fn fake_requeue_drain(h: &Harness) -> tokio::task::JoinHandle<()> {
        let st = Arc::clone(&h.st);
        tokio::spawn(async move {
            let mut observed = 0u64;
            loop {
                let t = st.drain_ctl.requeue_requested();
                if t > observed {
                    st.board
                        .drain
                        .last_requeue
                        .store(Some(Arc::new(RequeueResult {
                            ticket: t,
                            requeued: 3,
                            already_queued: 1,
                            scan_error: None,
                            backoff_cut: true,
                            coalesced: false,
                            at_ms: unix_ms_now(),
                        })));
                    st.board.drain.observed_requeue.store(t, Ordering::Release);
                    observed = t;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    #[tokio::test]
    async fn retry_stranded_reports_the_observed_requeue() {
        let h = harness(ReloadMode::Live);
        let drain = fake_requeue_drain(&h);
        let res = h.st.drain_retry_stranded(&args(json!({}))).await.unwrap();
        assert!(res["ticket"].as_u64().unwrap() >= 1);
        assert_eq!(res["completed"], true);
        assert_eq!(res["requeued"], 3);
        assert_eq!(res["already_queued"], 1);
        assert_eq!(res["backoff_cut"], true);
        assert_eq!(res["coalesced"], false);
        assert_eq!(res["scan_error"], Value::Null);
        assert_eq!(res["paused"], false);
        assert!(
            res["note"]
                .as_str()
                .unwrap()
                .contains("restart from record 0")
        );
        drain.abort();
    }

    #[tokio::test]
    async fn an_unobserved_retry_returns_its_ticket_without_counts() {
        let h = harness(ReloadMode::Live);
        let res =
            h.st.drain_retry_stranded(&args(json!({"wait_ms": 0})))
                .await
                .unwrap();
        assert!(res["ticket"].as_u64().unwrap() >= 1);
        assert_eq!(res["completed"], false);
        assert_eq!(res["requeued"], Value::Null);
        assert_eq!(res["already_queued"], Value::Null);
    }

    /// A request answered by a later scan reports itself coalesced, and the
    /// scan error (admin-only free text) is passed through.
    #[tokio::test]
    async fn a_retry_answered_by_a_newer_ticket_is_coalesced() {
        let h = harness(ReloadMode::Live);
        let first = h.st.drain_ctl.request_requeue();
        h.st.board
            .drain
            .last_requeue
            .store(Some(Arc::new(RequeueResult {
                ticket: first + 1,
                requeued: 0,
                already_queued: 2,
                scan_error: Some("permission denied on shard-0".into()),
                backoff_cut: false,
                coalesced: true,
                at_ms: unix_ms_now(),
            })));
        h.st.board
            .drain
            .observed_requeue
            .store(first + 1, Ordering::Release);
        let res =
            h.st.drain_retry_stranded(&args(json!({"wait_ms": 500})))
                .await
                .unwrap();
        assert_eq!(res["completed"], true);
        assert_eq!(res["coalesced"], true);
        assert_eq!(res["scan_error"], "permission denied on shard-0");
    }

    use crate::admin::test_support::{call, requests};
    use crate::metrics::AdminOutcomeLabel;

    #[tokio::test]
    async fn every_verb_answers_through_the_envelope_with_identity() {
        let h = harness(ReloadMode::Live);
        for (verb, extra) in [
            ("status", ""),
            ("reload", ""),
            ("drain.pause", r#","args":{"wait_ms":0}"#),
            ("drain.resume", r#","args":{"wait_ms":0}"#),
            ("drain.retry_stranded", r#","args":{"wait_ms":0}"#),
        ] {
            let r = call(
                &h.st,
                &format!(r#"{{"v":1,"id":"{verb}","verb":"{verb}"{extra}}}"#),
            )
            .await;
            assert!(r.ok, "{verb}: {r:?}");
            assert_eq!(r.id, json!(verb), "id echoed");
            assert_eq!(
                r.instance.as_ref().unwrap().pid,
                std::process::id(),
                "{verb}"
            );
        }
        for verb in [
            AdminVerbLabel::Status,
            AdminVerbLabel::Reload,
            AdminVerbLabel::DrainPause,
            AdminVerbLabel::DrainResume,
            AdminVerbLabel::DrainRetryStranded,
        ] {
            assert_eq!(
                requests(&h.st.metrics, verb, AdminOutcomeLabel::ok),
                1,
                "{verb:?}"
            );
        }
    }

    #[tokio::test]
    async fn errors_carry_identity_and_are_counted() {
        let h = harness(ReloadMode::Live);
        for line in [
            "not json",
            r#"{"v":2,"verb":"status"}"#,
            r#"{"v":1,"verb":"nope"}"#,
        ] {
            let r = call(&h.st, line).await;
            assert!(!r.ok, "{line}");
            assert!(r.instance.is_some(), "identity on every response: {line}");
        }
        assert_eq!(
            requests(
                &h.st.metrics,
                AdminVerbLabel::Invalid,
                AdminOutcomeLabel::error
            ),
            3
        );

        h.level_tx.send(ShutdownLevel::Graceful).unwrap();
        let r = call(
            &h.st,
            r#"{"v":1,"id":5,"verb":"drain.pause","args":{"wait_ms":0}}"#,
        )
        .await;
        assert_eq!(r.id, json!(5));
        assert_eq!(r.error.unwrap().code, ErrorCode::ShuttingDown);
        assert!(r.instance.is_some());
        assert_eq!(
            requests(
                &h.st.metrics,
                AdminVerbLabel::DrainPause,
                AdminOutcomeLabel::error
            ),
            1
        );
        assert!(
            call(&h.st, r#"{"v":1,"verb":"status"}"#).await.ok,
            "status still works after T0"
        );
    }

    /// A framing-level error (here a request that misses its deadline) never
    /// reaches `parse_request`; it is still counted under `_invalid` and its
    /// reply still carries the identity.
    #[tokio::test]
    async fn a_framing_error_is_counted_as_invalid_and_carries_identity() {
        use crate::admin::framing::{Limits, serve_connection};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let h = harness(ReloadMode::Live);
        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let st = Arc::clone(&h.st);
        let limits = Limits {
            request_deadline: Duration::from_millis(100),
            ..Limits::PRODUCTION
        };
        let served =
            tokio::spawn(async move { serve_connection(server, uid(), &*st, &limits).await });
        let (rd, mut wr) = client.into_split();
        wr.write_all(br#"{"v":1"#).await.unwrap(); // no newline
        let mut line = String::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            BufReader::new(rd).read_line(&mut line),
        )
        .await
        .expect("answered within 2 s")
        .unwrap();
        let r: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(r["error"]["code"], "bad_request", "{r}");
        assert_eq!(r["instance"]["pid"], std::process::id());
        served.await.unwrap();
        assert_eq!(
            requests(
                &h.st.metrics,
                AdminVerbLabel::Invalid,
                AdminOutcomeLabel::error
            ),
            1
        );
    }

    #[tokio::test]
    async fn retry_stranded_is_refused_after_t0() {
        let h = harness(ReloadMode::Live);
        h.level_tx.send(ShutdownLevel::Graceful).unwrap();
        let e =
            h.st.drain_retry_stranded(&args(json!({"wait_ms": 0})))
                .await
                .unwrap_err();
        assert_eq!(e.code, ErrorCode::ShuttingDown);
    }
}
