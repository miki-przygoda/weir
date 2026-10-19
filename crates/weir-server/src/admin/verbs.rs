//! The admin verbs (spec §11) and the state they act on. `status` reads the
//! board; `reload` goes through `reload.rs`'s single loop; the drain verbs
//! store intent in `DrainControl` and wait for the drain's observation on
//! the board. Neither the drain nor the Lifecycle ever awaits a client.

use std::{sync::Arc, time::SystemTime};

use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};
use weir_status::{
    LivezCheck, Thresholds,
    admin::{AdminError, ErrorCode, InstanceId, Verb},
};

use crate::{
    drain::control::DrainControl,
    lifecycle::ShutdownLevel,
    metrics::{AdminVerbLabel, Metrics},
    reload::{self, ReloadRequest, ReloadTrigger, ReloadWaitError},
    status::StatusBoard,
};

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::test_support::{ReloadMode, harness};
    use crate::metrics::{ConfigReloadLabel, ConfigReloadOutcome, ConfigReloadTrigger};

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
}
