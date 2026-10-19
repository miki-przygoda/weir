//! One reload path for SIGHUP and the admin socket (spec §3.2, Foundations
//! spec §5.10).
//!
//! Before this existed, a SIGHUP handler was installed only by a TLS build with
//! `tcp_bind` set, and only once the runtime was up. Every other daemon kept the
//! default disposition, so `kill -HUP` (a `systemctl reload`, a log-rotation
//! script, a terminal hang-up on a bare run) killed it outright, skipping the
//! seal and the drain. `main` now sets SIGHUP to `SIG_IGN` before
//! `Config::load`, and this module owns everything after that:
//!
//! - [`Reloader`] is the single reload function. It reloads what is reloadable
//!   (today: the TLS material, once registered) and otherwise reports an honest
//!   `noop`, counting every outcome in `weir_config_reloads_total`.
//! - [`spawn_reload_loop`] serializes reloads through a bounded queue
//!   ([`RELOAD_QUEUE_DEPTH`]). The SIGHUP task only enqueues; the admin `reload`
//!   verb enqueues a [`ReloadRequest`] with a reply channel through
//!   [`request_reload_and_wait`] and answers with the [`ReloadReport`]'s wire
//!   shape ([`ReloadReport::to_wire`]), so both share one code path.
//! - Every report is stamped with its finish time and published to the status
//!   board's `reload` cell ([`Reloader::attach_board`]), which only the
//!   Reloader writes.
//! - [`spawn_sighup_task`] replaces the `SIG_IGN` disposition with tokio's
//!   handler. When the queue is full (a SIGHUP flood) the request is dropped and
//!   counted as `outcome="dropped"` rather than queued without bound.
//!
//! Nothing here touches the ack path, and no lock is shared with it.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

#[cfg(all(unix, feature = "tls"))]
use crate::socket::tls::ReloadableServerConfig;

use crate::metrics::{ConfigReloadLabel, ConfigReloadOutcome, ConfigReloadTrigger, Metrics};
use crate::status::{StatusBoard, unix_ms_now};

/// Capacity of the reload queue. Reloads are serialized, so more than a few
/// pending requests carry no information: each reads the current files anyway.
pub(crate) const RELOAD_QUEUE_DEPTH: usize = 8;

/// [`ReloadReport::note`] when TLS is configured but its first load has not
/// happened yet (a SIGHUP in the moment between the SIGHUP task starting and the
/// TLS listener registering). Accurate: the pending load reads the current files.
pub(crate) const NOTE_TLS_NOT_LOADED_YET: &str = "tls_not_loaded_yet";

/// [`ReloadReport::note`] when this build and config have nothing reloadable.
pub(crate) const NOTE_NOTHING_RELOADABLE: &str = "nothing_reloadable";

/// The wire name of the TLS material item in the admin `reload` answer
/// (spec §11), and in the status document's reload evidence.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
    )
)]
pub(crate) const ITEM_TLS_CERTIFICATES: &str = "tls_certificates";

/// How long the admin verb waits for room in the 8-deep reload queue.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
    )
)]
pub(crate) const ADMIN_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(1);

/// How long the admin verb waits for the loop's answer.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
    )
)]
pub(crate) const ADMIN_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// What asked for a reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReloadTrigger {
    Sighup,
    /// The admin socket's `reload` verb (Foundations), through
    /// [`request_reload_and_wait`].
    #[cfg_attr(not(test), allow(dead_code))] // Task 6.14 wires the admin socket
    Admin,
}

impl ReloadTrigger {
    fn label(self) -> ConfigReloadTrigger {
        match self {
            ReloadTrigger::Sighup => ConfigReloadTrigger::sighup,
            ReloadTrigger::Admin => ConfigReloadTrigger::admin,
        }
    }

    /// How log lines name the trigger.
    fn log_name(self) -> &'static str {
        match self {
            ReloadTrigger::Sighup => "SIGHUP",
            ReloadTrigger::Admin => "admin reload",
        }
    }
}

/// How a reload went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReloadOutcome {
    /// Something was reloaded.
    #[cfg_attr(not(all(unix, feature = "tls")), allow(dead_code))]
    Ok,
    /// Nothing was reloadable; nothing changed.
    Noop,
    /// A reload was attempted and rejected; the previous config stays.
    #[cfg_attr(not(all(unix, feature = "tls")), allow(dead_code))]
    Failed,
}

impl ReloadOutcome {
    fn label(self) -> ConfigReloadOutcome {
        match self {
            ReloadOutcome::Ok => ConfigReloadOutcome::ok,
            ReloadOutcome::Noop => ConfigReloadOutcome::noop,
            ReloadOutcome::Failed => ConfigReloadOutcome::failed,
        }
    }
}

/// The result of one reload. The admin verb answers with its wire shape,
/// [`ReloadReport::to_wire`]; the status board keeps the last one.
#[derive(Debug, Clone)]
pub(crate) struct ReloadReport {
    pub trigger: ReloadTrigger,
    pub outcome: ReloadOutcome,
    /// What was reloaded (`"tls"`).
    pub changed: Vec<&'static str>,
    /// What was rejected, and why. The previous value stays in force.
    pub rejected: Vec<(&'static str, String)>,
    /// Why a `noop` was a noop ([`NOTE_TLS_NOT_LOADED_YET`],
    /// [`NOTE_NOTHING_RELOADABLE`]).
    pub note: Option<&'static str>,
    /// When the reload finished (unix ms). Stamped by the Reloader.
    pub at_unix_ms: u64,
}

impl ReloadReport {
    /// The admin wire shape (spec §5.10/§11). One item today: the TLS
    /// material. The config file is reported as not reloaded.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
        )
    )]
    pub(crate) fn to_wire(&self) -> weir_status::admin::ReloadResult {
        use weir_status::admin::{
            NotReloaded, ReloadItemOutcomeWire as Item, ReloadItemWire as ReloadItem,
            ReloadOutcomeWire as Out, ReloadResult,
        };
        let outcome = match self.outcome {
            ReloadOutcome::Ok => Out::Applied,
            ReloadOutcome::Noop => Out::Noop,
            ReloadOutcome::Failed => Out::Failed,
        };
        let tls = if self.changed.contains(&"tls") {
            ReloadItem {
                item: ITEM_TLS_CERTIFICATES.into(),
                outcome: Item::Reloaded,
                detail: None,
            }
        } else if let Some((_, why)) = self.rejected.iter().find(|(k, _)| *k == "tls") {
            ReloadItem {
                item: ITEM_TLS_CERTIFICATES.into(),
                outcome: Item::Failed,
                detail: Some(why.clone()),
            }
        } else {
            ReloadItem {
                item: ITEM_TLS_CERTIFICATES.into(),
                outcome: Item::NotConfigured,
                detail: self.note.map(str::to_string),
            }
        };
        ReloadResult {
            trigger: match self.trigger {
                ReloadTrigger::Sighup => "sighup",
                ReloadTrigger::Admin => "admin",
            }
            .into(),
            at_unix_ms: self.at_unix_ms,
            outcome,
            items: vec![tls],
            not_reloaded: vec![NotReloaded {
                item: "config_file".into(),
                reason_code: "restart_required".into(),
                reason: "the config file is read once at startup; edits to weir.toml or the \
                         environment need a restart"
                    .into(),
            }],
        }
    }
}

/// One request on the reload queue.
pub(crate) struct ReloadRequest {
    pub trigger: ReloadTrigger,
    /// Where to send the report. SIGHUP sends `None`; the admin verb will send
    /// `Some` and answer with what comes back.
    pub reply: Option<oneshot::Sender<ReloadReport>>,
}

/// The single reload function, shared as `Arc<Reloader>`: the reload loop holds
/// one clone and the TLS listener branch another, for [`register_tls`].
///
/// [`register_tls`]: Reloader::register_tls
pub(crate) struct Reloader {
    /// Set once the TLS listener has loaded its material. `OnceLock::get` is
    /// wait-free.
    #[cfg(all(unix, feature = "tls"))]
    tls: OnceLock<ReloadableServerConfig>,
    /// Whether this daemon is configured with TLS at all (`tcp_bind` set in a
    /// TLS build). Distinguishes "not loaded yet" from "nothing reloadable".
    tls_configured: bool,
    metrics: Arc<Metrics>,
    /// Where the last report is published (spec §5.2 `reload` cell). Set
    /// once at startup; unset in unit tests that do not need it.
    /// `OnceLock::get` is wait-free.
    board: OnceLock<Arc<StatusBoard>>,
}

impl Reloader {
    pub(crate) fn new(metrics: Arc<Metrics>, tls_configured: bool) -> Arc<Self> {
        Arc::new(Self {
            #[cfg(all(unix, feature = "tls"))]
            tls: OnceLock::new(),
            tls_configured,
            metrics,
            board: OnceLock::new(),
        })
    }

    /// Hands the reloader the status board, so each report lands in its
    /// `reload` cell. Once: a second call is a bug and is ignored.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
        )
    )]
    pub(crate) fn attach_board(&self, board: Arc<StatusBoard>) {
        if self.board.set(board).is_err() {
            warn!("status board attached to the reloader twice; keeping the first");
        }
    }

    /// Hands the reloader the live TLS config. The clone shares the listener's
    /// `ArcSwap`, so a reload is visible to the next accept.
    #[cfg(all(unix, feature = "tls"))]
    pub(crate) fn register_tls(&self, t: ReloadableServerConfig) {
        if self.tls.set(t).is_err() {
            // One TLS listener per daemon: a second registration is a bug, and
            // the first config is the one the listener serves.
            warn!("TLS config registered twice for reload; keeping the first");
        }
    }

    /// Reloads whatever is reloadable, stamps and publishes the report, and
    /// counts the outcome. Blocking (the TLS
    /// branch reads and parses files): the reload loop runs it on the blocking
    /// pool.
    pub(crate) fn reload(&self, t: ReloadTrigger) -> ReloadReport {
        let mut report = ReloadReport {
            trigger: t,
            outcome: ReloadOutcome::Noop,
            changed: Vec::new(),
            rejected: Vec::new(),
            note: None,
            at_unix_ms: 0,
        };
        #[cfg(all(unix, feature = "tls"))]
        if let Some(tls) = self.tls.get() {
            self.reload_tls(tls, &mut report);
            return self.finish(report);
        }
        if self.tls_configured {
            report.note = Some(NOTE_TLS_NOT_LOADED_YET);
            info!(
                "{}: TLS is configured but not loaded yet; the pending load reads the current \
                 files, so there is nothing to reload",
                t.log_name()
            );
        } else {
            report.note = Some(NOTE_NOTHING_RELOADABLE);
            info!(
                "{} received: nothing reloadable in this build/config (config is read once at \
                 startup); nothing changed",
                t.log_name()
            );
        }
        self.finish(report)
    }

    /// Stamps, publishes and counts one report.
    fn finish(&self, mut report: ReloadReport) -> ReloadReport {
        report.at_unix_ms = unix_ms_now();
        // Publish first: anyone who sees the counter move also sees the report.
        if let Some(board) = self.board.get() {
            board.reload.publish(report.clone(), report.at_unix_ms);
        }
        self.count(&report);
        report
    }

    /// The TLS branch, with the fail-safe semantics and the
    /// `weir_tls_config_reloads_total{outcome}` contract it always had.
    #[cfg(all(unix, feature = "tls"))]
    fn reload_tls(&self, tls: &ReloadableServerConfig, report: &mut ReloadReport) {
        use crate::metrics::{TlsReloadLabel, TlsReloadOutcome};
        let name = report.trigger.log_name();
        match tls.reload() {
            Ok(()) => {
                self.metrics
                    .tls_config_reloads
                    .get_or_create(&TlsReloadLabel {
                        outcome: TlsReloadOutcome::ok,
                    })
                    .inc();
                report.outcome = ReloadOutcome::Ok;
                report.changed.push("tls");
                info!("{name}: TLS certificates reloaded");
            }
            Err(e) => {
                self.metrics
                    .tls_config_reloads
                    .get_or_create(&TlsReloadLabel {
                        outcome: TlsReloadOutcome::failed,
                    })
                    .inc();
                report.outcome = ReloadOutcome::Failed;
                report.rejected.push(("tls", e.to_string()));
                error!(error = %e, "{name}: TLS reload failed; keeping previous certs");
            }
        }
    }

    fn count(&self, report: &ReloadReport) {
        self.metrics
            .config_reloads
            .get_or_create(&ConfigReloadLabel {
                trigger: report.trigger.label(),
                outcome: report.outcome.label(),
            })
            .inc();
    }
}

/// The bounded reload queue, unspawned: [`spawn_reload_loop`] uses it, and the
/// tests use it to stand in for a stalled loop.
pub(crate) fn reload_channel() -> (mpsc::Sender<ReloadRequest>, mpsc::Receiver<ReloadRequest>) {
    mpsc::channel(RELOAD_QUEUE_DEPTH)
}

/// Spawns the loop that serves reload requests one at a time, and returns its
/// queue. Must be called inside the runtime.
pub(crate) fn spawn_reload_loop(r: Arc<Reloader>) -> mpsc::Sender<ReloadRequest> {
    let (tx, rx) = reload_channel();
    tokio::spawn(reload_loop(r, rx));
    tx
}

/// Serves reload requests one at a time, so reloads never overlap. The reload
/// itself reads and parses files, so it runs on the blocking pool rather than
/// on a runtime worker.
async fn reload_loop(r: Arc<Reloader>, mut rx: mpsc::Receiver<ReloadRequest>) {
    while let Some(req) = rx.recv().await {
        let reloader = Arc::clone(&r);
        let trigger = req.trigger;
        let report = match tokio::task::spawn_blocking(move || reloader.reload(trigger)).await {
            Ok(report) => report,
            Err(e) => {
                // A panic inside a reload, or the runtime shutting down. The
                // previous config stays in force either way; a waiting admin
                // caller sees its reply channel close.
                error!(error = %e, "{}: reload did not complete", trigger.log_name());
                continue;
            }
        };
        debug!(
            trigger = ?report.trigger,
            outcome = ?report.outcome,
            changed = ?report.changed,
            rejected = ?report.rejected,
            note = ?report.note,
            "reload finished"
        );
        if let Some(reply) = req.reply {
            // The caller may have given up waiting; that is not an error.
            let _ = reply.send(report);
        }
    }
}

/// Enqueues a reply-less reload request without ever blocking. A full queue (or
/// a loop that is gone) drops the request and counts it as
/// `outcome="dropped"`. Returns whether the request was queued.
pub(crate) fn request_reload(
    tx: &mpsc::Sender<ReloadRequest>,
    trigger: ReloadTrigger,
    metrics: &Metrics,
) -> bool {
    let why = match tx.try_send(ReloadRequest {
        trigger,
        reply: None,
    }) {
        Ok(()) => return true,
        Err(mpsc::error::TrySendError::Full(_)) => "the reload queue is full",
        Err(mpsc::error::TrySendError::Closed(_)) => "the reload loop has stopped",
    };
    count_dropped(metrics, trigger);
    warn!("{}: {why}; dropping this request", trigger.log_name());
    false
}

/// Counts a request that never ran (`outcome="dropped"`).
fn count_dropped(metrics: &Metrics, trigger: ReloadTrigger) {
    metrics
        .config_reloads
        .get_or_create(&ConfigReloadLabel {
            trigger: trigger.label(),
            outcome: ConfigReloadOutcome::dropped,
        })
        .inc();
}

/// Why [`request_reload_and_wait`] has no report.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
    )
)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReloadWaitError {
    /// No room in the queue within the enqueue timeout (counted `dropped`).
    QueueFull,
    /// The loop has stopped (runtime shutting down; counted `dropped`).
    LoopGone,
    /// The loop dropped the reply: the reload panicked. Previous config stays.
    NoAnswer,
    /// No answer within the reply timeout. The reload may still complete.
    TimedOut,
}

/// The admin `reload` verb's path: the SAME queue and loop SIGHUP uses, with
/// a reply channel, so both triggers run one reload function, serialized.
/// Unlike [`request_reload`] it waits for room, up to `enqueue_timeout`,
/// then for the answer, up to `reply_timeout`. A request that never got into
/// the queue is counted `dropped`; one that did is counted by the Reloader.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the admin verb (Task 6.9) and main (Task 6.14) call it"
    )
)]
pub(crate) async fn request_reload_and_wait(
    tx: &mpsc::Sender<ReloadRequest>,
    trigger: ReloadTrigger,
    metrics: &Metrics,
    enqueue_timeout: Duration,
    reply_timeout: Duration,
) -> Result<ReloadReport, ReloadWaitError> {
    let (reply, answer) = oneshot::channel();
    let request = ReloadRequest {
        trigger,
        reply: Some(reply),
    };
    match tokio::time::timeout(enqueue_timeout, tx.send(request)).await {
        Ok(Ok(())) => {}
        Ok(Err(_closed)) => {
            count_dropped(metrics, trigger);
            warn!(
                "{}: the reload loop has stopped; dropping this request",
                trigger.log_name()
            );
            return Err(ReloadWaitError::LoopGone);
        }
        Err(_elapsed) => {
            count_dropped(metrics, trigger);
            warn!(
                "{}: the reload queue stayed full for {enqueue_timeout:?}; dropping this request",
                trigger.log_name()
            );
            return Err(ReloadWaitError::QueueFull);
        }
    }
    match tokio::time::timeout(reply_timeout, answer).await {
        Ok(Ok(report)) => Ok(report),
        Ok(Err(_)) => Err(ReloadWaitError::NoAnswer),
        Err(_) => Err(ReloadWaitError::TimedOut),
    }
}

/// Installs the SIGHUP handler, replacing the `SIG_IGN` that `main` set before
/// `Config::load`, and spawns the task that turns each SIGHUP into a reload
/// request. Must be called inside the runtime.
#[cfg(unix)]
pub(crate) fn spawn_sighup_task(tx: mpsc::Sender<ReloadRequest>, metrics: Arc<Metrics>) {
    use tokio::signal::unix::{SignalKind, signal};
    // Installed here, synchronously, so the handler is in place when this
    // returns. A failure leaves SIGHUP ignored, which is still safe.
    let mut hup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            error!(
                error = %e,
                "failed to install the SIGHUP handler; SIGHUP stays ignored and reloads nothing"
            );
            return;
        }
    };
    tokio::spawn(async move {
        // tokio coalesces signals that arrive before the task is polled, so a
        // flood yields at most one wake per poll; the queue bounds the rest.
        while hup.recv().await.is_some() {
            request_reload(&tx, ReloadTrigger::Sighup, &metrics);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().0)
    }

    fn reloads(m: &Metrics, trigger: ConfigReloadTrigger, outcome: ConfigReloadOutcome) -> u64 {
        m.config_reloads
            .get_or_create(&ConfigReloadLabel { trigger, outcome })
            .get()
    }

    use weir_status::admin::{ReloadItemOutcomeWire, ReloadOutcomeWire};

    #[test]
    fn reports_carry_a_time_and_map_to_the_wire_shape() {
        let r = Reloader::new(metrics(), false);
        let report = r.reload(ReloadTrigger::Admin);
        assert!(
            report.at_unix_ms > 1_600_000_000_000,
            "{}",
            report.at_unix_ms
        );
        let w = report.to_wire();
        assert_eq!(w.trigger, "admin");
        assert_eq!(w.at_unix_ms, report.at_unix_ms);
        assert_eq!(w.outcome, ReloadOutcomeWire::Noop);
        assert_eq!(w.items.len(), 1);
        assert_eq!(w.items[0].item, ITEM_TLS_CERTIFICATES);
        assert_eq!(w.items[0].outcome, ReloadItemOutcomeWire::NotConfigured);
        assert_eq!(w.items[0].detail.as_deref(), Some(NOTE_NOTHING_RELOADABLE));
        assert_eq!(w.not_reloaded.len(), 1);
        assert_eq!(w.not_reloaded[0].item, "config_file");
        assert_eq!(w.not_reloaded[0].reason_code, "restart_required");
    }

    /// spec §5.2: the board's reload cell is written by the Reloader only.
    #[test]
    fn the_last_report_is_published_to_the_board() {
        let board = crate::status::StatusBoard::for_test();
        let r = Reloader::new(metrics(), false);
        r.attach_board(Arc::clone(&board));
        assert!(board.reload.evidence().is_none());
        r.reload(ReloadTrigger::Sighup);
        let last = board.reload.evidence().expect("published");
        assert_eq!(last.trigger, weir_status::ReloadTriggerCode::Sighup);
        assert_eq!(last.outcome, weir_status::ReloadOutcomeCode::Noop);
    }

    #[tokio::test]
    async fn an_admin_reload_waits_for_its_own_report() {
        let m = metrics();
        let tx = spawn_reload_loop(Reloader::new(Arc::clone(&m), false));
        // The production timeouts the admin verb uses (1 s enqueue, 10 s reply).
        let report = request_reload_and_wait(
            &tx,
            ReloadTrigger::Admin,
            &m,
            ADMIN_ENQUEUE_TIMEOUT,
            ADMIN_REPLY_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(report.trigger, ReloadTrigger::Admin);
        assert_eq!(report.outcome, ReloadOutcome::Noop);
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::noop),
            1
        );
    }

    #[tokio::test]
    async fn an_admin_reload_on_a_full_queue_is_refused_and_counted() {
        let m = metrics();
        let (tx, _stalled) = reload_channel();
        for _ in 0..RELOAD_QUEUE_DEPTH {
            assert!(request_reload(&tx, ReloadTrigger::Sighup, &m));
        }
        let e = request_reload_and_wait(
            &tx,
            ReloadTrigger::Admin,
            &m,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert_eq!(e, ReloadWaitError::QueueFull);
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::dropped),
            1
        );
    }

    #[tokio::test]
    async fn an_admin_reload_with_no_loop_reports_it() {
        let m = metrics();
        let (tx, rx) = reload_channel();
        drop(rx);
        let e = request_reload_and_wait(
            &tx,
            ReloadTrigger::Admin,
            &m,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert_eq!(e, ReloadWaitError::LoopGone);
    }

    /// A default daemon has nothing to reload: the honest answer is a counted
    /// noop, and the TLS counter does not move.
    #[test]
    fn reload_without_tls_is_a_counted_noop() {
        let m = metrics();
        let r = Reloader::new(Arc::clone(&m), false);
        let report = r.reload(ReloadTrigger::Sighup);
        assert_eq!(report.trigger, ReloadTrigger::Sighup);
        assert_eq!(report.outcome, ReloadOutcome::Noop);
        assert_eq!(report.note, Some(NOTE_NOTHING_RELOADABLE));
        assert!(report.changed.is_empty() && report.rejected.is_empty());
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::noop),
            1
        );
        for outcome in [
            ConfigReloadOutcome::ok,
            ConfigReloadOutcome::failed,
            ConfigReloadOutcome::dropped,
        ] {
            assert_eq!(reloads(&m, ConfigReloadTrigger::sighup, outcome), 0);
        }
        // The admin trigger is counted under its own label.
        r.reload(ReloadTrigger::Admin);
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::noop),
            1
        );
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::noop),
            1
        );
    }

    /// A SIGHUP between the SIGHUP task starting and the TLS listener
    /// registering its config: nothing to do, because the pending load reads the
    /// current files, and the note says so.
    #[test]
    fn tls_configured_but_not_registered_is_noop_with_note() {
        let m = metrics();
        let r = Reloader::new(Arc::clone(&m), true);
        let report = r.reload(ReloadTrigger::Sighup);
        assert_eq!(report.outcome, ReloadOutcome::Noop);
        assert_eq!(report.note, Some(NOTE_TLS_NOT_LOADED_YET));
        assert!(report.changed.is_empty() && report.rejected.is_empty());
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::noop),
            1
        );
    }

    /// Review Focus 5: a SIGHUP flood against a stalled reload loop. Enqueueing
    /// never blocks, at most RELOAD_QUEUE_DEPTH requests wait, and every extra
    /// one is dropped and counted.
    #[test]
    fn burst_of_requests_never_blocks_and_counts_drops() {
        let m = metrics();
        // The receiver is never polled: the loop is stalled.
        let (tx, rx) = reload_channel();
        let queued = (0..100)
            .filter(|_| request_reload(&tx, ReloadTrigger::Sighup, &m))
            .count();
        assert_eq!(queued, RELOAD_QUEUE_DEPTH);
        assert_eq!(rx.len(), RELOAD_QUEUE_DEPTH);
        assert_eq!(
            reloads(
                &m,
                ConfigReloadTrigger::sighup,
                ConfigReloadOutcome::dropped
            ),
            (100 - RELOAD_QUEUE_DEPTH) as u64
        );
        // Nothing ran: queued requests are not counted until served.
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::noop),
            0
        );
    }

    /// A loop that is gone (runtime shutting down) drops the request too,
    /// without blocking or panicking.
    #[test]
    fn a_closed_queue_drops_and_counts() {
        let m = metrics();
        let (tx, rx) = reload_channel();
        drop(rx);
        assert!(!request_reload(&tx, ReloadTrigger::Sighup, &m));
        assert_eq!(
            reloads(
                &m,
                ConfigReloadTrigger::sighup,
                ConfigReloadOutcome::dropped
            ),
            1
        );
    }

    /// The loop serves a request and answers on its reply channel: the seam the
    /// admin `reload` verb will use.
    #[tokio::test]
    async fn the_loop_answers_a_request_with_its_report() {
        let m = metrics();
        let tx = spawn_reload_loop(Reloader::new(Arc::clone(&m), false));
        let (reply, answer) = oneshot::channel();
        tx.send(ReloadRequest {
            trigger: ReloadTrigger::Admin,
            reply: Some(reply),
        })
        .await
        .unwrap();
        let report = answer.await.expect("the loop answers");
        assert_eq!(report.trigger, ReloadTrigger::Admin);
        assert_eq!(report.outcome, ReloadOutcome::Noop);
        assert_eq!(
            reloads(&m, ConfigReloadTrigger::admin, ConfigReloadOutcome::noop),
            1
        );
    }

    /// After a burst the loop is not wedged: it serves what was queued, and a
    /// later request still gets an answer. Every one of the 100 is accounted
    /// for, as served (noop) or dropped.
    #[tokio::test]
    async fn the_loop_is_not_wedged_by_a_burst() {
        let m = metrics();
        let tx = spawn_reload_loop(Reloader::new(Arc::clone(&m), false));
        for _ in 0..100 {
            request_reload(&tx, ReloadTrigger::Sighup, &m);
        }
        let (reply, answer) = oneshot::channel();
        tx.send(ReloadRequest {
            trigger: ReloadTrigger::Admin,
            reply: Some(reply),
        })
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), answer)
            .await
            .expect("the loop must answer after a burst")
            .unwrap();
        // The admin request queued behind every accepted SIGHUP, so they have
        // all been served by now.
        let served = reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::noop);
        let dropped = reloads(
            &m,
            ConfigReloadTrigger::sighup,
            ConfigReloadOutcome::dropped,
        );
        assert_eq!(served + dropped, 100, "served {served}, dropped {dropped}");
        assert!(served >= RELOAD_QUEUE_DEPTH as u64, "served {served}");
    }

    #[cfg(all(unix, feature = "tls"))]
    mod tls {
        use super::*;
        use crate::metrics::{TlsReloadLabel, TlsReloadOutcome};
        use std::path::{Path, PathBuf};

        /// Writes a CA, a leaf signed by it and the leaf's key into `dir`.
        fn write_material(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
            let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca_key = rcgen::KeyPair::generate().unwrap();
            let ca = ca_params.self_signed(&ca_key).unwrap();
            let leaf_params = rcgen::CertificateParams::new(vec!["weir-test".to_string()]).unwrap();
            let leaf_key = rcgen::KeyPair::generate().unwrap();
            let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
            let (cert, key, ca_path) = (
                dir.join("cert.pem"),
                dir.join("key.pem"),
                dir.join("ca.pem"),
            );
            std::fs::write(&cert, leaf.pem()).unwrap();
            std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
            std::fs::write(&ca_path, ca.pem()).unwrap();
            (cert, key, ca_path)
        }

        fn tls_reloads(m: &Metrics, outcome: TlsReloadOutcome) -> u64 {
            m.tls_config_reloads
                .get_or_create(&TlsReloadLabel { outcome })
                .get()
        }

        /// With TLS registered, a reload swaps in the rotated material and
        /// bumps both counters; a broken rotation keeps the previous config
        /// and counts `failed` on both.
        #[test]
        fn registered_tls_reloads_and_fails_safe() {
            let dir = crate::testutil::scratch_dir("reload_tls");
            let (cert, key, ca) = write_material(&dir);
            let tls = ReloadableServerConfig::load(cert.clone(), key, ca).unwrap();
            let m = metrics();
            let r = Reloader::new(Arc::clone(&m), true);
            r.register_tls(tls.clone());

            let before = tls.current();
            write_material(&dir);
            let report = r.reload(ReloadTrigger::Sighup);
            assert_eq!(report.outcome, ReloadOutcome::Ok);
            assert_eq!(report.changed, vec!["tls"]);
            assert_eq!(report.note, None);
            assert!(
                !Arc::ptr_eq(&before, &tls.current()),
                "the listener's clone must see the reloaded config"
            );
            let w = report.to_wire();
            assert_eq!(w.outcome, weir_status::admin::ReloadOutcomeWire::Applied);
            assert_eq!(
                w.items[0].outcome,
                weir_status::admin::ReloadItemOutcomeWire::Reloaded
            );
            assert_eq!(tls_reloads(&m, TlsReloadOutcome::ok), 1);
            assert_eq!(
                reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::ok),
                1
            );

            std::fs::write(&cert, "not a certificate").unwrap();
            let kept = tls.current();
            let report = r.reload(ReloadTrigger::Sighup);
            assert_eq!(report.outcome, ReloadOutcome::Failed);
            assert!(report.changed.is_empty());
            assert_eq!(report.rejected.len(), 1);
            assert_eq!(report.rejected[0].0, "tls");
            assert!(Arc::ptr_eq(&kept, &tls.current()), "fail-safe: config kept");
            let w = report.to_wire();
            assert_eq!(w.outcome, weir_status::admin::ReloadOutcomeWire::Failed);
            assert_eq!(
                w.items[0].outcome,
                weir_status::admin::ReloadItemOutcomeWire::Failed
            );
            assert!(
                w.items[0].detail.is_some(),
                "the rejection reason is admin detail"
            );
            assert_eq!(tls_reloads(&m, TlsReloadOutcome::failed), 1);
            assert_eq!(
                reloads(&m, ConfigReloadTrigger::sighup, ConfigReloadOutcome::failed),
                1
            );
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// Registration is once: a second call is ignored, and the first
        /// config keeps receiving reloads.
        #[test]
        fn a_second_registration_is_ignored() {
            let dir = crate::testutil::scratch_dir("reload_tls_twice");
            let a_dir = dir.join("a");
            let b_dir = dir.join("b");
            crate::testutil::mkdir_p(&a_dir);
            crate::testutil::mkdir_p(&b_dir);
            let (c, k, ca) = write_material(&a_dir);
            let first = ReloadableServerConfig::load(c, k, ca).unwrap();
            let (c, k, ca) = write_material(&b_dir);
            let second = ReloadableServerConfig::load(c, k, ca).unwrap();
            let r = Reloader::new(metrics(), true);
            r.register_tls(first.clone());
            r.register_tls(second.clone());
            let (f, s) = (first.current(), second.current());
            assert_eq!(r.reload(ReloadTrigger::Sighup).outcome, ReloadOutcome::Ok);
            assert!(!Arc::ptr_eq(&f, &first.current()));
            assert!(Arc::ptr_eq(&s, &second.current()));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
