//! The drain's writes to the status board (spec §5.2, §5.4 "Publication").
//!
//! The drain thread (and its supervisor) is the single writer of the drain and
//! sink cells. Every write is a wait-free atomic or ArcSwap store; with no
//! board (the detached test wiring) every write is a no-op, so the drain's
//! logic never branches on whether it is observed.

use std::sync::{Arc, atomic::Ordering::Relaxed};

use super::DrainHeartbeat;
use crate::{
    metrics::DrainStateValue,
    sink::SinkHealth,
    status::{DrainExitInfo, InFlight, RequeueResult, SinkFailureKind, StatusBoard},
};

/// Cap on any free text the drain stores for the admin view (spec §5.2).
pub(crate) const ADMIN_TEXT_MAX: usize = 2048;

/// Makes a sink or probe message safe to keep for the admin view: URL userinfo
/// passwords and credential query parameters redacted
/// (`sink::redact_url_password`, which also runs the query-credential pass),
/// control characters replaced with `.`, and the result capped at
/// [`ADMIN_TEXT_MAX`] bytes on a char boundary. Never reaches the public view:
/// the public status type has no field that can hold it (spec §5.1).
pub(crate) fn sanitize_admin_text(s: &str) -> String {
    let redacted = crate::sink::redact_url_password(s);
    let mut out: String = redacted
        .chars()
        .map(|c| if c.is_control() { '.' } else { c })
        .collect();
    if out.len() > ADMIN_TEXT_MAX {
        let mut cut = ADMIN_TEXT_MAX;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
    }
    out
}

/// The drain's handle on the status board. `None` = unobserved (tests).
pub(crate) struct Publisher {
    board: Option<Arc<StatusBoard>>,
}

impl Publisher {
    pub(crate) fn new(board: Option<Arc<StatusBoard>>) -> Self {
        Self { board }
    }

    /// The unobserved publisher of the pre-Foundations test wrappers.
    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self { board: None }
    }

    pub(crate) fn state(&self, s: &DrainStateValue) {
        if let Some(b) = &self.board {
            b.drain.set_state(s.clone());
        }
    }

    pub(crate) fn paused(&self, paused_since_ms: u64, held: bool) {
        if let Some(b) = &self.board {
            b.drain.set_paused(paused_since_ms, held);
        }
    }

    pub(crate) fn generation(&self, g: u64) {
        if let Some(b) = &self.board {
            b.drain.set_generation(g);
        }
    }

    pub(crate) fn in_flight(&self, f: Option<InFlight>) {
        if let Some(b) = &self.board {
            b.drain.set_in_flight(f);
        }
    }

    pub(crate) fn queue_lens(&self, pending: usize, backlog: usize) {
        if let Some(b) = &self.board {
            b.drain.set_queue_lens(pending, backlog);
        }
    }

    /// A `len()` snapshot of the drain channel. Last-writer-wins with the
    /// flushers' snapshots (spec §4): a store, never an inc/dec.
    pub(crate) fn channel_depth(&self, depth: usize) {
        if let Some(b) = &self.board {
            b.drain_depth.store(depth, Relaxed);
        }
    }

    pub(crate) fn replay(&self, total: u64, remaining: u64) {
        if let Some(b) = &self.board {
            b.drain.set_replay(total, remaining);
        }
    }

    pub(crate) fn blocked_since(&self, unix_ms: u64) {
        if let Some(b) = &self.board {
            b.drain.set_blocked_since(unix_ms);
        }
    }

    pub(crate) fn stopping(&self, deadline_unix_ms: Option<u64>) {
        if let Some(b) = &self.board {
            b.drain.set_stopping(deadline_unix_ms);
        }
    }

    /// Publishes the result BEFORE the observed ticket, so an admin client that
    /// polls for its ticket always finds the matching result.
    pub(crate) fn requeue(&self, r: &RequeueResult) {
        if let Some(b) = &self.board {
            b.drain.set_last_requeue(r.clone());
            b.drain.set_observed_requeue(r.ticket);
        }
    }

    pub(crate) fn exit(&self, info: DrainExitInfo) {
        if let Some(b) = &self.board {
            b.drain.set_exit(info);
        }
    }

    pub(crate) fn sink_health(&self, h: &SinkHealth) {
        if let Some(b) = &self.board {
            let reason = match h {
                SinkHealth::Degraded(r) | SinkHealth::Down(r) => Some(sanitize_admin_text(r)),
                _ => None,
            };
            b.sink.set_health(h, reason);
        }
    }

    pub(crate) fn sink_success(&self) {
        if let Some(b) = &self.board {
            b.sink.record_success();
        }
    }

    pub(crate) fn sink_failure(&self, kind: SinkFailureKind, msg: &str) {
        if let Some(b) = &self.board {
            b.sink.record_failure(kind, sanitize_admin_text(msg));
        }
    }
}

/// RAII marker for "a sink commit is in flight" (spec §5.4 CommitGuard).
///
/// Construction stores `commit_in_flight_since = now` and stamps
/// `last_progress_ms`; `Drop` clears the marker, does the same stamp and bumps
/// the heartbeat. That drop bump is the drain's one heartbeat bump per commit
/// (it replaced the bump `process_segment_ctl` made after each commit), so a
/// commit moves the heartbeat exactly once, as it always has; a second bump at
/// construction would only change the count, since the commit's `BusyGuard`
/// already tells a long commit from a wedge (spec §4.8). Taken immediately before the commit `select!` in
/// `commit_batch_ctl`, so every way out of a commit — success, the
/// `commit_timeout` early return, the contract-violation return, `Abandoned` at
/// the deadline, and a panic unwinding through it — clears the marker. A
/// marker left set would read as a wedged drain (`drain_wedged`) forever.
pub(crate) struct CommitGuard<'a> {
    publish: &'a Publisher,
    hb: &'a DrainHeartbeat,
}

impl<'a> CommitGuard<'a> {
    pub(crate) fn new(publish: &'a Publisher, hb: &'a DrainHeartbeat) -> Self {
        if let Some(b) = &publish.board {
            b.drain.commit_started();
        }
        Self { publish, hb }
    }
}

impl Drop for CommitGuard<'_> {
    fn drop(&mut self) {
        if let Some(b) = &self.publish.board {
            b.drain.commit_finished();
        }
        self.hb.bump();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn sanitize_redacts_credentials_and_strips_control_characters() {
        let raw = "error sending request for url \
                   (https://user:hunter2@sink.example/ingest?token=s3cr3t&x=1): \
                   connection refused\n\u{1b}[31mspoofed log line\u{7}";
        let out = sanitize_admin_text(raw);
        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("s3cr3t"), "{out}");
        assert!(
            out.contains("sink.example"),
            "the host stays for diagnosis: {out}"
        );
        assert!(!out.chars().any(char::is_control), "{out:?}");
    }

    #[test]
    fn sanitize_caps_at_2048_bytes_on_a_char_boundary() {
        let raw = "é".repeat(3000); // 6000 bytes, 2 bytes per char
        let out = sanitize_admin_text(&raw);
        assert!(out.len() <= ADMIN_TEXT_MAX);
        assert!(out.len() >= ADMIN_TEXT_MAX - 1);
        assert!(out.chars().all(|c| c == 'é'), "never cut inside a char");
    }

    #[test]
    fn a_publisher_without_a_board_accepts_every_write() {
        let p = Publisher::none();
        let hb = DrainHeartbeat::new();
        p.state(&DrainStateValue::draining);
        p.paused(1, true);
        p.generation(3);
        p.in_flight(None);
        p.queue_lens(1, 2);
        p.channel_depth(7);
        p.replay(3, 1);
        p.blocked_since(0);
        p.stopping(Some(1));
        p.sink_health(&SinkHealth::Down("x".into()));
        p.sink_success();
        p.sink_failure(SinkFailureKind::Transient, "x");
        {
            let _g = CommitGuard::new(&p, &hb);
        }
        assert!(hb.get() >= 1, "the heartbeat moves with or without a board");
    }

    #[test]
    fn a_commit_guard_marks_the_commit_in_flight_and_clears_it_on_drop() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let hb = DrainHeartbeat::new();
        let before = hb.get();
        {
            let _g = CommitGuard::new(&p, &hb);
            assert!(board.drain.commit_in_flight_since_ms.load(Relaxed) > 0);
            assert!(board.drain.last_progress_ms.load(Relaxed) > 0);
        }
        assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0);
        assert_eq!(hb.get(), before + 1, "bumped once, on drop");
    }

    #[test]
    fn a_commit_guard_clears_the_marker_when_the_commit_unwinds() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let hb = DrainHeartbeat::new();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = CommitGuard::new(&p, &hb);
            panic!("sink panicked mid-commit");
        }));
        assert!(r.is_err());
        assert_eq!(board.drain.commit_in_flight_since_ms.load(Relaxed), 0);
    }

    #[test]
    fn sink_text_is_sanitised_before_it_reaches_the_board() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        p.sink_failure(
            SinkFailureKind::Transient,
            &format!(
                "https://u:hunter2@h/x?token=s3cr3t \u{7}{}",
                "z".repeat(5000)
            ),
        );
        let e = board
            .sink
            .last_error
            .load_full()
            .expect("last_error stored");
        assert!(!e.contains("hunter2") && !e.contains("s3cr3t"));
        assert!(!e.chars().any(char::is_control));
        assert!(e.len() <= ADMIN_TEXT_MAX);
        p.sink_health(&SinkHealth::Down("https://u:hunter2@h/ down".into()));
        let r = board
            .sink
            .probe_reason
            .load_full()
            .expect("probe_reason stored");
        assert!(!r.contains("hunter2"));
    }

    #[test]
    fn requeue_publishes_the_result_and_the_observed_ticket() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        let r = RequeueResult {
            ticket: 4,
            requeued: 2,
            already_queued: 1,
            scan_error: None,
            backoff_cut: true,
            coalesced: false,
            at_ms: 9,
        };
        p.requeue(&r);
        assert_eq!(board.drain.observed_requeue.load(Relaxed), 4);
        assert_eq!(board.drain.last_requeue.load_full().as_deref(), Some(&r));
    }

    #[test]
    fn channel_depth_goes_to_the_shared_depth_atomic() {
        let board = StatusBoard::for_test();
        let p = Publisher::new(Some(Arc::clone(&board)));
        p.channel_depth(17);
        assert_eq!(board.drain_depth.load(Relaxed), 17);
    }
}
