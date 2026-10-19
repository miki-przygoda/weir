//! Foundations PR 2 drain tests (operability spec §16 "Drain", new tests 1-19).
//! A child of `drain::tests`, so the existing helpers and `MockSink` are in scope.

use super::*;
use crate::drain::control::{DrainControl, DrainRuntime};
use crate::drain::publish::Publisher;
use crate::drain::{DrainCtx, DrainExit, Wait, spawn_with, wait_slice};
use crate::status::{DrainExitReason, StatusBoard};
use crate::wab::{
    format::Compression,
    segment::{WabSegment, segment_path},
};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering::Relaxed},
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// A sealed shard-0 segment with an explicit counter, so a test can make many.
fn seg(dir: &Path, counter: u64, payloads: &[&[u8]]) -> PathBuf {
    let shard_dir = dir.join("shard_00");
    crate::testutil::mkdir_p(&shard_dir);
    let mut s =
        WabSegment::create(&segment_path(&shard_dir, counter), 0, Compression::None).unwrap();
    for p in payloads {
        s.write_record(p).unwrap();
    }
    s.seal().unwrap()
}

fn rt_with(
    control: DrainControl,
    board: Option<Arc<StatusBoard>>,
    initial_backlog: Vec<PathBuf>,
) -> DrainRuntime {
    DrainRuntime {
        control,
        board,
        initial_backlog,
    }
}

fn start<S: Sink + 'static>(
    rx: crossbeam_channel::Receiver<PathBuf>,
    sink: &Arc<S>,
    config: DrainConfig,
    metrics: &Arc<Metrics>,
    runtime: DrainRuntime,
) -> JoinHandle<DrainExit> {
    spawn_with(
        rx,
        Arc::clone(sink),
        config,
        Arc::clone(metrics),
        DrainHeartbeat::new(),
        runtime,
    )
}

/// Joins `h`, failing the test if the drain has not exited within `limit`.
fn join_within(h: JoinHandle<DrainExit>, limit: Duration) -> DrainExit {
    let deadline = Instant::now() + limit;
    while !h.is_finished() {
        assert!(
            Instant::now() < deadline,
            "the drain did not exit within {limit:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    h.join().expect("the drain thread panicked")
}

/// `fast_config` with the health poll pushed out of the test's way.
fn quiet(dir: PathBuf) -> DrainConfig {
    DrainConfig {
        health_poll_interval: Duration::from_secs(60),
        ..fast_config(dir)
    }
}

#[test]
fn a_detached_drain_reports_a_clean_stop_when_its_channel_closes() {
    let dir = tmp_dir("fnd_detached_exit");
    let sealed = seg(&dir, 1, &[b"one"]);
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(sealed.clone()).unwrap();
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(DrainControl::detached(), None, Vec::new()),
    );
    wait_for(5, "the segment is delivered", || !sealed.exists());
    drop(tx);
    let exit = join_within(h, Duration::from_secs(5));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(exit.abandoned, None);
    assert_eq!((exit.pending_left, exit.backlog_left), (0, 0));
    assert_eq!(exit.last_panic, None);
    assert_eq!(sink.call_count(), 1);
    assert_drain_reports_stopped(&metrics, "detached channel close");
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn every_drain_start_takes_a_new_generation() {
    let dir = tmp_dir("fnd_generation");
    let control = DrainControl::detached();
    let publish = Publisher::none();
    for _ in 0..2 {
        let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
        drop(tx);
        let mut ledger = fresh_ledger();
        let exit = crate::drain::drain_thread_with(
            rx,
            Arc::new(MockSink::with_responses([])),
            quiet(dir.clone()),
            noop_metrics(),
            true,
            DrainHeartbeat::new(),
            &mut ledger,
            &control,
            &publish,
            Vec::new(),
        );
        assert_eq!(exit.reason, DrainExitReason::Stopped);
    }
    assert_eq!(control.generation(), 2);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 1 (the zombie regression): an Attached drain whose every
/// flusher Sender is gone stays alive, keeps delivering what its rescan finds on
/// disk, and exits only on request_stop.
#[test]
fn an_attached_drain_outlives_its_flushers_and_stops_only_on_request() {
    let dir = tmp_dir("fnd_zombie");
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(256);
    drop(tx); // shard_count = 1 and its flusher died: nothing else holds a Sender
    let control = DrainControl::attached();
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(control.clone(), None, Vec::new()),
    );

    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !h.is_finished(),
        "an attached drain must not read Disconnected as shutdown"
    );

    let late = seg(&dir, 1, &[b"late"]);
    wait_for(
        5,
        "the idle rescan delivers a segment found on disk",
        || sink.call_count() == 1,
    );
    assert!(!h.is_finished());

    control.request_stop(Duration::from_secs(5));
    let exit = join_within(h, Duration::from_secs(2));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert!(!late.exists());
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 3: a stop with budget to spare delivers everything still
/// queued (here: all of it in the channel) and exits on natural completion.
#[test]
fn a_stop_with_budget_left_delivers_everything_queued_then_exits() {
    let dir = tmp_dir("fnd_stop_drains_queue");
    let segs: Vec<PathBuf> = (1..=5)
        .map(|n| seg(&dir, n, &[format!("s{n}").as_bytes()]))
        .collect();
    let (tx, rx) = crossbeam_channel::bounded(256);
    for s in &segs {
        tx.send(s.clone()).unwrap();
    }
    drop(tx);
    let control = DrainControl::attached();
    control.request_stop(Duration::from_millis(500)); // input already closed at start
    let sink = Arc::new(MockSink::with_responses([]));
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &noop_metrics(),
        rt_with(control, None, Vec::new()),
    );
    let exit = join_within(h, Duration::from_secs(2));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(sink.call_count(), 5);
    assert!(
        segs.iter().all(|s| !s.exists()),
        "all confirmed and deleted"
    );
    assert_eq!(
        (exit.pending_left, exit.backlog_left, exit.abandoned),
        (0, 0, None)
    );
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn wait_slice_reads_a_disconnect_by_lifetime() {
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    drop(tx);
    let hb = DrainHeartbeat::new();
    let publish = Publisher::none();
    let mut pending = VecDeque::new();
    let mut gone = false;

    let detached = DrainControl::detached();
    let ctx = DrainCtx {
        control: &detached,
        publish: &publish,
        hb: &hb,
    };
    assert_eq!(
        wait_slice(
            &rx,
            &ctx,
            Duration::from_millis(10),
            &mut pending,
            0,
            &mut gone
        ),
        Wait::Disconnected
    );
    assert!(!gone);

    let attached = DrainControl::attached();
    let ctx = DrainCtx {
        control: &attached,
        publish: &publish,
        hb: &hb,
    };
    assert_eq!(
        wait_slice(
            &rx,
            &ctx,
            Duration::from_millis(10),
            &mut pending,
            0,
            &mut gone
        ),
        Wait::TimedOut
    );
    assert!(gone, "attached: the disconnect is remembered");
    let t = Instant::now();
    assert_eq!(
        wait_slice(
            &rx,
            &ctx,
            Duration::from_millis(30),
            &mut pending,
            0,
            &mut gone
        ),
        Wait::TimedOut
    );
    assert!(
        t.elapsed() >= Duration::from_millis(25),
        "a gone channel sleeps the slice, never spins"
    );
    assert!(hb.get() >= 3, "every slice bumps the heartbeat");
}

#[test]
fn wait_slice_returns_stop_once_the_deadline_has_passed() {
    let (_tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let control = DrainControl::attached();
    control.force_stop();
    let (hb, publish) = (DrainHeartbeat::new(), Publisher::none());
    let ctx = DrainCtx {
        control: &control,
        publish: &publish,
        hb: &hb,
    };
    let t = Instant::now();
    assert_eq!(
        wait_slice(
            &rx,
            &ctx,
            Duration::from_secs(1),
            &mut VecDeque::new(),
            0,
            &mut false
        ),
        Wait::Stop
    );
    assert!(
        t.elapsed() < Duration::from_millis(20),
        "Stop is returned without waiting"
    );
}

/// Spec §16 drain test 14 (alternation). The spec's figure is a 300-segment
/// backlog; 30 keeps the run short and still separates the two behaviours by
/// an order of magnitude: without alternation the second live send waits for
/// the whole backlog (~1.5 s), with it for at most one backlog segment and one
/// live segment.
#[test]
fn live_segments_alternate_with_a_long_backlog() {
    let dir = tmp_dir("fnd_alternate");
    let backlog: Vec<PathBuf> = (1..=30).map(|n| seg(&dir, n, &[b"b"])).collect();
    let per_commit = Duration::from_millis(50);
    let sink = Arc::new(MockSink::with_yielding_commit(per_commit, 1000));
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(1);
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &noop_metrics(),
        rt_with(DrainControl::detached(), None, backlog),
    );
    wait_for(5, "replay under way", || sink.call_count() >= 1);
    let mut worst = Duration::ZERO;
    for n in 1001..=1006 {
        let live = seg(&dir, n, &[b"l"]);
        let t = Instant::now();
        tx.send(live).unwrap();
        worst = worst.max(t.elapsed());
    }
    // A send on a bounded(1) channel waits for the drain to take the previous
    // live segment: at most one backlog segment plus one live segment (2× the
    // segment time), plus 100 ms of scheduling and fsync slack.
    assert!(
        worst <= per_commit * 2 + Duration::from_millis(100),
        "a live sender waited {worst:?} behind the replay backlog"
    );
    drop(tx);
    join_within(h, Duration::from_secs(10));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 15: the replay list plus a panic on its first segment.
/// The respawn has no backlog (the panic took it), but its first healthy probe
/// is a recovery edge, so the rescan finds every segment on disk again.
#[test]
fn an_initial_backlog_survives_a_panic_on_its_first_segment() {
    let dir = tmp_dir("fnd_backlog_panic");
    let segs: Vec<PathBuf> = (1..=3).map(|n| seg(&dir, n, &[b"r"])).collect();
    let sink = Arc::new(MockSink::with_panic(1, []));
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(DrainControl::detached(), None, segs.clone()),
    );
    wait_for(
        10,
        "every replayed segment is delivered after the respawn",
        || segs.iter().all(|s| !s.exists()),
    );
    assert_eq!(drain_panics(&metrics), 1);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn next_queued_alternates_and_falls_back_to_whichever_side_has_work() {
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let p = |s: &str| PathBuf::from(s);
    let mut pending = VecDeque::from([p("l1"), p("l2")]);
    let mut backlog = VecDeque::from([p("b1"), p("b2"), p("b3")]);
    tx.send(p("c1")).unwrap();
    let mut turn = false;
    let mut got = Vec::new();
    while let Some(x) = crate::drain::next_queued(&mut pending, &mut backlog, &rx, &mut turn) {
        got.push(x);
    }
    assert_eq!(got, ["l1", "b1", "l2", "b2", "c1", "b3"].map(PathBuf::from));
}

/// The rescan drains the channel into `pending` FIRST, so a path that is both
/// in the channel and on disk is counted as already queued, never queued twice.
#[test]
fn a_rescan_drains_the_channel_first_and_never_double_queues() {
    let dir = tmp_dir("fnd_rescan_dedup");
    let a = seg(&dir, 1, &[b"a"]);
    let b = seg(&dir, 2, &[b"b"]);
    let (tx, rx) = crossbeam_channel::bounded(256);
    tx.send(a.clone()).unwrap();
    let (mut pending, mut backlog) = (VecDeque::new(), VecDeque::new());
    let out = crate::drain::requeue_unconfirmed(
        &fast_config(dir.clone()),
        &noop_metrics(),
        Some(&rx),
        &mut pending,
        &mut backlog,
        None,
        &|_| false,
        &DrainHeartbeat::new(),
    );
    assert_eq!(pending, VecDeque::from([a.clone()]));
    assert_eq!(backlog, VecDeque::from([b.clone()]));
    assert_eq!(
        (out.requeued, out.already_queued, out.scan_error),
        (1, 1, None)
    );
    assert!(rx.is_empty());
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 13: auto-resume during replay. With the replay list
/// still in the backlog, a healthy health poll must not rescan (the old gate
/// looked only at `pending`, so it rescanned every poll for the whole replay
/// and double-queued whatever sat in the channel).
#[test]
fn the_idle_rescan_waits_for_the_backlog_to_empty() {
    crate::drain::RESCAN_CALLS.store(0, Relaxed);
    let dir = tmp_dir("fnd_idle_gate");
    let backlog: Vec<PathBuf> = (1..=5).map(|n| seg(&dir, n, &[b"b"])).collect();
    let sink = Arc::new(MockSink::with_yielding_commit(
        Duration::from_millis(100),
        1000,
    ));
    let cfg = DrainConfig {
        health_poll_interval: Duration::from_millis(20),
        ..fast_config(dir.clone())
    };
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    let h = start(
        rx,
        &sink,
        cfg,
        &noop_metrics(),
        rt_with(DrainControl::detached(), None, backlog),
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        sink.call_count() < 5,
        "the replay must still be running for this to prove anything"
    );
    assert_eq!(
        crate::drain::RESCAN_CALLS.load(Relaxed),
        0,
        "a health poll during replay must not rescan"
    );
    wait_for(5, "replay finished", || sink.call_count() == 5);
    wait_for(
        2,
        "the idle rescan resumes once both queues are empty",
        || crate::drain::RESCAN_CALLS.load(Relaxed) > 0,
    );
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 11: retry-stranded while the probe says Down. The
/// automatic resume never fires (no healthy probe), the verb bypasses that
/// gate, drains the channel first and dedups against it, and resolves the ticket.
#[test]
fn retry_stranded_rescans_while_the_probe_says_down_and_dedups_the_channel() {
    let dir = tmp_dir("fnd_retry_stranded");
    let c = seg(&dir, 1, &[b"c"]);
    let sink = Arc::new(MockSink {
        yield_for: Duration::from_millis(400),
        health_script: Mutex::new(
            std::iter::repeat_n(SinkHealth::Down("probe refused".into()), 10_000).collect(),
        ),
        ..MockSink::with_responses([])
    });
    let board = StatusBoard::for_test();
    let control = DrainControl::detached();
    let cfg = DrainConfig {
        health_poll_interval: Duration::from_millis(50),
        ..fast_config(dir.clone())
    };
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(
        rx,
        &sink,
        cfg,
        &noop_metrics(),
        rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()),
    );
    tx.send(c.clone()).unwrap();
    wait_for(5, "c's commit is in flight", || sink.call_count() == 1);

    // While c commits: `a` is stranded on disk AND sitting in the channel; `b` only on disk.
    let a = seg(&dir, 2, &[b"a"]);
    let b = seg(&dir, 3, &[b"b"]);
    tx.send(a.clone()).unwrap();
    let ticket = control.request_requeue();
    wait_for(5, "the ticket is observed", || {
        control.requeue_observed() >= ticket
    });

    let r = board
        .drain
        .last_requeue
        .load_full()
        .expect("a published result");
    assert_eq!(r.ticket, ticket);
    assert!(!r.coalesced && r.scan_error.is_none());
    assert_eq!(
        (r.requeued, r.already_queued),
        (1, 1),
        "b requeued; a already queued (moved out of the channel before the scan)"
    );
    assert_eq!(board.drain.observed_requeue.load(Relaxed), ticket);
    wait_for(10, "a and b delivered", || !a.exists() && !b.exists());
    assert_eq!(
        sink.call_count(),
        3,
        "each segment reaches the sink exactly once"
    );
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 12: 1,000 back-to-back requests produce at most one scan
/// per 5 s window and at most one backoff cut per retry episode, and a cut is
/// never below base_retry_delay (W2 preserved).
#[test]
fn a_flood_of_retry_stranded_requests_scans_once_and_cuts_one_backoff() {
    crate::drain::VERB_RESCANS.store(0, Relaxed);
    let dir = tmp_dir("fnd_requeue_flood");
    let s = seg(&dir, 1, &[b"x"]);
    let hint = Duration::from_secs(2);
    let sink = Arc::new(MockSink::with_responses([
        Err(MockError::TransientWithRetryAfter(hint)),
        Err(MockError::TransientWithRetryAfter(hint)),
    ]));
    let cfg = DrainConfig {
        base_retry_delay: Duration::from_millis(100),
        ..quiet(dir.clone())
    };
    let control = DrainControl::detached();
    let board = StatusBoard::for_test();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(
        rx,
        &sink,
        cfg,
        &noop_metrics(),
        rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()),
    );
    tx.send(s.clone()).unwrap();
    wait_for(5, "the first attempt failed", || sink.call_count() == 1);

    let mut tickets = std::collections::BTreeSet::new();
    for i in 0..1000 {
        tickets.insert(control.request_requeue());
        if i % 100 == 99 {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    wait_for(10, "delivered on the third attempt", || !s.exists());

    let ts = sink.call_timestamps();
    assert_eq!(ts.len(), 3);
    let (first_gap, second_gap) = (ts[1] - ts[0], ts[2] - ts[1]);
    assert!(
        first_gap >= Duration::from_millis(100) && first_gap < Duration::from_millis(1000),
        "the first backoff is cut once, never below base_retry_delay: {first_gap:?}"
    );
    assert!(
        second_gap >= Duration::from_millis(1900),
        "at most one cut per retry episode: {second_gap:?}"
    );
    assert_eq!(
        crate::drain::VERB_RESCANS.load(Relaxed),
        1,
        "one scan per 5 s window"
    );
    assert!(
        tickets.len() < 1000,
        "requests fold into the outstanding ticket"
    );
    let last = board.drain.last_requeue.load_full().unwrap();
    assert!(
        last.coalesced,
        "later requests are answered from the window's scan"
    );
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 8: pause in Draining. 300 timed sends into a small
/// channel never time out (the paused drain keeps emptying it), nothing is
/// committed, and resume delivers everything in arrival order.
#[test]
fn a_paused_drain_keeps_taking_from_the_channel_and_resumes_in_order() {
    let dir = tmp_dir("fnd_pause_head");
    let control = DrainControl::detached();
    control.request_pause(); // before spawn: the pause lives in DrainControl
    let sink = Arc::new(MockSink::with_responses([]));
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::bounded::<PathBuf>(16);
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &metrics,
        rt_with(control.clone(), None, Vec::new()),
    );
    let mut expected = Vec::new();
    for n in 1..=300u64 {
        let rec = format!("r{n:03}");
        let p = seg(&dir, n, &[rec.as_bytes()]);
        expected.push(rec.into_bytes());
        tx.send_timeout(p, Duration::from_millis(500))
            .expect("a paused drain must keep taking from the channel");
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(sink.call_count(), 0, "nothing is committed while paused");
    assert_eq!(metrics.drain_paused.get(), 1.0);
    control.request_resume();
    wait_for(30, "all 300 delivered", || sink.call_count() == 300);
    assert_eq!(
        committed_bytes(&sink),
        expected,
        "resume delivers in arrival order"
    );
    assert_eq!(metrics.drain_paused.get(), 0.0);
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 18: a respawn while paused stays paused.
#[test]
fn a_respawned_drain_is_still_paused() {
    let dir = tmp_dir("fnd_pause_respawn");
    let first = seg(&dir, 1, &[b"boom"]);
    let sink = Arc::new(MockSink::with_panic(1, []));
    let control = DrainControl::detached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(
        rx,
        &sink,
        fast_config(dir.clone()),
        &metrics,
        rt_with(control.clone(), None, Vec::new()),
    );
    tx.send(first.clone()).unwrap();
    wait_for(5, "the first commit panicked", || {
        drain_panics(&metrics) == 1
    });
    control.request_pause(); // inside the supervisor's 100 ms respawn backoff
    // Respawn, then several 50 ms health polls whose rescan re-queues `first`.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        sink.call_count(),
        1,
        "the respawned drain honours the pause it inherited"
    );
    assert!(first.exists());
    assert_eq!(metrics.drain_paused.get(), 1.0);
    control.request_resume();
    wait_for(5, "delivered after resume", || !first.exists());
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

/// A stop while paused exits at once: the pause is honoured (owner decision).
#[test]
fn a_stop_while_paused_exits_without_delivering() {
    let dir = tmp_dir("fnd_pause_stop");
    let s = seg(&dir, 1, &[b"x"]);
    let control = DrainControl::attached();
    control.request_pause();
    let sink = Arc::new(MockSink::with_responses([]));
    let (tx, rx) = crossbeam_channel::unbounded();
    tx.send(s.clone()).unwrap();
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &noop_metrics(),
        rt_with(control.clone(), None, Vec::new()),
    );
    std::thread::sleep(Duration::from_millis(100));
    control.request_stop(Duration::from_secs(10));
    let exit = join_within(h, Duration::from_millis(500));
    assert_eq!(exit.reason, DrainExitReason::Stopped);
    assert_eq!(
        exit.pending_left, 1,
        "the queued segment is left for replay"
    );
    assert_eq!(sink.call_count(), 0);
    assert!(s.exists());
    drop(tx);
    std::fs::remove_dir_all(dir).ok();
}

/// Spec §16 drain test 9: pause mid-segment at a commit-batch boundary. The
/// commit in flight finishes, the next is not started, `held` is published,
/// and resume re-sends nothing: every record reaches the sink exactly once.
#[test]
fn a_pause_mid_segment_holds_at_a_batch_boundary_and_resumes_without_duplicates() {
    let dir = tmp_dir("fnd_pause_held");
    let (sealed, records) = make_numbered_segment(&dir, 10);
    let sink = Arc::new(MockSink {
        yield_for: Duration::from_millis(100),
        max_batch: 2, // 5 commits
        ..MockSink::with_responses([])
    });
    let board = StatusBoard::for_test();
    let control = DrainControl::detached();
    let metrics = noop_metrics();
    let (tx, rx) = crossbeam_channel::unbounded();
    let h = start(
        rx,
        &sink,
        quiet(dir.clone()),
        &metrics,
        rt_with(control.clone(), Some(Arc::clone(&board)), Vec::new()),
    );
    tx.send(sealed.clone()).unwrap();
    wait_for(5, "two commits", || sink.call_count() >= 2);

    control.request_pause();
    wait_for(5, "held is published", || board.drain.held.load(Relaxed));
    let held_at = sink.call_count();
    assert!(held_at < 5, "held before the segment finished");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(sink.call_count(), held_at, "no commit starts while held");
    assert!(
        sealed.exists(),
        "a held segment is neither confirmed nor deleted"
    );
    assert!(board.drain.paused_since_ms.load(Relaxed) > 0);
    assert_eq!(metrics.drain_paused.get(), 1.0);

    control.request_resume();
    wait_for(5, "delivered", || !sealed.exists());
    assert_eq!(
        committed_bytes(&sink),
        records,
        "every record exactly once, in order"
    );
    assert_eq!(sink.call_count(), 5);
    assert!(!board.drain.held.load(Relaxed));
    drop(tx);
    join_within(h, Duration::from_secs(5));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn process_segment_ctl_stops_before_a_batch_when_paused() {
    let dir = tmp_dir("fnd_batch_gate");
    let (sealed, _) = make_numbered_segment(&dir, 6);
    let sink = MockSink::with_batch_size(2, []);
    let control = DrainControl::detached();
    control.request_pause();
    let (publish, hb) = (Publisher::none(), DrainHeartbeat::new());
    let ctx = DrainCtx {
        control: &control,
        publish: &publish,
        hb: &hb,
    };
    let mut dl = DeadLetterWriter::open(&dir).unwrap();
    let r = block_on(crate::drain::process_segment_ctl(
        &sealed,
        &sink,
        &fast_config(dir.clone()),
        &noop_metrics(),
        &mut dl,
        2,
        false,
        &ctx,
    ));
    assert!(
        matches!(r, ProcessResult::Paused { processed: 2 }),
        "the cursor is the skip, untouched"
    );
    assert_eq!(sink.call_count(), 0, "no commit after a pause is seen");
    std::fs::remove_dir_all(dir).ok();
}
