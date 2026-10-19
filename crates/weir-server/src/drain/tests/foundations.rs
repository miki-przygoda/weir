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
    sync::Arc,
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
