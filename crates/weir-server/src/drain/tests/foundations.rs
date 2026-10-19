//! Foundations PR 2 drain tests (operability spec §16 "Drain", new tests 1-19).
//! A child of `drain::tests`, so the existing helpers and `MockSink` are in scope.

use super::*;
use crate::drain::control::{DrainControl, DrainRuntime};
use crate::drain::publish::Publisher;
use crate::drain::{DrainExit, spawn_with};
use crate::status::{DrainExitReason, StatusBoard};
use crate::wab::{
    format::Compression,
    segment::{WabSegment, segment_path},
};
use std::{
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
