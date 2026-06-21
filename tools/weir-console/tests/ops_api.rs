#![cfg(unix)]
mod ops_support;
use tempfile::tempdir;
use weir_console::ops::{self, OpsConfig, OpsError};
use std::path::PathBuf;

fn cfg_with(weir_ctl: PathBuf, dir: PathBuf) -> OpsConfig {
    OpsConfig {
        weir_ctl,
        wab_dir: dir,
        metrics_addr: "127.0.0.1:9185".into(),
        socket: PathBuf::from("/run/weir/weir.sock"),
        read_only: false,
    }
}

#[tokio::test]
async fn run_ctl_missing_binary_is_not_found() {
    let dir = tempdir().unwrap();
    let cfg = cfg_with(dir.path().join("does-not-exist"), dir.path().to_path_buf());
    // dead_letter shells out; a missing binary must map to NotFound, not a panic.
    let err = ops::dead_letter(&cfg).await.unwrap_err();
    assert!(matches!(err, OpsError::NotFound(_)), "got {err:?}");
}

#[tokio::test]
async fn run_ctl_nonzero_exit_surfaces_error_message() {
    let dir = tempdir().unwrap();
    let fail = ops_support::write_fail_stub(dir.path());
    let cfg = cfg_with(fail, dir.path().to_path_buf());
    let err = ops::dead_letter(&cfg).await.unwrap_err();
    match err {
        OpsError::CtlFailed(msg) => assert!(msg.contains("daemon unreachable"), "msg = {msg}"),
        other => panic!("expected CtlFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn status_up_carries_metrics_summary() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let cfg = cfg_with(stub.bin, dir.path().to_path_buf());
    let v = ops::status(&cfg).await.unwrap();
    assert_eq!(v["daemon"], "up");
    assert_eq!(v["summary"]["sink_health"], "healthy");
    assert_eq!(v["summary"]["queue_depth"], 7);
}

#[tokio::test]
async fn status_down_when_ctl_fails() {
    let dir = tempdir().unwrap();
    let fail = ops_support::write_fail_stub(dir.path());
    let cfg = cfg_with(fail, dir.path().to_path_buf());
    let v = ops::status(&cfg).await.unwrap(); // a down daemon is not an error
    assert_eq!(v["daemon"], "down");
}

#[tokio::test]
async fn dead_letter_parses_list() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let cfg = cfg_with(stub.bin, dir.path().to_path_buf());
    let v = ops::dead_letter(&cfg).await.unwrap();
    assert_eq!(v["count"], 2);
    assert_eq!(v["segments"][0]["segment"], "dl_00000001.wab.sealed");
}
