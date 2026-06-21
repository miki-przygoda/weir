# weir-console Ops Control Panel (view E) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the Ops Control Panel view to `weir-console` — dead-letter requeue/drop (preview → confirm → execute) plus a live operational-status header — implemented by shelling out to the sibling `weir-ctl` binary with `--json`.

**Architecture:** Extend the existing (workspace-excluded) `tools/weir-console/` crate. A new `ops` module runs `weir-ctl --json …` via `tokio::process::Command` and parses the result; new `/api/ops/*` axum routes wrap it; a new `ops.html` + `ops.js` reuse the hr2 shell. The Explorer's read-only `wab` module is untouched. No new crates — only the `process` feature is added to the existing `tokio` dep.

**Tech Stack:** Rust, axum 0.7, tokio (process), serde_json; vanilla HTML/CSS/JS; `node --test` for the frontend smoke. Tests use a stub `weir-ctl` shell script (unix).

**Canonical spec:** `docs/superpowers/specs/2026-06-21-weir-console-ops-panel-design.md`

**Verified `weir-ctl` facts (from `crates/weir-ctl/src/main.rs`):**
- `--json` is a **global** flag (any position). On failure it prints a `{"error": "..."}` object to **stderr** and exits non-zero.
- `weir-ctl --json metrics --addr <host:port>` → summary object with keys: `accepted, ack, nack, fsync_avg_ms, queue_depth, wab_bytes_on_disk, dead_letter_bytes_on_disk, sink_type, sink_health, flusher_panics, fsync_failures`. (Fails non-zero if the endpoint has no `weir_` series / is unreachable.)
- `weir-ctl --json dl list --wab-dir <dir>` → `{dead_letter_dir, count, total_bytes, segments:[{segment, bytes}]}`.
- `weir-ctl --json dl drop --wab-dir <dir> [--yes]` → whole-store; dry-run without `--yes`.
- `weir-ctl --json dl requeue --wab-dir <dir> --socket <path> --durability <sync|batched|buffered> [--yes]` → whole-store; dry-run without `--yes`; connects to the socket and re-pushes.
- Defaults: socket `/run/weir/weir.sock`, metrics addr `127.0.0.1:9185`.

**Existing files (do not break):**
- `tools/weir-console/src/lib.rs` = `pub mod server; pub mod wab;`
- `tools/weir-console/src/server.rs` exposes `pub fn router(wab_dir: PathBuf) -> Router` and `pub fn router_with_static(wab_dir, static_dir)`, plus `AppState { wab_dir, static_dir }`. The Explorer tests call `weir_console::server::router(root)`.
- `tools/weir-console/static/{index.html, explorer.js, weir.css}` + `explorer.test.mjs`.

---

### Task 1: Add the `process` feature to tokio

**Files:**
- Modify: `tools/weir-console/Cargo.toml`

- [ ] **Step 1: Add the feature**

In `tools/weir-console/Cargo.toml`, find the `tokio` dependency line (currently enables `rt-multi-thread, macros, net`) and add `process`. For example, if it reads:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net"] }
```

change it to:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "process"] }
```

- [ ] **Step 2: Confirm it builds**

Run: `cargo build --manifest-path tools/weir-console/Cargo.toml`
Expected: `Finished` with no errors.

- [ ] **Step 3: Commit**

```bash
git add tools/weir-console/Cargo.toml tools/weir-console/Cargo.lock
git commit -m "build(weir-console): enable tokio process feature for ops shell-out"
```

---

### Task 2: `ops` module core — config, error, and the `weir-ctl` runner

**Files:**
- Create: `tools/weir-console/src/ops.rs`
- Modify: `tools/weir-console/src/lib.rs`
- Create: `tools/weir-console/tests/ops_support/mod.rs` (stub-`weir-ctl` helper)
- Create: `tools/weir-console/tests/ops_api.rs` (integration tests)

- [ ] **Step 1: Write the stub-`weir-ctl` helper**

`tools/weir-console/tests/ops_support/mod.rs` — writes tiny shell scripts that stand in for `weir-ctl`, recording their argv and emitting canned `--json`. Unix-only (the helper uses `chmod`).

```rust
//! Test support: writes stub `weir-ctl` executables (shell scripts) that record
//! their argv to a log and emit canned --json per subcommand, so the ops shell-out
//! plumbing can be tested without a real daemon. Unix-only.
#![cfg(unix)]
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub struct Stub {
    pub bin: PathBuf,
    pub args_log: PathBuf,
}

/// A stub that succeeds and emits canned JSON. It appends each invocation's argv
/// (one line per call) to `args_log` so a test can assert flag presence (e.g. `--yes`).
pub fn write_ok_stub(dir: &Path) -> Stub {
    let args_log = dir.join("args.log");
    let bin = dir.join("weir-ctl");
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> "{log}"
case "$*" in
  *metrics*) printf '%s' '{{"accepted":5,"ack":4,"nack":1,"fsync_avg_ms":50.0,"queue_depth":7,"wab_bytes_on_disk":4096,"dead_letter_bytes_on_disk":2048,"sink_type":"http","sink_health":"healthy","flusher_panics":0,"fsync_failures":0}}' ;;
  *requeue*) case "$*" in *--yes*) printf '%s' '{{"requeued_records":5,"requeued_segments":2}}';; *) printf '%s' '{{"would_requeue_records":5,"would_requeue_segments":2,"unreadable":0}}';; esac ;;
  *drop*) case "$*" in *--yes*) printf '%s' '{{"dropped":2,"dropped_bytes":1024}}';; *) printf '%s' '{{"dry_run":true,"candidate_segments":2,"candidate_records":5,"candidate_bytes":1024}}';; esac ;;
  *list*) printf '%s' '{{"dead_letter_dir":"/x/dead_letter","count":2,"total_bytes":1024,"segments":[{{"segment":"dl_00000001.wab.sealed","bytes":512}},{{"segment":"dl_00000002.wab.sealed","bytes":512}}]}}' ;;
  *) printf '%s' '{{}}' ;;
esac
"#,
        log = args_log.display()
    );
    fs::write(&bin, script).unwrap();
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
    Stub { bin, args_log }
}

/// A stub that fails: prints a `{"error":...}` object to stderr and exits non-zero,
/// like real `weir-ctl --json` on a connect/scrape failure.
pub fn write_fail_stub(dir: &Path) -> PathBuf {
    let bin = dir.join("weir-ctl-fail");
    let script = "#!/bin/sh\nprintf '%s' '{\"error\":\"daemon unreachable\"}' >&2\nexit 1\n";
    fs::write(&bin, script).unwrap();
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

pub fn read_args(log: &Path) -> String {
    fs::read_to_string(log).unwrap_or_default()
}
```

- [ ] **Step 2: Write the failing tests for the runner**

`tools/weir-console/tests/ops_api.rs` — start with the runner-level tests. (More are appended in later tasks.)

```rust
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
```

- [ ] **Step 3: Run them to verify they fail to compile (no `ops` module yet)**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: FAIL — `unresolved import weir_console::ops`.

- [ ] **Step 4: Implement the module core**

`tools/weir-console/src/ops.rs`:

```rust
//! Ops Control Panel backend: shells out to the sibling `weir-ctl` binary with
//! `--json` for every operation (status, dead-letter list, requeue, drop). Nothing
//! here opens the daemon socket or parses /metrics directly — `weir-ctl` is the single
//! tested execution path, so the console can't drift from the CLI's behavior.
use std::path::{Path, PathBuf};
use std::process::Stdio;
use serde_json::{Value, json};
use tokio::process::Command;

/// Where to find `weir-ctl` and which daemon/wab to target. Cloned into `AppState`.
#[derive(Clone)]
pub struct OpsConfig {
    pub weir_ctl: PathBuf,
    pub wab_dir: PathBuf,
    pub metrics_addr: String,
    pub socket: PathBuf,
    pub read_only: bool,
}

#[derive(Debug)]
pub enum OpsError {
    /// `weir-ctl` could not be spawned (not found / not executable).
    NotFound(String),
    /// `weir-ctl` ran but exited non-zero; carries the surfaced error message.
    CtlFailed(String),
    /// `weir-ctl` stdout was not the expected JSON.
    BadOutput(String),
    /// A mutation was attempted while the server is in --read-only mode.
    ReadOnly,
}

impl std::fmt::Display for OpsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpsError::NotFound(m) => write!(f, "weir-ctl not found: {m}"),
            OpsError::CtlFailed(m) => write!(f, "weir-ctl failed: {m}"),
            OpsError::BadOutput(m) => write!(f, "weir-ctl produced unexpected output: {m}"),
            OpsError::ReadOnly => write!(f, "read-only mode: mutations are disabled"),
        }
    }
}

/// The durability tier for a requeue, validated before reaching the command line.
#[derive(Clone, Copy)]
pub enum Durability {
    Sync,
    Batched,
    Buffered,
}
impl Durability {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sync" => Some(Durability::Sync),
            "batched" => Some(Durability::Batched),
            "buffered" => Some(Durability::Buffered),
            _ => None,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Durability::Sync => "sync",
            Durability::Batched => "batched",
            Durability::Buffered => "buffered",
        }
    }
}

/// Spawn `weir-ctl <args>` (no shell), return parsed stdout JSON or a typed error.
async fn run_ctl(weir_ctl: &Path, args: &[&str]) -> Result<Value, OpsError> {
    let out = Command::new(weir_ctl)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| {
            OpsError::NotFound(format!(
                "{} ({e}); pass --weir-ctl <path> or put weir-ctl on PATH",
                weir_ctl.display()
            ))
        })?;
    if out.status.success() {
        serde_json::from_slice(&out.stdout)
            .map_err(|e| OpsError::BadOutput(format!("invalid JSON on stdout: {e}")))
    } else {
        // weir-ctl --json emits {"error": "..."} on stderr; surface it, else raw stderr.
        let stderr = String::from_utf8_lossy(&out.stderr);
        let msg = serde_json::from_str::<Value>(stderr.trim())
            .ok()
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_else(|| stderr.trim().to_string());
        Err(OpsError::CtlFailed(msg))
    }
}

/// Live operational status. Wraps `weir-ctl metrics --json`; a connect/scrape failure
/// (daemon down or wrong addr) is reported as `{"daemon":"down"}`, NOT an error — only a
/// missing binary / bad output is a real error.
pub async fn status(cfg: &OpsConfig) -> Result<Value, OpsError> {
    match run_ctl(&cfg.weir_ctl, &["--json", "metrics", "--addr", cfg.metrics_addr.as_str()]).await {
        Ok(summary) => Ok(json!({ "daemon": "up", "metrics_addr": cfg.metrics_addr, "summary": summary })),
        Err(OpsError::CtlFailed(_)) => Ok(json!({ "daemon": "down", "metrics_addr": cfg.metrics_addr })),
        Err(other) => Err(other),
    }
}

/// The actionable dead-letter list (`weir-ctl dl list --json`).
pub async fn dead_letter(cfg: &OpsConfig) -> Result<Value, OpsError> {
    let wab = cfg.wab_dir.to_string_lossy();
    run_ctl(&cfg.weir_ctl, &["--json", "dl", "list", "--wab-dir", wab.as_ref()]).await
}
```

- [ ] **Step 5: Register the module**

`tools/weir-console/src/lib.rs` — add `ops`:

```rust
pub mod ops;
pub mod server;
pub mod wab;
```

- [ ] **Step 6: Run the runner tests**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: PASS — `run_ctl_missing_binary_is_not_found` and `run_ctl_nonzero_exit_surfaces_error_message`.

- [ ] **Step 7: Commit**

```bash
git add tools/weir-console/src/ops.rs tools/weir-console/src/lib.rs tools/weir-console/tests/ops_support tools/weir-console/tests/ops_api.rs
git commit -m "feat(weir-console): ops module core (weir-ctl runner, OpsConfig, status, dl list)"
```

---

### Task 3: `status` and `dead_letter` behavior tests

**Files:**
- Modify: `tools/weir-console/tests/ops_api.rs`

- [ ] **Step 1: Write the failing tests**

Append to `tools/weir-console/tests/ops_api.rs`:

```rust
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
```

- [ ] **Step 2: Run them**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: PASS (all 5 so far). `status`/`dead_letter` already exist from Task 2, so these pass without new src changes.

- [ ] **Step 3: Commit**

```bash
git add tools/weir-console/tests/ops_api.rs
git commit -m "test(weir-console): ops status up/down + dl list parsing"
```

---

### Task 4: `requeue` and `drop_dl` — the `--yes` guard and read-only gate

**Files:**
- Modify: `tools/weir-console/src/ops.rs`
- Modify: `tools/weir-console/tests/ops_api.rs`

- [ ] **Step 1: Write the failing tests**

Append to `tools/weir-console/tests/ops_api.rs`:

```rust
use ops::Durability;

#[tokio::test]
async fn requeue_preview_omits_yes_and_passes_durability() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let log = stub.args_log.clone();
    let cfg = cfg_with(stub.bin, dir.path().to_path_buf());
    let v = ops::requeue(&cfg, Durability::Sync, false).await.unwrap();
    assert_eq!(v["would_requeue_records"], 5);
    let args = ops_support::read_args(&log);
    assert!(args.contains("requeue"), "{args}");
    assert!(args.contains("--durability sync"), "{args}");
    assert!(!args.contains("--yes"), "preview must NOT pass --yes: {args}");
}

#[tokio::test]
async fn requeue_commit_passes_yes() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let log = stub.args_log.clone();
    let cfg = cfg_with(stub.bin, dir.path().to_path_buf());
    let v = ops::requeue(&cfg, Durability::Batched, true).await.unwrap();
    assert_eq!(v["requeued_records"], 5);
    assert!(ops_support::read_args(&log).contains("--yes"));
}

#[tokio::test]
async fn drop_preview_omits_yes_commit_passes_yes() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let log = stub.args_log.clone();
    let cfg = cfg_with(stub.bin, dir.path().to_path_buf());

    let prev = ops::drop_dl(&cfg, false).await.unwrap();
    assert_eq!(prev["candidate_segments"], 2);
    assert!(!ops_support::read_args(&log).contains("--yes"));

    let done = ops::drop_dl(&cfg, true).await.unwrap();
    assert_eq!(done["dropped"], 2);
    assert!(ops_support::read_args(&log).contains("--yes"));
}

#[tokio::test]
async fn read_only_blocks_mutations_without_spawning() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let log = stub.args_log.clone();
    let mut cfg = cfg_with(stub.bin, dir.path().to_path_buf());
    cfg.read_only = true;
    assert!(matches!(ops::requeue(&cfg, Durability::Sync, true).await, Err(OpsError::ReadOnly)));
    assert!(matches!(ops::drop_dl(&cfg, true).await, Err(OpsError::ReadOnly)));
    assert!(matches!(ops::requeue(&cfg, Durability::Sync, false).await, Err(OpsError::ReadOnly)));
    // The stub must never have run.
    assert!(ops_support::read_args(&log).is_empty(), "read-only must not spawn weir-ctl");
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: FAIL — `no function requeue`/`drop_dl` in `ops`.

- [ ] **Step 3: Implement `requeue` and `drop_dl`**

Append to `tools/weir-console/src/ops.rs`:

```rust
/// Requeue ALL dead-letter records through the daemon (`weir-ctl dl requeue`).
/// `commit = false` is a dry-run preview (no `--yes`); `commit = true` executes.
pub async fn requeue(cfg: &OpsConfig, durability: Durability, commit: bool) -> Result<Value, OpsError> {
    if cfg.read_only {
        return Err(OpsError::ReadOnly);
    }
    let wab = cfg.wab_dir.to_string_lossy();
    let sock = cfg.socket.to_string_lossy();
    let mut args: Vec<&str> = vec![
        "--json", "dl", "requeue",
        "--wab-dir", wab.as_ref(),
        "--socket", sock.as_ref(),
        "--durability", durability.as_str(),
    ];
    if commit {
        args.push("--yes");
    }
    run_ctl(&cfg.weir_ctl, &args).await
}

/// Drop ALL dead-letter segments (`weir-ctl dl drop`). `commit = false` is a dry-run
/// preview (no `--yes`); `commit = true` deletes.
pub async fn drop_dl(cfg: &OpsConfig, commit: bool) -> Result<Value, OpsError> {
    if cfg.read_only {
        return Err(OpsError::ReadOnly);
    }
    let wab = cfg.wab_dir.to_string_lossy();
    let mut args: Vec<&str> = vec!["--json", "dl", "drop", "--wab-dir", wab.as_ref()];
    if commit {
        args.push("--yes");
    }
    run_ctl(&cfg.weir_ctl, &args).await
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/src/ops.rs tools/weir-console/tests/ops_api.rs
git commit -m "feat(weir-console): ops requeue/drop with dry-run preview + read-only gate"
```

---

### Task 5: Wire the Ops routes into the axum server

**Files:**
- Modify: `tools/weir-console/src/server.rs`
- Modify: `tools/weir-console/tests/ops_api.rs`

- [ ] **Step 1: Write the failing HTTP tests**

Append to `tools/weir-console/tests/ops_api.rs`:

```rust
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn ops_router(weir_ctl: PathBuf, dir: PathBuf, read_only: bool) -> axum::Router {
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    let ops = OpsConfig {
        weir_ctl,
        wab_dir: dir.clone(),
        metrics_addr: "127.0.0.1:9185".into(),
        socket: PathBuf::from("/run/weir/weir.sock"),
        read_only,
    };
    weir_console::server::router_with_ops(dir, static_dir, ops)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn http_status_and_dead_letter_ok() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let app = ops_router(stub.bin, dir.path().to_path_buf(), false);

    let resp = app.clone().oneshot(Request::get("/api/ops/status").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["daemon"], "up");

    let resp = app.oneshot(Request::get("/api/ops/dead-letter").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_requeue_preview_no_yes_commit_yes() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let log = stub.args_log.clone();
    let app = ops_router(stub.bin, dir.path().to_path_buf(), false);

    let resp = app.clone().oneshot(
        Request::post("/api/ops/requeue/preview?durability=sync").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app.oneshot(
        Request::post("/api/ops/requeue?durability=sync").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let args = ops_support::read_args(&log);
    // exactly one of the two requeue calls carried --yes (the commit, not the preview)
    let yes_lines = args.lines().filter(|l| l.contains("requeue") && l.contains("--yes")).count();
    let no_yes_lines = args.lines().filter(|l| l.contains("requeue") && !l.contains("--yes")).count();
    assert_eq!(yes_lines, 1, "{args}");
    assert_eq!(no_yes_lines, 1, "{args}");
}

#[tokio::test]
async fn http_requeue_bad_durability_is_400() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let app = ops_router(stub.bin, dir.path().to_path_buf(), false);
    let resp = app.oneshot(
        Request::post("/api/ops/requeue?durability=bogus").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_read_only_mutations_are_403() {
    let dir = tempdir().unwrap();
    let stub = ops_support::write_ok_stub(dir.path());
    let app = ops_router(stub.bin, dir.path().to_path_buf(), true);
    for path in ["/api/ops/requeue", "/api/ops/requeue/preview", "/api/ops/drop", "/api/ops/drop/preview"] {
        let resp = app.clone().oneshot(Request::post(path).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{path}");
    }
}
```

This task adds `http-body-util` as a dev-dependency (to read response bodies). In `tools/weir-console/Cargo.toml` under `[dev-dependencies]`, add:

```toml
http-body-util = "0.1"
```

(`tower` with the `util` feature and `tempfile` are already dev-deps from the Explorer tests.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: FAIL — `no function router_with_ops`.

- [ ] **Step 3: Implement the routes**

Edit `tools/weir-console/src/server.rs`. First extend the imports and `AppState`:

```rust
use crate::ops::{self, OpsConfig};
use crate::wab::{self, WabError};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub wab_dir: Arc<PathBuf>,
    pub static_dir: Arc<PathBuf>,
    pub ops: Arc<OpsConfig>,
}
```

Add the ops error mapper and the requeue query type (place near the existing `err_response`):

```rust
fn ops_err_response(e: ops::OpsError) -> Response {
    let code = match e {
        ops::OpsError::ReadOnly => StatusCode::FORBIDDEN,
        ops::OpsError::CtlFailed(_) => StatusCode::BAD_GATEWAY,
        ops::OpsError::NotFound(_) | ops::OpsError::BadOutput(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(json!({ "error": e.to_string() }))).into_response()
}

fn ops_json(r: Result<serde_json::Value, ops::OpsError>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => ops_err_response(e),
    }
}

#[derive(Deserialize)]
struct RequeueQuery {
    #[serde(default)]
    durability: Option<String>,
}

async fn requeue_handler(ops_cfg: &OpsConfig, q: RequeueQuery, commit: bool) -> Response {
    let d = q.durability.as_deref().unwrap_or("batched");
    let Some(dur) = ops::Durability::parse(d) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("invalid durability {d:?} (expected sync|batched|buffered)") })),
        )
            .into_response();
    };
    ops_json(ops::requeue(ops_cfg, dur, commit).await)
}
```

Replace the existing `router` / `router_with_static` with the three-tier builders (keep the two existing public names working so the Explorer tests are unaffected):

```rust
pub fn router(wab_dir: PathBuf) -> Router {
    let ops = default_ops(&wab_dir);
    router_with_ops(wab_dir, default_static_dir(), ops)
}

pub fn router_with_static(wab_dir: PathBuf, static_dir: PathBuf) -> Router {
    let ops = default_ops(&wab_dir);
    router_with_ops(wab_dir, static_dir, ops)
}

/// A default ops config (weir-ctl on PATH, weir-ctl/daemon defaults). Used by the
/// Explorer-only constructors; `main`/tests inject a real one via `router_with_ops`.
fn default_ops(wab_dir: &PathBuf) -> OpsConfig {
    OpsConfig {
        weir_ctl: PathBuf::from("weir-ctl"),
        wab_dir: wab_dir.clone(),
        metrics_addr: "127.0.0.1:9185".to_string(),
        socket: PathBuf::from("/run/weir/weir.sock"),
        read_only: false,
    }
}

pub fn router_with_ops(wab_dir: PathBuf, static_dir: PathBuf, ops: OpsConfig) -> Router {
    let state = AppState {
        wab_dir: Arc::new(wab_dir),
        static_dir: Arc::new(static_dir.clone()),
        ops: Arc::new(ops),
    };
    Router::new()
        .route(
            "/api/wab/segments",
            get(|State(s): State<AppState>| async move {
                match wab::inventory(&s.wab_dir) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/wab/segment",
            get(|State(s): State<AppState>, Query(q): Query<SegQuery>| async move {
                match wab::records(&s.wab_dir, &q.path, q.offset, q.limit) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/wab/dead-letter",
            get(|State(s): State<AppState>| async move {
                match wab::dead_letter(&s.wab_dir) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/wab/verify",
            get(|State(s): State<AppState>, Query(q): Query<PathQuery>| async move {
                match wab::verify(&s.wab_dir, &q.path) {
                    Ok(r) => Json(r).into_response(),
                    Err(e) => err_response(e),
                }
            }),
        )
        .route(
            "/api/ops/status",
            get(|State(s): State<AppState>| async move { ops_json(ops::status(&s.ops).await) }),
        )
        .route(
            "/api/ops/dead-letter",
            get(|State(s): State<AppState>| async move { ops_json(ops::dead_letter(&s.ops).await) }),
        )
        .route(
            "/api/ops/requeue/preview",
            post(|State(s): State<AppState>, Query(q): Query<RequeueQuery>| async move {
                requeue_handler(&s.ops, q, false).await
            }),
        )
        .route(
            "/api/ops/requeue",
            post(|State(s): State<AppState>, Query(q): Query<RequeueQuery>| async move {
                requeue_handler(&s.ops, q, true).await
            }),
        )
        .route(
            "/api/ops/drop/preview",
            post(|State(s): State<AppState>| async move { ops_json(ops::drop_dl(&s.ops, false).await) }),
        )
        .route(
            "/api/ops/drop",
            post(|State(s): State<AppState>| async move { ops_json(ops::drop_dl(&s.ops, true).await) }),
        )
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .with_state(state)
}
```

Keep the existing `SegQuery`, `def_limit`, `PathQuery`, `err_response`, and `default_static_dir` definitions as they are.

- [ ] **Step 4: Run the full ops test suite**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test ops_api`
Expected: PASS (14 tests). Also run the Explorer suite to confirm no regression:
Run: `cargo test --manifest-path tools/weir-console/Cargo.toml --test wab_api`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/src/server.rs tools/weir-console/Cargo.toml tools/weir-console/Cargo.lock tools/weir-console/tests/ops_api.rs
git commit -m "feat(weir-console): /api/ops/* routes (status, dl list, requeue, drop) + read-only 403"
```

---

### Task 6: CLI args + `weir-ctl` resolution in `main`

**Files:**
- Modify: `tools/weir-console/src/main.rs`

- [ ] **Step 1: Rewrite `main.rs`**

`tools/weir-console/src/main.rs` — add the four new args, resolve/probe `weir-ctl`, and build the router via `router_with_ops`:

```rust
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use weir_console::ops::OpsConfig;

#[derive(Parser)]
#[command(name = "weir-console", about = "Inspect and operate a weir wab directory.")]
struct Args {
    /// The weir wab directory to inspect (read-only Explorer + dead-letter target).
    #[arg(long)]
    wab_dir: PathBuf,
    /// Address to bind the console (localhost only by default).
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
    /// Daemon /metrics address for the Ops status header.
    #[arg(long, default_value = "127.0.0.1:9185")]
    metrics_addr: String,
    /// Daemon Unix socket used by `dl requeue` to re-push records.
    #[arg(long, default_value = "/run/weir/weir.sock")]
    socket: PathBuf,
    /// Path to the `weir-ctl` binary. Default: next to this exe, then PATH.
    #[arg(long)]
    weir_ctl: Option<PathBuf>,
    /// Disable all Ops mutations (requeue/drop + their previews).
    #[arg(long)]
    read_only: bool,
}

/// Resolve the weir-ctl binary: explicit flag, else a sibling of our own exe, else PATH.
fn resolve_weir_ctl(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(p) = explicit {
        return p;
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join("weir-ctl");
            if cand.is_file() {
                return cand;
            }
        }
    }
    PathBuf::from("weir-ctl")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.wab_dir.is_dir() {
        eprintln!("weir-console: --wab-dir {:?} is not a directory", args.wab_dir);
        std::process::exit(2);
    }

    let weir_ctl = resolve_weir_ctl(args.weir_ctl);
    // Probe once so a misconfigured weir-ctl is a clear warning at startup, not a
    // surprise on the first Ops action. The Explorer works without it.
    match std::process::Command::new(&weir_ctl).arg("--version").output() {
        Ok(o) if o.status.success() => {}
        _ => eprintln!(
            "weir-console: warning — could not run weir-ctl at {:?}; Ops actions will error \
             until you pass --weir-ctl <path> or put weir-ctl on PATH",
            weir_ctl
        ),
    }

    let ops = OpsConfig {
        weir_ctl,
        wab_dir: args.wab_dir.clone(),
        metrics_addr: args.metrics_addr,
        socket: args.socket,
        read_only: args.read_only,
    };
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    let app = weir_console::server::router_with_ops(args.wab_dir.clone(), static_dir, ops);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!(
        "weir-console for {:?} at http://{}{}",
        args.wab_dir,
        args.bind,
        if args.read_only { " (read-only)" } else { "" }
    );
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 2: Build + run the whole tool test suite**

Run: `cargo build --manifest-path tools/weir-console/Cargo.toml`
Expected: `Finished`.
Run: `cargo test --manifest-path tools/weir-console/Cargo.toml`
Expected: all pass (5 wab + 14 ops).

- [ ] **Step 3: Curl smoke (Ops endpoints, no real daemon)**

```bash
mkdir -p "$CLAUDE_JOB_DIR/tmp/ops-smoke/dead_letter"
cargo build --manifest-path tools/weir-console/Cargo.toml
target_dir=$(cargo metadata --manifest-path tools/weir-console/Cargo.toml --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
"$target_dir/debug/weir-console" --wab-dir "$CLAUDE_JOB_DIR/tmp/ops-smoke" --bind 127.0.0.1:18811 --read-only &
echo $! > /tmp/wc-ops.pid
sleep 2
curl -s http://127.0.0.1:18811/ops.html | grep -q "Ops" && echo "OPS PAGE OK" || echo "OPS PAGE MISSING (expected until Task 7)"
curl -s http://127.0.0.1:18811/api/ops/status     # daemon down (no metrics) -> {"daemon":"down",...}
echo
curl -s -o /dev/null -w "requeue(read-only)=%{http_code}\n" -X POST http://127.0.0.1:18811/api/ops/requeue
kill "$(cat /tmp/wc-ops.pid)"
```

Expected: `/api/ops/status` returns `{"daemon":"down",...}` (no real daemon, not an error); the read-only requeue returns `403`. (The `ops.html` grep may say MISSING until Task 7 — that's fine here.) Kill only the PID we started; no `pkill`.

- [ ] **Step 4: Commit**

```bash
git add tools/weir-console/src/main.rs
git commit -m "feat(weir-console): ops CLI args (--metrics-addr/--socket/--weir-ctl/--read-only) + weir-ctl resolve/probe"
```

---

### Task 7: Frontend — Ops page (`ops.html` + `ops.js`) and nav activation

**Files:**
- Create: `tools/weir-console/static/ops.html`
- Create: `tools/weir-console/static/ops.js`
- Modify: `tools/weir-console/static/index.html` (activate the Ops nav link)

- [ ] **Step 1: Create `ops.html`**

`tools/weir-console/static/ops.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>weir-console · Ops</title>
<link rel="stylesheet" href="weir.css" />
<style>
  .wc-chip { font-size: 10px; padding: 1px 6px; border: 1px solid var(--n-border); margin-left: 4px; }
  .wc-ok { color: var(--n-green); } .wc-bad { color: var(--n-rose); }
  .wc-rec { font-family: 'JetBrains Mono', monospace; font-size: 11px; border-bottom: 1px solid var(--n-border-lt); padding: 4px 0; }
  .wc-actions button { margin-right: 8px; }
  #ops-status { font-size: 12px; padding: 8px; border: 1px solid var(--n-border); margin-bottom: 12px; }
  #modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6); align-items: center; justify-content: center; }
  .wc-modal-box { background: var(--n-bg); border: 1px solid var(--n-border); padding: 16px; max-width: 520px; width: 90%; }
</style>
</head>
<body>
<div class="statusbar top">
  <span>weir-console · Ops · <span data-weir-version></span></span>
  <span class="sb-spacer"></span>
  <span class="sb-dim">dead-letter management</span>
</div>
<nav class="nav">
  <a href="index.html">Explorer</a>
  <a href="ops.html" class="active">Ops</a>
  <a href="#" class="wc-disabled" title="coming soon">Live</a>
</nav>
<div class="wrap">
  <div id="ops-status">loading status…</div>
  <div class="panel">
    <div class="panel-title">Dead-letter store
      <span class="pt-right wc-actions">
        <button id="act-requeue">Requeue all</button>
        <button id="act-drop">Drop all</button>
      </span>
    </div>
    <div class="panel-body" id="dl-body">loading…</div>
  </div>
</div>
<div id="modal"></div>
<script src="ops.js"></script>
</body>
</html>
```

- [ ] **Step 2: Create `ops.js`**

`tools/weir-console/static/ops.js`:

```js
const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach((e) => (e.textContent = "1.2.0"));

async function getJSON(url, opts) {
  const r = await fetch(url, opts);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}
function chip(t, c) { return `<span class="wc-chip ${c || ""}">${t}</span>`; }

// ── pure helpers (unit-tested) ──
function statusLine(s) {
  if (!s || s.daemon !== "up") return `daemon ${chip("down", "wc-bad")}`;
  const m = s.summary || {};
  const health = m.sink_health === "healthy" ? chip("healthy", "wc-ok") : chip(m.sink_health || "?", "wc-bad");
  const alarms = (m.flusher_panics || 0) + (m.fsync_failures || 0);
  const alarmChip = alarms > 0 ? chip(`${alarms} alarm(s)`, "wc-bad") : "";
  return `daemon ${chip("up", "wc-ok")} · sink ${m.sink_type || "?"} ${health} · ` +
    `acc ${m.accepted || 0} / ack ${m.ack || 0} / nack ${m.nack || 0} · ` +
    `queue ${m.queue_depth || 0} · fsync ${m.fsync_avg_ms ?? "?"}ms · ` +
    `wab ${m.wab_bytes_on_disk || 0}B · dl ${m.dead_letter_bytes_on_disk || 0}B ${alarmChip}`;
}
function dlSummary(dl) { return `${dl.count || 0} dead-letter segment(s) · ${dl.total_bytes || 0} bytes`; }
function dropConfirmMatches(typed, count) { return String(typed).trim() === String(count); }
function requeuePreviewText(p) {
  const recs = p.would_requeue_records ?? p.requeued_records ?? 0;
  const segs = p.would_requeue_segments ?? p.requeued_segments ?? 0;
  const skipped = p.unreadable ?? 0;
  return `would requeue ${recs} record(s) from ${segs} segment(s)` +
    (skipped ? ` · ${skipped} corrupt segment(s) skipped` : "");
}

// ── live status ──
let daemonLive = false;
async function refreshStatus() {
  try {
    const s = await getJSON("/api/ops/status");
    daemonLive = s.daemon === "up";
    $("#ops-status").innerHTML = statusLine(s);
  } catch (e) {
    $("#ops-status").innerHTML = `<span class="wc-bad">status error: ${e.message}</span>`;
  }
}

// ── dead-letter panel ──
async function refreshDeadLetter() {
  try {
    const dl = await getJSON("/api/ops/dead-letter");
    const rows = (dl.segments || []).map((s) => `<div class="wc-rec">${s.segment} · ${s.bytes}B</div>`).join("");
    $("#dl-body").innerHTML = `<p class="sb-dim">${dlSummary(dl)}</p>${rows || "<p class='sb-dim'>empty</p>"}`;
  } catch (e) {
    $("#dl-body").innerHTML = `<span class="wc-bad">error: ${e.message}</span>`;
  }
}

// ── modal + live-confirm ──
function openModal(html) { const m = $("#modal"); m.innerHTML = `<div class="wc-modal-box">${html}</div>`; m.style.display = "flex"; }
function closeModal() { const m = $("#modal"); if (!m) return; m.innerHTML = ""; m.style.display = "none"; }
function liveWarn() {
  return daemonLive
    ? `<p class="wc-bad">⚠ the daemon appears to be running. <label><input type="checkbox" id="live-ok"> I understand the daemon is running</label></p>`
    : "";
}
function liveConfirmed() { if (!daemonLive) return true; const c = $("#live-ok"); return !!(c && c.checked); }

// ── requeue flow ──
async function requeueFlow() {
  let preview;
  try { preview = await getJSON("/api/ops/requeue/preview?durability=batched", { method: "POST" }); }
  catch (e) { openModal(`<p class="wc-bad">preview failed: ${e.message}</p><button id="x-close">close</button>`); $("#x-close").onclick = closeModal; return; }
  openModal(
    `<div class="panel-title">Requeue all</div>
     <p>${requeuePreviewText(preview)}</p>
     <p class="sb-dim">Re-delivery is at-least-once; identical payloads are deduped by the sink's idempotency key.</p>
     <label>Durability: <select id="rq-dur">
       <option value="batched" selected>batched</option><option value="sync">sync</option><option value="buffered">buffered</option>
     </select></label>
     ${liveWarn()}
     <div class="wc-actions"><button id="rq-go">Requeue</button><button id="rq-cancel">Cancel</button></div>
     <div id="rq-result"></div>`
  );
  $("#rq-cancel").onclick = closeModal;
  $("#rq-go").onclick = async () => {
    if (!liveConfirmed()) { $("#rq-result").innerHTML = `<span class="wc-bad">tick the box to proceed</span>`; return; }
    const dur = $("#rq-dur").value;
    try {
      const r = await getJSON(`/api/ops/requeue?durability=${encodeURIComponent(dur)}`, { method: "POST" });
      $("#rq-result").innerHTML = `<span class="wc-ok">requeued ${r.requeued_records ?? 0} record(s)</span>`;
      refreshStatus(); refreshDeadLetter();
    } catch (e) { $("#rq-result").innerHTML = `<span class="wc-bad">${e.message}</span>`; }
  };
}

// ── drop flow ──
async function dropFlow() {
  let preview;
  try { preview = await getJSON("/api/ops/drop/preview", { method: "POST" }); }
  catch (e) { openModal(`<p class="wc-bad">preview failed: ${e.message}</p><button id="x-close">close</button>`); $("#x-close").onclick = closeModal; return; }
  const count = preview.candidate_segments ?? 0;
  openModal(
    `<div class="panel-title">Drop all dead-letter segments</div>
     <p class="wc-bad">IRREVERSIBLE — would delete ${count} segment(s) / ${preview.candidate_records ?? 0} record(s) / ${preview.candidate_bytes ?? 0} bytes.</p>
     <label>Type the segment count (<b>${count}</b>) to confirm: <input id="dr-count" /></label>
     ${liveWarn()}
     <div class="wc-actions"><button id="dr-go" disabled>Drop</button><button id="dr-cancel">Cancel</button></div>
     <div id="dr-result"></div>`
  );
  $("#dr-cancel").onclick = closeModal;
  const inp = $("#dr-count"), go = $("#dr-go");
  inp.oninput = () => { go.disabled = !dropConfirmMatches(inp.value, count); };
  go.onclick = async () => {
    if (!liveConfirmed()) { $("#dr-result").innerHTML = `<span class="wc-bad">tick the box to proceed</span>`; return; }
    try {
      const r = await getJSON("/api/ops/drop", { method: "POST" });
      $("#dr-result").innerHTML = `<span class="wc-ok">dropped ${r.dropped ?? 0} segment(s)</span>`;
      refreshStatus(); refreshDeadLetter();
    } catch (e) { $("#dr-result").innerHTML = `<span class="wc-bad">${e.message}</span>`; }
  };
}

function wireActions() {
  const rq = $("#act-requeue"), dr = $("#act-drop");
  if (rq) rq.onclick = requeueFlow;
  if (dr) dr.onclick = dropFlow;
}

async function main() {
  wireActions();
  await refreshStatus();
  await refreshDeadLetter();
  if (typeof setInterval === "function") setInterval(refreshStatus, 5000);
}
main();
```

- [ ] **Step 3: Activate the Ops nav link in `index.html`**

In `tools/weir-console/static/index.html`, change the disabled Ops link:

```html
  <a href="#" class="wc-disabled" title="coming soon">Ops</a>
```

to:

```html
  <a href="ops.html">Ops</a>
```

- [ ] **Step 4: Validate the static files + curl smoke**

```bash
node --check tools/weir-console/static/ops.js && echo "OK: ops.js parses"
mkdir -p "$CLAUDE_JOB_DIR/tmp/ops-smoke2"
target_dir=$(cargo metadata --manifest-path tools/weir-console/Cargo.toml --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
cargo build --manifest-path tools/weir-console/Cargo.toml
"$target_dir/debug/weir-console" --wab-dir "$CLAUDE_JOB_DIR/tmp/ops-smoke2" --bind 127.0.0.1:18812 &
echo $! > /tmp/wc-ops2.pid
sleep 2
curl -s http://127.0.0.1:18812/ops.html | grep -q 'src="ops.js"' && echo "OPS HTML OK"
curl -s http://127.0.0.1:18812/index.html | grep -q 'href="ops.html"' && echo "NAV ACTIVATED OK"
kill "$(cat /tmp/wc-ops2.pid)"
```

Expected: `OK: ops.js parses`, `OPS HTML OK`, `NAV ACTIVATED OK`. Kill only the PID we started.

- [ ] **Step 5: Commit**

```bash
git add tools/weir-console/static/ops.html tools/weir-console/static/ops.js tools/weir-console/static/index.html
git commit -m "feat(weir-console): Ops frontend (status header, dead-letter requeue/drop flows)"
```

---

### Task 8: Frontend smoke test (node, no backend)

**Files:**
- Create: `tools/weir-console/static/ops.test.mjs`

- [ ] **Step 1: Write the DOM-stubbed render + flow test**

`tools/weir-console/static/ops.test.mjs` — mirrors `explorer.test.mjs`: strips the trailing auto-`main()`, wraps the source in a `Function` so its top-level declarations become returnable, and injects `document`/`fetch` stubs that memoise one element per selector and record fetch calls.

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// ops.js is a plain <script> (no exports) that runs document.querySelectorAll(...) and
// main() at load. We strip the auto-main, wrap the source so its declarations are
// returnable, and inject minimal document/fetch stubs (memoised elements; recorded fetch).
function loadOps(documentStub, fetchStub) {
  let src = readFileSync(new URL("./ops.js", import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, "");
  const factory = new Function(
    "document",
    "fetch",
    "setInterval",
    src +
      "\nreturn { statusLine, dlSummary, dropConfirmMatches, requeuePreviewText, refreshStatus, refreshDeadLetter, requeueFlow, dropFlow };"
  );
  // setInterval is referenced in main (stripped) but keep a no-op for safety.
  return factory(documentStub, fetchStub, () => 0);
}

function makeDom() {
  const els = {};
  const makeEl = () => ({
    innerHTML: "", textContent: "", value: "", disabled: false, checked: false,
    onclick: null, oninput: null, style: {},
    querySelector: () => null, querySelectorAll: () => [],
  });
  return {
    els,
    document: {
      querySelector: (sel) => (els[sel] ??= makeEl()),
      querySelectorAll: () => [],
    },
  };
}

function makeFetch(routes) {
  const calls = [];
  const fetchStub = (url, opts) => {
    calls.push({ url, opts });
    const key = Object.keys(routes).find((k) => url.startsWith(k));
    const body = key ? routes[key] : {};
    return Promise.resolve({ ok: true, statusText: "OK", json: () => Promise.resolve(body) });
  };
  return { fetchStub, calls };
}

const UP = {
  daemon: "up",
  summary: { accepted: 5, ack: 4, nack: 1, fsync_avg_ms: 50.0, queue_depth: 7, wab_bytes_on_disk: 4096, dead_letter_bytes_on_disk: 2048, sink_type: "http", sink_health: "healthy", flusher_panics: 0, fsync_failures: 0 },
};
const DL = { dead_letter_dir: "/x", count: 2, total_bytes: 1024, segments: [{ segment: "dl_00000001.wab.sealed", bytes: 512 }, { segment: "dl_00000002.wab.sealed", bytes: 512 }] };

const tick = () => new Promise((r) => setTimeout(r, 0));

test("pure helpers format status / dl / previews / drop gate", () => {
  // NOTE: ordering — these are pure and don't touch the DOM, so any stub works.
  const dom = makeDom();
  const app = loadOps(dom.document, () => Promise.resolve({ ok: true, json: () => Promise.resolve({}) }));
  assert.match(app.statusLine(UP), /daemon/);
  assert.match(app.statusLine(UP), /healthy/);
  assert.match(app.statusLine({ daemon: "down" }), /down/);
  assert.equal(app.dlSummary(DL), "2 dead-letter segment(s) · 1024 bytes");
  assert.ok(app.dropConfirmMatches("2", 2));
  assert.ok(!app.dropConfirmMatches("1", 2));
  assert.match(app.requeuePreviewText({ would_requeue_records: 5, would_requeue_segments: 2, unreadable: 0 }), /would requeue 5 record\(s\) from 2 segment\(s\)/);
});

test("status header + dead-letter panel render from mock JSON", async () => {
  const dom = makeDom();
  const { fetchStub } = makeFetch({ "/api/ops/status": UP, "/api/ops/dead-letter": DL });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus();
  await app.refreshDeadLetter();
  assert.match(dom.els["#ops-status"].innerHTML, /daemon/);
  assert.match(dom.els["#ops-status"].innerHTML, /queue 7/);
  assert.match(dom.els["#dl-body"].innerHTML, /dl_00000001\.wab\.sealed/);
});

test("drop flow: confirm button stays disabled until the typed count matches", async () => {
  const dom = makeDom();
  const { fetchStub } = makeFetch({
    "/api/ops/status": { daemon: "down" },
    "/api/ops/drop/preview": { dry_run: true, candidate_segments: 2, candidate_records: 5, candidate_bytes: 1024 },
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus(); // daemonLive = false
  await app.dropFlow();
  const inp = dom.els["#dr-count"], go = dom.els["#dr-go"];
  assert.equal(go.disabled, true, "starts disabled");
  inp.value = "9"; inp.oninput();
  assert.equal(go.disabled, true, "wrong count keeps it disabled");
  inp.value = "2"; inp.oninput();
  assert.equal(go.disabled, false, "matching count enables it");
});

test("requeue flow: confirm calls the commit endpoint", async () => {
  const dom = makeDom();
  const { fetchStub, calls } = makeFetch({
    "/api/ops/status": { daemon: "down" },
    "/api/ops/requeue/preview": { would_requeue_records: 5, would_requeue_segments: 2, unreadable: 0 },
    "/api/ops/requeue": { requeued_records: 5, requeued_segments: 2 },
    "/api/ops/dead-letter": DL,
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus();
  await app.requeueFlow();
  dom.els["#rq-dur"].value = "batched";
  await dom.els["#rq-go"].onclick();
  await tick();
  const committed = calls.find((c) => c.url.startsWith("/api/ops/requeue?") && c.opts && c.opts.method === "POST");
  assert.ok(committed, "a POST to the requeue commit endpoint was made");
  assert.match(dom.els["#rq-result"].innerHTML, /requeued 5/);
});

test("live daemon gates the action until the box is checked", async () => {
  const dom = makeDom();
  const { fetchStub, calls } = makeFetch({
    "/api/ops/status": UP, // daemon live
    "/api/ops/drop/preview": { dry_run: true, candidate_segments: 2, candidate_records: 5, candidate_bytes: 1024 },
    "/api/ops/drop": { dropped: 2 },
  });
  const app = loadOps(dom.document, fetchStub);
  await app.refreshStatus(); // daemonLive = true
  await app.dropFlow();
  const inp = dom.els["#dr-count"], go = dom.els["#dr-go"];
  inp.value = "2"; inp.oninput();
  // live-ok checkbox not checked -> clicking go must NOT call the drop endpoint
  await go.onclick();
  await tick();
  assert.ok(!calls.some((c) => c.url === "/api/ops/drop"), "must not drop until the live box is ticked");
  assert.match(dom.els["#dr-result"].innerHTML, /tick the box/);
});
```

- [ ] **Step 2: Run it**

Run: `node --test tools/weir-console/static/ops.test.mjs`
Expected: `# pass 5`, `# fail 0`. Do not weaken assertions to force a pass — if `ops.js` genuinely changed behavior, fix `ops.js`; if a stub is mechanically incomplete, fix the harness but keep every assertion.

- [ ] **Step 3: Commit**

```bash
git add tools/weir-console/static/ops.test.mjs
git commit -m "test(weir-console): ops frontend smoke (status, dl panel, drop gate, requeue commit, live gate)"
```

---

### Task 9: README + final polish

**Files:**
- Modify: `tools/weir-console/README.md`

- [ ] **Step 1: Extend the README**

Add an Ops section to `tools/weir-console/README.md` (after the existing Explorer/HTTP-API content). Insert:

```markdown
## Ops Control Panel

The **Ops** tab manages the **dead-letter store** and shows a live status header. It
**shells out to `weir-ctl`** for every operation (`--json`), so it reuses the CLI's
tested behavior rather than re-implementing it.

- **Status header** — `weir-ctl metrics --json`: daemon up/down, sink health, accepted/
  ack/nack, queue depth, avg fsync ms, WAB + dead-letter bytes, and panic/fsync-failure
  alarms. A down daemon is shown plainly, not as an error.
- **Requeue all** — re-submits every dead-lettered record through the daemon's socket
  (at-least-once; the sink's idempotency key dedupes identical payloads). Shows a dry-run
  preview, a durability selector, then executes.
- **Drop all** — permanently deletes the dead-letter segments. Shows a dry-run preview
  and requires typing the segment count to confirm.
- When the daemon **appears live**, both actions require an extra confirmation.

### Ops flags

- `--metrics-addr <host:port>` — daemon `/metrics` for the status header (default
  `127.0.0.1:9185`).
- `--socket <path>` — daemon Unix socket used by requeue (default `/run/weir/weir.sock`).
- `--weir-ctl <path>` — the `weir-ctl` binary (default: next to this exe, then `PATH`).
- `--read-only` — disable all Ops mutations (requeue/drop + previews); status + listing
  remain.

### Ops HTTP API

- `GET /api/ops/status`, `GET /api/ops/dead-letter`
- `POST /api/ops/requeue/preview`, `POST /api/ops/requeue?durability=<sync|batched|buffered>`
- `POST /api/ops/drop/preview`, `POST /api/ops/drop`

> The console is unauthenticated and localhost-only by default; it both reveals record
> contents and can mutate the dead-letter store. Do not expose it. Use `--read-only` for
> shared/demo instances.
```

- [ ] **Step 2: fmt + clippy the tool**

Run: `cargo fmt --manifest-path tools/weir-console/Cargo.toml`
Run: `cargo clippy --manifest-path tools/weir-console/Cargo.toml --all-targets -- -D warnings`
Expected: clean. Fix any warning at the source (no `#[allow]`).

- [ ] **Step 3: Full tool test + workspace-untouched check**

Run: `cargo test --manifest-path tools/weir-console/Cargo.toml`
Expected: all pass (5 wab + 14 ops integration).
Run: `node --test tools/weir-console/static/ops.test.mjs && node --test tools/weir-console/static/explorer.test.mjs`
Expected: ops 5/5, explorer 3/3.
Run: `cargo build --workspace && git status --short Cargo.lock`
Expected: workspace builds; the **root** `Cargo.lock` is unchanged (empty output). If the root lockfile shows modified, revert that — only `tools/weir-console/Cargo.lock` may change.

- [ ] **Step 4: Commit**

```bash
git add tools/weir-console/README.md tools/weir-console/src tools/weir-console/Cargo.lock
git commit -m "docs(weir-console): README for the Ops Control Panel + fmt/clippy polish"
```

---

## Self-review

**Spec coverage:** status header — Task 2 (`status`) + Task 7 (`statusLine`); dead-letter list — Task 2 (`dead_letter`) + Task 7; requeue (preview/commit, durability, at-least-once note) — Task 4 + Task 7; drop (preview/commit, typed-count confirm) — Task 4 + Task 7; live-daemon warning + extra confirm — Task 7 (`liveWarn`/`liveConfirmed`) + Task 8 (live-gate test); shell-out to `weir-ctl --json` (dry-run = preview, `--yes` = commit) — Tasks 2/4, guarded by tests in Task 4/5; new CLI args + `weir-ctl` resolution — Task 6; `--read-only` disables all four mutation endpoints — Task 4 (module) + Task 5 (403) + Task 6 (flag); nav activation — Task 7; testing (stub `weir-ctl` + node smoke) — Tasks 2–5 + Task 8; README + read-only/localhost warning — Task 9; out-of-scope (Live view, daemon control, per-segment, auth) — not implemented. ✓ all covered.

**Placeholder scan:** no TBD/TODO; every code step has complete code and exact commands with expected output.

**Type consistency:** `OpsConfig { weir_ctl, wab_dir, metrics_addr, socket, read_only }`, `OpsError { NotFound, CtlFailed, BadOutput, ReadOnly }`, `Durability { Sync, Batched, Buffered }::{parse, as_str}`, and `ops::{status, dead_letter, requeue, drop_dl}` are used identically across the module, the server routes, `main`, and the tests. Frontend field reads (`s.daemon`, `s.summary.*`, `dl.count/segments[].segment/bytes`, `p.would_requeue_*`/`candidate_*`, `r.requeued_records`/`dropped`) match the verified `weir-ctl --json` shapes and the stub. The server keeps `router`/`router_with_static` working (delegating to `router_with_ops`) so the existing Explorer tests are unaffected.
