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
