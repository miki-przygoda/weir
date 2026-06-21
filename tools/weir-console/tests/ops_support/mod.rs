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
