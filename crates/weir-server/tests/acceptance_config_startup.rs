//! Acceptance tests for epic d10-and-batch-error, behaviour group
//! config-startup (spec flows 1-3): what `weir-server` does at startup when
//! the config file it was given, or the default one, is or is not there.
//!
//! Each test drives the built `weir-server` binary (or the Docker image built
//! from `deploy/docker/Dockerfile`) and judges it by its exit status, its log
//! and what it leaves on disk, never by its internals.
//!
//! The Docker tests (CONFIG-STARTUP-05 and -08) need a `weir-server:local`
//! image built from `deploy/docker/Dockerfile`; they run only when
//! `WEIR_ACCEPTANCE_DOCKER=1` is set and fail (rather than skip) when it is set
//! and the image or Docker is missing. docker.yml's smoke job sets it and
//! runs both against the image it builds (`config_startup_08_docker_tests_run_in_ci`
//! guards that wiring).

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_weir-server");
const DEFAULT_PATH: &str = "/etc/weir/weir.toml";
const IMAGE: &str = "weir-server:local";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

// ── helpers ──────────────────────────────────────────────────────────────────

/// A scratch dir under the system temp dir, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "weir_acc_cfg_{tag}_{}_{}",
            std::process::id(),
            rand_suffix()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A daemon layout in a scratch dir: an existing (empty) WAB dir, a socket
/// dir, and a free metrics port.
struct Layout {
    scratch: Scratch,
    wab_dir: PathBuf,
    socket_path: PathBuf,
    metrics_port: u16,
}

impl Layout {
    fn new(tag: &str) -> Self {
        let scratch = Scratch::new(tag);
        let wab_dir = scratch.path().join("wab");
        let run = scratch.path().join("run");
        fs::create_dir_all(&wab_dir).unwrap();
        fs::create_dir_all(&run).unwrap();
        Layout {
            socket_path: run.join("weir.sock"),
            wab_dir,
            metrics_port: free_port(),
            scratch,
        }
    }

    fn admin_socket(&self) -> PathBuf {
        self.scratch.path().join("wab.admin.sock")
    }

    /// Everything under the scratch dir (files and dirs), relative paths.
    fn tree(&self) -> Vec<PathBuf> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
            if let Ok(rd) = fs::read_dir(dir) {
                for e in rd.flatten() {
                    let p = e.path();
                    out.push(p.strip_prefix(root).unwrap().to_path_buf());
                    if p.is_dir() {
                        walk(root, &p, out);
                    }
                }
            }
        }
        let mut out = Vec::new();
        walk(self.scratch.path(), self.scratch.path(), &mut out);
        out.sort();
        out
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A base command for the daemon: no inherited `WEIR_*` env, logs piped.
fn weir() -> Command {
    let mut cmd = Command::new(BIN);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("WEIR_") {
            cmd.env_remove(k);
        }
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Runs a command that is expected to exit on its own; kills it after
/// `limit`. Returns the output and whether it exited by itself.
fn run_to_exit(mut cmd: Command, limit: Duration) -> (Output, bool, Duration) {
    let started = Instant::now();
    let mut child = cmd.spawn().expect("spawn weir-server");
    let exited = loop {
        if child.try_wait().unwrap().is_some() {
            break true;
        }
        if started.elapsed() >= limit {
            let _ = child.kill();
            break false;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let took = started.elapsed();
    (child.wait_with_output().unwrap(), exited, took)
}

fn combined(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A running daemon whose stdout and stderr lines are collected.
struct Running {
    child: Child,
    rx: mpsc::Receiver<String>,
    log: Vec<String>,
}

impl Running {
    fn spawn(mut cmd: Command) -> Self {
        let mut child = cmd.spawn().expect("spawn weir-server");
        let (tx, rx) = mpsc::channel();
        let streams: Vec<Box<dyn Read + Send>> = vec![
            Box::new(child.stdout.take().unwrap()),
            Box::new(child.stderr.take().unwrap()),
        ];
        for s in streams {
            let tx = tx.clone();
            thread::spawn(move || {
                for line in BufReader::new(s).lines().map_while(Result::ok) {
                    let _ = tx.send(line);
                }
            });
        }
        Running {
            child,
            rx,
            log: Vec::new(),
        }
    }

    /// Collects log lines until `done(log)` holds, the process exits, or the
    /// timeout passes.
    fn wait_for(&mut self, done: impl Fn(&[String]) -> bool) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while Instant::now() < deadline && !done(&self.log) {
            match self.rx.recv_timeout(Duration::from_millis(50)) {
                Ok(line) => self.log.push(line),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self.child.try_wait().unwrap().is_some() {
                        // drain whatever is left
                        while let Ok(l) = self.rx.recv_timeout(Duration::from_millis(200)) {
                            self.log.push(l);
                        }
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    fn line(&self, needle: &str) -> Option<&String> {
        self.log.iter().find(|l| l.contains(needle))
    }

    fn dump(&self) -> String {
        self.log.join("\n")
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The daemon is up: it logged "weir starting" and bound its producer socket.
fn started(layout_socket: PathBuf) -> impl Fn(&[String]) -> bool {
    move |log: &[String]| log.iter().any(|l| l.contains("weir starting")) && layout_socket.exists()
}

/// Whether `path` is known to be absent. A file there, or a path that can't
/// be checked (`try_exists` errs, e.g. an unreadable /etc/weir), returns
/// false with a skip note on stderr: the tests that need no default file
/// skip on such a host, as `system.rs` does, instead of failing.
fn default_config_absent(path: &Path) -> bool {
    match path.try_exists() {
        Ok(false) => true,
        Ok(true) => {
            eprintln!("skipping: {} exists on this host", path.display());
            false
        }
        Err(e) => {
            eprintln!(
                "skipping: cannot tell whether {} exists: {e}",
                path.display()
            );
            false
        }
    }
}

fn d10_message(path: &Path) -> String {
    format!(
        "config file {} (passed with --config) does not exist",
        path.display()
    )
}

// ── CONFIG-STARTUP-01 ────────────────────────────────────────────────────────

/// Given no file at /etc/weir/missing.toml, `weir-server --config
/// /etc/weir/missing.toml` exits at once, non-zero, with the D10 message.
#[test]
fn config_startup_01_explicit_missing_config_refuses_to_start() {
    let missing = Path::new("/etc/weir/missing.toml");
    assert!(
        !missing.exists(),
        "precondition: {} must not exist",
        missing.display()
    );
    let layout = Layout::new("01");

    let mut cmd = weir();
    cmd.args(["--config", "/etc/weir/missing.toml"])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let (out, exited, took) = run_to_exit(cmd, Duration::from_secs(15));
    let log = combined(&out);

    assert!(exited, "weir-server kept running: {log}");
    assert!(
        took < Duration::from_secs(5),
        "exits at once, took {took:?}: {log}"
    );
    assert!(
        !out.status.success(),
        "must exit non-zero: {:?}\n{log}",
        out.status
    );
    // weir's existing config-error exit status: an invalid config value takes
    // the same exit, so compare against it rather than a hard-coded number.
    let mut bad = weir();
    bad.args(["--shard-count", "0"])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let (bad_out, bad_exited, _) = run_to_exit(bad, Duration::from_secs(15));
    assert!(
        bad_exited && !bad_out.status.success(),
        "{}",
        combined(&bad_out)
    );
    assert!(
        combined(&bad_out).contains("invalid config value"),
        "reference config error: {}",
        combined(&bad_out)
    );
    assert_eq!(
        out.status.code(),
        bad_out.status.code(),
        "same exit status as another config error: {log}"
    );
    assert!(
        log.contains("config file /etc/weir/missing.toml (passed with --config) does not exist"),
        "D10 message missing: {log}"
    );
}

// ── CONFIG-STARTUP-02 ────────────────────────────────────────────────────────

/// The D10 error comes from config loading, before preflight, recovery or any
/// bind: with the metrics port already taken and a WAB dir that does not
/// exist, the error is still the config one, and nothing is created.
#[test]
fn config_startup_02_d10_error_touches_nothing() {
    let layout = Layout::new("02");
    let missing = layout.scratch.path().join("no-such.toml");
    // Seed a WAB dir with nothing in it; preflight would lock it (.weir.lock)
    // and recovery would create shard dirs.
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let taken_port = taken.local_addr().unwrap().port();

    let mut cmd = weir();
    cmd.args(["--config", missing.to_str().unwrap()])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", taken_port.to_string())
        .env("WEIR_LOG_LEVEL", "debug");
    let before = layout.tree();
    let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
    let after = layout.tree();
    let log = combined(&out);
    drop(taken);

    assert!(
        exited && !out.status.success(),
        "must refuse to start: {log}"
    );
    assert!(log.contains(&d10_message(&missing)), "D10 message: {log}");
    // A bind of the taken metrics port would have produced a port error first.
    assert!(
        !log.to_lowercase().contains("address in use") && !log.contains("in use"),
        "no socket bind may be attempted: {log}"
    );
    assert_eq!(before, after, "no file may be created: {log}");
    assert!(
        fs::read_dir(&layout.wab_dir).unwrap().next().is_none(),
        "no WAB file or lock in the WAB dir: {log}"
    );
    assert!(!layout.socket_path.exists(), "no producer socket: {log}");
    assert!(!layout.admin_socket().exists(), "no admin socket: {log}");
    assert!(
        !missing.exists(),
        "the missing config file must not be created: {log}"
    );

    // And with a WAB dir that does not exist, the error is still the config
    // one: config loading runs before any path validation or preflight.
    let mut cmd = weir();
    let absent_wab = layout.scratch.path().join("absent-wab");
    cmd.args(["--config", missing.to_str().unwrap()])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &absent_wab)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
    let log = combined(&out);
    assert!(
        exited && !out.status.success(),
        "must refuse to start: {log}"
    );
    assert!(
        log.contains(&d10_message(&missing)),
        "D10 message first: {log}"
    );
    assert!(
        !absent_wab.exists(),
        "the WAB dir must not be created: {log}"
    );
}

// ── CONFIG-STARTUP-03 ────────────────────────────────────────────────────────

/// The D10 error names the path and says --config, and prints no env values
/// (and, the file not existing, no file contents).
#[test]
fn config_startup_03_d10_error_prints_the_path_only() {
    let layout = Layout::new("03");
    let missing = layout.scratch.path().join("absent-weir.toml");
    const TOKEN: &str = "acc03-bearer-token-value-7f3a";
    const URL_SECRET: &str = "acc03urlpassword9c1e";

    let mut cmd = weir();
    cmd.args(["--config", missing.to_str().unwrap()])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string())
        .env("WEIR_LOG_LEVEL", "trace")
        .env("WEIR_SINK_TYPE", "http")
        .env(
            "WEIR_SINK_URL",
            format!("http://acc03user:{URL_SECRET}@127.0.0.1:1/ingest"),
        )
        .env("WEIR_SINK_BEARER_TOKEN", TOKEN);
    let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
    let log = combined(&out);

    assert!(
        exited && !out.status.success(),
        "must refuse to start: {log}"
    );
    assert!(
        log.contains(&d10_message(&missing)),
        "names the path: {log}"
    );
    assert!(log.contains("passed with --config"), "{log}");
    for secret in [TOKEN, URL_SECRET, "acc03user", "127.0.0.1:1"] {
        assert!(!log.contains(secret), "env value {secret:?} printed: {log}");
    }
    let wab = layout.wab_dir.to_string_lossy().into_owned();
    assert!(!log.contains(&wab), "env value (wab dir) printed: {log}");
    let sock = layout.socket_path.to_string_lossy().into_owned();
    assert!(!log.contains(&sock), "env value (socket) printed: {log}");
}

// ── CONFIG-STARTUP-04 ────────────────────────────────────────────────────────

/// A valid file passed with --config is read and the server starts with its
/// values.
#[test]
fn config_startup_04_explicit_existing_config_is_read() {
    let layout = Layout::new("04");
    let cfg = layout.scratch.path().join("weir.toml");
    fs::write(
        &cfg,
        format!(
            "[server]\nsocket_path = \"{}\"\nwab_dir = \"{}\"\nmetrics_port = {}\n\
             shard_count = 3\nsink_type = \"noop\"\nlog_level = \"info\"\n",
            layout.socket_path.display(),
            layout.wab_dir.display(),
            layout.metrics_port
        ),
    )
    .unwrap();

    let mut cmd = weir();
    cmd.args(["--config", cfg.to_str().unwrap()]);
    let mut d = Running::spawn(cmd);
    d.wait_for(started(layout.socket_path.clone()));

    assert!(d.alive(), "weir-server must start: {}", d.dump());
    assert!(
        layout.socket_path.exists(),
        "socket from the file: {}",
        d.dump()
    );
    let src = d
        .line("config source")
        .unwrap_or_else(|| panic!("no config source line: {}", d.dump()))
        .clone();
    assert!(src.contains("INFO"), "{src}");
    assert!(src.contains(&cfg.display().to_string()), "{src}");
    assert!(src.contains("--config"), "{src}");
    assert!(src.contains("loaded=true"), "{src}");
    let starting = d.line("weir starting").unwrap().clone();
    assert!(
        starting.contains("shards=3"),
        "shard_count from the file: {starting}"
    );
    assert!(
        d.line("passed with --config").is_none(),
        "no D10 error: {}",
        d.dump()
    );
}

// ── CONFIG-STARTUP-05 ────────────────────────────────────────────────────────

/// With no --config and a file at /etc/weir/weir.toml, the file is read and
/// the server starts. Run in the image, with the file bind-mounted at the
/// default path (the box has no root to create /etc/weir/weir.toml).
#[test]
fn config_startup_05_default_file_present_is_read() {
    if !docker_enabled() {
        return;
    }
    let scratch = Scratch::new("05");
    let cfg = scratch.path().join("weir.toml");
    fs::write(
        &cfg,
        "[server]\nsocket_path = \"/run/weir/weir.sock\"\nwab_dir = \"/var/lib/weir/wab\"\n\
         shard_count = 3\nsink_type = \"noop\"\nlog_level = \"info\"\n",
    )
    .unwrap();
    set_mode(scratch.path(), 0o755);
    set_mode(&cfg, 0o644);

    let c = Container::run(&["-v", &format!("{}:{DEFAULT_PATH}:ro", cfg.display())]);
    let log = c.wait_log(|l| l.contains("weir starting") && l.contains("config source"));
    assert!(c.running(), "the container must keep running: {log}");
    let src = find(&log, "config source");
    assert!(src.contains("INFO"), "{src}");
    assert!(src.contains(DEFAULT_PATH), "{src}");
    assert!(src.contains("default path"), "{src}");
    assert!(src.contains("loaded=true"), "the file was read: {src}");
    let starting = find(&log, "weir starting");
    assert!(
        starting.contains("shards=3"),
        "shard_count from the file: {starting}"
    );
    assert!(!log.contains("NOOP SINK: no config file"), "{log}");
}

// ── CONFIG-STARTUP-06 ────────────────────────────────────────────────────────

/// No --config, no default file, WEIR_* env only: the server starts, an INFO
/// line names the default path as not loaded, and no "will become a startup
/// error" warning appears.
#[test]
fn config_startup_06_missing_default_starts_and_logs_source() {
    if !default_config_absent(Path::new(DEFAULT_PATH)) {
        return;
    }
    let layout = Layout::new("06");
    let mut cmd = weir();
    cmd.env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string())
        .env("WEIR_LOG_LEVEL", "info");
    let mut d = Running::spawn(cmd);
    d.wait_for(started(layout.socket_path.clone()));

    assert!(d.alive(), "weir-server must start: {}", d.dump());
    assert!(layout.socket_path.exists(), "{}", d.dump());
    let src = d
        .line("config source")
        .unwrap_or_else(|| panic!("no config source line: {}", d.dump()))
        .clone();
    assert!(src.contains("INFO"), "INFO level: {src}");
    assert!(src.contains(DEFAULT_PATH), "names the default path: {src}");
    assert!(src.contains("default path"), "origin: {src}");
    assert!(src.contains("loaded=false"), "absent: {src}");
    let all = d.dump();
    assert!(
        !all.contains("will become a startup error"),
        "the old deprecation warning is gone: {all}"
    );
}

// ── CONFIG-STARTUP-07 ────────────────────────────────────────────────────────

/// No --config, no default file, no sink configured: the server starts and
/// logs the NOOP SINK WARN.
#[test]
fn config_startup_07_missing_default_noop_sink_warns() {
    if !default_config_absent(Path::new(DEFAULT_PATH)) {
        return;
    }
    let layout = Layout::new("07");
    let mut cmd = weir();
    cmd.env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let mut d = Running::spawn(cmd);
    let sock = layout.socket_path.clone();
    d.wait_for(move |log| {
        started(sock.clone())(log) && log.iter().any(|l| l.contains("NOOP SINK"))
    });

    assert!(d.alive(), "weir-server must start: {}", d.dump());
    let warn = d
        .line("NOOP SINK")
        .unwrap_or_else(|| panic!("no NOOP SINK WARN: {}", d.dump()))
        .clone();
    assert!(warn.contains("WARN"), "WARN level: {warn}");
    assert!(
        warn.contains(DEFAULT_PATH),
        "names the default path: {warn}"
    );
    assert!(warn.contains("acknowledged"), "records acked: {warn}");
    assert!(warn.contains("DISCARDED"), "and discarded: {warn}");
}

// ── CONFIG-STARTUP-08 ────────────────────────────────────────────────────────

/// `docker run weir-server:local` with WEIR_* env only and no bind mount:
/// it starts, the INFO line names the default path as absent, and the NOOP
/// SINK WARN appears.
#[test]
fn config_startup_08_docker_image_starts_env_only() {
    if !docker_enabled() {
        return;
    }
    let c = Container::run(&["-e", "WEIR_LOG_LEVEL=info", "-e", "WEIR_SHARD_COUNT=2"]);
    let log = c.wait_log(|l| l.contains("NOOP SINK") && l.contains("config source"));
    assert!(c.running(), "the container must keep running: {log}");
    let src = find(&log, "config source");
    assert!(src.contains("INFO"), "{src}");
    assert!(src.contains(DEFAULT_PATH), "{src}");
    assert!(src.contains("default path"), "{src}");
    assert!(src.contains("loaded=false"), "absent: {src}");
    let starting = find(&log, "weir starting");
    assert!(
        starting.contains("shards=2"),
        "WEIR_* env applied: {starting}"
    );
    let warn = find(&log, "NOOP SINK");
    assert!(
        warn.contains("WARN") && warn.contains("DISCARDED"),
        "{warn}"
    );
    assert!(!log.contains("passed with --config"), "{log}");
}

/// CONFIG-STARTUP-05 and -08 skip without WEIR_ACCEPTANCE_DOCKER=1, so a CI
/// job must set it and run them against the image it built, or the env-only
/// start (spec flow 2) is never exercised: docker.yml's smoke step always
/// bind-mounts a config file. This guards that wiring.
#[test]
fn config_startup_08_docker_tests_run_in_ci() {
    let wf = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.github/workflows/docker.yml"),
    )
    .unwrap();
    let start = wf
        .find("WEIR_ACCEPTANCE_DOCKER")
        .expect("a docker.yml step sets WEIR_ACCEPTANCE_DOCKER");
    // The step runs from its `- name:` line to the next one.
    let from = wf[..start].rfind("- name:").expect("inside a step");
    let to = wf[start..].find("- name:").map_or(wf.len(), |i| start + i);
    let step = &wf[from..to];
    assert!(
        step.contains(r#"WEIR_ACCEPTANCE_DOCKER: "1""#),
        "set to \"1\": {step}"
    );
    assert!(
        step.contains(&format!("docker tag weir-server:smoke {IMAGE}")),
        "the tests' {IMAGE} is the image the job built: {step}"
    );
    assert!(step.contains("--test acceptance_config_startup"), "{step}");
    for t in [
        "config_startup_05_default_file_present_is_read",
        "config_startup_08_docker_image_starts_env_only",
    ] {
        assert!(step.contains(t), "runs {t}: {step}");
    }
    let build = wf
        .find("tags: weir-server:smoke")
        .expect("the smoke job builds weir-server:smoke");
    assert!(build < from, "runs after the image build");
}

// ── CONFIG-STARTUP-09 ────────────────────────────────────────────────────────

/// The Dockerfile's CMD is ["weir-server"], and the comment above it says to
/// bind-mount /etc/weir/weir.toml and to pass --config only for another path.
#[test]
fn config_startup_09_dockerfile_cmd_and_comment() {
    let dockerfile = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/docker/Dockerfile"),
    )
    .unwrap();
    let lines: Vec<&str> = dockerfile.lines().collect();
    let cmd_idx: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("CMD "))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(cmd_idx.len(), 1, "exactly one CMD instruction: {cmd_idx:?}");
    let i = cmd_idx[0];
    assert_eq!(lines[i].trim(), r#"CMD ["weir-server"]"#);

    let mut comment = Vec::new();
    for l in lines[..i].iter().rev() {
        match l.trim_start().strip_prefix('#') {
            Some(c) => comment.push(c.trim()),
            None => break,
        }
    }
    comment.reverse();
    let comment = comment.join(" ");
    assert!(!comment.is_empty(), "a comment above CMD");
    assert!(comment.contains("bind-mount"), "{comment}");
    assert!(comment.contains("/etc/weir/weir.toml"), "{comment}");
    assert!(comment.contains("--config"), "{comment}");
    assert!(
        comment.contains("only for a file at another path"),
        "{comment}"
    );
}

// ── CONFIG-STARTUP-10 ────────────────────────────────────────────────────────

/// weir.service still passes `--config /etc/weir/weir.toml`; on a host
/// without that file its ExecStart fails with the D10 error on stderr (which
/// the unit sends to the journal), so systemd marks the unit failed.
///
/// `systemctl start weir` needs root to install the unit; this test runs the
/// unit's ExecStart command line (with the built binary in place of
/// /usr/local/bin/weir-server) on this host; it skips when the host has
/// /etc/weir/weir.toml.
#[test]
fn config_startup_10_systemd_execstart_fails_on_missing_file() {
    if !default_config_absent(Path::new(DEFAULT_PATH)) {
        return;
    }
    let unit = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/systemd/weir.service"),
    )
    .unwrap();
    let exec = unit
        .lines()
        .find_map(|l| l.trim().strip_prefix("ExecStart="))
        .expect("an ExecStart= line");
    let mut argv = exec.split_whitespace();
    assert_eq!(argv.next(), Some("/usr/local/bin/weir-server"));
    let args: Vec<&str> = argv.collect();
    assert_eq!(
        args,
        ["--config", DEFAULT_PATH],
        "the unit still passes --config"
    );
    assert!(
        unit.contains("StandardError=journal"),
        "stderr goes to the journal"
    );

    let layout = Layout::new("10");
    let mut cmd = weir();
    cmd.args(&args)
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(exited, "must not keep running: {}", combined(&out));
    assert!(
        !out.status.success(),
        "a non-zero exit fails the unit: {:?}",
        out.status
    );
    assert!(
        stderr.contains(&d10_message(Path::new(DEFAULT_PATH))),
        "the D10 error on stderr (the journal): {}",
        combined(&out)
    );
}

// ── CONFIG-STARTUP-11 ────────────────────────────────────────────────────────

/// No option makes an explicit missing --config optional: --help lists none,
/// and setting WEIR_CONFIG_OPTIONAL (or a flag of that name) does not change
/// the refusal.
#[test]
fn config_startup_11_no_knob_makes_explicit_config_optional() {
    let mut help = weir();
    help.arg("--help");
    let (out, exited, _) = run_to_exit(help, Duration::from_secs(10));
    let text = combined(&out);
    assert!(exited && out.status.success(), "{text}");
    assert!(text.contains("--config"), "help lists --config: {text}");
    let lower = text.to_lowercase();
    assert!(!lower.contains("config-optional"), "{text}");
    assert!(!lower.contains("config_optional"), "{text}");
    assert!(!lower.contains("optional-config"), "{text}");
    // The ENVIRONMENT section says --config has no env variable at all.
    assert!(
        text.contains("--config has no environment variable"),
        "{text}"
    );

    let layout = Layout::new("11");
    let missing = layout.scratch.path().join("absent.toml");
    for val in ["1", "true"] {
        let mut cmd = weir();
        cmd.args(["--config", missing.to_str().unwrap()])
            .env("WEIR_SOCKET_PATH", &layout.socket_path)
            .env("WEIR_WAB_DIR", &layout.wab_dir)
            .env("WEIR_METRICS_PORT", layout.metrics_port.to_string())
            .env("WEIR_CONFIG_OPTIONAL", val)
            .env("WEIR_CONFIG_FILE_OPTIONAL", val);
        let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
        let log = combined(&out);
        assert!(exited && !out.status.success(), "still refuses: {log}");
        assert!(log.contains(&d10_message(&missing)), "{log}");
    }
    // A flag of that name is not an option either.
    let mut cmd = weir();
    cmd.args(["--config", missing.to_str().unwrap(), "--config-optional"])
        .env("WEIR_SOCKET_PATH", &layout.socket_path)
        .env("WEIR_WAB_DIR", &layout.wab_dir)
        .env("WEIR_METRICS_PORT", layout.metrics_port.to_string());
    let (out, exited, _) = run_to_exit(cmd, Duration::from_secs(15));
    assert!(exited && !out.status.success(), "{}", combined(&out));
    assert!(!layout.socket_path.exists());
}

// ── Docker helpers ───────────────────────────────────────────────────────────

fn docker_enabled() -> bool {
    if std::env::var("WEIR_ACCEPTANCE_DOCKER").as_deref() != Ok("1") {
        eprintln!("skipping: set WEIR_ACCEPTANCE_DOCKER=1 (needs the {IMAGE} image)");
        return false;
    }
    let ok = Command::new("docker")
        .args(["image", "inspect", IMAGE])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(
        ok,
        "WEIR_ACCEPTANCE_DOCKER=1 but no {IMAGE} image: \
         docker build -t {IMAGE} -f deploy/docker/Dockerfile ."
    );
    true
}

fn set_mode(p: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(p, fs::Permissions::from_mode(mode)).unwrap();
}

fn find(log: &str, needle: &str) -> String {
    log.lines()
        .find(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("no line with {needle:?}: {log}"))
        .to_string()
}

/// A detached container from the image, removed on drop.
struct Container(String);

impl Container {
    fn run(extra: &[&str]) -> Self {
        let name = format!("weir-acc-cfg-{}-{}", std::process::id(), rand_suffix());
        let mut args = vec!["run", "-d", "--name", &name];
        args.extend_from_slice(extra);
        args.push(IMAGE);
        let out = Command::new("docker").args(&args).output().unwrap();
        assert!(
            out.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Container(name)
    }

    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.0])
            .output()
            .unwrap();
        combined(&out)
    }

    fn wait_log(&self, done: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            let log = self.logs();
            if done(&log) || Instant::now() >= deadline || !self.running() {
                return log;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    fn running(&self) -> bool {
        let out = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}}", &self.0])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim() == "true"
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", "-v", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u128
}

// ── the default-path precondition ────────────────────────────────────────────

/// The tests that need no file at the default path run only when that path is
/// known to be absent: an existing file (weir installed the documented way)
/// or a path that can't be checked (an unreadable /etc/weir) makes them skip,
/// not fail, since neither says anything about the code under test.
#[test]
fn config_startup_default_absent_precondition() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("precond");
    let absent = scratch.path().join("weir.toml");
    assert!(default_config_absent(&absent), "absent: run");

    fs::write(&absent, "").unwrap();
    assert!(!default_config_absent(&absent), "present: skip");

    let locked = scratch.path().join("locked");
    fs::create_dir(&locked).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    let hidden = locked.join("weir.toml");
    // Root reads through mode 000, so the unreadable case is judged only
    // when the dir really is unreadable to this user.
    if fs::read_dir(&locked).is_err() {
        assert!(!default_config_absent(&hidden), "unreadable: skip");
    }
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
}
