//! `WeirServer` handle + builder.
//!
//! Spawns a fresh `weir-server` child process per test, manages its temp
//! directories, and exposes lifecycle helpers. Drops cleanly via SIGTERM
//! (or SIGKILL during a `Drop` after-test cleanup) and removes the temp
//! directory on drop.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

use weir_client::WeirClient;

use crate::util::{free_port, process_lock};

/// A running `weir-server` instance owned by a test.
///
/// Construct via [`WeirServer::builder`]. Drop cleans up the child and the
/// temp directory; consume via [`shutdown`](Self::shutdown) for a graceful
/// SIGTERM that the test asserts on.
pub struct WeirServer {
    child: Option<Child>,
    binary_path: PathBuf,
    /// Absolute path to the Unix socket the daemon is listening on.
    pub socket_path: PathBuf,
    /// Absolute path to the WAB directory the daemon writes segments into.
    pub wab_dir: PathBuf,
    /// Absolute path to the generated TOML config file.
    pub config_path: PathBuf,
    /// Root temp directory; cleaned up on drop.
    pub tmp_dir: PathBuf,
    /// Loopback port the metrics endpoint is bound to.
    pub metrics_port: u16,
    /// `batch_deadline_ms` value the daemon was launched with — handy for
    /// bench scenarios that name themselves after the deadline.
    pub batch_deadline_ms: u64,
    /// Held for the lifetime of the handle to serialise process spawning.
    _proc_lock: MutexGuard<'static, ()>,
}

/// Builder for [`WeirServer`]. Defaults match the `tests/system.rs` shape;
/// for bench-flavoured defaults use [`bench_preset`](Self::bench_preset).
pub struct WeirServerBuilder {
    tag: String,
    binary_path: Option<PathBuf>,
    shard_count: usize,
    worker_count: usize,
    batch_size: usize,
    batch_deadline_ms: u64,
    max_connections: usize,
    shutdown_timeout_secs: u64,
    log_level: &'static str,
    /// Extra `[server]` lines appended to the generated TOML. Used for the
    /// less-common knobs the builder doesn't expose first-class.
    extra_config_lines: Vec<String>,
    ready_timeout: Duration,
    /// When true, the child process's stdout/stderr are redirected to
    /// /dev/null instead of a log file. Used by tests that exercise
    /// situations where the log file itself would fail to write
    /// (e.g. `RLIMIT_FSIZE = 0`).
    silence_logs: bool,
    /// Optional `Command::pre_exec` callback applied to the child before
    /// `exec`. Runs in the fork/exec gap so the closure must be
    /// async-signal-safe (the unsafety lives on the setter, not here).
    pre_exec: Option<Box<dyn FnMut() -> std::io::Result<()> + Send + Sync + 'static>>,
    /// Extra environment variables passed to the child process. Used for
    /// `WEIR_SINK_URL` and friends that aren't sourced from the TOML.
    env: Vec<(String, String)>,
    /// Override for the WAB directory path. If set, used verbatim instead of
    /// `tmp_dir/wab`. Used by tests that need the WAB on a separate
    /// filesystem (e.g. a pre-mounted small tmpfs for ENOSPC testing).
    wab_dir_override: Option<PathBuf>,
}

impl WeirServer {
    /// Creates a builder seeded with system-test defaults: 1 shard, 2
    /// workers, batch_size=100, batch_deadline_ms=20, max_connections=256,
    /// shutdown_timeout_secs=3, log_level="warn".
    pub fn builder(tag: &str) -> WeirServerBuilder {
        WeirServerBuilder {
            tag: tag.to_string(),
            binary_path: None,
            shard_count: 1,
            worker_count: 2,
            batch_size: 100,
            batch_deadline_ms: 20,
            max_connections: 256,
            shutdown_timeout_secs: 3,
            log_level: "warn",
            extra_config_lines: Vec::new(),
            ready_timeout: Duration::from_secs(15),
            silence_logs: false,
            pre_exec: None,
            env: Vec::new(),
            wab_dir_override: None,
        }
    }

    /// Returns the PID of the child process. Panics if the child has been
    /// reaped (after `shutdown` / `sigterm` / `kill_ungracefully`).
    pub fn child_pid(&self) -> u32 {
        self.child.as_ref().expect("child has been reaped").id()
    }

    // ── Network helpers ──────────────────────────────────────────────────────

    /// Opens a fresh [`WeirClient`] connection. Panics if the daemon refuses
    /// the connection — the caller should be sure the daemon is healthy.
    pub fn client(&self) -> WeirClient {
        WeirClient::connect(&self.socket_path)
            .unwrap_or_else(|e| panic!("failed to connect to {}: {e}", self.socket_path.display()))
    }

    pub fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.metrics_port)
    }

    /// Scrapes `/metrics` and returns the body as a string. Uses a hand-rolled
    /// HTTP/1.0 GET because the dev-deps of the testkit are deliberately small;
    /// the test crate can layer `ureq` on top if it wants.
    pub fn scrape_metrics(&self) -> String {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", self.metrics_port))
            .expect("connect to metrics endpoint");
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .expect("write GET");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("read /metrics response");
        // Strip the HTTP head; tests want the body.
        match response.find("\r\n\r\n") {
            Some(idx) => response[idx + 4..].to_string(),
            None => response,
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /// Kills the server immediately with SIGKILL. The socket and temp files
    /// remain on disk. Used to simulate a crash for crash-recovery tests.
    pub fn kill_ungracefully(&mut self) {
        if let Some(ref mut child) = self.child {
            #[cfg(unix)]
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGKILL);
            }
            let _ = child.wait();
            self.child = None;
        }
    }

    /// Kills the server with SIGKILL then restarts it with the same config.
    /// Simulates crash recovery: `bind_cleanup` in the socket layer detects
    /// and removes the stale socket left behind by the crash.
    pub fn restart_in_place(&mut self) {
        self.kill_ungracefully();

        // Append to existing log so both runs appear in diagnostics.
        let log_path = self.tmp_dir.join("weir.log");
        let log_file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)
            .unwrap();

        let child = Command::new(&self.binary_path)
            .args(["--config", self.config_path.to_str().unwrap()])
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("failed to respawn weir-server");

        self.child = Some(child);
        self.wait_ready(Duration::from_secs(15));
    }

    /// Sends SIGTERM, waits for the process to exit, and returns how long it
    /// took. Does NOT remove the temp directory — Drop handles cleanup — so
    /// callers can inspect WAB files after the process has exited.
    pub fn sigterm(&mut self) -> Duration {
        let t = Instant::now();
        if let Some(ref mut child) = self.child {
            #[cfg(unix)]
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
            let _ = child.wait();
            self.child = None;
        }
        t.elapsed()
    }

    /// Sends SIGTERM, waits for the process to exit cleanly, then drops the
    /// handle (which removes the temp directory).
    pub fn shutdown(mut self) {
        self.sigterm();
        // Drop runs, cleanup happens.
    }

    /// Blocks until the server is ready to accept connections.
    ///
    /// Uses an actual connect attempt rather than checking file existence —
    /// correctly handles crash-restart scenarios where a stale socket file
    /// from the previous run is still on disk but nobody is listening yet.
    /// Detects early process exit and prints the log for diagnostics.
    pub fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            #[cfg(unix)]
            if std::os::unix::net::UnixStream::connect(&self.socket_path).is_ok() {
                return;
            }
            if let Some(ref mut child) = self.child
                && let Ok(Some(status)) = child.try_wait()
            {
                let log_path = self.tmp_dir.join("weir.log");
                let log = std::fs::read_to_string(&log_path).unwrap_or_default();
                panic!(
                    "weir-server exited early with {status} before socket was ready\n\
                     socket: {}\nlog:\n{log}",
                    self.socket_path.display()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let log_path = self.tmp_dir.join("weir.log");
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        panic!(
            "weir-server did not become ready within {:?}: {}\nlog:\n{log}",
            timeout,
            self.socket_path.display()
        );
    }
}

impl Drop for WeirServer {
    fn drop(&mut self) {
        // SIGKILL on drop: tests are torn down quickly. If a test wanted a
        // graceful shutdown it should have called .shutdown() explicitly.
        self.kill_ungracefully();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

impl WeirServerBuilder {
    /// Reseeds the builder with the load-test defaults: 4 shards, 4 workers,
    /// `batch_size=64`, `log_level="error"`, `batch_deadline_ms` from the
    /// `WEIR_BENCH_DEADLINE` env var (default 1). Equivalent to the
    /// pre-extraction `LoadHandle::start_impl` defaults.
    pub fn bench_preset(mut self) -> Self {
        self.shard_count = 4;
        self.worker_count = 4;
        self.batch_size = 64;
        self.batch_deadline_ms = bench_deadline_from_env();
        self.log_level = "error";
        self
    }

    pub fn shard_count(mut self, n: usize) -> Self {
        self.shard_count = n;
        self
    }

    pub fn worker_count(mut self, n: usize) -> Self {
        self.worker_count = n;
        self
    }

    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    pub fn batch_deadline_ms(mut self, ms: u64) -> Self {
        self.batch_deadline_ms = ms;
        self
    }

    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }

    pub fn shutdown_timeout_secs(mut self, n: u64) -> Self {
        self.shutdown_timeout_secs = n;
        self
    }

    pub fn log_level(mut self, level: &'static str) -> Self {
        self.log_level = level;
        self
    }

    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Path to the `weir-server` binary. Required — testkit lives in a
    /// separate crate so it can't read `env!("CARGO_BIN_EXE_weir-server")`
    /// itself; callers pass it in via the [`weir_server!`](crate::weir_server)
    /// macro (which calls this method automatically) or by hand.
    pub fn binary_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.binary_path = Some(path.into());
        self
    }

    /// Redirect the child's stdout/stderr to `/dev/null` instead of a log
    /// file. Use for tests that exercise situations where the log file
    /// itself would fail to write (e.g. `RLIMIT_FSIZE = 0`).
    pub fn silence_logs(mut self) -> Self {
        self.silence_logs = true;
        self
    }

    /// Sets an environment variable on the child process. Used for the
    /// env-only knobs the TOML can't carry (notably `WEIR_SINK_URL`, whose
    /// `mysql://user:pass@host/db` form contains credentials).
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Overrides the WAB directory path. The builder normally uses
    /// `tmp_dir/wab`; this knob is for tests that need the WAB on a
    /// separately-mounted filesystem (e.g. a small tmpfs for ENOSPC).
    /// The caller is responsible for creating the directory.
    pub fn wab_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.wab_dir_override = Some(path.into());
        self
    }

    /// Install a `Command::pre_exec` callback that runs in the child between
    /// fork and exec. Used for `RLIMIT_*` setup, signal-mask tweaks, etc.
    ///
    /// # Safety
    ///
    /// The closure executes in the fork/exec gap. The standard library lists
    /// the constraints in detail; the short version is: only call
    /// async-signal-safe functions, do not allocate, do not touch globals.
    /// libc's `signal`, `setrlimit`, `sigprocmask`, and the raw syscall
    /// wrappers are safe; almost nothing else is.
    pub unsafe fn pre_exec<F>(mut self, f: F) -> Self
    where
        F: FnMut() -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.pre_exec = Some(Box::new(f));
        self
    }

    /// Appends an extra `key = value` line to the generated `[server]` block.
    /// Use for less-common knobs the builder doesn't expose first-class
    /// (e.g. `wab_segment_max_bytes`, `sink_max_batch_size`,
    /// `metrics_bind`, `peer_uid_check`).
    pub fn extra_config(mut self, line: impl Into<String>) -> Self {
        self.extra_config_lines.push(line.into());
        self
    }

    /// Spawns the server and blocks until it's ready (or panics with the
    /// captured log on failure).
    pub fn start(self) -> WeirServer {
        let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
        let metrics_port = free_port();
        let tmp_dir =
            std::env::temp_dir().join(format!("weir_test_{}_{}", self.tag, std::process::id()));
        let wab_dir = self
            .wab_dir_override
            .clone()
            .unwrap_or_else(|| tmp_dir.join("wab"));
        let socket_dir = tmp_dir.join("run");
        let socket_path = socket_dir.join("weir.sock");
        let config_path = tmp_dir.join("weir.toml");
        let log_path = tmp_dir.join("weir.log");

        // Create the wab dir if no override was supplied (override callers
        // create it themselves so they can place it on a custom filesystem).
        if self.wab_dir_override.is_none() {
            std::fs::create_dir_all(&wab_dir).unwrap();
        }
        std::fs::create_dir_all(&socket_dir).unwrap();
        // WAB dir must be mode 0o700.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wab_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }

        let mut config = format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = {}\n\
             worker_count          = {}\n\
             batch_size            = {}\n\
             batch_deadline_ms     = {}\n\
             max_connections       = {}\n\
             shutdown_timeout_secs = {}\n\
             log_level             = \"{}\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
            self.shard_count,
            self.worker_count,
            self.batch_size,
            self.batch_deadline_ms,
            self.max_connections,
            self.shutdown_timeout_secs,
            self.log_level,
        );
        for line in &self.extra_config_lines {
            config.push_str(line);
            if !line.ends_with('\n') {
                config.push('\n');
            }
        }
        std::fs::write(&config_path, &config).unwrap();

        let binary_path = self.binary_path.expect(
            "binary_path is required — use the `weir_server!(tag)` macro or call \
             .binary_path(env!(\"CARGO_BIN_EXE_weir-server\")) explicitly",
        );
        let mut cmd = Command::new(&binary_path);
        cmd.args(["--config", config_path.to_str().unwrap()]);
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        if self.silence_logs {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        } else {
            let log_file = std::fs::File::create(&log_path).unwrap();
            cmd.stdout(Stdio::from(log_file.try_clone().unwrap()))
                .stderr(Stdio::from(log_file));
        }
        if let Some(pre_exec) = self.pre_exec {
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                // SAFETY: the unsafety contract was discharged at the
                // `unsafe fn pre_exec(...)` setter; we just thread the
                // closure through to Command here.
                unsafe { cmd.pre_exec(pre_exec) };
            }
        }
        let child = cmd.spawn().expect("failed to spawn weir-server");

        let mut handle = WeirServer {
            child: Some(child),
            binary_path,
            socket_path,
            wab_dir,
            config_path,
            tmp_dir,
            metrics_port,
            batch_deadline_ms: self.batch_deadline_ms,
            _proc_lock,
        };

        let ready_timeout = self.ready_timeout;
        handle.wait_ready(ready_timeout);
        handle
    }
}

/// Read `WEIR_BENCH_DEADLINE` from env, defaulting to 1ms. Used by
/// [`WeirServerBuilder::bench_preset`] so CI can sweep deadlines without
/// rebuilding.
fn bench_deadline_from_env() -> u64 {
    std::env::var("WEIR_BENCH_DEADLINE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}
