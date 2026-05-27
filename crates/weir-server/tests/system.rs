//! System integration tests — exercises the real `weir-server` binary.
//!
//! Each test uses `ServerHandle` to spawn the binary, wait for the socket to
//! be ready, and clean everything up on drop (even on panic). Tests are
//! independent: each gets its own temp directory, socket path, WAB dir, and
//! metrics port so they can run in parallel without interference.
//!
//! # Running
//!
//! Each test spawns a real OS process with several threads. Running all tests
//! with maximum parallelism can exhaust OS resources on dev machines. Use a
//! bounded thread count to keep things stable:
//!
//! ```sh
//! cargo test -p weir-server --test system -- --test-threads=4
//! ```
//!
//! These tests are Unix-only because weir-server only binds Unix sockets.

#![cfg(unix)]

use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use weir_client::{ClientError, WeirClient};
use weir_core::Durability;

// ── Process serialiser ────────────────────────────────────────────────────────
//
// Each test holds this lock for its entire lifetime via `ServerHandle`.
// This means at most one server process is alive at a time regardless of
// `--test-threads`, which prevents OS resource exhaustion on dev machines and
// CI runners without requiring callers to remember `--test-threads=1`.

fn process_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
}

// ── Port allocator ────────────────────────────────────────────────────────────

/// Asks the OS for a free TCP port. The listener is dropped immediately (brief
/// TOCTOU window, but acceptable in tests — far safer than a fixed range that
/// clashes with stale processes from previous runs).
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .unwrap()
        .port()
}

// ── ServerHandle ──────────────────────────────────────────────────────────────

/// Owns a running `weir-server` process and its associated temp directories.
///
/// Cleans up (SIGTERM + wait + rm temp dir) on drop. The binary path comes
/// from `env!("CARGO_BIN_EXE_weir-server")` — Cargo resolves this at compile
/// time to the binary being tested in the current profile.
struct ServerHandle {
    child: Option<Child>,
    pub socket_path: PathBuf,
    pub wab_dir: PathBuf,
    pub metrics_port: u16,
    config_path: PathBuf,
    tmp_dir: PathBuf,
    /// Held for the lifetime of the handle to serialise process spawning.
    _proc_lock: std::sync::MutexGuard<'static, ()>,
}

impl ServerHandle {
    /// Starts a fresh weir-server instance and blocks until the socket is ready.
    ///
    /// `tag` is used to name the temp directory (helps with post-mortem debugging).
    fn start(tag: &str) -> Self {
        Self::start_impl(tag, 1)
    }

    /// Like `start`, but configures `shard_count` WAB shards.
    fn start_sharded(tag: &str, shard_count: usize) -> Self {
        Self::start_impl(tag, shard_count)
    }

    fn start_impl(tag: &str, shard_count: usize) -> Self {
        let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
        let metrics_port = free_port();
        let tmp_dir = std::env::temp_dir().join(format!("weir_sys_{}_{}", tag, std::process::id()));
        let wab_dir = tmp_dir.join("wab");
        let socket_dir = tmp_dir.join("run");
        let socket_path = socket_dir.join("weir.sock");
        let config_path = tmp_dir.join("weir.toml");
        let log_path = tmp_dir.join("weir.log");

        fs::create_dir_all(&wab_dir).unwrap();
        fs::create_dir_all(&socket_dir).unwrap();
        // WAB dir must be mode 0o700.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).unwrap();
        }

        // Write a minimal config.
        let config = format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = {}\n\
             worker_count          = 2\n\
             batch_size            = 100\n\
             batch_deadline_ms     = 20\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"warn\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
            shard_count,
        );
        fs::write(&config_path, config).unwrap();

        let log_file = fs::File::create(&log_path).unwrap();
        let binary = env!("CARGO_BIN_EXE_weir-server");
        let child = Command::new(binary)
            .args(["--config", config_path.to_str().unwrap()])
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("failed to spawn weir-server");

        let mut handle = Self {
            child: Some(child),
            socket_path,
            wab_dir,
            metrics_port,
            config_path,
            tmp_dir,
            _proc_lock,
        };

        // Wait up to 15 s for the socket to appear.
        handle.wait_ready(Duration::from_secs(15));
        handle
    }

    /// Kills the server immediately with SIGKILL. The socket and temp files remain
    /// on disk. Used to simulate a crash for crash-recovery tests.
    fn kill_ungracefully(&mut self) {
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
    ///
    /// Simulates crash recovery: `bind_cleanup` in the socket layer detects and
    /// removes the stale socket left behind by the crash.
    fn restart_in_place(&mut self) {
        self.kill_ungracefully();

        // Append to existing log so both runs appear in diagnostics.
        let log_path = self.tmp_dir.join("weir.log");
        let log_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)
            .unwrap();

        let binary = env!("CARGO_BIN_EXE_weir-server");
        let child = Command::new(binary)
            .args(["--config", self.config_path.to_str().unwrap()])
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("failed to respawn weir-server");

        self.child = Some(child);
        self.wait_ready(Duration::from_secs(15));
    }

    /// Blocks until the server is ready to accept connections.
    ///
    /// Uses an actual connect attempt rather than checking file existence — this
    /// correctly handles crash-restart scenarios where a stale socket file from
    /// the previous run is still on disk but nobody is listening yet.
    ///
    /// Also detects early process exit and prints the log for diagnostics.
    fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if std::os::unix::net::UnixStream::connect(&self.socket_path).is_ok() {
                return;
            }
            if let Some(ref mut child) = self.child
                && let Ok(Some(status)) = child.try_wait()
            {
                let log_path = self.tmp_dir.join("weir.log");
                let log = fs::read_to_string(&log_path).unwrap_or_default();
                panic!(
                    "weir-server exited early with {status} before socket was ready\n\
                     socket: {}\nlog:\n{log}",
                    self.socket_path.display()
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
        let log_path = self.tmp_dir.join("weir.log");
        let log = fs::read_to_string(&log_path).unwrap_or_default();
        panic!(
            "weir-server did not become ready within {:?}: {}\nlog:\n{log}",
            timeout,
            self.socket_path.display()
        );
    }

    /// Returns a connected WeirClient.
    fn client(&self) -> WeirClient {
        WeirClient::connect(&self.socket_path)
            .unwrap_or_else(|e| panic!("failed to connect to {}: {e}", self.socket_path.display()))
    }

    /// Returns the metrics URL for this instance.
    fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.metrics_port)
    }

    /// Fetches /metrics and returns the body as a string.
    fn scrape_metrics(&self) -> String {
        ureq::get(&self.metrics_url())
            .call()
            .expect("metrics request failed")
            .into_string()
            .expect("metrics body read failed")
    }

    /// Sends SIGTERM, waits for the process to exit, and returns how long it
    /// took. Does NOT remove the temp directory — Drop handles cleanup — so
    /// callers can inspect WAB files after the process has exited.
    fn sigterm(&mut self) -> Duration {
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

    /// Sends SIGTERM and waits for the process to exit cleanly.
    fn shutdown(mut self) {
        if let Some(ref mut child) = self.child {
            #[cfg(unix)]
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
            let _ = child.wait();
        }
        self.child = None; // prevent Drop from double-killing
        let _ = fs::remove_dir_all(&self.tmp_dir);
    }

    /// Starts the server with `RLIMIT_FSIZE = 0` so every WAB write fails with
    /// `EFBIG`. `SIGXFSZ` is ignored in the child so the signal does not kill
    /// the process; instead writes return an error that the server surfaces as
    /// `Nack(InternalError)`.
    ///
    /// stdout/stderr are silenced (`/dev/null`) because the log file itself
    /// would also fail to write under `RLIMIT_FSIZE = 0`.
    fn start_disk_full(tag: &str) -> Self {
        use std::os::unix::process::CommandExt;

        let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
        let metrics_port = free_port();
        let tmp_dir = std::env::temp_dir().join(format!("weir_sys_{}_{}", tag, std::process::id()));
        let wab_dir = tmp_dir.join("wab");
        let socket_dir = tmp_dir.join("run");
        let socket_path = socket_dir.join("weir.sock");
        let config_path = tmp_dir.join("weir.toml");

        fs::create_dir_all(&wab_dir).unwrap();
        fs::create_dir_all(&socket_dir).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).unwrap();
        }

        let config = format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = 1\n\
             worker_count          = 2\n\
             batch_size            = 100\n\
             batch_deadline_ms     = 20\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"warn\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
        );
        fs::write(&config_path, config).unwrap();

        let binary = env!("CARGO_BIN_EXE_weir-server");
        let mut cmd = Command::new(binary);
        cmd.args(["--config", config_path.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        unsafe {
            cmd.pre_exec(|| {
                libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
                let rl = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                libc::setrlimit(libc::RLIMIT_FSIZE, &rl);
                Ok(())
            });
        }

        let child = cmd
            .spawn()
            .expect("failed to spawn weir-server (disk-full)");

        let mut handle = Self {
            child: Some(child),
            socket_path,
            wab_dir,
            metrics_port,
            config_path,
            tmp_dir,
            _proc_lock,
        };

        handle.wait_ready(Duration::from_secs(15));
        handle
    }

    /// Starts the server with `RLIMIT_NOFILE` capped at `nofile_limit`.
    ///
    /// This lets tests verify the server degrades gracefully (refuses new
    /// connections, does not crash) when it runs out of file descriptors.
    /// The server is otherwise identical to a `start()` instance.
    fn start_with_nofile_limit(tag: &str, nofile_limit: u64) -> Self {
        use std::os::unix::process::CommandExt;

        let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
        let metrics_port = free_port();
        let tmp_dir = std::env::temp_dir().join(format!("weir_sys_{}_{}", tag, std::process::id()));
        let wab_dir = tmp_dir.join("wab");
        let socket_dir = tmp_dir.join("run");
        let socket_path = socket_dir.join("weir.sock");
        let config_path = tmp_dir.join("weir.toml");

        fs::create_dir_all(&wab_dir).unwrap();
        fs::create_dir_all(&socket_dir).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).unwrap();
        }

        let config = format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = 1\n\
             worker_count          = 2\n\
             batch_size            = 100\n\
             batch_deadline_ms     = 20\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"warn\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
        );
        fs::write(&config_path, config).unwrap();

        let binary = env!("CARGO_BIN_EXE_weir-server");
        let mut cmd = Command::new(binary);
        cmd.args(["--config", config_path.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        unsafe {
            cmd.pre_exec(move || {
                let rl = libc::rlimit {
                    rlim_cur: nofile_limit,
                    rlim_max: nofile_limit,
                };
                libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
                Ok(())
            });
        }

        let child = cmd
            .spawn()
            .expect("failed to spawn weir-server (nofile-limit)");

        let mut handle = Self {
            child: Some(child),
            socket_path,
            wab_dir,
            metrics_port,
            config_path,
            tmp_dir,
            _proc_lock,
        };

        handle.wait_ready(Duration::from_secs(15));
        handle
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.tmp_dir);
    }
}

// Helper: sum all file bytes under a directory tree.
fn wab_dir_bytes(dir: &Path) -> u64 {
    let Ok(rd) = fs::read_dir(dir) else { return 0 };
    let mut total = 0u64;
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            total += wab_dir_bytes(&p);
        } else {
            total += fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

// Helper: count .wab and .wab.sealed files in a directory tree.
fn count_wab_files(dir: &Path, ext: &str) -> usize {
    let Ok(rd) = fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0;
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            count += count_wab_files(&p, ext);
        } else if p.to_str().map(|s| s.ends_with(ext)).unwrap_or(false) {
            count += 1;
        }
    }
    count
}

// Helper: collect every byte from all files under a directory tree.
fn read_wab_bytes(dir: &Path) -> Vec<u8> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.extend_from_slice(&read_wab_bytes(&p));
        } else if let Ok(bytes) = fs::read(&p) {
            out.extend_from_slice(&bytes);
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// ── Basic push / ack ──────────────────────────────────────────────────────────

#[test]
fn smoke_single_push_ack() {
    let srv = ServerHandle::start("smoke");
    let mut client = srv.client();
    client.push(b"hello weir", Durability::Sync).unwrap();
}

#[test]
fn all_durability_tiers_acked() {
    let srv = ServerHandle::start("durability");
    let mut client = srv.client();
    for (label, tier) in [
        ("Sync", Durability::Sync),
        ("Batched", Durability::Batched),
        ("Buffered", Durability::Buffered),
    ] {
        client
            .push(format!("tier-{label}").as_bytes(), tier)
            .unwrap_or_else(|e| panic!("{label} push failed: {e}"));
    }
}

#[test]
fn multiple_sequential_pushes_same_connection() {
    let srv = ServerHandle::start("sequential");
    let mut client = srv.client();
    for i in 0..50u32 {
        client
            .push(format!("record-{i:04}").as_bytes(), Durability::Batched)
            .unwrap_or_else(|e| panic!("push #{i} failed: {e}"));
    }
}

// ── Health check ──────────────────────────────────────────────────────────────

#[test]
fn health_check_returns_ok() {
    let srv = ServerHandle::start("health");
    let mut client = srv.client();
    client.health_check().unwrap();
}

// ── Concurrent producers ──────────────────────────────────────────────────────

#[test]
fn concurrent_producers_all_acked() {
    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 100;

    let srv = ServerHandle::start("concurrent");
    let socket_path = srv.socket_path.clone();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect failed: {e}"));
                for i in 0..RECORDS_PER_THREAD {
                    let payload = format!("thread-{t:02}-record-{i:04}");
                    client
                        .push(payload.as_bytes(), Durability::Batched)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    let mut failures = 0usize;
    for h in handles {
        if h.join().is_err() {
            failures += 1;
        }
    }
    assert_eq!(failures, 0, "{failures}/{THREADS} producer threads failed");
}

#[test]
fn many_connections_open_simultaneously() {
    const CONN_COUNT: usize = 20;

    let srv = ServerHandle::start("many_conns");

    // Open all connections first, then push from each, to exercise the
    // semaphore-based connection cap under load.
    let clients: Vec<WeirClient> = (0..CONN_COUNT)
        .map(|i| {
            WeirClient::connect(&srv.socket_path).unwrap_or_else(|e| panic!("connect {i}: {e}"))
        })
        .collect();

    for (i, mut client) in clients.into_iter().enumerate() {
        client
            .push(format!("conn-{i}").as_bytes(), Durability::Buffered)
            .unwrap_or_else(|e| panic!("push on conn {i}: {e}"));
    }
}

// ── WAB on-disk verification ──────────────────────────────────────────────────

#[test]
fn records_written_to_wab_on_disk() {
    let srv = ServerHandle::start("wab_disk");
    let mut client = srv.client();

    for i in 0..20u32 {
        client
            .push(format!("wab-record-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    // Give the flusher thread a moment to write.
    thread::sleep(Duration::from_millis(100));

    let active = count_wab_files(&srv.wab_dir, ".wab");
    let sealed = count_wab_files(&srv.wab_dir, ".wab.sealed");
    assert!(
        active + sealed > 0,
        "no WAB files found in {} after 20 pushes",
        srv.wab_dir.display()
    );
}

#[test]
fn wab_writes_nonzero_bytes_to_disk_after_sync_pushes() {
    // Verify bytes are physically written to the WAB directory.
    // (The weir_wab_bytes_on_disk metric gauge is not yet wired to the pipeline;
    //  this test checks the filesystem directly instead.)
    let srv = ServerHandle::start("wab_bytes");
    let mut client = srv.client();
    for _ in 0..20 {
        client
            .push(b"gauge-test-payload", Durability::Sync)
            .unwrap();
    }
    thread::sleep(Duration::from_millis(150));

    let total = wab_dir_bytes(&srv.wab_dir);
    assert!(
        total > 0,
        "WAB directory should contain > 0 bytes after 20 Sync writes, got {total}"
    );
}

// ── Metrics accuracy ──────────────────────────────────────────────────────────

#[test]
fn metrics_endpoint_responds_with_openmetrics_content() {
    let srv = ServerHandle::start("metrics_up");
    let body = srv.scrape_metrics();
    assert!(!body.is_empty(), "metrics endpoint returned empty body");
    // OpenMetrics text format always ends with EOF marker.
    assert!(
        body.contains("weir_") || body.contains("# EOF"),
        "metrics body does not look like OpenMetrics: {body:.200}"
    );
}

#[test]
fn metrics_all_19_families_registered() {
    let srv = ServerHandle::start("metrics_families");
    let body = srv.scrape_metrics();

    // All 19 metric families must appear in the output as # HELP lines.
    // Data lines only appear for pre-initialised gauges and histograms;
    // counter families that haven't been incremented yet show only HELP/TYPE.
    for family in [
        "weir_records_accepted",
        "weir_records_ack",
        "weir_records_nack",
        "weir_accept_latency_seconds",
        "weir_connection_idle_timeout",
        "weir_wab_segments",
        "weir_wab_bytes_on_disk",
        "weir_wab_fsync_duration_seconds",
        "weir_sink_commit_duration_seconds",
        "weir_sink_commit_records",
        "weir_sink_health",
        "weir_queue_depth",
        "weir_recovery_records_replayed",
        "weir_recovery_segments_quarantined",
        "weir_wab_unexpected_mode",
        "weir_dead_letter_bytes_on_disk",
        "weir_dead_letter_full",
        "weir_drain_state",
        "weir_dead_letter_blocked_duration_seconds",
    ] {
        assert!(
            body.contains(&format!("# HELP {family}")),
            "metric family not registered in /metrics: {family}"
        );
    }
}

#[test]
fn drain_state_shows_draining_and_not_blocked() {
    let srv = ServerHandle::start("drain_state");
    let body = srv.scrape_metrics();

    // weir_drain_state is pre-initialised so all label values appear on the
    // first scrape with exactly one set to 1.
    assert!(
        body.contains("weir_drain_state{state=\"draining\"} 1"),
        "drain should be in Draining state on startup; body:\n{body:.500}"
    );
    assert!(
        body.contains("weir_drain_state{state=\"retrying_transient\"} 0"),
        "retrying_transient should be 0 on startup"
    );
    assert!(
        body.contains("weir_drain_state{state=\"blocked_dead_letter_full\"} 0"),
        "blocked_dead_letter_full should be 0 on startup"
    );
}

#[test]
fn sink_health_shows_healthy_via_noop_sink() {
    let srv = ServerHandle::start("sink_health");
    let body = srv.scrape_metrics();

    // NoopSink always reports Healthy; weir_sink_health is pre-initialised.
    assert!(
        body.contains("weir_sink_health{state=\"healthy\"} 1"),
        "NoopSink should report Healthy; body:\n{body:.500}"
    );
    assert!(
        body.contains("weir_sink_health{state=\"degraded\"} 0"),
        "degraded should be 0 with NoopSink"
    );
    assert!(
        body.contains("weir_sink_health{state=\"down\"} 0"),
        "down should be 0 with NoopSink"
    );
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

#[test]
fn server_shuts_down_cleanly_on_sigterm() {
    let srv = ServerHandle::start("shutdown");
    let mut client = srv.client();
    client.push(b"before-shutdown", Durability::Sync).unwrap();

    // SIGTERM — ServerHandle::shutdown sends it and waits.
    srv.shutdown();
    // If we reach here the process exited without hanging.
}

#[test]
fn server_exits_and_socket_disappears_after_sigterm() {
    let srv = ServerHandle::start("socket_gone");
    let socket_path = srv.socket_path.clone();

    assert!(socket_path.exists(), "socket should exist before shutdown");
    srv.shutdown();

    // The daemon removes its socket on clean exit.
    assert!(
        !socket_path.exists(),
        "socket should be removed after clean shutdown"
    );
}

// ── Reconnect / restart ───────────────────────────────────────────────────────

#[test]
fn new_connection_accepted_after_previous_client_drops() {
    let srv = ServerHandle::start("reconnect");

    {
        let mut c1 = srv.client();
        c1.push(b"first connection", Durability::Buffered).unwrap();
    } // c1 dropped → connection closed

    // New client should connect immediately.
    let mut c2 = srv.client();
    c2.push(b"second connection", Durability::Buffered).unwrap();
}

// ── Payload edge cases ────────────────────────────────────────────────────────

#[test]
fn empty_payload_is_accepted() {
    let srv = ServerHandle::start("empty_payload");
    let mut client = srv.client();
    client.push(b"", Durability::Sync).unwrap();
}

#[test]
fn arbitrary_binary_payload_accepted() {
    // Renamed from binary_payload_round_trips: there is no Pop API, so no
    // round-trip occurs. The test verifies the server doesn't strip null bytes
    // or high bytes in any text-mode handling.
    let srv = ServerHandle::start("binary_payload");
    let mut client = srv.client();
    let payload: Vec<u8> = (0u8..=255).collect();
    client.push(&payload, Durability::Sync).unwrap();
}

#[test]
fn large_payload_accepted() {
    let srv = ServerHandle::start("large_payload");
    let mut client = srv.client();
    // 1 MiB — well within MAX_PAYLOAD_HARD_CAP (16 MiB).
    let payload = vec![0x42u8; 1024 * 1024];
    client.push(&payload, Durability::Batched).unwrap();
}

// ── Stress ────────────────────────────────────────────────────────────────────

#[test]
fn sustained_load_1000_records_single_client() {
    let srv = ServerHandle::start("sustained");
    let mut client = srv.client();
    for i in 0..1000u32 {
        client
            .push(format!("load-{i:06}").as_bytes(), Durability::Buffered)
            .unwrap_or_else(|e| panic!("record {i} failed: {e}"));
    }
}

#[test]
fn mixed_durability_under_concurrent_load() {
    const THREADS: usize = 6;
    const RECORDS: usize = 50;

    let srv = ServerHandle::start("mixed_load");
    let socket_path = srv.socket_path.clone();

    let tiers = [Durability::Sync, Durability::Batched, Durability::Buffered];
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            let tier = tiers[t % tiers.len()];
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect: {e}"));
                for i in 0..RECORDS {
                    client
                        .push(format!("t{t}-r{i}").as_bytes(), tier)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("producer thread panicked");
    }
}

// ── Crash recovery ────────────────────────────────────────────────────────────

#[test]
fn server_restarts_after_sigkill() {
    let mut srv = ServerHandle::start("crash_restart");
    srv.client()
        .push(b"before-crash", Durability::Sync)
        .unwrap();

    srv.kill_ungracefully();

    // Socket file persists on disk (SIGKILL left it behind).
    assert!(
        srv.socket_path.exists(),
        "socket should remain after SIGKILL"
    );

    // bind_cleanup removes the stale socket; server starts clean.
    srv.restart_in_place();
    srv.client()
        .push(b"after-restart", Durability::Sync)
        .unwrap();
}

#[test]
fn wab_data_preserved_across_crash_restart() {
    let mut srv = ServerHandle::start("wab_crash");
    let mut client = srv.client();
    for i in 0..20u32 {
        client
            .push(format!("crash-rec-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }
    drop(client);
    thread::sleep(Duration::from_millis(150));

    let bytes_before = wab_dir_bytes(&srv.wab_dir);
    assert!(bytes_before > 0, "WAB should have data before crash");

    srv.kill_ungracefully();

    let bytes_after_kill = wab_dir_bytes(&srv.wab_dir);
    assert_eq!(
        bytes_before, bytes_after_kill,
        "WAB data must not change during a crash"
    );

    srv.restart_in_place();

    let bytes_after_restart = wab_dir_bytes(&srv.wab_dir);
    assert!(
        bytes_after_restart > 0,
        "WAB data must persist across crash + restart"
    );
}

// ── Fault injection ───────────────────────────────────────────────────────────

#[test]
fn readonly_wab_dir_prevents_startup() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;

    let _lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let tmp_dir = std::env::temp_dir().join(format!("weir_fault_ro_{}", std::process::id()));
    let wab_dir = tmp_dir.join("wab");
    let socket_dir = tmp_dir.join("run");
    let socket_path = socket_dir.join("weir.sock");
    let config_path = tmp_dir.join("weir.toml");
    let metrics_port = free_port();

    fs::create_dir_all(&wab_dir).unwrap();
    fs::create_dir_all(&socket_dir).unwrap();

    // Remove all permissions so the server cannot create shard subdirs.
    fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o000)).unwrap();

    // When the test harness runs as root, chmod 0o000 doesn't prevent access —
    // root bypasses DAC. Drop privileges in the child to uid `nobody` (65534)
    // so the permission bit actually bites. socket_dir is widened so the
    // dropped child can bind the socket; the test target is wab_dir access,
    // not socket creation.
    let drop_to_nobody = unsafe { libc::geteuid() } == 0;
    if drop_to_nobody {
        fs::set_permissions(&socket_dir, fs::Permissions::from_mode(0o777)).unwrap();
        fs::set_permissions(&tmp_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let config = format!(
        "[server]\n\
         socket_path  = \"{}\"\n\
         wab_dir      = \"{}\"\n\
         metrics_port = {}\n\
         shard_count  = 1\n\
         worker_count = 2\n\
         batch_size   = 100\n\
         batch_deadline_ms = 20\n\
         log_level    = \"error\"\n",
        socket_path.display(),
        wab_dir.display(),
        metrics_port,
    );
    fs::write(&config_path, config).unwrap();

    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut cmd = Command::new(binary);
    cmd.args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    if drop_to_nobody {
        unsafe {
            cmd.pre_exec(|| {
                // 65534 is the conventional `nobody` uid in Linux containers
                // (busybox, Debian, Ubuntu, Alpine all default to this). If the
                // setuid call fails (uid doesn't exist on this system), let
                // the child exec proceed — the test will then fall back to
                // the original behavior, which is the only thing we can do
                // without a guaranteed-present unprivileged uid.
                let _ = libc::setgid(65534);
                let _ = libc::setuid(65534);
                Ok(())
            });
        }
    }

    let mut child = cmd.spawn().expect("failed to spawn weir-server");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut exit_status = None;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            exit_status = Some(status);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    if exit_status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }

    // Restore permissions so cleanup can remove the directory.
    fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).ok();
    fs::remove_dir_all(&tmp_dir).ok();

    let failed = exit_status.map(|s| !s.success()).unwrap_or(false);
    assert!(
        failed,
        "weir-server should fail to start when wab_dir is unreadable/unwritable \
         (running {}as root)",
        if drop_to_nobody { "originally " } else { "not " }
    );
}

// ── Multi-shard correctness ───────────────────────────────────────────────────

#[test]
fn all_pushes_acked_with_multiple_shards() {
    let srv = ServerHandle::start_sharded("multi_shard_ack", 4);
    let mut client = srv.client();
    for i in 0..100u32 {
        client
            .push(format!("shard-rec-{i:04}").as_bytes(), Durability::Batched)
            .unwrap_or_else(|e| panic!("record {i} failed: {e}"));
    }
}

#[test]
fn shard_directories_created_on_disk() {
    let srv = ServerHandle::start_sharded("shard_dirs", 3);
    let mut client = srv.client();
    client
        .push(b"trigger-shard-creation", Durability::Sync)
        .unwrap();
    thread::sleep(Duration::from_millis(100));

    // WAB creates shard_00, shard_01, shard_02 directories.
    let mut found = 0usize;
    for entry in fs::read_dir(&srv.wab_dir).unwrap().flatten() {
        if entry.file_type().unwrap().is_dir() {
            let name = entry.file_name();
            if name
                .to_str()
                .map(|n| n.starts_with("shard_"))
                .unwrap_or(false)
            {
                found += 1;
            }
        }
    }
    assert!(
        found >= 3,
        "expected at least 3 shard dirs in {}, found {found}",
        srv.wab_dir.display()
    );
}

#[test]
fn concurrent_producers_all_acked_with_multiple_shards() {
    const THREADS: usize = 4;
    const RECORDS_PER_THREAD: usize = 50;

    let srv = ServerHandle::start_sharded("multi_shard_conc", 4);
    let socket_path = srv.socket_path.clone();

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let path = socket_path.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path)
                    .unwrap_or_else(|e| panic!("thread {t}: connect: {e}"));
                for i in 0..RECORDS_PER_THREAD {
                    client
                        .push(format!("t{t}-r{i}").as_bytes(), Durability::Sync)
                        .unwrap_or_else(|e| panic!("thread {t} record {i}: {e}"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("producer thread panicked");
    }
}

// ── Graceful shutdown under load ──────────────────────────────────────────────

/// Verifies that SIGTERM under concurrent Sync load produces no silent drops.
///
/// Every push that returned `Ok` must be on disk (Sync durability guarantee).
/// Every push that did not complete must surface as `ClientError::Io` so the
/// producer knows it needs to retry — not a silent half-write or a panic.
#[test]
fn graceful_shutdown_under_load() {
    const THREADS: usize = 8;
    const PUSH_BEFORE_SIGTERM: Duration = Duration::from_secs(2);
    // shutdown_timeout_secs=3 in config + buffer for process exit overhead.
    const MAX_SHUTDOWN_SECS: u64 = 8;

    let mut srv = ServerHandle::start("shutdown_load");

    let ok_count = Arc::new(AtomicU64::new(0));
    let io_err_count = Arc::new(AtomicU64::new(0));
    let unexpected_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let ok = Arc::clone(&ok_count);
            let io_err = Arc::clone(&io_err_count);
            let unexpected = Arc::clone(&unexpected_count);
            let path = srv.socket_path.clone();
            thread::spawn(move || {
                let mut client = match WeirClient::connect(&path) {
                    Ok(c) => c,
                    Err(_) => return, // server already gone
                };
                loop {
                    match client.push(b"shutdown-load", Durability::Sync) {
                        Ok(()) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ClientError::Io(_)) => {
                            // Connection closed — expected during shutdown.
                            io_err.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => {
                            // Nack or protocol error — not expected.
                            unexpected.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    // Let the threads push for a bit, then signal shutdown.
    thread::sleep(PUSH_BEFORE_SIGTERM);
    let shutdown_elapsed = srv.sigterm();

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    let oks = ok_count.load(Ordering::Relaxed);
    let io_errs = io_err_count.load(Ordering::Relaxed);
    let unexpected = unexpected_count.load(Ordering::Relaxed);
    let wab_bytes = wab_dir_bytes(&srv.wab_dir);

    // Server must exit within a reasonable bound after SIGTERM.
    assert!(
        shutdown_elapsed < Duration::from_secs(MAX_SHUTDOWN_SECS),
        "server took {shutdown_elapsed:?} to shut down — expected < {MAX_SHUTDOWN_SECS}s"
    );

    // No unexpected errors: threads may see Ok or Io(EOF), never Nack/Protocol.
    assert_eq!(
        unexpected, 0,
        "{unexpected} unexpected errors (Nack or Protocol) during shutdown — \
         producers should only see Ok or Io"
    );

    // Every Ok means a Sync-flushed record. The WAB must have bytes.
    assert!(
        oks > 0,
        "expected successful pushes before SIGTERM; got 0 — \
         either the server started too slowly or {PUSH_BEFORE_SIGTERM:?} was too short"
    );
    assert!(
        wab_bytes > 0,
        "WAB has 0 bytes on disk after {oks} successful Sync pushes — \
         possible silent data loss"
    );

    println!(
        "graceful_shutdown_under_load: {oks} ok, {io_errs} io_err, \
         {unexpected} unexpected | wab={wab_bytes}B | shutdown={shutdown_elapsed:?}"
    );
}

// ── Stalled client isolation ──────────────────────────────────────────────────

/// A client that connects, sends one Push frame, and then never reads the Ack
/// must not block or slow down other connections.
///
/// The stalled connection holds a permit in the connection semaphore and keeps
/// the server's per-connection async task suspended (waiting to read the next
/// frame). Other connections must get their own permits and proceed normally.
#[test]
fn stalled_client_does_not_block_other_connections() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Envelope, Header, MessageType};

    const CONCURRENT_RECORDS: usize = 50;
    const CONCURRENT_DEADLINE: Duration = Duration::from_secs(5);

    let srv = ServerHandle::start("stall_isolation");

    // Pre-encode one Push frame for the stalled client to send.
    let payload = b"stall";
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let frame = Envelope::new(header, payload.to_vec()).encode();

    let stop_stall = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Stalled client: connects, sends one frame, holds the connection open
    // without ever reading the Ack. The server task for this connection is
    // suspended waiting for the next frame — which never comes.
    let stall_handle = {
        let path = srv.socket_path.clone();
        let stop = Arc::clone(&stop_stall);
        thread::spawn(move || {
            let mut stream = RawStream::connect(&path).expect("stall: connect");
            stream.write_all(&frame).expect("stall: write frame");
            // Deliberately do not read the Ack. Hold the connection open.
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(50));
            }
        })
    };

    // Let the stall thread connect and send its frame before we proceed.
    thread::sleep(Duration::from_millis(100));

    // Concurrent client: 50 Sync pushes while the stalled connection is held.
    let mut client = srv.client();
    let t0 = Instant::now();
    for i in 0..CONCURRENT_RECORDS {
        client
            .push(format!("concurrent-{i}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("concurrent push {i} failed: {e}"));
    }
    let elapsed = t0.elapsed();

    stop_stall.store(true, Ordering::Relaxed);
    stall_handle.join().expect("stall thread panicked");

    assert!(
        elapsed < CONCURRENT_DEADLINE,
        "{CONCURRENT_RECORDS} pushes took {elapsed:?} with stalled connection held — \
         expected < {CONCURRENT_DEADLINE:?} (stalled client may be blocking the worker)"
    );

    srv.client()
        .health_check()
        .expect("server unresponsive after stalled client test");
}

// ── Partial frame injection ───────────────────────────────────────────────────

/// Sending a valid header then only half the declared payload bytes before
/// closing the connection must not corrupt the server's per-connection state
/// machine. The next fresh connection must work normally.
#[test]
fn partial_frame_does_not_corrupt_next_connection() {
    use std::{io::Write, os::unix::net::UnixStream as RawStream};
    use weir_core::{Envelope, HEADER_LEN, Header, MessageType};

    let srv = ServerHandle::start("partial_frame");

    // Build a valid Push frame with a 64-byte payload.
    let payload = vec![0xabu8; 64];
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let frame = Envelope::new(header, payload).encode();

    {
        let mut stream = RawStream::connect(&srv.socket_path).expect("connect for partial frame");
        // Write only header + first 16 bytes of the 64-byte payload.
        stream
            .write_all(&frame[..HEADER_LEN + 16])
            .expect("write partial frame");
        // Drop the stream — connection dies mid-frame.
    }

    // Give the server time to observe the EOF and clean up the connection.
    thread::sleep(Duration::from_millis(50));

    // A fresh connection must work normally — the partial frame must not have
    // left the server's read state in a corrupt position.
    srv.client()
        .push(b"after-partial-frame", Durability::Sync)
        .expect("push failed after partial frame injection");

    srv.client()
        .health_check()
        .expect("server unresponsive after partial frame test");
}

// ── Write-error handling (EFBIG / ENOSPC) ─────────────────────────────────────

/// A WAB write failure caused by `RLIMIT_FSIZE = 0` (kernel returns `EFBIG`)
/// must produce `Nack(InternalError)` on the client, not a server crash or a
/// silent data drop. EFBIG is the cheapest write-failure mode to simulate
/// without root: see `enospc_returns_nack_not_crash` for the
/// production-shaped ENOSPC variant.
#[test]
fn efbig_returns_nack_not_crash() {
    use weir_core::NackReason;

    let srv = ServerHandle::start_disk_full("efbig");
    let mut client = srv.client();

    // With RLIMIT_FSIZE=0 the first WAB segment header write fails immediately.
    let result = client.push(b"should-nack", Durability::Sync);
    assert!(
        matches!(result, Err(ClientError::Nack(NackReason::InternalError))),
        "expected Nack(InternalError) from EFBIG-throttled server, got {result:?}"
    );

    // Server must still be alive and accepting connections after the nack.
    srv.client()
        .health_check()
        .expect("server unresponsive after EFBIG nack");
}

/// ENOSPC variant — production-shaped write failure (filesystem out of space)
/// rather than EFBIG (file-size rlimit hit). Requires a small pre-mounted
/// filesystem at the path in `WEIR_TEST_ENOSPC_DIR`; ignored by default
/// because creating one needs root.
///
/// Setup (run once, as root, before invoking this test):
///
/// ```sh
/// sudo mkdir -p /mnt/weir-enospc
/// sudo mount -t tmpfs -o size=64K tmpfs /mnt/weir-enospc
/// sudo chmod 0700 /mnt/weir-enospc
/// sudo chown $USER /mnt/weir-enospc
/// WEIR_TEST_ENOSPC_DIR=/mnt/weir-enospc \
///   cargo test -p weir-server --test system -- --ignored enospc_returns_nack_not_crash
/// sudo umount /mnt/weir-enospc && sudo rmdir /mnt/weir-enospc
/// ```
///
/// The 64 KiB tmpfs is small enough that the first WAB segment header
/// (16 KiB pre-allocated) plus a single Sync record fills it; subsequent
/// pushes must Nack rather than panic.
#[test]
#[ignore = "requires WEIR_TEST_ENOSPC_DIR pointing at a small pre-mounted tmpfs (see test docstring)"]
fn enospc_returns_nack_not_crash() {
    use std::os::unix::process::CommandExt;
    use weir_core::NackReason;

    let enospc_dir = std::env::var("WEIR_TEST_ENOSPC_DIR").expect(
        "WEIR_TEST_ENOSPC_DIR not set — see test docstring for the tmpfs setup procedure",
    );
    let enospc_dir = PathBuf::from(enospc_dir);

    let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let metrics_port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("weir_sys_enospc_{}", std::process::id()));
    let wab_dir = enospc_dir.join("wab");
    let socket_dir = tmp_dir.join("run");
    let socket_path = socket_dir.join("weir.sock");
    let config_path = tmp_dir.join("weir.toml");

    fs::create_dir_all(&socket_dir).expect("create socket dir");
    // WAB dir on the small filesystem; tolerate "already exists" from a prior run.
    if let Err(e) = fs::create_dir(&wab_dir)
        && e.kind() != std::io::ErrorKind::AlreadyExists
    {
        panic!("create wab_dir on {}: {e}", enospc_dir.display());
    }
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).unwrap();
    }

    let config = format!(
        "[server]\n\
         socket_path           = \"{}\"\n\
         wab_dir               = \"{}\"\n\
         metrics_port          = {}\n\
         shard_count           = 1\n\
         worker_count          = 2\n\
         batch_size            = 100\n\
         batch_deadline_ms     = 20\n\
         shutdown_timeout_secs = 3\n\
         log_level             = \"warn\"\n",
        socket_path.display(),
        wab_dir.display(),
        metrics_port,
    );
    fs::write(&config_path, config).unwrap();

    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut cmd = Command::new(binary);
    cmd.args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Ignore SIGXFSZ defensively; not strictly needed for ENOSPC but harmless.
    unsafe {
        cmd.pre_exec(|| {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            Ok(())
        });
    }
    let child = cmd.spawn().expect("failed to spawn weir-server (enospc)");

    let mut handle = ServerHandle {
        child: Some(child),
        socket_path,
        wab_dir: wab_dir.clone(),
        metrics_port,
        config_path,
        tmp_dir,
        _proc_lock,
    };
    handle.wait_ready(Duration::from_secs(15));

    // Push records until one fails with Nack(InternalError). The 64 KiB tmpfs
    // should fill within a small handful of records.
    let mut client = handle.client();
    let mut saw_nack = false;
    for i in 0..200u32 {
        let payload = vec![0xAAu8; 1024]; // 1 KiB; tmpfs holds ~64.
        match client.push(&payload, Durability::Sync) {
            Ok(()) => continue,
            Err(ClientError::Nack(NackReason::InternalError)) => {
                saw_nack = true;
                break;
            }
            Err(other) => panic!("unexpected error on record {i}: {other:?}"),
        }
    }
    assert!(
        saw_nack,
        "filesystem at {} did not return ENOSPC within 200 × 1 KiB records — \
         is it larger than 64 KiB? Check tmpfs size.",
        enospc_dir.display()
    );

    // Server must still be alive after the nack.
    handle
        .client()
        .health_check()
        .expect("server unresponsive after ENOSPC nack");
}

// ── WAB data integrity after crash ────────────────────────────────────────────

/// Every Sync push that returned `Ok` must be present on disk byte-for-byte
/// after the server is killed with SIGKILL.
///
/// This tests the "Sync durability" contract at the byte level: if the client
/// got an `Ok`, the payload must be in the WAB file (the fsync happened before
/// the ack was sent).
#[test]
fn wab_data_integrity_after_crash() {
    const N: usize = 50;

    let mut srv = ServerHandle::start_sharded("wab_integrity", 1);
    let mut client = srv.client();

    let mut acked: Vec<Vec<u8>> = Vec::new();
    for i in 0..N {
        let payload = format!("integrity-{i:05}").into_bytes();
        match client.push(&payload, Durability::Sync) {
            Ok(()) => acked.push(payload),
            Err(_) => break, // server died mid-push
        }
    }

    assert!(!acked.is_empty(), "no pushes acked before crash");

    // Crash without cleanup — WAB files stay on disk exactly as they were.
    srv.kill_ungracefully();

    let wab_bytes = read_wab_bytes(&srv.wab_dir);
    assert!(
        !wab_bytes.is_empty(),
        "WAB directory empty after {} acked Sync pushes",
        acked.len()
    );

    // Every acked payload must appear verbatim in the WAB bytes.
    for payload in &acked {
        let found = wab_bytes
            .windows(payload.len())
            .any(|w| w == payload.as_slice());
        assert!(
            found,
            "acked payload {:?} not found in WAB bytes — possible data loss",
            String::from_utf8_lossy(payload)
        );
    }
}

// ── Socket takeover data safety ───────────────────────────────────────────────

/// `bind_cleanup` removes the socket file (even if another process is
/// listening) so that crash-recovery always succeeds. When a second server
/// takes the socket path the first server's WAB files must be left entirely
/// untouched — the socket file and the WAB are independent resources.
#[test]
fn socket_takeover_does_not_corrupt_wab_data() {
    const N: usize = 20;

    let srv_a = ServerHandle::start("socket_takeover");
    let mut client = srv_a.client();

    for i in 0..N {
        client
            .push(format!("srv-a-{i}").as_bytes(), Durability::Sync)
            .expect("push to server A failed");
    }

    let wab_before = wab_dir_bytes(&srv_a.wab_dir);
    assert!(wab_before > 0, "server A must have written WAB bytes");

    // Spawn server B at the same socket path (it will call bind_cleanup and
    // take over the socket). Use Command directly — we hold the process lock
    // via srv_a and need two processes alive at once intentionally.
    let second_tmp =
        std::env::temp_dir().join(format!("weir_sys_takeover_b_{}", std::process::id()));
    let second_wab = second_tmp.join("wab");
    let second_config = second_tmp.join("weir.toml");

    fs::create_dir_all(&second_wab).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&second_wab, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(
        &second_config,
        format!(
            "[server]\n\
             socket_path           = \"{}\"\n\
             wab_dir               = \"{}\"\n\
             metrics_port          = {}\n\
             shard_count           = 1\n\
             worker_count          = 2\n\
             batch_size            = 100\n\
             batch_deadline_ms     = 20\n\
             shutdown_timeout_secs = 3\n\
             log_level             = \"warn\"\n",
            srv_a.socket_path.display(),
            second_wab.display(),
            free_port(),
        ),
    )
    .unwrap();

    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut child_b = Command::new(binary)
        .args(["--config", second_config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server B");

    // Give server B time to start and take the socket.
    thread::sleep(Duration::from_millis(500));

    // Server A's WAB files must be completely untouched.
    let wab_after = wab_dir_bytes(&srv_a.wab_dir);
    assert_eq!(
        wab_before, wab_after,
        "server B startup modified server A's WAB bytes ({wab_before} → {wab_after})"
    );

    // Verify payload bytes are still present.
    let wab_bytes = read_wab_bytes(&srv_a.wab_dir);
    for i in 0..N {
        let payload = format!("srv-a-{i}").into_bytes();
        assert!(
            wab_bytes
                .windows(payload.len())
                .any(|w| w == payload.as_slice()),
            "payload srv-a-{i} missing from server A's WAB after socket takeover"
        );
    }

    let _ = child_b.kill();
    let _ = child_b.wait();
    let _ = fs::remove_dir_all(&second_tmp);
}

// ── File descriptor limit exhaustion ─────────────────────────────────────────

/// When the server hits its `RLIMIT_NOFILE` ceiling it must not crash —
/// new connections are refused or queued by the kernel, and connections
/// within the fd budget continue to work normally.
#[test]
fn fd_limit_exhaustion_does_not_crash_server() {
    use std::os::unix::net::UnixStream as RawStream;

    // 128 fds: comfortably above server startup overhead (~20 fds) but low
    // enough that opening 200 connections will exhaust the limit.
    const NOFILE_LIMIT: u64 = 128;
    const FLOOD_CONNS: usize = 200;

    let srv = ServerHandle::start_with_nofile_limit("fd_limit", NOFILE_LIMIT);

    // Flood the server with raw connections and keep them open.
    let mut open: Vec<RawStream> = Vec::new();
    for _ in 0..FLOOD_CONNS {
        match RawStream::connect(&srv.socket_path) {
            Ok(s) => open.push(s),
            Err(_) => break, // kernel backlog full — stop
        }
    }

    // Hold connections briefly to let the server try (and fail) to accept them.
    thread::sleep(Duration::from_millis(200));

    drop(open);

    // After releasing the flood, the server must still be alive.
    thread::sleep(Duration::from_millis(100));
    srv.client()
        .health_check()
        .expect("server crashed or hung under fd pressure");

    // A normal Sync push must succeed after the fd pressure is relieved.
    srv.client()
        .push(b"after-fd-flood", Durability::Sync)
        .expect("push failed after fd-limit flood");
}

// ── Metrics accuracy ──────────────────────────────────────────────────────────

#[test]
fn records_accepted_counter_increments_after_sync_pushes() {
    const N: u32 = 10;

    let srv = ServerHandle::start("metrics_accepted");
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("acc-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_accepted_total{{tier=\"sync\"}} {N}");
    assert!(
        body.contains(&expected),
        "expected '{expected}' in metrics; body:\n{body:.800}"
    );
}

#[test]
fn records_ack_counter_increments_after_sync_pushes() {
    const N: u32 = 7;

    let srv = ServerHandle::start("metrics_ack");
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("ack-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }

    let body = srv.scrape_metrics();
    let expected = format!("weir_records_ack_total{{tier=\"sync\"}} {N}");
    assert!(
        body.contains(&expected),
        "expected '{expected}' in metrics; body:\n{body:.800}"
    );
}

// ── Per-shard record ordering ─────────────────────────────────────────────────

/// With a single-shard server, records submitted sequentially from a single
/// producer must appear in submission order in the raw WAB bytes.
///
/// This is the fundamental append-log ordering contract. Any change to the
/// batching or queue path that accidentally reorders records will be caught
/// here.
#[test]
fn per_shard_records_appear_in_submission_order() {
    const N: usize = 30;

    // Single shard: all records go to the same WAB file, so order is preserved.
    let srv = ServerHandle::start_sharded("ordering", 1);
    let mut client = srv.client();

    for i in 0..N {
        client
            .push(format!("order-{i:05}").as_bytes(), Durability::Sync)
            .expect("push failed");
    }

    // Flush any remaining data and give the server time to seal.
    srv.client()
        .health_check()
        .expect("server unresponsive after pushes");

    let wab_bytes = read_wab_bytes(&srv.wab_dir);
    assert!(!wab_bytes.is_empty(), "WAB must have data after pushes");

    // Find the byte offset of each payload in the WAB.
    let mut prev_offset: Option<usize> = None;
    for i in 0..N {
        let payload = format!("order-{i:05}").into_bytes();
        let offset = wab_bytes
            .windows(payload.len())
            .position(|w| w == payload.as_slice())
            .unwrap_or_else(|| panic!("payload order-{i:05} not found in WAB bytes"));

        if let Some(prev) = prev_offset {
            assert!(
                offset > prev,
                "record order-{i:05} at offset {offset} appears before the previous record \
                 at offset {prev} — submission order not preserved"
            );
        }
        prev_offset = Some(offset);
    }
}

// ── Batch deadline timer accuracy ─────────────────────────────────────────────

/// With `batch_deadline_ms = 20`, each individual Sync push must complete
/// within a generous multiple of the deadline. A push that exceeds 5×deadline
/// indicates the batch timer is being starved (e.g. the accept loop is
/// spinning) and latency would be non-deterministic in production.
#[test]
fn batch_deadline_timer_keeps_latency_bounded() {
    const SAMPLES: usize = 20;
    const DEADLINE_MS: u64 = 20; // matches start_impl config
    const MAX_EACH: Duration = Duration::from_millis(DEADLINE_MS * 5); // 100 ms
    const MAX_P99: Duration = Duration::from_millis(DEADLINE_MS * 3); // 60 ms

    let srv = ServerHandle::start("deadline_accuracy");
    let mut client = srv.client();
    let mut latencies: Vec<Duration> = Vec::with_capacity(SAMPLES);

    for i in 0..SAMPLES {
        let t0 = Instant::now();
        client
            .push(format!("timer-{i}").as_bytes(), Durability::Sync)
            .expect("push failed");
        latencies.push(t0.elapsed());
    }

    // Every sample must finish within 5 × deadline.
    for (i, &lat) in latencies.iter().enumerate() {
        assert!(
            lat <= MAX_EACH,
            "sample {i} took {lat:?} — exceeded 5 × batch_deadline_ms ({MAX_EACH:?})"
        );
    }

    // p99 (here: worst sample across 20) must be within 3 × deadline.
    let mut sorted = latencies.clone();
    sorted.sort();
    let p99 = sorted[sorted.len() - 1]; // max of 20 samples ≈ p99
    assert!(
        p99 <= MAX_P99,
        "p99 latency {p99:?} exceeded 3 × batch_deadline_ms ({MAX_P99:?})"
    );
}

// ── Metrics across crash-restart ──────────────────────────────────────────────

/// Within a single server process the per-tier counters must be internally
/// consistent: `records_accepted` never exceeds the number of pushes made, and
/// `records_ack` never exceeds `records_accepted`. Three rounds across
/// restarts exercise the assertion on a fresh atomic each time.
///
/// Note: this is a *per-session* invariant. The cross-restart resets and the
/// recovery counter are tested separately
/// (`metrics_reset_to_zero_after_restart`, `recovery_replays_records_after_crash`).
#[test]
fn metrics_internally_consistent_per_session() {
    const PUSHES_PER_ROUND: u32 = 10;
    const ROUNDS: u32 = 3;

    let mut srv = ServerHandle::start("metrics_per_session");

    for round in 0..ROUNDS {
        let mut client = srv.client();
        for i in 0..PUSHES_PER_ROUND {
            client
                .push(
                    format!("round-{round}-rec-{i}").as_bytes(),
                    Durability::Sync,
                )
                .unwrap_or_else(|e| panic!("push failed (round {round}, rec {i}): {e}"));
        }

        let body = srv.scrape_metrics();

        let accepted = parse_metric(&body, "weir_records_accepted_total{tier=\"sync\"}");
        let acked = parse_metric(&body, "weir_records_ack_total{tier=\"sync\"}");

        assert!(
            accepted <= u64::from(PUSHES_PER_ROUND),
            "round {round}: records_accepted ({accepted}) exceeds pushes made \
             ({PUSHES_PER_ROUND}) — phantom records in counter"
        );
        assert!(
            acked <= accepted,
            "round {round}: records_ack ({acked}) > records_accepted ({accepted})"
        );

        if round + 1 < ROUNDS {
            srv.restart_in_place();
        }
    }
}

/// `records_accepted_total` and `records_ack_total` are in-process atomics —
/// they must reset to 0 on every restart, even if records were on disk.
/// This documents the deliberate non-persistence: Prometheus counters are
/// cumulative *within a process*, and an external scraper handles restart
/// gaps via the `_created` timestamp.
#[test]
fn metrics_reset_to_zero_after_restart() {
    let mut srv = ServerHandle::start("metrics_reset");

    // Drive both counters above zero.
    let mut client = srv.client();
    for i in 0..5u32 {
        client
            .push(format!("pre-restart-{i}").as_bytes(), Durability::Sync)
            .unwrap();
    }
    drop(client);

    let before = srv.scrape_metrics();
    let accepted_before = parse_metric(&before, "weir_records_accepted_total{tier=\"sync\"}");
    let acked_before = parse_metric(&before, "weir_records_ack_total{tier=\"sync\"}");
    assert!(
        accepted_before >= 5 && acked_before >= 5,
        "expected counters to be ≥5 before restart (accepted={accepted_before}, acked={acked_before})",
    );

    srv.restart_in_place();

    // Immediately after restart, before any new pushes.
    let after = srv.scrape_metrics();
    let accepted_after = parse_metric(&after, "weir_records_accepted_total{tier=\"sync\"}");
    let acked_after = parse_metric(&after, "weir_records_ack_total{tier=\"sync\"}");
    assert_eq!(
        accepted_after, 0,
        "records_accepted_total should reset to 0 after restart, got {accepted_after}"
    );
    assert_eq!(
        acked_after, 0,
        "records_ack_total should reset to 0 after restart, got {acked_after}"
    );
}

/// Crash recovery must replay the records on disk and increment
/// `weir_recovery_records_replayed_total` accordingly. Closes the gap that
/// the audit flagged: the metric exists but no test asserted it advances.
///
/// Procedure:
/// 1. Push N Sync records — guaranteed durable in the active WAB segment.
/// 2. SIGKILL the server (active segment left as `.wab`, no footer).
/// 3. Restart — recovery should seal the active segment and replay it.
/// 4. Scrape metrics; assert `weir_recovery_records_replayed_total >= N`.
#[test]
fn recovery_replays_records_after_crash() {
    const N: u32 = 25;

    let mut srv = ServerHandle::start_sharded("recovery_replay", 1);
    let mut client = srv.client();
    for i in 0..N {
        client
            .push(format!("recover-{i:05}").as_bytes(), Durability::Sync)
            .unwrap();
    }
    drop(client);

    srv.kill_ungracefully();
    srv.restart_in_place();

    // Give the drain a moment to process the replayed segment so the counter
    // has actually been incremented before we scrape.
    thread::sleep(Duration::from_millis(200));

    let body = srv.scrape_metrics();
    let replayed = parse_metric(&body, "weir_recovery_records_replayed_total");
    assert!(
        replayed >= u64::from(N),
        "expected weir_recovery_records_replayed_total >= {N}, got {replayed}\n\
         recovery did not replay all crashed records — metric: {replayed}\n\
         metrics body excerpt:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_recovery") || l.starts_with("weir_wab_segments"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ── MySQL sink integration ────────────────────────────────────────────────────

/// End-to-end check that records pushed to a daemon configured with
/// `sink_type = "mysql"` arrive in the configured table — and arrive there
/// as one multi-row INSERT per batch, demonstrating the IOPS-compression
/// story.
///
/// Ignored by default because it requires a running MySQL server reachable
/// at the URL in `WEIR_TEST_MYSQL_URL`. Setup, e.g. with Docker:
///
/// ```sh
/// docker run --rm -d --name weir-test-mysql \
///   -e MYSQL_ROOT_PASSWORD=test \
///   -e MYSQL_DATABASE=weir_test \
///   -p 3306:3306 mysql:8.0
/// # Wait ~10s for mysqld to come up.
/// docker exec weir-test-mysql mysql -ptest weir_test -e "
///   CREATE TABLE weir_records (
///     id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
///     payload VARBINARY(4096) NOT NULL,
///     UNIQUE KEY(payload(255))
///   );"
/// WEIR_TEST_MYSQL_URL=mysql://root:test@127.0.0.1:3306/weir_test \
///   cargo test -p weir-server --test system -- --ignored mysql_sink_end_to_end
/// ```
#[test]
#[ignore = "requires WEIR_TEST_MYSQL_URL pointing at a running MySQL with a prepared schema (see docstring)"]
fn mysql_sink_end_to_end() {
    use std::os::unix::process::CommandExt;

    const N: u32 = 100;

    let mysql_url = std::env::var("WEIR_TEST_MYSQL_URL").expect(
        "WEIR_TEST_MYSQL_URL not set — see the test docstring for the docker-compose recipe",
    );

    // Spawn weir-server with sink_type = mysql, pointing at the live server.
    let _proc_lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
    let metrics_port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("weir_sys_mysql_{}", std::process::id()));
    let wab_dir = tmp_dir.join("wab");
    let socket_dir = tmp_dir.join("run");
    let socket_path = socket_dir.join("weir.sock");
    let config_path = tmp_dir.join("weir.toml");
    let log_path = tmp_dir.join("weir.log");

    fs::create_dir_all(&wab_dir).unwrap();
    fs::create_dir_all(&socket_dir).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).unwrap();
    }

    // sink_type = mysql; URL passed via env so credentials never touch the
    // config file (production-shaped — see the operations docs).
    let config = format!(
        "[server]\n\
         socket_path           = \"{}\"\n\
         wab_dir               = \"{}\"\n\
         metrics_port          = {}\n\
         shard_count           = 1\n\
         worker_count          = 2\n\
         batch_size            = 200\n\
         batch_deadline_ms     = 5\n\
         shutdown_timeout_secs = 5\n\
         sink_type             = \"mysql\"\n\
         sink_max_batch_size   = 1000\n\
         sink_mysql_table      = \"weir_records\"\n\
         sink_mysql_column     = \"payload\"\n\
         sink_mysql_insert_mode = \"ignore\"\n\
         log_level             = \"warn\"\n",
        socket_path.display(),
        wab_dir.display(),
        metrics_port,
    );
    fs::write(&config_path, config).unwrap();

    let log_file = fs::File::create(&log_path).unwrap();
    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut cmd = Command::new(binary);
    cmd.args(["--config", config_path.to_str().unwrap()])
        .env("WEIR_SINK_URL", &mysql_url)
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file));
    // Make sure pre_exec hooks are not inherited from previous test infrastructure.
    unsafe {
        cmd.pre_exec(|| Ok(()));
    }
    let child = cmd.spawn().expect("failed to spawn weir-server (mysql)");

    let mut handle = ServerHandle {
        child: Some(child),
        socket_path,
        wab_dir,
        metrics_port,
        config_path,
        tmp_dir,
        _proc_lock,
    };
    handle.wait_ready(Duration::from_secs(15));

    let mut client = handle.client();
    for i in 0..N {
        client
            .push(format!("mysql-rec-{i:05}").as_bytes(), Durability::Sync)
            .unwrap_or_else(|e| panic!("push {i}: {e}"));
    }
    drop(client);

    // Give the drain a moment to drain the sealed segment into MySQL.
    thread::sleep(Duration::from_secs(2));

    let body = handle.scrape_metrics();
    let committed = parse_metric(
        &body,
        "weir_sink_commit_records_total{outcome=\"committed\"}",
    );
    let commit_count = parse_metric(&body, "weir_sink_commit_duration_seconds_count");

    assert!(
        committed >= u64::from(N),
        "expected ≥{N} committed records, got {committed}\nmetrics excerpt:\n{}",
        body.lines()
            .filter(|l| l.starts_with("weir_sink_"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        commit_count > 0,
        "expected at least one Sink::commit() call to have been recorded"
    );
    // The point of MySqlSink: many records per commit. With N=100 and the
    // drain reading whole sealed segments at once, ratio should be ≥ 10×.
    // Loose bound so the test isn't flaky on tiny-batch edge cases.
    let ratio = committed as f64 / commit_count as f64;
    assert!(
        ratio >= 10.0,
        "expected ≥10:1 records-per-commit IOPS compression, got {ratio:.1}:1 \
         ({committed} records / {commit_count} commits)"
    );
}

fn parse_metric(body: &str, prefix: &str) -> u64 {
    for line in body.lines() {
        if line.starts_with(prefix)
            && let Some(val) = line.split_whitespace().next_back()
            && let Ok(n) = val.parse()
        {
            return n;
        }
    }
    0
}
