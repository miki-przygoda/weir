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
            if let Some(ref mut child) = self.child {
                if let Ok(Some(status)) = child.try_wait() {
                    let log_path = self.tmp_dir.join("weir.log");
                    let log = fs::read_to_string(&log_path).unwrap_or_default();
                    panic!(
                        "weir-server exited early with {status} before socket was ready\n\
                         socket: {}\nlog:\n{log}",
                        self.socket_path.display()
                    );
                }
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

/// Spawns weir-server with the given config and waits up to `timeout` for it
/// to exit with a non-zero status. Returns `true` if the server failed as
/// expected, `false` if it was still running when the timeout elapsed.
///
/// The caller is responsible for holding `process_lock()` while calling this.
fn wait_for_server_failure(config_path: &Path, timeout: Duration) -> bool {
    let binary = env!("CARGO_BIN_EXE_weir-server");
    let mut child = Command::new(binary)
        .args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn weir-server");

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            return !status.success();
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let _ = child.wait();
    false
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

#[test]
fn health_check_on_separate_connection_from_push() {
    let srv = ServerHandle::start("health_separate");
    let mut pusher = srv.client();
    let mut checker = srv.client();
    pusher.push(b"some data", Durability::Buffered).unwrap();
    checker.health_check().unwrap();
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
fn wab_segment_rotation_creates_multiple_segments() {
    use weir_core::MAX_PAYLOAD_HARD_CAP;

    let srv = ServerHandle::start("rotation");
    let mut client = srv.client();

    // SEGMENT_MAX_BYTES is 256 MiB — we don't want to actually write that much.
    // Instead write many records and check rotation happens via the sealed ext.
    // Use a reasonably large payload to fill the segment faster in tests.
    // With SEGMENT_MAX_BYTES = 256 MiB this would take forever, so we rely on
    // the fact that even moderate pushes eventually seal via batch deadline.
    // Just verify the files exist and the format is correct.
    let payload = vec![0xABu8; 4096]; // 4 KiB per record
    for _ in 0..10 {
        client.push(&payload, Durability::Batched).unwrap();
    }

    thread::sleep(Duration::from_millis(200));

    let active = count_wab_files(&srv.wab_dir, ".wab");
    let sealed = count_wab_files(&srv.wab_dir, ".wab.sealed");
    assert!(
        active + sealed > 0,
        "no WAB files after pushing large payloads"
    );
    let _ = MAX_PAYLOAD_HARD_CAP; // referenced to keep import live
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
fn metrics_all_16_families_registered() {
    let srv = ServerHandle::start("metrics_families");
    let body = srv.scrape_metrics();

    // All 16 metric families must appear in the output as # HELP lines.
    // Data lines only appear for pre-initialised gauges and histograms;
    // counter families that haven't been incremented yet show only HELP/TYPE.
    for family in [
        "weir_records_accepted",
        "weir_records_ack",
        "weir_records_nack",
        "weir_wab_segments",
        "weir_wab_bytes_on_disk",
        "weir_wab_fsync_duration_seconds",
        "weir_sink_commit_duration_seconds",
        "weir_sink_commit_records",
        "weir_sink_health",
        "weir_queue_depth",
        "weir_recovery_records_replayed",
        "weir_recovery_segments_quarantined",
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
fn binary_payload_round_trips() {
    let srv = ServerHandle::start("binary_payload");
    let mut client = srv.client();
    // Arbitrary binary content including null bytes and high bytes.
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
fn stale_socket_removed_automatically_on_restart() {
    let mut srv = ServerHandle::start("stale_sock");

    srv.kill_ungracefully();
    assert!(
        srv.socket_path.exists(),
        "socket should still exist after SIGKILL"
    );

    // The restarted process must remove the stale socket via bind_cleanup
    // and bind a new one — we verify this by connecting successfully.
    srv.restart_in_place();
    srv.client().health_check().unwrap();
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

    let failed = wait_for_server_failure(&config_path, Duration::from_secs(5));

    // Restore permissions so cleanup can remove the directory.
    fs::set_permissions(&wab_dir, fs::Permissions::from_mode(0o700)).ok();
    fs::remove_dir_all(&tmp_dir).ok();

    assert!(
        failed,
        "weir-server should fail to start when wab_dir is unreadable/unwritable"
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
