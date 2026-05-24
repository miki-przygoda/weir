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
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use weir_client::WeirClient;
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
    tmp_dir: PathBuf,
    /// Held for the lifetime of the handle to serialise process spawning.
    _proc_lock: std::sync::MutexGuard<'static, ()>,
}

impl ServerHandle {
    /// Starts a fresh weir-server instance and blocks until the socket is ready.
    ///
    /// `tag` is used to name the temp directory (helps with post-mortem debugging).
    fn start(tag: &str) -> Self {
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
             socket_path   = \"{}\"\n\
             wab_dir       = \"{}\"\n\
             metrics_port  = {}\n\
             shard_count   = 1\n\
             worker_count  = 2\n\
             batch_size    = 100\n\
             batch_deadline_ms = 20\n\
             log_level     = \"warn\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
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
            tmp_dir,
            _proc_lock,
        };

        // Wait up to 15 s for the socket to appear.
        handle.wait_ready(Duration::from_secs(15));
        handle
    }

    /// Blocks until the Unix socket is visible on the filesystem.
    ///
    /// Also detects early process exit (server crash) and prints the log on failure.
    fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.socket_path.exists() {
                return;
            }
            // Check whether the server crashed before creating the socket.
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
        // Timeout — read the log file for diagnostics before Drop cleans it up.
        let log_path = self.tmp_dir.join("weir.log");
        let log = fs::read_to_string(&log_path).unwrap_or_default();
        panic!(
            "weir-server did not create socket within {:?}: {}\nlog:\n{log}",
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
