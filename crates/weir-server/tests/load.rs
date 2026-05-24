//! Concurrency and throughput load tests for weir-server.
//!
//! These run as part of the dedicated `load` CI job and are NOT marked
//! `#[ignore]` — they are the baseline for an ongoing performance improvement
//! effort and must stay green on every push.
//!
//! # Running locally
//!
//! ```sh
//! # Default (1 ms deadline):
//! cargo test -p weir-server --test load --release -- --nocapture
//!
//! # Specific deadline:
//! WEIR_BENCH_DEADLINE=2 cargo test -p weir-server --test load --release -- --nocapture
//! ```
//!
//! # Deadline comparison
//!
//! CI runs the full suite twice per iteration — once with `WEIR_BENCH_DEADLINE=1`
//! and once with `WEIR_BENCH_DEADLINE=2` — and appends all results to the same
//! `load_results.jsonl`. Scenario names include a `_d{N}ms` suffix so
//! `deploy/avg_benchmarks.py` can render a side-by-side comparison table.
//!
//! # Performance work
//!
//! TODO(perf): these numbers are the starting baseline. Planned improvement areas:
//!   - End-to-end latency: reduce socket-recv → WAB-fsync → Ack round-trip
//!   - Thundering-herd throughput: profile queue contention when N threads push
//!     simultaneously; evaluate lock-free queue alternatives
//!   - Connection setup cost: measure and reduce Unix socket accept latency
//!   - Batching efficiency: tune batch_size / batch_deadline_ms sweet spot
//! Improvements are tracked by re-running this suite and comparing to the
//! numbers committed in docs/benchmarks.md at the time.

#![cfg(unix)]

use std::{
    io::Write,
    net::TcpListener,
    os::unix::net::UnixStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Barrier, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use weir_client::{ClientError, WeirClient};
use weir_core::{Durability, Envelope, Header, MessageType};

// ── Process serialiser ─────────────────────────────────────────────────────

fn process_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .unwrap()
        .port()
}

/// Reads `WEIR_BENCH_DEADLINE` from the environment (default: 1).
/// CI sets this to 1 and 2 in successive passes to populate the comparison.
fn bench_deadline_ms() -> u64 {
    std::env::var("WEIR_BENCH_DEADLINE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// Returns the scenario name with the active deadline suffix embedded,
/// e.g. `"single_thread_buffered_d1ms"`.
fn bench_tag(base: &str) -> String {
    format!("{base}_d{}ms", bench_deadline_ms())
}

// ── LoadHandle ─────────────────────────────────────────────────────────────
//
// Lighter than system.rs's ServerHandle: no crash-recovery plumbing, no
// metrics scraping. `batch_deadline_ms` is read from `WEIR_BENCH_DEADLINE`
// so the same binary can be driven at different deadline values by CI.

struct LoadHandle {
    child: Option<Child>,
    pub socket_path: PathBuf,
    pub deadline_ms: u64,
    tmp_dir: PathBuf,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl LoadHandle {
    fn start(tag: &str) -> Self {
        Self::start_impl(tag, 256)
    }

    /// Start with a deliberately low `max_connections` cap so the ramp test
    /// can exercise connection-drop behaviour without needing hundreds of threads.
    fn start_capped(tag: &str, max_connections: usize) -> Self {
        Self::start_impl(tag, max_connections)
    }

    fn start_impl(tag: &str, max_connections: usize) -> Self {
        let _lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
        let deadline_ms = bench_deadline_ms();
        let metrics_port = free_port();
        let tmp_dir =
            std::env::temp_dir().join(format!("weir_load_{}_{}", tag, std::process::id()));
        let wab_dir = tmp_dir.join("wab");
        let socket_dir = tmp_dir.join("run");
        let socket_path = socket_dir.join("weir.sock");
        let config_path = tmp_dir.join("weir.toml");
        let log_path = tmp_dir.join("weir.log");

        std::fs::create_dir_all(&wab_dir).unwrap();
        std::fs::create_dir_all(&socket_dir).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wab_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }

        let config = format!(
            "[server]\n\
             socket_path       = \"{}\"\n\
             wab_dir           = \"{}\"\n\
             metrics_port      = {}\n\
             shard_count       = 4\n\
             worker_count      = 4\n\
             batch_size        = 64\n\
             batch_deadline_ms = {}\n\
             max_connections   = {}\n\
             log_level         = \"error\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
            deadline_ms,
            max_connections,
        );
        std::fs::write(&config_path, &config).unwrap();

        let log_file = std::fs::File::create(&log_path).unwrap();
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
            deadline_ms,
            tmp_dir,
            _lock,
        };
        handle.wait_ready(Duration::from_secs(15));
        handle
    }

    fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if UnixStream::connect(&self.socket_path).is_ok() {
                return;
            }
            if let Some(ref mut child) = self.child {
                if let Ok(Some(status)) = child.try_wait() {
                    let log =
                        std::fs::read_to_string(self.tmp_dir.join("weir.log")).unwrap_or_default();
                    panic!("weir-server exited early ({status})\nlog:\n{log}");
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        let log = std::fs::read_to_string(self.tmp_dir.join("weir.log")).unwrap_or_default();
        panic!(
            "weir-server not ready within {timeout:?}: {}\nlog:\n{log}",
            self.socket_path.display()
        );
    }

    fn client(&self) -> WeirClient {
        WeirClient::connect(&self.socket_path).expect("connect")
    }
}

impl Drop for LoadHandle {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

// ── Output helpers ─────────────────────────────────────────────────────────

fn emit_throughput(scenario: &str, threads: usize, total_records: usize, elapsed: Duration) {
    let rps = total_records as f64 / elapsed.as_secs_f64();
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"threads\":{threads},\
         \"total_records\":{total_records},\
         \"wall_ms\":{},\"throughput_rps\":{}}}",
        elapsed.as_millis(),
        rps as u64,
    );
}

fn emit_latency(scenario: &str, samples: usize, sorted_us: &[u64]) {
    let p = |pct: f64| -> u64 {
        let idx = ((samples as f64 * pct / 100.0) as usize).min(samples - 1);
        sorted_us[idx]
    };
    let mean = sorted_us.iter().sum::<u64>() / samples as u64;
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"samples\":{samples},\
         \"mean_us\":{mean},\"p50_us\":{},\"p95_us\":{},\
         \"p99_us\":{},\"p999_us\":{},\"max_us\":{}}}",
        p(50.0),
        p(95.0),
        p(99.0),
        p(99.9),
        sorted_us.last().copied().unwrap_or(0),
    );
}

// ── Thundering-herd helper ─────────────────────────────────────────────────
//
// All N threads connect first, then synchronise on a barrier so their first
// push() calls hit the server at the same instant.

fn thundering_herd(srv: &LoadHandle, n_threads: usize, records_per_thread: usize) -> Duration {
    let barrier = Arc::new(Barrier::new(n_threads + 1));
    let socket = srv.socket_path.clone();

    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let b = Arc::clone(&barrier);
            let path = socket.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path).expect("connect");
                b.wait();
                for _ in 0..records_per_thread {
                    client.push(b"bench", Durability::Sync).expect("push");
                }
            })
        })
        .collect();

    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("thread panicked");
    }
    t0.elapsed()
}

// ── Ramp-to-saturation helper ──────────────────────────────────────────────

struct LevelResult {
    acks: u64,
    nacks: u64,
    io_errors: u64,
    duration: Duration,
}

fn run_ramp_level(srv: &LoadHandle, n_threads: usize, duration: Duration) -> LevelResult {
    let acks = Arc::new(AtomicU64::new(0));
    let nacks = Arc::new(AtomicU64::new(0));
    let io_errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(n_threads + 1));

    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let acks = Arc::clone(&acks);
            let nacks = Arc::clone(&nacks);
            let io_errs = Arc::clone(&io_errors);
            let stop = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let path = srv.socket_path.clone();

            thread::spawn(move || {
                let conn = WeirClient::connect(&path);
                b.wait();
                let mut client = match conn {
                    Ok(c) => c,
                    Err(_) => {
                        io_errs.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };
                while !stop.load(Ordering::Relaxed) {
                    match client.push(b"ramp", Durability::Buffered) {
                        Ok(()) => {
                            acks.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ClientError::Io(_)) => {
                            io_errs.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => {
                            nacks.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    barrier.wait();
    let t0 = Instant::now();
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    LevelResult {
        acks: acks.load(Ordering::Relaxed),
        nacks: nacks.load(Ordering::Relaxed),
        io_errors: io_errors.load(Ordering::Relaxed),
        duration: t0.elapsed(),
    }
}

// ── Load tests ─────────────────────────────────────────────────────────────

/// Baseline: single producer, Buffered (no fsync wait).
#[test]
fn baseline_single_thread_throughput_buffered() {
    const RECORDS: usize = 500;
    let srv = LoadHandle::start("single_buffered");
    let mut client = srv.client();

    let t0 = Instant::now();
    for _ in 0..RECORDS {
        client.push(b"bench", Durability::Buffered).expect("push");
    }
    let elapsed = t0.elapsed();

    emit_throughput(&bench_tag("single_thread_buffered"), 1, RECORDS, elapsed);
}

/// Baseline: single producer, Sync (fsync per batch).
#[test]
fn baseline_single_thread_throughput_sync() {
    const RECORDS: usize = 300;
    let srv = LoadHandle::start("single_sync");
    let mut client = srv.client();

    let t0 = Instant::now();
    for _ in 0..RECORDS {
        client.push(b"bench", Durability::Sync).expect("push");
    }
    let elapsed = t0.elapsed();

    emit_throughput(&bench_tag("single_thread_sync"), 1, RECORDS, elapsed);
}

/// Latency percentiles: single Sync producer, every push timed individually.
#[test]
fn baseline_latency_percentiles_sync() {
    const SAMPLES: usize = 300;
    let srv = LoadHandle::start("latency_sync");
    let mut client = srv.client();

    let mut us: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t = Instant::now();
        client.push(b"lat", Durability::Sync).expect("push");
        us.push(t.elapsed().as_micros() as u64);
    }
    us.sort_unstable();

    emit_latency(&bench_tag("latency_sync"), SAMPLES, &us);
}

/// Thundering herd — 8 threads.
#[test]
fn thundering_herd_8_threads() {
    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 200;
    let srv = LoadHandle::start("herd_8");
    let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
    emit_throughput(
        &bench_tag("thundering_herd_8_threads"),
        THREADS,
        THREADS * RECORDS_PER_THREAD,
        elapsed,
    );
}

/// Thundering herd — 32 threads.
#[test]
fn thundering_herd_32_threads() {
    const THREADS: usize = 32;
    const RECORDS_PER_THREAD: usize = 100;
    let srv = LoadHandle::start("herd_32");
    let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
    emit_throughput(
        &bench_tag("thundering_herd_32_threads"),
        THREADS,
        THREADS * RECORDS_PER_THREAD,
        elapsed,
    );
}

/// Thundering herd — 64 threads.
#[test]
fn thundering_herd_64_threads() {
    const THREADS: usize = 64;
    const RECORDS_PER_THREAD: usize = 50;
    let srv = LoadHandle::start("herd_64");
    let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
    emit_throughput(
        &bench_tag("thundering_herd_64_threads"),
        THREADS,
        THREADS * RECORDS_PER_THREAD,
        elapsed,
    );
}

/// Connection churn: repeated connect → push → disconnect cycles.
#[test]
fn connection_churn() {
    const ROUNDS: usize = 100;
    let srv = LoadHandle::start("conn_churn");

    let t0 = Instant::now();
    for _ in 0..ROUNDS {
        let mut client = srv.client();
        client.push(b"churn", Durability::Buffered).expect("push");
    }
    let elapsed = t0.elapsed();

    let cps = ROUNDS as f64 / elapsed.as_secs_f64();
    let scenario = bench_tag("connection_churn");
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"threads\":1,\
         \"total_records\":{ROUNDS},\
         \"wall_ms\":{},\"throughput_rps\":{}}}",
        elapsed.as_millis(),
        cps as u64,
    );
}

/// Ramp-to-saturation: DoS resistance verification.
///
/// Starts a server with `max_connections = 48` then escalates through thread
/// levels [8, 16, 32, 48, 64, 96]. Above the cap the server drops excess
/// connections gracefully (client sees an I/O error) while continuing to
/// serve the allowed connections. The health-check assertion after every
/// level proves the server did not crash.
#[test]
fn ramp_to_saturation() {
    const MAX_CONN: usize = 48;
    const LEVEL_DURATION: Duration = Duration::from_secs(3);
    const LEVELS: &[usize] = &[8, 16, 32, 48, 64, 96];

    let srv = LoadHandle::start_capped("ramp", MAX_CONN);
    let d = srv.deadline_ms;

    println!(
        "\n{:<10} {:>10} {:>10} {:>8} {:>8} {:>12}",
        "threads", "acks", "RPS", "nacks", "io_errs", "status"
    );
    println!("{}", "-".repeat(62));

    for &n in LEVELS {
        let result = run_ramp_level(&srv, n, LEVEL_DURATION);
        let rps = result.acks as f64 / result.duration.as_secs_f64();
        let status = if result.io_errors > 0 {
            "SATURATED"
        } else {
            "ok"
        };

        println!(
            "{:<10} {:>10} {:>10.0} {:>8} {:>8} {:>12}",
            n, result.acks, rps, result.nacks, result.io_errors, status
        );

        println!(
            "BENCH: {{\"scenario\":\"ramp_{n}_threads_d{d}ms\",\"threads\":{n},\
             \"acks\":{},\"nacks\":{},\"io_errors\":{},\
             \"wall_ms\":{},\"throughput_rps\":{}}}",
            result.acks,
            result.nacks,
            result.io_errors,
            result.duration.as_millis(),
            rps as u64,
        );

        let mut health = srv.client();
        assert!(
            health.health_check().is_ok(),
            "server became unresponsive after {n}-thread level (status: {status})"
        );
    }
}

/// Fire-and-forget overload: packets arriving faster than the server can drain them.
///
/// N threads each open a raw `UnixStream` and write properly-encoded Push frames
/// as fast as the kernel socket buffer allows — without ever reading the ack.
/// The server's internal queue fills, backpressure propagates through the socket
/// send buffer, and writers eventually block or get reset.
///
/// The critical assertion is that after the blast the server is still alive and
/// responsive to new connections, proving it doesn't crash or deadlock under
/// queue saturation.
#[test]
fn fire_and_forget_overload() {
    const THREADS: usize = 32;
    const BLAST_DURATION: Duration = Duration::from_secs(5);

    let srv = LoadHandle::start("fire_forget");
    let d = srv.deadline_ms;

    let frames_sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Pre-encode one Push frame to reuse across all writes.
    let payload: Vec<u8> = b"overload"[..].to_vec();
    let header = Header::new(
        MessageType::Push,
        Durability::Buffered,
        0,
        payload.len() as u32,
    );
    let frame = Envelope::new(header, payload).encode();

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let sent = Arc::clone(&frames_sent);
            let stop = Arc::clone(&stop);
            let frame = frame.clone();
            let path = srv.socket_path.clone();

            thread::spawn(move || {
                let mut stream = match UnixStream::connect(&path) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                // Short write timeout: don't block the thread indefinitely when
                // the kernel send buffer is full; treat it as backpressure and exit.
                let _ = stream.set_write_timeout(Some(Duration::from_millis(50)));

                while !stop.load(Ordering::Relaxed) {
                    match stream.write_all(&frame) {
                        Ok(()) => {
                            sent.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            })
        })
        .collect();

    thread::sleep(BLAST_DURATION);
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let total = frames_sent.load(Ordering::Relaxed);
    let rps = total as f64 / BLAST_DURATION.as_secs_f64();

    println!(
        "BENCH: {{\"scenario\":\"fire_and_forget_overload_d{d}ms\",\"threads\":{THREADS},\
         \"total_records\":{total},\
         \"wall_ms\":{},\"throughput_rps\":{}}}",
        BLAST_DURATION.as_millis(),
        rps as u64,
    );

    // Server must still be alive and accepting new connections after the blast.
    let mut health = srv.client();
    assert!(
        health.health_check().is_ok(),
        "server became unresponsive after fire-and-forget overload ({total} frames sent)"
    );
}
