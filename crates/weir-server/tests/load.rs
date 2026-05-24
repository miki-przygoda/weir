//! Concurrency and throughput load tests for weir-server.
//!
//! These run as part of the dedicated `load` CI job (5 iterations, results
//! averaged and written to `docs/benchmarks.md`). They are NOT marked
//! `#[ignore]` — they are the baseline for an ongoing performance improvement
//! effort and must stay green on every push.
//!
//! # Running locally
//!
//! ```sh
//! cargo test -p weir-server --test load -- --nocapture
//! ```
//!
//! # Output format
//!
//! Each test emits one `BENCH: <json>` line to stdout. The CI script
//! (`scripts/avg_benchmarks.py`) collects these across 5 runs, averages them,
//! and rewrites `docs/benchmarks.md`.
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
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier, OnceLock},
    thread,
    time::{Duration, Instant},
};

use weir_client::WeirClient;
use weir_core::Durability;

// ── Process serialiser (same pattern as system.rs) ─────────────────────────

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

// ── LoadHandle ─────────────────────────────────────────────────────────────
//
// Lighter than system.rs's ServerHandle: no crash-recovery plumbing, no
// metrics scraping. Uses a shorter batch_deadline (2 ms) and 4 shards so
// throughput numbers are not dominated by the deadline timer.

struct LoadHandle {
    child: Option<Child>,
    pub socket_path: PathBuf,
    tmp_dir: PathBuf,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl LoadHandle {
    fn start(tag: &str) -> Self {
        let _lock = process_lock().lock().unwrap_or_else(|e| e.into_inner());
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
             batch_deadline_ms = 2\n\
             log_level         = \"error\"\n",
            socket_path.display(),
            wab_dir.display(),
            metrics_port,
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
            tmp_dir,
            _lock,
        };
        handle.wait_ready(Duration::from_secs(15));
        handle
    }

    fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if std::os::unix::net::UnixStream::connect(&self.socket_path).is_ok() {
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

/// Prints a throughput result line that the CI averaging script can parse.
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

/// Prints a latency result line.
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
// push() calls hit the server at the same instant. This is the "same
// nanosecond" burst the user wants to stress-test.

fn thundering_herd(srv: &LoadHandle, n_threads: usize, records_per_thread: usize) -> Duration {
    let barrier = Arc::new(Barrier::new(n_threads + 1));
    let socket = srv.socket_path.clone();

    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let b = Arc::clone(&barrier);
            let path = socket.clone();
            thread::spawn(move || {
                let mut client = WeirClient::connect(&path).expect("connect");
                b.wait(); // wait until all threads are connected
                for _ in 0..records_per_thread {
                    client.push(b"bench", Durability::Sync).expect("push");
                }
            })
        })
        .collect();

    barrier.wait(); // release all threads simultaneously
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("thread panicked");
    }
    t0.elapsed()
}

// ── Load tests ─────────────────────────────────────────────────────────────

/// Baseline: single producer, Buffered (no fsync wait).
/// Establishes the ceiling throughput for a sequential producer.
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

    emit_throughput("single_thread_buffered", 1, RECORDS, elapsed);
}

/// Baseline: single producer, Sync (fsync per batch).
/// Shows the fsync-per-batch cost for sequential workloads.
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

    emit_throughput("single_thread_sync", 1, RECORDS, elapsed);
}

/// Latency percentiles: single Sync producer, every push timed individually.
/// Captures p50 / p95 / p99 / p99.9 / max latency.
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

    emit_latency("latency_sync", SAMPLES, &us);
}

/// Thundering herd — 8 threads.
/// All threads connect, synchronise on a barrier, then push simultaneously.
#[test]
fn thundering_herd_8_threads() {
    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 200;
    let srv = LoadHandle::start("herd_8");
    let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
    emit_throughput(
        "thundering_herd_8_threads",
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
        "thundering_herd_32_threads",
        THREADS,
        THREADS * RECORDS_PER_THREAD,
        elapsed,
    );
}

/// Thundering herd — 64 threads.
/// Maximum concurrency test: 64 simultaneous producers, same-instant burst.
#[test]
fn thundering_herd_64_threads() {
    const THREADS: usize = 64;
    const RECORDS_PER_THREAD: usize = 50;
    let srv = LoadHandle::start("herd_64");
    let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
    emit_throughput(
        "thundering_herd_64_threads",
        THREADS,
        THREADS * RECORDS_PER_THREAD,
        elapsed,
    );
}

/// Connection churn: repeated connect → push → disconnect cycles.
/// Measures the overhead of connection establishment under load.
#[test]
fn connection_churn() {
    const ROUNDS: usize = 100;
    let srv = LoadHandle::start("conn_churn");

    let t0 = Instant::now();
    for _ in 0..ROUNDS {
        let mut client = srv.client();
        client.push(b"churn", Durability::Buffered).expect("push");
        // client drops here, closing the connection
    }
    let elapsed = t0.elapsed();

    // Report as connections/second rather than records/second.
    let cps = ROUNDS as f64 / elapsed.as_secs_f64();
    println!(
        "BENCH: {{\"scenario\":\"connection_churn\",\"threads\":1,\
         \"total_records\":{ROUNDS},\
         \"wall_ms\":{},\"throughput_rps\":{}}}",
        elapsed.as_millis(),
        cps as u64,
    );
}
