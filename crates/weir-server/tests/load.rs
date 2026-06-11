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
//! Improvements are tracked by re-running this suite and comparing to the
//! numbers committed in `docs/benchmarks/latest.md` at the time. The
//! [post-v0.4 perf pass](../../../CHANGELOG.md) addressed the original
//! performance-improvement TODO list (end-to-end latency, thundering-herd
//! queue contention, batching efficiency); future improvements are
//! catalogued in the CHANGELOG rather than inline here.

#![cfg(unix)]

use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use weir_client::{ClientError, WeirClient};
use weir_core::{Durability, Envelope, Header, MessageType};
use weir_testkit::weir_server;

// ── Bench-deadline tag ─────────────────────────────────────────────────────

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
    let min = sorted_us.first().copied().unwrap_or(0);
    // Population stddev over sorted_us (integer µs).
    let variance = sorted_us
        .iter()
        .map(|&v| {
            let diff = v as i64 - mean as i64;
            (diff * diff) as u64
        })
        .sum::<u64>()
        / samples as u64;
    let stddev_us = (variance as f64).sqrt() as u64;
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"samples\":{samples},\
         \"min_us\":{min},\"mean_us\":{mean},\"stddev_us\":{stddev_us},\"p50_us\":{},\"p75_us\":{},\
         \"p95_us\":{},\"p99_us\":{},\"p999_us\":{},\"max_us\":{}}}",
        p(50.0),
        p(75.0),
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

fn thundering_herd(
    srv: &weir_testkit::WeirServer,
    n_threads: usize,
    records_per_thread: usize,
) -> Duration {
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

fn run_ramp_level(
    srv: &weir_testkit::WeirServer,
    n_threads: usize,
    duration: Duration,
    durability: Durability,
) -> LevelResult {
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
                let Ok(mut client) = conn else {
                    io_errs.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                while !stop.load(Ordering::Relaxed) {
                    match client.push(b"ramp", durability) {
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
    const RECORDS: usize = 1_000;
    let srv = weir_server!("single_buffered").bench_preset().start();
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
    const RECORDS: usize = 500;
    let srv = weir_server!("single_sync").bench_preset().start();
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
    const SAMPLES: usize = 2000;
    let srv = weir_server!("latency_sync").bench_preset().start();
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

/// Latency percentiles: single Batched producer.
#[test]
fn baseline_latency_percentiles_batched() {
    const SAMPLES: usize = 2000;
    let srv = weir_server!("latency_batched").bench_preset().start();
    let mut client = srv.client();

    let mut us: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t = Instant::now();
        client.push(b"lat", Durability::Batched).expect("push");
        us.push(t.elapsed().as_micros() as u64);
    }
    us.sort_unstable();

    emit_latency(&bench_tag("latency_batched"), SAMPLES, &us);
}

/// Latency percentiles: single Buffered producer.
#[test]
fn baseline_latency_percentiles_buffered() {
    const SAMPLES: usize = 2000;
    let srv = weir_server!("latency_buffered").bench_preset().start();
    let mut client = srv.client();

    let mut us: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t = Instant::now();
        client.push(b"lat", Durability::Buffered).expect("push");
        us.push(t.elapsed().as_micros() as u64);
    }
    us.sort_unstable();

    emit_latency(&bench_tag("latency_buffered"), SAMPLES, &us);
}

/// Thundering herd — 8 threads.
#[test]
fn thundering_herd_8_threads() {
    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 200;
    let srv = weir_server!("herd_8").bench_preset().start();
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
    let srv = weir_server!("herd_32").bench_preset().start();
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
    let srv = weir_server!("herd_64").bench_preset().start();
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
    let srv = weir_server!("conn_churn").bench_preset().start();

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

    let srv = weir_server!("ramp")
        .bench_preset()
        .max_connections(MAX_CONN)
        .start();
    let d = srv.batch_deadline_ms;

    println!(
        "\n{:<10} {:>10} {:>10} {:>8} {:>8} {:>12}",
        "threads", "acks", "RPS", "nacks", "io_errs", "status"
    );
    println!("{}", "-".repeat(62));

    for &n in LEVELS {
        let result = run_ramp_level(&srv, n, LEVEL_DURATION, Durability::Buffered);
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

/// Sync-tier saturation ramp: same as `ramp_to_saturation` but uses Sync
/// durability to stress the group-fsync path under escalating concurrency.
///
/// Confirms the server survives concurrent Sync producers hitting the fsync
/// bottleneck and that it degrades gracefully once the connection cap is
/// exceeded (same assertion as the Buffered ramp).
#[test]
fn ramp_to_saturation_sync() {
    const MAX_CONN: usize = 48;
    const LEVEL_DURATION: Duration = Duration::from_secs(3);
    const LEVELS: &[usize] = &[8, 16, 32, 48, 64, 96];

    let srv = weir_server!("ramp_sync")
        .bench_preset()
        .max_connections(MAX_CONN)
        .start();
    let d = srv.batch_deadline_ms;

    println!(
        "\n{:<10} {:>10} {:>10} {:>8} {:>8} {:>12}",
        "threads", "acks", "RPS", "nacks", "io_errs", "status"
    );
    println!("{}", "-".repeat(62));

    for &n in LEVELS {
        let result = run_ramp_level(&srv, n, LEVEL_DURATION, Durability::Sync);
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
            "BENCH: {{\"scenario\":\"ramp_sync_{n}_threads_d{d}ms\",\"threads\":{n},\
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

    let srv = weir_server!("fire_forget").bench_preset().start();
    let d = srv.batch_deadline_ms;

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
                let Ok(mut stream) = UnixStream::connect(&path) else {
                    return;
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

// ── Compression ratio (records per Sink::commit call) ──────────────────────────
//
// The headline IOPS-compression claim: N client pushes are collapsed into M
// downstream sink commits, where M < N. This scenario configures the daemon
// so segments seal at bench-scale volume, drives push traffic, scrapes the
// Prometheus counters once the drain has worked through everything, and
// emits a single BENCH line with the literal ratio.
//
// Configuration choices:
//   - `wab_segment_max_bytes = 64 KiB`: small enough that ~150 × 256 B
//     records seal one segment (and the test pushes enough to seal several).
//   - `sink_max_batch_size = 10_000`: bigger than any plausible segment so
//     the drain delivers each sealed segment to the sink in one commit().
//
// Property assertion: `records_committed / commit_count ≥ 10`. The number is
// conservative — on a clean run the observed ratio is much higher — but
// resistant to noisy CI runners and the rare case where a flush deadline
// triggers an extra small batch.

#[test]
fn compression_ratio_records_per_commit() {
    const N_RECORDS: usize = 5_000;
    const RECORD_BYTES: usize = 256;
    const SEGMENT_MAX_BYTES: u64 = 64 * 1024;
    const SINK_MAX_BATCH: usize = 10_000;
    const DRAIN_DEADLINE: Duration = Duration::from_secs(20);
    const STABLE_ROUNDS: u32 = 3;

    let srv = weir_server!("compression_ratio")
        .bench_preset()
        .extra_config(format!("wab_segment_max_bytes = {SEGMENT_MAX_BYTES}"))
        .extra_config(format!("sink_max_batch_size   = {SINK_MAX_BATCH}"))
        .start();

    let payload = vec![0xAAu8; RECORD_BYTES];
    let mut client = srv.client();
    for i in 0..N_RECORDS {
        client
            .push(&payload, Durability::Sync)
            .unwrap_or_else(|e| panic!("push {i} failed: {e}"));
    }
    drop(client);

    // Wait for the committed counter to plateau. We don't require all
    // N_RECORDS to be committed — the last segment is typically still active
    // (and therefore not yet seen by the drain) when the test stops pushing.
    // The compression-ratio measurement is meaningful as long as we've
    // committed most records and the counter has stabilised.
    let mut prev_committed = u64::MAX;
    let mut stable = 0u32;
    let deadline = Instant::now() + DRAIN_DEADLINE;
    let (committed, commit_count) = loop {
        thread::sleep(Duration::from_millis(200));
        let body = srv.scrape_metrics();
        let committed = parse_load_metric(
            &body,
            "weir_sink_commit_records_total{outcome=\"committed\"}",
        );
        let commit_count = parse_load_metric(&body, "weir_sink_commit_duration_seconds_count");

        if committed == prev_committed && commit_count > 0 {
            stable += 1;
            if stable >= STABLE_ROUNDS {
                break (committed, commit_count);
            }
        } else {
            stable = 0;
            prev_committed = committed;
        }
        if Instant::now() >= deadline {
            panic!(
                "drain did not reach a stable committed count within {DRAIN_DEADLINE:?}; \
                 last seen: {committed} committed in {commit_count} commits"
            );
        }
    };

    // Sanity floor: we should have committed the bulk of what we pushed.
    // Anything less than half points at a broken drain, not a healthy plateau.
    assert!(
        committed >= (N_RECORDS as u64) / 2,
        "expected ≥{} committed records, only saw {committed} — drain is not flowing",
        N_RECORDS / 2,
    );

    let ratio = committed as f64 / commit_count.max(1) as f64;
    let d = srv.batch_deadline_ms;
    println!(
        "BENCH: {{\"scenario\":\"compression_ratio_d{d}ms\",\"records_committed\":{committed},\
         \"sink_commits\":{commit_count},\"records_per_commit\":{ratio:.2},\
         \"segment_max_bytes\":{SEGMENT_MAX_BYTES},\"record_bytes\":{RECORD_BYTES}}}"
    );

    assert!(
        ratio >= 10.0,
        "expected ≥10:1 records-per-commit IOPS compression, got {ratio:.1}:1 \
         ({committed} records / {commit_count} commits at {SEGMENT_MAX_BYTES} B/segment)"
    );

    // Server must still be responsive after the run.
    let mut health = srv.client();
    assert!(
        health.health_check().is_ok(),
        "server became unresponsive after compression-ratio scenario"
    );
}

/// Sweeps `agent_count` (= shard_count = worker_count) across a range and
/// runs the herd_64 workload at each. The peak of the resulting throughput
/// curve tells us the ideal agent:core ratio on the host. Used to derive
/// the startup recommendation surfaced via tracing.
///
/// Each `(agent_count)` is run 3 trials and the median is reported.
/// Ignored by default — this is investigation tooling, not a regression
/// bench. Run explicitly with `--ignored sweep_agent_count_vs_throughput`.
#[test]
#[ignore = "investigation-only; run explicitly with --ignored"]
fn sweep_agent_count_vs_throughput() {
    const THREADS: usize = 64;
    const RECORDS_PER_THREAD: usize = 100;
    const TRIALS: usize = 3;
    let agent_counts = [1usize, 2, 3, 4, 6, 8];

    let cores = num_cpus_avail();
    eprintln!("\n=== agent_count sweep on {} cores ===", cores);
    eprintln!(
        "scenario: herd of {} threads × {} Sync records each",
        THREADS, RECORDS_PER_THREAD
    );
    eprintln!();
    eprintln!("agents | median RPS | min      | max      | agents/cores");
    eprintln!("-------|-----------:|---------:|---------:|-------------");

    for n in agent_counts {
        let mut trials_rps: Vec<u64> = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            // Fresh server per trial so per-shard state doesn't carry over.
            let srv = weir_server!("sweep")
                .bench_preset()
                .shard_count(n)
                .worker_count(n)
                .start();
            let elapsed = thundering_herd(&srv, THREADS, RECORDS_PER_THREAD);
            let rps = ((THREADS * RECORDS_PER_THREAD) as f64 / elapsed.as_secs_f64()) as u64;
            trials_rps.push(rps);
        }
        trials_rps.sort_unstable();
        let median = trials_rps[trials_rps.len() / 2];
        let min = *trials_rps.first().unwrap();
        let max = *trials_rps.last().unwrap();
        let ratio = n as f64 / cores as f64;
        eprintln!(
            "{:>6} | {:>10} | {:>8} | {:>8} | {:.2}",
            n, median, min, max, ratio
        );
    }
    eprintln!();
}

fn num_cpus_avail() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn parse_load_metric(body: &str, prefix: &str) -> u64 {
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
