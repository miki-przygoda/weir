//! weir-bench — benchmark harness for the weir daemon.
//!
//! Connects to an already-running `weir-server` over its Unix socket and runs
//! configurable throughput and latency scenarios, emitting results in the same
//! BENCH JSON format used by `tests/load.rs` so output files can be merged and
//! compared across runs.
//!
//! # Usage
//!
//! ```text
//! weir-bench --socket /run/weir/weir.sock [OPTIONS]
//!
//! Options:
//!   --socket PATH       Unix socket path (required)
//!   --samples N         Records per latency/throughput scenario [default: 10000]
//!   --payload N         Payload size in bytes [default: 256]
//!   --deadline-ms N     Deadline tag embedded in scenario names
//!                       [env: WEIR_BENCH_DEADLINE; default: 1]
//!   --output PATH       Append JSONL result lines to PATH (creates if absent)
//!   --only WHAT         Run only: latency | throughput | herd | churn
//!                       [default: all]
//!   --help              Print this message
//! ```

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use weir_core::Durability;

// ── Usage ─────────────────────────────────────────────────────────────────────

const USAGE: &str = "\
Usage: weir-bench --socket PATH [OPTIONS]

Options:
  --socket PATH       Unix socket path (required)
  --samples N         Records per latency/throughput scenario [default: 10000]
  --payload N         Payload size in bytes [default: 256]
  --deadline-ms N     Deadline tag in scenario names
                      [env: WEIR_BENCH_DEADLINE; default: 1]
  --output PATH       Append JSONL result lines to PATH (creates if absent)
  --only WHAT         Run only: latency | throughput | herd | churn
                      [default: all]
  --help              Print this message
";

// ── Argument parsing ──────────────────────────────────────────────────────────

struct Args {
    socket: PathBuf,
    samples: usize,
    payload_size: usize,
    deadline_ms: u64,
    output: Option<PathBuf>,
    only: String,
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut pargs = pico_args::Arguments::from_env();

    if pargs.contains("--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }

    let deadline_ms = pargs
        .opt_value_from_str("--deadline-ms")?
        .or_else(|| {
            std::env::var("WEIR_BENCH_DEADLINE")
                .ok()
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(1u64);

    let only: String = pargs
        .opt_value_from_str("--only")?
        .unwrap_or_else(|| "all".into());

    Ok(Args {
        socket: pargs.value_from_str("--socket")?,
        samples: pargs.opt_value_from_str("--samples")?.unwrap_or(10_000),
        payload_size: pargs.opt_value_from_str("--payload")?.unwrap_or(256),
        deadline_ms,
        output: pargs.opt_value_from_str("--output")?,
        only,
    })
}

// ── Output helpers ────────────────────────────────────────────────────────────

fn bench_tag(base: &str, deadline_ms: u64) -> String {
    format!("{base}_d{deadline_ms}ms")
}

fn p_us(sorted: &[u64], pct: f64) -> u64 {
    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    let idx = ((n as f64 * pct / 100.0) as usize).min(n - 1);
    sorted[idx]
}

fn emit_latency(scenario: &str, sorted_us: &[u64], out: &mut Option<fs::File>) {
    let n = sorted_us.len();
    if n == 0 {
        return;
    }
    let mean = sorted_us.iter().sum::<u64>() / n as u64;
    let min = sorted_us[0];
    let max = sorted_us[n - 1];
    let line = format!(
        "{{\"scenario\":\"{scenario}\",\"samples\":{n},\
         \"min_us\":{min},\"mean_us\":{mean},\
         \"p50_us\":{},\"p75_us\":{},\"p95_us\":{},\
         \"p99_us\":{},\"p999_us\":{},\"max_us\":{}}}",
        p_us(sorted_us, 50.0),
        p_us(sorted_us, 75.0),
        p_us(sorted_us, 95.0),
        p_us(sorted_us, 99.0),
        p_us(sorted_us, 99.9),
        max,
    );
    println!("BENCH: {line}");
    if let Some(f) = out {
        writeln!(f, "{line}").ok();
    }
}

fn emit_throughput(
    scenario: &str,
    threads: usize,
    total: usize,
    elapsed: Duration,
    out: &mut Option<fs::File>,
) {
    let rps = total as f64 / elapsed.as_secs_f64();
    let line = format!(
        "{{\"scenario\":\"{scenario}\",\"threads\":{threads},\
         \"total_records\":{total},\
         \"wall_ms\":{},\"throughput_rps\":{}}}",
        elapsed.as_millis(),
        rps as u64,
    );
    println!("BENCH: {line}");
    if let Some(f) = out {
        writeln!(f, "{line}").ok();
    }
}

// ── Benchmark runners (Unix only) ─────────────────────────────────────────────

#[cfg(unix)]
use weir_client::WeirClient;

#[cfg(unix)]
fn run_latency(socket: &Path, durability: Durability, payload: &[u8], samples: usize) -> Vec<u64> {
    let mut client = WeirClient::connect(socket).unwrap_or_else(|e| {
        eprintln!("error: cannot connect to {}: {e}", socket.display());
        std::process::exit(1);
    });
    let mut us = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t = Instant::now();
        client.push(payload, durability).expect("push failed");
        us.push(t.elapsed().as_micros() as u64);
    }
    us.sort_unstable();
    us
}

#[cfg(unix)]
fn run_throughput_single(
    socket: &Path,
    durability: Durability,
    payload: &[u8],
    records: usize,
) -> Duration {
    let mut client = WeirClient::connect(socket).unwrap_or_else(|e| {
        eprintln!("error: cannot connect to {}: {e}", socket.display());
        std::process::exit(1);
    });
    let t0 = Instant::now();
    for _ in 0..records {
        client.push(payload, durability).expect("push failed");
    }
    t0.elapsed()
}

#[cfg(unix)]
fn run_herd(socket: &Path, threads: usize, rpt: usize, payload: Arc<Vec<u8>>) -> Duration {
    // All threads connect first, then synchronise on a barrier so their first
    // push() calls hit the server at the same instant.
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let b = Arc::clone(&barrier);
            let s = socket.to_path_buf();
            let p = Arc::clone(&payload);
            thread::spawn(move || {
                let mut client = WeirClient::connect(&s).unwrap_or_else(|e| {
                    eprintln!("error: cannot connect to {}: {e}", s.display());
                    std::process::exit(1);
                });
                b.wait();
                for _ in 0..rpt {
                    client.push(p.as_slice(), Durability::Buffered).expect("push failed");
                }
            })
        })
        .collect();
    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("herd thread panicked");
    }
    t0.elapsed()
}

#[cfg(unix)]
fn run_churn(socket: &Path, rounds: usize, payload: &[u8]) -> Duration {
    let t0 = Instant::now();
    for _ in 0..rounds {
        let mut client = WeirClient::connect(socket).unwrap_or_else(|e| {
            eprintln!("error: cannot connect to {}: {e}", socket.display());
            std::process::exit(1);
        });
        client.push(payload, Durability::Buffered).expect("push failed");
    }
    t0.elapsed()
}

// ── Orchestration ─────────────────────────────────────────────────────────────

#[cfg(unix)]
fn run_bench(args: Args) {
    let only = args.only.as_str();
    let valid = ["all", "latency", "throughput", "herd", "churn"];
    if !valid.contains(&only) {
        eprintln!("error: --only must be one of: latency, throughput, herd, churn\n\n{USAGE}");
        std::process::exit(1);
    }

    let mut out: Option<fs::File> = args.output.as_deref().map(|p| {
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .unwrap_or_else(|e| {
                eprintln!("error: cannot open output file {}: {e}", p.display());
                std::process::exit(1);
            })
    });

    let payload = vec![0u8; args.payload_size];
    let payload_arc = Arc::new(payload.clone());
    let d = args.deadline_ms;

    eprintln!(
        "weir-bench: socket={} samples={} payload={}B deadline={}ms",
        args.socket.display(),
        args.samples,
        args.payload_size,
        args.deadline_ms,
    );

    // ── Latency: single thread, per-push µs timing ────────────────────────────
    if only == "all" || only == "latency" {
        for (durability, name) in [
            (Durability::Sync, "sync"),
            (Durability::Batched, "batched"),
            (Durability::Buffered, "buffered"),
        ] {
            eprintln!("  latency/{name} ({} samples)…", args.samples);
            let sorted = run_latency(&args.socket, durability, &payload, args.samples);
            emit_latency(&bench_tag(&format!("latency_{name}"), d), &sorted, &mut out);
        }
    }

    // ── Throughput: single thread, total RPS ──────────────────────────────────
    if only == "all" || only == "throughput" {
        for (durability, name) in [
            (Durability::Buffered, "buffered"),
            (Durability::Sync, "sync"),
        ] {
            eprintln!("  throughput/{name} ({} records)…", args.samples);
            let elapsed =
                run_throughput_single(&args.socket, durability, &payload, args.samples);
            emit_throughput(
                &bench_tag(&format!("single_thread_{name}"), d),
                1,
                args.samples,
                elapsed,
                &mut out,
            );
        }
    }

    // ── Thundering herd: N threads, Buffered ──────────────────────────────────
    if only == "all" || only == "herd" {
        for &threads in &[8usize, 32, 64] {
            let rpt = (args.samples / threads).max(1);
            eprintln!("  herd/{threads} threads ({rpt} records/thread)…");
            let elapsed =
                run_herd(&args.socket, threads, rpt, Arc::clone(&payload_arc));
            emit_throughput(
                &bench_tag(&format!("thundering_herd_{threads}_threads"), d),
                threads,
                threads * rpt,
                elapsed,
                &mut out,
            );
        }
    }

    // ── Connection churn: new connection per push ─────────────────────────────
    if only == "all" || only == "churn" {
        let rounds = args.samples.min(1_000);
        eprintln!("  churn ({rounds} rounds)…");
        let elapsed = run_churn(&args.socket, rounds, &payload);
        emit_throughput(
            &bench_tag("connection_churn", d),
            1,
            rounds,
            elapsed,
            &mut out,
        );
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            std::process::exit(1);
        }
    };

    #[cfg(not(unix))]
    {
        let _ = args;
        eprintln!("error: weir-bench is only supported on Unix");
        std::process::exit(1);
    }

    #[cfg(unix)]
    run_bench(args);
}
