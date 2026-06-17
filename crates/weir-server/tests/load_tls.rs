//! TCP + mutual-TLS throughput / latency benchmark (feature = "tls").
//!
//! The plaintext Unix-socket pipeline is benchmarked by `tests/load.rs`. This
//! suite measures the **TCP + mutual-TLS** producer path — the only path where
//! the rustls crypto provider (ring / aws-lc-rs) and the TLS record layer are on
//! the hot loop — so we can quantify the cost of mTLS and catch regressions in
//! it (e.g. a crypto-backend change).
//!
//! Scenarios (each emits a `BENCH:` JSONL line, same format as `tests/load.rs`,
//! so `deploy/avg_benchmarks.py` can consume them):
//!
//! * `tls_handshake_rate` — full mTLS handshakes per second (each connection
//!   does a fresh handshake + one record). This is the asymmetric-crypto +
//!   certificate-verification cost — the metric most sensitive to the crypto
//!   provider.
//! * `tls_throughput_buffered` — sustained `Buffered` pushes over a single
//!   established connection: bulk AES-GCM record throughput, no fsync floor.
//! * `tls_throughput_sync` — sustained `Sync` pushes over TLS: the realistic
//!   durable-over-TLS rate (fsync-bound, so it reflects the pipeline, not the
//!   cipher).
//! * `tls_latency_buffered` — per-push latency percentiles over an established
//!   TLS connection.
//!
//! These are `#[ignore]`d so they do NOT run in the regular `cargo test`
//! (the numbers are meaningless in a debug build, and the mTLS path is already
//! smoke-tested by `tests/tls_listener.rs`). Run them explicitly, in release:
//!
//! ```text
//! cargo test --features tls --test load_tls --release -- --ignored --nocapture
//! ```
//!
//! Numbers are workload- and machine-dependent (and noisy on a loaded host);
//! treat them as relative, not absolute.

#![cfg(feature = "tls")]

use std::time::{Duration, Instant};

use weir_client::{ClientTlsConfig, WeirClient};
use weir_core::Durability;
use weir_testkit::{tls::TlsFixture, weir_server};

// ── Output helpers (same JSONL shape as tests/load.rs) ──────────────────────

fn emit_throughput(scenario: &str, total_records: usize, elapsed: Duration) {
    let rps = total_records as f64 / elapsed.as_secs_f64();
    println!(
        "BENCH: {{\"scenario\":\"{scenario}\",\"threads\":1,\
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

// ── TLS client config bound to a fixture ────────────────────────────────────

/// Builds a `ClientTlsConfig` borrowing the fixture's PEM paths. `server_name`
/// is `"weir-server"` to match the fixture server cert's SAN (see `TlsFixture`).
fn tls_cfg(fx: &TlsFixture) -> ClientTlsConfig<'_> {
    ClientTlsConfig {
        client_cert: &fx.client_cert_path,
        client_key: &fx.client_key_path,
        ca_cert: &fx.ca_cert_path,
        server_name: "weir-server",
        default_durability: None,
    }
}

// ── Scenarios ───────────────────────────────────────────────────────────────

/// Full mTLS handshakes per second: each iteration opens a fresh TCP connection,
/// completes the mutual-TLS handshake (the first `push` drives rustls's deferred
/// handshake), sends one `Buffered` record (no fsync, so the handshake dominates),
/// then drops the connection. Isolates the asymmetric-crypto + cert-verification
/// cost — the figure most sensitive to the rustls provider.
#[test]
#[ignore = "benchmark; run with --features tls --test load_tls --release -- --ignored --nocapture"]
fn tls_handshake_rate() {
    const HANDSHAKES: usize = 1_000;
    let fx = TlsFixture::generate("tlsbench_hs");
    let srv = weir_server!("tlsbench_hs").tls(&fx).bench_preset().start();
    let addr = srv.tcp_addr();

    let t0 = Instant::now();
    for _ in 0..HANDSHAKES {
        let mut client = WeirClient::connect_tls(addr, tls_cfg(&fx)).expect("connect_tls");
        client.push(b"hs", Durability::Buffered).expect("push");
        // client dropped here → connection closes.
    }
    emit_throughput("tls_handshake_rate", HANDSHAKES, t0.elapsed());
}

/// Sustained `Buffered` throughput over one established mTLS connection: bulk
/// AES-GCM record encryption with no fsync floor, so it reflects the TLS record
/// layer + pipeline rather than the disk.
#[test]
#[ignore = "benchmark; run with --features tls --test load_tls --release -- --ignored --nocapture"]
fn tls_throughput_buffered() {
    const WARMUP: usize = 200;
    const RECORDS: usize = 20_000;
    let fx = TlsFixture::generate("tlsbench_tput_buf");
    let srv = weir_server!("tlsbench_tput_buf")
        .tls(&fx)
        .bench_preset()
        .start();
    let mut client = WeirClient::connect_tls(srv.tcp_addr(), tls_cfg(&fx)).expect("connect_tls");

    for _ in 0..WARMUP {
        client.push(b"warm", Durability::Buffered).expect("warmup");
    }
    let t0 = Instant::now();
    for _ in 0..RECORDS {
        client.push(b"bench", Durability::Buffered).expect("push");
    }
    emit_throughput("tls_throughput_buffered", RECORDS, t0.elapsed());
}

/// Sustained `Sync` throughput over one established mTLS connection: the
/// realistic durable-over-TLS rate. fsync-bound, so this is dominated by the
/// pipeline, not the cipher — included for a realistic operating number.
#[test]
#[ignore = "benchmark; run with --features tls --test load_tls --release -- --ignored --nocapture"]
fn tls_throughput_sync() {
    const WARMUP: usize = 50;
    const RECORDS: usize = 5_000;
    let fx = TlsFixture::generate("tlsbench_tput_sync");
    let srv = weir_server!("tlsbench_tput_sync")
        .tls(&fx)
        .bench_preset()
        .start();
    let mut client = WeirClient::connect_tls(srv.tcp_addr(), tls_cfg(&fx)).expect("connect_tls");

    for _ in 0..WARMUP {
        client.push(b"warm", Durability::Sync).expect("warmup");
    }
    let t0 = Instant::now();
    for _ in 0..RECORDS {
        client.push(b"bench", Durability::Sync).expect("push");
    }
    emit_throughput("tls_throughput_sync", RECORDS, t0.elapsed());
}

/// Per-push latency over an established mTLS connection (`Buffered`, so the TLS
/// record round-trip — not fsync — is the dominant cost being measured).
#[test]
#[ignore = "benchmark; run with --features tls --test load_tls --release -- --ignored --nocapture"]
fn tls_latency_buffered() {
    const WARMUP: usize = 200;
    const SAMPLES: usize = 2_000;
    let fx = TlsFixture::generate("tlsbench_lat_buf");
    let srv = weir_server!("tlsbench_lat_buf")
        .tls(&fx)
        .bench_preset()
        .start();
    let mut client = WeirClient::connect_tls(srv.tcp_addr(), tls_cfg(&fx)).expect("connect_tls");

    for _ in 0..WARMUP {
        client.push(b"warm", Durability::Buffered).expect("warmup");
    }
    let mut us: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t = Instant::now();
        client.push(b"lat", Durability::Buffered).expect("push");
        us.push(t.elapsed().as_micros() as u64);
    }
    us.sort_unstable();
    emit_latency("tls_latency_buffered", SAMPLES, &us);
}
