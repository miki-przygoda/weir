//! Limit-finding experiments — open questions, not gates.
//!
//! Every test here is `#[ignore]`: they run for minutes to hours, they answer
//! questions rather than assert invariants, and several deliberately drive weir
//! past the point where it behaves well. Nothing in CI runs them, and the `lint`
//! job's `clippy --all-targets` is what keeps them compiling.
//!
//! ```sh
//! cargo test -p weir-server --test research --release -- --ignored --nocapture --test-threads=1
//! # or one at a time, which is how they are meant to be run:
//! cargo test -p weir-server --test research --release -- --ignored --nocapture sustained_durable
//! ```
//!
//! `--test-threads=1` is mandatory for the same reason as `tests/load_batch.rs`,
//! and `BENCH:` lines carry a leading newline for the same reason: under
//! `--test-threads=1` libtest leaves the cursor mid-line, so an unprefixed line
//! defeats the project-wide `grep '^BENCH: '`.

#![cfg(unix)]

use std::time::{Duration, Instant};

use weir_core::Durability;
use weir_testkit::weir_server;

fn parse_metric_f64(body: &str, prefix: &str) -> f64 {
    for line in body.lines() {
        if line.starts_with(prefix)
            && let Some(val) = line.split_whitespace().next_back()
            && let Ok(n) = val.parse()
        {
            return n;
        }
    }
    f64::NAN
}

/// Reads an env var as a `u64`, falling back to `default`. Lets a long
/// experiment be shortened for a smoke run without editing the file.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn emit(fields: &str) {
    println!("\nBENCH: {{{fields}}}");
}

/// Applies `WEIR_BENCH_WAB_DIR` if set, the same knob `tests/load_drain.rs`
/// uses, so an experiment can be pointed at a different filesystem.
///
/// This is what makes the sustained experiments falsifiable. They found a
/// strictly periodic oscillation in `Durable` throughput on a SATA SSD -- ~880
/// rec/s at ~1.09 ms fsync alternating with ~179 rec/s at ~5.56 ms on a ~4.5
/// minute cycle -- which is either something weir does or something the storage
/// does. Re-running the identical code against a RAM disk answers that: if the
/// oscillation survives, it is weir; if it disappears, it is the disk.
fn with_wab_dir(b: weir_testkit::WeirServerBuilder, tag: &str) -> weir_testkit::WeirServerBuilder {
    match std::env::var("WEIR_BENCH_WAB_DIR") {
        Ok(d) if !d.is_empty() => {
            let dir = std::path::PathBuf::from(d).join(tag);
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)
                .unwrap_or_else(|e| panic!("WEIR_BENCH_WAB_DIR: create {}: {e}", dir.display()));
            b.wab_dir(dir)
        }
        _ => b,
    }
}

/// Names the storage every result line, because a number from this suite is
/// meaningless without it.
fn wab_backing() -> &'static str {
    if std::env::var_os("WEIR_BENCH_WAB_DIR").is_some() {
        "external"
    } else {
        "default"
    }
}

// ── E2: does sustained Durable load degrade the fsync path? ────────────────

/// Samples one `Durable` producer's throughput and the daemon's own fsync
/// latency, continuously, for `WEIR_RESEARCH_SECS` (default 1800).
///
/// **Why this exists.** Running `tests/load_batch.rs` five times back to back on
/// beast produced 877, 857, 874, 310, 178 rec/s on the unbatched `Durable`
/// control — a 4.9x collapse — while `batch_single_buffered` in the *same* passes
/// held 49,585 / 49,155 / 49,138 / 49,015 / 49,183, a spread of 1.01x. A CPU or
/// scheduler effect would have moved both. Two minutes idle restored 887 rec/s.
/// That points at the fsync path degrading under sustained write load and
/// recovering when rested, which — if real — is the single most important thing
/// a `Durable`-tier user does not currently know, because every published
/// `Durable` figure weir has is a short run taken against a rested disk.
///
/// This test does not assert the effect. It samples the curve so the shape,
/// onset and steady state can be read off, and it reports `Buffered` alongside
/// as the control that isolates fsync from everything else.
#[test]
#[ignore = "runs for 30 minutes by default; operator-run"]
fn sustained_durable_throughput_over_time() {
    let total = Duration::from_secs(env_u64("WEIR_RESEARCH_SECS", 1800));
    let sample = Duration::from_secs(env_u64("WEIR_RESEARCH_SAMPLE_SECS", 30));

    let srv = with_wab_dir(weir_server!("r_sustain").bench_preset(), "r_sustain").start();
    let mut client = srv.client();

    let fsync_mean_us = |body: &str| -> f64 {
        let sum = parse_metric_f64(body, "weir_wab_fsync_duration_seconds_sum");
        let count = parse_metric_f64(body, "weir_wab_fsync_duration_seconds_count");
        sum / count * 1e6
    };

    let started = Instant::now();
    let mut window_start = Instant::now();
    let mut window_records: u64 = 0;
    let mut prev = srv.scrape_metrics();
    let (mut prev_sum, mut prev_count) = (
        parse_metric_f64(&prev, "weir_wab_fsync_duration_seconds_sum"),
        parse_metric_f64(&prev, "weir_wab_fsync_duration_seconds_count"),
    );

    while started.elapsed() < total {
        client
            .push(b"sustained", Durability::Durable)
            .expect("push");
        window_records += 1;

        if window_start.elapsed() >= sample {
            let elapsed = window_start.elapsed();
            let wab = wab_backing();
            prev = srv.scrape_metrics();
            let sum = parse_metric_f64(&prev, "weir_wab_fsync_duration_seconds_sum");
            let count = parse_metric_f64(&prev, "weir_wab_fsync_duration_seconds_count");
            // Windowed mean, not the cumulative one: a cumulative average over a
            // 30-minute run is dominated by whatever happened first and would
            // hide exactly the drift this test is looking for.
            let window_fsync_us = (sum - prev_sum) / (count - prev_count) * 1e6;
            emit(&format!(
                "\"scenario\":\"sustained_durable\",\"wab\":\"{wab}\",\
                 \"elapsed_s\":{:.0},\"rps\":{:.0},\
                 \"fsync_window_us\":{:.1},\"fsync_cumulative_us\":{:.1},\
                 \"wab_bytes\":{:.0},\"wab_segments\":{:.0}",
                started.elapsed().as_secs_f64(),
                window_records as f64 / elapsed.as_secs_f64(),
                window_fsync_us,
                fsync_mean_us(&prev),
                parse_metric_f64(&prev, "weir_wab_bytes_on_disk"),
                parse_metric_f64(&prev, "weir_wab_segments"),
            ));
            (prev_sum, prev_count) = (sum, count);
            window_records = 0;
            window_start = Instant::now();
        }
    }
    srv.shutdown();
}

/// The control for the experiment above: the identical loop at `Buffered`, which
/// acks on the memory write and touches no fsync. If this curve is flat while
/// the `Durable` one decays, the decay is the fsync path and not the machine.
#[test]
#[ignore = "runs for 30 minutes by default; operator-run"]
fn sustained_buffered_throughput_over_time() {
    let total = Duration::from_secs(env_u64("WEIR_RESEARCH_SECS", 1800));
    let sample = Duration::from_secs(env_u64("WEIR_RESEARCH_SAMPLE_SECS", 30));

    let srv = with_wab_dir(weir_server!("r_sustainb").bench_preset(), "r_sustainb").start();
    let mut client = srv.client();

    let started = Instant::now();
    let mut window_start = Instant::now();
    let mut window_records: u64 = 0;

    while started.elapsed() < total {
        client
            .push(b"sustained", Durability::Buffered)
            .expect("push");
        window_records += 1;

        if window_start.elapsed() >= sample {
            let elapsed = window_start.elapsed();
            let wab = wab_backing();
            let body = srv.scrape_metrics();
            emit(&format!(
                "\"scenario\":\"sustained_buffered\",\"wab\":\"{wab}\",\
                 \"elapsed_s\":{:.0},\"rps\":{:.0},\
                 \"wab_bytes\":{:.0},\"wab_segments\":{:.0}",
                started.elapsed().as_secs_f64(),
                window_records as f64 / elapsed.as_secs_f64(),
                parse_metric_f64(&body, "weir_wab_bytes_on_disk"),
                parse_metric_f64(&body, "weir_wab_segments"),
            ));
            window_records = 0;
            window_start = Instant::now();
        }
    }
    srv.shutdown();
}

// ── E5: what do these rates look like in bytes? ────────────────────────────

/// Throughput against payload size, in both rec/s and MB/s.
///
/// Every throughput figure weir publishes uses a 5-byte record, which is a
/// latency probe wearing a throughput costume: it measures per-record overhead
/// with the payload rounded to nothing. A user sizing a deployment has records
/// of hundreds or thousands of bytes and needs to know which of the two numbers
/// binds — the per-record cost or the byte cost. This sweeps far enough to find
/// the crossover.
#[test]
#[ignore = "operator-run; ~10 minutes"]
fn payload_size_sweep() {
    // 5 B is the published figure's payload, kept as the left-hand anchor so
    // this sweep can be tied back to it; 64 KiB is `max_payload_bytes`-scale.
    const SIZES: &[usize] = &[5, 64, 512, 4_096, 32_768, 65_536];
    // Fewer records at large payloads: 10k x 64 KiB is 640 MiB per scenario,
    // which measures the disk filling rather than the daemon.
    let records_for = |size: usize| if size >= 32_768 { 2_000 } else { 10_000 };

    for &size in SIZES {
        let payload = vec![b'x'; size];
        let n = records_for(size);

        // `MAX_PAYLOAD_HARD_CAP` (16 MiB) bounds the whole batch body, not just a
        // record, so a fixed 256-record frame becomes illegal at large payloads:
        // 256 x 64 KiB is exactly 16 MiB before framing overhead. Hold the frame
        // near 4 MiB instead and let the record count fall out of it.
        let batch_n = (4 * 1024 * 1024 / size).clamp(1, 256);

        for (tag, tier, batch) in [
            ("single_durable", Durability::Durable, 1usize),
            ("batch_durable", Durability::Durable, batch_n),
            ("single_buffered", Durability::Buffered, 1),
            ("batch_buffered", Durability::Buffered, batch_n),
        ] {
            let srv = weir_server!("r_pay").bench_preset().batch_size(256).start();
            let mut client = srv.client();

            let t0 = Instant::now();
            if batch == 1 {
                for _ in 0..n {
                    client.push(&payload, tier).expect("push");
                }
            } else {
                let frame: Vec<&[u8]> = vec![payload.as_slice(); batch];
                for _ in 0..(n / batch) {
                    let out = client.push_batch(&frame, tier).expect("push_batch");
                    assert!(out.all_accepted(), "{tag}: batch not fully accepted");
                }
            }
            let elapsed = t0.elapsed();
            let sent = if batch == 1 { n } else { (n / batch) * batch };
            let rps = sent as f64 / elapsed.as_secs_f64();
            emit(&format!(
                "\"scenario\":\"payload_{tag}\",\"payload_bytes\":{size},\
                 \"batch_records\":{batch},\"records\":{sent},\
                 \"wall_ms\":{},\"throughput_rps\":{:.0},\
                 \"throughput_mib_s\":{:.2}",
                elapsed.as_millis(),
                rps,
                rps * size as f64 / (1024.0 * 1024.0),
            ));
            srv.shutdown();
        }
    }
}
