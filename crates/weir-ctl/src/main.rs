//! `weir-ctl` — admin and inspection CLI for the weir daemon.
//!
//! A thin operator tool over the daemon's existing surfaces: the Unix socket
//! (HealthCheck / Push frames, via `weir-client`) and the Prometheus `/metrics`
//! endpoint. No new daemon-side API is required.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use weir_client::WeirClient;
use weir_core::Durability;

/// Default daemon Unix socket. Override with `--socket`.
const DEFAULT_SOCKET: &str = "/run/weir/weir.sock";
/// Default `/metrics` endpoint. Override with `--addr`.
const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9090";

#[derive(Parser)]
#[command(
    name = "weir-ctl",
    version,
    about = "Admin and inspection CLI for the weir daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check that the daemon is alive and answering on its socket.
    Health {
        /// Path to the daemon's Unix socket.
        #[arg(long, default_value = DEFAULT_SOCKET)]
        socket: PathBuf,
    },
    /// Push a single record (debugging / smoke testing).
    Push {
        /// Payload bytes (taken as UTF-8 from the command line).
        payload: String,
        /// Durability tier: sync | batched | buffered.
        #[arg(long, default_value = "batched", value_parser = parse_durability)]
        durability: Durability,
        /// Path to the daemon's Unix socket.
        #[arg(long, default_value = DEFAULT_SOCKET)]
        socket: PathBuf,
    },
    /// Scrape the daemon's Prometheus endpoint and print a health summary.
    Metrics {
        /// host:port of the daemon's `/metrics` endpoint.
        #[arg(long, default_value = DEFAULT_METRICS_ADDR)]
        addr: String,
        /// Print the full raw exposition instead of the summary.
        #[arg(long)]
        raw: bool,
    },
}

fn parse_durability(s: &str) -> Result<Durability, String> {
    match s.to_ascii_lowercase().as_str() {
        "sync" => Ok(Durability::Sync),
        "batched" => Ok(Durability::Batched),
        "buffered" => Ok(Durability::Buffered),
        other => Err(format!(
            "unknown durability {other:?} (expected sync | batched | buffered)"
        )),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Health { socket } => cmd_health(&socket),
        Command::Push {
            payload,
            durability,
            socket,
        } => cmd_push(&socket, payload.as_bytes(), durability),
        Command::Metrics { addr, raw } => cmd_metrics(&addr, raw),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("weir-ctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn cmd_health(socket: &Path) -> Result<(), String> {
    let mut client =
        WeirClient::connect(socket).map_err(|e| format!("connect {}: {e}", socket.display()))?;
    client
        .health_check()
        .map_err(|e| format!("health check failed: {e}"))?;
    println!("OK  daemon healthy at {}", socket.display());
    Ok(())
}

fn cmd_push(socket: &Path, payload: &[u8], durability: Durability) -> Result<(), String> {
    let mut client =
        WeirClient::connect(socket).map_err(|e| format!("connect {}: {e}", socket.display()))?;
    client
        .push(payload, durability)
        .map_err(|e| format!("push failed: {e}"))?;
    println!("ack  {} bytes, {durability:?}", payload.len());
    Ok(())
}

fn cmd_metrics(addr: &str, raw: bool) -> Result<(), String> {
    let body = scrape(addr)?;
    if raw {
        print!("{body}");
        return Ok(());
    }
    print_summary(&body);
    Ok(())
}

/// Minimal HTTP/1.0 GET of `/metrics` — keeps weir-ctl free of an HTTP client
/// dependency (the daemon's metrics server speaks plain HTTP/1.0).
fn scrape(addr: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("set timeout: {e}"))?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .map_err(|e| format!("write GET: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("read /metrics: {e}"))?;
    match response.split_once("\r\n\r\n") {
        Some((_head, body)) => Ok(body.to_string()),
        None => Ok(response),
    }
}

/// Sums every sample whose line starts with `prefix` (handles label sets, e.g.
/// `weir_records_ack_total{tier="sync"} 12`).
fn sum_metric(body: &str, prefix: &str) -> f64 {
    body.lines()
        .filter(|l| l.starts_with(prefix))
        .filter_map(|l| l.split_whitespace().next_back())
        .filter_map(|v| v.parse::<f64>().ok())
        .sum()
}

/// Returns the value of an exact-match metric line (no label set), if present.
fn get_metric(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .find(|l| l.starts_with(name) && l[name.len()..].starts_with(' '))
        .and_then(|l| l.split_whitespace().next_back())
        .and_then(|v| v.parse::<f64>().ok())
}

fn print_summary(body: &str) {
    // Counters are non-negative integers; render them as such (avoids `-0`).
    let accepted = sum_metric(body, "weir_records_accepted_total") as u64;
    let acked = sum_metric(body, "weir_records_ack_total") as u64;
    let nacked = sum_metric(body, "weir_records_nack_total") as u64;

    let fsync_sum = get_metric(body, "weir_wab_fsync_duration_seconds_sum").unwrap_or(0.0);
    let fsync_count = get_metric(body, "weir_wab_fsync_duration_seconds_count").unwrap_or(0.0);
    let fsync_avg_ms = if fsync_count > 0.0 {
        fsync_sum / fsync_count * 1000.0
    } else {
        0.0
    };

    let queue_depth = get_metric(body, "weir_queue_depth").unwrap_or(0.0) as u64;
    let panics = get_metric(body, "weir_wab_flusher_panics_total").unwrap_or(0.0) as u64;
    let fsync_failures = get_metric(body, "weir_wab_fsync_failures_total").unwrap_or(0.0) as u64;
    let dead_letter_bytes = get_metric(body, "weir_dead_letter_bytes_on_disk").unwrap_or(0.0);
    let wab_bytes = get_metric(body, "weir_wab_bytes_on_disk").unwrap_or(0.0);

    // Health flags worth surfacing loudly.
    let sink_health = active_label(body, "weir_sink_health", "state").unwrap_or_else(|| "?".into());

    println!("── weir ──────────────────────────────────");
    println!("ingest    accepted {accepted}  ack {acked}  nack {nacked}");
    println!(
        "durability fsync avg {fsync_avg_ms:.2} ms  wab {:.1} MiB on disk",
        wab_bytes / 1_048_576.0
    );
    println!("queue     depth {queue_depth}");
    println!("sink      health: {sink_health}");
    println!(
        "dead-ltr  {:.1} MiB on disk",
        dead_letter_bytes / 1_048_576.0
    );

    // Loud warnings for the durability hazards.
    if panics > 0 {
        println!("\n⚠ flusher panics: {panics} — a shard is offline until restart");
    }
    if fsync_failures > 0 {
        println!(
            "⚠ fsync failures: {fsync_failures} — DURABILITY HAZARD (data may not be on stable storage)"
        );
    }
    if nacked > 0 {
        println!("ℹ {nacked} records nacked — check producer behaviour / capacity");
    }
}

/// For a gauge-vector family where exactly one label value is 1.0 (e.g.
/// `weir_sink_health{state="healthy"} 1`), returns that active label value.
fn active_label(body: &str, metric: &str, label: &str) -> Option<String> {
    let needle = format!("{metric}{{");
    for line in body.lines() {
        if !line.starts_with(&needle) {
            continue;
        }
        let value: f64 = line.split_whitespace().next_back()?.parse().ok()?;
        if value != 1.0 {
            continue;
        }
        // Extract label="value" for the requested label key.
        let key = format!("{label}=\"");
        if let Some(start) = line.find(&key) {
            let rest = &line[start + key.len()..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}
