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
    /// Inspect the on-disk WAB: active/sealed/confirmed segments + bytes per shard.
    Segments {
        /// Path to the daemon's WAB directory (the `wab_dir` config value).
        #[arg(long)]
        wab_dir: PathBuf,
    },
    /// Inspect and manage the dead-letter store.
    #[command(subcommand)]
    Dl(DlCommand),
}

/// Subcommands under `weir-ctl dl`.
#[derive(Subcommand)]
enum DlCommand {
    /// List dead-letter segments (count + bytes).
    List {
        /// Path to the daemon's WAB directory.
        #[arg(long)]
        wab_dir: PathBuf,
    },
    /// Delete ALL dead-letter segments. Irreversible — defaults to a dry run.
    Drop {
        /// Path to the daemon's WAB directory.
        #[arg(long)]
        wab_dir: PathBuf,
        /// Actually delete. Without this flag, prints what would be deleted.
        #[arg(long)]
        yes: bool,
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
        Command::Segments { wab_dir } => cmd_segments(&wab_dir),
        Command::Dl(dl) => match dl {
            DlCommand::List { wab_dir } => cmd_dl_list(&wab_dir),
            DlCommand::Drop { wab_dir, yes } => cmd_dl_drop(&wab_dir, yes),
        },
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

/// On-disk segment accounting for one shard directory.
struct ShardStat {
    name: String,
    active: u64,
    sealed: u64,
    confirmed: u64,
    bytes: u64,
}

fn cmd_segments(wab_dir: &Path) -> Result<(), String> {
    let entries =
        std::fs::read_dir(wab_dir).map_err(|e| format!("read {}: {e}", wab_dir.display()))?;

    let mut shards: Vec<ShardStat> = Vec::new();
    let mut dl_files: u64 = 0;
    let mut dl_bytes: u64 = 0;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();

        // The dead-letter store is a sibling of the shard dirs, not a shard.
        if name == "dead_letter" {
            if let Ok(files) = std::fs::read_dir(&path) {
                for f in files.flatten() {
                    if f.path().is_file() {
                        dl_files += 1;
                        dl_bytes += f.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
            continue;
        }

        let mut st = ShardStat {
            name,
            active: 0,
            sealed: 0,
            confirmed: 0,
            bytes: 0,
        };
        if let Ok(files) = std::fs::read_dir(&path) {
            for f in files.flatten() {
                let fp = f.path();
                let Some(fname) = fp.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let sz = f.metadata().map(|m| m.len()).unwrap_or(0);
                // Order matters: `.wab.confirmed` and `.wab.sealed` both end in
                // a longer suffix than the bare `.wab`, so test them first.
                if fname.ends_with(".wab.confirmed") {
                    st.confirmed += 1;
                } else if fname.ends_with(".wab.sealed") {
                    st.sealed += 1;
                    st.bytes += sz;
                } else if fname.ends_with(".wab") {
                    st.active += 1;
                    st.bytes += sz;
                }
            }
        }
        shards.push(st);
    }

    if shards.is_empty() && dl_files == 0 {
        println!("no shard directories under {}", wab_dir.display());
        return Ok(());
    }

    shards.sort_by(|a, b| a.name.cmp(&b.name));
    println!(
        "{:<8} {:>7} {:>7} {:>10} {:>12}",
        "shard", "active", "sealed", "confirmed", "bytes"
    );
    let (mut ta, mut ts, mut tc, mut tb) = (0u64, 0u64, 0u64, 0u64);
    for s in &shards {
        println!(
            "{:<8} {:>7} {:>7} {:>10} {:>12}",
            s.name,
            s.active,
            s.sealed,
            s.confirmed,
            fmt_bytes(s.bytes)
        );
        ta += s.active;
        ts += s.sealed;
        tc += s.confirmed;
        tb += s.bytes;
    }
    println!(
        "{:<8} {:>7} {:>7} {:>10} {:>12}",
        "total",
        ta,
        ts,
        tc,
        fmt_bytes(tb)
    );
    println!("(active = being written; sealed = awaiting drain; confirmed = drained marker)");
    if dl_files > 0 {
        println!("dead-letter: {dl_files} file(s), {}", fmt_bytes(dl_bytes));
    }
    Ok(())
}

fn fmt_bytes(b: u64) -> String {
    const K: f64 = 1024.0;
    let f = b as f64;
    if f >= K * K * K {
        format!("{:.1} GiB", f / (K * K * K))
    } else if f >= K * K {
        format!("{:.1} MiB", f / (K * K))
    } else if f >= K {
        format!("{:.1} KiB", f / K)
    } else {
        format!("{b} B")
    }
}

// ── Dead-letter (`dl`) ──────────────────────────────────────────────────────────

fn dead_letter_dir(wab_dir: &Path) -> PathBuf {
    wab_dir.join("dead_letter")
}

/// Returns `(path, size)` for every `dl_*.wab` segment in the dead-letter dir,
/// sorted by name. A missing dead-letter directory is treated as empty.
fn dl_segments(dl_dir: &Path) -> Result<Vec<(PathBuf, u64)>, String> {
    let entries = match std::fs::read_dir(dl_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", dl_dir.display())),
    };
    let mut out = Vec::new();
    for f in entries.flatten() {
        let p = f.path();
        let is_dl = p
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("dl_") && n.ends_with(".wab"));
        if p.is_file() && is_dl {
            let sz = f.metadata().map(|m| m.len()).unwrap_or(0);
            out.push((p, sz));
        }
    }
    out.sort();
    Ok(out)
}

fn cmd_dl_list(wab_dir: &Path) -> Result<(), String> {
    let dl_dir = dead_letter_dir(wab_dir);
    let segs = dl_segments(&dl_dir)?;
    if segs.is_empty() {
        println!("dead-letter store is empty ({})", dl_dir.display());
        return Ok(());
    }
    println!("{:<26} {:>12}", "segment", "bytes");
    let mut total = 0u64;
    for (p, sz) in &segs {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        println!("{name:<26} {:>12}", fmt_bytes(*sz));
        total += sz;
    }
    println!(
        "{:<26} {:>12}",
        format!("total ({})", segs.len()),
        fmt_bytes(total)
    );
    Ok(())
}

fn cmd_dl_drop(wab_dir: &Path, yes: bool) -> Result<(), String> {
    let dl_dir = dead_letter_dir(wab_dir);
    let segs = dl_segments(&dl_dir)?;
    if segs.is_empty() {
        println!("dead-letter store is empty; nothing to drop");
        return Ok(());
    }
    let total: u64 = segs.iter().map(|(_, s)| *s).sum();
    if !yes {
        println!(
            "would delete {} dead-letter segment(s) ({}) under {}",
            segs.len(),
            fmt_bytes(total),
            dl_dir.display()
        );
        println!("re-run with --yes to confirm — this is irreversible.");
        return Ok(());
    }
    for (p, _) in &segs {
        std::fs::remove_file(p).map_err(|e| format!("remove {}: {e}", p.display()))?;
    }
    println!(
        "dropped {} dead-letter segment(s) ({})",
        segs.len(),
        fmt_bytes(total)
    );
    println!("note: if the daemon is running, restart it so its dead-letter accounting refreshes.");
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
