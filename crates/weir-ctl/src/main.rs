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
use weir_core::{Durability, Payload};
use weir_wab::SegmentReader;

/// Default daemon Unix socket. Override with `--socket`.
const DEFAULT_SOCKET: &str = "/run/weir/weir.sock";
/// Default `/metrics` endpoint. Override with `--addr`. Matches the daemon's
/// metrics default (config `metrics_port` = 9185); a mismatch would make
/// `weir-ctl metrics` fail out-of-the-box against a default daemon (S27).
const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9185";

#[derive(Parser)]
#[command(
    name = "weir-ctl",
    version,
    about = "Admin and inspection CLI for the weir daemon"
)]
struct Cli {
    /// Emit machine-readable JSON instead of the human table, for the
    /// read/inspect subcommands (health, metrics, segments, dl list). The human
    /// output stays the default. Mutating commands (push, dl drop/requeue) emit
    /// a small JSON result object under --json.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check that the daemon is alive and answering on its socket.
    Health {
        /// Path to the daemon's Unix socket.
        #[arg(long, visible_alias = "socket-path", default_value = DEFAULT_SOCKET)]
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
        #[arg(long, visible_alias = "socket-path", default_value = DEFAULT_SOCKET)]
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
        #[arg(long, env = "WEIR_WAB_DIR")]
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
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
    },
    /// Delete ALL dead-letter segments. Irreversible — defaults to a dry run.
    Drop {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Actually delete. Without this flag, prints what would be deleted.
        #[arg(long)]
        yes: bool,
    },
    /// Re-submit dead-lettered records back through the daemon's socket, then
    /// delete each segment once all its records are re-accepted. Defaults to a
    /// dry run. Re-delivery is at-least-once: if interrupted partway through a
    /// segment, that segment's already-pushed records are re-sent on the next
    /// run (the sink's idempotency key dedupes identical payloads).
    ///
    /// Skip semantics: a sealed segment with ANY unreadable/corrupt record is
    /// skipped WHOLESALE (left in place, nothing from it requeued) so a corrupt
    /// segment is never partially re-delivered. Recovering the readable prefix
    /// of such a segment is a manual step.
    Requeue {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Daemon Unix socket to push the records back through.
        #[arg(long, visible_alias = "socket-path", default_value = DEFAULT_SOCKET)]
        socket: PathBuf,
        /// Durability tier for the re-pushed records: sync | batched | buffered.
        #[arg(long, default_value = "batched", value_parser = parse_durability)]
        durability: Durability,
        /// Actually requeue. Without this flag, prints what would be requeued.
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
    let json = cli.json;
    let result = match cli.command {
        Command::Health { socket } => cmd_health(&socket, json),
        Command::Push {
            payload,
            durability,
            socket,
        } => cmd_push(&socket, payload.as_bytes(), durability, json),
        Command::Metrics { addr, raw } => cmd_metrics(&addr, raw, json),
        Command::Segments { wab_dir } => cmd_segments(&wab_dir, json),
        Command::Dl(dl) => match dl {
            DlCommand::List { wab_dir } => cmd_dl_list(&wab_dir, json),
            DlCommand::Drop { wab_dir, yes } => cmd_dl_drop(&wab_dir, yes, json),
            DlCommand::Requeue {
                wab_dir,
                socket,
                durability,
                yes,
            } => cmd_dl_requeue(&wab_dir, &socket, durability, yes, json),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if json {
                // Under --json, emit a structured error object to stderr so a
                // consumer can parse failures the same way it parses successes.
                // Still goes to stderr (not stdout) and keeps the non-zero exit.
                eprintln!("{}", error_json(&e));
            } else {
                eprintln!("weir-ctl: {e}");
            }
            ExitCode::FAILURE
        }
    }
}

/// Connects to the daemon's Unix socket, turning a connect failure into an
/// operator-friendly error. A failed connect almost always means the daemon
/// isn't running or `--socket` points at the wrong path, so we say so rather
/// than surface a bare `No such file or directory`.
fn connect_client(socket: &Path) -> Result<WeirClient, String> {
    WeirClient::connect(socket).map_err(|e| {
        format!(
            "connect {}: {e}\n  hint: is the weir daemon running, and is --socket the right path? \
             (default {DEFAULT_SOCKET})",
            socket.display()
        )
    })
}

/// Prints a `serde_json::Value` as a pretty, machine-readable block on stdout.
/// Centralised so every `--json` path emits the same shape of output.
fn print_json(value: &serde_json::Value) {
    // `to_string_pretty` cannot fail for a Value built in-process; if it somehow
    // did, fall back to the compact form rather than panicking.
    match serde_json::to_string_pretty(value) {
        Ok(s) => println!("{s}"),
        Err(_) => println!("{value}"),
    }
}

/// The machine-readable form of any command failure: a single `error` string.
/// Emitted to stderr under `--json` (the success payload goes to stdout), so a
/// consumer can parse failures the same way it parses successes.
fn error_json(msg: &str) -> serde_json::Value {
    serde_json::json!({ "error": msg })
}

/// The machine-readable form of a successful `health` check. (The command only
/// reaches the print path once the health check has succeeded, so `healthy` is
/// always `true` here; a failed check returns an Err that `main` prints to
/// stderr with a non-zero exit.)
fn health_json(socket: &Path) -> serde_json::Value {
    serde_json::json!({
        "healthy": true,
        "socket": socket.display().to_string(),
    })
}

fn cmd_health(socket: &Path, json: bool) -> Result<(), String> {
    let mut client = connect_client(socket)?;
    client
        .health_check()
        .map_err(|e| format!("health check failed: {e}"))?;
    if json {
        print_json(&health_json(socket));
    } else {
        println!("OK  daemon healthy at {}", socket.display());
    }
    Ok(())
}

/// The machine-readable form of a successful `push`. (Only reached after the
/// push is acked, so `acked` is always `true` here.)
fn push_json(bytes: usize, durability: Durability) -> serde_json::Value {
    serde_json::json!({
        "acked": true,
        "bytes": bytes,
        "durability": format!("{durability:?}"),
    })
}

fn cmd_push(
    socket: &Path,
    payload: &[u8],
    durability: Durability,
    json: bool,
) -> Result<(), String> {
    let mut client = connect_client(socket)?;
    client
        .push(payload, durability)
        .map_err(|e| format!("push failed: {e}"))?;
    if json {
        print_json(&push_json(payload.len(), durability));
    } else {
        println!("ack  {} bytes, {durability:?}", payload.len());
    }
    Ok(())
}

fn cmd_metrics(addr: &str, raw: bool, json: bool) -> Result<(), String> {
    let body = scrape(addr)?;
    if raw {
        // --raw dumps whatever the endpoint returned, unchanged. It takes
        // precedence over --json: --raw is the escape hatch for the verbatim
        // Prometheus exposition, which is not JSON.
        print!("{body}");
        return Ok(());
    }
    // A summary built from an endpoint with no weir_* series would print a tidy
    // all-zeros "healthy" report — which against the wrong port or a non-weir
    // service is actively misleading. Fail loudly instead.
    if !has_weir_metrics(&body) {
        return Err(format!(
            "no weir metrics found at {addr} — is this a weir daemon's /metrics endpoint, \
             and is --addr correct? (default {DEFAULT_METRICS_ADDR})"
        ));
    }
    if json {
        print_json(&summary_json(&body));
    } else {
        print_summary(&body);
    }
    Ok(())
}

/// True if the exposition contains at least one `weir_` series line — i.e. this
/// really is a weir daemon's `/metrics`, not the wrong port or another service.
fn has_weir_metrics(body: &str) -> bool {
    body.lines().any(|l| l.starts_with("weir_"))
}

/// On-disk segment accounting for one shard directory.
struct ShardStat {
    name: String,
    active: u64,
    sealed: u64,
    confirmed: u64,
    bytes: u64,
}

fn cmd_segments(wab_dir: &Path, json: bool) -> Result<(), String> {
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
        // Count it through the SAME dl_* / suffix filter as `dl list` and
        // `dl drop` (dl_segments) so the two views can't disagree — previously
        // this counted every file, including non-dl strays the dl commands skip
        // (G06).
        if name == "dead_letter" {
            if let Ok(segs) = dl_segments(&path) {
                dl_files += segs.len() as u64;
                dl_bytes += segs.iter().map(|(_, s)| *s).sum::<u64>();
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

    shards.sort_by(|a, b| a.name.cmp(&b.name));

    if json {
        print_json(&segments_json(wab_dir, &shards, dl_files, dl_bytes));
        return Ok(());
    }

    if shards.is_empty() {
        if dl_files == 0 {
            println!("no shard directories under {}", wab_dir.display());
        } else {
            // The daemon hasn't created shard dirs yet (or this is a stale wab_dir),
            // but there are dead-letter files — show just those, not an empty table.
            println!("no shard directories yet under {}", wab_dir.display());
            println!(
                "dead-letter: {}, {}",
                plural(dl_files, "file", "files"),
                fmt_bytes(dl_bytes)
            );
        }
        return Ok(());
    }

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
        println!(
            "dead-letter: {}, {}",
            plural(dl_files, "file", "files"),
            fmt_bytes(dl_bytes)
        );
    }
    Ok(())
}

/// The machine-readable form of the segments view: per-shard objects, a totals
/// object, and the dead-letter rollup. Field names mirror the human table
/// headers; bytes are raw integers so a consumer can do arithmetic on them.
fn segments_json(
    wab_dir: &Path,
    shards: &[ShardStat],
    dl_files: u64,
    dl_bytes: u64,
) -> serde_json::Value {
    let (mut ta, mut ts, mut tc, mut tb) = (0u64, 0u64, 0u64, 0u64);
    let shard_json: Vec<serde_json::Value> = shards
        .iter()
        .map(|s| {
            ta += s.active;
            ts += s.sealed;
            tc += s.confirmed;
            tb += s.bytes;
            serde_json::json!({
                "shard": s.name,
                "active": s.active,
                "sealed": s.sealed,
                "confirmed": s.confirmed,
                "bytes": s.bytes,
            })
        })
        .collect();
    serde_json::json!({
        "wab_dir": wab_dir.display().to_string(),
        "shards": shard_json,
        "total": {
            "active": ta,
            "sealed": ts,
            "confirmed": tc,
            "bytes": tb,
        },
        "dead_letter": {
            "files": dl_files,
            "bytes": dl_bytes,
        },
    })
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

/// Validates that the WAB directory exists and is readable, mirroring how
/// `cmd_segments` opens it (`std::fs::read_dir`). The dead-letter commands
/// otherwise treat a missing `dead_letter/` SUBDIR as an empty store
/// (NotFound → empty), which would silently swallow a missing or mistyped
/// `--wab-dir` into an empty-Ok and mask the misconfiguration. Checking the
/// PARENT dir here makes a bad `--wab-dir` error (non-zero exit) like
/// `segments` does, while a valid wab_dir with no dead-letters yet still
/// reports empty cleanly.
fn ensure_wab_dir(wab_dir: &Path) -> Result<(), String> {
    std::fs::read_dir(wab_dir)
        .map(|_| ())
        .map_err(|e| format!("read {}: {e}", wab_dir.display()))
}

/// The bare active dead-letter file the daemon is currently writing.
///
/// `DeadLetterWriter::write_records` (server `drain/dead_letter.rs`) creates a
/// bare `dl_<counter>.wab`, appends to it, then renames it to
/// `dl_<counter>.wab.sealed`. So a bare `dl_*.wab` is EITHER the segment a live
/// daemon is creating/writing/sealing RIGHT NOW, or an orphaned partial left by
/// a failed write. The CLI cannot tell those apart from the outside, so the
/// destructive paths (`dl requeue`, `dl drop`) treat every bare `.wab` as
/// off-limits: reading+deleting one could race the daemon's `seal()` and lose or
/// duplicate dead-letter records (a torn tail reads as a clean `None`, so a
/// subset would be requeued and the file then removed under the daemon's feet).
/// Informational commands (`dl list`, `segments`) may still COUNT the bare file.
fn is_active_dl_wab(name: &str) -> bool {
    name.starts_with("dl_") && name.ends_with(".wab")
}

/// An immutable, fully-sealed dead-letter segment (`dl_<counter>.wab.sealed`).
/// Once sealed the daemon never reopens or renames it, so it is safe for the CLI
/// to read and delete even against a live daemon.
fn is_sealed_dl_wab(name: &str) -> bool {
    name.starts_with("dl_") && name.ends_with(".wab.sealed")
}

/// Returns `(path, size)` for dead-letter segments in the dead-letter dir,
/// sorted by name. A missing dead-letter directory is treated as empty.
///
/// `include_active` controls whether the daemon's bare active `dl_*.wab` files
/// are included:
///
/// - INFORMATIONAL callers (`dl list`, `segments`) pass `true`: dead-letter
///   records are written then SEALED, so on disk they are `dl_NNNNNNNN.wab.sealed`
///   — the original `ends_with(".wab")` filter never matched them and the store
///   looked empty (F40). Counting the bare `.wab` too lets these views also
///   surface an orphaned/in-flight partial.
/// - DESTRUCTIVE callers (`dl requeue`, `dl drop`) pass `false`: they read then
///   `remove_file`, so they must match ONLY immutable `.wab.sealed` and never the
///   bare active file (see [`is_active_dl_wab`] — a TOCTOU against the daemon's
///   `seal()` would silently lose/duplicate dead-letter records).
fn dl_segments_filtered(
    dl_dir: &Path,
    include_active: bool,
) -> Result<Vec<(PathBuf, u64)>, String> {
    let entries = match std::fs::read_dir(dl_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", dl_dir.display())),
    };
    let mut out = Vec::new();
    for f in entries.flatten() {
        let p = f.path();
        let is_match = p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
            // Order matters: a sealed file ends in BOTH ".wab.sealed" and (via the
            // bare check below) would never match ".wab", so test sealed first.
            is_sealed_dl_wab(n) || (include_active && is_active_dl_wab(n))
        });
        if p.is_file() && is_match {
            let sz = f.metadata().map(|m| m.len()).unwrap_or(0);
            out.push((p, sz));
        }
    }
    out.sort();
    Ok(out)
}

/// Informational listing: counts sealed segments AND the daemon's bare active
/// `dl_*.wab`. Used by `dl list` and `segments` — never for delete/requeue.
fn dl_segments(dl_dir: &Path) -> Result<Vec<(PathBuf, u64)>, String> {
    dl_segments_filtered(dl_dir, true)
}

/// Destructive listing: matches ONLY immutable `dl_*.wab.sealed`. The bare active
/// `dl_*.wab` is deliberately excluded so `dl requeue` / `dl drop` can never
/// read-then-delete the file a live daemon is writing/sealing (see
/// [`is_active_dl_wab`]).
fn dl_sealed_segments(dl_dir: &Path) -> Result<Vec<(PathBuf, u64)>, String> {
    dl_segments_filtered(dl_dir, false)
}

/// The machine-readable form of `dl list`: one object per segment plus a
/// count/total rollup. An empty store is a valid result (empty array, zero
/// totals), not a special-cased message — a consumer always gets the same shape.
fn dl_list_json(dl_dir: &Path, segs: &[(PathBuf, u64)]) -> serde_json::Value {
    let total: u64 = segs.iter().map(|(_, s)| *s).sum();
    let segments: Vec<serde_json::Value> = segs
        .iter()
        .map(|(p, sz)| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            serde_json::json!({ "segment": name, "bytes": sz })
        })
        .collect();
    serde_json::json!({
        "dead_letter_dir": dl_dir.display().to_string(),
        "count": segs.len(),
        "total_bytes": total,
        "segments": segments,
    })
}

fn cmd_dl_list(wab_dir: &Path, json: bool) -> Result<(), String> {
    ensure_wab_dir(wab_dir)?;
    let dl_dir = dead_letter_dir(wab_dir);
    let segs = dl_segments(&dl_dir)?;

    if json {
        print_json(&dl_list_json(&dl_dir, &segs));
        return Ok(());
    }

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

/// The machine-readable form of `dl drop`. Covers all three outcomes — an empty
/// store, a dry run, and a real run — by including only the keys relevant to
/// each: `candidate_bytes` is present only on a dry run, `failures` only on a
/// real run. `dry_run == true` ⇒ nothing was deleted (`dropped`/`dropped_bytes`
/// are zero).
fn dl_drop_json(
    dry_run: bool,
    candidates: usize,
    candidate_bytes: Option<u64>,
    dropped: usize,
    dropped_bytes: u64,
    failures: Option<usize>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("dry_run".into(), serde_json::json!(dry_run));
    obj.insert("candidates".into(), serde_json::json!(candidates));
    if let Some(cb) = candidate_bytes {
        obj.insert("candidate_bytes".into(), serde_json::json!(cb));
    }
    obj.insert("dropped".into(), serde_json::json!(dropped));
    obj.insert("dropped_bytes".into(), serde_json::json!(dropped_bytes));
    if let Some(f) = failures {
        obj.insert("failures".into(), serde_json::json!(f));
    }
    serde_json::Value::Object(obj)
}

fn cmd_dl_drop(wab_dir: &Path, yes: bool, json: bool) -> Result<(), String> {
    ensure_wab_dir(wab_dir)?;
    let dl_dir = dead_letter_dir(wab_dir);
    // DESTRUCTIVE: read-then-delete, so match ONLY immutable `.wab.sealed`. The
    // bare active `dl_*.wab` is off-limits (a live daemon may be sealing it).
    let segs = dl_sealed_segments(&dl_dir)?;
    if segs.is_empty() {
        if json {
            print_json(&dl_drop_json(!yes, 0, None, 0, 0, None));
        } else {
            println!("dead-letter store is empty; nothing to drop");
        }
        return Ok(());
    }
    let total: u64 = segs.iter().map(|(_, s)| *s).sum();
    if !yes {
        if json {
            print_json(&dl_drop_json(true, segs.len(), Some(total), 0, 0, None));
        } else {
            println!(
                "would delete {} dead-letter segment(s) ({}) under {}",
                segs.len(),
                fmt_bytes(total),
                dl_dir.display()
            );
            println!("re-run with --yes to confirm — this is irreversible.");
        }
        return Ok(());
    }
    // Deletion is irreversible, so don't bail on the first failure and leave a
    // silent partial deletion: attempt every file, then report what was dropped
    // vs what failed and fail non-zero if any failed (G05).
    let mut dropped = 0usize;
    let mut dropped_bytes = 0u64;
    let mut failures: Vec<String> = Vec::new();
    for (p, sz) in &segs {
        match std::fs::remove_file(p) {
            Ok(()) => {
                dropped += 1;
                dropped_bytes += *sz;
            }
            Err(e) => failures.push(format!("{}: {e}", p.display())),
        }
    }
    if json {
        print_json(&dl_drop_json(
            false,
            segs.len(),
            None,
            dropped,
            dropped_bytes,
            Some(failures.len()),
        ));
    } else {
        println!(
            "dropped {dropped} of {} dead-letter segment(s) ({})",
            segs.len(),
            fmt_bytes(dropped_bytes)
        );
        println!(
            "note: a running daemon refreshes its dead-letter accounting \
             (weir_dead_letter_bytes_on_disk) on its next health-poll cycle — no restart needed."
        );
    }
    if !failures.is_empty() {
        return Err(format!(
            "{} dead-letter segment(s) could not be removed:\n  {}",
            failures.len(),
            failures.join("\n  ")
        ));
    }
    Ok(())
}

/// Reads every record out of one dead-letter segment, verifying each record's
/// CRC as it goes (via the shared `SegmentReader`). Returns an error — without
/// any partial result — if the header is invalid or any record fails to decode,
/// so a corrupt segment is never partially requeued.
fn read_segment_records(path: &Path) -> Result<Vec<Payload>, String> {
    let reader = SegmentReader::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for (i, rec) in reader.enumerate() {
        match rec {
            Ok(p) => out.push(p),
            Err(e) => return Err(format!("{}: record {i}: {e}", path.display())),
        }
    }
    Ok(out)
}

/// What a `dl requeue` dry run would do: how many records are recoverable and
/// which segments couldn't be read (and so would be skipped).
struct DryRunSummary {
    total_records: u64,
    unreadable: Vec<String>,
}

/// Counts the recoverable records across `segs` (reading + CRC-verifying each)
/// and collects per-segment read errors. Pure over the filesystem inputs, so the
/// counting logic is unit-testable without a daemon.
fn dry_run_summary(segs: &[(PathBuf, u64)]) -> DryRunSummary {
    let mut total_records = 0u64;
    let mut unreadable = Vec::new();
    for (p, _sz) in segs {
        match read_segment_records(p) {
            Ok(recs) => total_records += recs.len() as u64,
            Err(e) => unreadable.push(e),
        }
    }
    DryRunSummary {
        total_records,
        unreadable,
    }
}

/// The machine-readable form of `dl requeue`. The three outcomes report
/// different counters, so each is its own constructor:
/// [`DlRequeueJson::empty`] (empty store), [`DlRequeueJson::dry_run`] (what a
/// real run would do), and [`DlRequeueJson::done`] (a completed real run).
enum DlRequeueJson {}

impl DlRequeueJson {
    /// Empty dead-letter store: nothing to requeue.
    fn empty(dry_run: bool) -> serde_json::Value {
        serde_json::json!({
            "dry_run": dry_run,
            "segments": 0,
            "requeued_records": 0,
            "segments_cleared": 0,
        })
    }

    /// Dry-run preview: how many records/segments WOULD be requeued, and how
    /// many segments are unreadable (and so would be skipped).
    fn dry_run(segments: usize, unreadable: usize, requeuable_records: u64) -> serde_json::Value {
        serde_json::json!({
            "dry_run": true,
            "segments": segments,
            "readable_segments": segments - unreadable,
            "unreadable_segments": unreadable,
            "requeuable_records": requeuable_records,
        })
    }

    /// A completed real run.
    fn done(
        segments: usize,
        requeued_records: u64,
        segments_cleared: usize,
        skipped: usize,
        delete_failures: usize,
        durability: Durability,
    ) -> serde_json::Value {
        serde_json::json!({
            "dry_run": false,
            "segments": segments,
            "requeued_records": requeued_records,
            "segments_cleared": segments_cleared,
            "skipped_segments": skipped,
            "delete_failures": delete_failures,
            "durability": format!("{durability:?}"),
        })
    }
}

fn cmd_dl_requeue(
    wab_dir: &Path,
    socket: &Path,
    durability: Durability,
    yes: bool,
    json: bool,
) -> Result<(), String> {
    ensure_wab_dir(wab_dir)?;
    let dl_dir = dead_letter_dir(wab_dir);
    // DESTRUCTIVE: each segment is read then `remove_file`d after its records are
    // acked, so match ONLY immutable `.wab.sealed`. The bare active `dl_*.wab` is
    // off-limits — snapshotting it would race the live daemon's `seal()` and could
    // requeue a torn-tail subset before deleting it (see `is_active_dl_wab`).
    let segs = dl_sealed_segments(&dl_dir)?;
    if segs.is_empty() {
        if json {
            print_json(&DlRequeueJson::empty(!yes));
        } else {
            println!("dead-letter store is empty; nothing to requeue");
        }
        return Ok(());
    }

    // Dry run: count records per segment (reading + CRC-verifying each) and
    // report what WOULD be requeued. Unreadable segments are surfaced here too.
    if !yes {
        let DryRunSummary {
            total_records,
            unreadable,
        } = dry_run_summary(&segs);
        if json {
            print_json(&DlRequeueJson::dry_run(
                segs.len(),
                unreadable.len(),
                total_records,
            ));
            return Ok(());
        }
        // Report readable-of-total so the segment count reconciles with `dl list`
        // (which counts every segment, readable or not).
        println!(
            "would requeue {total_records} record(s) from {} of {} dead-letter segment(s) \
             under {} through {}",
            segs.len() - unreadable.len(),
            segs.len(),
            dl_dir.display(),
            socket.display(),
        );
        println!(
            "re-run with --yes to confirm. Re-delivery is at-least-once: a record may be \
             delivered more than once if the run is interrupted (the sink's idempotency key \
             dedupes identical payloads)."
        );
        if !unreadable.is_empty() {
            println!(
                "\n⚠ {} of {} segment(s) could not be read and would be SKIPPED:\n  {}",
                unreadable.len(),
                segs.len(),
                unreadable.join("\n  ")
            );
        }
        return Ok(());
    }

    // Real run. Connect once, then requeue segment-by-segment. A segment is
    // deleted only after ALL of its records are re-accepted, so a crash bounds
    // duplication to at most the in-flight segment.
    let mut client = connect_client(socket)?;

    let mut total_requeued: u64 = 0;
    let mut segments_cleared: usize = 0;
    let mut skipped: Vec<String> = Vec::new();
    let mut delete_failures: Vec<String> = Vec::new();

    for (path, _sz) in &segs {
        // Read (and CRC-verify) the whole segment before pushing anything, so a
        // corrupt segment is skipped wholesale rather than partially requeued.
        let records = match read_segment_records(path) {
            Ok(r) => r,
            Err(e) => {
                skipped.push(e);
                continue;
            }
        };

        for (i, rec) in records.iter().enumerate() {
            if let Err(e) = client.push(rec.as_ref(), durability) {
                // A push failure is operational (daemon down / nacking). Abort
                // the whole run rather than hammering a failing daemon. The
                // current segment stays on disk; the records pushed from it so
                // far (i of them) may duplicate on the next run.
                return Err(format!(
                    "push failed after requeuing {total_requeued} record(s) from \
                     {segments_cleared} segment(s); {} left in place \
                     ({i}/{} of it pushed — those may duplicate on the next run): {e}",
                    path.display(),
                    records.len(),
                ));
            }
            total_requeued += 1;
        }

        // Every record re-accepted (each push is acked per its durability tier:
        // after fsync for sync/batched; after in-memory enqueue for buffered).
        // Delete the segment. If the delete fails the records are still safely
        // requeued, but the file will re-requeue (duplicate) on the next run —
        // surface it loudly rather than silently.
        match std::fs::remove_file(path) {
            Ok(()) => segments_cleared += 1,
            Err(e) => delete_failures.push(format!("{}: {e}", path.display())),
        }
    }

    if json {
        print_json(&DlRequeueJson::done(
            segs.len(),
            total_requeued,
            segments_cleared,
            skipped.len(),
            delete_failures.len(),
            durability,
        ));
    } else {
        println!(
            "requeued {total_requeued} record(s) from {segments_cleared} dead-letter segment(s) \
             through {} ({durability:?})",
            socket.display(),
        );
        println!(
            "note: requeued records re-enter the pipeline and the drain will attempt delivery \
             again; if the sink still rejects them they will be dead-lettered anew."
        );
        if !skipped.is_empty() {
            println!(
                "\n⚠ {} segment(s) were SKIPPED (unreadable) and left in place:\n  {}",
                skipped.len(),
                skipped.join("\n  ")
            );
        }
    }
    // Aggregate BOTH failure conditions into one error so the stderr summary
    // reflects everything that went wrong — previously a delete failure masked
    // the skip count (both were printed above, but only the delete failure was
    // returned). Exit code is non-zero if either occurred.
    let mut problems: Vec<String> = Vec::new();
    if !delete_failures.is_empty() {
        problems.push(format!(
            "{} segment(s) were requeued but could not be deleted (they will requeue again \
             next run — remove them manually):\n  {}",
            delete_failures.len(),
            delete_failures.join("\n  ")
        ));
    }
    if !skipped.is_empty() {
        problems.push(format!(
            "{} dead-letter segment(s) could not be read",
            skipped.len()
        ));
    }
    if !problems.is_empty() {
        return Err(problems.join("\n"));
    }
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

/// The parsed metrics summary, shared by the human (`print_summary`) and JSON
/// (`summary_json`) renderers so both views report identically-derived numbers.
struct MetricsSummary {
    accepted: u64,
    acked: u64,
    nacked: u64,
    fsync_avg_ms: f64,
    queue_depth: u64,
    panics: u64,
    fsync_failures: u64,
    dead_letter_bytes: u64,
    wab_bytes: u64,
    sink_health: String,
    sink_type: String,
}

/// Parses the metric values `print_summary` and `summary_json` both need from a
/// Prometheus exposition body.
fn parse_summary(body: &str) -> MetricsSummary {
    let fsync_sum = get_metric(body, "weir_wab_fsync_duration_seconds_sum").unwrap_or(0.0);
    let fsync_count = get_metric(body, "weir_wab_fsync_duration_seconds_count").unwrap_or(0.0);
    let fsync_avg_ms = if fsync_count > 0.0 {
        fsync_sum / fsync_count * 1000.0
    } else {
        0.0
    };

    MetricsSummary {
        // Counters are non-negative integers; render them as such (avoids `-0`).
        accepted: sum_metric(body, "weir_records_accepted_total") as u64,
        acked: sum_metric(body, "weir_records_ack_total") as u64,
        nacked: sum_metric(body, "weir_records_nack_total") as u64,
        fsync_avg_ms,
        queue_depth: get_metric(body, "weir_queue_depth").unwrap_or(0.0) as u64,
        panics: get_metric(body, "weir_wab_flusher_panics_total").unwrap_or(0.0) as u64,
        fsync_failures: get_metric(body, "weir_wab_fsync_failures_total").unwrap_or(0.0) as u64,
        dead_letter_bytes: get_metric(body, "weir_dead_letter_bytes_on_disk").unwrap_or(0.0) as u64,
        wab_bytes: get_metric(body, "weir_wab_bytes_on_disk").unwrap_or(0.0) as u64,
        // Health flags worth surfacing loudly.
        sink_health: active_label(body, "weir_sink_health", "state").unwrap_or_else(|| "?".into()),
        sink_type: active_label(body, "weir_sink_info", "sink_type").unwrap_or_else(|| "?".into()),
    }
}

/// The machine-readable form of the metrics summary. Field names mirror the
/// human labels; byte gauges are raw integers (not pretty `fmt_bytes` strings)
/// so a consumer can do arithmetic on them.
fn summary_json(body: &str) -> serde_json::Value {
    let s = parse_summary(body);
    serde_json::json!({
        "accepted": s.accepted,
        "ack": s.acked,
        "nack": s.nacked,
        "fsync_avg_ms": s.fsync_avg_ms,
        "queue_depth": s.queue_depth,
        "wab_bytes_on_disk": s.wab_bytes,
        "dead_letter_bytes_on_disk": s.dead_letter_bytes,
        "sink_type": s.sink_type,
        "sink_health": s.sink_health,
        "flusher_panics": s.panics,
        "fsync_failures": s.fsync_failures,
    })
}

fn print_summary(body: &str) {
    let MetricsSummary {
        accepted,
        acked,
        nacked,
        fsync_avg_ms,
        queue_depth,
        panics,
        fsync_failures,
        dead_letter_bytes,
        wab_bytes,
        sink_health,
        sink_type,
    } = parse_summary(body);

    // Labels padded to a single consistent width so the values line up.
    println!("── weir ──────────────────────────────────");
    println!(
        "{:<10} accepted {accepted}  ack {acked}  nack {nacked}",
        "ingest"
    );
    println!(
        "{:<10} fsync avg {fsync_avg_ms:.2} ms  wab {} on disk",
        "durability",
        fmt_bytes(wab_bytes)
    );
    println!("{:<10} depth {queue_depth}", "queue");
    println!("{:<10} type: {sink_type}  health: {sink_health}", "sink");
    println!(
        "{:<10} {} on disk",
        "dead-ltr",
        fmt_bytes(dead_letter_bytes)
    );

    // Loud warnings for the durability hazards.
    if sink_type == "noop" {
        println!(
            "\n⚠ sink: noop — records are acked then DISCARDED, not delivered downstream. \
             Set --sink-type (http/mysql/postgres/clickhouse) to forward records."
        );
    }
    if panics > 0 {
        println!(
            "\n⚠ flusher {} — a shard is offline until restart",
            plural(panics, "panic", "panics")
        );
    }
    if fsync_failures > 0 {
        println!(
            "⚠ {} — DURABILITY HAZARD (data may not be on stable storage)",
            plural(fsync_failures, "fsync failure", "fsync failures")
        );
    }
    if nacked > 0 {
        println!(
            "ℹ {} nacked — check producer behaviour / capacity",
            plural(nacked, "record", "records")
        );
    }
}

/// `"1 record"` / `"3 records"` — count-aware singular/plural for summary lines.
fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_weir_metrics_detects_weir_series() {
        // A real weir exposition has weir_ series; the wrong port / another
        // service does not.
        assert!(has_weir_metrics(
            "# HELP weir_records_accepted ...\nweir_records_accepted_total{tier=\"sync\"} 3"
        ));
        assert!(!has_weir_metrics(
            "# HELP go_gc_duration_seconds ...\ngo_goroutines 12"
        ));
        assert!(!has_weir_metrics(""));
    }

    #[test]
    fn dl_segments_finds_sealed_files_not_just_bare_wab() {
        // Regression for F40: dead-letter files are sealed (dl_NNN.wab.sealed);
        // the old `ends_with(".wab")` filter missed them entirely.
        let dir = std::env::temp_dir().join(format!("weir_ctl_dl_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("dl_00000001.wab.sealed"), b"sealed-record").unwrap();
        std::fs::write(dir.join("dl_00000002.wab"), b"orphan-partial").unwrap();
        std::fs::write(dir.join("not_a_dl_file.txt"), b"ignore").unwrap();

        let segs = dl_segments(&dir).unwrap();
        let names: Vec<String> = segs
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["dl_00000001.wab.sealed", "dl_00000002.wab"],
            "must find sealed dead-letter files (and orphan partials), not the .txt"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dl_segments_missing_dir_is_empty() {
        let dir = std::env::temp_dir().join("weir_ctl_dl_nonexistent_xyzzy");
        assert!(dl_segments(&dir).unwrap().is_empty());
    }

    #[test]
    fn dl_list_and_drop_error_on_missing_wab_dir_but_empty_without_subdir() {
        // A missing/typo'd `--wab-dir` must error (non-zero exit), like
        // `segments` does — not be swallowed into an empty-Ok via the
        // dead_letter/ subdir's NotFound→empty path. But a VALID wab_dir that
        // simply has no dead_letter/ subdir yet must still report empty cleanly.

        // (1) Missing wab dir → error.
        let missing =
            std::env::temp_dir().join(format!("weir_ctl_missing_wab_{}_xyzzy", std::process::id()));
        std::fs::remove_dir_all(&missing).ok();
        assert!(
            cmd_dl_list(&missing, false).is_err(),
            "dl list on a missing wab dir must error, not report empty"
        );
        assert!(
            cmd_dl_drop(&missing, false, false).is_err(),
            "dl drop on a missing wab dir must error, not report empty"
        );

        // (2) Existing wab dir with no dead_letter/ subdir → empty/Ok.
        let present =
            std::env::temp_dir().join(format!("weir_ctl_present_wab_{}", std::process::id()));
        std::fs::create_dir_all(&present).unwrap();
        cmd_dl_list(&present, false)
            .expect("dl list on an empty wab dir must report empty cleanly");
        cmd_dl_drop(&present, false, false)
            .expect("dl drop on an empty wab dir must report empty cleanly");
        std::fs::remove_dir_all(&present).ok();
    }

    #[test]
    fn dl_drop_removes_sealed_segments_only_not_active_wab() {
        // G05: --yes drops all matched dl segments. (The accumulation loop also
        // continues past per-file failures and reports them, but an unremovable
        // file can't be created portably — root bypasses perms — so this covers
        // the all-succeed path.)
        //
        // TOCTOU fix: `drop` reads-then-deletes, so it must touch ONLY immutable
        // `.wab.sealed`. A bare `dl_*.wab` is the daemon's active/in-flight file
        // and must be left in place.
        let wab = std::env::temp_dir().join(format!("weir_ctl_drop_{}", std::process::id()));
        let dl = wab.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        std::fs::write(dl.join("dl_00000001.wab.sealed"), b"a").unwrap();
        std::fs::write(dl.join("dl_00000002.wab.sealed"), b"b").unwrap();
        let active = dl.join("dl_00000003.wab"); // daemon's active file
        std::fs::write(&active, b"in-flight").unwrap();
        std::fs::write(dl.join("keep.txt"), b"not-a-dl-file").unwrap();

        cmd_dl_drop(&wab, true, false).unwrap();

        // The sealed segments are gone; the active `.wab` and the non-dl file are
        // untouched.
        assert!(
            dl_sealed_segments(&dl).unwrap().is_empty(),
            "all sealed dl segments dropped"
        );
        assert!(
            active.exists(),
            "the daemon's active dl_*.wab must NOT be deleted by drop"
        );
        assert!(
            dl.join("keep.txt").exists(),
            "non-dl file must be left alone"
        );
        std::fs::remove_dir_all(&wab).ok();
    }

    // ── Requeue ──────────────────────────────────────────────────────────────

    /// Writes a valid sealed dead-letter segment `dl_<counter>.wab.sealed` that
    /// `SegmentReader` can read: header + `[len][crc][payload]` per record +
    /// sentinel. (The reader stops at the sentinel, so the footer is omitted.)
    fn write_dl_segment(dl_dir: &Path, counter: u64, records: &[&[u8]]) {
        use std::io::Write;
        std::fs::create_dir_all(dl_dir).unwrap();
        let path = dl_dir.join(format!("dl_{counter:08}.wab.sealed"));
        let mut f = std::fs::File::create(&path).unwrap();
        // Shard ID 0xFFFF is the dead-letter marker the daemon uses.
        f.write_all(&weir_wab::format::build_segment_header(0xFFFF))
            .unwrap();
        for r in records {
            f.write_all(&(r.len() as u32).to_le_bytes()).unwrap();
            // Same CRC32 (IEEE) SegmentReader verifies — see weir-wab.
            f.write_all(&crc32fast::hash(r).to_le_bytes()).unwrap();
            f.write_all(r).unwrap();
        }
        f.write_all(&weir_wab::format::build_sentinel()).unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn read_segment_records_reads_all_in_order() {
        let dir = std::env::temp_dir().join(format!("weir_ctl_rq_read_{}", std::process::id()));
        let dl = dir.join("dead_letter");
        write_dl_segment(&dl, 1, &[b"alpha", b"beta", b"gamma"]);
        let path = dl.join("dl_00000001.wab.sealed");
        let recs = read_segment_records(&path).unwrap();
        let got: Vec<&[u8]> = recs.iter().map(|p| p.as_ref()).collect();
        assert_eq!(got, vec![b"alpha".as_ref(), b"beta", b"gamma"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_segment_records_errors_on_corrupt_record() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("weir_ctl_rq_crc_{}", std::process::id()));
        let dl = dir.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        let path = dl.join("dl_00000001.wab.sealed");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&weir_wab::format::build_segment_header(0xFFFF))
            .unwrap();
        let payload = b"corruptme";
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&0xdead_beefu32.to_le_bytes()).unwrap(); // wrong CRC
        f.write_all(payload).unwrap();
        f.write_all(&weir_wab::format::build_sentinel()).unwrap();
        f.sync_all().unwrap();

        let err = read_segment_records(&path).unwrap_err();
        assert!(err.contains("record 0"), "err: {err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn dry_run_summary_counts_readable_and_flags_unreadable() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("weir_ctl_rq_sum_{}", std::process::id()));
        let dl = dir.join("dead_letter");
        // One valid segment with 2 records.
        write_dl_segment(&dl, 1, &[b"r1", b"r2"]);
        // One corrupt segment (bad CRC) that must be flagged, not counted.
        let bad = dl.join("dl_00000002.wab.sealed");
        let mut f = std::fs::File::create(&bad).unwrap();
        f.write_all(&weir_wab::format::build_segment_header(0xFFFF))
            .unwrap();
        f.write_all(&3u32.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap(); // wrong CRC
        f.write_all(b"bad").unwrap();
        f.write_all(&weir_wab::format::build_sentinel()).unwrap();
        f.sync_all().unwrap();

        let segs = dl_segments(&dl).unwrap();
        assert_eq!(segs.len(), 2);
        let summary = dry_run_summary(&segs);
        assert_eq!(
            summary.total_records, 2,
            "only the 2 readable records count"
        );
        assert_eq!(
            summary.unreadable.len(),
            1,
            "the corrupt segment is flagged"
        );
        assert!(
            summary.unreadable[0].contains("record 0"),
            "{:?}",
            summary.unreadable
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn requeue_empty_store_is_ok_without_connecting() {
        // Empty store: returns Ok and never touches the socket (so a bogus
        // socket path is harmless).
        let wab = std::env::temp_dir().join(format!("weir_ctl_rq_empty_{}", std::process::id()));
        std::fs::create_dir_all(&wab).unwrap();
        let bogus = Path::new("/nonexistent/weir.sock");
        cmd_dl_requeue(&wab, bogus, Durability::Batched, true, false).unwrap();
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn requeue_dry_run_does_not_connect() {
        // Dry run (yes = false) must read + count without connecting — a bogus
        // socket must NOT cause an error.
        let wab = std::env::temp_dir().join(format!("weir_ctl_rq_dry_{}", std::process::id()));
        let dl = wab.join("dead_letter");
        write_dl_segment(&dl, 1, &[b"one", b"two"]);
        let bogus = Path::new("/nonexistent/weir.sock");
        cmd_dl_requeue(&wab, bogus, Durability::Batched, false, false).unwrap();
        // Dry run leaves the segment in place.
        assert_eq!(dl_segments(&dl).unwrap().len(), 1);
        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn requeue_real_run_errors_when_daemon_unreachable() {
        // With records present and --yes, the real run must attempt to connect;
        // an unreachable socket surfaces a connect error and leaves the segment
        // untouched (nothing was requeued).
        let wab = std::env::temp_dir().join(format!("weir_ctl_rq_conn_{}", std::process::id()));
        let dl = wab.join("dead_letter");
        write_dl_segment(&dl, 1, &[b"rec"]);
        let bogus = Path::new("/nonexistent/weir.sock");
        let err = cmd_dl_requeue(&wab, bogus, Durability::Batched, true, false).unwrap_err();
        assert!(err.contains("connect"), "err: {err}");
        // The segment is left in place since nothing could be requeued.
        assert_eq!(dl_segments(&dl).unwrap().len(), 1);
        std::fs::remove_dir_all(&wab).ok();
    }

    // ── TOCTOU: destructive paths touch only `.wab.sealed`, never active `.wab` ──

    #[test]
    fn destructive_listing_excludes_active_wab() {
        // Minimum-bar guard for the TOCTOU fix: the destructive segment listing
        // (`dl_sealed_segments`, used by requeue/drop) must match ONLY immutable
        // `.wab.sealed` and exclude the daemon's bare active `dl_*.wab`. The
        // informational listing (`dl_segments`) still counts the bare file.
        let dir = std::env::temp_dir().join(format!("weir_ctl_destr_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("dl_00000001.wab.sealed"), b"sealed").unwrap();
        std::fs::write(dir.join("dl_00000002.wab"), b"active").unwrap(); // daemon's active file
        std::fs::write(dir.join("not_a_dl_file.txt"), b"ignore").unwrap();

        let names = |segs: Vec<(PathBuf, u64)>| -> Vec<String> {
            segs.iter()
                .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
                .collect()
        };

        // Destructive: sealed only.
        assert_eq!(
            names(dl_sealed_segments(&dir).unwrap()),
            vec!["dl_00000001.wab.sealed"],
            "destructive listing must exclude the bare active .wab"
        );
        // Informational: sealed + bare active (unchanged behavior).
        assert_eq!(
            names(dl_segments(&dir).unwrap()),
            vec!["dl_00000001.wab.sealed", "dl_00000002.wab"],
            "informational listing still counts the active .wab"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An in-process fake daemon that speaks the Push/Ack wire protocol over a
    /// Unix socket: it accepts ONE connection, then reads Push frames and replies
    /// with an Ack for each, recording every payload it received. Used to assert
    /// the delete-only-after-ack contract without standing up the real daemon.
    struct FakeDaemon {
        socket: PathBuf,
        handle: Option<std::thread::JoinHandle<Vec<Vec<u8>>>>,
    }

    impl FakeDaemon {
        fn start(socket: PathBuf) -> Self {
            use std::os::unix::net::UnixListener;
            let listener = UnixListener::bind(&socket).expect("bind fake daemon socket");
            let handle = std::thread::spawn(move || {
                let mut received: Vec<Vec<u8>> = Vec::new();
                let (mut stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(_) => return received,
                };
                // Read frames until the client disconnects (EOF on the header read).
                loop {
                    let mut header_buf = [0u8; weir_core::HEADER_LEN];
                    if std::io::Read::read_exact(&mut stream, &mut header_buf).is_err() {
                        break; // clean EOF / disconnect
                    }
                    let header = weir_core::Header::decode(&header_buf).expect("decode header");
                    let payload_len = header.payload_len() as usize;
                    let mut payload = vec![0u8; payload_len];
                    if payload_len > 0 {
                        std::io::Read::read_exact(&mut stream, &mut payload).expect("read payload");
                    }
                    // Consume the trailing CRC word (4 bytes) of the request frame.
                    let mut crc = [0u8; 4];
                    std::io::Read::read_exact(&mut stream, &mut crc).expect("read req crc");
                    received.push(payload);

                    // Reply with a well-formed Ack frame (empty payload).
                    let ack = weir_core::Envelope::new(
                        weir_core::Header::new(
                            weir_core::MessageType::Ack,
                            weir_core::Durability::Sync,
                            0,
                        ),
                        Vec::new(),
                    )
                    .encode();
                    std::io::Write::write_all(&mut stream, &ack).expect("write ack");
                }
                received
            });
            FakeDaemon {
                socket,
                handle: Some(handle),
            }
        }

        /// Joins the daemon thread and returns the payloads it received. The
        /// client drops at the end of the requeue call, so the daemon sees EOF
        /// and the loop exits.
        fn into_received(mut self) -> Vec<Vec<u8>> {
            self.handle.take().unwrap().join().expect("daemon thread")
        }
    }

    impl Drop for FakeDaemon {
        fn drop(&mut self) {
            std::fs::remove_file(&self.socket).ok();
        }
    }

    #[test]
    fn requeue_deletes_sealed_segments_only_after_acks() {
        // The core sev-9 contract: against a daemon that acks every push, a real
        // `dl requeue --yes` (a) re-delivers EVERY record, and (b) deletes the
        // `.wab.sealed` segments — but only AFTER the acks. (c) A bare active
        // `dl_*.wab` is never read or deleted.
        let base = std::env::temp_dir().join(format!("weir_ctl_rq_ack_{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let wab = base.join("wab");
        let dl = wab.join("dead_letter");

        // Two sealed segments to be requeued, plus the daemon's active file.
        write_dl_segment(&dl, 1, &[b"a1", b"a2"]);
        write_dl_segment(&dl, 2, &[b"b1", b"b2", b"b3"]);
        let active = dl.join("dl_00000003.wab");
        std::fs::write(&active, b"in-flight-do-not-touch").unwrap();
        let active_before = std::fs::read(&active).unwrap();

        // Short socket path (Unix socket paths are length-limited).
        let socket = std::env::temp_dir().join(format!("wctl_rq_{}.sock", std::process::id()));
        std::fs::remove_file(&socket).ok();
        let daemon = FakeDaemon::start(socket.clone());

        cmd_dl_requeue(&wab, &socket, Durability::Batched, true, false)
            .expect("requeue should succeed");

        let received = daemon.into_received();

        // (a) Every record from BOTH sealed segments reached the daemon, in order.
        assert_eq!(
            received,
            vec![
                b"a1".to_vec(),
                b"a2".to_vec(),
                b"b1".to_vec(),
                b"b2".to_vec(),
                b"b3".to_vec(),
            ],
            "daemon must receive every requeued record in segment+record order"
        );

        // (b) Both sealed segments are deleted (after the acks — the requeue call
        // only returned Ok once every push was acked, then removed the files).
        assert!(
            dl_sealed_segments(&dl).unwrap().is_empty(),
            "sealed segments must be deleted after their records are acked"
        );

        // (c) The bare active `.wab` was never read or deleted — still present and
        // byte-for-byte unchanged.
        assert!(active.exists(), "active dl_*.wab must not be deleted");
        assert_eq!(
            std::fs::read(&active).unwrap(),
            active_before,
            "active dl_*.wab must not be modified"
        );

        std::fs::remove_dir_all(&base).ok();
    }

    // ── `--json` machine-readable output (sweep #6) ──────────────────────────
    //
    // Each read/inspect subcommand's `--json` shape is built by a pure helper
    // (`health_json` / `summary_json` / `segments_json` / `dl_list_json`) that
    // the command function calls. The tests exercise those helpers directly:
    // they assert the emitted text is valid JSON (round-trips through
    // serde_json) and carries the expected top-level keys, deterministically and
    // without stdout capture or a live daemon.

    /// Parses a `serde_json::Value` through its serialized form, the same way an
    /// external consumer of `--json` output would — proving the rendered text is
    /// valid JSON, not just that the in-memory `Value` exists.
    fn round_trip(value: &serde_json::Value) -> serde_json::Value {
        let rendered = serde_json::to_string_pretty(value).expect("must serialize");
        serde_json::from_str(&rendered).expect("--json output must be valid JSON")
    }

    #[test]
    fn health_json_has_expected_keys() {
        let v = round_trip(&health_json(Path::new("/run/weir/weir.sock")));
        assert_eq!(v["healthy"], serde_json::json!(true));
        assert_eq!(v["socket"], serde_json::json!("/run/weir/weir.sock"));
    }

    #[test]
    fn metrics_summary_json_has_expected_keys() {
        // A representative exposition with the series print_summary reads.
        let body = "\
weir_records_accepted_total{tier=\"sync\"} 5
weir_records_ack_total{tier=\"sync\"} 4
weir_records_nack_total{reason=\"bad_payload_crc\"} 1
weir_wab_fsync_duration_seconds_sum 0.5
weir_wab_fsync_duration_seconds_count 10
weir_queue_depth 7
weir_wab_flusher_panics_total 0
weir_wab_fsync_failures_total 0
weir_dead_letter_bytes_on_disk 2048
weir_wab_bytes_on_disk 4096
weir_sink_health{state=\"healthy\"} 1
weir_sink_info{sink_type=\"http\"} 1
";
        let v = round_trip(&summary_json(body));
        // Top-level keys exist and carry the parsed values.
        assert_eq!(v["accepted"], serde_json::json!(5));
        assert_eq!(v["ack"], serde_json::json!(4));
        assert_eq!(v["nack"], serde_json::json!(1));
        assert_eq!(v["queue_depth"], serde_json::json!(7));
        assert_eq!(v["wab_bytes_on_disk"], serde_json::json!(4096));
        assert_eq!(v["dead_letter_bytes_on_disk"], serde_json::json!(2048));
        assert_eq!(v["sink_type"], serde_json::json!("http"));
        assert_eq!(v["sink_health"], serde_json::json!("healthy"));
        assert_eq!(v["flusher_panics"], serde_json::json!(0));
        assert_eq!(v["fsync_failures"], serde_json::json!(0));
        // fsync avg = sum/count*1000 = 0.5/10*1000 = 50 ms.
        assert_eq!(v["fsync_avg_ms"], serde_json::json!(50.0));
    }

    #[test]
    fn segments_json_has_expected_keys_and_totals() {
        let shards = vec![
            ShardStat {
                name: "shard-0".into(),
                active: 1,
                sealed: 2,
                confirmed: 3,
                bytes: 100,
            },
            ShardStat {
                name: "shard-1".into(),
                active: 0,
                sealed: 1,
                confirmed: 0,
                bytes: 50,
            },
        ];
        let v = round_trip(&segments_json(Path::new("/var/lib/weir"), &shards, 4, 200));
        assert_eq!(v["wab_dir"], serde_json::json!("/var/lib/weir"));
        // Per-shard array preserved.
        assert!(v["shards"].is_array());
        assert_eq!(v["shards"].as_array().unwrap().len(), 2);
        assert_eq!(v["shards"][0]["shard"], serde_json::json!("shard-0"));
        assert_eq!(v["shards"][0]["bytes"], serde_json::json!(100));
        // Totals are summed across shards.
        assert_eq!(v["total"]["active"], serde_json::json!(1));
        assert_eq!(v["total"]["sealed"], serde_json::json!(3));
        assert_eq!(v["total"]["confirmed"], serde_json::json!(3));
        assert_eq!(v["total"]["bytes"], serde_json::json!(150));
        // Dead-letter rollup.
        assert_eq!(v["dead_letter"]["files"], serde_json::json!(4));
        assert_eq!(v["dead_letter"]["bytes"], serde_json::json!(200));
    }

    #[test]
    fn dl_list_json_has_expected_keys_from_tmp_wab() {
        // Build a real tmp dead-letter dir and feed its actual listing through
        // the JSON helper, exercising the same `dl_segments` path the command
        // uses end-to-end.
        let wab = std::env::temp_dir().join(format!("weir_ctl_dljson_{}", std::process::id()));
        let dl = wab.join("dead_letter");
        write_dl_segment(&dl, 1, &[b"x"]);
        write_dl_segment(&dl, 2, &[b"y", b"z"]);
        let segs = dl_segments(&dl).unwrap();

        let v = round_trip(&dl_list_json(&dl, &segs));
        assert_eq!(v["count"], serde_json::json!(2));
        assert!(v["total_bytes"].is_u64());
        assert!(v["segments"].is_array());
        assert_eq!(v["segments"].as_array().unwrap().len(), 2);
        assert_eq!(
            v["segments"][0]["segment"],
            serde_json::json!("dl_00000001.wab.sealed")
        );
        assert!(v["dead_letter_dir"].is_string());

        std::fs::remove_dir_all(&wab).ok();
    }

    #[test]
    fn dl_list_json_empty_store_is_stable_shape() {
        // An empty store still yields the same shape (zero count, empty array) —
        // not a special-cased message — so a consumer can parse unconditionally.
        let dir = std::env::temp_dir().join("weir_ctl_dljson_empty_xyzzy");
        let v = round_trip(&dl_list_json(&dir, &[]));
        assert_eq!(v["count"], serde_json::json!(0));
        assert_eq!(v["total_bytes"], serde_json::json!(0));
        assert_eq!(v["segments"], serde_json::json!([]));
    }

    #[test]
    fn push_json_has_expected_keys() {
        let v = round_trip(&push_json(128, Durability::Sync));
        assert_eq!(v["acked"], serde_json::json!(true));
        assert_eq!(v["bytes"], serde_json::json!(128));
        assert_eq!(v["durability"], serde_json::json!("Sync"));
    }

    #[test]
    fn dl_drop_json_has_expected_keys_per_outcome() {
        // Empty store: no candidate_bytes / failures keys.
        let empty = round_trip(&dl_drop_json(true, 0, None, 0, 0, None));
        assert_eq!(empty["dry_run"], serde_json::json!(true));
        assert_eq!(empty["candidates"], serde_json::json!(0));
        assert_eq!(empty["dropped"], serde_json::json!(0));
        assert!(empty.get("candidate_bytes").is_none());
        assert!(empty.get("failures").is_none());

        // Dry run: candidate_bytes present, failures absent.
        let dry = round_trip(&dl_drop_json(true, 3, Some(900), 0, 0, None));
        assert_eq!(dry["candidates"], serde_json::json!(3));
        assert_eq!(dry["candidate_bytes"], serde_json::json!(900));
        assert_eq!(dry["dropped"], serde_json::json!(0));
        assert!(dry.get("failures").is_none());

        // Real run: failures present, candidate_bytes absent.
        let done = round_trip(&dl_drop_json(false, 3, None, 2, 500, Some(1)));
        assert_eq!(done["dry_run"], serde_json::json!(false));
        assert_eq!(done["dropped"], serde_json::json!(2));
        assert_eq!(done["dropped_bytes"], serde_json::json!(500));
        assert_eq!(done["failures"], serde_json::json!(1));
        assert!(done.get("candidate_bytes").is_none());
    }

    #[test]
    fn dl_requeue_json_has_expected_keys_per_outcome() {
        let empty = round_trip(&DlRequeueJson::empty(true));
        assert_eq!(empty["dry_run"], serde_json::json!(true));
        assert_eq!(empty["segments"], serde_json::json!(0));
        assert_eq!(empty["requeued_records"], serde_json::json!(0));
        assert_eq!(empty["segments_cleared"], serde_json::json!(0));

        let dry = round_trip(&DlRequeueJson::dry_run(5, 2, 40));
        assert_eq!(dry["dry_run"], serde_json::json!(true));
        assert_eq!(dry["segments"], serde_json::json!(5));
        assert_eq!(dry["readable_segments"], serde_json::json!(3));
        assert_eq!(dry["unreadable_segments"], serde_json::json!(2));
        assert_eq!(dry["requeuable_records"], serde_json::json!(40));

        let done = round_trip(&DlRequeueJson::done(5, 40, 4, 1, 0, Durability::Batched));
        assert_eq!(done["dry_run"], serde_json::json!(false));
        assert_eq!(done["segments"], serde_json::json!(5));
        assert_eq!(done["requeued_records"], serde_json::json!(40));
        assert_eq!(done["segments_cleared"], serde_json::json!(4));
        assert_eq!(done["skipped_segments"], serde_json::json!(1));
        assert_eq!(done["delete_failures"], serde_json::json!(0));
        assert_eq!(done["durability"], serde_json::json!("Batched"));
    }

    #[test]
    fn json_error_path_emits_parseable_json() {
        // Drive a guaranteed-failure path: connecting to a socket that cannot
        // exist yields an Err, which `main` renders under --json via
        // `error_json`. The rendered object must round-trip as valid JSON and
        // carry the error message in an `error` string field.
        let bogus =
            std::env::temp_dir().join(format!("weir_ctl_no_such_sock_{}.sock", std::process::id()));
        let err = connect_client(&bogus).expect_err("connecting to a missing socket must fail");
        let v = round_trip(&error_json(&err));
        assert!(
            v["error"].is_string(),
            "expected an `error` string field, got: {v}"
        );
        // The error text is preserved verbatim (operator hint included).
        assert_eq!(v["error"].as_str().unwrap(), err);
    }
}
