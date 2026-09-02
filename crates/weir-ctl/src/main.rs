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
    /// read/inspect subcommands (health, metrics, segments, dl list,
    /// quarantine list, quarantine inspect). The human output stays the
    /// default. Mutating commands (push, dl drop/requeue) emit a small JSON
    /// result object under --json.
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
        /// Durability tier: durable | buffered (sync, batched accepted as
        /// legacy aliases for durable).
        #[arg(long, default_value = "durable", value_parser = parse_durability)]
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
    /// Inspect and recover quarantined segments.
    ///
    /// Quarantine holds forensic copies of segments where recovery met
    /// corruption. Acked records may sit AFTER the corrupt record, and those
    /// records exist nowhere else — this is how you get them back.
    #[command(subcommand)]
    Quarantine(QuarantineCommand),
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
    /// run, and a dedup-capable sink will NOT filter those duplicates — since
    /// 2.0.3 the per-record idempotency key is derived from a record's WAB
    /// coordinate as well as its bytes, and a requeue re-pushes into a new
    /// segment, so the sink sees genuinely distinct records and accepts both.
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
        /// Durability tier for the re-pushed records: durable | buffered
        /// (sync, batched accepted as legacy aliases for durable).
        #[arg(long, default_value = "durable", value_parser = parse_durability)]
        durability: Durability,
        /// Actually requeue. Without this flag, prints what would be requeued.
        #[arg(long)]
        yes: bool,
    },
}

/// Subcommands under `weir-ctl quarantine`.
#[derive(Subcommand)]
enum QuarantineCommand {
    /// List quarantined segments (count + bytes + origin shard).
    List {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
    },
    /// Report what is readable in one quarantined segment: how many records
    /// verify, how many are corrupt and at what offsets, and whether the reader
    /// lost the record framing entirely.
    Inspect {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Segment file name, as printed by `quarantine list`.
        segment: String,
    },
    /// Re-submit recoverable records from quarantined segments through the
    /// daemon's socket, then delete each segment once all of them are accepted.
    /// Defaults to a dry run.
    ///
    /// Records that fail verification are SKIPPED, not re-sent — unlike
    /// `dl requeue`, which skips a corrupt segment wholesale. Every quarantined
    /// segment is corrupt by definition, so that rule would recover nothing;
    /// here the corrupt RECORD is skipped and the rest of the segment is
    /// recovered.
    ///
    /// Re-delivery is at-least-once, and it WILL re-send records that already
    /// reached the sink: recovery delivered the valid prefix when it sealed it,
    /// and that prefix lives in this same file as the preserved tail. The dry
    /// run prints the count before you pass --yes.
    ///
    /// A dedup-capable sink will NOT filter those duplicates. The dedup token
    /// is derived from a batch's contents AND its boundaries, and a requeue
    /// re-batches — so the sink sees genuinely distinct batches and accepts
    /// both.
    ///
    /// A segment is deleted only once EVERY one of its recoverable records has
    /// been ACCEPTED, not merely pushed — if a push fails or is Nacked partway
    /// through, the segment stays on disk. A segment that yields no recoverable
    /// records at all, OR that desyncs at any point (however many records were
    /// recovered before the desync), is also left in place: bytes past a
    /// desync were never reached, so no report can substitute for records
    /// nobody has read. The recoverable prefix is still requeued in that case
    /// — only the delete is withheld.
    ///
    /// --durability buffered is refused: its ack means only "entered the
    /// in-memory queue", not durably written, and this command deletes what is
    /// often the only surviving copy of the record once every push is
    /// accepted. Use sync or batched (the default).
    Requeue {
        /// Path to the daemon's WAB directory.
        #[arg(long, env = "WEIR_WAB_DIR")]
        wab_dir: PathBuf,
        /// Daemon Unix socket to push the records back through.
        #[arg(long, visible_alias = "socket-path", default_value = DEFAULT_SOCKET)]
        socket: PathBuf,
        /// Durability tier for the re-pushed records: durable (sync, batched
        /// accepted as legacy aliases). NOT buffered — refused, see above.
        #[arg(long, default_value = "durable", value_parser = parse_durability)]
        durability: Durability,
        /// Actually requeue. Without this flag, prints what would be requeued.
        #[arg(long)]
        yes: bool,
    },
}

fn parse_durability(s: &str) -> Result<Durability, String> {
    match s.to_ascii_lowercase().as_str() {
        // "durable" is the canonical 2.0 spelling — it's what every current
        // doc uses and what `Durability::Display` now emits. "sync" and
        // "batched" are kept accepted, same as the deprecated Rust consts and
        // the retired `0x02` wire byte: scripts and runbooks in the wild use
        // them, and breaking them buys nothing.
        "durable" | "sync" | "batched" => Ok(Durability::Durable),
        "buffered" => Ok(Durability::Buffered),
        other => Err(format!(
            "unknown durability {other:?} (expected durable | buffered; sync and batched \
             are accepted as legacy aliases for durable)"
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
        Command::Quarantine(q) => match q {
            QuarantineCommand::List { wab_dir } => cmd_quarantine_list(&wab_dir, json),
            QuarantineCommand::Inspect { wab_dir, segment } => {
                cmd_quarantine_inspect(&wab_dir, &segment, json)
            }
            QuarantineCommand::Requeue {
                wab_dir,
                socket,
                durability,
                yes,
            } => cmd_quarantine_requeue(&wab_dir, &socket, durability, yes, json),
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

/// Walks `wab_dir`, classifying each shard directory's segment files by suffix
/// (`.wab.confirmed` > `.wab.sealed` > `.wab`, longest-suffix first so the bare
/// `.wab` test can't shadow the others) and rolling up the dead-letter sibling
/// through the same `dl_segments` filter the `dl` commands use. Returns the
/// shards (sorted by name) plus the dead-letter `(file_count, bytes)`. Factored
/// out of `cmd_segments` so this load-bearing accounting is unit-testable
/// without capturing stdout. Note: `confirmed` markers contribute 0 bytes (only
/// active + sealed segments hold live data).
fn scan_segments(wab_dir: &Path) -> Result<(Vec<ShardStat>, u64, u64), String> {
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
    Ok((shards, dl_files, dl_bytes))
}

fn cmd_segments(wab_dir: &Path, json: bool) -> Result<(), String> {
    let (shards, dl_files, dl_bytes) = scan_segments(wab_dir)?;

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
    let mut reader =
        SegmentReader::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for (i, rec) in reader.by_ref().enumerate() {
        match rec {
            Ok(p) => out.push(p),
            Err(e) => return Err(format!("{}: record {i}: {e}", path.display())),
        }
    }
    // A torn/unsealed tail (no clean-end sentinel) means the segment was not
    // fully sealed — e.g. a `.wab.sealed` truncated by disk damage (the
    // MissingFooter case the Explorer surfaces). Requeue must NOT push the
    // readable prefix and then delete the file: that silently drops a damaged
    // segment the daemon's own recovery would QUARANTINE for inspection. Skip it
    // wholesale (left in place + flagged), mirroring the corrupt-record path.
    if reader.terminated_cleanly() != Some(true) {
        return Err(format!(
            "{}: torn/unsealed tail (no clean-end sentinel) — left in place for inspection",
            path.display()
        ));
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
        println!("re-run with --yes to confirm. {DL_REQUEUE_DUPLICATE_WARNING}");
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

// ── Quarantine ───────────────────────────────────────────────────────────────

fn quarantine_dir(wab_dir: &Path) -> PathBuf {
    wab_dir.join("quarantine")
}

/// Every quarantined segment with its size, sorted by name.
///
/// A missing directory yields an empty list, not an error: no quarantine dir is
/// the normal, healthy state.
///
/// **The extension is NOT `.wab.sealed` only** — that assumption would hide
/// exactly the segments this command exists for. Quarantine names are
/// `{shard_name}__{original_file_name}` (`recovery.rs` `quarantine` /
/// `copy_to_quarantine`), and there are two producers writing different
/// extensions:
///
/// - **crash recovery** processes only files ending in `EXT_ACTIVE` (`.wab`),
///   so its copies are `shard_00__seg_00000001.wab`. This is the mid-file
///   corruption case — the one where acked records sit AFTER the corruption,
///   which is the entire premise of this feature;
/// - **the drain** quarantines sealed segments, so its copies end `.wab.sealed`.
///
/// On top of that, `non_clobbering_dest` appends `.1` … `.10000` on a name
/// collision, to *either* form. So `shard_00__seg_00000001.wab.sealed.1` is a
/// legal quarantined name.
///
/// Match the `.wab` / `.wab.sealed` stem with an optional numeric suffix. Do not
/// tighten this to one extension — see
/// `quarantine_list_finds_both_extensions_and_collision_suffixes` in the tests.
fn quarantine_segments(q_dir: &Path) -> Result<Vec<(PathBuf, u64)>, String> {
    let entries = match std::fs::read_dir(q_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {e}", q_dir.display())),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        // Mirrors `dl_segments_filtered`'s `p.is_file()` guard: quarantine/ is
        // only ever populated by `quarantine()` / `copy_to_quarantine()`, which
        // write files, but a name-matching directory (however unlikely) must not
        // be offered up as a segment to inspect/requeue.
        let is_segment = path.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(is_quarantined_segment_name);
        if !is_segment {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        out.push((path, size));
    }
    out.sort();
    Ok(out)
}

/// Whether a quarantine directory entry is a preserved WAB segment.
///
/// Accepts `…wab` and `…wab.sealed`, each optionally followed by the `.N`
/// collision suffix `non_clobbering_dest` adds. Anything else in the directory
/// (an operator's notes, a partial copy) is ignored rather than offered up for
/// requeue.
fn is_quarantined_segment_name(name: &str) -> bool {
    // Strip a trailing `.<digits>` collision suffix, if any, then require one of
    // the two segment extensions. Named constants, not string literals, so a
    // future extension change shows up here without a grep (M2).
    let stem = match name.rsplit_once('.') {
        Some((head, tail)) if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) => head,
        _ => name,
    };
    stem.ends_with(weir_wab::format::EXT_ACTIVE) || stem.ends_with(weir_wab::format::EXT_SEALED)
}

/// The `{shard_name}` prefix of a quarantine entry name
/// (`{shard_name}__{original_file_name}`), if the `__` separator is present.
fn origin_shard(name: &str) -> Option<&str> {
    name.split_once("__").map(|(shard, _)| shard)
}

/// The machine-readable form of `quarantine list`: one object per segment
/// (name, bytes, origin shard) plus a count/total rollup. Mirrors
/// `dl_list_json`'s shape — an empty store is a valid result (empty array,
/// zero totals), not a special case.
fn quarantine_list_json(q_dir: &Path, segs: &[(PathBuf, u64)]) -> serde_json::Value {
    let total: u64 = segs.iter().map(|(_, s)| *s).sum();
    let segments: Vec<serde_json::Value> = segs
        .iter()
        .map(|(p, sz)| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            serde_json::json!({
                "segment": name,
                "bytes": sz,
                "origin_shard": origin_shard(name),
            })
        })
        .collect();
    serde_json::json!({
        "quarantine_dir": q_dir.display().to_string(),
        "count": segs.len(),
        "total_bytes": total,
        "segments": segments,
    })
}

/// The human-readable `quarantine list` table, as printable lines. Pure — no
/// I/O — mirrors how `summary_warnings` / `print_summary` are split, so the
/// rendering logic is unit-testable without capturing stdout.
fn quarantine_list_lines(q_dir: &Path, segs: &[(PathBuf, u64)]) -> Vec<String> {
    if segs.is_empty() {
        return vec![format!("quarantine is empty ({})", q_dir.display())];
    }
    let mut lines = vec![format!("{:<44} {:>12}  origin shard", "segment", "bytes")];
    let mut total = 0u64;
    for (p, sz) in segs {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        lines.push(format!(
            "{name:<44} {:>12}  {}",
            fmt_bytes(*sz),
            origin_shard(name).unwrap_or("?")
        ));
        total += sz;
    }
    lines.push(format!(
        "{:<44} {:>12}",
        format!("total ({})", segs.len()),
        fmt_bytes(total)
    ));
    lines.push(
        "run `weir-ctl quarantine inspect --wab-dir <dir> <segment>` on each before deciding \
         whether to requeue or discard — a segment's presence here does not say how much of it \
         is recoverable."
            .to_string(),
    );
    lines
}

/// Writes `value` as pretty JSON to `out`, with the same "fall back to the
/// compact form on an impossible serialize failure" behavior as the shared
/// `print_json` — which is hardcoded to real stdout and so isn't usable here,
/// since quarantine's writer-injected commands (below) need every branch to go
/// through `out` so a test can capture exactly what an operator would see.
fn write_json_to(out: &mut impl Write, value: &serde_json::Value) {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    let _ = writeln!(out, "{text}");
}

/// `cmd_quarantine_list`'s actual logic, writing through `out` instead of
/// directly to stdout. Split out (I3) so a test can capture exactly what an
/// operator would see — including whether anything is printed at all — rather
/// than only checking `.is_ok()`, which stays green even if this function is
/// gutted to do nothing.
fn quarantine_list_write(wab_dir: &Path, json: bool, out: &mut impl Write) -> Result<(), String> {
    ensure_wab_dir(wab_dir)?;
    let q_dir = quarantine_dir(wab_dir);
    let segs = quarantine_segments(&q_dir)?;

    if json {
        write_json_to(out, &quarantine_list_json(&q_dir, &segs));
        return Ok(());
    }
    for line in quarantine_list_lines(&q_dir, &segs) {
        let _ = writeln!(out, "{line}");
    }
    Ok(())
}

fn cmd_quarantine_list(wab_dir: &Path, json: bool) -> Result<(), String> {
    quarantine_list_write(wab_dir, json, &mut std::io::stdout())
}

/// One record `RecoveryReader` stepped over. Carries `declared_len` and
/// `reason` alongside `offset` (I2): offsets alone cannot distinguish a
/// 1-record loss from a corrupted length that tiled to a clean end and
/// swallowed several intact records along the way — Task 1's As-built section
/// is explicit that "the `Skipped` names the exact byte range", and this is the
/// consumer that must actually surface it, not just the offset that range
/// starts at.
#[derive(Debug)]
struct SkippedRecord {
    offset: u64,
    declared_len: u32,
    reason: String,
}

/// What a forensic read of one quarantined segment found.
#[derive(Debug)]
struct QuarantineReport {
    recovered: usize,
    skipped: usize,
    desynced: bool,
    skipped_records: Vec<SkippedRecord>,
    desync_reason: Option<String>,
}

/// One full forensic pass over a quarantined segment: every payload that
/// verified (in submission order) plus the same tallies
/// [`quarantine_inspect_report`] exposes.
///
/// `inspect` only needs the tallies; `requeue` additionally needs the actual
/// payloads to push. Both read through the SAME match arms — in particular the
/// `#[non_exhaustive]` wildcard arm below — rather than two copies that could
/// silently drift apart (an unknown future `RecoveryItem` variant counted as
/// recovered by one path and skipped by the other would make `list`/`inspect`
/// and `requeue` disagree about what is recoverable).
///
/// Unlike `read_segment_records` (used for dead-letter segments), this never
/// bails out on the first corrupt record: every quarantined segment is corrupt
/// by definition (that is why it is here), so the whole point is to see what
/// survives on both sides of the damage.
fn quarantine_read(path: &Path) -> Result<(Vec<Payload>, QuarantineReport), String> {
    let reader = weir_wab::RecoveryReader::open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut records = Vec::new();
    let mut r = QuarantineReport {
        recovered: 0,
        skipped: 0,
        desynced: false,
        skipped_records: Vec::new(),
        desync_reason: None,
    };
    for item in reader {
        match item {
            weir_wab::RecoveryItem::Record(payload) => {
                r.recovered += 1;
                records.push(payload);
            }
            weir_wab::RecoveryItem::Skipped {
                offset,
                declared_len,
                reason,
            } => {
                r.skipped += 1;
                r.skipped_records.push(SkippedRecord {
                    offset,
                    declared_len,
                    reason,
                });
            }
            weir_wab::RecoveryItem::Desynced { reason, .. } => {
                r.desynced = true;
                r.desync_reason = Some(reason);
            }
            // `RecoveryItem` is #[non_exhaustive] (it ships in a published
            // crate), so this arm is REQUIRED to compile. Counting an unknown
            // variant as recovered would overstate what requeue can deliver, and
            // counting it as skipped would understate it — so it is neither.
            other => {
                return Err(format!(
                    "unknown RecoveryItem variant {other:?} — weir-ctl is older \
                     than the weir-wab it is reading; upgrade weir-ctl before \
                     trusting this report"
                ));
            }
        }
    }
    Ok((records, r))
}

/// Reads `path` with [`weir_wab::RecoveryReader`] and tallies what it finds,
/// discarding the payloads. See [`quarantine_read`] for the shared pass.
fn quarantine_inspect_report(path: &Path) -> Result<QuarantineReport, String> {
    quarantine_read(path).map(|(_records, report)| report)
}

/// The machine-readable form of `quarantine inspect`. The `note` differs by
/// outcome (M3) — a `desynced: true` report and a `desynced: false` report do
/// not carry the same warning, so a script reading only `desynced` is never
/// handed a `note` that contradicts the data beside it.
fn quarantine_inspect_json(segment: &str, report: &QuarantineReport) -> serde_json::Value {
    let skipped_records: Vec<serde_json::Value> = report
        .skipped_records
        .iter()
        .map(|r| {
            serde_json::json!({
                "offset": r.offset,
                "declared_len": r.declared_len,
                "reason": r.reason,
            })
        })
        .collect();
    let note = if report.desynced {
        "desynced=true means the reader could not establish where the next record began past \
         this point. Records after it are NOT recoverable by this tool — this forensic copy is \
         the only place they could still exist."
    } else {
        "desynced=false means every byte in the segment was accounted for; it does NOT mean \
         every record was recovered. A length corrupted to a plausible value can swallow intact \
         records inside its declared byte range without desyncing — see skipped_records for what \
         verification actually caught."
    };
    serde_json::json!({
        "segment": segment,
        "recovered": report.recovered,
        "skipped": report.skipped,
        "skipped_records": skipped_records,
        "desynced": report.desynced,
        "desync_reason": report.desync_reason,
        "note": note,
    })
}

/// The human-readable `quarantine inspect` report, as printable lines. Pure —
/// mirrors `quarantine_list_lines`. Each skipped record prints its byte range
/// (I2) — `offset..offset+8+declared_len`, the 8 being the length+CRC fields —
/// and its `reason`, so "see the skipped records above" (below) is an
/// operator-actionable pointer instead of a bare offset that can't distinguish
/// a 1-record loss from a range that swallowed several.
fn quarantine_inspect_lines(segment: &str, report: &QuarantineReport) -> Vec<String> {
    let mut lines = vec![segment.to_string()];
    lines.push(format!(
        "  recovered: {} record(s) verified — safe to re-deliver",
        report.recovered
    ));
    lines.push(format!(
        "  skipped:   {} record(s) failed verification",
        report.skipped
    ));
    for r in &report.skipped_records {
        let end = r.offset + 8 + r.declared_len as u64;
        lines.push(format!(
            "    - offset {}, declared_len {} bytes (range {}..{end}) — {}",
            r.offset, r.declared_len, r.offset, r.reason
        ));
    }
    if let Some(reason) = &report.desync_reason {
        // I1/I3-b: this is the state an operator most needs the truth in, and
        // the property that must survive any future rewording is this
        // sentence's claim — "not recoverable by this tool" — not its exact
        // phrasing. `quarantine_inspect_reports_a_desync` pins the claim, not
        // the prose.
        lines.push(format!("  desynced: {reason}"));
        lines.push(
            "  the reader could not tell where the next record began past that point. Records \
             after it are NOT recoverable by this tool — this forensic copy is the only place \
             they could still exist, so do not discard it on the strength of this report alone."
                .to_string(),
        );
    } else {
        // I3-b: the property that must survive rewording is that this denies
        // "every record was recovered" — not this exact paragraph.
        lines.push(
            "  clean end: every byte in this segment is accounted for. That is NOT the same as \
             \"every record was recovered\" — a length corrupted to a plausible value can tile \
             exactly to the end of the file and swallow intact records inside its declared byte \
             range without ever desyncing. See the skipped records above for what verification \
             actually caught; a clean end alone is not license to delete this segment."
                .to_string(),
        );
    }
    lines
}

/// `cmd_quarantine_inspect`'s actual logic, writing through `out` instead of
/// directly to stdout — same rationale as `quarantine_list_write` (I3, I1):
/// this is the function a test drives to observe the printed desync/clean-end
/// verdict, rather than only the `QuarantineReport`'s fields.
fn quarantine_inspect_write(
    wab_dir: &Path,
    segment: &str,
    json: bool,
    out: &mut impl Write,
) -> Result<(), String> {
    ensure_wab_dir(wab_dir)?;
    let q_dir = quarantine_dir(wab_dir);
    let path = q_dir.join(segment);
    let report = quarantine_inspect_report(&path)?;

    if json {
        write_json_to(out, &quarantine_inspect_json(segment, &report));
        return Ok(());
    }
    for line in quarantine_inspect_lines(segment, &report) {
        let _ = writeln!(out, "{line}");
    }
    Ok(())
}

fn cmd_quarantine_inspect(wab_dir: &Path, segment: &str, json: bool) -> Result<(), String> {
    quarantine_inspect_write(wab_dir, segment, json, &mut std::io::stdout())
}

// ── Quarantine: requeue ──────────────────────────────────────────────────────
//
// THE DESTRUCTIVE ONE. `requeue` reads quarantined segments, pushes their
// recoverable records back through the daemon's socket, and deletes each
// segment only once every one of its records has been ACCEPTED — not merely
// pushed. A quarantined segment is often the ONLY surviving copy of
// acked-durable records, so the ordering rule is absolute: if any push fails,
// is Nacked, or the connection drops, the segment stays on disk. Weir's crown
// invariant is that an ack is never a false ack; this command is the same
// promise pointed the other way.
//
// Two deliberate inversions of `dl requeue`, not bugs to "fix" into
// consistency:
//
// 1. `dl requeue` skips a corrupt segment WHOLESALE so a corrupt segment is
//    never partially delivered. Every quarantined segment is corrupt by
//    definition, so that rule would recover nothing here — the corrupt
//    RECORD is skipped instead (via `RecoveryReader`) and the rest of the
//    segment is recovered. That inversion is the entire point of this
//    command.
// 2. A segment that desyncs is not deleted, even with `--yes`, however many
//    records were recovered before the desync point. Bytes past that point
//    were never reached — recovery quarantined the WHOLE file precisely
//    because it could not rule out a trailing tail — and no report can
//    substitute for records nobody has read. The recoverable prefix IS still
//    requeued; only the delete is withheld. (Review round 1, Important 1: the
//    original guard only checked `records.is_empty()`, which protected a
//    desync before the first record but deleted the file — with unread bytes
//    still inside it — the moment even one good record preceded the
//    corruption. Crash recovery's two mid-file quarantine reasons are a CRC
//    mismatch, which `Skipped`s and reads on, and an oversized `payload_len`,
//    which desyncs — and a good record before an oversized-length corruption
//    is not an edge case.)

/// What a requeue would do, computed without connecting or mutating anything.
struct QuarantineRequeuePlan {
    segments: usize,
    total_records: usize,
    total_skipped: usize,
    segments_desynced: usize,
    /// Segments whose forensic read itself failed (open error, unparseable
    /// header) — distinct from `segments_desynced`, which read fine but lost
    /// the record framing partway through. Collected rather than aborting the
    /// whole plan (review round 1, Minor 2): `quarantine/` is exactly the
    /// directory most likely to contain an operator-dropped junk file, and
    /// one such file must not hide every other segment's plan.
    unreadable: Vec<String>,
}

/// Computes [`QuarantineRequeuePlan`] by forensically reading (never mutating)
/// every quarantined segment under `q_dir`. Shared by the dry-run printout and
/// its `--json` twin, so the two can never disagree about the numbers.
fn quarantine_requeue_plan(q_dir: &Path) -> Result<QuarantineRequeuePlan, String> {
    let mut plan = QuarantineRequeuePlan {
        segments: 0,
        total_records: 0,
        total_skipped: 0,
        segments_desynced: 0,
        unreadable: Vec::new(),
    };
    for (path, _sz) in quarantine_segments(q_dir)? {
        plan.segments += 1;
        match quarantine_inspect_report(&path) {
            Ok(r) => {
                plan.total_records += r.recovered;
                plan.total_skipped += r.skipped;
                if r.desynced {
                    plan.segments_desynced += 1;
                }
            }
            Err(e) => plan.unreadable.push(e),
        }
    }
    Ok(plan)
}

/// The sentence this dry run exists to make explicit: requeueing WILL re-send
/// records that already reached the sink, and a dedup-capable sink will NOT
/// filter them. Shared verbatim between the human and `--json` (`note`)
/// output so the two can't drift into saying different things about the same
/// run — the exact drift class Task 4's own brief warns against (the plan's
/// first commit-message draft claimed the dedup token *would* save you; it
/// does not, because the token covers a batch's boundaries as well as its
/// contents, and a requeue re-batches).
/// What `dl requeue` tells an operator about duplicates, in one place so the
/// human and any future machine-readable output cannot drift apart — the same
/// reason [`QUARANTINE_REQUEUE_DUPLICATE_WARNING`] exists.
///
/// The claim this replaced was true until 2.0.3 and is now the opposite of the
/// truth. `RecordId::for_record` (weir-sink-sdk) hashes a record's WAB segment
/// and index alongside its bytes, and `dl requeue` re-pushes through the socket
/// — so the record lands at a NEW coordinate and presents a NEW key. The drain's
/// own retry of a segment still dedupes, because it re-reads the same
/// coordinate; an operator re-running an interrupted requeue does not.
const DL_REQUEUE_DUPLICATE_WARNING: &str = "Re-delivery is at-least-once: a record may be delivered more than once if the \
     run is interrupted. A dedup-capable sink will NOT filter those duplicates — since 2.0.3 the \
     per-record idempotency key covers a record's WAB coordinate as well as its bytes, and a \
     requeue re-pushes into a new segment, so the sink sees genuinely distinct records.";

const QUARANTINE_REQUEUE_DUPLICATE_WARNING: &str = "requeueing WILL re-send records that already reached the sink: recovery delivered the \
     valid prefix when it sealed it, and that prefix lives in the same file as the preserved \
     tail. A dedup-capable sink will NOT filter these duplicates — the dedup token is derived \
     from a batch's contents AND its boundaries, and a requeue re-batches, so the sink sees \
     genuinely distinct batches and accepts both.";

fn quarantine_requeue_empty_json(dry_run: bool) -> serde_json::Value {
    serde_json::json!({
        "dry_run": dry_run,
        "segments": 0,
        "requeued_records": 0,
        "segments_cleared": 0,
    })
}

fn quarantine_requeue_dry_run_json(plan: &QuarantineRequeuePlan) -> serde_json::Value {
    serde_json::json!({
        "dry_run": true,
        "segments": plan.segments,
        "requeuable_records": plan.total_records,
        // Named `_total` (review round 3, item 3), matching `done`-JSON's own
        // scalar `skipped_records_total`: `done` also has a `skipped_records`
        // key, but that one is an ARRAY of per-record entries. Same command,
        // same-looking key, two incompatible types was a wart for anything
        // parsing both — there is no array to name here (a dry run never
        // reads far enough to build per-record entries; it only tallies
        // `QuarantineReport.skipped`), so the scalar took the rename instead
        // of the array.
        "skipped_records_total": plan.total_skipped,
        "segments_desynced": plan.segments_desynced,
        "unreadable_segments": plan.unreadable.len(),
        "note": QUARANTINE_REQUEUE_DUPLICATE_WARNING,
    })
}

/// One quarantined segment's skipped records, from a REAL run — carried
/// through to the real run's human output and its `--json` shape (review
/// round 2, N2). The dry run already surfaced the aggregate skip count and
/// pointed at `quarantine inspect`; the real run said nothing at all, so an
/// operator who runs `--yes` directly (or a script that never sees the dry
/// run) learned nothing about records that were just permanently destroyed —
/// the same information-asymmetry class Important 1 (round 1) was about,
/// which that round closed for desyncs but not for skips. Reuses Task 3's
/// `SkippedRecord` (offset/declared_len/reason) rather than a parallel type.
struct RequeueSkippedSegment {
    segment: String,
    records: Vec<SkippedRecord>,
}

/// Tally of one real `requeue --yes` run, built up as segments are processed.
/// Passed to [`quarantine_requeue_done_json`] as a single unit instead of a
/// growing positional-argument list.
struct QuarantineRequeueOutcome {
    segments: usize,
    requeued_records: u64,
    segments_cleared: usize,
    segments_desynced_left: usize,
    segments_empty_left: usize,
    unreadable_segments: usize,
    delete_failures: usize,
    skipped_records_total: u64,
    skipped_segments: Vec<RequeueSkippedSegment>,
}

fn quarantine_requeue_done_json(
    outcome: &QuarantineRequeueOutcome,
    durability: Durability,
) -> serde_json::Value {
    let skipped_records: Vec<serde_json::Value> = outcome
        .skipped_segments
        .iter()
        .flat_map(|s| {
            s.records.iter().map(move |r| {
                serde_json::json!({
                    "segment": s.segment,
                    "offset": r.offset,
                    "declared_len": r.declared_len,
                    "reason": r.reason,
                })
            })
        })
        .collect();
    serde_json::json!({
        "dry_run": false,
        "segments": outcome.segments,
        "requeued_records": outcome.requeued_records,
        "segments_cleared": outcome.segments_cleared,
        "segments_desynced_left_in_place": outcome.segments_desynced_left,
        "segments_empty_left_in_place": outcome.segments_empty_left,
        "unreadable_segments": outcome.unreadable_segments,
        "delete_failures": outcome.delete_failures,
        "skipped_records_total": outcome.skipped_records_total,
        "skipped_records": skipped_records,
        "durability": format!("{durability:?}"),
    })
}

/// `cmd_quarantine_requeue`'s actual logic, writing through `out` instead of
/// directly to stdout — same rationale as `quarantine_list_write` /
/// `quarantine_inspect_write` (I3, I1): the dry run's duplicate-count warning
/// must actually reach the operator, not merely exist in a struct, and only a
/// test that captures the printed text can pin that.
fn quarantine_requeue_write(
    wab_dir: &Path,
    socket: &Path,
    durability: Durability,
    yes: bool,
    json: bool,
    out: &mut impl Write,
) -> Result<(), String> {
    // Refused unconditionally, before touching the filesystem or the socket,
    // for both the dry run and the real run (review round 1, Minor 1): a
    // `Buffered` ack means only "entered the daemon's in-memory queue", not
    // durably written (`weir-core/src/durability.rs`), and this command
    // deletes the quarantined segment — often the only surviving copy of the
    // record — once every push is "accepted". A crash between that ack and
    // the next fsync would lose the record for good with nothing left to
    // recover it from. `dl requeue` accepts this risk for dead-lettered
    // records and documents it in a comment; quarantine's stakes are strictly
    // higher (dead-letter usually isn't the LAST copy of anything — that is
    // this whole feature's premise), so this command refuses rather than
    // merely documents. `sync`/`batched` both fsync before acking and are
    // unaffected.
    if durability == Durability::Buffered {
        return Err(
            "quarantine requeue refuses --durability buffered: its ack means only \"entered \
             the in-memory queue\", not durably written, and this command deletes the \
             quarantined segment — often the only surviving copy of the record — once every \
             push is accepted. A crash between that ack and the next fsync would lose the \
             record for good. Use sync or batched (the default)."
                .to_string(),
        );
    }

    ensure_wab_dir(wab_dir)?;
    let q_dir = quarantine_dir(wab_dir);
    let segs = quarantine_segments(&q_dir)?;

    if segs.is_empty() {
        if json {
            write_json_to(out, &quarantine_requeue_empty_json(!yes));
        } else {
            let _ = writeln!(out, "quarantine is empty; nothing to requeue");
        }
        return Ok(());
    }

    // Dry run: forensically read every segment (no connect, no mutation) and
    // report what WOULD be requeued.
    if !yes {
        let plan = quarantine_requeue_plan(&q_dir)?;
        if json {
            write_json_to(out, &quarantine_requeue_dry_run_json(&plan));
            return Ok(());
        }
        let _ = writeln!(
            out,
            "would requeue {} record(s) from {} quarantined segment(s) under {} through {}",
            plan.total_records,
            plan.segments,
            q_dir.display(),
            socket.display(),
        );
        let _ = writeln!(out, "{QUARANTINE_REQUEUE_DUPLICATE_WARNING}");
        if plan.total_skipped > 0 {
            let _ = writeln!(
                out,
                "\n⚠ {} record(s) across these segments failed verification and will be \
                 SKIPPED — run `weir-ctl quarantine inspect` on each segment for the byte \
                 ranges.",
                plan.total_skipped
            );
        }
        if plan.segments_desynced > 0 {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) desynced during this read; records past the desync point \
                 are not recoverable by this tool. EVERY segment that desyncs is left in place \
                 with --yes, however many records were recovered before the desync point — \
                 those records ARE still requeued, but the file itself is kept, because \
                 deleting it would destroy the unread bytes recovery quarantined it to \
                 preserve.",
                plan.segments_desynced
            );
        }
        if !plan.unreadable.is_empty() {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) could not be read at all and would be SKIPPED:\n  {}",
                plan.unreadable.len(),
                plan.unreadable.join("\n  ")
            );
        }
        let _ = writeln!(out, "\nre-run with --yes to confirm.");
        return Ok(());
    }

    // Real run. Connect once, then requeue segment-by-segment. Push every
    // recoverable record from a segment BEFORE touching its file, and delete
    // the segment only if every one of those pushes was accepted AND the read
    // reached a clean end (no desync) — see the module-level comment above
    // for why both halves of that rule are non-negotiable.
    let mut client = connect_client(socket)?;

    let mut total_requeued: u64 = 0;
    let mut segments_cleared: usize = 0;
    let mut segments_desynced_left: Vec<String> = Vec::new();
    let mut segments_empty_left: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    let mut delete_failures: Vec<String> = Vec::new();
    // Review round 2, N2: carried through regardless of a segment's OTHER
    // outcome — a segment can be fully deleted (the common case: some records
    // Skipped, the rest accepted) and still have lost real, CRC-valid data
    // that the operator was never told about in this run's own output.
    let mut total_skipped_records: u64 = 0;
    let mut skipped_segments: Vec<RequeueSkippedSegment> = Vec::new();

    for (path, _sz) in &segs {
        let (records, mut report) = match quarantine_read(path) {
            Ok(v) => v,
            Err(e) => {
                // The segment itself couldn't be opened/parsed — distinct from
                // a desync (which reads fine and then loses framing partway
                // through). Collect and move on (review round 1, Minor 2):
                // `quarantine/` is exactly the directory most likely to hold
                // an operator-dropped junk file, and one such file must not
                // block every other segment from being requeued.
                unreadable.push(e);
                continue;
            }
        };

        for (i, rec) in records.iter().enumerate() {
            if let Err(e) = client.push(rec.as_ref(), durability) {
                // A push failure or Nack is operational (daemon down / rejecting
                // the record). Abort the whole run rather than hammering a
                // failing daemon. THE data-loss guard: this segment — and every
                // segment not yet reached — stays on disk. The records pushed
                // from THIS segment so far (i of them) may duplicate on the
                // next run, which is within the at-least-once contract; losing
                // the remaining unpushed records because we deleted the file
                // anyway would not be.
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

        // Review round 2, N2: record what THIS segment's read skipped, before
        // deciding the segment's fate below — a segment with skips can still
        // end up fully deleted (the ordinary case: recovery quarantined it
        // for exactly one corrupt record among several good ones), and that
        // is precisely the run an operator most needs this information from.
        // `report.skipped_records` is moved out here; `report.skipped` (a
        // count) and `report.desynced` stay available below (Copy fields).
        if report.skipped > 0 {
            total_skipped_records += report.skipped as u64;
            skipped_segments.push(RequeueSkippedSegment {
                segment: path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string(),
                records: std::mem::take(&mut report.skipped_records),
            });
        }

        // Review round 1, Important 1: a desync means bytes past that point
        // were never reached, REGARDLESS of how many records were recovered
        // (and just pushed, above) before it. The file is the only place
        // those unread bytes exist; deleting it destroys them with nothing to
        // show for what was lost. This check comes FIRST and is independent
        // of `records.is_empty()` — the original bug was gating it on that.
        if report.desynced {
            segments_desynced_left.push(format!(
                "{}: desynced after {} record(s) recovered and requeued above ({} skipped) — \
                 unread bytes remain past the desync point and were NOT deleted",
                path.display(),
                records.len(),
                report.skipped
            ));
            continue;
        }

        if records.is_empty() {
            // A clean end with nothing recovered — every record in the
            // segment was individually corrupt (Skipped), but the reader
            // never lost the framing. Deleting a segment that yielded
            // nothing destroys the only forensic copy with nothing to show
            // for it: leave it in place and report it.
            segments_empty_left.push(format!(
                "{}: no recoverable records ({} skipped, clean end)",
                path.display(),
                report.skipped
            ));
            continue;
        }

        // Every record in this segment was ACCEPTED (the loop above would have
        // returned on the first failure) and the read reached a clean end.
        // Only now is it safe to delete. review round 2, N2: this is also the
        // ordinary path for a segment with SOME `Skipped` records and the
        // rest accepted — deleting it is the already-adjudicated, correct
        // behaviour (a flag gating on `skipped > 0` would fire on every
        // quarantined segment, since that is the whole reason each one is
        // here); what was missing was telling the operator it happened, which
        // the `skipped_segments` collection above now does regardless of this
        // branch.
        match std::fs::remove_file(path) {
            Ok(()) => segments_cleared += 1,
            Err(e) => delete_failures.push(format!("{}: {e}", path.display())),
        }
    }

    let outcome = QuarantineRequeueOutcome {
        segments: segs.len(),
        requeued_records: total_requeued,
        segments_cleared,
        segments_desynced_left: segments_desynced_left.len(),
        segments_empty_left: segments_empty_left.len(),
        unreadable_segments: unreadable.len(),
        delete_failures: delete_failures.len(),
        skipped_records_total: total_skipped_records,
        skipped_segments,
    };

    if json {
        write_json_to(out, &quarantine_requeue_done_json(&outcome, durability));
    } else {
        let _ = writeln!(
            out,
            "requeued {total_requeued} record(s) from {segments_cleared} quarantined \
             segment(s) through {} ({durability:?})",
            socket.display(),
        );
        // Review round 2, N2: printed unconditionally when any record was
        // skipped THIS run — including for segments that were otherwise fully
        // deleted above, not only the ones left in place below. Mirrors
        // `quarantine_inspect_lines`' per-record byte-range format so the
        // detail an operator gets here is the same they'd get from
        // `quarantine inspect`, just after the fact rather than before.
        if !outcome.skipped_segments.is_empty() {
            let lines: Vec<String> = outcome
                .skipped_segments
                .iter()
                .map(|s| {
                    let ranges: Vec<String> = s
                        .records
                        .iter()
                        .map(|r| {
                            let end = r.offset + 8 + r.declared_len as u64;
                            format!(
                                "offset {}, declared_len {} bytes (range {}..{end}) — {}",
                                r.offset, r.declared_len, r.offset, r.reason
                            )
                        })
                        .collect();
                    format!(
                        "{}: {} record(s) skipped\n      {}",
                        s.segment,
                        s.records.len(),
                        ranges.join("\n      ")
                    )
                })
                .collect();
            let _ = writeln!(
                out,
                "\n⚠ {} record(s) across {} segment(s) failed verification during this run and \
                 were NOT recovered. If a segment was otherwise fully readable it has been \
                 deleted above, and these specific records are now permanently unrecoverable — \
                 this is the already-decided cost of --yes on a segment with any corrupt \
                 record, not a new failure:\n  {}",
                outcome.skipped_records_total,
                outcome.skipped_segments.len(),
                lines.join("\n  ")
            );
        }
        if !segments_desynced_left.is_empty() {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) DESYNCED and were left in place — their recovered records \
                 (if any) were requeued above, but unread bytes past the desync point remain on \
                 disk and were NOT deleted:\n  {}",
                segments_desynced_left.len(),
                segments_desynced_left.join("\n  ")
            );
        }
        if !segments_empty_left.is_empty() {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) yielded no recoverable records and were left in place:\n  {}",
                segments_empty_left.len(),
                segments_empty_left.join("\n  ")
            );
        }
        if !unreadable.is_empty() {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) could not be read at all and were SKIPPED:\n  {}",
                unreadable.len(),
                unreadable.join("\n  ")
            );
        }
        if !delete_failures.is_empty() {
            let _ = writeln!(
                out,
                "\n⚠ {} segment(s) were requeued but could not be deleted (they will requeue \
                 again next run — remove them manually):\n  {}",
                delete_failures.len(),
                delete_failures.join("\n  ")
            );
        }
    }

    // Review round 2, N2 (judgment call, stated explicitly rather than
    // inherited): a nonzero `skipped_records_total` does NOT, on its own,
    // turn this run into a nonzero exit.
    //
    // The operative population is every segment this command DELETES, not
    // every quarantined segment. (Not all quarantined segments carry a skip:
    // an oversized `payload_len` quarantines with `skipped = 0, desynced =
    // true`.) But a delete requires a clean end, and a clean-ended segment can
    // only have been quarantined for a CRC mismatch — which is precisely the
    // thing that surfaces as a skip. So every successful delete carries at
    // least one skip, and treating "some record was skipped somewhere" as a
    // run-level problem would make a normal, fully-successful requeue exit
    // non-zero in the ordinary case — indistinguishable from an actual
    // failure, and exactly the "always fires, so it's noise" trap the
    // decision not to gate `--yes` on `skipped > 0` already rejected once.
    // The information asymmetry that was actually wrong is closed by the
    // human/JSON output above, which is unconditional; a script that cares
    // must inspect `skipped_records_total`, not the exit code, for this
    // specific signal. `problems` below is therefore unchanged by skips —
    // only desyncs, empty segments, unreadable segments, and delete failures
    // (none of which are the expected/adjudicated common case) make the run
    // exit non-zero.
    let mut problems: Vec<String> = Vec::new();
    if !delete_failures.is_empty() {
        problems.push(format!(
            "{} segment(s) were requeued but could not be deleted",
            delete_failures.len()
        ));
    }
    if !segments_desynced_left.is_empty() {
        problems.push(format!(
            "{} quarantined segment(s) desynced and were left in place with unread bytes \
             remaining",
            segments_desynced_left.len()
        ));
    }
    if !segments_empty_left.is_empty() {
        problems.push(format!(
            "{} quarantined segment(s) yielded no recoverable records and were left in place",
            segments_empty_left.len()
        ));
    }
    if !unreadable.is_empty() {
        problems.push(format!(
            "{} quarantined segment(s) could not be read and were left in place",
            unreadable.len()
        ));
    }
    if !problems.is_empty() {
        return Err(problems.join("\n"));
    }
    Ok(())
}

fn cmd_quarantine_requeue(
    wab_dir: &Path,
    socket: &Path,
    durability: Durability,
    yes: bool,
    json: bool,
) -> Result<(), String> {
    quarantine_requeue_write(
        wab_dir,
        socket,
        durability,
        yes,
        json,
        &mut std::io::stdout(),
    )
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
/// `weir_records_ack_total{tier="durable"} 12`).
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

/// The durability-hazard / info warnings for a metrics summary, as printable
/// lines (in priority order). Pure — no I/O — so this decision logic, the CLI's
/// primary operator-foolproofing against silent data loss, is unit-testable
/// without capturing stdout: noop ⇒ acked-then-DISCARDED, flusher panics ⇒ shard
/// offline, fsync failures ⇒ DURABILITY HAZARD, plus a nacked-records info line.
fn summary_warnings(s: &MetricsSummary) -> Vec<String> {
    let mut out = Vec::new();
    if s.sink_type == "noop" {
        out.push(
            "\n⚠ sink: noop — records are acked then DISCARDED, not delivered downstream. \
             Set --sink-type (http/mysql/postgres/clickhouse) to forward records."
                .to_string(),
        );
    }
    if s.panics > 0 {
        out.push(format!(
            "\n⚠ flusher {} — a shard is offline until restart",
            plural(s.panics, "panic", "panics")
        ));
    }
    if s.fsync_failures > 0 {
        out.push(format!(
            "⚠ {} — DURABILITY HAZARD (data may not be on stable storage)",
            plural(s.fsync_failures, "fsync failure", "fsync failures")
        ));
    }
    if s.nacked > 0 {
        out.push(format!(
            "ℹ {} nacked — check producer behaviour / capacity",
            plural(s.nacked, "record", "records")
        ));
    }
    out
}

fn print_summary(body: &str) {
    let s = parse_summary(body);

    // Labels padded to a single consistent width so the values line up.
    println!("── weir ──────────────────────────────────");
    println!(
        "{:<10} accepted {}  ack {}  nack {}",
        "ingest", s.accepted, s.acked, s.nacked
    );
    println!(
        "{:<10} fsync avg {:.2} ms  wab {} on disk",
        "durability",
        s.fsync_avg_ms,
        fmt_bytes(s.wab_bytes)
    );
    println!("{:<10} depth {}", "queue", s.queue_depth);
    println!(
        "{:<10} type: {}  health: {}",
        "sink", s.sink_type, s.sink_health
    );
    println!(
        "{:<10} {} on disk",
        "dead-ltr",
        fmt_bytes(s.dead_letter_bytes)
    );

    for warning in summary_warnings(&s) {
        println!("{warning}");
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
            "# HELP weir_records_accepted ...\nweir_records_accepted_total{tier=\"durable\"} 3"
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
        f.write_all(&weir_wab::format::build_segment_header(
            0xFFFF,
            weir_wab::format::Compression::None,
        ))
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
        f.write_all(&weir_wab::format::build_segment_header(
            0xFFFF,
            weir_wab::format::Compression::None,
        ))
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
    fn read_segment_records_skips_torn_tail_without_sentinel() {
        use std::io::Write;
        // A `.wab.sealed` whose records are all CRC-valid but which has NO
        // clean-end sentinel (truncated tail / never fully sealed). requeue must
        // SKIP it (return Err -> left in place + flagged) rather than push the
        // readable prefix and delete the file — that would silently drop a torn
        // segment the daemon would quarantine (F2).
        let dir = std::env::temp_dir().join(format!("weir_ctl_rq_torn_{}", std::process::id()));
        let dl = dir.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        let path = dl.join("dl_00000001.wab.sealed");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&weir_wab::format::build_segment_header(
            0xFFFF,
            weir_wab::format::Compression::None,
        ))
        .unwrap();
        let payload = b"survivor";
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&weir_wab::format::crc32(payload).to_le_bytes())
            .unwrap();
        f.write_all(payload).unwrap();
        f.sync_all().unwrap(); // NO sentinel/footer — torn tail
        let err = read_segment_records(&path).unwrap_err();
        assert!(
            err.contains("torn") || err.contains("sentinel"),
            "torn-tail segment must be skipped, got: {err}"
        );
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
        f.write_all(&weir_wab::format::build_segment_header(
            0xFFFF,
            weir_wab::format::Compression::None,
        ))
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
        cmd_dl_requeue(&wab, bogus, Durability::Durable, true, false).unwrap();
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
        cmd_dl_requeue(&wab, bogus, Durability::Durable, false, false).unwrap();
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
        let err = cmd_dl_requeue(&wab, bogus, Durability::Durable, true, false).unwrap_err();
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
        /// Acks every push.
        fn start(socket: PathBuf) -> Self {
            Self::start_with_nacks(socket, std::collections::HashSet::new())
        }

        /// `nack_at` names the 0-indexed pushes (in arrival order across the
        /// single connection this fake accepts) that get Nacked instead of
        /// Acked — used by the quarantine `requeue` guard tests that need a
        /// real accept/push/Nack round trip, not merely a connect failure
        /// (`/nonexistent.sock` already covers that, and cannot exercise "the
        /// daemon accepted the connection then Nacked a push").
        fn start_with_nacks(socket: PathBuf, nack_at: std::collections::HashSet<usize>) -> Self {
            use std::os::unix::net::UnixListener;
            let listener = UnixListener::bind(&socket).expect("bind fake daemon socket");
            let handle = std::thread::spawn(move || {
                let mut received: Vec<Vec<u8>> = Vec::new();
                let (mut stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(_) => return received,
                };
                let mut n = 0usize;
                // Read frames until the client disconnects (EOF on the header
                // read) or this fake sends a Nack (which, like the real daemon,
                // ends the connection — see `note_nack` in weir-client).
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

                    let nacked = nack_at.contains(&n);
                    let reply = if nacked {
                        weir_core::Envelope::new(
                            weir_core::Header::new(
                                weir_core::MessageType::Nack,
                                header.durability(),
                                0,
                            ),
                            vec![weir_core::NackReason::PayloadTooLarge as u8],
                        )
                    } else {
                        weir_core::Envelope::new(
                            weir_core::Header::new(
                                weir_core::MessageType::Ack,
                                header.durability(),
                                0,
                            ),
                            Vec::new(),
                        )
                    };
                    n += 1;
                    std::io::Write::write_all(&mut stream, &reply.encode()).expect("write reply");
                    if nacked {
                        break;
                    }
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

        cmd_dl_requeue(&wab, &socket, Durability::Durable, true, false)
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
weir_records_accepted_total{tier=\"durable\"} 5
weir_records_ack_total{tier=\"durable\"} 4
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
        let v = round_trip(&push_json(128, Durability::Durable));
        assert_eq!(v["acked"], serde_json::json!(true));
        assert_eq!(v["bytes"], serde_json::json!(128));
        assert_eq!(v["durability"], serde_json::json!("Durable"));
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

        let done = round_trip(&DlRequeueJson::done(5, 40, 4, 1, 0, Durability::Durable));
        assert_eq!(done["dry_run"], serde_json::json!(false));
        assert_eq!(done["segments"], serde_json::json!(5));
        assert_eq!(done["requeued_records"], serde_json::json!(40));
        assert_eq!(done["segments_cleared"], serde_json::json!(4));
        assert_eq!(done["skipped_segments"], serde_json::json!(1));
        assert_eq!(done["delete_failures"], serde_json::json!(0));
        assert_eq!(done["durability"], serde_json::json!("Durable"));
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

    #[test]
    fn parse_durability_is_case_insensitive_and_rejects_unknown() {
        // "durable" is the canonical 2.0 spelling and must parse.
        assert_eq!(parse_durability("durable").unwrap(), Durability::Durable);
        assert_eq!(parse_durability("DURABLE").unwrap(), Durability::Durable);
        // "sync" and "batched" are kept working as legacy aliases.
        assert_eq!(parse_durability("SYNC").unwrap(), Durability::Durable);
        assert_eq!(parse_durability("batched").unwrap(), Durability::Durable);
        assert_eq!(parse_durability("BuFfErEd").unwrap(), Durability::Buffered);
        let err = parse_durability("fast").unwrap_err();
        assert!(
            err.contains("durable")
                && err.contains("sync")
                && err.contains("batched")
                && err.contains("buffered"),
            "the error must name the canonical `durable` value and the legacy \
             sync/batched aliases, got: {err}"
        );
    }

    #[test]
    fn cli_definition_is_valid() {
        // clap's own structural self-check (catches conflicting flags, bad
        // value_parsers, duplicate names) — a compile-adjacent guard on the CLI.
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    fn summary_with(
        sink_type: &str,
        nacked: u64,
        panics: u64,
        fsync_failures: u64,
    ) -> MetricsSummary {
        MetricsSummary {
            accepted: 0,
            acked: 0,
            nacked,
            fsync_avg_ms: 0.0,
            queue_depth: 0,
            panics,
            fsync_failures,
            dead_letter_bytes: 0,
            wab_bytes: 0,
            sink_health: "healthy".into(),
            sink_type: sink_type.into(),
        }
    }

    #[test]
    fn summary_warnings_flag_the_durability_hazards() {
        // A clean summary on a real sink → no warnings.
        assert!(summary_warnings(&summary_with("http", 0, 0, 0)).is_empty());

        // noop → the acked-then-DISCARDED warning (the loudest data-loss signal).
        let noop = summary_warnings(&summary_with("noop", 0, 0, 0));
        assert!(
            noop.iter().any(|w| w.contains("DISCARDED")),
            "noop sink must warn about discarded records, got {noop:?}"
        );

        // flusher panics → shard-offline; fsync failures → DURABILITY HAZARD.
        let panicked = summary_warnings(&summary_with("http", 0, 2, 0));
        assert!(
            panicked.iter().any(|w| w.contains("offline")),
            "{panicked:?}"
        );
        let hazard = summary_warnings(&summary_with("http", 0, 0, 1));
        assert!(
            hazard.iter().any(|w| w.contains("DURABILITY HAZARD")),
            "{hazard:?}"
        );

        // nacked records → the info line.
        let nacked = summary_warnings(&summary_with("http", 5, 0, 0));
        assert!(nacked.iter().any(|w| w.contains("nacked")), "{nacked:?}");
    }

    #[test]
    fn scan_segments_classifies_suffixes_and_rolls_up_dead_letter() {
        let dir = std::env::temp_dir().join(format!("weir_ctl_scan_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let shard = dir.join("shard_00");
        std::fs::create_dir_all(&shard).unwrap();
        // One of each lifecycle extension. confirmed must add 0 bytes; active +
        // sealed contribute their on-disk size.
        std::fs::write(shard.join("seg_00000003.wab"), vec![0u8; 100]).unwrap();
        std::fs::write(shard.join("seg_00000002.wab.sealed"), vec![0u8; 200]).unwrap();
        std::fs::write(shard.join("seg_00000001.wab.confirmed"), vec![0u8; 9999]).unwrap();
        // A dead-letter sibling with one dl_*.wab segment.
        let dl = dir.join("dead_letter");
        std::fs::create_dir_all(&dl).unwrap();
        std::fs::write(dl.join("dl_00000001.wab"), vec![0u8; 50]).unwrap();

        let (shards, dl_files, dl_bytes) = scan_segments(&dir).unwrap();
        assert_eq!(shards.len(), 1);
        let s = &shards[0];
        assert_eq!((s.active, s.sealed, s.confirmed), (1, 1, 1));
        assert_eq!(
            s.bytes, 300,
            "only active(100) + sealed(200) count; confirmed adds 0 bytes"
        );
        assert_eq!(dl_files, 1);
        assert_eq!(dl_bytes, 50);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn metric_primitives_sum_get_and_active_label() {
        let body = "\
weir_records_accepted_total{tier=\"durable\"} 3
weir_records_accepted_total{tier=\"buffered\"} 4
weir_records_accepted_total 99
weir_queue_depth 7
weir_sink_health{state=\"healthy\"} 1
weir_sink_health{state=\"degraded\"} 0
";
        // sum_metric sums every line sharing the prefix (label series + bare).
        assert_eq!(
            sum_metric(body, "weir_records_accepted_total"),
            3.0 + 4.0 + 99.0
        );
        // get_metric matches the EXACT bare metric (a space after the name), not a
        // labelled sibling and not a longer-named metric.
        assert_eq!(get_metric(body, "weir_queue_depth"), Some(7.0));
        assert_eq!(get_metric(body, "weir_records_accepted_total"), Some(99.0));
        assert_eq!(get_metric(body, "weir_sink_health"), None); // only labelled series
        // active_label returns the label of the series whose value == 1.0.
        assert_eq!(
            active_label(body, "weir_sink_health", "state").as_deref(),
            Some("healthy")
        );
    }

    // ── Quarantine ───────────────────────────────────────────────────────────

    /// A fresh, empty temp directory unique to this test process + label. The
    /// quarantine tests below need both a `wab_dir` and its `quarantine/`
    /// subdirectory per test, so — unlike the older dl tests above, which
    /// inline a single path each — this is worth factoring once.
    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "weir_ctl_quarantine_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Writes a quarantine-style segment fixture at `q_dir/name`: header +
    /// `[len][crc][payload]` per record + sentinel — the same shape Task 1's
    /// `write_segment` test helper builds. No 32-byte footer is written; that is
    /// deliberate, see the module doc on `RecoveryReader`'s footer-less
    /// clean-end check (`recovery_reader.rs:171-172`).
    ///
    /// `corrupt_payload_at`, when `Some(i)`, flips a byte in record `i`'s
    /// payload (leaving its declared length intact) so its CRC fails to verify
    /// — a `Skipped`, not a `Desynced`. `corrupt_len_at`, when `Some(i)`, wrecks
    /// record `i`'s LENGTH field instead — unrecoverable by design, since the
    /// reader can no longer know where the next record begins, so it desyncs.
    fn write_q_segment_inner(
        q_dir: &Path,
        name: &str,
        records: &[&[u8]],
        corrupt_payload_at: Option<usize>,
        corrupt_len_at: Option<usize>,
    ) {
        use std::io::Write;
        std::fs::create_dir_all(q_dir).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&weir_wab::format::build_segment_header(
            0,
            weir_wab::format::Compression::None,
        ));
        for (i, r) in records.iter().enumerate() {
            let mut len = r.len() as u32;
            if corrupt_len_at == Some(i) {
                // A wildly implausible length: the reader cannot locate the
                // next record, so it must give up rather than guess.
                len = u32::MAX - 1;
            }
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(r).to_le_bytes());
            let mut payload = r.to_vec();
            if corrupt_payload_at == Some(i) {
                payload[0] ^= 0xff; // CRC now mismatches; length still correct
            }
            buf.extend_from_slice(&payload);
        }
        buf.extend_from_slice(&weir_wab::format::build_sentinel());
        std::fs::File::create(q_dir.join(name))
            .unwrap()
            .write_all(&buf)
            .unwrap();
    }

    fn write_q_segment(q_dir: &Path, name: &str, records: &[&[u8]]) {
        write_q_segment_inner(q_dir, name, records, None, None);
    }

    fn write_q_segment_with_corruption(
        q_dir: &Path,
        name: &str,
        records: &[&[u8]],
        corrupt_payload_at: usize,
    ) {
        write_q_segment_inner(q_dir, name, records, Some(corrupt_payload_at), None);
    }

    /// Review finding I1's fixture: a record whose LENGTH is corrupted to an
    /// implausible value, so `RecoveryReader` desyncs rather than guessing.
    /// The plan's Task 4 sketch names a 3-arg `write_q_segment_bad_length(&q,
    /// name, 0)` that builds its own fixed record set; this one takes the
    /// records explicitly instead, so Task 4 can reuse or thinly wrap it.
    fn write_q_segment_bad_length(
        q_dir: &Path,
        name: &str,
        records: &[&[u8]],
        corrupt_len_at: usize,
    ) {
        write_q_segment_inner(q_dir, name, records, None, Some(corrupt_len_at));
    }

    #[test]
    fn quarantine_list_on_a_missing_dir_is_not_an_error() {
        // No quarantine dir is the normal, healthy case.
        let dir = tmp_dir("q_list_missing");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(cmd_quarantine_list(&dir, false).is_ok());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_list_reports_segments_and_bytes() {
        // I3-a: previously asserted only `.is_ok()` on `cmd_quarantine_list`,
        // so gutting it to print nothing left this test green (review finding
        // I3, mutation M4). Drives `quarantine_list_write` directly — the
        // function `cmd_quarantine_list` is now a one-line wrapper over — and
        // asserts on the bytes it actually wrote.
        let dir = tmp_dir("q_list");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"ab", b"cd"]);

        let mut out = Vec::new();
        quarantine_list_write(&dir, false, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        // N2: the total row also prints "48 B" (there is only one segment, so
        // the total equals it), so `text.contains("48 B")` over the whole
        // listing was satisfied even if the per-segment column were blanked.
        // Require the segment's own row to carry both its name and its size.
        let row = text
            .lines()
            .find(|l| l.contains("shard_00__seg_00000001.wab"))
            .unwrap_or_else(|| panic!("the segment name must appear in the listing, got: {text}"));
        assert!(
            row.contains("shard_00"),
            "the origin shard must be on the segment's own row, got: {row}"
        );
        // header(24) + 2 records of len 2 (4+4+2 each = 10*2 = 20) + sentinel(4).
        assert!(
            row.contains("48 B"),
            "the segment's on-disk size must be on its OWN row, not just the total, got: {row}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_list_json_has_expected_keys_from_tmp_wab() {
        // I3-b: until now the quarantine JSON had no shape test at all —
        // mirrors `dl_list_json_has_expected_keys_from_tmp_wab`. Deleting
        // `note` / `origin_shard` / `skipped_records` (review's M8) left the
        // suite green before this existed.
        let dir = tmp_dir("q_list_json");
        let q = dir.join("quarantine");
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"x"]);
        let segs = quarantine_segments(&q).unwrap();

        let v = round_trip(&quarantine_list_json(&q, &segs));
        assert_eq!(v["count"], serde_json::json!(1));
        assert!(v["total_bytes"].is_u64());
        assert!(v["segments"].is_array());
        assert_eq!(
            v["segments"][0]["segment"],
            serde_json::json!("shard_00__seg_00000001.wab")
        );
        assert_eq!(
            v["segments"][0]["origin_shard"],
            serde_json::json!("shard_00")
        );
        assert!(v["quarantine_dir"].is_string());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn quarantine_list_json_empty_store_is_stable_shape() {
        // Mirrors `dl_list_json_empty_store_is_stable_shape`: an empty store is
        // the same shape as a populated one, not a special case.
        let dir = tmp_dir("q_list_json_empty");
        let v = round_trip(&quarantine_list_json(&dir, &[]));
        assert_eq!(v["count"], serde_json::json!(0));
        assert_eq!(v["total_bytes"], serde_json::json!(0));
        assert_eq!(v["segments"], serde_json::json!([]));
    }

    #[test]
    fn quarantine_list_finds_both_extensions_and_collision_suffixes() {
        // THE regression test for this command. Crash recovery quarantines
        // ACTIVE segments, so its copies end `.wab` — and that is the mid-file
        // corruption case, the one where acked records sit after the corruption.
        // A `.wab.sealed`-only filter lists zero of them and reports success.
        // The drain contributes `.wab.sealed`, and non_clobbering_dest appends
        // `.N` to either on a name collision.
        let dir = tmp_dir("q_list_exts");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"a"]);
        write_q_segment(&q, "shard_01__seg_00000002.wab.sealed", &[b"b"]);
        write_q_segment(&q, "shard_00__seg_00000001.wab.1", &[b"c"]);
        write_q_segment(&q, "shard_01__seg_00000002.wab.sealed.2", &[b"d"]);
        std::fs::write(q.join("operator-notes.txt"), b"not a segment").unwrap();

        let segs = quarantine_segments(&q).unwrap();
        assert_eq!(
            segs.len(),
            4,
            "both extensions and their collision suffixes must be listed, got {segs:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_segments_ignores_a_directory_with_a_segment_like_name() {
        // I3-c: defends the `is_file()` guard added beyond the plan's snippet.
        // quarantine/ is only ever populated with FILES by
        // quarantine()/copy_to_quarantine(), but a same-named directory
        // (however unlikely) must never be offered up as a segment.
        let dir = tmp_dir("q_list_dir_decoy");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab", &[b"a"]);
        std::fs::create_dir_all(q.join("shard_01__seg_00000002.wab")).unwrap();

        let segs = quarantine_segments(&q).unwrap();
        let names: Vec<String> = segs
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["shard_00__seg_00000001.wab"],
            "a directory must never be listed as a segment, got {names:?}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn origin_shard_parses_the_shard_prefix() {
        // I3-c: `origin_shard` had no direct test; mutating it to always
        // return `None` left the suite green.
        assert_eq!(origin_shard("shard_00__seg_00000001.wab"), Some("shard_00"));
        assert_eq!(origin_shard("no_separator.wab"), None);
    }

    #[test]
    fn quarantine_inspect_reports_recovered_and_skipped_counts() {
        // The diagnostic that does not exist today: which records are readable,
        // which are not, and where.
        let dir = tmp_dir("q_inspect");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        let report = quarantine_inspect_report(&q.join(name)).unwrap();
        assert_eq!(report.recovered, 2, "records either side of the corruption");
        assert_eq!(report.skipped, 1);
        assert!(
            !report.desynced,
            "a payload-only corruption must not desync"
        );

        // I2: `declared_len` and `reason` must survive, not just the offset —
        // they are what lets an operator tell a 1-record loss from a range
        // that swallowed several intact records.
        assert_eq!(report.skipped_records.len(), 1);
        let skipped = &report.skipped_records[0];
        assert_eq!(skipped.declared_len, 6, "\"BADREC\" is 6 bytes");
        assert_eq!(skipped.reason, "CRC mismatch");

        // And it must actually reach the printed output, not stop at the struct.
        let lines = quarantine_inspect_lines(name, &report).join("\n");
        assert!(
            lines.contains("declared_len 6"),
            "the printed report must surface declared_len, got: {lines}"
        );
        assert!(
            lines.contains("CRC mismatch"),
            "the printed report must surface the reason, got: {lines}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_inspect_reports_a_desync() {
        // I1: nothing anywhere exercised `desynced == true` before this test.
        // Inverting the `Desynced` arm (r.desynced = false; r.desync_reason =
        // None;) left the suite green and made the binary print the CLEAN END
        // paragraph, exit 0, for a segment whose framing was genuinely lost —
        // the one output state an operator most needs the truth in.
        let dir = tmp_dir("q_inspect_desync");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab";
        write_q_segment_bad_length(&q, name, &[b"good1", b"BADLEN", b"unreachable"], 1);

        let report = quarantine_inspect_report(&q.join(name)).unwrap();
        assert!(
            report.desynced,
            "an implausible length must desync, got {report:?}"
        );
        assert!(
            report.desync_reason.is_some(),
            "the reason must be populated so the operator can act on it"
        );
        assert_eq!(
            report.recovered, 1,
            "only the record before the bad length is recovered"
        );

        let mut out = Vec::new();
        quarantine_inspect_write(&dir, name, false, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap().to_lowercase();
        assert!(
            text.contains("not recoverable by this tool"),
            "the printed report must say the tail past the desync is unreachable, got: {text}"
        );
        assert!(
            !text.contains("clean end"),
            "a desynced segment must NOT print the clean-end paragraph, got: {text}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// Whether `word` appears in `text` as a whole word, not merely as a
    /// substring of some other word ("not" inside "nothing", "note",
    /// "cannot"). Case-sensitive; callers lowercase both sides first. Exists
    /// because a plain `.contains("not")` is satisfied by vocabulary that has
    /// nothing to do with negation (review finding N1).
    fn contains_word(text: &str, word: &str) -> bool {
        text.split(|c: char| !c.is_alphabetic()).any(|w| w == word)
    }

    #[test]
    fn quarantine_inspect_lines_distinguish_accounted_for_from_recovered() {
        // I3-b + N1: the previous version asserted `contains("not") &&
        // contains("recovered")` over the JOINED output. Neither conjunct
        // pinned anything: "recovered" is satisfied by the unrelated
        // `recovered: N record(s)` COUNTER line, present in every report
        // regardless of outcome, and "not" is satisfied by any word merely
        // containing those letters ("nothing", "note", "cannot"). So the
        // precise misreading this whole feature exists to prevent —
        // "clean end: nothing is missing from this segment. It is safe to
        // delete." — passed this test unchanged.
        //
        // Fixed by pinning the verdict line STRUCTURALLY (it is always the
        // last line `quarantine_inspect_lines` pushes, in both branches) and
        // requiring "not" to appear as a whole WORD via `contains_word`, so
        // the assertion can tell the claim from its negation rather than just
        // detecting related vocabulary. This still does not pin verbatim
        // prose — a full rewording that keeps the claim intact (e.g. "the
        // reader accounted for every byte here. That does not imply every
        // record was recovered.") stays green.
        let clean = QuarantineReport {
            recovered: 1,
            skipped: 0,
            desynced: false,
            skipped_records: Vec::new(),
            desync_reason: None,
        };
        let mut clean_lines = quarantine_inspect_lines("seg.wab", &clean);
        let clean_verdict = clean_lines.pop().unwrap().to_lowercase();
        assert!(
            clean_verdict.contains("recover"),
            "the clean-end verdict must speak about recovery, got: {clean_verdict}"
        );
        assert!(
            contains_word(&clean_verdict, "not"),
            "the clean-end verdict must NEGATE 'every record recovered' with the word \"not\", \
             got: {clean_verdict}"
        );

        let desynced = QuarantineReport {
            recovered: 1,
            skipped: 0,
            desynced: true,
            skipped_records: Vec::new(),
            desync_reason: Some("an implausible length".to_string()),
        };
        let mut desync_lines = quarantine_inspect_lines("seg.wab", &desynced);
        let desync_verdict = desync_lines.pop().unwrap().to_lowercase();
        assert!(
            contains_word(&desync_verdict, "not") && desync_verdict.contains("recoverable"),
            "the desync verdict must say records past that point are NOT recoverable, \
             got: {desync_verdict}"
        );
        assert_ne!(
            clean_verdict, desync_verdict,
            "the two outcomes must not print the same verdict"
        );
    }

    #[test]
    fn quarantine_inspect_json_note_differs_by_outcome() {
        // I3-b (JSON shape) + M3: the `note` must not be the same text
        // regardless of outcome — a script reading `desynced: true` must not
        // be handed the clean-end reassurance beside it.
        let clean = QuarantineReport {
            recovered: 2,
            skipped: 1,
            desynced: false,
            skipped_records: vec![SkippedRecord {
                offset: 37,
                declared_len: 6,
                reason: "CRC mismatch".to_string(),
            }],
            desync_reason: None,
        };
        let v = round_trip(&quarantine_inspect_json("seg.wab", &clean));
        assert_eq!(v["recovered"], serde_json::json!(2));
        assert_eq!(v["skipped"], serde_json::json!(1));
        assert_eq!(v["desynced"], serde_json::json!(false));
        assert_eq!(v["skipped_records"][0]["offset"], serde_json::json!(37));
        assert_eq!(
            v["skipped_records"][0]["declared_len"],
            serde_json::json!(6)
        );
        assert_eq!(
            v["skipped_records"][0]["reason"],
            serde_json::json!("CRC mismatch")
        );
        // N1: same fix as the human-wording test above — "not" must be a
        // whole word (via `contains_word`), not a substring any related word
        // would satisfy ("nothing", "note"). Split into two assertions so
        // each conjunct is independently meaningful.
        let clean_note = v["note"].as_str().unwrap().to_lowercase();
        assert!(
            clean_note.contains("recover"),
            "the clean-end note must speak about recovery, got: {clean_note}"
        );
        assert!(
            contains_word(&clean_note, "not"),
            "the clean-end note must NEGATE 'every record recovered' with the word \"not\", \
             got: {clean_note}"
        );

        let desynced = QuarantineReport {
            recovered: 1,
            skipped: 0,
            desynced: true,
            skipped_records: Vec::new(),
            desync_reason: Some("record declares an implausible length".to_string()),
        };
        let v2 = round_trip(&quarantine_inspect_json("seg2.wab", &desynced));
        assert_eq!(v2["desynced"], serde_json::json!(true));
        let desync_note = v2["note"].as_str().unwrap().to_lowercase();
        assert!(
            contains_word(&desync_note, "not") && desync_note.contains("recoverable"),
            "the desync note must say records past that point are NOT recoverable, \
             got: {desync_note}"
        );
        assert_ne!(
            clean_note, desync_note,
            "the two outcomes must not share a note"
        );
    }

    // ── Requeue ──────────────────────────────────────────────────────────────
    //
    // Reuses `FakeDaemon` (defined above, alongside `dl requeue`'s tests) rather
    // than a second copy: `start_with_nacks` was added to it specifically for
    // the guard tests below, which need a real accept/push/Nack round trip — not
    // merely a connect failure (`/nonexistent.sock` already covers that, and
    // cannot exercise "the daemon accepted the connection then Nacked a push").

    fn fake_daemon_socket(label: &str) -> PathBuf {
        // Short prefix, matching `requeue_deletes_sealed_segments_only_after_acks`'s
        // `wctl_rq_*.sock` above: Unix socket paths are length-limited
        // (SUN_LEN, ~104 bytes on macOS), and `std::env::temp_dir()` alone can
        // already eat half that budget.
        std::env::temp_dir().join(format!("wctl_qrq_{label}_{}.sock", std::process::id()))
    }

    #[test]
    fn quarantine_requeue_defaults_to_a_dry_run() {
        let dir = tmp_dir("q_requeue_dry");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        // No socket needed: a dry run must not connect.
        cmd_quarantine_requeue(
            &dir,
            Path::new("/nonexistent.sock"),
            Durability::Durable,
            false,
            false,
        )
        .unwrap();
        assert!(q.join(name).exists(), "a dry run must not delete anything");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_reports_the_already_delivered_prefix() {
        // Requeueing re-sends records that already reached the sink when
        // recovery sealed the valid prefix. That is within the at-least-once
        // contract, but the operator is told the count BEFORE they pass --yes.
        let dir = tmp_dir("q_requeue_dupes");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment_with_corruption(
            &q,
            "shard_00__seg_00000001.wab.sealed",
            &[b"pre1", b"pre2", b"BADREC", b"post1"],
            2,
        );
        let plan = quarantine_requeue_plan(&q).unwrap();
        assert_eq!(
            plan.total_records, 3,
            "3 verifiable records across the corruption"
        );
        assert_eq!(plan.segments_desynced, 0);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_dry_run_prints_the_duplicate_count() {
        // The brief's own load-bearing requirement: "the dry run must print
        // the duplicate count explicitly, so the operator decides rather than
        // discovers." Pinned against `quarantine_requeue_write`'s actual
        // output — not merely the plan struct — the same I3 lesson Task 3
        // already learned: a struct-level assertion stays green even if the
        // printed text is gutted.
        let dir = tmp_dir("q_requeue_print_count");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment_with_corruption(
            &q,
            "shard_00__seg_00000001.wab.sealed",
            &[b"pre1", b"pre2", b"BADREC", b"post1"],
            2,
        );

        let mut out = Vec::new();
        quarantine_requeue_write(
            &dir,
            Path::new("/nonexistent.sock"),
            Durability::Durable,
            false,
            false,
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("3 record"),
            "the dry run must print the exact requeuable-record count (3), got: {text}"
        );
        // I2 (review round 1): `contains("already") || contains("duplicate")`
        // cannot distinguish this claim from its exact WRONG negation — "a
        // dedup-capable sink WILL filter these duplicates for you" also
        // contains "duplicate" and would leave this assertion green. Require
        // the specific negated claim as a whole word ("NOT" next to "filter"),
        // and separately require the wrong reassurance form is absent.
        let lower = text.to_lowercase();
        assert!(
            contains_word(&lower, "not") && lower.contains("filter"),
            "the dry run must state the sink will NOT filter the duplicates, got: {text}"
        );
        assert!(
            !lower.contains("will filter") && !lower.contains("filter these duplicates for you"),
            "the dry run must not contain the wrong reassurance form, got: {text}"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dl_requeue_duplicate_warning_states_the_corrected_dedup_claim() {
        // The sibling `quarantine requeue` warning was corrected and pinned;
        // `dl requeue` was missed and kept asserting the OPPOSITE — that the
        // sink's idempotency key "dedupes identical payloads" — in both its
        // --help text and the line printed immediately before an operator
        // types --yes. True until 2.0.3, false since: RecordId covers a
        // record's WAB coordinate, and a requeue re-pushes into a new segment.
        //
        // Pin the CONSTANT, not merely that something got printed, and use
        // `contains_word` so "not" is not satisfied by "nothing" — the trap
        // the sibling test already documents.
        let w = DL_REQUEUE_DUPLICATE_WARNING.to_lowercase();
        assert!(
            contains_word(&w, "not") && w.contains("filter"),
            "must say a dedup-capable sink will NOT filter these duplicates; got: \
             {DL_REQUEUE_DUPLICATE_WARNING}"
        );
        assert!(
            !w.contains("dedupes identical payloads"),
            "the pre-2.0.3 claim came back: {DL_REQUEUE_DUPLICATE_WARNING}"
        );
        // The reason must survive too, or a later editor deletes the "not" as
        // redundant: name the coordinate.
        assert!(
            w.contains("coordinate"),
            "must say WHY (the key covers the WAB coordinate); got: \
             {DL_REQUEUE_DUPLICATE_WARNING}"
        );
    }

    #[test]
    fn quarantine_requeue_duplicate_warning_states_the_corrected_dedup_claim() {
        // I2 (review round 1): a direct, structural pin on the CONSTANT itself
        // (not just that it got printed) — the plan's original commit-message
        // draft claimed a dedup-capable sink WOULD filter requeue's
        // duplicates; it was proofread-corrected to the opposite, and this is
        // the wording the operator actually reads before passing --yes. This
        // is the same trap class Task 3's round-2 review already fixed once
        // (`contains("not")` matching `"nothing"`) — reusing `contains_word`.
        let w = QUARANTINE_REQUEUE_DUPLICATE_WARNING.to_lowercase();
        assert!(
            contains_word(&w, "not") && w.contains("filter"),
            "must state the sink will NOT filter the duplicates, got: {w}"
        );
        assert!(
            !w.contains("will filter") && !w.contains("filter these duplicates for you"),
            "must not contain the wrong reassurance form, got: {w}"
        );
    }

    #[test]
    fn a_segment_that_desyncs_with_no_records_is_not_deleted() {
        // Nothing to confirm, and deleting it destroys the only forensic copy.
        // (The plan's Task 4 sketch calls `write_q_segment_bad_length(&q, name,
        // 0)` — 3 args — but the helper Task 3 actually built takes an explicit
        // record list too; corrupting record 0's length with nothing before it
        // is the equivalent fixture: zero records recovered before the desync.)
        let dir = tmp_dir("q_requeue_desync");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_bad_length(&q, name, &[b"unreachable"], 0);

        let plan = quarantine_requeue_plan(&q).unwrap();
        assert_eq!(plan.total_records, 0);
        assert_eq!(plan.segments_desynced, 1);

        // Not deleted even WITH --yes: connect to a fake daemon that would Ack
        // anything (there is nothing to push, so it never gets the chance).
        // The overall run still reports non-zero (a segment needing manual
        // attention was left behind — this mirrors `dl requeue`'s aggregated
        // "problems" exit for unreadable segments), but the file itself must
        // survive.
        let socket = fake_daemon_socket("desync");
        let _daemon = FakeDaemon::start(socket.clone());
        let err = cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false)
            .expect_err("a segment with nothing recoverable must be reported, not silently OK");
        assert!(
            err.contains("no recoverable records") || err.contains("left in place"),
            "{err}"
        );
        assert!(
            q.join(name).exists(),
            "a segment that desyncs before yielding any record must survive --yes"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_does_not_delete_a_segment_when_a_push_is_nacked() {
        // THE data-loss guard: a segment is deleted only after EVERY one of its
        // records has been ACCEPTED, not merely pushed. A quarantined segment
        // is often the only surviving copy of acked-durable records, so a
        // false "requeued" that then deletes the file is unrecoverable data
        // loss. This is worth more than every other test in this task
        // combined (brief, "This is the destructive one").
        let dir = tmp_dir("q_requeue_nack");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2", b"good3"], 1);

        // 3 recoverable records (good1, good2, good3); Nack the SECOND push
        // (index 1) so the segment is left with some records already pushed
        // and some not — the exact partial-progress case the ordering rule
        // exists for.
        let socket = fake_daemon_socket("nack");
        let _daemon =
            FakeDaemon::start_with_nacks(socket.clone(), std::collections::HashSet::from([1]));

        let err = cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false)
            .expect_err("a Nacked push must surface as an error, not a silent partial success");
        assert!(
            err.contains("push failed") || err.to_lowercase().contains("nack"),
            "{err}"
        );
        assert!(
            q.join(name).exists(),
            "the segment must NOT be deleted when a push failed/was Nacked partway through"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_yes_deletes_a_segment_once_every_record_is_accepted() {
        // The positive path the guard tests above are guarding: when every
        // recoverable record IS accepted, the segment IS deleted, and the
        // corrupt record itself was correctly excluded from what got pushed.
        let dir = tmp_dir("q_requeue_success");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        let socket = fake_daemon_socket("success");
        let daemon = FakeDaemon::start(socket.clone());

        cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false).unwrap();
        assert!(
            !q.join(name).exists(),
            "every recoverable record was accepted; the segment must be deleted"
        );
        // The corrupt record ("BADREC") must be EXCLUDED from what got pushed —
        // only the two verified records either side of it.
        assert_eq!(
            daemon.into_received(),
            vec![b"good1".to_vec(), b"good2".to_vec()],
            "requeue must push exactly the verified records, in order, and skip the corrupt one"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_never_deletes_a_segment_that_desynced_even_with_records_recovered_first()
    {
        // Review round 1, Important 1 — the finding that mattered: the
        // original guard was `records.is_empty()`, which only protected a
        // desync BEFORE the first record. Crash recovery's OTHER mid-file
        // quarantine reason (an oversized `payload_len` field, as opposed to
        // a CRC mismatch) makes RecoveryReader desync, and it is entirely
        // ordinary for one or more good records to precede that corruption —
        // reproduced live in the review against a real daemon: 4 records sat
        // untouched past the corruption, requeue delivered only the
        // 3-record duplicate prefix, deleted the segment, and exited 0.
        //
        // good1/good2 precede the corrupted length; whatever follows it must
        // never be silently discarded by treating "some records recovered"
        // as "safe to delete".
        let dir = tmp_dir("q_requeue_desync_with_records");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_bad_length(
            &q,
            name,
            &[b"good1", b"good2", b"BADLEN", b"unreachable"],
            2,
        );

        let report = quarantine_inspect_report(&q.join(name)).unwrap();
        assert_eq!(
            report.recovered, 2,
            "the two records before the bad length are genuinely recoverable"
        );
        assert!(
            report.desynced,
            "fixture sanity: the bad length must desync"
        );

        let socket = fake_daemon_socket("desync_with_records");
        let daemon = FakeDaemon::start(socket.clone()); // Acks everything

        let err = cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false)
            .expect_err("a segment that desynced must be reported, not silently OK");
        assert!(
            err.to_lowercase().contains("desync"),
            "the error must name the desync, got: {err}"
        );
        assert!(
            q.join(name).exists(),
            "a segment that desynced must survive --yes, however many records were recovered \
             before the desync point"
        );
        // The recoverable prefix IS still requeued — only the delete is
        // withheld.
        assert_eq!(
            daemon.into_received(),
            vec![b"good1".to_vec(), b"good2".to_vec()],
            "the records recovered before the desync must still be pushed even though the \
             file itself is kept"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_leaves_an_all_skipped_clean_end_segment_undeleted() {
        // Review round 2, N1: the round-1 fix reordered the delete guard to
        // check `report.desynced` FIRST — correct behaviour, but it meant
        // `a_segment_that_desyncs_with_no_records_is_not_deleted`'s fixture
        // (a corrupted LENGTH field) now exits through that new branch and
        // never reaches `records.is_empty()` at all. With
        // `if false && records.is_empty()`, the whole suite stayed green: the
        // "yielded nothing → never delete" guard had no test left holding it,
        // and disabling it silently turns a zero-record segment from
        // `Err(…left in place)` + file on disk into `Ok(())` + file deleted.
        //
        // This fixture reaches that branch the OTHER way: a corrupted PAYLOAD
        // (not length) on the segment's only record. That's a CRC mismatch,
        // which `Skipped`s and reads on to a clean end (the sentinel) rather
        // than desyncing — recovered=0, skipped=1, desynced=false.
        let dir = tmp_dir("q_requeue_all_skipped");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"only"], 0);

        let report = quarantine_inspect_report(&q.join(name)).unwrap();
        assert_eq!(report.recovered, 0, "fixture sanity: nothing recoverable");
        assert_eq!(
            report.skipped, 1,
            "fixture sanity: the one record is skipped"
        );
        assert!(
            !report.desynced,
            "fixture sanity: a CRC mismatch must NOT desync — this must reach the \
             records.is_empty() branch, not the report.desynced one"
        );

        let socket = fake_daemon_socket("all_skipped");
        let daemon = FakeDaemon::start(socket.clone()); // would Ack anything

        let err = cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false)
            .expect_err("a segment with nothing recoverable must be reported, not silently OK");
        assert!(
            err.contains("no recoverable records"),
            "the error must name the actual reason (not a desync), got: {err}"
        );
        assert!(
            q.join(name).exists(),
            "an all-skipped, clean-end segment must survive --yes"
        );
        assert!(
            daemon.into_received().is_empty(),
            "nothing was recoverable, so nothing should have been pushed"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_refuses_buffered_durability() {
        // Review round 1, Minor 1: `Buffered` acks before any fsync
        // (`weir-core/src/durability.rs`), and this command deletes the
        // quarantined segment — often the only surviving copy — once every
        // push is "accepted". Refused unconditionally: before connecting
        // (bogus socket proves it), and even for a dry run, which never
        // connects anyway but must not imply buffered is fine to pass with
        // `--yes` next.
        let dir = tmp_dir("q_requeue_buffered");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        write_q_segment(&q, "shard_00__seg_00000001.wab.sealed", &[b"a"]);
        let bogus = Path::new("/nonexistent.sock");

        let dry_err = cmd_quarantine_requeue(&dir, bogus, Durability::Buffered, false, false)
            .expect_err("buffered must be refused even as a dry run");
        assert!(dry_err.to_lowercase().contains("buffered"), "{dry_err}");

        let real_err = cmd_quarantine_requeue(&dir, bogus, Durability::Buffered, true, false)
            .expect_err("buffered must be refused for the real run");
        assert!(real_err.to_lowercase().contains("buffered"), "{real_err}");

        // Refused before anything was touched.
        assert!(q.join("shard_00__seg_00000001.wab.sealed").exists());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_skips_an_unreadable_segment_but_still_processes_the_rest() {
        // Review round 1, Minor 2: a single unopenable entry (an operator's
        // truncated/junk file — `quarantine/` accepts hand-dropped files same
        // as `dl_requeue`'s dead-letter dir) must not abort the whole plan or
        // the whole real run. `dl requeue` already collects such segments and
        // continues; this mirrors it.
        let dir = tmp_dir("q_requeue_unreadable");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        // Sorts before the good one (`0000000` < `0000001`), so if the loop
        // aborted at the first bad entry the good one would never be reached.
        std::fs::write(q.join("shard_00__seg_00000000.wab.sealed"), b"junk").unwrap();
        let good_name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment(&q, good_name, &[b"real1", b"real2"]);

        // Dry run: must still report the good segment's plan, not abort with
        // nothing printed.
        let mut out = Vec::new();
        quarantine_requeue_write(
            &dir,
            Path::new("/nonexistent.sock"),
            Durability::Durable,
            false,
            false,
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("2 record"),
            "the readable segment's plan must still be printed, got: {text}"
        );
        assert!(
            text.to_lowercase().contains("could not be read"),
            "the unreadable segment must be reported, not silently swallowed, got: {text}"
        );

        // Real run: the readable segment is still fully requeued + deleted;
        // the junk file is left in place and reported (non-zero exit).
        let socket = fake_daemon_socket("unreadable");
        let daemon = FakeDaemon::start(socket.clone());
        let err = cmd_quarantine_requeue(&dir, &socket, Durability::Durable, true, false)
            .expect_err("an unreadable segment must be reported as a problem");
        assert!(err.to_lowercase().contains("could not be read"), "{err}");
        assert!(
            !q.join(good_name).exists(),
            "the readable segment must still be fully requeued and deleted"
        );
        assert!(
            q.join("shard_00__seg_00000000.wab.sealed").exists(),
            "the unreadable junk file must be left in place, not silently deleted or skipped \
             without a trace"
        );
        assert_eq!(
            daemon.into_received(),
            vec![b"real1".to_vec(), b"real2".to_vec()]
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_real_run_reports_skipped_records_even_when_the_segment_is_deleted() {
        // Review round 2, N2: the information asymmetry Important 1 (round 1)
        // was about, left open for skips. The dry run already warns about
        // skipped records; the real run said NOTHING — not even for a
        // segment that ends up fully deleted because its one corrupt record
        // was skipped and everything else was accepted, which is the
        // ORDINARY case (every quarantined segment has at least one skip by
        // construction — that is why it is quarantined at all). An operator
        // running --yes directly, or automation reading only stdout/JSON,
        // learned nothing about the corrupt record that is now permanently
        // gone. This does NOT reopen the delete-on-skip decision (a segment
        // that is otherwise fully readable is still, correctly, deleted) —
        // it only asserts the operator is TOLD.
        let dir = tmp_dir("q_requeue_report_skips");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let name = "shard_00__seg_00000001.wab.sealed";
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);

        let socket = fake_daemon_socket("report_skips");
        let _daemon = FakeDaemon::start(socket.clone());

        let mut out = Vec::new();
        let result =
            quarantine_requeue_write(&dir, &socket, Durability::Durable, true, false, &mut out);
        assert!(
            result.is_ok(),
            "a skip alone must not fail the run — that would make every successful requeue \
             non-zero, since every segment this command DELETES carries a skip by construction \
             (a delete needs a clean end, and a clean end implies a CRC-mismatch quarantine): \
             {result:?}"
        );
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("1 record") && text.to_lowercase().contains("skip"),
            "the real run must report the skipped record, got: {text}"
        );
        assert!(
            text.contains("declared_len 6"),
            "the real run must surface the byte range (not just a count), \"BADREC\" is 6 \
             bytes, got: {text}"
        );
        assert!(
            !q.join(name).exists(),
            "the segment was otherwise fully recoverable and must still be deleted — this \
             test is about being TOLD, not about changing the delete outcome"
        );

        // Same information, structured, in --json — rebuild the fixture since
        // the run above deleted it.
        write_q_segment_with_corruption(&q, name, &[b"good1", b"BADREC", b"good2"], 1);
        let socket2 = fake_daemon_socket("report_skips_json");
        let _daemon2 = FakeDaemon::start(socket2.clone());
        let mut json_out = Vec::new();
        quarantine_requeue_write(
            &dir,
            &socket2,
            Durability::Durable,
            true,
            true,
            &mut json_out,
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
        assert_eq!(v["skipped_records_total"], serde_json::json!(1));
        assert_eq!(
            v["skipped_records"][0]["declared_len"],
            serde_json::json!(6)
        );
        assert_eq!(
            v["skipped_records"][0]["reason"],
            serde_json::json!("CRC mismatch")
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_counts_skips_from_segments_it_does_not_delete() {
        // Review round 3, item 1. The N2 collection sits BEFORE the
        // desynced/empty/delete branching precisely so it fires regardless of a
        // segment's eventual fate — but nothing held that property, and moving
        // the collection down beside `remove_file` (the likeliest refactor here)
        // left every test green while `skipped_records_total` under-reported.
        //
        // Two segments with different fates, each contributing one skip:
        //   seg 1: good / BADREC(payload) / good   -> clean end, DELETED
        //   seg 2: good / BADREC(payload) / BADLEN -> skip then DESYNC, KEPT
        // The total is 2 only if skips are counted for the kept segment too.
        let dir = tmp_dir("q_requeue_skips_across_fates");
        let q = dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        let deleted = "shard_00__seg_00000001.wab.sealed";
        let kept = "shard_00__seg_00000002.wab.sealed";
        write_q_segment_with_corruption(&q, deleted, &[b"good1", b"BADREC", b"good2"], 1);
        write_q_segment_inner(&q, kept, &[b"good3", b"BADREC", b"good4"], Some(1), Some(2));

        let socket = fake_daemon_socket("skips_across_fates");
        let _daemon = FakeDaemon::start(socket.clone());

        let mut json_out = Vec::new();
        // A desynced segment makes the run exit non-zero by design, so the
        // result is deliberately not unwrapped — the JSON is still emitted and
        // is what this test is about.
        let result = quarantine_requeue_write(
            &dir,
            &socket,
            Durability::Durable,
            true,
            true,
            &mut json_out,
        );
        assert!(
            result.is_err(),
            "a desynced segment must still make the run exit non-zero: {result:?}"
        );

        let v: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
        assert_eq!(
            v["skipped_records_total"],
            serde_json::json!(2),
            "one skip from the deleted segment and one from the KEPT desynced segment; \
             counting only deleted segments gives 1. JSON: {v}"
        );
        assert_eq!(
            v["skipped_records"].as_array().map(Vec::len),
            Some(2),
            "both per-record entries must survive, not just the deleted segment's: {v}"
        );

        // The fates really did differ — otherwise this test could pass while
        // pinning nothing about "regardless of fate".
        assert!(
            !q.join(deleted).exists(),
            "the clean-ended segment must have been deleted"
        );
        assert!(
            q.join(kept).exists(),
            "the desynced segment must have been left in place"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn quarantine_requeue_dry_run_json_has_expected_keys() {
        // N3: nothing pinned this shape before — renaming any key left the
        // suite green, unlike `dl requeue`'s JSON or `quarantine list`'s.
        let plan = QuarantineRequeuePlan {
            segments: 3,
            total_records: 10,
            total_skipped: 2,
            segments_desynced: 1,
            unreadable: vec!["bad.wab.sealed: truncated".to_string()],
        };
        let v = round_trip(&quarantine_requeue_dry_run_json(&plan));
        assert_eq!(v["dry_run"], serde_json::json!(true));
        assert_eq!(v["segments"], serde_json::json!(3));
        assert_eq!(v["requeuable_records"], serde_json::json!(10));
        // Review round 3, item 3: `skipped_records_total`, a scalar —
        // matching `done`-JSON's own scalar of the same name, since `done`
        // separately has an ARRAY under the bare `skipped_records` key. Same
        // command, same-looking key with two incompatible types was the wart;
        // this pins the rename so it can't silently drift back.
        assert_eq!(v["skipped_records_total"], serde_json::json!(2));
        assert!(
            v.get("skipped_records").is_none(),
            "the dry-run JSON must not resurrect a bare `skipped_records` key — that name is \
             reserved for `done`-JSON's ARRAY, and a dry run never builds per-record entries"
        );
        assert_eq!(v["segments_desynced"], serde_json::json!(1));
        assert_eq!(v["unreadable_segments"], serde_json::json!(1));
        assert!(v["note"].is_string());
    }

    #[test]
    fn quarantine_requeue_done_json_has_expected_keys() {
        // N3: same trap for the real-run JSON shape — this also pins the new
        // N2 `skipped_records`/`skipped_records_total` fields structurally.
        let outcome = QuarantineRequeueOutcome {
            segments: 5,
            requeued_records: 12,
            segments_cleared: 3,
            segments_desynced_left: 1,
            segments_empty_left: 1,
            unreadable_segments: 1,
            delete_failures: 1,
            skipped_records_total: 2,
            skipped_segments: vec![RequeueSkippedSegment {
                segment: "seg.wab.sealed".to_string(),
                records: vec![SkippedRecord {
                    offset: 24,
                    declared_len: 6,
                    reason: "CRC mismatch".to_string(),
                }],
            }],
        };
        let v = round_trip(&quarantine_requeue_done_json(&outcome, Durability::Durable));
        assert_eq!(v["dry_run"], serde_json::json!(false));
        assert_eq!(v["segments"], serde_json::json!(5));
        assert_eq!(v["requeued_records"], serde_json::json!(12));
        assert_eq!(v["segments_cleared"], serde_json::json!(3));
        assert_eq!(v["segments_desynced_left_in_place"], serde_json::json!(1));
        assert_eq!(v["segments_empty_left_in_place"], serde_json::json!(1));
        assert_eq!(v["unreadable_segments"], serde_json::json!(1));
        assert_eq!(v["delete_failures"], serde_json::json!(1));
        assert_eq!(v["durability"], serde_json::json!("Durable"));
        assert_eq!(v["skipped_records_total"], serde_json::json!(2));
        assert_eq!(
            v["skipped_records"][0]["segment"],
            serde_json::json!("seg.wab.sealed")
        );
        assert_eq!(v["skipped_records"][0]["offset"], serde_json::json!(24));
        assert_eq!(
            v["skipped_records"][0]["declared_len"],
            serde_json::json!(6)
        );
        assert_eq!(
            v["skipped_records"][0]["reason"],
            serde_json::json!("CRC mismatch")
        );
    }
}
