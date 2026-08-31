mod config;
mod drain;
mod metrics;
mod models;
mod queue;
mod sink;
#[cfg(unix)]
mod socket;
mod wab;
mod worker;

/// Shared accept/reject cases for the two independent path-format validators —
/// `socket::validate_socket_path` and `config::validate_path_format_inner`.
/// They are deliberately NOT merged (separate trust-boundary checks in separate
/// layers), so this vector is the drift guard: both test modules run every case
/// through their own validator and must agree. Adding a rule to one validator
/// without the other now fails a test here.
#[cfg(test)]
pub(crate) mod path_validation_test_vectors {
    /// `(path, should_pass)` — cross-platform format rules (absolute, no `..`).
    pub(crate) const CASES: &[(&str, bool)] = &[
        ("/run/weir/weir.sock", true),
        ("/var/lib/weir/data", true),
        ("relative/path.sock", false),
        ("weir.sock", false),
        ("/var/../etc/weir.sock", false),
        ("/run/weir/../weir.sock", false),
    ];

    /// Null-byte cases. The null check is `#[cfg(unix)]` in both validators.
    #[cfg(unix)]
    pub(crate) const UNIX_ONLY_CASES: &[(&str, bool)] = &[("/run/weir\0/x.sock", false)];
}

use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use tracing::{info, warn};

use config::{Config, SinkType};
use drain::DrainConfig;
use models::WorkUnit;
#[cfg(feature = "clickhouse-sink")]
use sink::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
#[cfg(feature = "http-sink")]
use sink::http::{HttpBatchMode, HttpSink, HttpSinkConfig};
#[cfg(feature = "mysql-sink")]
use sink::mysql::{MySqlSink, MySqlSinkConfig};
use sink::noop::NoopSink;
#[cfg(feature = "postgres-sink")]
use sink::postgres::{PostgresSink, PostgresSinkConfig};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn compute_wab_bytes_on_disk(wab_dir: &Path) -> u64 {
    let Ok(dir) = std::fs::read_dir(wab_dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in dir.flatten() {
        let shard_path = entry.path();
        if !shard_path.is_dir() {
            continue;
        }
        // Skip the daemon's reserved subdirs — they aren't shards. dead_letter/
        // (dl_*.wab.sealed) has its own weir_dead_letter_bytes_on_disk gauge, and
        // quarantine/ holds forensic copies (both `.wab` and `.wab.sealed`,
        // depending on whether crash recovery or the drain preserved them; see
        // docs/wab_format.md); counting either here would double-count
        // live-segment bytes (G15, mirrors recovery's skip).
        let dir_name = shard_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if dir_name == "dead_letter" || dir_name == "quarantine" {
            continue;
        }
        let Ok(shard_dir) = std::fs::read_dir(&shard_path) else {
            continue;
        };
        for file in shard_dir.flatten() {
            let fpath = file.path();
            let name = fpath.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".wab") || name.ends_with(".wab.sealed") {
                total += std::fs::metadata(&fpath).map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// Number of 5 s samples the growth warning considers (60 s).
const WAB_GROWTH_WINDOW: usize = 12;

/// How long the WAB growth warning stays quiet after firing. The polling task
/// evaluates the warning every 5 s, so without this it would log 60 times a
/// minute for as long as the condition held.
const WAB_GROWTH_WARN_INTERVAL: Duration = Duration::from_secs(300);

/// Whether the growth warning is allowed to fire again at `now`.
///
/// Extracted from the polling task's closure for the same reason
/// [`should_warn_wab_growth`] and [`drain_is_healthy`] were: it is the third
/// piece of the warning's decision and the only one that was left inline, so it
/// was the only one no test could reach. `saturating_duration_since` rather than
/// `elapsed`/subtraction so a `now` behind `last_warned` reads as "not yet due"
/// instead of panicking.
fn growth_warning_is_due(last_warned: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    match last_warned {
        // Never warned: the first qualifying window fires immediately.
        None => true,
        Some(t) => now.saturating_duration_since(t) >= WAB_GROWTH_WARN_INTERVAL,
    }
}

/// Fold one WAB-size scan into the polling task's state, returning the size on
/// success and `None` when the scan failed to join.
///
/// The `Err` arm lives in here rather than at the call site on purpose. The
/// defect this replaced was `.await.unwrap_or(0)`, which is a call-site
/// substitution — a test of a function that already takes `Option<u64>` would
/// pass with that bug fully reintroduced. Taking the `JoinResult` itself is what
/// makes the test load-bearing.
///
/// A join failure must not be papered over in either direction. Storing 0 reads
/// as "the WAB is empty" and lifts the cap entirely until the next tick, failing
/// open on the one feature whose job is to fail closed; pushing the previous
/// value again reads as "not growing" and suppresses the growth warning for a
/// whole window. So neither the gauge, the shared atomic, nor the window moves —
/// the last known size stands, 5 s stale, which is exactly the staleness the cap
/// is already documented to carry.
fn apply_wab_scan(
    scan: Result<u64, tokio::task::JoinError>,
    samples: &mut Vec<u64>,
    metrics: &crate::metrics::Metrics,
    wab_bytes_now: &std::sync::atomic::AtomicU64,
) -> Option<u64> {
    let bytes = match scan {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                error = %e,
                "WAB byte scan failed to join; keeping the last known size. The \
                 wab_max_bytes cap and weir_wab_bytes_on_disk are stale until the \
                 next tick."
            );
            return None;
        }
    };
    metrics.wab_bytes_on_disk.set(bytes as f64);
    wab_bytes_now.store(bytes, std::sync::atomic::Ordering::Relaxed);
    samples.push(bytes);
    if samples.len() > WAB_GROWTH_WINDOW {
        samples.remove(0);
    }
    Some(bytes)
}

/// Whether to warn that the WAB is growing unbounded.
///
/// All three must hold:
/// 1. no cap is set — with one set, the cap handles it and the warning is noise;
/// 2. the window is full and shows net growth that never gave ground;
/// 3. the sink is unhealthy or the drain is not draining.
///
/// Condition 3 is the one that matters. Sustained growth under a healthy drain
/// is just a fast producer; warning on that teaches operators to ignore the
/// message, which defeats the purpose.
///
/// Condition 2 is deliberately NOT "every step was non-decreasing". A *degraded*
/// sink — one delivering some batches and failing others, which the spec names as
/// a case this must catch — deletes a confirmed segment every so often, and each
/// deletion drops the byte count for one sample. Under a strict per-step rule a
/// single such dip anywhere in the 12-sample window resets the whole detector, so
/// the warning would only ever fire for a *fully* dead sink. The rule here is
/// weaker per-step but just as strong end-to-end: the window must end above where
/// it started and must never have fallen below where it started. A WAB that is
/// genuinely draining dips under its own starting point and still does not warn.
fn should_warn_wab_growth(samples: &[u64], wab_max_bytes: u64, drain_healthy: bool) -> bool {
    if wab_max_bytes != 0 || drain_healthy || samples.len() < WAB_GROWTH_WINDOW {
        return false;
    }
    let window = &samples[samples.len() - WAB_GROWTH_WINDOW..];
    let first = window[0];
    let last = window[window.len() - 1];
    last > first && window.iter().all(|&s| s >= first)
}

/// Reads the live `weir_sink_health` and `weir_drain_state` gauges and
/// composes them into condition 3 of `should_warn_wab_growth`: is the drain
/// healthy right now.
///
/// Returns `true` only when the sink is healthy AND the drain is actively
/// draining. That `&&` is the whole point: condition 3 is "sink unhealthy OR
/// not draining", and by De Morgan that is exactly NOT(healthy AND draining).
/// The negation happens in the caller — `should_warn_wab_growth` takes this
/// value as `drain_healthy` and returns `false` early when it is `true`. So a
/// swapped `&&`/`||` here silently inverts which failures the warning covers.
///
/// Pulled out of the background task's closure (rather than left inline) so this
/// composition — not just the pure predicate — is unit-tested; a swapped
/// `&&`/`||` or a wrong label variant here would previously compile and pass
/// every existing test.
fn drain_is_healthy(metrics: &crate::metrics::Metrics) -> bool {
    // `sink_health` and `drain_state` are Gauge families with the current
    // state's series set to 1.0, so the live value reads back without any new
    // plumbing.
    let sink_healthy = metrics
        .sink_health
        .get_or_create(&crate::metrics::SinkHealthLabel {
            state: crate::metrics::SinkHealthState::healthy,
        })
        .get()
        > 0.0;
    let draining = metrics
        .drain_state
        .get_or_create(&crate::metrics::DrainStateLabel {
            state: crate::metrics::DrainStateValue::draining,
        })
        .get()
        > 0.0;
    sink_healthy && draining
}

// ── TLS SIGHUP reload task ────────────────────────────────────────────────────

/// Spawn a background task that listens for SIGHUP and hot-reloads TLS certs.
///
/// The task is fail-safe: if `reload()` returns `Err`, the old `ServerConfig`
/// is retained and an error is logged. Both outcomes increment the
/// `tls_config_reloads` counter with the appropriate label so operators can
/// alert on sustained reload failures.
#[cfg(feature = "tls")]
fn spawn_tls_reload_task(
    tls: crate::socket::tls::ReloadableServerConfig,
    metrics: std::sync::Arc<crate::metrics::Metrics>,
) {
    use crate::metrics::{TlsReloadLabel, TlsReloadOutcome};
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGHUP handler; cert reload disabled");
                return;
            }
        };
        while hup.recv().await.is_some() {
            match tls.reload() {
                Ok(()) => {
                    metrics
                        .tls_config_reloads
                        .get_or_create(&TlsReloadLabel {
                            outcome: TlsReloadOutcome::ok,
                        })
                        .inc();
                    tracing::info!("SIGHUP: TLS certificates reloaded");
                }
                Err(e) => {
                    metrics
                        .tls_config_reloads
                        .get_or_create(&TlsReloadLabel {
                            outcome: TlsReloadOutcome::failed,
                        })
                        .inc();
                    tracing::error!(error = %e, "SIGHUP: TLS reload failed; keeping previous certs");
                }
            }
        }
    });
}

/// Returns the configured sink URL, or a clear startup error if it's absent.
/// Config validation already guarantees it's set for URL-requiring sinks, so
/// this is belt-and-suspenders — but a `Result` beats a panic on the startup
/// path, and one message replaces five near-identical `.expect` strings.
#[cfg(any(
    feature = "http-sink",
    feature = "mysql-sink",
    feature = "postgres-sink",
    feature = "clickhouse-sink"
))]
fn require_sink_url(
    config: &Config,
    sink_label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    config.sink_url.0.clone().ok_or_else(|| {
        format!("sink_type = {sink_label} requires a sink URL (set --sink-url or WEIR_SINK_URL)")
            .into()
    })
}

/// Wraps a built sink and spawns the drain — the tail shared by every sink arm.
/// `drain::spawn` is generic over the sink but returns the same
/// `JoinHandle<()>`, so each arm produces a uniform handle for the join
/// sequence later in `main`.
fn build_and_spawn_drain<S: sink::Sink + 'static>(
    sink: S,
    drain_rx: crossbeam_channel::Receiver<std::path::PathBuf>,
    drain_config: DrainConfig,
    metrics: Arc<metrics::Metrics>,
) -> std::thread::JoinHandle<()> {
    drain::spawn(drain_rx, Arc::new(sink), drain_config, metrics)
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tighten process umask to 0o077 immediately. Defense-in-depth: every
    // file-creation path in weir specifies its mode bits explicitly today, so
    // the umask is currently a no-op for daemon-created files. The tightening
    // matters for any future code path that forgets to specify a mode — the
    // umask makes "daemon-private" the default rather than "world-readable
    // minus the inherited 0o022 mask".
    //
    // Note: bind_hardened temporarily tightens umask further (to 0o177) for
    // the socket-create syscall and restores the previous value afterwards.
    // That nested tightening sees the value set here as its "previous", so
    // restoration is consistent.
    //
    // Safety: libc::umask is always safe; it swaps the process umask and
    // returns the previous value. No invariants to uphold.
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }

    let (config, config_warnings) = Config::load()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&config.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Replay any advisory warnings collected during Config::load — it runs
    // before this subscriber exists, so emitting them there would silently drop
    // them (a TOML typo would take defaults unnoticed). See Config::load (F54).
    for w in &config_warnings {
        warn!("config: {w}");
    }

    // On macOS the durable-write path uses F_BARRIERFSYNC, which orders writes
    // and survives a process/OS crash but is NOT a guaranteed media flush on
    // sudden power loss (a drive with a volatile write cache can still lose the
    // most recent writes). Surface this once at startup so it isn't a surprise.
    #[cfg(target_os = "macos")]
    warn!(
        "durability note: this is a macOS build — fsync uses F_BARRIERFSYNC, which is \
         NOT power-loss-durable on drives with a volatile write cache. Run production \
         data paths on Linux (fdatasync). See the durability section of the README."
    );

    info!(
        socket = %config.socket_path.display(),
        wab_dir = %config.wab_dir.display(),
        shards = config.shard_count,
        workers = config.worker_count,
        "weir starting"
    );

    // ── Concurrency-vs-cores advisory ─────────────────────────────────────────
    // Each shard runs a flusher thread; each worker runs a worker thread.
    // Together those threads form the "agent" pool that does the real per-
    // record work. Too many agents on too few cores degrades throughput
    // (context-switching costs + shrinks the records-per-fsync ratio because
    // producers can't run concurrently); too few agents leaves storage-side
    // fsync parallelism unused.
    //
    // The recommendation here is advisory only — the operator always
    // overrides via shard_count / worker_count config. It exists because the
    // current defaults (shard_count = 1, worker_count = 2) won't be a good
    // fit for most production-sized machines, and the empirical sweet spot
    // is non-obvious enough that we'd rather surface it than make users
    // discover it by benchmarking.
    advise_agent_count(config.shard_count, config.worker_count);

    // ── Metrics ───────────────────────────────────────────────────────────────

    let (metrics_struct, registry) = metrics::Metrics::new();
    let metrics = Arc::new(metrics_struct);
    let registry = Arc::new(registry);

    // ── Queue ─────────────────────────────────────────────────────────────────

    // One queue partition per worker. Pushes are routed by shard_id so every
    // record for a given shard lands on a single worker's partition — see
    // worker::spawn_workers for the per-shard FIFO invariant.
    let (queue_tx, queue_rx) = queue::new::<WorkUnit>(config.worker_count);

    // ── Drain channel ─────────────────────────────────────────────────────────

    let (drain_tx, drain_rx) = crossbeam_channel::bounded::<std::path::PathBuf>(256);
    // Startup-only sender for replaying sealed-but-unconfirmed segments from a
    // previous run. Replay runs AFTER the drain consumer is spawned (below) so
    // the blocking bounded-channel sends drain live instead of dead-locking the
    // startup thread on a recovery backlog larger than the channel capacity (B3).
    // Dropped immediately after replay so it doesn't keep the channel open at
    // shutdown.
    let replay_tx = drain_tx.clone();

    // ── Coalesce hint: shared EWMA of fsync latency ───────────────────────────
    //
    // Flusher threads update this after each fsync; worker threads read it once
    // per batch to size their coalesce window. Lock-free, Relaxed ordering —
    // it is a heuristic, not a correctness signal.
    //
    // Initial value 200 µs matches the old fixed COALESCE_WINDOW constant so
    // behaviour before the first fsync is unchanged.
    let coalesce_hint = Arc::new(AtomicU64::new(200));

    // ── WAB (one flusher thread per shard) ────────────────────────────────────

    let wab_config = wab::WabConfig {
        shard_count: config.shard_count,
        batch_size: config.batch_size,
        batch_deadline: Duration::from_millis(config.batch_deadline_ms),
        segment_max_bytes: config.wab_segment_max_bytes,
        // 0 = idle-seal disabled (historical behaviour).
        segment_max_age: (config.wab_segment_max_age_secs > 0)
            .then(|| Duration::from_secs(config.wab_segment_max_age_secs)),
        // "none" (default) keeps segments at format v1, byte-identical to weir
        // 1.x. Validated at config load, so any other string is unreachable.
        compression: match config.wab_compression.as_str() {
            "zstd" => weir_wab::format::Compression::Zstd,
            _ => weir_wab::format::Compression::None,
        },
        compression_level: config.wab_compression_level,
    };
    let wab_handle = wab::spawn(
        config.wab_dir.clone(),
        wab_config,
        drain_tx,
        Arc::clone(&metrics),
        Arc::clone(&coalesce_hint),
    )?;

    // ── Workers (queue → per-shard Batch channels → flusher directly) ────────
    //
    // Workers send `Batch`es directly to the WAB flusher via the shard channels
    // returned by `wab::spawn`. No bridge thread: the hop is
    // worker → Batch-channel → flusher.

    let worker_handles = worker::spawn_workers(
        &queue_rx,
        wab_handle.shard_txs,
        config.shard_count,
        config.worker_count,
        config.batch_size,
        Duration::from_millis(config.batch_deadline_ms),
        coalesce_hint,
    );

    // ── Drain ─────────────────────────────────────────────────────────────────

    let drain_config = DrainConfig {
        wab_dir: config.wab_dir.clone(),
        dead_letter_max_bytes: config.dead_letter_max_bytes,
        dead_letter_check_interval: Duration::from_secs(config.dead_letter_check_interval_secs),
        base_retry_delay: Duration::from_millis(config.sink_retry_base_delay_ms),
        max_retries: config.sink_max_retries,
        max_respawns: drain::MAX_DRAIN_RESPAWNS,
        // Backstop >= 60s and >= 2x the sink's own timeout, so it only fires if a
        // sink hangs without honouring its internal timeout.
        commit_timeout: Duration::from_secs(config.sink_timeout_secs.saturating_mul(2).max(60)),
        health_poll_interval: Duration::from_secs(config.health_poll_interval_secs),
        // A health probe is a liveness check, not a delivery, so cap it well below
        // commit_timeout: 5s, or the sink's own timeout if that's shorter.
        health_probe_timeout: Duration::from_secs(config.sink_timeout_secs.min(5)),
    };
    // Surface the configured sink type as a metric so operators (and
    // `weir-ctl metrics`) can see whether records actually go downstream or to
    // the discard-everything noop sink.
    metrics
        .sink_info
        .get_or_create(&metrics::SinkInfoLabel {
            sink_type: config.sink_type.as_str().to_string(),
        })
        .set(1.0);

    // Sink selection. drain::spawn is generic over the sink type but returns
    // the same JoinHandle<()> regardless, so both arms produce a uniform
    // drain_handle for the join sequence later in this function.
    let drain_handle = match config.sink_type {
        SinkType::Noop => {
            warn!(
                "sink: noop — records are acked and DISCARDED, not forwarded anywhere. \
                 This is for soak-testing the pipeline; set --sink-type to deliver downstream."
            );
            build_and_spawn_drain(NoopSink, drain_rx, drain_config, Arc::clone(&metrics))
        }
        #[cfg(feature = "http-sink")]
        SinkType::Http => {
            let url = require_sink_url(&config, "http")?;
            // Bearer token read from env at startup (never from config file).
            // Logged only as a presence boolean — the token itself never reaches
            // a log line.
            let bearer_token = std::env::var("WEIR_SINK_BEARER_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(Arc::from);
            // One-shot startup hint: with no token set the sink POSTs
            // unauthenticated, and the first symptom is otherwise a flood of 401
            // dead-letters at drain time. Gated to the http arm, so it only fires
            // when the http sink is actually selected.
            if bearer_token.is_none() {
                warn!(
                    "sink: http — WEIR_SINK_BEARER_TOKEN is unset; requests will be sent \
                     UNAUTHENTICATED (no Authorization header). If the endpoint requires auth, \
                     set WEIR_SINK_BEARER_TOKEN or expect 401s to dead-letter at drain time."
                );
            }
            // Validated to "none"|"ndjson" at config load; "ndjson" sends the
            // whole batch as one newline-delimited POST, anything else (i.e.
            // "none") keeps the per-record default.
            let batch_mode = if config.sink_http_batch == "ndjson" {
                HttpBatchMode::Ndjson
            } else {
                HttpBatchMode::PerRecord
            };
            // sink_http_concurrency only applies to per-record POSTs; in ndjson
            // mode the whole batch is one POST, so report it as inert rather than
            // logging a misleading "concurrency = 8".
            let concurrency_display = match batch_mode {
                HttpBatchMode::Ndjson => "n/a (ndjson)".to_string(),
                HttpBatchMode::PerRecord => config.sink_http_concurrency.to_string(),
            };
            info!(
                // The URL may carry userinfo (user:password@host); redact the
                // password so it never reaches a log line (S25).
                url = %crate::sink::redact_url_password(&url),
                bearer = bearer_token.is_some(),
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                concurrency = %concurrency_display,
                batch = %config.sink_http_batch,
                "sink: http"
            );
            let http_cfg = HttpSinkConfig {
                url,
                timeout: Duration::from_secs(config.sink_timeout_secs),
                max_batch_size: config.sink_max_batch_size,
                batch_mode,
                bearer_token,
                send_idempotency_key: config.sink_send_idempotency_key,
                concurrency: config.sink_http_concurrency,
            };
            let sink = HttpSink::new(http_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build HTTP sink: {e}"))
            })?;
            build_and_spawn_drain(sink, drain_rx, drain_config, Arc::clone(&metrics))
        }
        #[cfg(feature = "mysql-sink")]
        SinkType::Mysql => {
            let url = require_sink_url(&config, "mysql")?;
            let insert_mode = config.sink_mysql_insert_mode;
            info!(
                // URL omitted from the log line — it contains credentials.
                // The MySqlSink Debug impl redacts the password, but logging
                // even the user component here gives operators no information
                // they can't get from `weir-server --help` plus their config.
                table = %config.sink_mysql_table,
                column = %config.sink_mysql_column,
                insert_mode = ?config.sink_mysql_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: mysql"
            );
            let mysql_cfg = MySqlSinkConfig {
                url,
                table: config.sink_mysql_table.clone(),
                column: config.sink_mysql_column.clone(),
                insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = MySqlSink::new(mysql_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build MySQL sink: {e}"))
            })?;
            build_and_spawn_drain(sink, drain_rx, drain_config, Arc::clone(&metrics))
        }
        #[cfg(feature = "postgres-sink")]
        SinkType::Postgres => {
            let url = require_sink_url(&config, "postgres")?;
            info!(
                // URL omitted from the log line for the same reason as MySQL.
                table = %config.sink_postgres_table,
                column = %config.sink_postgres_column,
                insert_mode = ?config.sink_postgres_insert_mode,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: postgres"
            );
            let pg_cfg = PostgresSinkConfig {
                url,
                table: config.sink_postgres_table.clone(),
                column: config.sink_postgres_column.clone(),
                insert_mode: config.sink_postgres_insert_mode,
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = PostgresSink::new(pg_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build Postgres sink: {e}"))
            })?;
            build_and_spawn_drain(sink, drain_rx, drain_config, Arc::clone(&metrics))
        }
        #[cfg(feature = "clickhouse-sink")]
        SinkType::ClickHouse => {
            let url = require_sink_url(&config, "clickhouse")?;
            info!(
                // URL omitted from the log line — it may carry credentials.
                database = %config.sink_clickhouse_database,
                table = %config.sink_clickhouse_table,
                column = %config.sink_clickhouse_column,
                timeout_secs = config.sink_timeout_secs,
                max_batch_size = config.sink_max_batch_size,
                "sink: clickhouse"
            );
            let ch_cfg = ClickHouseSinkConfig {
                url,
                database: config.sink_clickhouse_database.clone(),
                table: config.sink_clickhouse_table.clone(),
                column: config.sink_clickhouse_column.clone(),
                max_batch_size: config.sink_max_batch_size,
                timeout: Duration::from_secs(config.sink_timeout_secs),
            };
            let sink = ClickHouseSink::new(ch_cfg).map_err(|e| {
                Box::<dyn std::error::Error>::from(format!("failed to build ClickHouse sink: {e}"))
            })?;
            build_and_spawn_drain(sink, drain_rx, drain_config, Arc::clone(&metrics))
        }
    };

    // ── Replay recovery backlog (drain consumer is now live) ──────────────────
    //
    // Replay sealed-but-unconfirmed segments from a previous run now that the
    // drain thread is consuming. Done here rather than inside wab::spawn so the
    // blocking sends into the bounded drain channel are drained live — a backlog
    // larger than the channel capacity would otherwise dead-lock startup (B3).
    // Runs before the socket binds, so the recovery backlog is queued ahead of
    // any newly-accepted traffic.
    wab::replay_unconfirmed(&config.wab_dir, config.shard_count, &replay_tx, &metrics)?;
    drop(replay_tx); // release the startup sender so shutdown can close the drain channel

    // ── Tokio runtime: socket accept loop + metrics server ────────────────────

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        // Bind metrics listener before starting the socket loop. Default
        // metrics_bind is 127.0.0.1 — see Config::metrics_bind for the
        // security reasoning around exposing this surface on the network.
        let metrics_listener =
            tokio::net::TcpListener::bind((config.metrics_bind, config.metrics_port))
                .await
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to bind metrics endpoint to {}:{} ({e}) — another process \
                             (often another weir-server instance) is using that port. Change it \
                             with --metrics-port / WEIR_METRICS_PORT (or --metrics-bind / \
                             WEIR_METRICS_BIND).",
                            config.metrics_bind, config.metrics_port
                        ),
                    )
                })?;
        metrics::server::spawn(
            metrics_listener,
            Arc::clone(&registry),
            config.metrics_max_connections,
        );

        info!(
            bind = %config.metrics_bind,
            port = config.metrics_port,
            max_connections = config.metrics_max_connections,
            "metrics endpoint listening"
        );

        // Background: poll queue depth every 500 ms.
        let queue_tx_bg = queue_tx.clone();
        let metrics_q = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                metrics_q.queue_depth.set(queue_tx_bg.len() as f64);
            }
        });

        // Background: scan WAB shard dirs for on-disk byte usage every 5 s.
        // The scan is synchronous I/O (read_dir + metadata per entry) so it
        // runs inside spawn_blocking — putting it directly on a tokio worker
        // would stall the runtime in proportion to the number of segments.
        //
        // Shared with the connection handlers so the ingest path can consult the
        // WAB size without doing any I/O of its own — this task already walks
        // the directory for the gauge, so the value is free.
        let wab_bytes_now = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Whether the WAB cap is currently rejecting pushes. Created ONCE here
        // and cloned into every listener so the hysteresis low-water mark is
        // daemon-wide: a per-listener flag would let the Unix and TCP sides
        // disagree about whether ingest has resumed.
        let wab_cap_rejecting = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let wab_dir_bg = config.wab_dir.clone();
        let metrics_w = Arc::clone(&metrics);
        let wab_bytes_bg = Arc::clone(&wab_bytes_now);
        // Captured before the task is spawned so the growth-warning predicate
        // sees the operator's configured cap without reaching back into `config`.
        let cap_for_warning = config.wab_max_bytes;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            // Rolling window of on-disk-byte samples feeding should_warn_wab_growth,
            // and the last time that warning actually fired (rate-limited below).
            let mut samples: Vec<u64> = Vec::with_capacity(WAB_GROWTH_WINDOW + 1);
            let mut last_warned: Option<std::time::Instant> = None;
            loop {
                interval.tick().await;
                let wab_dir = wab_dir_bg.clone();
                let scan = tokio::task::spawn_blocking(move || {
                    compute_wab_bytes_on_disk(&wab_dir)
                })
                .await;
                // On a join failure the last known size stands and this tick
                // contributes no sample — see `apply_wab_scan` for why neither
                // 0 nor a repeated value is safe here.
                let Some(bytes) =
                    apply_wab_scan(scan, &mut samples, &metrics_w, &wab_bytes_bg)
                else {
                    continue;
                };
                let drain_healthy = drain_is_healthy(&metrics_w);
                let due = growth_warning_is_due(last_warned, std::time::Instant::now());
                if due && should_warn_wab_growth(&samples, cap_for_warning, drain_healthy) {
                    warn!(
                        wab_bytes = bytes,
                        growth_bytes = bytes.saturating_sub(samples[0]),
                        window_secs = WAB_GROWTH_WINDOW * 5,
                        "WAB is growing while the sink is not healthy and no wab_max_bytes \
                         is set — producers are still being acked into an unbounded buffer. \
                         Set wab_max_bytes to bound it."
                    );
                    last_warned = Some(std::time::Instant::now());
                }
            }
        });

        // Shutdown coordination: signal handler → shutdown_tx → socket::run exits.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // ONE shared connection-cap semaphore for ALL listeners (Unix + TCP).
        // Both listeners clone this Arc, so the COMBINED cap across all transports
        // is exactly max_connections — not 2×max_connections as it would be with
        // two independent semaphores.
        let conn_sem = Arc::new(tokio::sync::Semaphore::new(config.max_connections));

        // Optional TCP+mTLS listener (feature = "tls"). It runs its own accept
        // loop concurrently with the Unix loop, feeding the SAME pipeline via a
        // cloned queue_tx. Both listeners share `conn_sem` above so the combined
        // connection cap is the single global max_connections value.
        //
        // `tcp_shutdown_tx` is fired by the same signal handler as the Unix
        // loop's `shutdown_tx` so SIGTERM/Ctrl-C drains both listeners.
        #[cfg(feature = "tls")]
        let (tcp_shutdown_tx, tcp_join) = {
            use socket::tcp::{self, TcpConfig};
            use socket::tls::ReloadableServerConfig;

            match config.tcp_bind {
                Some(bind_addr) => {
                    // Config validation guarantees the three tls_* paths are
                    // present whenever tcp_bind is set, so these expects can't
                    // fire on a validated Config.
                    let cert = config
                        .tls_cert_path
                        .clone()
                        .expect("config validation guarantees tls_cert_path when tcp_bind is set");
                    let key = config
                        .tls_key_path
                        .clone()
                        .expect("config validation guarantees tls_key_path when tcp_bind is set");
                    let ca = config.tls_client_ca_path.clone().expect(
                        "config validation guarantees tls_client_ca_path when tcp_bind is set",
                    );

                    let tls = ReloadableServerConfig::load(cert, key, ca).map_err(|e| {
                        std::io::Error::other(format!("failed to load TLS config: {e}"))
                    })?;

                    // Bind in the caller so a bad bind address fails startup
                    // (rather than the spawned accept task) — see socket::tcp
                    // module docs on the bound-addr design.
                    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
                    let actual_addr = listener.local_addr()?;
                    info!(addr = %actual_addr, "TCP+mTLS listener bound");

                    let tcp_config = TcpConfig {
                        max_connections: config.max_connections,
                        max_payload_bytes: config.max_payload_bytes,
                        shard_count: config.shard_count,
                        shutdown_timeout_secs: config.shutdown_timeout_secs,
                        connection_read_timeout_secs: config.connection_read_timeout_secs,
                        handshake_timeout_secs: config.tls_handshake_timeout_secs,
                        wab_max_bytes: config.wab_max_bytes,
                        wab_bytes_now: Arc::clone(&wab_bytes_now),
                        wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
                    };

                    let (tcp_shutdown_tx, tcp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                    let tcp_queue_tx = queue_tx.clone();
                    let tcp_metrics = Arc::clone(&metrics);

                    // Spawn SIGHUP reload task before moving `tls` into tcp::run.
                    // The clone shares the same inner ArcSwap so a reload from the
                    // SIGHUP task is immediately visible to the accept loop's
                    // `tls.current()` call.
                    spawn_tls_reload_task(tls.clone(), Arc::clone(&metrics));

                    // Pass a clone of the shared semaphore so Unix + TCP draw
                    // from the same permit pool — true global cap.
                    let tcp_sem = Arc::clone(&conn_sem);
                    let tcp_join = tokio::spawn(async move {
                        // tcp::run owns the handler-shutdown watch internally and
                        // signals handlers BEFORE draining — no extra plumbing needed here.
                        let res = tcp::run(
                            tcp_config,
                            listener,
                            tls,
                            tcp_queue_tx,
                            tcp_sem,
                            tcp_shutdown_rx,
                            tcp_metrics,
                        )
                        .await;
                        if let Err(e) = res {
                            tracing::error!(error = %e, "TCP+mTLS listener exited with error");
                        }
                    });

                    (Some(tcp_shutdown_tx), Some(tcp_join))
                }
                None => (None, None),
            }
        };

        tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut sigterm = tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )
                .expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = sigterm.recv() => { info!("received SIGTERM, initiating shutdown"); }
                    _ = tokio::signal::ctrl_c() => { info!("received Ctrl-C, initiating shutdown"); }
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl-C handler");
                info!("received Ctrl-C, initiating shutdown");
            }
            #[cfg(feature = "tls")]
            if let Some(tx) = tcp_shutdown_tx {
                let _ = tx.send(());
            }
            let _ = shutdown_tx.send(());
        });

        // Socket accept loop — blocks until shutdown signal.
        #[cfg(unix)]
        {
            use socket::SocketConfig;
            let socket_config = SocketConfig {
                socket_path: config.socket_path.clone(),
                max_connections: config.max_connections,
                max_payload_bytes: config.max_payload_bytes,
                shutdown_timeout_secs: config.shutdown_timeout_secs,
                connection_read_timeout_secs: config.connection_read_timeout_secs,
                shard_count: config.shard_count,
                peer_uid_check: config.peer_uid_check,
                wab_max_bytes: config.wab_max_bytes,
                wab_bytes_now: Arc::clone(&wab_bytes_now),
                wab_cap_rejecting: Arc::clone(&wab_cap_rejecting),
            };
            socket::run(socket_config, queue_tx, shutdown_rx, Arc::clone(&metrics), conn_sem).await?;
        }
        #[cfg(not(unix))]
        {
            // On non-Unix builds weir-server is not supported; just wait for shutdown.
            let _ = shutdown_rx.await;
        }

        // Await the TCP+mTLS listener's graceful drain before leaving the runtime.
        // The signal handler already fired `tcp_shutdown_tx`, so `tcp::run` is
        // draining its in-flight connections; without this await `drop(rt)` below
        // would cancel that drain the moment the Unix loop returns (e.g. when there
        // are no Unix connections), severing live TCP connections instead of
        // letting them finish within `shutdown_timeout_secs` (S09).
        #[cfg(feature = "tls")]
        if let Some(h) = tcp_join {
            let _ = h.await;
        }

        Ok::<(), std::io::Error>(())
    })?;

    // ── Graceful pipeline drain ───────────────────────────────────────────────
    //
    // queue_tx moved into socket::run and dropped when it returns.
    // Workers observe Disconnected → flush remaining Batches → drop shard_txs
    //   clones → flusher Disconnected.
    // WAB flushers observe Disconnected → seal segments → exit → drop drain_tx.
    // Drain thread observes drain_rx Disconnected → drains pending segments → exits.
    //
    // The queue-depth background task holds a `queue_tx.clone()`. Dropping the
    // runtime aborts that task so the clone is released — otherwise workers
    // never observe `Disconnected` and `worker_handles.join()` deadlocks.
    drop(rt);

    info!("socket layer shut down; waiting for pipeline to drain");

    for h in worker_handles {
        h.join().ok();
    }
    for h in wab_handle.join_handles {
        h.join().ok();
    }
    drain_handle.join().ok();

    info!("weir shut down cleanly");
    Ok(())
}

/// Logs a recommendation if `shard_count` / `worker_count` look unusual
/// for the host's core count. Advisory only — operator config wins.
///
/// **Empirical basis.** A sweep on a 4-core sandbox (herd of 64 producers
/// × Durable records, `tests/load.rs::sweep_agent_count_vs_throughput`) showed:
///
/// | agent_count | median RPS  | vs cores |
/// |-------------|-------------|----------|
/// | 1           | 29 k (peak) | 0.25     |
/// | 2           | 26 k        | 0.50     |
/// | 3           | 23 k (low)  | 0.75     |
/// | 4 (default) | 25 k        | 1.00     |
/// | 6           | 25 k        | 1.50     |
/// | 8           | 25 k        | 2.00     |
///
/// Why the peak is so low: each agent = one worker thread + one flusher
/// thread, and on a 4-core machine those threads compete with the tokio
/// runtime workers and the accept loop. Fewer agents also means a fatter
/// group fsync (more concurrent producers' records share one batch),
/// which is the dominant win on Durable workloads.
///
/// Heuristic: reserve ~2 cores for the tokio runtime / accept loop / OS,
/// give each remaining core a 2-thread budget for one agent. So
/// `recommended = max(1, (cores - 2) / 2)`. Validated at 4 cores; should
/// extrapolate sensibly but isn't proven on high-core production hardware.
fn advise_agent_count(shard_count: usize, worker_count: usize) {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    if cores == 0 {
        return; // Couldn't probe the system; skip the advisory.
    }

    let recommended = recommended_agent_count(cores);
    // Use the larger of the two as the "agent count" proxy because each
    // contributes one OS thread per unit.
    let actual = shard_count.max(worker_count);

    if actual > 2 * recommended {
        tracing::warn!(
            cores,
            shard_count,
            worker_count,
            recommended_agent_count = recommended,
            "shard_count/worker_count is significantly above the recommended value for this \
             core count. Each agent uses a worker thread + a flusher thread; on machines \
             where 2 × agent_count > cores you'll likely see CPU contention reduce throughput \
             and shrink the records-per-fsync ratio. This is advisory — override via config \
             if you've measured your workload."
        );
    } else if recommended >= 4 && actual * 4 < recommended {
        tracing::info!(
            cores,
            shard_count,
            worker_count,
            recommended_agent_count = recommended,
            "shard_count/worker_count is below the recommended agent count for this core \
             count. This is fine and often optimal: shard_count is a parallelism knob, not a \
             throughput dial. Raising it helps only with many concurrent producers to spread \
             across shards AND parallel-fsync-capable storage (NVMe RAID, virtualised block \
             devices); with few connections more shards collapse group-fsync batching and can \
             REDUCE throughput (the relationship is non-monotonic). The real durable-throughput \
             lever is connection density into a few shards. Advisory only — see the shard_count \
             tuning notes in docs/operations/configuration.md."
        );
    }
}

/// Recommended `agent_count` (= shard_count = worker_count for a balanced
/// config) given the host's logical core count. See [`advise_agent_count`]
/// for the empirical derivation.
fn recommended_agent_count(cores: usize) -> usize {
    // Reserve ~2 cores for tokio runtime + accept + OS; budget each
    // remaining core for one agent's two threads (worker + flusher).
    cores.saturating_sub(2).max(2) / 2
}

#[cfg(test)]
mod tuning_tests {
    use super::recommended_agent_count;

    #[test]
    fn recommended_agent_count_scales_with_cores() {
        // Minimum is 1 — single-core hosts still need an agent.
        assert_eq!(recommended_agent_count(1), 1);
        assert_eq!(recommended_agent_count(2), 1);
        // 4-core sandbox peak in the sweep was at agent_count=1.
        assert_eq!(recommended_agent_count(4), 1);
        // 8-core: 6 cores available for agents / 2 threads each = 3.
        assert_eq!(recommended_agent_count(8), 3);
        // 16-core: 14 / 2 = 7.
        assert_eq!(recommended_agent_count(16), 7);
        // 32-core: 30 / 2 = 15.
        assert_eq!(recommended_agent_count(32), 15);
    }
}

#[cfg(test)]
mod wab_bytes_tests {
    use super::compute_wab_bytes_on_disk;

    /// G15: the gauge counts only live shard-segment bytes — the dead_letter/ and
    /// quarantine/ reserved subdirs (which carry .wab.sealed files too) must be
    /// skipped, or their bytes would be double-counted against their own gauges.
    #[test]
    fn compute_wab_bytes_skips_dead_letter_and_quarantine() {
        let root = std::env::temp_dir().join(format!("weir_g15_{}", std::process::id()));
        let shard = root.join("shard_00");
        let dl = root.join("dead_letter");
        let q = root.join("quarantine");
        for d in [&shard, &dl, &q] {
            std::fs::create_dir_all(d).unwrap();
        }
        // 100 live shard bytes; 999 in dead_letter; 999 in quarantine.
        std::fs::write(shard.join("seg_00000001.wab.sealed"), vec![0u8; 100]).unwrap();
        std::fs::write(dl.join("dl_00000001.wab.sealed"), vec![0u8; 999]).unwrap();
        std::fs::write(q.join("shard_00__seg_00000001.wab.sealed"), vec![0u8; 999]).unwrap();

        assert_eq!(
            compute_wab_bytes_on_disk(&root),
            100,
            "only live shard segments count; dead_letter + quarantine are skipped"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::{WAB_GROWTH_WARN_INTERVAL, growth_warning_is_due, should_warn_wab_growth};
    use std::time::{Duration, Instant};

    /// Spec §7: "rate limiting holds across repeated polls." The warning's other
    /// two inputs were extracted for unit testing; this one stayed inline in the
    /// polling closure and so went untested until now.
    #[test]
    fn growth_warning_rate_limit_gates_on_the_full_interval() {
        let base = Instant::now();
        assert!(
            growth_warning_is_due(None, base),
            "the first qualifying window must warn immediately"
        );
        assert!(
            !growth_warning_is_due(Some(base), base),
            "a warning just emitted must not repeat on the same instant"
        );
        assert!(
            !growth_warning_is_due(
                Some(base),
                base + WAB_GROWTH_WARN_INTERVAL - Duration::from_secs(1)
            ),
            "one second short of the interval is not due"
        );
        assert!(
            growth_warning_is_due(Some(base), base + WAB_GROWTH_WARN_INTERVAL),
            "exactly the interval is due"
        );
        assert!(growth_warning_is_due(
            Some(base),
            base + WAB_GROWTH_WARN_INTERVAL * 10
        ));
    }

    /// The shape the rate limit actually has to survive: the task polls every
    /// 5 s, and the warning conditions can stay satisfied for hours. Ten minutes
    /// of ticks with the condition permanently true must produce three warnings
    /// (t=0, t=300, t=600), not 121.
    #[test]
    fn growth_warning_rate_limit_holds_across_repeated_polls() {
        let base = Instant::now();
        let mut last_warned: Option<Instant> = None;
        let mut fired = 0u32;
        for tick in 0..=120u64 {
            let now = base + Duration::from_secs(tick * 5);
            if growth_warning_is_due(last_warned, now) {
                fired += 1;
                last_warned = Some(now);
            }
        }
        assert_eq!(
            fired, 3,
            "600 s of 5 s polls with the condition always true must warn at \
             t=0/300/600 only"
        );
    }

    #[test]
    fn growth_warning_fires_only_when_all_three_conditions_hold() {
        // Growing + unhealthy + no cap => warn.
        assert!(should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            0,
            false,
        ));
        // Growing + HEALTHY => no warning. This is the condition that keeps the
        // message trustworthy: a fast producer must not trigger it.
        assert!(!should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            0,
            true,
        ));
        // Growing + unhealthy but a cap IS set => no warning; the cap handles it.
        assert!(!should_warn_wab_growth(
            &[
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            64 * 1024 * 1024,
            false,
        ));
        // Flat or shrinking => no warning.
        assert!(!should_warn_wab_growth(
            &[500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500],
            0,
            false,
        ));
        assert!(!should_warn_wab_growth(
            &[
                1200, 1100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100
            ],
            0,
            false,
        ));
        // Not enough samples yet => no warning.
        assert!(!should_warn_wab_growth(&[100, 200, 300], 0, false));
    }

    /// The shape a *degraded* sink produces: net growth interrupted by dips
    /// wherever a batch did get delivered and its segment was confirmed and
    /// deleted. The spec names "slow or degraded sink" as a case this must
    /// catch, and a strict per-step non-decrease rule never fires on it — one
    /// dip anywhere in the window resets the detector.
    #[test]
    fn growth_warning_fires_on_a_dipping_but_net_growing_window() {
        assert!(should_warn_wab_growth(
            &[
                100, 300, 250, 500, 420, 700, 650, 900, 850, 1100, 1000, 1300
            ],
            0,
            false,
        ));
        // A dip exactly back to the starting value is still not giving ground.
        assert!(should_warn_wab_growth(
            &[100, 200, 100, 300, 200, 400, 300, 500, 400, 600, 500, 700],
            0,
            false,
        ));
    }

    /// The counter-case that keeps the weaker rule honest: a WAB that is really
    /// draining goes BELOW where the window started, and must not warn however
    /// high it ends up. Without this, "net growth" alone would fire on a sawtooth
    /// that is comfortably keeping up.
    #[test]
    fn growth_warning_stays_silent_when_the_window_dips_below_its_start() {
        assert!(!should_warn_wab_growth(
            &[
                1000, 1200, 900, 1400, 800, 1600, 700, 1800, 600, 2000, 500, 2200
            ],
            0,
            false,
        ));
        // A single sample one byte under the start is enough to disqualify it.
        assert!(!should_warn_wab_growth(
            &[100, 200, 300, 400, 500, 99, 700, 800, 900, 1000, 1100, 1200],
            0,
            false,
        ));
    }
}

#[cfg(test)]
mod drain_is_healthy_tests {
    use super::drain_is_healthy;
    use crate::metrics::{
        DrainStateLabel, DrainStateValue, Metrics, SinkHealthLabel, SinkHealthState,
    };

    /// Drives the `sink_health` gauge family exactly the way
    /// `drain::set_sink_health` does in production: one-hot, active state at
    /// 1.0, the rest 0.0.
    fn set_sink_health(m: &Metrics, active: SinkHealthState) {
        for s in [
            SinkHealthState::healthy,
            SinkHealthState::degraded,
            SinkHealthState::down,
        ] {
            let v = if s == active { 1.0 } else { 0.0 };
            m.sink_health
                .get_or_create(&SinkHealthLabel { state: s })
                .set(v);
        }
    }

    /// Drives the `drain_state` gauge family exactly the way
    /// `drain::set_drain_state` does in production: one-hot, active state at
    /// 1.0, the rest 0.0.
    fn set_drain_state(m: &Metrics, active: DrainStateValue) {
        for s in [
            DrainStateValue::draining,
            DrainStateValue::retrying_transient,
            DrainStateValue::blocked_dead_letter_full,
            DrainStateValue::stopped,
        ] {
            let v = if s == active { 1.0 } else { 0.0 };
            m.drain_state
                .get_or_create(&DrainStateLabel { state: s })
                .set(v);
        }
    }

    #[test]
    fn sink_healthy_and_draining_is_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::draining);
        assert!(
            drain_is_healthy(&m),
            "healthy sink + actively draining must compose to healthy (no warning)"
        );
    }

    #[test]
    fn sink_healthy_but_not_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::retrying_transient);
        assert!(
            !drain_is_healthy(&m),
            "a healthy sink that isn't actively draining must not compose to healthy"
        );
    }

    #[test]
    fn sink_unhealthy_but_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::down);
        set_drain_state(&m, DrainStateValue::draining);
        assert!(
            !drain_is_healthy(&m),
            "an unhealthy sink must not compose to healthy even while draining"
        );
    }

    #[test]
    fn sink_unhealthy_and_not_draining_is_not_healthy() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::down);
        set_drain_state(&m, DrainStateValue::blocked_dead_letter_full);
        assert!(!drain_is_healthy(&m));
    }

    /// The case that made the growth warning structurally unable to fire: the
    /// drain thread is gone, so nothing updates these gauges any more. The
    /// supervisor now publishes `stopped` on its way out, and a stopped drain
    /// must not read as healthy however healthy the *sink* last looked — that
    /// last reading is stale by definition once nobody is probing it.
    #[test]
    fn stopped_drain_is_not_healthy_even_with_a_healthy_sink() {
        let m = Metrics::new().0;
        set_sink_health(&m, SinkHealthState::healthy);
        set_drain_state(&m, DrainStateValue::stopped);
        assert!(
            !drain_is_healthy(&m),
            "a drain that has exited must never compose to healthy — this is what \
             lets the WAB growth warning fire when delivery has stopped for good"
        );
    }
}

#[cfg(test)]
mod apply_wab_scan_tests {
    use super::{WAB_GROWTH_WINDOW, apply_wab_scan};
    use crate::metrics::Metrics;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A real `JoinError`. Aborting a task is the only way to construct one —
    /// the type has no public constructor — and it is also the realistic cause:
    /// the blocking scan is cancelled at runtime shutdown.
    async fn join_error() -> tokio::task::JoinError {
        let h = tokio::spawn(async { std::future::pending::<()>().await });
        h.abort();
        h.await.expect_err("an aborted task must yield a JoinError")
    }

    #[tokio::test]
    async fn failed_scan_leaves_the_cap_reading_the_last_known_size() {
        let m = Metrics::new().0;
        let mut samples = vec![4_096];
        // What the cap actually reads. If a failed scan ever writes 0 here, the
        // cap is lifted entirely until the next tick — the fail-open this test
        // exists to prevent.
        let live = AtomicU64::new(4_096);

        let out = apply_wab_scan(Err(join_error().await), &mut samples, &m, &live);

        assert_eq!(out, None, "a failed scan yields no size");
        assert_eq!(
            live.load(Ordering::Relaxed),
            4_096,
            "a failed scan must not move the value the WAB cap is checked against"
        );
        assert_eq!(
            samples,
            vec![4_096],
            "a failed scan must not push a sample — a repeated value reads as \
             'not growing' and would suppress the growth warning for a window"
        );
    }

    #[tokio::test]
    async fn successful_scan_publishes_the_size_and_extends_the_window() {
        let m = Metrics::new().0;
        let mut samples = Vec::new();
        let live = AtomicU64::new(0);

        let out = apply_wab_scan(Ok(8_192), &mut samples, &m, &live);

        assert_eq!(out, Some(8_192));
        assert_eq!(live.load(Ordering::Relaxed), 8_192);
        assert_eq!(samples, vec![8_192]);
    }

    #[tokio::test]
    async fn the_window_is_bounded_and_drops_from_the_front() {
        let m = Metrics::new().0;
        let mut samples = Vec::new();
        let live = AtomicU64::new(0);

        // One more than the window holds, so the oldest must fall off.
        for i in 0..=WAB_GROWTH_WINDOW as u64 {
            apply_wab_scan(Ok(i), &mut samples, &m, &live);
        }

        assert_eq!(samples.len(), WAB_GROWTH_WINDOW);
        assert_eq!(
            samples[0], 1,
            "the oldest sample is evicted, not the newest"
        );
        assert_eq!(samples[samples.len() - 1], WAB_GROWTH_WINDOW as u64);
    }
}
